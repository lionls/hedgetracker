"""Flow behaviour end to end, with the SEC network replaced by fixtures."""

from __future__ import annotations

from dataclasses import dataclass, replace

import pandas as pd
import pyarrow.parquet as pq
import pytest

from hedgetracker import data, settings, storage
from hedgetracker.flows import sec_13f
from hedgetracker.schema import ARROW_SCHEMA


@dataclass(frozen=True)
class FakeReport:
    """Stands in for ``edgar.thirteenf.ThirteenF``."""

    infotable: pd.DataFrame | None
    report_period: str
    accession_number: str


@dataclass(frozen=True)
class FakeAttachment:
    """Stands in for ``edgar.attachments.Attachment``."""

    document: str = "infotable.xml"
    content: str | None = "<informationTable/>"

    def download(self) -> str | None:
        return self.content


@dataclass(frozen=True)
class FakeMatch:
    """Result of a selector query: the information-table documents that matched."""

    documents: tuple[FakeAttachment, ...]

    def __len__(self) -> int:
        return len(self.documents)

    def get_by_index(self, index: int) -> FakeAttachment:
        return self.documents[index]


@dataclass(frozen=True)
class FakeAttachments:
    """Stands in for ``edgar.attachments.Attachments``: a filing's document index.

    ``documents`` are the information-table documents the selectors match;
    ``index_size`` overrides how many documents the submission claims to hold. An
    empty index is how edgartools represents a submission it could not retrieve,
    while a populated index that matches no information table is a filing that
    reports no positions.
    """

    documents: tuple[FakeAttachment, ...] = ()
    index_size: int | None = None

    def __len__(self) -> int:
        return len(self.documents) if self.index_size is None else self.index_size

    def query(self, selector: str) -> FakeMatch:
        return FakeMatch(self.documents)


#: A submission that indexes exactly one information table document.
INDEXED_TABLE = FakeAttachments((FakeAttachment(),))

#: A submission whose documents could not be retrieved.
UNREADABLE_SUBMISSION = FakeAttachments()


@dataclass(frozen=True)
class FakeFiling:
    """Stands in for ``edgar.Filing``, so the real ``data.holdings_frame`` runs."""

    cik: int
    company: str
    filing_date: str
    accession_no: str
    report: FakeReport
    form: str = "13F-HR"
    attachments: FakeAttachments = INDEXED_TABLE

    def obj(self) -> FakeReport:
        return self.report


def _filing(
    cik: int,
    accession_no: str,
    report_period: str,
    infotable,
    attachments: FakeAttachments = INDEXED_TABLE,
) -> FakeFiling:
    return FakeFiling(
        cik=cik,
        company=f"FAKE FUND {cik}",
        filing_date="2024-08-14",
        accession_no=accession_no,
        report=FakeReport(
            infotable=infotable, report_period=report_period, accession_number=accession_no
        ),
        attachments=attachments,
    )


@pytest.fixture
def sec_edgar(monkeypatch, infotable):
    """Replace the EDGAR client: two Q3 2024 filings reporting on different quarters, one empty."""
    filings = [
        _filing(1661222, "0001661222-24-000003", "2024-06-30", infotable()),
        _filing(
            1067983,
            "0001067983-24-000009",
            "2023-12-31",
            infotable(Issuer="APPLE INC", Cusip="037833100"),
        ),
        _filing(2034595, "0002034595-24-000001", "2024-06-30", None),
    ]
    identities: list[str] = []
    monkeypatch.setattr(data, "configure_identity", identities.append)
    monkeypatch.setattr(data, "quarterly_13f_filings", lambda year, quarter: (filings, 7))
    return identities


def test_extracts_every_filing_and_reports_a_summary(tmp_path, sec_edgar):
    summary = sec_13f.extract_quarterly_13f(
        user_email=settings.sec_identity_email(), year=2024, quarter=3, base_dir=tmp_path
    )

    assert summary == {
        "year": 2024,
        "quarter": 3,
        "base_dir": str(tmp_path),
        "filings": 3,
        "amendments_skipped": 7,
        "written": 2,
        "skipped_no_holdings": 1,
        "holdings_rows": 2,
    }
    assert sec_edgar == [settings.sec_identity_email()]


def test_partitions_follow_the_report_period_not_the_index_window(tmp_path, sec_edgar):
    sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    lake = tmp_path / "13f_holdings"
    assert (lake / "year=2024" / "quarter=2" / "0001661222.parquet").exists()
    # Filed during Q3 2024, but it reports on Q4 2023.
    assert (lake / "year=2023" / "quarter=4" / "0001067983.parquet").exists()
    # The filing without holdings produced no file at all.
    assert not (lake / "year=2024" / "quarter=2" / "0002034595.parquet").exists()


def test_written_files_conform_to_the_schema_and_to_their_partition(tmp_path, sec_edgar):
    sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    files = sorted(tmp_path.rglob("*.parquet"))
    assert len(files) == 2
    for path in files:
        assert pq.read_schema(path) == ARROW_SCHEMA
        holdings = pq.read_table(path).to_pandas()
        assert (holdings["cik"] == path.stem).all()
        year, quarter = storage.year_quarter(holdings["report_period"].iloc[0])
        assert path.parent.parent.name == f"year={year}"
        assert path.parent.name == f"quarter={quarter}"


def test_rerunning_the_same_window_changes_nothing(tmp_path, sec_edgar):
    first = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )
    before = {path: pq.read_table(path).to_pandas() for path in sorted(tmp_path.rglob("*.parquet"))}

    second = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    assert first == second
    after = {path: pq.read_table(path).to_pandas() for path in sorted(tmp_path.rglob("*.parquet"))}
    assert set(after) == set(before)
    for path, holdings in after.items():
        assert holdings.equals(before[path])


def test_limit_caps_how_many_filings_are_extracted(tmp_path, sec_edgar):
    summary = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path, limit=1
    )

    assert summary["filings"] == 1
    assert summary["written"] == 1
    assert len(list(tmp_path.rglob("*.parquet"))) == 1


def test_a_permanently_failing_filing_fails_the_run_after_writing_the_rest(
    tmp_path, monkeypatch, sec_edgar
):
    """Every other filing must still reach the lake before the run reports failure."""
    from prefect import task

    real_extract = sec_13f.extract_13f_holdings.fn

    @task(name="extract-13f-holdings")
    def failing_extract(filing, base_dir):
        if filing.cik == 1067983:
            raise RuntimeError("SEC returned no usable document")
        return real_extract(filing, base_dir)

    monkeypatch.setattr(sec_13f, "extract_13f_holdings", failing_extract)

    with pytest.raises(RuntimeError, match="1 of 3 13F-HR filings failed"):
        sec_13f.extract_quarterly_13f(
            user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
        )

    assert (tmp_path / "13f_holdings" / "year=2024" / "quarter=2" / "0001661222.parquet").exists()


def test_an_unreadable_submission_is_retried_on_a_fresh_handle(tmp_path, monkeypatch, infotable):
    """A submission the SEC refused must be re-read, not recorded as having no holdings.

    edgartools caches its homepage-derived stub on the filing it was asked about,
    so the second attempt has to go through a new handle — otherwise the retry
    reads the same empty index and the run can never recover.
    """
    broken = _filing(
        1661222,
        "0001661222-24-000003",
        "2024-06-30",
        infotable(),
        attachments=UNREADABLE_SUBMISSION,
    )
    second_attempts: list[tuple[int, str]] = []

    def refetch(**kwargs) -> FakeFiling:
        second_attempts.append((kwargs["cik"], kwargs["accession_no"]))
        # The same filing, this time with a submission the selectors can read.
        return replace(broken, attachments=INDEXED_TABLE)

    monkeypatch.setattr(data, "configure_identity", lambda email: None)
    monkeypatch.setattr(data, "quarterly_13f_filings", lambda year, quarter: ([broken], 0))
    monkeypatch.setattr(data, "Filing", refetch)

    summary = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    assert second_attempts == [(1661222, "0001661222-24-000003")]
    assert summary["written"] == 1
    assert (tmp_path / "13f_holdings" / "year=2024" / "quarter=2" / "0001661222.parquet").exists()


def test_an_unreadable_submission_is_reported_as_a_failure_not_as_no_holdings(monkeypatch):
    """An unreadable submission must fail the filing, never be skipped as empty."""
    broken = _filing(
        1661222, "0001661222-24-000003", "2024-06-30", None, attachments=UNREADABLE_SUBMISSION
    )
    # The second attempt is just as unreadable, as it is while the SEC is refusing.
    monkeypatch.setattr(data, "Filing", lambda **kwargs: broken)

    with pytest.raises(data.SECDocumentUnavailable, match="0001661222-24-000003"):
        data.holdings_frame(broken)


def test_an_unretrievable_table_document_is_reported_as_a_failure():
    filing = _filing(
        1661222,
        "0001661222-24-000003",
        "2024-06-30",
        None,
        attachments=FakeAttachments((FakeAttachment(content=None),)),
    )

    with pytest.raises(data.SECDocumentUnavailable, match="could not be retrieved"):
        data.holdings_frame(filing)


def test_a_filing_reporting_no_holdings_is_skipped_rather_than_failed():
    """A 13F-HR can legitimately report nothing; only an unreadable index is a failure."""
    filing = _filing(
        1661222,
        "0001661222-24-000003",
        "2024-06-30",
        None,
        attachments=FakeAttachments((), index_size=1),
    )

    assert data.holdings_frame(filing) is None
