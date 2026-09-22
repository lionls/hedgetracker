"""Flow behaviour end to end, with the SEC network replaced by fixtures."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hedgetracker import data, settings, storage
from hedgetracker.flows import sec_13f
from hedgetracker.schema import ARROW_SCHEMA, enforce_schema


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
class FakeHomepage:
    """Stands in for ``edgar.filing_homepage.FilingHomepage``.

    ``period_of_report`` is what the filing's index page states, which is how an
    already-extracted filing is recognised without downloading its submission; the
    extraction has to cope with a page that states nothing.
    """

    period_of_report: str | None = None


class UnreadableHomepage:
    """Stands in for an index page the SEC answered with an error page."""

    @property
    def period_of_report(self) -> str:
        raise RuntimeError("SEC answered the index page request with an error page")


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
    homepage: FakeHomepage | UnreadableHomepage = FakeHomepage()

    def obj(self) -> FakeReport:
        return self.report


def _filing(
    cik: int,
    accession_no: str,
    report_period: str,
    infotable,
    attachments: FakeAttachments = INDEXED_TABLE,
    index_page: FakeHomepage | UnreadableHomepage | None = None,
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
        homepage=(
            FakeHomepage(period_of_report=report_period) if index_page is None else index_page
        ),
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
        "skipped_existing": 0,
        "skipped_no_holdings": 1,
        "holdings_rows": 2,
        # Both files were written with their filer's name, so nothing was left to
        # name: the pass only ever touches a file from before the column existed.
        "named_files": 0,
        "named_rows": 0,
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


def test_rerunning_a_window_skips_what_the_lake_already_holds(tmp_path, sec_edgar):
    """An accepted filing does not change, so a second run leaves its file alone."""
    first = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )
    before = {path: path.stat().st_mtime_ns for path in sorted(tmp_path.rglob("*.parquet"))}

    second = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    assert first["written"] == 2
    # The two extracted filings are skipped, the third still reports no holdings.
    assert second == {
        **first,
        "written": 0,
        "skipped_existing": 2,
        "skipped_no_holdings": 1,
        "holdings_rows": 0,
    }
    # The same files, untouched: a rewrite would move their timestamps.
    assert {path: path.stat().st_mtime_ns for path in sorted(tmp_path.rglob("*.parquet"))} == before


def test_an_already_extracted_filing_is_not_read_again(tmp_path, monkeypatch, sec_edgar):
    """Skipping has to avoid the submission download, not just the write."""
    sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    read: list[str] = []
    real_holdings_frame = data.holdings_frame

    def recording_holdings_frame(filing):
        read.append(filing.accession_no)
        return real_holdings_frame(filing)

    monkeypatch.setattr(data, "holdings_frame", recording_holdings_frame)

    summary = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    # Only the filing that has no file yet is read; the extracted two are not.
    assert read == ["0002034595-24-000001"]
    assert summary["skipped_existing"] == 2


def test_a_merged_lake_still_skips_the_filings_it_holds(tmp_path, monkeypatch, sec_edgar):
    """Compaction must not cost the extraction its record of what the lake holds."""
    sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )
    storage.compact_holdings(tmp_path)

    read: list[str] = []
    real_holdings_frame = data.holdings_frame

    def recording_holdings_frame(filing):
        read.append(filing.accession_no)
        return real_holdings_frame(filing)

    monkeypatch.setattr(data, "holdings_frame", recording_holdings_frame)

    summary = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    # Both merged filings are recognised without downloading their submissions;
    # only the one that reports no holdings is read, and nothing is written back
    # into the partitions the merge produced.
    assert read == ["0002034595-24-000001"]
    assert summary["skipped_existing"] == 2
    assert summary["written"] == 0
    for year, quarter in ((2024, 2), (2023, 4)):
        partition = tmp_path / "13f_holdings" / f"year={year}" / f"quarter={quarter}"
        assert [path.name for path in partition.glob("*.parquet")] == ["holdings.parquet"]


def test_sweeping_a_window_names_a_file_extracted_before_the_lake_had_the_name(
    tmp_path, sec_edgar, infotable
):
    """A lake from the older schema is named by the sweep that skips its filings.

    The name is not in the information table, so a file written before the
    extractor recorded it can only be named from the window's index entry — which
    costs no request and no download, because the filing is recognised as already
    extracted before its documents are read.
    """
    part = tmp_path / "13f_holdings" / "year=2024" / "quarter=2" / "0001661222.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    legacy_schema = pa.schema([field for field in ARROW_SCHEMA if field.name != "filer_name"])
    legacy = enforce_schema(infotable().assign(cik="0001661222")).drop(columns=["filer_name"])
    pq.write_table(pa.Table.from_pandas(legacy, schema=legacy_schema), part)

    summary = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    assert summary["skipped_existing"] == 1
    assert summary["named_files"] == 1
    assert summary["named_rows"] == 1
    named = pq.read_table(part).to_pandas()
    assert named.loc[0, "filer_name"] == "FAKE FUND 1661222"
    # Naming put the file on the current schema, so a second sweep writes nothing.
    assert named.loc[0, "nameOfIssuer"] == "AMAZON.COM INC"
    modified = part.stat().st_mtime_ns
    again = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )
    assert (again["named_files"], again["named_rows"]) == (0, 0)
    assert part.stat().st_mtime_ns == modified


@pytest.mark.parametrize("index_page", [FakeHomepage(), UnreadableHomepage()])
def test_a_filing_without_a_usable_index_page_is_extracted_anyway(
    tmp_path, monkeypatch, infotable, index_page
):
    """Without a stated period there is nothing to skip on, so the filing is read."""
    filing = _filing(
        1661222,
        "0001661222-24-000003",
        "2024-06-30",
        infotable(),
        index_page=index_page,
    )
    monkeypatch.setattr(data, "configure_identity", lambda email: None)
    monkeypatch.setattr(data, "quarterly_13f_filings", lambda year, quarter: ([filing], 0))

    summary = sec_13f.extract_quarterly_13f(
        user_email="mark@gmail.com", year=2024, quarter=3, base_dir=tmp_path
    )

    assert summary["written"] == 1
    assert summary["skipped_existing"] == 0
    assert (tmp_path / "13f_holdings" / "year=2024" / "quarter=2" / "0001661222.parquet").exists()


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


def test_quarter_windows_covers_every_year_and_quarter_that_has_closed():
    """The sweep is the cross product of the requested years and quarters, oldest first."""
    assert data.quarter_windows(2024, 2025, today=date(2026, 1, 1)) == [
        (2024, 1),
        (2024, 2),
        (2024, 3),
        (2024, 4),
        (2025, 1),
        (2025, 2),
        (2025, 3),
        (2025, 4),
    ]


def test_quarter_windows_opens_at_the_quarter_end_and_not_before():
    """A quarter is a window only once it has ended; the day it ends it still is not."""
    assert data.quarter_windows(2026, today=date(2026, 9, 12)) == [(2026, 1), (2026, 2)]
    assert data.quarter_windows(2026, today=date(2026, 9, 30)) == [(2026, 1), (2026, 2)]
    assert data.quarter_windows(2026, today=date(2026, 10, 1)) == [
        (2026, 1),
        (2026, 2),
        (2026, 3),
    ]


def test_quarter_windows_honours_a_quarter_subset():
    assert data.quarter_windows(2025, 2025, quarters=(4, 2), today=date(2026, 1, 1)) == [
        (2025, 2),
        (2025, 4),
    ]


def test_quarter_windows_rejects_years_edgar_does_not_index_and_inverted_ranges():
    with pytest.raises(ValueError, match="index starts in 1993"):
        data.quarter_windows(data.FIRST_INDEXED_YEAR - 1, today=date(2026, 1, 1))

    with pytest.raises(ValueError, match="before start_year"):
        data.quarter_windows(2025, 2024, today=date(2026, 1, 1))


def test_backfill_sweeps_every_closed_window_and_aggregates_the_totals(tmp_path, sec_edgar):
    """Four windows of 2024 over the same fixtures: the first writes, the rest skip."""
    summary = sec_13f.backfill_13f(
        user_email="mark@gmail.com", start_year=2024, end_year=2024, base_dir=tmp_path
    )

    assert summary == {
        "base_dir": str(tmp_path),
        "start_year": 2024,
        "end_year": 2024,
        "windows": 4,
        "windows_swept": 4,
        "filings": 12,
        "written": 2,
        "skipped_existing": 6,
        "holdings_rows": 2,
        "named_files": 0,
        "named_rows": 0,
        "per_window": [
            {
                "year": 2024,
                "quarter": 1,
                "written": 2,
                "skipped_existing": 0,
                "holdings_rows": 2,
            },
            *[
                {
                    "year": 2024,
                    "quarter": quarter,
                    "written": 0,
                    "skipped_existing": 2,
                    "holdings_rows": 0,
                }
                for quarter in (2, 3, 4)
            ],
        ],
    }
    # Both reporters wrote onto the same partition once; the sweep did not rewrite them.
    assert len(list(tmp_path.rglob("*.parquet"))) == 2


def test_backfill_sweeps_only_the_requested_quarters(tmp_path, sec_edgar):
    summary = sec_13f.backfill_13f(
        user_email="mark@gmail.com",
        start_year=2024,
        end_year=2024,
        quarters=(2, 4),
        base_dir=tmp_path,
    )

    assert [(entry["year"], entry["quarter"]) for entry in summary["per_window"]] == [
        (2024, 2),
        (2024, 4),
    ]


def test_backfill_keeps_going_after_a_failing_window_and_names_it(tmp_path, monkeypatch, sec_edgar):
    """A window that fails must not hide the windows that succeeded, and must fail the run."""
    passing_window = data.quarterly_13f_filings

    def flaky_window(year, quarter):
        if (year, quarter) == (2024, 2):
            raise RuntimeError("SEC index unavailable")
        return passing_window(year, quarter)

    monkeypatch.setattr(data, "quarterly_13f_filings", flaky_window)

    with pytest.raises(RuntimeError, match=r"1 of 4 index windows failed \(first: 2024Q2"):
        sec_13f.backfill_13f(
            user_email="mark@gmail.com", start_year=2024, end_year=2024, base_dir=tmp_path
        )

    # The other three windows still reached the lake.
    assert (tmp_path / "13f_holdings" / "year=2024" / "quarter=2" / "0001661222.parquet").exists()


def test_backfill_refuses_a_range_whose_windows_have_not_closed(tmp_path, sec_edgar):
    """A range with nothing to read yet is an error, not an empty success."""
    next_year = date.today().year + 1

    with pytest.raises(ValueError, match="no closed EDGAR index window"):
        sec_13f.backfill_13f(
            user_email="mark@gmail.com",
            start_year=next_year,
            end_year=next_year,
            base_dir=tmp_path,
        )

    assert not list(tmp_path.rglob("*.parquet"))
