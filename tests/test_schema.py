"""Schema conformance: the contract every file in the data lake satisfies."""

from __future__ import annotations

import pandas as pd
import pytest

from hedgetracker.schema import ARROW_SCHEMA, EXPECTED_SCHEMA, enforce_schema, to_arrow_table


def test_source_columns_are_mapped_to_the_lake_schema(infotable):
    holdings = enforce_schema(infotable())

    assert list(holdings.columns) == list(EXPECTED_SCHEMA)
    assert holdings.loc[0, "nameOfIssuer"] == "AMAZON.COM INC"
    assert holdings.loc[0, "titleOfClass"] == "Common Stock"
    assert holdings.loc[0, "sshPrnamt"] == 83760
    assert holdings.loc[0, "sshPrnamtType"] == "Shares"
    assert holdings.loc[0, "Sole"] == 0
    assert holdings.loc[0, "Shared"] == 0
    assert holdings.loc[0, "None"] == 83760
    assert holdings.loc[0, "ticker"] == "AMZN"


def test_ticker_comes_from_the_cusip_not_from_the_filing(infotable):
    # Pre-2013 information tables are embedded rather than XML, and edgartools only
    # annotates the XML form, so the bundled reference map is what decides.
    without_column = enforce_schema(infotable().drop(columns=["Ticker"]))
    with_a_wrong_one = enforce_schema(infotable(Ticker="ZZZZ"))

    assert without_column.loc[0, "ticker"] == "AMZN"
    assert with_a_wrong_one.loc[0, "ticker"] == "AMZN"


def test_a_cusip_the_reference_map_does_not_know_leaves_the_ticker_null(infotable):
    holdings = enforce_schema(infotable(Cusip="999999999"))

    assert list(holdings.columns) == list(EXPECTED_SCHEMA)
    assert holdings["ticker"].isna().all()


def test_ticker_lookup_survives_the_letter_case_a_filer_used(infotable):
    # Filers do write the letters in lower case, the bundled map does not.
    holdings = enforce_schema(infotable(Cusip="29355a107"))

    assert holdings.loc[0, "ticker"] == "ENPH"
    # Only the lookup is case-insensitive: the stored CUSIP stays as filed.
    assert holdings.loc[0, "cusip"] == "29355a107"


def test_columns_missing_from_the_source_are_created_as_nulls(infotable):
    holdings = enforce_schema(infotable().drop(columns=["PutCall", "OtherManager", "Cusip"]))

    assert list(holdings.columns) == list(EXPECTED_SCHEMA)
    for column in ("putCall", "otherManager", "cusip", "ticker"):
        assert column in holdings.columns
        assert holdings[column].isna().all()


def test_dtypes_exactly_match_the_declared_schema(infotable):
    holdings = enforce_schema(infotable())

    assert {column: str(dtype) for column, dtype in holdings.dtypes.items()} == EXPECTED_SCHEMA


def test_arrow_table_conforms_to_the_declared_arrow_schema(infotable):
    assert to_arrow_table(enforce_schema(infotable())).schema == ARROW_SCHEMA


def test_placeholder_tokens_become_nulls_not_strings(infotable):
    holdings = enforce_schema(infotable(PutCall="nan", OtherManager="None", Class="  "))

    assert holdings["putCall"].isna().all()
    assert holdings["otherManager"].isna().all()
    assert holdings["titleOfClass"].isna().all()


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (37833100, "037833100"),  # int, leading zero stripped by parsing
        (37833100.0, "037833100"),  # float rendering of the same
        ("594918104", "594918104"),  # already complete
        ("G0457F107", "G0457F107"),  # alphanumeric CUSIP, padded no further
        (" 023135106 ", "023135106"),
    ],
)
def test_cusip_leading_zeros_are_restored(infotable, source, expected):
    holdings = enforce_schema(infotable(Cusip=source))

    assert holdings.loc[0, "cusip"] == expected


def test_numeric_fields_survive_formatting_and_reject_non_integers(infotable):
    holdings = enforce_schema(infotable(**{"Value": "1,234", "SharesPrnAmount": "1.5"}))

    assert holdings.loc[0, "value"] == 1234
    assert pd.isna(holdings.loc[0, "sshPrnamt"])


def test_report_period_is_timezone_free_nanoseconds(infotable):
    holdings = enforce_schema(
        infotable(report_period=pd.Timestamp("2024-06-30", tz="America/New_York"))
    )

    assert str(holdings["report_period"].dtype) == "datetime64[ns]"
    # 2024-06-30T00:00 in New York is 04:00 UTC on the same day.
    assert holdings.loc[0, "report_period"] == pd.Timestamp("2024-06-30 04:00:00")


def test_unparseable_report_period_becomes_null(infotable):
    holdings = enforce_schema(infotable(report_period="not a date"))

    assert holdings["report_period"].isna().all()


def test_unmapped_columns_are_dropped(infotable):
    holdings = enforce_schema(infotable(Unexpected="dropped"))

    assert list(holdings.columns) == list(EXPECTED_SCHEMA)
