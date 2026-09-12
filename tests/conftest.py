"""Shared test fixtures."""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="session", autouse=True)
def prefect_api():
    """Run every flow and task test against a throwaway Prefect API.

    Flow runs, task run tracking, concurrency limits and result storage then need
    no server or database of their own.
    """
    with prefect_test_harness():
        yield


@pytest.fixture
def infotable() -> Callable[..., pd.DataFrame]:
    """Factory for a frame shaped like edgartools' 13F information table."""

    def build(**overrides: object) -> pd.DataFrame:
        row: dict[str, object] = {
            "Issuer": "AMAZON.COM INC",
            "Class": "Common Stock",
            "Cusip": "023135106",
            "Value": 16517000,
            "PutCall": "",
            "InvestmentDiscretion": "SOLE",
            "OtherManager": "",
            "SharesPrnAmount": 83760,
            "Type": "Shares",
            "SoleVoting": 0,
            "SharedVoting": 0,
            "NonVoting": 83760,
            "Ticker": "AMZN",
        }
        row.update(overrides)
        return pd.DataFrame([row])

    return build
