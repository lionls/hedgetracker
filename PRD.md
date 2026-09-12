
# Product Requirements Document (PRD): SEC 13F-HR Extraction Pipeline for prefect

## 1. Project Objective
Build a robust, idempotent Prefect data pipeline that extracts quarterly hedge fund holdings (13F-HR filings) from the SEC EDGAR database, cleans the data, enforces a strict schema, and writes the output to a partitioned Parquet data lake.

## 2. Tech Stack
* **Orchestrator:** Prefect 3.x
* **SEC Extraction:** `edgartools`
* **Data Manipulation:** `pandas`
* **Data Storage:** `pyarrow` (for Parquet)
* **Environment:** Python 3.10+

## 3. Data Lake Architecture
The pipeline must save files locally (or via standard pathing that can be mapped to S3/GCS) using the following Hive-style partitioning:
`{base_dir}/13f_holdings/year={YYYY}/quarter={Q}/{CIK}.parquet`

## 4. Strict Schema Enforcement
SEC XML filings are inconsistent. The pipeline MUST enforce this exact schema using PyArrow before writing to Parquet. If a column is missing in the source data, it must be created and filled with nulls.

```python
EXPECTED_SCHEMA = {
    "cik": "string",
    "report_period": "datetime64[ns]",
    "accession_number": "string",
    "nameOfIssuer": "string",
    "titleOfClass": "string",
    "cusip": "string",
    "value": "Int64",          # Pandas nullable integer
    "sshPrnamt": "Int64",      # Shares
    "sshPrnamtType": "string",
    "putCall": "string",       # "Put", "Call", or missing (null)
    "investmentDiscretion": "string",
    "otherManager": "string",
    "Sole": "Int64",           # Voting authority
    "Shared": "Int64",
    "None": "Int64"
}

```

## 5. Flow and Task Specifications

### Task 1: `extract_13f_holdings(filing)`

**Decorator:** `@task(retries=3, retry_delay_seconds=15, tags=["sec-api"])`
**Inputs:** A single `Filing` object from `edgartools`.
**Logic:**

1. **Rate Limiting:** Implement Prefect's `rate_limit("sec-api", occupy=1)` to ensure we stay under the SEC's strict 10 requests/second limit.
2. **Extraction:** Call `filing.obj()`.
3. **Filtering:** If the filing has no `infotable` or `holdings` is None (e.g., notice filings), return `None` and exit gracefully.
4. **Metadata:** Append `cik`, `report_period`, and `accession_number` to the DataFrame.
5. **Schema Conformance (CRITICAL):**
* Reindex the DataFrame using `EXPECTED_SCHEMA.keys()`.
* Force `cusip` to string and use `.str.zfill(9)` to restore any leading zeros stripped by pandas.
* Explicitly cast all columns to match `EXPECTED_SCHEMA`. Convert string representations of "nan" or "None" to actual null types.


6. **Storage:** Write to Parquet using `pyarrow` engine. If the file already exists, overwrite it (idempotency).

### Flow 1: `extract_quarterly_13f(user_email, year, quarter, base_dir)`

**Decorator:** `@flow(name="13F-HR Extraction Pipeline")`
**Inputs:** email (string), year (int), quarter (int), base_dir (string/Path).
**Logic:**

1. Set the edgartools identity: `set_identity(user_email)`.
2. Fetch all 13F-HR filings for the quarter using `get_filings(form="13F-HR", year=year, quarter=quarter)`.
3. Use Prefect's `.map()` to execute the `extract_13f_holdings` task concurrently across all retrieved filings.

## 6. Execution Instructions for the LLM

Write the complete Python script containing these flows and tasks. Include standard logging. Add an `if __name__ == "__main__":` block that executes the flow for Q3 2024 as an example. Keep the code modular and well-commented.
