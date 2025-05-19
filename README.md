# Table Comparison Tool

A Python utility for comparing two tables from either DuckDB or BigQuery to identify differences. This script is designed for non-regression testing of data pipelines, ensuring that data transformations or refactorings don't introduce unexpected changes. It can be run as a CLI application.

## Features

- Compares two tables (DuckDB or BigQuery) and identifies differences (value changes, missing rows, additional rows).
- Supports custom primary key columns for row matching.
- Allows ignoring specific columns during comparison (e.g., `updated_at` fields).
- Supports casting columns to a common type before comparison (e.g., stringified timestamps to TIMESTAMP).
- Limits the number of differences displayed.
- Single script for both DuckDB and BigQuery, selectable via a `db_type` argument.

## Installation

1. **Clone the repository (if you haven't already):**

   ```bash
   git clone https://github.com/Bayrem-gb/E42-CompareTables.git
   cd table-comparison-tool
   ```
2. **Create a virtual environment (recommended):**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows use `venv\\Scripts\\activate`
   ```
3. **Install dependencies:**
  
   ```bash
   pip install -r requirements.txt
   ```

## Authentication and Configuration (BigQuery)

To use this tool with BigQuery, you need to authenticate with Google Cloud.

### Using a Service Account (Recommended for Production/Automation)

1. **Create a Service Account Key:** In the Google Cloud Console, create a service account with the necessary permissions (at least `BigQuery Data Viewer` and `BigQuery Job User` for the projects/datasets you want to access). Download the JSON key file.
2. **Set Environment Variable:** Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your downloaded JSON key file.
   * On Linux/macOS:
     ```bash
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"
     ```
   * On Windows (PowerShell):
     ```powershell
     $env:GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"
     ```

   You can also add this line to your shell profile (e.g., `.bashrc`, `.zshrc`) for persistence, or manage it via a `.env` file loaded by your application/script if preferred (though `GOOGLE_APPLICATION_CREDENTIALS` is the standard method for Google Cloud libraries).

### Using User Credentials (for Local Development)

If you're running the script locally and have the `gcloud` CLI installed and authenticated (`gcloud auth application-default login`), the BigQuery client library will typically pick up these credentials automatically.

## Usage

The script `compare_tables.py` is the main entry point.

### Command Line Interface (CLI)

**General Format:**

```bash
python compare_tables.py <db_type> <TABLE1> <TABLE2> [options]
```

**Arguments:**

* `db_type`: `duckdb` or `bigquery`. Specifies the database system.
* `TABLE1`: Name of the first table.
  * For DuckDB: e.g., `main.table1` or `table1` (if in default schema).
  * For BigQuery: e.g., `project.dataset.table` or `dataset.table` (if the project is the default for your authenticated client).
* `TABLE2`: Name of the second table. Same format as `TABLE1`.

**Options:**

* `--pk-cols <cols>`: Comma-separated columns for the primary key (default: `id`). Example: `--pk-cols user_id,order_id`
* `--limit <num>`: Maximum number of differences to display (default: None). Example: `--limit 50`
* `--ignore-cols <cols>`: Comma-separated columns to ignore. Example: `--ignore-cols last_updated,etl_timestamp`
* `--scalar-casts <casts>`: Columns to cast before comparison (format: `col1=TYPE,col2=TYPE`). Example: `--scalar-casts created_at=TIMESTAMP,amount=NUMERIC`
  * Supported types: `STRING`, `FLOAT64`, `BOOL`, `DATE`, `TIMESTAMP`, `INT64`, `BYTES`, `NUMERIC`, `BIGNUMERIC`, `JSON`, `TIME`.
* `-h`, `--help`: Show help message.

**Examples:**

* **DuckDB:**
1. run the setup_duckdb :
  ```bash
    python bayrem\setup_duckdb_tables.py
   ```
   This will generate duckdb , it will contain two generated tables for testing the main script : demo_table_A and demo_table_B.

2. run the compare_table script on duckdb tables :    
  ```bash
  python compare_tables.py duckdb demo_table_A demo_table_B --pk-cols id --ignore-cols last_login --limit 20
  ```
* **BigQuery:**
  ```bash
  # Ensure GOOGLE_APPLICATION_CREDENTIALS is set or you are logged in via gcloud
  python compare_tables.py bigquery myproject.dataset_prod.customers myproject.dataset_staging.customers_temp --pk-cols customer_id --scalar-casts signup_date=DATE
  ```

## Output Format

The script outputs differences as JSON lines. Each line represents a row with discrepancies or a row present in only one table.

Example for a row with value differences:

```json
{"id": 10739, "diffs": {"alumnized_at": ["2025-01-09T23:20:10.538250+00:00", null], "_status": "value_differences"}}
```

Example for a row present only in the first table:

```json
{"id": 10740, "diffs": {"some_col": ["value_from_table1", null], "_status": "present_in_table1_only"}}
```

## How It Works

1. The script connects to the specified database (DuckDB or BigQuery).
2. It fetches the schemas (column names) for both tables.
3. It identifies common columns, excluding specified ignored columns and primary key columns from the direct value comparison set.
4. A SQL query is constructed using a `FULL OUTER JOIN` on the primary key columns. This allows detection of rows present in one table but not the other.
5. Casts specified via `--scalar-casts` are applied within the SQL query before comparison.
6. The `WHERE` clause of the SQL query filters for rows where:
   * Primary keys don't match across tables (indicating a row is in one table only).
   * Or, for rows with matching primary keys, any of the compared non-PK columns have different values (using `IS DISTINCT FROM` for NULL-safe comparison).
7. The query results are processed, and differences are formatted into JSON lines, highlighting the primary key(s) and the specific columns that differ or indicating if a row is unique to one table.
8. This approach is generally efficient as the heavy lifting of comparison is done by the database engine, minimizing data transfer to the client running the script.
