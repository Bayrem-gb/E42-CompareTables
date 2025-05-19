#!/usr/bin/env python3
"""
This script compares two tables from either DuckDB or BigQuery to identify differences.
It can be used as a CLI tool.

CLI Usage:
  For DuckDB:
  python compare_tables.py duckdb <TABLE1> <TABLE2> [options]
  Example: python compare_tables.py duckdb demo_table_A demo_table_B --pk-cols id --ignore-cols last_login --limit 20

  For BigQuery:
  python compare_tables.py bigquery <PROJECT.DATASET.TABLE1> <PROJECT.DATASET.TABLE2> [options]
  Example: python compare_tables.py bigquery gcp_project.dataset1.tableA gcp_project.dataset2.tableB --pk-cols user_id,event_id --ignore-cols updated_at --scalar-casts date_col=DATE
"""

import argparse
import json
import sys
from typing import Dict, List, Optional, Tuple, Union, Any

# Conditionally import database clients
try:
    import duckdb
except ImportError:
    duckdb = None

try:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
except ImportError:
    bigquery = None
    NotFound = None # Define NotFound as None if google.cloud is not available


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare two tables from DuckDB or BigQuery.",
        formatter_class=argparse.RawTextHelpFormatter # To keep help formatting
    )
    
    parser.add_argument(
        "db_type",
        choices=["duckdb", "bigquery"],
        help="Type of the database to connect to."
    )
    parser.add_argument(
        "TABLE1",
        help="First table name.\n  - For DuckDB: e.g., 'main.table1' or 'table1' (if in default schema)\n  - For BigQuery: e.g., 'project.dataset.table' or 'dataset.table' (if project is default for client)"
    )
    parser.add_argument(
        "TABLE2",
        help="Second table name. Same format as TABLE1."
    )
    parser.add_argument(
        "--pk-cols",
        type=lambda s: [col.strip() for col in s.split(",") if col.strip()],
        default=["id"],
        help="Comma-separated columns to use as the primary key. (default: ['id'])"
    )
    parser.add_argument(
        "--limit",
        type=lambda s: None if s.lower() == "null" else int(s),
        default=None,
        help="Maximum number of diffs to display before exiting. (default: None)"
    )
    parser.add_argument(
        "--ignore-cols",
        type=lambda s: [col.strip() for col in s.split(",") if col.strip()] if s else [],
        default=[],
        help="Comma-separated columns to ignore in the diff. (default: [])"
    )
    parser.add_argument(
        "--scalar-casts",
        type=lambda s: {k.strip(): v.strip() for k, v in (item.split('=') for item in s.split(',') if '=' in item)} if s else {},
        default={},
        help=(
            "Columns to cast to a common type before comparison (format: col1=TYPE,col2=TYPE2).\n"
            "Supported types (case-insensitive): STRING, FLOAT64, BOOL, DATE, TIMESTAMP, INT64, BYTES, NUMERIC, BIGNUMERIC, JSON, TIME.\n"
            "Example: created_at=TIMESTAMP,amount=NUMERIC"
        )
    )
    
    return parser.parse_args()


def _quote_identifier(identifier: str, db_type: str) -> str:
    """Quotes an identifier based on the database type."""
    if db_type == "duckdb":
        # DuckDB uses double quotes for identifiers.
        # Escaping inner double quotes (e.g. by doubling them) is not handled here,
        # assuming simple identifiers that don't contain double quotes.
        return f'"{identifier}"'
    elif db_type == "bigquery":
        # BigQuery uses backticks.
        return f'`{identifier}`'
    # Should not be reached if db_type is validated, but as a fallback:
    return identifier


def _get_duckdb_table_columns(conn: Any, table_name: str) -> List[str]:
    """Get column names for a given DuckDB table."""
    try:
        # Quote table name parts if a schema is involved to handle special characters or keywords
        if '.' in table_name:
            schema, name = table_name.split('.', 1)
            quoted_table_name = f'\"{schema}\".\"{name}\"'
        else:
            quoted_table_name = f'\"{table_name}\"'
        return [col[0] for col in conn.execute(f"DESCRIBE {quoted_table_name}").fetchall()]
    except Exception as e:
        raise RuntimeError(f"Error describing DuckDB table {table_name}: {e}")


def _get_cast_expression_for_sql_expr(sql_expression_to_cast: str, cast_type: str) -> str:
    """
    Creates a CAST expression string for a given SQL expression and cast type.
    Validates the cast type.
    """
    normalized_cast_type = cast_type.upper()
    # Supported types are relevant for the CAST syntax.
    # These are common SQL types; db-specific validation might be more nuanced
    # but this covers the explicitly supported ones.
    valid_types = [
        "STRING", "VARCHAR",  # String types
        "FLOAT64", "DOUBLE", "FLOAT", # Floating point
        "BOOL", "BOOLEAN",      # Boolean
        "DATE",                 # Date
        "TIMESTAMP",            # Timestamp
        "INT64", "INTEGER", "BIGINT", "INT", "SMALLINT", "TINYINT", # Integer types
        "BYTES",                # Byte array
        "NUMERIC", "DECIMAL",    # Exact numeric
        "BIGNUMERIC",           # BigQuery specific, but general concept
        "JSON",                 # JSON type
        "TIME"                  # Time
    ]
    # Allow some flexibility by checking common synonyms / variations.
    # The actual CAST operation relies on the DB supporting the target type name.
    if normalized_cast_type not in valid_types:
        # Attempt to map some common SQL type names to the ones used in validation/casting logic
        # This is a basic mapping, can be expanded
        mapping = {
            "TEXT": "STRING",
            "INT": "INT64",
            "INTEGER": "INT64",
            "BIGINT": "INT64",
            "DOUBLE": "FLOAT64",
            "BOOLEAN": "BOOL",
            "DECIMAL": "NUMERIC"
        }
        normalized_cast_type = mapping.get(normalized_cast_type, normalized_cast_type)

        if normalized_cast_type not in valid_types:
            raise ValueError(
                f"Unsupported cast type: '{cast_type}'. Normalized to '{normalized_cast_type}'. "
                f"Supported/checked types are: {', '.join(valid_types)}"
            )
    
    return f"CAST({sql_expression_to_cast} AS {normalized_cast_type})"


def _parse_bigquery_table_name(table_name: str, client_project: Optional[str]) -> str:
    """Parse and fully qualify a BigQuery table name."""
    parts = table_name.split(".")
    if len(parts) == 3:
        return table_name
    elif len(parts) == 2:
        if not client_project:
            raise ValueError(
                f"Table name '{table_name}' is missing project ID, and BigQuery client has no default project. "
                f"Provide full name as 'project.dataset.table' or ensure client is initialized with a project."
            )
        return f"{client_project}.{parts[0]}.{parts[1]}"
    else:
        raise ValueError(f"Invalid BigQuery table name format: '{table_name}'. Expected [project.]dataset.table or dataset.table")


def _get_bigquery_table_columns(client: Any, table_ref_str: str) -> List[str]:
    """Get column names for a given BigQuery table."""
    try:
        table = client.get_table(table_ref_str)
        return [field.name for field in table.schema]
    except NotFound: # Ensure NotFound is correctly referenced or defined
        raise RuntimeError(f"BigQuery table not found: {table_ref_str}")
    except Exception as e:
        raise RuntimeError(f"Error describing BigQuery table {table_ref_str}: {e}")


def _build_select_expression(col: str, table_alias_in_cte_from: str, scalar_casts: Dict[str, str], db_type: str) -> str:
    """
    Builds a single select expression for a column for use in a CTE.
    e.g., 't1."col_name" AS "t1_col_name"' or 'CAST(t1."col_name" AS TYPE) AS "t1_col_name"'.
    Handles quoting and casting based on db_type.
    """
    # Reference to the original column from the source table, properly quoted.
    # e.g., t1."col" (DuckDB) or t1.`col` (BigQuery)
    source_col_ref = f"{table_alias_in_cte_from}.{_quote_identifier(col, db_type)}"

    final_value_expression_for_select: str
    if col in scalar_casts:
        cast_type = scalar_casts[col]
        # _get_cast_expression_for_sql_expr will validate cast_type and build the CAST string
        final_value_expression_for_select = _get_cast_expression_for_sql_expr(source_col_ref, cast_type)
    else:
        final_value_expression_for_select = source_col_ref
            
    # Alias for this expression in the CTE, also quoted.
    # e.g., "t1_col" (DuckDB) or `t1_col` (BigQuery)
    output_alias_for_cte = _quote_identifier(f"{table_alias_in_cte_from}_{col}", db_type)
    
    return f"{final_value_expression_for_select} AS {output_alias_for_cte}"


def compare_tables_internal(
    db_type: str,
    conn_or_client: Any,
    table1_name: str,
    table2_name: str,
    pk_cols: List[str],
    ignore_cols: List[str],
    scalar_casts: Dict[str, str],
    limit: Optional[int]
) -> List[Dict[str, Any]]:
    """Internal logic for comparing two tables from the specified database."""
    
    t1_cols_list: List[str]
    t2_cols_list: List[str]
    table1_sql_ref: str = table1_name
    table2_sql_ref: str = table2_name

    if db_type == "duckdb":
        t1_cols_list = _get_duckdb_table_columns(conn_or_client, table1_name)
        t2_cols_list = _get_duckdb_table_columns(conn_or_client, table2_name)
        # DuckDB table names might need quoting if they contain special chars or match keywords
        # Proper quoting handles schemas as well: "schema_name"."table_name"
        def quote_duckdb_name(name):
            if '.' in name:
                return '.'.join(f'\"{part}\"' for part in name.split('.'))
            return f'\"{name}\"'
        table1_sql_ref = quote_duckdb_name(table1_name)
        table2_sql_ref = quote_duckdb_name(table2_name)
    elif db_type == "bigquery":
        client_project = conn_or_client.project
        t1_full_ref = _parse_bigquery_table_name(table1_name, client_project)
        t2_full_ref = _parse_bigquery_table_name(table2_name, client_project)
        t1_cols_list = _get_bigquery_table_columns(conn_or_client, t1_full_ref)
        t2_cols_list = _get_bigquery_table_columns(conn_or_client, t2_full_ref)
        table1_sql_ref = f"`{t1_full_ref}`"
        table2_sql_ref = f"`{t2_full_ref}`"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    if not pk_cols:
        raise ValueError("Primary key columns (--pk-cols) must be specified and not empty.")
    
    common_schema_cols = list(set(t1_cols_list).intersection(set(t2_cols_list)))
    for pk_col in pk_cols:
        if pk_col not in common_schema_cols:
            raise ValueError(f"Primary key column '{pk_col}' not found as a common column in both tables.")

    compare_cols = sorted([col for col in common_schema_cols if col not in pk_cols and col not in ignore_cols])
    
    if not compare_cols:
        if not ignore_cols:
             print("Warning: No columns to compare (all common columns are PKs). Will only check for row existence based on PKs.", file=sys.stderr)
        else:
             print("Warning: No columns to compare (all common non-PK columns were ignored). Will only check for row existence based on PKs.", file=sys.stderr)

    # Select expressions for table1 and table2 with aliasing and casting
    # These expressions will refer to original columns in t1 and t2
    t1_select_exprs_for_cte = [_build_select_expression(col, 't1', scalar_casts, db_type) for col in pk_cols + compare_cols]
    t2_select_exprs_for_cte = [_build_select_expression(col, 't2', scalar_casts, db_type) for col in pk_cols + compare_cols]

    join_on_pks = " AND ".join([
        f"t1.{_quote_identifier(f't1_{pk}', db_type)} = t2.{_quote_identifier(f't2_{pk}', db_type)}"
        for pk in pk_cols
    ])

    diff_conditions_list = []
    for col in compare_cols:
        # Referencing aliased columns from CTEs
        t1_col_ref_in_cte = _quote_identifier(f't1_{col}', db_type)
        t2_col_ref_in_cte = _quote_identifier(f't2_{col}', db_type)
        diff_conditions_list.append(f"t1.{t1_col_ref_in_cte} IS DISTINCT FROM t2.{t2_col_ref_in_cte}")
    
    t1_only_conditions = " AND ".join([
        f"t2.{_quote_identifier(f't2_{pk}', db_type)} IS NULL" for pk in pk_cols
    ])
    t2_only_conditions = " AND ".join([
        f"t1.{_quote_identifier(f't1_{pk}', db_type)} IS NULL" for pk in pk_cols
    ])

    # Build the WHERE clause
    where_parts = []
    if diff_conditions_list:
        where_parts.append(f"({' OR '.join(diff_conditions_list)})")
    if pk_cols: # only add existence checks if PKs are defined
        where_parts.append(f"({t1_only_conditions})")
        where_parts.append(f"({t2_only_conditions})")
    
    if not where_parts: # Should only happen if no PKs and no compare_cols (error checked earlier)
        where_clause = "1=0" # No conditions defined, effectively no results
    else:
        where_clause = " OR ".join(where_parts)
        
    coalesced_pk_selects = [
        f"COALESCE(t1.{_quote_identifier(f't1_{pk}', db_type)}, t2.{_quote_identifier(f't2_{pk}', db_type)}) AS {_quote_identifier(pk, db_type)}"
        for pk in pk_cols
    ]

    query = f"""
    WITH table1_prepared AS (
        SELECT {', '.join(t1_select_exprs_for_cte)} FROM {table1_sql_ref} t1
    ),
    table2_prepared AS (
        SELECT {', '.join(t2_select_exprs_for_cte)} FROM {table2_sql_ref} t2
    )
    SELECT
        {', '.join(coalesced_pk_selects)},
        t1.*, 
        t2.*  
    FROM table1_prepared t1
    FULL OUTER JOIN table2_prepared t2 ON {join_on_pks}
    WHERE {where_clause}
    """

    if limit is not None:
        query += f" LIMIT {limit}"
    
    results_iter: Union[List[Tuple], Any]
    description: List[Tuple[str, Any]]

    if db_type == "duckdb":
        executed_query = conn_or_client.execute(query)
        results_iter = executed_query.fetchall()
        description = executed_query.description
    elif db_type == "bigquery":
        query_job = conn_or_client.query(query)
        results_iter = list(query_job.result())
        description = [(field.name, field.field_type) for field in query_job.schema]
    
    output_diffs = []
    final_column_names = [desc[0] for desc in description]

    for row_tuple in results_iter:
        row_dict = dict(zip(final_column_names, row_tuple))
        diff_entry: Dict[str, Any] = {}
        pk_output_values: Dict[str, Any] = {pk: row_dict.get(pk) for pk in pk_cols}
        diff_entry.update(pk_output_values)
        
        diff_details: Dict[str, List[Optional[Any]]] = {}
        status_indicators = [] 

        is_t1_only = all(row_dict.get(f"t2_{pk}") is None for pk in pk_cols) and any(row_dict.get(f"t1_{pk}") is not None for pk in pk_cols)
        is_t2_only = all(row_dict.get(f"t1_{pk}") is None for pk in pk_cols) and any(row_dict.get(f"t2_{pk}") is not None for pk in pk_cols)

        if is_t1_only:
            status_indicators.append("present_in_table1_only")
            for col in compare_cols:
                diff_details[col] = [row_dict.get(f"t1_{col}"), None]
        elif is_t2_only:
            status_indicators.append("present_in_table2_only")
            for col in compare_cols:
                diff_details[col] = [None, row_dict.get(f"t2_{col}")]
        else: 
            has_value_diff = False
            for col in compare_cols:
                val1 = row_dict.get(f"t1_{col}")
                val2 = row_dict.get(f"t2_{col}")
                
                if db_type == "bigquery":
                    if not isinstance(val1, (str, int, float, bool, type(None))):
                        val1 = str(val1)
                    if not isinstance(val2, (str, int, float, bool, type(None))):
                        val2 = str(val2)
                
                if val1 != val2:
                    diff_details[col] = [val1, val2]
                    has_value_diff = True
            if has_value_diff:
                status_indicators.append("value_differences")

        if diff_details or "present_in_table1_only" in status_indicators or "present_in_table2_only" in status_indicators:
            diff_entry["diffs"] = diff_details
            if status_indicators:
                 diff_entry["diffs"]["_status"] = ",".join(status_indicators)
            output_diffs.append(diff_entry)
            
    return output_diffs


def run_comparison(
    db_type: str,
    table1: str,
    table2: str,
    pk_cols: List[str],
    limit: Optional[int] = None,
    ignore_cols: List[str] = [],
    scalar_casts: Dict[str, str] = {}
) -> List[Dict[str, Any]]:
    """
    Connects to the specified database and runs the table comparison.
    This is the main entry point for using the comparison logic programmatically.
    """
    conn_or_client: Any = None
    if db_type == "duckdb":
        if not duckdb:
            raise ImportError("DuckDB library is not installed. Please install with `pip install duckdb`.")
        conn_or_client = duckdb.connect(database=':memory:', read_only=False) 
        # Demo table creation logic
        if table1 == "demo_table_A" and table2 == "demo_table_B":
            print("INFO: Using demo_table_A and demo_table_B with DuckDB. Creating them in-memory.", file=sys.stderr)
            # Simplified schema for brevity, ensure it matches expected columns
            conn_or_client.execute("""CREATE OR REPLACE TABLE demo_table_A (id INTEGER PRIMARY KEY, name VARCHAR, value INTEGER, last_seen TIMESTAMP);
                                    INSERT INTO demo_table_A VALUES (1, 'Alice', 100, '2023-01-01 10:00:00'), (2, 'Bob', 200, '2023-01-02 11:00:00'), (3, 'Charlie', 300, '2023-01-03 12:00:00'), (5, 'Eve_Old', 500, '2023-01-05 14:00:00');""")
            conn_or_client.execute("""CREATE OR REPLACE TABLE demo_table_B (id INTEGER PRIMARY KEY, name VARCHAR, value INTEGER, last_seen TIMESTAMP);
                                    INSERT INTO demo_table_B VALUES (1, 'Alice', 100, '2023-01-01 10:00:00'), (2, 'Bob', 250, '2023-01-02 11:30:00'), (4, 'David', 400, '2023-01-04 13:00:00'), (5, 'Eve_New', 550, '2023-01-05 14:30:00');""")
    elif db_type == "bigquery":
        if not bigquery:
            raise ImportError("Google Cloud BigQuery library is not installed. Please install with `pip install google-cloud-bigquery`.")
        conn_or_client = bigquery.Client()
        # Note: In-memory demo table creation for BigQuery is not practical here.
        # Users should ensure their BigQuery tables exist.
    else:
        raise ValueError(f"Unsupported db_type: '{db_type}'. Choose 'duckdb' or 'bigquery'.")

    try:
        return compare_tables_internal(
            db_type=db_type,
            conn_or_client=conn_or_client,
            table1_name=table1,
            table2_name=table2,
            pk_cols=pk_cols,
            ignore_cols=ignore_cols,
            scalar_casts=scalar_casts,
            limit=limit
        )
    finally:
        if hasattr(conn_or_client, 'close') and conn_or_client is not None:
            if db_type == "duckdb":
                conn_or_client.close()


def main():
    """Command-line entry point."""
    args = parse_arguments()
    
    if not args.pk_cols:
        print("Error: --pk-cols cannot be empty.", file=sys.stderr)
        sys.exit(1)

    try:
        results = run_comparison(
            db_type=args.db_type,
            table1=args.TABLE1,
            table2=args.TABLE2,
            pk_cols=args.pk_cols,
            limit=args.limit,
            ignore_cols=args.ignore_cols,
            scalar_casts=args.scalar_casts
        )
        
        for diff in results:
            print(json.dumps(diff, default=str)) 
    except (ImportError, ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {type(e).__name__} - {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 