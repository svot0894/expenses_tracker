
import logging
from pathlib import Path
from typing import Any

import duckdb
import yaml

from .transformations.transformations import TRANSFORMATIONS
from .validations.validations import VALIDATIONS

CONFIG_PATH = Path("config/banks.yml")
DATABASE_PATH = Path("data/expenses.duckdb")

logger = logging.getLogger(__name__)


# step 0: load configuration for the given bank
def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        config: dict[str, Any] = yaml.safe_load(file)

    return config

# step 1: initialize silver tables ({bank_name}_ready and quarantine_records)
def initialize_silver_tables(conn: duckdb.DuckDBPyConnection, bank_name: str):
    
    # Create ingestion log table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _silver_processed (
            source_file_id INTEGER PRIMARY KEY,
            bank_name VARCHAR NOT NULL,
            row_count_valid BIGINT NOT NULL,
            row_count_invalid BIGINT NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Create silver tables for the given bank
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {bank_name}_ready (
            transaction_id STRING PRIMARY KEY,
            transaction_date DATE,
            description VARCHAR,
            amount DECIMAL(18, 2),
            category VARCHAR,
            account VARCHAR,
            currency VARCHAR
        );
    """)

    # Create quarantine table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_records (
            quarantine_id UUID PRIMARY KEY DEFAULT uuid(),
            source_bank VARCHAR NOT NULL,
            source_table VARCHAR NOT NULL,
            source_file_id INTEGER,
            source_row_id VARCHAR,
            record_data JSON NOT NULL,
            validation_errors JSON NOT NULL,
            status VARCHAR NOT NULL DEFAULT 'review' CHECK (status in ('review', 'ready', 'ignore')),
            quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

# Step 1.5: determine which files are pending ingestion
def get_pending_files(conn: duckdb.DuckDBPyConnection, bank_name: str):
    """Gets pending files by comparing _ingested_files against _silver_processed."""

    results = conn.execute(f"""
    SELECT DISTINCT if.file_id
    FROM _ingested_files as if
    LEFT JOIN _silver_processed as sp
        ON if.file_id = sp.source_file_id
    WHERE
        sp.source_file_id is NULL
        AND if.status = 'success'
        AND if.file_path LIKE '%{bank_name}%'
        ;
    """).fetchall()

    return [row[0] for row in results]

# step 2: read data from source bronze table ({bank_name}_raw)
def read_bronze_data(conn: duckdb.DuckDBPyConnection, bank_name: str, file_id: int) -> duckdb.DuckDBPyRelation:
    """Reads data from bronze table ({bank_name}_raw)."""
    return conn.sql(
        f"""
        SELECT *
        FROM {bank_name}_raw
        WHERE "_source_file_id" = ?
        """,
        params=[file_id],
    )

# step 3: apply transformations
def transform_data(bronze_data: duckdb.DuckDBPyRelation, config: dict[str, Any]):
    if "mappings" in config:
        # rename columns
        rename_map: dict[str, str] = {source: target for target, source in config["mappings"].items()}
        bronze_data = bronze_data.rename(**rename_map)

    if "transformations" in config:
        for rule in config["transformations"]:
            column, operation = rule["column"], rule["operation"]

            transformation = TRANSFORMATIONS.get(operation)

            if transformation is None:
                raise ValueError(
                    f"Unknown transformation: {operation}"
                )

            # parameters for the transformation function
            params = {
                key: value
                for key, value in rule.items()
                if key not in ("operation", "column")
            }

            # Most transformations operate on their target column.
            if operation != "combine_columns":
                params["column"] = column

            expression = TRANSFORMATIONS[operation](**params)

            # Update an existing column or create a new one
            if column.lower() in {c.lower() for c in bronze_data.columns}:
                bronze_data = bronze_data.project(
                    f"* REPLACE ({expression} AS {column})"
                )
            else:
                bronze_data = bronze_data.project(
                    f"*, {expression} AS {column}"
                )

    return bronze_data

# step 4: apply validations (splits data into valid and invalid rows)
def validate_data(
    data: duckdb.DuckDBPyRelation,
    bank_config: dict[str, Any],
) -> tuple[
    duckdb.DuckDBPyRelation,
    duckdb.DuckDBPyRelation,
]:
    """Split data into valid and invalid DuckDB relations."""

    rules = bank_config.get("validations", [])
    error_expressions: list[str] = []

    for rule in rules:
        column, operation = rule["column"], rule["operation"]

        validator = VALIDATIONS.get(operation)

        if validator is None:
            raise ValueError(
                f"Unknown validation: {operation}"
            )

        if column not in data.columns:
            raise ValueError(
                f"Validation column does not exist: {column}"
            )

        # SQL condition identifying invalid rows
        condition = validator(column)

        error_message = f"{column}: {operation}"

        error_expressions.append(
            f"CASE WHEN ({condition}) "
            f"THEN '{error_message}' "
            f"ELSE NULL END"
        )

    # Build a list of failed validation rules
    if error_expressions:
        errors_sql = (
            "list_filter("
            f"[{', '.join(error_expressions)}], "
            "lambda x : x IS NOT NULL)"
        )
    else:
        errors_sql = "[]::VARCHAR[]"

    validated_data = data.project(
        f"*, {errors_sql} AS __validation_errors"
    )

    valid_data = validated_data.filter(
        "len(__validation_errors) = 0"
    )

    invalid_data = validated_data.filter(
        "len(__validation_errors) > 0"
    )

    return valid_data, invalid_data


# step 5: write valid rows to silver table ({bank_name}_ready)
def write_valid_data(
    valid_data: duckdb.DuckDBPyRelation,
    bank_name: str,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Write valid records to the bank's Silver table."""

    silver_columns = [
        "transaction_id",
        "transaction_date",
        "description",
        "amount",
        "account",
        "currency",
    ]

    valid_data.create_view("tmp_valid_data", replace=True)

    columns = ", ".join(
        col for col in silver_columns if col in valid_data.columns
    )

    try:
        conn.execute(
            f"""
            INSERT INTO {bank_name}_ready BY NAME
            SELECT {columns}
            FROM tmp_valid_data
            """
        )
    finally:
        conn.execute("DROP VIEW IF EXISTS tmp_valid_data")


# step 6: write invalid rows to shared quarantine table for further investigation (quarantine_records)
def write_invalid_data(invalid_data: duckdb.DuckDBPyRelation, bank_name: str, conn: duckdb.DuckDBPyConnection) -> None:
    """Writes invalid data into quarantine table."""

    quarantine_columns = [
            "source_bank",
            "source_table",
            "source_file_id",
            "source_row_id",
            "record_data",
            "validation_errors"
    ]

    # Preserve the original record as JSON and attach its validation failures.
    record_columns = [
        column for column in invalid_data.columns
        if column != "__validation_errors"
    ]
    record_fields = ", ".join(
        f'"{column}" : "{column}"' for column in record_columns
    )
    source_file_id = (
        '"source_file_id"' if "source_file_id" in invalid_data.columns else "NULL"
    )
    source_row_id = (
        '"source_row_id"' if "source_row_id" in invalid_data.columns else "NULL"
    )
    quarantine_data = invalid_data.project(
        f"'{bank_name}' AS source_bank, "
        f"'{bank_name}_raw' AS source_table, "
        f"{source_file_id} AS source_file_id, "
        f"{source_row_id} AS source_row_id, "
        f"to_json({{{record_fields}}}) AS record_data, "
        "to_json(__validation_errors) AS validation_errors"
    )

    quarantine_data.create_view("tmp_invalid_data", replace=True)

    try:
        conn.execute(
            f"""
            INSERT INTO quarantine_records ({', '.join(quarantine_columns)})
            SELECT {', '.join(quarantine_columns)}
            FROM tmp_invalid_data
            """
        )
    finally:
        conn.execute("DROP VIEW IF EXISTS tmp_invalid_data")

# step 7: orchestrate the entire process for a given bank
def process_bank_data(
    conn: duckdb.DuckDBPyConnection,
    bank_name: str,
    bank_config: dict[str, Any],
) -> None:

    initialize_silver_tables(conn, bank_name)

    pending_files = get_pending_files(conn, bank_name)

    for file_id in pending_files:
        logger.info(
            "Processing Silver batch %s for %s",
            file_id,
            bank_name,
        )

        conn.execute("BEGIN TRANSACTION")

        try:
            bronze_data = read_bronze_data(
                conn,
                bank_name,
                file_id,
            )

            transformed_data = transform_data(
                bronze_data,
                bank_config,
            )

            valid_data, invalid_data = validate_data(
                transformed_data,
                bank_config,
            )

            valid_count: int = len(valid_data)
            invalid_count: int = len(invalid_data)

            write_valid_data(valid_data, bank_name, conn)
            write_invalid_data(invalid_data, bank_name, conn)

            conn.execute(
                """
                INSERT INTO _silver_processed (
                    source_file_id,
                    bank_name,
                    row_count_valid,
                    row_count_invalid
                )
                VALUES (?, ?, ?, ?)
                """,
                [
                    file_id,
                    bank_name,
                    valid_count,
                    invalid_count,
                ],
            )

            conn.execute("COMMIT")

        except Exception:
            conn.execute("ROLLBACK")
            logger.exception(
                "Silver processing failed for batch %s",
                file_id,
            )
            raise

# MAIN
def main():
    logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
        )
    
    config = load_config(CONFIG_PATH)

    # initialize database connection
    conn = duckdb.connect(DATABASE_PATH)

    for bank, config in config["banks"].items():
        logger.info(f"Processing data for bank: {bank}")
        process_bank_data(conn, bank, config["silver"])
        logger.info(f"Finished processing data for bank: {bank}")

if __name__ == "__main__":
    main()