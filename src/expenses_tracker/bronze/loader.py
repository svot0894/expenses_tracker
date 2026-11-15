
import logging
import shutil
from pathlib import Path
from typing import Dict, Any
from hashlib import sha256

import duckdb
import yaml


CONFIG_PATH = Path("config/banks.yml")
DATABASE_PATH = Path("data/expenses.duckdb")

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Configuration and discovery
# --------------------------------------------------

def load_config(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        config: Dict[str, Any] = yaml.safe_load(file)

    return config


def discover_files(
    incoming_folder: Path,
    banks: Dict[str, Any],
):
    """Yield (bank_name, file_path) pairs."""
    for bank, config in banks.items():
        config = config["bronze"]
        
        pattern = config["files"]["pattern"].format(
            period_start="*",
            period_end="*",
        )

        bank_folder = incoming_folder / bank

        for file_path in sorted(bank_folder.glob(pattern)):
            if file_path.is_file():
                yield bank, file_path


# --------------------------------------------------
# Database and ingestion history
# --------------------------------------------------

def initialize_database(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS
            _ingested_files_file_id_seq
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS _ingested_files (
            file_id INTEGER PRIMARY KEY
                DEFAULT nextval('_ingested_files_file_id_seq'),
            file_path TEXT NOT NULL,
            checksum TEXT NOT NULL,
            row_count INTEGER DEFAULT 0,
            status TEXT NOT NULL
                CHECK (status IN ('running', 'success', 'failed')),
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def get_latest_ingestion(conn: duckdb.DuckDBPyConnection, file_path: Path):
    return conn.execute(
        """
        SELECT file_id, checksum, status
        FROM _ingested_files
        WHERE file_path = ?
        ORDER BY file_id DESC
        LIMIT 1
        """,
        [str(file_path)],
    ).fetchone()


def start_ingestion(
    conn: duckdb.DuckDBPyConnection,
    file_path: Path,
    checksum: str,
) -> int:
    """Create a new ingestion attempt."""
    result = conn.execute(
        """
        INSERT INTO _ingested_files (
            file_path, checksum, status
        )
        VALUES (?, ?, 'running')
        RETURNING file_id
        """,
        [str(file_path), checksum],
    ).fetchone()

    return result[0] if result else 0


def update_ingestion(
    conn: duckdb.DuckDBPyConnection ,
    file_id: int,
    status: str,
    row_count: int = 0,
) -> None:
    conn.execute(
        """
        UPDATE _ingested_files
        SET status = ?, row_count = ?
        WHERE file_id = ?
        """,
        [status, row_count, file_id],
    )


# --------------------------------------------------
# CSV reading and schema validation
# --------------------------------------------------

def read_bank_file(conn: duckdb.DuckDBPyConnection, file_path: Path, bank_config: Dict[str, Any]):
    options = bank_config["read_options"]

    return conn.read_csv(
        str(file_path),
        delimiter=options["delimiter"],
        quotechar=options["quotechar"],
        encoding=options["encoding"],
        skiprows=options["skiplines"],
        all_varchar=True,
        header=True,
    )


def get_base_schema(bank_config: Dict[str, Any]) -> list[str]:
    return [
        column["name"]
        for column in bank_config["columns"]
    ]


def validate_schema(
    actual_columns: list[str],
    bank_config: Dict[str, Any],
) -> None:
    expected_columns = get_base_schema(bank_config)

    column_config = {
        column["name"]: column
        for column in bank_config["columns"]
    }

    extra_columns = set(actual_columns) - set(expected_columns)
    missing_columns = set(expected_columns) - set(actual_columns)

    if extra_columns:
        logger.warning(
            "Extra columns detected: %s",
            sorted(extra_columns),
        )

    for column in expected_columns:
        if column not in missing_columns:
            continue

        required = column_config.get(column, {}).get(
            "required", True
        )

        if required:
            raise ValueError(
                f"Missing required column: {column}"
            )

        logger.warning(
            "Missing optional column: %s",
            column,
        )


# --------------------------------------------------
# Bronze ingestion
# --------------------------------------------------
def quote_identifier(value: str) -> str:
    """Safely quote an identifier for use in SQL statements."""
    return '"' + value.replace('"', '""') + '"'


def write_bronze(
    conn: duckdb.DuckDBPyConnection,
    bank: str,
    bank_data: duckdb.DuckDBPyRelation,
    bank_config: Dict[str, Any],
) -> int:
    """Write columns by name in the expected schema order."""

    expected_columns = get_base_schema(bank_config)
    actual_columns = set(bank_data.columns)

    table_name = f"{bank}_raw"

    column_definitions = ", ".join(
        f"{quote_identifier(column)} VARCHAR"
        for column in expected_columns
    )

    target_columns = ", ".join(
        f"{quote_identifier(column)}"
        for column in expected_columns
    )

    projections: list[str] = []

    for column in expected_columns:
        if column in actual_columns:
            projections.append(quote_identifier(column))
        else:
            projections.append(
                f"NULL::VARCHAR AS {quote_identifier(column)}"
            )

    select_columns = ", ".join(projections)

    # The relation is only available during this function.
    view_name = "_current_ingestion_batch"
    bank_data.create_view(view_name, replace=True)

    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_definitions}
            )
            """
        )

        row_count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {quote_identifier(view_name)}
            """
        ).fetchone()

        conn.execute(
            f"""
            INSERT INTO {table_name} ({target_columns})
            SELECT {select_columns}
            FROM {quote_identifier(view_name)}
            """
        )

        return row_count[0] if row_count else 0

    finally:
        conn.execute(
            f"DROP VIEW IF EXISTS {view_name}"
        )


# --------------------------------------------------
# File archival
# --------------------------------------------------

def archive_file(
    file_path: Path,
    processed_folder: Path,
) -> None:
    processed_folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination = processed_folder / file_path.name

    if destination.exists():
        raise FileExistsError(
            f"Archive destination already exists: {destination}"
        )

    shutil.move(str(file_path), str(destination))

    logger.info("Archived file: %s", destination)


# --------------------------------------------------
# Process a single file
# --------------------------------------------------
def calculate_checksum(file_path: str) -> str:
    h = sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()


def process_file(
    conn: duckdb.DuckDBPyConnection,
    bank: str,
    file_path: Path,
    bank_config: Dict[str, Any],
    processed_folder: Path,
) -> None:
    logger.info("Processing file: %s", file_path)

    checksum = calculate_checksum(str(file_path))

    previous = get_latest_ingestion(conn, file_path)

    if previous:
        _, previous_checksum, previous_status = previous

        if previous_status == "success":
            if previous_checksum != checksum:
                raise ValueError(
                    f"Previously ingested file has changed: {file_path}"
                )

            logger.info("Already ingested: %s", file_path)
            archive_file(file_path, processed_folder)
            return

        if previous_status == "running":
            raise RuntimeError(
                f"Unresolved ingestion attempt for {file_path}. "
                "Verify its outcome before retrying."
            )

    file_id = start_ingestion(conn, file_path, checksum)

    try:
        bank_data = read_bank_file(
            conn,
            file_path,
            bank_config,
        )

        validate_schema(
            bank_data.columns,
            bank_config,
        )

        # Rows and the success status commit together.
        conn.execute("BEGIN TRANSACTION")

        try:
            row_count = write_bronze(
                conn,
                bank,
                bank_data,
                bank_config,
            )

            update_ingestion(
                conn,
                file_id,
                status="success",
                row_count=row_count,
            )

            conn.execute("COMMIT")

        except Exception:
            conn.execute("ROLLBACK")
            raise

    except Exception:
        update_ingestion(
            conn,
            file_id,
            status="failed",
        )

        logger.exception(
            "Ingestion failed: %s",
            file_path,
        )
        raise

    logger.info(
        "Successfully ingested %s (%d rows)",
        file_path,
        row_count,
    )

    # Archival happens after the database commit.
    archive_file(file_path, processed_folder)


# --------------------------------------------------
# Main orchestration
# --------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    config = load_config(CONFIG_PATH)

    data_folder = Path(config["data_folder"])
    incoming_folder = data_folder / "incoming"
    processed_folder = data_folder / "processed"

    files = list(
        discover_files(
            incoming_folder,
            config["banks"],
        )
    )

    if not files:
        logger.info("No files found.")
        return

    DATABASE_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with duckdb.connect(str(DATABASE_PATH)) as conn:
        initialize_database(conn)

        for bank, file_path in files:
            process_file(
                conn=conn,
                bank=bank,
                file_path=file_path,
                bank_config=config["banks"][bank]["bronze"],
                processed_folder=processed_folder / bank,
            )


if __name__ == "__main__":
    main()