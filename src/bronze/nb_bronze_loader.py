import glob
from hashlib import sha256
import duckdb as dd
import yaml

with open("config/banks.yml", "r") as f:
    banks_config = yaml.safe_load(f)

DATA_FOLDER = banks_config["data_folder"]

# connect to duckdb (use context manager)
with dd.connect(database="data/expenses.duckdb") as conn:

    # make sure the _ingested_files table exists
    conn.execute("""
    CREATE OR REPLACE SEQUENCE _ingested_files_file_id_seq;
    CREATE OR REPLACE TABLE _ingested_files (
        file_id INTEGER PRIMARY KEY DEFAULT nextval('_ingested_files_file_id_seq'),
        file_path TEXT NOT NULL,
        checksum TEXT,
        status TEXT check (status IN ('running', 'success', 'failed')),
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    def _calculate_checksum(file_path):
        h = sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    # find files matching the pattern defined in the banks.yml configuration
    files = dict()
    for bank, config in banks_config["banks"].items():
        pattern = config["files"]["pattern"]
        final_pattern = f"{DATA_FOLDER}/{bank}/{pattern.format(period_start='*', period_end='*')}"
        bank_files = glob.glob(final_pattern)
        files[bank] = bank_files
        print(f"Found files for {bank}: {bank_files}")

    # check _ingested_files to decide whether the file is already ingested
    for bank, bank_files in files.items():
        for file in bank_files:
            try:
                ingested = conn.execute("SELECT 1 FROM _ingested_files WHERE file_path = ? AND status = 'success'", (file,)).fetchone()
                if ingested:
                    print(f"File already ingested: {file}")
                    continue
                # mark the file as to be ingested
                conn.execute("INSERT INTO _ingested_files (file_path, checksum, status) VALUES (?, ?, ?)", (file, _calculate_checksum(file), 'running'))
            except Exception as e:
                print(f"Failed to check or mark file: {file}. Error: {e}")
                continue


            # read file using duckdb
            try:
                bank_config = banks_config["banks"][bank]
                read_options = bank_config["read_options"]
                df = dd.read_csv(
                    file,
                    delimiter=read_options["delimiter"],
                    quotechar=read_options["quotechar"],
                    encoding=read_options["encoding"],
                    skiprows=read_options["skiplines"],
                    all_varchar=True,
                    header=True
                ).to_df()
            except Exception as e:
                print(f"Failed to read file: {file}. Error: {e}")
                conn.execute("UPDATE _ingested_files SET status = ? WHERE file_path = ?", ('failed', file))
                continue

            # compare header against the base_schema defined in the banks.yml configuration
            # if drifts are detected:
                # extra column: warn
                # missing optional column: warn
                # missing required column: fail
            current_header = df.columns.tolist()
            base_schema = bank_config["base_schema"].split(";")
            extra_columns = [col for col in current_header if col not in base_schema]
            missing_columns = [col for col in base_schema if col not in current_header]

            if extra_columns:
                print(f"Warning: Extra columns detected in {file}: {extra_columns}")
            for col in missing_columns:
                if col not in base_schema:
                    continue
                if next((col_conf for col_conf in bank_config["columns"] if col_conf["name"] == col), {}).get("required", True):
                    raise ValueError(f"Missing required column {col} in {file}")
                else:
                    print(f"Warning: Missing optional column {col} in {file}")

            # write rows to bronze table and record ingested rows
            conn.execute(f"CREATE TABLE IF NOT EXISTS {bank}_raw AS SELECT * FROM df WHERE 1=0")
            conn.execute(f"INSERT INTO {bank}_raw SELECT * FROM df")

            # log the ingestion result (success or failure)
            conn.execute("UPDATE _ingested_files SET status = ? WHERE file_path = ?", ('success', file))
            print(f"Successfully ingested file: {file}")

conn.close()