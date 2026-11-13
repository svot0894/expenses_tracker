import glob
import os
import shutil

import duckdb as dd
import yaml

from expenses_tracker.common.calculations import calculate_checksum

with open("config/banks.yml", "r") as f:
    banks_config = yaml.safe_load(f)

INCOMING_FOLDER = f'{banks_config["data_folder"]}/incoming'
PROCESSED_FOLDER = f'{banks_config["data_folder"]}/processed'


# connect to duckdb (use context manager)
conn = dd.connect(database="data/expenses.duckdb")

# make sure the _ingested_files table exists
conn.execute("""
CREATE SEQUENCE IF NOT EXISTS _ingested_files_file_id_seq;
CREATE TABLE IF NOT EXISTS _ingested_files (
    file_id INTEGER PRIMARY KEY DEFAULT nextval('_ingested_files_file_id_seq'),
    file_path TEXT NOT NULL,
    checksum TEXT,
    row_count INT DEFAULT 0,
    status TEXT check (status IN ('running', 'success', 'failed')),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")


# find files matching the pattern defined in the banks.yml configuration
files = []
for bank, config in banks_config["banks"].items():
    pattern = config["files"]["pattern"]
    final_pattern = f"{INCOMING_FOLDER}/{bank}/{pattern.format(period_start='*', period_end='*')}"  # TBD
    bank_files = glob.glob(final_pattern)
    files.append((bank, bank_files))


if not any(bank_files for _, bank_files in files):
    print("No files found.")
    exit()


# check _ingested_files to decide whether the file is already ingested
for bank, bank_files in files:
    print(f"Processing files for bank: {bank}...")
    for file in bank_files:
        print(f"Processing file: {file}...")
        try:
            ingested = conn.execute("SELECT file_id, status FROM _ingested_files WHERE file_path = ? ORDER BY ingested_at DESC LIMIT 1", (file,)).fetchone()
            if ingested and ingested[1] == 'success':
                print(f"File already ingested: {file}")
                continue
            elif ingested and ingested[1] == 'failed':
                conn.execute("UPDATE _ingested_files SET checksum = ?, status = ? WHERE file_id = ?", (calculate_checksum(file), 'running', ingested[0]))
            else:
                conn.execute("INSERT INTO _ingested_files (file_path, checksum, status) VALUES (?, ?, ?)", (file, calculate_checksum(file), 'running'))
        except Exception as e:
            raise RuntimeError(f"Failed to check or mark file: {file}. Error: {e}")


        try:
            bank_config = banks_config["banks"][bank]
            read_options = bank_config["read_options"]
            bank_data = conn.read_csv(
                file,
                delimiter=read_options["delimiter"],
                quotechar=read_options["quotechar"],
                encoding=read_options["encoding"],
                skiprows=read_options["skiplines"],
                all_varchar=True,
                header=True
            )
        except Exception as e:
            conn.execute("UPDATE _ingested_files SET status = ? WHERE file_path = ?", ('failed', file))
            raise RuntimeError(f"Failed to read file: {file}. Error: {e}")

        # VALIDATION: schema drift
            # extra column: warn
            # missing optional column: warn
            # missing required column: fail
        
        current_header = bank_data.columns
        base_schema = bank_config["base_schema"].split(";")

        extra_columns = [col for col in current_header if col not in base_schema]
        missing_columns = [col for col in base_schema if col not in current_header]

        if extra_columns:
            print(f"Warning: Extra columns detected in {file}: {extra_columns}")
        for col in missing_columns:
            col_conf = next((col_conf for col_conf in bank_config["columns"] if col_conf["name"] == col), {})
            if col_conf.get("required", True):
                raise ValueError(f"Missing required column {col} in {file}")
            else:
                print(f"Warning: Missing optional column {col} in {file}")

        # write rows to bronze table and record ingested rows
        conn.execute(f"CREATE TABLE IF NOT EXISTS {bank}_raw AS SELECT * FROM bank_data WHERE 1=0")
        conn.execute(f"INSERT INTO {bank}_raw SELECT * FROM bank_data")

        # row count
        row_count = bank_data.shape[0]

        # log the ingestion result (success or failure)
        conn.execute("UPDATE _ingested_files SET row_count = ?, status = ? WHERE file_path = ?", (row_count, 'success', file))
        print(f"Successfully ingested file: {file}")

        # move file to processed folder
        processed_folder = f"{PROCESSED_FOLDER}/{bank}"
        os.makedirs(processed_folder, exist_ok=True)
        shutil.move(file, processed_folder)
conn.close()