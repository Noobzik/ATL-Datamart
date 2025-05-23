import gc
import sys
import json
import signal
import os
from pathlib import Path
from io import BytesIO
from minio import Minio
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, DateTime, text
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, TimeRemainingColumn
from rich import print

# Add state management
STATE_FILE = Path(__file__).parent / "import_state.json"

def load_state():
    if (STATE_FILE).exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_file": None, "rows_inserted": 0}

def save_state(filename, rows_inserted):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_file": filename, "rows_inserted": rows_inserted}, f)

def ensure_table_exists(engine, dataframe: pd.DataFrame, table_name: str) -> None:
    """
    Creates the table if it doesn't exist based on DataFrame schema
    """
    metadata = MetaData()

    # Map pandas dtypes to SQLAlchemy types
    type_map = {
        'int64': Integer,
        'float64': Float,
        'object': String,
        'datetime64[ns]': DateTime
    }

    # Create table definition
    columns = []
    for column_name, dtype in dataframe.dtypes.items():
        sql_type = type_map.get(str(dtype), String)
        columns.append(Column(column_name, sql_type))

    # Define table
    Table(table_name, metadata, *columns)

    # Create table if it doesn't exist
    metadata.create_all(engine)


def write_data_postgres(dataframe: pd.DataFrame, filename: str, resume_from: int = 0) -> bool:
    """
    Dumps a Dataframe to the DBMS engine with resumption capability
    """
    # Add signal handling
    interrupted = False
    def signal_handler(signum, frame):
        nonlocal interrupted
        print("\n[yellow]Interrupt received, completing current chunk...")
        interrupted = True

    signal.signal(signal.SIGINT, signal_handler)

    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect() as connection:
            # Validate table before starting
            try:
                connection.execute(f"SELECT 1 FROM {db_config['dbms_table']} LIMIT 1")
                progress.print("[green]Table validation successful")
            except Exception:
                # Table doesn't exist or is corrupted, create new
                ensure_table_exists(engine, dataframe, db_config["dbms_table"])
                return True

            success: bool = True
            chunk_size = 50000
            total_rows = len(dataframe)
            rows_inserted = resume_from

            with Progress(
                SpinnerColumn(),
                *Progress.get_default_columns(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                refresh_per_second=1
            ) as progress:
                task = progress.add_task(
                    f"[cyan]Inserting {total_rows:,} rows (resuming from {resume_from:,})...",
                    total=total_rows
                )
                progress.update(task, completed=resume_from)

                for chunk_start in range(resume_from, total_rows, chunk_size):
                    if interrupted:
                        progress.print("[yellow]Gracefully stopping after current chunk")
                        break

                    chunk_end = min(chunk_start + chunk_size, total_rows)
                    chunk = dataframe.iloc[chunk_start:chunk_end]

                    # Each chunk gets its own transaction
                    try:
                        with connection.begin():
                            chunk.to_sql(
                                db_config["dbms_table"],
                                connection,
                                index=False,
                                if_exists='append',
                                method='multi'
                            )
                            # Verify chunk insertion by checking total rows
                            verify_sql = text(
                                f"SELECT COUNT(*) FROM {db_config['dbms_table']}"
                            )
                            total_in_db = connection.execute(verify_sql).scalar()
                            expected_rows = resume_from + rows_inserted + len(chunk)

                            if total_in_db == expected_rows:
                                rows_inserted += len(chunk)
                                progress.update(task, completed=rows_inserted)
                                progress.print(f"Chunk verified: {rows_inserted:,}/{total_rows:,} rows (Total in DB: {total_in_db:,})")
                                save_state(filename, rows_inserted)
                            else:
                                raise Exception(
                                    f"Chunk verification failed: DB has {total_in_db:,} rows, "
                                    f"expected {expected_rows:,}"
                                )

                    except Exception as chunk_error:
                        progress.print(f"[red]Error inserting chunk: {chunk_error}")
                        save_state(filename, rows_inserted)  # Save progress on error
                        continue

                final_status = "[green]" if rows_inserted == total_rows else "[yellow]"
                progress.print(f"{final_status}Inserted and verified {rows_inserted:,}/{total_rows:,} rows")

    except Exception as e:
        success: bool = False
        print(f"[red]Critical Error: {str(e)}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def main() -> None:
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "my-bucket"

    # Load previous state
    state = load_state()

    for obj in client.list_objects(bucket):
        try:
            data = client.get_object(bucket, obj.object_name)
            buffer = BytesIO(data.read())
            parquet_df: pd.DataFrame = pd.read_parquet(buffer, engine='pyarrow')

            # Display parquet file statistics
            print(f"\nFile: {obj.object_name}")
            print(f"Total rows: {len(parquet_df):,}")
            print(f"Total columns: {len(parquet_df.columns)}")
            print(f"Memory usage: {parquet_df.memory_usage().sum() / 1024**2:.2f} MB")
            # print("\nColumn types:")
            # print(parquet_df.dtypes)
            # print("-" * 50)

            clean_column_name(parquet_df)
            # Resume from last position if it's the interrupted file
            resume_from = state["rows_inserted"] if obj.object_name == state["last_file"] else 0
            if not write_data_postgres(parquet_df, obj.object_name, resume_from):
                del parquet_df
                gc.collect()
                return

            del parquet_df
            gc.collect()

        except Exception as e:
            print(f"Error processing file {obj.object_name}: {e}")
            continue

    # Clear state file after successful completion
    # if STATE_FILE.exists():
    #     os.remove(STATE_FILE)


if __name__ == '__main__':
    sys.exit(main())
