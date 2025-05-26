import sys
import psycopg2
from pathlib import Path
from rich import print

def main():
    sql_path = Path(__file__).parent.parent.parent / "models/datamart.sql"
    if not sql_path.exists():
        print(f"[red]SQL file not found: {sql_path}")
        sys.exit(1)
    with open(sql_path) as f:
        sql = f.read()
    # Only connect to the datamart DB and run the SQL
    try:
        conn = psycopg2.connect(
            dbname="nyc_datamart",
            user="admin",
            password="admin",
            host="localhost",
            port="15435"
        )
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        conn.close()
        print("[green]Datamart schema loaded successfully.")
    except Exception as e:
        print(f"[red]Error loading datamart schema: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
