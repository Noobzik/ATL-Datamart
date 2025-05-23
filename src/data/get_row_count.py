import sys
from sqlalchemy import create_engine, text

# Database configuration
db_config = {
    "dbms_engine": "postgresql",
    "dbms_username": "admin",
    "dbms_password": "admin",
    "dbms_ip": "localhost",
    "dbms_port": "15432",
    "dbms_database": "nyc_warehouse",
    "dbms_table": "nyc_raw"
}

database_url = (
    f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
    f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
)

def get_row_count():
    engine = create_engine(database_url)
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT COUNT(*) FROM {db_config['dbms_table']}"))
        count = result.scalar()
        print(f"Table '{db_config['dbms_table']}' has {count:,} rows.")

if __name__ == "__main__":
    get_row_count()
