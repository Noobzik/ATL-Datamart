import os
import logging
from sqlalchemy import create_engine, text

db_config = {
    "dbms_engine": os.getenv("DBMS_ENGINE"),
    "dbms_username": os.getenv("DBMS_USERNAME"),
    "dbms_password": os.getenv("DBMS_PASSWORD"),
    "dbms_ip": os.getenv("DBMS_IP"),
    "dbms_port": os.getenv("DBMS_PORT"),
    "dbms_database": os.getenv("DBMS_DATABASE"),
}
db_config["database_url"] = (
    f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
    f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
)

engine = create_engine(db_config["database_url"])


def connect_to_database():
    """
    Connect to the database

    Returns:
        - bool: True if the connection is successful, False otherwise.
    """
    try:
        with engine.connect():
            logging.info("Connection successful!")
            return True
            
    except Exception as e:
        logging.error(f"Error connection to the database: {e}")
        return False


def disconnect_from_database():
    """
    Disconnect from the database

    Parameters:
        - engine: The database engine.
        
    Returns:
        - bool: True if the disconnection is successful, False otherwise.
    """
    try:
        engine.dispose()
        logging.info("Disconnected from the database.")
        return True

    except Exception as e:
        logging.error(f"Error disconnecting from the database: {e}")
        return False


def drop_table(table_name):
    """
    Drop a table in the database

    Parameters:
        - engine: The database engine.
        - table_name (str): The name of the table to drop.

    Returns:
        - bool: True if the table is dropped successfully, False otherwise.
    """
    try:
        with engine.connect() as connection:
            query = text(f"DROP TABLE IF EXISTS {table_name}")
            connection.execute(query)
            logging.info(f"Table {table_name} dropped successfully.")
            return True

    except Exception as e:
        logging.error(f"Error dropping table {table_name}: {e}")
        return False