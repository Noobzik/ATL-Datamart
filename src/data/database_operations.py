import os
import logging
import pandas as pd
import urllib.request
from sqlalchemy import create_engine, text
from urllib.error import URLError, HTTPError

def connect_to_database(database_name):
    """
    Connect to the database

    Parameters:
        - database_name (str): The name of the database to connect to.

    Returns:
        - engine: The database engine, None if the connection fails.
    """
    db_config = {
        "dbms_engine": os.getenv("DBMS_ENGINE"),
        "dbms_username": os.getenv("DBMS_USERNAME"),
        "dbms_password": os.getenv("DBMS_PASSWORD"),
        "dbms_ip": os.getenv("DBMS_IP"),
        "dbms_port": os.getenv("DBMS_PORT"),
    }
    db_config["dbms_database"] = database_name
    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )

    engine = create_engine(db_config["database_url"])
    try:
        with engine.connect():
            logging.info("Connection successful!")
            return engine
            
    except Exception as e:
        logging.error(f"Error connection to the database: {e}")
        return None


def disconnect_from_database(engine):
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


def drop_table(engine, table_name):
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
            connection.commit()
            logging.info(f"Table {table_name} dropped successfully.")
            return True

    except Exception as e:
        logging.error(f"Error dropping table {table_name}: {e}")
        return False


def execute_sql_file(file, engine):
    """
    Execute a SQL file in the database

    Parameters:
        - file (str): The path to the SQL file to execute.
        - engine: The database engine.

    Returns:
        - bool: True if the file is executed successfully, False otherwise.
    """
    with open(file, "r") as file:
        try:
            query = file.read()
            with engine.connect() as connection:
                connection.execute(text(query))
                connection.commit()
                logging.info(f"File {file} executed successfully.")
                return True
        
        except Exception as e:
            logging.error(f"Error executing file {file}: {e}")
            return False


def initialize_datamart(folder_path: str):
    """
    Initialize the datamart database

    Parameters:
        - folder_path (str): The path to the folder containing the Parquet files.

    Returns:
        - bool: True if the datamart is initialized successfully, False otherwise.
    """
    # Connect to the datamart database
    datamart_engine = connect_to_database(os.getenv("DBMS_DATAMART_DATABASE"))
    if datamart_engine is None:
        return False

    # Create tables
    if execute_sql_file("creation.sql", datamart_engine):
        logging.info("Tables created successfully.")
    else:
        logging.error("Error creating tables.")
        return False

    # Load csv file to load data in dim_location table
    url = f"{os.getenv('DATA_SOURCE_BASE_URL')}/misc/taxi_zone_lookup.csv"
    file_path = os.path.join(folder_path, "taxi_zone_lookup.csv")
    try:
        if not os.path.exists(file_path):
            urllib.request.urlretrieve(url, file_path)
        logging.info("taxi_zone_lookup.csv downloaded successfully.")
        columns = ["location_id", "borough", "zone", "service_zone"]
        try:
            df_location = pd.read_csv(file_path, names=columns, header=0)
            df_location.to_sql("dim_location", datamart_engine, if_exists="append", index=False)
            logging.info("Data from taxi_zone_lookup.csv loaded into dim_location table successfully.")
        except Exception as e:
            logging.error(f"Error loading data from taxi_zone_lookup.csv into dim_location table: {e}")
            return False
    except (URLError) as e:
        logging.error(f"Error downloading taxi_zone_lookup.csv: {str(e)}")
        return False

    # Insert data into tables (wait...)
    if execute_sql_file("insertion.sql", datamart_engine):
        logging.info("Data inserted successfully.")
    else:
        logging.error("Error inserting data.")
        return False

    # Disconnect from the databases
    if not disconnect_from_database(datamart_engine):
        return False

    return True