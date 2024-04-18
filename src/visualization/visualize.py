from database_operations import connect_to_database, disconnect_from_database

def main():
    # Connect to the datamart database
    datamart_engine = connect_to_database(os.getenv('DBMS_DATAMART_DATABASE'))
    if datamart_engine is None:
        sys.exit(1)