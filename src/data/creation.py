import sys

from sqlalchemy import create_engine, text

def execute_sql_file(file_path: str, db_config: dict) -> bool:
    """
    Execute a SQL file into the DBMS engine

    Parameters:
        - file_path (str) : The path to the SQL file to execute
        - db_config (dict) : The configuration of the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the execution of the SQL file is successful, False if either
        execution is failed
    """
    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        print(db_config["database_url"])
        engine = create_engine(db_config["database_url"])
        with engine.connect() as connection:
            success: bool = True
            with open(file_path, 'r') as file:
                sql_commands = file.read().split(';')
                for command in sql_commands:
                    if command.strip() != '':
                        connection.execute(text(command))
                        print(connection.execute(text(command)))
            print("SQL file executed successfully.")
        return True
    
    except Exception as e:
        success: bool = False
        print(f"Error executing the SQL file: {e}")
        return success
    
def main() -> None:
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_market"
    }
    sql_file_path = "creation.sql"
    execute_sql_file(sql_file_path, db_config)
    
if __name__ == "__main__":
    sys.exit(main())