import matplotlib.pyplot as plt
import pandas as pd

from sqlalchemy import create_engine


def main():

    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw",
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success: bool = True
            print("Connection successful!")
    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    query_payment = "SELECT * FROM payment_dimension"
    query_time = "SELECT * FROM time_dimension"

    fare_amount(query_payment, engine)
    total_amount(query_payment, engine)
    tolls_amount(query_payment, engine)
    fare_vs_tip(query_payment, engine)
    distrib_trip(query_time, engine)


def fare_amount(data_payment, engine):
    data_payment = pd.read_sql(data_payment, engine)

    with plt.style.context("Solarize_Light2"):
        plt.figure(figsize=(10, 6))
        plt.hist(
            data_payment["fare_amount"], bins=50, color="skyblue", edgecolor="black"
        )
        plt.title("Distribution of Fare Amounts")
        plt.xlabel("Fare Amount")
        plt.ylabel("Frequency")
        plt.grid(True)
        plt.tight_layout()
        plt.show()


def total_amount(data_payment, engine):
    data_payment = pd.read_sql(data_payment, engine)

    with plt.style.context("Solarize_Light2"):
        plt.figure(figsize=(10, 6))
        plt.hist(
            data_payment["total_amount"], bins=50, color="skyblue", edgecolor="black"
        )
        plt.title("Distribution of Total Amount")
        plt.xlabel("Total Amount")
        plt.ylabel("Frequency")
        plt.grid(True)
        plt.tight_layout()
        plt.show()


def tolls_amount(data_payment, engine):
    data_payment = pd.read_sql(data_payment, engine)

    with plt.style.context("Solarize_Light2"):
        plt.figure(figsize=(10, 6))
        plt.hist(
            data_payment["tolls_amount"], bins=50, color="skyblue", edgecolor="black"
        )
        plt.title("Distribution of Tolls Amount")
        plt.xlabel("Tolls Amount")
        plt.ylabel("Frequency")
        plt.grid(True)
        plt.tight_layout()
        plt.show()


# fare_vs_tip/2: if there's a relationship, we would see one line.
# But because it's "spread_out", it means that there's not parallelism
def fare_vs_tip(data_payment, engine):
    data_payment = pd.read_sql(data_payment, engine)

    with plt.style.context("Solarize_Light2"):
        plt.figure(figsize=(10, 6))
        plt.scatter(
            data_payment["fare_amount"],
            data_payment["tip_amount"],
            color="skyblue",
            edgecolor="black",
        )
        plt.title("Fare Amount vs Tip Amount")
        plt.xlabel("Fare Amount")
        plt.ylabel("Tip Amount")
        plt.grid(True)
        plt.tight_layout()
        plt.show()


def distrib_trip(data_time, engine):
    data_time = pd.read_sql(data_time, engine)

    with plt.style.context("Solarize_Light2"):
        plt.figure(figsize=(10, 6))
        plt.hist(
            data_time["trip_distance"], bins="auto", color="skyblue", edgecolor="black"
        )
        plt.title("Distribution of Distance Trip")
        plt.xlabel("Lenght")
        plt.ylabel("Frequency")
        plt.grid(True)
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    main()
