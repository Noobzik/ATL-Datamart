import os
import sys
import logging
import calendar
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text

def main():
    logging.info("test")
    # Connect to the datamart database
    db_config = {
        "dbms_engine": os.getenv("DBMS_ENGINE"),
        "dbms_username": os.getenv("DBMS_USERNAME"),
        "dbms_password": os.getenv("DBMS_PASSWORD"),
        "dbms_ip": os.getenv("DBMS_IP"),
        "dbms_port": os.getenv("DBMS_PORT"),
        "dbms_database": os.getenv("DBMS_DATAMART_DATABASE"),
    }
    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )

    datamart_engine = create_engine(db_config["database_url"])
    try:
        with datamart_engine.connect() as conn:
            logging.info("Connection to the database successful.")

            # Load data from the database
            print(f"Loading data from the {os.getenv('DBMS_DATAMART_DATABASE')} database...")
            query = 'SELECT * FROM "fact_trip_without_outliers" LIMIT 1000000 ;'
            res = conn.execute(text(query))
            df = pd.DataFrame(res.fetchall(), columns=res.keys())
            print("Data loaded successfully")

            # KPIs calculation
            total_trips = len(df)
            df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60  # Duration in minutes
            average_trip_duration = df['trip_duration'].mean()
            average_trip_distance = df['trip_distance'].mean()
            total_fare_amount = df['fare_amount'].sum()
            percentage_extra_fare = (df['extra'] > 0).mean() * 100

            # Visualizations
            plt.figure(figsize=(12, 8))


            # Visualization 1: Total trips
            # Extract month and year from the datetime column
            df['pickup_month'] = df['tpep_pickup_datetime'].dt.month
            df['pickup_year'] = df['tpep_pickup_datetime'].dt.year

            # Group by month and year to count the number of trips
            monthly_trips = df.groupby(['pickup_year', 'pickup_month']).size().reset_index(name='trip_count')

            # Plot the number of trips per month for each year
            plt.subplot(2, 3, 1)
            sns.barplot(x='pickup_month', y='trip_count', hue='pickup_year', data=monthly_trips)
            plt.title('Number of Trips per Month')
            plt.xlabel('Month')
            plt.ylabel('Number of Trips')
            plt.xticks(ticks=range(1, 13), labels=[calendar.month_abbr[i] for i in range(1, 13)])  # Use abbreviated month names
            plt.legend(title='Year')


            # Visualization 2: Passenger count
            plt.subplot(2, 3, 2)
            sns.countplot(x=df['passenger_count'])
            plt.title('Number of Trips by Passenger Count')
            plt.xlabel('Passenger Count')
            plt.ylabel('Number of Trips')


            # Visualization 3: Trips by day
            plt.subplot(2, 3, 3)
            sns.countplot(x=df['tpep_pickup_datetime'].dt.day_name())
            plt.title('Number of Trips by Day of the Week')
            plt.xlabel('Day of the Week')
            plt.ylabel('Number of Trips')


            # Visualization 4: Trips by hours 
            plt.subplot(2, 3, 4)
            sns.countplot(x=df['tpep_pickup_datetime'].dt.hour)
            plt.title('Number of Trips by Hour of the Day')
            plt.xlabel('Hour of the Day')
            plt.ylabel('Number of Trips')


            # Visualization 6: Trip duration
            plt.subplot(2, 3, 5)
            sns.histplot(df['trip_duration'], bins=30, kde=True)
            plt.title('Distribution of Trip Duration')
            plt.xlabel('Trip Duration (minutes)')
            plt.ylabel('Number of Trips')


            # Visualization 6: Total amount
            plt.subplot(2, 3, 6)
            sns.histplot(df['total_amount'], bins=30, kde=True)
            plt.title('Distribution of Total Amount')
            plt.xlabel('Total Amount ($)')
            plt.ylabel('Number of Trips')


            plt.tight_layout()
            plt.show()


            # Print KPIs
            print("Key Performance Indicators (KPIs):")
            print(f"Total Trips: {total_trips}")
            print(f"Average Trip Duration: {average_trip_duration:.2f} minutes")
            print(f"Average Trip Distance: {average_trip_distance:.2f} miles")
            print(f"Total Fare Amount: ${total_fare_amount:.2f}")
            print(f"Percentage of trips with extra fare: {percentage_extra_fare:.2f}%")

    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        sys.exit(1)

    datamart_engine.dispose()

if __name__ == '__main__':
    sys.exit(main())
