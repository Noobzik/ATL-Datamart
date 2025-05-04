import requests

# URL of the file to download
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

# Path where the file will be saved
save_path = "C:/Users/Mehdi/Desktop/DSI/ATL-Datamart-Mehdi/data/raw/yellow_tripdata_2024-01.parquet"

# Send a GET request to download the file
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Open the file in write-binary mode and save the content
    with open(save_path, 'wb') as file:
        file.write(response.content)
    print(f"File successfully downloaded and saved to {save_path}")
else:
    print(f"Failed to download the file. Status code: {response.status_code}")