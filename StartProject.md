docker-compose up | docker compose up
pip install -r requirements.txt
python src/data/grab_parquet.py
docker exec -it atl-datamart_data-warehouse_1 psql -U admin
admin=# CREATE DATABASE nyc_warehouse;
docker exec -it atl-datamart_data-mart_1 psql -U admin
admin=# CREATE DATABASE nyc_datamart;
python src/data/dump_to_sql.py