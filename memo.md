# Memo for Ryan

## Head of parquet

### Headers

vendorid | tpep_pickup_datetime | tpep_dropoff_datetime | passenger_count | trip_distance | ratecodeid | store_and_fwd_flag | pulocationid | dolocationid | payment_type | fare_amount | extra | mta_tax | tip_amount | tolls_amount | improvement_surcharge | total_amount | congestion_surcharge | airport_fee

### 5 first raws

   VendorID tpep_pickup_datetime tpep_dropoff_datetime  passenger_count  trip_distance  RatecodeID store_and_fwd_flag  PULocationID  DOLocationID  payment_type  fare_amount  extra  mta_tax  tip_amount  tolls_amount  improvement_surcharge  total_amount  congestion_surcharge  airport_fee
0         2  2023-01-01 00:32:10   2023-01-01 00:40:36              1.0           0.97         1.0                  N           161           141             2          9.3   1.00      0.5        0.00           0.0                    1.0         14.30                   2.5         0.00
1         2  2023-01-01 00:55:08   2023-01-01 01:01:27              1.0           1.10         1.0                  N            43           237             1          7.9   1.00      0.5        4.00           0.0                    1.0         16.90                   2.5         0.00
2         2  2023-01-01 00:25:04   2023-01-01 00:37:49              1.0           2.51         1.0                  N            48           238             1         14.9   1.00      0.5       15.00           0.0                    1.0         34.90                   2.5         0.00
3         1  2023-01-01 00:03:48   2023-01-01 00:13:25              0.0           1.90         1.0                  N           138             7             1         12.1   7.25      0.5        0.00           0.0                    1.0         20.85                   0.0         1.25
4         2  2023-01-01 00:10:29   2023-01-01 00:21:19              1.0           1.43         1.0                  N           107            79             1         11.4   1.00      0.5        3.28           0.0                    1.0         19.68                   2.5         0.00


Obervation:
- Cast passenger_count to interger
- Delete any raws with 0 as passenger_count?

