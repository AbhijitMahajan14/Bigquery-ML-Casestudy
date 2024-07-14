from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'taxi_model_training',
    default_args=default_args,
    schedule_interval='@monthly',
) as dag:

    start_task = DummyOperator(
        task_id='start_training',
    )

    fare_model_task = BigQueryInsertJobOperator(
        task_id='train_fare_model',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE MODEL `skilful-gantry-426403-k1.bq_ml_poc.taxi_fare_model`
                OPTIONS(model_type='linear_reg', input_label_cols=['fare']) AS
                SELECT
                  trip_miles,EXTRACT(HOUR FROM trip_start_timestamp) AS start_hour,
                  EXTRACT(DAYOFWEEK FROM trip_start_timestamp) AS start_dayofweek,
                  EXTRACT(MONTH FROM trip_start_timestamp) AS start_month,
                  EXTRACT(YEAR FROM trip_start_timestamp) AS start_year,
                  pickup_community_area,dropoff_community_area,tips,tolls,
                  extras,pickup_latitude,pickup_longitude,
                  dropoff_latitude,dropoff_longitude,fare
                FROM
                  `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                WHERE
                  fare IS NOT NULL AND trip_miles IS NOT NULL AND trip_seconds > 0
                  AND trip_miles > 0 AND trip_seconds IS NOT NULL AND pickup_latitude IS NOT NULL 
                  AND pickup_longitude IS NOT NULL AND dropoff_latitude IS NOT NULL AND dropoff_longitude IS NOT NULL;
                """,
                "useLegacySql": False,
            }
        },
    )

    duration_model_task = BigQueryInsertJobOperator(
        task_id='train_duration_model',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE MODEL `skilful-gantry-426403-k1.bq_ml_poc.taxi_trip_duration_model`
                OPTIONS(model_type='linear_reg', input_label_cols=['trip_seconds']) AS
                SELECT
                  trip_miles, EXTRACT(HOUR FROM trip_start_timestamp) AS start_hour,
                  EXTRACT(DAYOFWEEK FROM trip_start_timestamp) AS start_dayofweek,
                  EXTRACT(MONTH FROM trip_start_timestamp) AS start_month,
                  EXTRACT(YEAR FROM trip_start_timestamp) AS start_year,
                  pickup_community_area,dropoff_community_area,fare,
                  tips,tolls,extras,pickup_latitude,pickup_longitude,
                  dropoff_latitude,dropoff_longitude,trip_seconds
                FROM
                  `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                WHERE
                  trip_seconds IS NOT NULL AND trip_miles IS NOT NULL AND trip_seconds > 0
                  AND trip_miles > 0 AND fare IS NOT NULL
                  AND pickup_latitude IS NOT NULL AND pickup_longitude IS NOT NULL
                  AND dropoff_latitude IS NOT NULL AND dropoff_longitude IS NOT NULL;
                """,
                "useLegacySql": False,
            }
        },
    )

    end_task = DummyOperator(
        task_id='end_training',
    )

    start_task >> fare_model_task >> duration_model_task >> end_task
