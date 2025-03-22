# import statements
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStorageObjectSensor
# from airflow.operators.python import BranchPythonOperator
# from airflow.utils.trigger_rule import TriggerRule

# Custom Python logic for derriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(dag_id='GCS_to_BQ_and_AGG',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:

# Dummy strat task
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

#Sensor to check file in GCP Bucket
    check_file = GoogleCloudStorageObjectSensor(
        task_id='check_file',
        bucket='sales_data_staging_dcp',
        object='input_sales_data/customer_purchasing_behaviors.csv',
        mode='poke',
        poke_interval=60,
        timeout=120,
        soft_fail=True,
        dag=dag
    )

# GCS to BigQuery data load Operator and task
    gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
                task_id='gcs_to_bq_load',
                bucket='sales_data_staging_dcp',
                source_objects=['input_sales_data/customer_purchasing_behaviors.csv'],
                destination_project_dataset_table='e-centaur-453713-b5.sales_data_analysis.gcs_to_bq_table',
                schema_fields=[
    {"name": "user_id","type": "INTEGER","mode": "NULLABLE","description": "unique_customer_id"},
    {"name": "age","type": "INTEGER","mode": "NULLABLE","description": "CustomerAge"},
    {"name": "annual_income","type": "FLOAT","mode": "NULLABLE","description": "Customer's Annual Income"},
    {"name": "purchase_amount","type": "FLOAT","mode": "NULLABLE","description": "Billing Amount"},
    {"name": "loyalty_score","type": "FLOAT","mode": "NULLABLE","description": "Customer Loyalty Score"},
    {"name": "region","type": "STRING","mode": "NULLABLE","description": "India Zone"},
    {"name": "purchase_frequency","type": "INTEGER","mode": "NULLABLE","description": "Customer purchase frequency in Days"}
],
                skip_leading_rows=1,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE',
    dag=dag)

# BigQuery task, operator
    create_aggr_bq_table = BigQueryOperator(
    task_id='create_aggr_bq_table',
    use_legacy_sql=False,
    allow_large_results=True,
    sql="CREATE OR REPLACE TABLE e-centaur-453713-b5.sales_data_analysis.bq_table_aggr AS \
         SELECT \
                region,\
                age,\
                SUM(purchase_amount) as sum_data_value\
         FROM e-centaur-453713-b5.sales_data_analysis.gcs_to_bq_table \
         GROUP BY \
                region,\
                age",
    dag=dag)

# Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Settting up task  dependency
start >> check_file >> gcs_to_bq_load >> create_aggr_bq_table >> end