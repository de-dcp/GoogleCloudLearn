# Import dependencies

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from google.cloud import storage

# Global Variable Defintiion


project_id = "e-centaur-453713-b5"
bucket_name = "sales_data_staging_dcp"
input_file_path = "input_sales_data"
processed_file_path = "processed"
error_file_path = "error"

# Python logic to derive yetsreday's date
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def check_input_file(ti):
    storage_client = storage.Client()
    file_list = []
    filename = ''
    csv_file_found = False
    blobs = storage_client.list_blobs(bucket_name, prefix='input_sales_data')
    for blob in blobs:
        file_list.append(blob.name)

    for file_name in file_list:
        #print(f"Iterating over filename {file_name}")
        if file_name.split('.')[-1].lower() == 'csv':
            filename = file_name
            #print(f"CSV file Found {filename}")
            break

    if filename == '':
        print("fail")
    else:
        print("proceed")
        csv_file_found = True
        filename = filename.split('/')[-1].lower()
        ti.xcom_push(key='input_file', value=filename)

    return 'file_present_task' if csv_file_found else 'file_missing_task'



# DAG definitions
with DAG(dag_id='cross_comm',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:

    # Bash operator , task
    bash_task = BashOperator(
    task_id = 'bash_task',
    bash_command = "date;echo 'Starting the DAG for loading Sale data into Datawarehouse'",
    )

    # This Task checks for source file in input directory
    check_input_file_task = BranchPythonOperator(
    task_id ='check_input_file_task',
    python_callable=check_input_file,
    provide_context=True,
    dag=dag)

    file_present_task = BashOperator(
    task_id = 'file_present_task',
    bash_command = "date;echo 'Starting the DAG for loading Sale data into Datawarehouse'; echo 'file name {{ ti.xcom_pull(task_ids='check_input_file_task', key='input_file') }}'",
    )

    file_missing_task = BashOperator(
    task_id = 'file_missing_task',
    bash_command = "date;echo 'Data not available for loading into Datawarehouse'",
    )

    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    no_file_to_process_end = DummyOperator(
        task_id='no_file_to_process_end',
        dag=dag,
    )


# Setting up Task dependencies using Airflow standard notations
bash_task >> check_input_file_task
check_input_file_task >> file_present_task >> end
check_input_file_task >> file_missing_task >> no_file_to_process_end


