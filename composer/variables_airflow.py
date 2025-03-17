# Import
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

def print_hello():
    print({{ var.value.get('support_email') }})
    print('Hey I am Python operator')

# Define the DAG

dag = DAG(
    'live_exchange_rates',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 21 * * *',
    catchup=False
)

# Define the Tasks

# Dummy Start task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

## fetching and displaying variable below
fetch_exchange_rates = BashOperator(
    task_id='bash_task',
    bash_command="echo '{{ var.value.get('support_email') }}'; echo 'bash task executed'",
    dag=dag,
)

pyhon_task = PythonOperator(
    task_id='pyhon_task',
    python_callable=print_hello,
    dag=dag)


# Define the Dependencies
start >> fetch_exchange_rates >> pyhon_task



