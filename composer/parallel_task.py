from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta

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


def random_branch():
    from random import randint
    print("branch_a" if randint(1, 2) == 1 else "branch_b")

with DAG(dag_id='Test_Parallel_Task',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:

    start = DummyOperator(
        task_id = 'Start',
        dag = dag
    )

    decide_branch = PythonOperator(
        task_id = 'decide_branch',
        python_callable = random_branch,
        dag =dag
    )

    branch_a = BashOperator(
        task_id='branch_a',
        bash_command="date;echo 'Hey I am branch a'",
    )

    branch_b = BashOperator(
        task_id='branch_b',
        bash_command="date;echo 'Hey I am branch b'",
    )


    end = DummyOperator(
        task_id = 'END',
        trigger_rule='one_success',
        dag = dag
    )


# settingup depedency
start >> decide_branch >> branch_a >> end
start >> decide_branch >> branch_b >> end