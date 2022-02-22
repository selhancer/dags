from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime 

def _helloworld():
    print('hello world, this is Python!')
with DAG(
    dag_id='selo_1',
    schedule_interval='@daily',
    default_args={
          "start_date":datetime(2022,2,19),
    }
) as dag:

    hello_bash = BashOperator(
        task_id='hello_bash',
        bash_command='echo hello_world, this is Bash',
    )

    hello_python = PythonOperator(
       task_id='hello_python',
       python_callable=_helloworld,
    )

hello_bash >> hello_python