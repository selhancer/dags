
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

def _helloworld():
    print('hello world, this is Python!')

with DAG(
    dag_id='selo_1',
    schedule_interval='@daily'
) as dag:

    hello_bash = BashOperator(
        task_id='hello_bash',
        bash_command='echo hello_world, this is Bash',
    )

    hello_python = PythonOperator(
        task_id='hello_python',
        python_callable=_helloworld(),
    )

hello_bash >> hello_python
