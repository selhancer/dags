from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.utils.task_group import TaskGroup

from random import uniform

default_args={'start_date':datetime(2022,2,28)}
def _picking_number(ti):
    number=uniform(1,10)
    ti.xcom_push(key='picked_number',value=number)

def _find_total(ti):
    numbers=ti.xcom_pull(key='picked_number',task_ids=['picking_numbers.picking_number1','picking_numbers.picking_number2'])
    total=sum(numbers)
    ti.xcom_push(key='total_number',value=total)

def _bigger_than_10_processing():
    print('Task result:','bigger_than_10_processing is processed succesfully!!')

def _processed_result():
    print('Task result:','processed_result is processed succesfully!!')

def _choose_processing_model(ti):
    total=ti.xcom_pull(key='total_number',task_ids=['find_total'])
    if int(total[0])>10:
        return 'bigger_than_10_processing'
    else:
        return 'lower_than_10_processing'

def _lower_than_10_processing():
    print('Task result:','lower_than_10_processing is processed succesfully!!')

with DAG ('all_in_one_dag',schedule_interval='@daily',catchup=False,default_args=default_args
      ) as dag :

    with TaskGroup('picking_numbers') as picking_numbers:
        picking_number1=PythonOperator(
            task_id='picking_number1',
            python_callable=_picking_number
        )

        picking_number2=PythonOperator(
            task_id='picking_number2',
            python_callable=_picking_number
        )

    find_total=PythonOperator(task_id='find_total',python_callable=_find_total,trigger_rule='all_done')

    choose_processing_model=BranchPythonOperator(task_id='choose_processing_model',python_callable=_choose_processing_model)
    bigger_than_10_processing=PythonOperator(task_id='bigger_than_10_processing',python_callable=_bigger_than_10_processing)
    lower_than_10_processing=PythonOperator(task_id='lower_than_10_processing',python_callable=_lower_than_10_processing)
    processed_result=PythonOperator(task_id='processed_result',python_callable=_processed_result,trigger_rule='none_failed')
[picking_numbers] >>find_total>>choose_processing_model>>[bigger_than_10_processing,lower_than_10_processing] >>processed_result
