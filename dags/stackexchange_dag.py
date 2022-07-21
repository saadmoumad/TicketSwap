from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.decorators import dag, task

from datetime import datetime, timedelta

default_args = {
    'owner': 'saad',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def _download_and_unzip():
    print('Downloading nad unziping DONE')
    return 

def _file_processing(file_name):
    print('Processing {} DONE'.format(file_name))
    return

def _check_sql_conn():
    i = 1
    if i:
        return 'create_schema'
    raise TypeError("Only integers are allowed")
    return 'con_fail'

def _create_schema():
    print('Schema Created')
    return

def _parquet_to_sql():
    print('uploading')
    return

def _con_fail():
    print("con failed")

def _finish():
    print('done')
    return

with DAG('StackExchange',
                default_args=default_args,
                 start_date=datetime(2022, 7 ,17),
                 schedule_interval='@daily', catchup=False, tags=['SE']) as dag:

        download_and_unzip = PythonOperator(task_id = "download_and_unzip",
                                            python_callable = _download_and_unzip)

        file_processing_tasks = [PythonOperator(task_id= f"processing_{file_name}",
                                                python_callable= _file_processing,
                                                op_kwargs={"file_name": file_name}) for file_name in ['post', 'comments',
                                                                                                            'badges']
                                                ]
        
        check_sql_conn = BranchPythonOperator(task_id = "check_sql_conn",
                                        python_callable = _check_sql_conn)

        con_fail = PythonOperator(task_id = "con_fail",
                                       python_callable = _con_fail)

        create_schema = PythonOperator(task_id = "create_schema",
                                       python_callable = _create_schema)

        parquet_to_sql = PythonOperator(task_id = "parquet_to_sql",
                                            python_callable = _parquet_to_sql)
        
        finish = PythonOperator(task_id= "finish",
                                python_callable= _finish,
                                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)


download_and_unzip >> file_processing_tasks >> check_sql_conn >> create_schema >> parquet_to_sql >> finish
download_and_unzip >> file_processing_tasks >> check_sql_conn >> con_fail >> finish

