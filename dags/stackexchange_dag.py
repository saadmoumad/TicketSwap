
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import dag, task
import pandas as pd

from datetime import datetime, timedelta
import sys
import os

sys.path.append(Variable.get("scripts_path"))

import Dumps_downloader 
import Processing_utils
import Sql_utils

default_args = {
    'owner': 'saad',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def _download_and_unzip():
    print('Downloading and unziping....')

    url_prefix = Variable.get("main_url")
    sites = Variable.get("sites").split(',')
    raw_data_path = Variable.get("raw_data_path")

    d_object = Dumps_downloader.DD(url_prefix)
    map = d_object.Sites_mapping_generator()

    for site in sites:
        download_url ='{}/{}'.format(url_prefix, map[site][0][0])
        full_path = os.path.join(raw_data_path, site, map[site][1])
        print('Dowlaodind {} to {} dir'.format(site, full_path))
        d_object.download_and_unzip(download_url, full_path)
    return map

def _file_processing(file_name, ti):
    processing_obj = Processing_utils.file_processing()
    areas_list = ti.xcom_pull(task_ids="make_staging_areas")
    
    for path in areas_list:
        full_file_path = os.path.join(Variable.get("raw_data_path"), '/'.join(path.split('/')[-2:]), file_name)
        file_name = file_name.lower()[:-4]
        if file_name == 'posts':
            #print(sys.version)
            #print(pd.__version__)
            #print(full_file_path)
            #print(path)
            print('Processing {}'.format(file_name))
            df = processing_obj.posts_processing(full_file_path)
            processing_obj.save_parquet(df, file_name, path)
        if file_name == 'comments':
            print('Processing {}'.format(file_name))
            df = processing_obj.comments_processing(full_file_path)
            processing_obj.save_parquet(df, file_name, path)
        if file_name == 'badges':
            df = processing_obj.badges_processing(full_file_path)
            processing_obj.save_parquet(df, file_name, path)
        if file_name == 'tags':
            print('Processing {}'.format(file_name))
            df = processing_obj.tags_processing(full_file_path)
            processing_obj.save_parquet(df, file_name, path)
        if file_name == 'users':
            print('Processing {}'.format(file_name))
            df = processing_obj.users_processing(full_file_path)
            processing_obj.save_parquet(df, file_name, path)
        if file_name == 'votes':
            print('Processing {}'.format(file_name))
            df = processing_obj.votes_processing(full_file_path)
            processing_obj.save_parquet(df, file_name, path)
        if file_name == 'posthistory':
            print('Processing {}'.format(file_name))
            df = processing_obj.postHistory_processing(full_file_path)
            processing_obj.save_parquet(df, file_name, path)
        if file_name == 'postlinks':
            print('Processing {}'.format(file_name))
            df = processing_obj.postLinks_processing(full_file_path)
            processing_obj.save_parquet(df, file_name, path)
        
        
    return

def _make_staging_areas(ti):
    processing_obj = Processing_utils.file_processing()
    areas_list = list()
    map = ti.xcom_pull(task_ids="download_and_unzip")
    sites = Variable.get("sites").split(',')
    staging_area_path = Variable.get("staging_area_path")
    for site in sites:
        full_path = os.path.join(staging_area_path, site,map[site][1])
        processing_obj.make_staging_area(full_path)
        areas_list.append(full_path)
        print('Staging area for{}: {}'.format(site, full_path))
    return areas_list


def _check_sql_conn(ti):
    sql_object = Sql_utils.Sql_helper(host=Variable.get("db_host"),
                                 user=Variable.get("db_user"),
                                 password=Variable.get("db_pw"),
                                 database=Variable.get("db"))
    engine = sql_object.check_connection()
    if engine:
        ti.xcom_push(key='postgres_urs', value=sql_object.sql_url)
        return 'parquet_to_sql'
    return 'con_failed'


def _parquet_to_sql(ti):
    sql_object = Sql_utils.Sql_helper(host=Variable.get("db_host"),
                                 user=Variable.get("db_user"),
                                 password=Variable.get("db_pw"),
                                 database=Variable.get("db"))
    engine= sql_object.check_connection()
    areas_list = ti.xcom_pull(task_ids="make_staging_areas")
    #areas_list = ['/opt/airflow/staging_area/conlang/08-Jun-2022 21:32']
    for path in areas_list:
        site = path.split('/')[-2]
        p_files = os.listdir(path)
        for file in p_files:
            parquet_file_path = os.path.join(path, file)
            sql_object.parquet_to_sql(parquet_file_path, site+'_'+file[:-8], engine)
            print('File: {} Pushed to {} as {}'.format(parquet_file_path, sql_object.sql_url, site+'_'+file[:-8]))
    return

def _con_failed():
    #Log uptracking and Actions on failure 
    message = "unable to connect to postgresql://{}:{}@{}:{}/{}".format(Variable.get('db_user'),
                                                            Variable.get('db_pw'),
                                                            Variable.get('db_host'),
                                                            5432,
                                                            Variable.get('db')) 
    print(message)

def _finish():
    print('Done')
    return


with DAG('StackExchange',
                default_args=default_args,
                 start_date=datetime(2022, 7 ,17),
                 schedule_interval='@daily', catchup=False, tags=['SE']) as dag:

        download_and_unzip = PythonOperator(task_id = "download_and_unzip",
                                            python_callable = _download_and_unzip)

        file_processing_tasks = [PythonOperator(task_id= f"processing_{file_name}",
                                                python_callable= _file_processing,
                                                op_kwargs={"file_name": file_name}) for file_name in ['Comments.xml', 'Users.xml', 
                                                                                                        'Votes.xml', 'Tags.xml',
                                                                                                     'PostHistory.xml', 'PostLinks.xml', 
                                                                                                     'Posts.xml', 'Badges.xml']
                                                ]
        
        make_staging_areas = PythonOperator(task_id = "make_staging_areas",
                                        python_callable = _make_staging_areas)

        check_sql_conn = BranchPythonOperator(task_id = "check_sql_conn",
                                        python_callable = _check_sql_conn)

        con_failed = PythonOperator(task_id = "con_failed",
                                       python_callable = _con_failed)

        '''create_schema = PythonOperator(task_id = "create_schema",
                                       python_callable = _create_schema)'''

        parquet_to_sql = PythonOperator(task_id = "parquet_to_sql",
                                            python_callable = _parquet_to_sql)
        
        finish = PythonOperator(task_id= "finish",
                                python_callable= _finish,
                                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)


download_and_unzip >> make_staging_areas >> file_processing_tasks >> check_sql_conn >> con_failed >> finish
check_sql_conn >> parquet_to_sql >> finish

