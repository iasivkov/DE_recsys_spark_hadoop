from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.decorators import dag
from airflow.configuration import conf
import os


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

dags_path = conf.get('core', 'DAGS_FOLDER')
# костыль, чтобы путь к папке scripts указать
scripts_path = os.path.join('/','/'.join([x for x in dags_path.split('/')[0:-1] if x]), 'scripts')

# Стартуем даг каждый день в 9 часов утра. 
# Чтобы оперативно обновлять витрины для рекомендаций и витрину по пользователям 
@dag(schedule_interval='0 9 * * *', start_date=datetime(2023, 10, 25))
def recsys_first_city_dag():

    
    # объявляем задачу с помощью SparkSubmitOperator
    event_city_dds = SparkSubmitOperator(
                            task_id='spark_submit_friends',
                            application=os.path.join(scripts_path,'event_city.py'),
                            conn_id='yarn_spark',
                            executor_cores=4,
                            application_args = ['/user/iasivkov/data/geo/',
                                                '/user/iasivkov/data/snapshots/geo/geo_t',
                                                '/user/iasivkov/data/dds/']   
                            )

    ( event_city_dds )

dag = recsys_first_city_dag() 

dag
