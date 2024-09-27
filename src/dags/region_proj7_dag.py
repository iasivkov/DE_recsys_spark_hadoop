from datetime import datetime
from airflow import DAG
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

# Стартуем даг каждый понедельник в 0 часов утра. 
# Обновляем витрину по регионам раз в неделю 
@dag(schedule_interval='0 0 * * 1', start_date=datetime(2023, 10, 25))
def region_dag():

    
    # объявляем задачу с помощью SparkSubmitOperator
    region_dm = SparkSubmitOperator(
                            task_id='spark_submit_region_dm',
                            application=os.path.join(scripts_path, 'region_dm.py'),
                            conn_id='yarn_spark',
                            executor_cores=4,
                            executor_memory='4G',
                            application_args=['/user/iasivkov/data/dds', '/user/iasivkov/data/analytics']
                            )

    ( region_dm )

dag = region_dag() 

dag
