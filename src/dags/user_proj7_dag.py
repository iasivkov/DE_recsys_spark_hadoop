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

# Стартуем даг каждый день в 12 часов. Чтобы можно было предлагать пользователю актуальную рекламу и рекомендации друзей
# можно и чаще, если позволяют ресрусы.
@dag(schedule_interval='0 12 * * *', start_date=datetime(2023, 10, 25))
def sprint7_user_dag():

    
    # объявляем задачу с помощью SparkSubmitOperator
    user_dm = SparkSubmitOperator(
                            task_id='spark_submit_user',
                            application=os.path.join(scripts_path, 'user_dm.py'),
                            conn_id='yarn_spark',
                            executor_cores=4,
                            application_args=['/user/iasivkov/data/dds', '/user/iasivkov/data/analytics']
                            )
                            
    rec_sys_dm = SparkSubmitOperator(
                            task_id='spark_submit_friends',
                            application=os.path.join(scripts_path, 'rec_sys.py'),
                            conn_id='yarn_spark',
                            executor_cores=4,
                            application_args = ['/user/iasivkov/data/dds', '/user/iasivkov/data/analytics']   
                            )

    ( user_dm>> rec_sys_dm )

dag = sprint7_user_dag() 

dag
