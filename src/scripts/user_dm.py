from datetime import datetime, timedelta 

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F

import os
import sys

from tools import read_df

def travel(df: DataFrame)-> DataFrame:
    win = Window.partitionBy('user_id').orderBy('message_ts')
    df_travel = df.select('user_id', 'message_ts', 'city', F.when(F.col('city') == F.lead('city').over(win), 
                                                                 None).otherwise(F.col('city')).alias('travel')).dropna()
    return df_travel.select('user_id','travel').groupBy('user_id').agg(F.count('travel').alias('travel_count'),
                                                                       F.collect_list('travel').alias('travel_array'))

def home(df: DataFrame, days: int=27) -> DataFrame:
   
    window = Window.partitionBy(['user_id','city']).orderBy('date')
    user_df = df.select('user_id', 'city', F.col('message_ts').cast('date').alias('date')).\
    withColumn('num', F.dense_rank().over(window))
    user_df = user_df.groupBy(['user_id', 'city', F.expr('date_sub(date, num)')]).agg(F.countDistinct('date').alias('seq'),
                                                                              F.max('date').alias('end')).\
    select('user_id','city','seq', 'end')

    return user_df.filter('seq>={}'.format(days)).orderBy(['end','seq', 'city']).groupBy('user_id').agg(F.last('city').alias('city'),
                                                                                           F.last('end').alias('end'),
                                                                                           F.last('seq').alias('seq')).select(['user_id','city'])

def last_city(df: DataFrame) -> DataFrame:
    window = Window.partitionBy('user_id').orderBy(F.desc('message_ts'))

    return df.withColumn("last", F.row_number().over(window)).filter('last==1').drop('last').withColumnRenamed('city','act_city')\
    .select('user_id', 'act_city', 'message_ts','timezone' )

def local_time(df: DataFrame) -> DataFrame:
    return df.withColumn('local_time', F.from_utc_timestamp(F.col('message_ts'),F.col('timezone'))).\
                drop('timezone')
    

#-------------------------------------------------------------------------------------------------
def main():
    dds_path = sys.argv[1]
    dm_path = sys.argv[2]

    
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    df = read_df(dds_path, sql)
    user_df = df\
    .withColumn('user_id',  F.coalesce(F.col('event.message_from'), 
                                F.col('event.reaction_from'),
                                F.col('event.user')))\
    .select('user_id',
            'city',
            'timezone',
            F.coalesce("event.datetime","event.message_ts").alias('message_ts').cast('timestamp'))
    last_city_df = last_city(user_df)
    home_city_df = home(user_df)
    travel_df = travel(user_df)
    user_city_df = last_city_df.join(home_city_df, 'user_id', 'left').join(travel_df,'user_id', 'left')
    final_df = local_time(user_city_df)
    user_dm_path = os.path.join(dm_path, 'dm_user')
    
    final_df.write.mode('overwrite').parquet(user_dm_path)

    
if __name__ == "__main__":
    main()