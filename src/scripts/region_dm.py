import calendar
from datetime import datetime, timedelta 
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F

import os
import sys

from tools import read_df 

def last_city(df: DataFrame) -> DataFrame:
    window = Window.partitionBy('user_id').orderBy(F.desc('message_ts'))

    return df.withColumn("last", F.row_number().over(window)).filter('last==1').drop('last').withColumnRenamed('city','act_city')\
    .select('user_id', 'act_city')

def df_events_week_month_reg(df_city: DataFrame, df_last_city: DataFrame) -> DataFrame:
    window = Window().partitionBy('user_id').orderBy('date')
    
    df_w_m_reg = df_city\
        .withColumn('user_id',  F.coalesce(F.col('event.message_from'), 
                                F.col('event.reaction_from'),
                                F.col('event.user')))\
        .withColumn("month",F.trunc(F.col("date"), "month"))\
        .withColumn("week",F.trunc(F.col("date"), "week"))\
        .withColumn("num_msg",F.row_number().over(window))\
        .withColumn("if_reg",F.when(F.col('num_msg') == 1,1).otherwise(0))\
        .drop('num_msg').join(df_last_city.select('user_id','act_city'), 'user_id')
    return df_w_m_reg
    
def df_week_month_activity(df_w_m_reg: DataFrame) -> DataFrame:
    w_window = Window().partitionBy('city','week')
    m_window = Window().partitionBy('city','month')
    df_w_m_activity = df_w_m_reg.select('month', 'week', F.coalesce(F.col('city'),F.col('act_city')).alias('city'), 'if_reg', 'event_type','date').dropna()\
                        .withColumn("week_message",F.sum(F.when(F.col('event_type') == "message",1).otherwise(0)).over(w_window))\
                        .withColumn("week_reaction",F.sum(F.when(F.col('event_type') == "reaction",1).otherwise(0)).over(w_window))\
                        .withColumn("week_subscription",F.sum(F.when(F.col('event_type') == "subscription",1).otherwise(0)).over(w_window))\
                        .withColumn("week_user",F.sum(F.col('if_reg')).over(w_window))\
                        .withColumn("month_message",F.sum(F.when(F.col('event_type') == "message",1).otherwise(0)).over(m_window)) \
                        .withColumn("month_reaction",F.sum(F.when(F.col('event_type') == "reaction",1).otherwise(0)).over(m_window)) \
                        .withColumn("month_subscription",F.sum(F.when(F.col('event_type') == "subscription",1).otherwise(0)).over(m_window))\
                        .withColumn("month_user",F.sum(F.col('if_reg')).over(m_window))\
                        .withColumnRenamed('city', 'region_id')\
                        .drop('if_reg', 'event_type')\
                        .orderBy('date')\
                        .drop('date')\
                        .dropDuplicates()
    return df_w_m_activity
                        
def main():
    dds_path = sys.argv[1]
    dm_path = sys.argv[2]
    
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
   
    df_city = read_df(os.path.join(dds_path, 'events_city'), sql)
    last_city_df = last_city(df_city.select(F.col('event.message_from').alias('user_id'),
                                            'city',
                                            F.col('event.message_ts').alias('message_ts')
                                            ).dropna()
                            )
    df_w_m_reg = df_events_week_month_reg(df_city, last_city_df)
    df_w_m_act = df_week_month_activity(df_w_m_reg)
    df_w_m_act.write.mode('overwrite').partitionBy('region_id').parquet(os.path.join(dm_path, 'dm_region'))
  
if __name__ == "__main__":
    main()