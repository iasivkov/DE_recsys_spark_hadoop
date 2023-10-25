import calendar
from datetime import datetime, timedelta, timezone 

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F
import os
import sys

from tools import read_df, sph_dist
    
def get_user_channel(df: DataFrame) -> DataFrame:
    df_user_channel = df\
                        .select('event_type', 'event.user','event.subscription_channel', 'timezone')\
                        .filter(F.col('event_type') == 'subscription')\
                        .drop('event_type')\
                        .dropDuplicates().dropna()
    return df_user_channel

def get_pairs(df_events: DataFrame, df_user_channel: DataFrame, df_dm_user: DataFrame) -> DataFrame:
    
    # добавляем информацию о городе и локальном времени
    df_user_channel_city = df_user_channel.alias('df_u').join(df_dm_user.alias('df_d')\
                                                             .select('user_id','act_city'),
                                                             df_dm_user.user_id == df_user_channel.user,
                                                             'inner')\
                            .select('df_u.user', 'df_u.subscription_channel', 'df_d.act_city', 'df_u.timezone')
    
    # составляем пары по подписке на один и тот же канал
    df_pairs = df_user_channel_city.alias('df1')\
                .join(df_user_channel_city.alias('df2'), ['subscription_channel', 
                                                    'act_city'], 'inner')\
                .select(F.col('df1.user').alias('user_left'),
                        F.col('df2.user').alias('user_right'),
                        F.col('act_city').alias('zone_id'),
                        F.col('df2.timezone'))\
                .filter(F.col('user_left') != F.col('user_right'))
    
    # оставляем уникальные комбинации без перестановок
    df_pairs = df_pairs.withColumn('pair',F.sort_array(F.array('user_left', 'user_right')))\
                        .dropDuplicates(['pair']).drop('pair')
    
    # убираем пары, которые уже имели переписку
    df_pairs = df_pairs.join(df_events, [df_pairs.user_left == df_events.event.message_from,
                                        df_pairs.user_right == df_events.event.message_to], how='leftanti')\
                        .join(df_events, [df_pairs.user_left == df_events.event.message_to,
                                        df_pairs.user_right == df_events.event.message_from], how='leftanti')
    return df_pairs

def get_nearest(df_pairs: DataFrame, df_events: DataFrame, rec_dist: int=1) -> DataFrame:
    df_dist = sph_dist(df_pairs.join(df_events.select(F.coalesce(F.col('event.message_from'), 
                                                        F.col('event.reaction_from'),
                                                        F.col('event.user')
                                                        ).alias('user_left'),
                                            F.col('lat').alias('lat'),
                                            F.col('lon').alias('lon')
                                            ).alias('df_left'),
                                    'user_left',
                                    'inner'
                                    )\
                                .join(df_events.select(F.coalesce(F.col('event.message_from'), 
                                                            F.col('event.reaction_from'),
                                                            F.col('event.user')
                                                            ).alias('user_right'),
                                                F.col('lat').alias('lat_2'),
                                                F.col('lon').alias('lon_2')
                                                ).alias('df_right'),
                                    'user_right',
                                    'inner'
                                    )
                        ).filter('dist <= {}'.format(rec_dist))

    return df_dist.select('user_left', 'user_right', 
                            F.lit(datetime.now(timezone.utc)).alias('processed_dttm'),
                            'zone_id', F.from_utc_timestamp(F.lit(datetime.now(timezone.utc)),F.col('timezone')).alias('local_time'))                        

def main():
    dds_path = sys.argv[1]
    dm_path = sys.argv[2]
    
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    df_events_path = os.path.join(dds_path, 'events_city')
    df_user_dm_path = os.path.join(dm_path, 'dm_user')
    
    df_user_dm = read_df(df_user_dm_path, sql)
    df_events = read_df(df_events_path, sql)
    
    df_user_channel = get_user_channel(df_events)
    df_pairs = get_pairs(df_events, df_user_channel, df_user_dm)
    df_friends = get_nearest(df_pairs, df_events) 
    
    df_friends.write.mode('overwrite').partitionBy('zone_id').parquet(os.path.join(dm_path, 'dm_rec_sys'))
    
  
if __name__ == "__main__":
    main()
    