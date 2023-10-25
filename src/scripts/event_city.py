from datetime import datetime, timedelta 

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F

import os
import sys

from tools import read_df, sph_dist

def message_city(base_input_path: str, geo_path: str, session: SparkSession, frac: float=0.05):
    import os
    import pyspark.sql.functions as F    
    from pyspark.sql import Window
    
    sql = session

    geo_df = read_df(geo_path,session).withColumnRenamed('lat','lat_2').withColumnRenamed('lng','lon_2')#.\    withColumn('date', F.lit(date).cast('date'))
   
    events = read_df(os.path.join(base_input_path, 'events'), session)
    events = events.sample(frac)

    event_geo_df = events.crossJoin(geo_df)
    event_city_df = sph_dist(event_geo_df)

    window = Window.partitionBy(F.col('event')).orderBy(F.asc('dist'))
    event_city_df_rank = event_city_df.withColumn('rank', F.row_number().over(window))
    
    res_df = event_city_df_rank.filter('rank==1').drop('rank','lat_2', 'lon_2', 'id', 'coords')
    
    return res_df
    
def main():
    base_input_path = sys.argv[1]
    geo_path = sys.argv[2]
    dds_path = sys.argv[3]   
    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
    
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    dds_path = os.path.join(dds_path, 'events_city')
    message_city(base_input_path, geo_path, sql).write.mode('overwrite').partitionBy("date").parquet(dds_path)
    
if __name__ == "__main__":
    main()
