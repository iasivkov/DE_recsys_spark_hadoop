from pyspark.sql.functions import asin, sin, cos, sqrt, pow, col, lit
from pyspark.sql import SparkSession, DataFrame
from typing import List
import math

def read_df(path: str, session: SparkSession) -> DataFrame:
        return session.read.parquet(path)
          
def sph_dist(df: DataFrame, R: float=6371.0) -> DataFrame:

    return df.withColumn('dist',
                         2*R*asin(sqrt(pow(sin((col('lat_2')-col('lat'))*lit(math.pi/360)),2)\
                         + cos(col('lat_2'))*cos(col('lat'))*pow(sin((col('lon_2')-col('lon'))*lit(math.pi/360)),2)))
                         )