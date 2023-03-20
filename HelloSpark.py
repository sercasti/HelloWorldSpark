import sys
import configparser

from pyspark.sql import *
from pyspark import SparkConf
from lib.logger import Log4j


if __name__ == "__main__":
    # Boilerplate spark Configuration
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    conf =  spark_conf

    # Create spark session
    spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Starting HelloSpark")

    survey_raw_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/sample.csv")

    count_df = survey_raw_df.filter("Age < 40") \
        .select("Age", "Gender", "Country", "state") \
        .groupBy("Country") \
        .count()
    
    count_df.show()

    spark.stop()
