from pyspark import *
from pyspark import SparkConf, SparkContext
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# from utility import *


spark = SparkSession.builder.master('local[3]').appName('assignment_1').getOrCreate()

Schema = StructType([StructField("Username", StringType(), False),
                     StructField(" Identifier", IntegerType(), True),
                     StructField("One-time password", StringType(), False),
                     StructField("Recovery code", StringType(), True),
                     StructField("First name", StringType(), True),
                     StructField("Last name", StringType(), True),
                     StructField("Department", StringType(), True),
                     StructField("Location", StringType(), True)])

df = spark.read.format("csv").schema(Schema).option("header", True).option("Delimiter", ";"). \
    load("data/username-password-recovery-code.csv")
df.printSchema()
df.show(20, False)


df.write.mode('overwrite').csv("JSON_File/csv_file")

df.write.mode('overwrite').json("JSON_File/json_file")

print('Reading JSON file')
df_json = spark.read.json("JSON_File/json_file")
df_json.show(20, False)
