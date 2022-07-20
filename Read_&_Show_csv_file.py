import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

#Read & show the CSV file
import pandas as pd

df_pyspark= spark.read.csv('C:\SparkPractice\sample2.csv')

spark.read.option('header', 'true').csv('C:\SparkPractice\sample2.csv').show()
