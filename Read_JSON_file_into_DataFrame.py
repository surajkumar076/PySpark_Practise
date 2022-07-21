"""
Created on Thu Jul 21 00:49:05 2022

@author: suraj
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

df_pyspark= spark.read.json('C:\SparkPractice\colors.json')
df_pyspark.printSchema()
spark.read.option('header', 'true').csv('C:\SparkPractice\colors.json').show()
