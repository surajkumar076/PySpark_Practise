'''
+----+---+
| _c0|_c1|
+----+---+
|Name|Age|
|Adam| 34|
|Alex| 33|
| Mia| 36|
| Ola| 39|
+----+---+
'''
"""
Created on Thu Jul 21 00:32:38 2022

@author: suraj
"""

import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

import pandas as pd
df_pyspark=spark.read.csv('C:\SparkPractice\sample2.csv')

spark.read.option('header','true').csv('C:\SparkPractice\sample2.csv').show()

#DROP COLUMN IN PySpark DataFrame
df_pyspark.drop('bonus_percent')

df_pyspark.drop('bonus_percent').show()
