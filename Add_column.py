'''
+----+---+-------------+
| _c0|_c1|bonus_percent|
+----+---+-------------+
|Name|Age|          0.3|
|Adam| 34|          0.3|
|Alex| 33|          0.3|
| Mia| 36|          0.3|
| Ola| 39|          0.3|
+----+---+-------------+
'''
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 21 00:07:39 2022

@author: suraj
"""

import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
spark = SparkSession.builder.appName('Practise').getOrCreate()

spark

import pandas as pd
df_pyspark=spark.read.csv('C:\SparkPractice\sample2.csv')

spark.read.option('header','true').csv('C:\SparkPractice\sample2.csv').show()

#ADD NEW COLUMN IN PySpark DataFrame
df_pyspark.withColumn("bonus_percent", lit(0.3)).show()
