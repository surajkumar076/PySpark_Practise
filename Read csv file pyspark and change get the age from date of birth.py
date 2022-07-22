'''
Output :-
+----------+----------+------+----------+
|Total Cost| BirthDate|Gender|   Product|
+----------+----------+------+----------+
|      1000|1995-03-06|  Male|Technology|       
|      2000|1997-03-06|  Male|       Car|
|      3000|1993-03-06|Female|    Beauty|
|      4000|1994-03-06|  Male|      Bike|
|      5000|1992-03-06|Female|    Beauty|
|      6000|1991-03-06|Female|    Parlor|
+----------+----------+------+----------+

+----------+----------+------+----------+---+
|Total Cost| BirthDate|Gender|   Product|age|
+----------+----------+------+----------+---+
|      1000|1995-03-06|  Male|Technology| 27|
|      2000|1997-03-06|  Male|       Car| 25|
|      3000|1993-03-06|Female|    Beauty| 29|
|      4000|1994-03-06|  Male|      Bike| 28|
|      5000|1992-03-06|Female|    Beauty| 30|
|      6000|1991-03-06|Female|    Parlor| 31|
+----------+----------+------+----------+---+
'''
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practise').getOrCreate()

#Read & show the CSV file
import pandas as pd

File_Path= spark.read.csv('C:\SparkPractice\sheet1.csv')

Df_PySpark = spark.read.option('header', 'true').csv('C:\SparkPractice\sheet1.csv')
Df_PySpark.show()

# Calculated the age From Date of Birth:-

from pyspark.sql import functions as F
def current_date():
    return F.from_utc_timestamp(F.current_timestamp(), 'Asia/Calcutta').cast('date')
Calculated_age = Df_PySpark.withColumn('age', (F.months_between(current_date(), F.col('BirthDate')) / 12).cast('int'))
Calculated_age.show()
