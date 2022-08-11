'''
Output:-
+------+-------+------+
|row_id|time_id|target|
+------+-------+------+
|1220_1|   1220|     0|
|1220_2|   1220|     0|
|1221_0|   1221|     0|
|1221_2|   1221|     0|
|1222_0|   1222|     0|
|1222_1|   1222|     0|
|1222_2|   1222|     0|
|1223_0|   1223|     0|
+------+-------+------+
'''

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practise').getOrCreate()
spark

spark.read.parquet("C:\SparkPractice\eg_sample.parquet") 
parDF=spark.read.parquet("C:\SparkPractice\eg_sample.parquet")
parDF.show()
