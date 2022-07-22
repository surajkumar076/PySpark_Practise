import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Practise').getOrCreate()
spark

spark.read.parquet("C:\SparkPractice\eg_sample.parquet") 
parDF=spark.read.parquet("C:\SparkPractice\eg_sample.parquet")
parDF.show()
