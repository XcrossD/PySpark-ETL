from pyspark.sql import SparkSession

ss = SparkSession.builder.appName('Anime ETL').getOrCreate()
df = ss.read.json("json")
df.show()