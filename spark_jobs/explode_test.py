from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

#schema
schema = StructType([StructField("name", StringType(), True),
                     StructField("age", IntegerType(), True),
                     StructField("Interests", ArrayType(StringType(), True), True)])

data = [("Ramon", 31, ["Football", "Sex", "Drinks"])]
df = spark.createDataFrame(data, schema)


df.select("name", "age", F.explode("Interests")).show()  #creates one row per interest