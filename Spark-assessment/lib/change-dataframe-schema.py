from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col
from pyspark.sql.types import StructType, StructField, StringType, LongType


spark = SparkSession.builder \
    .appName("ChangeDataFrameStructure") \
    .master("local[3]") \
    .getOrCreate()


json_rdd = spark.sparkContext.parallelize([
    """{ "name" : "john doe", "dob" : "01-01-1980" }""",
    """{ "name" : "john adam", "dob" : "01-01-1960", "phone" : 1234567890 }""",
    """{ "name" : "alice smith", "dob" : "05-12-1995" }""",
    """{ "name" : "bob brown", "dob" : "03-03-1975", "phone" : 9876543210 }"""
])


df = spark.read.json(json_rdd)


df.show(truncate=False)


df_structured = df.select(struct(col("name"), col("dob"), col("phone")).alias("personal_data"))

df_structured.show(truncate=False)

df_single_partition = df_structured.coalesce(1)
output_path = "../data/structure_changed_dataframe.json"
df_single_partition.write.mode('overwrite').json(output_path)


spark.stop()
