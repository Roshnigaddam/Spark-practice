from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col
import json


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


df_structured = df.select(struct(
    col("name").alias("name"),
    col("dob").alias("dob"),
    col("phone").alias("phone")
).alias("personal_data"))



df_structured.show(truncate=False)


data = df_structured.collect()


data_dict = [{"personal_data": row.personal_data.asDict()} for row in data]


output_path = "../data/structure_changed_dataframe.json"
with open(output_path, 'w') as f:
    json.dump(data_dict, f, indent=4)



spark.stop()
