from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, avg
from pyspark.sql.functions import lit


spark = SparkSession.builder \
    .appName("FilteringExample") \
    .getOrCreate()


data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Filter
filteredDF = df.filter(df.Age < 30)


filteredDF.show()


# Aggregate Functions
df.agg(max("Age").alias("Max Age"), min("Age").alias("Min Age"), avg("Age").alias("Avg Age")).show()





# Rename Column 
df_renamed = df.withColumnRenamed("Age", "Years")


df_renamed.show()

# Sample DataFrame
data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Add new column "
df_with_new_column = df.withColumn("Country", lit("USA"))


df_with_new_column.show()


spark.stop()
