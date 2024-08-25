from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, avg
from pyspark.sql.functions import lit

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FilteringExample") \
    .getOrCreate()

# Sample DataFrame
data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Filter DataFrame for Age less than 30
filteredDF = df.filter(df.Age < 30)

# Show Results
filteredDF.show()


# Apply Aggregate Functions
df.agg(max("Age").alias("Max Age"), min("Age").alias("Min Age"), avg("Age").alias("Avg Age")).show()





# Rename Column "Age" to "Years"
df_renamed = df.withColumnRenamed("Age", "Years")

# Show DataFrame
df_renamed.show()






# Sample DataFrame
data = [("Alice", 25), ("Bob", 30), ("Cathy", 28)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Add new column "Country" with default value "USA"
df_with_new_column = df.withColumn("Country", lit("USA"))

# Show DataFrame
df_with_new_column.show()

# Stop Spark Session
spark.stop()
