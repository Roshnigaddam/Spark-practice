from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("Partions") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("../data/dataSource/flight*.parquet")

    logger.info("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.info("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    partitionedDF.write \
        .format("avro") \
        .mode("overwrite") \
        .save("../data/dataSink/avro/")

    # flightTimeParquetDF.write \
    #     .format("json") \
    #     .mode("overwrite") \
    #     .option("path", "../dat/dataSink/json/") \
    #     .partitionBy("OP_CARRIER", "ORIGIN") \
    #     .option("maxRecordsPerFile", 10000) \
    #     .save()