from pyspark.sql import SparkSession

import sys

from logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .getOrCreate()

    logger = Log4j(spark)
    csv_file = "../data/sample.csv"
    # if len(sys.argv) != 2:
    #     logger.error("Usage: HelloSpark <filename>")
    #     sys.exit(-1)

    surveyDF = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_file)

    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

    countDF.show()


    #  # Read JSON file
    json_file = "../data/survey.json"
    jsonDF = spark.read.option("multiline","true").json(json_file)

    # # Create a temporary view for JSON data
    jsonDF.createOrReplaceTempView("json_tbl")

    # # Perform SQL query on JSON data
    logger.info("Processing JSON file...")
    countDF_json = spark.sql("SELECT Country, COUNT(1) AS count FROM json_tbl WHERE Age < 40 GROUP BY Country")
    # countDF_json.show()

    
    # # # Read Parquet file
    parquet_file = "../data/sample-p.parquet"
    parquetDF = spark.read.parquet(parquet_file)
    parquetDF.show()

    # # # Create a temporary view for Parquet data
    parquetDF.createOrReplaceTempView("parquet_tbl")

    # # # # Perform SQL query on Parquet data
    logger.info("Processing Parquet file...")
    countDF_parquet = spark.sql("SELECT Country, COUNT(1) AS count FROM parquet_tbl WHERE Age < 40 GROUP BY Country")
    countDF_parquet = spark.sql("SELECT * FROM parquet_tbl")
    countDF_parquet.show()

    spark.stop()