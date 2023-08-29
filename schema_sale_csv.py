from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType
)

spark = SparkSession.builder.master("local").appName("WordCounter").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [
        StructField("stationID", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True),
    ]
)

df = spark.read.schema(schema).csv("./data/1800_weather.csv")
df.printSchema()

