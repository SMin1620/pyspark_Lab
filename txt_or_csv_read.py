from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# SparkSession 생성
spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# TXT 파일 읽기
lines = spark.sparkContext.textFile("./data/fake_person.txt")

# 스키마 정의
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# 스키마를 적용하여 데이터프레임 생성
names = spark.read.option("sep", " ").schema(schema).csv("./data/Marvel-names.txt")

# 데이터프레임 출력
names.show()

"""
SparkSession을 사용하여 Spark 애플리케이션을 초기화합니다.

sparkContext.textFile("./data/fake_person.txt")를 사용하여 "./data/fake_person.txt" 파일의 내용을 텍스트 파일로 읽어옵니다. 각 줄은 RDD (Resilient Distributed Dataset) 요소로 나타납니다.

StructType과 StructField를 사용하여 데이터프레임의 스키마를 정의합니다. StructType은 여러 개의 StructField로 구성되며, 각 필드는 이름, 데이터 타입 및 널 허용 여부를 지정합니다.

spark.read.option("sep", " ").schema(schema).csv("./data/Marvel-names.txt")를 사용하여 "./data/Marvel-names.txt" 파일을 읽고, 스키마를 적용하여 데이터프레임을 생성합니다. 여기서 "sep" 옵션을 사용하여 데이터를 공백으로 분리합니다.

names.show()를 사용하여 생성된 데이터프레임을 출력합니다.
"""
