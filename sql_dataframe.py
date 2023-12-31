from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("WordCounter").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

people = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("./data/fakefriends-header.csv")
)

print("Inferred schema : ")
people.printSchema()

print("Name column : ")
people.select("name").show()

print("Filter age < 21 : ")
people.filter(people.age < 21).show()

print("Group by age : ")
people.groupby("age").count().show()

print("Make everyone + 10y older : ")
people.select(people.name, people.age + 10).show()
