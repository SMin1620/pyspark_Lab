import re as regex
from pyspark.sql import SparkSession
from konlpy.tag import Okt


def normalizeWords(text):
    return regex.compile(r"\W+", regex.UNICODE).split(text)


spark = SparkSession.builder.master("local").appName("WordCounter").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark.sparkContext.textFile("./data/DaddyLongLeg_book_korean.txt")
words = lines.flatMap(normalizeWords)
# wordCounts = words.countByValue(): countByValue() 함수로 각 단어별 개수(counts)를 계산하고 결과 값을 wordCounts에 저장합니다.
wordCounts = words.countByValue()

# 명사 추출 로직
nlpy = Okt()
nouns = nlpy.nouns(lines)
print(nouns)

# wordCounts 딕셔너리에 있는 아이템들을 value(빈도수 값) 기준으로 정렬하고 그 결과물들을 순회(iterate).
# for word, count in sorted(wordCounts.items(), key=lambda x: x[1]):
#     cleanWord = word.encode("utf-8", "ignore")
#     if count > 10:
#         print(cleanWord.decode("utf-8"), count)

spark.stop()
