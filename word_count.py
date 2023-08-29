# import re as regex: 이 줄은 Python의 정규 표현식 모듈인 're'를 'regex'라는 이름으로 불러오는 코드입니다.
# 이후 코드에서는 'regex'라는 이름으로 정규 표현식 관련 기능을 사용합니다
import re as regex
from pyspark.sql import SparkSession

# 이 함수는 입력된 텍스트를 단어로 분리하는 역할을 합니다.
# 정규표현식 \W+를 사용하여 비문자(non-word) 문자열이나 공백 등을 기준으로 텍스트를 분리합니다.
def normalizeWords(text):
    return regex.compile(r"\W+", regex.UNICODE).split(text)


spark = SparkSession.builder.master("local").appName("WordCounter").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines = spark.sparkContext.textFile("./data/DaddyLongLeg_book_korean.txt")
words = lines.flatMap(normalizeWords)
# wordCounts = words.countByValue(): countByValue() 함수로 각 단어별 개수(counts)를 계산하고 결과 값을 wordCounts에 저장합니다.
wordCounts = words.countByValue()

# wordCounts 딕셔너리에 있는 아이템들을 value(빈도수 값) 기준으로 정렬하고 그 결과물들을 순회(iterate).
for word, count in sorted(wordCounts.items(), key=lambda x: x[1]):
    cleanWord = word.encode("utf-8", "ignore")
    if count > 10:
        print(cleanWord.decode("utf-8"), count)

spark.stop()

"""
나는 11
말이다 11
아는 11
마시는 11
가고 11
벌써 11
여자 11
분의 11
뜻임 11
나옴 11
.
.
.
"""