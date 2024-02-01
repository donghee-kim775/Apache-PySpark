from pyspark.sql import SparkSession

# SparkSession
spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())

# 데이터세트 경로
csv_file = "./excode/example/departuredelays.csv"

# 읽고 임시뷰를 생성
# 스키마 추론
# + 더 큰 파일의 경우 스키마를 지정해주도록 하자
df = (spark.read.format('csv')
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csv_file))

df.createOrReplaceTempView("us_delay_flights_tbl")

# 주행거리가 1000마일 이상인 모든 항공편 찾기
# spark 쿼리문
spark.sql("""SELECT distance, origin, destination
            FROM us_delay_flights_tbl WHERE distance > 1000
            ORDER BY distance DESC""").show(10)
# DataFrame 쿼리문
from pyspark.sql.functions import col, desc
(df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)


# 샌프란시스코(SFO) 와 시카고(ORD) 간 2시간 이상 지연이 있었던 모든 항공편을 찾기
spark.sql("""SELECT date, delay, origin, destination
            FROM us_delay_flights_tbl
            WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
            ORDER by delay DESC""").show(10)

# 지연시간에 따른 쿼리
spark.sql("""SELECT delay, origin, destination,
          CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay >= 120 AND delay <= 360 THEN 'Long Delays'
            WHEN delay >= 60 AND delay <= 120 THEN 'Very Long Delays'
            WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
            WHEN delay = 0 THEN 'No Delays'
            ELSE 'Early'
          END AS Flight_Delays
          FROM us_delay_flights_tbl
          ORDER BY origin, delay DESC""").show(10)