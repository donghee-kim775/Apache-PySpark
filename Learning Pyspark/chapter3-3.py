from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *
sc = SparkContext('local')
spark = SparkSession(sc)

# 프로그래밍적인 방법으로 스키마를 정의한다.
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)
                          ])
# path
sf_fire_file = "./excode/example/sf-fire-calls.csv"

# DataFrameReader 인터페이스로 csv파일 읽기
fire_df = spark.read.csv(sf_fire_file, header = True, schema = fire_schema)

# Parquet로 저장
# parquet_path = 'sf-fire-calls3.parquet'
# fire_df.write.format("parquet").save(parquet_path)

# 테이블로 저장
# parquet_table = 's-fire-calls3'
# fire_df.write.format("parquet").saveAsTable(parquet_table)

# 샌프란시스코 소방서에서 특정한 관점에서 살피기
few_fire_df = (fire_df
               .select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "MedicalIncident"))

few_fire_df.show(5, truncate = False)

# 화재 신고로 기록된 CallType 종류가 몇가지인지 알고싶다면?
# countDistinct()를 써서 신고 타입의 개수를 리턴함
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show())

# 아래의 질의어로 Null이 아닌 신고 타입의 목록을 알 수 있다.
# null이 아닌 개별 CallType을 추출
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))

# Column 변경 : df.withColumnRenamed(a,b)
# Delay -> ResponseDelayedinMins
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5)
 .show(5, False))

# 문자열 type -> date type
# to_timestamp() / to_date()
fire_ts_df = (new_fire_df
              .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
              .drop("CallDate")
              .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
              .drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
              .drop("AvailableDtTm"))

# 변환된 Column 가져오기
(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))

# 지난 7일 동안 기록된 통화 수를 확인해보자.
# dayofmonth()
# dayofyear()
# dayofweek()
(fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 .show())

# 가장 흔한 형태의 신고는 무엇인가?
# groupBy()
# orderBy()
# count()
(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(10, False))

import pyspark.sql.functions as F

(fire_ts_df
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
         F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
         .show())