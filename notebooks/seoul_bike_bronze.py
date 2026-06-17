# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists seoul_bike.bronze;
# MAGIC create schema if not exists seoul_bike.silver;
# MAGIC create schema if not exists seoul_bike.gold;

# COMMAND ----------

dbutils.fs.ls("/Volumes/seoul_bike/bronze/raw/")

# COMMAND ----------

# 경로 변수
OD_PATH     = "/Volumes/seoul_bike/bronze/raw/tpss_bcycl_od_statnhm_20260609.csv"
MASTER_PATH = "/Volumes/seoul_bike/bronze/raw/station_master.csv"

# OD 적재
df_od = (
    spark.read.format("csv")
    .option("header", "true")
    .option("encoding", "EUC-KR")
    .option("inferSchema", "false")  # Bronze는 전부 string
    .load(OD_PATH)
)

df_od.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("seoul_bike.bronze.od_raw")

print(f"OD: {df_od.count():,}행")
df_od.show(3, truncate=False)

# COMMAND ----------

# 마스터 적재
df_master = (
    spark.read.format("csv")
    .option("header", "true")
    .option("encoding", "EUC-KR")
    .option("inferSchema", "false")
    .load(MASTER_PATH)
)

df_master.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("seoul_bike.bronze.station_master")

print(f"마스터: {df_master.count():,}행")
df_master.show(3, truncate=False)

# COMMAND ----------

%pip install openpyxl
%pip install pandas
import pandas as pd

# pandas로 xlsx 읽어서 spark DataFrame으로 변환
df_station = spark.createDataFrame(
    pd.read_excel("/Volumes/seoul_bike/bronze/raw/station_info.xlsx").rename(columns=lambda x: x.strip()).astype(str)
)

df_station.printSchema()
df_station.show(3, truncate=False)

# COMMAND ----------

import pandas as pd

df_station = spark.createDataFrame(
    pd.read_excel("/Volumes/seoul_bike/bronze/raw/station_info.xlsx").astype(str)
)

df_station.printSchema()
df_station.show(3, truncate=False)

# COMMAND ----------

import pandas as pd

# 헤더 무시하고 처음 10행 원본으로 확인
df_raw = pd.read_excel(
    "/Volumes/seoul_bike/bronze/raw/station_info.xlsx",
    header=None
)
print(df_raw.head(10).to_string())

# COMMAND ----------

import pandas as pd

df_pd = pd.read_excel(
    "/Volumes/seoul_bike/bronze/raw/station_info.xlsx",
    header=None,
    skiprows=5  # 0~4행 헤더 스킵
)

# 필요한 컬럼만 추출 + 이름 정리
df_pd = df_pd[[0, 1, 2, 4, 5, 8]].copy()
df_pd.columns = ["대여소번호", "대여소명", "자치구", "위도", "경도", "거치대수"]

# OD 조인 키 맞추기: 102 → ST-102
df_pd["대여소_ID"] = "ST-" + df_pd["대여소번호"].astype(str).str.strip()

# NaN 행 제거
df_pd = df_pd.dropna(subset=["대여소번호"])

print(df_pd.shape)
print(df_pd.head(3))

# COMMAND ----------

# pandas DataFrame → Spark DataFrame → Delta 테이블
df_station = spark.createDataFrame(df_pd)

df_station.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("seoul_bike.bronze.station_info")

print(f"저장 완료: {df_station.count():,}행")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM seoul_bike.bronze.station_info LIMIT 5

# COMMAND ----------

df_merged = (
    spark.read.format("csv")
    .option("header", "true")
    .option("encoding", "utf-8")
    .option("inferSchema", "false")
    .load("/Volumes/seoul_bike/bronze/raw/od_merged_3months.csv")
)

df_merged.write.format("delta").mode("append").saveAsTable("seoul_bike.bronze.od_raw")

print(f"적재 완료: {df_merged.count():,}행")