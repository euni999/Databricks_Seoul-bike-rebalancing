# Databricks notebook source
# MAGIC %sql
# MAGIC -- 두 테이블 조인 키 샘플 비교
# MAGIC SELECT 
# MAGIC     m.`대여소_ID`, 
# MAGIC     i.`대여소_ID`   AS info_ID, 
# MAGIC     i.`대여소명`, 
# MAGIC     i.`거치대수`
# MAGIC FROM seoul_bike.bronze.station_master m
# MAGIC LEFT JOIN seoul_bike.bronze.station_info i 
# MAGIC     ON m.`대여소_ID` = i.`대여소_ID`
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*)                    AS `전체`, 
# MAGIC     COUNT(i.`대여소_ID`)        AS `매칭됨`, 
# MAGIC     COUNT(*) - COUNT(i.`대여소_ID`) AS `미매칭`
# MAGIC FROM seoul_bike.bronze.station_master m
# MAGIC LEFT JOIN seoul_bike.bronze.station_info i 
# MAGIC     ON m.`대여소_ID` = i.`대여소_ID`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- station_info에만 있고 station_master에 없는 ID 샘플
# MAGIC SELECT i.`대여소_ID`, i.`대여소명`, i.`자치구`
# MAGIC FROM seoul_bike.bronze.station_info i
# MAGIC LEFT JOIN seoul_bike.bronze.station_master m
# MAGIC     ON i.`대여소_ID` = m.`대여소_ID`
# MAGIC WHERE m.`대여소_ID` IS NULL
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- station_master에만 있고 station_info에 없는 ID 샘플  
# MAGIC SELECT m.`대여소_ID`
# MAGIC FROM seoul_bike.bronze.station_master m
# MAGIC LEFT JOIN seoul_bike.bronze.station_info i
# MAGIC     ON m.`대여소_ID` = i.`대여소_ID`
# MAGIC WHERE i.`대여소_ID` IS NULL
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT `시작_대여소_ID`)                              AS `od_대여소_수`,
# MAGIC     COUNT(DISTINCT CASE WHEN m.`대여소_ID` IS NOT NULL 
# MAGIC           THEN `시작_대여소_ID` END)                              AS `master_매칭`,
# MAGIC     COUNT(DISTINCT CASE WHEN i.`대여소_ID` IS NOT NULL 
# MAGIC           THEN `시작_대여소_ID` END)                              AS `info_매칭`
# MAGIC FROM seoul_bike.bronze.od_raw o
# MAGIC LEFT JOIN seoul_bike.bronze.station_master m ON o.`시작_대여소_ID` = m.`대여소_ID`
# MAGIC LEFT JOIN seoul_bike.bronze.station_info i   ON o.`시작_대여소_ID` = i.`대여소_ID`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver: OD + 대여소 좌표 조인 및 타입 캐스팅
# MAGIC CREATE OR REPLACE TABLE seoul_bike.silver.od_enriched AS
# MAGIC SELECT
# MAGIC     TO_DATE(o.`기준_날짜`, 'yyyyMMdd')        AS `기준_날짜`,
# MAGIC     CAST(o.`기준_시간대` AS INT)              AS `기준_시간대`,
# MAGIC     CAST(o.`기준_시간대` AS INT) / 100        AS `hour`,  -- HHMM → 시간
# MAGIC     o.`집계_기준`,
# MAGIC     o.`시작_대여소_ID`,
# MAGIC     o.`시작_대여소명`,
# MAGIC     CAST(ms.`위도` AS DOUBLE)                 AS `시작_위도`,
# MAGIC     CAST(ms.`경도` AS DOUBLE)                 AS `시작_경도`,
# MAGIC     o.`종료_대여소_ID`,
# MAGIC     o.`종료_대여소명`,
# MAGIC     CAST(me.`위도` AS DOUBLE)                 AS `종료_위도`,
# MAGIC     CAST(me.`경도` AS DOUBLE)                 AS `종료_경도`,
# MAGIC     CAST(o.`전체_건수` AS INT)                AS `전체_건수`,
# MAGIC     CAST(o.`전체_이용_분` AS DOUBLE)          AS `전체_이용_분`,
# MAGIC     CAST(o.`전체_이용_거리` AS DOUBLE)        AS `전체_이용_거리`
# MAGIC FROM seoul_bike.bronze.od_raw o
# MAGIC LEFT JOIN seoul_bike.bronze.station_master ms ON o.`시작_대여소_ID` = ms.`대여소_ID`
# MAGIC LEFT JOIN seoul_bike.bronze.station_master me ON o.`종료_대여소_ID` = me.`대여소_ID`
# MAGIC WHERE CAST(o.`전체_건수` AS INT) > 0          -- 건수 0 이하 제거
# MAGIC   AND CAST(o.`전체_이용_거리` AS DOUBLE) >= 0  -- 거리 음수 제거

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE seoul_bike.silver.od_enriched AS
# MAGIC SELECT
# MAGIC     TO_DATE(o.`기준_날짜`, 'yyyyMMdd')            AS `기준_날짜`,
# MAGIC     CAST(o.`기준_시간대` AS INT)                  AS `기준_시간대`,
# MAGIC     FLOOR(CAST(o.`기준_시간대` AS INT) / 100)     AS `hour`,  -- 7.4 → 7
# MAGIC     o.`집계_기준`,
# MAGIC     o.`시작_대여소_ID`,
# MAGIC     o.`시작_대여소명`,
# MAGIC     CAST(ms.`위도` AS DOUBLE)                     AS `시작_위도`,
# MAGIC     CAST(ms.`경도` AS DOUBLE)                     AS `시작_경도`,
# MAGIC     o.`종료_대여소_ID`,
# MAGIC     o.`종료_대여소명`,
# MAGIC     CAST(me.`위도` AS DOUBLE)                     AS `종료_위도`,
# MAGIC     CAST(me.`경도` AS DOUBLE)                     AS `종료_경도`,
# MAGIC     CAST(o.`전체_건수` AS INT)                    AS `전체_건수`,
# MAGIC     CAST(o.`전체_이용_분` AS DOUBLE)              AS `전체_이용_분`,
# MAGIC     CAST(o.`전체_이용_거리` AS DOUBLE)            AS `전체_이용_거리`
# MAGIC FROM seoul_bike.bronze.od_raw o
# MAGIC LEFT JOIN seoul_bike.bronze.station_master ms ON o.`시작_대여소_ID` = ms.`대여소_ID`
# MAGIC LEFT JOIN seoul_bike.bronze.station_master me ON o.`종료_대여소_ID` = me.`대여소_ID`
# MAGIC WHERE CAST(o.`전체_건수` AS INT) > 0
# MAGIC   AND CAST(o.`전체_이용_거리` AS DOUBLE) >= 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM seoul_bike.silver.od_enriched LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE seoul_bike.silver.od_enriched AS
# MAGIC SELECT
# MAGIC     TO_DATE(o.`기준_날짜`, 'yyyyMMdd')            AS `기준_날짜`,
# MAGIC     CAST(o.`기준_시간대` AS INT)                  AS `기준_시간대`,
# MAGIC     FLOOR(CAST(o.`기준_시간대` AS INT) / 100)     AS `hour`,
# MAGIC     o.`집계_기준`,
# MAGIC     o.`시작_대여소_ID`,
# MAGIC     o.`시작_대여소명`,
# MAGIC     CAST(ms.`위도` AS DOUBLE)                     AS `시작_위도`,
# MAGIC     CAST(ms.`경도` AS DOUBLE)                     AS `시작_경도`,
# MAGIC     o.`종료_대여소_ID`,
# MAGIC     o.`종료_대여소명`,
# MAGIC     CAST(me.`위도` AS DOUBLE)                     AS `종료_위도`,
# MAGIC     CAST(me.`경도` AS DOUBLE)                     AS `종료_경도`,
# MAGIC     CAST(o.`전체_건수` AS INT)                    AS `전체_건수`,
# MAGIC     CAST(o.`전체_이용_분` AS DOUBLE)              AS `전체_이용_분`,
# MAGIC     CAST(o.`전체_이용_거리` AS DOUBLE)            AS `전체_이용_거리`,
# MAGIC     DAYOFWEEK(TO_DATE(o.`기준_날짜`, 'yyyyMMdd')) - 1  AS `요일`,
# MAGIC     CASE
# MAGIC         WHEN FLOOR(CAST(o.`기준_시간대` AS INT) / 100) BETWEEN 7 AND 9  THEN '출근'
# MAGIC         WHEN FLOOR(CAST(o.`기준_시간대` AS INT) / 100) BETWEEN 18 AND 20 THEN '퇴근'
# MAGIC         WHEN FLOOR(CAST(o.`기준_시간대` AS INT) / 100) BETWEEN 23 AND 24
# MAGIC           OR FLOOR(CAST(o.`기준_시간대` AS INT) / 100) BETWEEN 0 AND 5   THEN '심야'
# MAGIC         ELSE '기타'
# MAGIC     END                                         AS `시간대_구분`,
# MAGIC     ROUND(CAST(o.`전체_이용_거리` AS DOUBLE) / 1000, 2) AS `이동거리_km`,
# MAGIC     CASE
# MAGIC         WHEN CAST(o.`전체_이용_분` AS DOUBLE) > 0
# MAGIC         THEN ROUND(CAST(o.`전체_이용_거리` AS DOUBLE) / 1000
# MAGIC              / (CAST(o.`전체_이용_분` AS DOUBLE) / 60), 2)
# MAGIC         ELSE NULL
# MAGIC     END                                         AS `평균속도_kmh`
# MAGIC FROM seoul_bike.bronze.od_raw o
# MAGIC LEFT JOIN seoul_bike.bronze.station_master ms ON o.`시작_대여소_ID` = ms.`대여소_ID`
# MAGIC LEFT JOIN seoul_bike.bronze.station_master me ON o.`종료_대여소_ID` = me.`대여소_ID`
# MAGIC WHERE CAST(o.`전체_건수` AS INT) > 0
# MAGIC   AND CAST(o.`전체_이용_거리` AS DOUBLE) >= 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM seoul_bike.silver.od_enriched