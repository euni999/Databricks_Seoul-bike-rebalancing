# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE seoul_bike.gold.station_net_flow AS
# MAGIC WITH outflow AS (
# MAGIC     SELECT 
# MAGIC         `시작_대여소_ID`   AS `대여소_ID`,
# MAGIC         `시작_대여소명`    AS `대여소명`,
# MAGIC         `시작_위도`        AS `위도`,
# MAGIC         `시작_경도`        AS `경도`,
# MAGIC         hour,
# MAGIC         SUM(`전체_건수`)   AS `유출`
# MAGIC     FROM seoul_bike.silver.od_enriched
# MAGIC     GROUP BY `시작_대여소_ID`, `시작_대여소명`, `시작_위도`, `시작_경도`, hour
# MAGIC ),
# MAGIC inflow AS (
# MAGIC     SELECT 
# MAGIC         `종료_대여소_ID`   AS `대여소_ID`,
# MAGIC         hour,
# MAGIC         SUM(`전체_건수`)   AS `유입`
# MAGIC     FROM seoul_bike.silver.od_enriched
# MAGIC     GROUP BY `종료_대여소_ID`, hour
# MAGIC )
# MAGIC SELECT
# MAGIC     o.`대여소_ID`, o.`대여소명`, o.`위도`, o.`경도`, o.hour,
# MAGIC     o.`유출`,
# MAGIC     COALESCE(i.`유입`, 0)           AS `유입`,
# MAGIC     COALESCE(i.`유입`, 0) - o.`유출` AS `net_flow`
# MAGIC FROM outflow o
# MAGIC LEFT JOIN inflow i ON o.`대여소_ID` = i.`대여소_ID` AND o.hour = i.hour

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold 2: 출근(7~9시) 기준 재배치 우선순위
# MAGIC CREATE OR REPLACE TABLE seoul_bike.gold.rush_hour_rebalancing AS
# MAGIC SELECT
# MAGIC     `대여소_ID`, `대여소명`, `위도`, `경도`,
# MAGIC     SUM(`유출`)     AS `출근_유출`,
# MAGIC     SUM(`유입`)     AS `출근_유입`,
# MAGIC     SUM(`net_flow`) AS `출근_net_flow`  -- 음수일수록 자전거 부족 = 재배치 1순위
# MAGIC FROM seoul_bike.gold.station_net_flow
# MAGIC WHERE `hour` BETWEEN 7 AND 9
# MAGIC GROUP BY `대여소_ID`, `대여소명`, `위도`, `경도`
# MAGIC ORDER BY `출근_net_flow` ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM seoul_bike.gold.station_net_flow ORDER BY net_flow ASC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM seoul_bike.gold.rush_hour_rebalancing LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE seoul_bike.gold.station_net_flow AS
# MAGIC WITH outflow AS (
# MAGIC     SELECT
# MAGIC         `시작_대여소_ID`  AS `대여소_ID`,
# MAGIC         `시작_대여소명`   AS `대여소명`,
# MAGIC         `시작_위도`       AS `위도`,
# MAGIC         `시작_경도`       AS `경도`,
# MAGIC         `기준_날짜`, `요일`, `시간대_구분`, `hour`,
# MAGIC         SUM(`전체_건수`)  AS `유출`
# MAGIC     FROM seoul_bike.silver.od_enriched
# MAGIC     GROUP BY `시작_대여소_ID`, `시작_대여소명`, `시작_위도`, `시작_경도`,
# MAGIC              `기준_날짜`, `요일`, `시간대_구분`, `hour`
# MAGIC ),
# MAGIC inflow AS (
# MAGIC     SELECT
# MAGIC         `종료_대여소_ID`  AS `대여소_ID`,
# MAGIC         `기준_날짜`, `hour`,
# MAGIC         SUM(`전체_건수`)  AS `유입`
# MAGIC     FROM seoul_bike.silver.od_enriched
# MAGIC     GROUP BY `종료_대여소_ID`, `기준_날짜`, `hour`
# MAGIC )
# MAGIC SELECT
# MAGIC     o.`대여소_ID`, o.`대여소명`, o.`위도`, o.`경도`,
# MAGIC     o.`기준_날짜`, o.`요일`, o.`시간대_구분`, o.`hour`,
# MAGIC     o.`유출`,
# MAGIC     COALESCE(i.`유입`, 0)           AS `유입`,
# MAGIC     COALESCE(i.`유입`, 0) - o.`유출` AS `net_flow`
# MAGIC FROM outflow o
# MAGIC LEFT JOIN inflow i ON o.`대여소_ID` = i.`대여소_ID`
# MAGIC                    AND o.`기준_날짜` = i.`기준_날짜`
# MAGIC                    AND o.`hour` = i.`hour`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE seoul_bike.gold.rush_hour_rebalancing AS
# MAGIC SELECT
# MAGIC     `대여소_ID`, `대여소명`, `위도`, `경도`, `기준_날짜`, `요일`,
# MAGIC     SUM(`유출`)     AS `출근_유출`,
# MAGIC     SUM(`유입`)     AS `출근_유입`,
# MAGIC     SUM(`net_flow`) AS `출근_net_flow`
# MAGIC FROM seoul_bike.gold.station_net_flow
# MAGIC WHERE `시간대_구분` = '출근'
# MAGIC GROUP BY `대여소_ID`, `대여소명`, `위도`, `경도`, `기준_날짜`, `요일`
# MAGIC ORDER BY `출근_net_flow` ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM seoul_bike.gold.station_net_flow

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM seoul_bike.gold.rush_hour_rebalancing