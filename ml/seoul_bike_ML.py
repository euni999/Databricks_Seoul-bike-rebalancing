# Databricks notebook source
# MAGIC %sql
# MAGIC -- ML 피처 테이블: 대여소×날짜×시간대별 집계
# MAGIC CREATE OR REPLACE TABLE seoul_bike.gold.ml_features AS
# MAGIC SELECT
# MAGIC     `대여소_ID`,
# MAGIC     `대여소명`,
# MAGIC     `위도`,
# MAGIC     `경도`,
# MAGIC     `기준_날짜`,
# MAGIC     `요일`,
# MAGIC     `hour`,
# MAGIC     `시간대_구분`,
# MAGIC     SUM(`유출`)     AS `유출`,
# MAGIC     SUM(`유입`)     AS `유입`,
# MAGIC     SUM(`net_flow`) AS `net_flow`
# MAGIC FROM seoul_bike.gold.station_net_flow
# MAGIC GROUP BY `대여소_ID`, `대여소명`, `위도`, `경도`, `기준_날짜`, `요일`, `hour`, `시간대_구분`
# MAGIC ORDER BY `대여소_ID`, `기준_날짜`, `hour`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM seoul_bike.gold.ml_features

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import LabelEncoder

mlflow.set_experiment("/seoul_bike_demand_forecast")

df = spark.sql("""
    SELECT `대여소_ID`, `요일`, `hour`, `시간대_구분`, `유출`
    FROM seoul_bike.gold.ml_features
""").toPandas()

print(f"학습 데이터: {len(df):,}행")

time_map = {"출근": 0, "퇴근": 1, "심야": 2, "기타": 3}
df["시간대_코드"] = df["시간대_구분"].map(time_map)

# 대여소_ID 인코딩 (ST-1645 같은 문자열 → 숫자)
le = LabelEncoder()
df["대여소_코드"] = le.fit_transform(df["대여소_ID"])

X = df[["대여소_코드", "요일", "hour", "시간대_코드"]]
y = df["유출"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

with mlflow.start_run(run_name="all_stations_xgboost"):
    model = XGBRegressor(
        n_estimators=100,
        max_depth=6,        # 대여소 수가 많아서 깊이 늘림
        learning_rate=0.1,
        random_state=42,
        n_jobs=-1           # 병렬 처리
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    mlflow.log_params({"n_estimators": 100, "max_depth": 6, "learning_rate": 0.1})
    mlflow.log_metrics({"mae": mae, "rmse": rmse})
    mlflow.sklearn.log_model(model, "model")

    print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}")

# COMMAND ----------

# MAGIC %pip install xgboost

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import LabelEncoder

mlflow.set_experiment("/seoul_bike_demand_forecast")

df = spark.sql("""
    SELECT `대여소_ID`, `요일`, `hour`, `시간대_구분`, `유출`
    FROM seoul_bike.gold.ml_features
""").toPandas()

time_map = {"출근": 0, "퇴근": 1, "심야": 2, "기타": 3}
df["시간대_코드"] = df["시간대_구분"].map(time_map)

le = LabelEncoder()
df["대여소_코드"] = le.fit_transform(df["대여소_ID"])

X = df[["대여소_코드", "요일", "hour", "시간대_코드"]]
y = df["유출"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

input_example = X_train.iloc[:5]  # signature 자동 추론용

with mlflow.start_run(run_name="all_stations_xgboost_v2"):
    model = XGBRegressor(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    mlflow.log_params({"n_estimators": 100, "max_depth": 6, "learning_rate": 0.1})
    mlflow.log_metrics({"mae": mae, "rmse": rmse})

    # signature + input_example 추가 (경고 해결)
    mlflow.sklearn.log_model(
        model,
        name="model",
        input_example=input_example
    )

    print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}")

# 예측 결과 Gold 테이블로 저장
df["예측_유출"] = model.predict(X)
df["예측_유출"] = df["예측_유출"].clip(lower=0).round().astype(int)  # 음수 제거, 정수화

result = spark.createDataFrame(
    df[["대여소_ID", "요일", "hour", "시간대_구분", "유출", "예측_유출"]]
)

result.write.format("delta").mode("overwrite").saveAsTable(
    "seoul_bike.gold.demand_forecast"
)

print(f"예측 테이블 저장 완료: {result.count():,}행")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     `대여소_ID`, `요일`, `hour`, `시간대_구분`,
# MAGIC     `유출`        AS `실제_유출`,
# MAGIC     `예측_유출`,
# MAGIC     ABS(`유출` - `예측_유출`) AS `오차`
# MAGIC FROM `seoul_bike`.`gold`.`demand_forecast`
# MAGIC WHERE `시간대_구분` = '출근'
# MAGIC ORDER BY `오차` DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ST-1718 평균 유출량 vs 전체 평균 비교
# MAGIC SELECT
# MAGIC     '전체 평균'  AS `구분`, ROUND(AVG(`유출`), 1) AS `평균_유출`
# MAGIC FROM seoul_bike.gold.ml_features
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'ST-1718'   AS `구분`, ROUND(AVG(`유출`), 1) AS `평균_유출`
# MAGIC FROM seoul_bike.gold.ml_features
# MAGIC WHERE `대여소_ID` = 'ST-1718'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `대여소_ID`, `대여소명`, `위도`, `경도`
# MAGIC FROM seoul_bike.gold.ml_features
# MAGIC WHERE `대여소_ID` = 'ST-1718'
# MAGIC LIMIT 1

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import LabelEncoder

mlflow.set_experiment("/seoul_bike_demand_forecast")

df = spark.sql("""
    SELECT `대여소_ID`, `요일`, `hour`, `시간대_구분`, `유출`
    FROM seoul_bike.gold.ml_features
""").toPandas()

time_map = {"출근": 0, "퇴근": 1, "심야": 2, "기타": 3}
df["시간대_코드"] = df["시간대_구분"].map(time_map)

# 이상치 대여소 제거: 대여소별 평균 유출이 전체 평균 3배 초과
station_mean = df.groupby("대여소_ID")["유출"].mean()
threshold    = df["유출"].mean() * 3
normal_stations = station_mean[station_mean <= threshold].index
df_filtered = df[df["대여소_ID"].isin(normal_stations)].copy()

removed = len(station_mean) - len(normal_stations)
print(f"제거된 이상치 대여소: {removed}개")
print(f"학습 데이터: {len(df_filtered):,}행")

le = LabelEncoder()
df_filtered["대여소_코드"] = le.fit_transform(df_filtered["대여소_ID"])

X = df_filtered[["대여소_코드", "요일", "hour", "시간대_코드"]]
y = df_filtered["유출"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

input_example = X_train.iloc[:5]

with mlflow.start_run(run_name="outlier_removed_xgboost"):
    model = XGBRegressor(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    mlflow.log_params({
        "n_estimators": 100,
        "max_depth": 6,
        "learning_rate": 0.1,
        "outlier_threshold": f"mean * 3 ({threshold:.1f})"
    })
    mlflow.log_metrics({"mae": mae, "rmse": rmse})
    mlflow.sklearn.log_model(model, name="model", input_example=input_example)

    print(f"이상치 제거 후 → MAE: {mae:.2f}, RMSE: {rmse:.2f}")
    print(f"기존 모델      → MAE: 3.54, RMSE: 6.09")