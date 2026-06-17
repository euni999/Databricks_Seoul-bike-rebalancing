# Databricks notebook source
from datetime import datetime, timedelta

today      = datetime.now()
last_month = (today.replace(day=1) - timedelta(days=1))
ym         = last_month.strftime("%Y%m")

# 테스트용: 기존 파일 복사 (실제 운영 시 수동 업로드로 대체)
src = "/Volumes/seoul_bike/bronze/raw/tpss_bcycl_od_statnhm_20260609.csv"
dst = f"/Volumes/seoul_bike/bronze/raw/od_{ym}.csv"

dbutils.fs.cp(src, dst)
print(f"완료: od_{ym}.csv")