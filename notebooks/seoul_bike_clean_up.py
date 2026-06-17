# Databricks notebook source
from datetime import datetime, timedelta

today     = datetime.now()
cutoff    = (today.replace(day=1) - timedelta(days=180))
cutoff_ym = cutoff.strftime("%Y%m")

files   = dbutils.fs.ls("/Volumes/seoul_bike/bronze/raw/")
deleted = []

for f in files:
    name = f.name.replace("od_", "").replace(".csv", "")
    if name.isdigit() and name < cutoff_ym:
        dbutils.fs.rm(f.path)
        deleted.append(f.name)

print(f"삭제 {len(deleted)}개" if deleted else "삭제 대상 없음")