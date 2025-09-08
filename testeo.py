#!/usr/bin/env python
import sys
from pyspark.sql import SparkSession

def is_path(s: str) -> bool:
    return s.startswith("/") or s.startswith("dbfs:/") or "://" in s

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Uso: run_bdr.py <catalog.schema.table | ruta delta>")

    source = sys.argv[1]
    spark = SparkSession.builder.appName("BDR-Launcher").getOrCreate()

    # Lee por nombre de tabla o por ruta (asumimos Delta si es ruta)
    if is_path(source):
        df = spark.read.format("delta").load(source)
    else:
        df = spark.table(source)

    df.show(20, truncate=False)
    print("count:", df.count())
    print("Spark appId:", spark.sparkContext.applicationId)
    spark.stop()
