#!/usr/bin/env python
from pyspark.sql import SparkSession

# Pon aquí tu ruta (ABFS/DBFS/S3/local montado)
XLSX_PATH = "abfss://<container>@<account>.dfs.core.windows.net/ruta/archivo.xlsx"
# Ejemplos:
# XLSX_PATH = "dbfs:/FileStore/tables/ejemplo.xlsx"
# XLSX_PATH = "/dbfs/FileStore/tables/ejemplo.xlsx"  # vía FUSE

spark = SparkSession.builder.appName("ReadXLSX").getOrCreate()

df = (
    spark.read
         .format("com.crealytics.spark.excel")
         .option("header", "true")        # usa la primera fila como cabecera
         .option("inferSchema", "true")    # infiere tipos
         # .option("sheetName", "Hoja1")   # opcional: especifica hoja
         .load(XLSX_PATH)
)

df.show(10, truncate=False)

spark.stop()
