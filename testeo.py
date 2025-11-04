from pyspark.sql import functions as F
# Ejemplo si guardas un log en una Delta (ajústalo a tu esquema)
# logs: (ts_run, user, table, source, date_part, target_ts, affected_rows, status)
try:
    display(
      spark.table("ws_na_stress_test.update_logs")
        .where( (F.col("table")==table_name) &
                (F.col("source")==data_source) &
                (F.col("date_part")==data_date_part))
        .orderBy(F.desc("ts_run"))
        .limit(10)
    )
except:
    displayHTML("<i>Sin tabla de logs todavía.</i>")
