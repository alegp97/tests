
def get_source():
    # 1) Preferir inyección desde el notebook
    for k in ("source", "table_name", "RUTA_TABLA"):
        if k in globals() and str(globals()[k]).strip():
            return str(globals()[k]).strip()
    # 2) Fallback a argv si lo ejecutas como script
    if len(sys.argv) >= 2:
        return sys.argv[1]
    raise SystemExit("Uso: run_tabla.py <catalog.schema.table | ruta delta>")

# Reutiliza la sesión del notebook si existe; si no, crea una
spark = globals().get("spark") or SparkSession.builder.appName("Tabla-Launcher").getOrCreate()

source = get_source()

# Carga por ruta Delta (abfss:/dbfs:/s3:/...) o por nombre UC
if is_path(source):
    df = spark.read.format("delta").load(source)
else:
    df = spark.table(source)

print("Spark appId:", spark.sparkContext.applicationId)
print("Versión Spark:", spark.version)
df.show(20, truncate=False)
print("count:", df.count())
