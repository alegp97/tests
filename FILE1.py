from datetime import datetime
from zoneinfo import ZoneInfo
import sys, os, io, traceback, logging
from contextlib import redirect_stdout, redirect_stderr

# === Config ruta del log ===
ts = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%Y%m%d_%H%M%S")
log_path_local = f"logs/log_{ts}.log"
os.makedirs("logs", exist_ok=True)

# === Tee para duplicar a consola + fichero ===
class Tee(io.TextIOBase):
    def __init__(self, *streams):
        self.streams = streams
        self.encoding = getattr(streams[0], "encoding", "utf-8")
    def write(self, s):
        for st in self.streams:
            st.write(s)
        return len(s)
    def flush(self):
        for st in self.streams:
            try: st.flush()
            except Exception: pass
    def isatty(self): return False
    def writable(self): return True

with open(log_path_local, "w", buffering=1, encoding="utf-8") as lf:
    tee_out = Tee(sys.stdout, lf)
    tee_err = Tee(sys.stderr, lf)

    # Enviar logging también al tee
    logging.basicConfig(
        level=logging.INFO,
        handlers=[logging.StreamHandler(tee_out)],
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Capturar *cualquier* excepción no manejada con traceback completo
    def excepthook(exctype, value, tb):
        traceback.print_exception(exctype, value, tb, file=tee_err)
    sys.excepthook = excepthook

    # (Opcional) redirigir warnings al logging
    import warnings
    def _warn_to_log(message, category, filename, lineno, file=None, line=None):
        logging.warning(warnings.formatwarning(message, category, filename, lineno, line))
    warnings.showwarning = _warn_to_log

    # (Opcional) añadir logs de Spark/Java (log4j) al mismo fichero
    try:
        log4j = spark._jvm.org.apache.log4j
        root = log4j.LogManager.getRootLogger()
        layout = log4j.PatternLayout("%d{ISO8601} %-5p %c: %m%n")
        appender = log4j.FileAppender(layout, log_path_local, True)
        appender.setImmediateFlush(True); appender.activateOptions()
        root.addAppender(appender)
    except Exception:
        pass  # si no hay Spark o log4j, lo ignoramos

    # === Ejecutar tu .py con stdout/stderr redirigidos ===
    globals_for_script = {
        "spark": spark,           # si no usas Spark, elimina esta línea
        "arg1": "pruebaArgumento",
        "arg2": "prueba argumento 2",
    }

    try:
        with redirect_stdout(tee_out), redirect_stderr(tee_err):
            code = open("./test_spark_local.py", encoding="utf-8").read()
            exec(compile(code, "test_spark_local.py", "exec"), globals_for_script)
    except Exception:
        # También cae aquí cualquier excepción dentro del exec
        traceback.print_exc(file=tee_err)

print(f"Salida guardada en {log_path_local}")
