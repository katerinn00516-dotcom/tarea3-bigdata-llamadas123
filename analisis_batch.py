cat > analisis_batch.py << 'EOF'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD)
Procesamiento en BATCH con Apache Spark (PySpark)

Problema: servicios de ambulancia (basica y medicalizada) que terminan SIN traslado
del paciente. Datos: Llamadas de Urgencias y Emergencias - Linea 123 (Datos Abiertos Bogota).

Uso:
    spark-submit analisis_batch.py llamadas123_julio2025.csv --explorar
    spark-submit analisis_batch.py llamadas123_julio2025.csv
"""
import os
import re
import sys
import unicodedata

from pyspark.sql import SparkSession, functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ----------------------------------------------------------------------------
# CONFIGURACION (ajustar despues de correr con --explorar)
# ----------------------------------------------------------------------------
args = [a for a in sys.argv[1:] if not a.startswith("--")]
CSV_PATH = args[0] if args else "llamadas123_julio2025.csv"
MODO_EXPLORAR = "--explorar" in sys.argv
OUT_DIR = "resultados_batch"
ENCODING = "UTF-8"

REGEX_MEDICALIZADA = "MEDICALIZ"
REGEX_BASICA = "BASIC"
REGEX_SIN_TRASLADO = "SIN TRASLADO|NO TRASLAD|NO REQUIERE TRASLADO|SIN_TRASLADO|NO SE TRASLAD"
COLS_TEXTO_TRASLADO = ["TIPO_INCIDENTE", "CLASIFICACION_FINAL"]
# ----------------------------------------------------------------------------

os.makedirs(OUT_DIR, exist_ok=True)


def norm(nombre):
    s = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"\W+", "_", s.strip().upper()).strip("_")


spark = (SparkSession.builder.appName("Tarea3_Batch_Llamadas123")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------------------------------
# 1. CARGA
# ----------------------------------------------------------------------------
with open(CSV_PATH, encoding=ENCODING, errors="ignore") as f:
    cabecera = f.readline()
SEP = ";" if cabecera.count(";") > cabecera.count(",") else ","

df = (spark.read.option("header", True).option("sep", SEP)
      .option("encoding", ENCODING)
      .csv("file://" + os.path.abspath(CSV_PATH)))
df = df.toDF(*[norm(c) for c in df.columns])

print("\n=== 1. CARGA ===")
print("Separador detectado:", repr(SEP))
print("Columnas:", df.columns)
df.printSchema()
print("Registros cargados:", df.count())

if MODO_EXPLORAR:
    df.show(5, truncate=False)
    for c in ["UNIDAD", "TIPO_INCIDENTE", "CLASIFICACION_FINAL", "PRIORIDAD",
              "PRIORIDAD_FINAL", "GENERO", "RECEPCION", "RED"]:
        if c in df.columns:
            print("\n--- Valores de", c, "---")
            df.groupBy(c).count().orderBy(F.desc("count")).show(30, truncate=False)
    print("\nModo exploracion terminado: ajuste las REGEX de la seccion CONFIGURACION.")
    spark.stop()
    sys.exit(0)

# ----------------------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACION
# ----------------------------------------------------------------------------
print("\n=== 2. LIMPIEZA ===")
CON_TILDE = "\u00c1\u00c9\u00cd\u00d3\u00da\u00dc\u00d1"
SIN_TILDE = "AEIOUUN"
for c, t in df.dtypes:
    if t == "string":
        col = F.translate(F.upper(F.trim(F.col(c))), CON_TILDE, SIN_TILDE)
        df = df.withColumn(c, F.when(col == "", None).otherwise(col))

n0 = df.count()
df = df.dropDuplicates()
print("Duplicados eliminados:", n0 - df.count())

print("Nulos por columna:")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show(truncate=False)

if "EDAD" in df.columns:
    df = df.withColumn("EDAD_NUM", F.regexp_extract("EDAD", r"(\d+)", 1).cast("int"))
    df = df.withColumn("EDAD_NUM", F.when((F.col("EDAD_NUM") >= 0) & (F.col("EDAD_NUM") <= 110),
                                          F.col("EDAD_NUM")))
    df = df.withColumn(
        "RANGO_EDAD",
        F.when(F.col("EDAD_NUM").isNull(), "SIN DATO")
         .when(F.col("EDAD_NUM") < 12, "0-11")
         .when(F.col("EDAD_NUM") < 18, "12-17")
         .when(F.col("EDAD_NUM") < 30, "18-29")
         .when(F.col("EDAD_NUM") < 60, "30-59")
         .otherwise("60+"))

cols_fecha = [c for c in df.columns if c.startswith("FECHA")]
if cols_fecha:
    cf = cols_fecha[0]
    ts = F.coalesce(F.to_timestamp(cf, "yyyy-MM-dd HH:mm:ss"),
                    F.to_timestamp(cf, "yyyy-MM-dd'T'HH:mm:ss"),
                    F.to_timestamp(cf, "dd/MM/yyyy HH:mm:ss"),
                    F.to_timestamp(cf, "dd/MM/yyyy HH:mm"),
                    F.to_timestamp(cf, "yyyy-MM-dd HH:mm"))
    df = df.withColumn("FECHA_TS", ts).withColumn("HORA", F.hour("FECHA_TS"))
    print("Columna de fecha usada:", cf)

tipo = (F.when(F.col("UNIDAD").rlike(REGEX_MEDICALIZADA), "MEDICALIZADA")
         .when(F.col("UNIDAD").rlike(REGEX_BASICA), "BASICA")
         .otherwise("OTRA"))
df = df.withColumn("TIPO_AMBULANCIA", tipo)

cols_txt = [c for c in COLS_TEXTO_TRASLADO if c in df.columns]
texto = F.concat_ws(" ", *[F.coalesce(F.col(c), F.lit("")) for c in cols_txt])
df = df.withColumn("SIN_TRASLADO", texto.rlike(REGEX_SIN_TRASLADO).cast("int"))

amb = df.filter(F.col("TIPO_AMBULANCIA") != "OTRA").cache()
total_amb = amb.count()
print("\nServicios de ambulancia (basica + medicalizada):", total_amb)
if total_amb == 0:
    print("!! No se detectaron ambulancias: revise REGEX_BASICA / REGEX_MEDICALIZADA con --explorar")
    sys.exit(1)
if amb.filter("SIN_TRASLADO = 1").count() == 0:
    print("!! Ningun servicio marcado SIN_TRASLADO: revise REGEX_SIN_TRASLADO con --explorar")

# ----------------------------------------------------------------------------
# 3. EDA
# ----------------------------------------------------------------------------
def resumen(d, *cols):
    return (d.groupBy(*cols)
             .agg(F.count("*").alias("total"), F.sum("SIN_TRASLADO").alias("sin_traslado"))
             .withColumn("pct_sin_traslado", F.round(100 * F.col("sin_traslado") / F.col("total"), 2)))


print("\n=== 3. EDA ===")
res = {}
res["por_tipo_ambulancia"] = resumen(amb, "TIPO_AMBULANCIA")
if "LOCALIDAD" in amb.columns:
    res["por_localidad"] = resumen(amb, "LOCALIDAD").orderBy(F.desc("sin_traslado"))
if "RANGO_EDAD" in amb.columns:
    res["por_rango_edad"] = resumen(amb, "RANGO_EDAD").orderBy("RANGO_EDAD")
if "GENERO" in amb.columns:
    res["por_genero"] = resumen(amb, "GENERO")
for pc in ["PRIORIDAD_FINAL", "PRIORIDAD"]:
    if pc in amb.columns:
        res["por_prioridad"] = resumen(amb, pc)
        break
if "HORA" in amb.columns:
    res["por_hora"] = resumen(amb.filter(F.col("HORA").isNotNull()), "HORA").orderBy("HORA")

pdfs = {}
for nombre, d in res.items():
    print("\n--- %s ---" % nombre)
    d.show(25, truncate=False)
    pdfs[nombre] = d.toPandas()
    pdfs[nombre].to_csv(os.path.join(OUT_DIR, nombre + ".csv"), index=False)

# ----------------------------------------------------------------------------
# 4. EJEMPLO CON RDDs
# ----------------------------------------------------------------------------
if "LOCALIDAD" in amb.columns:
    print("\n=== 4. RDD: localidades con mas servicios sin traslado ===")
    rdd = (amb.select("LOCALIDAD", "SIN_TRASLADO").rdd
              .filter(lambda r: r[0] is not None)
              .map(lambda r: (r[0], (1, r[1] or 0)))
              .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))
    top = rdd.map(lambda kv: (kv[0], kv[1][0], kv[1][1])).takeOrdered(10, key=lambda x: -x[2])
    for loc, tot, sin in top:
        print("%-25s total=%-6d sin_traslado=%d" % (loc, tot, sin))

# ----------------------------------------------------------------------------
# 5. VISUALIZACION
# ----------------------------------------------------------------------------
def grafico(nombre, x, y, titulo, ylabel, horizontal=False, top=None):
    if nombre not in pdfs:
        return
    p = pdfs[nombre].dropna(subset=[x])
    if top:
        p = p.head(top)
    plt.figure(figsize=(9, 5))
    if horizontal:
        plt.barh(p[x].astype(str)[::-1], p[y][::-1], color="#1f77b4")
        plt.xlabel(ylabel)
    else:
        plt.bar(p[x].astype(str), p[y], color="#1f77b4")
        plt.ylabel(ylabel)
        plt.xticks(rotation=45, ha="right")
    plt.title(titulo)
    plt.tight_layout()
    ruta = os.path.join(OUT_DIR, "grafico_%s_%s.png" % (nombre, y))
    plt.savefig(ruta, dpi=120)
    plt.close()
    print("Grafico guardado:", ruta)


print("\n=== 5. GRAFICOS ===")
grafico("por_tipo_ambulancia", "TIPO_AMBULANCIA", "pct_sin_traslado",
        "% de servicios sin traslado por tipo de ambulancia", "% sin traslado")
grafico("por_localidad", "LOCALIDAD", "sin_traslado",
        "Top 10 localidades con mas servicios sin traslado", "Servicios sin traslado",
        horizontal=True, top=10)
grafico("por_rango_edad", "RANGO_EDAD", "pct_sin_traslado",
        "% sin traslado por rango de edad", "% sin traslado")
grafico("por_hora", "HORA", "sin_traslado",
        "Servicios sin traslado por hora del dia", "Servicios sin traslado")
if "por_prioridad" in pdfs:
    pc = pdfs["por_prioridad"].columns[0]
    grafico("por_prioridad", pc, "pct_sin_traslado",
            "% sin traslado por prioridad", "% sin traslado")

print("\nResultados en la carpeta:", OUT_DIR)
spark.stop()
EOF