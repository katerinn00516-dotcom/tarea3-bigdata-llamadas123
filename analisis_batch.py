#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Tarea 3 - Big Data (UNAD) - Procesamiento BATCH con Apache Spark
#
# Problema: servicios de emergencia en salud de la linea 123 (Bogota) que NO registran
# recepcion del paciente (RECEPCION vacia), usado como indicador aproximado de servicios
# que terminan sin traslado. El dataset no distingue ambulancia basica / medicalizada.
#
# Uso: spark-submit analisis_batch.py llamadas123_utf8.csv

import os
import re
import sys
import unicodedata

from pyspark.sql import SparkSession, functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

args = [a for a in sys.argv[1:] if not a.startswith("--")]
CSV_PATH = args[0] if args else "llamadas123_utf8.csv"
OUT_DIR = "resultados_batch"
ENCODING = "UTF-8"   # el CSV original esta en CP850; se convierte antes con iconv
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
      .option("encoding", ENCODING).option("nullValue", "NULL")
      .csv("file://" + os.path.abspath(CSV_PATH)))
df = df.toDF(*[norm(c) for c in df.columns])

print("\n=== 1. CARGA ===")
print("Columnas:", df.columns)
print("Registros cargados:", df.count())

# ----------------------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACION
# ----------------------------------------------------------------------------
print("\n=== 2. LIMPIEZA ===")
CON_TILDE = "\u00c1\u00c9\u00cd\u00d3\u00da\u00dc\u00d1"
SIN_TILDE = "AEIOUUN"
for c, t in df.dtypes:
    if t == "string":
        col = F.translate(F.upper(F.trim(F.col(c))), CON_TILDE, SIN_TILDE)
        df = df.withColumn(c, F.when(col.isin("", "NULL"), None).otherwise(col))

n0 = df.count()
df = df.dropDuplicates()
print("Duplicados eliminados:", n0 - df.count())

# Fila con datos de paciente (GENERO distinto de N/A)
df = df.withColumn("PACIENTE_REGISTRADO",
                   F.when(F.col("GENERO").isNull() | (F.col("GENERO") == "N/A"), 0).otherwise(1))
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, F.when(F.col(c) == "N/A", None).otherwise(F.col(c)))

# UNIDAD es la unidad de la EDAD (anos / meses / dias), no el tipo de ambulancia
df = df.withColumnRenamed("UNIDAD", "UNIDAD_EDAD")
edad = F.regexp_extract("EDAD", r"(\d+)", 1).cast("int")
df = df.withColumn(
    "EDAD_ANIOS",
    F.when(F.col("EDAD").isNull(), F.lit(None).cast("int"))
     .when(F.col("UNIDAD_EDAD").rlike("^ANO"), edad)
     .when(F.col("UNIDAD_EDAD").rlike("^(MES|DIA)"), F.lit(0)))
df = df.withColumn("EDAD_ANIOS", F.when(F.col("EDAD_ANIOS").between(0, 110), F.col("EDAD_ANIOS")))
df = df.withColumn(
    "RANGO_EDAD",
    F.when(F.col("EDAD_ANIOS").isNull(), "SIN DATO")
     .when(F.col("EDAD_ANIOS") < 12, "0-11")
     .when(F.col("EDAD_ANIOS") < 18, "12-17")
     .when(F.col("EDAD_ANIOS") < 30, "18-29")
     .when(F.col("EDAD_ANIOS") < 60, "30-59")
     .otherwise("60+"))


def a_ts(c):
    # Convierte textos como 2025-07 01 00:05:50 en timestamp
    limpio = F.regexp_replace(
        F.col(c), r"^(\d{4})\D+(\d{2})\D+(\d{2})\D+(\d{2}:\d{2}:\d{2}).*$", "$1-$2-$3 $4")
    return F.to_timestamp(limpio, "yyyy-MM-dd HH:mm:ss")


cf = [c for c in df.columns if c.startswith("FECHA")][0]
df = (df.withColumn("FECHA_TS", a_ts(cf))
        .withColumn("RECEPCION_TS", a_ts("RECEPCION"))
        .withColumn("HORA", F.hour("FECHA_TS"))
        .withColumn("DIA_SEMANA", F.date_format("FECHA_TS", "E")))
mins = (F.unix_timestamp("RECEPCION_TS") - F.unix_timestamp("FECHA_TS")) / 60
df = df.withColumn("MIN_A_RECEPCION", F.when(mins >= 0, mins))

# Variable objetivo (indicador aproximado de "sin traslado")
df = df.withColumn("SIN_RECEPCION", F.col("RECEPCION").isNull().cast("int"))
# Nombre corto del incidente (texto antes del guion)
df = df.withColumn("TIPO_CORTO", F.trim(F.split(F.col("TIPO_INCIDENTE"), " - ").getItem(0)))

df = df.cache()
total = df.count()
print("Registros tras limpieza:", total)
print("Fechas de despacho interpretadas:", df.filter("FECHA_TS is not null").count())
print("Fechas de recepcion interpretadas:", df.filter("RECEPCION_TS is not null").count())
print("Servicios SIN recepcion:", df.filter("SIN_RECEPCION = 1").count())
print("\nPaciente registrado vs sin recepcion:")
df.groupBy("PACIENTE_REGISTRADO", "SIN_RECEPCION").count().orderBy("PACIENTE_REGISTRADO", "SIN_RECEPCION").show()

# Base del analisis: solo servicios con paciente registrado
df = df.filter("PACIENTE_REGISTRADO = 1").cache()
print("Servicios con paciente registrado (base del analisis):", df.count())

# ----------------------------------------------------------------------------
# 3. EDA
# ----------------------------------------------------------------------------
def resumen(d, *cols):
    return (d.groupBy(*cols)
             .agg(F.count("*").alias("total"), F.sum("SIN_RECEPCION").alias("sin_recepcion"))
             .withColumn("pct_sin_recepcion", F.round(100 * F.col("sin_recepcion") / F.col("total"), 2)))


print("\n=== 3. EDA ===")
res = {
    "por_paciente_registrado": resumen(df, "PACIENTE_REGISTRADO"),
    "por_tipo_incidente": resumen(df, "TIPO_CORTO").orderBy(F.desc("total")),
    "por_localidad": resumen(df, "LOCALIDAD").orderBy(F.desc("sin_recepcion")),
    "por_prioridad": resumen(df, "PRIORIDAD_FINAL").orderBy(F.desc("total")),
    "por_rango_edad": resumen(df, "RANGO_EDAD").orderBy("RANGO_EDAD"),
    "por_genero": resumen(df, "GENERO"),
    "por_hora": resumen(df.filter(F.col("HORA").isNotNull()), "HORA").orderBy("HORA"),
    "por_dia_semana": resumen(df.filter(F.col("DIA_SEMANA").isNotNull()), "DIA_SEMANA"),
    "tiempo_a_recepcion_por_prioridad": (
        df.filter(F.col("MIN_A_RECEPCION").isNotNull())
          .groupBy("PRIORIDAD_FINAL")
          .agg(F.count("*").alias("servicios"),
               F.round(F.avg("MIN_A_RECEPCION"), 1).alias("min_promedio"))
          .orderBy(F.desc("servicios"))),
}

pdfs = {}
for nombre, d in res.items():
    print("\n--- %s ---" % nombre)
    d.show(25, truncate=False)
    pdfs[nombre] = d.toPandas()
    pdfs[nombre].to_csv(os.path.join(OUT_DIR, nombre + ".csv"), index=False)

# ----------------------------------------------------------------------------
# 4. EJEMPLO CON RDDs
# ----------------------------------------------------------------------------
print("\n=== 4. RDD: localidades con mas servicios sin recepcion ===")
rdd = (df.select("LOCALIDAD", "SIN_RECEPCION").rdd
         .filter(lambda r: r[0] is not None)
         .map(lambda r: (r[0], (1, r[1] or 0)))
         .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))
top = rdd.map(lambda kv: (kv[0], kv[1][0], kv[1][1])).takeOrdered(10, key=lambda x: -x[2])
for loc, tot, sin in top:
    print("%-25s total=%-6d sin_recepcion=%d" % (loc, tot, sin))

# ----------------------------------------------------------------------------
# 5. VISUALIZACION
# ----------------------------------------------------------------------------
def grafico(nombre, x, y, titulo, ylabel, horizontal=False, top=None, minimo=None):
    p = pdfs[nombre].dropna(subset=[x])
    if minimo:
        p = p[p["total"] >= minimo]
    if y.startswith("pct") and top:
        p = p.sort_values(y, ascending=False)
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
grafico("por_tipo_incidente", "TIPO_CORTO", "pct_sin_recepcion",
        "% de servicios sin recepcion por tipo de incidente (min. 30 casos)",
        "% sin recepcion", horizontal=True, top=12, minimo=30)
grafico("por_localidad", "LOCALIDAD", "sin_recepcion",
        "Top 10 localidades con mas servicios sin recepcion", "Servicios sin recepcion",
        horizontal=True, top=10)
grafico("por_prioridad", "PRIORIDAD_FINAL", "pct_sin_recepcion",
        "% sin recepcion por prioridad", "% sin recepcion")
grafico("por_rango_edad", "RANGO_EDAD", "pct_sin_recepcion",
        "% sin recepcion por rango de edad", "% sin recepcion")
grafico("por_hora", "HORA", "sin_recepcion",
        "Servicios sin recepcion por hora del dia", "Servicios sin recepcion")

print("\nResultados en la carpeta:", OUT_DIR)
spark.stop()
