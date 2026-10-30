from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, desc
from pyspark.sql.functions import sum as _sum, round as _round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Streaming123").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("incidente_id", IntegerType()),
    StructField("localidad", StringType()),
    StructField("tipo_incidente", StringType()),
    StructField("prioridad", StringType()),
    StructField("edad", IntegerType()),
    StructField("genero", StringType()),
    StructField("sin_recepcion", IntegerType()),
    StructField("timestamp", IntegerType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "llamadas_123")
       .option("startingOffsets", "latest")
       .load())

eventos = (raw.select(from_json(col("value").cast("string"), schema).alias("d"))
           .select("d.*")
           .withColumn("ts", col("timestamp").cast("timestamp")))

# 1) Por ventana de 1 minuto: servicios y % sin traslado por tipo de incidente
por_ventana = (eventos.groupBy(window("ts", "1 minute"), "tipo_incidente")
               .agg(count("*").alias("servicios"),
                    _sum("sin_recepcion").alias("sin_traslado"))
               .withColumn("pct_sin_traslado",
                           _round(col("sin_traslado") * 100 / col("servicios"), 1))
               .orderBy("window", desc("servicios")))

# 2) Top 10 acumulado de localidades
top_localidades = (eventos.groupBy("localidad").count()
                   .orderBy(desc("count")).limit(10))

q1 = (por_ventana.writeStream.outputMode("complete").format("console")
      .option("truncate", "false").option("numRows", 20)
      .trigger(processingTime="60 seconds").start())

q2 = (top_localidades.writeStream.outputMode("complete").format("console")
      .option("truncate", "false")
      .trigger(processingTime="60 seconds").start())

spark.streams.awaitAnyTermination()
