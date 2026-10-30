# Tarea 3 – Big Data (UNAD): Servicios de emergencia sin recepción – Línea 123 de Bogotá

Análisis batch con PySpark y simulación en tiempo real con Kafka + Spark Structured Streaming.

## Problema
Servicios de emergencia en salud de la línea 123 de Bogotá que no registran recepción del paciente (`RECEPCION` vacía), usada como indicador aproximado de servicios que terminan sin traslado. El dataset no tiene una columna de traslado ni distingue tipos de ambulancia.

## Datos
Llamadas de Urgencias y Emergencias – Línea 123 (Datos Abiertos Bogotá), julio de 2025, 9 867 registros.
https://datosabiertos.bogota.gov.co/dataset/llamadas-de-urgencias-y-emergencias-que-ingresan-a-traves-de-la-linea-123

## Requisitos
Ubuntu 22.04, Java 11, Spark 3.5.8, Python 3, Kafka 3.9.2
`pip install pandas matplotlib kafka-python`

## Ejecución batch
```bash
wget -O llamadas123_julio2025.csv "https://datosabiertos.bogota.gov.co/dataset/b99881e0-8750-4fa8-af6c-3f30c9e38708/resource/fa221f24-593a-4d3c-b697-e2f42ccbb314/download/llamadas123_julio2025.csv"
iconv -f CP850 -t UTF-8 llamadas123_julio2025.csv > llamadas123_utf8.csv
spark-submit analisis_batch.py llamadas123_utf8.csv
```
Los resultados (CSV y gráficos) quedan en `resultados_batch/`.

## Ejecución en tiempo real
```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic llamadas_123
python3 kafka_producer_123.py    # terminal 1
spark-submit --conf spark.sql.shuffle.partitions=4 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer_123.py    # terminal 2
```
Los datos del productor son simulados, con probabilidades calibradas con el análisis batch.

## Videos
- Video 1 (batch): https://youtu.be/gZ2TRWLWk4o
- Video 2 (Kafka y Streaming): https://youtu.be/gZ2TRWLWk4o
