from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

# Initialiser la SparkSession
spark = SparkSession.builder \
    .appName("LogAnalysisStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Connexion au flux Socket du service 'data-generator'
lines = spark.readStream \
    .format("socket") \
    .option("host", "data-generator") \
    .option("port", 9998) \
    .load()

# 2. Parsing des logs avec Regex (format : IP--[Date] "Method URL Protocol" Status Size)
# On extrait l'IP, l'URL, le Code HTTP et la Taille 
logs_df = lines.select(
    regexp_extract(col("value"), r'^([^--]+)', 1).alias("ip_address"),
    regexp_extract(col("value"), r'\"(\S+)\s(\S+)\s*.*\"', 2).alias("url"),
    regexp_extract(col("value"), r'\s(\d{3})\s', 1).cast("int").alias("http_code"),
    regexp_extract(col("value"), r'\s(\d+)$', 1).cast("int").alias("response_size")
)

# --- ANALYSE STREAM 1 : Détection des erreurs en temps réel [cite: 55, 59] ---
# On filtre pour ne garder que les codes 404 ou 500
error_counts = logs_df.filter(col("http_code").isin(404, 500)) \
    .groupBy("http_code") \
    .count()

# --- ANALYSE STREAM 2 : Surveillance de l'activité par IP [cite: 62, 64] ---
# On compte les requêtes par IP pour détecter des anomalies
ip_activity = logs_df.groupBy("ip_address").count().sort(col("count").desc())

# 3. Affichage dans la console (Mode 'complete' pour voir l'agrégation totale)
query_errors = error_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query_ips = ip_activity.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Attendre la fin du flux
query_errors.awaitTermination()
query_ips.awaitTermination()