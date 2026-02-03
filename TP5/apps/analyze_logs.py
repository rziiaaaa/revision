from pyspark import SparkContext

# Initialize Spark Context
sc = SparkContext(appName="AnalyseLogsApacheRDD")

# --- AJOUT ICI ---
# On demande à Spark de ne parler que s'il y a une vraie erreur critique
sc.setLogLevel("ERROR")
# -----------------

# Load the log file
log_file = "hdfs://hdfs-namenode:9000/data/web_server.log"
logs_rdd = sc.textFile(log_file)

# afficher les 10 premières lignes du fichier de log
print("Les 10 premières lignes du fichier de log sont :")
for line in logs_rdd.take(10):
    print(f"RQZIIAA : {line}")


import re
# Définir le pattern regex pour parser les logs
log_pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[(.*?)\] "(.*?) (.*?) HTTP.*" (\d+) (\d+)'
# Fonction pour parser une ligne de log
def parse_log_line(line):
    match = re.match(log_pattern, line)
    if match:
        return (
            match.group(1), # IP
            match.group(2), # Timestamp
            match.group(3), # HTTP Method
            match.group(4), # URL
            int(match.group(5)), # HTTP Status
            int(match.group(6)) # Response Size
        )
    else:
        return None
    
# Parser les logs
parsed_logs_rdd = logs_rdd.map(parse_log_line).filter(lambda x: x is not None)
# Afficher 10 exemples
print("Exemple de logs parsés :")
for log in parsed_logs_rdd.take(10):
    print(f"AWYA : {log}")

# Nombre total de requêtes
total_requests = parsed_logs_rdd.count()
print(f"Nombre total de requêtes : {total_requests}")

# Top 5 des URLs les plus demandées
top_urls = parsed_logs_rdd.map(lambda x: (x[3], 1)).reduceByKey(lambda a, b: a + b).takeOrdered(5, key=lambda x: -x[1])
print("Les 5 URLs les plus demandées :")
for url, count in top_urls:
    print(f"{url}: {count}")