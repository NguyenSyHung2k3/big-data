from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, regexp_replace, concat_ws

spark = SparkSession.builder \
    .appName("HDFS Example") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

data = spark.read.csv("hdfs://namenode:9000/user/root/location.csv", header=True, inferSchema=True)

data.show(5)

es_index_name = "geo_genre"  # Name of the Elasticsearch index

data.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "true") \
    .option("es.resource", "geo_genre/_doc") \
    .option("es.batch.size.entries", "1000") \
    .mode("append") \
    .save(es_index_name)

print(f"Data successfully pushed to Elasticsearch index '{es_index_name}'.")

spark.stop()