from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, regexp_replace, concat_ws

# spark = SparkSession.builder \
#     .appName("HDFS Example") \
#     .master("spark://spark-master:7077") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.executor.cores", "2") \
#     .getOrCreate()

# data = spark.read.csv("hdfs://namenode:9000/user/root/output.csv", header=True, inferSchema=True)

spark = SparkSession.builder.appName("Music").getOrCreate()

data = spark.read.csv("output.csv", header=True)

data.show(5)

# ------------------------------------Processing--------------------------------------

# geo_data = spark.read.csv("hdfs://namenode:9000/user/root/geo.csv", header=True, inferSchema=True)

geo_data = spark.read.csv("geo.csv", header=True)

geo_data.show(5)

data = data.withColumn("Genres", split(col("Genres"), ", ")) \
           .withColumn("AvailableMarkets", split(col("AvailableMarkets"), ", "))

exploded_genres = data.select(
    col("TrackID"),
    explode(col("Genres")).alias("Genre"),
    col("AvailableMarkets")
)

exploded_data = exploded_genres.select(
    col("TrackID"),
    regexp_replace(col("Genre"), "[\\[\\]']", "").alias("Genre"),
    explode(col("AvailableMarkets")).alias("Market")
)

result = exploded_data.groupBy("Genre", "Market").agg(count("*").alias("Count"))

final_result = result.join(
    geo_data,
    result["Market"] == geo_data["Country Code"],
    "left"
).select(
    col("Genre"),
    col("Market"),
    col("Count"),
    col("Latitude"),
    col("Longitude")
)

final_result = final_result.filter(
    (col("Latitude").isNotNull()) & 
    (col("Longitude").isNotNull()) & 
    (col("Latitude") != "") & 
    (col("Longitude") != "")
)

final_result = final_result.withColumn(
    "location",
    concat_ws(",", col("Latitude"), col("Longitude"))  # Combine Latitude and Longitude
)

final_result = final_result.filter(col("location") != "")

final_result.show(20, truncate=False)

pandas_df = final_result.toPandas()

output_file = "location.csv"  # Specify your file name and path
pandas_df.to_csv(output_file, index=False)

print(f"CSV file has been saved to {output_file}")


# es_index_name = "map1"  # Name of the Elasticsearch index

# final_result.write \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.nodes", "elasticsearch") \
#     .option("es.port", "9200") \
#     .option("es.nodes.wan.only", "true") \
#     .option("es.resource", "map1/_doc") \
#     .option("es.batch.size.entries", "1000") \
#     .mode("append") \
#     .save(es_index_name)

# print(f"Data successfully pushed to Elasticsearch index '{es_index_name}'.")

# -------------------------------------------------------------------------------------
# push data to elasticsearch

# data.write \
#     .format("org.elasticsearch.spark.sql") \
#     .option("es.nodes", "elasticsearch") \
#     .option("es.port", "9200") \
#     .option("es.nodes.wan.only", "true") \
#     .option("es.batch.size.entries", "1000") \
#     .mode("append") \
#     .save("test")

# print("Data successfully pushed to Elasticsearch index 'test'.")

spark.stop()