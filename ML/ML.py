from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Path to the input and output JSON files
spark = SparkSession.builder.appName("Music").config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

df = spark.read.csv("output.csv", header=True)
df = df.repartition(4)

#-----dropna-------
df_dropna = df.dropna()
# print(df.head(5))
#--------
columns_to_cast = ["Acousticness", "Energy", "Liveness", "Loudness", "Tempo", "Popularity"]
for col_name in columns_to_cast:
    df = df.withColumn(col_name, col(col_name).cast("double"))
#--------
indexer = StringIndexer(inputCol="Explicit", outputCol="ExplicitIndex")
df = indexer.fit(df).transform(df)
# print(df['ExplicitIndex'].head(5))
#----------------------------
df = df.na.drop(subset=["Acousticness", "Energy", "Liveness", "Loudness", "Tempo", "ExplicitIndex", "Popularity"])

#----assemble features------
assembler = VectorAssembler(
    inputCols=["Acousticness", "Energy", "Liveness", "Loudness", "Tempo", "ExplicitIndex"],
    outputCol="features",
    handleInvalid="skip"  
)
df = assembler.transform(df)

#-----
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
df = scaler.fit(df).transform(df)
#-----
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
#-----
# lr = LinearRegression(featuresCol="scaledFeatures", labelCol="Popularity")

# lr_model = lr.fit(train_data)

# predictions = lr_model.transform(test_data)

# evaluator = RegressionEvaluator(labelCol="Popularity", predictionCol="prediction", metricName="rmse")
# rmse = evaluator.evaluate(predictions)
# print(f"Root Mean Squared Error (RMSE): {rmse}")
#-----
# kmeans = KMeans(featuresCol="scaledFeatures", k=3)
# kmeans_model = kmeans.fit(df)

# predictions = kmeans_model.transform(df)
# evaluator = ClusteringEvaluator()
# silhouette = evaluator.evaluate(predictions)
# print(f"Silhouette with squared euclidean distance: {silhouette}")
#--------------------------
# from pyspark.ml.recommendation import ALS
# from pyspark.sql.functions import monotonically_increasing_id
# df = df.withColumn("User_ID", (monotonically_increasing_id() % 10).cast("int"))
# # ALS Model
# df['User_ID'].head(5)
#--------------------------
# from pyspark.ml.feature import Normalizer
# from pyspark.ml.linalg import DenseVector
# from pyspark.sql.functions import udf, col

# # Normalize the features
# normalizer = Normalizer(inputCol="scaledFeatures", outputCol="normFeatures")
# df = normalizer.transform(df)

# # Select a specific track ID to compare with
# specific_track_id = df.select("Track ID").first()[0]
# song_1_row = df.filter(col("Track ID") == specific_track_id).select("normFeatures").first()

# if song_1_row is None:
#     print(f"Track ID '{specific_track_id}' not found in the dataset.")
# else:
#     # Extract the feature vector of the specific track
#     song_1_vector = song_1_row[0]

#     # Define a UDF to calculate the dot product
#     def compute_similarity(vec):
#         if vec is None:
#             return 0.0
#         print(f"Processing vector: {vec}")
#         return float(vec.dot(song_1_vector))

#     similarity_udf = udf(compute_similarity, "double")

#     # Apply the UDF to calculate similarity for all tracks
#     df = df.withColumn("similarity", similarity_udf(col("normFeatures")))

#     # Get the top 10 recommendations
#     recommendations = df.orderBy(col("similarity").desc()).limit(10)

#     # Display the recommendations
#     recommendations.select("Track ID", "Track Name", "similarity").show(truncate=False)
#---------------from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors

# Scale features
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors
import numpy as np
from pyspark.sql.functions import lit
# Drop the existing scaledFeatures column if it exists
if "scaledFeatures" in df.columns:
    df = df.drop("scaledFeatures")

# Scale features
scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
df = df.cache()

df = scaler.fit(df).transform(df)
reference_row = df.filter(col("TrackID") == "7EhJMeBeptBfJh8kkjZXjw").select("scaledFeatures").first()

specific_track_id = df.select("TrackID").first()[0]

# Compute distances manually or use clustering
from pyspark.sql.functions import udf
from pyspark.ml.linalg import DenseVector
if reference_row is None:
    print("Reference track not found.")
else:
    reference_vector = reference_row[0].toArray()
    broadcast_reference = spark.sparkContext.broadcast(reference_vector)
    # Define the UDF for Euclidean distance
    @udf("double")
    def euclidean_distance(vec1, vec2):
        if vec1 is None or vec2 is None:
            return float("inf")  # Handle missing vectors gracefully
        # Convert both to NumPy arrays
        vec1_array = np.array(vec1.toArray())
        vec2_array = np.array(vec2)
        return float(np.linalg.norm(vec1_array - vec2_array))

        # Add the distance column
    df = df.withColumn("distance", euclidean_distance(col("scaledFeatures"), lit(reference_vector)))
    recommendations = df.orderBy("distance").limit(10)
    recommendations.select("Track ID", "TrackName", "distance").show(truncate=False)