from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.storagelevel import StorageLevel
# Path to the input and output JSON files
spark = SparkSession.builder.appName("Music") \
    .getOrCreate()
df = spark.read.csv("output.csv", header=True)
df = df.dropna()

#-------------------
columns_to_cast = ["Acousticness", "Energy", "Liveness", "Loudness", "Tempo", "Popularity"]
for col_name in columns_to_cast:
    df = df.withColumn(col_name, col(col_name).cast("double"))

#-------------------
indexer = StringIndexer(inputCol="Explicit", outputCol="ExplicitIndex")
df = indexer.fit(df).transform(df)
assembler = VectorAssembler(
    inputCols=["Acousticness", "Energy", "Liveness", "Loudness", "Tempo", "ExplicitIndex"],
    outputCol="features",
    handleInvalid="skip"
)
df = assembler.transform(df)

#------------------
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
df = scaler.fit(df).transform(df)

#------------------
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

#------------------
kmeans = KMeans(featuresCol="scaledFeatures", k=3)
kmeans_model = kmeans.fit(df)
predictions = kmeans_model.transform(df)

#------------------

evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)
print(f"Silhouette with squared euclidean distance: {silhouette}")