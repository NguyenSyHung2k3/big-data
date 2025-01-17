from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from xgboost.spark import SparkXGBRegressor  # Nhập XGBoostRegressor
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import isnan, when, count

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("Music") \
    .getOrCreate()

# Đọc dữ liệu từ CSV
df = spark.read.csv("output.csv", header=True, sep=';', inferSchema=True)

# Hiển thị DataFrame
df.show(truncate=False)

# Bước 1: Khởi tạo Spark session
spark = SparkSession.builder.appName("Popularity").getOrCreate()

# Bước 2: Tạo DataFrame từ file CSV
df = spark.read.csv("output.csv", header=True, sep=';', inferSchema=True)

# Bước 3: Chuyển đổi cột 'Popularity' thành kiểu IntegerType
df = df.withColumn("Popularity", df["Popularity"].cast(IntegerType()))

# Bước 4: Kiểm tra và xử lý giá trị null hoặc NaN trong cột 'Popularity'
df.select([count(when(isnan(c), c)).alias(c) for c in ["Popularity"]]).show()
df = df.na.drop(subset=["Popularity"])  # Loại bỏ các hàng có giá trị null hoặc NaN

# Bước 5: Tiền xử lý dữ liệu
# Chuyển đổi các cột danh mục thành số
indexer_artists = StringIndexer(inputCol="Artists", outputCol="Artists_Index", handleInvalid="keep")
indexer_genres = StringIndexer(inputCol="Genres", outputCol="Genres_Index", handleInvalid="keep")

# Tạo VectorAssembler để kết hợp các đặc trưng
assembler = VectorAssembler(inputCols=["Artists_Index", "Genres_Index", "Acousticness", "Energy",
                                        "Instrumentalness", "Liveness", "Loudness",
                                        "Mode", "Speechiness", "Tempo", "TimeSignature", "Valence"],
                            outputCol="features")

# Bước 6: Chuẩn hóa các đặc trưng số
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=True)

# Bước 7: Sử dụng XGBoostRegressor
xgboost_reg = SparkXGBRegressor(features_col="scaled_features", label_col="Popularity")

# Bước 8: Tạo Pipeline bao gồm tất cả các bước
pipeline = Pipeline(stages=[indexer_artists, indexer_genres, assembler, scaler, xgboost_reg])

# Bước 9: Chia dữ liệu thành tập huấn luyện và tập kiểm tra
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# Bước 10: Tuning hyperparameters với CrossValidator
paramGrid = ParamGridBuilder() \
    .addGrid(xgboost_reg.max_depth, [3, 6, 10]) \
    .addGrid(xgboost_reg.learning_rate, [0.1, 0.01, 0.001]) \
    .addGrid(xgboost_reg.reg_lambda, [1, 10]) \
    .build()

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=RegressionEvaluator(labelCol="Popularity", predictionCol="prediction", metricName="mae"),
                          numFolds=3)  # 3-fold CrossValidation

# Bước 11: Huấn luyện mô hình
cv_model = crossval.fit(train_df)

# Bước 12: Dự đoán trên tập kiểm tra
predictions = cv_model.transform(test_df)

# Bước 13: Tính toán các metric lỗi
evaluator = RegressionEvaluator(labelCol="Popularity", predictionCol="prediction")
mae = evaluator.evaluate(predictions)
mse = evaluator.setMetricName("mse").evaluate(predictions)
r2 = evaluator.setMetricName("r2").evaluate(predictions)

# Bước 14: Hiển thị kết quả dự đoán
predictions.select("TrackName", "Artists", "Genres", "Popularity", "prediction").show()

# Bước 15: In các metric lỗi
print(f"Mean Absolute Error (MAE): {mae}")
print(f"Mean Squared Error (MSE): {mse}")
print(f"R-squared: {r2}")

# Dừng Spark session
spark.stop()