
import json
import csv
import ijson
import uuid
from pyspark.sql import SparkSession
import random
from decimal import Decimal
import ijson
import json
import uuid
import random
from decimal import Decimal

# Path to the input and output JSON files
spark = SparkSession.builder.appName("Music").getOrCreate()
df = spark.read.csv("output.csv", header=True)
print(df.show)