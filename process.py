#!/usr/bin/env python
# coding: utf-8

# In[123]:



from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import round, mean, min, max, col, floor, avg, explode


# In[124]:


spark = SparkSession.builder.appName("Music").getOrCreate()
df = spark.read.csv("output.csv", header=True)


# In[125]:


float_columns = ["Popularity", "Acousticness", "Energy", "Instrumentalness", 
                 "Liveness", "Loudness", "Speechiness", "Tempo", "Valence"]

int_columns = ["Mode", "Time Signature"]


# In[126]:


for col in float_columns:
    df = df.withColumn(col, df[col].cast(FloatType()))

# Chuyển đổi các cột integer
for col in int_columns:
    df = df.withColumn(col, df[col].cast(IntegerType()))
mean_value = df.select(mean("Popularity")).collect()[0][0]
df = df.fillna({"Popularity": mean_value})


# In[127]:


df = df.withColumn("Popularity", round(df["Popularity"]).cast(IntegerType()))
df = df.filter((df["Popularity"] >= 0) & (df["Popularity"] <= 100))

df.printSchema()


# In[128]:


from pyspark.sql.functions import when
df = df.withColumn("Popularity_Level", 
                   when(df["Popularity"] >= 80, "High")
                   .when(df["Popularity"] >= 50, "Medium")
                   .otherwise("Low"))
df.head(5)


# In[129]:


filtered_df = df.filter(
    (df["Energy"] >= 0) & (df["Energy"] <= 1) &
    (df["Acousticness"] >= 0) & (df["Acousticness"] <= 1) &
    (df["Valence"] >= 0) & (df["Valence"] <= 1)
)


# In[130]:


filtered_df.describe(["Popularity", "Acousticness", "Energy", "Liveness", "Tempo", "Valence"]).show()


# In[131]:


filtered_df.groupBy("Genres").count().orderBy("count", ascending=False).show()


# In[132]:


filtered_df.printSchema()


# In[133]:


filtered_df.groupBy("Artists").count().orderBy("count", ascending=False).show()


# In[134]:


import matplotlib.pyplot as plt
filtered_df.limit(1000).select("Popularity", "Energy").toPandas().plot.scatter(x="Energy", y="Popularity")
plt.title("Energy vs Popularity")
plt.xlabel("Energy")
plt.ylabel("Popularity")
plt.show()


# In[135]:


filtered_df.sample(fraction=0.1, seed=42).limit(1000).select("Valence", "Loudness").toPandas().plot.scatter(x="Valence", y="Loudness")

plt.title("Valence vs Loudness")
plt.xlabel("Valence")
plt.ylabel("Loudness")
plt.show()


# In[136]:


filtered_df.printSchema()


# In[137]:


binned_df = filtered_df.withColumn("Energy_Bin", (floor(col("Energy") * 10) / 10))

grouped_df = binned_df.groupBy("Energy_Bin").agg(avg("Popularity").alias("Avg_Popularity"))

# Hiển thị kết quả
grouped_df.orderBy("Energy_Bin").show()


# In[ ]:


grouped_pandas_df = grouped_df.orderBy("Energy_Bin").toPandas()

import matplotlib.pyplot as plt

plt.plot(grouped_pandas_df["Energy_Bin"], grouped_pandas_df["Avg_Popularity"], marker="o")
plt.title("Average Popularity by Energy Bins")
plt.xlabel("Energy Bins")
plt.ylabel("Average Popularity")
plt.show()


# In[51]:


top_songs = filtered_df.filter(col("Popularity") > 80)
top_songs.show()


# In[ ]:


from pyspark.sql.functions import when, col, regexp_replace, split, explode, size
import pandas as pd
# Step 1: Remove null or empty values in Genres
filtered_df = filtered_df.filter((col("Genres").isNotNull()) & (size(col("Genres")) > 0))
df_exploded = filtered_df.withColumn("Genre", explode(col("Genres")))
genre_counts_spark = df_exploded.groupBy("Genre").count().orderBy(col("count").desc())

genre_counts_spark_df = genre_counts_spark.toPandas()
print(genre_counts_spark_df)


# In[108]:


genre_popularity = (
    df_exploded.groupBy("Genre")
    .agg(avg("Popularity").alias("Average Popularity"))
    .orderBy(col("Average Popularity").desc())
)
genre_popularity_pd = genre_popularity.toPandas()
import matplotlib.pyplot as plt
plt.figure(figsize=(12, 6))
plt.bar(genre_popularity_pd["Genre"].head(20), genre_popularity_pd["Average Popularity"].head(20))
plt.title("Average Popularity by Genre")
plt.xlabel("Genre")
plt.ylabel("Average Popularity")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()



# In[152]:


from pyspark.sql.functions import regexp_replace, split, col, explode

filtered_df = filtered_df.withColumn(
    "Artists",
    split(
        regexp_replace(col("Artists"), r"[\[\]'\" ]+", ""), ","
    )
)
df_exploded = filtered_df.withColumn("Artist", explode(col("Artists")))


# In[156]:


artist_popularity = (
    df_exploded.groupBy("Artist")
    .agg({"Popularity": "avg"})
    .withColumnRenamed("avg(Popularity)", "Average Popularity")
    .orderBy(col("Average Popularity").desc())
)
artist_popularity.show()


# In[162]:


from pyspark.sql.functions import avg, count
df_exploded = filtered_df.withColumn("Artist", explode(col("Artists"))) \
                         .withColumn("Genre", explode(split(col("Genres"), ",")))

df_exploded.sample(fraction=0.1, seed=42).limit(1000).select("Artist", "Genre", "Popularity").show(truncate=False)


# In[163]:


artist_genre_popularity = (
    df_exploded.groupBy("Artist", "Genre")
    .agg(
        avg("Popularity").alias("Average Popularity"),
        count("*").alias("Track Count")
    )
    .orderBy(col("Average Popularity").desc())
)

# Show the results
artist_genre_popularity.show()


# In[171]:


artist_genre_popularity_pd = artist_genre_popularity.toPandas()

# Example: Filter for a specific artist
specific_artist = "ArianaGrande"
artist_genre_subset = artist_genre_popularity_pd[artist_genre_popularity_pd["Artist"] == specific_artist].head(20)

# Plot the popularity of genres for the specific artist
import matplotlib.pyplot as plt
plt.figure(figsize=(10, 6))
plt.bar(artist_genre_subset["Genre"], artist_genre_subset["Average Popularity"])
plt.title(f"Genres Influencing Popularity of {specific_artist}")
plt.xlabel("Genre")
plt.ylabel("Average Popularity")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# In[169]:


# Group by Genre to find the most influential genres overall
top_genres = (
    artist_genre_popularity.groupBy("Genre")
    .agg(
        avg("Average Popularity").alias("Overall Average Popularity"),
        count("*").alias("Artist Count")
    )
    .orderBy(col("Overall Average Popularity").desc())
)

# Convert to Pandas and visualize
top_genres_pd = top_genres.sample(fraction=0.1, seed=42).limit(20).toPandas()

# Plot top genres by average popularity
plt.figure(figsize=(10, 6))
plt.bar(top_genres_pd["Genre"], top_genres_pd["Overall Average Popularity"])
plt.title("Top Genres by Average Popularity Across All Artists")
plt.xlabel("Genre")
plt.ylabel("Average Popularity")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# In[172]:


genre_artist_popularity = (
    df_exploded.groupBy("Genre", "Artist")
    .agg(
        avg("Popularity").alias("Average Popularity"),
        count("*").alias("Track Count")
    )
    .orderBy("Genre", col("Average Popularity").desc())
)

# Show the top artists per genre
genre_artist_popularity.show()


# In[173]:


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("Genre").orderBy(col("Average Popularity").desc())
top_artists_per_genre = genre_artist_popularity.withColumn("Rank", row_number().over(window_spec))

# Filter for the top N artists per genre
top_artists_per_genre = top_artists_per_genre.filter(col("Rank") <= 3)

# Show the results
top_artists_per_genre.show()

