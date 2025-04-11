import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, dayofweek, when
from scipy.stats import ttest_ind

# Step 1: Load data into MongoDB

connection = MongoClient("mongodb://localhost:27017/")  # connecting to mongodb
data_base = connection["twitter_db"]  # Accesing the database in pycharm
collection_name = data_base["tweets"]  # Accessing  the collection
collection_name.drop()  # Emptying out the collection to make sure no duplicate
tweeter_csv = pd.read_csv("twitter_dataset.csv")  # Reading the CSV file
tweeter_csv["Timestamp"] = pd.to_datetime(
    tweeter_csv["Timestamp"], format="%d-%m-%y %H:%M"
    )  # timestamp conversion to datetime format
tweet_dic = tweeter_csv.to_dict(orient="records")  # Conersion of data frame into a list of dictionaries
collection_name.insert_many(tweet_dic)  # inserting the list of dictionaries into MongoDB
collection_name.create_index("Timestamp")  # creating an index of the timestamp for a better query
mongo_count = collection_name.count_documents({})  # counting the total number of data in our collection
if mongo_count != len(tweet_dic):  # Checking whether the number of documents matches
    raise Exception("Check 1 Failed: MongoDB record count doesn’t match inserted data!")
connection.close()  # Error message upon failure

# Step 2: Create SparkSession and suppress warnings
spark = SparkSession.builder \
    .appName("TwitterAnalysis") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/twitter_db.tweets") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/twitter_db.tweets") \
    .getOrCreate()  # creating a Spark session and configuring it to connect to MongoDB
spark.sparkContext.setLogLevel("ERROR")  # Error message upon failure

# Step 3: Load data into Spark
starting_time = time.time()  # getting the start time from analyses
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()  # loading data from MongoDb into Spark
row_count = df.count()  # counting the number of rows in spark
if row_count != mongo_count:  # checking whether the number of loaded data is the same as the number of documents in
    # mongo
    raise Exception("Check 3 Failed: Spark row count doesn’t match MongoDB!")  # Error message upon failure

# Step 4: Process data
# Creating a new column engagement by adding likes and retweets
# extracting the hour from the timestamp column
# Creating a new column DayOfWeek based on the day of the week from the timestamp column
# converting the days of the week from numeric to names of the day
df = df.withColumn(
    "Engagement", col("Likes") + col("Retweets")
    ) \
    .withColumn("Hour", hour("Timestamp")) \
    .withColumn(
    "DayOfWeek",
    when(dayofweek("Timestamp") == 1, "Sunday")
    .when(dayofweek("Timestamp") == 2, "Monday")
    .when(dayofweek("Timestamp") == 3, "Tuesday")
    .when(dayofweek("Timestamp") == 4, "Wednesday")
    .when(dayofweek("Timestamp") == 5, "Thursday")
    .when(dayofweek("Timestamp") == 6, "Friday")
    .when(dayofweek("Timestamp") == 7, "Saturday")
    .otherwise("Unknown")
    )
df = df.repartition(10, "Hour")  # Repartitioning the data by hour for better parallel processing
if "Engagement" not in df.columns or "Hour" not in df.columns or "DayOfWeek" not in df.columns:  # Verifying the
    # presence of essential columns
    raise Exception("Check 4 Failed: Missing Engagement, Hour, or DayOfWeek column!")  # Error message upon failure

# Step 5: Analyze engagement by hour and day
df.createOrReplaceTempView("tweets")  # Creating a temporary view of the DataFrame for SQL queries
# Executing a SQL query to get the average engagement per hour and day of the week
hour_day_engagement = spark.sql(
    """
        SELECT Hour, DayOfWeek, ROUND(AVG(Engagement), 2) AS Avg_Engagement
        FROM tweets
        GROUP BY Hour, DayOfWeek
        ORDER BY Hour, DayOfWeek
    """
    )

if hour_day_engagement.count() == 0:  # checking whether query has returned something
    raise Exception("Check 5 Failed: No results from analysis!")  # Error message upon failure

# checking the Distribution of data
print("\nTimestamp Distribution Check:")
print("Tweets per Hour:")
spark.sql("SELECT Hour, COUNT(*) as Tweet_Count FROM tweets GROUP BY Hour ORDER BY Hour").show(
    24
    )  # Showing the tweet count per hour
print("Tweets per Day:")
spark.sql("SELECT DayOfWeek, COUNT(*) as Tweet_Count FROM tweets GROUP BY DayOfWeek ORDER BY DayOfWeek").show(
    7
    )  # Showing the tweet count per day

# Step 6: Identify best and worst times
print("\nStep 6: Best and worst times for engagement:")
# getting the stats based on hours
hour_stats = spark.sql(
    """
        SELECT Hour, ROUND(AVG(Engagement), 2) AS Avg_Engagement
        FROM tweets
        GROUP BY Hour
    """
    )
best_hour = hour_stats.orderBy(hour_stats["Avg_Engagement"].desc()).limit(1)
worst_hour = hour_stats.orderBy(hour_stats["Avg_Engagement"]).limit(1)
print("Best hour:", best_hour.collect()[0]["Hour"], f"({best_hour.collect()[0]['Avg_Engagement']})")
print("Worst hour:", worst_hour.collect()[0]["Hour"], f"({worst_hour.collect()[0]['Avg_Engagement']})")
# getting the stats based on the day of the week
day_stats = spark.sql(
    """
        SELECT DayOfWeek, ROUND(AVG(Engagement), 2) AS Avg_Engagement
        FROM tweets
        GROUP BY DayOfWeek
    """
    )
best_day = day_stats.orderBy(day_stats["Avg_Engagement"].desc()).limit(1)
worst_day = day_stats.orderBy(day_stats["Avg_Engagement"]).limit(1)
print("Best day:", best_day.collect()[0]["DayOfWeek"], f"({best_day.collect()[0]['Avg_Engagement']})")
print("Worst day:", worst_day.collect()[0]["DayOfWeek"], f"({worst_day.collect()[0]['Avg_Engagement']})")
#getting the stats based on the hour-day of the week engagement
hour_day_stats = spark.sql(
    """
        SELECT Hour, DayOfWeek, ROUND(AVG(Engagement), 2) AS Avg_Engagement
        FROM tweets
        GROUP BY Hour, DayOfWeek
    """
    )
best_hour_day = hour_day_stats.orderBy(hour_day_stats["Avg_Engagement"].desc()).limit(1)
worst_hour_day = hour_day_stats.orderBy(hour_day_stats["Avg_Engagement"]).limit(1)
print(
    "Best hour-day:",
    f"{best_hour_day.collect()[0]['Hour']} on {best_hour_day.collect()[0]['DayOfWeek']} ("
    f"{best_hour_day.collect()[0]['Avg_Engagement']})"
    )
print(
    "Worst hour-day:",
    f"{worst_hour_day.collect()[0]['Hour']} on {worst_hour_day.collect()[0]['DayOfWeek']} ("
    f"{worst_hour_day.collect()[0]['Avg_Engagement']})"
    )

if best_hour.count() == 0 or best_day.count() == 0 or best_hour_day.count() == 0:
    raise Exception("Check 6 Failed: No results for best/worst times!")

# Step 7: Time significance with dynamic T-test, effect size, and practical analysis
best_hour_row = hour_day_engagement.orderBy(col("Avg_Engagement").desc()).first()# Getting the best hour
worst_hour_row = hour_day_engagement.orderBy("Avg_Engagement").first()# Getting the worst hour
best_hour = best_hour_row["Hour"] # extraction of the best hour
worst_hour = worst_hour_row["Hour"]# extraction of the worst hour
best_hour_avg = best_hour_row["Avg_Engagement"]# Average engagement for best hour
worst_hour_avg = worst_hour_row["Avg_Engagement"]# Average engagement for worst hour
# Count the number of tweets in the best and worst hours
best_hour_count = spark.sql(f"SELECT COUNT(*) AS count FROM tweets WHERE Hour = {best_hour}").collect()[0]["count"]
worst_hour_count = spark.sql(f"SELECT COUNT(*) AS count FROM tweets WHERE Hour = {worst_hour}").collect()[0]["count"]
print(f"Best Hour ({best_hour}) has {best_hour_count} tweets, Worst Hour ({worst_hour}) has {worst_hour_count} tweets")
if best_hour_count < 30 or worst_hour_count < 30: #checking whether data is reliable
    print("Warning: One or both hours have fewer than 30 tweets, T-test may be unreliable.")
# Extract engagement data for T-test
best_hour_data = spark.sql(f"SELECT Engagement FROM tweets WHERE Hour = {best_hour}").toPandas()["Engagement"]
worst_hour_data = spark.sql(f"SELECT Engagement FROM tweets WHERE Hour = {worst_hour}").toPandas()["Engagement"]
# Perform T-test to compare the engagement between best and worst hour
t_stat, p_value = ttest_ind(best_hour_data, worst_hour_data)
# Calculate Cohen's d for effect size
mean_diff = np.mean(best_hour_data) - np.mean(worst_hour_data) #difference in mean
pooled_std = np.sqrt((np.std(best_hour_data, ddof=1) ** 2 + np.std(worst_hour_data, ddof=1) ** 2) / 2)
cohen_d = mean_diff / pooled_std
# Practical significance metrics
overall_mean = spark.sql("SELECT ROUND(AVG(Engagement), 2) AS Mean_Engagement FROM tweets").collect()[0][
    "Mean_Engagement"]
hour_range = best_hour_avg - worst_hour_avg #difference between best and worst day
relative_range = (hour_range / overall_mean) * 100  # Percentage relative to mean
# Classify practical significance

# Classify practical significance based on the relative range
range_classification = "small" if relative_range < 10 else "moderate" if relative_range < 20 else "large"
# Classify effect size based on Cohen's d
cohen_classification = "small" if cohen_d < 0.2 else "medium" if cohen_d < 0.5 else "large" if cohen_d < 0.8 else \
    "very large"
print(f"Best Hour: {best_hour} ({best_hour_avg}), Worst Hour: {worst_hour} ({worst_hour_avg})")
print(f"T-test (Best vs Worst Hour): t-stat = {t_stat:.2f}, p-value = {p_value:.4f}")
print(f"Effect size (Cohen's d): {cohen_d:.2f} ({cohen_classification} effect)")
print(f"Overall mean engagement: {overall_mean}")
print(
    f"Hourly engagement range: {round(hour_range, 2)} ({relative_range:.1f}% of mean, {range_classification} "
    f"difference)"
    )
if p_value <= 0.05:
    print("Significant difference detected between best and worst hour (p ≤ 0.05).")
    print(
        f"Practical difference is {range_classification} (range: {hour_range}, {relative_range:.1f}% of mean) and "
        f"effect size is {cohen_classification} (Cohen's d: {cohen_d:.2f})."
        )
    print(
        "Conclusion: Time has a statistically significant affect on the engagement but the practicality must be "
        "checked with visuals as well "
        )
else:
    print("No significant difference between best and worst hour (p > 0.05).")
    print(
        "Conclusion: Time does not significantly affect engagement statically, the practicality must be checked with "
        "other factor ."
        )
# Step 8: Visualizing best/worst times, hourly trends, and hour-day heatmap with subplots
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 18), height_ratios=[1, 1, 1.5])
plt.suptitle("Twitter Engagement Analysis", fontsize=16)

# Subplot 1: Bar chart for best/worst times
best_hour = hour_day_engagement.orderBy(col("Avg_Engagement").desc()).first()
worst_hour = hour_day_engagement.orderBy("Avg_Engagement").first()
best_day = spark.sql(
    "SELECT DayOfWeek, ROUND(AVG(Engagement), 2) AS Avg_Engagement FROM tweets GROUP BY DayOfWeek ORDER BY "
    "Avg_Engagement DESC"
    ).first()
worst_day = spark.sql(
    "SELECT DayOfWeek, ROUND(AVG(Engagement), 2) AS Avg_Engagement FROM tweets GROUP BY DayOfWeek ORDER BY "
    "Avg_Engagement"
    ).first()
best_hour_day = hour_day_engagement.orderBy(col("Avg_Engagement").desc()).first()
worst_hour_day = hour_day_engagement.orderBy("Avg_Engagement").first()
labels = ['Best Hour', 'Worst Hour', 'Best Day', 'Worst Day', 'Best Hour-Day', 'Worst Hour-Day']
values = [best_hour["Avg_Engagement"], worst_hour["Avg_Engagement"],
          best_day["Avg_Engagement"], worst_day["Avg_Engagement"],
          best_hour_day["Avg_Engagement"], worst_hour_day["Avg_Engagement"]]
ax1.bar(labels, values, color=['green', 'red', 'green', 'red', 'green', 'red'])
ax1.set_title("Best and Worst Times for Engagement")
ax1.set_ylabel("Avg Engagement")
ax1.tick_params(axis='x', rotation=45)

# Subplot 2: Line plot for hourly engagement trends
hour_stats = spark.sql(
    """
        SELECT Hour, ROUND(AVG(Engagement), 2) AS Avg_Engagement
        FROM tweets
        GROUP BY Hour
        ORDER BY Hour
    """
    ).toPandas()
ax2.plot(hour_stats["Hour"], hour_stats["Avg_Engagement"], marker='o', color='blue')
ax2.set_title("Hourly Engagement Trends")
ax2.set_xlabel("Hour of Day")
ax2.set_ylabel("Avg Engagement")
ax2.grid(True)

# Subplot 3: Heatmap for hour-day engagement
hour_day_stats = hour_day_engagement.toPandas()
pivot_table = hour_day_stats.pivot(index="Hour", columns="DayOfWeek", values="Avg_Engagement")
# Ensure consistent day order
day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
pivot_table = pivot_table.reindex(columns=day_order)
sns.heatmap(pivot_table, annot=True, cmap="YlGnBu", fmt=".2f", ax=ax3)
ax3.set_title("Engagement Heatmap by Hour and Day")
ax3.set_xlabel("Day of Week")
ax3.set_ylabel("Hour")

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.show()
project_time = time.time() - starting_time
print(f"project took {project_time:.2f} seconds")
# Final cleanup
spark.stop()
