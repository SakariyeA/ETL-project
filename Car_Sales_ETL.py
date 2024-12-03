# Databricks notebook source
# MAGIC %md
# MAGIC Load the data set, read it into spark dataframe, check and confirm column types, remove duplicates, add new column, filter and save the cleaned and transformed data.
# MAGIC

# COMMAND ----------

# Load the car sales dataset from its location in DBFS
file_path = "dbfs:/user/hive/warehouse/car_info"

# Read the Delta table into a Spark DataFrame
df = spark.read.format("delta").load(file_path)

# Display the first few rows to inspect the data
display(df)

# Check the schema to confirm column types
df.printSchema()


# 1. Remove duplicates based on VIN (Vehicle Identification Number)
df_cleaned = df.dropDuplicates(["vin"])

# 2. Add a new column for "car_age" calculated from the current year and the 'year' column
from pyspark.sql.functions import col, lit
current_year = 2024  # Replace with the current year dynamically if needed
df_cleaned = df_cleaned.withColumn("car_age", lit(current_year) - col("year"))

# 3. Filter out rows with missing or invalid selling prices
df_filtered = df_cleaned.filter(col("sellingprice").isNotNull() & (col("sellingprice") > 0))

# Display the transformed DataFrame
display(df_filtered)

# Save the cleaned and transformed data back to a location for further analysis
output_path = "dbfs:/user/hive/warehouse/car_sales_cleaned"
df_filtered.write.mode("overwrite").parquet(output_path)

print(f"Transformed data saved to: {output_path}")

# COMMAND ----------

# Save the cleaned and transformed DataFrame as a Delta table
table_name = "car_sales_cleaned"

df_filtered.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Transformed data saved as table: {table_name}")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM car_sales_cleaned;
# MAGIC

# COMMAND ----------

# List of columns to check for NULL values
columns_to_check = ["make", "model", "trim", "body", "transmission"]

# Drop rows where any of the specified columns have NULL values
df_non_null = df_filtered.dropna(subset=columns_to_check)

# Display the DataFrame after dropping rows with NULLs
df_non_null.show(truncate=False)

# Overwrite the previous transformed data table
table_name = "car_sales_cleaned"

df_non_null.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Rows with NULL values in {columns_to_check} removed. Table updated: {table_name}")


# COMMAND ----------

#verify the results
print(f"Rows before cleanup: {df_filtered.count()}")
print(f"Rows after cleanup: {df_non_null.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM car_sales_cleaned;

# COMMAND ----------

# MAGIC %md
# MAGIC Total Sales By State

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE default.total_sales_by_state
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT state, COUNT(*) AS total_sales
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY state
# MAGIC ORDER BY total_sales DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Average Price By Make

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.avg_price_by_make AS
# MAGIC SELECT make, AVG(sellingprice) AS avg_selling_price
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY make
# MAGIC ORDER BY avg_selling_price DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Most Sold Car Models

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.top_10_models AS
# MAGIC SELECT model, COUNT(*) AS sales_count
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY model
# MAGIC ORDER BY sales_count DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Number Of Cars Sold Per Year

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.cars_sold_per_year AS
# MAGIC SELECT year, COUNT(*) AS cars_sold
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY year
# MAGIC ORDER BY year;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Distributions Of Cars by Body Type

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.body_type_distribution AS
# MAGIC SELECT body, COUNT(*) AS total_count
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY body
# MAGIC ORDER BY total_count DESC;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Condition-Wise Average Selling Price

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.condition_avg_price AS
# MAGIC SELECT condition, AVG(sellingprice) AS avg_price
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY condition
# MAGIC ORDER BY condition;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC States with the Highest Average Market Value 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.highest_avg_market_value_by_state AS
# MAGIC SELECT state, AVG(est_market_value) AS avg_market_value
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY state
# MAGIC ORDER BY avg_market_value DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Colors of Cars Sold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.top_10_colors AS
# MAGIC SELECT color, COUNT(*) AS total_sold
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY color
# MAGIC ORDER BY total_sold DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Transmission type Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.transmission_distribution AS
# MAGIC SELECT transmission, COUNT(*) AS total_count
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY transmission
# MAGIC ORDER BY total_count DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Cars with Selling Price above Market Value,

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.above_market_value_sales AS
# MAGIC SELECT *
# MAGIC FROM car_sales_cleaned
# MAGIC WHERE sellingprice > est_market_value;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Sellers with the most Listings

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.top_10_sellers AS
# MAGIC SELECT seller, COUNT(*) AS listings
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY seller
# MAGIC ORDER BY listings DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Cars with Odometer readings above 100,000 miles

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.high_odometer_cars AS
# MAGIC SELECT *
# MAGIC FROM car_sales_cleaned
# MAGIC WHERE odometer > 100000;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Top 5 Most Popular Interior Colors 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.popular_interior_colors AS
# MAGIC SELECT interior, COUNT(*) AS count
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY interior
# MAGIC ORDER BY count DESC
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Average Selling Price By Year 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.avg_price_by_year AS
# MAGIC SELECT year, AVG(sellingprice) AS avg_selling_price
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY year
# MAGIC ORDER BY year;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Percentage of Cars Sold by Condition

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.condition_percentage AS
# MAGIC SELECT condition, 
# MAGIC        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY condition
# MAGIC ORDER BY percentage DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Proportion of Automatic vs. Manual Transmission Cars

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.transmission_proportion AS
# MAGIC SELECT transmission, 
# MAGIC        COUNT(*) AS count,
# MAGIC        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY transmission
# MAGIC ORDER BY percentage DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Cars with the Largest Price Difference (Selling Price vs. Market Value)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.largest_price_diff AS
# MAGIC SELECT vin, make, model, year, est_market_value, sellingprice, 
# MAGIC        sellingprice - est_market_value AS price_difference
# MAGIC FROM car_sales_cleaned
# MAGIC ORDER BY ABS(price_difference) DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Most Expensive Cars Sold by Make and Model

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.most_expensive_cars AS
# MAGIC SELECT make, model, MAX(sellingprice) AS max_selling_price
# MAGIC FROM car_sales_cleaned
# MAGIC GROUP BY make, model
# MAGIC ORDER BY max_selling_price DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Correlation Between Car Age and Selling Price

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.age_price_correlation AS
# MAGIC SELECT (2024 - year) AS car_age, AVG(sellingprice) AS avg_selling_price
# MAGIC FROM car_sales_cleaned
# MAGIC WHERE year IS NOT NULL
# MAGIC GROUP BY (2024 - year)
# MAGIC ORDER BY car_age;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Delete Null Values In condition_avg_price

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM condition_avg_price
# MAGIC WHERE condition IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC  delete null values in condition_percentage

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM condition_percentage
# MAGIC WHERE condition IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC Delete values in total_sales_by_state where state abbreviation is larger than 2 characters

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM total_sales_by_state
# MAGIC WHERE LENGTH(state) > 2;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC in transmission_distribution and _proportion delete the value where transmission is equal to 'Sedan'

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM transmission_distribution
# MAGIC WHERE transmission == 'Sedan';

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM transmission_proportion
# MAGIC WHERE transmission == 'Sedan';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM top_10_colors

# COMMAND ----------

# MAGIC %md
# MAGIC Replace the - value in color to turqoise

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE top_10_colors
# MAGIC SET color = REPLACE(color, '—', 'turqoise');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM top_10_colors

# COMMAND ----------

# MAGIC %md
# MAGIC Replace the - value in Interior to Red

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE popular_interior_colors
# MAGIC SET interior = REPLACE(interior, '—', 'red');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM popular_interior_colors

# COMMAND ----------

dbutils.notebook.exit("")
