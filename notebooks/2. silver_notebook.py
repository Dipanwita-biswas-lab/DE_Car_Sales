# Databricks notebook source
# MAGIC %md
# MAGIC # Read Data
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import split, col, sum, concat, lit, to_date, lpad

# COMMAND ----------

df= spark.read.format("parquet")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@sadesales.dfs.core.windows.net/")

# COMMAND ----------

# df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Divide the Model column to category and ID
# MAGIC
# MAGIC

# COMMAND ----------

df= df.withColumn("Model_Name", split(col("Model_ID"), "-")[1])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # calculate the revenue/unit
# MAGIC

# COMMAND ----------

df= df.withColumn("Price/Unit", col("Revenue")/col("Units_Sold"))
df.display()

# COMMAND ----------

# units sold in each year/branch
df.groupBy("Year", "BranchName").agg(sum("Units_Sold").alias("Total_Units_Sold")).sort("Year", "Total_Units_Sold", ascending=[True, False]).display()

# COMMAND ----------

display(df.groupBy("Year", "BranchName").agg(sum("Units_Sold").alias("Total_Units_Sold")).sort("Year", "Total_Units_Sold", ascending=[True, False]))

# COMMAND ----------

df = df.withColumn("day_str", lpad(col("day").cast("string"), 2, '0'))
df = df.withColumn("month_str", lpad(col("month").cast("string"), 2, '0'))
df = df.withColumn("year_str", col("year").cast("string"))

df = df.withColumn(
    "Date",
    to_date(concat(df["year_str"], lit('-'), df["month_str"], lit('-'), df["day_str"]), 'yyyy-mm-dd')
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # write to silver
# MAGIC

# COMMAND ----------

df.write.mode("overwrite").format("parquet").save("abfss://silver@sadesales.dfs.core.windows.net/Car_Sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@sadesales.dfs.core.windows.net/Car_Sales`

# COMMAND ----------

