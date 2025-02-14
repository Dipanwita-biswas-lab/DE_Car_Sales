# Databricks notebook source
from pyspark.sql.functions import *
# ?col,filter, isnull, isnotnull
from delta.tables import DeltaTable; 

# COMMAND ----------

# MAGIC %md
# MAGIC -- check data in silver data
# MAGIC -- 
# MAGIC

# COMMAND ----------

sql_query  = spark.sql('select * from parquet.`abfss://silver@sadesales.dfs.core.windows.net/Car_Sales`')

# COMMAND ----------

sql_query.display()

# COMMAND ----------

sql_source  = spark.sql('select distinct Model_ID, Product_name from parquet.`abfss://silver@sadesales.dfs.core.windows.net/Car_Sales`')

# COMMAND ----------

# create a blank schema
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    df_sink = spark.sql('select distinct  model_dim_key, Model_ID, Product_name from parquet.`abfss://gold@sadesales.dfs.core.windows.net/dim_model`')
else:   
    df_sink = spark.sql('select distinct 1 as model_dim_key,Model_ID, Product_name from parquet.`abfss://silver@sadesales.dfs.core.windows.net/Car_Sales` where 1=0')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

dim_model_df=sql_source.join(df_sink, sql_source.Model_ID == df_sink.Model_ID, 'left').select(df_sink["Model_dim_key"], sql_source["Model_ID"], sql_source["Product_name"])

# COMMAND ----------

dim_model_df.display()

# COMMAND ----------

dim_model_df_new=dim_model_df.filter(col("model_dim_key").isNull())

# COMMAND ----------

dim_model_df_old=dim_model_df.filter(col("Model_dim_key").isNotNull())

# COMMAND ----------

# new dimension values
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    max_ID= spark.sql("Select max(model_dim_key) from cars_catalog.gold.dim_model").collect()[0][0]
else:
    max_ID=1
dim_model_df_new=dim_model_df_new.withColumn("model_dim_key",max_ID +  monotonically_increasing_id())
dim_model_df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC for Old dimenion values we will do upsert / update 

# COMMAND ----------


dim_model= dim_model_df_new.union(dim_model_df_old)
dim_model.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD - Type 1
# MAGIC
# MAGIC Now merge the dim_dealer df with the actual table

# COMMAND ----------


if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    # incremental load
    target_table = DeltaTable.forPath(spark, 'abfss://gold@sadesales.dfs.core.windows.net/dim_model')
    target_table.alias('target').merge(dim_dealer.alias('dim_model'), "target.model_dim_key = dim_model.model_dim_key")\
        .whenMatchedUpdateAll().whenNotMatchedInsertAll()
else:
# initialsload
    dim_model.write.format("delta").mode("overwrite").option("path", "abfss://gold@sadesales.dfs.core.windows.net/dim_model").saveAsTable("cars_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  cars_catalog.gold.dim_model

# COMMAND ----------

