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

sql_source = spark.sql('select distinct dealer_id, DealerName from parquet.`abfss://silver@sadesales.dfs.core.windows.net/Car_Sales`')

# COMMAND ----------

sql_query.display()

# COMMAND ----------

# create a blank schema
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    df_sink = spark.sql('select distinct  dealer_dim_key,dealer_id, DealerName from parquet.`abfss://gold@sadesales.dfs.core.windows.net/dim_dealer`')
else:   
    df_sink = spark.sql('select distinct 1 as dealer_dim_key,dealer_id, DealerName from parquet.`abfss://silver@sadesales.dfs.core.windows.net/Car_Sales` where 1=0')

# COMMAND ----------

df_sink.display()

# COMMAND ----------

dim_dealer_df=sql_source.join(df_sink, sql_source.dealer_id == df_sink.dealer_id, 'left').select(df_sink["Dealer_dim_key"], sql_source["dealer_id"], sql_source["DealerName"])

# COMMAND ----------

dim_dealer_df.display()

# COMMAND ----------

dim_dealer_df_new=dim_dealer_df.filter(col("Dealer_dim_key").isNull())

# COMMAND ----------

dim_dealer_df_old=dim_dealer_df.filter(col("Dealer_dim_key").isNotNull())

# COMMAND ----------

# new dimension values
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    max_ID= spark.sql("Select max(Dealer_dim_key) from cars_catalog.gold.dim_dealer").collect()[0][0]
else:
    max_ID=1
dim_dealer_df_new=dim_dealer_df_new.withColumn("Dealer_dim_key",max_ID +  monotonically_increasing_id())
dim_dealer_df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC for Old dimenion values we will do upsert / update 

# COMMAND ----------


dim_dealer= dim_dealer_df_new.union(dim_dealer_df_old)
dim_dealer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD - Type 1
# MAGIC
# MAGIC Now merge the dim_dealer df with the actual table

# COMMAND ----------


if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    # incremental load
    target_table = DeltaTable.forPath(spark, 'abfss://gold@sadesales.dfs.core.windows.net/dim_dealer')
    target_table.alias('target').merge(dim_dealer.alias('dim_dealer'), "target.Dealer_dim_key = dim_dealer.Dealer_dim_key")\
        .whenMatchedUpdateAll().whenNotMatchedInsertAll()
else:
# initialsload
    dim_dealer.write.format("delta").mode("overwrite").option("path", "abfss://gold@sadesales.dfs.core.windows.net/dim_dealer").saveAsTable("cars_catalog.gold.dim_dealer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  cars_catalog.gold.dim_dealer

# COMMAND ----------

