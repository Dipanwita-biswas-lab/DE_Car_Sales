# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import  DeltaTable

# COMMAND ----------

sql_check = spark.sql('select distinct * from parquet.`abfss://silver@sadesales.dfs.core.windows.net/Car_Sales`')

# COMMAND ----------

sql_check.display()

# COMMAND ----------

sql_source= spark.sql('select distinct branch_id, BranchName from parquet.`abfss://silver@sadesales.dfs.core.windows.net/Car_Sales`')

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    sql_sink= spark.sql('select distinct branch_id, BranchName from parquet.`abfss://gold@sadesales.dfs.core.windows.net/dim_branch`')
else:
    sql_sink= spark.sql('select distinct 1 as branch_key, branch_id, BranchName from parquet.`abfss://silver@sadesales.dfs.core.windows.net/Car_Sales` where 1=0')

# COMMAND ----------

dim_branch_compare= sql_source.join(sql_sink, sql_source.branch_id==sql_sink.branch_id, 'left').select(sql_sink.branch_key,sql_source.branch_id, sql_source.BranchName)
dim_branch_compare.display()

# COMMAND ----------

dim_branch_new=dim_branch_compare.filter(dim_branch_compare.branch_key.isNull())
dim_branch_new.display()

# COMMAND ----------

dim_branch_old=dim_branch_compare.filter(dim_branch_compare.branch_key.isNotNull())

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    max_id= spark.sql('select max(branch_key) as max_id from parquet.`abfss://gold@sadesales.dfs.core.windows.net/dim_branch`')
else:
    max_id=1
dim_branch_new= dim_branch_new.withColumn('branch_key', max_id+monotonically_increasing_id())
dim_branch_new.display()

# COMMAND ----------

dim_branch=dim_branch_new.union(dim_branch_old)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
   delta_branch= DeltaTable.forPath(spark, "abfss://gold@sadesales.dfs.core.windows.net/dim_branch")
   delta_branch.alias("target").merge(dim_branch.alias("source"), "target.branch_key = source.branch_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    dim_branch.write.format("delta").option("path", "abfss://gold@sadesales.dfs.core.windows.net/dim_branch").mode("overwrite").saveAsTable("cars_catalog.gold.dim_branch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_branch

# COMMAND ----------

