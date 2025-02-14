# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE CATALOG

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG cars_catalog;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.gold;

# COMMAND ----------

