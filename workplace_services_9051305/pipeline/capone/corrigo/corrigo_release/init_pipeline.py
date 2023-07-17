# Databricks notebook source
# DBTITLE 1,Imports
from datetime import timezone
import datetime

# COMMAND ----------

# DBTITLE 1,Generating "DWH Update Date"
"""
Data refresh timestamp for a specific run
"""
refresh_timestamp = datetime.datetime.now(timezone.utc)
dbutils.jobs.taskValues.set("UpdateDate", str(refresh_timestamp))

# COMMAND ----------

# MAGIC %md