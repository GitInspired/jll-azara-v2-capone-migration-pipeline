# Databricks notebook source
# MAGIC %md
# MAGIC # Create table for OVF

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import re
import os
from azarautils import ClientObject
client_obj                   = ClientObject(client_id = os.getenv("CLIENT_ID"),client_name = os.getenv('CLIENT_NAME'))
catalog                      = client_obj.catalog
db                           = f"{catalog}.{client_obj.databricks_client_base_db}"
custom_db                    = f"{catalog}.{client_obj.databricks_client_custom_db}"
storage_account              = client_obj.client_azstorage_account
client_secret_scope          = client_obj.client_secret_scope
client_data_container        = client_obj.client_data_container
client_deltatables_container = client_obj.client_deltatables_container
client_custom_db             = f"{catalog}.{client_obj.databricks_client_custom_db}"
data_container               = client_data_container
container                    = client_deltatables_container


# dbutils.fs.ls(source_container)
## New link - Container 'Data' is for files and 'deltatables' is for external tables
# abfss://data@dev0007642176bankofamuc.dfs.core.windows.net

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading data into dataframe

# COMMAND ----------

def load_tracker(tab_name):
    table = f"{client_custom_db}.{tab_name}"
    view  = f"{client_custom_db}.vw_{tab_name}"
    preferred_dir = 'ovf'
    data_path = f"abfss://deltatables@{storage_account}.dfs.core.windows.net/{preferred_dir}/{tab_name}/data"

    df1 = spark.sql("""select * from {view} """.format(view=view))
    cnt = df1.cache().count()
    df1.write.mode('overwrite').option("format", "delta").option("path", data_path).saveAsTable(table)
    print('loading ',table, ' record count ', cnt)

# COMMAND ----------

# table list
list = [
['ssdv_vw_ovf_financial_workorderinvoice'],
['ssdv_vw_ovf_tm1_detailanalysiscube'],
['ssdv_vw_ovf_commitmentdetail'],
]

# COMMAND ----------

for file_info in list:
    tab_name   = file_info[0]
    load_tracker(tab_name)
