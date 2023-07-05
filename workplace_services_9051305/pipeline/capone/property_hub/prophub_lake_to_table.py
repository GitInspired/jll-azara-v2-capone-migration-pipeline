# Databricks notebook source
# MAGIC %md
# MAGIC # Creating tables/views in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC * Importing the file from blob and creating dataframe using the data.
# MAGIC * Applying transformations and business logic
# MAGIC * Creating tables/views on Databricks and Snowflake.

# COMMAND ----------

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

# source_container = f"abfss://data@{storage_account}.dfs.core.windows.net"
#Enable below commented line to browse through sub folders to find file to upload , copy the file path from container and then disable it back
#dbutils.fs.ls(f"{source_container}/") 


# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading data into dataframe

# COMMAND ----------

def load_3614(src_file,tmp_name,tab_name):

    file_path = f"abfss://{data_container}@{storage_account}.dfs.core.windows.net/".format(data_container=client_data_container,storage_account=storage_account)
    file_location = file_path + src_file
    file_type     = "CSV"
    infer_schema  = "False" #"True"
    file_header   = "True"

    table = f"{client_custom_db}.{tab_name}"
    preferred_dir = 'prophub'

    data_path = f"abfss://deltatables@{storage_account}.dfs.core.windows.net/{preferred_dir}/{tab_name}/data"

    # print(tab_name,data_path)

    df1 = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", file_header).option("delimiter", "|").option("quote", "\"")\
                                      .option("escape", "\"").option("encoding", "UTF-8").option('multiLine', True).load(file_location)
    cnt = df1.cache().count()
    for each in df1.schema.names:
        df1 = df1.withColumnRenamed(each,re.sub(r"._$", "",re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',\
                   each.replace(' ', '').replace(':', '_').replace('/', '_').replace('(', '_').replace(')', '_').replace('-', '_').replace('__', '_'))))

    df1.write.mode('overwrite').option("format", "delta").option("path", data_path).saveAsTable(table)
    print('loading ',file_location,' to table ',tab_name, ' record count ', cnt)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Applying Transformations and Creating Temp View

# COMMAND ----------

# file name, temporary table name, table name
list   = [['OneView_CapitalOne_Workplace_Services_Property_4684.csv', 'OneView_CapitalOne_Workplace_Services_Property_4684',     'capitalone_custom_ssdv_workplace_services_property_4684'],
          ['OneView_Capital_One_Workplace_Services_Property_4684.csv','OneView_Capital_One_Workplace_Services_Property_4684','capitalone_custom_clientssdv_workplace_services_property_4684']
         ]
for source in list:
    load_3614(source[0],source[1],source[2])