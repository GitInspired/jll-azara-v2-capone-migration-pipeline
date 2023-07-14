# Databricks notebook source
# MAGIC %md
# MAGIC **Use this Notebook to define the required variables**

# COMMAND ----------

# var_client_id        = 7745730
# var_client_name      = "capone"

# var_raw_db = "jll_azara_raw"
# var_client_raw_db    = "jll_azara_0007745730_capitalone_raw"
# var_client_custom_db = "jll_azara_0007745730_capitalone_custom"



# Please use these variables for Dev

# Imports
import os
from azarautils import ClientObject

# Client Config
client_obj                   = ClientObject(client_id=os.getenv("CLIENT_ID"),client_name=os.getenv("CLIENT_NAME"))
client_secret_scope          = client_obj.client_secret_scope
var_client_id=os.getenv("CLIENT_ID")
#var_client_name=os.getenv("CLIENT_NAME")  
var_client_name      = "capone"
catalog                      = client_obj.catalog
var_raw_db             = f"{catalog}.jll_azara_raw"
var_azara_business_db        = f"{catalog}.jll_azara_business"
var_client_raw_db            = f"{catalog}.{client_obj.databricks_client_raw_db}"
var_client_custom_db         = f"{catalog}.{client_obj.databricks_client_custom_db}"

# COMMAND ----------

print(var_client_id)