# Databricks notebook source
# DBTITLE 1,client_variables
var_client_id        = "7745730"
var_client_name      = "capone"

var_client_raw_db    = "jll_azara_0007745730_capitalone_raw"
var_client_custom_db = "jll_azara_0007745730_capitalone_custom"

# COMMAND ----------

# DBTITLE 1,ref_f0101_capone
spark.sql(""" 
CREATE OR REPLACE VIEW hive_metastore.{var_client_custom_db}.raw_ref_f0101_{var_client_name}
AS 
    SELECT 
        F0101.* 
    FROM 
        hive_metastore.jll_azara_raw.ref_f0101 F0101
    JOIN
        hive_metastore.{var_client_custom_db}.raw_ref_f0006_{var_client_name} F0006
            on F0006.MCMCU = F0101.ABMCU  
    JOIN
        hive_metastore.{var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F0006.MCCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,ref_f0116_capone
spark.sql(""" 
CREATE OR REPLACE VIEW hive_metastore.{var_client_custom_db}.raw_ref_f0116_{var_client_name}
AS 
    SELECT 
        F0116.* 
    FROM 
        hive_metastore.jll_azara_raw.ref_f0116 F0116
    JOIN
        hive_metastore.{var_client_custom_db}.raw_ref_f0101_{var_client_name} F0101
            on F0116.ALAN8 = F0101.ABAN8  
               
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,ref_f0413_capone
spark.sql(""" 
CREATE OR REPLACE VIEW hive_metastore.{var_client_custom_db}.raw_ref_f0413_{var_client_name}
AS 
    SELECT 
        F0413.* 
    FROM 
        hive_metastore.jll_azara_raw.ref_f0413 F0413
    JOIN
        hive_metastore.{var_client_custom_db}.raw_ref_f0414_{var_client_name} F0414
            on F0413.RMPYID = F0414.RNPYID               
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))