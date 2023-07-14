# Databricks notebook source
# MAGIC %run ./notebook_variables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Notebook for E1 ref Views
# MAGIC * raw_ref_f0010_capone
# MAGIC * raw_ref_f554301_capone
# MAGIC * raw_ref_f564314d_capone
# MAGIC * raw_ref_f4301_capone
# MAGIC * raw_ref_f4311_capone
# MAGIC * raw_ref_f0006_capone
# MAGIC * raw_ref_f0411_capone
# MAGIC * raw_ref_f0414_capone
# MAGIC * raw_ref_f0101
# MAGIC * raw_ref_f0413
# MAGIC * raw_ref_f0013
# MAGIC * raw_ref_f4209_capone
# MAGIC * raw_ref_f564314t_capone
# MAGIC * raw_ref_f0902_capone
# MAGIC * raw_ref_f550101_capone
# MAGIC * raw_ref_f0005
# MAGIC * raw_ref_f0116
# MAGIC * raw_ref_f0911_capone
# MAGIC * raw_ref_f0901_capone
# MAGIC * raw_ref_f550902_capone
# MAGIC * raw_ref_detailanalysiscube_capone

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0010_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0010_{var_client_name}
AS 
    SELECT 
        * 
    FROM 
        {var_raw_db}.ref_f0010
    WHERE
        upper(ccname) like 'CAPITAL ONE%'                  
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f554301_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f554301_{var_client_name} 
AS 
    SELECT 
        f554301.* 
    FROM 
        {var_raw_db}.ref_f554301 f554301
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on f554301.ODKCOO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f564314d_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f564314d_{var_client_name} 
AS 
    SELECT 
        f564314d.* 
    FROM 
        {var_raw_db}.ref_f564314d f564314d
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on f564314d.VDKCOO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f4301_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f4301_{var_client_name} 
AS 
    SELECT 
        F4301.* 
    FROM 
        {var_raw_db}.ref_f4301 F4301
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name}  F0010
            on F4301.PHKCOO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f4311_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f4311_{var_client_name}
AS 
    SELECT 
        F4311.* 
    FROM 
        {var_raw_db}.ref_f4311 F4311
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F4311.PDCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0006_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0006_{var_client_name}
AS 
    SELECT 
        F0006.* 
    FROM 
        {var_raw_db}.ref_f0006 F0006
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F0006.MCCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0411_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0411_{var_client_name}
AS 
    SELECT 
        F0411.* 
    FROM 
        {var_raw_db}.ref_f0411 F0411
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F0411.RPKCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0414_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0414_{var_client_name}
AS 
    SELECT 
        F0414.* 
    FROM 
        {var_raw_db}.ref_f0414 F0414
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F0414.RNCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0101

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0101
AS 
    SELECT 
        *
    FROM 
        {var_raw_db}.ref_f0101            
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0413

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0413
AS 
    SELECT 
        *
    FROM 
        {var_raw_db}.ref_f0413           
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0013
# MAGIC

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0013
AS 
    SELECT 
        *
    FROM 
        {var_raw_db}.ref_f0013           
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f4209_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f4209_{var_client_name}
AS 
    SELECT 
        F4209.* 
    FROM 
        {var_raw_db}.ref_f4209 F4209
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F4209.HOKCOO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f564314t_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f564314t_{var_client_name}
AS 
    SELECT 
        F564314T.* 
    FROM 
        {var_raw_db}.ref_f564314t F564314T
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F564314T.VHCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0902_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0902_{var_client_name}
AS 
    SELECT 
        F0902.* 
    FROM 
        {var_raw_db}.ref_f0902 F0902
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F0902.GBCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f550101_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f550101_{var_client_name}
AS 
    SELECT 
        F550101.* 
    FROM 
        {var_raw_db}.ref_f550101 F550101
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F550101.PTCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0005

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0005
AS 
    SELECT 
        *
    FROM 
        {var_raw_db}.ref_f0005           
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0116

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0116
AS 
    SELECT 
        *
    FROM 
        {var_raw_db}.ref_f0116          
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0911_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0911_{var_client_name}
AS 
    SELECT 
        F0911.* 
    FROM 
        {var_raw_db}.ref_f0911 F0911
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F0911.GLKCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f0901_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f0901_{var_client_name}
AS 
    SELECT 
        F0901.* 
    FROM 
        {var_raw_db}.ref_f0901 F0901
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F0901.GMCO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_f550902_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_f550902_{var_client_name}
AS 
    SELECT 
        F550902.* 
    FROM 
        {var_raw_db}.ref_f550902 F550902
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F550902.BACO = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ###### View raw_ref_detailanalysiscube_capone

# COMMAND ----------

spark.sql(""" 
CREATE OR REPLACE VIEW {var_client_custom_db}.raw_ref_detailanalysiscube_{var_client_name}
AS 
    SELECT 
        DA.* 
    FROM 
        {var_raw_db}.ref_detailanalysiscube DA
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on DA.company = F0010.CCCO                 
""".format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))