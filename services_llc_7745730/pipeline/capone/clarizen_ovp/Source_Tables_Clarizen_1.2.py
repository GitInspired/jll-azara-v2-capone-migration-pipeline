# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook is used to create below tables**
# MAGIC 1. clientname_raw_edp_snapshot_clarizen_user
# MAGIC 2. clientname_raw_edp_snapshot_clarizen_program
# MAGIC 3. clientname_raw_edp_snapshot_clarizen_project
# MAGIC 4. clientname_raw_edp_snapshot_clarizen_contactprojectlink
# MAGIC 5. clientname_raw_edp_snapshot_clarizen_directorycontacts
# MAGIC 6. clientname_raw_edp_snapshot_clarizen_spendrole

# COMMAND ----------

# DBTITLE 1,client_variables
import os
from azarautils import ClientObject

# Client Config
client_obj           = ClientObject(client_id=os.getenv("CLIENT_ID"),client_name=os.getenv("CLIENT_NAME"))
client_secret_scope  = client_obj.client_secret_scope
catalog              = client_obj.catalog

var_client_id        = os.getenv("CLIENT_ID")
var_client_name      = os.getenv("CLIENT_NAME")

var_azara_raw_db     = f"{catalog}.jll_azara_raw"

var_client_raw_db    = f"{catalog}.{client_obj.databricks_client_raw_db}"
var_client_custom_db = f"{catalog}.{client_obj.databricks_client_custom_db}"

# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_user
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_user
select * from 
    {var_azara_raw_db}.edp_snapshot_clarizen_user 
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));



# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_program
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_program
select 
    pgrm.*
from  
    {var_azara_raw_db}.edp_snapshot_clarizen_program pgrm 
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust
    on pgrm.C_ProgramCustomer=cust.ID 
where  cust.C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

# MAGIC %sql
# MAGIC select cust.C_OVClientID,cust.* from jll_azara_catalog.jll_azara_raw.edp_snapshot_clarizen_program pgrm
# MAGIC inner join jll_azara_catalog.jll_azara_raw.edp_snapshot_clarizen_customer cust
# MAGIC  on pgrm.C_ProgramCustomer=cust.ID 
# MAGIC  --where  cust.C_OVClientID = '7745730'
# MAGIC --select * from jll_azara_catalog.jll_azara_raw.edp_snapshot_clarizen_customer

# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_project
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project as 
select
    proj.*
from
    {var_azara_raw_db}.edp_snapshot_clarizen_project proj
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust
on cust.ID=proj.c_customer
where  cust.C_OVClientID = {var_client_id} 
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_contactprojectlink
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_contactprojectlink
select 
    conprojlnk.*
from  
    {var_azara_raw_db}.edp_snapshot_clarizen_contactprojectlink conprojlnk
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_project proj
    on conprojlnk.Project=proj.Id
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust 
    on cust.ID=proj.c_customer
where  cust.C_OVClientID = {var_client_id} 
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_directorycontacts
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_directorycontacts
select
    cdir.*
from    
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_directorycontacts cdir
    on cust.ID=cdir.c_customer
where  cust.C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_spendrole
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_spendrole
select 
    spendrl.*
from  
    {var_azara_raw_db}.edp_snapshot_clarizen_spendrole spendrl
inner join  
    {var_azara_raw_db}.edp_snapshot_clarizen_contactprojectlink cntprjlnk
    on spendrl.id= cntprjlnk.C_RoleinSpend 
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_project proj
    on cntprjlnk.Project=proj.Id
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust 
    on cust.ID=proj.c_customer
where  cust.C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

