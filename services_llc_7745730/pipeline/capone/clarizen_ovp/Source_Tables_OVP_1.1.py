# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook is used to create below tables**
# MAGIC 1.  clientname_ovp_tbl_sharepoint_templatemapping
# MAGIC 2.  clientname_ovp_tbl_der_webs_global_por
# MAGIC 3.  Clientname_ovp_tbl_raw_all_userdata_riskregister
# MAGIC 4.  clientname_raw_edp_snapshot_clarizen_project
# MAGIC 5.  clientname_raw_edp_snapshot_clarizen_milestone
# MAGIC 6.  clientname_raw_edp_snapshot_clarizen_generictask
# MAGIC 7.  clientname_raw_edp_snapshot_clarizen_customer
# MAGIC 8.  clientname_ovp_tbl_raw_all_userdata_projectquantities
# MAGIC 9.  clientname_ovp_tbl_raw_webs_xref
# MAGIC 10. clientname_ovp_tbl_raw_all_userdata_clients
# MAGIC 11. clientname_raw_edp_snapshot_clarizen_jobsite
# MAGIC 12. Clientname_ovp_tbl_raw_all_userdata_projectdirectory
# MAGIC 13. Clientname_raw_edp_snapshot_clarizen_projectstatsindicator
# MAGIC 14. Clientname_raw_edp_snapshot_clarizen_servicetype
# MAGIC 15. Clientname_raw_edp_snapshot_clarizen_jllprojecttype
# MAGIC 16. Clientname_raw_edp_snapshot_clarizen_customvalue
# MAGIC 17. clientname_ovp_bac_preproject_planning_customfields 
# MAGIC 18. clientname_ovp_tbl_raw_all_userdata_pmdcommentary
# MAGIC 19. clientname_ovp_tbl_raw_all_userdata_milestonesprojectactivities

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

print(var_client_id)
print(var_client_name)


# COMMAND ----------

# DBTITLE 1,clientname_ovp_tbl_sharepoint_templatemapping
spark.sql(f""" 
CREATE OR REPLACE TABLE 
    {var_client_custom_db}.{var_client_name}_ovp_tbl_sharepoint_templatemapping as
select  
    *
FROM 
    {var_azara_raw_db}.edp_snapshot_ovp_tblshrpointtempltemapping
WHERE 
    CompanyNbr  IN 
    ('2454','2456','25521','63')
""")



# COMMAND ----------

# DBTITLE 1,clientname_ovp_tbl_der_webs_global_por
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_ovp_tbl_der_webs_global_por as 
select
    *
from
    {var_azara_raw_db}.edp_snapshot_ovp_tblder_webs_global_por
where
    ClientNbr IN ('2454','2456','25521','63')
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, 
var_client_name=var_client_name));



# COMMAND ----------

# DBTITLE 1,Clientname_ovp_tbl_raw_all_userdata_riskregister
spark.sql(f""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_riskregister as
select 
    b.* 
FROM 
    {var_azara_raw_db}.edp_snapshot_ovp_tblder_webs_global_por AS gpor 
JOIN 
    {var_azara_raw_db}.edp_snapshot_ovp_tblraw_alusrdtariskregstr AS b
ON 
    gpor.SiteID = b.SiteID
AND 
    gpor.WebId  = b.WebID
AND 
    gpor.tp_id  = b.ProjectID
WHERE 
    ClientNbr IN ('2454','2456','25521','63')
""");  



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

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_milestone
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_milestone
select 
    milestone.*
from  
    {var_azara_raw_db}.edp_snapshot_clarizen_milestone milestone
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_project proj
    on milestone.project=proj.Project
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust 
    on cust.ID=proj.c_customer
where  cust.C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));



# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_generictask
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_generictask
select 
    generic.*
from  
    {var_azara_raw_db}.edp_snapshot_clarizen_generictask generic
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_project proj
    on generic.project=proj.Project
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust 
    on cust.ID=proj.c_customer
where  cust.C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));



# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_customer
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_customer
select 
*
from
    {var_azara_raw_db}.edp_snapshot_clarizen_customer 
where C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));




# COMMAND ----------

# DBTITLE 1,Clientname_ovp_tbl_raw_all_userdata_projectquantities
spark.sql(f""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_projectquantities as
select 
    pq.* 
FROM 
    {var_azara_raw_db}.edp_snapshot_ovp_tblder_webs_global_por AS gpor
JOIN 
    {var_azara_raw_db}.edp_snapshot_ovp_tblraw_projectquantities AS pq
ON 
    gpor.SiteID = pq.SiteID 
AND 
    gpor.WebId  = pq.WebID
AND 
    gpor.tp_id  = pq.ProjectID
WHERE 
    gpor.clientNbr IN ('2454','2456','25521','63')
""");  

# COMMAND ----------

# DBTITLE 1,Clientname_ovp_tbl_raw_webs_xref
spark.sql(f""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_webs_xref as
select 
    x.* 
from
    (select distinct SiteId ,WebID  
From  
    {var_azara_raw_db}.edp_snapshot_ovp_tblder_webs_global_por
WHERE
    ClientNbr IN ('2454','2456','25521','63')) AS gpor
left JOIN
    {var_azara_raw_db}.edp_snapshot_ovp_tblraw_websxref as x
ON
    gpor.SiteId = x.SiteId
AND
    gpor.WebID = x.WebId
""")

# COMMAND ----------

# DBTITLE 1,clientname_ovp_tbl_raw_all_userdata_clients
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_clients as 
select
    *
from
    {var_azara_raw_db}.edp_snapshot_ovp_tblraw_alluserdata_clients
where
    ClientID IN ('2454','2456','25521','63')
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, 
var_client_name=var_client_name));

# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_jobsite
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_jobsite as 
select
    *
from
    {var_azara_raw_db}.edp_snapshot_clarizen_jobsite
where  
    C_OVCID = {var_client_id} 
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

# DBTITLE 1,Clientname_ovp_tbl_raw_all_userdata_projectdirectory
spark.sql(f""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_projectdirectory as
select 
    pd.* 
FROM 
    {var_azara_raw_db}.edp_snapshot_ovp_tblder_webs_global_por AS gpor
JOIN 
    {var_azara_raw_db}.edp_snapshot_ovp_tblalluserdataprjdirectory AS pd
ON 
    gpor.SiteID = pd.SiteID
AND 
    gpor.WebId  = pd.WebID
AND 
    gpor.tp_id  = pd.ProjectID
WHERE 
    gpor.ClientNbr IN ('2454','2456','25521','63')
""");  

# COMMAND ----------

# DBTITLE 1,clientname_raw_edp_snapshot_clarizen_projectstatsindicator
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_projectstatsindicator
select 
    prjstind.*
from  
    {var_azara_raw_db}.edp_snapshot_clarizen_projectstatsindicator prjstind
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_project proj
    on prjstind.C_ProjectStatusIndicator=proj.Project
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust 
    on cust.ID=proj.c_customer
where  cust.C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

# DBTITLE 1,Clientname_raw_edp_snapshot_clarizen_servicetype
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_servicetype
select 
    distinct
    srvctp.*
from  
    {var_azara_raw_db}.edp_snapshot_clarizen_servicetype srvctp
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_project prj
    on srvctp.ID = prj.C_ServiceType
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust 
    on cust.ID=prj.c_customer
where  cust.C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

# DBTITLE 1,Clientname_raw_edp_snapshot_clarizen_jllprojecttype
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_jllprojecttype
select 
    distinct
    jllpt.*
from  
    {var_azara_raw_db}.edp_snapshot_clarizen_jllprojecttype jllpt
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_project prj
    on jllpt.ID = prj.C_JLLProjectType
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust 
    on cust.ID=prj.c_customer
where  cust.C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

# DBTITLE 1,Clientname_raw_edp_snapshot_clarizen_customvalue
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_customvalue
select
    custval.*
from  
    {var_azara_raw_db}.edp_snapshot_clarizen_customer cust
inner join 
    {var_azara_raw_db}.edp_snapshot_clarizen_customvalue custval 
on cust.ID=custval.c_customer
where  cust.C_OVClientID = {var_client_id}
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_id=var_client_id,var_client_name=var_client_name));

# COMMAND ----------

# DBTITLE 1,clientname_ovp_tbl_raw_all_userdata_jobsiteaddress
spark.sql(f""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_jobsiteaddress as
select 
    b.* 
FROM 
    {var_azara_raw_db}.edp_snapshot_ovp_tblder_webs_global_por AS gpor
JOIN 
    {var_azara_raw_db}.edp_snapshot_ovp_tblraw_jobsiteaddress AS b
ON 
    gpor.SiteID = b.SiteID
AND 
    gpor.WebId  = b.WebID
AND 
    gpor.tp_id  = b.int1
where
      gpor.ClientNbr IN ('2454','2456','25521','63')
"""); 

# COMMAND ----------

# DBTITLE 1,clientname_ovp_tbl_raw_all_userdata_pmdcommentary
spark.sql(f""" 
CREATE OR REPLACE TABLE 
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_pmdcommentary as
select  
    b.*
FROM 
    {var_azara_raw_db}.edp_snapshot_ovp_tblder_webs_global_por AS gpor
JOIN 
    {var_azara_raw_db}.edp_snapshot_ovp_tblraw_pmdcommentary AS b
    ON  gpor.SiteId = b.SiteId
    AND gpor.WebId  = b.WebId
    AND gpor.tp_id =  b.ProjectID
WHERE 
    gpor.ClientNbr IN ('2454','2456','25521','63')
""")

# COMMAND ----------

spark.sql(f""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_milestonesprojectactivities as
select 
    b.* 
FROM 
    {var_azara_raw_db}.edp_snapshot_ovp_tblder_webs_global_por AS gpor
JOIN 
    {var_azara_raw_db}.edp_snapshot_ovp_tblraw_milestoneprojects AS b
ON 
    gpor.SiteId = b.SiteID
AND 
    gpor.WebId = b.WebID
AND 
    gpor.tp_id = b.ProjectID
WHERE 
    gpor.ClientNbr IN ('2454','2456','25521','63')
"""); 