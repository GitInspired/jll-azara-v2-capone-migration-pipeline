# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook is used to create below Views**
# MAGIC 1.  

# COMMAND ----------

# DBTITLE 1,client_variables
import os
from azarautils import ClientObject

# Client Config
client_obj           = ClientObject(client_id=os.getenv("CLIENT_ID"),client_name=os.getenv("CLIENT_NAME"))
client_secret_scope  = client_obj.client_secret_scope
catalog              = client_obj.catalog


var_client_id        = os.getenv("CLIENT_ID")
var_tenant_id        = "12098922879739"
var_client_name      = os.getenv("CLIENT_NAME")

var_azara_raw_db     = f"{catalog}.jll_azara_raw"

var_client_raw_db    = f"{catalog}.{client_obj.databricks_client_raw_db}"
var_client_custom_db = f"{catalog}.{client_obj.databricks_client_custom_db}"

# COMMAND ----------

# DBTITLE 1,tmp_users
spark.sql(""" 
create or replace temp view users as	 
  SELECT
    concat('/User/',external_id) as external_id
    ,display_name
	FROM
    {var_client_custom_db}.datavault_dbo_s_users
  -- LEFT JOIN datavault.dbo.s_users_record_source_tracking as urst
	-- 	ON u.external_id = urst.id
	-- WHERE
  
  --   u.dss_current_version = 1 and urst.id IS NULL  -- record deleted
""".format(var_client_custom_db=var_client_custom_db))



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users

# COMMAND ----------

# DBTITLE 1,tmp_risk_details_RiskRegister
spark.sql(""" 
create or replace temp view risk_details as	 
     Select  rd.Title
      ,rd.Description
      ,rd.clz_risk_category
  	  ,rd.clz_risk_type
      ,rd.clz_initial_likelihood
	  ,rd.clz_initial_consequences
      ,rd.clz_action_to_achieve_mitigation
      ,rd.clz_impact_of_occurrence
      ,rd.clz_risk_status
      --,REPLACE(rd.created_by,'/User/','') as created_by
      ,rd.created_at
      ,rd.due_at
	  ,rd.planned_for
	  ,rd.external_id
      ,createdby.display_name as CreatedBy
      ,modifiedby.display_name as ModifiedBy
	  ,res.display_name as Responsibility
	  ,rd.clz_initial_risk_factor_score as Initial_risk_factor_score
	  ,rd.created_at as date
	  --,REPLACE(rd.last_updated_by,'/User/','') as last_updated_by
	  --,REPLACE(rd.assigned_to,'/User/','') as assigned_to
  FROM {var_client_custom_db}.datavault_dbo_s_risk_details as rd 
  LEFT JOIN users AS createdby
   ON rd.created_by = createdby.external_id 
  LEFT JOIN users AS modifiedby 
   ON rd.last_updated_by = modifiedby.external_id 
  LEFT JOIN users AS res 
   ON rd.assigned_to = res.external_id 
--   LEFT JOIN datavault.dbo.s_risk_details_record_source_tracking as rd_rst 
--     ON '/Risk/' + rd.external_id = rd_rst.id
--   WHERE rd.dss_current_version = 1
--     and rd_rst.id IS NULL -- records deleted
""".format(var_client_custom_db=var_client_custom_db))



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from risk_details

# COMMAND ----------

# DBTITLE 1,tmp_related_work_RiskRegister
spark.sql(""" 
create or replace temp view related_work as
(SELECT 
       rd.Title
      ,rd.Description
      ,rd.clz_risk_category
  	  ,rd.clz_risk_type
      ,rd.clz_initial_likelihood
	  ,rd.clz_initial_consequences
      ,rd.clz_action_to_achieve_mitigation
      ,rd.clz_impact_of_occurrence
      ,rd.clz_risk_status
      --,rd.created_by
      ,rd.created_at
      ,rd.due_at
	  ,rw.workitem as planned_for
	  ,rd.external_id
	  --,rd.last_updated_by
	  --,rd.assigned_to
      ,rd.CreatedBy
      ,rd.ModifiedBy
	  ,rd.Responsibility
	  ,rd.Initial_risk_factor_score
	  ,rd.date as date
	FROM risk_details as rd
	JOIN (
		SELECT rw.* 
		FROM {var_client_custom_db}.datavault_dbo_s_related_work_link as rw 
		LEFT JOIN risk_details as rd 
			ON rw.workitem = rd.planned_for 
			AND rw.c_case = '/Risk/' + rd.external_id
		WHERE rd.planned_for IS NULL
		) as rw
	  ON '/Risk/' + rd.external_id = rw.c_case)
   """.format(var_client_custom_db=var_client_custom_db))



# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from related_work
# MAGIC
# MAGIC SELECT 
# MAGIC        rd.Title
# MAGIC       ,rd.Description
# MAGIC       ,rd.clz_risk_category
# MAGIC   	  ,rd.clz_risk_type
# MAGIC       ,rd.clz_initial_likelihood
# MAGIC 	  ,rd.clz_initial_consequences
# MAGIC       ,rd.clz_action_to_achieve_mitigation
# MAGIC       ,rd.clz_impact_of_occurrence
# MAGIC       ,rd.clz_risk_status
# MAGIC       --,rd.created_by
# MAGIC       ,rd.created_at
# MAGIC       ,rd.due_at
# MAGIC 	  --,rw.workitem as planned_for
# MAGIC 	  ,rd.external_id
# MAGIC 	  --,rd.last_updated_by
# MAGIC 	  --,rd.assigned_to
# MAGIC       ,rd.CreatedBy
# MAGIC       ,rd.ModifiedBy
# MAGIC 	  ,rd.Responsibility
# MAGIC 	  ,rd.Initial_risk_factor_score
# MAGIC 	  ,rd.date as date
# MAGIC     ,rd.planned_for
# MAGIC 	FROM risk_details as rd
# MAGIC 	JOIN (
# MAGIC 		SELECT rw.* 
# MAGIC 		FROM jll_azara_catalog.jll_azara_0007745730_capitalone_custom.datavault_dbo_s_related_work_link as rw 
# MAGIC 		LEFT JOIN risk_details as rd 
# MAGIC 			ON rw.workitem = rd.planned_for 
# MAGIC 			AND rw.c_case = '/Risk/' + rd.external_id
# MAGIC 		WHERE rd.planned_for IS NULL
# MAGIC 		) as rw
# MAGIC 	  ON '/Risk/' + rd.external_id = rw.c_case

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rw.* 
# MAGIC 		FROM jll_azara_catalog.jll_azara_0007745730_capitalone_custom.datavault_dbo_s_related_work_link as rw 
# MAGIC 		LEFT JOIN risk_details as rd 
# MAGIC 			ON rw.workitem = rd.planned_for 
# MAGIC 			AND rw.c_case = '/Risk/' + rd.external_id
# MAGIC 		WHERE rd.planned_for IS NULL

# COMMAND ----------

# MAGIC  %sql
# MAGIC  select *
# MAGIC  FROM jll_azara_catalog.jll_azara_0007745730_capitalone_custom.stage_ovp_dbo_tblder_clarizen_projects as cp 
# MAGIC   -- left JOIN risk_details as rd 
# MAGIC   --   ON cp.id = rd.planned_for

# COMMAND ----------

# DBTITLE 1,ovp_vwtbl_Clarizen_RiskRegister
spark.sql(""" 
CREATE OR REPLACE table
{var_client_custom_db}.ovp_vwtbl_Clarizen_RiskRegister as
    SELECT 	  
	   cp.Client_Nbr as Client_Nbr
	  ,cp.Clarizen_Customer_Nbr as Clarizen_Customer_Nbr
	  ,cp.Job_Nbr as Job_Nbr
      ,'' as Phase
      ,rd.Title as Title
      ,rd.Description as Description
      ,rd.clz_risk_category as Risk_Category
  	  ,rd.clz_risk_type as Risk_Type
      ,CASE 
		When rd.clz_initial_likelihood= 'Almost Certain' then 0.9
		When rd.clz_initial_likelihood= 'Likely' then 0.7
		When rd.clz_initial_likelihood= 'Moderate' then 0.5
		When rd.clz_initial_likelihood= 'Rare' then 0.3
		When rd.clz_initial_likelihood= 'Unlikely' then 0.1
		else 0.0
	   END as Probability_Score
      ,CASE 
		When rd.clz_initial_consequences = 'Catastrophic' then 8
		When rd.clz_initial_consequences = 'Major' then 4
		When rd.clz_initial_consequences = 'Moderate' then 2
		When rd.clz_initial_consequences = 'Minor' then 1
		When rd.clz_initial_consequences = 'Insignificant' then 0.5
		else 0.0
	   END as Impact_Score
	  ,CASE 
		When rd.clz_initial_likelihood= 'Almost Certain' then 0.9
		When rd.clz_initial_likelihood= 'Likely' then 0.7
		When rd.clz_initial_likelihood= 'Moderate' then 0.5
		When rd.clz_initial_likelihood= 'Rare' then 0.3
		When rd.clz_initial_likelihood= 'Unlikely' then 0.1
		else 0.0
	   END *
       CASE 
		When rd.clz_initial_consequences = 'Catastrophic' then 8
		When rd.clz_initial_consequences = 'Major' then 4
		When rd.clz_initial_consequences = 'Moderate' then 2
		When rd.clz_initial_consequences = 'Minor' then 1
		When rd.clz_initial_consequences = 'Insignificant' then 0.5
		else 0.0
	   END as Risk_Score
      ,rd.clz_action_to_achieve_mitigation as Action_to_be_Taken
      ,rd.clz_impact_of_occurrence as Plan_to_Address_the_Risk
      ,rd.Responsibility as Responsibility
      ,'' as Plan_Approved
      ,CASE WHEN rd.clz_risk_status IN ('Closed', 'Mitagated - KIV') 
	    THEN 1 
		ELSE 0
	   END as Complete
      ,rd.created_at as Risk_Date
      ,rd.due_at as Action_Closed_Date
	  ,rd.clz_risk_status as Risk_status
      ,rd.CreatedBy as CreatedBy
      ,rd.ModifiedBy as ModifiedBy
	  ,'Clarizen' as SourceSystem
	  ,rd.Initial_risk_factor_score as Initial_risk_factor_score
	  ,rd.date as date
  
  FROM {var_client_custom_db}.stage_ovp_dbo_tblder_clarizen_projects as cp 
  JOIN risk_details as rd 
    ON cp.id = rd.planned_for
 

UNION ALL

  SELECT 	  
	   cp.Client_Nbr as Client_Nbr
	  ,cp.Clarizen_Customer_Nbr as Clarizen_Customer_Nbr
	  ,cp.Job_Nbr as Job_Nbr
      ,'' as Phase
      ,rd.Title as Title
      ,rd.Description as Description
      ,rd.clz_risk_category as Risk_Category
  	  ,rd.clz_risk_type as Risk_Type
      ,CASE 
		When rd.clz_initial_likelihood= 'Almost Certain' then 0.9
		When rd.clz_initial_likelihood= 'Likely' then 0.7
		When rd.clz_initial_likelihood= 'Moderate' then 0.5
		When rd.clz_initial_likelihood= 'Rare' then 0.3
		When rd.clz_initial_likelihood= 'Unlikely' then 0.1
		else 0.0
	   END as Probability_Score
      ,CASE 
		When rd.clz_initial_consequences = 'Catastrophic' then 8
		When rd.clz_initial_consequences = 'Major' then 4
		When rd.clz_initial_consequences = 'Moderate' then 2
		When rd.clz_initial_consequences = 'Minor' then 1
		When rd.clz_initial_consequences = 'Insignificant' then 0.5
		else 0.0
	   END as Impact_Score
	  ,CASE 
		When rd.clz_initial_likelihood= 'Almost Certain' then 0.9
		When rd.clz_initial_likelihood= 'Likely' then 0.7
		When rd.clz_initial_likelihood= 'Moderate' then 0.5
		When rd.clz_initial_likelihood= 'Rare' then 0.3
		When rd.clz_initial_likelihood= 'Unlikely' then 0.1
		else 0.0
	   END *
       CASE 
		When rd.clz_initial_consequences = 'Catastrophic' then 8
		When rd.clz_initial_consequences = 'Major' then 4
		When rd.clz_initial_consequences = 'Moderate' then 2
		When rd.clz_initial_consequences = 'Minor' then 1
		When rd.clz_initial_consequences = 'Insignificant' then 0.5
		else 0.0
	   END as Risk_Score
      ,rd.clz_action_to_achieve_mitigation as Action_to_be_Taken
      ,rd.clz_impact_of_occurrence as Plan_to_Address_the_Risk
      ,rd.Responsibility as Responsibility
      ,'' as Plan_Approved
      ,CASE WHEN rd.clz_risk_status IN ('Closed', 'Mitagated - KIV') 
	    THEN 1 
		ELSE 0
	   END as Complete
      ,rd.created_at as Risk_Date
      ,rd.due_at as Action_Closed_Date
	  ,rd.clz_risk_status as Risk_status
      ,rd.CreatedBy as CreatedBy
      ,rd.ModifiedBy as ModifiedBy
	 ,'Clarizen' as SourceSystem
	  ,rd.Initial_risk_factor_score as Initial_risk_factor_score
	  ,rd.date as date
   FROM  {var_client_custom_db}.stage_ovp_dbo_tblder_clarizen_projects AS cp 
  JOIN related_work as rd 
    ON cp.id = rd.planned_for
  
""".format(var_client_custom_db=var_client_custom_db))



# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT *  FROM jll_azara_catalog.jll_azara_0007745730_capitalone_custom.stage_ovp_dbo_tblder_clarizen_projects as cp 
# MAGIC  

# COMMAND ----------

# DBTITLE 1,ovp_vw_Clarizen_RiskRegister
spark.sql(""" 
CREATE OR REPLACE view
{var_client_custom_db}.ovp_vw_Clarizen_RiskRegister as
    SELECT * from {var_client_custom_db}.ovp_vwtbl_Clarizen_RiskRegister
""".format(var_client_custom_db=var_client_custom_db))	  

  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ovp_vw_Clarizen_RiskRegister

# COMMAND ----------

# DBTITLE 1,DW_All_Clients_OVP_vw_Clarizen_ProjectQuantities
spark.sql(f""" 
CREATE OR REPLACE VIEW
{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_ProjectQuantities
AS
SELECT
   rcl.client_id        as Client_Nbr 
  ,rcl.source_system_id as Clarizen_Customer_Nbr
  ,rcl.client_name      as Client
  ,c_project_id         as Job_Nbr
  ,spd.created_on
  ,spd.last_updated_on
  ,spd.name             as Job_Name
  ,CASE WHEN spd.c_unit_of_measure = 'Square Meter' THEN spd.c_gross_area    * 10.764 ELSE spd.c_gross_area             END as Total_Gross_SqFt
  ,CASE WHEN spd.c_unit_of_measure = 'Square Meter' THEN spd.c_gross_area             ELSE spd.c_gross_area    / 10.764 END as Total_Gross_SM
  ,CASE WHEN spd.c_unit_of_measure = 'Square Meter' THEN spd.c_rentable_area          ELSE spd.c_rentable_area / 10.764 END as Total_Rentable_SM
  ,CASE WHEN spd.c_unit_of_measure = 'Square Meter' THEN spd.c_rentable_area * 10.764 ELSE spd.c_rentable_area          END as Total_Rentable_SqFt
  ,CASE WHEN spd.c_unit_of_measure = 'Square Meter' THEN spd.c_usable_area   * 10.764 ELSE spd.c_usable_area            END as Total_Useable_SqFt
  ,CASE WHEN spd.c_unit_of_measure = 'Square Meter' THEN spd.c_usable_area            ELSE spd.c_usable_area   / 10.764 END as Total_Useable_SM
  ,''                   as Total_Acreage      --Coded blank in RED
  ,''                   as Cubes_Workstations --Coded blank in RED
  ,''                   as People_Moved       --Coded blank in RED
  ,''                   as Total_Seats        --Coded blank in RED
  ,''                   as Offices            --Coded blank in RED
  ,''                   as Residential_Units  --Coded blank in RED
  ,''                   as Beds               --Coded blank in RED
  ,''                   as Hotel_Rooms        --Coded blank in RED
  ,''                   as Classrooms         --Coded blank in RED
  ,''                   as Parking_Spaces     --Coded blank in RED
  ,'Clarizen'           as SourceSystem
FROM (SELECT DISTINCT ID FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project WHERE ID IS NOT NULL OR ID <> '' ORDER BY ID) hpt  -- Emulating H_Project Table
JOIN {var_client_custom_db}.datavault_dbo_s_project_details spd ON hpt.id = spd.id
JOIN {var_client_custom_db}.datavault_dbo_r_clients         rcl ON UPPER(rcl.dss_record_source) = 'STAGE_CLARIZEN.DBO.CUSTOMER' AND spd.client_id = rcl.client_code
/*LEFT JOIN (SELECT distinct is_deleted, id, LastUpdatedBy, LastUpdatedOn FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project WHERE UPPER(is_deleted) = 'TRUE') rst ON hpt.id = rst.id 
    WHERE rst.id IS NULL OR rst.id = '' */
""")

# COMMAND ----------

# DBTITLE 1,DW_All_Clients_OVP_vw_Clarizen_ProjectDirectory
spark.sql(""" 
CREATE OR REPLACE VIEW
{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_ProjectDirectory
AS
  SELECT 
    DISTINCT 
     p.Client_Nbr  
	,p.Clarizen_Customer_Nbr
	,p.Job_Nbr
    ,dc.name as Name
    ,dc.c_lastname as Last_Name
    ,dc.c_firstname as First_Name
    ,dc.c_company as Company
    ,dc.c_company_vendor as Company_Vendor
    ,dc.c_type_of_contact AS Type_of_Contact
    ,dc.c_service AS Service
    ,dc.c_job_title AS Job_Title
    ,dc.c_business_phone AS Business_Phone
    ,dc.c_mobile_phone AS Mobile_Phone
    ,dc.c_fax_number AS Fax_Number
    ,dc.c_email_address AS EMail_Address
    ,dc.c_address AS Address
    ,'' AS City
    ,'' AS State
    ,'' AS Postal_Code
    ,'' AS Country
    ,sr.name as Role_in_OVF
    ,'' as ShowOnHomePage
    ,'' as Order_ON_Home_Page  -- not used in OVP4.0
    ,dc.c_comments AS Notes
    ,'Clarizen' as SourceSystem
  -- SELECT COUNT(*) -- 233,759 -- SELECT TOP 500 pd.project, sr.[name], dcp.clz_role_in_spend, dc.* -- SELECT DISTINCT dcp.clz_role_in_spend 
  FROM {var_client_custom_db}.stage_OVP_dbo_tblDer_Clarizen_Projects as p
  JOIN {var_client_custom_db}.datavault_dbo_s_clz_directory_contact_project_links as dcp
    ON p.id = dcp.project
	  AND dcp.client_id is not null
  	 --WHERE c_project_id = '00063P190747'
  JOIN 
  ( SELECT 
       dc.id
      ,dc.name
      ,dc.c_lastname 
      ,dc.c_firstname
      ,dc.c_company
      ,dc.c_company_vendor
      ,dc.c_type_of_contact
      ,dc.c_service
      ,dc.c_job_title
      ,dc.c_business_phone
      ,dc.c_mobile_phone
      ,dc.c_fax_number
      ,dc.c_address 
	  ,dc.c_comments
	  ,dc.c_email_address
	  -- SELECT dc.*  -- 3434
	  FROM {var_client_custom_db}.datavault_dbo_r_c_directory_contacts as dc
	  -- LEFT JOIN	datavault.dbo.r_c_directory_contacts_record_source_tracking as rst with (nolock)
	  --   ON dc.id = rst.id
	  -- WHERE rst.id IS NULL  -- Not Deleted
  ) as dc
    ON dcp.clz_directory_contacts = dc.id
  LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_spend_role as sr
    ON dcp.clz_role_in_spend = sr.id
  --WHERE c_project_id = '01560P15A12E'
  -- LEFT JOIN datavault.dbo.s_clz_directory_contact_project_link_record_source_tracking as dcpl_rst with (nolock)
  --   ON '/C_DirectoryContactProjectLink/' + dcp.external_id = dcpl_rst.id
  -- WHERE dcpl_rst.id IS NULL -- records not deleted
   --AND c_project_id = '10025P200020'
   WHERE 
      ( dc.c_email_address IS NOT NULL OR dc.c_lastname IS NOT NULL OR dc.name IS NOT NULL)
""".format(var_client_custom_db=var_client_custom_db,var_azara_raw_db=var_azara_raw_db))


# COMMAND ----------

