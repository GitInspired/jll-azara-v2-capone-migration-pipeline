# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook is used to create below table**
# MAGIC 1. stage_OVP_dbo_tblDer_Clarizen_Projects

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

# DBTITLE 1,Assigning "refresh_date" (Updatedate) value
from datetime import timezone
import datetime

# Creating "Data Refresh Date"
_date = datetime.datetime.now(timezone.utc)
jobRunDate = dbutils.jobs.taskValues.get(taskKey="init_pipeline",key="UpdateDate",debugValue='{_date}'.format(_date=_date))
refresh_date = datetime.datetime.strptime(jobRunDate, '%Y-%m-%d %H:%M:%S.%f%z')

# COMMAND ----------

# DBTITLE 1,clarizen_clients
spark.sql(""" 
create or replace temporary view clarizen_clients as
	SELECT 
		client_code, 
		client_id, 
		source_system_id, 
		client_name
	FROM 
		{var_client_custom_db}.datavault_dbo_r_clients
	WHERE 
		dss_record_source = 'stage_clarizen.dbo.customer'
		""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from clarizen_clients

# COMMAND ----------

# DBTITLE 1,users
spark.sql(""" 
create or replace temporary view users as	 
  SELECT
    concat('/User/',external_id) as external_id
    ,first_name
    ,display_name
	  ,email
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

# DBTITLE 1,program
spark.sql(""" 
create or replace temporary view program as	
  SELECT 
    Distinct 
    concat('/Program/', external_id) as external_id
    ,sys_id
  FROM {var_client_custom_db}.datavault_dbo_s_program_details
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from program

# COMMAND ----------

# MAGIC %sql
# MAGIC --1--select * from  jll_azara_catalog.jll_azara_0007745730_capitalone_custom.datavault_dbo_s_program_details
# MAGIC
# MAGIC
# MAGIC
# MAGIC --2--select cust.C_OVClientID,cust.* from jll_azara_catalog.jll_azara_raw.edp_snapshot_clarizen_program pgrm
# MAGIC inner join jll_azara_catalog.jll_azara_raw.edp_snapshot_clarizen_customer cust
# MAGIC  on pgrm.C_ProgramCustomer=cust.ID 
# MAGIC  where  cust.C_OVClientID = '7745730'
# MAGIC --select * from jll_azara_catalog.jll_azara_raw.edp_snapshot_clarizen_customer
# MAGIC --select * from program

# COMMAND ----------

# DBTITLE 1,tmpJSA
spark.sql(""" 
create or replace temporary view tmpJSA as	 
  SELECT 
     Job_Nbr
    ,Location
    ,Address
    ,City
    ,State
    ,Postal_Code
    ,Job_Site_Address
    ,Asset_Type
    ,industry
    ,OVCPID
  FROM 
    {var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_JobSiteAddress
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmpJSA

# COMMAND ----------

# DBTITLE 1,tmpDirectoryContacts
spark.sql(""" 
create or replace temporary view tmpDirectoryContacts	 as	 
	SELECT 
		Job_Nbr, 
		Service, 
		MAX(LTRIM(CONCAT(First_Name,' ',Last_Name))) as DisplayName
	FROM 
		{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_ProjectDirectory
	WHERE 
		Service IN ('Lead Architect', 'Lead GC')
	GROUP BY 
		Job_Nbr, 
		Service
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from tmpDirectoryContacts
# MAGIC
# MAGIC SELECT 
# MAGIC 		Job_Nbr, 
# MAGIC 		Service, 
# MAGIC 		MAX(LTRIM(CONCAT(First_Name,' ',Last_Name))) as DisplayName
# MAGIC 	FROM 
# MAGIC 		{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_ProjectDirectory
# MAGIC 	WHERE 
# MAGIC 		Service IN ('Lead Architect', 'Lead GC')
# MAGIC 	GROUP BY 
# MAGIC 		Job_Nbr, 
# MAGIC 		Service

# COMMAND ----------

# DBTITLE 1,s_project_milestones
spark.sql(""" 
create or replace temporary view s_project_milestones as
  SELECT 
    ID, --(hash key replacement)
    client_id, 
    Duration, 
    start_date, 
    percent_completed, 
    actual_start_date, 
    baseline_start_date, 
    due_date, 
    actual_end_date, 
    baseline_due_date, 
    baseline_duration, 
    actual_duration
  FROM 
  {var_client_custom_db}.datavault_dbo_s_project_milestones 
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,Stage_OVP_dbo_s_work_item_current
spark.sql(""" 
create or replace temporary view Stage_OVP_dbo_s_work_item_current as	
  SELECT 
    --  wi.work_item_hash_key 
     project --(hash Key replacement)
    ,sys_id --(hash Key replacement)
    ,id 
    ,client_id --(hash Key replacement)
    ,c_milestone_number
    ,baseline_start_date
    ,start_date
    ,baseline_due_date
    ,due_date
    ,actual_start_date
    ,actual_end_date
    ,percent_completed
    ,Duration
	FROM {var_client_custom_db}.datavault_dbo_s_work_item
  -- ( record source traking not used)
	-- LEFT JOIN datavault.dbo.s_work_item_record_source_tracking as wi_rst WITH (READUNCOMMITTED)
	--   ON wi.id = wi_rst.id
	-- WHERE wi.dss_current_version = 1 and wi_rst.is_deleted IS NULL;
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,s_work_item_details
spark.sql(""" 
create or replace temporary view s_work_item_details as	
	SELECT 
		--  work_item_hash_key
     project --(hash Key replacement)
    ,sys_id --(hash Key replacement)
    ,client_id --(hash Key replacement)				
		,parent_project
		,name
		,last_updated_on
		,c_internal_type
	FROM {var_client_custom_db}.datavault_dbo_s_work_item_details
	WHERE c_internal_type IS NULL
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,tmpvw_stage_ovp_dbo_s_work_item_current
spark.sql(""" 
CREATE OR REPLACE TEMPORARY VIEW stage_ovp_dbo_s_work_item_current as
    SELECT id
		  ,client_id -- (Client_ID used to create hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
          ,project   -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
          ,sys_id    -- (SYS_ID added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
		  ,c_milestone_number
		  ,baseline_start_date
		  ,start_date
		  ,baseline_due_date
		  ,due_date
          ,actual_start_date
          ,actual_end_date
          ,percent_completed
          ,duration
     FROM {var_client_custom_db}.datavault_dbo_s_work_item
/*LEFT JOIN (
        SELECT is_deleted
              ,id
              ,LastUpdatedBy
              ,LastUpdatedOn
              ,NVL(UPPER(Project),'ABC') as Project
              ,NVL(UPPER(sysid),'ABC') as SYSID
              ,'WORKITEM' as record_source
         FROM {var_client_custom_db}.stage_clarizen_dbo_work_item
         ORDER BY batch_timestamp, is_deleted, id, LastUpdatedBy,LastUpdatedOn, Project, SYSID) rs ON wi.id = rs.id
    WHERE rs.is_deleted is NULL or rs.is_deleted = '' */
""".format(var_client_custom_db=var_client_custom_db)) 

# COMMAND ----------

# DBTITLE 1,wid
spark.sql("""
create or replace temporary view wid as	
	 SELECT 
     wi.project
    ,wi.sys_id
    ,wi.client_id
    ,wi.id
    ,wid.parent_project as wid_project
    ,wid.name as wid_name
    ,wid.last_updated_on
    ,wid.c_internal_type
	 FROM Stage_OVP_dbo_s_work_item_current as wi
	 JOIN s_work_item_details as wid
    -- ON wi.work_item_hash_key = wid.work_item_hash_key
		ON wi.project = wid.project AND wi.client_id = wid.client_id AND wi.sys_id = wid.Sys_id
""")

# COMMAND ----------

# DBTITLE 1,s_project_customer
spark.sql("""
create or replace temporary view s_project_customer as	
  SELECT 
    ID, --(hash key replacement)
    c_customer_Type, 
    c_customer_project_category
	FROM 
    {var_client_custom_db}.datavault_dbo_s_project_customer
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,wid_1
spark.sql("""
create or replace temporary view wid_1 as
	SELECT 
		Job_Nbr, 
		MAX(Actual_Start) as Actual_Start
	FROM 
		{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_Milestones
	WHERE 
		Activity_Number = '1'
	GROUP BY 
		Job_Nbr
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,wid_5
spark.sql("""
create or replace temporary view wid_5 as
	SELECT 
		Job_Nbr, 
		MAX(Actual_Finish) as Actual_Finish
	FROM 
		{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_Milestones
	WHERE 
		Activity_Number = '5'
	GROUP BY 
		Job_Nbr
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,pf
spark.sql("""
create or replace temporary view pf as	 	
  SELECT 
    ID,
    total_estimated_cost
    FROM 
    {var_client_custom_db}.datavault_dbo_s_Project_financials
""".format(var_client_custom_db=var_client_custom_db))


# COMMAND ----------

# DBTITLE 1,pfd
spark.sql("""
create or replace temporary view pfd as
SELECT 
  id, --hashkey
  c_global_currency, 
  cost_currency_type, 
  c_exchange_rate, 
  c_project_template
FROM 
  {var_client_custom_db}.datavault_dbo_s_Project_financial_Details
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,tmpComments
spark.sql("""
create or replace temporary view tmpComments as
	  SELECT 
			c_project_id as Job_Nbr, 
			psi.clz_notes as Comments, 
			psi.last_updated_at as modified_dt
	  FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project as p
	  JOIN {var_client_custom_db}.datavault_dbo_s_project_details AS pd
		ON p.id = pd.id
	    -- AND pd.dss_current_version = 1
	  JOIN Clarizen_clients as client
		ON pd.client_id = client.client_code
	  JOIN {var_client_custom_db}.datavault_dbo_s_clz_project_status_indicator as psi
		ON p.id = psi.clz_project_status_indicator
		-- and psi.dss_current_version = 1
	--   LEFT JOIN datavault.dbo.s_clz_project_status_indicator_record_source_tracking as psi_rst
	-- 	ON '/C_ProjectStatusIndicator/' + psi.[external_id] = psi_rst.id
	--   LEFT JOIN datavault.dbo.s_project_record_source_tracking as rst
	--     ON p.id = rst.id  -- 10/8/55 
	  WHERE 
   		-- rst.id IS NULL AND psi_rst.id IS NULL -- isdeleted from record_source_tracking
		P.ID IS NOT NULL AND P.is_deleted = FALSE --(added in place of record source tracking)
	  	AND psi.clz_type = 'Overall Project'
  """.format(var_client_custom_db=var_client_custom_db,var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name ))

# COMMAND ----------

# DBTITLE 1,tmpCommentsLast
spark.sql("""
create or replace temporary view tmpCommentsLast as
Select 
    DISTINCT
    a.Job_Nbr, 
    MAX(a.Comments) as Comments
FROM 
    tmpComments as a
JOIN 
    (SELECT 
        Job_Nbr, 
        MAX(modified_dt) as modified_dt 
    FROM 
        tmpComments 
    GROUP BY 
        Job_Nbr
    ) as b 
ON 
    a.Job_Nbr = b.Job_Nbr AND a.modified_dt = b.modified_dt 
GROUP BY 
    a.Job_Nbr
""")

# COMMAND ----------

# DBTITLE 1,stage_OVP_dbo_tblDer_Clarizen_Projects
spark.sql(""" 
CREATE OR REPLACE table
{var_client_custom_db}.stage_OVP_dbo_tblDer_Clarizen_Projects
AS
SELECT
	Client_Nbr,
	Clarizen_Customer_Nbr,
	Client,
	Job_Nbr,
	Job_Name,
	'' as SentToOVF,
	cast(NULL as TIMESTAMP) as Invoice_Approved_Date,
	cast(NULL as TIMESTAMP) as OVF_Close_Date,
	cast(NULL as TIMESTAMP) as I2C_Creation_Date,
	cast(NULL as TIMESTAMP) as OVF_Creation_Date,
	cast(NULL as TIMESTAMP) as Last_OVF_Modified_Date,
	cast(NULL as TIMESTAMP) as Last_OVF_Approved_Date,
	cast(NULL as TIMESTAMP) as Budget_Approved_Date,
	cast(NULL as TIMESTAMP) as Commitment_Approved_Date,
	cast(NULL as TIMESTAMP) as Budget_Change_Modified_Date,
	cast(NULL as TIMESTAMP) as Commitment_Change_Modified_Date,
	FullURL,
	PHP_Creation_Date,
	Description,
	Team_Lead,
	Project_Manager,
	LeadArchitect as Lead_Architect,
	LeadGC as Lead_GC,
	Email_Address,
	Project_Type,
	Project_Status,
	Project_Open,
	Product_Type,
	Property_Name,
	Address,
	City,
	State,
	Postal_Code,
	Comments,
	Job_Site_Address,
	PDS_Region,
	Location,
	Asset_Type,
	Owned_Leased,
	Industry,
	OVCP,
	RP08,
	RP09,
	RP10,
	RP12,
	RP13,
	RP23,
	RP26,
	RP27,
	RP28,
	RP29,
	RP30,
	AJD01,
	AJD02,
	AJD03,
	AJD04,
	AJD05,
	AJD06,
	AJD07,
	AJD08,
	AJD09,
	AJD10,
	AJD11,
	AJD12,
	AJD13,
	AJD14,
	AJD15,
	AJD16,
	AJD17,
	AJD18,
	AJD19,
	AJD20,
	AJD21,
	AJD22,
	AJD23,
	AJD24,
	AJD25,
	AJD26,
	AJD27,
	AJD28,
	AJD29,
	AJD30,
	Client_Project_Nbr,
	Global_Currency,
	Local_Currency,
	Exchange_Rate,
	'' as GEMS_Project_Num,
	'' as ExcelFinancials,
	RSF,
	USF,
	GSF,
	OVF_Project_Status,
	'' as Post_Edit_Code,
	JLL_Role,
	mile1_Actual_Start,
	mile5_Actual_Finish,
	Major_Market,
	Minor_Market,
	Template,
	Last_OVP_Modified_Date,
	Project_State,
	Overall_Summary,
	Duration,
	'' as DARM_Flag,
	'' as DARM_Exception,
	'' as DARM_Violations,
	'' as DARM_Columns,
	APN,
	Fitout_Type,
	Customer_Type,
	track_status as Track_Status,
	total_estimated_cost as Total_Estimated_Cost,
	risks as Risks,
	Status_Manually_Set,
	Customer_Region,
	Customer_Business_Unit,
	Customer_Project_Category,
	Customer_Work_Type,
	Customer_Project_Type,
	Customer_Project_Sub_Type,
	Customer_Project_Status,
	Start_Date,
	Stage,
	program,
	Percent_Complete,
	Document_Folder,
	CAST('{refresh_date}' as TIMESTAMP) as Warehouse_Load_Date,
	billable as Billable,
	Customer_Project_Manager,
	Business_Line,
	id,
	client_id,
	risk_level as Risk_Level,
	actual_start_date as Project_Actual_Start_Date,
	baseline_start_date as Project_Baseline_Start_Date,
	due_date as Project_Due_Date,
	actual_end_date as Project_Actual_End_Date,
	baseline_due_date as Project_Baseline_Due_Date,
	budget_summary as Budget_Summary,
	schedule_summary as Schedule_Summary,
	Project_Coordinator,
	actual_duration as Actual_Duration ,
	budget_status,
	Mitigation,
	closing_notes,
	last_updated_by as Last_Updated_By,
	Program_sys_id as Program_Sys_ID,
	I2C_Revenue,
	SourceSystem
FROM
	(SELECT 
		client.client_id as Client_Nbr
		,client.source_system_id as Clarizen_Customer_Nbr
		,client.client_name as Client
		,pd.c_project_id as Job_Nbr
		,pd.name AS Job_Name

		,CONCAT('https://app2.clarizen.com/Clarizen/Project/' , SUBSTRING(c_runtime_id,3,11)) as FullURL
		,pd.created_on as PHP_Creation_Date
		,pd.Description as Description
		,teamlead.display_name as Team_Lead
		,projectmgr.display_name as Project_Manager
		,projectmgr.email as Email_Address
		,pt.name as Project_Type
		,(CASE 
			When pd.Phase IN ('Initiate', 'Plan', 'Design', 'Construct', 'Closeout') 
			THEN CONCAT('Active - ', pd.Phase)
			Else pd.Phase
		END) as Project_Status
		,(CASE 
			when pd.State = 'Active' 
			THEN 1 ELSE 0 
		 END) as Project_Open
		,st.name as Product_Type
		,js.Location as Property_Name
		,js.Address as Address
		,js.City as City
		,js.State as State
		,js.Postal_Code as Postal_Code
  		,comm.Comments as Comments
		,js.Job_Site_Address as Job_Site_Address
		,pd.c_region as PDS_Region
		,js.Location as Location
		,js.Asset_Type as Asset_Type
		,'' as Owned_Leased
		,js.industry as Industry
		,js.OVCPID as OVCP
		,pd.c_RP08 as RP08
		,pd.c_RP09 as RP09
		,pd.c_RP10 as RP10
		,pd.c_RP12 as RP12
		,pd.c_RP13 as RP13
		,pd.c_RP23 as RP23
		,pd.c_RP26 as RP26
		,pd.c_RP27 as RP27
		,pd.c_RP28 as RP28
		,pd.c_RP29 as RP29
		,pd.c_RP30 as RP30
		,pd.c_AJD01 as AJD01
		,pd.c_AJD02 as AJD02
		,pd.c_AJD03 as AJD03
		,pd.c_AJD04 as AJD04
		,pd.c_AJD05 as AJD05
		,pd.c_AJD06 as AJD06
		,pd.c_AJD07 as AJD07
		,pd.c_AJD08 as AJD08
		,pd.c_AJD09 as AJD09
		,pd.c_AJD10 as AJD10
		,pd.c_AJD11 as AJD11
		,pd.c_AJD12 as AJD12
		,pd.c_AJD13 as AJD13
		,pd.c_AJD14 as AJD14
		,pd.c_AJD15 as AJD15
		,pd.c_AJD16 as AJD16
		,pd.c_AJD17 as AJD17
		,pd.c_AJD18 as AJD18
		,pd.c_AJD19 as AJD19
		,pd.c_AJD20 as AJD20
		,pd.c_AJD21 as AJD21
		,pd.c_AJD22 as AJD22
		,pd.c_AJD23 as AJD23
		,pd.c_AJD24 as AJD24
		,pd.c_AJD25 as AJD25
		,pd.c_AJD26 as AJD26
		,pd.c_AJD27 as AJD27
		,pd.c_AJD28 as AJD28
		,pd.c_AJD29 as AJD29
		,pd.c_AJD30 as AJD30
		,pd.c_customer_project_number as Client_Project_Nbr 
		,pfd.c_global_currency as Global_Currency
		,pfd.cost_currency_type as Local_Currency
		,cast(pfd.c_exchange_rate as FLOAT) as Exchange_Rate
		,CAST(CASE 
			WHEN pd.c_unit_of_measure = 'Square Meters' THEN pd.c_Rentable_Area * 10.7639 
			ELSE pd.c_rentable_area 
			END as int) as RSF
		,CAST(CASE
			WHEN pd.c_unit_of_measure = 'Square Meters' THEN pd.c_usable_area * 10.7639 
			ELSE pd.c_usable_area 
			END as int) as USF
		,CAST(CASE 
			WHEN pd.c_unit_of_measure = 'Square Meters' THEN pd.c_gross_area * 10.7639 
			ELSE pd.c_gross_area 
			END as int) as GSF
		,ovf.OVF_Project_Status as OVF_Project_Status
		,pd.c_jll_role as JLL_Role
		,wid_1.Actual_Start as mile1_Actual_Start
		,wid_5.Actual_Finish as mile5_Actual_Finish
	  -- Midlevel columns
		,pd.C_Major_Market as Major_Market
		,pd.C_Minor_Market as Minor_Market
		,pfd.c_project_template as Template
		,wid.last_updated_on as Last_OVP_Modified_Date
		,0 as I2C_Revenue
		,pd.c_architect_project_number as APN
		,pd.State as Project_State
		,pd.overall_summary as Overall_Summary
		,pm.Duration
		,pm.actual_start_date -- datetime2
		,pm.baseline_start_date -- datetime2
		,pm.due_date  -- datetime2
		,pm.actual_end_date -- datetime2
		,pm.baseline_due_date  -- datetime2
		,pm.actual_duration  -- decimal(19,4)
		,pd.c_fitout_type as Fitout_Type
		,pc.c_customer_Type as Customer_Type
		,pd.track_status
		,pf.total_estimated_cost
		,pd.track_status_manually_set as Status_Manually_Set
		,pd.risks
		,cv_region.name as Customer_Region
		,cv_bus_unit.name as Customer_Business_Unit
		,cv_proj_cat.name as Customer_Project_Category
		,cv_work_type.name as Customer_Work_Type
		,cv_proj_type.name as Customer_Project_Type
		,cv_proj_subtype.name as Customer_Project_Sub_Type
		,cv_proj_status.name as Customer_Project_Status
		,pm.start_date as Start_Date
		,pd.clz_stage as Stage
		,wid_program.wid_name as program  -- wid_program.program
		,pm.percent_completed as Percent_Complete -- decimal(38,10)
		,pd.clz_document_folder as Document_Folder -- nvarchar(max)
		,LeadArch.DisplayName as LeadArchitect
		,LeadGC.DisplayName as LeadGC
		,pd.billable
		,pd.c_customer_project_manager as Customer_Project_Manager
		,pd.c_business_line as Business_Line
		,p.id
		,pd.client_id
		,pd.c_risk_level as risk_level
		,pd.budget_summary  -- nvarchar(2000)
		,pd.schedule_summary -- nvarchar(2000)
		,projectcoor.display_name as Project_Coordinator -- nvarchar(200)
		,pd.budget_status
		,pd.Mitigation
		,'Clarizen' as SourceSystem
		,pd.closing_notes
		,last_updated.display_name as last_updated_by
		,Program_sys.sys_id as Program_sys_id
		--,null as last_updated_by
	FROM 
    	{var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project as p
	JOIN {var_client_custom_db}.datavault_dbo_s_project_details AS pd
		ON p.id = pd.id
	JOIN clarizen_clients as client
		ON pd.client_id = client.client_code
	JOIN pf AS pf
		ON pd.ID = pf.ID 
	JOIN pfd AS pfd
		ON pd.ID = pfd.ID 
	JOIN s_project_milestones as pm
		ON  pd.ID = pm.ID
		AND pd.client_id = pm.client_id
	LEFT JOIN s_project_customer AS pc
		ON pd.ID = pc.ID
		-- added 10/7/19
	LEFT JOIN wid as wid_program
			ON pd.program = wid_program.id
	LEFT JOIN wid as wid
		ON p.id = wid.id 
		AND pd.client_id = wid.client_id
	LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_service_type AS st
		ON pd.c_service_type = st.id 
	LEFT JOIN tmpDirectoryContacts	as LeadArch
	ON pd.c_project_id = LeadArch.Job_Nbr
	AND LeadArch.Service = 'Lead Architect'
	LEFT JOIN tmpDirectoryContacts	as LeadGC
	ON pd.c_project_id = LeadGC.Job_Nbr
	AND LeadGC.Service = 'Lead GC'
	LEFT JOIN users AS teamlead
		ON pd.c_team_lead = teamlead.external_id 
	LEFT JOIN users AS projectmgr
		ON pd.project_manager = projectmgr.external_id 
	LEFT JOIN users AS projectcoor
		ON pd.c_project_coordinator = projectcoor.external_id 
	LEFT JOIN users AS last_updated
		ON pd.last_updated_by = last_updated.external_id 
	LEFT JOIN program As Program_sys
		ON program_sys.external_id=pd.program
	LEFT JOIN wid_1 as wid_1
	ON pd.c_project_id = wid_1.Job_Nbr
	LEFT JOIN wid_5 as wid_5
	ON pd.c_project_id = wid_5.Job_Nbr
	LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_JLL_Project_Types AS pt
		ON pd.c_jll_project_type = pt.id 
	LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_custom_value as cv_region
		ON pd.clz_customer_region = cv_region.id
	LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_custom_value as cv_bus_unit
		ON pd.clz_customer_business_unit = cv_bus_unit.id
	LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_custom_value as cv_proj_cat
		ON pc.c_customer_project_category = cv_proj_cat.id
	LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_custom_value as cv_work_type
		ON pd.clz_customer_work_type = cv_work_type.id
	LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_custom_value as cv_proj_type
		ON pd.clz_customer_project_type = cv_proj_type.id
	LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_custom_value as cv_proj_subtype
		ON pd.clz_customer_project_subtype = cv_proj_subtype.id
	LEFT JOIN {var_client_custom_db}.datavault_dbo_r_clz_custom_value as cv_proj_status
		ON pd.clz_customer_project_status = cv_proj_status.id
	LEFT JOIN tmpJSA as js
		ON pd.c_project_id = js.Job_Nbr
	LEFT JOIN tmpCommentsLast as comm
		ON pd.c_project_id = comm.Job_Nbr
	LEFT JOIN 
		(Select 
  			distinct 
     		LTRIM(RTRIM(MCMCU)) as MCMCU, 
       		b.drdl01 as OVF_Project_Status
        From 
        	{var_client_custom_db}.raw_ref_f0006_capone a
        left join 
        	(select 
         		* 
        	from 
         		{var_client_custom_db}.raw_ref_f0005
           	where 
            	DRSY = '00' 
             	and drrt = '01'
            ) b
        on ltrim(rtrim(a.MCRP01)) = ltrim(Rtrim(b.DRKY))) as ovf
    ON pd.c_project_id = ovf.MCMCU
	WHERE
		P.ID IS NOT NULL AND P.is_deleted = FALSE --(added in place of record source tracking)
	  -- Record Source Tracking tables
	  -- LEFT JOIN datavault.dbo.r_clz_custom_value_record_tracking as cv_rst
		-- ON pd.c_jll_project_type = cv_rst.id
	  --LEFT JOIN datavault.dbo.s_work_item_record_source_tracking as wi_rst WITH (READUNCOMMITTED)
		--ON wi.id = wi_rst.id
	  -- LEFT JOIN datavault.dbo.s_project_record_source_tracking as rst
	  --   ON p.id = rst.id  -- 10/8/55 
	  -- WHERE rst.id IS NULL AND cv_rst.id IS NULL -- Project not deleted
	   --AND pd.c_project_id in ('P4036403','P4045702','P4037213','P4051812','P4037509')
		--and pd.c_project_id in ('22296P3C6D5C','22296P3C6D5D','22296P3C6D8E','22296P3C6D41')
	)
""".format(var_client_custom_db=var_client_custom_db,var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,Merge to stage_OVP_dbo_tblDer_Clarizen_Projects
spark.sql(""" 
    MERGE INTO 
        {var_client_custom_db}.stage_OVP_dbo_tblDer_Clarizen_Projects as target
    USING 
        ( 
            SELECT 
                DISTINCT 
                Client_id, 
                source_system_id as Clarizen_Customer_Nbr, 
                c_project_id as Job_Nbr
            FROM 
                {var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_Projects
            WHERE 
                c_project_id IS NOT NULL
        ) source
    ON target.Job_Nbr  = source.Job_Nbr
    
    WHEN NOT MATCHED BY TARGET
    THEN INSERT 
        (Client_Nbr, Clarizen_Customer_Nbr,Job_Nbr)
    VALUES 
        (source.Client_id,source.Clarizen_Customer_Nbr,source.Job_Nbr)
    
    WHEN NOT MATCHED BY SOURCE
    THEN DELETE

""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# %sql
# UPDATE jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.Stage_OVP_dbo_tblDer_Clarizen_Projects as b
# join tmpClarizenProjects as a
#   ON a.Job_Nbr = b.Job_Nbr
# SET
#   b.Client_Nbr = a.Client_Nbr,
#   b.Clarizen_Customer_Nbr = a.Clarizen_Customer_Nbr,
#   b.Client = a.Client,
#   b.Job_Nbr = a.Job_Nbr,
#   b.Job_Name = a.Job_Name,
#   b.SentToOVF = a.SentToOVF,
#   b.Invoice_Approved_Date = a.Invoice_Approved_Date,
#   b.OVF_Close_Date = a.OVF_Close_Date,
#   b.I2C_Creation_Date = a.I2C_Creation_Date,
#   b.OVF_Creation_Date = a.OVF_Creation_Date,
#   b.Last_OVF_Modified_Date = a.Last_OVF_Modified_Date,
#   b.Last_OVF_Approved_Date = a.Last_OVF_Approved_Date,
#   b.Budget_Approved_Date = a.Budget_Approved_Date,
#   b.Commitment_Approved_Date = a.Commitment_Approved_Date,
#   b.Budget_Change_Modified_Date = a.Budget_Change_Modified_Date,
#   b.Commitment_Change_Modified_Date = a.Commitment_Change_Modified_Date,
#   b.FullURL = a.FullURL,
#   b.PHP_Creation_Date = a.PHP_Creation_Date,
#   b.Description = a.Description,
#   b.Team_Lead = a.Team_Lead,
#   b.Project_Manager = a.Project_Manager,
#   b.Lead_Architect = a.Lead_Architect,
#   b.Lead_GC = a.Lead_GC,
#   b.Email_Address = a.Email_Address,
#   b.Project_Type = a.Project_Type,
#   b.Project_Status = a.Project_Status,
#   b.Project_Open = a.Project_Open,
#   b.Product_Type = a.Product_Type,
#   b.Property_Name = a.Property_Name,
#   b.Address = a.Address,
#   b.City = a.City,
#   b.State = a.State,
#   b.Postal_Code = a.Postal_Code,
#   b.Comments = a.Comments,
#   b.Job_Site_Address = a.Job_Site_Address,
#   b.PDS_Region = a.PDS_Region,
#   b.Location = a.Location,
#   b.Asset_Type = a.Asset_Type,
#   b.Owned_Leased = a.Owned_Leased,
#   b.Industry = a.Industry,
#   b.OVCP = a.OVCP,
#   b.RP08 = a.RP08,
#   b.RP09 = a.RP09,
#   b.RP10 = a.RP10,
#   b.RP12 = a.RP12,
#   b.RP13 = a.RP13,
#   b.RP23 = a.RP23,
#   b.RP26 = a.RP26,
#   b.RP27 = a.RP27,
#   b.RP28 = a.RP28,
#   b.RP29 = a.RP29,
#   b.RP30 = a.RP30,
#   b.AJD01 = a.AJD01,
#   b.AJD02 = a.AJD02,
#   b.AJD03 = a.AJD03,
#   b.AJD04 = a.AJD04,
#   b.AJD05 = a.AJD05,
#   b.AJD06 = a.AJD06,
#   b.AJD07 = a.AJD07,
#   b.AJD08 = a.AJD08,
#   b.AJD09 = a.AJD09,
#   b.AJD10 = a.AJD10,
#   b.AJD11 = a.AJD11,
#   b.AJD12 = a.AJD12,
#   b.AJD13 = a.AJD13,
#   b.AJD14 = a.AJD14,
#   b.AJD15 = a.AJD15,
#   b.AJD16 = a.AJD16,
#   b.AJD17 = a.AJD17,
#   b.AJD18 = a.AJD18,
#   b.AJD19 = a.AJD19,
#   b.AJD20 = a.AJD20,
#   b.AJD21 = a.AJD21,
#   b.AJD22 = a.AJD22,
#   b.AJD23 = a.AJD23,
#   b.AJD24 = a.AJD24,
#   b.AJD25 = a.AJD25,
#   b.AJD26 = a.AJD26,
#   b.AJD27 = a.AJD27,
#   b.AJD28 = a.AJD28,
#   b.AJD29 = a.AJD29,
#   b.AJD30 = a.AJD30,
#   b.Client_Project_Nbr = a.Client_Project_Nbr,
#   b.Global_Currency = a.Global_Currency,
#   b.Local_Currency = a.Local_Currency,
#   b.Exchange_Rate = a.Exchange_Rate,
#   b.GEMS_Project_Num = a.GEMS_Project_Num,
#   b.ExcelFinancials = a.ExcelFinancials,
#   b.RSF = a.RSF,
#   b.USF = a.USF,
#   b.GSF = a.GSF,
#   b.OVF_Project_Status = a.OVF_Project_Status,
#   b.Post_Edit_Code = a.Post_Edit_Code,
#   b.JLL_Role = a.JLL_Role,
#   b.mile1_Actual_Start = a.mile1_Actual_Start,
#   b.mile5_Actual_Finish = a.mile5_Actual_Finish,
#   b.Major_Market = a.Major_Market,
#   b.Minor_Market = a.Minor_Market,
#   b.Template = a.Template,
#   b.Last_OVP_Modified_Date = a.Last_OVP_Modified_Date,
#   b.Project_State = a.Project_State,
#   b.Overall_Summary = a.Overall_Summary,
#   b.Duration = a.Duration,
#   b.DARM_Flag = a.DARM_Flag,
#   b.DARM_Exception = a.DARM_Exception,
#   b.DARM_Violations = a.DARM_Violations,
#   b.DARM_Columns = a.DARM_Columns,
#   b.APN = a.APN,
#   b.Fitout_Type = a.Fitout_Type,
#   b.Customer_Type = a.Customer_Type,
#   b.Track_Status = a.Track_Status,
#   b.Total_Estimated_Cost = a.Total_Estimated_Cost,
#   b.Risks = a.Risks,
#   b.Status_Manually_Set = a.Status_Manually_Set,
#   b.Customer_Region = a.Customer_Region,
#   b.Customer_Business_Unit = a.Customer_Business_Unit,
#   b.Customer_Project_Category = a.Customer_Project_Category,
#   b.Customer_Work_Type = a.Customer_Work_Type,
#   b.Customer_Project_Type = a.Customer_Project_Type,
#   b.Customer_Project_Sub_Type = a.Customer_Project_Sub_Type,
#   b.Customer_Project_Status = a.Customer_Project_Status,
#   b.Start_Date = a.Start_Date,
#   b.Stage = a.Stage,
#   b.program = a.program,
#   b.Percent_Complete = a.Percent_Complete,
#   b.Document_Folder = a.Document_Folder,
#   b.Warehouse_Load_Date = a.Warehouse_Load_Date,
#   b.Billable = a.Billable,
#   b.Customer_Project_Manager = a.Customer_Project_Manager,
#   b.Business_Line = a.Business_Line,
#   b.id = a.id,
#   b.client_id = a.client_id,
#   b.Risk_Level = a.Risk_Level,
#   b.Project_Actual_Start_Date = a.Project_Actual_Start_Date,
#   b.Project_Baseline_Start_Date = a.Project_Baseline_Start_Date,
#   b.Project_Due_Date = a.Project_Due_Date,
#   b.Project_Actual_End_Date = a.Project_Actual_End_Date,
#   b.Project_Baseline_Due_Date = a.Project_Baseline_Due_Date,
#   b.Budget_Summary = a.Budget_Summary,
#   b.Schedule_Summary = a.Schedule_Summary,
#   b.Project_Coordinator = a.Project_Coordinator,
#   b.Actual_Duration = a.Actual_Duration,
#   b.budget_status = a.budget_status,
#   b.Mitigation = a.Mitigation,
#   b.closing_notes = a.closing_notes,
#   b.Last_Updated_By = a.Last_Updated_By,
#   b.Program_Sys_ID = a.Program_Sys_ID,
#   b.I2C_Revenue = a.I2C_Revenue,
#   b.SourceSystem = a.SourceSystem

# COMMAND ----------

# %sql
# MERGE INTO jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.stage_OVP_dbo_tblDer_Clarizen_Projects as target 
# USING tmpClarizenProjects as source
#   ON target.Job_Nbr = source.Job_Nbr
#   WHEN MATCHED THEN UPDATE SET *

# COMMAND ----------

# %sql
# select Count(*) from tmpClarizenProjects 
# where Job_Nbr  in (
# select Job_Nbr from 
# (
# select Job_Nbr, count(*) from jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.stage_OVP_dbo_tblDer_Clarizen_Projects group by 1 having count(*) > 1
# )) ;

# COMMAND ----------

# %sql
# -- select Job_Nbr, count(*) from jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.stage_OVP_dbo_tblDer_Clarizen_Projects group by 1 having count(*) > 1;

# COMMAND ----------

# %sql
# select Job_Nbr,id,* from jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.stage_OVP_dbo_tblDer_Clarizen_Projects
# where Job_Nbr in (
# select Job_Nbr from 
# (
# select Job_Nbr, count(*) from jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.stage_OVP_dbo_tblDer_Clarizen_Projects group by 1 having count(*) > 1
# )) order by Job_Nbr;

# create or replace table jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.stage_OVP_dbo_tblDer_Clarizen_Projects
# as select * from jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.stage_OVP_dbo_tblDer_Clarizen_Projects
# where Job_Nbr not in (
# select Job_Nbr from 
# (
# select Job_Nbr, count(*) from jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.stage_OVP_dbo_tblDer_Clarizen_Projects group by 1 having count(*) > 1));

# COMMAND ----------

# %sql
# create or replace temporary view tmpClarizenProjects
# as select * from temp_tmpClarizenProjects
# where Job_Nbr not in (
# select Job_Nbr from 
# (
# select Job_Nbr, count(*) from temp_tmpClarizenProjects group by 1 having count(*) > 1));