# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook is used to create below views**
# MAGIC 1. DW_All_Clients_OVP_vw_Clarizen_JobSiteAddress
# MAGIC 2. DW_All_Clients_OVP_vw_Clarizen_ProjectDirectory
# MAGIC 3. DW_All_Clients_OVP_vw_Clarizen_Milestones
# MAGIC 4. DW_All_Clients_OVP_vw_Clarizen_Projects
# MAGIC 5. DW_All_Clients_OVP_vw_Clarizen_HealthAndSafety
# MAGIC 6. DW_All_Clients_OVP_vw_Clarizen_CashFlowActuals

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
# from datetime import timezone
# import datetime

# # Creating "Data Refresh Date"
# _date = datetime.datetime.now(timezone.utc)
# jobRunDate = dbutils.jobs.taskValues.get(taskKey="init_pipeline",key="UpdateDate",debugValue='{_date}'.format(_date=_date))
# refresh_date = datetime.datetime.strptime(jobRunDate, '%Y-%m-%d %H:%M:%S.%f%z')

# COMMAND ----------

# DBTITLE 1,DW_All_Clients_OVP_vw_Clarizen_JobSiteAddress
spark.sql(""" 
CREATE OR REPLACE VIEW
{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_JobSiteAddress
AS

    WITH pa as (
      Select pa.id as pa_id, h_js.id as hs_id , h_js.client_Id
	  FROM {var_client_custom_db}.datavault_dbo_s_project_address AS pa
	  JOIN 
            (
            select 
            distinct 
                CJ.id, 
                spa.client_Id
            from 
                {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_jobsite CJ
            left join
                {var_client_custom_db}.datavault_dbo_s_project_address spa
                on CJ.id = spa.c_job_site_address
                where CJ.id is not null
    )
   as h_js
	   ON pa.c_job_site_address = h_js.id )
	  -- JOIN ( SELECT id, MAX(dss_load_date) dss_load_date 
		-- 	FROM datavault.dbo.h_c_job_site WITH (READUNCOMMITTED) GROUP BY id ) as h_js_max 
	  --  ON h_js.id = h_js_max.id
	  --  AND h_js.dss_load_date = h_js_max.dss_load_date
	  --  WHERE  pa.dss_current_version = 1

   SELECT DISTINCT  -- used to remove duplicate records for different clients
    cp.Client_Nbr 
	,cp.Clarizen_Customer_Nbr
    ,cp.Client
    ,cp.Job_Nbr
    ,js.name AS Location
    ,js.c_Client_Property_Code AS Client_Facility_Nbr
    ,'' as BIO
    ,'' as OVCP
	,js.c_address_1 as Address
    ,js.c_city as City
    ,js.c_state_province as State
    ,js.c_postal_code as Postal_Code
	,js.c_country as Country
    ,js.c_job_site_address_e1 as Job_Site_Address
    ,js.c_client_region as PDS_Region
    ,js.c_property_type as Asset_Type
    ,js.c_property_sub_type as Property_SubType
	,c.c_Client_Industry as Industry
    ,cp.RP10 
    ,cp.RP09
    ,cp.RP13
    ,js.c_ovcp_id as OVCPID
    ,js.c_mdm_id as MDM_ID
    ,js.c_latitude as Latitude
    ,js.c_longitude as Longitude
    ,'Clarizen' as SourceSystem

  -- SELECT COUNT(*)  -- 53,499 -- SELECT TOP 50 *
  FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project as p
  JOIN {var_client_custom_db}.stage_OVP_dbo_tblDer_Clarizen_Projects as cp
	ON p.id = cp.id
  JOIN {var_client_custom_db}.datavault_dbo_s_customer AS c
    ON cp.client_id = c.client_id 
	-- and c.dss_current_version = 1
  JOIN pa
   ON p.id = pa.pa_id
  JOIN {var_client_custom_db}.datavault_dbo_s_c_job_site AS js
   ON pa.hs_id = js.id
   AND pa.client_Id = js.client_Id
   WHERE P.ID is not null
  --  AND js.dss_current_version = 1
   --AND pd.client_id = js.client_id  -- need this or 1,200 dups 
  --WHERE pd.c_project_id IN ('25310P160003','30086P170001')
  -- LEFT JOIN datavault.dbo.s_job_site_record_source_tracking as jsrst with (nolock)
  --   ON js.c_job_site_hash_key = jsrst.c_job_site_hash_key
  -- WHERE jsrst.c_job_site_hash_key IS NULL  -- Project not deleted
  """.format(var_client_custom_db=var_client_custom_db,var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name))

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

# DBTITLE 1,DW_All_Clients_OVP_vw_Clarizen_Milestones
spark.sql(""" 
CREATE OR REPLACE VIEW
{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_Milestones
AS
  SELECT 
    Client_Nbr,
    Clarizen_Customer_Nbr,
    Client,
    Job_Nbr,
    Activity_Number,
    Activity,
    Baseline_Start,
    Baseline_Finish,
    Forecast_Start,
    Forecast_Finish,
    Actual_Start,
    Actual_Finish,
    '' as Show_On_Milestones_and_Project,
    Percent_Complete,
    Status_Comments,
    '' as Cause,
    '' as Show_On_Current_Activites,
    Last_Updated_on,
    Last_Updated_By,
    Duration,
    deliverable,
    Milestone_Status,
    SourceSystem
FROM
    {var_client_custom_db}.Stage_OVP_dbo_tblDer_Clarizen_Milestones
WHERE
    Activity_Number IS NOT NULL
""".format(var_client_custom_db=var_client_custom_db))


# COMMAND ----------

# DBTITLE 1,DW_All_Clients_OVP_vw_Clarizen_Projects
spark.sql(""" 
CREATE OR REPLACE VIEW
{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_Projects
AS
   SELECT 
       client.client_id 
	  ,client.source_system_id 
	  ,client.company_id as ovcid
	  ,client.client_name
	  ,pd.sys_id
	  ,c_project_id 
	  ,p.id as project
	  ,pd.name
	  ,pd.id as id --(hash key replacement)
	--   ,pd.project_hash_diff
	  ,pd.dss_update_time
	  ,pd.last_updated_on
	  ,pd.C_Major_Market
	  ,pd.C_Minor_Market
  -- SELECT COUNT(*)  -- 76,409  SELECT TOP 10 p.id, pd.*
  FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project as p
  JOIN {var_client_custom_db}.datavault_dbo_s_project_details AS pd
    ON p.id = pd.id 
	-- and pd.dss_current_version = 1 
  JOIN {var_client_custom_db}.datavault_dbo_r_clients as client
    ON client.dss_record_source = 'stage_clarizen.dbo.customer'
	AND pd.client_id = client.client_code
  WHERE
    p.id IS NOT NULL
--   LEFT JOIN datavault.dbo.s_project_record_source_tracking as rst
--     ON p.id = rst.id  -- 10/8/20 
--   WHERE rst.id IS NULL  -- Project not deleted
     -- AND c_project_id IN ('00022P180078','18693P425501')
    --AND c_project_id IS NULL
   -- IN ('22850P1800--2','22850P180176','22850P180182','22850P180183','22850P180184','22850P180186','22850P190001','22850P190002') ORDER BY c_project_Id
""".format(var_client_custom_db=var_client_custom_db,var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,DW_All_Clients_OVP_vw_Clarizen_HealthAndSafety
# spark.sql("""
# CREATE OR REPLACE view 
# 	{var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_HealthAndSafety
# AS
# SELECT 
# 	Distinct cp.Client_Nbr                                       as Client_Nbr,
# 	cp.Clarizen_Customer_Nbr                                     as Clarizen_Customer_Nbr,
# 	cp.Client                                                    as Client,
# 	cp.Job_Nbr                                                   as Job_Nbr,
# 	h.c_contractor_hours                                         as Contractor_Hours,
# 	h.c_first_aid_treatment_injuries                             as First_Aid_Treatment,
# 	h.c_inductions                                               as Inductions,
# 	h.c_jll_hours                                                as JLL_Hours,
# 	h.c_license_checks                                           as License_Checks,
# 	h.c_lost_time_injuries                                       as Lost_Time_Injuries,
# 	h.c_ltifr                                                    as LTIFR,
# 	(CASE WHEN h.c_total_hours > 0 THEN (h.c_lost_time_injuries * 200000 / h.c_total_hours) 
# 	END)                                                          as LTIFR_US,
# 	h.c_medical_treatment_injuries                                as Medical_Treatment_Injuries,
# 	h.c_report_period                                             as Report_Date,
# 	(Case WHEN cast(month(h.c_report_period) as int) > 9 THEN CAST(month(h.c_report_period) as varchar(30)) 
# 	ELSE '0' + CAST(month(h.c_report_period) as varchar(30)) 
# 	END + ' ' +  month(h.c_report_period))                          as Month,
# 	YEAR(h.c_report_period)                                       as Year,
# 	h.c_most_common_injury                                        as Most_Common_Injury,
# 	h.c_near_miss_incidents                                       as Near_Miss_Incidents,
# 	h.c_no_of_cases                                               as Number_of_Cases,
# 	h.c_no_of_hs_incidents_investigated                           as Number_of_HS_Incidents_Investigated,
# 	h.c_no_of_workers_on_compensation                             as Number_of_Workers_ON_Compensation,
# 	h.c_number_of_working_days_lost                               as Number_of_Working_Days_Lost,
# 	h.c_no_workers_on_rehabilitation                              as Number_Workers_ON_Rehabilitation,
# 	h.c_rejection_reason                                          as Rejection_Reason,
# 	h.c_status                                                    as Status,
# 	h.c_non_conformances_issued                                   as Non_conformances_Issued,
# 	h.c_notes                                                     as Notes,
# 	h.c_pdspm_inspections                                         as PDS_PM_Inspections,
# 	h.c_penalties_received                                        as Penalties_Issued,
# 	h.c_safety_responsibility                                     as Safety_Responsibility,
# 	h.c_site_staff_attending_hs_training                          as Site_Staff_Attending_HS_Training,
# 	h.c_tool_box_talks                                            as Toolbox_Talks,
# 	h.c_total_hours                                               as Total_Hours,
# 	h.c_weekly_inspections_completed_by_site_management           as Weekly_Inspections_Completed_By_Site_Management,
# 	NULL                                                          as Site_Management,
# 	h.c_wms_compliance_checks                                     as WMS_Compliance_Checks,
# 	NULL                                                          as Project_ID,
# 	h.c_number_of_fatalities                                      as Number_of_Fatalities,
# 	h.c_no_of_hs_incidents_reported                               as Number_of_HS_Incidents_Reported,
# 	NULL                                                          as Rejection_Reason_Email_Link,
# 	h.c_trifr                                                     as TRIFR,
# 	(CASE WHEN h.c_total_hours> 0 THEN (h.c_lost_time_injuries + h.c_first_aid_treatment_injuries) * 1000000 / h.c_total_hours 
# 	END)                                                          as TRIFR_US,
# 	'Clarizen'                                                    as SourceSystem
# From 
# 	{var_client_custom_db}.stage_ovp_dbo_tblder_clarizen_projects as cp
# JOIN 
# 	{var_client_custom_db}.datavault_dbo_s_c_hns_report as h
# ON 
# 	cp.id = h.c_work_item_for_health_safety_report
# LEFT JOIN 
# 	(select * from {var_client_custom_db}.datavault_dbo_s_c_hns_report where is_deleted=1) as hns_rst
# on 
# 	h.external_id = replace(hns_rst.id,'/C_HNSReport/','')
# WHERE 
# 	hns_rst.id IS NULL 
# """.format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,DW_All_Clients_OVP_vw_Clarizen_CashFlowActuals
# spark.sql(f""" 
# CREATE OR REPLACE VIEW
# {var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_CashFlowActuals
# AS
# SELECT
#     MAX(CashFlowForecastID) as CashFlowForecastID
#     ,a.Client_Nbr
#     ,a.Clarizen_Customer_Nbr
#     ,a.Job_Nbr
#     ,a.Work_Item_Name
#     ,a.FiscalYear
#     ,a.CostType
#     ,SUM(Period1)            as Period1
#     ,SUM(Period2)            as Period2
#     ,SUM(Period3)            as Period3
#     ,SUM(Period4)            as Period4
#     ,SUM(Period5)            as Period5
#     ,SUM(Period6)            as Period6
#     ,SUM(Period7)            as Period7
#     ,SUM(Period8)            as Period8
#     ,SUM(Period9)            as Period9
#     ,SUM(Period10)           as Period10
#     ,SUM(Period11)           as Period11
#     ,SUM(Period12)           as Period12
#     ,'Clarizen'              as SourceSystem
# FROM (
#     SELECT 
#         pcf.non_labor_resource_time_phase_id as CashFlowForecastID
#         ,rcl.client_id                        as Client_Nbr
#         ,rcl.source_system_id                 as Clarizen_Customer_Nbr
#         ,spd.c_project_id                     as Job_Nbr
#         ,spd.name                             as Work_Item_Name
#         ,YEAR(pcf.date)                       as FiscalYear
#         ,pcf.name                             as CostType
#         ,CASE WHEN MONTH(pcf.date) = 1  THEN pcf.actual_cost ELSE 0 END as Period1
#         ,CASE WHEN MONTH(pcf.date) = 2  THEN pcf.actual_cost ELSE 0 END as Period2
#         ,CASE WHEN MONTH(pcf.date) = 3  THEN pcf.actual_cost ELSE 0 END as Period3
#         ,CASE WHEN MONTH(pcf.date) = 4  THEN pcf.actual_cost ELSE 0 END as Period4
#         ,CASE WHEN MONTH(pcf.date) = 5  THEN pcf.actual_cost ELSE 0 END as Period5
#         ,CASE WHEN MONTH(pcf.date) = 6  THEN pcf.actual_cost ELSE 0 END as Period6
#         ,CASE WHEN MONTH(pcf.date) = 7  THEN pcf.actual_cost ELSE 0 END as Period7
#         ,CASE WHEN MONTH(pcf.date) = 8  THEN pcf.actual_cost ELSE 0 END as Period8
#         ,CASE WHEN MONTH(pcf.date) = 9  THEN pcf.actual_cost ELSE 0 END as Period9
#         ,CASE WHEN MONTH(pcf.date) = 10 THEN pcf.actual_cost ELSE 0 END as Period10
#         ,CASE WHEN MONTH(pcf.date) = 11 THEN pcf.actual_cost ELSE 0 END as Period11
#         ,CASE WHEN MONTH(pcf.date) = 12 THEN pcf.actual_cost ELSE 0 END as Period12
#     FROM 
#         (SELECT 
#             DISTINCT ID 
#         FROM 
#             {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project 
#         WHERE 
#             ID IS NOT NULL 
#             OR ID <> '' 
#         ORDER BY 
#             ID
#         ) hpt  -- Emulating H_Project Table
#     JOIN 
#         {var_client_custom_db}.datavault_dbo_s_project_details  spd 
#         ON hpt.id = spd.id
#     JOIN 
#         {var_client_custom_db}.datavault_dbo_r_clients  rcl 
#         ON UPPER(rcl.dss_record_source) = 'STAGE_CLARIZEN.DBO.CUSTOMER' 
#         AND spd.client_id = rcl.client_code
#  	JOIN 
#         {var_client_custom_db}.datavault_dbo_pit_non_labor_resource_time_phase_cash_flow  pcf 
#         ON hpt.id = pcf.work_item AND 
#         pcf.actual_cost <> 0
# /*LEFT JOIN (SELECT DISTINCT is_deleted, id, LastUpdatedBy, LastUpdatedOn
#                FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project 
#               WHERE UPPER(is_deleted) = 'TRUE') rst ON hpt.id = rst.id */
# /*LEFT JOIN (SELECT DISTINCT is_deleted, id, LastUpdatedOn 
#                FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_nonlaborsrclnkdrct) nlr_rst ON nlr.non_labor_resource_link_direct_id = nlr_rst.id*/
# --	  WHERE (nlr_rst.id IS NULL or nlr_rst.id = '') AND (rst.id IS NULL OR rst.id = '') -- isdeleted from record_source_tracking
#     ) as a
# GROUP BY 
#     a.Client_Nbr, 
#     a.Clarizen_Customer_Nbr, 
#     a.Job_Nbr, 
#     a.Work_Item_Name, 
#     a.FiscalYear, 
#     a.CostType
# """)

# COMMAND ----------

# DBTITLE 1,DW_All_Clients_OVP_vw_Clarizen_PMDCommentary
spark.sql(f"""
CREATE OR REPLACE VIEW {var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_PMDCommentary
AS
WITH Users 
  AS (
    SELECT 
        concat('/User/',u.external_id) as external_id
        ,u.first_name
        ,u.display_name
	FROM 
        jll_azara_catalog.jll_azara_0007745730_capitalone_custom.datavault_dbo_s_users as u
	-- LEFT JOIN 
    --     datavault.dbo.s_users_record_source_tracking as urst
	-- ON 
    --     u.external_id = urst.id
	-- WHERE 
    --     urst.id IS NULL 
)	
SELECT
     pd.Client_Nbr 	   
    ,pd.Clarizen_Customer_Nbr
	,pd.Job_Nbr
    ,psi.clz_type                                                                     as Title
    ,REPLACE(REPLACE(REPLACE(psi.clz_notes,'<p>',''),'&nbsp;',''),'</p>','')          as Comments	
	,psi.created_at                                                                   as created_dt
	,created.display_name                                                             as Created_By
    ,psi.last_updated_at                                                              AS modified_dt
    --,psi.last_updated_on AS modified_dt
    ,users.display_name                                                               as Modified_By
    ,psi.clz_status                                                                   as Status_Grade
    ,'Clarizen'                                                                       as SourceSystem
FROM 
    jll_azara_catalog.jll_azara_0007745730_capitalone_custom.stage_ovp_dbo_tblder_clarizen_projects as pd
JOIN 
    jll_azara_catalog.jll_azara_0007745730_capitalone_custom.datavault_dbo_s_clz_project_status_indicator as psi
ON 
    pd.id = psi.clz_project_status_indicator
LEFT JOIN 
    Users
ON 
    psi.last_updated_by = users.external_id 
LEFT JOIN 
    Users AS created
ON 
    psi.created_by = created.external_id 
-- LEFT JOIN 
--     datavault.dbo.s_clz_project_status_indicator_record_source_tracking as psi_rst
-- ON 
--     '/C_ProjectStatusIndicator/' + psi.external_id = psi_rst.id
-- WHERE 
--     psi_rst.id IS NULL
""")


# COMMAND ----------

spark.sql(""" 
create or replace temporary view Users 
  AS (
    SELECT 
        concat('/user/', u.external_id) as external_id,
        u.external_id as external_1
        ,u.first_name
        ,u.display_name
	FROM 
        jll_azara_catalog.jll_azara_0007745730_capitalone_custom.datavault_dbo_s_users as u
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users limit 10

# COMMAND ----------

