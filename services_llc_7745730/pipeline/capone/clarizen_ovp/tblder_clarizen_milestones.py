# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook is used to create below table**
# MAGIC 1. stage_ovp_dbo_tblder_clarizen_milestones

# COMMAND ----------

# DBTITLE 1,client variables
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
CREATE OR REPLACE TEMPORARY VIEW clarizen_clients as
    SELECT client_code
          ,client_id
	      ,source_system_id
		  ,client_name
     FROM {var_client_custom_db}.datavault_dbo_r_clients
    WHERE UPPER(dss_record_source) = 'STAGE_CLARIZEN.DBO.CUSTOMER'
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,users
spark.sql(""" 
CREATE OR REPLACE TEMPORARY VIEW users as
    SELECT CONCAT('/User/',external_id) as external_id
           ,display_name
     FROM {var_client_custom_db}.datavault_dbo_s_users
/*  LEFT JOIN (SELECT is_deleted, id, lastupdatedon, username FROM {var_azara_raw_db}.edp_snapshot_clarizen_user WHERE is_deleted = '1') urst on u.external_id = urst.id
    WHERE (urst.id IS NULL or urst.id = '') -- count is the same with record_source_tracking join */
""".format(var_client_custom_db=var_client_custom_db,var_azara_raw_db=var_azara_raw_db))   

# COMMAND ----------

# DBTITLE 1,work_item_detail
spark.sql(""" 
CREATE OR REPLACE TEMPORARY VIEW work_item_detail as
    SELECT sys_id      -- (Sys_ID used to create hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
		  ,client_id   -- (Client_ID used to create hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
          ,project     -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
          ,deliverable
          ,track_status
		  ,c_internal_type
		  ,c_milestone_number
		  ,parent_project
		  ,name
		  ,description
		  ,last_updated_on
		  ,last_updated_by
     FROM {var_client_custom_db}.datavault_dbo_s_work_item_details
""".format(var_client_custom_db=var_client_custom_db)) 

# COMMAND ----------

# DBTITLE 1,stage_ovp_dbo_s_work_item_current
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

# DBTITLE 1,datavault_dbo_s_work_item_milestone_details
spark.sql(""" 
CREATE OR REPLACE TEMPORARY VIEW datavault_dbo_s_work_item_milestone_details as
    SELECT distinct 
		   client_id                         as client_id
		  ,NVL(UPPER(m.Project),'ABC')       as project -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
          ,NVL(UPPER(m.sysid),'ABC')         as sys_id  -- (SYS_ID added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
		  ,C_Customertask                    as c_customer_task
		  ,Name                              as name
		  ,ParentProject                     as parent_project
		  ,Parent                            as parent
		  ,BudgetStatus                      as budget_status
		  ,LikesCount                        as likes_count
		  ,PostsCount                        as posts_count    
		  ,Phase                             as phase    
		  ,LastUpdatedBy                     as last_updated_by
		  ,LastUpdatedOn                     as last_updated_on      
          ,CreatedBy                         as created_by
		  ,CreatedOn                         as created_on
		  ,Description                       as description
		  ,C_ServiceProductE1Code            as c_service_product_e1_code
		  ,C_ProgramType                     as c_program_type
		  ,C_ScopeStatus                     as c_scope_status
		  ,C_MilestoneNumber                 as c_milestone_number
		  ,C_RiskRegistryScore               as c_risk_registry_score
		  ,C_RiskRegisterStatus              as c_risk_register_status
		  ,C_ReportingName                   as c_reporting_name
		  ,C_ReportingId                     as c_reporting_id
		  ,C_ScheduleStatus                  as c_schedule_status
		  ,C_Vendor                          as c_vendor
		  ,C_RuntimeId                       as c_runtime_id
		  ,C_InternalType                    as c_internal_type
		  ,C_ProjectBuildingSize             as c_project_building_size
		  ,C_ServiceProduct                  as c_service_product
		  ,MilestoneType                     as milestonetype
		  ,''                                as clz_total_estimated_effort
		  ,''                                as clz_sprint_remaining_effort
		  ,''                                as clz_sprint_estimated_effort
		  ,''                                as clz_sprint_story_count
		  ,C_NextGenProgram                  as clz_nextgen_program
		  ,C_ProjectProgramforMeetingMinutes as clz_project_program_for_meeting_minutes
		  ,''                                as clz_sprint_status
		  ,C_Visibletocustomer               as c_visible_to_customer
     	  ,C_AJD30
		  ,C_AJD29
		  ,C_AJD28
		  ,C_AJD26
		  ,C_AJD21
		  ,C_AJD27
		  ,C_AJD25
		  ,C_AJD24
		  ,C_AJD23
		  ,C_AJD22
		  ,C_AJD20
		  ,C_AJD19
		  ,C_AJD18
		  ,C_AJD16
		  ,C_AJD17
		  ,C_AJD15
		  ,C_AJD14
		  ,C_AJD13
		  ,C_AJD12
		  ,C_AJD11
		  ,C_AJD09
		  ,C_AJD10
		  ,C_AJD08
		  ,C_AJD07
		  ,C_AJD06
		  ,C_AJD05
		  ,C_AJD04
		  ,C_AJD03
		  ,C_AJD02
		  ,C_AJD01
		  ,C_RP30
		  ,C_RP29
		  ,C_RP28
		  ,C_RP27
		  ,C_RP26
		  ,C_RP23
		  ,C_RP13
		  ,C_RP12
		  ,C_RP10
		  ,C_RP09
		  ,C_RP08
		  ,'MILESTONE'                       as dss_record_source 
    	  ,LastUpdatedBySystemOn             as last_updated_by_system_on_at
     FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_milestone m
     JOIN (SELECT wi.project, wi.sysid, 'WORKITEM' as record_source, rc.client_id
                FROM {var_client_custom_db}.stage_clarizen_dbo_workitem wi
            LEFT JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where UPPER(dss_record_source) = 'PROJECT') rc
                  ON rc.id = wi.project
            ORDER BY project, sysid) h
       ON NVL(UPPER(m.Project),'ABC') = NVL(UPPER(h.Project),'ABC') 
      AND NVL(UPPER(m.sysid),'ABC')   = NVL(UPPER(h.sysid),'ABC')
    WHERE UPPER(is_deleted) = 'FALSE'
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name ))

# COMMAND ----------

# DBTITLE 1,tmp_milestone_details
spark.sql(""" 
CREATE OR REPLACE TEMPORARY VIEW tmp_milestone_details as
    SELECT wid.sys_id    -- (Sys_ID added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
	      ,wid.deliverable
		  ,wid.track_status
		  ,wic.baseline_start_date
		  ,wic.start_date
		  ,wic.baseline_due_date
		  ,wic.due_date
		  ,wic.actual_start_date
		  ,wic.actual_end_date
		  ,wic.percent_completed
		  ,wic.duration
          ,wim.project   -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
		  ,wim.client_id -- (Client_ID used to create hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
		  ,wim.parent_project
		  ,wim.c_milestone_number
		  ,wim.name
		  ,wim.description
		  ,wim.last_updated_on
		  ,usr.display_name as last_updated_by
     FROM stage_ovp_dbo_s_work_item_current            wic
     JOIN work_item_detail                             wid  ON wic.client_id = wid.client_id AND wic.project = wid.project AND wic.sys_id = wid.sys_id -- Field comparisons to substitute for work_item_hash_key
		                                                   AND NVL(wic.c_milestone_number,0) = NVL(wid.c_milestone_number,0)    
		                                                   AND wid.sys_id LIKE 'M-%' 
                                                           AND (wid.c_internal_type IS NULL or wid.c_internal_type = '') 
     JOIN datavault_dbo_s_work_item_milestone_details  wim  ON wic.client_id = wim.client_id AND wic.project = wim.project AND wic.sys_id = wim.sys_id -- Field comparisons to substitute for work_item_hash_key
		                                                   AND NVL(wic.c_milestone_number,0) = NVL(wim.c_milestone_number,0)
LEFT JOIN users                                        usr  ON wim.last_updated_by = usr.external_id
    WHERE (wim.client_id IS NOT NULL or wim.client_id <> '')
""") 

# COMMAND ----------

# DBTITLE 1,tmp_task_details
spark.sql(""" 
CREATE OR REPLACE TEMPORARY VIEW tmp_task_details as
    SELECT client_id          as client_id -- (Client_ID added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
          ,UPPER(sys_id)      as sys_id    -- (Sys_ID added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
          ,UPPER(project)     as project   -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
	      ,c_milestone_number as c_milestone_number
     FROM {var_client_custom_db}.datavault_dbo_s_work_item_task_details
""".format(var_client_custom_db=var_client_custom_db)) 

# COMMAND ----------

# DBTITLE 1,tmp_wi_details
spark.sql(""" 
CREATE OR REPLACE TEMPORARY VIEW tmp_wi_details as
   SELECT wid.sys_id
	     ,wid.deliverable
		 ,wid.track_status
		 ,wic.baseline_start_date
		 ,wic.start_date
		 ,wic.baseline_due_date
		 ,wic.due_date
		 ,wic.actual_start_date
		 ,wic.actual_end_date
		 ,wic.percent_completed
		 ,wic.duration
		 ,wid.project -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
         ,wid.client_id
		 ,wid.parent_project
		 ,wid.c_milestone_number
		 ,wid.name
		 ,wid.description
		 ,wid.last_updated_on
		 ,usr.display_name as last_updated_by     
     FROM stage_ovp_dbo_s_work_item_current            wic   
	 JOIN work_item_detail                             wid  ON wic.client_id = wid.client_id AND wic.project = wid.project AND wic.sys_id = wid.sys_id -- Field comparisons to substitute for work_item_hash_key
	                                                       AND NVL(wic.c_milestone_number,0) = NVL(wid.c_milestone_number,0)    
	                                                       AND (wid.c_internal_type IS NULL or wid.c_internal_type = '')
                                                           AND wid.sys_id LIKE 'T-%'      
	 JOIN tmp_task_details                             wit  ON wic.client_id = wit.client_id AND wic.project = wit.project AND wic.sys_id = wit.sys_id -- Field comparisons to substitute for work_item_hash_key
                                                           AND NVL(wic.c_milestone_number,0) = NVL(wit.c_milestone_number,0)     
LEFT JOIN users                                        usr  ON wid.last_updated_by = usr.external_id
    WHERE (wid.client_id IS NOT NULL or wid.client_id <> '')
""") 

# COMMAND ----------

# DBTITLE 1,stage_ovp_dbo_tblder_clarizen_milestones
spark.sql("""
CREATE OR REPLACE TABLE {var_client_custom_db}.stage_ovp_dbo_tblder_clarizen_milestones as
    SELECT cl.client_id                                              as client_nbr
		  ,cl.source_system_id                                       as clarizen_customer_nbr
		  ,cl.client_name                                            as client
		  ,pd.job_nbr                                                as job_nbr
		  ,CAST(CAST(mi.c_milestone_number as float) as varchar(25)) as activity_number
		  ,CASE WHEN (mi.c_milestone_number IS NULL or mi.c_milestone_number = '') THEN mi.name 
                ELSE CONCAT(CAST(CAST(mi.c_milestone_number as float) as varchar(25)), '. ', mi.name)
           END                                                       as activity
		  ,mi.baseline_start_date                                    as baseline_start
		  ,mi.baseline_due_date                                      as baseline_finish
		  ,mi.start_date                                             as forecast_start
		  ,mi.due_date                                               as forecast_finish
		  ,mi.actual_start_date                                      as actual_start
		  ,mi.actual_end_date                                        as actual_finish
          ,CAST(mi.percent_completed/100 as decimal(5,2))            as percent_complete
		  ,LEFT(mi.description,1000)                                 as status_comments
		  ,mi.last_updated_on                                        as last_updated_on
		  ,mi.last_updated_by                                        as last_updated_by
		  ,mi.duration                                               as duration
		  ,'Clarizen'                                                as sourcesystem
		  ,mi.parent_project                                         as parent_project
          ,mi.client_id                                              as client_id -- (Client_ID added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
          ,mi.project                                                as project   -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
		  ,mi.sys_id                                                 as sys_id    -- (Sys_ID used to create hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
		  ,CASE WHEN (mi.deliverable IS NOT NULL or mi.deliverable <> '') THEN 1 
            END                                                      as deliverable
		  ,CAST('{refresh_date}' as TIMESTAMP)                       as warehouse_load_date
		  ,mi.track_status                                           as milestone_status
     FROM {var_client_custom_db}.stage_ovp_dbo_tblder_clarizen_projects pd    
	 JOIN clarizen_clients                                              cl ON pd.clarizen_customer_nbr = cl.source_system_id 
	 JOIN (SELECT sys_id, deliverable, track_status, baseline_start_date, start_date, baseline_due_date, due_date, actual_start_date, actual_end_date, percent_completed,
	              duration, client_id, parent_project, c_milestone_number, name, description, last_updated_on, last_updated_by, project --(Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
		     FROM tmp_milestone_details
		    UNION ALL
		   SELECT sys_id, deliverable, track_status, baseline_start_date, start_date, baseline_due_date, due_date, actual_start_date, actual_end_date, percent_completed,
	              duration, client_id, parent_project, c_milestone_number, name, description, last_updated_on, last_updated_by, project --(Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
		     FROM tmp_wi_details
	      ) as mi ON pd.id = mi.parent_project AND cl.client_code = mi.client_id
""".format(var_client_custom_db=var_client_custom_db, refresh_date=refresh_date)) 