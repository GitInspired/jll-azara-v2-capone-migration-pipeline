# Databricks notebook source
# MAGIC %md
# MAGIC ###### Notebook for Corrigo Presentation Layer Views

# COMMAND ----------

# DBTITLE 1,client_variables
import os
from azarautils import ClientObject

# Client Config
client_obj           = ClientObject(client_id=os.getenv("CLIENT_ID"),client_name=os.getenv("CLIENT_NAME"))
client_secret_scope  = client_obj.client_secret_scope
catalog              = client_obj.catalog

var_client_id        = os.getenv("CLIENT_ID")
var_tenant_id        = "12098922878811"

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

# MAGIC %md

# COMMAND ----------

# DBTITLE 1,ssdv.vw_Corrigo_vbiUsers
'''
Version: 1, Creation Date:03/08/2023 , Created By: Vinayak Bhanoo
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiUsers
        AS
            WITH report_to
               AS
                  (
                    SELECT
                      employee_id reports_to_id,
                      -- CONCAT(first_name, ' ',last_name) AS Reports_To
                      user_screen_name AS Reports_To
                    FROM {var_client_custom_db}.raw_employees_employees_corrigo
                   ),
            Completion_Date
               AS 
                   (
                    SELECT 
                       employee_id, 
                       action_id, 
                       MAX(effective_at) AS Last_WO_Completion_Date
                    FROM {var_client_custom_db}.raw_work_order_activity_logs_wo_activitylogs_corrigo
                    WHERE action_id = 6 GROUP BY employee_id, action_id
                   ),
            Acceptance_Date
               AS 
                   (
                    SELECT 
                       employee_id, 
                       action_id, 
                       MAX(effective_at) AS Last_WO_Acceptance_Date
                    FROM {var_client_custom_db}.raw_work_order_activity_logs_wo_activitylogs_corrigo
                    WHERE action_id = 3 GROUP BY employee_id,action_id
                   )
            SELECT DISTINCT
                   h_masterClientsTen.client_Name AS Client_Name,
                   h_masterClientsTen.client_id AS Company_ID,
                   custom_dv_employees.employee_id AS ID,
                   custom_dv_employees.first_name AS First_Name,
                   custom_dv_employees.last_name AS Last_Name,
                   custom_dv_employees.user_screen_name AS Display_Name,
                   custom_dv_employees.login AS User_ID,
                   custom_dv_employees.role AS Role,
                   custom_dv_employees.job_title AS Job_Title,
                   custom_dv_employees.federal_id AS Federal_ID,--_Number,
                   custom_dv_employees.employee_number AS User_Number,
                   custom_dv_employees.organization AS Organization,
                   custom_dv_employees.language AS Language,
                   CAST(date_format(custom_dv_employees.last_action_taken_at, 'yyyy-MM-dd HH:mm:ss.SSSSSSS') as TIMESTAMP) as Date_Time_Last_Action,
                   CAST(date_format(Last_WO_Acceptance_Date.Last_WO_Acceptance_Date, 'yyyy-MM-dd HH:mm:ss.SSSSSSS') as TIMESTAMP) as Last_WO_Acceptance_Date,
                   CAST(date_format(Last_WO_Completion_Date.Last_WO_Completion_Date, 'yyyy-MM-dd HH:mm:ss.SSSSSSS') as TIMESTAMP) as Last_WO_Completion_Date,
                   custom_dv_employees.primary_address AS Address_1,
                   custom_dv_employees.secondary_address AS Address_2,
                   custom_dv_employees.city AS City,
                   custom_dv_employees.state_province AS State_Prov,
                   custom_dv_employees.zip_code AS Zip_Postal_Code,
                   custom_dv_employees.country AS Country,
                   custom_dv_employees.office_phone AS Office_Phone,
                   custom_dv_employees.mobile_phone AS Mobile_Phone,
                   custom_dv_employees.emergency_phone AS Emergency_Phone,
                   custom_dv_employees.primary_email AS Email_1,
                   custom_dv_employees.secondary_email AS Email_2,
                   custom_dv_employees.tertiary_email AS Email_3,
                   CASE 
                        WHEN UPPER(custom_dv_employees.is_removed) = 'TRUE' THEN 'Deleted' 
                        WHEN UPPER(custom_dv_employees.is_inactive) = 'TRUE' THEN 'InActive' 
                        ELSE 'Active' 
                    END AS User_Status,
                   -- '' as user_start_date,
                   -- '' as user_end_date,
                   report_to.Reports_To,
                   CAST('{refresh_date}' AS TIMESTAMP) as UpdateDate,
                   custom_dv_employees.score AS Score
                FROM {var_client_custom_db}.custom_dv_employees                                  custom_dv_employees
                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                     h_masterClientsTen
                  ON TRIM(custom_dv_employees.source_id) = TRIM(h_masterClientsTen.source_id) 
                 AND TRIM(custom_dv_employees.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                LEFT JOIN Completion_Date                                                        Last_WO_Completion_Date
                       ON custom_dv_employees.employee_id = Last_WO_Completion_Date.employee_id
                LEFT JOIN Acceptance_Date                                                        Last_WO_Acceptance_Date
                       ON custom_dv_employees.employee_id = Last_WO_Acceptance_Date.employee_id
                LEFT JOIN report_to                                                              report_to
                       ON TRIM(report_to.reports_to_id) = TRIM(custom_dv_employees.reports_to_id) ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date,var_azara_raw_db=var_azara_raw_db))

# COMMAND ----------

# DBTITLE 1,vw_Corrigo_vbiUserTeams
'''
Version: 1, Creation Date:03/08/2023 , Created By: Vinayak Bhanoo
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiUserTeams
          AS
           SELECT DISTINCT 
               dv_employees.client_Name AS Client_Name,
               dv_employees.client_id AS Company_ID,
               dv_employees.employee_id AS User_ID,
               dv_employees.employee_number AS User_Number,
               dv_employees.user_screen_name AS Display_Name,
               teams_emp.team_name AS Team_Name,
               CASE WHEN UPPER(teams_emp.is_removed) = 'TRUE' THEN 'Deleted' ELSE 'Active' END AS Team_Status,
               CASE WHEN UPPER(dv_employees.is_inactive) = 'TRUE' THEN 'In Active' ELSE 'Active' END AS User_Status,
               CASE WHEN UPPER(dv_employees.is_removed) = 'TRUE' THEN 'Yes' ELSE 'No' END AS Is_Deleted,
               CAST('{refresh_date}' as TIMESTAMP) as UpdateDate,
               CASE WHEN UPPER(teams_emp.is_removed) = 'TRUE' THEN 'Yes' ELSE 'No' END AS Is_Team_Deleted
            FROM {var_client_custom_db}.custom_dv_employees dv_employees
            JOIN {var_client_custom_db}.raw_employee_teams_emp_team_corrigo teams_emp
                   ON dv_employees.hk_h_employees = teams_emp.hk_h_employees ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiWorkOrderLineItems
'''
Version: 1, Creation Date:03/08/2023 , Created By: Vinayak Bhanoo
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiWorkOrderLineItems
                AS
                    SELECT DISTINCT
                        client_name    as Client_Name
                        ,client_id      as Company_ID
                        ,work_order_id  as Work_Order_ID
                        ,asset_id       as Asset_ID
                        ,asset_name     as Asset
                        ,asset_model    as Asset_Model
                        ,asset_category as Asset_Type
                        ,task_name      as Task
                        ,task_comment   as Description
                        ,task_status    as Disposition
                        ,COALESCE(Reverse(RIGHT(reverse (asset_full_path), length(reverse(asset_full_path)) - charindex('>',reverse(asset_full_path)))), asset_name) as Item_Asset_Location
                        ,concat(COALESCE(Reverse(RIGHT(reverse(asset_full_path), length(reverse (asset_full_path)) - charindex('>',reverse(asset_full_path)))), asset_name),'>',asset_name,'>',task_name)            as Task_Full_Path
                        ,task_code                           as Task_Code
                        ,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                        -- ,''                               as Is_Deleted
                    FROM {var_client_custom_db}.custom_work_order_task ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiTimeCardEntries
'''
Version: 1, Creation Date:03/08/2023 , Created By: Vinayak Bhanoo
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiTimeCardEntries
                    AS
                        SELECT DISTINCT 
                             h_masterClientsTen.client_name              as Client_Name
                            ,h_masterClientsTen.client_id                as Company_ID
                            ,raw_timeCardEntries.time_card_id            as ID
                            ,CAST(date_format(raw_timeCardEntries.time_card_started_at,   'yyyy-MM-dd HH:mm:ss.SSSSSSS') as TIMESTAMP) as Date_Time
                            ,CAST(date_format(raw_timeCardEntries.auto_record_started_at, 'yyyy-MM-dd HH:mm:ss.SSSSSSS') as TIMESTAMP) as Auto_recorded_Start
                            ,CAST(date_format(raw_timeCardEntries.auto_record_ended_at,   'yyyy-MM-dd HH:mm:ss.SSSSSSS') as TIMESTAMP) as Auto_recorded_End
                            ,raw_timeCardEntries.duration                as Duration
                            ,raw_timeCardEntries.labor_code              as Labor_Code
                            ,raw_timeCardEntries.labor_code_description  as Labor_Code_Description
                            ,raw_timeCardEntries.comment                 as Comment
                            ,dv_areas.area_name                          as Regarding
                            ,raw_timeCardEntries.employee_week_id        as TC_ID
                            ,raw_timeCardEntries.time_card_status        as TC_Status
                            ,raw_timeCardEntries.submitted_by            as TC_Submitted_by
                            ,CAST(date_format(raw_timeCardEntries.submitted_at, 'yyyy-MM-dd HH:mm:ss.SSSSSSS') as TIMESTAMP) as TC_Submitted_on
                            ,raw_timeCardEntries.approved_by             as TC_Approved_by
                            ,CAST(date_format(raw_timeCardEntries.approved_at, 'yyyy-MM-dd HH:mm:ss.SSSSSSS') as TIMESTAMP) as TC_Approved_on
                            ,NVL(raw_timeCardEntries.is_flagged,0)       as Flagged
                            ,raw_timeCardEntries.flag_comment            as Flag_Comment
                            ,raw_timeCardEntries.record_type_id          as Record_Type
                            ,iff(upper(raw_timeCardEntries.is_modified)='TRUE',1,0) as Was_Modified
                            ,raw_timeCardEntries.area_id                 as Area_ID
                            ,raw_timeCardEntries.work_order_number       as WO
                            ,raw_timeCardEntries.work_order_id           as Work_Order_ID
                            ,dv_employees.first_name                     as First_Name
                            ,dv_employees.last_name                      as Last_Name
                            ,dv_employees.user_screen_name               as Display_Name
                            ,raw_timeCardEntries.employee_id             as User_ID
                            ,dv_employees.federal_id                     as Federal_ID
                            ,dv_employees.job_title                      as Job_Title
                            ,CAST('{refresh_date}' as TIMESTAMP)         as UpdateDate
                            ,iff(upper(dv_employees.is_removed)='TRUE','Yes','No') as Is_Deleted
                        FROM {var_client_custom_db}.raw_time_card_entries_timecardentries_corrigo raw_timeCardEntries
                        JOIN {var_client_custom_db}.custom_hv_master_clients_tenants              h_masterClientsTen
                        ON TRIM(raw_timeCardEntries.source_id) = TRIM(h_masterClientsTen.source_id)
                        AND TRIM(raw_timeCardEntries.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                        LEFT JOIN {var_client_custom_db}.custom_dv_employees                      dv_employees
                               ON raw_timeCardEntries.hk_h_employees = dv_employees.hk_h_employees
                        LEFT JOIN {var_client_custom_db}.custom_dv_areas                          dv_areas
                               ON raw_timeCardEntries.area_id = dv_areas.area_id ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------


