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
var_tenant_id        = "14297946134365"

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

# DBTITLE 1,ssdv_vw_Corrigo_vbiContacts
'''
Version: 1, Creation Date:7/26/2023 , Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiContacts 
                AS
				SELECT DISTINCT
					 h_masterClientsTen.client_name    as Client_Name
					,h_masterClientsTen.client_id      as company_id
					,raw_contacts.contact_id           as Contact_ID
					,raw_contacts.first_name           as First_Name
					,raw_contacts.last_name            as Last_Name
					,raw_contacts.contact_login        as Login
					,raw_contacts.contact_email        as Email
					,raw_contacts.contact_nte          as NTE
					,raw_contacts.supervisor_name      as Supervisor
					,iff(upper(raw_contacts.can_create_requests)='TRUE','Yes','No') as Create_Requests
					,case 
						when raw_contacts.note_access_id = 1 then 'None'  
						when raw_contacts.note_access_id = 2 then 'View'  
						else 'Add/Edit'  
					end as Customer_Notes_Access
					,raw_contacts.max_priority_notifications as Notifications
					,iff(raw_contacts.no_wo_email_alerts = 1,'No','Yes') as Email_Wo_Notifications
					,case 
							when upper(raw_contacts.is_removed) = 'TRUE' then 'Yes'
							when upper(raw_contacts.is_removed) = 'FALSE' then 'No'
							else raw_contacts.is_removed
					end as Is_Deleted
					,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
					,iff(upper(raw_contacts.is_approved)='TRUE','Yes','No')     as Is_Approved
				FROM {var_client_custom_db}.raw_contacts_contacts_corrigo          raw_contacts
				JOIN {var_client_custom_db}.custom_hv_master_clients_tenants       h_masterClientsTen
				  ON TRIM(raw_contacts.source_id) = TRIM(h_masterClientsTen.source_id)
				 AND TRIM(raw_contacts.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
				WHERE TRIM(UPPER(raw_contacts.contact_type)) = 'CONTACT' ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv.vw_Corrigo_vbiUsers
'''
Version: 1, Creation Date:7/26/2023 , Created By: Varun Kancharla
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
Version: 1, Creation Date:7/26/2023 , Created By: Varun Kancharla
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
Version: 1, Creation Date:7/26/2023 , Created By: Varun Kancharla
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

# DBTITLE 1,ssdv_vw_Corrigo_vbiWorkZoneResponsibilities
'''
Version: 1, Creation Date:7/26/2023 , Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiWorkZoneResponsibilities
          			AS
					SELECT DISTINCT
							 h_masterClientsTen.client_name as Client_Name
							,h_masterClientsTen.client_id as Company_ID
							,CAST(dv_areas.area_id as INT) as Property_ID
							,dv_areas.area_name as Property
							,raw_responsibilities.responsibility as What
							,case when raw_areasResp.employee_type_id = 12 then raw_contacts.contact_name else dv_employees.user_screen_name end as Who
							,case when raw_areasResp.employee_type_id = 12 then raw_contacts.mobile_phone else dv_employees.mobile_phone end as Phone
							,case when raw_areasResp.employee_type_id = 12 then raw_contacts.contact_email else dv_employees.primary_email end as Email
							,case 
								when current_timestamp() between raw_areasResp.substitute_start_at and raw_areasResp.substitute_end_at then substEmployee.user_screen_name
							else null end as Replaced_by_Who
							,case 
								when current_timestamp() between raw_areasResp.substitute_start_at and raw_areasResp.substitute_end_at then substEmployee.mobile_phone
							else null end as Replaced_by_Phone
							,case 
								when current_timestamp() between raw_areasResp.substitute_start_at and raw_areasResp.substitute_end_at then substEmployee.primary_email
							else null end as Replaced_by_Email
							,raw_areasResp.locality as Where
							,case when raw_areasResp.employee_type_id = 12 then 
                              iff(upper(raw_contacts.is_removed) = 'TRUE', 'Yes', 'No') 
                            else iff(upper(dv_employees.is_inactive) = 'TRUE', 'Yes', 'No') end  as Is_Who_Inactive
					FROM {var_client_custom_db}.raw_responsibilities_resp_corrigo                                    raw_responsibilities
					LEFT JOIN {var_client_custom_db}.raw_areas_responsibilities_areas_resp_corrigo                   raw_areasResp
						   ON raw_responsibilities.hk_h_responsibilities = raw_areasResp.hk_h_responsibilities
					JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                                     h_masterClientsTen
					  ON TRIM(raw_responsibilities.source_id) = TRIM(h_masterClientsTen.source_id)
					 AND TRIM(raw_responsibilities.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
					JOIN {var_client_custom_db}.custom_dv_areas                                                 dv_areas
					       ON raw_areasResp.hk_h_areas = dv_areas.hk_h_areas
					LEFT JOIN {var_client_custom_db}. custom_dv_employees                                             dv_employees
						   ON TRIM(raw_areasResp.employee_id) = TRIM(dv_employees.employee_id)
					LEFT JOIN {var_client_custom_db}.custom_dv_employees                                             substEmployee
						   ON TRIM(raw_areasResp.replaced_employee_id) = TRIM(substEmployee.employee_id) 
                    LEFT JOIN {var_client_custom_db}.raw_contacts_contacts_corrigo                                  raw_contacts        on raw_areasResp.contact_id = raw_contacts.contact_id where raw_responsibilities.responsibility IS NOT NULL and upper(raw_areasResp.locality) = 'LOCAL' ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiWorkOrderCheckInOuts
'''
Version: 1, Creation Date:7/26/2023 , Created By: Varun Kancharla
Version: 2, Creation Date:8/11/2023 , Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiWorkOrderCheckInOuts
                AS
                    SELECT DISTINCT
                        h_masterClientsTen.client_Name AS Client_Name,
                        h_masterClientsTen.client_id AS Company_ID,
                        raw_workOrderTelemetry.work_order_id AS Work_Order_ID,
                        raw_workOrderTelemetry.work_order_number AS Work_Order,
                        COALESCE(raw_workOrderTelemetry.employee_id,raw_workOrderTelemetry.service_provider_id) AS User_ID,
                        CAST(raw_workOrderTelemetry.work_order_latitude as DECIMAL(9,7)) AS Work_Order_Lat,
                        CAST(raw_workOrderTelemetry.work_order_longitude  as DECIMAL(9,6)) AS Work_Order_Long,
                        raw_workOrderTelemetry.checkin_at AS CheckIn_Date,
                        raw_workOrderTelemetry.checkin_method AS CheckIn_Method,
                        CAST(raw_workOrderTelemetry.checkin_latitude  as DECIMAL(9,6)) AS CheckIn_Lat,
                        CAST(raw_workOrderTelemetry.checkin_longitude  as DECIMAL(9,6)) AS CheckIn_Long,
                        CAST(raw_workOrderTelemetry.checkin_distance as DECIMAL(9,2)) AS CheckIn_Distance,
                        raw_workOrderTelemetry.checkin_status AS CheckIn_Status,
                        raw_workOrderTelemetry.checkout_at AS CheckOut_Date,
                        raw_workOrderTelemetry.checkout_method AS CheckOut_Method,
                        CAST(raw_workOrderTelemetry.checkout_latitude as DECIMAL(9,6)) AS CheckOut_Lat,
                        CAST(raw_workOrderTelemetry.checkout_longitude as DECIMAL(9,6)) AS CheckOut_Long,
                        CAST(raw_workOrderTelemetry.checkout_distance as DECIMAL(9,2)) AS CheckOut_Distance,
                        raw_workOrderTelemetry.checkout_status AS CheckOut_Status,
                        CAST('{refresh_date}' as TIMESTAMP) AS UpdateDate
                    FROM {var_client_custom_db}.raw_work_order_telemetry_wo_telemetry_corrigo raw_workOrderTelemetry
                    JOIN {var_client_custom_db}.custom_hv_master_clients_tenants h_masterClientsTen
                    ON TRIM(raw_workOrderTelemetry.source_id) = TRIM(h_masterClientsTen.source_id) 
                    AND TRIM(raw_workOrderTelemetry.tenant_id) = TRIM(h_masterClientsTen.tenant_id) ;""".format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiSLAByPriority
'''
Version: 1, Creation Date:7/26/2023 , Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiSLAByPriority
                AS
                    SELECT DISTINCT 
                         h_masterClientsTen.client_name AS Client_Name
                        ,h_masterClientsTen.client_id AS Company_ID
                        ,raw_workorders.priority_id as Priority_ID
                        ,raw_workorders.priority AS Priority
                        ,max(due_in_minutes_sla) as Due_In_Minutes
                        ,max(respond_in_minutes_sla) as Respond_In_Minutes
                        ,max(acknowledge_in_minutes_sla) as Acknowledge_In_Minutes
                        ,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                    FROM {var_client_custom_db}.raw_work_orders_workorders_corrigo       raw_workorders
                    JOIN {var_client_custom_db}.custom_hv_master_clients_tenants         h_masterClientsTen
                      ON TRIM(raw_workorders.source_id) = TRIM(h_masterClientsTen.source_id) 
                     AND TRIM(raw_workorders.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                    GROUP BY ALL ;""".format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiTimeCardEntries
'''
Version: 1, Creation Date:7/26/2023 , Created By: Varun Kancharla
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
                        FROM {var_client_custom_db}.raw_time_card_entries_timecardentries_corrigo raw_timeCardEntries
                        JOIN {var_client_custom_db}.custom_hv_master_clients_tenants              h_masterClientsTen
                        ON TRIM(raw_timeCardEntries.source_id) = TRIM(h_masterClientsTen.source_id)
                        AND TRIM(raw_timeCardEntries.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                        LEFT JOIN {var_client_custom_db}.custom_dv_employees                      dv_employees
                               ON raw_timeCardEntries.hk_h_employees = dv_employees.hk_h_employees
                        LEFT JOIN {var_client_custom_db}.custom_dv_areas                          dv_areas
                               ON raw_timeCardEntries.area_id = dv_areas.area_id ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiPMRMVendorInvoiceItems
'''
Version: 1, Creation Date:7/31/2023 , Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiPMRMVendorInvoiceItems
                    AS
                    SELECT DISTINCT
                        B.CLIENT_NAME, 
                        a.TENANT_ID as CLIENT_ID,
                        CLIENT_ID as Company_ID,
                        a.schedule_id,
                        cost_category as category,
                        cost_item as item,
                        cost_description as description,
                        quantity,
                        cost_per_unit as unit_price_rate,
                        quantity * cost_per_unit as subtotal,
                        case when is_exclude='False' then 0 else 1 end as is_exclude ,
                        case when is_override='False' then 0 else 1 end as is_override ,
                        CAST('{refresh_date}' as TIMESTAMP) AS UpdateDate
                    FROM {var_client_custom_db}.raw_schedule_costs_schedules_corrigo A
                    JOIN {var_client_custom_db}.CUSTOM_HV_MASTER_CLIENTS_TENANTS B
                      ON A.TENANT_ID=B.TENANT_ID """.format(var_client_raw_db=var_client_raw_db,var_client_custom_db=var_client_custom_db,refresh_date=refresh_date)) 

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiProposalApprovals
'''
Version: 1, Creation Date: 8/11/2023 , Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiProposalApprovals
                   AS
                    SELECT DISTINCT 
                       h_masterClientsTen.client_name AS Client_Name,
                       h_masterClientsTen.client_id AS Company_ID,
                       raw_proposalItems.proposal_id AS Proposal_ID,
                       raw_proposalItems.order_index AS Step,
                       raw_proposalItems.last_updated_by AS Approver,
                       raw_proposalItems.response || ' - ' || CAST( raw_proposalItems.last_updated_at AS string) AS Response,
                       raw_proposalItems.comment AS Note,
                       case 
                           when raw_proposalItems.employee_type_id = 12 then raw_proposalItems.nte 
                           when raw_proposalItems.employee_type_id = 1 then raw_proposalItems.nte_limit 
                           else null 
                       end as Authority,
                       CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                    FROM {var_client_custom_db}.raw_proposal_items_propitems_corrigo            raw_proposalItems
                    JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                h_masterClientsTen
                      ON TRIM(raw_proposalItems.source_id) = TRIM(h_masterClientsTen.source_id) 
                     AND TRIM(raw_proposalItems.tenant_id) = TRIM(h_masterClientsTen.tenant_id) ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date,var_azara_raw_db=var_azara_raw_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiAssetActivityLog
'''
Version: 1, Creation Date: 8/11/2023 , Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiAssetActivityLog
        AS
            With assetActivityLogs
                as (
                    SELECT DISTINCT
                         h_masterClientsTen.client_name
                        ,h_masterClientsTen.client_id
                        ,CASE 
                            WHEN raw_assetActivityLogs.asset_id IS NOT NULL THEN raw_assetActivityLogs.asset_id
                            WHEN raw_assetActivityLogs.space_id IS NOT NULL THEN raw_assetActivityLogs.space_id
                            WHEN raw_assetActivityLogs.classification_id IS NOT NULL THEN raw_assetActivityLogs.classification_id
                        END as Asset_ID
                        ,CASE 
                            WHEN raw_assetActivityLogs.asset_id IS NOT NULL THEN raw_assets.name
                            WHEN raw_assetActivityLogs.space_id IS NOT NULL THEN raw_spaces.space_name
                            WHEN raw_assetActivityLogs.classification_id IS NOT NULL THEN raw_assetClassify.name
                        END as Asset_Name
                        ,CASE 
                            WHEN raw_assetActivityLogs.asset_id IS NOT NULL THEN raw_assets.type
                            WHEN raw_assetActivityLogs.space_id IS NOT NULL THEN raw_spaces.space_category
                            WHEN raw_assetActivityLogs.classification_id IS NOT NULL THEN raw_assetClassify.category
                        END as Asset_Category
                        ,CASE 
                            WHEN raw_assetActivityLogs.asset_id IS NOT NULL THEN raw_assets.is_offline
                            WHEN raw_assetActivityLogs.space_id IS NOT NULL THEN raw_spaces.is_offline
                            WHEN raw_assetActivityLogs.classification_id IS NOT NULL THEN raw_assetClassify.is_offline
                        END as Is_Offline
                        ,CASE 
                            WHEN raw_assetActivityLogs.asset_id IS NOT NULL THEN raw_assets.area_id
                            WHEN raw_assetActivityLogs.space_id IS NOT NULL THEN raw_spaces.area_id
                            WHEN raw_assetActivityLogs.classification_id IS NOT NULL THEN raw_assetClassify.area_id
                        END as Area_ID
                        ,CASE 
                            WHEN raw_assetActivityLogs.asset_id IS NOT NULL THEN raw_assets.orphan
                            WHEN raw_assetActivityLogs.space_id IS NOT NULL THEN raw_spaces.is_orphan
                            WHEN raw_assetActivityLogs.classification_id IS NOT NULL THEN raw_assetClassify.is_orphan
                        END as Is_Orphan
                        ,raw_assetActivityLogs.log_action
                        ,raw_assetActivityLogs.action_taken_by
                        ,raw_assetActivityLogs.log_comment
                        ,raw_assetActivityLogs.action_taken_at
                    FROM {var_client_custom_db}.raw_asset_activity_logs_asset_actlogs_corrigo        raw_assetActivityLogs
                    JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                     h_masterClientsTen
                      ON TRIM(raw_assetActivityLogs.source_id)  = TRIM(h_masterClientsTen.source_id)
                     AND TRIM(raw_assetActivityLogs.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                    LEFT JOIN {var_client_custom_db}.raw_assets_assets_corrigo                       raw_assets
                           ON TRIM(raw_assetActivityLogs.asset_id) = TRIM(raw_assets.asset_id)
                    LEFT JOIN {var_client_custom_db}.raw_spaces_spaces_corrigo                       raw_spaces
                           ON TRIM(raw_assetActivityLogs.space_id) = TRIM(raw_spaces.space_id)
                    LEFT JOIN {var_client_custom_db}.raw_asset_classifications_assetclass_corrigo    raw_assetClassify
                           ON TRIM(raw_assetActivityLogs.classification_id) = TRIM(raw_assetClassify.asset_classification_id)
                    )
                    SELECT DISTINCT
                         client_name
                        ,client_id as Company_ID
                        ,Asset_id
                        ,Asset_Name
                        ,Asset_Category
                        ,iff(upper(is_offline)='TRUE','Offline','Online') as Asset_Status
                        ,Area_ID as Property_ID
                        ,log_action as `Action`
                        ,action_taken_by as `By`
                        ,log_comment as `Comment`
                        ,action_taken_at as `Date`
                        ,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                        ,iff(upper(Is_Orphan)='TRUE','Yes','No') as Is_Deleted
                    FROM assetActivityLogs ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,clientssdv_vw_Corrigo_Customer_Custom
'''
Version: 1, Creation Date: 8/14/2023 , Created By: Varun Kancharla
'''

spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.clientssdv_vw_Corrigo_Customer_Custom 
                AS
                SELECT DISTINCT
                    object_ID as CustomerCustomID
                    ,Branch_Phone
                    ,Work_Zone_ID
                    ,DID
                    ,BU_Number
                    ,FM_Lead
                    ,JLL_MES_Technician
                    ,JLL_MES_HVAC_Engineer
                    ,Facility_Manager
                    ,Facility_Manager_Phone
                    ,Senior_Facility_Manager
                    ,Senior_Facility_Manager_Phone
                    ,FM_Site_Assessment_Needed
                    ,MCIM_Assessment_Link
                    ,Escalation_1_Name
                    ,Escalation_1_Phone
                    ,Escalation_2_Name
                    ,Escalation_2_Phone
                    ,Escalation_3_Name
                    ,Escalation_3_Phone
                    ,Escalation_4_Name
                    ,Escalation_4_Phone
                    ,External_Property_ID
                    ,Status
                    ,Union_Yes_No
                    ,Union_Scope
                    ,Floor_Area
                    ,Leased_or_Owned
                    ,ADA_Doors
                    ,ATM_Only_Walk_Up_or_Drive_Up
                    ,ATM_Only_Standing_or_Enclosed
                    ,ATM_Only_HVAC_PM_needed
                    ,ATM_Only_Is_JLL_responsible_for_the_Lighting
                    ,ATM_Only_Is_JLL_responsible_for_HVAC
                    ,ATM_Only_Does_it_have_a_Roof
                    ,ATM_Only_Is_there_landscaping_around_the_ATM_that_will_require
                    ,Event_Disaster_Storm_Property_Status
                    ,Fresh_Air_In_Take_Cafes_Only
                    ,Natural_Gas
                    ,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                FROM {var_client_custom_db}.Corrigo_Custom_Customer ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,clientssdv_vw_Corrigo_User_Custom
'''
Version: 1, Creation Date: 8/14/2023 , Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.clientssdv_vw_Corrigo_User_Custom 
                AS
                SELECT DISTINCT
                    Object_ID as UserCustomID
                    ,Dedicated_Client
                    ,JLL_Organization
                    ,Peoplesoft_Property_ID
                    ,Union_Local
                    ,Weekly_Scheduled_Hours
                    ,Primary_Job_Function
                    ,Exemption_Status
                    ,Time_Keeper
                    ,Time_Approver
                    ,CapOne_EID
                    ,Region
                    ,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                FROM {var_client_custom_db}.Corrigo_Custom_User ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiAssetRefrigerantLog
'''
Version: <1>, Creation Date: <7/11/2023>, Created By: <Varun Kancharla>
Version: <2>, Creation Date: <8/2/2023>, Updated By: <Varun Kancharla>
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiAssetRefrigerantLog
                    AS
                        SELECT DISTINCT
                            h_masterClientsTen.client_name as Client_Name
                            ,h_masterClientsTen.client_id as Company_ID
                            ,raw_refActicityLogs.asset_id as Asset_ID
                            ,raw_assets.name as Asset_Name
                            ,raw_areas.area_name as Property
                            ,raw_refActicityLogs.action_taken_on as Date
                            ,raw_refActicityLogs.work_order_id as Work_Order_ID
                            ,raw_refActicityLogs.performed_by as Performed_By
                            ,raw_refActicityLogs.certificate_name as Certification_Number_or_Name
                            ,raw_refActicityLogs.circuit_number as Circuit_Number
                            ,raw_refActicityLogs.circuit_capacity as Normal_Capacity
                            ,raw_refActicityLogs.transaction_type as Transaction
                            ,raw_refActicityLogs.quantity as Quantity_Used
                            ,raw_refActicityLogs.refrigerant_type as Transaction_Refrigerant_Type
                            ,raw_refActicityLogs.cylinder_number as Cylinder
                            ,raw_refActicityLogs.transaction_comment as Transaction_Comment
                            ,iff(upper(raw_refActicityLogs.exclude_leak_rate)='TRUE', 'Yes', 'No') as Exclude_from_leak_rate
                            ,raw_refActicityLogs.leak_rate as Current_Leak_Rate
                            ,raw_refActicityLogs.leak_threshold as Leak_Rate_threshold
                            ,raw_refActicityLogs.circuit_comment as Comments
                            ,raw_refActicityLogs.test_method as Test_Method
                            ,iff(UPPER(raw_refActicityLogs.is_test_successful)='TRUE', 'Yes', 'No') as Test_successful
                            ,iff(raw_refActicityLogs.status_id=1, 'Yes', 'No') as Deactivated
                            ,iff(UPPER(raw_refActicityLogs.is_contact_notified)='TRUE', 'Yes', 'No') as Contact_Notified
                            ,raw_refActicityLogs.transaction_id as Transaction_ID
                            ,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                            ,(raw_refActicityLogs.circuit_capacity * raw_refActicityLogs.refrigerant_gwp) as Global_Warming_Potential
                            ,iff(UPPER(raw_assets.deleted_flag)='TRUE', 'Yes', 'No') as Is_Deleted
                        FROM {var_client_custom_db}.raw_activity_logs_refrigerant_ref_act_logs_corrigo  raw_refActicityLogs
                        LEFT JOIN {var_client_custom_db}.raw_assets_assets_corrigo                      raw_assets 
                            ON raw_refActicityLogs.hk_h_assets = raw_assets.hk_h_assets
                        LEFT JOIN {var_client_custom_db}.raw_areas_areas_corrigo                        raw_areas
                            ON TRIM(raw_assets.area_id) = TRIM(raw_areas.area_id)
                        JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                    h_masterClientsTen
                        ON TRIM(raw_refActicityLogs.source_id) = TRIM(h_masterClientsTen.source_id)
                        AND TRIM(raw_refActicityLogs.tenant_id) = TRIM(h_masterClientsTen.tenant_id) ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,Grant Access
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_User_Custom TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiAssetActivityLog TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiAssetRefrigerantLog TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiContacts TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiPMRMVendorInvoiceItems TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiProposalApprovals TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiSLAByPriority TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiTimeCardEntries TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiUsers TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiUserTeams TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiWorkOrderLineItems TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_Corrigo_vbiWorkZoneResponsibilities TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_User_Custom TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiAssetActivityLog TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiAssetRefrigerantLog TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiContacts TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiPMRMVendorInvoiceItems TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiProposalApprovals TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiSLAByPriority TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiTimeCardEntries TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiUsers TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiUserTeams TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiWorkOrderLineItems TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.#ssdv_vw_Corrigo_vbiWorkZoneResponsibilities TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
