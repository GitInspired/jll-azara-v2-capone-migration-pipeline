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

# DBTITLE 1,ssdv_vw_Corrigo_vbiAssetRefrigerantLog
'''

Version: 1, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo
Version: 2, Creation Date: 03/08/2023, Updated By: Vinayak Bhanoo

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
                            ,iff(raw_refActicityLogs.exclude_leak_rate=1, 'Yes', 'No') as Exclude_from_leak_rate
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
                        FROM {var_client_custom_db}.raw_time_card_entries_timecardentries_corrigo raw_timeCardEntries
                        JOIN {var_client_custom_db}.custom_hv_master_clients_tenants              h_masterClientsTen
                        ON TRIM(raw_timeCardEntries.source_id) = TRIM(h_masterClientsTen.source_id)
                        AND TRIM(raw_timeCardEntries.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                        LEFT JOIN {var_client_custom_db}.custom_dv_employees                      dv_employees
                               ON raw_timeCardEntries.hk_h_employees = dv_employees.hk_h_employees
                        LEFT JOIN {var_client_custom_db}.custom_dv_areas                          dv_areas
                               ON raw_timeCardEntries.area_id = dv_areas.area_id ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiProposalApprovals
'''
Version: 1, Creation Date:03/08/2023 , Created By: Vinayak Bhanoo
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
                     AND TRIM(raw_proposalItems.tenant_id) = TRIM(h_masterClientsTen.tenant_id); """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date,var_azara_raw_db=var_azara_raw_db))

# COMMAND ----------

# DBTITLE 1,clientssdv_vw_Corrigo_Customer_Custom
'''
Version: 1, Creation Date: 08-08-2023, Created By: Vinayak Bhanoo
'''

spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.clientssdv_vw_Corrigo_Customer_Custom 
                AS
                SELECT DISTINCT
                    object_ID as CustomerCustomID
                    ,Super_Region
                    ,DID
                    ,BU_Number
                    ,Work_Zone_ID
                    ,External_Property_ID
                    ,Escalation_1
                    ,Escalation_1_Phone
                    ,Escalation_2
                    ,Escalation_2_Phone
                    ,Escalation_3
                    ,Escalation_3_Phone
                    ,After_Hours_Escalation_1_Shift_Phone
                    ,After_Hours_Escalation_2
                    ,After_Hours_Escalation_2_Phone
                    ,After_Hours_Escalation_3
                    ,After_Hours_Escalation_3_Phone
                    ,Status
                    ,Union_Yes_No 
                    ,Union_Scope
                    ,FM_Lead
                    ,FM_Onsite_Frequency
                    ,Chief_Engineer
                    ,Chief_Engineer_Phone
                    ,Assistant_Chief_Engineer
                    ,Assistant_Chief_Engineer_Phone
                    ,Senior_Facility_Manager
                    ,Senior_Facility_Manager_Phone
                    ,Facility_Manager
                    ,Facility_Manager_Phone
                    ,Assistant_Facility_Manager
                    ,Assistant_Facility_Manager_Phone
                    ,Regional_Facility_Manager
                    ,Regional_Facility_Manager_Phone
                    ,Regional_Engineering_Manager
                    ,Regional_Engineering_Manager_Phone
                    ,Regional_Lead
                    ,Regional_Lead_Phone
                    ,Natural_Gas
                    ,Inspections_Daily_Critical_Checks
                    ,Inspections_Weekly_Building_Rounds
                    ,Inspections_Monthly_Fire_Extinguisher
                    ,Inspections_Monthly_FM_Site_Inspection
                    ,Inspections_Monthly_Kitchen_Documents_Upload
                    ,Inspections_Monthly_PA_System_Work_Effort
                    ,Inspections_Monthly_Roof_Inspection
                    ,Inspections_Monthly_Satellite_Phone_Service_Test
                    ,Inspections_Monthly_SDS_Inspection
                    ,Inspections_Monthly_Spill_Response_Kit_and_AST_Inspections
                    ,Inspections_Monthly_Vehicle_Inspection
                    ,Inspections_Quarterly_Emergency_Response_Mock_Drills
                    ,Inspections_Quarterly_Portable_Ladder_Inspection
                    ,Inspections_Annual_FCI_Inspection
                    ,Inspections_Annual_Clock_Inspection_Fall_Back
                    ,Inspections_Annual_Clock_Inspection_Spring_Forward
                    ,Inspections_Annual_Fixed_Ladder_Inspection
                    ,Inspections_Annual_JLL_First_Aid_Kit_Inspections
                    ,Inspections_Annual_SDS_Inspection
                    ,Inspections_Annual_Spill_Response_Kit_and_AST_Inspection
                    ,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                FROM {var_client_custom_db}.Corrigo_Custom_Customer ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,clientssdv_vw_Corrigo_User_Custom
'''
Version: 1, Creation Date: 08-08-2023, Created By: Vinayak Bhanoo
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

# DBTITLE 1,clientssdv_vw_Corrigo_WorkOrder_Custom
'''
Version: 1, Creation Date: 08-08-2023, Created By: Vinayak Bhanoo
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.clientssdv_vw_Corrigo_WorkOrder_Custom 
                AS
                SELECT DISTINCT
                    Object_ID as WorkOrderCustomID
                    ,Accidental_Release_lbs
                    ,ADA_Reporting
                    ,Capital_One_ESS
                    ,Change_Control_Number
                    ,Contingency_Spend
                    ,Diesel_Fuel_Usage
                    ,Diesel_Quantity_gal
                    ,Energy_Savings
                    ,Event_Disaster_Storm_Related
                    ,Financial_Governance
                    ,Help_Desk_Review
                    ,HR_Case_Number
                    ,COALESCE(JCAP_OCP_Section,OCP_Section) AS JCAP_OCP_Section
                    ,JCAP_Admin_Only_WO
                    ,JDE_Export_Schedule_Start
                    ,Manual_Work_Order_Number
                    ,MCIM_FCI_Assessment_Link
                    ,MCIM_Assessments
                    ,NOC_Reactive_Proactive
                    ,NOC_Related
                    ,PRIME_Audit_Work_Order
                    ,Propane_Fuel_Usage
                    ,Propane_Quantity_lbs
                    ,Property_Management_Billing
                    ,Quality_Control_Audit
                    ,Refrigerant_Added_lbs
                    ,Refrigerant_Recovered_lbs
                    ,Refrigerant_Type
                    ,Regulatory_Agency
                    ,Re_Opening_Reason
                    ,SLA_Extension
                    ,Unique_Contract_Identifier_PM_PMRW_Schedules
                    ,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                FROM {var_client_custom_db}.Corrigo_Custom_WorkOrder ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.clientssdv_vw_Corrigo_Customer_Custom TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`"""); 
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.clientssdv_vw_Corrigo_User_Custom TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`"""); 
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.ssdv_vw_Corrigo_vbiAssetRefrigerantLog TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`"""); 
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.ssdv_vw_Corrigo_vbiProposalApprovals TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`"""); 
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.ssdv_vw_Corrigo_vbiTimeCardEntries TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`"""); 
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.ssdv_vw_Corrigo_vbiUsers TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`"""); 
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.ssdv_vw_Corrigo_vbiUserTeams TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`"""); 
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.ssdv_vw_Corrigo_vbiWorkOrderLineItems TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.#clientssdv_vw_Corrigo_Customer_Custom TO `jll-azara-custom-CapOneJLLAcctTeam-read`"""); 
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.#clientssdv_vw_Corrigo_User_Custom TO `jll-azara-custom-CapOneJLLAcctTeam-read`"""); 
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.#ssdv_vw_Corrigo_vbiAssetRefrigerantLog TO `jll-azara-custom-CapOneJLLAcctTeam-read`"""); 
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.#ssdv_vw_Corrigo_vbiProposalApprovals TO `jll-azara-custom-CapOneJLLAcctTeam-read`"""); 
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.#ssdv_vw_Corrigo_vbiTimeCardEntries TO `jll-azara-custom-CapOneJLLAcctTeam-read`"""); 
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.#ssdv_vw_Corrigo_vbiUsers TO `jll-azara-custom-CapOneJLLAcctTeam-read`"""); 
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.#ssdv_vw_Corrigo_vbiUserTeams TO `jll-azara-custom-CapOneJLLAcctTeam-read`"""); 
#spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.#ssdv_vw_Corrigo_vbiWorkOrderLineItems TO `jll-azara-custom-CapOneJLLAcctTeam-read`""");
