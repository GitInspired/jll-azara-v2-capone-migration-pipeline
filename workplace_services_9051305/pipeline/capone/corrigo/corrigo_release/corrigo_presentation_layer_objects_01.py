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

# DBTITLE 1,ssdv_vw_Corrigo_vbiCustomers
'''

Version: 1, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiCustomers
                AS
                select  
                    h_masterClientsTen.client_Name,
                    h_masterClientsTen.client_id as Company_ID,
                    dv_customers.customer_number as ID,
                    dv_customers.customer_name as Customer_Name,
                    dv_customers.doing_business_as as DBA,
                    dv_customers.tenant_code as Customer_Number,
                    cast(dv_customers.historical_status as int) as Historical_Status,
                    dv_customers.instructions as Special_Instructions,
                    coalesce(concat(coalesce(dv_customers.address_1, ''), ' ', coalesce(dv_customers.address_2, '') , ' ', coalesce(dv_customers.city, '') , ' ', coalesce(dv_customers.state_province, '') , ' ', coalesce(dv_customers.zip_code, '') , ' ', coalesce(dv_customers.country, '')), '') as Address,
                    dv_customers.main_contact as Main_Contact,
                    dv_customers.contact_phone_number as Main_Phone,
                    dv_customers.contact_email as Main_Email,
                    TRIM(dv_customers.area_name) as Property_Name,
                    dv_customers.area_number as Property_Number,
                    dv_customers.address_1 as Address_1,
                    dv_customers.address_2 as Address_2,
                    dv_customers.city as City,
                    dv_customers.state_province as State_Prov,
                    dv_customers.zip_code as Zip_Postal_Code,
                    dv_customers.country as Country,
                    dv_customers.bu_number as BU_Number,
                    NVL(TRIM(dv_customers.ovcp_id),'') as OVCP_ID,
                    CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                FROM {var_client_custom_db}.custom_dv_customers                    dv_customers
                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants       h_masterClientsTen
                  ON TRIM(dv_customers.source_id) = TRIM(h_masterClientsTen.source_id)
                 AND TRIM(dv_customers.tenant_id) = TRIM(h_masterClientsTen.tenant_id) ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiWorkOrderActivityLog
'''

Version: 2, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiWorkOrderActivityLog
                AS
                SELECT DISTINCT
                     h_masterClientsTen.client_name 	     as client_name
                    ,h_masterClientsTen.client_id		     as company_id
                    ,r_woActivityLogs.work_order_id		     as work_order_id
                    ,r_woActivityLogs.work_order_number	     as wo
                    ,r_woActivityLogs.performed_by			 as performed_by
                    ,r_woActivityLogs.ui_type				 as performed_with				
                    ,r_woActivityLogs.effective_at			 as datetime
                    ,r_woActivityLogs.action				 as action
                    ,r_woActivityLogs.comment 				 as comments
                    , CASE WHEN performed_by ='PMRM Module' AND actor_type_name='Company' THEN 4
                         WHEN performed_by='CorrigoNet' AND actor_type_name='Company' THEN 1
                         WHEN performed_by='Scheduler' AND actor_type_name='Company' THEN 8
                         WHEN performed_by='Integration API User' AND actor_type_name='Company' THEN 13
                         ELSE coalesce(r_woActivityLogs.employee_id,r_woActivityLogs.service_provider_id,r_woActivityLogs.contact_id)
                    END AS performed_by_id
	                ,dv_employees.role 						 as performed_by_role
                    ,case when actor_type_name='User' then 'JLL' else actor_type_name end  as performed_by_user_type
                    ,r_woActivityLogs.reason 				 as reason
                    ,CAST('{refresh_date}' as TIMESTAMP)                        as UpdateDate
                    ,work_order_activity_log_id              as Log_ID
                FROM {var_client_custom_db}.raw_work_order_activity_logs_wo_activitylogs_corrigo r_woActivityLogs
                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                     h_masterClientsTen
                  ON TRIM(r_woActivityLogs.source_id) = TRIM(h_masterClientsTen.source_id)
                 AND TRIM(r_woActivityLogs.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                LEFT JOIN {var_client_custom_db}.custom_dv_employees                    dv_employees
                       ON TRIM(r_woActivityLogs.employee_id) = TRIM(dv_employees.employee_id) ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiEquipmentWorkedOn
'''

Version: 1, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiEquipmentWorkedOn
                AS
                SELECT DISTINCT
                     h_masterClientsTen.client_name as Client_Name
                    ,h_masterClientsTen.client_id as Company_ID
                    ,r_equipmentWorkedOn.work_order_ID
                    ,IFF(r_equipmentWorkedOn.equipment_type_id = 'K', r_equipmentWorkedOn.asset_name, r_equipmentWorkedOn.equipment_name) as equipment
                    ,NVL(r_equipmentWorkedOn.comment,'') AS comment
                    ,r_equipmentWorkedOn.asset_ID as Asset_ID
                    ,dv_assets.equipment_model as Model
                    ,r_equipmentWorkedOn.work_order_number as WO
                    ,CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                    ,NVL(CASE WHEN UPPER(dv_assets.deleted_flag) = 'TRUE'  THEN 1
                          WHEN UPPER(dv_assets.deleted_flag) = 'FALSE' THEN 0
                        END,0) as is_deleted
                FROM {var_client_custom_db}.raw_equipment_worked_on_equipworkedon_corrigo  	 r_equipmentWorkedOn
                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                 h_masterClientsTen
                  ON TRIM(r_equipmentWorkedOn.source_id) = TRIM(h_masterClientsTen.source_id)
                 AND TRIM(r_equipmentWorkedOn.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                LEFT JOIN {var_client_custom_db}.custom_dv_equipment          dv_assets
                       ON TRIM(r_equipmentWorkedOn.asset_id) = TRIM(dv_assets.equipment_id) ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiAssetAttributes
'''

Version: 1, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiAssetAttributes 
                AS
                SELECT  DISTINCT
                         client_name             as Client_Name
                        ,client_id               as Company_ID 
                        ,asset_id                as Asset_ID 
                        ,attribute_name          as NAME
                        ,attribute_value         as VALUE
                        ,CAST('{refresh_date}' as TIMESTAMP)        as UpdateDate
                        ,coalesce(is_deleted, 0) as Is_Deleted
                        ,iff(removed_flag=1,'Yes','No') as Is_Removed
                FROM {var_client_custom_db}.custom_dv_asset_attributes
                WHERE is_orphan = 0 ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiProposals
'''

Version: 2, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiProposals
                AS
                (
                WITH proposals_items
                    AS 
                    (
                        SELECT
                             hk_h_proposals
                            ,response
                            ,item_status
                            ,last_updated_by
                            ,last_updated_at
                        FROM {var_client_custom_db}.raw_proposal_items_propitems_corrigo
                        QUALIFY row_number() over(partition by hk_h_proposals order by order_index desc, last_updated_at desc ) = 1   
                    ) 
                SELECT DISTINCT
                     h_masterClientsTen.client_name   as Client_Name
                    ,h_masterClientsTen.client_id     as Company_ID
                    ,raw_proposals.proposal_id        as Proposal_ID
                    ,raw_proposals.proposal_status    as Status
                    ,raw_proposals.proposal_type      as For
                    ,raw_proposals.work_order_number  as Association
                    ,raw_proposals.work_order_id      as Work_Order_ID 
                    ,ref_time_zones.time_zone_name    as Time 
                    ,cast(raw_proposals.proposal_amount as DECIMAL(38,4))    as Amount
                    ,raw_proposals.currency_code      as Currency
                    ,proposals_items.item_status      as My_Status
                    ,proposals_items.response         as Response
                    ,proposals_items.last_updated_by  as Who
                    ,CAST('{refresh_date}' as TIMESTAMP)                 as UpdateDate
                    ,raw_proposals.description        as Description
                FROM {var_client_custom_db}.raw_proposals_proposals_corrigo                raw_proposals
                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants               h_masterClientsTen
                  ON TRIM(raw_proposals.source_id) = TRIM(h_masterClientsTen.source_id)
                 AND TRIM(raw_proposals.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                JOIN proposals_items                                                       proposals_items
                  ON raw_proposals.hk_h_proposals = proposals_items.hk_h_proposals
                JOIN {var_client_custom_db}.ref_time_zones                                 ref_time_zones
                  ON raw_proposals.time_zone_id = ref_time_zones.time_zone_id ) ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiWorkZones
'''

Version: 1, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiWorkZones
                AS
                SELECT DISTINCT
                     dv_areas.client_name        as Client_Name 
                    ,dv_areas.client_id 		 as Company_ID
                    ,cast(dv_areas.area_id as int)  as ID
                    ,dv_areas.area_name          as Name
                    ,dv_areas.primary_address    as Street_Address_1
                    ,dv_areas.secondary_address  as Street_Address_2
                    ,dv_areas.city               as city
                    ,dv_areas.state_province     as State_Prov
                    ,dv_areas.zip                as ZIP_Postal_Code
                    ,dv_areas.country            as country
                    ,cast(case when upper(dv_areas.is_removed) = 'TRUE'  then 1
                                when upper(dv_areas.is_removed) = 'FALSE' then 0
                                else dv_areas.is_removed 
                     end  as int)            as Deleted
                    ,dv_areas.ovcp_id            as OVCP_ID
                    ,Case 
                            when upper(dv_areas.is_offline) = 'TRUE'  then 'Offline' 
                            when upper(dv_areas.is_offline) = 'FALSE' then 'Online'
                            Else dv_areas.is_offline End as Work_Zone_Status
                    ,CAST('{refresh_date}'   as TIMESTAMP)     as UpdateDate
                    ,dv_areas.wo_prefix      as wo_Prefix
                    ,CAST(dv_areas.latitude  as DECIMAL(9,7))  as latitude
                    ,cast(dv_areas.longitude as DECIMAL(9,6))  as longitude
                    ,dv_areas.tax_region  as Tax_Region
                    ,dv_areas.time_zone   as Time_Zone
                    ,dv_areas.area_number as WorkZone_Number
                FROM {var_client_custom_db}.custom_dv_areas                  dv_areas
                where  dv_areas.area_id <> '0' ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

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

# DBTITLE 1,ssdv_vw_Corrigo_vbiPMRMWorkOrders
'''

Version: 1, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiPMRMWorkOrders
                    AS
                        SELECT  DISTINCT
                            s.CLIENT_NAME, 
                            b.tenant_id as CLIENT_ID,
                            s.CLIENT_ID AS Company_ID,
                            b.schedule_id,
                            a.name  Schedule_Name,
                            b.event_at as event_date,
                            work_order_number  as WO_Number,
                            b.work_order_id as WOID,
                            a.interval_type as schedule_interval
                        FROM {var_client_custom_db}.custom_hv_master_clients_tenants s
                        inner join {var_client_custom_db}.raw_schedule_events_schedevents_corrigo b
                        on s.tenant_id=b.tenant_id
                        left join {var_client_custom_db}.raw_schedules_schedules_corrigo a
                        on b.schedule_id=a.schedule_id
                        left join {var_client_custom_db}.raw_work_orders_workorders_corrigo w
                        on b.work_order_id=w.work_order_id """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiServiceProviders
'''

Version: 1, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiServiceProviders
          AS
           With _WO AS (
            Select distinct 
                 service_provider_id,
                 max(case when action_id = 3 then effective_at end) as Last_WO_Acceptance_Date,
                 max(case when action_id = 6 then effective_at end) as Last_WO_Completion_Date
            from {var_client_custom_db}.raw_work_order_activity_logs_wo_activitylogs_corrigo 
            group by service_provider_id 
                        )         
         
                    SELECT distinct
                         client_name as Client_Name,
                         client_id as Company_ID,
                         a.service_provider_id as ID,
                         full_name as Display_Name,
                         last_name as WON_Listing,
                         service_provider_number as Number,
                         connection_status as Connection_Status,
                         score as Score,
                         SLA_Score,
                         SLA_Counts,
                         satisfaction_score as Satisfaction_Score,
                         satisfaction_counts as Satisfaction_Counts,
                         response_sla_score as Response_Sla_Score,
                         response_sla_counts as Response_Sla_Counts,
                         completion_sla_score as Completion_Sla_Score,
                         completion_sla_counts as Completion_Sla_Counts,
                         invoice_submit_sla_score Invoice_Submit_sla_score,
                         invoice_submit_sla_counts,
                         Provider_Label as Label,
                         federal_id as Federal_ID,
                         Organization,
                         Organization_Number,
                         Language,
                         zip_code as zip_Postal_code,
                         service_radius as Service_Radius,
                         last_invited_on as Last_Invited_Date,
                         date_format(Last_WO_Acceptance_Date, 'yyyy-MM-dd HH:mm:ss.SSSSSSS') as Last_WO_Acceptance_Date,
                         date_format(Last_WO_Completion_Date, 'yyyy-MM-dd HH:mm:ss.SSSSSSS') as Last_WO_Completion_Date,
                         case  
                         when is_payment_electronic= 'False' then '0' 
                         when is_payment_electronic= 'True' then '1' end as Pay_Electronically,
                         office_phone as Phone,
                         email as Email,
                         CAST('{refresh_date}' as TIMESTAMP) as UpdateDate,
                         case when upper(removed_flag) = 'TRUE'  THEN 'Yes'
                              when upper(removed_flag) = 'FALSE' THEN 'No' end as Is_Deleted,
                         primary_address as Address_1,
                         secondary_address as Address_2,
                         city as City_Town,
                         state_province as State_Province,
                         country as Country
                    FROM {var_client_custom_db}.custom_dv_service_providers a 
                    left outer join _WO b on a.service_provider_id = b.service_provider_id
                    ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))        

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiServiceProviderPriceLists
'''

Version: 1, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo
Version: 2, Creation Date: 24/07/2023, Modified By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiServiceProviderPriceLists
          AS
            With _providers as 
            (
              Select distinct 
                    service_provider_id, 
                    full_name,
                    case when lower(is_removed)='true' then 'Yes' else 'No' end as provider_is_deleted
              from {var_client_custom_db}.raw_service_providers_serviceproviders_corrigo
            )  
            Select
                client_name, 
                client_id as company_id, 
                raw_rates.service_provider_id, 
                full_name as service_provider_name,
                item_name, 
                item_description, 
                cost_category as category, 
                CAST(vendor_rate as DECIMAL(19,4)) as rate,  
                case when vendor_rate_type_id like 'E' then 'Yes' else 'No' end as manual_override,
                CAST('{refresh_date}' as TIMESTAMP) as UpdateDate, 
                currency_code as local_currency, 
                provider_is_deleted
            from {var_client_custom_db}.custom_dv_service_provider_rates      raw_rates
            left outer join _providers p on raw_rates.service_provider_id = p.service_provider_id
             ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiServiceProviderInsurance
'''

Version: 1, Creation Date: 21/07/2023, Created By: Vinayak Bhanoo

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiServiceProviderInsurance
          AS
            With expired_status as 
            (
          	 select 
		          service_provider_id,
		          max(case when insurance_status_id=1 then 1 else 0 end) as ins_status
	         from {var_client_custom_db}.raw_service_provider_insurance_svcprovins_corrigo
		     group by service_provider_id
            ),
            _providers as 
            (
              Select distinct 
                    service_provider_id, 
                    full_name as service_provider_display_name
              from {var_client_custom_db}.raw_service_providers_serviceproviders_corrigo
            )   
              Select 
                   client_name, 
                   client_id as company_id,
                   raw_insurance.service_provider_id,
                   service_provider_display_name,
                   raw_insurance.insurance_name as coverage,
                   insurance_status as coverage_status, 
                   coverage_amount, 
                   insurance_starts_on as insurance_start_date, 
                   raw_insurance.insurance_ends_on as insurance_end_date, 
                   insurance_comment as comment,
                   case when es.ins_status= 1 then 'Expired' else Null end as insurance_status,
                   case when insurance_status_id = 1 then 1 else 0 end as is_expired,
                   CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
              from {var_client_custom_db}.custom_dv_service_provider_insurance raw_insurance
              join expired_status es on raw_insurance.service_provider_id = es.service_provider_id 
              left outer join _providers p on raw_insurance.service_provider_id = p.service_provider_id ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))
