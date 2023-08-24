# Databricks notebook source
Notebook for Corrigo Presentation Layer Views

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

# DBTITLE 1,ssdv_vw_Corrigo_vbiWorkOrderCallCenter
'''
Version: 1, Creation Date: 8/24/2023, Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiWorkOrderCallCenter
                    AS
                        with created_by as (
                         select 
                            raw_woal.hk_h_work_orders,
                         	raw_woal.work_order_id,
                         	case when raw_woal.ui_type = 'Customer Portal' and case when emp.employee_type_id=1 then 'JLL'
                         		when emp.employee_type_id=24 then 'Provider'
                         		when emp.employee_type_id=12 then 'Contact'
                         		when emp.employee_type_id=4 then 'Company'
                         	end = 'Contact' then 1 else 0 end is_portal,
                         	emp.employee_id as created_by_id, 
                         	emp.role created_by_role,
                            emp.employee_role_id created_by_role_id
                         from {var_client_custom_db}.raw_work_order_activity_logs_wo_activitylogs_corrigo raw_woal 
                         Left join {var_client_custom_db}.custom_dv_employees                             emp 
                         on raw_woal.employee_id = emp.employee_id
                         where
                         	raw_woal.work_order_id <> 0
                         and
                         	raw_woal.action_id = 1
                         ),
                         
                         touched_by_csr as (
                         select distinct
                         	raw_woal.hk_h_work_orders,
                            raw_woal.work_order_id,
                         	1 is_touched
                         	from {var_client_custom_db}.raw_work_order_activity_logs_wo_activitylogs_corrigo raw_woal 
                            Left join {var_client_custom_db}.custom_dv_employees                             emp 
                            on raw_woal.employee_id = emp.employee_id
                          where
                          	raw_woal.work_order_id <> 0
                          and
                          	raw_woal.action_id != 1 
                          and 
                          	lower(emp.role) like '%call center%' 
                          ),
                          
                          final as 
                          (
                          select 
                             wo.hk_h_work_orders,
                             wo.tenant_id,
                             wo.source_id,
                             wo.work_order_id,
                             case when upper(type_category) = 'BASIC'    THEN 1
                                  when upper(type_category) = 'PMRM'     THEN 2
                                  when upper(type_category) = 'REQUEST'  THEN 4 end as work_order_type_id,
                             wo.work_order_number,
                             wo.created_at as Work_Order_Action_DateTime,
                             coalesce(cp.is_portal, 0) is_portal,
                             cp.created_by_id, 
                             cp.created_by_role,
                             cp.created_by_role_id,
                             coalesce(t.is_touched, 0) is_touched
                          from {var_client_custom_db}.custom_dv_workorders wo 
                          left join created_by cp    on wo.hk_h_work_orders = cp.hk_h_work_orders
                          left join touched_by_csr t on wo.hk_h_work_orders = t.hk_h_work_orders
                          where upper(type_category) in ('BASIC','PMRM','REQUEST')
                          )
                          
                          Select 
                             wo.work_order_id,
                             wo.work_order_number,
                             work_order_type_id,
                             1 AS Work_Order_Action_ID,
                             raw_area.area_id as Owner_Community_ID,
                             Work_Order_Action_DateTime,
                             YEAR(Work_Order_Action_DateTime)||'/'|| right('0'||MONTH(Work_Order_Action_DateTime),2) AS Work_Order_Action_Month,
                             raw_loc.country_name as Country,
                             raw_area.country as Country_Code,
                             raw_loc.country_name as Country_Display_As,
                             raw_area.location_id as Actor_ID,
                             15  as Actor_Type_ID,
                             'P' as Addr_Type_ID,
                             case when lower(created_by_role) like '%call center%' then created_by_id else null end as Employee_ID,
                             case when lower(created_by_role) like '%call center%' then created_by_role_id else null end as Role_ID,
                             case when lower(created_by_role) like '%call center%' then created_by_role else null end as Role_Display_As,
                             IFF(work_order_type_id = 2, 1, 0) as Has_1,-- PMRM WOs
                             IFF(work_order_type_id != 2 AND is_touched > 0 AND is_portal = 1, 1, 0) as Has_5b, -- source is customer portal and touched by CSR 
                             IFF(work_order_type_id != 2 AND is_touched = 0 AND is_portal = 1, 1, 0) as Has_2, -- source is customer portal and not touched by CSR
                             IFF(work_order_type_id != 2 AND lower(created_by_role) like '%call center%' AND is_portal = 0, 1, 0) as Has_4,  -- source is non customer                                portal and created by CSR
                             IFF(work_order_type_id != 2 AND is_touched > 0 AND is_portal = 0, 1, 0) as Has_5a,  -- source is non customer portal and touched by CSR
                             IFF(work_order_type_id != 2 AND is_touched = 0 AND is_portal = 0, 1, 0) as Has_3, -- source is non customer portal and not touched by CSR
                             CAST('{refresh_date}' as TIMESTAMP) as UpdateDate
                        from final wo 
                        join {var_azara_raw_db}.l_workorders_areas                                lwo 
                        on wo.hk_h_work_orders = lwo.hk_h_work_orders
                        left join {var_client_custom_db}.custom_dv_areas                          raw_area 
                        on lwo.hk_h_areas = raw_area.hk_h_areas
                        left join {var_client_custom_db}.raw_locations_locations_corrigo          raw_loc 
                        on raw_area.hk_h_areas = raw_loc.hk_h_areas
                        JOIN {var_client_custom_db}.custom_hv_master_clients_tenants              h_masterClientsTen
                        ON  TRIM(wo.source_id) = TRIM(h_masterClientsTen.source_id)
                        AND TRIM(wo.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                        ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date,var_azara_raw_db=var_azara_raw_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiProcedures
'''
Version: 1, Creation Date: 8/24/2023, Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiProcedures
                    AS
                        SELECT DISTINCT
                            client_id as company_id,
                            client_name,
                            work_order_id,
                            procedure_id as procedure_number,
                            flag,
                            inspection_name as procedure,
                            asset_id,
                            asset_name,
                            inspection_status as procedure_status,
                            document_title as attachment_name,
                            document_url as attachment_url,
                            inspection_order as step,
                            case when status_id = 3 then  'Yes' else 'No' end as done, 
                            case when inspection_detail != '' then inspection_detail else master_inspection_detail end as description,
                            case when response = '1' then 'Yes' when response = '0' then 'No' else response end as response,
                            inspection_comment as comments,
                            case when upper(removed_flag) = 'TRUE' then 'Yes' else 'No' end as deleted,
                            count (inspection_order) over (partition by work_order_id, procedure_id)  total_steps,
                            sum(case when status_id = 3 then 1 else 0 end)  over (partition by work_order_id, procedure_id)  steps_completed,
                            work_order_number,
                            CAST('{refresh_date}' as TIMESTAMP) as UpdateDate,
                            procedure_list_id,
                            case when response = '1' then 'Yes' when response = '0' then 'No' else response end as response_numeric,
                            procedure_category as procedure_category
                        FROM  {var_client_custom_db}.custom_dv_inspections ; """.format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiAssets
'''

Version: 1, Creation Date: 8/24/2023, Created By: Varun Kancharla

'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiAssets 
                AS
                (       
                SELECT DISTINCT
                     client_name as Client_Name
                    ,client_id as Company_ID
                    /** Property Data    **/
                    ,area_id as Property_ID
                    ,area_name as Property_Name
                    ,area_number as Property_Number
                    ,primary_address as Property_Street_Address_1
                    ,secondary_address as Property_Street_Address_2
                    ,city as Property_City
                    ,state_province as Property_State_Prov
                    ,zip as Property_ZIP_Postal_Code
                    ,country as Property_Country
                    /** Area Details    **/
                    ,asset_id as ID
                    ,0 as Asset_Level
                    ,area_name as Asset_Name
                    ,area_name as Full_Path
                    ,coalesce(concat(coalesce(primary_address, ''), ' ', coalesce(secondary_address, '') , ' ', coalesce(city, '') , ' ', coalesce(state_province, '') , ' ', coalesce(zip, '') , ' ', coalesce(country, '')), '') as Asset_Address
                    ,primary_address as Street_Address_1
                    ,secondary_address as Street_Address_2
                    ,city as City
                    ,state_province as State_Prov
                    ,zip as ZIP_Postal_Code
                    ,country as Country
                    ,'Community' as Asset_Category
                    ,'Workzone' as Asset_Model
                    ,'NULL' as Asset_Parent
                    ,'NULL' as Asset_Grandparent
                    ,'NULL' as Tag_ID
                    /**  Refrigerant **/   
                    ,'NULL' as 	Serial_Number
                    ,'NULL' as Capacity
                    ,'NULL' as Refrigerant_Type
                    ,case when is_offline = 1 then 'Offline' else 'Online' end as Status
                    ,case when is_removed = 1 then 'Yes' else 'No' end as Is_Deleted
                    ,'NULL' as UpdateDate
                    ,latitude as Latitude
                    ,longitude as Longitude
                    ,'NULL' as Asset_Parent_ID  
                FROM {var_client_custom_db}.custom_dv_areas 

                UNION ALL

                SELECT DISTINCT
                     dv_spaces.client_name as Client_Name
                    ,dv_spaces.client_id as Company_ID
                    /** Property Data **/
                    ,dv_spaces.area_id as Property_ID
                    ,dv_areas.area_name as Property_Name
                    ,dv_spaces.area_number as Property_Number
                    ,dv_spaces.primary_address as Property_Street_Address_1
                    ,dv_areas.secondary_address as Property_Street_Address_2
                    ,dv_areas.city as Property_City
                    ,dv_areas.state_province as Property_State_Prov
                    ,dv_areas.zip as Property_ZIP_Postal_Code
                    ,dv_areas.country as Property_Country
                    /** Space Details **/
                    ,dv_spaces.space_id as ID
                    ,dv_spaces.space_level as Asset_Level
                    ,dv_spaces.space_name as Asset_Name
                    ,dv_spaces.space_full_path as Full_Path
                    ,coalesce(concat(coalesce(raw_locations.primary_address, ''), ' ', coalesce(raw_locations.secondary_address, '') , ' ', coalesce(raw_locations.city, '') , ' ', coalesce(raw_locations.state_province, '') , ' ', coalesce(raw_locations.zip_code, '') , ' ', coalesce(raw_locations.country, '')), '') as Asset_Address
                    ,raw_locations.primary_address as Street_Address_1
                    ,raw_locations.secondary_address as Street_Address_2
                    ,raw_locations.city as City
                    ,raw_locations.state_province as State_Prov
                    ,raw_locations.zip_code as ZIP_Postal_Code
                    ,raw_locations.country as Country
                    ,dv_spaces.space_category as Asset_Category
                    ,dv_spaces.space_model as Asset_Model
                    ,dv_spaces.space_parent as Asset_Parent
                    ,dv_spaces.space_grandparent as Asset_Grandparent
                    ,'NULL' as Tag_ID
                    /**  Refrigerant **/   
                    ,'NULL' as 	Serial_Number
                    ,'NULL' as Capacity
                    ,'NULL' as Refrigerant_Type
                    ,case when dv_spaces.is_offline = 1 then 'Offline' else 'Online' end as Status
                    ,case when dv_spaces.is_orphan = 1 then 'Yes' else 'No' end as Is_Deleted
                    ,'NULL' as UpdateDate
                    ,raw_locations.latitude as Latitude
                    ,raw_locations.longitude as Longitude
                    ,dv_spaces.parent_id as Asset_Parent_ID
                FROM {var_client_custom_db}.custom_dv_spaces                          dv_spaces
                LEFT JOIN {var_client_custom_db}.custom_dv_areas                      dv_areas
                       ON TRIM(dv_spaces.area_id) = TRIM(dv_areas.area_id)
                LEFT JOIN {var_client_custom_db}.raw_locations_locations_corrigo      raw_locations
                       ON TRIM(dv_spaces.location_id) = TRIM(raw_locations.location_id)       
                -- WHERE IS_Deleted = 0 

                UNION ALL

                SELECT DISTINCT
                     dv_assetclassifications.client_name as Client_Name
                    ,dv_assetclassifications.client_id as Company_ID
                    /** Property Data **/
                    ,dv_assetclassifications.area_id as Property_ID
                    ,dv_areas.area_name as Property_Name
                    ,dv_assetclassifications.area_number as Property_Number
                    ,dv_areas.primary_address as Property_Street_Address_1
                    ,dv_areas.secondary_address as Property_Street_Address_2
                    ,dv_areas.city as Property_City
                    ,dv_areas.state_province as Property_State_Prov
                    ,dv_areas.zip as Property_ZIP_Postal_Code
                    ,dv_areas.country as Property_Country
                    /** Asset Classification Details **/
                    ,dv_assetclassifications.asset_classification_id as ID
                    ,dv_assetclassifications.level as Asset_Level
                    ,dv_assetclassifications.name as Asset_Name
                    ,dv_assetclassifications.full_path as Full_Path
                    ,coalesce(concat(coalesce(raw_locations.primary_address, ''), ' ', coalesce(raw_locations.secondary_address, '') , ' ', coalesce(raw_locations.city, '') , ' ', coalesce(raw_locations.state_province, '') , ' ', coalesce(raw_locations.zip_code, '') , ' ', coalesce(raw_locations.country, '')), '') as Asset_Address
                    ,raw_locations.primary_address as Street_Address_1
                    ,raw_locations.secondary_address as Street_Address_2
                    ,raw_locations.city as City
                    ,raw_locations.state_province as State_Prov
                    ,raw_locations.zip_code as ZIP_Postal_Code
                    ,raw_locations.country as Country
                    ,dv_assetclassifications.category as Asset_Category
                    ,dv_assetclassifications.model as Asset_Model
                    ,dv_assetclassifications.parent as Asset_Parent
                    ,dv_assetclassifications.grand_parent as Asset_Grandparent
                    ,'NULL' as Tag_ID
                    /**  Refrigerant **/   
                    ,'NULL' as 	Serial_Number
                    ,'NULL' as Capacity
                    ,'NULL' as Refrigerant_Type
                    ,case when dv_assetclassifications.is_offline = 1 then 'Offline' else 'Online' end as Status
                    ,case when dv_assetclassifications.is_orphan = 1 then 'Yes' else 'No' end as Is_Deleted
                    ,'NULL' as UpdateDate
                    ,raw_locations.latitude as Latitude
                    ,raw_locations.longitude as Longitude
                    ,Parent_ID as Asset_Parent_ID  
                FROM {var_client_custom_db}.custom_dv_asset_classifications     dv_assetclassifications
                LEFT JOIN {var_client_custom_db}.custom_dv_areas                dv_areas
                       ON TRIM(dv_assetclassifications.area_id)     = TRIM(dv_areas.area_id)
                LEFT JOIN {var_client_custom_db}.raw_locations_locations_corrigo      raw_locations
                       ON TRIM(dv_assetclassifications.location_id) = TRIM(raw_locations.location_id)       
                -- WHERE IS_Deleted = 0 

                UNION ALL

                SELECT DISTINCT
                     dv_equipment.client_name as Client_Name
                    ,dv_equipment.client_id as Company_ID
                    /** Property Data **/
                    ,dv_equipment.area_id as Property_ID
                    ,dv_areas.area_name as Property_Name
                    ,dv_areas.area_number as Property_Number
                    ,dv_areas.primary_address as Property_Street_Address_1
                    ,dv_areas.secondary_address as Property_Street_Address_2
                    ,dv_areas.city as Property_City
                    ,dv_areas.state_province as Property_State_Prov
                    ,dv_areas.zip as Property_ZIP_Postal_Code
                    ,dv_areas.country as Property_Country
                    /** Equipment Details **/
                    ,dv_equipment.equipment_id as ID
                    ,dv_equipment.equipment_level as Asset_Level
                    ,dv_equipment.equipment_name as Asset_Name
                    ,dv_equipment.equipment_full_path as Full_Path
                    ,coalesce(concat(coalesce(dv_areas.primary_address, ''), ' ', coalesce(dv_areas.secondary_address, '') , ' ', coalesce(dv_areas.city, '') , ' ', coalesce(dv_areas.state_province, '') , ' ', coalesce(dv_areas.zip, '') , ' ', coalesce(dv_areas.country, '')), '') as Asset_Address
                    ,dv_equipment.area_address_1 as Street_Address_1
                    ,dv_equipment.area_address_2 as Street_Address_2
                    ,dv_equipment.area_city as City
                    ,dv_equipment.area_state_province as State_Prov
                    ,dv_equipment.area_zip_code as ZIP_Postal_Code
                    ,dv_equipment.area_country_name as Country
                    ,dv_equipment.equipment_category as Asset_Category
                    ,dv_equipment.equipment_model as Asset_Model
                    ,dv_equipment.asset_parent as Asset_Parent
                    ,dv_equipment.asset_grandparent as Asset_Grandparent
                    ,dv_equipment.tag_number as Tag_ID
                    /**  Refrigerant **/   
                    ,dv_equipment.serial_number as 	Serial_Number
                    ,dv_equipment.capacity as Capacity
                    ,dv_equipment.refrigent_type as Refrigerant_Type
                    ,case when lower(dv_equipment.offline_flag) = 'true' then 'Offline' else 'Online' end as Status
                    ,case when lower(dv_equipment.orphan) = 'true' then 'Yes' else 'No' end as Is_Deleted
                    ,'NULL' as UpdateDate
                    ,dv_areas.latitude as Latitude
                    ,dv_areas.longitude as Longitude
                    ,dv_equipment.asset_parent_id as Asset_Parent_ID 
                FROM {var_client_custom_db}.custom_dv_equipment            dv_equipment
                LEFT JOIN {var_client_custom_db}.custom_dv_areas           dv_areas
                       ON TRIM(dv_equipment.area_id)     = TRIM(dv_areas.area_id)
                WHERE lower(dv_equipment.deleted_flag) = 'false' ) ; """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_Corrigo_vbiPMRMScheduling
'''
Version: 1, Creation Date: 08/24/2023, Created By: Varun Kancharla
'''
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_Corrigo_vbiPMRMScheduling
        AS

with  LAST_OCCURRENCE as 
(
SELECT 
	 TENANT_ID,
	 SCHEDULE_ID,
	 WORK_ORDER_ID, 
	EVENT_AT from {var_client_custom_db}.raw_schedule_events_schedevents_corrigo
where  EVENT_AT < CURRENT_TIMESTAMP()  and   is_done='True'
QUALIFY ROW_NUMBER() OVER(PARTITION BY TENANT_ID ,SCHEDULE_ID ORDER BY  EVENT_AT DESC ) =1
),

 NEXT_OCCURRENCE as 
(
SELECT 
	 TENANT_ID,
	 SCHEDULE_ID,
	 WORK_ORDER_ID, 
	EVENT_AT from {var_client_custom_db}.raw_schedule_events_schedevents_corrigo
where  EVENT_AT > CURRENT_TIMESTAMP() and   is_done='False'
QUALIFY ROW_NUMBER() OVER(PARTITION BY TENANT_ID ,SCHEDULE_ID ORDER BY  EVENT_AT  ) =1
)

select distinct 
    h_masterClientsTen.client_name,
    h_masterClientsTen.client_id as company_id , 
    schedules_schedules.schedule_id AS id,
    schedules_schedules.name  as PMRM_Schedule_Name,
    CASE 
		WHEN IS_SUSPENDED='True' THEN 1 
		ELSE 0 
	END IS_SUSPENDED,
    Last_Occurrence.EVENT_AT as Last_Occurrence,	
    Next_Occurrence.EVENT_AT as  Next_Occurrence,
    create_ahead,
    CASE 
		WHEN is_auto_start='True' THEN 1 
		ELSE 0 
	END is_auto_start,
    schedules_schedules.type,
    interval_type as interval,
    frequency,
	(CASE WHEN IS_AUTO_ASSIGN = 'True' THEN 1 WHEN IS_AUTO_ASSIGN = 'False' Then 0 ELSE 'is_auto_assign' END) AS is_auto_assign,
	(CASE WHEN IS_AUTO_SEND = 'True' THEN 1 WHEN IS_AUTO_SEND = 'False' Then 0 ELSE 'is_auto_send' END) AS is_auto_send,
	(CASE WHEN IS_AUTO_EQUIPMENT = 'True' THEN 1 WHEN IS_AUTO_EQUIPMENT = 'False' Then 0 ELSE 'is_auto_equipment' END) AS is_auto_equipment,
    schedules_schedules.work_zone_number as customer,
    contacts_contacts.contact_id,
    contacts_contacts.contact_name,
    FULL_NAME AS ASSIGNED_TO,
 accounts.expense_account,
    po_number,
    bill_to_type,
    CASE 
		WHEN is_pre_bill='True' THEN 1 
		ELSE 0 
	END is_pre_bill,
   vendor_nte as Service_Provider_NTE  ,
   contact_nte as customer_nte,
    billing_rule,
    schedules_schedules.specialty,
    starts_on as start_date,
		ends_on as end_date,
split_part(Last_Occurrence.EVENT_AT,' ',2) start_time,
schedules_schedules.priority,
starts_on as  Season_Start,	
Season_End,
work_zone_number,
last_updated_by,
last_updated_at as last_updated,
    CASE 
		WHEN is_deleted='True' THEN 1 
		ELSE 0 
	END is_deleted,
null contract_reference_id,
CAST('{refresh_date}' as TIMESTAMP) as UpdateDate,
Charge_Code,
schedules_schedules.area_id as WorkZone_ID
from {var_client_custom_db}.raw_schedules_schedules_corrigo schedules_schedules
join {var_client_custom_db}.custom_hv_master_clients_tenants    h_masterClientsTen
                  ON TRIM(schedules_schedules.source_id) = TRIM(h_masterClientsTen.source_id)
                 AND TRIM(schedules_schedules.tenant_id) = TRIM(h_masterClientsTen.tenant_id) 

left join {var_client_custom_db}.raw_contacts_contacts_corrigo contacts_contacts
on contacts_contacts.contact_id=schedules_schedules.contact_id
left join LAST_OCCURRENCE       LAST_OCCURRENCE 
ON TRIM(schedules_schedules.schedule_id) =LAST_OCCURRENCE.schedule_id
left join NEXT_OCCURRENCE       NEXT_OCCURRENCE 
ON TRIM(schedules_schedules.schedule_id) =NEXT_OCCURRENCE.schedule_id 
left join {var_client_custom_db}.custom_dv_service_providers  service_providers 
on service_providers.service_provider_id=schedules_schedules.employee_id
left join {var_client_custom_db}.raw_work_orders_workorders_corrigo workorders 
on workorders.WORK_ORDER_ID=LAST_OCCURRENCE.WORK_ORDER_ID
left join {var_client_custom_db}.raw_work_orders_accountspayable_corrigo  accounts 
on accounts.hk_h_work_orders=workorders.hk_h_work_orders
left join {var_client_custom_db}.raw_work_orders_invoices_corrigo  inv
on workorders.hk_h_work_orders=inv.hk_h_work_orders
""".format(var_client_custom_db=var_client_custom_db,refresh_date=refresh_date))
