# Databricks notebook source
# DBTITLE 1,Setting up the configs
# Please use these variables for Dev

# Imports
import os
from azarautils import ClientObject

# Client Config
client_obj                   = ClientObject(client_id=os.getenv("CLIENT_ID"),client_name=os.getenv("CLIENT_NAME"))
client_secret_scope          = client_obj.client_secret_scope

catalog                      = client_obj.catalog
var_azara_raw_db             = f"{catalog}.jll_azara_raw"
var_azara_business_db        = f"{catalog}.jll_azara_business"
var_client_raw_db            = f"{catalog}.{client_obj.databricks_client_raw_db}"
var_client_custom_db         = f"{catalog}.{client_obj.databricks_client_custom_db}"

# Client Storage
storage_account              = client_obj.client_azstorage_account
client_data_container        = client_obj.client_data_container
client_deltatables_container = client_obj.client_deltatables_container

# Storage Type
storageType = "abfss"
storage     = "dfs"

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# DBTITLE 1,custom_dv_customers
'''
Creating the dataframe for "df_customers"
'''
df_customers  = spark.sql(""" SELECT DISTINCT
                                rcc.customer_id as customer_number,
                                rcc.customer_name as customer_name,
                                rcc.tenant_code as tenant_code,
                                rcc.doing_business_as as doing_business_As,
                                case upper(rcc.historical_status)  
                                    when 'FALSE' then 0 
                                    when 'TRUE' then 1
                                    else rcc.historical_status end as historical_status,
                                rcc.main_contact as main_contact,
                                rcc.contact_phone_number as contact_phone_number,
                                rcc.contact_email as contact_email,
                                rcc.instructions as instructions,
                                rcc.business_unit_number as bu_number,
                                rcc.primary_address as address_1,
                                rcc.secondary_address as address_2,
                                rcc.city as city,
                                rcc.state_province as state_province,
                                rcc.zip_code as zip_code,
                                rcc.country as country,
                                rac.hk_h_areas,
                                rcc.area_id,
                                rac.area_name as area_name,
                                rac.area_number,
                                rac.property_id as ovcp_id,
                                'Null' as customer_subledger,
                                rcc.space_id as space_id,
                                rcc.tenant_id,
                                rcc.source_id,
                                rcc.hk_h_customers
                            FROM      {var_client_custom_db}.raw_customers_customers_corrigo   rcc
                            LEFT JOIN {var_client_custom_db}.raw_areas_areas_corrigo           rac
                                   ON TRIM(rcc.area_id) = TRIM(rac.area_id)  ; """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_customers"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_customers'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_customers.write.format('delta')\
            .mode('overwrite')\
            .option('path',f'{deltaLakePath}')\
            .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_dv_areas
'''
Creating the dataframe for "df_areas"
'''
df_areas  = spark.sql(""" SELECT DISTINCT
                                 coalesce(r_areas.hk_h_areas,ref_edp_properties.hk_ref_edp_properties) as hk_h_areas
                                ,h_masterClientsTen.client_id as client_id
                                ,h_masterClientsTen.client_name as client_name
                                ,coalesce(r_areas.source_id,ref_edp_properties.source_id) as source_id
                                ,r_areas.tenant_id as tenant_id
                                ,coalesce(r_areas.area_id,ref_edp_properties.id) as area_id
                                ,coalesce(r_areas.area_name,ref_edp_properties.propertyname) as area_name
                                ,r_areas.area_number as area_number
                                ,coalesce(r_areas.property_id, ref_edp_properties.Id) as ovcp_id
                                ,r_areas.is_offline as is_offline
                                ,r_areas.asset_id as asset_id
                                ,r_areas.location_id as location_id
                                ,r_areas.work_order_prefix as wo_prefix
                                ,r_areas.tax_region as tax_region
                                ,r_areas.time_zone as time_zone
                                ,coalesce(r_areas.is_removed,ref_edp_properties.is_deleted) as is_removed
                                ,coalesce(r_areas.primary_address,ref_edp_properties.addressline1text) as primary_address
                                ,coalesce(r_areas.secondary_address,ref_edp_properties.addressline2text) as secondary_address
                                ,coalesce(r_areas.city,ref_edp_properties.cityname) as city
                                ,coalesce(r_areas.zip,ref_edp_properties.postalcode) as zip
                                ,coalesce(r_areas.country,ref_edp_properties.countryname) as country
                                ,coalesce(r_areas.latitude,ref_edp_properties.latitude) as latitude
                                ,coalesce(r_areas.longitude,ref_edp_properties.longitude) as longitude
                                ,coalesce(ref_edp_properties.propertyTypeDescription) as type
                                ,coalesce(ref_edp_properties.clientPropertyMasterCode) as client_code
                                ,ref_edp_properties.ownershipTypeDescription as ownership_type
                                ,coalesce(ref_edp_properties.clientsubregioncode) as primary_region
                                ,coalesce(r_locations.state_province, ref_edp_properties.countrysubentityname) as state_province
                                ,h_marketFav.metro_statistical_area_code as metro_area_code
                                ,h_marketFav.metro_statistical_area_name as metro_area_name
                                ,'NULL' as campus
                                ,coalesce(ref_edp_properties.clientbusinessunitname) as business_unit
                                ,case when ref_edp_properties.hk_ref_edp_properties is not null then 1 else 0 end as is_from_edp  
                            FROM {var_client_custom_db}.raw_areas_areas_corrigo                 r_areas
                            JOIN {var_client_custom_db}.custom_hv_master_clients_tenants        h_masterClientsTen
                              ON TRIM(r_areas.source_id) = TRIM(h_masterClientsTen.source_id)
                             AND TRIM(r_areas.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                            LEFT JOIN {var_client_custom_db}.raw_locations_locations_corrigo    r_locations
                                   ON r_areas.hk_h_areas = r_locations.hk_h_areas
                            FULL OUTER JOIN {var_azara_raw_db}.ref_edp_properties ref_edp_properties
                                         ON r_areas.hk_h_areas = ref_edp_properties.hk_ref_edp_properties
                            LEFT JOIN {var_client_custom_db}.custom_hv_market_favorability      h_marketFav
                                   ON ref_edp_properties.hk_ref_edp_properties = h_marketFav.hk_h_areas
                            WHERE ref_edp_properties.sourcetype!='ovcp_id' OR (ref_edp_properties.hk_ref_edp_properties IS NULL) 
                            OR (ref_edp_properties.hk_ref_edp_properties IS NULL AND r_areas.property_id IS NULL) ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db,var_client_raw_db=var_client_raw_db))

'''
Creating the final table "custom_dv_areas"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_areas'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_areas.write.format('delta') \
        .mode('overwrite') \
        .option('path',f'{deltaLakePath}') \
        .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_dv_employees
'''
Creating the dataframe for "df_employees"
'''
df_employees = spark.sql(""" SELECT DISTINCT
                                     r_employee.hk_h_employees
                                    ,h_masterClientsTen.client_id
                                    ,h_masterClientsTen.client_Name
                                    ,r_employee.source_id
                                    ,r_employee.tenant_id
                                    ,r_employee.employee_id
                                    ,r_employee.login
                                    ,r_employee.user_screen_name
                                    ,r_employee.first_name
                                    ,r_employee.last_name
                                    ,r_employee.employee_number
                                    ,r_employee.job_title
                                    ,r_employee.role
                                    ,r_employee.federal_id
                                    ,r_employee.organization
                                    ,r_employee.language
                                    ,r_employee.bill_zero
                                    ,r_employee.last_action_taken_at
                                    ,r_employee.primary_address
                                    ,r_employee.secondary_address
                                    ,r_employee.city
                                    ,r_employee.state_province
                                    ,r_employee.zip_code
                                    ,r_employee.country
                                    ,r_employee.office_phone
                                    ,r_employee.fax
                                    ,r_employee.mobile_phone
                                    ,r_employee.emergency_phone
                                    ,r_employee.primary_email
                                    ,r_employee.secondary_email
                                    ,r_employee.tertiary_email
                                    ,r_employee.is_supplier
                                    ,r_employee.is_removed
                                    ,r_employee.last_sync_at
                                    ,r_employee.employee_role_id
                                    ,r_employee.is_inactive
                                    ,r_employee.reports_to_id
                                    ,r_employee.employee_type
                                    ,r_employee.employee_type_id
                                    ,case
                                        when lower(r_employee.JOB_TITLE) like any ('%facil%', '%site%') and lower(r_employee.JOB_TITLE) like any ('%manager%', '%mgm%', '%lead%', '%senior%')
                                            then 1
                                        else 0
                                    end as facility_manager_flag
                                FROM {var_client_custom_db}.raw_employees_employees_corrigo          r_employee
                                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants         h_masterClientsTen
                                  ON TRIM(r_employee.source_id) = TRIM(h_masterClientsTen.source_id)
                                 AND TRIM(r_employee.tenant_id) = TRIM(h_masterClientsTen.tenant_id) ; """.format(var_client_custom_db=var_client_custom_db))
                                            
'''
Creating the final table "custom_dv_employees"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_employees'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_employees.write.format('delta') \
            .mode('overwrite') \
            .option('path',f'{deltaLakePath}') \
            .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_dv_asset_attributes
'''
Creating the dataframe for "df_asset_attributes"
'''
df_asset_attributes = spark.sql(""" SELECT DISTINCT
                                                r_assetAttr.hk_h_asset_attributes,
                                                r_assetAttr.hk_h_assets,
                                                h_masterClientsTen.client_name,
                                                r_assetAttr.tenant_id as tenant_id,
                                                h_masterClientsTen.client_id as client_id,
                                                r_assetAttr.asset_id as asset_id,
                                                r_assetAttr.asset_attribute_id as attribute_id,
                                                r_assetAttr.name as attribute_name,
                                                r_assetAttr.value as attribute_value,
                                                r_assetAttr.source_id,
                                                nvl(iff(upper(r_assets.orphan) = 'TRUE',1,0),0) as is_orphan,
                                                nvl(r_assets.deleted_flag,0) as is_deleted,
                                                nvl(iff(upper(r_assetAttr.is_removed) = 'TRUE',1,0),0) as removed_flag,
                                                r_assetAttr.currency_code,
                                                r_assetAttr.data_type
                                            FROM {var_client_custom_db}.raw_asset_attributes_assetattr_corrigo  r_assetAttr
                                            JOIN {var_client_custom_db}.custom_hv_master_clients_tenants        h_masterClientsTen
                                              ON TRIM(r_assetAttr.source_id) = TRIM(h_masterClientsTen.source_id)
                                            AND TRIM(r_assetAttr.tenant_id) = TRIM(h_masterClientsTen.tenant_id) 
                                            LEFT JOIN {var_client_custom_db}.raw_assets_assets_corrigo          r_assets
                                                   ON r_assetAttr.hk_h_assets = r_assets.hk_h_assets ; """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_asset_attributes"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_asset_attributes'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_asset_attributes.write.format('delta')\
                       .mode('overwrite')\
                       .option('path',f'{deltaLakePath}')\
                       .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_dv_inspections
'''
Creating the dataframe for "df_inspections"
'''
df_inspections = spark.sql(""" SELECT DISTINCT
                                    h_masterClientsTen.client_id,
                                    h_masterClientsTen.client_name,
                                    r_inspections.tenant_id,
                                    r_inspections.source_id,
                                    r_inspections.hk_h_inspections,
                                    r_inspections.inspection_id,
                                    r_inspections.work_order_id,
                                    r_inspections.Work_order_number,
                                    r_inspections.procedure_id,
                                    r_inspections.procedure_list_id,
                                    concat(cast(r_inspections.procedure_list_id as string),'-', cast(r_inspections.inspection_order as string)) as procedure_unique_id,
                                    r_inspections.master_inspection_id as master_inspection_detail_id,
                                    r_inspections.inspection_order,
                                    r_inspections.inspection_name,
                                    case when position('VSI',r_inspections.inspection_name) >0 then 'VSI'
                                    when position('DRA', r_inspections.inspection_name) >0 then 'DRA'
                                    else 'N/A'
                                    end as inspection_type,
                                    r_inspections.comment as inspection_comment,
                                    r_inspections.inspection_detail,
                                    r_inspections.master_inspection_detail,
                                    r_inspections.status_id,
                                    r_inspections.exception as flag,
                                    r_inspections.inspection_status,
                                    case 
                                        when LTRIM(RTRIM(lower(r_inspections.response))) = 'yes' then 'Yes' 
                                        when LTRIM(RTRIM(lower(r_inspections.response))) = 'no' then 'No' 
                                        when (LTRIM(RTRIM(lower(r_inspections.response))) = 'na' or LTRIM(RTRIM(lower(r_inspections.response))) = 'n/a') then 'N/A' else r_inspections.response 
                                    end as response,
                                    r_inspections.is_removed as removed_flag,
                                    r_inspections.asset_id,
                                    r_inspections.asset_name,
                                    r_inspections.hk_h_work_orders,
                                    r_documents.document_title,
                                    r_documents.document_url,
                                    'r_inspections.procedure_category' as procedure_category
                                FROM {var_client_custom_db}.raw_inspections_inspections_corrigo       r_inspections
                                LEFT JOIN {var_client_custom_db}.raw_documents_documents_corrigo      r_documents ON r_inspections.hk_h_work_orders = r_documents.hk_h_work_orders and r_documents.object_type_id = 37
                                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants          h_masterClientsTen
                                  ON TRIM(h_masterClientsTen.tenant_id)=TRIM(r_inspections.tenant_id) 
                                 AND TRIM(h_masterClientsTen.source_id)=TRIM(r_inspections.source_id) 
                                 ;  """.format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db))

'''
Creating the final table "custom_dv_inspections"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_inspections'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_inspections.write.format('delta')\
              .mode('overwrite')\
              .option('path',f'{deltaLakePath}')\
              .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_dv_incidents
'''
Creating the dataframe for "df_incidents"
'''
df_incidents = spark.sql(""" SELECT DISTINCT
                                     h_masterClientsTen.client_id
                                    ,h_masterClientsTen.client_name 
                                    ,raw_incidents.tenant_id
                                    ,raw_incidents.source_id
                                    ,raw_incidents.hk_h_incidents
                                    ,raw_incidents.incident_id
                                    ,raw_incidents.incident_number
                                    ,raw_incidents.incident_occurred_at
                                    ,raw_incidents.incident_status
                                    ,raw_incidents.incident_summary
                                    ,raw_incidents.incident_type
                                    ,raw_incidents.incident_category
                                    ,raw_incidents.business_impact
                                    ,raw_incidents.severity
                                    ,raw_incidents.area_id
                                    ,raw_areas.property_id as ovcp_id
                                    ,raw_incidents.area_number
                                    ,raw_areas.area_name
                                    ,raw_incidents.incident_description
                                    ,raw_incidents.last_sync_at
                                    ,raw_incidents.hk_h_work_orders
                                    FROM {var_client_custom_db}.raw_incidents_incidents_corrigo    raw_incidents
                                    LEFT JOIN {var_client_custom_db}.raw_areas_areas_corrigo       raw_areas
                                        ON TRIM(raw_incidents.area_id) = TRIM(raw_areas.area_id)
                                    JOIN {var_client_custom_db}.custom_hv_master_clients_tenants   h_masterClientsTen
                                      ON TRIM(raw_incidents.source_id) = TRIM(h_masterClientsTen.source_id)
                                     AND TRIM(raw_incidents.tenant_id) = TRIM(h_masterClientsTen.tenant_id) ; """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_incidents"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_incidents'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_incidents.write.format('delta')\
            .mode('overwrite') \
            .option('path',f'{deltaLakePath}') \
            .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_dv_workorders
'''
Creating the dataframe for "df_workorders"
'''
df_workorders = spark.sql(""" SELECT DISTINCT
                                    r_workOrders.hk_h_work_orders,
                                    r_woCompletionNote.hk_h_work_orders as hk_h_work_orders_completion_note,
                                    r_workOrders.tenant_id,
                                    r_workOrders.work_order_number,
                                    r_workOrders.work_order_id,
                                    r_workOrders.source_id,
                                    h_masterClientsTen.client_id,
                                    h_masterClientsTen.client_name,
                                    r_workOrders.description,
                                    r_workOrders.status as work_order_status,
                                    r_workOrders.priority,
                                    r_workOrders.speciality,
                                    r_workOrders.customer_contact,
                                    h_woActivitiesLatest.last_action_reason_comment,
                                    h_masterClientsTen.base_url || r_workOrders.work_order_id as application_url,
                                    r_workOrders.time_zone,
                                    r_workOrders.created_at,
                                    r_workOrders.utc_created_at,
                                    r_workOrders.due_at,
                                    r_workOrders.utc_due_at,
                                    r_workOrders.assigned_at,
                                    r_workOrders.on_hold_at,
                                    r_workOrders.paused_at,
                                    r_workOrders.cancelled_at,
                                    r_workOrders.sent_to_provider_at,
                                    r_workOrders.sent_to_provider_last_at,
                                    r_workOrders.last_action_at,
                                    r_workOrders.utc_last_action_at,
                                    r_workOrders.completed_first_at,
                                    r_workOrders.completed_last_at,
                                    r_workOrders.reopened_at,
                                    r_workOrders.picked_up_at,
                                    r_workOrders.started_at,
                                    r_workOrders.utc_started_at,
                                    r_workOrders.onsite_sla_at,
                                    r_workOrders.utc_onsite_sla_at,
                                    r_workOrders.acknowledge_sla_at,
                                    r_workOrders.utc_acknowledge_sla_at,
                                    r_workOrders.vendor_nte,
                                    r_workOrders.type,
                                    r_workOrders.type_category,
                                    case
                                    when iff(iff(status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold'), 1, 0) = 1, datediff(minute, current_timestamp(), due_at), null) < 0 then 'Overdue'
                                    when iff(iff(status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold'), 1, 0) = 1, datediff(minute, current_timestamp(), due_at), null) between 0 and 239 then '<4'
                                    when iff(iff(status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold'), 1, 0) = 1, datediff(minute, current_timestamp(), due_at), null) between 240 and 479 then '4-8h'
                                    when iff(iff(status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold'), 1, 0) = 1, datediff(minute, current_timestamp(), due_at), null) between 480 and 1439 then '8-24h'
                                    when iff(iff(status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold'), 1, 0) = 1, datediff(minute, current_timestamp(), due_at), null) between 1440 and 2879 then '24-48h'
                                    when iff(iff(status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold'), 1, 0) = 1, datediff(minute, current_timestamp(), due_at), null) between 2880 and 4319 then '48-72h'
                                    when iff(iff(status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold'), 1, 0) = 1, datediff(minute, current_timestamp(), due_at), null) > 4320 then '>72'
                                    end as pending_completion_segment,
                                    case
                                    when case when r_workOrders.status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold')
                                    and datediff(minute, utc_due_at, current_timestamp()) < 0
                                    then 1 else 0
                                    end = 1 then chr(8987)
                                    when (case when c_work_order_priority.standard='Emergency' then 1 else 0 end = 1 or case when c_work_order_priority.standard='Urgent' then 1 else 0 end = 1) and case when r_workOrders.status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold')
                                    and datediff(minute, utc_due_at, current_timestamp()) < 0
                                    then 1 else 0
                                    end = 0 then chr(10071)
                                    when iff(datediff(hour,r_workOrders.utc_due_at,current_timestamp()) < 24,1,0)= 1 and case when r_workOrders.status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold')
                                    and datediff(minute, utc_due_at, current_timestamp()) < 0
                                    then 1 else 0
                                    end = 0 and case when c_work_order_priority.standard='Emergency' then 1 else 0 end = 0 and case when c_work_order_priority.standard='Urgent' then 1 else 0 end = 0 then '24h' || chr(9201)
                                    when iff(datediff(hour,r_workOrders.utc_due_at,current_timestamp()) BETWEEN 24 AND 48,1,0) = 1 and case when r_workOrders.status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold')
                                    and datediff(minute, utc_due_at, current_timestamp()) < 0
                                    then 1 else 0
                                    end = 0 and case when c_work_order_priority.standard='Emergency' then 1 else 0 end = 0 and case when c_work_order_priority.standard='Urgent' then 1 else 0 end = 0 and iff(datediff(hour,r_workOrders.utc_due_at,current_timestamp()) < 24,1,0) = 0 then '48h' || chr(9201)
                                    when iff(CASE WHEN r_workOrders.status IN ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold') THEN 1 ELSE 0 END = 1 and datediff(day, r_workOrders.created_at, current_timestamp()) > 30, 1, 0) = 1 and case when r_workOrders.status in ('Open', 'Open: Paused', 'Open: In Progress','New', 'On Hold')
                                    and datediff(minute, utc_due_at, current_timestamp()) < 0
                                    then 1 else 0
                                    end = 0 and case when c_work_order_priority.standard='Emergency' then 1 else 0 end = 0 and case when c_work_order_priority.standard='Urgent' then 1 else 0 end = 0 and iff(datediff(hour,r_workOrders.utc_due_at,current_timestamp() ) < 24,1,0) = 0 and iff(datediff(hour,r_workOrders.utc_due_at,current_timestamp()) BETWEEN 24 AND 48,1,0) = 0 then chr(9888)
                                    else ''
                                    end as requires_attention_icon,
                                    r_workOrders.failure_code,
                                    r_workOrders.failure_category,
                                    r_woCompletionNote.completion_note,
                                    h_woDocumentsTallies.documents_tally,
                                    r_woActions.emergency_escalation_reason,
                                    r_workOrders.picked_up_last_at,
                                    h_woVerifications.verified_by,
                                    h_woVerifications.verified_at,
                                    h_woVerifications.verification_comment,
                                    h_woVerifications.verification_value,
                                    h_woSource.ui_type as origination_source,
                                    case
                                    when r_workOrders.speciality= 'Inspection' and r_workOrders.description like '%EHS%' then 'EHS'
                                    when r_workOrders.speciality= 'Inspection' and r_workOrders.description like '%DRA%' then 'DRA'
                                    when r_workOrders.speciality= 'Inspection' and r_workOrders.description like '%VSI%' then 'VSI'
                                    else 'Other'
                                    end as assessment_type,
                                    h_assessmentCompletion.assessment_completion as assessment_completion,
                                    -- case	
                                    -- when h_assessmentCompletion.assessment_completion = 'Complete' then 'Completed'
                                    -- when h_assessmentCompletion.assessment_completion = 'Incomplete' and r_workOrders.utc_due_at< CURRENT_TIMESTAMP then 'Overdue'
                                    -- when r_workOrders.completed_last_at<= r_workOrders.due_at and h_assessmentCompletion.assessment_completion='Incomplete' and r_workOrders.utc_due_at>= CURRENT_TIMESTAMP then 'Scheduled'
                                    -- when r_workOrders.completed_last_at<= r_workOrders.due_at and h_assessmentCompletion.assessment_completion='Not Started' and r_workOrders.utc_due_at>= CURRENT_TIMESTAMP then'Scheduled'
                                    -- when r_workOrders.completed_last_at > r_workOrders.due_at and h_assessmentCompletion.assessment_completion = 'Not Started' then 'Overdue'
                                    -- when r_workOrders.completed_last_at IS NULL and r_workOrders.last_action_at<= r_workOrders.due_at and h_assessmentCompletion.assessment_completion='Incomplete' and r_workOrders.utc_due_at< CURRENT_TIMESTAMP then 'Overdue'
                                    -- when r_workOrders.completed_last_at IS NULL and r_workOrders.last_action_at<= r_workOrders.due_at and h_assessmentCompletion.assessment_completion='Incomplete' and r_workOrders.utc_due_at>= CURRENT_TIMESTAMP then 'Scheduled'
                                    -- when r_workOrders.completed_last_at IS NULL and r_workOrders.last_action_at<= r_workOrders.due_at and h_assessmentCompletion.assessment_completion='Not Started' and r_workOrders.utc_due_at>= CURRENT_TIMESTAMP then 'Scheduled'
                                    -- when r_workOrders.completed_last_at IS NULL and r_workOrders.last_action_at<= r_workOrders.due_at and h_assessmentCompletion.assessment_completion='Not Started' and r_workOrders.utc_due_at< CURRENT_TIMESTAMP then 'Overdue'
                                    -- when r_workOrders.completed_last_at IS NOT NULL and r_workOrders.last_action_at<= r_workOrders.due_at and h_assessmentCompletion.assessment_completion IS NULL and r_workOrders.utc_due_at>=CURRENT_TIMESTAMP then 'Scheduled'
                                    -- when r_workOrders.completed_last_at IS NOT NULL and r_workOrders.last_action_at<= r_workOrders.due_at and h_assessmentCompletion.assessment_completion IS NULL and r_workOrders.utc_due_atWHEN r_workOrders.completed_last_at IS NULL and r_workOrders.last_action_at<= r_workOrders.due_at then 'Scheduled'
                                    -- when r_workOrders.completed_last_at IS NULL and r_workOrders.last_action_at<= r_workOrders.due_at and h_assessmentCompletion.assessment_completion IS NULL then 'Scheduled'
                                    -- else 'Need Calculation'
                                    -- end as assessment_completion_type,-- Had some issue (Fixing)
                                    'NULL' as assessment_completion_type, 
                                    case 
                                    when r_workOrders.failure_code is not null and r_workOrders.failure_code <> 'Not a Failure' then TRUE
                                    else FALSE
                                    end as has_failure_code_flag,
                                    case when h_woEquipment_jacs.has_valid_jacs = 1 then TRUE
                                    else FALSE end as has_jacs_code_flag,
                                    NVL2(h_woEquipment_jacs.hk_h_work_orders,TRUE,FALSE) as has_engineering_services_flag,
                                    case when c_specialty_trade.hx_flag = 1 then True
                                    else False end as workplace_experience_flag
                                    
                                FROM      {var_client_custom_db}.raw_work_orders_workorders_corrigo                   r_workOrders
                                LEFT JOIN {var_client_custom_db}.raw_work_order_completion_notes_wo_compnote_corrigo  r_woCompletionNote
                                        ON r_workOrders.hk_h_work_orders = r_woCompletionNote.hk_h_work_orders
                                LEFT JOIN {var_client_custom_db}.custom_hv_work_order_activities_latest               h_woActivitiesLatest
                                    ON r_workOrders.hk_h_work_orders = h_woActivitiesLatest.hk_h_work_orders
                                LEFT JOIN {var_client_custom_db}.custom_hv_work_order_documents_tallies               h_woDocumentsTallies
                                    ON r_workOrders.hk_h_work_orders = h_woDocumentsTallies.hk_h_work_orders
                                JOIN      {var_client_custom_db}.custom_hv_master_clients_tenants                     h_masterClientsTen
                                    ON TRIM(r_workOrders.source_id) = TRIM(h_masterClientsTen.source_id)
                                    AND TRIM(r_workOrders.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                                LEFT JOIN {var_azara_raw_db}.c_work_order_priority                                    c_work_order_priority 
                                        ON upper(r_workOrders.priority) = upper(c_work_order_priority.variant)
                                LEFT JOIN {var_client_custom_db}.raw_work_orders_wo_actions_corrigo                   r_woActions
                                        ON r_woActions.hk_h_work_orders = r_workOrders.hk_h_work_orders
                                LEFT JOIN {var_client_custom_db}.custom_hv_work_order_verifications                   h_woVerifications 
                                    ON h_woVerifications.hk_h_work_orders = r_workOrders.hk_h_work_orders 
                                        AND h_woVerifications.verification_rank=1
                                LEFT JOIN {var_client_custom_db}.custom_hv_work_order_source                          h_woSource
                                    ON h_woSource.hk_h_work_orders = r_workOrders.hk_h_work_orders
                                LEFT JOIN {var_client_custom_db}.custom_hv_assessment_completion                      h_assessmentCompletion
                                    ON h_assessmentCompletion.hk_h_work_orders = r_workOrders.hk_h_work_orders
                                LEFT JOIN {var_client_custom_db}.custom_hv_work_order_equipment_jacs                  h_woEquipment_jacs
                                    ON r_workOrders.hk_h_work_orders = h_woEquipment_jacs.hk_h_work_orders
                                LEFT JOIN {var_azara_raw_db}.c_specialty_trade                                        c_specialty_trade
                                    ON r_workOrders.speciality = c_specialty_trade.specialty; """.format(var_azara_raw_db=var_azara_raw_db,var_client_raw_db=var_client_raw_db,var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_workorders"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_workorders'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_workorders.write.format('delta')\
             .mode('overwrite')\
             .option('path',f'{deltaLakePath}')\
             .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_dv_service_providers
'''
Creating the dataframe for "df_service_providers"
'''
df_service_providers = spark.sql(""" SELECT DISTINCT 
                                           raw_service_providers.hk_h_service_providers
                                          ,h_masterClientsTen.client_id
                                          ,h_masterClientsTen.client_name
                                          ,raw_service_providers.source_id
                                          ,raw_service_providers.tenant_id
                                          ,raw_service_providers.service_provider_id
                                          ,raw_service_providers.login
                                          ,raw_service_providers.full_name
                                          ,raw_service_providers.first_name
                                          ,raw_service_providers.last_name
                                          ,raw_service_providers.won_status_id
                                          ,raw_service_providers.connection_status
                                          ,raw_service_providers.provider_label
                                          ,raw_service_providers.service_provider_number
                                          ,raw_service_providers.federal_id
                                          ,raw_service_providers.external_id
                                          ,raw_service_providers.organization
                                          ,raw_service_providers.organization_number
                                          ,raw_service_providers.language
                                          ,raw_service_providers.zip_code
                                          ,raw_service_providers.service_radius
                                          ,cast(raw_service_providers.is_payment_electronic as boolean)
                                          ,raw_service_providers.office_phone
                                          ,raw_service_providers.email
                                          ,raw_service_providers.score
                                          ,raw_service_providers.sla_score
                                          ,raw_service_providers.sla_counts
                                          ,raw_service_providers.satisfaction_score
                                          ,raw_service_providers.satisfaction_counts
                                          ,raw_service_providers.response_sla_score
                                          ,raw_service_providers.response_sla_counts
                                          ,raw_service_providers.completion_sla_score
                                          ,raw_service_providers.completion_sla_counts
                                          ,raw_service_providers.invoice_submit_sla_score
                                          ,raw_service_providers.invoice_submit_sla_counts
                                          ,raw_service_providers.last_invited_on
                                          ,raw_service_providers.default_price_list
                                          ,cast(raw_service_providers.tax_warn_only as boolean) tax_warning_flag
                                          ,cast(raw_service_providers.is_free_text as boolean) free_text_flag
                                          ,cast(raw_service_providers.is_removed as boolean) removed_flag
                                          ,cast(raw_service_providers.is_inactive as boolean) inactive_flag
                                          ,raw_service_providers.primary_address
                                          ,raw_service_providers.secondary_address
                                          ,raw_service_providers.city
                                          ,raw_service_providers.state_province
                                          ,raw_service_providers.country
                                          ,raw_service_providers.last_sync_at
                                     FROM {var_client_custom_db}.raw_service_providers_serviceproviders_corrigo  raw_service_providers
                                     JOIN {var_client_custom_db}.custom_hv_master_clients_tenants          h_masterClientsTen
                                       ON h_masterClientsTen.tenant_id=raw_service_providers.tenant_id 
                                      AND h_masterClientsTen.source_id=raw_service_providers.source_id ; """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_service_providers"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_service_providers'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_service_providers.write.format('delta')\
                    .mode('overwrite') \
                    .option('path',f'{deltaLakePath}') \
                    .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_dv_service_provider_rates
'''
Creating the dataframe for "df_service_provider_rates"
'''
df_service_provider_rates = spark.sql(""" SELECT DISTINCT 
                                                h_masterClientsTen.client_id,
                                                h_masterClientsTen.client_name,
                                                raw_rates.tenant_id,
                                                raw_rates.service_provider_id,
                                                raw_rates.rate_card_id,
                                                raw_rates.hk_h_rates,
                                                raw_rates.login,
                                                raw_rates.item_list_id,
                                                raw_rates.price_list_id,
                                                raw_rates.price_list_name,
                                                raw_rates.portfolio_id,
                                                raw_rates.portfolio_name,
                                                raw_rates.vendor_rate,
                                                raw_rates.vendor_rate_type_id,
                                                raw_rates.currency_id,
                                                raw_rates.currency_name,
                                                raw_rates.currency_code,
                                                raw_rates.vendor_rate_type,
                                                raw_rates.item_name,
                                                raw_rates.item_description,
                                                raw_rates.cost_category,
                                                raw_rates.last_sync_at
                                          FROM {var_client_custom_db}.raw_rates_svcprovrate_corrigo       raw_rates
                                          JOIN {var_client_custom_db}.custom_hv_master_clients_tenants    h_masterClientsTen
                                            ON h_masterClientsTen.tenant_id=raw_rates.tenant_id 
                                           AND h_masterClientsTen.source_id=raw_rates.source_id ; """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_service_provider_rates"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_service_provider_rates'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_service_provider_rates.write.format('delta')\
                    .mode('overwrite') \
                    .option('path',f'{deltaLakePath}') \
                    .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_dv_service_provider_insurance
'''
Creating the dataframe for "df_service_provider_insurance"
'''
df_service_provider_insurance = spark.sql(""" SELECT DISTINCT 
                                                    h_masterClientsTen.client_id,
                                                    h_masterClientsTen.client_name,
                                                    raw_insurance.tenant_id,
                                                    raw_insurance.insurance_id,
                                                    raw_insurance.hk_h_service_provider_insurance,
                                                    raw_insurance.service_provider_id,
                                                    raw_insurance.service_provider_login,
                                                    raw_insurance.insurance_name,
                                                    raw_insurance.coverage_amount,
                                                    raw_insurance.insurance_starts_on,
                                                    raw_insurance.insurance_ends_on,
                                                    raw_insurance.insurance_comment,
                                                    raw_insurance.insurance_status_id,
                                                    raw_insurance.insurance_status,
                                                    raw_insurance.is_expired,
                                                    raw_insurance.last_sync_at
                                               FROM {var_client_custom_db}.raw_service_provider_insurance_svcprovins_corrigo   raw_insurance
                                               JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                    h_masterClientsTen
                                                 ON h_masterClientsTen.tenant_id=raw_insurance.tenant_id 
                                                AND h_masterClientsTen.source_id=raw_insurance.source_id ; """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_service_provider_insurance"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_service_provider_insurance'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_service_provider_insurance.write.format('delta')\
                    .mode('overwrite') \
                    .option('path',f'{deltaLakePath}') \
                    .saveAsTable('{}.{}'.format(Database_Name, Table_Name))   



# COMMAND ----------

# DBTITLE 1,custom_dv_spaces
'''
Creating the dataframe for "df_spaces"
'''
df_spaces = spark.sql(""" SELECT DISTINCT 
                                r_spaces.hk_h_spaces
                               ,h_masterClientsTen.client_id
                               ,h_masterClientsTen.client_name
                               ,r_spaces.source_id
                               ,r_spaces.tenant_id
                               ,r_spaces.space_id
                               ,r_spaces.space_level
                               ,r_spaces.space_name
                               ,r_spaces.space_full_path
                               ,r_spaces.space_category
                               ,r_spaces.space_model
                               ,r_spaces.space_parent
                               ,r_spaces.space_grandparent
                               ,r_spaces.primary_address
                               ,r_spaces.lease_starts_on
                               ,r_spaces.lease_ends_on
                               ,r_spaces.is_offline
                               ,r_spaces.is_orphan
                               ,r_spaces.area_id
                               ,r_spaces.area_number
                               ,r_spaces.location_id
                               ,r_spaces.parent_id
                         FROM {var_client_custom_db}.raw_spaces_spaces_corrigo              r_spaces
                         JOIN {var_client_custom_db}.custom_hv_master_clients_tenants       h_masterClientsTen
                         ON TRIM(r_spaces.source_id) = TRIM(h_masterClientsTen.source_id)
                         AND TRIM(r_spaces.tenant_id) = TRIM(h_masterClientsTen.tenant_id) ; """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_spaces"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_spaces'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_spaces.write.format('delta')\
                    .mode('overwrite') \
                    .option('path',f'{deltaLakePath}') \
                    .saveAsTable('{}.{}'.format(Database_Name, Table_Name)) 



# COMMAND ----------

# DBTITLE 1,custom_dv_asset_classifications
'''
Creating the dataframe for "df_asset_classifications"
'''
df_asset_classifications = spark.sql(""" SELECT Distinct
                                         raw_assetClassify.tenant_id
                                        ,h_masterClientsTen.client_id
                                        ,h_masterClientsTen.client_name
                                        ,raw_assetClassify.asset_classification_id
                                        ,raw_assetClassify.source_id
                                        ,raw_assetClassify.hk_h_asset_classifications
                                        ,raw_assetClassify.level
                                        ,raw_assetClassify.name
                                        ,raw_assetClassify.full_path
                                        ,raw_assetClassify.parent_classification_id
                                        ,raw_assetClassify.category
                                        ,raw_assetClassify.model
                                        ,raw_assetClassify.parent
                                        ,raw_assetClassify.grand_parent
                                        ,raw_assetClassify.space_id
                                        ,raw_assetClassify.area_id
                                        ,raw_assetClassify.location_id
                                        ,raw_assetClassify.area_number
                                        ,raw_assetClassify.is_offline
                                        ,raw_assetClassify.is_orphan
                                        ,raw_assetClassify.parent_id
                    FROM {var_client_custom_db}.raw_asset_classifications_assetclass_corrigo  raw_assetClassify
                    JOIN {var_client_custom_db}.custom_hv_master_clients_tenants              h_masterClientsTen
                      ON h_masterClientsTen.tenant_id=raw_assetClassify.tenant_id 
                     AND h_masterClientsTen.source_id=raw_assetClassify.source_id ; """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_asset_classifications"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_asset_classifications'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_asset_classifications.write.format('delta')\
                    .mode('overwrite') \
                    .option('path',f'{deltaLakePath}') \
                    .saveAsTable('{}.{}'.format(Database_Name, Table_Name)) 

