# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import col, lower, explode, translate, split, trim, first, regexp_replace
from datetime import timezone
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType
import datetime

# COMMAND ----------

# DBTITLE 1,Setting up the configs
# Please use these variables for Dev
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

# DBTITLE 1,custom_dv_equipment
'''
Creating the dataframe for "df_equipment"
'''
df_equipment = spark.sql(""" SELECT DISTINCT
                                     r_assets.hk_h_assets as hk_equipment
                                    ,h_masterClientsTen.client_id
                                    ,h_masterClientsTen.client_name
                                    ,r_assets.source_id
                                    ,r_assets.tenant_id
                                    ,r_assets.asset_id as equipment_id
                                    ,r_assets.name as equipment_name
                                    ,r_assets.level as equipment_level
                                    ,r_assets.path as equipment_full_path
                                    ,r_assets.is_offline as offline_flag
                                    ,r_assets.orphan
                                    ,r_assets.area_id
                                    ,r_areas.area_name
                                    ,r_areas.area_number
                                    ,r_locations.primary_address as area_address_1
                                    ,r_locations.secondary_address as area_address_2
                                    ,r_locations.city as area_city
                                    ,r_locations.state_province as area_state_province
                                    ,r_locations.zip_code as area_zip_code
                                    ,r_locations.country as area_country_name
                                    ,r_assets.type as equipment_category
                                    ,r_assets.model_name as equipment_model
                                    ,r_assets.asset_parent
                                    ,r_assets.asset_grandparent
                                    ,r_assetAttr.value
                                    ,'' as capacity
                                    ,'' as refrigent_type
                                    ,case 
                                        when r_assets.deleted_flag = 1 then TRUE 
                                        else FALSE 
                                    end as deleted_flag
                                    ,r_assets.serial_number
                                    ,r_assets.status
                                    ,r_assets.tag_id as tag_number
                                    ,case
                                            when r_assetAttrPvt.criticality_band is null
                                                then null
                                            when r_assetAttrPvt.criticality_band in ('Critical', 'Very High', 'High', 'Medium', 'Low', 'None')
                                                then r_assetAttrPvt.criticality_band
                                            when r_assetAttrPvt.criticality_band='a.CRITICAL'
                                                then 'Critical'
                                            when r_assetAttrPvt.criticality_band='b.VERY HIGH'
                                                then 'Very High'
                                            when r_assetAttrPvt.criticality_band='c.HIGH'
                                                then 'High'
                                            when r_assetAttrPvt.criticality_band='d.MEDIUM'
                                                then 'Medium'
                                            when r_assetAttrPvt.criticality_band='e.LOW'
                                                then 'Low'
                                            when r_assetAttrPvt.criticality_band='f.NONE'
                                                then 'None'
                                            else 'Other'
                                        end as criticality_band
                                        ,r_areas.latitude
                                        ,r_areas.longitude
                                        ,parent_id as asset_parent_id
                                FROM      {var_client_custom_db}.raw_assets_assets_corrigo                    r_assets
                                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                  h_masterClientsTen
                                  ON TRIM(r_assets.source_id) = TRIM(h_masterClientsTen.source_id)
                                 AND TRIM(r_assets.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                                LEFT JOIN {var_client_custom_db}.raw_asset_attributes_assetattr_corrigo       r_assetAttr
                                       ON r_assets.hk_h_assets = r_assetAttr.hk_h_assets 
                                LEFT JOIN {var_client_custom_db}.custom_hv_asset_attributes_pivot             r_assetAttrPvt
                                       ON r_assets.hk_h_assets = r_assetAttrPvt.hk_h_assets
                                LEFT JOIN {var_client_custom_db}.raw_areas_areas_corrigo                      r_areas
                                       ON r_assets.hk_h_areas = r_areas.hk_h_areas
                                LEFT JOIN {var_client_custom_db}.raw_locations_locations_corrigo              r_locations
                                       ON r_areas.hk_h_areas = r_locations.hk_h_areas
                                WHERE r_assets.type_id=8 ; """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_equipment"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_dv_equipment'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_equipment.write.format('delta') \
                  .mode('overwrite') \
                  .option('path',f'{deltaLakePath}') \
                  .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,ref_time_zones (static table)
if (spark.sql(f"show tables in {var_client_custom_db}")
         .filter(col("tableName") == "ref_time_zones")
         .count() > 0):
    pass   
else:
    file_location = f"abfss://{client_data_container}@{storage_account}.dfs.core.windows.net/corrigo_static_files/ref_timeZones.csv"
    file_type     = "CSV"
    file_header   = "True"
    encoding      = 'utf-8'
    infer_schema  = "True"

    df_timeZone = spark.read.format(file_type).option("header", file_header).option("delimiter", ",").option("encoding", encoding).option("inferSchema", infer_schema)\
                                    .option('multiLine', True).option("quote", '"').option("escape", "\"").load(file_location)
    
    df_timeZone = df_timeZone.withColumn("dss_create_time",current_timestamp()).withColumn("dss_update_time",current_timestamp())

    '''
    Creating the static table "ref_time_zones"
    '''
    Database_Name = var_client_custom_db
    Table_Name = 'ref_time_zones'

    container = client_deltatables_container

    lake_account = storage_account

    blob_path = '/corrigo/'
    table_path = '{}/data/'.format(Table_Name)

    deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

    df_timeZone.write.format('delta') \
                .mode('overwrite') \
                .option('path',f'{deltaLakePath}') \
                .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,custom_work_order_task
'''
Creating the dataframe for "df_workOrderTask"
'''
df_workOrderTask = spark.sql( """   SELECT DISTINCT
                                             h_masterClientsTen.client_name
                                            ,raw_workOrderTasks.tenant_id
                                            ,h_masterClientsTen.client_id
                                            ,raw_workOrderTasks.work_order_id
                                            ,case    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 4 then raw_workOrderTasks.area_id    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 8 then raw_workOrderTasks.asset_id    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (1, 5, 6, 7) then raw_workOrderTasks.classification_id    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (2, 3) then raw_workOrderTasks.space_id    
                                                else null   
                                            end as asset_id
                                            ,case    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 4 then areas.name    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 8 then equipment.name    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (1, 5, 6, 7) then assetClassifications.name    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (2, 3) then spaces.name    
                                                else null   
                                            end as asset_name
                                            ,raw_workOrderTasks.task as task_name
                                            ,raw_workOrderTasks.comment as task_comment
                                            ,raw_workOrderTasks.task_status
                                            ,raw_workOrderTasks.task_code as task_code
                                            ,case    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 4 then 'Workzone'    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 8 then equipment.model_name    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (1, 5, 6, 7) then assetClassifications.model_name    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (2, 3) then spaces.model_name    
                                                else null   
                                            end as asset_model 
                                            ,case    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 4 then 'Community'    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 8 then equipment.type    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (1, 5, 6, 7) then assetClassifications.type    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (2, 3) then spaces.type    
                                                else null   
                                            end as asset_category
                                            ,case    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 4 then areas.name    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) = 8 then equipment.path    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (1, 5, 6, 7) then assetClassifications.path    
                                                when COALESCE(equipment.type_id,areas.type_id,assetClassifications.type_id,spaces.type_id) in (2, 3) then spaces.path    
                                                else null
                                            end as asset_full_path 
                                            ,raw_workOrderTasks.work_order_task_id
                                            ,COALESCE(equipment.deleted_flag,areas.deleted_flag,assetClassifications.deleted_flag,spaces.deleted_flag) as is_deleted
                                    FROM      {var_client_custom_db}.raw_work_order_tasks_wo_tasks_corrigo              raw_workOrderTasks
                                    LEFT JOIN {var_client_custom_db}.raw_assets_assets_corrigo                          equipment
                                           ON raw_workOrderTasks.asset_id = equipment.asset_id
                                    LEFT JOIN {var_client_custom_db}.raw_assets_assets_corrigo                          areas
                                           ON raw_workOrderTasks.area_id = areas.asset_id
                                    LEFT JOIN {var_client_custom_db}.raw_assets_assets_corrigo                          spaces
                                           ON raw_workOrderTasks.space_id = spaces.asset_id
                                    LEFT JOIN {var_client_custom_db}.raw_assets_assets_corrigo                          assetClassifications
                                           ON raw_workOrderTasks.classification_id = assetClassifications.asset_id
                                    JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                        h_masterClientsTen
                                      ON TRIM(raw_workOrderTasks.source_id) = TRIM(h_masterClientsTen.source_id)
                                     AND TRIM(raw_workOrderTasks.tenant_id) = TRIM(h_masterClientsTen.tenant_id) """.format(var_client_custom_db=var_client_custom_db))

'''
Creating the final table "custom_dv_work_order_task"
'''
Database_Name = var_client_custom_db
Table_Name = 'custom_work_order_task'

container = client_deltatables_container

lake_account = storage_account

blob_path = '/corrigo/'
table_path = '{}/data/'.format(Table_Name)

deltaLakePath = "{storageType}://{container}@{lake_account}.{storage}.core.windows.net{blob_path}{table_path}".format(storageType=storageType,container=container,lake_account=lake_account,storage=storage,blob_path=blob_path,table_path=table_path)

df_workOrderTask.write.format('delta') \
                .mode('overwrite') \
                .option('path',f'{deltaLakePath}') \
                .saveAsTable('{}.{}'.format(Database_Name, Table_Name))

# COMMAND ----------

# DBTITLE 1,Corrigo_Custom_Customer
# Final Table Name
Table_Name = 'Corrigo_Custom_Customer'

# Target Column list
target_col_list = [ "object_id"
                    ,"Super_Region"
                    ,"DID"
                    ,"BU_Number"
                    ,"Work_Zone_ID"
                    ,"External_Property_ID"
                    ,"Escalation_1"
                    ,"Escalation_1_Phone"
                    ,"Escalation_2"
                    ,"Escalation_2_Phone"
                    ,"Escalation_3"
                    ,"Escalation_3_Phone"
                    ,"After_Hours_Escalation_1_Shift_Phone"
                    ,"After_Hours_Escalation_2"
                    ,"After_Hours_Escalation_2_Phone"
                    ,"After_Hours_Escalation_3"
                    ,"After_Hours_Escalation_3_Phone"
                    ,"Status"
                    ,"Union_Yes_No" 
                    ,"Union_Scope"
                    ,"FM_Lead"
                    ,"FM_Onsite_Frequency"
                    ,"Chief_Engineer"
                    ,"Chief_Engineer_Phone"
                    ,"Assistant_Chief_Engineer"
                    ,"Assistant_Chief_Engineer_Phone"
                    ,"Senior_Facility_Manager"
                    ,"Senior_Facility_Manager_Phone"
                    ,"Facility_Manager"
                    ,"Facility_Manager_Phone"
                    ,"Assistant_Facility_Manager"
                    ,"Assistant_Facility_Manager_Phone"
                    ,"Regional_Facility_Manager"
                    ,"Regional_Facility_Manager_Phone"
                    ,"Regional_Engineering_Manager"
                    ,"Regional_Engineering_Manager_Phone"
                    ,"Regional_Lead"
                    ,"Regional_Lead_Phone"
                    ,"Natural_Gas"
                    ,"Inspections_Daily_Critical_Checks"
                    ,"Inspections_Weekly_Building_Rounds"
                    ,"Inspections_Monthly_Fire_Extinguisher"
                    ,"Inspections_Monthly_FM_Site_Inspection"
                    ,"Inspections_Monthly_Kitchen_Documents_Upload"
                    ,"Inspections_Monthly_PA_System_Work_Effort"
                    ,"Inspections_Monthly_Roof_Inspection"
                    ,"Inspections_Monthly_Satellite_Phone_Service_Test"
                    ,"Inspections_Monthly_SDS_Inspection"
                    ,"Inspections_Monthly_Spill_Response_Kit_and_AST_Inspections"
                    ,"Inspections_Monthly_Vehicle_Inspection"
                    ,"Inspections_Quarterly_Emergency_Response_Mock_Drills"
                    ,"Inspections_Quarterly_Portable_Ladder_Inspection"
                    ,"Inspections_Annual_FCI_Inspection"
                    ,"Inspections_Annual_Clock_Inspection_Fall_Back"
                    ,"Inspections_Annual_Clock_Inspection_Spring_Forward"
                    ,"Inspections_Annual_Fixed_Ladder_Inspection"
                    ,"Inspections_Annual_JLL_First_Aid_Kit_Inspections"
                    ,"Inspections_Annual_SDS_Inspection"
                    ,"Inspections_Annual_Spill_Response_Kit_and_AST_Inspection"]

# Reading data from "custom_attribute_values" into dataframe
df = spark.read.table(f'{var_client_custom_db}.raw_custom_attribute_values')
attr_val_df = df.filter(lower(df['actor_type']) == 'customer').withColumnRenamed("source_id","attr_source_id").withColumnRenamed("tenant_id","attr_tenant_id")

# Reading data from "custom_hv_master_clients_tenants" into dataframe
client_df = spark.read.table(f'{var_client_custom_db}.custom_hv_master_clients_tenants')

# Perform an inner join on the common key column (assuming it's "common_column")
join_condition = attr_val_df["attr_tenant_id"] == client_df["tenant_id"]
joined_df = attr_val_df.join(client_df, join_condition, "inner")

#Converting attributes to a list, exploding the rows and removing special characters 
list_df = attr_val_df.select(["object_id","attributes"]).\
    withColumn("attributes", regexp_replace(col("attributes"),'^{"|"}$', "")).\
        withColumn("attributes", split(col("attributes"),'","')).withColumn("attributes", explode(col("attributes")))

#splitting the exploded rows to column name and values and trimming for whitespaces in column name
split_df = list_df.withColumn("att_col", split(col("attributes"),'":"').getItem(0)).\
    withColumn("att_val", split(col("attributes"),'":"').getItem(1))

#Replacing whitespaces and slashes with "_", removing brackets replacing multiple "_" with a single one           
split_df = split_df.withColumn("att_col", regexp_replace(col("att_col"),"[^a-zA-Z0-9_]+", "_")).\
    withColumn("att_col", regexp_replace(col("att_col"),"^_|_$", "")).withColumn("att_val",translate("att_val",'\\',""))

#Transposing the rows for attribute columns
transpose_df = split_df.groupBy("object_id").pivot('att_col').agg(first('att_val'))

#Joining transposed DF with existing joined_df
df3 = joined_df.join(transpose_df,"object_id","left")

#Final column list having all target and derived column names
missing_col_list = list(set(target_col_list) - set(df3.columns))

for colm in missing_col_list:
    df3 = df3.withColumn(colm, lit(None).cast(StringType()))

# Save the result as a new table
df3.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{var_client_custom_db}.{Table_Name}")

# COMMAND ----------

# DBTITLE 1,Corrigo_Custom_User
# Final Table Name
Table_Name = 'Corrigo_Custom_User'

# Target Column list
target_col_list = [ "object_id"
                    ,"Dedicated_Client"
                    ,"JLL_Organization"
                    ,"Peoplesoft_Property_ID"
                    ,"Union_Local"
                    ,"Weekly_Scheduled_Hours"
                    ,"Primary_Job_Function"
                    ,"Exemption_Status"
                    ,"Time_Keeper"
                    ,"Time_Approver"
                    ,"CapOne_EID"
                    ,"Region"]

# Reading data from "custom_attribute_values" into dataframe
df = spark.read.table(f'{var_client_custom_db}.raw_custom_attribute_values')
attr_val_df = df.filter(lower(df['actor_type']) == 'user').withColumnRenamed("source_id","attr_source_id").withColumnRenamed("tenant_id","attr_tenant_id")

# Reading data from "custom_hv_master_clients_tenants" into dataframe
client_df = spark.read.table(f'{var_client_custom_db}.custom_hv_master_clients_tenants')

# Perform an inner join on the common key column (assuming it's "common_column")
join_condition = attr_val_df["attr_tenant_id"] == client_df["tenant_id"]
joined_df = attr_val_df.join(client_df, join_condition, "inner")

#Converting attributes to a list, exploding the rows and removing special characters 
list_df = attr_val_df.select(["object_id","attributes"]).\
    withColumn("attributes", regexp_replace(col("attributes"),'^{"|"}$', "")).\
        withColumn("attributes", split(col("attributes"),'","')).withColumn("attributes", explode(col("attributes")))

#splitting the exploded rows to column name and values and trimming for whitespaces in column name
split_df = list_df.withColumn("att_col", split(col("attributes"),'":"').getItem(0)).\
    withColumn("att_val", split(col("attributes"),'":"').getItem(1))

#Replacing whitespaces and slashes with "_", removing brackets replacing multiple "_" with a single one           
split_df = split_df.withColumn("att_col", regexp_replace(col("att_col"),"[^a-zA-Z0-9_]+", "_")).\
    withColumn("att_col", regexp_replace(col("att_col"),"^_|_$", "")).withColumn("att_val",translate("att_val",'\\',""))

#Transposing the rows for attribute columns
transpose_df = split_df.groupBy("object_id").pivot('att_col').agg(first('att_val'))

#Joining transposed DF with existing joined_df
df3 = joined_df.join(transpose_df,"object_id","left")

#Final column list having all target and derived column names
missing_col_list = list(set(target_col_list) - set(df3.columns))

for colm in missing_col_list:
    df3 = df3.withColumn(colm, lit(None).cast(StringType()))

# Save the result as a new table
df3.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{var_client_custom_db}.{Table_Name}")

# COMMAND ----------

# DBTITLE 1,Corrigo_Custom_WorkOrder
# Final Table Name
Table_Name = 'Corrigo_Custom_WorkOrder'

# Target Column list
target_col_list = [ "object_id"
                    ,"Accidental_Release_lbs"
                    ,"ADA_Reporting"
                    ,"Capital_One_ESS"
                    ,"Change_Control_Number"
                    ,"Contingency_Spend"
                    ,"Diesel_Fuel_Usage"
                    ,"Diesel_Quantity_gal"
                    ,"Energy_Savings"
                    ,"Event_Disaster_Storm_Related"
                    ,"Financial_Governance"
                    ,"Help_Desk_Review"
                    ,"HR_Case_Number"
                    ,"JCAP_OCP_Section"
                    ,"JCAP_Admin_Only_WO"
                    ,"JDE_Export_Schedule_Start"
                    ,"Manual_Work_Order_Number"
                    ,"MCIM_FCI_Assessment_Link"
                    ,"MCIM_Assessments"
                    ,"NOC_Reactive_Proactive"
                    ,"NOC_Related"
                    ,"PRIME_Audit_Work_Order"
                    ,"Propane_Fuel_Usage"
                    ,"Propane_Quantity_lbs"
                    ,"Property_Management_Billing"
                    ,"Quality_Control_Audit"
                    ,"Refrigerant_Added_lbs"
                    ,"Refrigerant_Recovered_lbs"
                    ,"Refrigerant_Type"
                    ,"Regulatory_Agency"
                    ,"Re_Opening_Reason"
                    ,"SLA_Extension"
                    ,"Unique_Contract_Identifier_PM_PMRW_Schedules"]

# Reading data from "custom_attribute_values" into dataframe
df = spark.read.table(f'{var_client_custom_db}.raw_custom_attribute_values')
attr_val_df = df.filter(lower(df['actor_type']) == 'work order').withColumnRenamed("source_id","attr_source_id").withColumnRenamed("tenant_id","attr_tenant_id")

# Reading data from "custom_hv_master_clients_tenants" into dataframe
client_df = spark.read.table(f'{var_client_custom_db}.custom_hv_master_clients_tenants')

# Perform an inner join on the common key column (assuming it's "common_column")
join_condition = attr_val_df["attr_tenant_id"] == client_df["tenant_id"]
joined_df = attr_val_df.join(client_df, join_condition, "inner")

#Converting attributes to a list, exploding the rows and removing special characters 
list_df = attr_val_df.select(["object_id","attributes"]).\
    withColumn("attributes", regexp_replace(col("attributes"),'^{"|"}$', "")).\
        withColumn("attributes", split(col("attributes"),'","')).withColumn("attributes", explode(col("attributes")))

#splitting the exploded rows to column name and values and trimming for whitespaces in column name
split_df = list_df.withColumn("att_col", split(col("attributes"),'":"').getItem(0)).\
    withColumn("att_val", split(col("attributes"),'":"').getItem(1))

#Replacing whitespaces and slashes with "_", removing brackets replacing multiple "_" with a single one           
split_df = split_df.withColumn("att_col", regexp_replace(col("att_col"),"[^a-zA-Z0-9_]+", "_")).\
    withColumn("att_col", regexp_replace(col("att_col"),"^_|_$", "")).withColumn("att_val",translate("att_val","\\",""))

#Transposing the rows for attribute columns
transpose_df = split_df.groupBy("object_id").pivot('att_col').agg(first('att_val'))

#Joining transposed DF with existing joined_df
df3 = joined_df.join(transpose_df,"object_id","left")

#Final column list having all target and derived column names
missing_col_list = list(set(target_col_list) - set(df3.columns))

for colm in missing_col_list:
    df3 = df3.withColumn(colm, lit(None).cast(StringType()))

# Save the result as a new table
df3.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{var_client_custom_db}.{Table_Name}")

# COMMAND ----------


