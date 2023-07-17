# Databricks notebook source
# MAGIC %md
# MAGIC ###### Notebook for Corrigo Helper Views

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

var_azara_raw_db        = f"{catalog}.jll_azara_raw"
var_azara_business_db   = f"{catalog}.jll_azara_business"

var_client_raw_db    = f"{catalog}.{client_obj.databricks_client_raw_db}"
var_client_custom_db = f"{catalog}.{client_obj.databricks_client_custom_db}"

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# DBTITLE 1,custom_hv_master_clients_tenants
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_master_clients_tenants
                AS
                SELECT
                    rtc.hk_l_masterclients_tenants,
                    rmc.hk_h_master_clients,
                    rtc.hk_h_tenants,
                    rtc.source_id,
                    rmc.client_id,
                    rtc.client_name,
                    rtc.tenant_id,
                    'https://' ||  case
                    when rtc.tenant_name = 'JLL Corporate Offices' then 'jll-corporate-offices'
                    when lower(substr(rtc.tenant_name, 1, 4)) = 'jll ' then 'jll-' || regexp_replace(substr(lower(rtc.tenant_name), 5), '\\s', '')
                    else regexp_replace(lower(rtc.tenant_name), '\\s', '')
                    end || '.corrigo.com/corpnet/workorder/workorderdetails.aspx/' as base_url
                 FROM {var_client_custom_db}.raw_master_clients               rmc
                 JOIN {var_client_custom_db}.raw_tenants_etldatabases_corrigo rtc
                   ON rmc.hk_h_master_clients = rtc.hk_h_master_clients ; """.format(var_azara_raw_db=var_azara_raw_db,var_azara_business_db=var_azara_business_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db))

# COMMAND ----------

# DBTITLE 1,custom_hv_asset_attributes_pivot
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_asset_attributes_pivot
                AS
                SELECT
                    hk_h_assets,
                    max(case
                           when trim(name)='Criticality Band'
                           then value
                        else null end) as criticality_band
                FROM {var_client_custom_db}.raw_asset_attributes_assetattr_corrigo
                WHERE is_removed = 'False'
                group by hk_h_assets ; """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,custom_hv_market_favorability
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_market_favorability 
                AS
                SELECT
                    ref_markets.hk_ref_markets as hk_h_market_favorability,
                    ref_markets.DashboardID as tenant_id,
                    ref_markets.source_id,
                    hv_master_clients_tenants.client_id,
                    ref_edp_properties.hk_ref_edp_properties as hk_h_areas,
                    s_master_properties_properties_propertyhub_current.hk_h_master_properties,
                    ref_markets.PropertyID as property_id,
                    ref_markets.market_base_year as year,
                    ref_markets.market_favor_current as favorability_in_current_year,
                    ref_markets.market_favor_next as favorability_in_next_year,
                    ref_markets.market_favor_2_years_out as favorability_in_two_years,
                    ref_markets.market_favor_3_years_out as favorability_in_three_years,
                    ref_markets.market_favor_4_years_out as favorability_in_four_years,
                    ref_markets.MarketType as market_type,
                    ref_markets.MarketName as market_name,
                    ref_markets.OccupancyMarketName as occupancy_market,
                    ref_markets.MarketCode as market_code,
                    CASE 
                        WHEN LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END as STRING),2) = '12' THEN 'Falling'
                        WHEN LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),2) = '11' OR LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),2) = '10' THEN 'Peaking'
                        WHEN LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),1) = '1' 
                          OR LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),1) = '2' THEN 'Falling'
                        WHEN LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),1) = '3' 
                          OR LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),1) = '4' 
                          OR LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),1) = '5' THEN 'Bottoming'
                        WHEN LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),1) = '6' 
                          OR LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),1) = '7' 
                          OR LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),1) = '8' THEN 'Rising'
                        WHEN LEFT(CAST(CASE ref_markets.ClockPositionTime WHEN 'n/a' THEN NULL 
                            ELSE ref_markets.ClockPositionTime END AS VARCHAR(20)),1) = '9' THEN 'Peaking'
                        ELSE 'Unknown'
                    END as market_cycle, 
                    ref_markets.MarketId as market_id,
                    ref_markets.ClockPositionTime as clock_time,
                    ref_markets.msa as metro_statistical_area_code,
                    ref_markets.msa_name as metro_statistical_area_name
                FROM {var_azara_raw_db}.ref_markets ref_markets 
                join {var_client_custom_db}.custom_hv_master_clients_tenants hv_master_clients_tenants 
                    on hv_master_clients_tenants.tenant_id = ref_markets.dashboardid 
                 and hv_master_clients_tenants.source_id = ref_markets.source_id 
                LEFT JOIN ( {var_azara_raw_db}.h_master_properties h_master_properties 
                                 JOIN {var_client_raw_db}.s_master_properties_properties_propertyhub_current s_master_properties_properties_propertyhub_current 
                                     ON s_master_properties_properties_propertyhub_current.hk_h_master_properties = h_master_properties.hk_h_master_properties) 
                             ON h_master_properties.property_id = ref_markets.ovcp_id 
                LEFT JOIN({var_azara_raw_db}.ref_edp_properties ref_edp_properties 
                                 JOIN {var_azara_raw_db}.ref_edp_companyclients ref_edp_companyclients 
                                     ON ref_edp_companyclients.id = ref_edp_properties.companyidentifier 
                            JOIN {var_azara_raw_db}.h_master_clients h_master_clients 
                                     ON h_master_clients.client_id = ref_edp_companyclients.sourcevalue) 
                             ON ref_edp_properties.sourcevalue = ref_markets.propertyid and ref_edp_properties.sourcetype !='ovcp_id'
                where trim(hv_master_clients_tenants.client_id) = '{var_client_id}' ; """.format(var_azara_raw_db=var_azara_raw_db,var_azara_business_db=var_azara_business_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_client_id=var_client_id))

# COMMAND ----------

# DBTITLE 1,custom_hv_areas_ownership_types  -- 
# spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_areas_ownership_types
#                 AS
#                 SELECT 
#                     s_areas_properties_ovla_current.`hk_h_areas` as hk_h_areas,
#                     CASE s_tenures_tenures_ovla_current.tenuretype 
#                             WHEN 'Main Lease' THEN 'Leased' 
#                             WHEN 'Sub Lease' THEN 'Leased' 
#                             ELSE s_tenures_tenures_ovla_current.tenuretype 
#                     END as ownership_type 
#                 FROM {var_azara_raw_db}.h_tenures h_tenures 
#                 JOIN {var_azara_business_db}.s_tenures_tenures_ovla_current s_tenures_tenures_ovla_current 
#                     ON s_tenures_tenures_ovla_current.hk_h_tenures = h_tenures.hk_h_tenures 
#                 JOIN {var_azara_raw_db}.l_tenures_properties l_tenures_properties 
#                     ON l_tenures_properties.hk_h_tenures = s_tenures_tenures_ovla_current.hk_h_tenures
#                 JOIN {var_client_raw_db}.s_areas_properties_ovla_current s_areas_properties_ovla_current 
#                     ON s_areas_properties_ovla_current.hk_h_areas = l_tenures_properties.hk_h_areas 
#                 LEFT JOIN {var_azara_raw_db}.l_masterproperties_areas l_masterproperties_areas 
#                     ON l_masterproperties_areas.hk_h_areas = s_areas_properties_ovla_current.hk_h_areas
#                 LEFT JOIN {var_azara_raw_db}.h_areas h_areas
#                     ON s_areas_properties_ovla_current.hk_h_areas = h_areas.hk_h_areas
#                 WHERE l_masterproperties_areas.hk_h_areas IS NULL QUALIFY ROW_NUMBER() OVER(PARTITION BY s_areas_properties_ovla_current.hk_h_areas 
#                         ORDER BY CASE s_tenures_tenures_ovla_current.tenuretype  WHEN 'Owned' THEN 1  WHEN 'Main Lease' THEN 2  WHEN 'Sub Lease' THEN 3 END,s_tenures_tenures_ovla_current.leasestartdate NULLS FIRST) =1
#                   AND h_areas.tenant_id = '{var_tenant_id}' or h_areas.tenant_id = '{var_client_id}'   ; """.format(var_azara_raw_db=var_azara_raw_db,var_azara_business_db=var_azara_business_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db,  var_tenant_id=var_tenant_id, var_client_id=var_client_id))

# COMMAND ----------

# DBTITLE 1,custom_hv_work_order_documents_tallies
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_work_order_documents_tallies
                AS
                SELECT
                     hk_h_work_orders
                    ,COUNT(document_object_id) AS documents_tally
                FROM {var_client_custom_db}.raw_documents_documents_corrigo
                GROUP BY hk_h_work_orders ; """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,custom_hv_work_order_activities_latest
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_work_order_activities_latest
                AS
                SELECT 
                      hk_h_work_orders
                     ,substring(max(IFNULL(effective_at || '!@!' || comment, '1900-01-01 00:00:00.000!@!')), 27) as last_action_reason_comment 
                FROM {var_client_custom_db}.raw_work_order_activity_logs_wo_activitylogs_corrigo
                GROUP BY hk_h_work_orders ; """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,custom_hv_work_order_verifications
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_work_order_verifications
                AS
                SELECT
                    CAST(MD5(
                    NVL(CAST(raw_woVerification.tenant_id AS VARCHAR(200)),'null') ||'||'||
                    NVL(CAST(raw_woVerification.work_order_number AS VARCHAR(200)),'null') ||'||'||
                    NVL(CAST(raw_woVerification.work_order_id AS VARCHAR(200)),'null')
                    ) AS CHAR(32)) AS hk_h_work_orders,
                    raw_woVerification.source_id,
                    raw_woVerification.tenant_id,
                    raw_woVerification.work_order_id,
                    raw_woVerification.work_order_number,
                    max(case when raw_woVerification.verification_value
                    in ('Positive','Neutral') then 1 else 0 end) over(partition by raw_woVerification.tenant_id,raw_woVerification.work_order_id,raw_woVerification.work_order_number) AS once_verified_positive_neutral,
                    max(case when raw_woVerification.verification_value 
                    in ('Negative','Not Completed') then 1 else 0 end) over(partition by raw_woVerification.tenant_id,raw_woVerification.work_order_id,raw_woVerification.work_order_number) AS once_verified_negative_not_completed,
                    max(case when raw_woVerification.verification_value
                    in ('Neutral') then 1 else 0 end) over(partition by raw_woVerification.tenant_id,raw_woVerification.work_order_id,raw_woVerification.work_order_number) AS once_verified_neutral,
                    raw_woAccountspayable.ap_status,
                    raw_woVerification.verification_value,
                    dense_rank() over(partition by raw_woVerification.source_id, 
                                                                                 raw_woVerification.tenant_id, 
                                                                                 raw_woVerification.work_order_id, 
                                                                                 raw_woVerification.work_order_number 
                                                                                 order by raw_woVerification.verified_at desc, raw_woVerification.hk_h_work_order_verification_values) AS verification_rank,
                    raw_woVerification.verified_by,
                    raw_woVerification.verified_at,
                    raw_woVerification.verification_comment
                FROM      {var_client_custom_db}.raw_work_order_verification_values_verif_corrigo      raw_woVerification	   
                LEFT JOIN {var_client_custom_db}.raw_work_orders_accountspayable_corrigo               raw_woAccountspayable
                       ON raw_woVerification.hk_h_work_orders = raw_woAccountspayable.hk_h_work_orders
                qualify dense_rank() over(partition by raw_woVerification.source_id,
                     raw_woVerification.tenant_id,
                     raw_woVerification.work_order_id,
                     raw_woVerification.work_order_number order by raw_woVerification.verified_at desc, raw_woVerification.hk_h_work_order_verification_values) =1 ; """.format(var_azara_raw_db=var_azara_raw_db,var_azara_business_db=var_azara_business_db,var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,custom_hv_work_order_source
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_work_order_source
                AS
                SELECT
                    raw_workOrders.hk_h_work_orders,
                    raw_woActivityLogs.action,
                    raw_woActivityLogs.performed_by,
                    raw_woActivityLogs.ui_type                
                FROM {var_client_custom_db}.raw_work_order_activity_logs_wo_activitylogs_corrigo raw_woActivityLogs
                JOIN {var_client_custom_db}.raw_work_orders_workorders_corrigo                   raw_workOrders
                  ON raw_workOrders.hk_h_work_orders = raw_woActivityLogs.hk_h_work_orders
                WHERE raw_woActivityLogs.action = 'Created' ; """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,custom_hv_work_order_inspections_responses
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_work_order_inspections_responses
                AS
                SELECT
                    raw_workOrders.hk_h_work_orders,
                    raw_workOrders.source_id,
                    raw_workOrders.tenant_id,
                    h_masterClientsTen.client_id,
                    sum(case
                    when upper(ltrim(rtrim(raw_inspections.response)))='YES'
                    then 1
                    else 0
                    end) AS assessment_responses_yes_tally,
                    sum(case
                    when upper(ltrim(rtrim(raw_inspections.response)))='NO'
                    then 1
                    else 0
                    end) AS assessment_responses_no_tally,
                    sum(case
                    when upper(ltrim(rtrim(raw_inspections.response))) IN ('N/A','NA')
                    then 1
                    else 0
                    end) AS assessment_responses_na_tally,
                    case
                    when raw_workOrders.speciality= 'Inspection' and raw_workOrders.description like '%EHS%' then 'EHS'
                    when raw_workOrders.speciality= 'Inspection' and raw_workOrders.description like '%DRA%' then 'DRA'
                    when raw_workOrders.speciality= 'Inspection' and raw_workOrders.description like '%VSI%' then 'VSI'
                    else 'Other'
                    end AS assessment_type,
                    case
                    when sum(case when upper(ltrim(rtrim(raw_inspections.response)))='YES' then 1 else 0 end) + sum(case when upper(ltrim(rtrim(raw_inspections.response)))='NO' then 1 else 0 end) = 0 then 0
                    else sum(case when upper(ltrim(rtrim(raw_inspections.response)))='YES' then 1 else 0 end)/(sum(case when upper(ltrim(rtrim(raw_inspections.response)))='YES' then 1 else 0 end)+sum(case when upper(ltrim(rtrim(raw_inspections.response)))='NO' then 1 else 0 end))
                    end AS assessment_proportion
                FROM {var_client_custom_db}.raw_inspections_inspections_corrigo            raw_inspections
                JOIN {var_client_custom_db}.raw_work_orders_workorders_corrigo             raw_workOrders
                  ON raw_inspections.hk_h_work_orders = raw_workOrders.hk_h_work_orders
                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants               h_masterClientsTen
                  ON TRIM(raw_inspections.source_id) = TRIM(h_masterClientsTen.source_id)
                 AND TRIM(raw_inspections.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                GROUP BY raw_workOrders.hk_h_work_orders, raw_workOrders.source_id, raw_workOrders.tenant_id, 
                         h_masterClientsTen.client_id,raw_workOrders.speciality,raw_workOrders.description  """.format(var_azara_raw_db=var_azara_raw_db,var_azara_business_db=var_azara_business_db,var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,custom_hv_assessment_completion
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_assessment_completion
                AS
                SELECT
                    hk_h_work_orders,
                    assessment_type,
                    CASE
                        when assessment_type = 'VSI' and assessment_responses_yes_tally = 56 then 'Complete'
                        when assessment_type = 'VSI' and assessment_responses_yes_tally < 56 and assessment_responses_yes_tally > 0 then 'Incomplete'
                        when assessment_type = 'VSI' and (assessment_responses_yes_tally = 0 or assessment_responses_yes_tally is null) then 'Not Started'
                        when assessment_type = 'DRA' and assessment_responses_yes_tally = 59 then 'Complete'
                        when assessment_type = 'DRA' and assessment_responses_yes_tally < 59 and assessment_responses_yes_tally > 0 then 'Incomplete'
                        when assessment_type = 'DRA' and (assessment_responses_yes_tally = 0 or assessment_responses_yes_tally is null) then 'Not Started'
                        else null
                    END AS assessment_completion,
                        case
                        when assessment_proportion > 0.95 then 1
                        when assessment_proportion > 0.9 and assessment_proportion <= 0.95 then 2
                        when assessment_proportion > 0.8 and assessment_proportion <= 0.9 then 3
                        when assessment_proportion > 0.7 and assessment_proportion <= 0.8 then 4
                        when assessment_proportion <= 0.7 then 5
                    END AS assessment_grade
                FROM {var_client_custom_db}.custom_hv_work_order_inspections_responses ; """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,custom_hv_work_order_equipment_jacs
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.custom_hv_work_order_equipment_jacs 
                AS
                SELECT
                    raw_equipmentWorkedOn.hk_h_work_orders,
                    max(case when c_jacs_standards.jacs_code_number is not null then 1 else 0 end) AS has_valid_jacs
                    
                FROM      {var_client_custom_db}.raw_equipment_worked_on_equipworkedon_corrigo   raw_equipmentWorkedOn
                LEFT JOIN {var_client_custom_db}.raw_assets_assets_corrigo                       raw_assets
                			 ON TRIM(raw_equipmentWorkedOn.asset_id) = TRIM(raw_assets.asset_id)
                JOIN {var_client_custom_db}.custom_hv_master_clients_tenants                     h_masterClientsTen
						      ON TRIM(raw_assets.source_id) = TRIM(h_masterClientsTen.source_id)
						     AND TRIM(raw_assets.tenant_id) = TRIM(h_masterClientsTen.tenant_id)
                LEFT JOIN {var_azara_raw_db}.c_jacs_standards                                                    c_jacs_standards
                       ON TRIM(c_jacs_standards.name) = TRIM(raw_assets.model_name)
                group by raw_equipmentWorkedOn.hk_h_work_orders ;  """.format(var_azara_raw_db=var_azara_raw_db,var_azara_business_db=var_azara_business_db,var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# MAGIC %md