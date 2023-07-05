# Databricks notebook source
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
var_client_base_db   = f"{catalog}.{client_obj.databricks_client_base_db}"
var_client_custom_db = f"{catalog}.{client_obj.databricks_client_custom_db}"

# COMMAND ----------

# DBTITLE 1,Clientssdv_vw_CapitalOne_OneView_Client_Property (old)
# spark.sql("""
# CREATE or replace VIEW 
#     {var_client_custom_db}.Clientssdv_vw_CapitalOne_OneView_Client_Property AS
# select
#   pr.client_id AS oneviewclientid, 
#   pr.client_name AS txtclientname, 
#   pr.is_deleted AS isdeleted, 
#   mp.property_id AS oneviewclientpropertyid, 
#   pr.area_type AS area_type, 
#   pr.campus AS campus, 
#   pr.industry_sector AS industry_sector, 
#   pr.leased_owned AS leased_owned, 
#   pr.property_type AS property_type, 
#   pr.site_group AS site_group, 
#   pr.status AS status, 
#   pr.unit_of_measure AS unit_of_measure, 
#   pr.address_1 AS address_1, 
#   pr.address_2 AS address_2, 
#   pr.address_3 AS address_3, 
#   pr.address_4 AS address_4, 
#   pr.city AS city, 
#   pr.client_property_code AS client_property_code, 
#   pr.comments AS comments, 
#   pr.country AS country, 
#   pr.county AS county, 
#   cast(pr.import_latitude as decimal(18,12)) AS import_latitude, 
#   cast(pr.import_longitude as decimal(18,12)) AS import_longitude, 
#   pr.postal_code AS postal_code, 
#   pr.property_name AS property_name, 
#   pr.rsf AS rsf, 
#   pr.site_id AS site_id, 
#   pr.state_province AS state_province, 
#   pr.time_zone AS time_zone, 
#   pr.region_hierarchy_1 AS region_hierarchy_1, 
#   pr.region_hierarchy_2 AS region_hierarchy_2, 
#   pr.region_hierarchy_3 AS region_hierarchy_3, 
#   pr.region_hierarchy_4 AS region_hierarchy_4, 
#   pr.region_hierarchy_5 AS region_hierarchy_5, 
#   pr.region_hierarchy_6 AS region_hierarchy_6, 
#   pr.region_hierarchy_7 AS region_hierarchy_7, 
#   pr.region_hierarchy_8 AS region_hierarchy_8, 
#   pr.updated_by AS updated_by, 
#   pr.updated_date AS updated_date, 
#   pr.mdm_id AS mdm_id, 
#   pr.request_status AS request_status, 
#   pr.auto_approved_date AS autoapproveddate, 
#   pr.who_auto_approved AS who_auto_approved, 
#   pr.request_source AS request_source, 
#   pr.reviewed_post_auto AS reviewed_post_auto, 
#   pr.hub_process AS hub_process, 
#   pr.alternate_property_name AS alternate_property_name, 
#   pr.created_date AS created_date, 
#   pr.created_by AS created_by, 
#   pr.occupied_vacant AS occupied_vacant, 
#   pr.requestor_name AS requestor_name, 
#   pr.requestor_email AS requestor_email, 
#   pr.approved_rejected_note AS approved_rejected_note, 
#   pr.rejected_date AS rejected_date, 
#   pr.approved_date AS approved_date, 
#   pr.reporting_business_unit AS reporting_business_unit, 
#   pr.property_subtype AS property_subtype, 
#   pr.property_approval AS property_approval, 
#   pr.auto_approve_review_status AS auto_approve_review_status, 
#   pr.client_region_level_1 AS clientregion_level1, 
#   pr.ovcp_view_id AS ovcp_view_id, 
#   pr.service_start_date AS service_start_date, 
#   pr.service_end_date AS service_end_date, 
#   pr.country_iso_2_char AS countryiso2char, 
#   pr.country_iso_3_char AS countryiso3char, 
#   pr.country_iso_3_digit AS countryiso3digit, 
#   pr.request_property_name AS request_property_name, 
#   pr.request_address_1 AS request_address_1, 
#   pr.request_address_2 AS request_address_2, 
#   pr.request_city AS request_city, 
#   pr.request_state AS request_state, 
#   pr.request_postal_code AS request_postal_code, 
#   pr.request_country AS request_country, 
#   pr.mdm_tid AS mdm_tid, 
#   pr.gmt_offset AS gmt_offset, 
#   pr.mailability_score AS mailability_score 
  
# from {var_client_raw_db}.s_master_properties_properties_propertyhub_current pr 
# INNER JOIN {var_azara_raw_db}.h_master_properties mp on pr.hk_h_master_properties = mp.hk_h_master_properties 
# """.format(var_client_raw_db=var_client_raw_db, var_azara_raw_db=var_azara_raw_db, var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,Clientssdv_vw_CapitalOne_OneView_Client_Property
spark.sql("""
CREATE or replace VIEW 
    {var_client_custom_db}.Clientssdv_vw_CapitalOne_OneView_Client_Property AS
WITH 
    _properties_propertyhub AS (
    select 
        *
    from 
        (SELECT 
            *,  ROW_NUMBER() 
        OVER 
            ( PARTITION BY hk_h_master_properties  
              ORDER BY  
              updated_date desc,  dss_load_date desc
            ) row_num 
        FROM 
            {var_client_raw_db}.s_master_properties_properties_propertyhub_current ) A  
    WHERE row_num = 1
)
select
  pr.client_id AS oneviewclientid, 
  pr.client_name AS txtclientname, 
  pr.is_deleted AS isdeleted, 
  d1.property_id AS oneviewclientpropertyid, 
  pr.area_type AS area_type, 
  pr.campus AS campus, 
  pr.industry_sector AS industry_sector, 
  pr.leased_owned AS leased_owned, 
  pr.property_type AS property_type, 
  pr.site_group AS site_group, 
  pr.status AS status, 
  pr.unit_of_measure AS unit_of_measure, 
  pr.address_1 AS address_1, 
  pr.address_2 AS address_2, 
  pr.address_3 AS address_3, 
  pr.address_4 AS address_4, 
  pr.city AS city, 
  pr.client_property_code AS client_property_code, 
  pr.comments AS comments, 
  pr.country AS country, 
  pr.county AS county, 
  cast(pr.import_latitude as DOUBLE) AS import_latitude, 
  cast(pr.import_longitude as DOUBLE) AS import_longitude, 
  pr.postal_code AS postal_code, 
  pr.property_name AS property_name, 
  pr.rsf AS rsf, 
  pr.site_id AS site_id, 
  pr.state_province AS state_province, 
  pr.time_zone AS time_zone, 
  pr.region_hierarchy_1 AS region_hierarchy_1, 
  pr.region_hierarchy_2 AS region_hierarchy_2, 
  pr.region_hierarchy_3 AS region_hierarchy_3, 
  pr.region_hierarchy_4 AS region_hierarchy_4, 
  pr.region_hierarchy_5 AS region_hierarchy_5, 
  pr.region_hierarchy_6 AS region_hierarchy_6, 
  pr.region_hierarchy_7 AS region_hierarchy_7, 
  pr.region_hierarchy_8 AS region_hierarchy_8, 
  pr.updated_by AS updated_by, 
  pr.updated_date AS updated_date, 
  pr.mdm_id AS mdm_id, 
  pr.request_status AS request_status, 
  pr.auto_approved_date AS autoapproveddate, 
  pr.who_auto_approved AS who_auto_approved, 
  pr.request_source AS request_source, 
  pr.reviewed_post_auto AS reviewed_post_auto, 
  pr.hub_process AS hub_process, 
  pr.alternate_property_name AS alternate_property_name, 
  pr.created_date AS created_date, 
  pr.created_by AS created_by, 
  pr.occupied_vacant AS occupied_vacant, 
  pr.requestor_name AS requestor_name, 
  pr.requestor_email AS requestor_email, 
  pr.approved_rejected_note AS approved_rejected_note, 
  pr.rejected_date AS rejected_date, 
  pr.approved_date AS approved_date, 
  pr.reporting_business_unit AS reporting_business_unit, 
  pr.property_subtype AS property_subtype, 
  pr.property_approval AS property_approval, 
  pr.auto_approve_review_status AS auto_approve_review_status, 
  pr.client_region_level_1 AS clientregion_level1, 
  pr.ovcp_view_id AS ovcp_view_id, 
  pr.service_start_date AS service_start_date, 
  pr.service_end_date AS service_end_date, 
  pr.country_iso_2_char AS countryiso2char, 
  pr.country_iso_3_char AS countryiso3char, 
  pr.country_iso_3_digit AS countryiso3digit, 
  pr.request_property_name AS request_property_name, 
  pr.request_address_1 AS request_address_1, 
  pr.request_address_2 AS request_address_2, 
  pr.request_city AS request_city, 
  pr.request_state AS request_state, 
  pr.request_postal_code AS request_postal_code, 
  pr.request_country AS request_country, 
  pr.mdm_tid AS mdm_tid, 
  pr.gmt_offset AS gmt_offset, 
  pr.mailability_score AS mailability_score 
from 
    _properties_propertyhub pr 
JOIN 
    {var_client_base_db}.dv_master_properties d1 
on 
    d1.hk_h_properties = pr.hk_h_master_properties 
""".format(var_client_raw_db=var_client_raw_db, var_client_base_db=var_client_base_db, var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_OneView_Client (old)
# spark.sql("""
# CREATE or replace VIEW 
#     {var_client_custom_db}.ssdv_vw_OneView_Client AS
#  select  
#     7745730 AS OneViewClientID,
#     client_name,
#     is_deleted as isdeleted,
#     city,
#     state_province as State,
#     country,
#     address_1 as Address_Line_1,
#     address_2 as Address_Line_2,
#     postal_code,
#     jll_vertical,
#     peoplesoft_code,
#     ad_security_group,
#     pdm_status,
#     property_tracker_id as property_trackerid,
#     person_tracker_id as person_trackerid,
#     services,
#     usage,
#     service_providers_jpmc,
#     updated_date,
#     ovcp_view_id,
#     jll_region,
#     jll_business_group,
#     client_industry,
#     prod_ovcp_site_active,
#     transition_migration_data_manager,
#     transition_migration
#   from  {var_client_raw_db}.s_master_clients_clients_propertyhub_current
#   """.format(var_client_raw_db=var_client_raw_db, var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_oneview_client
spark.sql("""
CREATE OR REPLACE VIEW 
    {var_client_custom_db}.ssdv_vw_OneView_Client AS
SELECT 
    CAST(cl.client_id AS BIGINT)  as oneviewclientid
    ,CAST(pr.client_name AS STRING)  as client_name
    ,CAST(pr.is_deleted AS STRING)  as isdeleted
    ,CAST(pr.city AS STRING)  as city
    ,CAST(pr.state_province AS STRING)  as state
    ,CAST(pr.country AS STRING)  as country
    ,CAST(pr.address_1 AS STRING)  as address_line_1
    ,CAST(pr.address_2 AS STRING)  as address_line_2
    ,CAST(pr.postal_code AS STRING)  as postal_code
    ,CAST(pr.jll_vertical AS STRING)  as jll_vertical
    ,CAST(pr.peoplesoft_code AS STRING)  as peoplesoft_code
    ,CAST(pr.ad_security_group AS STRING)  as ad_security_group
    ,CAST(pr.pdm_status AS STRING)  as pdm_status
    ,CAST(pr.property_tracker_id AS STRING)  as property_trackerid
    ,CAST(pr.person_tracker_id AS STRING)  as person_trackerid
    ,CAST(pr.services AS STRING)  as services
    ,CAST(pr.usage AS STRING)  as usage
    ,CAST(pr.service_providers_jpmc AS STRING)  as service_providers_jpmc
    ,CAST(pr.updated_date AS TIMESTAMP)  as updated_date
    ,CAST(pr.ovcp_view_id AS STRING)  as ovcp_view_id
    ,CAST(pr.jll_region AS STRING)  as jll_region
    ,CAST(pr.jll_business_group AS STRING)  as jll_business_group
    ,CAST(pr.client_industry AS STRING)  as client_industry
    ,CAST(pr.prod_ovcp_site_active AS STRING)  as prod_ovcp_site_active
    ,CAST(pr.transition_migration_data_manager AS STRING)  as transition_migration_data_manager
    ,CAST(pr.transition_migration AS STRING)  as transition_migration
FROM
    {var_client_raw_db}.s_master_clients_clients_propertyhub_current pr 
join 
    {var_client_base_db}.dv_master_clients cl 
on 
    cl.hk_h_clients = pr.hk_h_master_clients   
""".format(var_client_raw_db=var_client_raw_db, var_client_base_db=var_client_base_db, var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_OneView_Client_Property (old)
# spark.sql("""
# CREATE or replace VIEW {var_client_custom_db}.ssdv_vw_OneView_Client_Property AS
# select
#   pr.client_id AS oneviewclientid, 
#   pr.client_name AS txtclientname, 
#   pr.is_deleted AS isdeleted, 
#   mp.property_id AS oneviewclientpropertyid, 
#   pr.area_type AS area_type, 
#   pr.campus AS campus, 
#   pr.industry_sector AS industry_sector, 
#   pr.leased_owned AS leased_owned, 
#   pr.property_type AS property_type, 
#   pr.site_group AS site_group, 
#   pr.status AS status, 
#   pr.unit_of_measure AS unit_of_measure, 
#   pr.address_1 AS address_1, 
#   pr.address_2 AS address_2, 
#   pr.address_3 AS address_3, 
#   pr.address_4 AS address_4, 
#   pr.city AS city, 
#   pr.client_property_code AS client_property_code, 
#   pr.comments AS comments, 
#   pr.country AS country, 
#   pr.county AS county, 
#   cast(pr.import_latitude as double) AS import_latitude, 
#   cast(pr.import_longitude as double) AS import_longitude,  
#   pr.postal_code AS postal_code, 
#   pr.property_name AS property_name, 
#   pr.rsf AS rsf, 
#   pr.site_id AS site_id, 
#   pr.state_province AS state_province, 
#   pr.time_zone AS time_zone, 
#   pr.region_hierarchy_1 AS region_hierarchy_1, 
#   pr.region_hierarchy_2 AS region_hierarchy_2, 
#   pr.region_hierarchy_3 AS region_hierarchy_3, 
#   pr.region_hierarchy_4 AS region_hierarchy_4, 
#   pr.region_hierarchy_5 AS region_hierarchy_5, 
#   pr.region_hierarchy_6 AS region_hierarchy_6, 
#   pr.region_hierarchy_7 AS region_hierarchy_7, 
#   pr.region_hierarchy_8 AS region_hierarchy_8, 
#   pr.updated_by AS updated_by, 
#   pr.updated_date AS updated_date, 
#   pr.mdm_id AS mdm_id, 
#   pr.request_status AS request_status, 
#   pr.auto_approved_date AS autoapproveddate, 
#   pr.who_auto_approved AS who_auto_approved, 
#   pr.request_source AS request_source, 
#   pr.reviewed_post_auto AS reviewed_post_auto, 
#   pr.hub_process AS hub_process, 
#   pr.alternate_property_name AS alternate_property_name, 
#   pr.created_date AS created_date, 
#   pr.created_by AS created_by, 
#   pr.occupied_vacant AS occupied_vacant, 
#   pr.requestor_name AS requestor_name, 
#   pr.requestor_email AS requestor_email, 
#   pr.approved_rejected_note AS approved_rejected_note, 
#   pr.rejected_date AS rejected_date, 
#   pr.approved_date AS approved_date, 
#   pr.reporting_business_unit AS reporting_business_unit, 
#   pr.property_subtype AS property_subtype, 
#   pr.property_approval AS property_approval, 
#   pr.auto_approve_review_status AS auto_approve_review_status, 
#   pr.client_region_level_1 AS clientregion_level1, 
#   pr.ovcp_view_id AS ovcp_view_id, 
#   pr.service_start_date AS service_start_date, 
#   pr.service_end_date AS service_end_date, 
#   pr.country_iso_2_char AS countryiso2char, 
#   pr.country_iso_3_char AS countryiso3char, 
#   pr.country_iso_3_digit AS countryiso3digit, 
#   pr.request_property_name AS request_property_name, 
#   pr.request_address_1 AS request_address_1, 
#   pr.request_address_2 AS request_address_2, 
#   pr.request_city AS request_city, 
#   pr.request_state AS request_state, 
#   pr.request_postal_code AS request_postal_code, 
#   pr.request_country AS request_country, 
#   pr.mdm_tid AS mdm_tid, 
#   pr.gmt_offset AS gmt_offset, 
#   pr.mailability_score AS mailability_score 
  
# from {var_client_raw_db}.s_master_properties_properties_propertyhub_current pr 
# INNER JOIN {var_azara_raw_db}.h_master_properties mp on pr.hk_h_master_properties = mp.hk_h_master_properties 
# """.format(var_client_raw_db=var_client_raw_db, var_azara_raw_db=var_azara_raw_db, var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_OneView_Client_Property
spark.sql("""
CREATE OR REPLACE VIEW 
    {var_client_custom_db}.ssdv_vw_OneView_Client_Property AS
WITH 
    _properties_propertyhub AS (
    select 
        *
    from 
        (SELECT 
            *,  ROW_NUMBER() 
        OVER 
            ( PARTITION BY hk_h_master_properties  
              ORDER BY  
              updated_date desc,  dss_load_date desc
            ) row_num 
        FROM 
            {var_client_raw_db}.s_master_properties_properties_propertyhub_current ) A  
    WHERE row_num = 1
)
select
    CAST(pr.client_id AS BIGINT)  as oneviewclientid
    ,CAST(pr.client_name AS STRING)  as txtclientname
    ,CAST(pr.is_deleted AS STRING)  as isdeleted
    ,CAST(d1.property_id AS BIGINT)  as oneviewclientpropertyid
    ,CAST(pr.area_type AS STRING)  as area_type
    ,CAST(pr.campus AS STRING)  as campus
    ,CAST(pr.industry_sector AS STRING)  as industry_sector
    ,CAST(pr.leased_owned AS STRING)  as leased_owned
    ,CAST(pr.property_type AS STRING)  as property_type
    ,CAST(pr.site_group AS STRING)  as site_group
    ,CAST(pr.status AS STRING)  as status
    ,CAST(pr.unit_of_measure AS STRING)  as unit_of_measure
    ,CAST(pr.address_1 AS STRING)  as address_1
    ,CAST(pr.address_2 AS STRING)  as address_2
    ,CAST(pr.address_3 AS STRING)  as address_3
    ,CAST(pr.address_4 AS STRING)  as address_4
    ,CAST(pr.city AS STRING)  as city
    ,CAST(pr.client_property_code AS STRING)  as client_property_code
    ,CAST(pr.comments AS STRING)  as comments
    ,CAST(pr.country AS STRING)  as country
    ,CAST(pr.county AS STRING)  as county
    ,CAST(pr.import_latitude AS DOUBLE)  as import_latitude
    ,CAST(pr.import_longitude AS DOUBLE)  as import_longitude
    ,CAST(pr.postal_code AS STRING)  as postal_code
    ,CAST(pr.property_name AS STRING)  as property_name
    ,CAST(pr.rsf AS DOUBLE)  as rsf
    ,CAST(pr.site_id AS STRING)  as site_id
    ,CAST(pr.state_province AS STRING)  as state_province
    ,CAST(pr.time_zone AS STRING)  as time_zone
    ,CAST(pr.region_hierarchy_1 AS STRING)  as region_hierarchy_1
    ,CAST(pr.region_hierarchy_2 AS STRING)  as region_hierarchy_2
    ,CAST(pr.region_hierarchy_3 AS STRING)  as region_hierarchy_3
    ,CAST(pr.region_hierarchy_4 AS STRING)  as region_hierarchy_4
    ,CAST(pr.region_hierarchy_5 AS STRING)  as region_hierarchy_5
    ,CAST(pr.region_hierarchy_6 AS STRING)  as region_hierarchy_6
    ,CAST(pr.region_hierarchy_7 AS STRING)  as region_hierarchy_7
    ,CAST(pr.region_hierarchy_8 AS STRING)  as region_hierarchy_8
    ,CAST(pr.updated_by AS STRING)  as updated_by
    ,CAST(pr.updated_date AS TIMESTAMP)  as updated_date
    ,CAST(pr.mdm_id AS BIGINT)  as mdm_id
    ,CAST(pr.request_status AS STRING)  as request_status
    ,CAST(pr.auto_approved_date AS TIMESTAMP)  as autoapproveddate
    ,CAST(pr.who_auto_approved AS STRING)  as who_auto_approved
    ,CAST(pr.request_source AS STRING)  as request_source
    ,CAST(pr.reviewed_post_auto AS TIMESTAMP)  as reviewed_post_auto
    ,CAST(pr.hub_process AS STRING)  as hub_process
    ,CAST(pr.alternate_property_name AS STRING)  as alternate_property_name
    ,CAST(pr.created_date AS TIMESTAMP)  as created_date
    ,CAST(pr.created_by AS STRING)  as created_by
    ,CAST(pr.occupied_vacant AS STRING)  as occupied_vacant
    ,CAST(pr.requestor_name AS STRING)  as requestor_name
    ,CAST(pr.requestor_email AS STRING)  as requestor_email
    ,CAST(pr.approved_rejected_note AS STRING)  as approved_rejected_note
    ,CAST(pr.rejected_date AS TIMESTAMP)  as rejected_date
    ,CAST(pr.approved_date AS TIMESTAMP)  as approved_date
    ,CAST(pr.reporting_business_unit AS STRING)  as reporting_business_unit
    ,CAST(pr.property_subtype AS STRING)  as property_subtype
    ,CAST(pr.property_approval AS STRING)  as property_approval
    ,CAST(pr.auto_approve_review_status AS STRING)  as auto_approve_review_status
    ,CAST(pr.client_region_level_1 AS STRING)  as clientregion_level1
    ,CAST(pr.ovcp_view_id AS STRING)  as ovcp_view_id
    ,CAST(pr.service_start_date AS date)  as service_start_date
    ,CAST(pr.service_end_date AS date)  as service_end_date
    ,CAST(pr.country_iso_2_char AS STRING)  as countryiso2char
    ,CAST(pr.country_iso_3_char AS STRING)  as countryiso3char
    ,CAST(pr.country_iso_3_digit AS STRING)  as countryiso3digit
    ,CAST(pr.request_property_name AS STRING)  as request_property_name
    ,CAST(pr.request_address_1 AS STRING)  as request_address_1
    ,CAST(pr.request_address_2 AS STRING)  as request_address_2
    ,CAST(pr.request_city AS STRING)  as request_city
    ,CAST(pr.request_state AS STRING)  as request_state
    ,CAST(pr.request_postal_code AS STRING)  as request_postal_code
    ,CAST(pr.request_country AS STRING)  as request_country
    ,CAST(pr.mdm_tid AS STRING)  as mdm_tid
    ,CAST(pr.gmt_offset AS STRING)  as gmt_offset
    ,CAST(pr.mailability_score AS DOUBLE)  as mailability_score
from 
    _properties_propertyhub pr 
JOIN 
    {var_client_base_db}.dv_master_properties d1 
on 
    d1.hk_h_properties = pr.hk_h_master_properties 
""".format(var_client_raw_db=var_client_raw_db, var_client_base_db=var_client_base_db, var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_oneview_client_property_services (old)
# spark.sql("""
# CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_oneview_client_property_services AS
# SELECT 
#     CAST(OV_clientID as BIGINT) as oneviewclientid,
#     CAST(OVCPID as BIGINT) as oneviewclientpropertyid,
#     clientname as client_name ,
#     services as services ,
#     usage as usage ,
#     cast(date_format(nvl(StartDate,'1753-01-01'), 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp) AS start_date,
#     cast(date_format(nvl(EndDate ,'9999-12-31'), 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp) AS end_date
# from      
#      {var_client_custom_db}.{var_client_name}_custom_ovcpservices
#      where OV_clientID = {var_client_id}
# """.format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name, var_client_id=var_client_id))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_oneview_client_property_services
spark.sql("""
CREATE OR REPLACE VIEW 
    {var_client_custom_db}.ssdv_vw_OneView_Client_Property_Services AS
WITH properties_propertyhub AS 
    (select 
        hk_h_master_properties,
        client_id, 
        client_name,
        mdm_tid
    from 
        (SELECT 
            *, ROW_NUMBER() 
        OVER 
            ( PARTITION BY hk_h_master_properties 
              ORDER BY dss_load_date desc 
            ) row_num 
        FROM 
            {var_client_raw_db}.s_master_properties_properties_propertyhub_current ) A
    WHERE row_num=1
    ) 
SELECT 
    CAST(pr.client_id AS BIGINT)    as oneviewclientid 
    ,CAST(D1.property_id AS BIGINT)  as oneviewclientpropertyid 
    ,CAST(pr.client_name AS STRING)  as client_name
    ,CAST(srv.services AS STRING)    as services 
    ,CAST(srv.usage AS STRING)       as usage 
    ,cast(date_format(nvl(srv.StartDate,'1753-01-01'), 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp) AS start_date
    ,cast(date_format(nvl(srv.EndDate ,'9999-12-31'), 'yyyy-MM-dd HH:mm:ss.SSS')  as timestamp) AS end_date
from
    properties_propertyhub pr 
JOIN 
    {var_client_base_db}.dv_master_properties D1  
ON  
    D1.hk_h_properties = PR.hk_h_master_properties
JOIN 
    {var_client_custom_db}.{var_client_name}_custom_ovcpservices srv 
ON 
    srv.OV_clientID = pr.client_id 
    and srv.OVCPID =D1.property_id
""".format(var_client_raw_db=var_client_raw_db, var_client_base_db=var_client_base_db, var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_oneview_client_property_custom
spark.sql("""
Create OR Replace VIEW {var_client_custom_db}.ssdv_vw_oneview_client_property_custom AS 
select 
    CAST(oneviewclientid as BIGINT) as oneviewclientid, 
    isDeleted as isdeleted, 
    CAST(oneviewclientpropertyid as BIGINT) as oneviewclientpropertyid, 
    CAST(last_update_ts as TIMESTAMP) as last_update_ts, 
    clientname as clientname, 
    propertysummary_propertyname as property_summary_property_name ,
    propertysummary_alternatepropertyname as property_summary_alternate_property_name ,
    propertysummary_address1 as property_summary_address_1 ,
    propertysummary_address2 as property_summary_address_2 ,
    propertysummary_city as property_summary_city ,
    propertysummary_county as property_summary_county ,
    propertysummary_state_province as property_summary_state_province ,
    propertysummary_postalcode as property_summary_postal_code ,
    propertysummary_country as property_summary_country ,
    propertysummary_countryiso2char as property_summary_countryiso2char ,
    propertysummary_timezone as property_summary_time_zone ,
    propertysummary_gmtoffset as property_summary_gmt_offset ,
    propertysummary_latitude as property_summary_latitude ,
    propertysummary_longitude as property_summary_longitude ,
    propertysummary_geoupdatesoff as property_summary_geo_updates_off ,
    propertyvalidation_additionalinfo_primarykey as property_validation_additional_info_primary_key ,
    propertyvalidation_additionalinfo_pdmid as property_validation_additional_info_pdm_id ,
    propertyvalidation_additionalinfo_serviceprovider as property_validation_additional_info_service_provider ,
    propertyvalidation_additionalinfo_countryiso3char as property_validation_additional_info_countryiso3char ,
    propertyvalidation_additionalinfo_countryiso3digit as property_validation_additional_info_countryiso3digit ,
    propertyrequestdetails_requestsource as property_request_details_request_source ,
    propertyrequestdetails_requeststatus as property_request_details_request_status ,
    propertyrequestdetails_requestorname as property_request_details_requestor_name ,
    propertyrequestdetails_approveddate as property_request_details_approved_date ,
    propertyrequestdetails_requestoremail as property_request_details_requestor_email ,
    propertyrequestdetails_rejecteddate as property_request_details_rejected_date ,
    propertyrequestdetails_approved_rejectednote as property_request_details_approved_rejected_note ,
    propertydetails_clientpropertycode as property_details_client_property_code ,
    propertydetails_wpssuperregion as property_details_wps_super_region ,
    propertydetails_internalzip as property_details_internal_zip ,
    propertydetails_propertytype as property_details_property_type ,
    propertydetails_regionhierarchypath as property_details_region_hierarchy_path ,
    propertydetails_propertysubtype as property_details_property_subtype ,
    propertydetails_nexsite as property_details_nex_site ,
    propertydetails_unitofmeasure as property_details_unit_of_measure ,
    propertydetails_bankingregion as property_details_banking_region ,
    propertydetails_areatype as property_details_area_type ,
    propertydetails_propertystatus as property_details_property_status ,
    propertydetails_leased_owned as property_details_leased_owned ,
    propertydetails_occupied_vacant as property_details_occupied_vacant ,
    propertydetails_propertysize as property_details_property_size ,
    propertydetails_darksite as property_details_dark_site ,
    propertydetails_ovlabuilding as property_details_ovla_building ,
    propertydetails_did as property_details_did ,
    propertydetails_ovlaland as property_details_ovla_land ,
    propertydetails_financialresponsibility as property_details_financial_responsibility ,
    propertydetails_jllmanagedland as property_details_jll_managed_land ,
    propertydetails_network_wps as property_details_network_wps ,
    propertydetails_areacommentary as property_details_area_commentary ,
    propertydetails_multi_usesite as property_details_multi_use_site ,
    propertydetails_action as property_details_action ,
    PropertyDetails_SiteCriticality as property_details_site_criticality,
    cast(propertydetails_altaccountsqft as float) as property_details_alt_account_sqft ,   
    PropertyDetails_E1BU as property_details_e1_bu,
    propertydetails_bunumber as property_details_bu_number ,
    `PropertyDetails_ProposedSite?` as property_details_proposed_site ,
    propertydetails_sitegroup as property_details_site_group ,
    propertydetails_campus as property_details_campus ,
    propertydetails_siteid as property_details_site_id ,
    propertydetails_capitaloneclientcode as property_details_capital_one_client_code ,
    propertydetails_reportingbusinessunit as property_details_reporting_business_unit ,
    propertydetails_format as property_details_format ,
    propertydetails_level_i as property_details_level_i ,
    propertydetails_level_ii as property_details_level_ii ,
    propertydetails_recordtype as property_details_record_type ,
    propertydetails_projectjsa as property_details_project_jsa ,
    propertydetails_assessmentarea as property_details_assessment_area ,
    propertydetails_dropdown_10 as property_details_dropdown_100 ,
    propertydetails_manhattanid_wpsonl as property_details_manhattan_id_wps_only ,
    propertydetails_unusedpicklist as property_details_unused_picklist ,
    propertydetails_geocode_tractincomelevel as property_details_geocode_tract_income_level ,
    property_hiddenfieldforlinking_managedifm as property_hidden_field_for_linking_managed_ifm ,
    property_hiddenfieldforlinking_oneviewovcp_idforlinking as property_hidden_field_for_linking_oneview_ovcp_id_for_linking ,
    property_recordmetrics_createddate as property_record_metrics_created_date ,
    property_recordmetrics_createdby as property_record_metrics_created_by ,
    property_recordmetrics_updatedby as property_record_metrics_updated_by ,
    property_recordmetrics_updateddate as property_record_metrics_updated_date ,
    property_recordmetrics_mdm_property_id as property_record_metrics_mdm_property_id ,
    property_hiddensection_locationoccupiedby as property_hidden_section_location_occupied_by ,
    property_hiddensection_industrysector as property_hidden_section_industry_sector ,
    property_hiddensection_address3 as property_hidden_section_address_3 ,
    property_hiddensection_address4 as property_hidden_section_address_4 ,
    property_hiddensection_propertyapproval as property_hidden_section_property_approval ,
    property_hiddensection_autoapprovereviewstatus as property_hidden_section_auto_approve_review_status ,
    property_hiddensection_requestid as property_hidden_section_request_id ,
    property_hiddensection_geocodesavedby as property_hidden_section_geocode_saved_by ,
    property_hiddensection_geocodesaveddate as property_hidden_section_geocode_saved_date ,
    property_hiddensection_ovlaid_delete as property_hidden_section_ovla_id_delete ,
    property_hiddensection_datacentertier as property_hidden_section_data_center_tier ,
    property_hiddensection_clientbusinessunitpath as property_hidden_section_client_business_unit_path ,
    facilitymanagement_fmdetails_fmprimary as facility_management_fm_details_fm_primary ,
    facilitymanagement_fmdetails_sfmname as facility_management_fm_details_sfm_name ,
    facilitymanagement_fmdetails_mesescalation as facility_management_fm_details_mes_escalation ,
    facilitymanagement_fmdetails_jllfmresponsibility as facility_management_fm_details_jll_fm_responsibility ,
    facilitymanagement_fmdetails_alternate1 as facility_management_fm_details_alternate_1 ,
  --  facilitymanagement_fmdetails_branchmanager as facility_management_fm_details_branch_manager ,
    facilitymanagement_fmdetails_fmnumber as facility_management_fm_details_fm_number ,
    facilitymanagement_fmdetails_alternate2 as facility_management_fm_details_alternate_2 ,
    facilitymanagement_fmdetails_sfmnumber as facility_management_fm_details_sfm_number ,
    facilitymanagement_fmdetails_mesnumber as facility_management_fm_details_mes_number ,
    facilitymanagement_fmdetails_regionalengineeringmanager as facility_management_fm_details_regional_engineering_manager ,
    facilitymanagement_fmdetails_alternate3 as facility_management_fm_details_alternate_3 ,
    facilitymanagement_fmdetails_branchmanagernumber as facility_management_fm_details_branch_manager_number ,
    facilitymanagement_fmdetails_alternate4 as facility_management_fm_details_alternate_4 ,
    facilitymanagement_hiddenfields_delete_alternate4 as facility_management_hidden_fields_delete_alternate_4 ,
    requestinformation_requestpropertyname as request_information_request_property_name ,
    requestinformation_requestaddress1 as request_information_request_address_1 ,
    requestinformation_requestaddress2 as request_information_request_address_2 ,
    requestinformation_requestcity as request_information_request_city ,
    requestinformation_requeststate as request_information_request_state ,
    requestinformation_requestpostalcode as request_information_request_postal_code ,
    requestinformation_requestcountry as request_information_request_country ,
    additionalpropertydetails_general_propertynotes as additional_property_details_general_property_notes ,
    cast(additionalpropertydetails_general_activationdate as timestamp) as additional_property_details_general_activation_date ,
    cast(additionalpropertydetails_general_inactivateddate as timestamp) as additional_property_details_general_inactivated_date ,
    cast(additionalpropertydetails_general_disposaldate as timestamp) as additional_property_details_general_disposal_date ,
    txtlastcomment as txtlastcomment
from 
    {var_client_custom_db}.{var_client_name}_custom_property_3626
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))