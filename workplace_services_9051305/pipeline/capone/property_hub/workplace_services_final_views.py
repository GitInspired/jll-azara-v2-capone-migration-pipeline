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

# DBTITLE 1,ssdv_vw_OneView_Client (old)
# spark.sql("""
# CREATE or replace VIEW {var_client_custom_db}.ssdv_vw_OneView_Client AS
 
#  select  
#     9051305 AS OneViewClientID,
#     client_name,
#     is_deleted    as isdeleted,
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
#   from {var_client_raw_db}.s_master_clients_clients_propertyhub_current
#   """.format(var_client_raw_db=var_client_raw_db,  var_client_custom_db=var_client_custom_db))

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

# DBTITLE 1,ssdv_vw_oneview_client_property_custom
spark.sql("""
Create OR Replace VIEW {var_client_custom_db}.ssdv_vw_oneview_client_property_custom AS 
select 
    CAST(OneViewClientID as BIGINT) as oneviewclientid,
    isDeleted as isdeleted,
    CAST(OneViewClientPropertyID as BIGINT) as oneviewclientpropertyid,
    CAST(Last_Update_TS as TIMESTAMP) as last_update_ts,
    ClientName as clientname,
    Details_PropertyDetails_PropertySubType as details_property_details_property_subtype,
    Details_PropertyDetails_PropertyStatus as details_property_details_property_status,
    Details_PropertyDetails_ClientRegion as details_property_details_client_region,
    Details_PropertyDetails_PropertyType as details_property_details_property_type,
    Details_PropertyDetails_SiteID as details_property_details_site_id,
    Details_PropertySummary_PropertyName as details_property_summary_property_name,
    Details_PropertySummary_AlternatePropertyName as details_property_summary_alternate_property_name,
    Details_PropertySummary_Address1 as details_property_summary_address_1,
    Details_PropertySummary_Address2 as details_property_summary_address_2,
    Details_PropertySummary_Address3 as details_property_summary_address_3,
    Details_PropertySummary_Address4 as details_property_summary_address_4,
    Details_PropertySummary_City as details_property_summary_city,
    Details_PropertySummary_County as details_property_summary_county,
    Details_PropertySummary_State_Province as details_property_summary_state_province,
    Details_PropertySummary_CountryIso2Char as details_property_summary_countryiso2char,
    Details_PropertySummary_Country as details_property_summary_country,
    Details_PropertySummary_PostalCode as details_property_summary_postal_code,
    Details_PropertySummary_TimeZone as details_property_summary_time_zone,
    Details_PropertySummary_GMTOffset as details_property_summary_gmt_offset,
    Details_PropertySummary_Latitude as details_property_summary_latitude,
    Details_PropertySummary_Longitude as details_property_summary_longitude,
    Details_PropertySummary_GeoUpdatesOFF as details_property_summary_geo_updates_off,
    Details_HiddenSection_IndustrySector as details_hidden_section_industry_sector,
    Details_HiddenSection_PropertyApproval as details_hidden_section_property_approval,
    Details_HiddenSection_AutoApproveReviewStatus as details_hidden_section_auto_approve_review_status,
    Details_HiddenSection_RequestID as details_hidden_section_request_id,
    Details_HiddenSection_GeocodeSavedBy as details_hidden_section_geocode_saved_by,
    cast(Details_HiddenSection_GeocodeSavedDate as timestamp) as details_hidden_section_geocode_saved_date,
    Details_HiddenSection_ManagedIFM as details_hidden_section_managed_ifm,
    Details_HiddenSection_ClientBusinessUnitPath as details_hidden_section_client_business_unit_path,
    Details_PropertyValidation_AdditionalInfo_PrimaryKey as details_property_validation_additional_info_primary_key,
    Details_PropertyValidation_AdditionalInfo_ServiceProvider as details_property_validation_additional_info_service_provider,
    Details_PropertyValidation_AdditionalInfo_CountryIso3Char as details_property_validation_additional_info_countryiso3char,
    Details_PropertyValidation_AdditionalInfo_CountryIso3Digit as details_property_validation_additional_info_countryiso3digit,
    CAST(Details_PropertyDetails_MainOVCPID as BIGINT) as details_property_details_main_ovcp_id,
    Details_PropertyDetails_WPSSuperRegion as details_property_details_wps_super_region,
    Details_PropertyDetails_ClientPropertyCode as details_property_details_client_property_code,
    Details_PropertyDetails_Campus as details_property_details_campus,
    Details_PropertyDetails_PropertySize as details_property_details_property_size,
    Details_PropertyDetails_SiteGroup as details_property_details_site_group,
    Details_PropertyDetails_UnitofMeasure as details_property_details_unit_of_measure,
    Details_PropertyDetails_AreaType as details_property_details_area_type,
    Details_PropertyDetails_ReportingBusinessUnit as details_property_details_reporting_business_unit,
    Details_PropertyDetails_Leased_Owned as details_property_details_leased_owned,
    Details_PropertyDetails_JLLStaffingFrequency as details_property_details_jll_staffing_frequency,
    Details_PropertyDetails_Occupied_Vacant as details_property_details_occupied_vacant,
    Details_PropertyDetails_DataCenterTier as details_property_details_data_center_tier,
    Details_PropertyDetails_RegionHierarchyPath as details_property_details_region_hierarchy_path,
    Details_PropertyDetails_SiteCriticality as details_property_details_site_criticality,
    Details_PropertyDetails_DarkSite as details_property_details_dark_site,
    Details_RecordMetrics_CreatedDate as details_record_metrics_created_date,
    Details_RecordMetrics_CreatedBy as details_record_metrics_created_by,
    Details_RecordMetrics_UpdatedDate as details_record_metrics_updated_date,
    Details_RecordMetrics_UpdatedBy as details_record_metrics_updated_by,
    Details_RecordMetrics_MDM_Property_ID as details_record_metrics_mdm_property_id,
    Details_PropertyRequestDetails_RequestSource as details_property_request_details_request_source,
    Details_PropertyRequestDetails_RequestorName as details_property_request_details_requestor_name,
    Details_PropertyRequestDetails_RequestorEmail as details_property_request_details_requestor_email,
    Details_PropertyRequestDetails_Approved_RejectedNote as details_property_request_details_approved_rejected_note,
    Details_PropertyRequestDetails_RequestStatus as details_property_request_details_request_status,
    cast(Details_PropertyRequestDetails_ApprovedDate as timestamp)  as details_property_request_details_approved_date,
    Details_PropertyRequestDetails_RejectedDate as details_property_request_details_rejected_date,
    RequestInformation_RequestPropertyName as request_information_request_property_name,
    RequestInformation_RequestAddress1 as request_information_request_address_1,
    RequestInformation_RequestAddress2 as request_information_request_address_2,
    RequestInformation_RequestCity as request_information_request_city,
    RequestInformation_RequestState as request_information_request_state,
    RequestInformation_RequestPostalCode as request_information_request_postal_code,
    RequestInformation_RequestCountry as request_information_request_country,
    FacilityManagement_ChiefEngineer_ChiefEngineer_Name as facility_management_chief_engineer_chief_engineer_name,
    FacilityManagement_ChiefEngineer_ChiefEngineer_Email as facility_management_chief_engineer_chief_engineer_email,
    FacilityManagement_ChiefEngineer_ChiefEngineer_Phone as facility_management_chief_engineer_chief_engineer_phone,
    FacilityManagement_CapitalOnePortfolioManager_CapitalOnePortfolioManagerName as facility_management_capital_one_portfolio_manager_capital_one_portfolio_manager_name,
    FacilityManagement_CapitalOnePortfolioManager_CapitalOnePortfolioManagerPhone as facility_management_capital_one_portfolio_manager_capital_one_portfolio_manager_phone,
    FacilityManagement_AssistantChiefEngineer_AssistantChiefEngineer_Name as facility_management_assistant_chief_engineer_assistant_chief_engineer_name,
    FacilityManagement_AssistantChiefEngineer_AssistantChiefEngineer_Email as facility_management_assistant_chief_engineer_assistant_chief_engineer_email,
    FacilityManagement_AssistantChiefEngineer_AssistantChiefEngineer_Phone as facility_management_assistant_chief_engineer_assistant_chief_engineer_phone,
    FacilityManagement_SeniorFacilityManager_SeniorFacilityManager_Name as facility_management_senior_facility_manager_senior_facility_manager_name,
    FacilityManagement_SeniorFacilityManager_SeniorFacilityManager_Phone as facility_management_senior_facility_manager_senior_facility_manager_phone,
    FacilityManagement_SeniorFacilityManager_SeniorFacilityManager_Email as facility_management_senior_facility_manager_senior_facility_manager_email,
    FacilityManagement_FacilityManager_FacilityManager_Name as facility_management_facility_manager_facility_manager_name,
    FacilityManagement_FacilityManager_FacilityManager_Email as facility_management_facility_manager_facility_manager_email,
    FacilityManagement_FacilityManager_FacilityManager_Phone as facility_management_facility_manager_facility_manager_phone,
    FacilityManagement_FacilityManager_FMOnsiteFrequency as facility_management_facility_manager_fm_onsite_frequency,
    FacilityManagement_AssistantFM_AssistantFM_Name as facility_management_assistant_fm_assistant_fm_name,
    FacilityManagement_AssistantFM_AssistantFM_Email as facility_management_assistant_fm_assistant_fm_email,
    FacilityManagement_AssistantFM_AssistantFM_Phone as facility_management_assistant_fm_assistant_fm_phone,
    FacilityManagement_RegionalFM_RegionalFM_Name as facility_management_regional_fm_regional_fm_name,
    FacilityManagement_RegionalFM_RegionalFM_Email as facility_management_regional_fm_regional_fm_email,
    FacilityManagement_RegionalFM_RegionalFM_Phone as facility_management_regional_fm_regional_fm_phone,
    FacilityManagement_RegionalEngineeringManager_RegionalEngineeringManager_Name as facility_management_regional_engineering_manager_regional_engineering_manager_name,
    FacilityManagement_RegionalEngineeringManager_RegionalEngineeringManager_Email as facility_management_regional_engineering_manager_regional_engineering_manager_email,
    FacilityManagement_RegionalEngineeringManager_RegionalEngineeringManager_Phone as facility_management_regional_engineering_manager_regional_engineering_manager_phone,
    FacilityManagement_RegionalLeads_RegionalLead as facility_management_regional_leads_regional_lead,
    FacilityManagement_RegionalLeads_RegionalLead_Phone as facility_management_regional_leads_regional_lead_phone,
    `FacilityManagement_InfrastructureOperationsManager_PrimaryInfrastructureOperationsManager–Name` as facility_management_infrastructure_operations_manager_primary_infrastructure_operations_manager_name,
    `FacilityManagement_InfrastructureOperationsManager_PrimaryInfrastructureOperationsManager–Phone` as facility_management_infrastructure_operations_manager_primary_infrastructure_operations_manager_phone,
    `FacilityManagement_InfrastructureOperationsManager_SecondaryInfrastructureOperationsManager–Name` as facility_management_infrastructure_operations_manager_secondary_infrastructure_operations_manager_name,
    `FacilityManagement_InfrastructureOperationsManager_SecondaryInfrastructureOperationsManager–Phone` as facility_management_infrastructure_operations_manager_secondary_infrastructure_operations_manager_phone,
    txtLastComment as txtlastcomment
from 
    {var_client_custom_db}.{var_client_name}_custom_ssdv_workplace_services_property_4684
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,clientssdv_vw_clientssdv_oneview_client_property_custom
spark.sql("""
Create OR Replace VIEW {var_client_custom_db}.clientssdv_vw_clientssdv_oneview_client_property_custom AS 
select
    nclientid as nclientid,
    isDeleted as isdeleted,
    nLeaseID as nleaseid,
    cast(Last_Update_TS as timestamp) as last_update_ts,
    ClientName as clientname,
    Details_PropertyDetails_PropertySubType as details_property_details_property_subtype,
    Details_PropertyDetails_PropertyStatus as details_property_details_property_status,
    Details_PropertyDetails_ClientRegion as details_property_details_client_region,
    Details_PropertyDetails_PropertyType as details_property_details_property_type,
    Details_PropertyDetails_SiteID as details_property_details_site_id,
    Details_PropertySummary_PropertyName as details_property_summary_property_name,
    Details_PropertySummary_AlternatePropertyName as details_property_summary_alternate_property_name,
    Details_PropertySummary_Address1 as details_property_summary_address_1,
    Details_PropertySummary_Address2 as details_property_summary_address_2,
    Details_PropertySummary_Address3 as details_property_summary_address_3,
    cast(Details_PropertySummary_Address4 as varchar(100)) as details_property_summary_address_4,
    Details_PropertySummary_City as details_property_summary_city,
    Details_PropertySummary_County as details_property_summary_county,
    Details_PropertySummary_State_Province as details_property_summary_state_province,
    Details_PropertySummary_Country as details_property_summary_country,
    Details_PropertySummary_CountryIso2Char as details_property_summary_countryiso2char,
    Details_PropertySummary_PostalCode as details_property_summary_postal_code,
    Details_PropertySummary_TimeZone as details_property_summary_time_zone,
    Details_PropertySummary_GMTOffset as details_property_summary_gmt_offset,
    Details_PropertySummary_Latitude as details_property_summary_latitude,
    Details_PropertySummary_Longitude as details_property_summary_longitude,
    Details_PropertySummary_GeoUpdatesOFF as details_property_summary_geo_updates_off,
    Details_HiddenSection_IndustrySector as details_hidden_section_industry_sector,
    Details_PropertyDetails_AreaType as details_hidden_section_area_type,
    Details_HiddenSection_PropertyApproval as details_hidden_section_property_approval,
    Details_HiddenSection_AutoApproveReviewStatus as details_hidden_section_auto_approve_review_status,
    Details_HiddenSection_RequestID as details_hidden_section_request_id,
    Details_HiddenSection_GeocodeSavedBy as details_hidden_section_geocode_saved_by,
    Details_HiddenSection_GeocodeSavedDate as details_hidden_section_geocode_saved_date,
    Details_PropertyValidation_AdditionalInfo_PrimaryKey as details_property_validation_additional_info_primary_key,
    Details_PropertyValidation_AdditionalInfo_ServiceProvider as details_property_validation_additional_info_service_provider,
    Details_PropertyValidation_AdditionalInfo_CountryIso3Char as details_property_validation_additional_info_countryiso3char,
    Details_PropertyValidation_AdditionalInfo_CountryIso3Digit as details_property_validation_additional_info_countryiso3digit,
    Details_PropertyDetails_RegionHierarchyPath as details_property_validation_additional_info_region_hierarchy_path,
    CAST(Details_PropertyDetails_MainOVCPID as BIGINT) as details_property_details_main_ovcp_id,
    Details_PropertyDetails_WPSSuperRegion as details_property_details_wps_super_region,
    Details_PropertyDetails_ClientPropertyCode as details_property_details_client_property_code,
    Details_PropertyDetails_Campus as details_property_details_campus,
    cast(Details_PropertyDetails_PropertySize as float)as details_property_details_property_size,
    Details_PropertyDetails_SiteGroup as details_property_details_site_group,
    Details_PropertyDetails_UnitofMeasure as details_property_details_unit_of_measure,
    Details_PropertyDetails_Leased_Owned as details_property_details_leased_owned,
    Details_PropertyDetails_ReportingBusinessUnit as details_property_details_reporting_business_unit,
    Details_PropertyDetails_Occupied_Vacant as details_property_details_occupied_vacant,
    Details_PropertyDetails_JLLStaffingFrequency as details_property_details_jll_staffing_frequency,
    cast(Details_RecordMetrics_CreatedDate as timestamp) as details_record_metrics_created_date,
    Details_RecordMetrics_CreatedBy as details_record_metrics_created_by,
    cast(Details_RecordMetrics_UpdatedDate  as timestamp) as details_record_metrics_updated_date,
    Details_RecordMetrics_UpdatedBy as details_record_metrics_updated_by,
    Details_RecordMetrics_MDM_Property_ID as details_record_metrics_mdm_property_id,
    Details_PropertyRequestDetails_RequestSource  as details_property_request_details_request_source,
    Details_PropertyRequestDetails_RequestorName as details_property_request_details_requestor_name,
    Details_PropertyRequestDetails_RequestorEmail as details_property_request_details_requestor_email,
    Details_PropertyRequestDetails_Approved_RejectedNote as details_property_request_details_approved_rejected_note,
    Details_PropertyRequestDetails_RequestStatus as details_property_request_details_request_status,
    Details_PropertyRequestDetails_ApprovedDate as details_property_request_details_approved_date,
    Details_PropertyRequestDetails_RejectedDate as details_property_request_details_rejected_date,
    RequestInformation_RequestPropertyName as request_information_request_property_name,
    RequestInformation_RequestAddress1 as request_information_request_address_1,
    RequestInformation_RequestAddress2 as request_information_request_address_2,
    RequestInformation_RequestCity as request_information_request_city,
    RequestInformation_RequestState as request_information_request_state,
    RequestInformation_RequestPostalCode as request_information_request_postal_code,
    RequestInformation_RequestCountry as request_information_request_country,
    FacilityManagement_ChiefEngineer_ChiefEngineer_Name as facility_management_chief_engineer_chief_engineer_name,
    FacilityManagement_ChiefEngineer_ChiefEngineer_Email as facility_management_chief_engineer_chief_engineer_email,
    FacilityManagement_ChiefEngineer_ChiefEngineer_Phone as facility_management_chief_engineer_chief_engineer_phone,
    FacilityManagement_CapitalOnePortfolioManager_CapitalOnePortfolioManagerName as facility_management_capital_one_portfolio_manager_capital_one_portfolio_manager_name,
    FacilityManagement_CapitalOnePortfolioManager_CapitalOnePortfolioManagerPhone as facility_management_capital_one_portfolio_manager_capital_one_portfolio_manager_phone,
    FacilityManagement_AssistantChiefEngineer_AssistantChiefEngineer_Name as facility_management_assistant_chief_engineer_assistant_chief_engineer_name,
    FacilityManagement_AssistantChiefEngineer_AssistantChiefEngineer_Email as facility_management_assistant_chief_engineer_assistant_chief_engineer_email,
    FacilityManagement_AssistantChiefEngineer_AssistantChiefEngineer_Phone as facility_management_assistant_chief_engineer_assistant_chief_engineer_phone,
    FacilityManagement_SeniorFacilityManager_SeniorFacilityManager_Name as facility_management_senior_facility_manager_senior_facility_manager_name,
    FacilityManagement_SeniorFacilityManager_SeniorFacilityManager_Phone as facility_management_senior_facility_manager_senior_facility_manager_phone,
    FacilityManagement_SeniorFacilityManager_SeniorFacilityManager_Email as facility_management_senior_facility_manager_senior_facility_manager_email,
    FacilityManagement_FacilityManager_FacilityManager_Name as facility_management_facility_manager_facility_manager_name,
    FacilityManagement_FacilityManager_FacilityManager_Email as facility_management_facility_manager_facility_manager_email,
    FacilityManagement_FacilityManager_FacilityManager_Phone as facility_management_facility_manager_facility_manager_phone,
    FacilityManagement_FacilityManager_FMOnsiteFrequency as facility_management_facility_manager_fm_onsite_frequency,
    FacilityManagement_AssistantFM_AssistantFM_Name as facility_management_assistant_fm_assistant_fm_name,
    FacilityManagement_AssistantFM_AssistantFM_Email as facility_management_assistant_fm_assistant_fm_email,
    FacilityManagement_AssistantFM_AssistantFM_Phone as facility_management_assistant_fm_assistant_fm_phone,
    FacilityManagement_RegionalFM_RegionalFM_Name as facility_management_regional_fm_regional_fm_name,
    FacilityManagement_RegionalFM_RegionalFM_Email as facility_management_regional_fm_regional_fm_email,
    FacilityManagement_RegionalFM_RegionalFM_Phone as facility_management_regional_fm_regional_fm_phone,
    FacilityManagement_RegionalEngineeringManager_RegionalEngineeringManager_Name as facility_management_regional_engineering_manager_regional_engineering_manager_name,
    FacilityManagement_RegionalEngineeringManager_RegionalEngineeringManager_Email as facility_management_regional_engineering_manager_regional_engineering_manager_email,
    FacilityManagement_RegionalEngineeringManager_RegionalEngineeringManager_Phone as facility_management_regional_engineering_manager_regional_engineering_manager_phone,
    FacilityManagement_RegionalLeads_RegionalLead as facility_management_regional_leads_regional_lead,
    FacilityManagement_RegionalLeads_RegionalLead_Phone as facility_management_regional_leads_regional_lead_phone,
    `FacilityManagement_InfrastructureOperationsManager_PrimaryInfrastructureOperationsManager–Name` as facility_management_infrastructure_operations_manager_primary_infrastructure_operations_manager_name,
    `FacilityManagement_InfrastructureOperationsManager_PrimaryInfrastructureOperationsManager–Phone` as facility_management_infrastructure_operations_manager_primary_infrastructure_operations_manager_phone,
    `FacilityManagement_InfrastructureOperationsManager_SecondaryInfrastructureOperationsManager–Name` as facility_management_infrastructure_operations_manager_secondary_infrastructure_operations_manager_name,
    `FacilityManagement_InfrastructureOperationsManager_SecondaryInfrastructureOperationsManager–Phone` as facility_management_infrastructure_operations_manager_secondary_infrastructure_operations_manager_phone,
    txtLastComment as txtlastcomment

from 
    {var_client_custom_db}.{var_client_name}_custom_clientssdv_workplace_services_property_4684
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,Grant Access
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.ssdv_vw_OneView_Client TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");           
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.ssdv_vw_OneView_Client_Property TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");           
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.ssdv_vw_oneview_client_property_custom TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");           
spark.sql("""GRANT SELECT ON VIEW jll_azara_catalog.jll_azara_0009051305_capitalone_custom.clientssdv_vw_clientssdv_oneview_client_property_custom TO `jll-azara-custom-CapOneJLLAcctTeam-preprod`""");           
         
