# Databricks notebook source
# MAGIC %md
# MAGIC ###### Notebook for Corrigo Raw Views

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

# MAGIC %md

# COMMAND ----------

# DBTITLE 1,raw_work_orders_workorders_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_orders_workorders_corrigo
                AS 
                SELECT 
                    hwo.tenant_id, hwo.work_order_number, hwo.work_order_id, hwo.source_id, hwo.dss_record_source as h_dss_record_source, hwo.dss_load_date as h_dss_load_date, hwo.dss_create_time as h_dss_create_time,
                    cwo.*  
                FROM {var_client_raw_db}.s_work_orders_workorders_corrigo_current  cwo
                LEFT JOIN {var_azara_raw_db}.h_work_orders                         hwo 
                  ON hwo.hk_h_work_orders = cwo.hk_h_work_orders
                WHERE trim(hwo.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_service_providers_serviceproviders_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_service_providers_serviceproviders_corrigo
                AS 
                SELECT 
                    hsp.tenant_id, hsp.service_provider_id, hsp.source_id, hsp.dss_record_source as h_dss_record_source, hsp.dss_load_date as h_dss_load_date, hsp.dss_create_time as h_dss_create_time,
                    lsp.hk_l_serviceproviders_rates, lsp.hk_h_rates_tenant_id, lsp.hk_h_rates_service_provider_id, lsp.hk_h_rates_rate_card_id, lsp.dss_record_source as l_sp_dss_record_source, lsp.dss_load_date as l_sp_dss_load_date, 
                    lsp.dss_create_time as l_sp_dss_create_time,
                    lspi.hk_l_serviceproviders_serviceproviderinsurance, lspi.hk_h_service_provider_insurance, lspi.dss_record_source as lspi_dss_record_source, lspi.dss_load_date as lspi_dss_load_date, lspi.dss_create_time as lspi_dss_create_time,
                    lws.hk_l_workorders_serviceproviders, lws.hk_h_work_orders,lws.dss_load_date as lws_dss_load_date
                    ,lws.dss_create_time as lws_dss_create_time,ssp.*  
                FROM {var_client_raw_db}.s_service_providers_serviceproviders_corrigo_current  ssp
                LEFT JOIN {var_azara_raw_db}.h_service_providers                                    hsp
                       ON hsp.hk_h_service_providers = ssp.hk_h_service_providers
                LEFT JOIN {var_azara_raw_db}.l_workorders_serviceproviders                          lws
                       ON lws.hk_h_service_providers = ssp.hk_h_service_providers
                LEFT JOIN {var_azara_raw_db}.l_serviceproviders_rates                               lsp
                       ON ssp.hk_h_service_providers = lsp.hk_h_service_providers
                LEFT JOIN {var_azara_raw_db}.l_serviceproviders_serviceproviderinsurance            lspi
                       ON ssp.hk_h_service_providers = lspi.hk_h_service_providers       
                WHERE trim(hsp.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_areas_areas_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_areas_areas_corrigo
                AS 
                SELECT 
                    harea.tenant_id, harea.area_id, harea.source_id, harea.dss_record_source as h_dss_record_source, harea.dss_load_date as h_dss_load_date,    harea.dss_create_time as h_dss_create_time, 
                    larea.hk_l_workorders_areas, larea.hk_h_work_orders, larea.dss_record_source as l_dss_record_source, larea.dss_load_date as l_dss_load_date, larea.dss_create_time as l_dss_create_time,
                    lmparea.hk_l_masterproperties_areas as hk_l_masterproperties_areas, lmparea.hk_h_master_properties, lmparea.dss_record_source as l_mpa_dss_record_source, lmparea.dss_load_date as l_mpa_dss_load_date, 
                    lmparea.dss_create_time as l_mpa_dss_create_time, 
                    aacc.*  
                FROM {var_client_raw_db}.s_areas_areas_corrigo_current      aacc
                LEFT JOIN {var_azara_raw_db}.h_areas                             harea
				  ON harea.hk_h_areas = aacc.hk_h_areas
                LEFT JOIN {var_azara_raw_db}.l_workorders_areas                  larea
                  ON aacc.hk_h_areas = larea.hk_h_areas
                LEFT JOIN {var_azara_raw_db}.l_masterproperties_areas            lmparea
                  ON aacc.hk_h_areas = lmparea.hk_h_areas
                WHERE trim(harea.tenant_id) = "{var_tenant_id}" or trim(harea.tenant_id) = "{var_client_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id, var_client_id=var_client_id))

# COMMAND ----------

# DBTITLE 1,raw_locations_locations_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_locations_locations_corrigo
                AS 
                SELECT 
                    hloc.tenant_id, hloc.location_id, hloc.source_id, hloc.dss_record_source as h_dss_record_source, hloc.dss_load_date as h_dss_load_date, hloc.dss_create_time as h_dss_create_time,
                    lloc.hk_l_areas_locations, lloc.hk_h_areas, lloc.dss_record_source as l_dss_record_source, lloc.dss_load_date as l_dss_load_date, lloc.dss_create_time as l_dss_create_time,
                    sloc.*  
                FROM {var_client_raw_db}.s_locations_locations_corrigo_current sloc
                LEFT JOIN {var_azara_raw_db}.h_locations                            hloc
				       ON hloc.hk_h_locations = sloc.hk_h_locations
                LEFT JOIN {var_azara_raw_db}.l_areas_locations                      lloc
                       ON sloc.hk_h_locations = lloc.hk_h_locations
                WHERE trim(hloc.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_asset_attributes_assetattr_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_asset_attributes_assetattr_corrigo
                AS 
                SELECT 
                    haa.tenant_id, haa.asset_attribute_id, haa.source_id, haa.dss_record_source as h_dss_record_source, haa.dss_load_date as h_dss_load_date, haa.dss_create_time as h_dss_create_time,             
                    lac.hk_l_assets_assetattributes, lac.hk_h_assets, lac.dss_record_source as l_dss_record_source, lac.dss_load_date as l_dss_load_date, lac.dss_create_time as l_dss_create_time,
                    caa.*  
                FROM {var_client_raw_db}.s_asset_attributes_assetattr_corrigo_current  caa
                LEFT JOIN {var_azara_raw_db}.h_asset_attributes                             haa
                       ON haa.hk_h_asset_attributes = caa.hk_h_asset_attributes
                LEFT JOIN {var_azara_raw_db}.l_assets_assetattributes                       lac
                       ON caa.hk_h_asset_attributes = lac.hk_h_asset_attributes
                WHERE TRIM(haa.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_asset_classifications_assetclass_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_asset_classifications_assetclass_corrigo
                AS 
                SELECT 
                    hac.tenant_id, hac.asset_classification_id, hac.source_id, hac.dss_record_source as h_dss_record_source, hac.dss_load_date as h_dss_load_date, hac.dss_create_time as h_dss_create_time,
                    laac.hk_l_areas_assetclassifications, laac.hk_h_areas, laac.dss_record_source as laac_dss_record_source, laac.dss_load_date as laac_dss_load_date, laac.dss_create_time as laac_dss_create_time,
                    cac.*  
                FROM {var_client_raw_db}.s_asset_classifications_assetclass_corrigo_current  cac
                LEFT JOIN {var_azara_raw_db}.h_asset_classifications                              hac
                       ON hac.hk_h_asset_classifications = cac.hk_h_asset_classifications
                LEFT JOIN {var_azara_raw_db}.l_areas_assetclassifications                         laac
                       ON cac.hk_h_asset_classifications = laac.hk_h_asset_classifications
                WHERE trim(hac.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_assets_assets_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_assets_assets_corrigo
                AS 
                SELECT 
                    ha.tenant_id, ha.asset_id, ha.source_id, ha.dss_record_source as h_dss_record_source, ha.dss_load_date as h_dss_load_date, ha.dss_create_time as h_dss_create_time,
                    laa.hk_h_areas, laa.hk_l_areas_assets, laa.dss_record_source as l_dss_record_source, laa.dss_load_date as l_dss_load_date, laa.dss_create_time as l_dss_create_time,
                    ca.*  
                FROM {var_client_raw_db}.s_assets_assets_corrigo_current  ca
                LEFT JOIN {var_azara_raw_db}.h_assets                          ha
                       ON ha.hk_h_assets = ca.hk_h_assets
                LEFT JOIN {var_azara_raw_db}.l_areas_assets                    laa
                       ON ca.hk_h_assets = laa.hk_h_assets
                WHERE trim(ha.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_customers_customers_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_customers_customers_corrigo
                AS 
                SELECT 
                    hc.tenant_id, hc.customer_id, hc.source_id, hc.dss_record_source as h_dss_record_source, hc.dss_load_date as h_dss_load_date, hc.dss_create_time as h_dss_create_time,
                    lwc.hk_l_workorders_customers, lwc.hk_h_work_orders, lwc.dss_record_source as l_dss_record_source, lwc.dss_load_date as l_dss_load_date, lwc.dss_create_time as l_dss_create_time,
                    cc.*  
                FROM {var_client_raw_db}.s_customers_customers_corrigo_current  cc
                left join {var_azara_raw_db}.h_customers                             hc
                       on hc.hk_h_customers = cc.hk_h_customers
                left join {var_azara_raw_db}.l_workorders_customers                  lwc
                       on hc.hk_h_customers = lwc.hk_h_customers
                WHERE trim(hc.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_documents_documents_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_documents_documents_corrigo
                AS 
                SELECT 
                    hd.tenant_id, hd.document_id, hd.source_id, hd.dss_record_source as h_dss_record_source, hd.dss_load_date as h_dss_load_date, hd.dss_create_time as h_dss_create_time,
                    lwd.hk_l_workorders_documents, lwd.hk_h_work_orders, lwd.dss_record_source as l_dss_record_source, lwd.dss_load_date as l_dss_load_date, lwd.dss_create_time as l_dss_create_time,
                    cd.*  
                FROM {var_client_raw_db}.s_documents_documents_corrigo_current  cd
                LEFT JOIN {var_azara_raw_db}.h_documents                             hd
                       ON hd.hk_h_documents = cd.hk_h_documents
                LEFT JOIN {var_azara_raw_db}.l_workorders_documents                  lwd
                       ON cd.hk_h_documents = lwd.hk_h_documents
                WHERE trim(hd.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_employees_employees_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_employees_employees_corrigo
                AS 
                SELECT 
                    hemp.tenant_id, hemp.employee_id, hemp.source_id, hemp.dss_record_source as h_dss_record_source, hemp.dss_load_date as h_dss_load_date, hemp.dss_create_time as h_dss_create_time,
                    lemp.hk_l_workorders_employees, lemp.hk_h_work_orders, lemp.dss_record_source as l_dss_record_source, lemp.dss_load_date as l_dss_load_date, lemp.dss_create_time as l_dss_create_time,
                    semp.*  
                FROM {var_client_raw_db}.s_employees_employees_corrigo_current  semp
                LEFT JOIN {var_azara_raw_db}.h_employees                             hemp
                       on hemp.hk_h_employees = semp.hk_h_employees
                left join {var_azara_raw_db}.l_workorders_employees                  lemp
                       on semp.hk_h_employees = lemp.hk_h_employees
                WHERE trim(hemp.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_inspections_inspections_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_inspections_inspections_corrigo
                AS 
                SELECT 
                    hins.tenant_id, hins.inspection_id, hins.source_id, hins.dss_record_source as h_dss_record_source, hins.dss_load_date as h_dss_load_date, hins.dss_create_time as h_dss_create_time,
                    lins.hk_l_workorders_inspections, lins.hk_h_work_orders, lins.dss_record_source as l_dss_record_source, lins.dss_load_date as l_dss_load_date, lins.dss_create_time as l_dss_create_time,
                    sins.*  
                FROM {var_client_raw_db}.s_inspections_inspections_corrigo_current sins
                LEFT JOIN {var_azara_raw_db}.h_inspections                              hins
                       ON hins.hk_h_inspections = sins.hk_h_inspections
                LEFT JOIN {var_azara_raw_db}.l_workorders_inspections                   lins
                       ON sins.hk_h_inspections = lins.hk_h_inspections
                WHERE trim(hins.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_proposals_proposals_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_proposals_proposals_corrigo
                AS 
                SELECT 
                    hps.tenant_id, hps.proposal_id, hps.source_id, hps.dss_record_source as h_dss_record_source, hps.dss_load_date as h_dss_load_date, hps.dss_create_time as h_dss_create_time,
                    lps.hk_l_workorders_proposals, lps.hk_h_work_orders, lps.dss_record_source as l_dss_record_source, lps.dss_load_date as l_dss_load_date, lps.dss_create_time as l_dss_create_time,
                    sps.*  
                FROM {var_client_raw_db}.s_proposals_proposals_corrigo_current       sps
                LEFT JOIN {var_azara_raw_db}.h_proposals                             hps
                       ON hps.hk_h_proposals = sps.hk_h_proposals
                LEFT JOIN {var_azara_raw_db}.l_workorders_proposals                  lps
                       ON sps.hk_h_proposals = lps.hk_h_proposals
                WHERE trim(hps.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_schedules_schedules_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_schedules_schedules_corrigo
                AS 
                SELECT 
                    hsc.tenant_id, hsc.schedule_id, hsc.source_id, hsc.dss_record_source as h_dss_record_source, hsc.dss_load_date as h_dss_load_date, hsc.dss_create_time as h_dss_create_time,
                    lsc.hk_l_schedules_areas, lsc.hk_h_areas, lsc.dss_record_source as l_dss_record_source, lsc.dss_load_date as l_dss_load_date, lsc.dss_create_time as l_dss_create_time,
                    ssc.*  
                FROM {var_client_raw_db}.s_schedules_schedules_corrigo_current  ssc
                LEFT JOIN {var_azara_raw_db}.h_schedules                             hsc
                       ON hsc.hk_h_schedules = ssc.hk_h_schedules
                LEFT JOIN {var_azara_raw_db}.l_schedules_areas                       lsc
                       ON ssc.hk_h_schedules = lsc.hk_h_schedules
                WHERE trim(hsc.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_equipment_worked_on_equipworkedon_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_equipment_worked_on_equipworkedon_corrigo
                AS 
                SELECT 
                    hewo.tenant_id, hewo.equipment_worked_on_id, hewo.source_id, hewo.dss_record_source as h_dss_record_source, hewo.dss_load_date as h_dss_load_date, hewo.dss_create_time as h_dss_create_time,
                    lwoe.hk_l_workorders_equipment, lwoe.hk_h_work_orders, lwoe.dss_record_source as l_dss_record_source, lwoe.dss_load_date as l_dss_load_date, lwoe.dss_create_time as l_dss_create_time,
                    sewoc.*  
                FROM {var_client_raw_db}.s_equipment_worked_on_equipworkedon_corrigo_current sewoc
                left join {var_azara_raw_db}.h_equipment_worked_on                                hewo
                       on hewo.hk_h_equipment_worked_on = sewoc.hk_h_equipment_worked_on
                left join {var_azara_raw_db}.l_workorders_equipment                               lwoe
                       on sewoc.hk_h_equipment_worked_on = lwoe.hk_h_equipment_worked_on
                WHERE trim(hewo.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_proposal_items_propitems_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_proposal_items_propitems_corrigo
                AS 
                SELECT 
                    hpi.tenant_id, hpi.proposal_item_id, hpi.source_id, hpi.dss_record_source as h_dss_record_source, hpi.dss_load_date as h_dss_load_date, hpi.dss_create_time as h_dss_create_time,
                    lpi.hk_l_proposals_proposalitems, lpi.hk_h_proposals, lpi.dss_record_source as l_dss_record_source, lpi.dss_load_date as l_dss_load_date, lpi.dss_create_time as l_dss_create_time,
                    spic.*  
                FROM {var_client_raw_db}.s_proposal_items_propitems_corrigo_current  spic
                LEFT JOIN {var_azara_raw_db}.h_proposal_items                             hpi 
                       ON hpi.hk_h_proposal_items = spic.hk_h_proposal_items
                LEFT JOIN {var_azara_raw_db}.l_proposals_proposalitems                    lpi
                       ON spic.hk_h_proposal_items = lpi.hk_h_proposal_items
                WHERE trim(hpi.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_rates_svcprovrate_corrigo_current
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_rates_svcprovrate_corrigo
                AS 
                SELECT 
                    hr.tenant_id, hr.service_provider_id, hr.rate_card_id, hr.source_id, hr.dss_record_source as h_dss_record_source, hr.dss_load_date as h_dss_load_date, hr.dss_create_time as h_dss_create_time,
                    sspr.*  
                FROM {var_client_raw_db}.s_rates_svcprovrate_corrigo_current sspr
                LEFT JOIN {var_azara_raw_db}.h_rates                              hr 
                       ON hr.hk_h_rates = sspr.hk_h_rates
                WHERE trim(hr.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_schedule_events_schedevents_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_schedule_events_schedevents_corrigo
                AS 
                SELECT 
                    hse.tenant_id, hse.schedule_event_id, hse.source_id, hse.dss_record_source as h_dss_record_source, hse.dss_load_date as h_dss_load_date, hse.dss_create_time as h_dss_create_time,
                    ssec.*  
                FROM {var_client_raw_db}.s_schedule_events_schedevents_corrigo_current ssec
                LEFT JOIN {var_azara_raw_db}.h_schedule_events                         hse
                       ON hse.hk_h_schedule_events = ssec.hk_h_schedule_events
                WHERE trim(hse.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_spaces_spaces_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_spaces_spaces_corrigo
                AS 
                SELECT 
                    hsp.tenant_id, hsp.space_id, hsp.source_id, hsp.dss_record_source as h_dss_record_source, hsp.dss_load_date as h_dss_load_date, hsp.dss_create_time as h_dss_create_time,
                    lsp.hk_l_workorders_spaces, lsp.hk_h_work_orders, lsp.dss_record_source as l_dss_record_source, lsp.dss_load_date as l_dss_load_date, lsp.dss_create_time as l_dss_create_time,
                    ssp.*  
                FROM {var_client_raw_db}.s_spaces_spaces_corrigo_current      ssp
                LEFT JOIN {var_azara_raw_db}.h_spaces                              hsp 
                       ON hsp.hk_h_spaces = ssp.hk_h_spaces
                LEFT JOIN {var_azara_raw_db}.l_workorders_spaces                   lsp
                       ON ssp.hk_h_spaces = lsp.hk_h_spaces
                WHERE trim(hsp.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_work_orders_invoices_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_orders_invoices_corrigo
                AS 
                SELECT *  from {var_client_raw_db}.s_work_orders_invoices_corrigo_current ; """.format(var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db))

# COMMAND ----------

# DBTITLE 1,raw_schedule_procedures_schedprocs_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_schedule_procedures_schedprocs_corrigo
                AS 
                SELECT 
                    hsp.tenant_id, hsp.schedule_procedure_id, hsp.source_id, hsp.dss_record_source as h_dss_record_source, hsp.dss_load_date as h_dss_load_date, hsp.dss_create_time as h_dss_create_time,
                    sspc.*  
                FROM {var_client_raw_db}.s_schedule_procedures_schedprocs_corrigo_current sspc
                LEFT JOIN {var_azara_raw_db}.h_schedule_procedures                             hsp
                       ON hsp.hk_h_schedule_procedures = sspc.hk_h_schedule_procedures
                WHERE trim(hsp.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_schedule_tasks_schedtasks_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_schedule_tasks_schedtasks_corrigo
                AS 
                SELECT 
                    hst.tenant_id, hst.schedule_task_id, hst.source_id, hst.dss_record_source as h_dss_record_source, hst.dss_load_date as h_dss_load_date, hst.dss_create_time as h_dss_create_time,
                    lsta.hk_l_scheduletasks_assets, lsta.hk_h_assets, lsta.dss_record_source as lsta_dss_record_source , lsta.dss_load_date as lsta_dss_load_date ,lsta.dss_create_time as lsta_dss_create_time,
                    lsts.hk_l_scheduletasks_schedules, lsts.hk_h_schedules, lsts.dss_record_source as lsts_dss_record_source, lsts.dss_load_date as lsts_dss_load_date, lsts.dss_create_time as lsts_dss_create_time,
                    sstc.*  
                FROM {var_client_raw_db}.s_schedule_tasks_schedtasks_corrigo_current sstc
                LEFT JOIN {var_azara_raw_db}.h_schedule_tasks                             hst 
                       ON hst.hk_h_schedule_tasks = sstc.hk_h_schedule_tasks
                LEFT JOIN {var_azara_raw_db}.l_scheduletasks_assets                       lsta
                       ON sstc.hk_h_schedule_tasks = lsta.hk_h_schedule_tasks
                LEFT JOIN {var_azara_raw_db}.l_scheduletasks_schedules                    lsts
                       ON sstc.hk_h_schedule_tasks = lsts.hk_h_schedule_tasks
                WHERE trim(hst.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_service_provider_insurance_svcprovins_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_service_provider_insurance_svcprovins_corrigo
                AS 
                SELECT 
                    hspi.tenant_id, hspi.insurance_id, hspi.source_id, hspi.dss_record_source as h_dss_record_source, hspi.dss_load_date as h_dss_load_date, hspi.dss_create_time as h_dss_create_time,
                    sspic.*  
                FROM {var_client_raw_db}.s_service_provider_insurance_svcprovins_corrigo_current sspic
                LEFT JOIN {var_azara_raw_db}.h_service_provider_insurance                             hspi
                       ON hspi.hk_h_service_provider_insurance = sspic.hk_h_service_provider_insurance
                WHERE trim(hspi.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_tenants_etldatabases_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_tenants_etldatabases_corrigo
                AS 
                SELECT 
                    ht.tenant_id, ht.source_id, ht.dss_record_source as h_dss_record_source, ht.dss_load_date as h_dss_load_date, ht.dss_create_time as h_dss_create_time,
                    lmt.hk_h_master_clients, lmt.hk_l_masterclients_tenants, lmt.dss_record_source as l_dss_record_source, lmt.dss_load_date as l_dss_load_date, lmt.dss_create_time as l_dss_create_time,
                    stc.*  
                FROM {var_client_raw_db}.s_tenants_etldatabases_corrigo_current stc
                LEFT JOIN {var_azara_raw_db}.h_tenants                               ht 
                       ON ht.hk_h_tenants = stc.hk_h_tenants
                LEFT JOIN {var_azara_raw_db}.l_masterclients_tenants                 lmt
                       ON stc.hk_h_tenants = lmt.hk_h_tenants 
                WHERE trim(ht.tenant_id) = "{var_tenant_id}" or trim(ht.tenant_id) = "{var_client_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id, var_client_id=var_client_id))

# COMMAND ----------

# DBTITLE 1,raw_time_card_entries_timecardentries_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_time_card_entries_timecardentries_corrigo
                AS 
                SELECT 
                    htce.tenant_id, htce.time_card_id, htce.source_id, htce.dss_record_source as h_dss_record_source, htce.dss_load_date as h_dss_load_date, htce.dss_create_time as h_dss_create_time,
                    letce.hk_l_employees_timecardentries, letce.hk_h_employees, letce.dss_record_source as el_dss_record_source, letce.dss_load_date as el_dss_load_date, letce.dss_create_time as el_dss_create_time,
                    lwtce.hk_l_workorders_timecardentries, lwtce.hk_h_work_orders, lwtce.dss_record_source as wl_dss_record_source, lwtce.dss_load_date as wl_dss_load_date, lwtce.dss_create_time as wl_dss_create_time,
                    stcec.*  
                FROM {var_client_raw_db}.s_time_card_entries_timecardentries_corrigo_current stcec
                LEFT JOIN {var_azara_raw_db}.h_time_card_entries                             htce
                       ON stcec.hk_h_time_card_entries = htce.hk_h_time_card_entries
                LEFT JOIN {var_azara_raw_db}.l_employees_timecardentries                     letce
                       ON stcec.hk_h_time_card_entries = letce.hk_h_time_card_entries
                LEFT JOIN {var_azara_raw_db}.l_workorders_timecardentries                    lwtce
                       ON stcec.hk_h_time_card_entries = lwtce.hk_h_time_card_entries
                WHERE trim(htce.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_work_orders_custwoval_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_orders_custwoval_corrigo
                AS 
                SELECT * FROM {var_client_raw_db}.s_work_orders_custwoval_corrigo_current ; """.format(var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db))
                      

# COMMAND ----------

# DBTITLE 1,raw_work_orders_parent_wos_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_orders_parent_wos_corrigo
                AS 
                SELECT * FROM {var_client_raw_db}.s_work_orders_parent_wos_corrigo_current ; """.format(var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db))


# COMMAND ----------

# DBTITLE 1,raw_work_order_activity_logs_wo_activitylogs_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_order_activity_logs_wo_activitylogs_corrigo
                AS 
                SELECT 
                    hwoal.tenant_id, hwoal.work_order_activity_log_id, hwoal.source_id, hwoal.work_order_number, hwoal.work_order_id, hwoal.dss_record_source as h_dss_record_source, 
                    hwoal.dss_load_date as h_dss_load_date, hwoal.dss_create_time as h_dss_create_time,
                    lwoal.hk_l_workorders_workorderactivitylogs, lwoal.hk_h_work_orders, lwoal.dss_record_source as l_dss_record_source, lwoal.dss_load_date as l_dss_load_date, lwoal.dss_create_time as l_dss_create_time,
                    swoal.*  
                FROM {var_client_raw_db}.s_work_order_activity_logs_wo_activitylogs_corrigo_current swoal
                LEFT JOIN {var_azara_raw_db}.h_work_order_activity_logs                                  hwoal
                       ON hwoal.hk_h_work_order_activity_logs = swoal.hk_h_work_order_activity_logs
                LEFT JOIN {var_azara_raw_db}.l_workorders_workorderactivitylogs                          lwoal 
                       ON swoal.hk_h_work_order_activity_logs = lwoal.hk_h_work_order_activity_logs 
                WHERE trim(hwoal.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_work_order_completion_notes_wo_compnote_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_order_completion_notes_wo_compnote_corrigo
                AS 
                SELECT 
                    hwocn.tenant_id, hwocn.note_id, hwocn.source_id, hwocn.dss_record_source as h_dss_record_source, hwocn.dss_load_date as h_dss_load_date, hwocn.dss_create_time as h_dss_create_time,
                    lwocn.hk_l_workorders_workordercompletionnotes, lwocn.hk_h_work_orders, lwocn.dss_record_source as l_dss_record_source, lwocn.dss_load_date as l_dss_load_date, lwocn.dss_create_time as l_dss_create_time,
                    swocn.*  
                FROM {var_client_raw_db}.s_work_order_completion_notes_wo_compnote_corrigo_current swocn
                LEFT JOIN {var_azara_raw_db}.h_work_order_completion_notes                              hwocn
                       ON hwocn.hk_h_work_order_completion_notes = swocn.hk_h_work_order_completion_notes
                LEFT JOIN {var_azara_raw_db}.l_workorders_workordercompletionnotes                      lwocn
                       ON swocn.hk_h_work_order_completion_notes = lwocn.hk_h_work_order_completion_notes
                WHERE trim(hwocn.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_work_orders_wo_actions_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_orders_wo_actions_corrigo
                AS 
                SELECT * FROM {var_client_raw_db}.s_work_orders_wo_actions_corrigo_current ; """.format(var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db))

# COMMAND ----------

# DBTITLE 1,raw_work_orders_wo_costs_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_orders_wo_costs_corrigo
                AS 
                SELECT * FROM {var_client_raw_db}.s_work_orders_wo_costs_corrigo_current ; """.format(var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db))

# COMMAND ----------

# DBTITLE 1,raw_work_order_cost_details_wo_costdetail_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_order_cost_details_wo_costdetail_corrigo
                AS 
                SELECT 
                    hwocd.tenant_id, hwocd.work_order_cost_id, hwocd.source_id, hwocd.dss_record_source as h_dss_record_source, hwocd.dss_load_date as h_dss_load_date, hwocd.dss_create_time as h_dss_create_time,
                    swocd.*  
                FROM {var_client_raw_db}.s_work_order_cost_details_wo_costdetail_corrigo_current swocd
                LEFT JOIN {var_azara_raw_db}.h_work_order_cost_details                                hwocd
                       ON hwocd.hk_h_work_order_cost_details = swocd.hk_h_work_order_cost_details
                WHERE trim(hwocd.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_work_order_telemetry_wo_telemetry_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_order_telemetry_wo_telemetry_corrigo
                AS 
                SELECT 
                    hwot.tenant_id, hwot.work_order_telemetry_id, hwot.source_id, hwot.work_order_number, hwot.work_order_id, hwot.dss_record_source as h_dss_record_source, hwot.dss_load_date as h_dss_load_date, hwot.dss_create_time as h_dss_create_time,
                    swot.*,
                    lwot.hk_l_workorders_workordertelemetry as l_hk_l_workorders_workordertelemetry, lwot.hk_h_work_orders as l_hk_h_work_orders, lwot.dss_record_source as l_dss_record_source, lwot.dss_load_date as l_dss_load_date, lwot.dss_create_time as l_dss_create_time
                FROM {var_client_raw_db}.s_work_order_telemetry_wo_telemetry_corrigo_current swot
                LEFT JOIN {var_azara_raw_db}.h_work_order_telemetry                               hwot
                       ON hwot.hk_h_work_order_telemetry = swot.hk_h_work_order_telemetry
                LEFT JOIN {var_azara_raw_db}.l_workorders_workordertelemetry                      lwot
                       ON swot.hk_h_work_order_telemetry = lwot.hk_h_work_order_telemetry
                WHERE trim(hwot.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_work_order_verification_values_verif_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_order_verification_values_verif_corrigo
                AS 
                SELECT 
                    hwvv.tenant_id, hwvv.verification_id, hwvv.source_id, hwvv.dss_record_source as h_dss_record_source, hwvv.dss_load_date as h_dss_load_date, hwvv.dss_create_time as h_dss_create_time,
                    lwv.hk_l_workorders_workorderverivicationvalues, lwv.hk_h_work_orders, lwv.dss_record_source as l_dss_record_source, lwv.dss_load_date as l_dss_load_date, lwv.dss_create_time as l_dss_create_time,
                    swvv.*  
                FROM {var_client_raw_db}.s_work_order_verification_values_verif_corrigo_current            swvv
                LEFT JOIN {var_azara_raw_db}.h_work_order_verification_values                                   hwvv
                       ON hwvv.hk_h_work_order_verification_values = swvv.hk_h_work_order_verification_values
                LEFT JOIN {var_azara_raw_db}.l_workorders_workorderverivicationvalues                           lwv
                       ON swvv.hk_h_work_order_verification_values = lwv.hk_h_work_order_verification_values
                WHERE trim(hwvv.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_work_orders_accountspayable_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_orders_accountspayable_corrigo
                AS 
                SELECT * FROM {var_client_raw_db}.s_work_orders_accountspayable_corrigo_current ; """.format(var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db))

# COMMAND ----------

# DBTITLE 1,raw_master_clients
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_master_clients
                AS
                SELECT * FROM {var_azara_raw_db}.h_master_clients WHERE trim(client_id) = "{var_client_id}"; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_id=var_client_id))

# COMMAND ----------

# DBTITLE 1,raw_master_properties_properties_propertyhub
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_master_properties_properties_propertyhub
                AS
                SELECT 
                    hmp.property_id as h_property_id, hmp.source_id as h_source_id, hmp.dss_record_source as h_dss_record_source, hmp.dss_load_date as h_dss_load_date, hmp.dss_create_time as h_dss_create_time, 
                    smph.*
                From {var_client_raw_db}.s_master_properties_properties_propertyhub_current smph
                join  {var_azara_raw_db}.h_master_properties                                     hmp
                  on hmp.hk_h_master_properties = smph.hk_h_master_properties
                WHERE trim(smph.client_id) = "{var_client_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_id=var_client_id, var_client_raw_db=var_client_raw_db))

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# DBTITLE 1,raw_incident_activity_logs_incidents_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_incident_activity_logs_incidents_corrigo
                AS 
                SELECT 
                    hial.tenant_id, hial.incident_log_id, hial.source_id, hial.dss_record_source as h_dss_record_source, hial.dss_load_date as h_dss_load_date, hial.dss_create_time as h_dss_create_time,
                    lial.hk_l_incidents_incidentactivitylogs, lial.hk_h_incidents, lial.dss_record_source as l_dss_record_source, lial.dss_load_date as l_dss_load_date, lial.dss_create_time as l_dss_create_time,
                    sial.*  
                FROM {var_client_raw_db}.s_incident_activity_logs_incidents_corrigo_current  sial
                left join {var_azara_raw_db}.h_incident_activity_logs                             hial
                       on sial.hk_h_incident_activity_logs = hial.hk_h_incident_activity_logs
                left join {var_azara_raw_db}.l_incidents_incidentactivitylogs                     lial
                       on sial.hk_h_incident_activity_logs = lial.hk_h_incident_activity_logs
                WHERE trim(hial.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_incident_assets_incidents_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_incident_assets_incidents_corrigo
                AS 
                SELECT 
                    lia.hk_h_assets, lia.hk_h_incidents, lia.dss_record_source as l_dss_record_source, lia.dss_load_date as l_dss_load_date, lia.dss_create_time as l_dss_create_time,
                    sia.*  
                FROM {var_client_raw_db}.s_incident_assets_incidents_corrigo_current   sia
                left join {var_azara_raw_db}.l_incident_assets                              lia
                       on sia.hk_l_incident_assets = lia.hk_l_incident_assets ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_incident_tasks_incidents_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_incident_tasks_incidents_corrigo
                AS 
                SELECT 
                    hit.tenant_id, hit.incident_task_id, hit.source_id, hit.dss_record_source as h_dss_record_source, hit.dss_load_date as h_dss_load_date, hit.dss_create_time as h_dss_create_time,
                    lit.hk_l_incidents_incidenttasks, lit.hk_h_incidents, lit.dss_record_source as l_dss_record_source, lit.dss_load_date as l_dss_load_date, lit.dss_create_time as l_dss_create_time,
                    sit.*  
                FROM {var_client_raw_db}.s_incident_tasks_incidents_corrigo_current   sit
                left join {var_azara_raw_db}.h_incident_tasks                              hit
                       on sit.hk_h_incident_tasks = hit.hk_h_incident_tasks
                left join {var_azara_raw_db}.l_incidents_incidenttasks                     lit
                       on sit.hk_h_incident_tasks = lit.hk_h_incident_tasks
                WHERE TRIM(hit.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))


# COMMAND ----------

# DBTITLE 1,raw_incidents_incidents_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_incidents_incidents_corrigo
                AS 
                SELECT 
                    hi.tenant_id, hi.incident_id, hi.source_id, hi.dss_record_source as h_dss_record_source, hi.dss_load_date as h_dss_load_date, hi.dss_create_time as h_dss_create_time,
                    lwoi.hk_l_workorders_incidents, lwoi.hk_h_work_orders, lwoi.dss_record_source as l_dss_record_source, lwoi.dss_load_date as l_dss_load_date, lwoi.dss_create_time as l_dss_create_time,
                    si.*  
                FROM {var_client_raw_db}.s_incidents_incidents_corrigo_current  si
                left join {var_azara_raw_db}.h_incidents                             hi
                       on si.hk_h_incidents = hi.hk_h_incidents
                left join {var_azara_raw_db}.l_workorders_incidents                  lwoi
                       on si.hk_h_incidents = lwoi.hk_h_incidents
                WHERE trim(hi.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_work_order_tasks_wo_tasks_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_work_order_tasks_wo_tasks_corrigo
                AS 
                SELECT 
                    hwot.tenant_id, hwot.work_order_task_id, hwot.source_id, hwot.dss_record_source as h_dss_record_source, hwot.dss_load_date as h_dss_load_date, hwot.dss_create_time as h_dss_create_time,
                    lwot.hk_l_workorder_tasks_workordertasks, lwot.hk_h_work_orders, lwot.dss_record_source as l_dss_record_source, lwot.dss_load_date as l_dss_load_date, lwot.dss_create_time as l_dss_create_time,
                    swot.*  
                FROM {var_client_raw_db}.s_work_order_tasks_wo_tasks_corrigo_current   swot
                left join {var_azara_raw_db}.h_work_order_tasks                             hwot
                       on swot.hk_h_work_order_tasks = hwot.hk_h_work_order_tasks
                left join {var_azara_raw_db}.l_workorder_tasks_workordertasks               lwot
                       on swot.hk_h_work_order_tasks = lwot.hk_h_work_order_tasks
                WHERE trim(hwot.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_responsibilities_resp_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_responsibilities_resp_corrigo
                AS 
                SELECT
                    hr.tenant_id, hr.responsibility_id, hr.responsibility_type_id, hr.source_id, hr.dss_record_source as h_dss_record_source, hr.dss_load_date as h_dss_load_date, hr.dss_create_time as h_dss_create_time,
                    sr.*
                FROM {var_client_raw_db}.s_responsibilities_resp_corrigo_current   sr
                LEFT JOIN {var_azara_raw_db}.h_responsibilities                    hr
                       ON sr.hk_h_responsibilities = hr.hk_h_responsibilities
                WHERE TRIM(hr.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_areas_responsibilities_areas_resp_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_areas_responsibilities_areas_resp_corrigo
                AS 
                SELECT
                    lar.hk_h_areas, lar.hk_h_responsibilities, lar.dss_record_source as l_dss_record_source, lar.dss_load_date as l_dss_load_date, lar.dss_create_time as l_dss_create_time,
                    sar.*
                FROM {var_client_raw_db}.s_areas_responsibilities_areas_resp_corrigo_current    sar
                LEFT JOIN {var_azara_raw_db}.l_areas_responsibilities                           lar 
                       ON sar.hk_l_areas_responsibilities= lar.hk_l_areas_responsibilities ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_contacts_contacts_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_contacts_contacts_corrigo
                AS 
                SELECT
                    hc.tenant_id, hc.contact_id, hc.contact_type_id, hc.source_id, hc.dss_record_source as h_dss_record_source, hc.dss_load_date as h_dss_load_date, hc.dss_create_time as h_dss_create_time,
                    sc.*
                FROM {var_client_raw_db}.s_contacts_contacts_corrigo_current sc
                LEFT JOIN {var_azara_raw_db}.h_contacts                           hc
                       ON sc.hk_h_contacts=hc.hk_h_contacts
                WHERE TRIM(hc.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_customer_contacts_cust_contacts_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_customer_contacts_cust_contacts_corrigo
                AS 
                SELECT 
                    lcc.hk_h_customers,lcc.hk_h_contacts,lcc.dss_record_source as l_dss_record_source,lcc.dss_load_date as l_dss_load_date,lcc.dss_create_time as l_dss_create_time,
                    scc.*
                FROM {var_client_raw_db}.s_customer_contacts_cust_contacts_corrigo_current      scc
                LEFT JOIN {var_azara_raw_db}.l_customer_contacts                                lcc 
                       ON scc.hk_l_customer_contacts = lcc.hk_l_customer_contacts """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_activity_logs_refrigerant_ref_act_logs_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_activity_logs_refrigerant_ref_act_logs_corrigo
                 AS 
                 SELECT 
                     hal.tenant_id, hal.transaction_id, hal.source_id, hal.dss_record_source as h_dss_record_source, hal.dss_load_date as h_dss_load_date, hal.dss_create_time as h_dss_create_time,
                     lal.hk_l_assets_refrigerant_activitylogs, lal.hk_h_assets, lal.dss_record_source as l_dss_record_source, lal.dss_load_date as l_dss_load_date, lal.dss_create_time as l_dss_create_time,
                     sal.*  
                 FROM {var_client_raw_db}.s_activity_logs_refrigerant_ref_act_logs_corrigo_current   sal
                 LEFT JOIN {var_azara_raw_db}.h_activity_logs_refrigerant                                hal
                         on sal.hk_h_activity_logs_refrigerant = hal.hk_h_activity_logs_refrigerant
                 LEFT JOIN {var_azara_raw_db}.l_assets_refrigerant_activitylogs                           lal
                        on sal.hk_h_activity_logs_refrigerant = lal.hk_h_activity_logs_refrigerant
                 WHERE trim(hal.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_employee_teams_emp_team_corrigo
# spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_employee_teams_emp_team_corrigo
#                 AS 
#                 SELECT
#                     he.team_id, he.source_id, he.dss_record_source as h_dss_record_source, he.dss_load_date as h_dss_load_date, he.dss_create_time as h_dss_create_time,
#                     le.hk_h_employees, le.hk_l_employees_employeeteams, le.dss_record_source as l_dss_record_source, le.dss_load_date as l_dss_load_date, le.dss_create_time as l_dss_create_time,
#                     se.*
#                 FROM {var_client_raw_db}.s_employee_teams_emp_team_corrigo_current se
#                 LEFT JOIN {var_azara_raw_db}.h_employee_teams                           he 
#                        ON he.hk_h_employee_teams= se.hk_h_employee_teams 
#                 LEFT JOIN {var_azara_raw_db}.l_employees_employeeteams                  le 
#                        ON le.hk_h_employee_teams=se.hk_h_employee_teams
#                  WHERE trim(he.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# DBTITLE 1,raw_portfolios_portfolios_corrigo_current
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_portfolios_portfolios_corrigo
                 AS 
                 SELECT 
                     hp.tenant_id, hp.portfolio_id, hp.source_id, hp.dss_record_source as h_dss_record_source, hp.dss_load_date as h_dss_load_date, hp.dss_create_time as h_dss_create_time,
                     lp.hk_l_areas_portfolios, lp.hk_h_areas, lp.dss_record_source as l_dss_record_source, lp.dss_load_date as l_dss_load_date, lp.dss_create_time as l_dss_create_time,
                     sp.*  
                 FROM       {var_client_raw_db}.s_portfolios_portfolios_corrigo_current   sp
                 LEFT JOIN {var_azara_raw_db}.h_portfolios                              hp
                        ON sp.hk_h_portfolios = hp.hk_h_portfolios
                 LEFT JOIN {var_azara_raw_db}.l_areas_portfolios                         lp
                        ON sp.hk_h_portfolios = lp.hk_h_portfolios
                 WHERE trim(hp.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_areas_portfolios_areasportfolios_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_areas_portfolios_areasportfolios_corrigo
                AS 
                SELECT 
                    hp.tenant_id, hp.portfolio_id, hp.source_id, hp.dss_record_source as h_dss_record_source, hp.dss_load_date as h_dss_load_date, hp.dss_create_time as h_dss_create_time,
                    lap.hk_h_areas, lap.hk_h_portfolios, lap.dss_record_source as l_dss_record_source, lap.dss_load_date as l_dss_load_date, lap.dss_create_time as l_dss_create_time,
                    sap.*  
                FROM      {var_client_raw_db}.s_areas_portfolios_areasportfolios_corrigo_current   sap
                LEFT JOIN {var_azara_raw_db}.l_areas_portfolios                                    lap
                       ON sap.hk_l_areas_portfolios = lap.hk_l_areas_portfolios
                LEFT JOIN {var_azara_raw_db}.h_portfolios                                          hp
                       ON lap.hk_h_portfolios = hp.hk_h_portfolios
                WHERE trim(hp.tenant_id) = "{var_tenant_id}" ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))

# COMMAND ----------

# DBTITLE 1,raw_service_specialties_services_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_service_specialties_services_corrigo
                AS
                SELECT 
                    hss.tenant_id, hss.specialty_id , hss.source_id, hss.dss_record_source as h_dss_record_source, hss.dss_load_date as h_dss_load_date, hss.dss_create_time as h_dss_create_time,
                    ssss.*
                From      {var_client_raw_db}.s_service_specialties_services_corrigo_current           ssss
                LEFT JOIN {var_azara_raw_db}.h_service_specialties                                     hss
                       ON hss.hk_h_service_specialties = ssss.hk_h_service_specialties ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db, var_tenant_id=var_tenant_id))


# COMMAND ----------

# DBTITLE 1,raw_service_specialties_assigned_services_corrigo
spark.sql(""" CREATE OR REPLACE VIEW {var_client_custom_db}.raw_service_specialties_assigned_services_corrigo
                AS
                SELECT 
                    lssa.hk_h_areas, lssa.hk_h_service_specialties, lssa.dss_record_source as l_dss_record_source, lssa.dss_load_date as l_dss_load_date, lssa.dss_create_time as l_dss_create_time,
                    sssas.*
                From      {var_client_raw_db}.s_service_specialties_assigned_services_corrigo_current          sssas
                LEFT JOIN {var_azara_raw_db}.l_service_specialties_assigned                                    lssa
                       ON lssa.hk_l_service_specialties_assigned = sssas.hk_l_service_specialties_assigned ; """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db, var_client_raw_db=var_client_raw_db))

# COMMAND ----------

# DBTITLE 1,raw_schedule_costs_schedules_corrigo
spark.sql(""" CREATE OR REPLACE  VIEW {var_client_custom_db}.raw_schedule_costs_schedules_corrigo
                AS
                    SELECT  
                        hs.schedule_cost_id, hs.tenant_id, hs.source_id, hs.dss_record_source as h_dss_record_source, hs.dss_load_date as h_dss_load_date, hs.dss_create_time as h_dss_create_time,
                        lsc.hk_l_schedulecosts_schedules, lsc.hk_h_schedules, lsc.dss_record_source as l_dss_record_source, lsc.dss_load_date as l_dss_load_date, lsc.dss_create_time as l_dss_create_time,
                        ssc.*
                    FROM      {var_client_raw_db}.s_schedule_costs_schedules_corrigo_current ssc 
                    LEFT JOIN {var_azara_raw_db}.h_schedule_costs                            hs
                           ON ssc.hk_h_schedule_costs = hs.hk_h_schedule_costs
                    LEFT JOIN {var_azara_raw_db}.l_schedulecosts_schedules                   lsc
                           ON ssc.hk_h_schedule_costs = lsc.hk_h_schedule_costs """.format(var_azara_raw_db=var_azara_raw_db,var_client_custom_db=var_client_custom_db,var_client_raw_db=var_client_raw_db)) 

# COMMAND ----------

# MAGIC %md