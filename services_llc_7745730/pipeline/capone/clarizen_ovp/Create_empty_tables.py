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
var_client_custom_db = f"{catalog}.{client_obj.databricks_client_custom_db}"

# COMMAND ----------

# DBTITLE 1,stage_ovp_dbo_tblder_clarizen_projects (Metadata)
spark.sql(f""" 
CREATE OR REPLACE TABLE {var_client_custom_db}.stage_ovp_dbo_tblder_clarizen_projects_temp
    (Client_Nbr string
    ,Clarizen_Customer_Nbr string
    ,Client string
    ,Job_Nbr string
    ,Job_Name string
    ,SentToOVF string
    ,Invoice_Approved_Date timestamp
    ,OVF_Close_Date timestamp
    ,I2C_Creation_Date timestamp
    ,OVF_Creation_Date timestamp
    ,Last_OVF_Modified_Date timestamp
    ,Last_OVF_Approved_Date timestamp
    ,Budget_Approved_Date timestamp
    ,Commitment_Approved_Date timestamp
    ,Budget_Change_Modified_Date timestamp
    ,Commitment_Change_Modified_Date timestamp
    ,FullURL string
    ,PHP_Creation_Date string
    ,Description string
    ,Team_Lead string
    ,Project_Manager string
    ,Lead_Architect string
    ,Lead_GC string
    ,Email_Address string
    ,Project_Type string
    ,Project_Status string
    ,Project_Open int
    ,Product_Type string
    ,Property_Name string
    ,Address string
    ,City string
    ,State string
    ,Postal_Code string
    ,Comments string
    ,Job_Site_Address string
    ,PDS_Region string
    ,Location string
    ,Asset_Type string
    ,Owned_Leased string
    ,Industry string
    ,OVCP string
    ,RP08 string
    ,RP09 string
    ,RP10 string
    ,RP12 string
    ,RP13 string
    ,RP23 string
    ,RP26 string
    ,RP27 string
    ,RP28 string
    ,RP29 string
    ,RP30 string
    ,AJD01 string
    ,AJD02 string
    ,AJD03 string
    ,AJD04 string
    ,AJD05 string
    ,AJD06 string
    ,AJD07 string
    ,AJD08 string
    ,AJD09 string
    ,AJD10 string
    ,AJD11 string
    ,AJD12 string
    ,AJD13 string
    ,AJD14 string
    ,AJD15 string
    ,AJD16 string
    ,AJD17 string
    ,AJD18 string
    ,AJD19 string
    ,AJD20 string
    ,AJD21 string
    ,AJD22 string
    ,AJD23 string
    ,AJD24 string
    ,AJD25 string
    ,AJD26 string
    ,AJD27 string
    ,AJD28 string
    ,AJD29 string
    ,AJD30 string
    ,Client_Project_Nbr string
    ,Global_Currency string
    ,Local_Currency string
    ,Exchange_Rate float
    ,GEMS_Project_Num string
    ,ExcelFinancials string
    ,RSF int
    ,USF int
    ,GSF int
    ,OVF_Project_Status string
    ,Post_Edit_Code string
    ,JLL_Role string
    ,mile1_Actual_Start timestamp
    ,mile5_Actual_Finish timestamp
    ,Major_Market string
    ,Minor_Market string
    ,Template string
    ,Last_OVP_Modified_Date timestamp
    ,Project_State string
    ,Overall_Summary string
    ,Duration string
    ,DARM_Flag string
    ,DARM_Exception string
    ,DARM_Violations string
    ,DARM_Columns string
    ,APN string
    ,Fitout_Type string
    ,Customer_Type string
    ,Track_Status string
    ,Total_Estimated_Cost string
    ,Risks string
    ,Status_Manually_Set string
    ,Customer_Region string
    ,Customer_Business_Unit string
    ,Customer_Project_Category string
    ,Customer_Work_Type string
    ,Customer_Project_Type string
    ,Customer_Project_Sub_Type string
    ,Customer_Project_Status string
    ,Start_Date string
    ,Stage string
    ,program string
    ,Percent_Complete string
    ,Document_Folder string
    ,Warehouse_Load_Date timestamp
    ,Billable string
    ,Customer_Project_Manager string
    ,Business_Line string
    ,id string
    ,client_id string
    ,Risk_Level string
    ,Project_Actual_Start_Date string
    ,Project_Baseline_Start_Date string
    ,Project_Due_Date string
    ,Project_Actual_End_Date string
    ,Project_Baseline_Due_Date string
    ,Budget_Summary string
    ,Schedule_Summary string
    ,Project_Coordinator string
    ,Actual_Duration string
    ,budget_status string
    ,Mitigation string
    ,closing_notes string
    ,Last_Updated_By string
    ,Program_Sys_ID string
    ,I2C_Revenue int
    ,SourceSystem string)
""")

# COMMAND ----------

# DBTITLE 1,stage_ovp_dbo_tblder_clarizen_milestones (Metadata)
spark.sql(f""" 
CREATE OR REPLACE TABLE {var_client_custom_db}.stage_ovp_dbo_tblder_clarizen_milestones_temp
    (client_nbr string
    ,clarizen_customer_nbr string
    ,client string
    ,job_nbr string
    ,activity_number string
    ,activity string
    ,baseline_start timestamp
    ,baseline_finish timestamp
    ,forecast_start timestamp
    ,forecast_finish timestamp
    ,actual_start timestamp
    ,actual_finish timestamp
    ,percent_complete decimal(5,2)
    ,status_comments string
    ,last_updated_on string
    ,last_updated_by string
    ,duration decimal(19,4)
    ,sourcesystem string
    ,parent_project string
    ,client_id string
    ,project string
    ,sys_id string
    ,deliverable int
    ,warehouse_load_date timestamp
    ,milestone_status string)
""")