# Databricks notebook source
# DBTITLE 1,client variables
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

# DBTITLE 1,ssdv_vw_OVP_ProjectDetailMappings
spark.sql(f"""  
CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_OVP_ProjectDetailMappings
AS
SELECT
   CompanyNbr
  ,SiteId
  ,WebId
  ,TemplateName
  ,Activity1
  ,Activity2
  ,Activity3
  ,Activity4
  ,Activity5
  ,Activity6
  ,Activity7
  ,Activity8
  ,Activity9
  ,Activity10
  ,RP08
  ,RP09
  ,RP10
  ,RP12
  ,RP13
  ,RP23
  ,RP26
  ,RP27
  ,RP28
  ,RP29
  ,RP30
  ,AJD1
  ,AJD2
  ,AJD3
  ,AJD4
  ,AJD5
  ,AJD6
  ,AJD7
  ,AJD8
  ,AJD9
  ,AJD10
  ,AJD11
  ,AJD12
  ,AJD13
  ,AJD14
  ,AJD15
  ,AJD16
  ,AJD17
  ,AJD18
  ,AJD19
  ,AJD20
  ,AJD21
  ,AJD22
  ,AJD23
  ,AJD24
  ,AJD25
  ,AJD26
  ,AJD27
  ,AJD28
  ,AJD29
  ,AJD30
  ,CPN 
  FROM {var_client_custom_db}.{var_client_name}_ovp_tbl_sharepoint_templatemapping 
 WHERE CompanyNbr IN  ('2454','2456','25521','63')
""")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_OVP_ProjectDetailMappings 

# COMMAND ----------

# DBTITLE 1,ssdv_vw_OVP_RiskRegister
spark.sql(f"""  
CREATE OR REPLACE view {var_client_custom_db}.ssdv_vw_OVP_RiskRegister
AS
SELECT gpor.JobNbr                         as job_nbr
      ,b.Phase
	  ,''                                  as Title
      ,b.Description
	  ,''                                  as Risk_Category
      ,b.RiskType                          as Risk_Type
      ,cast(b.ProbabilityScore as FLOAT)   as Probability_Score
      ,cast(b.ImpactScore as decimal(11,1))                       as Impact_Score
      ,b.RiskScore                         as Risk_Score
      ,b.ActiontobeTaken                   as Action_to_be_Taken
      ,b.PlantoAddresstheRisk              as Plan_to_Address_the_Risk
      ,b.Responsibility
      ,cast((case b.PlanApproved
        when '' then 'False'
        else b.planapproved
        end) as boolean)                              as Plan_Approved                  
      ,cast((case b.complete
          when false then '0'
          when true then '1'
          else b.complete
          end) as int)                            as complete
      ,replace(cast(date_format(try_to_timestamp(b.RiskDate,'d/M/yyyy H:mm'), 'yyyy-MM-dd HH:mm:ss.SSSSSS') as TIMESTAMP),'T',' ')                                   as Risk_Date                  
      ,replace(cast(date_format(try_to_timestamp(b.ActionDate,'d/M/yyyy H:mm'), 'yyyy-MM-dd HH:mm:ss') as TIMESTAMP),'T',' ')                                   as Action_Closed_Date          
	  ,b.Riskstatus                        as Risk_status
      ,'Project App'                       as SourceSystem
  FROM {var_client_custom_db}.capitalone_ovp_tbl_der_webs_global_por AS gpor 
  JOIN {var_client_custom_db}.capitalone_ovp_tbl_raw_all_userdata_riskregister AS b 
    ON gpor.SiteID = b.SiteID
   AND gpor.WebId  = b.WebID
   AND gpor.tp_id  = b.ProjectID
 WHERE gpor.ClientNbr IN ('2454','2456','25521','63')
    AND gpor.ProjectStatus <> 'Migrated'
 UNION ALL
 SELECT Job_Nbr
      ,Phase
	  ,Title
      ,Description
      ,Risk_Category
      ,Risk_Type
      ,cast(Probability_Score as FLOAT)
      ,Impact_Score
      ,Risk_Score
      ,Action_to_be_Taken
      ,Plan_to_Address_the_Risk
      ,Responsibility
      ,cast((case Plan_Approved
        when '' then 'False'
        else plan_approved
        end) as boolean)                              as Plan_Approved
      ,Complete
      ,replace(date_format(cast(Risk_Date as TIMESTAMP),'yyyy-MM-dd HH:mm:ss.SSSSSS'),'T',' ')
      ,replace(cast(date_format(try_to_timestamp(action_closed_date,'d/M/yyyy H:mm'), 'yyyy-MM-dd HH:mm:ss') as TIMESTAMP),'T',' ') 
	  ,Risk_status
      ,SourceSystem
 FROM {var_client_custom_db}.ovp_vwtbl_clarizen_riskregister
  WHERE Clarizen_Customer_Nbr IN ('C-119101','C-119102','C-119103','C-123901','C-13800');
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_OVP_RiskRegister

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT gpor.JobNbr                         as job_nbr
# MAGIC       ,b.Phase
# MAGIC 	  ,''                                  as Title
# MAGIC       ,b.Description
# MAGIC 	  ,''                                  as Risk_Category
# MAGIC       ,b.RiskType                          as Risk_Type
# MAGIC       ,cast(b.ProbabilityScore as FLOAT)   as Probability_Score
# MAGIC       ,cast(b.ImpactScore as decimal(11,1))                       as Impact_Score
# MAGIC       ,b.RiskScore                         as Risk_Score
# MAGIC       ,b.ActiontobeTaken                   as Action_to_be_Taken
# MAGIC       ,b.PlantoAddresstheRisk              as Plan_to_Address_the_Risk
# MAGIC       ,b.Responsibility
# MAGIC       ,b.PlanApproved                      as Plan_Approved                  
# MAGIC       ,cast((case b.complete
# MAGIC           when false then '0'
# MAGIC           when true then '1'
# MAGIC           else b.complete
# MAGIC           end) as int)                            as complete
# MAGIC       ,replace(cast(date_format(try_to_timestamp(b.RiskDate,'d/M/yyyy H:mm'), 'yyyy-MM-dd HH:mm:ss.SSSSSS') as TIMESTAMP),'T',' ')                                   as Risk_Date                  
# MAGIC       ,replace(cast(date_format(try_to_timestamp(b.ActionDate,'d/M/yyyy H:mm'), 'yyyy-MM-dd HH:mm:ss') as TIMESTAMP),'T',' ')                                   as Action_Closed_Date          
# MAGIC 	  ,b.Riskstatus                        as Risk_status
# MAGIC       ,'Project App'                       as SourceSystem
# MAGIC   FROM jll_azara_catalog.jll_azara_0007745730_capitalone_custom.capitalone_ovp_tbl_der_webs_global_por AS gpor 
# MAGIC   JOIN jll_azara_catalog.jll_azara_0007745730_capitalone_custom.capitalone_ovp_tbl_raw_all_userdata_riskregister AS b 
# MAGIC     ON gpor.SiteID = b.SiteID
# MAGIC    AND gpor.WebId  = b.WebID
# MAGIC    AND gpor.tp_id  = b.ProjectID
# MAGIC  WHERE gpor.ClientNbr IN ('2454','2456','25521','63')
# MAGIC     AND gpor.ProjectStatus <> 'Migrated'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Job_Nbr
# MAGIC       ,Phase
# MAGIC 	  ,Title
# MAGIC       ,Description
# MAGIC       ,Risk_Category
# MAGIC       ,Risk_Type
# MAGIC       ,cast(Probability_Score as FLOAT)
# MAGIC       ,Impact_Score
# MAGIC       ,Risk_Score
# MAGIC       ,Action_to_be_Taken
# MAGIC       ,Plan_to_Address_the_Risk
# MAGIC       ,Responsibility
# MAGIC       ,cast((case Plan_Approved
# MAGIC         when '' then 'False'
# MAGIC         else plan_approved
# MAGIC         end) as boolean)                              as Plan_Approved
# MAGIC       ,Complete
# MAGIC       ,replace(date_format(cast(Risk_Date as TIMESTAMP),'yyyy-MM-dd HH:mm:ss.SSSSSS'),'T',' ')
# MAGIC       ,replace(cast(date_format(try_to_timestamp(action_closed_date,'d/M/yyyy H:mm'), 'yyyy-MM-dd HH:mm:ss') as TIMESTAMP),'T',' ') 
# MAGIC 	  ,Risk_status
# MAGIC       ,SourceSystem
# MAGIC  FROM jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ovp_vwtbl_clarizen_riskregister
# MAGIC   WHERE Clarizen_Customer_Nbr IN ('C-119101','C-119102','C-119103','C-123901','C-13800')

# COMMAND ----------

# MAGIC %sql
# MAGIC --  select distinct PlanApproved from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.capitalone_ovp_tbl_raw_all_userdata_riskregister
# MAGIC
# MAGIC
# MAGIC select distinct PlanApproved from jll_azara_catalog.jll_azara_raw.edp_snapshot_ovp_tblraw_alusrdtariskregstr 

# COMMAND ----------

# DBTITLE 1,ssdv_vw_OVP_ProjectQuantities
spark.sql(f"""  
CREATE OR REPLACE view {var_client_custom_db}.ssdv_vw_OVP_ProjectQuantities
AS
SELECT gpor.Client
      ,gpor.JobNbr                              AS ProjectID
      ,pq.tp_Created
	  ,author.name                              as CreatedBy
      ,pq.tp_Modified
	  ,editor.name                              as ModifiedBy
      ,gpor.JobName                             as job_name
      ,TotalGrossSqFt                           as Total_Gross_SqFt
      ,TotalGrossSM                             as Total_Gross_SM
      ,TotalRentableSqFt                        as Total_Rentable_SqFt
      ,TotalRentableSM                          as Total_Rentable_SM
      ,TotalUseableSqFt                         as Total_Useable_SqFt
      ,TotalUseableSM                           as Total_Useable_SM
      ,TotalAcreage                             as Total_Acreage
      ,`Cubes/Workstations`                     as cubes_workstations
      ,PeopleMoved                              as People_Moved
      ,TotalSeats                               as Total_Seats
      ,Offices
      ,ResidentialUnits                         as Residential_Units
      ,Beds
      ,HotelRooms                               as hotel_rooms
      ,Classrooms
      ,ParkingSpaces                            as Parking_Spaces
	  ,'Project App'                            as SourceSystem
FROM 
    {var_client_custom_db}.{var_client_name}_ovp_tbl_der_webs_global_por AS gpor
JOIN 
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_projectquantities AS pq
ON 
    gpor.SiteID = pq.SiteID 
AND 
    gpor.WebId  = pq.WebID
AND 
    gpor.tp_id  = pq.ProjectID
LEFT JOIN 
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_webs_xref as x
ON 
    pq.SiteId = x.SiteId 
AND 
    pq.WebID = x.WebId
LEFT JOIN 
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_userinformationlist as author 
On 
    x.SiteID = author.SiteID 
And 
    x.ParentWebId = author.WebID
And 
    pq.tp_Author = author.tp_id
LEFT JOIN 
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_userinformationlist as editor 
On 
    x.SiteID = editor.SiteID 
And 
    x.ParentWebId = editor.WebID
And 
    pq.tp_Editor = editor.tp_id
WHERE 
    gpor.clientNbr IN ('2454','2456','25521','63')
AND 
    gpor.ProjectStatus <> 'Migrated'
UNION ALL
SELECT 
       Client
      ,Job_Nbr                         as projectid
      ,created_on
	  ,''                              as CreatedBy
      ,last_updated_on
	  ,''                              as ModifiedBy
      ,Job_Name
      ,Total_Gross_SqFt
      ,Total_Gross_SM
      ,Total_Rentable_SqFt
      ,Total_Rentable_SM
      ,Total_Useable_SqFt
      ,Total_Useable_SM
      ,Total_Acreage
      ,Cubes_Workstations
      ,People_Moved
      ,Total_Seats
      ,Offices
      ,Residential_Units
      ,Beds
      ,Hotel_Rooms
      ,Classrooms
      ,Parking_Spaces
	  ,SourceSystem
FROM 
    {var_client_custom_db}.dw_all_clients_ovp_vw_clarizen_projectquantities
WHERE 
    Clarizen_Customer_Nbr IN ('C-119101','C-119102','C-119103','C-123901','C-13800')
""")

# COMMAND ----------

# DBTITLE 1,ssdv_vw_OVP_ProjectDirectory
spark.sql(f"""  
CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_OVP_ProjectDirectory
AS
SELECT
   GPOR.JobNbr                                  as Job_Nbr
  ,PRDY.LastName                                as Last_Name
  ,PRDY.FirstName                               as First_Name
  ,PRDY.Company
  ,PRDY.nvarchar21                              as Type_of_Contact
  ,PRDY.nvarchar22                              as Service
  ,PRDY.JobTitle                                as Job_Title
  ,PRDY.nvarchar11                              as Business_Phone
  ,PRDY.nvarchar13                              as Mobile_Phone
  ,PRDY.nvarchar14                              as Fax_Number
  ,PRDY.nvarchar23                              as E_Mail_Address
  ,PRDY.ntext2                                  as Address
  ,PRDY.nvarchar15                              as City
  ,PRDY.nvarchar16                              as State
  ,PRDY.nvarchar17                              as Postal_Code
  ,PRDY.nvarchar18                              as Country
  ,PRDY.Role_In_OVF                             as Role_in_OVF
  ,PRDY.ShowOnHomePage
  ,''                                           as Order_ON_Home_Page -- Coded as NULL in RED
  ,PRDY.ntext3                                  as Notes
  ,PRDY.tp_created
  ,PRDY.CreatedBy
  ,PRDY.tp_modified
  ,PRDY.ModifiedBy
  ,'Project App'                                as SourceSystem
  FROM {var_client_custom_db}.{var_client_name}_ovp_tbl_der_webs_global_por GPOR
  JOIN {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_projectdirectory PRDY  
  ON GPOR.SiteId = PRDY.SiteID AND GPOR.WebId = PRDY.WebID AND GPOR.tp_id = PRDY.ProjectID
 WHERE ClientNbr IN ('2454','2456','25521','63')
   AND UPPER(GPOR.ProjectStatus) <> 'MIGRATED'
UNION ALL
SELECT 
   Job_Nbr
  ,Last_Name
  ,First_Name
  ,Company
  ,Type_of_Contact
  ,Service
  ,Job_Title
  ,Business_Phone
  ,Mobile_Phone
  ,Fax_Number
  ,EMail_Address
  ,Address
  ,City
  ,State
  ,Postal_Code
  ,Country
  ,Role_in_OVF
  ,ShowOnHomePage
  ,Order_ON_Home_Page
  ,Notes
  ,'' as tp_created
  ,'' as CreatedBy
  ,'' as tp_modified
  ,'' as ModifiedBy
  ,SourceSystem
  FROM {var_client_custom_db}.DW_All_Clients_OVP_vw_Clarizen_ProjectDirectory
 WHERE Clarizen_Customer_Nbr IN ('C-119101','C-119102','C-119103','C-123901','C-13800')
""")

# COMMAND ----------

# DBTITLE 1,ssdv_vw_OVP_Milestones
spark.sql(f"""  
CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_OVP_Milestones
AS
SELECT gpor.JobNbr as Job_Nbr
      ,b.ActivityNum as Activity_Number
      ,b.Activity
      ,b.BaselineStart as Baseline_Start
      ,b.ForecastStart as Forecast_Start
      ,b.BaselineFinish as Baseline_Finish
      ,b.ForecastFinish as Forecast_Finish
      ,b.`ShowonMilestones&Project` as Show_On_Milestones_and_Project
      ,b.ActualStart as Actual_Start
      ,b.ActualFinish as Actual_Finish
      ,b.PercentComplete as Percent_Complete
      ,b.StatusComments as Status_Comments
      ,b.Cause
      ,Null AS ShowONCurrentActivites
	  ,Null AS Milestone_Status
	  ,'Project App' as SourceSystem
  FROM {var_client_custom_db}.capitalone_ovp_tbl_der_webs_global_por AS gpor 
  JOIN {var_client_custom_db}.capitalone_ovp_tbl_raw_all_userdata_milestonesprojectactivities AS b 
    ON gpor.SiteId = b.SiteID
   AND gpor.WebId = b.WebID
   AND gpor.tp_id = b.ProjectID
 WHERE ClientNbr IN ('2454','2456','25521','63')
   AND gpor.ProjectStatus <> 'Migrated'
 UNION ALL
 SELECT Job_Nbr
      ,Activity_Number
      ,Activity
      ,Baseline_Start
      ,Forecast_Start
      ,Baseline_Finish
      ,Forecast_Finish
      ,Show_On_Milestones_and_Project
      ,Actual_Start
      ,Actual_Finish
      ,Percent_Complete
      ,Status_Comments
      ,Cause
      ,'' AS ShowONCurrentActivites
	  ,Milestone_Status
	  ,SourceSystem
 FROM {var_client_custom_db}.dw_all_clients_ovp_vw_clarizen_milestones
  WHERE Clarizen_Customer_Nbr IN ('C-119101','C-119102','C-119103','C-123901','C-13800')
""")

# COMMAND ----------

# MAGIC  %sql
# MAGIC  --select count(*) from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_OVP_Milestones
# MAGIC  select * from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_OVP_Milestones

# COMMAND ----------

# DBTITLE 1,ssdv_vw_OVP_JobSiteAddress
spark.sql(f"""  
CREATE OR REPLACE VIEW {var_client_custom_db}.ssdv_vw_OVP_JobSiteAddress
AS
SELECT 
     gpor.JobNbr as Job_Nbr
    ,COALESCE(b.nvarchar1,gpor.PropertyName) AS Location
    ,b.nvarchar3 AS Client_Facility_Nbr
    ,b.BIO
    ,b.OVCP
    ,gpor.Address
    ,gpor.City
    ,gpor.State
    ,gpor.PostalCode as Postal_Code
    ,addbook.Country
    ,gpor.JobSiteAddress as Job_Site_Address
    ,gpor.PDSRegion as PDS_Region
    ,gpor.AssetType as Asset_Type
    ,NULL as Property_SubType
    ,gpor.Industry
    ,gpor.RP10 --as P&G Sub-Region
    ,gpor.RP09 --as P&G Region
    ,gpor.RP13 --as P&G Site
    ,gpor.OVCP as OVCPID
    ,NULL as MDM_ID
    ,0.00 as Latitude
    ,0.00 as Longitude
    ,'Project App' as SourceSystem
FROM 
    {var_client_custom_db}.capitalone_ovp_tbl_der_webs_global_por AS gpor
JOIN 
    {var_client_custom_db}.capitalone_ovp_tbl_raw_all_userdata_jobsiteaddress AS b
    ON gpor.SiteID = b.SiteID
    AND gpor.WebId  = b.WebID
    AND gpor.tp_id  = b.int1
LEFT JOIN 
    {var_client_custom_db}.Stage_E1_dbo_tblJDE_AddressBook as addbook
	ON gpor.JobNbr = addbook.MCMCU
WHERE 
    ClientNbr 
     IN ('2454','2456','25521','63')
    AND UPPER(gpor.ProjectStatus) <> 'MIGRATED'

 UNION ALL

SELECT 
     Job_Nbr
    ,Location
    ,Client_Facility_Nbr
    ,BIO
    ,OVCP
    ,Address
    ,City
    ,State
    ,Postal_Code
    ,Country
    ,Job_Site_Address
    ,PDS_Region
    ,Asset_Type
    ,Property_SubType
    ,Industry
    ,RP10 
    ,RP09
    ,RP13
    ,OVCPID
    ,MDM_ID
    ,Latitude
    ,Longitude
    ,'Clarizen' as SourceSystem 
FROM
    {var_client_custom_db}.dw_all_clients_ovp_vw_clarizen_jobsiteaddress
WHERE 
    Clarizen_Customer_Nbr IN ('C-119101','C-119102','C-119103','C-123901','C-13800')
""")

# COMMAND ----------

# %sql
# --select count(*) from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_OVP_JobSiteAddress
# select * from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_OVP_JobSiteAddress

# COMMAND ----------

# %sql

# SELECT 
#      gpor.JobNbr as Job_Nbr
#     ,COALESCE(b.nvarchar1,gpor.PropertyName) AS Location
#     ,b.nvarchar3 AS Client_Facility_Nbr
#     ,b.BIO
#     ,b.OVCP
#     ,gpor.Address
#     ,gpor.City
#     ,gpor.State
#     ,gpor.PostalCode as Postal_Code
#     ,addbook.Country
#     ,gpor.JobSiteAddress as Job_Site_Address
#     ,gpor.PDSRegion as PDS_Region
#     ,gpor.AssetType as Asset_Type
#     ,NULL as Property_SubType
#     ,gpor.Industry
#     ,gpor.RP10 --as P&G Sub-Region
#     ,gpor.RP09 --as P&G Region
#     ,gpor.RP13 --as P&G Site
#     ,gpor.OVCP as OVCPID
#     ,NULL as MDM_ID
#     ,0.00 as Latitude
#     ,0.00 as Longitude
#     ,'Project App' as SourceSystem
# FROM 
#     jll_azara_catalog.jll_azara_0007745730_capitalone_custom.capitalone_ovp_tbl_der_webs_global_por AS gpor
# JOIN 
#     jll_azara_catalog.jll_azara_0007745730_capitalone_custom.capitalone_ovp_tbl_raw_all_userdata_jobsiteaddress AS b
#     ON gpor.SiteID = b.SiteID
#     AND gpor.WebId  = b.WebID
#     AND gpor.tp_id  = b.int1
# LEFT JOIN 
#     jll_azara_catalog.jll_azara_0007745730_capitalone_custom.Stage_E1_dbo_tblJDE_AddressBook as addbook
# 	ON gpor.JobNbr = addbook.MCMCU
# WHERE 
#     ClientNbr 
#      IN ('2454','2456','25521','63')
#     AND UPPER(gpor.ProjectStatus) <> 'MIGRATED'    

# COMMAND ----------

# %sql
# SELECT 
#      Job_Nbr
#     ,Location
#     ,Client_Facility_Nbr
#     ,BIO
#     ,OVCP
#     ,Address
#     ,City
#     ,State
#     ,Postal_Code
#     ,Country
#     ,Job_Site_Address
#     ,PDS_Region
#     ,Asset_Type
#     ,Property_SubType
#     ,Industry
#     ,RP10 
#     ,RP09
#     ,RP13
#     ,OVCPID
#     ,MDM_ID
#     ,Latitude
#     ,Longitude
#     ,'Clarizen' as SourceSystem 
# FROM
#     jll_azara_catalog.jll_azara_0007745730_capitalone_custom.dw_all_clients_ovp_vw_clarizen_jobsiteaddress
# WHERE 
#     Clarizen_Customer_Nbr IN ('C-119101','C-119102','C-119103','C-123901','C-13800')

# COMMAND ----------

# DBTITLE 1,Clientssdv_vw_OVP_PMDCommentary
spark.sql(f"""
CREATE OR REPLACE VIEW {var_client_custom_db}.Clientssdv_vw_OVP_PMDCommentary
AS
SELECT 
       gpor.JobNbr                        as job_nbr
      ,b.Title
      ,b.Comments
      ,b.tp_Created
	  ,b.CreatedBy                        as CreatedBy
      ,b.tp_Modified                      AS Modified
      ,b.ModifiedBy                       as Modified_By
      ,b.StatusGrade                      as Status_Grade
	  ,nvarchar3                          as Change_Driver
	  ,nvarchar4                          as Change_Owner
	  ,'Project App'                      as SourceSystem
FROM 
    jll_azara_catalog.jll_azara_0007745730_capitalone_custom.capitalone_ovp_tbl_der_webs_global_por AS gpor
JOIN 
    jll_azara_catalog.jll_azara_0007745730_capitalone_custom.capitalone_ovp_tbl_raw_all_userdata_pmdcommentary AS b
ON 
    gpor.SiteID = b.SiteID
AND 
    gpor.WebId  = b.WebID
AND 
    gpor.tp_id  = b.ProjectID
WHERE 
    ClientNbr IN ('2454','2456','25521','63')
AND 
    gpor.ProjectStatus <> 'Migrated'
UNION ALL
SELECT 
       Job_Nbr
      ,Title
      ,Comments
      ,created_dt
	  ,Created_By                     as createdby
      ,Modified_dt
      ,Modified_By
      ,Status_Grade
	  ,''                             as Change_Driver
	  ,''                             as Change_Owner
      ,SourceSystem
FROM 
    jll_azara_catalog.jll_azara_0007745730_capitalone_custom.DW_All_Clients_OVP_vw_Clarizen_PMDCommentary
WHERE 
    Clarizen_Customer_Nbr IN ('C-119101','C-119102','C-119103','C-123901','C-13800')
""")

# COMMAND ----------

