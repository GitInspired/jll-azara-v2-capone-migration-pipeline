# Databricks notebook source
# MAGIC %md
# MAGIC #### Dependency list for fms
# MAGIC * 
# MAGIC

# COMMAND ----------

# DBTITLE 1,ssdv_vw_fms_employee    Created by: Wang Wai Wan
spark.sql("""

create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_fms_employee  as

with _final as (	
SELECT
 C_ID
,EMPID            as Employee_ID
,EMPSTAT          as Employee_Status
,EMPTYPE          as Employee_Type
,EName
,FName
,MNAME            as Middle_Name
,LNAME            as Last_Name
,DISP_NAME        as Full_Name
,ABBR_NAME        as Abbr_Name
,TITLE            as Job_Code
,GRADELEVEL       as Grade_Level
,GROUP_           as Group_Code
,to_date(HDATE)   as Hire_Date
,iff(BADGEID = '',null,BADGEID) as Badge_ID
,Company
,EMAIL        
,OCC1             as Staff_Count
,iff(trim(Phone) = '','None',Phone) as Phone
,iff(POS_ID = '',null,POS_ID) as Position_ID
,iff(PRIMARYLOC = '',null,PRIMARYLOC) as Primary_Location
,NULL             as Use_Standard
,SPCLNEEDS        as Special_Needs
,iff(SPACESTD = '',null,SPACESTD) as Space_Standard
,to_date(TDATE)   as Termination_Date
,TITLE            as Job_Title
,USER_ID          as User_Logon
,WORKTYPE         as Work_Type
,COSTCENTER       as Internal_GL_Account_Number
,HRLOCDESC        as HR_Location_Description
,iff(ISSVCPRVDR = FALSE,0,1)  as Service_Provider_Flag
,MAILSTOP         as Mailstop
,iff(MAN_MNG = FALSE,0,1) as Manually_Manage_Flag
,MANAGERID        as Supervisor_ID
,iff(CREATEDBY = '',null,CREATEDBY) as CREATED_BY
,iff(year(CREATEDON) = 1900,to_date(CREATEDON),to_timestamp(CREATEDON) ) as CREATED_ON
,to_timestamp(DATEADDED)        as Date_Added
,MODIFIEDBY       as MODIFIED_BY
,iff(year(MODIFIEDON) = 1900,to_date(MODIFIEDON),to_timestamp(MODIFIEDON) ) as MODIFIED_ON
,udp_update_ts    as Last_Updated_Date
,to_date(C_DATE01)    as C_DATE01
,to_date(C_DATE02)    as C_DATE02
,to_date(C_DATE03)    as C_DATE03
,iff(C_FLAG01 = FALSE,0,1) as C_FLAG01
,iff(C_FLAG02 = FALSE,0,1) as C_FLAG02
,iff(C_FLAG03 = FALSE,0,1) as C_FLAG03
,C_LOOKUP01
,C_LOOKUP02
,C_LOOKUP03
,C_NUM01
,C_NUM02
,C_NUM03
,C_TEXT01
,C_TEXT02
,C_TEXT03
,iff(C_TEXT04 = '',null,C_TEXT04) as C_TEXT04
,iff(C_TEXT05 = '',null,C_TEXT05) as C_TEXT05
,iff(C_TEXT06 = '',null,C_TEXT06) as C_TEXT06
,iff(C_TEXT07 = '',null,C_TEXT07) as C_TEXT07
,iff(C_TEXT08 = '',null,C_TEXT08) as C_TEXT08
,iff(C_TEXT09 = '',null,C_TEXT09) as C_TEXT09
,iff(C_TEXT10 = '',null,C_TEXT10) as C_TEXT10
,iff(C_TEXT11 = '',null,C_TEXT11) as C_TEXT11
,iff(C_TEXT12 = '',null,C_TEXT12) as C_TEXT12
,iff(C_TEXT13 = '',null,C_TEXT13) as C_TEXT13
,iff(C_TEXT14 = '',null,C_TEXT14) as C_TEXT14
,iff(C_TEXT15 = '',null,C_TEXT15) as C_TEXT15
  	 
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_employees
WHERE C_ID = '7745730'
)
select
 CAST(c_id AS BIGINT)   as   c_id
,CAST(employee_id AS STRING)   as   employee_id
,CAST(employee_status AS STRING)   as   employee_status
,CAST(employee_type AS STRING)   as   employee_type
,CAST(ename AS STRING)   as   ename
,CAST(fname AS STRING)   as   fname
,CAST(middle_name AS STRING)   as   middle_name
,CAST(last_name AS STRING)   as   last_name
,CAST(full_name AS STRING)   as   full_name
,CAST(abbr_name AS STRING)   as   abbr_name
,CAST(job_code AS STRING)   as   job_code
,CAST(grade_level AS STRING)   as   grade_level
,CAST(group_code AS STRING)   as   group_code
,CAST(hire_date AS TIMESTAMP)   as   hire_date
,CAST(badge_id AS STRING)   as   badge_id
,CAST(company AS STRING)   as   company
,CAST(email AS STRING)   as   email
,CAST(staff_count AS BIGINT)   as   staff_count
,CAST(phone AS STRING)   as   phone
,CAST(position_id AS STRING)   as   position_id
,CAST(primary_location AS STRING)   as   primary_location
,CAST(use_standard AS STRING)   as   use_standard
,CAST(special_needs AS STRING)   as   special_needs
,CAST(space_standard AS STRING)   as   space_standard
,CAST(termination_date AS TIMESTAMP)   as   termination_date
,CAST(job_title AS STRING)   as   job_title
,CAST(user_logon AS STRING)   as   user_logon
,CAST(work_type AS STRING)   as   work_type
,CAST(internal_gl_account_number AS STRING)   as   internal_gl_account_number
,CAST(hr_location_description AS STRING)   as   hr_location_description
,CAST(service_provider_flag AS int)   as   service_provider_flag
,CAST(mailstop AS STRING)   as   mailstop
,CAST(manually_manage_flag AS int)   as   manually_manage_flag
,CAST(supervisor_id AS STRING)   as   supervisor_id
,CAST(created_by AS STRING)   as   created_by
,CAST(created_on AS TIMESTAMP)   as   created_on
,CAST(date_added AS TIMESTAMP)   as   date_added
,CAST(modified_by AS STRING)   as   modified_by
,CAST(modified_on AS TIMESTAMP)   as   modified_on
,CAST(last_updated_date AS TIMESTAMP)   as   last_updated_date
,CAST(c_date01 AS TIMESTAMP)   as   c_date01
,CAST(c_date02 AS TIMESTAMP)   as   c_date02
,CAST(c_date03 AS TIMESTAMP)   as   c_date03
,CAST(c_flag01 AS int)   as   c_flag01
,CAST(c_flag02 AS int)   as   c_flag02
,CAST(c_flag03 AS int)   as   c_flag03
,CAST(c_lookup01 AS STRING)   as   c_lookup01
,CAST(c_lookup02 AS STRING)   as   c_lookup02
,CAST(c_lookup03 AS STRING)   as   c_lookup03
,CAST(c_num01 AS DECIMAL(10,2))   as   c_num01
,CAST(c_num02 AS DECIMAL(10,2))   as   c_num02
,CAST(c_num03 AS DECIMAL(10,2))   as   c_num03
,CAST(c_text01 AS STRING)   as   c_text01
,CAST(c_text02 AS STRING)   as   c_text02
,CAST(c_text03 AS STRING)   as   c_text03
,CAST(c_text04 AS STRING)   as   c_text04
,CAST(c_text05 AS STRING)   as   c_text05
,CAST(c_text06 AS STRING)   as   c_text06
,CAST(c_text07 AS STRING)   as   c_text07
,CAST(c_text08 AS STRING)   as   c_text08
,CAST(c_text09 AS STRING)   as   c_text09
,CAST(c_text10 AS STRING)   as   c_text10
,CAST(c_text11 AS STRING)   as   c_text11
,CAST(c_text12 AS STRING)   as   c_text12
,CAST(c_text13 AS STRING)   as   c_text13
,CAST(c_text14 AS STRING)   as   c_text14
,CAST(c_text15 AS STRING)   as   c_text15
from _final

""")
#date_added has timezone issue

# COMMAND ----------

# DBTITLE 1,ssdv_vw_fms_employee_zone    Created by: Wang Wai Wan
spark.sql("""

create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_fms_employee_zone  as
with _final as (
select
 c_id
,ax_key       as zone_key
,empid        as employee_id
,createdby    as created_by
,createdon    as created_on
,modifiedby   as modified_by
,modifiedon   as modified_on
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_emp_zone_assignments
where c_id = '7745730'
)
select
 CAST(c_id AS BIGINT)   as   c_id
,CAST(zone_key AS STRING)   as   zone_key
,CAST(employee_id AS STRING)   as   employee_id
,CAST(created_by AS STRING)   as   created_by
,CAST(created_on AS TIMESTAMP)   as   created_on
,CAST(modified_by AS STRING)   as   modified_by
,CAST(modified_on AS TIMESTAMP)   as   modified_on
from _final


""")


# COMMAND ----------

# DBTITLE 1,ssdv_vw_fms_zone    Created by: Wang Wai Wan
spark.sql("""
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_fms_zone as

with _final as (
select
 z.c_id
,b.ovcp_id       as oneview_client_property_id
,z.ax_key        as zone_key
,z.ay_key        as zone_type_key
,z.bldgcode      as building_code
,z.name          as zone_name
,z.ax_descrip    as zone_description
,z.TARGET        as target_people_to_zone_ratio
,z.ay_descrip    as zone_type_description
,iff(z.ismultirm = FALSE,0,1) as is_multi_room
,iff(z.isocczone = FALSE,0,1) as is_occupant_zone
,z.typeid        as type_id
,z.createdby     as created_by
,z.createdon     as created_on
,z.modifiedby    as modified_by
,z.modifiedon    as modified_on
,z.c_date01
,z.c_date02
,z.c_date03
,iff(z.c_flag01 = FALSE,0,1) as c_flag01
,iff(z.c_flag02 = FALSE,0,1) as c_flag02
,iff(z.c_flag03 = FALSE,0,1) as c_flag03
,z.c_lookup01
,z.c_lookup02
,z.c_lookup03
,z.c_num01
,z.c_num02
,z.c_num03
,z.c_text01
,z.c_text02
,z.c_text03
from           jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_zones     z
     left join jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_buildings b on z.bldgcode = b.bldgcode and z.c_id = b.c_id
where trim(z.c_id) = '7745730'
)
select
 CAST(c_id AS BIGINT)   as   c_id
,CAST(oneview_client_property_id AS BIGINT)   as   oneview_client_property_id
,CAST(zone_key AS STRING)   as   zone_key
,CAST(zone_type_key AS STRING)   as   zone_type_key
,CAST(building_code AS STRING)   as   building_code
,CAST(zone_name AS STRING)   as   zone_name
,CAST(zone_description AS STRING)   as   zone_description
,CAST(target_people_to_zone_ratio AS DECIMAL(18,0))   as   target_people_to_zone_ratio
,CAST(zone_type_description AS STRING)   as   zone_type_description
,CAST(is_multi_room AS int)   as   is_multi_room
,CAST(is_occupant_zone AS int)   as   is_occupant_zone
,CAST(type_id AS STRING)   as   type_id
,CAST(created_by AS STRING)   as   created_by
,CAST(created_on AS TIMESTAMP)   as   created_on
,CAST(modified_by AS STRING)   as   modified_by
,CAST(modified_on AS TIMESTAMP)   as   modified_on
,CAST(c_date01 AS TIMESTAMP)   as   c_date01
,CAST(c_date02 AS TIMESTAMP)   as   c_date02
,CAST(c_date03 AS TIMESTAMP)   as   c_date03
,CAST(c_flag01 AS int)   as   c_flag01
,CAST(c_flag02 AS int)   as   c_flag02
,CAST(c_flag03 AS int)   as   c_flag03
,CAST(c_lookup01 AS STRING)   as   c_lookup01
,CAST(c_lookup02 AS STRING)   as   c_lookup02
,CAST(c_lookup03 AS STRING)   as   c_lookup03
,CAST(c_num01 AS DECIMAL(10,2))   as   c_num01
,CAST(c_num02 AS DECIMAL(10,2))   as   c_num02
,CAST(c_num03 AS DECIMAL(10,2))   as   c_num03
,CAST(c_text01 AS STRING)   as   c_text01
,CAST(c_text02 AS STRING)   as   c_text02
,CAST(c_text03 AS STRING)   as   c_text03
from _final

""")

# COMMAND ----------

# DBTITLE 1,mart_fms_vw_moves
spark.sql("""
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.mart_fms_vw_moves as
with _final as (
select 
 C_ID
,MOVEID as move_id
,A0ERGO as ergonomic_requirement
,ACTUALMOVE as actual_move_on
,APPRBY as  approved_by
,APPRDATE as approved_on
,BOXES as are_boxes_required
,CATTABLE as catalog_table
,COMPLCOMP as complete_computer
,COMPLFAC as complete_facilities
,COMPLTEL as complete_telephone
,COMPNOTES as computer_notes
,CONN_RQMTS as connectivity_requirements
,COST_EST as estimated_cost
,DATERCVD as received_on
,EMAIL as email
,FAX as fax_phone_number
,FMCOSTFLAG as from_cost_allocation            
,FRCOSTCODE as from_cost_code
,fromPRTRS as from_printers
,fromRMID as from_room_id
,fromSRVRS as from_servers
,FURN_RQD as furniture_change_required
,FURNNOTES as furniture_notes
,FURNTASKS as furniture_tasks_complete
,GROUP_ as department                    
,IT_SVC_RQD  as it_services_required
,IT_WO  as  it_work_order_number
,KEY_LOCKID  as lock_id
,KEY_RQD as key_request
,KEY_TYPE as  key_type
,LAN as lan_connection_flag
,LANPRTR as lan_printer_flag
,LOCKMOVE lock_move_flag
,MODEM as modem_phone_number
,MOVECOST as move_cost
,MOVEFAX as move_fax
,MOVEMODEM as move_modem
,MOVEPC as move_pc
,MOVEPHONE as move_phone
,MOVEPROJ as move_project
,MOVEREASON as move_reason
,MVRDESC as move_reason_description
,SCHEDMOVE as scheduled_move_time
,MOVETYPE as move_type
,DESCRIP as move_type_description
,MOVETYPE as move_item_type
,NEWEMPFLAG as is_employee_new
,NEWPHONE as is_phone_new
,NOTES as notes
,PHONE as phone_number
,PMVID as parent_move_id
,QTY as quantity
,REQBYID as requestor_id
,REQBYNAME as requestor_name
,REQDATE as requested_move_on
,REQEMAIL as requestor_email
,User_ID as employee_id
,REQFORNAME as employee_to_move          
,REQPHONE as requestor_phone
,REQPHONE2 as requestor_alternate_phone
,RQDESC as asset_description
,SCHEDMOVE as schedule_move_on
,SPCLNEEDS as special_needs
,STATUS as status
,STATUSDESC as status_description
,STATUSNOTE as status_notes
,TAGNO as asset_tag_number
,TECH_COMPL as it_tasks_completed_on
,TECH_COMPL as it_tasks_complete
,TECH_NOTES as technology_work_notes
,TELNOTES as telephone_notes
,TOBLDGFLR as to_building_floor_code
,TOCOSTCODE as to_cost_code
,TOCOSTFLAG as to_cost_allocation         
,TOPRTRS as to_printers
,TORMID as to_room_id
,TOSRVRS as to_servers
,UPDEMPFLAG as  update_employee_info_flag
,User_ID as user_id
,C_DATE01 as custom_date_01
,C_DATE02 as custom_date_02
,C_DATE03 as custom_date_03
,C_DATE04 as custom_date_04
,C_FLAG01 as custom_flag_01
,C_FLAG02 as custom_flag_02
,C_FLAG03 as custom_flag_03
,C_FLAG04 as custom_flag_04
,C_LOOKUP01 as custom_lookup_01
,C_LOOKUP02  as custom_lookup_02
,C_LOOKUP03 as custom_lookup_03
,C_LOOKUP04 as custom_lookup_04
,C_NUM01 as custom_numeric_01
,C_NUM02 as custom_numeric_02
,C_NUM03 as custom_numeric_03
,C_NUM03 as custom_numeric_04
,C_TEXT01 as custom_text_01
,C_TEXT02 as custom_text_02
,C_TEXT03 as custom_text_03
,C_TEXT04 as custom_text_04
,C_TEXT05 as custom_text_05
,C_TEXT06 as custom_text_06
,C_TEXT07 as custom_text_07
,C_TEXT08 as custom_text_08
,C_TEXT09 as custom_text_09
,C_TEXT10 as custom_text_10
,C_TEXT11 as custom_text_11
,C_TEXT12 as custom_text_12
,current_timestamp() as dss_update_time
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_future_moves
)

select
 C_ID AS C_ID 
,move_id AS Move_ID   
,ergonomic_requirement AS     	Ergonomic_Requirement                         
,actual_move_on AS	Actual_Move_Date   
,approved_by AS Approved_By
,approved_on AS Approved_Date
,are_boxes_required AS Boxes_Required_Flag  
,catalog_table AS Catalog_Table   
,complete_computer AS Complete_Computer  
,complete_facilities AS Complete_Facilities        
,complete_telephone AS Complete_Telephone   
,computer_notes AS Computer_Notes     
,connectivity_requirements AS Connectivity_Requirements        
,estimated_cost AS Estimated_Cost  
,received_on AS Date_Received       
,email AS Email     
,fax_phone_number AS Fax_Phone_Number         
,from_cost_allocation AS from_Cost_Allocation   
,from_cost_code AS from_Cost_Code     
,from_printers AS from_Printers       
,from_room_id AS from_Room_ID      
,from_servers AS from_Servers          
,furniture_change_required AS Furniture_Change_Flag      
,furniture_notes AS Furniture_Notes    
,furniture_tasks_complete AS Furniture_Tasks_Complete_Flag      
,department AS Department                
,it_services_required AS IT_Services_Required      
,it_work_order_number AS IT_Work_Order_Number             
,lock_id AS Lock_ID     
,key_request AS Key_Request_Flag        
,key_type AS Key_Type      
,lan_connection_flag AS LAN_Connection_Flag      
,lan_printer_flag AS LAN_Printer_Flag                 
,lock_move_flag AS Lock_Move_Flag        
,modem_phone_number AS Modem_Phone_Number               
,move_cost AS Move_Cost      
,move_fax AS Move_Fax_Flag              
,move_modem AS Move_Modem_Flag      
,move_pc AS Move_PC_Flag       
,move_phone AS Move_Phone_Instrmnt_Flag                      
,move_project AS Move_Project              
,move_reason AS Move_Reason                     
,move_reason_description AS Move_Reason_Description       
,scheduled_move_time AS Scheduled_Move_Time             
,move_type AS Move_Type             
,move_type_description AS Move_Type_Description         
,move_item_type AS Move_Item_Type             
,is_employee_new AS New_Employee_Flag                       
,is_phone_new AS New_Phone                
,notes AS Notes                     
,phone_number AS Phone_Number 
,parent_move_id AS Parent_Move_ID           
,quantity AS Quantity               
,requestor_id AS Requestor_ID              
,requestor_name AS Requestor_Name           
,requested_move_on AS Requested_Move_Date             
,requestor_email AS Requestor_Email                          
,employee_id AS Employee_ID                     
,employee_to_move AS Employee_to_Move                      
,requestor_phone AS Requestor_Phone 
,requestor_alternate_phone AS Requestor_Alternate_Phone  
,asset_description AS Asset_Description             
,schedule_move_on AS Move_Date                  
,special_needs AS Special_Needs                  
,status AS Status           
,status_description AS Status_Desc              
,status_notes AS Status_Notes                        
,asset_tag_number AS Asset_Tag_Number                         
,it_tasks_completed_on AS IT_Tasks_Completion_Date                       
,it_tasks_complete AS IT_Tasks_Complete_Flag   
,technology_work_notes AS Technology_Work_Notes      
,telephone_notes AS Telephone_Notes                  
,to_building_floor_code AS To_Building_Floor_Code                    
,to_cost_code AS To_Cost_code                  
,to_cost_allocation AS To_Cost_Allocation            
,to_printers AS To_Printers               
,to_room_id AS To_Room_ID                        
,to_servers AS To_Servers                        
,update_employee_info_flag AS Update_Employee_Info_Flag                       
,user_id AS User_ID
,custom_date_01 AS C_DATE01  
,custom_date_02 AS C_DATE02  
,custom_date_03 AS C_DATE03  
,custom_date_04 AS C_DATE04  
,custom_flag_01 AS C_FLAG01  
,custom_flag_02 AS C_FLAG02  
,custom_flag_03 AS C_FLAG03  
,custom_flag_04 AS C_FLAG04  
,custom_lookup_01 AS C_LOOKUP01
,custom_lookup_02 AS C_LOOKUP02
,custom_lookup_03 AS C_LOOKUP03
,custom_lookup_04 AS C_LOOKUP04
,custom_numeric_01 AS C_NUM01   
,custom_numeric_02 AS C_NUM02   
,custom_numeric_03 AS C_NUM03   
,custom_numeric_04 AS C_NUM04   
,custom_text_01 AS C_TEXT01  
,custom_text_02 AS C_TEXT02  
,custom_text_03 AS C_TEXT03       
,custom_text_04 AS C_TEXT04  
,custom_text_05 AS C_TEXT05  
,custom_text_06 AS C_TEXT06    
,custom_text_07 AS C_TEXT07  
,custom_text_08 AS C_TEXT08  
,custom_text_09 AS C_TEXT09    
,custom_text_10 AS C_TEXT10  
,custom_text_11 AS C_TEXT11  
,custom_text_12 AS C_TEXT12                               
from _final



""") 


# COMMAND ----------

# DBTITLE 1,ssdv_vw_FMS_Space_Zone , Version: 1.0, Created_By : Sanjay Gupta
spark.sql("""
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.mart_fms_vw_buildings as
with _final as (
select
 C_ID
,OVCP_ID as OneView_Client_Property_ID
,ALTBLDGDSC as Alternate_Property_Name
,AREATYPE as Area_Type
,AREAUNIT  AS  Unit_of_Measure
,ASSIGNABLE  AS  Assignable_Area
,BLDGCODE  AS  Building_Code
,BLDGCOND  AS  Building_Condition
,BLDGDESC  AS  Building_Description
,BLDGSTATUS  AS  Building_Status
,BLDGTYPE  AS  Building_Type
,BOMASTD  AS  Boma_Standard
,BOOKVALUE  AS  Book_Value
,CITY  AS  City
,COMMENTS  AS  Comments
,CONSDATE  AS  Cons_Date
,COUNTRY  AS  Country
,COUNTY  AS  COUNTY
,CREATEDBY  AS  CREATED_BY
,CREATEDON  AS  CREATED_ON
,CURRCODE  AS  Currency_Type
,DISTRICT  AS  District
,FIRMREGION  AS  Firm_Region
,GROSS  AS  Gross_Area
,INDUSTRY  AS  Industry
,ISBENCHMRK  AS  Benchmarking_Flag
,ISCHGOUT  AS  Charge_Out_Flag
,ISNOTMOVE  AS  SSFMove_Restrict_Flag
,LATITUDE  AS  Latitude
,LONGITUDE  AS  Longitude
,LVLOFSVC  AS  Service_Level
,MARKET  AS  Market
,METRO  AS  Metro
,MODIFIEDBY  AS  MODIFIED_BY
,MODIFIEDON  AS  MODIFIED_ON
,MORTGAGE  AS  Mortgage_Data
,OPEXP  AS  Op_Exp
,OWNERSHIP  AS  Ownership
,OWNINGORG  AS  Owning_Organization
,POLYLINED  AS  Polylined
,PRICE  AS  Purchase_Price
,PROPSUBTYP  AS  Property_Sub_Type
,PROPTYPE  AS  Property_Type
,RATE  AS  Rate
,REGIONID  AS  Region_ID
,RENTABLE  AS  Rentable_Area
,RENTAREA  AS  Rentable_Area_by_Lease
,RENTLC  AS  RENTLC
,SITECODE  AS  Site_Code
,STATE  AS  State
,STATUS  AS  Status
,STRATEGY  AS  strategy
,STREET  AS  Address
,STREET2  AS  Address_2
,STREET3  AS  Address_3
,STREET4  AS  Address_4
,TIMEZONE  AS  Time_Zone
,URL  AS  URL
,USABLE  AS  Usable_Area
,VALUATION  AS  Market_Value
,zip  AS  Zip_Code
,null AS 	Benchmarking_Geography                  
,null AS	Benchmarking_Name                       
,null	AS Benchmarking_Use_Type 
,C_DATE01
,C_DATE02
,C_DATE03
,C_DATE04
,C_DATE05
,C_DATE06
,C_DATE07
,C_DATE08
,C_DATE09
,C_DATE10
,C_FLAG01
,C_FLAG02
,C_FLAG03
,C_FLAG04
,C_FLAG05
,C_FLAG06
,C_FLAG07
,C_FLAG08
,C_FLAG09
,C_FLAG10
,C_LOOKUP01
,C_LOOKUP02
,C_LOOKUP03
,C_LOOKUP04
,C_LOOKUP05
,C_LOOKUP06
,C_LOOKUP07
,C_LOOKUP08
,C_LOOKUP09
,C_LOOKUP10
,C_NUM01
,C_NUM02
,C_NUM03
,C_NUM04
,C_NUM05
,C_NUM06
,C_NUM07
,C_NUM08
,C_NUM09
,C_NUM10
,C_TEXT01
,C_TEXT02
,C_TEXT03
,C_TEXT04
,C_TEXT05
,C_TEXT06
,C_TEXT07
,C_TEXT08
,C_TEXT09
,C_TEXT10
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_buildings
)

select
 cast(C_ID as int) C_ID
,cast(OneView_Client_Property_ID as int) OneView_Client_Property_ID
,Alternate_Property_Name
,case when ltrim(rtrim(area_type)) ='' then null else area_type end  area_type
,case when ltrim(rtrim(Unit_of_Measure)) ='' then null else Unit_of_Measure end  Unit_of_Measure
,cast(Assignable_Area as decimal(10,4)) Assignable_Area
,Building_Code
,Building_Condition
,Building_Description
,Building_Status
,case when ltrim(rtrim(Building_Type)) ='' then null else Building_Type end  Building_Type
,Boma_Standard
,Book_Value
,City
,Comments
,cast(Cons_Date as timestamp) Cons_Date
,Country
,COUNTY
,CREATED_BY
,cast(CREATED_ON  as timestamp)  CREATED_ON
,case when ltrim(rtrim(Currency_Type)) ='' then null else Currency_Type end  Currency_Type
,District
,Firm_Region
,cast(Gross_Area as decimal(10,4)) Gross_Area
,case when ltrim(rtrim(Industry)) ='' then null else Industry end  Industry
--,cast(Benchmarking_Flag as BOOLEAN) Benchmarking_Flag
,cast((case when Benchmarking_Flag =TRUE THEN 1 else 0 end ) as integer) Benchmarking_Flag
,cast((case when Charge_Out_Flag =TRUE THEN 1 else 0 end ) as integer) Charge_Out_Flag
,cast((case when SSFMove_Restrict_Flag =TRUE THEN 1 else 0 end ) as integer) SSFMove_Restrict_Flag
,cast(Latitude as decimal(10,6) ) Latitude
,cast(Longitude as decimal(10,6) ) Longitude
,case when ltrim(rtrim(Service_Level)) ='' then null else Service_Level end  Service_Level
,Market
,Metro
,MODIFIED_BY
,cast(MODIFIED_ON as timestamp) MODIFIED_ON
,Mortgage_Data
,cast(Op_Exp as float) Op_Exp
,Ownership
,Owning_Organization
,Polylined
,cast(Purchase_Price as int) Purchase_Price
,Property_Sub_Type
,Property_Type
,cast(Rate as float) Rate
,Region_ID
,cast(Rentable_Area as decimal(10,4)) Rentable_Area
,cast(Rentable_Area_by_Lease as integer) Rentable_Area_by_Lease
,RENTLC
,Site_Code
,State
,Status
,Strategy
,Address
,Address_2
,Address_3
,Address_4
,case when ltrim(rtrim(Time_Zone)) ='' then null else Time_Zone end  Time_Zone 
,URL
,cast(Usable_Area as decimal(10,4)) Usable_Area
,Market_Value
,Zip_Code
,cast(Benchmarking_Geography as int) Benchmarking_Geography
,cast(Benchmarking_Name as int) Benchmarking_Name
,cast(Benchmarking_Use_Type as int) Benchmarking_Use_Type
,cast(C_DATE01 as timestamp) C_DATE01
,cast(C_DATE02 as timestamp) C_DATE02
,cast(C_DATE03 as timestamp) C_DATE03
,cast(C_DATE04 as timestamp) C_DATE04
,cast(C_DATE05 as timestamp) C_DATE05
,cast(C_DATE06 as timestamp) C_DATE06
,cast(C_DATE07 as timestamp) C_DATE07
,cast(C_DATE08 as timestamp) C_DATE08
,cast(C_DATE09 as timestamp) C_DATE09
,cast(C_DATE10 as timestamp) C_DATE10
,cast((case when C_FLAG01 =TRUE THEN 1 else 0 end ) as integer) C_FLAG01 
,cast((case when C_FLAG02 =TRUE THEN 1 else 0 end ) as integer) C_FLAG02
,cast((case when C_FLAG03 =TRUE THEN 1 else 0 end ) as integer) C_FLAG03
,cast((case when C_FLAG04 =TRUE THEN 1 else 0 end ) as integer) C_FLAG04
,cast((case when C_FLAG05 =TRUE THEN 1 else 0 end ) as integer) C_FLAG05
,cast((case when C_FLAG06 =TRUE THEN 1 else 0 end ) as integer) C_FLAG06
,cast((case when C_FLAG07 =TRUE THEN 1 else 0 end ) as integer) C_FLAG07
,cast((case when C_FLAG08 =TRUE THEN 1 else 0 end ) as integer) C_FLAG08
,cast((case when C_FLAG09 =TRUE THEN 1 else 0 end ) as integer) C_FLAG09
,cast((case when C_FLAG10 =TRUE THEN 1 else 0 end ) as integer) C_FLAG10
,C_LOOKUP01
,C_LOOKUP02
,C_LOOKUP03
,C_LOOKUP04
,C_LOOKUP05
,C_LOOKUP06
,C_LOOKUP07
,C_LOOKUP08
,C_LOOKUP09
,C_LOOKUP10
,cast(C_NUM01 as decimal(10,2)) C_NUM01
,cast(C_NUM02 as decimal(10,2)) C_NUM02
,cast(C_NUM03 as decimal(10,2)) C_NUM03
,cast(C_NUM04 as decimal(10,2)) C_NUM04
,cast(C_NUM05 as decimal(10,2)) C_NUM05
,cast(C_NUM06 as decimal(10,2)) C_NUM06
,cast(C_NUM07 as decimal(10,2)) C_NUM07
,cast(C_NUM08 as decimal(10,2)) C_NUM08
,cast(C_NUM09 as decimal(10,2)) C_NUM09
,cast(C_NUM10 as decimal(10,2)) C_NUM10
,case when ltrim(rtrim(C_TEXT01)) ='' then null else C_TEXT01 end  C_TEXT01 
,case when ltrim(rtrim(C_TEXT02)) ='' then null else C_TEXT02 end  C_TEXT02
,case when ltrim(rtrim(C_TEXT03)) ='' then null else C_TEXT03 end  C_TEXT03
,case when ltrim(rtrim(C_TEXT04)) ='' then null else C_TEXT04 end  C_TEXT04
,case when ltrim(rtrim(C_TEXT05)) ='' then null else C_TEXT05 end  C_TEXT05
,case when ltrim(rtrim(C_TEXT06)) ='' then null else C_TEXT06 end  C_TEXT06
,case when ltrim(rtrim(C_TEXT07)) ='' then null else C_TEXT07 end  C_TEXT07
,case when ltrim(rtrim(C_TEXT08)) ='' then null else C_TEXT08 end  C_TEXT08
,case when ltrim(rtrim(C_TEXT09)) ='' then null else C_TEXT09 end  C_TEXT09
,case when ltrim(rtrim(C_TEXT10)) ='' then null else C_TEXT10 end  C_TEXT10
from _final
   
""")



spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_FMS_Space_Zone as
with _final as 
(
select
 s.C_ID
,b.ovcp_id AS	OneView_Client_Property_ID      
,s.AX_KEY as zone_key   
,s.RMID as room_id
,s.AZ_KEY as space_zone_key
,s.BLDGCODE as building_code  
                                             	 
from           jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_room_zone_assignments s
     left join jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_buildings             b on b.C_ID = s.C_ID  and b.BLDGCODE = s.BLDGCODE
)

select 
 cast(C_ID as int) C_ID
,cast(OneView_Client_Property_ID as int) OneView_Client_Property_ID
,Zone_Key
,Room_ID
,Space_Zone_Key
,Building_Code
 from _final
 where C_ID = '7745730'
""")


# COMMAND ----------

# DBTITLE 1,ssdv_vw_FMS_Space_Type , Version: 1.0, Created_By : Sanjay Gupta
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_FMS_Space_Type as
with _final as
(

select
 C_ID
,SPACETYPE AS	Space_Type_Code
,SPACETYPDS AS	Space_Type_Description
,SPACECAT AS  	Space_Category
,CAT1  AS	Space_Category1
,SPACECLASS	 AS Space_Class
,RATE	 AS Charge_Rate 
,ALLOC AS  	Proration 
,ALTGROUP  AS	Grouping
,ISEXCLUDE AS 	Exclude_Flag
,ISFLEXHTL  AS	Flexible_Hoteling_Flag
,ISMTGRPT  AS 	Meeting_Reporting_Flag
,ISPVTOFFC  AS	Private_Office_Flag
,ISWORKSTN  AS	Workstation_Flag
,CREATEDBY AS	CREATED_BY
,CREATEDON  AS	CREATED_ON 
,MODIFIEDBY AS	MODIFIED_BY
,MODIFIEDON AS	MODIFIED_ON
,C_DATE01 AS C_DATE01
,C_DATE02 AS C_DATE02 
,C_DATE03 AS C_DATE03
,C_FLAG01 AS C_FLAG01
,C_FLAG02 AS C_FLAG02
,C_FLAG03 AS C_FLAG03
,C_LOOKUP01 AS C_LOOKUP01
,C_LOOKUP02 AS C_LOOKUP02
,C_LOOKUP03 AS C_LOOKUP03
,C_NUM01 AS C_NUM01
,C_NUM02 AS C_NUM02
,C_NUM03 AS C_NUM03
,C_TEXT01 AS C_TEXT01
,C_TEXT02 AS C_TEXT02
,C_TEXT03 AS C_TEXT03
,C_TEXT04 AS C_TEXT04
,C_TEXT05 AS C_TEXT05
,C_TEXT06 AS C_TEXT06
,C_TEXT07 AS C_TEXT07
,C_TEXT08 AS C_TEXT08
,C_TEXT09 AS C_TEXT09
,C_TEXT10 AS C_TEXT10
,C_TEXT11 AS C_TEXT11
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_space_types
)

select
 cast(C_ID as int) C_ID
,cast(Space_Type_Code as varchar(30)) Space_Type_Code
,cast(Space_Type_Description as varchar(100)) Space_Type_Description
,cast(Space_Category as varchar(3)) Space_Category
,case when ltrim(rtrim(Space_Category1)) ='' then null else Space_Category1 end  Space_Category1
,cast(Space_Class as varchar(50)) Space_Class 
,cast(Charge_Rate as float) Charge_Rate
,cast(Proration as varchar(2)) Proration
,cast(Grouping as varchar(10)) Grouping
,cast((case when Exclude_Flag =TRUE THEN 1 else 0 end ) as integer)  Exclude_Flag
,cast((case when Flexible_Hoteling_Flag =TRUE THEN 1 else 0 end ) as integer)  Flexible_Hoteling_Flag
,cast((case when Meeting_Reporting_Flag =TRUE THEN 1 else 0 end ) as integer)  Meeting_Reporting_Flag
,cast((case when Private_Office_Flag =TRUE THEN 1 else 0 end ) as integer) Private_Office_Flag
,cast((case when Workstation_Flag =TRUE THEN 1 else 0 end ) as integer)  Workstation_Flag
,case when ltrim(rtrim(CREATED_BY)) ='' then null else CREATED_BY end CREATED_BY
,cast(CREATED_ON as timestamp )  CREATED_ON
,case when ltrim(rtrim(MODIFIED_BY)) ='' then null else MODIFIED_BY end MODIFIED_BY
,cast(MODIFIED_ON as timestamp ) MODIFIED_ON
,cast(C_DATE01 as timestamp ) C_DATE01
,cast(C_DATE02 as timestamp ) C_DATE02
,cast(C_DATE03 as timestamp ) C_DATE03
,cast((case when C_FLAG01 =TRUE THEN 1 else 0 end ) as integer) C_FLAG01
,cast((case when C_FLAG02 =TRUE THEN 1 else 0 end ) as integer) C_FLAG02
,cast((case when C_FLAG03 =TRUE THEN 1 else 0 end ) as integer) C_FLAG03
,cast(C_LOOKUP01 as varchar(50)) C_LOOKUP01
,cast(C_LOOKUP02 as varchar(50)) C_LOOKUP02
,cast(C_LOOKUP03 as varchar(50))  C_LOOKUP03
,cast(C_NUM01 as decimal(10,2) ) C_NUM01
,cast(C_NUM02 as decimal(10,2) ) C_NUM02
,cast(C_NUM03 as decimal(10,2) ) C_NUM03
,case when ltrim(rtrim(C_TEXT01)) ='' then null else C_TEXT01 end  C_TEXT01 
,case when ltrim(rtrim(C_TEXT02)) ='' then null else C_TEXT02 end  C_TEXT02
,case when ltrim(rtrim(C_TEXT03)) ='' then null else C_TEXT03 end  C_TEXT03
,case when ltrim(rtrim(C_TEXT04)) ='' then null else C_TEXT04 end  C_TEXT04
,case when ltrim(rtrim(C_TEXT05)) ='' then null else C_TEXT05 end  C_TEXT05
,case when ltrim(rtrim(C_TEXT06)) ='' then null else C_TEXT06 end  C_TEXT06
,case when ltrim(rtrim(C_TEXT07)) ='' then null else C_TEXT07 end  C_TEXT07
,case when ltrim(rtrim(C_TEXT08)) ='' then null else C_TEXT08 end  C_TEXT08
,case when ltrim(rtrim(C_TEXT09)) ='' then null else C_TEXT09 end  C_TEXT09
,case when ltrim(rtrim(C_TEXT10)) ='' then null else C_TEXT10 end  C_TEXT10
,case when ltrim(rtrim(C_TEXT11)) ='' then null else C_TEXT11 end  C_TEXT11
from _final
where C_ID = '7745730'

 """)


# COMMAND ----------

# DBTITLE 1,Clientssdv_vw_BAC_FMS_MAC_Active , Version: 1.0,Created_By : Sanjay Gupta
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.Clientssdv_vw_BAC_FMS_MAC_Active as
with _final as (
select
 C_ID 
,Move_ID 
,Ergonomic_Requirement 
,CAST(Actual_Move_Date  AS timestamp) Actual_Move_Date
,Approved_By   
,CAST(Approved_Date    AS timestamp) Approved_Date  
,Boxes_Required_Flag  
,Catalog_Table
,Complete_Computer
,Complete_Facilities 
,Complete_Telephone 
,Computer_Notes  
,Connectivity_Requirements  
,Estimated_Cost 
,CAST(Date_Received  AS timestamp) Date_Received
,Email 
,Fax_Phone_Number     
,from_Cost_Allocation 
,from_Cost_Code       
,from_Printers        
,from_Room_ID         
,from_Servers  
,Furniture_Change_Flag 		
,Furniture_Notes       		
,Furniture_Tasks_Complete_Flag     
,Department                   
,IT_Services_Required         
,IT_Work_Order_Number         
,Lock_ID            		
,Key_Request_Flag   		   
,Key_Type           		
,LAN_Connection_Flag		  
,LAN_Printer_Flag                  
,Lock_Move_Flag            	
,Modem_Phone_Number       
,CAST(Move_Cost AS FLOAT) Move_Cost
,Move_Fax_Flag 
,Move_Modem_Flag  
,Move_PC_Flag     
,Move_Phone_Instrmnt_Flag                   
,Move_Project                   
,Move_Reason                     
,Move_Reason_Description             
,Scheduled_Move_Time                  
,Move_Type                  
,Move_Type_Description              
,Move_Item_Type                  
,New_Employee_Flag                    
,New_Phone                     
,Notes                          
,Phone_Number      
,Parent_Move_ID        
,CAST(Quantity  AS INT) Quantity
,Requestor_ID 
,Requestor_Name 
,CAST(Requested_Move_Date  AS timestamp) Requested_Move_Date
,Requestor_Email  
,Employee_ID          
,Employee_to_Move                           
,Requestor_Phone      
,Requestor_Alternate_Phone   
,Asset_Description                  
,CAST(Move_Date   AS timestamp) Move_Date
,Special_Needs      
,case when ltrim(rtrim(Status)) ='' then null else Status end  Status            
,Status_Desc          
,Status_Notes             
,Asset_Tag_Number       
,CAST(IT_Tasks_Completion_Date    AS timestamp)  IT_Tasks_Completion_Date
,IT_Tasks_Complete_Flag       
,Technology_Work_Notes        
,Telephone_Notes              
,To_Building_Floor_Code         
,To_Cost_code                 
,To_Cost_Allocation           
,To_Printers                  
,To_Room_ID                    
,To_Servers                   
,Update_Employee_Info_Flag      
,User_ID        
,CAST(C_DATE01 AS   timestamp)  C_DATE01
,CAST(C_DATE02 AS    timestamp) C_DATE02
,CAST(C_DATE03 AS    timestamp) C_DATE03
,CAST(C_DATE04 AS    timestamp) C_DATE04
,C_FLAG01       
,C_FLAG02   
,C_FLAG03   
,C_FLAG04   
,C_LOOKUP01 
,C_LOOKUP02 
,C_LOOKUP03 
,C_LOOKUP04 
,CAST(C_NUM01 AS  DECIMAL(10,2)) C_NUM01
,CAST(C_NUM02 AS  DECIMAL(10,2)) C_NUM02
,CAST(C_NUM03 AS  DECIMAL(10,2)) C_NUM03
,CAST(C_NUM04 AS  DECIMAL(10,2)) C_NUM04
,C_TEXT01 
,C_TEXT02 
,C_TEXT03 
,C_TEXT04 
,C_TEXT05 
,C_TEXT06 
,C_TEXT07 
,C_TEXT08 
,C_TEXT09 
,C_TEXT10 
,C_TEXT11 
,C_TEXT12  
,null as Posted_Date     
from  jll_azara_catalog.jll_azara_0007745730_capitalone_custom.mart_fms_vw_moves
WHERE C_ID = '7745730'
)
  
select
 C_ID 
,Move_ID   
,Ergonomic_Requirement                         
,Actual_Move_Date   
,Approved_By
,Approved_Date
,Boxes_Required_Flag  
,nvl(Catalog_Table,null)   Catalog_Table
,Complete_Computer  
,Complete_Facilities        
,Complete_Telephone   
,Computer_Notes     
,Connectivity_Requirements        
,Estimated_Cost  
,Date_Received       
,Email     
,Fax_Phone_Number         
,from_Cost_Allocation   
,from_Cost_Code     
,from_Printers       
,from_Room_ID      
,from_Servers          
,Furniture_Change_Flag      
,Furniture_Notes    
,Furniture_Tasks_Complete_Flag      
,Department                
,IT_Services_Required      
,IT_Work_Order_Number             
,Lock_ID     
,Key_Request_Flag        
,Key_Type      
,LAN_Connection_Flag      
,LAN_Printer_Flag                 
,Lock_Move_Flag        
,Modem_Phone_Number               
,Move_Cost      
,Move_Fax_Flag              
,Move_Modem_Flag      
,Move_PC_Flag       
,Move_Phone_Instrmnt_Flag                      
,Move_Project              
,Move_Reason                     
,Move_Reason_Description       
,Scheduled_Move_Time             
,Move_Type             
,Move_Type_Description         
,Move_Item_Type             
,New_Employee_Flag                       
,New_Phone                
,Notes                     
,Phone_Number 
,Parent_Move_ID           
,Quantity               
,Requestor_ID              
,Requestor_Name           
,Requested_Move_Date             
,Requestor_Email                          
,Employee_ID                     
,Employee_to_Move                      
,Requestor_Phone 
,Requestor_Alternate_Phone  
,Asset_Description             
,Move_Date                  
,Special_Needs                  
,Status         
,Status_Desc              
,Status_Notes                        
,Asset_Tag_Number                         
,IT_Tasks_Completion_Date                       
,IT_Tasks_Complete_Flag   
,Technology_Work_Notes      
,Telephone_Notes                  
,To_Building_Floor_Code                    
,To_Cost_code                  
,To_Cost_Allocation            
,To_Printers               
,To_Room_ID                        
,To_Servers                        
,Update_Employee_Info_Flag                       
,User_ID
,C_DATE01  
,C_DATE02  
,C_DATE03  
,C_DATE04  
,C_FLAG01  
,C_FLAG02  
,C_FLAG03  
,C_FLAG04  
,C_LOOKUP01
,C_LOOKUP02
,C_LOOKUP03
,C_LOOKUP04
,C_NUM01   
,C_NUM02   
,C_NUM03   
,C_NUM04   
,C_TEXT01  
,C_TEXT02  
,C_TEXT03    
,C_TEXT04  
,C_TEXT05  
,C_TEXT06    
,C_TEXT07  
,C_TEXT08  
,C_TEXT09    
,C_TEXT10  
,C_TEXT11  
,C_TEXT12          
,CAST(Posted_Date AS INT) Posted_Date
from _FINAL
 """)


# COMMAND ----------

# DBTITLE 1,Clientssdv_vw_BAC_FMS_MAC_Archive , Version: 1.0, Created_By : Sanjay Gupta
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.Clientssdv_vw_BAC_FMS_MAC_Archive as

with _final as (

select 
 C_ID
,MOVEID as Move_ID
,A0ERGO as Ergonomic_Requirement
,ACTUALMOVE as Actual_Move_Date
,APPRBY as  Approved_By
,APPRDATE as Approved_Date
,BOXES as Boxes_Required_Flag
,CATTABLE as Catalog_Table
,COMPLCOMP as Complete_Computer
,COMPLFAC as Complete_Facilities
,COMPLTEL as Complete_Telephone
,COMPNOTES as Computer_Notes
,CONN_RQMTS as Connectivity_Requirements
,COST_EST as Estimated_Cost
,DATERCVD as Date_Received
,EMAIL as Email
,FAX as Fax_Phone_Number
,FMCOSTFLAG as from_Cost_Allocation            
,FRCOSTCODE as from_Cost_Code
,fromPRTRS as from_Printers
,fromRMID as from_Room_ID
,fromSRVRS as from_Servers
,FURN_RQD as Furniture_Change_Flag
,FURNNOTES as Furniture_Notes
,FURNTASKS as Furniture_Tasks_Complete_Flag
,GROUP_ as Department                    
,IT_SVC_RQD  as IT_Services_Required
,IT_WO  as  IT_Work_Order_Number
,KEY_LOCKID  as Lock_ID
,KEY_RQD as Key_Request_Flag
,KEY_TYPE as  Key_Type
,LAN as LAN_Connection_Flag
,LANPRTR as LAN_Printer_Flag
,LOCKMOVE Lock_Move_Flag
,MODEM as Modem_Phone_Number
,MOVECOST as Move_Cost
,MOVEFAX as Move_Fax_Flag
,MOVEMODEM as Move_Modem_Flag
,MOVEPC as Move_PC_Flag
,MOVEPHONE as Move_Phone_Instrmnt_Flag
,MOVEPROJ as Move_Project
,MOVEREASON as Move_Reason
,MVRDESC as Move_Reason_Description
,SCHEDMOVE as Scheduled_Move_Time
,MOVETYPE as Move_Type
,DESCRIP as Move_Type_Description
,MOVETYPE as Move_Item_Type
,NEWEMPFLAG as New_Employee_Flag
,NEWPHONE as New_Phone
,NOTES as Notes
,PHONE as Phone_Number
,PMVID as Parent_Move_ID
,QTY as Quantity
,REQBYID as Requestor_ID
,REQBYNAME as Requestor_Name
,REQDATE as Requested_Move_Date
,REQEMAIL as Requestor_Email
,User_ID as Employee_ID
,REQFORNAME as Employee_to_Move          
,REQPHONE as Requestor_Phone
,REQPHONE2 as Requestor_Alternate_Phone
,RQDESC as Asset_Description
,SCHEDMOVE as Move_Date
,SPCLNEEDS as Special_Needs
,STATUS as Status
,STATUSDESC as Status_Desc
,STATUSNOTE as Status_Notes
,TAGNO as Asset_Tag_Number	
,TECH_COMPL as IT_Tasks_Completion_Date
,TECH_COMPL as IT_Tasks_Complete_Flag
,TECH_NOTES as Technology_Work_Notes
,TELNOTES as Telephone_Notes
,TOBLDGFLR as To_Building_Floor_Code
,TOCOSTCODE as To_Cost_code
,TOCOSTFLAG as To_Cost_Allocation         
,TOPRTRS as To_Printers
,TORMID as To_Room_ID
,TOSRVRS as To_Servers
,UPDEMPFLAG as  Update_Employee_Info_Flag
,User_ID as User_ID
,C_DATE01 
,C_DATE02 
,C_DATE03 
,C_DATE04 
,C_FLAG01 
,C_FLAG02 
,C_FLAG03 
,C_FLAG04 
,C_LOOKUP01  
,C_LOOKUP02  
,C_LOOKUP03 
,C_LOOKUP04 
,C_NUM01  
,C_NUM02 
,C_NUM03 
,C_NUM04 
,C_TEXT01
,C_TEXT02
,C_TEXT03
,C_TEXT04
,C_TEXT05
,C_TEXT06
,C_TEXT07
,C_TEXT08
,C_TEXT09
,C_TEXT10
,C_TEXT11
,C_TEXT12
,POSTED as Posted_Date
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_moves

)
select
 C_ID
,Move_ID
,Ergonomic_Requirement
,cast(Actual_Move_Date as timestamp ) Actual_Move_Date
,Approved_By
,cast(Approved_Date as timestamp)  Approved_Date
,cast(Boxes_Required_Flag as int) Boxes_Required_Flag
,Catalog_Table
,Complete_Computer
,Complete_Facilities
,Complete_Telephone
,Computer_Notes
,Connectivity_Requirements
,cast(Estimated_Cost as decimal(10,0))  Estimated_Cost
,cast(Date_Received as timestamp) Date_Received
,Email
,Fax_Phone_Number
,from_Cost_Allocation
,from_Cost_Code
,from_Printers
,from_Room_ID
,from_Servers
,Furniture_Change_Flag
,Furniture_Notes
,cast(Furniture_Tasks_Complete_Flag as int) Furniture_Tasks_Complete_Flag
,Department
,IT_Services_Required
,IT_Work_Order_Number
,Lock_ID
,Key_Request_Flag
,Key_Type
,cast(LAN_Connection_Flag as int)  LAN_Connection_Flag
,cast(LAN_Printer_Flag as int) LAN_Printer_Flag
,cast(Lock_Move_Flag as int) Lock_Move_Flag
,Modem_Phone_Number
,cast(Move_Cost as float) Move_Cost
,cast(Move_Fax_Flag as int) Move_Fax_Flag
,cast(Move_Modem_Flag as int) Move_Modem_Flag
,cast(Move_PC_Flag as int) Move_PC_Flag
,cast(Move_Phone_Instrmnt_Flag as int) Move_Phone_Instrmnt_Flag
,Move_Project
,Move_Reason
,Move_Reason_Description
,Scheduled_Move_Time
,Move_Type
,Move_Type_Description
,Move_Item_Type
,cast(New_Employee_Flag as int) New_Employee_Flag
,New_Phone
,Notes
,Phone_Number
,Parent_Move_ID
,cast(Quantity as int) Quantity
,Requestor_ID
,Requestor_Name
,cast(Requested_Move_Date as timestamp)
,Requestor_Email
,Employee_ID
,Employee_to_Move
,Requestor_Phone
,Requestor_Alternate_Phone
,Asset_Description
,cast(Move_Date as timestamp) Move_Date
,Special_Needs
,Status
,Status_Desc
,Status_Notes
,Asset_Tag_Number
,cast(IT_Tasks_Completion_Date as timestamp) IT_Tasks_Completion_Date
,cast(IT_Tasks_Complete_Flag as int) IT_Tasks_Complete_Flag
,Technology_Work_Notes
,Telephone_Notes
,To_Building_Floor_Code
,To_Cost_code
,To_Cost_Allocation
,To_Printers
,To_Room_ID
,To_Servers
,cast(Update_Employee_Info_Flag as int) Update_Employee_Info_Flag
,User_ID
,cast(C_DATE01 as timestamp) C_DATE01
,cast(C_DATE02 as timestamp) C_DATE02
,cast(C_DATE03 as timestamp) C_DATE03
,cast(C_DATE04 as timestamp) C_DATE04
,cast(C_FLAG01 as int)  C_FLAG01
,cast(C_FLAG02 as int)  C_FLAG02
,cast(C_FLAG03 as int)  C_FLAG03
,cast(C_FLAG04 as int)  C_FLAG04
,C_LOOKUP01
,C_LOOKUP02
,C_LOOKUP03
,C_LOOKUP04
,cast(C_NUM01 as decimal(10,2) ) C_NUM01
,cast(C_NUM02 as decimal(10,2) ) C_NUM02
,cast(C_NUM03 as decimal(10,2) ) C_NUM03
,cast(C_NUM04 as decimal(10,2) ) C_NUM04
,C_TEXT01
,C_TEXT02
,C_TEXT03
,C_TEXT04
,C_TEXT05
,C_TEXT06
,C_TEXT07
,C_TEXT08
,C_TEXT09
,C_TEXT10
,C_TEXT11
,C_TEXT12
,Posted_Date
from _final
WHERE C_ID = '7745730'
 
 """)

# COMMAND ----------

# DBTITLE 1,ssdv_vw_FMS_Building , Version: 1.0, Created_By : Sanjay Gupta
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_FMS_Building as

select
 C_ID
,OneView_Client_Property_ID
,Alternate_Property_Name
,Area_Type
,Unit_of_Measure
,Assignable_Area
,Building_Code
,Building_Condition
,Building_Description
,Building_Status
,Building_Type
,Boma_Standard
,Book_Value
,City
,Comments
,Cons_Date
,Country
,COUNTY
,CREATED_BY
,CREATED_ON
,Currency_Type
,District
,Firm_Region
,Gross_Area
,Industry
,Benchmarking_Flag
,Charge_Out_Flag
,SSFMove_Restrict_Flag
,Latitude
,Longitude
,Service_Level
,Market
,Metro
,MODIFIED_BY
,MODIFIED_ON
,Mortgage_Data
,Op_Exp
,Ownership
,Owning_Organization
,Polylined
,Purchase_Price
,Property_Sub_Type
,Property_Type
,Rate
,Region_ID
,Rentable_Area
,Rentable_Area_by_Lease
,RENTLC
,Site_Code
,State
,Status
,Strategy
,Address
,Address_2
,Address_3
,Address_4
,Time_Zone
,URL
,Usable_Area
,Market_Value
,Zip_Code
,Benchmarking_Geography
,Benchmarking_Name
,Benchmarking_Use_Type
,C_DATE01
,C_DATE02
,C_DATE03
,C_DATE04
,C_DATE05
,C_DATE06
,C_DATE07
,C_DATE08
,C_DATE09
,C_DATE10
,C_FLAG01
,C_FLAG02
,C_FLAG03
,C_FLAG04
,C_FLAG05
,C_FLAG06
,C_FLAG07
,C_FLAG08
,C_FLAG09
,C_FLAG10
,C_LOOKUP01
,C_LOOKUP02
,C_LOOKUP03
,C_LOOKUP04
,C_LOOKUP05
,C_LOOKUP06
,C_LOOKUP07
,C_LOOKUP08
,C_LOOKUP09
,C_LOOKUP10
,C_NUM01
,C_NUM02
,C_NUM03
,C_NUM04
,C_NUM05
,C_NUM06
,C_NUM07
,C_NUM08
,C_NUM09
,C_NUM10
,C_TEXT01
,C_TEXT02
,C_TEXT03
,C_TEXT04
,C_TEXT05
,C_TEXT06
,C_TEXT07
,C_TEXT08
,C_TEXT09
,C_TEXT10
from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.mart_fms_vw_buildings
WHERE C_ID = '7745730'

 """)

# COMMAND ----------

# DBTITLE 1,ssdv_vw_FMS_Client ,Version: 1.0, Created_By : Sanjay Gupta
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_FMS_Client as

with _final as (
select
 cast(C_ID as int) C_ID
,C_NAME as Client_Name
,INDUSTRY as INDUSTRY
,cast(C_DATE01 as timestamp) C_DATE01
,cast(C_DATE02 as timestamp) C_DATE02
,cast(C_DATE03 as timestamp) C_DATE03
,cast(C_FLAG01 as int) C_FLAG01
,cast(C_FLAG02 as int) C_FLAG02
,cast(C_FLAG03 as int) C_FLAG03
,C_LOOKUP01 
,C_LOOKUP02
,C_LOOKUP03
,cast(C_NUM01  as decimal(10,2)) C_NUM01 
,cast(C_NUM02  as decimal(10,2))  C_NUM02
,cast(C_NUM03   as decimal(10,2)) C_NUM03
,C_TEXT01
,C_TEXT02
,C_TEXT03
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_client_info
WHERE C_ID = '7745730'
)

select
	   C_ID
,Client_Name
,INDUSTRY
,C_DATE01
,C_DATE02
,C_DATE03
,C_FLAG01
,C_FLAG02
,C_FLAG03
,C_LOOKUP01
,C_LOOKUP02
,C_LOOKUP03
,C_NUM01
,C_NUM02
,C_NUM03
,C_TEXT01
,C_TEXT02
,C_TEXT03
from _final


 """)

# COMMAND ----------

# DBTITLE 1,ssdv_vw_FMS_MAC_Active , Version: 1.0, Created_By : Sanjay Gupta
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_FMS_MAC_Active as

with _final as (

select
 C_ID
,MOVEID  as Move_ID
,A0ERGO   as Ergonomic_Requirement
,ACTUALMOVE   as Actual_Move_Date
,APPRBY  as Approved_By
,APPRDATE  as Approved_Date
,BOXES   as Boxes_Required_Flag
,CATTABLE   as Catalog_Table
,COMPLCOMP as Complete_Computer
,COMPLFAC  as Complete_Facilities
,COMPLTEL   as Complete_Telephone
,COMPNOTES  as Computer_Notes
,CONN_RQMTS  as Connectivity_Requirements
,COST_EST  as Estimated_Cost
,DATERCVD  as Date_Received
,EMAIL  as Email
,FAX  as Fax_Phone_Number
,FMCOSTFLAG  as from_Cost_Allocation
,FRCOSTCODE  as from_Cost_Code
,fromPRTRS  as from_Printers
,fromRMID  as from_Room_ID
,fromSRVRS  as from_Servers
,FURN_RQD  as Furniture_Change_Flag
,FRCOSTCODE  as Furniture_Notes
,FURNTASKS  as Furniture_Tasks_Complete_Flag
,GROUP_ as Department
,IT_SVC_RQD  as IT_Services_Required
,IT_WO  as IT_Work_Order_Number
,KEY_LOCKID  as Lock_ID
,KEY_RQD  as Key_Request_Flag
,KEY_TYPE  as Key_Type
,LAN  as LAN_Connection_Flag
,LANPRTR  as LAN_Printer_Flag
,LOCKMOVE as Lock_Move_Flag
,MODEM  as Modem_Phone_Number
,MOVECOST  as Move_Cost
,MOVEFAX  as Move_Fax_Flag
,MOVEMODEM  as Move_Modem_Flag
,MOVEPC  as Move_PC_Flag
,MOVEPHONE  as Move_Phone_Instrmnt_Flag
,MOVEPROJ  as Move_Project
,MOVEREASON  as Move_Reason
,MOVEREASON  as Move_Reason_Description
,MOVETIME  as Scheduled_Move_Time
,MOVETYPE as Move_Type
,DESCRIP  as Move_Type_Description
,MRTYPE  as Move_Item_Type
,NEWEMPFLAG  as New_Employee_Flag
,NEWPHONE  as New_Phone
,NOTES  as Notes
,PHONE  as Phone_Number
,PMVID  as Parent_Move_ID
,QTY  as Quantity
,REQBYID as Requestor_ID
,REQBYNAME as Requestor_Name
,REQDATE  as Requested_Move_Date
,REQEMAIL as Requestor_Email
,REQFORID  as Employee_ID
,REQFORNAME  as Employee_to_Move
,REQPHONE  as Requestor_Phone
,REQPHONE2   as Requestor_Alternate_Phone
,RQDESC   as Asset_Description
,SCHEDMOVE  as Move_Date
,SPCLNEEDS  as Special_Needs
,STATUS  as Status
,STATUSDESC  as Status_Desc
,STATUSNOTE  as Status_Notes
,TAGNO   as Asset_Tag_Number
,TECH_CDATE  as IT_Tasks_Completion_Date
,TECH_COMPL  as IT_Tasks_Complete_Flag
,TECH_NOTES  as Technology_Work_Notes
,TELNOTES  as Telephone_Notes
,TOBLDGFLR   as To_Building_Floor_Code
,TOCOSTCODE  as To_Cost_code
,TOCOSTFLAG  as To_Cost_Allocation
,TOPRTRS  as To_Printers
,TORMID  as To_Room_ID
,TOSRVRS  as To_Servers
,UPDEMPFLAG  as Update_Employee_Info_Flag
,User_ID  as User_ID
,C_DATE01
,C_DATE02
,C_DATE03
,C_DATE04
,C_FLAG01
,C_FLAG02
,C_FLAG03
,C_FLAG04
,C_LOOKUP01
,C_LOOKUP02
,C_LOOKUP03
,C_LOOKUP04
,C_NUM01
,C_NUM02
,C_NUM03
,C_NUM04
,C_TEXT01
,C_TEXT02
,C_TEXT03
,C_TEXT04
,C_TEXT05
,C_TEXT06
,C_TEXT07
,C_TEXT08
,C_TEXT09
,C_TEXT10
,C_TEXT11
,C_TEXT12  
,NULL  as C_TEXT25
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_future_moves    
)

select
 cast(C_ID as int) C_ID
,Move_ID
,Ergonomic_Requirement
,cast(Actual_Move_Date as timestamp) Actual_Move_Date
,Approved_By
,cast(Approved_Date as timestamp)  Approved_Date
,cast((case when Boxes_Required_Flag =TRUE THEN 1 else 0 end ) as integer)  Boxes_Required_Flag
,case when ltrim(rtrim(Catalog_Table)) ='' then null else Catalog_Table end  Catalog_Table
,Complete_Computer
,Complete_Facilities
,Complete_Telephone
,Computer_Notes
,Connectivity_Requirements
,cast(Estimated_Cost as integer) Estimated_Cost
,cast(Date_Received as timestamp)  Date_Received
,Email
,Fax_Phone_Number
,from_Cost_Allocation
,from_Cost_Code
,from_Printers
,from_Room_ID
,from_Servers
,Furniture_Change_Flag
,Furniture_Notes
,cast(Furniture_Tasks_Complete_Flag as int) Furniture_Tasks_Complete_Flag
,Department
,IT_Services_Required
,IT_Work_Order_Number
,Lock_ID
,Key_Request_Flag
,Key_Type
,cast((case when LAN_Connection_Flag =TRUE THEN 1 else 0 end ) as integer)  LAN_Connection_Flag
,cast((case when LAN_Printer_Flag =TRUE THEN 1 else 0 end ) as integer)   LAN_Printer_Flag
,cast((case when Lock_Move_Flag =TRUE THEN 1 else 0 end ) as integer)   Lock_Move_Flag
,Modem_Phone_Number
,cast(Move_Cost as float) Move_Cost
,cast((case when Move_Fax_Flag =TRUE THEN 1 else 0 end ) as integer)  Move_Fax_Flag
,cast((case when Move_Modem_Flag =TRUE THEN 1 else 0 end ) as integer)    Move_Modem_Flag
,cast((case when Move_PC_Flag =TRUE THEN 1 else 0 end ) as integer)    Move_PC_Flag
,cast((case when Move_Phone_Instrmnt_Flag =TRUE THEN 1 else 0 end ) as integer)    Move_Phone_Instrmnt_Flag
,Move_Project
,Move_Reason
,Move_Reason_Description
,Scheduled_Move_Time
,Move_Type
,Move_Type_Description
--- ,Move_Item_Type
,case when ltrim(rtrim(Move_Item_Type)) ='' then null else Move_Item_Type end  Move_Item_Type 
,cast((case when New_Employee_Flag =TRUE THEN 1 else 0 end ) as integer)   New_Employee_Flag
,New_Phone
,Notes
,Phone_Number
,Parent_Move_ID
,cast(Quantity as int) Quantity
,Requestor_ID
,Requestor_Name
,cast(Requested_Move_Date as timestamp) Requested_Move_Date
,Requestor_Email
,Employee_ID
,Employee_to_Move
,Requestor_Phone
,Requestor_Alternate_Phone
,Asset_Description
,cast(Move_Date as timestamp)  Move_Date
,Special_Needs
,case when ltrim(rtrim(Status)) ='' then null else Status end  Status 
,Status_Desc
,Status_Notes
,Asset_Tag_Number
,cast(IT_Tasks_Completion_Date as timestamp) IT_Tasks_Completion_Date
,cast((case when IT_Tasks_Complete_Flag =TRUE THEN 1 else 0 end ) as integer)   IT_Tasks_Complete_Flag
,Technology_Work_Notes
,Telephone_Notes
,To_Building_Floor_Code
,To_Cost_code
,To_Cost_Allocation
,To_Printers
,To_Room_ID
,To_Servers
,cast((case when Update_Employee_Info_Flag =TRUE THEN 1 else 0 end ) as integer)  Update_Employee_Info_Flag
,User_ID
,cast(C_DATE01 as timestamp) C_DATE01
,cast(C_DATE02 as timestamp) C_DATE02
,cast(C_DATE03 as timestamp) C_DATE03
,cast(C_DATE04 as timestamp) C_DATE04
,cast((case when C_FLAG01 =TRUE THEN 1 else 0 end ) as integer) C_FLAG01 
,cast((case when C_FLAG02 =TRUE THEN 1 else 0 end ) as integer) C_FLAG02
,cast((case when C_FLAG03 =TRUE THEN 1 else 0 end ) as integer) C_FLAG03
,cast((case when C_FLAG04 =TRUE THEN 1 else 0 end ) as integer) C_FLAG04
,C_LOOKUP01
,C_LOOKUP02
,C_LOOKUP03
,C_LOOKUP04
,cast(C_NUM01 as decimal(10,2)) C_NUM01
,cast(C_NUM02 as decimal(10,2)) C_NUM02
,cast(C_NUM03 as decimal(10,2)) C_NUM03
,cast(C_NUM04 as decimal(10,2)) C_NUM04
,case when ltrim(rtrim(C_TEXT01)) ='' then null else C_TEXT01 end  C_TEXT01 
,case when ltrim(rtrim(C_TEXT02)) ='' then null else C_TEXT02 end  C_TEXT02
,case when ltrim(rtrim(C_TEXT03)) ='' then null else C_TEXT03 end  C_TEXT03
,case when ltrim(rtrim(C_TEXT04)) ='' then null else C_TEXT04 end  C_TEXT04
,case when ltrim(rtrim(C_TEXT05)) ='' then null else C_TEXT05 end  C_TEXT05
,case when ltrim(rtrim(C_TEXT06)) ='' then null else C_TEXT06 end  C_TEXT06
,case when ltrim(rtrim(C_TEXT07)) ='' then null else C_TEXT07 end  C_TEXT07
,case when ltrim(rtrim(C_TEXT08)) ='' then null else C_TEXT08 end  C_TEXT08
,case when ltrim(rtrim(C_TEXT09)) ='' then null else C_TEXT09 end  C_TEXT09
,case when ltrim(rtrim(C_TEXT10)) ='' then null else C_TEXT10 end  C_TEXT10
,case when ltrim(rtrim(C_TEXT11)) ='' then null else C_TEXT11 end  C_TEXT11
,case when ltrim(rtrim(C_TEXT12)) ='' then null else C_TEXT12 end  C_TEXT12  
,case when ltrim(rtrim(C_TEXT25)) ='' then null else C_TEXT25 end  C_TEXT25                          
from _final
WHERE C_ID = '7745730'
 """)



# COMMAND ----------

# DBTITLE 1,ssdv_vw_FMS_Move_Comment_History : Version: 1.0, Created_By : Sanjay Gupta
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_FMS_Move_Comment_History as

with _final as (
  
select 
 C_ID
,Z8_KEY AS Z8_Key
,CDATE AS Comment_Date
,COMMENTS AS Comments
,DESCRIP AS Description
,EMPID AS Employee_ID     
,MOVEID AS Move_ID
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_move_comments
)

select
 cast(C_ID as int) C_ID
,Z8_Key
,cast(Comment_Date as timestamp) Comment_Date
,Comments
,Description
,Employee_ID
,Move_ID
from _final
WHERE C_ID = '7745730'

""")

# COMMAND ----------

# DBTITLE 1,ssdv_vw_fms_space_employee, Version: 1.0, Created_By : Sanjay Gupta
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_fms_space_employee as


with _final as (
SELECT a.C_ID 
,b.OVCP_ID as OneView_Client_Property_ID 
,a.BLDGRMID as Building_Room_ID 
,a.EMPID Employee_ID 
,a.PRIMARYRM as Primary_Room 
,a.BLDGCODE as Building_Code 
,a.FLOORCODE as Floor_Code 
,a.RMID as Room_ID 
,a.CREATEDBY as CREATED_BY 
,a.CREATEDON as CREATED_ON 
,a.MODIFIEDBY as MODIFIED_BY 
,a.MODIFIEDON as MODIFIED_ON 
,a.C_DATE01 
,a.C_DATE02 
,a.C_DATE03 
,a.C_FLAG01 
,a.C_FLAG02 
,a.C_FLAG03 
,a.C_LOOKUP01 
,a.C_LOOKUP02 
,a.C_LOOKUP03 
,b.C_NUM01 as C_NUM01 
,b.C_NUM02 as C_NUM02 
,b.C_NUM03 as C_NUM03 
,a.C_TEXT01 
,a.C_TEXT02 
,a.C_TEXT03
FROM      jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_seat_assignments a
     join jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_buildings        b ON a.C_ID = b.C_ID and b.BLDGCODE = a.BLDGCODE
)

SELECT 
 cast(C_ID as int) C_ID 
,cast(OneView_Client_Property_ID as int) OneView_Client_Property_ID 
,Building_Room_ID 
,Employee_ID 
,cast((case when Primary_Room =TRUE THEN 1 else 0 end ) as integer) Primary_Room  
,Building_Code 
,Floor_Code 
,Room_ID 
,CREATED_BY 
,cast(CREATED_ON as timestamp) CREATED_ON 
,MODIFIED_BY 
,cast(MODIFIED_ON as timestamp) MODIFIED_ON 
,cast(C_DATE01 as timestamp) C_DATE01 
,cast(C_DATE02 as timestamp) C_DATE02 
,cast(C_DATE03 as timestamp) C_DATE03 
,cast((case when C_FLAG01 =TRUE THEN 1 else 0 end ) as integer) C_FLAG01 
,cast((case when C_FLAG02 =TRUE THEN 1 else 0 end ) as integer) C_FLAG02
,cast((case when C_FLAG03 =TRUE THEN 1 else 0 end ) as integer) C_FLAG03
,C_LOOKUP01 
,C_LOOKUP02 
,C_LOOKUP03 
,cast(C_NUM01 as decimal(10,2)) C_NUM01 
,cast(C_NUM02 as decimal(10,2)) C_NUM02 
,cast(C_NUM03 as decimal(10,2)) C_NUM03 
,case when ltrim(rtrim(C_TEXT01)) ='' then null else C_TEXT01 end  C_TEXT01 
,case when ltrim(rtrim(C_TEXT02)) ='' then null else C_TEXT02 end  C_TEXT02
,case when ltrim(rtrim(C_TEXT03)) ='' then null else C_TEXT03 end  C_TEXT03

FROM _final
WHERE C_ID = '7745730'

""")


# COMMAND ----------

# DBTITLE 1,ssdv_vw_FMS_Space_FMS,Version: 1.0, Created_By : Kritisha Harsh
spark.sql("""

create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_FMS_Space_FMS  as

SELECT
 cast(s.C_ID as int) AS C_ID
,cast(b.OVCP_ID as int) AS	OneView_Client_Property_ID  
,cast(s.area as decimal(18,7))   AS   	Area                
,cast(s.ASSIGNABLE as decimal(18,4)) AS	Assignable_Area     
,cast(s.BLDGCODE as varchar(30)) AS  	Building_Code                
,cast(s.capacity as int) AS  	Capacity            
,cast(s.createdby as varchar(256)) AS	CREATED_BY
,cast(s.createdon as timestamp) AS 	CREATED_ON 
,cast(s.floorcode as varchar(30)) AS 	Floor_Code               
,cast(s.gross as decimal(18,4)) AS	Gross_Area
,cast(( case when ltrim(rtrim(s.GROUPX)) ='' then null else s.GROUPX end) as varchar(1)) AS   	Group_Exception     
,cast((case when ltrim(rtrim(s.GROUPX_MV)) ='' then null else s.GROUPX_MV end) as varchar(1))  AS	Group_Exception_Move_Posting
,cast((case when s.isutilized =TRUE THEN 1 else 0 end )as int) AS	Utilized_Flag       
,cast(s.mailstop as varchar(100))   AS	Mailstop            
,cast(s.MAN_OCC as decimal(4,0)) AS   	Manual_Occupancy    
,cast(s.MEETCAP  as decimal(10,0))  AS 	Meeting_Room_Capacity                   
,cast(s.modifiedby as varchar(256))	 AS ASMODIFIED_BY
,cast(s.modifiedon as timestamp) AS	MODIFIED_ON
,cast(s.NONWK_CAP as decimal(4,0))  AS	Non_workspace_Capacity                  
,cast(s.occ as int)   AS    	Occupancy           
,cast(s.rentable as decimal(18,4))  AS	Rentable_Area       
,cast(s.rmid as varchar(30))     AS 	Room_ID              
,cast(s.rminfo as varchar(120))     AS	Notes               
,cast(s.rmname  as varchar(100))  AS 	Room_Name           
,cast(s.spacetype as varchar(30))  AS	Space_Type          
,cast(s.spcode as varchar(30))   AS 	Space_Standard_Code         
,cast(s.unitstatus as varchar(100))	 AS Unit_Status
,cast(s.usable as decimal(10,4))    AS 	Usable_Area  
,cast(s.C_DATE01 as timestamp) AS C_DATE01  
,cast(s.C_DATE02 as timestamp) AS C_DATE02 
,cast(s.C_DATE03 as timestamp) AS C_DATE03  
,cast((case when s.C_FLAG01 =TRUE THEN 1 else 0 end ) as integer) as C_FLAG01
,cast((case when s.C_FLAG02 =TRUE THEN 1 else 0 end ) as integer) as C_FLAG02
,cast((case when s.C_FLAG03 =TRUE THEN 1 else 0 end ) as integer) as C_FLAG03
,cast(s.C_LOOKUP01 as varchar(50)) AS C_LOOKUP01
,cast(s.C_LOOKUP02 as varchar(50)) AS C_LOOKUP02
,cast(s.C_LOOKUP03 as varchar(50)) AS C_LOOKUP03
,cast(s.C_NUM01 as decimal(10,2)) AS C_NUM01   
,cast(s.C_NUM02 as decimal(10,2)) AS C_NUM02   
,cast(s.C_NUM03 as decimal(10,2)) AS C_NUM03  
,cast(s.C_TEXT01 as varchar(200)) AS C_TEXT01  
,cast(s.C_TEXT02 as varchar(200)) AS C_TEXT02  
,cast(s.C_TEXT03 as varchar(200)) AS C_TEXT03   
 --,cast(s.vertices as string)      as    vertices             
from           jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_rooms s
     left join jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_buildings b on b.C_ID = s.C_ID and b.BLDGCODE = s.BLDGCODE
WHERE S.C_ID = '7745730'



""")


# COMMAND ----------

# DBTITLE 1,ssdv_vw_fms_Floor, Version: 1.0, Created_By : Kritisha Harsh

spark.sql("""
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_fms_Floor as		
	
SELECT
 cast(f.C_ID as int) AS C_ID
,cast(b.ovcp_id as int) AS	OneView_Client_Property_ID      
,cast(f.AREA as float) AS     	Ext_Gross_Area     
,cast((case when ltrim(rtrim(f.areaunit)) ='' then null else f.areaunit end) as varchar(50)) As	Area_Unit 
,cast(f.assignable as Decimal(10,4)) Assignable_Area 
,cast(f.BLDGCODE as varchar(30)) AS 	Building_Code         
,cast((case when ltrim(rtrim(f.createdby)) ='' then null else f.createdby end) as varchar(256)) As CREATED_BY
,cast(f.createdon as timestamp) AS CREATED_ON 
,cast(f.floorcode as varchar(4)) AS	Floor_Code               
,cast(f.floordesc as varchar(60)) AS	Floor_Description         
,cast(f.gross as decimal(10,4)) AS	Gross_Area
,cast(f.INTGROSS as float)	AS Interior_Gross 
,cast((case when f.ISCHGOUT =TRUE THEN 1 else 0 end ) as Int)	AS Charge_Out_Flag 
,cast((case when f.ISNOTMOVE =TRUE THEN 1 else 0 end ) as Int)	AS SSFMove_Restrict_Flag  
,cast((case when ltrim(rtrim(f.LVLOFSVC)) ='' then null else f.LVLOFSVC end) as varchar(100)) As Service_Level
,cast(( case when ltrim(rtrim(f.modifiedby)) ='' then null else f.modifiedby end) as varchar(256)) AS	MODIFIED_BY
,cast(f.modifiedon as timestamp) AS MODIFIED_ON
,cast(f.optn as varchar(4))  AS   	Option                
,cast(f.Rarea as decimal(10,1))   AS  	R_Area
,cast(f.rentable as decimal(10,4))	AS Rentable_Area 
,cast(f.RENTAREA as float)  AS	Rentable_Area_by_Ls                     
,cast(f.RENTLC as varchar(1))    AS	Area_by_Lease_Calc                      
,cast(f.rentrate as decimal(18,6))	AS Rent_Rate 
,cast(f.STATUS as varchar(10)) AS	Floor_Status
,cast(f.USABLE as decimal(10,4))	AS Usable_Area 
,cast(f.c_date01 as timestamp) AS C_DATE01  
,cast(f.c_date02 as timestamp) AS C_DATE02  
,cast(f.c_date03 as timestamp) AS C_DATE03  
,cast((case when f.c_flag01 =TRUE THEN 1 else 0 end ) as Int) AS C_FLAG01  
,cast((case when f.c_flag02 =TRUE THEN 1 else 0 end ) as Int) AS C_FLAG02  
,cast((case when f.c_flag03 =TRUE THEN 1 else 0 end ) as Int) AS C_FLAG03  
,cast(f.c_lookup01 as varchar(50)) AS C_LOOKUP01
,cast(f.c_lookup02 as varchar(50)) AS C_LOOKUP02
,cast(f.c_lookup03 as varchar(50)) AS C_LOOKUP03
,cast(f.c_num01 as decimal(10,2)) AS C_NUM01   
,cast(f.c_num02 as decimal(10,2)) AS C_NUM02   
,cast(f.c_num03 as decimal(10,2)) AS C_NUM03    
,cast((case when ltrim(rtrim(f.c_text01)) ='' then null else f.c_text01 end) as varchar(800)) as C_TEXT01
,cast((case when ltrim(rtrim(f.c_text02)) ='' then null else f.c_text02 end) as varchar(800)) as C_TEXT02  
,cast((case when ltrim(rtrim(f.c_text03)) ='' then null else f.c_text03 end) as varchar(800)) as C_TEXT03

from            jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_floors    f
     left join	jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_buildings b on trim(b.BLDGCODE) = trim(f.BLDGCODE) and trim(b.C_ID) = trim(f.C_ID)
WHERE f.C_ID = '7745730'
 
 """) 





# COMMAND ----------

# DBTITLE 1,ssdv_vw_FMS_Lookup,  Version: 1.0, Created_By : Kritisha Harsh
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_FMS_Lookup as
select 
 cast(C_ID as int)
,cast(Z1_Key as varchar(10))
,cast(TYPE as varchar(20)) AS Lookup_Type
,cast(val as varchar(100)) AS Value
,cast(descrip as varchar(100)) AS Description   
,cast(ISINACTIVE as boolean) AS Archived_Flag  
,cast((case when ltrim(rtrim(modifiedby)) ='' then null else modifiedby end) as varchar(256)) AS Modified_By
,cast(modifiedon as timestamp) AS Modified_On
,cast((case when ltrim(rtrim(createdby)) ='' then null else createdby end) as varchar(256)) AS Created_By
,cast(createdon as timestamp) AS Created_On
,cast(C_DATE01 as timestamp)  
,cast(C_DATE02 as timestamp) 
,cast(C_DATE03 as timestamp) 
,cast(C_FLAG01 as boolean)
,cast(C_FLAG02 as boolean) 
,cast(C_FLAG03  as boolean)
,cast(C_LOOKUP01 as varchar(50))
,cast(C_LOOKUP02 as varchar(50))
,cast(C_LOOKUP03 as varchar(50))
,cast(C_NUM01 as decimal(10,2))  
,cast(C_NUM02  as decimal(10,2))
,cast(C_NUM03 as decimal(10,2))  
,cast((case when ltrim(rtrim(C_TEXT01)) ='' then null else C_TEXT01 end)  as varchar(800)) as C_TEXT01
,cast((case when ltrim(rtrim(C_TEXT02)) ='' then null else C_TEXT02 end)  as varchar(800)) as C_TEXT02
,cast((case when ltrim(rtrim(C_TEXT03)) ='' then null else C_TEXT03 end)   as varchar(800)) as C_TEXT03   
,cast((case when ltrim(rtrim(C_TEXT04)) ='' then null else C_TEXT04 end)  as varchar(800)) as C_TEXT04
,cast((case when ltrim(rtrim(C_TEXT05)) ='' then null else C_TEXT05 end)  as varchar(800)) as C_TEXT05
,cast((case when ltrim(rtrim(C_TEXT06)) ='' then null else C_TEXT06 end)  as varchar(800)) as C_TEXT06  
,cast((case when ltrim(rtrim(C_TEXT07)) ='' then null else C_TEXT07 end) as varchar(800)) as C_TEXT07
,cast((case when ltrim(rtrim(C_TEXT08)) ='' then null else C_TEXT08 end)  as varchar(800)) as C_TEXT08
,cast((case when ltrim(rtrim(C_TEXT09)) ='' then null else C_TEXT09 end)  as varchar(800)) as C_TEXT09
,cast((case when ltrim(rtrim(C_TEXT10)) ='' then null else C_TEXT10 end)  as varchar(800)) as C_TEXT10
,cast((case when ltrim(rtrim(C_TEXT11)) ='' then null else C_TEXT11 end)  as varchar(800)) as C_TEXT11
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_lookup_table
WHERE C_ID = '7745730'
""")

# COMMAND ----------

# DBTITLE 1, ssdv_vw_FMS_Move_Status_History,Version: 1.0, Created_By : Kritisha Harsh
spark.sql("""
          
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_FMS_Move_Status_History as
select 
 cast(C_ID as int)
,cast(Z9_Key as varchar(40))
,cast(MoveID as varchar(20)) as Move_ID
,cast(NOTESLONG as varchar(4000)) as Status_Notes_Long
,cast(NOTESSHORT as varchar(200)) as Status_Notes_Short
,cast(SDATE as timestamp) as Status_Date
,cast(case when ltrim(rtrim(STATUS)) ='' then null else  STATUS end as varchar(1)) as Status_Code
from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_move_status_change
WHERE C_ID = '7745730'
""")

# COMMAND ----------

spark.sql("""
create or replace view jll_azara_catalog.jll_azara_0007642176_bankofamer_custom.ssdv_vw_FMS_Org as

With temp_org as (
select
 cast(C_ID as varchar(15) ) as C_ID	
,ORGCODE  	as	 Org_Code
,case when GROUP_ is null then 'NO ORG ASSIGNED' else GROUP_ end as Organization		
,OrgLevel  as		 Org_Level
,Parent as   Reports_To		
,PRIMARYMGR  as  Primary_Manager		
,FUNC_CODE  as  Functional_Code		
,DATEADDED  as  Date_Added		
,udp_update_ts   as Last_Updated_Date		
,MODIFIEDBY  as  MODIFIED_BY		
,MODIFIEDON  as  MODIFIED_ON		
,CREATEDBY  as  CREATED_BY		
,CREATEDON  as  CREATED_ON		
,GROUP1	 	
,GROUP10	 	
,GROUP11  		
,GROUP12  		
,GROUP13	 	
,GROUP14	 	
,GROUP15	 	
,GROUP16	 	
,GROUP17	 	
,GROUP18	 	
,GROUP19  		
,GROUP2  		
,GROUP20  		
,GROUP21	 	
,GROUP22	 	
,GROUP23	 	
,GROUP24	 	
,GROUP25	 	
,GROUP3	 	
,GROUP4  		
,GROUP5  		
,GROUP6  		
,GROUP7  		
,GROUP8  		
,GROUP9  		
,GROUPDES10	 	
,GROUPDES11  		
,GROUPDES12   		
,GROUPDES13	 	
,GROUPDES14  		
,GROUPDES15  		
,GROUPDES16	  	
,GROUPDES17	 	
,GROUPDES18	 	
,GROUPDES19  		
,GROUPDES20  		
,GROUPDES21  		
,GROUPDES22	 	
,GROUPDES23	 	
,GROUPDES24	 	
,GROUPDES25	 	
,GROUPDESC 	
,GROUPDESC1	 	
,GROUPDESC2	 	
,GROUPDESC3	 	
,GROUPDESC4	 	
,GROUPDESC5  		
,GROUPDESC6	 	
,GROUPDESC7	 	
,GROUPDESC8	 	
,GROUPDESC9	 	
,C_DATE01  		
,C_DATE02  		
,C_DATE03  		
,C_FLAG01  		
,C_FLAG02  		
,C_FLAG03  		
,C_LOOKUP01  		
,C_LOOKUP02	 	
,C_LOOKUP03	 	
,C_NUM01	 	
,C_NUM02	 	
,C_NUM03	 	
,C_TEXT01  		
,C_TEXT02  		
,C_TEXT03  		

from jll_azara_catalog.jll_azara_raw.edp_snapshot_fms_orgs	
WHERE C_ID = '7642176'	

UNION ALL	
select	
 'NO ORG ASSIGNED' AS C_ID	
,'NO ORG ASSIGNED' AS Org_Code	
,'NO ORG ASSIGNED' AS Organization   		
,0  AS 	Org_Level
,'NO ORG ASSIGNED'   AS   	Reports_To                              
,'NO ORG ASSIGNED'  AS	Primary_Manager     
,'NO ORG ASSIGNED'  	 AS Functional_Code     
,getdate() 	 AS Date_Added                 
,getdate() AS	Last_Updated_Date 
,'NO ORG ASSIGNED' 	 AS MODIFIED_BY
,getdate() 	 AS MODIFIED_ON  
,'NO ORG ASSIGNED'  AS	CREATED_BY
,getdate()  AS	CREATED_ON                            
,'NO ORG ASSIGNED'  AS	GROUP1    
,'NO ORG ASSIGNED'  AS	GROUP10   
,'NO ORG ASSIGNED'  AS	GROUP11   
,'NO ORG ASSIGNED'  AS	GROUP12   
,'NO ORG ASSIGNED'  AS	GROUP13   
,'NO ORG ASSIGNED'  AS	GROUP14   
,'NO ORG ASSIGNED'  AS	GROUP15   
,'NO ORG ASSIGNED'  AS	GROUP16   
,'NO ORG ASSIGNED'  AS	GROUP17   
,'NO ORG ASSIGNED'  AS	GROUP18   
,'NO ORG ASSIGNED'  AS	GROUP19   
,'NO ORG ASSIGNED'  AS	GROUP2    
,'NO ORG ASSIGNED'  AS	GROUP20   
,'NO ORG ASSIGNED'  AS	GROUP21   
,'NO ORG ASSIGNED'  AS	GROUP22   
,'NO ORG ASSIGNED'  AS	GROUP23   
,'NO ORG ASSIGNED'  AS	GROUP24   
,'NO ORG ASSIGNED'  AS	GROUP25   
,'NO ORG ASSIGNED'  AS	GROUP3    
,'NO ORG ASSIGNED'  AS	GROUP4    
,'NO ORG ASSIGNED'  AS	GROUP5    
,'NO ORG ASSIGNED'  AS	GROUP6    
,'NO ORG ASSIGNED'  AS	GROUP7    
,'NO ORG ASSIGNED'  AS	GROUP8    
,'NO ORG ASSIGNED'  AS	GROUP9    
,'NO ORG ASSIGNED'  AS	GROUPDES10
,'NO ORG ASSIGNED'  AS	GROUPDES11
,'NO ORG ASSIGNED'  AS	GROUPDES12
,'NO ORG ASSIGNED'  AS	GROUPDES13
,'NO ORG ASSIGNED'  AS	GROUPDES14
,'NO ORG ASSIGNED'  AS	GROUPDES15
,'NO ORG ASSIGNED'  AS	GROUPDES16
,'NO ORG ASSIGNED'  AS	GROUPDES17
,'NO ORG ASSIGNED'  AS	GROUPDES18
,'NO ORG ASSIGNED'  AS	GROUPDES19
,'NO ORG ASSIGNED'  AS	GROUPDES20
,'NO ORG ASSIGNED'  AS	GROUPDES21
,'NO ORG ASSIGNED'  AS	GROUPDES22
,'NO ORG ASSIGNED'  AS	GROUPDES23
,'NO ORG ASSIGNED'  AS	GROUPDES24
,'NO ORG ASSIGNED'  AS	GROUPDES25
,'NO ORG ASSIGNED'  AS	GROUPDESC 
,'NO ORG ASSIGNED'  AS	GROUPDESC1
,'NO ORG ASSIGNED'  AS	GROUPDESC2
,'NO ORG ASSIGNED'  AS	GROUPDESC3
,'NO ORG ASSIGNED'  AS	GROUPDESC4
,'NO ORG ASSIGNED'  AS	GROUPDESC5
,'NO ORG ASSIGNED'  AS	GROUPDESC6
,'NO ORG ASSIGNED'  AS	GROUPDESC7
,'NO ORG ASSIGNED'  AS	GROUPDESC8
,'NO ORG ASSIGNED'  AS	GROUPDESC9                  
,getdate() AS	C_DATE01  
,getdate()  AS	C_DATE02  
,getdate()  ASC_DATE03  	
,0 AS C_FLAG01  	
,0 AS C_FLAG02  	
,0 AS C_FLAG03  	
,'NO ORG ASSIGNED'  AS	C_LOOKUP01
,'NO ORG ASSIGNED'  AS	C_LOOKUP02
,'NO ORG ASSIGNED'  AS	C_LOOKUP03
,0 AS C_NUM01   	
,0 AS C_NUM02   	
,0 AS C_NUM03   	
,'NO ORG ASSIGNED' AS C_TEXT01  	
,'NO ORG ASSIGNED' AS C_TEXT02  	
,'NO ORG ASSIGNED' AS C_TEXT03 )
 
 select cast(C_ID as varchar(15) ) as C_ID	
,cast(Org_Code as varchar(50))		as Org_Code
,cast(Organization as varchar(50))as Organization		
,cast(Org_Level as int)		as Org_Level
,cast(Reports_To as varchar(50)) as Reports_To		
,cast(Primary_Manager as varchar(100)) as Primary_Manager		
,cast(Functional_Code as varchar(15)) as Functional_Code		
,cast(Date_Added as timestamp) as Date_Added		
,cast(Last_Updated_Date as timestamp) as Last_Updated_Date		
,cast(MODIFIED_BY as varchar(256)) as MODIFIED_BY		
,cast(MODIFIED_ON as timestamp) as MODIFIED_ON		
,cast(CREATED_BY as varchar(256)) as CREATED_BY		
,cast(CREATED_ON as timestamp) as CREATED_ON		
,cast(GROUP1	as varchar(50))	
,cast(GROUP10	as varchar(50))	
,cast(GROUP11 as varchar(50))		
,cast(GROUP12 as varchar(50))		
,cast(GROUP13	as varchar(50))	
,cast(GROUP14	as varchar(50))	
,cast(GROUP15	as varchar(50))	
,cast(GROUP16	as varchar(50))	
,cast(GROUP17	as varchar(50))
,cast(GROUP18	as varchar(50))	
,cast(GROUP19 as varchar(50))		
,cast(GROUP2 as varchar(50))		
,cast(GROUP20 as varchar(50))		
,cast(GROUP21	as varchar(50))	
,cast(GROUP22	as varchar(50))	
,cast(GROUP23	as varchar(50))	
,cast(GROUP24	as varchar(50))	
,cast(GROUP25	as varchar(50))	
,cast(GROUP3	as varchar(50))	
,cast(GROUP4 as varchar(50))		
,cast(GROUP5 as varchar(50))		
,cast(GROUP6 as varchar(50))	
,cast(GROUP7 as varchar(50))	
,cast(GROUP8 as varchar(50))		
,cast(GROUP9 as varchar(50))		
,cast(GROUPDES10	as varchar(240))	
,cast(GROUPDES11 as varchar(240))		
,cast(GROUPDES12  as varchar(240))		
,cast(GROUPDES13	as varchar(240))	
,cast(GROUPDES14 as varchar(240))		
,cast(GROUPDES15 as varchar(240))		
,cast(GROUPDES16	as varchar(240)) 	
,cast(GROUPDES17	as varchar(240))	
,cast(GROUPDES18	as varchar(240))	
,cast(GROUPDES19 as varchar(240))		
,cast(GROUPDES20 as varchar(240))		
,cast(GROUPDES21 as varchar(240))		
,cast(GROUPDES22	as varchar(240))	
,cast(GROUPDES23	as varchar(240))	
,cast(GROUPDES24	as varchar(240))	
,cast(GROUPDES25	as varchar(240))	
,cast(GROUPDESC	as varchar(240))	
,cast(GROUPDESC1	as varchar(240))	
,cast(GROUPDESC2	as varchar(240))	
,cast(GROUPDESC3	as varchar(240))	
,cast(GROUPDESC4	as varchar(240))	
,cast(GROUPDESC5 as varchar(240))		
,cast(GROUPDESC6	as varchar(240))	
,cast(GROUPDESC7	as varchar(240))	
,cast(GROUPDESC8	as varchar(240))	
,cast(GROUPDESC9	as varchar(240))	
,cast(C_DATE01 as timestamp)	
,cast(C_DATE02 as timestamp)		
,cast(C_DATE03 as timestamp)		
,cast(C_FLAG01 as timestamp)		
,cast(C_FLAG02 as timestamp)		
,cast(C_FLAG03 as timestamp)		
,cast(C_LOOKUP01 as varchar(50))		
,cast(C_LOOKUP02	as varchar(50))	
,cast(C_LOOKUP03	as varchar(50))	
,cast(C_NUM01	as decimal(10,2))	
,cast(C_NUM02	as decimal(10,2))	
,cast(C_NUM03	as decimal(10,2))	
,cast(C_TEXT01 as varchar(800))		
,cast(C_TEXT02 as varchar(800))		
,cast(C_TEXT03 as varchar(800))
from temp_org
 


 

""")  	

