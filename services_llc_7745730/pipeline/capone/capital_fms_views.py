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
,to_timestamp(HDATE)            as Hire_Date
,iff(BADGEID = '',null,BADGEID) as Badge_ID
,Company
,EMAIL        
,OCC1             as Staff_Count
,Phone
,iff(POS_ID = '',null,POS_ID) as Position_ID
,iff(PRIMARYLOC = '',null,PRIMARYLOC) as Primary_Location
,NULL             as Use_Standard
,SPCLNEEDS        as Special_Needs
,iff(SPACESTD = '',null,SPACESTD) as Space_Standard
,to_timestamp(TDATE)            as Termination_Date
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
,to_timestamp(CREATEDON)        as CREATED_ON
,to_timestamp(DATEADDED)        as Date_Added
,MODIFIEDBY       as MODIFIED_BY
,to_timestamp(MODIFIEDON)       as MODIFIED_ON
,udp_update_ts    as Last_Updated_Date
,to_timestamp(C_DATE01)    as C_DATE01
,to_timestamp(C_DATE02)    as C_DATE02
,to_timestamp(C_DATE03)    as C_DATE03
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
