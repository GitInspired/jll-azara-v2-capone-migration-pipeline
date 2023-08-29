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

# DBTITLE 1,ref_F55OVCPX_bofa_temp
spark.sql(f"""
create or replace temporary view ref_F55OVCPX_capone_temp as
select * From
    {var_azara_raw_db}.ref_F55OVCPX_bofa 
Where 
    OXY55OVCID = '7745730'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ref_F55OVCPX_capone_temp

# COMMAND ----------

# DBTITLE 1,pdmhub
# spark.sql(f"""
# create or replace temporary view pdmhub as
# SELECT 
#     pdmhub.OXMCU as BU_PROJ_ID, 
#     MAX(pdmhub.OXY55OVCPID) as ovcp_id
# FROM 
#     ref_F55OVCPX_bofa_temp pdmhub 
# JOIN 
#     (SELECT 
#         LTRIM(RTRIM(OXMCU)) as OXMCU, 
#         MAX(UPDATEDT) AS UPDATEDT
# 	FROM 
#         ref_F55OVCPX_bofa_temp
# 	GROUP BY 
#         LTRIM(RTRIM(OXMCU))
# 	) AS pdmhub_bu
#     ON  pdmhub_bu.OXMCU= LTRIM(RTRIM(pdmhub.OXMCU))
#     AND pdmhub_bu.UPDATEDT  = pdmhub.UPDATEDT
# GROUP BY 
#     pdmhub.BU_PROJ_ID
# """)

# COMMAND ----------

# DBTITLE 1,pdmhub
spark.sql(f"""
create or replace temporary view pdmhub as
SELECT 
    pdmhub.OXMCU as BU_PROJ_ID, 
    MAX(pdmhub.OXY55OVCPID) as ovcp_id
FROM 
    ref_F55OVCPX_capone_temp pdmhub
GROUP BY 
    OXMCU
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pdmhub

# COMMAND ----------

# DBTITLE 1,Stage_E1_dbo_tblJDE_AddressBook
spark.sql(f"""
create or replace table {var_client_custom_db}.Stage_E1_dbo_tblJDE_AddressBook as
SELECT 
	 LTRIM(RTRIM(F0006.MCMCU)) AS MCMCU
	,F0101.ABAN8
	,CASE 
        WHEN LEFT(LTRIM(F0006.MCMCU),5) = '11820'
		THEN F0111.WWMLNM
		ELSE F0101.ABALPH
	END AS PropertyName
    ,F0116.ALADD1 AS Address1
    ,F0116.ALADD2 AS Address2
    ,LTRIM(F0116.ALADD3) AS Address3
    ,LTRIM(F0116.ALADD4) AS Address4
    ,F0116.ALADDZ AS Postal_Code
    ,F0116.ALCTY1 AS City
    ,F0116.ALADDS AS State
    ,F0116.ALCTR  AS Country
    ,LTRIM(RTRIM(F0101.ABAC06)) AS Location_Type_cd
    ,'' as Location_Type
    ,LTRIM(RTRIM(F0101.ABAC15)) AS Asset_Type_cd
    ,'' as Asset_Type
    ,LTRIM(RTRIM(F0101.ABAC16)) AS PDS_Region_cd
    ,'' as PDS_Region
    ,LTRIM(RTRIM(F0101.ABAC17)) AS Owned_Leased_cd
    ,'' as Owned_Leased
    ,LTRIM(RTRIM(F0101.ABSIC))  AS Industry_cd
    ,'' as Industry
    ,pdmhub.ovcp_id AS ovcpid
FROM 
    {var_client_custom_db}.raw_ref_f0006_capone AS F0006
JOIN 
    {var_client_custom_db}.raw_ref_f0101 AS F0101
	ON F0006.MCAN8 = F0101.ABAN8
JOIN 
    (SELECT 
         ALAN8
		,MAX(ALEFTB) AS ALEFTB
	FROM 
        {var_client_custom_db}.raw_ref_f0116
	GROUP BY 
        ALAN8
	) AS AL
	ON F0006.MCAN8  = AL.ALAN8
JOIN 
    {var_client_custom_db}.raw_ref_f0116 AS F0116
	ON  AL.ALAN8   = F0116.ALAN8
	AND AL.ALEFTB = F0116.ALEFTB
LEFT OUTER JOIN 
    pdmhub AS pdmhub
	ON LTRIM(RTRIM(F0006.MCMCU)) = LTRIM(RTRIM(pdmhub.BU_PROJ_ID))
LEFT OUTER JOIN 
    {var_client_custom_db}.ref_f0111_capone AS F0111
	ON F0006.MCAN8 = F0111.WWAN8
	AND 0          = F0111.WWIDLN
	AND '11820'    = LEFT(F0006.MCMCU,5)
""")

# COMMAND ----------

# DBTITLE 1,F0005
spark.sql(f"""
create or replace temporary view F0005 
as
SELECT 
    *
FROM 
    {var_client_custom_db}.raw_ref_f0005
WHERE 
    DRSY = '01'
""")

# COMMAND ----------

# DBTITLE 1,Update Stage_E1_dbo_tblJDE_AddressBook
spark.sql(f"""
MERGE INTO           
    {var_client_custom_db}.Stage_E1_dbo_tblJDE_AddressBook as a
USING 
    (SELECT
        DISTINCT
        a.MCMCU as MCMCU,
        a.ABAN8 as ABAN8,
        C06.DRDL01 as C06_DRDL01,
        C15.DRDL01 as C15_DRDL01,
        C16.DRDL01 as C16_DRDL01,
        C17.DRDL01 as C17_DRDL01,
        SIC.DRDL01 as SIC_DRDL01
    FROM
        {var_client_custom_db}.Stage_E1_dbo_tblJDE_AddressBook as a
    LEFT JOIN 
        F0005 as C06
	    ON  C06.DRSY = '01' 
        AND C06.DRRT = '06' 
        AND LTRIM(RTRIM(C06.DRKY)) <> '' 
        AND LTRIM(RTRIM(C06.DRKY)) = a.Location_Type_cd 
	LEFT JOIN 
        F0005 as C15
	    ON  C15.DRSY = '01' 
        AND C15.DRRT = '15' 
        AND LTRIM(RTRIM(C15.DRKY)) <> '' 
        AND LTRIM(RTRIM(C15.DRKY)) = a.Asset_Type_cd
	LEFT JOIN 
        F0005 as C16
	    ON  C16.DRSY = '01' 
        AND C16.DRRT = '16' 
        AND LTRIM(RTRIM(C16.DRKY)) <> '' 
        AND LTRIM(RTRIM(C16.DRKY)) = a.PDS_Region_cd
	LEFT JOIN 
        F0005 as C17
	    ON  C17.DRSY = '01' 
        AND C17.DRRT = '17' 
        AND LTRIM(RTRIM(C17.DRKY)) <> '' 
        AND LTRIM(RTRIM(C17.DRKY)) = a.Owned_Leased_cd
	LEFT JOIN 
        F0005 as SIC
	    ON  SIC.DRSY = '01' 
        AND SIC.DRRT = 'SC' 
        AND LTRIM(RTRIM(SIC.DRKY)) <> '' 
        AND LTRIM(RTRIM(SIC.DRKY)) = a.Industry_cd
    ) as b
    ON  a.MCMCU = b.MCMCU
    AND a.ABAN8 = b.ABAN8

WHEN MATCHED THEN
    UPDATE SET
         a.Location_Type = b.C06_DRDL01
        ,a.Asset_Type    = b.C15_DRDL01
        ,a.PDS_Region    = b.C16_DRDL01
        ,a.Owned_Leased  = b.C17_DRDL01
        ,a.Industry      = b.SIC_DRDL01
""")