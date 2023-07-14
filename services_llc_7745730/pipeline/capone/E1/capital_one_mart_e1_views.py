# Databricks notebook source
# MAGIC %run ./notebook_variables

# COMMAND ----------

# DBTITLE 1,mart_e1_s_general_ledger
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.mart_e1_s_general_ledger
AS 
with _gl_raw as (
  select 
    ov.client_id, 
    gl.GLANI account_number, 
    gl.GLAID account_id, 
    gl.GLKCO document_company, 
    gl.GLDCT document_type, 
    gl.GLDOC document_number, 
    gl.GLJELN journal_entry_line_number, 
    gl.GLEXTL line_extension_code, 
    gl.GLSFX pay_item, 
    gl.GLSFXE pay_item_extension, 
    gl.GLPN period_number, 
    gl.GLFY fiscal_year, 
    gl.GLEXA supplier_name, 
    gl.GLAA amount, 
    gl.GLLT ledger_type, 
    gl.GLEXR explanation, 
    gl.GLVINV invoice_number, 
    gl.GLMCU business_unit, 
    gl.GLOBJ object_account, 
    gl.GLSUB subsidiary, 
    gl.GLCRCD transaction_currency, 
    gl.GLBCRC base_currency, 
    gl.GLPO purchase_order, 
    gl.GLWR01 work_order, 
    gl.GLSBL subledger, 
    gl.GLSBLT subledger_type, 
    gl.GLAN8 supplier, 
    gl.GLICUT batch_type, 
    gl.GLICU batch_number, 
    gl.GLTORG transaction_originator, 
    gl.GLCTRY fiscal_century, 
    gl.GLUPMT update_time, 
    gl.GLDGJ gl_date, 
    iff(
      length(gl.GLIVD) <= 4, 
      null, 
      dateadd(
        day, 
        (gl.GLIVD % 1000):: int - 1, 
        dateadd(
          year, 
          (gl.GLIVD / 1000):: int, 
          '1900-01-01'
        )
      )
    ):: date invoice_date, 
    gl.GLCO company_code, 
    gl.GLPYID payment_id, 
    gl.GLPOST posted, 
    dateadd(
      day, 
      (gl.GLUPMJ % 1000):: int - 1, 
      dateadd(
        year, 
        (gl.GLUPMJ / 1000):: int, 
        '1900-01-01'
      )
    ) update_date, 
    (gl.GLUPMT / 10000):: int upd_h, 
    (
      mod(gl.GLUPMT, 10000):: int / 100
    ):: int upd_m, 
    mod(gl.GLUPMT, 100):: int upd_s, 
    ov.multiplier 
  from 
    {var_client_custom_db}.raw_ref_f0911_{var_client_name} gl 
    join {var_client_custom_db}.mart_e1_l_oneview_company ov on gl.GLCO = ov.company_code 
    and ov.client_id = {var_client_id}
  where 
    gl.GLLT = 'AA' 
    and gl.GLFY in 
    ((year(current_date()) -2000), 
     (year(current_date()) -2000)-1, 
     (year(current_date()) -2000)+1)
), 
_gl as 
(
  select 
    CAST(MD5(Concat(document_company,'|', document_type, '|', document_number, '|', gl_date, '|', journal_entry_line_number, '|', line_extension_code, '|', ledger_type)) AS binary) 
    as general_ledger_hash_key,
    CAST(MD5(Concat(client_id, '|', document_company, '|', document_number, '|', document_type)) AS binary) 
    as document_hash_key,
    CAST(MD5(Concat(client_id, '|', company_code, '|', account_id, '|', business_unit, '|', object_account, '|', subsidiary)) AS binary) 
    as budget_hash_key,
    CAST(MD5(account_id) AS binary) 
    as account_id_hash_key,
    CAST(MD5(Concat(payment_id, '')) AS binary) 
    as payment_hash_key,
    CAST(MD5(Concat(client_id, '|', account_number, '|' , account_id, '|' , document_company, '|' , document_type, '|' , document_number, '|' , gl_date, '|' , journal_entry_line_number, '|' , line_extension_code, '|' , pay_item, '|' , pay_item_extension, '|' , period_number, '|' , fiscal_year, '|' , supplier_name, '|' , amount, '|' , ledger_type, '|' , explanation, '|' , invoice_number, '|' , invoice_date, '|' , business_unit, '|' , object_account, '|' , subsidiary, '|' , transaction_currency, '|' , base_currency, '|' , purchase_order, '|' , work_order, '|' , subledger, '|' , subledger_type, '|' , supplier, '|' , batch_type, '|' , batch_number, '|' , transaction_originator, '|' , fiscal_period, '|' , company_code, '|' , activity_date, '|', payment_id, '|', posted)) AS binary) 
    as hash_diff,
    current_date() load_date,
    'E1' record_source,
    *
from
(
  select 
    gl.client_id, 
    trim(replace(gl.account_number, '-', '.')) account_number, 
    trim(gl.account_id) account_id, 
    upper(trim(gl.document_company)) document_company, 
    upper(trim(gl.document_type)) document_type, 
    cast(gl.document_number as int) document_number, 
    dateadd(day,(gl.gl_date % 1000):: int - 1, 
    dateadd(year, (gl.gl_date / 1000):: int, 
    '1900-01-01')
    ):: date gl_date, 
    gl.journal_entry_line_number, 
    rtrim(gl.line_extension_code) line_extension_code, 
    upper(trim(pay_item)) pay_item, 
    upper(trim(pay_item_extension)) pay_item_extension, 
    cast(gl.period_number as int) period_number, 
    cast((gl.fiscal_century * 100 + gl.fiscal_year) as int)  fiscal_year, 
    trim(gl.supplier_name) supplier_name, 
    CAST(gl.amount / gl.multiplier as DOUBLE) amount, 
    rtrim(gl.ledger_type) ledger_type, 
    rtrim(gl.explanation) explanation, 
    trim(gl.invoice_number) invoice_number, 
    gl.invoice_date, 
    rtrim(gl.business_unit) business_unit, 
    rtrim(gl.object_account) object_account, 
    rtrim(gl.subsidiary) subsidiary, 
    rtrim(gl.transaction_currency) transaction_currency, 
    rtrim(gl.base_currency) base_currency, 
    rtrim(gl.purchase_order) purchase_order, 
    rtrim(gl.work_order) work_order, 
    trim(gl.subledger) subledger, 
    trim(gl.subledger_type) subledger_type, 
    cast(trim(gl.supplier) as int) supplier, 
    trim(gl.batch_type) batch_type, 
    CAST(gl.batch_number AS INT) batch_number, 
    trim(gl.transaction_originator) transaction_originator, 
    (((gl.fiscal_century * 100 + gl.fiscal_year) * 100) + gl.period_number)
    :: int fiscal_period, 
    trim(gl.company_code) company_code, 
    cast(
      case when gl.payment_id = 0 then -1 else gl.payment_id end as bigint
    ) payment_id, 
    dateadd(SECOND, upd_s,dateadd(MINUTE, upd_m, dateadd(HOUR, upd_h, update_date))) activity_date, 
    trim(gl.posted) posted 
  from 
    _gl_raw gl
))
select 
  * 
from 
  _gl
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name,var_client_id=var_client_id))

# COMMAND ----------

# DBTITLE 1,mart_e1_lkp_account
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.mart_e1_lkp_account
AS 
with _acct_id as (
  select 
    distinct account_id 
  from 
    {var_client_custom_db}.mart_e1_s_general_ledger
), 
_lkp_account as 
(
select
    CAST(
      MD5(
          account_id
      ) AS binary
    ) account_id_hash_key,
	*
from
(
  select 
    e1.client_id, 
    trim(gm.GMAID) account_id, 
    trim(gm.GMDL01) description, 
    gm.GMR009 gl_reporting_code_9 
  from 
    {var_client_custom_db}.raw_ref_f0901_{var_client_name} gm 
    join {var_client_custom_db}.mart_e1_l_oneview_company e1 on gm.GMCO = e1.company_code 
    join _acct_id gl on gm.GMAID = gl.account_id
))
select 
  *
from 
  _lkp_account
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,mart_e1_s_business_unit_master
spark.sql("""
create or replace view {var_client_custom_db}.mart_e1_s_business_unit_master as

with business_unit_master_raw  as (
      SELECT ovc.client_id, 
             ltrim(rtrim(mcmcu))                  AS business_unit, 
             ltrim(rtrim(mcstyl))                 AS business_unit_type, 
             ltrim(rtrim(mcdc))                   AS business_unit_description_compressed, 
             ltrim(rtrim(mcldm))                  AS business_unit_level_of_detail, 
             ltrim(rtrim(mcco))                   AS company_code, 
             mcan8                  			  			AS address_id, 
             mcan8o                               AS address_id_job_ar, 
			(CASE WHEN  ltrim(rtrim(mccnty))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mccnty))  END)          AS county_code,  
			(CASE WHEN  ltrim(rtrim(mcadds))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcadds))  END)          AS state_code,  
			(CASE WHEN  ltrim(rtrim(mcfmod))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcfmod))  END)          AS model_account_consolidation_flag,  
			(CASE WHEN  ltrim(rtrim(mcdl01))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcdl01))  END)          AS business_unit_description,               
			(CASE WHEN  ltrim(rtrim(mcdl02))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcdl02))  END)          AS business_unit_description02,  
			(CASE WHEN  ltrim(rtrim(mcdl03))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcdl03))  END)          AS business_unit_description03,  
			(CASE WHEN  ltrim(rtrim(mcdl04))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcdl04))  END)          AS business_unit_description04,  
			(CASE WHEN  ltrim(rtrim(mcrp01))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp01))  END)          AS division_code,  
			(CASE WHEN  ltrim(rtrim(mcrp02))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp02))  END)          AS region_code,  
			(CASE WHEN  ltrim(rtrim(mcrp03))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp03))  END)          AS group_code,  
			(CASE WHEN  ltrim(rtrim(mcrp04))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp04))  END)          AS branch_office_code,  
			(CASE WHEN  ltrim(rtrim(mcrp05))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp05))  END)          AS department_type_code,  
			(CASE WHEN  ltrim(rtrim(mcrp06))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp06))  END)          AS person_responsible_code,  
			(CASE WHEN  ltrim(rtrim(mcrp07))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp07))  END)          AS line_of_business_code,  
			(CASE WHEN  ltrim(rtrim(mcrp08))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp08))  END)          AS category_code_08,  
			(CASE WHEN  ltrim(rtrim(mcrp09))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp09))  END)          AS category_code_09,  
			(CASE WHEN  ltrim(rtrim(mcrp10))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp10))  END)          AS category_code_10,   
 			(CASE WHEN  ltrim(rtrim(mcrp11))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp11))  END)          AS category_code_11,  
			(CASE WHEN  ltrim(rtrim(mcrp12))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp12))  END)          AS category_code_12,  
			(CASE WHEN  ltrim(rtrim(mcrp13))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp13))  END)          AS category_code_13,  
			(CASE WHEN  ltrim(rtrim(mcrp14))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp14))  END)          AS category_code_14,  
			(CASE WHEN  ltrim(rtrim(mcrp15))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp15))  END)          AS category_code_15,          
			(CASE WHEN  ltrim(rtrim(mcrp16))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp16))  END)          AS category_code_16, 
            (CASE WHEN  ltrim(rtrim(mcrp17))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp17))  END)          AS category_code_17,   
            (CASE WHEN  ltrim(rtrim(mcrp18))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp18))  END)          AS category_code_18,               
            (CASE WHEN  ltrim(rtrim(mcrp19))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp19))  END)          AS category_code_19,                
            (CASE WHEN  ltrim(rtrim(mcrp20))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp20))  END)          AS category_code_20,           
            (CASE WHEN  ltrim(rtrim(mcrp21))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp21))  END)          AS category_code_21,           
            (CASE WHEN  ltrim(rtrim(mcrp22))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp22))  END)          AS category_code_22,               
            (CASE WHEN  ltrim(rtrim(mcrp23))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp23))  END)          AS category_code_23,              
            (CASE WHEN  ltrim(rtrim(mcrp24))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp24))  END)          AS category_code_24, 
            (CASE WHEN  ltrim(rtrim(mcrp25))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp25))  END)          AS category_code_25, 
            (CASE WHEN  ltrim(rtrim(mcrp26))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp26))  END)          AS category_code_26,              
            (CASE WHEN  ltrim(rtrim(mcrp27))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp27))  END)          AS category_code_27,             
            (CASE WHEN  ltrim(rtrim(mcrp28))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp28))  END)          AS category_code_28,               
            (CASE WHEN  ltrim(rtrim(mcrp29))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp29))  END)          AS category_code_29,                
            (CASE WHEN  ltrim(rtrim(mcrp30))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp30))  END)          AS category_code_30,   
            (CASE WHEN  ltrim(rtrim(mcrp31))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp31))  END)          AS category_code_31,   
            (CASE WHEN  ltrim(rtrim(mcrp32))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp32))  END)          AS category_code_32,   
            (CASE WHEN  ltrim(rtrim(mcrp33))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp33))  END)          AS category_code_33,   
            (CASE WHEN  ltrim(rtrim(mcrp34))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp34))  END)          AS category_code_34,   
            (CASE WHEN  ltrim(rtrim(mcrp35))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp35))  END)          AS category_code_35,   
            (CASE WHEN  ltrim(rtrim(mcrp36))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp36))  END)          AS category_code_36,   
            (CASE WHEN  ltrim(rtrim(mcrp37))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp37))  END)          AS category_code_37,   
            (CASE WHEN  ltrim(rtrim(mcrp38))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp38))  END)          AS category_code_38,   
            (CASE WHEN  ltrim(rtrim(mcrp39))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp39))  END)          AS category_code_39,   
            (CASE WHEN  ltrim(rtrim(mcrp40))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp40))  END)          AS category_code_40,   
            (CASE WHEN  ltrim(rtrim(mcrp41))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp41))  END)          AS category_code_41,   
            (CASE WHEN  ltrim(rtrim(mcrp42))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp42))  END)          AS category_code_42,   
            (CASE WHEN  ltrim(rtrim(mcrp43))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp43))  END)          AS category_code_43,   
            (CASE WHEN  ltrim(rtrim(mcrp44))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp44))  END)          AS category_code_44,   
            (CASE WHEN  ltrim(rtrim(mcrp45))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp45))  END)          AS category_code_45,   
            (CASE WHEN  ltrim(rtrim(mcrp46))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp46))  END)          AS category_code_46,   
            (CASE WHEN  ltrim(rtrim(mcrp47))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp47))  END)          AS category_code_47,   
            (CASE WHEN  ltrim(rtrim(mcrp48))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp48))  END)          AS category_code_48,   
            (CASE WHEN  ltrim(rtrim(mcrp49))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp49))  END)          AS category_code_49,   
            (CASE WHEN  ltrim(rtrim(mcrp50))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcrp50))  END)          AS category_code_50,   
            (CASE WHEN  ltrim(rtrim(mcpecc))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcpecc))  END)          AS posting_edit_business_unit,  
            (CASE WHEN  ltrim(rtrim(mcsbli))  IS NULL THEN ' ' ELSE  ltrim(rtrim(mcsbli))  END)          AS subledger_inactive_code,  
            dateadd(day, (mcupmj % 1000)::int - 1, dateadd(year, (mcupmj / 1000)::int, '1900-01-01'))::date AS date_updated, 									--calendar.calendar_date AS date_updated, 
            mcupmt AS time_updated 
		FROM   
			{var_client_custom_db}.raw_ref_f0006_{var_client_name} bu 
		JOIN   
			{var_client_custom_db}.mart_e1_l_oneview_company ovc ON bu.mcco = ovc.company_code 
),
  
 
user_defined_codes_raw as(
		SELECT 
			LTRIM(RTRIM(drsy))   AS product_code, 
			LTRIM(RTRIM(drrt))   AS user_defined_codes, 
			case when  LTRIM(RTRIM(drky)) is null then ' '  else LTRIM(RTRIM(drky)) end    AS user_defined_code ,
			LTRIM(RTRIM(drdl01)) AS user_defined_description 
        FROM   
			{var_raw_db}.ref_f0005  
		WHERE  
			LTRIM(RTRIM(drsy)) = '00' 
      ) ,
      
business_unit_masters as ( 
		SELECT 
			CAST( MD5( Concat(bu.client_id, '|', Rtrim( bu.company_code),'')) AS binary) oneview_company_hash_key, 
			CAST( MD5( Concat( Ltrim(Rtrim(bu.business_unit )),'')) AS binary)   business_unit_hash_key, 
			CAST( MD5( Concat(bu.client_id, '|', 
			bu.business_unit , '|',
			bu.business_unit_type, '|', 
			bu.business_unit_description_compressed , '|',  
			bu.business_unit_level_of_detail, '|',
			bu.company_code, '|', address_id, '|',
			bu.address_id_job_ar, '|', 
			bu.county_code, '|', 
			bu. state_code, '|',  
			bu.model_account_consolidation_flag, '|',             
			bu.business_unit_description, '|', 
			bu.business_unit_description02, '|', 
			bu.business_unit_description03, '|', 
			bu.business_unit_description04, '|',  
			bu.division_code , '|', 
			bu.region_code , '|',  
			bu.group_code , '|', 
			bu.branch_office_code,'|', 
			bu.department_type_code, '|', 
			bu.person_responsible_code,  
			bu.line_of_business_code,  '|', 
			bu.category_code_08, '|', 
			bu.category_code_09, '|', 
			bu.category_code_10, '|', 
			bu.category_code_11, '|', 
			bu.category_code_12, '|', 
			bu.category_code_13, '|', 
			bu.category_code_14, '|', 
			bu.category_code_15, '|', 
			bu.category_code_16, '|', 
			bu.category_code_17, '|', 
			bu.category_code_18, '|', 
			bu.category_code_19, '|', 
			bu.category_code_20, '|', 
			bu.category_code_21, '|', 
			bu.category_code_22, '|', 
			bu.category_code_23, '|', 
			bu.category_code_24, '|', 
			bu.category_code_25, '|',   
			bu.category_code_26, '|',  
			bu.category_code_27, '|',   
			bu.category_code_28, '|', 
			bu.category_code_29, '|', 
			bu.category_code_30, '|',  
			bu.category_code_31, '|',  
			bu.category_code_32, '|',  
			bu.category_code_33, '|',  
			bu.category_code_34, '|', 
			bu.category_code_35, '|',  
			bu.category_code_36, '|', 
			bu.category_code_37, '|',  
			bu.category_code_38, '|',  
			bu.category_code_39, '|',  
			bu.category_code_40, '|',  
			bu.category_code_41, '|',  
			bu.category_code_42, '|',  
			bu.category_code_43, '|',  
			bu.category_code_44, '|',  
			bu.category_code_45, '|',  
			bu.category_code_46, '|',  
			bu.category_code_47, '|', 
			bu.category_code_48, '|',  
			bu.category_code_49, '|', 
			bu.category_code_50, '|', 
			bu.posting_edit_business_unit,'|',   
			bu.subledger_inactive_code ,'|',  
			udc01.user_defined_description , '|',  
			udc02.user_defined_description , '|',  
			udc03.user_defined_description , '|',  
			udc04.user_defined_description , '|',   
			udc05.user_defined_description , '|',   
			udc06.user_defined_description , '|',  
			udc07.user_defined_description , '|',   
			udc08.user_defined_description , '|', 
			udc09.user_defined_description,  '|',  
			udc10.user_defined_description,  '|',   
			udc11.user_defined_description,  '|',  
			udc12.user_defined_description,  '|',   
			udc13.user_defined_description,  '|',  
			udc14.user_defined_description , '|', 
			udc15.user_defined_description , '|', 
			udc16.user_defined_description , '|', 
			udc17.user_defined_description , '|',  
			udc18.user_defined_description , '|',  
			udc19.user_defined_description , '|', 
			udc20.user_defined_description , '|', 
			udc21.user_defined_description,  '|', 
			udc22.user_defined_description,  '|', 
			udc23.user_defined_description,  '|', 
			udc24.user_defined_description,  '|', 
			udc25.user_defined_description,  '|', 
			udc26.user_defined_description,  '|', 
			udc27.user_defined_description,  '|', 
			udc28.user_defined_description,  '|', 
			udc29.user_defined_description,  '|', 
			udc30.user_defined_description,  '|', 
			udc31.user_defined_description,  '|', 
			udc32.user_defined_description,  '|', 
			udc33.user_defined_description,  '|', 
			udc34.user_defined_description,  '|', 
			udc35.user_defined_description,  '|', 
			udc36.user_defined_description,  '|', 
			udc37.user_defined_description,  '|', 
			udc38.user_defined_description,  '|', 
			udc39.user_defined_description,  '|', 
			udc40.user_defined_description,  '|', 
			udc41.user_defined_description,  '|', 
			udc42.user_defined_description,  '|', 
			udc43.user_defined_description,  '|', 
			udc44.user_defined_description , '|', 
			udc45.user_defined_description , '|', 
			udc46.user_defined_description , '|',  
			udc47.user_defined_description , '|', 
			udc48.user_defined_description , '|', 
			udc49.user_defined_description , '|', 
			udc50.user_defined_description , '|' )) AS binary) AS business_unit_hash_diff, 
			CAST( MD5( Concat(bu.address_id, '')) AS binary) address_book_hash_key, 
			CAST( MD5( Concat(Rtrim(bu.company_code), '' )) AS binary) company_code_hash_key, 
			CAST( MD5( Concat(bu.client_id, '')) AS binary) client_id_hash_key,
            bu.*, 
            udc01.user_defined_description             AS              division_description, 
            udc02.user_defined_description             AS              region_description,
            --udc03.user_defined_description           AS              group_description, 
            udc04.user_defined_description             AS              branch_office_description, 
            udc05.user_defined_description             AS              department_type_description, 
            udc06.user_defined_description             AS              person_responsible_description, 
            udc07.user_defined_description             AS              line_of_business_description, 
            udc08.user_defined_description             AS              category_description_08, 
            udc09.user_defined_description             AS              category_description_09, 
            udc10.user_defined_description             AS              category_description_10, 
            udc11.user_defined_description             AS              category_description_11, 
            udc12.user_defined_description             AS              category_description_12, 
            udc13.user_defined_description             AS              category_description_13, 
            udc14.user_defined_description             AS              category_description_14, 
            udc15.user_defined_description             AS              category_description_15, 
            udc16.user_defined_description             AS              category_description_16, 
            udc17.user_defined_description             AS              category_description_17, 
            udc18.user_defined_description             AS              category_description_18, 
            udc19.user_defined_description             AS              category_description_19, 
            udc20.user_defined_description             AS              category_description_20, 
            udc21.user_defined_description             AS              category_description_21, 
            udc22.user_defined_description             AS              category_description_22, 
            udc23.user_defined_description             AS              category_description_23, 
            udc24.user_defined_description             AS              category_description_24, 
            udc25.user_defined_description             AS              category_description_25, 
            udc26.user_defined_description             AS              category_description_26, 
            udc27.user_defined_description             AS              category_description_27, 
            udc28.user_defined_description             AS              category_description_28, 
            udc29.user_defined_description             AS              category_description_29, 
            udc30.user_defined_description             AS              category_description_30, 
            udc31.user_defined_description             AS              category_description_31, 
            udc32.user_defined_description             AS              category_description_32, 
            udc33.user_defined_description             AS              category_description_33, 
            udc34.user_defined_description             AS              category_description_34, 
            udc35.user_defined_description             AS              category_description_35, 
            udc36.user_defined_description             AS              category_description_36, 
            udc37.user_defined_description             AS              category_description_37, 
            udc38.user_defined_description             AS              category_description_38, 
            udc39.user_defined_description             AS              category_description_39, 
            udc40.user_defined_description             AS              category_description_40, 
            udc41.user_defined_description             AS              category_description_41, 
            udc42.user_defined_description             AS              category_description_42, 
            udc43.user_defined_description             AS              category_description_43, 
            udc44.user_defined_description             AS              category_description_44, 
            udc45.user_defined_description             AS              category_description_45, 
            udc46.user_defined_description             AS              category_description_46, 
            udc47.user_defined_description             AS              category_description_47, 
            udc48.user_defined_description             AS              category_description_48, 
            udc49.user_defined_description             AS              category_description_49, 
            udc50.user_defined_description             AS              category_description_50
        FROM   
			business_unit_master_raw bu 
        LEFT JOIN 
			user_defined_codes_raw udc01 ON bu.division_code = (udc01.user_defined_code) AND udc01.user_defined_codes = '01' 
        LEFT JOIN 
			user_defined_codes_raw udc02   ON bu.region_code   = (udc02.user_defined_code) 		   				AND udc02.user_defined_codes = '02'
        LEFT JOIN 
			user_defined_codes_raw udc03   ON bu.group_code 	  = (udc03.user_defined_code) 			  			AND udc03.user_defined_codes = '03' 
        LEFT JOIN 
			user_defined_codes_raw udc04   ON bu.branch_office_code = (udc04.user_defined_code)  		   	AND udc04.user_defined_codes = '04'              
		LEFT JOIN 
			user_defined_codes_raw udc05   ON bu.department_type_code =   (udc05.user_defined_code) 	   	AND udc05.user_defined_codes = '05' 
		LEFT JOIN 
			user_defined_codes_raw udc06   ON bu.person_responsible_code = (udc06.user_defined_code)	   	AND udc06.user_defined_codes = '06' 
             LEFT JOIN user_defined_codes_raw udc07   ON bu.line_of_business_code = (udc07.user_defined_code)   	  AND udc07.user_defined_codes = '07' 
             LEFT JOIN user_defined_codes_raw udc08   ON bu.category_code_08 = (udc08.user_defined_code)       	   	AND udc08.user_defined_codes = '08'              
			 LEFT JOIN user_defined_codes_raw udc09   ON bu.category_code_09 = (udc09.user_defined_code)        	   AND udc09.user_defined_codes = '09' 
             LEFT JOIN user_defined_codes_raw udc10   ON bu.category_code_10 = (udc10.user_defined_code)      		   AND udc10.user_defined_codes = '10' 
             LEFT JOIN user_defined_codes_raw udc11   ON bu.category_code_11 = (udc11.user_defined_code)      		   AND udc11.user_defined_codes = '11' 
             LEFT JOIN user_defined_codes_raw udc12   ON bu.category_code_12 = (udc12.user_defined_code)      		   AND udc12.user_defined_codes = '12' 
             LEFT JOIN user_defined_codes_raw udc13   ON bu.category_code_13 = (udc13.user_defined_code)      		   AND udc13.user_defined_codes = '13' 
             LEFT JOIN user_defined_codes_raw udc14   ON bu.category_code_14 = (udc14.user_defined_code)      		   AND udc14.user_defined_codes = '14'              
			 LEFT JOIN user_defined_codes_raw udc15   ON bu.category_code_15 = (udc15.user_defined_code)      		   AND udc15.user_defined_codes = '15' 
			 LEFT JOIN user_defined_codes_raw udc16   ON bu.category_code_16 = (udc16.user_defined_code)      		   AND udc16.user_defined_codes = '16'
             LEFT JOIN user_defined_codes_raw udc17   ON bu.category_code_17 = (udc17.user_defined_code)      		   AND udc17.user_defined_codes = '17' 
			 LEFT JOIN user_defined_codes_raw udc18   ON bu.category_code_18 = (udc18.user_defined_code)      		   AND udc18.user_defined_codes = '18' 
             LEFT JOIN user_defined_codes_raw udc19   ON bu.category_code_19 = (udc19.user_defined_code)      		   AND udc19.user_defined_codes = '19' 
             LEFT JOIN user_defined_codes_raw udc20   ON bu.category_code_20 = (udc20.user_defined_code)      		   AND udc20.user_defined_codes = '20' 
             LEFT JOIN user_defined_codes_raw udc21   ON bu.category_code_21 = (udc21.user_defined_code)      		   AND udc21.user_defined_codes = '21' 
             LEFT JOIN user_defined_codes_raw udc22   ON bu.category_code_22 = (udc22.user_defined_code)      		   AND udc22.user_defined_codes = '22' 
             LEFT JOIN user_defined_codes_raw udc23   ON bu.category_code_23 = (udc23.user_defined_code)      		   AND udc23.user_defined_codes = '23' 
             LEFT JOIN user_defined_codes_raw udc24   ON bu.category_code_24 = (udc24.user_defined_code)      		   AND udc24.user_defined_codes = '24' 
             LEFT JOIN user_defined_codes_raw udc25   ON bu.category_code_25 = (udc25.user_defined_code)      		   AND udc25.user_defined_codes = '25' 
             LEFT JOIN user_defined_codes_raw udc26   ON bu.category_code_26 = (udc26.user_defined_code)      		   AND udc26.user_defined_codes = '26' 
             LEFT JOIN user_defined_codes_raw udc27   ON bu.category_code_27 = (udc27.user_defined_code)      		   AND udc27.user_defined_codes = '27' 
             LEFT JOIN user_defined_codes_raw udc28   ON bu.category_code_28 = (udc28.user_defined_code)      		   AND udc28.user_defined_codes = '28' 
             LEFT JOIN user_defined_codes_raw udc29   ON bu.category_code_29 = (udc29.user_defined_code)      		   AND udc29.user_defined_codes = '29' 
             LEFT JOIN user_defined_codes_raw udc30   ON bu.category_code_30 = (udc30.user_defined_code)      		   AND udc30.user_defined_codes = '30' 
             LEFT JOIN user_defined_codes_raw udc31   ON bu.category_code_31 = (udc31.user_defined_code)      		   AND udc31.user_defined_codes = '31' 
             LEFT JOIN user_defined_codes_raw udc32   ON bu.category_code_32 = (udc32.user_defined_code)      		   AND udc32.user_defined_codes = '32' 
             LEFT JOIN user_defined_codes_raw udc33   ON bu.category_code_33 = (udc33.user_defined_code)      		   AND udc33.user_defined_codes = '33' 
             LEFT JOIN user_defined_codes_raw udc34   ON bu.category_code_34 = (udc34.user_defined_code)      		   AND udc34.user_defined_codes = '34' 
             LEFT JOIN user_defined_codes_raw udc35   ON bu.category_code_35 = (udc35.user_defined_code)      		   AND udc35.user_defined_codes = '35' 
             LEFT JOIN user_defined_codes_raw udc36   ON bu.category_code_36 = (udc36.user_defined_code)      		   AND udc36.user_defined_codes = '36'              
			 LEFT JOIN user_defined_codes_raw udc37   ON bu.category_code_37 =  (udc37.user_defined_code)   		   AND udc37.user_defined_codes = '37' 
             LEFT JOIN user_defined_codes_raw udc38   ON bu.category_code_38 = (udc38.user_defined_code)     	       AND udc38.user_defined_codes = '38' 
             LEFT JOIN user_defined_codes_raw udc39   ON bu.category_code_39 = (udc39.user_defined_code)     		   AND udc39.user_defined_codes = '39' 
             LEFT JOIN user_defined_codes_raw udc40   ON bu.category_code_40 = (udc40.user_defined_code)     		   AND udc40.user_defined_codes = '40'              			 
			LEFT JOIN  user_defined_codes_raw udc41  ON bu.category_code_41 = (udc41.user_defined_code)     		   AND udc41.user_defined_codes = '41'              
			LEFT JOIN  user_defined_codes_raw udc42  ON bu.category_code_42 = (udc42.user_defined_code)     		   AND udc42.user_defined_codes = '42' 
             LEFT JOIN user_defined_codes_raw udc43   ON bu.category_code_43 = (udc43.user_defined_code)     		   AND udc43.user_defined_codes = '43' 
            LEFT JOIN user_defined_codes_raw udc44    ON bu.category_code_44 = (udc44.user_defined_code)     		   AND udc44.user_defined_codes = '44' 
            LEFT JOIN user_defined_codes_raw udc45    ON bu.category_code_45 = (udc45.user_defined_code)     		   AND udc45.user_defined_codes = '45' 
            LEFT JOIN user_defined_codes_raw udc46    ON bu.category_code_46 = (udc46.user_defined_code)     		   AND udc46.user_defined_codes = '46' 
            LEFT JOIN user_defined_codes_raw udc47    ON bu.category_code_47 = (udc47.user_defined_code)     		   AND udc47.user_defined_codes = '47' 
            LEFT JOIN user_defined_codes_raw udc48    ON bu.category_code_48 = (udc48.user_defined_code)     		   AND udc48.user_defined_codes = '48' 
            LEFT JOIN user_defined_codes_raw udc49    ON bu.category_code_49 = (udc49.user_defined_code)     		   AND udc49.user_defined_codes = '49' 
            LEFT JOIN user_defined_codes_raw udc50    ON bu.category_code_50 = (udc50.user_defined_code)     	     AND udc50.user_defined_codes = '50' )
 select 
        oneview_company_hash_key
        ,business_unit_hash_key
        ,business_unit_hash_diff
        ,address_book_hash_key
        ,company_code_hash_key
        ,client_id_hash_key
        ,client_id
        ,business_unit
        ,business_unit_type
        ,business_unit_description_compressed
        ,business_unit_level_of_detail
        ,company_code
        ,address_id
        ,address_id_job_ar
        ,county_code
        ,state_code
        ,model_account_consolidation_flag
        ,business_unit_description
        ,business_unit_description02
        ,business_unit_description03
        ,business_unit_description04
        ,division_code
        ,region_code
        ,group_code
        ,branch_office_code
        ,department_type_code
        ,person_responsible_code
        ,line_of_business_code
        ,category_code_08
        ,category_code_09
        ,category_code_10
        ,category_code_11
        ,category_code_12
        ,category_code_13
        ,category_code_14
        ,category_code_15
        ,category_code_16
        ,category_code_17
        ,category_code_18
        ,category_code_19
        ,category_code_20
        ,category_code_21
        ,category_code_22
        ,category_code_23
        ,category_code_24
        ,category_code_25
        ,category_code_26
        ,category_code_27
        ,category_code_28
        ,category_code_29
        ,category_code_30
        ,category_code_31
        ,category_code_32
        ,category_code_33
        ,category_code_34
        ,category_code_35
        ,category_code_36
        ,category_code_37
        ,category_code_38
        ,category_code_39
        ,category_code_40
        ,category_code_41
        ,category_code_42
        ,category_code_43
        ,category_code_44
        ,category_code_45
        ,category_code_46
        ,category_code_47
        ,category_code_48
        ,category_code_49
        ,category_code_50
        ,posting_edit_business_unit
        ,subledger_inactive_code
        ,division_description
        ,region_description
        ,branch_office_description
        ,department_type_description
        ,person_responsible_description
        ,line_of_business_description
        ,category_description_08
        ,category_description_09
        ,category_description_10
        ,category_description_11
        ,category_description_12
        ,category_description_13
        ,category_description_14
        ,category_description_15
        ,category_description_16
        ,category_description_17
        ,category_description_18
        ,category_description_19
        ,category_description_20
        ,category_description_21
        ,category_description_22
        ,category_description_23
        ,category_description_24
        ,category_description_25
        ,category_description_26
        ,category_description_27
        ,category_description_28
        ,category_description_29
        ,category_description_30
        ,category_description_31
        ,category_description_32
        ,category_description_33
        ,category_description_34
        ,category_description_35
        ,category_description_36
        ,category_description_37
        ,category_description_38
        ,category_description_39
        ,category_description_40
        ,category_description_41
        ,category_description_42
        ,category_description_43
        ,category_description_44
        ,category_description_45
        ,category_description_46
        ,category_description_47
        ,category_description_48
        ,category_description_49
        ,category_description_50
        ,date_updated
        ,time_updated
   from business_unit_masters 
   where 
      client_id = '{var_client_id}'
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name,var_client_id=var_client_id, var_raw_db=var_raw_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_ap_ledger_helper_view
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.mart_e1_ap_ledger_helper_view
AS 
(
  select 
    CAST(
      MD5(
        Concat(
          client_id, 
          '|', 
          case when document_company is null then '' else document_company end, 
          '|', 
          case when document_number is null then '' else document_number end, 
          '|', 
          case when document_type is null then '' else document_type end, 
          '|', 
          case when pay_item is null then '' else pay_item end, 
          '|', 
          case when pay_item_extension_number is null then '' else pay_item_extension_number end, 
          '|', 
          case when address_id is null then '' else address_id end, 
          '|', 
          case when payee_address_id is null then '' else payee_address_id end, 
          '|', 
          case when invoice_number is null then '' else invoice_number end, 
          '|', 
          case when reference is null then '' else reference end, 
          '|', 
          case when pay_status_code is null then '' else pay_status_code end, 
          '|', 
          case when amount_gross is null then '' else amount_gross end, 
          '|', 
          case when amount_taxable is null then '' else amount_taxable end, 
          '|', 
          case when invoice_date is null then '' else invoice_date end, 
          '|', 
          case when invoice_date_original is null then '' else invoice_date_original end, 
          '|', 
          case when invoice_due_date is null then '' else invoice_due_date end, 
          '|', 
          case when gl_date is null then '' else gl_date end, 
          '|', 
          case when currency_code is null then '' else currency_code end, 
          '|', 
          case when purchase_order is null then '' else purchase_order end, 
          '|', 
          case when business_unit is null then '' else business_unit end, 
          '|', 
          case when object_account is null then '' else object_account end, 
          '|', 
          case when funding_number is null then '' else funding_number end, 
          '|', 
          case when object_account is null then '' else object_account end, 
          '|', 
          case when remark is null then '' else remark end, 
          '|', 
          case when payment_type_code is null then '' else payment_type_code end, 
          '|', 
          case when payment_type_description is null then '' else payment_type_description end, 
          '|', 
          case when fiscal_year is null then '' else fiscal_year end, 
          '|', 
          case when period_number is null then '' else period_number end, 
          '|', 
          case when amount_to_distribute is null then '' else amount_to_distribute end, 
          '|', 
          case when company_code is null then '' else company_code end
        )
      ) AS binary
    ) ap_ledger_hash_diff, 
    CAST(
      MD5(
        Concat(
          client_id, '|', document_company, 
          '|', document_number, '|', document_type, 
          '|', pay_item, '|', pay_item_extension_number
        )
      ) AS binary
    ) ap_ledger_hash_key, 
    CAST(
      MD5(
        Concat(
          client_id, '|', document_company, 
          '|', document_number, '|', document_type
        )
      ) AS binary
    ) document_hash_key, 
    CAST(
      MD5(
        Concat(address_id, '')
      ) AS binary
    ) address_book_hash_key, 
    current_date() as load_date, 
    current_date() as update_date, 
    'E1' record_source, 
    * 
  from 
    (
      select 
        e1.client_id, 
        ltrim(rtrim(RPCO)) company_code, 
        upper(ltrim(rtrim(RPKCO))) document_company, 
        cast(RPDOC as int) document_number, 
        upper(ltrim(rtrim(RPDCT))) document_type, 
        upper(ltrim(rtrim(RPSFX))) pay_item, 
        cast(RPSFXE as tinyint) pay_item_extension_number, 
        cast(RPAN8 as int) address_id, 
        cast(RPPYE as int) payee_address_id, 
        upper(ltrim(rtrim(RPVINV))) invoice_number, 
        upper(ltrim(rtrim(RPVR01))) reference, 
        upper(ltrim(rtrim(RPPST))) pay_status_code, 
        ltrim(rtrim(RPICUT)) batch_type, 
        cast(RPICU as int) batch_number, 
        cast(cast(RPAG as bigint) / e1.multiplier as numeric(18,3)) amount_gross, 
        cast(cast(RPATXA as bigint) / e1.multiplier as numeric(18,3)) amount_taxable, 
        cast(cast(RPATAD as bigint) / e1.multiplier as numeric(18,3)) amount_to_distribute, 
        dateadd(day,(a.RPDICJ % 1000):: int - 1,dateadd(year,(a.RPDICJ / 1000):: int, '1900-01-01')):: date invoice_date, 
        dateadd(day,(a.RPDIVJ % 1000):: int - 1,dateadd(year,(a.RPDIVJ / 1000):: int, '1900-01-01')):: date invoice_date_original, --TFS-35676
		dateadd(day, (a.RPDDJ % 1000)::int - 1, dateadd(year,(a.RPDDJ / 1000):: int, '1900-01-01')):: date invoice_due_date, 
		dateadd(day,(a.RPDGJ % 1000):: int - 1,dateadd(year,(a.RPDGJ / 1000):: int, '1900-01-01')):: date gl_date, 
		rtrim(RPCRCD) currency_code, 
		ltrim(rtrim(RPPO)) purchase_order, 
		ltrim(rtrim(RPMCU)) business_unit, 
		ltrim(rtrim(RPOBJ)) object_account, 
		a.RPRMK AS remark, --TFS-356769 
		ltrim(rtrim(a.RPPYIN)) AS payment_type_code, 
		case WHEN udc.DRDL01 IS NULL THEN '' ELSE udc.DRDL01 END AS payment_type_description, 
		cast(RPCTRY * 100 + RPFY as smallint) fiscal_year, 
		cast(RPPN as tinyint) period_number, 
		a.RPURAB as funding_number,  
    /*datetimefromparts(c3.Calendar_Year, c3.Calendar_Month, c3.Calendar_Day, round(a.RPUPMT, -4, 1) / 10000, (a.RPUPMT - round(a.RPUPMT, -4, 1)) / 100, a.RPUPMT - round(a.RPUPMT, -2, 1), 0) activity_date */
		current_date() as activity_date 
  from 
    {var_client_custom_db}.raw_ref_f0411_{var_client_name} a 
    join {var_client_custom_db}.mart_e1_l_oneview_company e1 on a.RPCO = e1.company_code 
    left join {var_raw_db}.ref_f0005 udc 
		on udc.DRRT = 'PY' and udc.DRSY = '00' and ltrim(udc.DRKY) = a.RPPYIN 
  where 
    a.RPFY in ((year(current_date()) -2000), (year(current_date()) -2000) -1, (year(current_date()) -2000) + 1)
) d
)
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name, var_raw_db=var_raw_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_lkp_ap_ledger_header
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.mart_e1_lkp_ap_ledger_header
AS 
(
  select 
    document_hash_key, 
    client_id, 
    reference, 
    cast(
      case when pay_status_code_count > 1 then 'Multiple Codes' 
	  when pay_status_code = 'A' then 'Approved for payment' 
	  when pay_status_code = 'H' then 'Held pending Approval' 
	  when pay_status_code = 'P' then 'Paid' 
	  else a.Pay_Status_Code 
	  end as varchar(50)
    ) as invoice_status 
  from 
    (
      select 
        document_hash_key, 
        client_id, 
        max(reference) reference, 
        count(distinct pay_status_code) pay_status_code_count, 
        max(pay_status_code) pay_status_code 
      from 
        {var_client_custom_db}.mart_e1_ap_ledger_helper_view 
      group by 
        document_hash_key, 
        client_id
    ) a
)
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_payment_helper_view
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.mart_e1_payment_helper_view
AS 
(
  with voided_payments AS (
    select 
      distinct RNPYID 
    from 
      (
        select 
          RNPYID, 
          max(case when RNDCTM = 'PK' then 1 else 0 end) PK, 
          max(case when RNDCTM = 'PO' then 1 else 0 end) PO 
        from 
          {var_client_custom_db}.raw_ref_f0414_{var_client_name}
        group by 
          RNPYID
      ) a 
    where 
      PK = 1 and PO = 1
  ) 
  select 
    CAST(
      MD5(
        Concat(
          e1.client_id, 
          '|', 
          upper(ltrim(rtrim(a.RNKCO))), 
          '|', 
          upper(ltrim(rtrim(a.RNDOC))), 
          '|', 
          upper(ltrim(rtrim(a.RNDCT))), 
          '|', 
          upper(ltrim(rtrim(a.RNSFX))
          ), 
          '|', 
          upper(ltrim(rtrim(a.RNSFXE))), 
          cast(a.RNPYID as bigint), 
          '|', 
          cast(a.RNAN8 as bigint)
		)
      ) AS binary
    ) payment_hash_key, 
    CAST(
      MD5(
        Concat(
          e1.client_id, 
          '|', 
          upper(ltrim(rtrim(a.RNKCO))), 
          '|', 
          upper(ltrim(rtrim(a.RNDOC))), 
          '|', 
          upper(ltrim(rtrim(a.RNDCT))), 
          '|', 
          upper(ltrim(rtrim(a.RNSFX))), 
          '|', 
          upper(ltrim(rtrim(a.RNSFXE)))
        )
      ) AS binary
    ) ap_ledger_hash_key, 
    CAST(
      MD5(
        concat(
          e1.client_id, 
          '|', 
          upper(ltrim(rtrim(a.RNKCO))), 
          '|', 
          upper(ltrim(rtrim(a.RNDOC))), 
          '|', 
          upper(ltrim(rtrim(a.RNDCT)))
        )
      ) AS binary
    ) document_hash_key, 
    CAST(
      MD5(
        concat(
          cast(a.RNPYID as bigint), 
          ''
        )
      ) AS binary
    ) payment_id_hash_key, 
    CAST(
      MD5(
        concat(
          cast(a.RNAN8 as bigint), 
          ''
        )
      ) AS binary
    ) address_book_hash_key, 
    current_date() load_date, 
    current_date() update_date, 
    'E1' record_source, 
    e1.client_id, 
    a.RNCO company_code, 
    ltrim(rtrim(a.RNKCO)) document_company, 
    ltrim(rtrim(a.RNDOC)) document_number, 
    ltrim(rtrim(a.RNDCT)) document_type, 
    ltrim(rtrim(a.RNSFX)) pay_item, 
    ltrim(rtrim(a.RNSFXE)) pay_item_extension_number, 
    cast(a.RNPYID as bigint) invoice_id, 
    cast(a.RNRC5 as bigint) line_item_id, 
    case when vp.RNPYID is not null then cast(1 as tinyint) else cast(0 as tinyint) end voided_payment_flag, 
    dateadd(day, (c.RMDMTJ % 1000):: int - 1, dateadd(year, (c.RMDMTJ / 1000):: int, '1900-01-01')):: date payment_date, 
    dateadd(day, (c.RMVDGJ % 1000):: int - 1, dateadd(year, (c.RMVDGJ / 1000):: int, '1900-01-01')):: date voided_date, 
    cast(a.RNPAAP / e1.multiplier as numeric(18, 3)) payment_amount_detail, 
    cast(c.RMPAAP / e1.multiplier as numeric(18, 3)) payment_amount, 
    cast(a.RNCTRY * 100 + a.RNFY as smallint) fiscal_year, 
    cast(a.RNPN as tinyint) period_number, 
    c.RMDOCM as payment_number, 
    /* datetimefromparts(c3.Calendar_Year, c3.Calendar_Month, c3.Calendar_Day, round(a.RNUPMT, -4, 1) / 10000, (a.RNUPMT - round(a.RNUPMT, -4, 1)) / 100, a.RNUPMT - round(a.RNUPMT, -2, 1), 0) activity_date */
    current_date() as activity_date 
  from 
    {var_client_custom_db}.raw_ref_f0414_{var_client_name} a 
    Inner Join {var_client_custom_db}.mart_e1_l_oneview_company e1 on a.RNCO = e1.company_code 
/*    Inner join hive_metastore.{var_client_custom_db}.bv_business_units_master_properties_bofa b on e1.company_code = b.company_code (need to check if it is required) */
    Inner join {var_raw_db}.ref_f0413 c on a.RNPYID = c.RMPYID 
--    Inner join {var_client_custom_db}.raw_ref_f0413_{var_client_name} c on a.RNPYID = c.RMPYID 
    Left join voided_payments vp on a.RNPYID = vp.RNPYID 
  where 
    a.RNFY in ((year(current_date()) -2000), (year(current_date()) -2000) -1, (year(current_date()) -2000) + 1)
)
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name, var_raw_db=var_raw_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_s_invoice_payment
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.mart_e1_s_invoice_payment
AS 
(
select 
  distinct client_id, 
  document_hash_key, 
  payment_id_hash_key, 
  invoice_id, 
  payment_date, 
  voided_date, 
  voided_payment_flag, 
  payment_amount, 
  fiscal_year, 
  period_number 
from 
  {var_client_custom_db}.mart_e1_payment_helper_view
)
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_lkp_budget
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.mart_e1_lkp_budget
AS 
(
with lkp_budget_temp AS (
select
    CAST(
      MD5(
        Concat(
          client_id, '|', company_code, 
          '|', account_id, '|', business_unit, 
          '|', object_account, '|', subsidiary
        )
      ) AS binary
    ) budget_hash_key,
	*
from
(select
	dense_rank() over (order by a.BACO, a.BAMCU, a.BAOBJ, a.BASUB) keyid,
	ov.client_id,
	rtrim(a.BACO) company_code,
	rtrim(a.BAAID) account_id,
	rtrim(ltrim(a.BAMCU)) business_unit,
	rtrim(ltrim(a.BAOBJ)) object_account,
	rtrim(ltrim(a.BASUB)) subsidiary,
	rtrim(ltrim(a.BAFLT)) from_ledger_type,
	rtrim(ltrim(a.BALT)) ledger_type,
	/*datetimefromparts(c3.Calendar_Year, c3.Calendar_Month, c3.Calendar_Day, round(a.BAUPMT, -4, 1) / 10000, (a.BAUPMT - round(a.RPUPMT, -4, 1)) / 100, a.BAUPMT - round(a.BAUPMT, -2, 1), 0) activity_date */
	current_date() as activity_date,
	/*cal1.Calendar_Date approval_date,*/
	current_date() as approval_date,
	/*cal.calendar_date request_date,*/
	current_date() as request_date,
	a.BARMK remark,
	a.BABUDG budget_approval,
	a.BAAA amount_raw,
	a.BADRQJ,
	a.BALT,
	row_number() over (partition by a.BACO, a.BAMCU, a.BAOBJ, a.BASUB order by a.BADRQJ desc, a.BAUPMJ desc, a.BAUPMT desc) rnk,
	row_number() over (partition by a.BACO, a.BAMCU, a.BAOBJ, a.BASUB order by a.BAUPMJ asc, a.BAUPMT asc) crono_rnk
from
	{var_client_custom_db}.raw_ref_f550902_{var_client_name} a
	Inner Join 
	{var_client_custom_db}.mart_e1_l_oneview_company ov on A.BACO = ov.company_code 
))
select
	budget_hash_key, 
	client_id, 
	remark, 
	budget_approval,
	amount_raw, 
	request_date,
	approval_date, 
	activity_date,
	ledger_type 
from lkp_budget_temp
where 
rnk = 1
)
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,mart_e1_l_oneview_company
spark.sql(""" 
create or replace view {var_client_custom_db}.mart_e1_l_oneview_company as

with _clt_name as (select oneviewclientid, txtclientname from {var_client_custom_db}.ssdv_vw_oneview_client_property),

_cmp_code as ( 
    select {var_client_id}            client_id,
           trim(a.CCCO)               company_code,
           trim(a.CCNAME)             company_name,
           a.CCCRCD                   currency_code,
           nvl(power(10, b.CVCDEC),1) multiplier 
    from {var_client_custom_db}.raw_ref_f0010_{var_client_name} a 
    join {var_raw_db}.ref_f0013                                 b on a.CCCRCD = b.CVCRCD ),

_client_company_s0 as ( 
    select cc.client_id ,
           cc.company_code ,
           cc.company_name ,
           ov.txtclientname as client_name,
           cc.currency_code ,
           cc.multiplier 
     from _cmp_code cc 
     join _clt_name ov on cc.client_id    = ov.oneviewclientid ),           

_client_company as ( 
    select distinct
           cast(md5(concat('', b.client_id, '|', b.company_code, '')) as binary) oneview_company_hash_key,
           cast(md5(concat('', b.client_id, '|', b.company_code, '|', client_name, '|', company_name, '|', currency_code, '|', multiplier)) as binary) oneview_company_hash_diff,
           cast(md5(concat('', b.client_id, '')) as binary) client_id_hash_key,
           cast(md5(concat('', b.company_code, '')) as binary) company_code_hash_key,
           'MDM' record_source ,
           client_id ,
           company_code ,
           client_name ,
           company_name ,
           currency_code ,
           multiplier 
    from _client_company_s0 b )

select distinct
  record_source 
 ,client_id
 ,company_code
 ,client_name
 ,company_name
 ,currency_code
 ,multiplier 
from _client_company
;   """.format(var_client_custom_db=var_client_custom_db, var_client_id=var_client_id, var_client_name=var_client_name, var_raw_db=var_raw_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_l_account_ledger
spark.sql(""" 
create or replace view {var_client_custom_db}.mart_e1_l_account_ledger as
with
 _balances_tmp as (
select
 o.client_id
,rtrim(GBCO)                                           company_code
,rtrim(GBMCU)                                          business_unit
,rtrim(GBOBJ)                                          object_account
,rtrim(GBSUB)                                          subsidiary
,rtrim(GBAID)                                          account_id
,rtrim(GBLT)                                           ledger_type
,GBCTRY * 100 + GBFY                                   fiscal_year
,rtrim(GBFQ)                                           fiscal_quarter
--,repeat('*', 8 - length(trim(GBSBL))) || trim(GBSBL) subledger_gl
,repeat('', 8 - length(trim(GBSBL))) || trim(GBSBL)    subledger_gl
,rtrim(GBSBLT)                                         subledger_type
,cast(GBAPYC as bigint)                                prior_year_forward_balance
,GBAN01
,GBAN02
,GBAN03
,GBAN04
,GBAN05
,GBAN06
,GBAN07
,GBAN08
,GBAN09
,GBAN10
,GBAN11
,GBAN12
,GBAN13
,GBAN14
,GBAND01
,GBAND02
,GBAND03
,GBAND04
,GBAND05
,GBAND06
,GBAND07
,GBAND08
,GBAND09
,GBAND10
,GBAND11
,GBAND12
,GBAND13
,GBAND14
,rtrim(GBCRCD)                                         currency_code_from
,rtrim(GBCRCX)                                         currency_code_denominated_in
,rtrim(GBPRGF)                                         purge_flag
,o.multiplier
,dateadd(day, (a.GBUPMT % 1000)::int - 1, dateadd(year, (a.GBUPMT / 1000)::int, '1900-01-01'))::date activity_date
,row_number() over(partition by o.client_id order by o.client_id) recordid
from      {var_client_custom_db}.raw_ref_f0902_{var_client_name}  a 
     join {var_client_custom_db}.mart_e1_l_oneview_company        o on a.GBCO = o.company_code
where abs(GBAN01)  + abs(GBAN02)  + abs(GBAN03)  + abs(GBAN04)  + abs(GBAN05)  + abs(GBAN06)  + abs(GBAN07)  + abs(GBAN08)  + abs(GBAN09)  + abs(GBAN10)  + 
      abs(GBAN11)  + abs(GBAN12)  + abs(GBAN13)  + abs(GBAN14)  + abs(GBAND01) + abs(GBAND02) + abs(GBAND03) + abs(GBAND04) + abs(GBAND05) + abs(GBAND06) + 
      abs(GBAND07) + abs(GBAND08) + abs(GBAND09) + abs(GBAND10) + abs(GBAND11) + abs(GBAND12) + abs(GBAND13) + abs(GBAND14) + abs(GBAPYC)> 0
  and GBFY in ((year(current_date()) - 2000), (year(current_date()) - 2000) - 1, (year(current_date()) - 2000) + 1, (year(current_date()) - 2000) - 2)
)
 ,_account_ledger as (
select distinct
 cast(md5(concat('', b.client_id, '|', b.company_code, '|', b.account_id, '|', b.business_unit, '|', b.object_account, '|', b.subsidiary)) as binary) account_ledger_hash_key,
--md5(concat_ws('', b.client_id, '|', b.company_code, '|', b.account_id, '|', b.business_unit, '|', b.object_account, '|', b.subsidiary)) account_ledger_hash_key,
'E1'             record_source
,b.client_id
,b.company_code  company
,b.account_id
,b.business_unit
,b.object_account
,b.subsidiary
from _balances_tmp b
)
select * from _account_ledger
;   """.format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,mart_e1_temp_balances_tmp
spark.sql("""
create or replace table {var_client_custom_db}.mart_e1_temp_balances_tmp as
select
 o.client_id
,rtrim(GBCO)                                           company_code
,rtrim(GBMCU)                                          business_unit
,rtrim(GBOBJ)                                          object_account
,rtrim(GBSUB)                                          subsidiary
,rtrim(GBAID)                                          account_id
,rtrim(GBLT)                                           ledger_type
,GBCTRY * 100 + GBFY                                   fiscal_year
,rtrim(GBFQ)                                           fiscal_quarter
--,repeat('*', 8 - length(trim(GBSBL))) || trim(GBSBL) subledger_gl
,repeat('', 8 - length(trim(GBSBL))) || trim(GBSBL)    subledger_gl
,rtrim(GBSBLT)                                         subledger_type
,cast(GBAPYC as bigint)                                prior_year_forward_balance
,GBAN01
,GBAN02
,GBAN03
,GBAN04
,GBAN05
,GBAN06
,GBAN07
,GBAN08
,GBAN09
,GBAN10
,GBAN11
,GBAN12
,GBAN13
,GBAN14
,GBAND01
,GBAND02
,GBAND03
,GBAND04
,GBAND05
,GBAND06
,GBAND07
,GBAND08
,GBAND09
,GBAND10
,GBAND11
,GBAND12
,GBAND13
,GBAND14
,rtrim(GBCRCD)                                         currency_code_from
,rtrim(GBCRCX)                                         currency_code_denominated_in
,rtrim(GBPRGF)                                         purge_flag
,o.multiplier
,dateadd(day, (a.GBUPMJ % 1000)::int - 1, dateadd(year, (a.GBUPMJ / 1000)::int, '1900-01-01'))::date activity_date
,GBUPMT
,row_number() over(partition by o.client_id order by rtrim(GBCO),rtrim(GBMCU),rtrim(GBOBJ),rtrim(GBSUB),rtrim(GBAID),rtrim(GBLT),rtrim(GBCTRY),trim(GBSBL),rtrim(GBSBLT)) recordid
from      {var_client_custom_db}.raw_ref_f0902_{var_client_name}  a 
     join {var_client_custom_db}.mart_e1_l_oneview_company        o on a.GBCO = o.company_code
where nvl(abs(GBAN01),0)  + nvl(abs(GBAN02),0)  + nvl(abs(GBAN03),0)  + nvl(abs(GBAN04),0)  + nvl(abs(GBAN05),0)  + nvl(abs(GBAN06),0)  + nvl(abs(GBAN07),0)  + 
      nvl(abs(GBAN08),0)  + nvl(abs(GBAN09),0)  + nvl(abs(GBAN10),0)  + nvl(abs(GBAN11),0)  + nvl(abs(GBAN12),0)  + nvl(abs(GBAN13),0)  + nvl(abs(GBAN14),0)  +
      nvl(abs(GBAND01),0) + nvl(abs(GBAND02),0) + nvl(abs(GBAND03),0) + nvl(abs(GBAND04),0) + nvl(abs(GBAND05),0) + nvl(abs(GBAND06),0) + nvl(abs(GBAND07),0) + 
      nvl(abs(GBAND08),0) + nvl(abs(GBAND09),0) + nvl(abs(GBAND10),0) + nvl(abs(GBAND11),0) + nvl(abs(GBAND12),0) + nvl(abs(GBAND13),0) + nvl(abs(GBAND14),0) +  
      nvl(abs(GBAPYC),0)> 0
  and GBFY in ((year(current_date()) - 2000), (year(current_date()) - 2000) - 1, (year(current_date()) - 2000) + 1, (year(current_date()) - 2000) - 2)
;   """.format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,mart_e1_s_balances
spark.sql(""" 
create or replace view {var_client_custom_db}.mart_e1_s_balances as

with _balances_tmp as (
 select * from {var_client_custom_db}.mart_e1_temp_balances_tmp
)
,_balance_amt as (
select 
 recordid
,prior_year_forward_balance
,1       period_number
,GBAND01 net_debit_amount
,GBAN01  net_posting_amount 
from _balances_tmp where abs(GBAND01) + abs(GBAN01) + abs(prior_year_forward_balance) > 0 union
select recordid,0,2  period_number,GBAND02 net_debit_amount,GBAN02 net_posting_amount from _balances_tmp where abs(GBAND02) + abs(GBAN02) > 0 union
select recordid,0,3  period_number,GBAND03 net_debit_amount,GBAN03 net_posting_amount from _balances_tmp where abs(GBAND03) + abs(GBAN03) > 0 union
select recordid,0,4  period_number,GBAND04 net_debit_amount,GBAN04 net_posting_amount from _balances_tmp where abs(GBAND04) + abs(GBAN04) > 0 union
select recordid,0,5  period_number,GBAND05 net_debit_amount,GBAN05 net_posting_amount from _balances_tmp where abs(GBAND05) + abs(GBAN05) > 0 union
select recordid,0,6  period_number,GBAND06 net_debit_amount,GBAN06 net_posting_amount from _balances_tmp where abs(GBAND06) + abs(GBAN06) > 0 union
select recordid,0,7  period_number,GBAND07 net_debit_amount,GBAN07 net_posting_amount from _balances_tmp where abs(GBAND07) + abs(GBAN07) > 0 union
select recordid,0,8  period_number,GBAND08 net_debit_amount,GBAN08 net_posting_amount from _balances_tmp where abs(GBAND08) + abs(GBAN08) > 0 union
select recordid,0,9  period_number,GBAND09 net_debit_amount,GBAN09 net_posting_amount from _balances_tmp where abs(GBAND09) + abs(GBAN09) > 0 union
select recordid,0,10 period_number,GBAND10 net_debit_amount,GBAN10 net_posting_amount from _balances_tmp where abs(GBAND10) + abs(GBAN10) > 0 union
select recordid,0,11 period_number,GBAND11 net_debit_amount,GBAN11 net_posting_amount from _balances_tmp where abs(GBAND11) + abs(GBAN11) > 0 union
select recordid,0,12 period_number,GBAND12 net_debit_amount,GBAN12 net_posting_amount from _balances_tmp where abs(GBAND12) + abs(GBAN12) > 0 union
select recordid,0,13 period_number,GBAND13 net_debit_amount,GBAN13 net_posting_amount from _balances_tmp where abs(GBAND13) + abs(GBAN13) > 0 union
select recordid,0,14 period_number,GBAND14 net_debit_amount,GBAN14 net_posting_amount from _balances_tmp where abs(GBAND14) + abs(GBAN14) > 0
)
,_balance_pivot as (
select 
 recordid
,prior_year_forward_balance
,net_debit_amount
,net_posting_amount
,period_number 
from _balance_amt 
where prior_year_forward_balance <> 0 or net_debit_amount <> 0 or net_posting_amount <> 0
)
,_s_balances as (
select
 cast(md5(concat('', a.client_id, '|', a.company_code, '|', a.account_id, '|', a.business_unit, '|', a.object_account, '|', a.subsidiary)) as binary) account_ledger_hash_key
-- md5(concat_ws('', a.client_id, '|', a.company_code, '|', a.account_id, '|', a.business_unit, '|', a.object_account, '|', a.subsidiary)) account_ledger_hash_key
,'E1'                                            record_source
,a.client_id
,a.company_code
,a.recordid
,a.business_unit
,a.object_account
,a.subsidiary
,a.account_id
,a.ledger_type
,a.fiscal_year
,a.fiscal_quarter
,a.subledger_gl
,a.subledger_type
,a.currency_code_from
,a.currency_code_denominated_in
,a.purge_flag
,b.prior_year_forward_balance / a.multiplier      prior_year_forward_balance
,b.period_number::int
,round(b.net_debit_amount/a.multiplier,2)         net_debit_amount
,round(b.net_posting_amount/a.multiplier,2)       net_posting_amount
,a.activity_date
,company_code
from      _balances_tmp  a
     join _balance_pivot b on a.recordid = b.recordid
)
select 
 account_ledger_hash_key
,recordid
,client_id
,company_code
,fiscal_year
,period_number
,fiscal_quarter
,ledger_type
,subledger_gl
,subledger_type
,currency_code_from
,currency_code_denominated_in
,purge_flag
,prior_year_forward_balance
,net_debit_amount
,net_posting_amount
,activity_date
from _s_balances
;   """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_h_ap_ledger
spark.sql("""
create or replace view {var_client_custom_db}.mart_e1_h_ap_ledger as
select 
 ap_ledger_hash_key, 
 load_date, 
 record_source, 
 client_id, 
 document_company, 
 document_number, 
 document_type, 
 pay_item,
 pay_item_extension_number 
from {var_client_custom_db}.mart_e1_ap_ledger_helper_view
;   """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_s_ap_ledger
spark.sql("""
create or replace view {var_client_custom_db}.mart_e1_s_ap_ledger as
select 
 ap_ledger_hash_key, 
 ap_ledger_hash_diff, 
 record_source, 
 load_date, 
 update_date, 
 client_id, 
 invoice_number, 
 reference, 
 pay_status_code, 
 amount_gross, 
 amount_taxable, 
 invoice_date, 
 invoice_date_original,
 invoice_due_date ,
 gl_date, 
 currency_code,
 purchase_order, 
 business_unit, 
 object_account, 
 remark,
 payment_type_code,
 payment_type_description,
 fiscal_year, 
 period_number,  
 activity_date,
 amount_to_distribute, 
 company_code, 
 funding_number 
from {var_client_custom_db}.mart_e1_ap_ledger_helper_view
;   """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_s_payment
spark.sql("""
create or replace view {var_client_custom_db}.mart_e1_s_payment as
select distinct 
 payment_hash_key, 
 load_date, 
 record_source, 
 payment_date, 
 voided_date, 
 payment_amount, 
 payment_number 
FROM  {var_client_custom_db}.mart_e1_payment_helper_view
;   """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_l_payment_details
spark.sql("""
create or replace view {var_client_custom_db}.mart_e1_l_payment_details as
select distinct
  cast(md5(concat(a.client_id, '|', a.document_company, '|', a.document_number, '|',  a.document_type, '|', a.pay_item, '|', a.pay_item_extension_number, (case when b.invoice_id is null   
  then -1 else b.invoice_id end) , '|', a.address_id)) as binary) payment_hash_key,
  a.ap_ledger_hash_key,
  a.document_hash_key,
--case when b.payment_id_hash_key is null then 	 CAST( MD5( Concat('-1' as bigint, '|','')) AS binary)  else b.payment_id_hash_key end as payment_id_hash_key,
  null as  payment_id_hash_key,
  a.address_book_hash_key,
  a.load_date,
  a.record_source,
  a.client_id,
  a.document_company,
  a.document_number,
  a.document_type,
  a.pay_item,
  a.pay_item_extension_number,
  case when b.invoice_id is null then -1 
       else b.invoice_id 
  end as invoice_id,
  case when b.voided_payment_flag is null then 0 
       else b.voided_payment_flag 
  end as voided_payment_flag
	       from {var_client_custom_db}.mart_e1_ap_ledger_helper_view a
left outer join {var_client_custom_db}.mart_e1_payment_helper_view   b on a.ap_ledger_hash_key = b.ap_ledger_hash_key
;   """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,mart_e1_s_address_book
spark.sql(""" 
create or replace view {var_client_custom_db}.mart_e1_s_address_book as

with _address_src as (
 select
 f1.ABAN8                address_id
,trim(f1.ABALPH)         name
,trim(f1.ABTAXC)         person_corporation_code
,trim(f1.ABTAX)          tax_id
,trim(f1.ABTX2)          personal_tax_id
,trim(f1.ABAC18)         supplier_type_code
,trim(nvl(f5.DRDL01,'')) supplier_type_description
,trim(f1.ABATPR)         supplier_flag
,trim(f6.ALADD1)         address_line_1
,trim(f6.ALADD2)         address_line_2
,trim(f6.ALADD3)         address_line_3
,trim(f6.ALADD3)         address_line_4
,trim(f6.ALADDZ)         postal_code
,trim(f6.ALCTY1)         city
,trim(f6.ALADDS)         state_code
,trim(us.DRDL01)         state
,trim(f6.ALCTR)          country_code
,trim(cn.DRDL01)         country
,case when f1.ABALPH like '%dummy%' then 'Y' else 'N' end dummy_flag
,row_number() over (partition by ALAN8 order by ALEFTB desc) nr 
from           {var_raw_db}.ref_f0101 f1
     left join {var_raw_db}.ref_f0005 f5 on trim(nvl(f1.ABAC18,'')) = trim(nvl(f5.DRKY,'')) and trim(f5.DRRT)  = '18' and trim(f5.DRSY) = '01'
     left join {var_raw_db}.ref_f0116 f6 on trim(nvl(f1.ABAN8,''))  = trim(nvl(f6.ALAN8,''))
     left join {var_raw_db}.ref_f0005 us on trim(nvl(f6.ALADDS,'')) = trim(nvl(us.DRKY,'')) and trim(us.DRRT)  = 'S'  and trim(us.DRSY) = '00'
     left join {var_raw_db}.ref_f0005 cn on trim(nvl(f6.ALCTR,''))  = trim(nvl(cn.DRKY,'')) and trim(cn.DRRT)  = 'CN' and trim(cn.DRSY) = '00'
)

,_address_raw as (
 select
 address_id::int
,name
,person_corporation_code
,tax_id
,personal_tax_id
,supplier_type_code
,supplier_type_description
,supplier_flag
,dummy_flag
,address_line_1
,address_line_2
,address_line_3
,address_line_4
,postal_code
,city
,state_code
,state
,country_code
,country
from _address_src where nr = 1
union all
select
 -1               address_id
,ltrim(trim(''))  name
,ltrim(trim(''))  person_corporation_code
,ltrim(trim(''))  tax_id
,ltrim(trim(''))  personal_tax_id
,ltrim(trim(''))  supplier_type_code
,ltrim(trim(''))  supplier_type_description
,ltrim(trim('N')) supplier_flag
,ltrim(trim(''))  address_line_1
,ltrim(trim(''))  address_line_2
,ltrim(trim(''))  address_line_3
,ltrim(trim(''))  address_line_4
,ltrim(trim(''))  postal_code
,ltrim(trim(''))  city
,ltrim(trim(''))  state_code
,ltrim(trim(''))  state
,ltrim(trim(''))  country_code
,ltrim(trim(''))  country
,ltrim(trim('N')) dummy_flag
)
select
 'E1'                              record_source
,cast(md5( Concat( address_id ,'')) AS binary) address_book_hash_key
--,md5(concat_ws(address_id ,''))  address_book_hash_key
,address_id
,nvl(name,'')                      name
,nvl(person_corporation_code,'')   person_corporation_code
,nvl(tax_id,'')                    tax_id
,nvl(personal_tax_id,'')           personal_tax_id
,nvl(supplier_type_code,'')        supplier_type_code
,nvl(supplier_type_description,'') supplier_type_description
,nvl(supplier_flag,'')             supplier_flag
,nvl(dummy_flag,'')                dummy_flag
,nvl(address_line_1,'')            address_line_1
,nvl(address_line_2,'')            address_line_2
,nvl(address_line_3,'')            address_line_3
,nvl(address_line_4,'')            address_line_4
,nvl(postal_code,'')               postal_code
,nvl(city,'')                      city
,nvl(state_code,'')                state_code
,nvl(state,'')                     state
,nvl(country_code,'')              country_code
,nvl(country,'')                   country
from _address_raw
;   """.format(var_client_custom_db=var_client_custom_db, var_raw_db=var_raw_db, var_client_name=var_client_name))