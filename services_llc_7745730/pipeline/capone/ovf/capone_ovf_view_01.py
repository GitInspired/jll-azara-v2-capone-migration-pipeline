# Databricks notebook source
# DBTITLE 1,bv_business_units_master_properties
spark.sql("""CREATE OR REPLACE VIEW jll_azara_catalog.jll_azara_0007745730_capitalone_custom.bv_business_units_master_properties
AS
SELECT DISTINCT
 OXMCU as hk_business_unit
,RIGHT('00000' || LEFT(LTRIM(RTRIM(OXCO)),5),5) as hk_financial_company
,OXY55OVCID as hk_h_master_clients
,OXY55OVCPID as hk_h_master_properties
,RIGHT('00000' || LEFT(LTRIM(RTRIM(OXCO)),5),5) as company_code
,OXMCU as business_unit
,OXY55OVCID as client_id
,OXY55OVCPID as property_id
FROM jll_azara_catalog.jll_azara_raw.ref_f55ovcpx_bofa where OXY55OVCID = '7745730'
""")

# COMMAND ----------

# DBTITLE 1,ssdv_vw_ovf_commitmentdetail
spark.sql("""
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.vw_ssdv_vw_ovf_commitmentdetail as
with _paiddetails as (
SELECT 
 f1.ODMCU
,fd.VDDOCO ComNumber
,fd.VDLNID LinNumber
,CAST(sum(fd.VDAG/100) AS DECIMAL(38,6)) AmountPaid
FROM           jll_azara_catalog.jll_azara_raw.ref_f554301  f1
     left join jll_azara_catalog.jll_azara_raw.ref_f564314d fd on f1.ODDOCO = fd.VDDOC1
where trim(ODKCOO) IN ('00063','02456','25521','44011','44012','44014','44015','44021','48233')
  and ODDCT = 'XP'
group by ODMCU, VDDOCO, VDLNID
)
,_f4311 as (
SELECT PDAEXP ,PDDOCO ,PDDSC1 ,PDLNID ,PDMCU ,PDOBJ ,PDSUB, PDDCTO ,NVL(PDAITM,'') PDAITM
FROM jll_azara_catalog.jll_azara_raw.ref_f4311
)
,_final as (
Select
 f1.PHMCU            as Project_Number
,f6.MCDC             as Project_Name
,f1.PHOORN           as Commitment_Number
,f4.PDAITM           as Detail_Description
,trim(f4.PDOBJ) || '.' || trim(f4.PDSUB) as Cost_Code
,CASE WHEN f4.PDOBJ like '%116100%' THEN 'CAP' ELSE 'EXP' END as Cap_Exp
,f4.PDDSC1           as Account_Description
,CAST(f4.PDAEXP/100 AS DECIMAL(38,2))    Commitment_Amount
,CASE WHEN trim(PDSUB) IN ('40605000','40605100','40605200','40605300','40605995','40605996','40605997','40605998','40605999') THEN 'Y' ELSE 'N' END as Tax_Y_N
,f1.PHAN8            as Vendor_ID
,vd.ABALPH           as Vendor_Name
,pd.AmountPaid       as Total_Paid
FROM           jll_azara_catalog.jll_azara_raw.ref_f4301 f1
     LEFT JOIN _f4311                                    f4 on f1.PHMCU = f4.PDMCU and CASE WHEN f1.PHOORN not rlike '^0-9' THEN 1 ELSE 0 END = 1 and f1.PHOORN = f4.PDDOCO
     LEFT JOIN jll_azara_catalog.jll_azara_raw.ref_f0006 f6 on f1.PHMCU = f6.MCMCU
     LEFT JOIN jll_azara_catalog.jll_azara_raw.ref_f0101 vd on f1.PHAN8 = vd.ABAN8
     LEFT JOIN _paiddetails                                   pd on f1.PHMCU = pd.ODMCU and f1.PHOORN = pd.ComNumber and f4.PDLNID = pd.LinNumber
WHERE trim(PHOKCO) IN ('00063','02456','25521','44011','44012','44014','44015','44021','48233')
  and PHDCTO = 'OS'
  and PDDCTO = 'OS'
)
select
 CAST(project_number AS STRING)   as   project_number
,CAST(project_name AS STRING)   as   project_name
,CAST(commitment_number AS STRING)   as   commitment_number
,CAST(detail_description AS STRING)   as   detail_description
,CAST(cost_code AS STRING)   as   cost_code
,CAST(cap_exp AS STRING)   as   cap_exp
,CAST(account_description AS STRING)   as   account_description
,CAST(commitment_amount AS DECIMAL(38,2))   as   commitment_amount
,CAST(tax_y_n AS STRING)   as   tax_y_n
,CAST(vendor_id AS DECIMAL(8,0))   as   vendor_id
,CAST(vendor_name AS STRING)   as   vendor_name
,CAST(total_paid AS decimal(38,6))   as   total_paid
from _final

""")

# ref_f4311 no data

# COMMAND ----------

# DBTITLE 1,ssdv_vw_ovf_tm1_detailanalysiscube
spark.sql("""
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.vw_ssdv_vw_ovf_tm1_detailanalysiscube as
with _detailanalysiscube  AS (
SELECT 
 a.hk_ref_DetailAnalysisCube
,a.ClientID
,a.Version
,a.Currency
,a.Year
,a.Month
,a.Company
,a.SubAccount
,a.GLAccount
,a.Subledger
,a.BusUnit
,a.Vendor
,a.Scale_Measure
,a.MeasureType
,a.Value
,a.LastUpdated
,a.source_id
,a.dss_record_source
,a.dss_load_date
,a.dss_create_time
FROM  jll_azara_catalog.jll_azara_raw.ref_detailanalysiscube a
)
,_companyattr as (
SELECT 
 a.hk_ref_CompanyAttr
,a.ClientID
,a.Company
,a.CodeName
,a.CreationDate
,a.Currency
,a.DisplayName
,a.Ledger
,a.Name
,a.NameCode
,a.Scale
,a.sDateAttr
,a.SecurityGrp
,a.TM1E1
,a.UnCount
,a.LastUpdated
,a.source_id
,a.dss_record_source
,a.dss_load_date
,a.dss_create_time
FROM jll_azara_catalog.jll_azara_raw.ref_companyattr a
where a.company in ('00063','02456','25521','44011','44012','44014','44015','44021','48233')
)
select
 d.clientid
,d.version
,d.currency
,d.year
,d.month
,d.company
,d.subaccount as sub_account
,d.glaccount  as gl_account
,NVL(d.subledger,'') AS subledger
,d.busunit
,NVL(d.vendor,'') AS vendor
,d.scale_measure
,d.measuretype
,d.value
,d.lastupdated
from      _detailanalysiscube d
     join _companyattr        c on d.company = c.company
where trim(c.company) in ('00063','02456','25521','44011','44012','44014','44015','44021','48233')

""")

# COMMAND ----------

# DBTITLE 1,ssdv_vw_ovf_financial_workorderinvoice
spark.sql("""
create or replace view jll_azara_catalog.jll_azara_0007745730_capitalone_custom.vw_ssdv_vw_ovf_financial_workorderinvoice AS
with _final as (
SELECT
 f1.RPKCO                               AS Client_Nbr
,f1.RPVR01                              AS Work_Order
,f1.RPVINV                              AS Invoice_Number
,CASE f1.RPPST WHEN 'A' THEN 'Approved for payment' WHEN 'H' THEN 'Held pending approval' WHEN 'P' THEN 'Paid' END AS Invoice_Status
,f1.RPAG/100                            AS Invoice_Total
,(f1.RPAG - (f1.RPAG - f1.RPATXA)) /100 AS Invoice_Amount
,(f1.RPAG - f1.RPATXA)/100              AS Tax_Amount
,iff(length(f1.RPDICJ) <= 4, null, dateadd(day, (f1.RPDICJ % 1000)::int - 1, dateadd(year, (f1.RPDICJ / 1000)::int, '1900-01-01')))::date AS  Invoice_Date
,iff(length(f1.RPDDJ)  <= 4, null, dateadd(day, (f1.RPDDJ  % 1000)::int - 1, dateadd(year, (f1.RPDDJ  / 1000)::int, '1900-01-01')))::date AS  Invoice_Due_Date
,iff(length(f3.RMDMTJ) <= 4, null, dateadd(day, (f3.RMDMTJ % 1000)::int - 1, dateadd(year, (f3.RMDMTJ / 1000)::int, '1900-01-01')))::date AS  Payment_Date
,f1.RPCRCD                              AS Currency_Code
FROM           jll_azara_catalog.jll_azara_raw.ref_f0411 f1
     LEFT JOIN jll_azara_catalog.jll_azara_raw.ref_f0414 f4 ON f1.RPDOC  = f4.RNDOC and f1.RPDCT = f4.RNDCT  and f1.RPCO = f4.RNCO
     LEFT JOIN jll_azara_catalog.jll_azara_raw.ref_f0413 f3 ON f4.RNPYID = f3.RMPYID
WHERE rtrim(RPKCO) IN ('00063','02456','25521','44011','44012','44014','44015','44021','48233')
  AND RPVR01 IS NOT NULL AND RPVR01 <> ''
)
select 
 CAST(client_nbr AS STRING)   as   client_nbr 
,CAST(work_order AS STRING)   as   work_order
,CAST(invoice_number AS STRING)   as   invoice_number
,CAST(invoice_status AS STRING)   as   invoice_status
,CAST(invoice_total AS decimal(21,6))   as   invoice_total
,CAST(invoice_amount AS decimal(23,6))   as   invoice_amount
,CAST(tax_amount AS decimal(22,6))   as   tax_amount
,CAST(invoice_date AS DATE)   as   invoice_date
,CAST(invoice_due_date AS TIMESTAMP)   as   invoice_due_date
,CAST(payment_date AS TIMESTAMP)   as   payment_date
,CAST(currency_code AS STRING)   as   currency_code
from _final

""")

# ref_f0411 no data

# COMMAND ----------

spark.sql("""drop view  jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_ovf_financial_workorderinvoice  """)
# spark.sql("""drop view  jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_ovf_tm1_detailanalysiscube  """)
# spark.sql("""drop view  jll_azara_catalog.jll_azara_0007745730_capitalone_custom.ssdv_vw_ovf_commitmentdetail  """)
