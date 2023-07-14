# Databricks notebook source
# MAGIC %run ./notebook_variables

# COMMAND ----------

# DBTITLE 1,ssdv_vw_ovf_tm1_detailanalysiscube
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.ssdv_vw_ovf_tm1_detailanalysiscube AS
SELECT 
    d.clientid as clientid,
    d.version as version,
    d.currency as currency,
    d.year as year,
    d.month as month,
    d.company as company,
    d.subaccount as sub_account,
    d.glaccount as gl_account,
    d.subledger as subledger,
    d.busunit as busunit,
    d.vendor as vendor,
    d.scale_measure as scale_measure,
    d.measuretype as measuretype,
    d.value as value,
    cast(d.lastupdated as date) as lastupdated
from      
     {var_client_custom_db}.raw_ref_detailanalysiscube_{var_client_name} d 
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name ))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_OVF_CommitmentDetail
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.ssdv_vw_ovf_commitmentdetail
AS 
    with PaidDetails as 
    (
		 SELECT 
             ODMCU
             ,VDDOCO comnumber
             ,VDLNID linnumber
             ,Sum(VDAG/100) amountpaid
		 FROM 
             {var_client_custom_db}.raw_ref_f554301_{var_client_name} f1
         join
             {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
                on f1.ODKCOO = F0010.CCCO 
		 left join 
             {var_client_custom_db}.raw_ref_f564314d_{var_client_name} fd 
                on f1.ODDOCO = fd.VDDOC1
		 where 
              ODDCT = 'XP'
		 group by
              ODMCU, VDDOCO, VDLNID
	)
		  
    Select
         F4301.PHMCU as project_number
		,F0006.MCDC as project_name
		,F4301.PHOORN as commitment_number
		,F4311.PDAITM as detail_description
		,LTRIM(RTRIM(F4311.PDOBJ)) || '.' || LTRIM(RTRIM(F4311.PDSUB)) as cost_code
        ,CASE 
			WHEN F4311.PDOBJ like '%116100%' THEN 'CAP'
			ELSE 'EXP'
	     END as cap_exp
		,F4311.PDDSC1 as account_description
		,F4311.PDAEXP/100 as commitment_amount
		,CASE 
			WHEN PDSUB IN       ('40605000','40605100','40605200','40605300','40605995','40605996','40605997','40605998','40605999')             THEN 'Y' ELSE 'N'
		  END as Tax_Y_N
		 ,F4301.PHAN8 as vendor_id
		 ,vendor.ABALPH as vendor_name
		 ,PaidDetails.AmountPaid as total_paid
    FROM 
        {var_client_custom_db}.raw_ref_f4301_{var_client_name} F4301
    JOIN
        {var_client_custom_db}.raw_ref_f0010_{var_client_name} F0010
            on F4301.PHOKCO = F0010.CCCO 
	LEFT JOIN 
        {var_client_custom_db}.raw_ref_f4311_{var_client_name} F4311
			on F4301.PHMCU = F4311.PDMCU
            and (CASE 
                    WHEN F4301.PHOORN not rlike '^0-9' 
                    THEN 1 ELSE 0 
                 END) = 1
			and F4301.PHOORN = F4311.PDDOCO
    LEFT JOIN 
        {var_client_custom_db}.raw_ref_f0006_{var_client_name} F0006
			on F4301.PHMCU = F0006.MCMCU
    LEFT JOIN 
        {var_raw_db}.ref_f0101 as vendor
			on  F4301.PHAN8 = vendor.ABAN8
    LEFT JOIN 
         PaidDetails
			on  F4301.PHMCU = PaidDetails.ODMCU
			and F4301.PHOORN = PaidDetails.comnumber
			and F4311.PDLNID = PaidDetails.linnumber
    WHERE 
	        PHDCTO = 'OS'
	    and PDDCTO = 'OS'
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name, var_raw_db=var_raw_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_ovf_business_unit_master
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.ssdv_vw_ovf_business_unit_master
AS
SELECT
          client_id, 
          business_unit, 
          business_unit_type, 
          business_unit_description_compressed, 
          business_unit_level_of_detail, 
          company_code, 
          address_id, 
          address_id_job_ar, 
          county_code, 
          state_code, 
          model_account_consolidation_flag, 
          business_unit_description, 
          business_unit_description02, 
          business_unit_description03, 
          business_unit_description04, 
          division_code, 
          region_code, 
          group_code, 
          branch_office_code, 
          department_type_code, 
          person_responsible_code, 
          line_of_business_code, 
          category_code_08, 
          category_code_09, 
          category_code_10, 
          category_code_11, 
          category_code_12, 
          category_code_13, 
          category_code_14, 
          category_code_15, 
          category_code_16, 
          category_code_17, 
          category_code_18, 
          category_code_19, 
          category_code_20, 
          category_code_21, 
          category_code_22, 
          category_code_23, 
          category_code_24, 
          category_code_25, 
          category_code_26, 
          category_code_27, 
          category_code_28, 
          category_code_29, 
          category_code_30, 
          category_code_31, 
          category_code_32, 
          category_code_33, 
          category_code_34, 
          category_code_35, 
          category_code_36, 
          category_code_37, 
          category_code_38, 
          category_code_39, 
          category_code_40, 
          category_code_41, 
          category_code_42, 
          category_code_43, 
          category_code_44, 
          category_code_45, 
          category_code_46, 
          category_code_47, 
          category_code_48, 
          category_code_49, 
          category_code_50, 
          posting_edit_business_unit, 
          subledger_inactive_code, 
          division_description, 
          region_description, 
          branch_office_description, 
          department_type_description, 
          person_responsible_description, 
          line_of_business_description, 
          category_description_08, 
          category_description_09, 
          category_description_10, 
          category_description_11, 
          category_description_12, 
          category_description_13, 
          category_description_14, 
          category_description_15, 
          category_description_16, 
          category_description_17, 
          category_description_18, 
          category_description_19, 
          category_description_20, 
          category_description_21, 
          category_description_22, 
          category_description_23, 
          category_description_24, 
          category_description_25, 
          category_description_26, 
          category_description_27, 
          category_description_28, 
          category_description_29, 
          category_description_30, 
          category_description_31, 
          category_description_32, 
          category_description_33, 
          category_description_34, 
          category_description_35, 
          category_description_36, 
          category_description_37, 
          category_description_38, 
          category_description_39, 
          category_description_40, 
          category_description_41, 
          category_description_42, 
          category_description_43, 
          category_description_44, 
          category_description_45, 
          category_description_46, 
          category_description_47, 
          category_description_48, 
          category_description_49, 
          category_description_50, 
          date_updated, 
          time_updated
from 
  {var_client_custom_db}.mart_e1_s_business_unit_master 
-- where client_id = '7745730'
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_ovf_ledger_actuals
spark.sql("""
CREATE OR REPLACE VIEW 
{var_client_custom_db}.ssdv_vw_ovf_ledger_actuals
AS
Select 
  il.company_code, 
  il.account_id, 
  il.account_number, 
  il.activity_date, 
  il.amount, 
  il.base_currency, 
  il.business_unit, 
  il.document_company, 
  il.document_number, 
  il.document_type, 
  il.explanation, 
  cast(il.fiscal_period as string) fiscal_period , 
  cast(il.fiscal_year as string) fiscal_year , 
  il.gl_date, 
  il.invoice_date, 
  il.invoice_number, 
  il.ledger_type, 
  il.object_account, 
  il.period_number, 
  il.purchase_order, 
  il.subledger, 
  il.subledger_type, 
  il.subsidiary, 
  cast(il.supplier as string) supplier, 
  ab.name as supplier_name, 
  il.transaction_currency, 
  il.transaction_originator, 
  il.work_order, 
  ac.description, 
  ac.gl_reporting_code_9, 
  lh.invoice_status, 
  lh.reference, 
  il.journal_entry_line_number, 
  il.line_extension_code, 
  bud.remark, 
  pay.payment_date, 
  il.payment_id, 
  il.supplier_name as alpha_explanation, 
  il.posted as posted_code 
FROM 
  {var_client_custom_db}.mart_e1_s_general_ledger il 
  INNER JOIN {var_client_custom_db}.mart_e1_lkp_account ac on il.client_id = ac.client_id 
  and il.account_id_hash_key = ac.account_id_hash_key 
  LEFT OUTER JOIN {var_client_custom_db}.mart_e1_lkp_ap_ledger_header lh on il.client_id = lh.client_id 
  and il.document_hash_key = lh.document_hash_key 
  LEFT OUTER JOIN {var_client_custom_db}.mart_e1_lkp_budget bud on il.client_id = bud.client_id 
  and il.budget_hash_key = bud.budget_hash_key 
  LEFT OUTER JOIN ( Select client_id, document_hash_key, MAX(payment_date) payment_date, MAX(voided_date) voided_date From {var_client_custom_db}.mart_e1_s_invoice_payment Group By client_id, document_hash_key ) pay on il.client_id = pay.client_id 
  and il.document_hash_key = pay.document_hash_key 
  LEFT OUTER JOIN {var_client_custom_db}.mart_e1_s_address_book as ab ON il.supplier = ab.address_id 
  where il.client_id = '{var_client_id}'
""".format(var_client_custom_db=var_client_custom_db, var_client_id=var_client_id ))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_ovf_financial_workorderinvoice
spark.sql("""
CREATE OR REPLACE VIEW  
{var_client_custom_db}.ssdv_vw_ovf_financial_workorderinvoice AS 
SELECT 
  F0411.RPKCO AS Client_Nbr
	 , F0411.RPVR01 AS Work_Order
     , F0411.RPVINV AS Invoice_Number
	 , CASE F0411.RPPST
	     WHEN 'A' THEN 'Approved for payment' 
	     WHEN 'H' THEN 'Held pending approval' 
	     WHEN 'P' THEN 'Paid' 
	   END AS Invoice_Status
	 , F0411.RPAG/100 AS Invoice_Total
	 ,(F0411.RPAG - (F0411.RPAG - F0411.RPATXA)) /100 AS  Invoice_Amount
	 , (F0411.RPAG - F0411.RPATXA)/100 AS Tax_Amount
  ,dateadd(day, (f0411.RPDICJ % 1000)::int - 1, dateadd(year, (f0411.RPDICJ / 1000)::int, '1900-01-01'))::date 
  AS Invoice_Date 
, iff(length(f0411.RPDDJ) <= 4, null, 
  dateadd(day, (f0411.RPDDJ % 1000)::int - 1, dateadd(year, (f0411.RPDDJ / 1000)::int, '1900-01-01')))::date 
  AS Invoice_Due_Date 
, iff(length(f0413.RMDMTJ) <= 4, null, dateadd(day, (f0413.RMDMTJ % 1000)::int - 1, dateadd(year, (f0413.RMDMTJ / 1000)::int, '1900-01-01')))::date 
  AS Payment_Date  
,f0411.RPCRCD AS Currency_Code 
 FROM {var_client_custom_db}.raw_ref_f0411_{var_client_name} f0411
	LEFT  JOIN {var_client_custom_db}.raw_ref_f0414_{var_client_name} f0414
	 ON F0411.RPDOC = F0414.RNDOC 
	 AND F0411.RPDCT = F0414.RNDCT 
	 AND F0411.RPCO = F0414.RNCO
	LEFT  JOIN {var_raw_db}.ref_f0413 f0413
	 ON F0414.RNPYID = F0413.RMPYID 
	 inner join {var_client_custom_db}.raw_ref_f0010_{var_client_name} f0010
	 on  f0411.RPKCO=f0010.CCCO 
	WHERE UPPER(f0010.CCNAME) like 'CAPITAL ONE%' AND 
	f0411.RPVR01 IS NOT NULL AND f0411.RPVR01 <> ''  
""".format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name, var_raw_db=var_raw_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_ovf_account_balance
spark.sql(""" 
create or replace view {var_client_custom_db}.ssdv_vw_ovf_account_balance as

select
    b.fiscal_year,
    b.period_number as posting_month,
    b.ledger_type,
    b.subledger_gl,
    a.company,
    a.business_unit,
    a.object_account,
    a.subsidiary,
    b.subledger_type,
    b.currency_code_from,
    b.currency_code_denominated_in,
    b.purge_flag,
    b.net_debit_amount,
    b.net_posting_amount,
    b.prior_year_forward_balance,
    b.period_number
from {var_client_custom_db}.mart_e1_l_account_ledger a
join {var_client_custom_db}.mart_e1_s_balances       b on a.account_ledger_hash_key = b.account_ledger_hash_key and a.client_id = b.client_id
;   """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,ssdv_vw_ovf_invoice_payment
spark.sql(""" 
create or replace view {var_client_custom_db}.ssdv_vw_ovf_invoice_payment as

with _invoice_payment as (
select
    b.client_id,
    b.Document_company                     client_nbr,
    b.document_number,
    b.document_type,
    b.pay_item,
    b.pay_item_extension_number,
    a.Reference                            reference,
    a.Invoice_Number                       invoice_number,
    case a.Pay_Status_Code 
      when 'A' then 'Approved for payment' 
      when 'H' then 'Held pending Approval' 
      when 'P' then 'Paid' 
      else a.Pay_Status_Code
    end                                    invoice_status,
    a.amount_gross                         invoice_total,
    a.amount_gross - case when a.amount_taxable > 0 
                          then (a.amount_gross - a.amount_taxable) 
                     else 0 
                     end                   invoice_amount,
    case when a.amount_taxable > 0 
         then a.amount_gross - a.amount_taxable 
         else 0 
    end                                    tax_amount,
    a.Invoice_Date                         invoice_date,
    a.invoice_date_original                invoice_date_original,
    a.invoice_due_date                     invoice_due_date,
    a.gl_date                              gl_date,
    a.Remark                               remark,
    a.payment_type_code,
    a.payment_type_description,
    nullif(pay.Payment_Date, '1900-01-01') payment_date,
    a.Currency_Code                        currency_code,
    a.business_unit                        business_unit,
    ab.address_id                          supplier_id,
    ab.name                                supplier_name,
    ab.Person_Corporation_Code             person_corporation_code,
    ab.Tax_ID                              tax_id, 
    ab.Personal_Tax_ID                     personal_tax_id,
    address_line_1                         address1,
    address_line_2                         address2,
    address_line_3                         address3,
    address_line_4                         address4,
    Postal_Code                            postal_code,
    city,
    state_code,
    state,
    country_code,
    country,
    a.Purchase_Order                       purchase_order,
    ab.address_id                          supplier_number,
    case when p.voided_payment_flag = 1 then 'Yes' 
         else null 
    end                                    is_voided,
    nullif(pay.Voided_Date, '1900-01-01')  voided_date,
    pay.Payment_Amount                     payment_amount,
    a.period_number                        posting_month,
    a.fiscal_year,
    a.object_account,
    a.amount_to_distribute,
    a.company_code,
    p.invoice_id,
    a.funding_number,
    pay.payment_number
     from {var_client_custom_db}.mart_e1_h_ap_Ledger       b   
     join {var_client_custom_db}.mart_e1_s_ap_Ledger       a   on a.ap_ledger_hash_key    = b.ap_ledger_hash_key and b.client_id = a.client_id
left join {var_client_custom_db}.mart_e1_l_payment_details p   on b.ap_ledger_hash_key    = p.ap_ledger_hash_key
left join {var_client_custom_db}.mart_e1_s_payment         pay on p.payment_hash_key      = pay.payment_hash_key
left join {var_client_custom_db}.mart_e1_s_address_book    ab  on p.address_book_hash_key = ab.address_book_hash_key)

select 
client_nbr,
document_number,
document_type,
pay_item,
pay_item_extension_number,
reference,
invoice_number,
invoice_status,
invoice_total,
invoice_amount,
tax_amount,
invoice_date,
payment_date,
currency_code,
business_unit,
supplier_id,
supplier_name,
person_corporation_code,
tax_id,
personal_tax_id,
purchase_order,
supplier_number,
is_voided,
voided_date,
payment_amount,
posting_month,
fiscal_year,
object_account,
amount_to_distribute,
company_code,
funding_number,
payment_number
from _invoice_payment
;   """.format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,clientssdv_vw_ovf_invoice_payment
spark.sql(""" 
create or replace view {var_client_custom_db}.clientssdv_vw_ovf_invoice_payment as

with _funding as ( 
  SELECT   
    MAX(XF.ODDOC) as fundingno   
   ,XP.ODDOCO   
   ,XP.ODYN   
  FROM (
   select   
      F554301.ODDOC   
     ,F554301.ODDOCO   
     ,F554301.ODDCT   
     ,F554301.ODUPMJ   
     ,F554301.ODYN   
     from {var_client_custom_db}.raw_ref_f554301_{var_client_name} F554301 
     join {var_client_custom_db}.mart_e1_l_oneview_company         ONEVIEW on F554301.ODCO = ONEVIEW.company_code
    where F554301.ODDCT = 'XP' and F554301.ODYN <> 'N') AS XP   
  RIGHT JOIN (
   select    
     F554301.ODDOC   
    ,F554301.ODDOCO   
    ,F554301.ODDCT   
    ,F554301.ODUPMJ   
    from {var_client_custom_db}.raw_ref_f554301_{var_client_name} F554301     
    join {var_client_custom_db}.mart_e1_l_oneview_company         ONEVIEW on F554301.ODCO = ONEVIEW.company_code
   where F554301.ODDCT = 'XF') AS XF  
    ON XP.ODDOC = XF.ODDOCO    
   AND XP.ODYN <> 'N'   
   GROUP BY XP.ODDOCO, XP.ODYN   
  ), 
   
_invstselements as ( 
   SELECT DISTINCT   
     F564314T.VHCO   
    ,F564314T.VHDOC1   
    ,F564314T.VHDCT1   
    ,F564314T.VHYN01   
    ,F0411.RPPST   
    ,F0411.RPVOD   
    ,F0411.RPDOC 
    ,F0411.RPSFX 
    ,(select max(F4209.HOSFXO) 
        from {var_client_custom_db}.raw_ref_f4209_{var_client_name}    F4209     
        join {var_client_custom_db}.mart_e1_l_oneview_company          ONEVIEW   on F4209.HOKCOO    = ONEVIEW.company_code
   left join {var_client_custom_db}.raw_ref_f554301_{var_client_name}  F554301   on F4209.HOKCOO    = F554301.ODCO    and F4209.HODOCO    = F554301.ODDOCO   
                                                                                and F4209.HODCTO    = F554301.ODDCTO  and F4209.HODCTO    = 'XI'   
                                                                                and F4209.HOASTS    = '2N'            and F4209.HORPER   <> '99999997' and F554301.ODDOCO IS NULL         
   left join {var_client_custom_db}.raw_ref_f564314t_{var_client_name} F564314T  on F564314T.VHCO   = F4209.HOKCOO    and F564314T.VHDOC1 = F4209.HODOCO    
                                                                                and F564314T.VHDCT1 = F4209.HODCTO
    ) AS blank1   
   ,(select max(F4209.HOSFXO) 
        from {var_client_custom_db}.raw_ref_f4209_{var_client_name}    F4209     
        join {var_client_custom_db}.mart_e1_l_oneview_company          ONEVIEW   on F4209.HOKCOO    = ONEVIEW.company_code
   left join {var_client_custom_db}.raw_ref_f554301_{var_client_name}  F554301   on F4209.HOKCOO    = F554301.ODCO    and F4209.HODOCO    = F554301.ODDOCO   
                                                                                and F4209.HODCTO    = F554301.ODDCTO  and F4209.HODCTO    = 'XI'   
                                                                                and F4209.HOASTS    = '2N'            and F4209.HORPER    = '99999997' and F554301.ODDOCO IS NULL         
   left join {var_client_custom_db}.raw_ref_f564314t_{var_client_name} F564314T  on F564314T.VHCO   = F4209.HOKCOO    and F564314T.VHDOC1 = F4209.HODOCO    
                                                                                and F564314T.VHDCT1 = F4209.HODCTO
    ) AS blank2     
   ,(select max(F4209.HOSFXO) 
        from {var_client_custom_db}.raw_ref_f4209_{var_client_name}    F4209     
        join {var_client_custom_db}.mart_e1_l_oneview_company          ONEVIEW   on F4209.HOKCOO    = ONEVIEW.company_code
   left join {var_client_custom_db}.raw_ref_f554301_{var_client_name}  F554301   on F4209.HOKCOO    = F554301.ODCO    and F4209.HODOCO    = F554301.ODDOCO   
                                                                                and F4209.HODCTO    = F554301.ODDCTO  and F4209.HODCTO    = 'XI'   
                                                                                and F4209.HOASTS    = '2N'            and F4209.HORPER    = '99999997' and F554301.ODDOCO IS NOT NULL          left join {var_client_custom_db}.raw_ref_f564314t_{var_client_name} F564314T  on F564314T.VHCO   = F4209.HOKCOO    and F564314T.VHDOC1 = F4209.HODOCO    
                                                                                and F564314T.VHDCT1 = F4209.HODCTO
    ) AS blank3       
        FROM {var_client_custom_db}.raw_ref_f564314t_{var_client_name} F564314T 
        JOIN {var_client_custom_db}.mart_e1_l_oneview_company          ov                      on F564314T.VHCO  = ov.company_code
   LEFT JOIN {var_client_custom_db}.raw_ref_f0411_{var_client_name}    F0411      on F564314T.VHCO  = F0411.RPCO and F564314T.VHDOC = F0411.RPDOC 
                                                                                     and F564314T.VHDCT = F0411.RPDCT   
  ) 
 
SELECT DISTINCT   
  ip.purchase_order as commitment_number,
  CASE WHEN (LTRIM(RTRIM(invstatus.VHYN01))='' OR (invstatus.VHYN01 IS NULL)) THEN   
       (CASE WHEN invstatus.blank2 IS NOT NULL                                THEN 'PM Approved - Ready to be Packaged'   
             WHEN invstatus.blank1 IS NOT NULL                                THEN 'Awaiting PM Approval'   
             WHEN invstatus.blank3 IS NOT NULL                                THEN    
             (CASE WHEN (LTRIM(RTRIM(funding.ODYN)) = '')                     THEN 'Packaged - Awaiting Package Approval'   
                   WHEN (funding.ODYN IS NULL)                                THEN 'PM Approved - Ready to be Packaged'   
              END)   
        ELSE 'Invoice Rejected'   
        END)   
       WHEN invstatus.VHYN01 = 'Y' AND invstatus.RPPST IN ('A','#','H')       THEN 'Package Approved - Awaiting Payment'   
       WHEN invstatus.VHYN01 = 'Y' AND invstatus.RPPST = 'P'                  THEN   
            (CASE WHEN invstatus.RPVOD = 'V'                                  THEN 'Voided Invoice'   
             ELSE 'Package Approved and Paid'   
             END)   
       WHEN invstatus.VHYN01 = 'N'                                            THEN 'Invoice Rejected'   
       WHEN invstatus.VHYN01 = 'Y'    
        AND (LTRIM(RTRIM(invstatus.RPPST)) = '' OR invstatus.RPPST IS NULL)    
        AND (LTRIM(RTRIM(invstatus.RPVOD)) = '' OR invstatus.RPVOD IS NULL)   THEN '* Invoice Status NOT Known - 1 *'      
  END               as invoicestatus,
--ip.* ,
  ip.client_nbr,
  ip.document_number,
  ip.document_type,
  ip.pay_item,
  ip.pay_item_extension_number,
  ip.reference,
  ip.invoice_number,
  ip.invoice_status,
  ip.invoice_total,
  ip.invoice_amount,
  ip.tax_amount,
  ip.invoice_date,
  ip.payment_date,
  ip.currency_code,
  ip.business_unit,
  ip.supplier_id,
  ip.supplier_name,
  ip.person_corporation_code,
  ip.tax_id,
  ip.personal_tax_id,
  ip.purchase_order,
  ip.supplier_number,
  ip.is_voided,
  ip.voided_date,
  ip.payment_amount,
  ip.posting_month,
  ip.fiscal_year,
  ip.object_account,
  ip.amount_to_distribute,
  ip.company_code,
  ip.funding_number,
  ip.payment_number,
  xx.name           as vendor_name, 
  xx.address_line_1 as vendor_address,  
  xx.address_line_2 as vendor_address1,
  xx.postal_code    as postal_code,
  xx.city           as city,
  xx.state_code     as state_code,
  xx.state          as state,
  zz.ABALPH         as project_manager
  
      FROM {var_client_custom_db}.ssdv_vw_ovf_invoice_payment       AS ip         
 LEFT JOIN {var_client_custom_db}.mart_e1_s_address_book            AS xx         ON ip.supplier_id   = xx.address_id 
 LEFT JOIN {var_client_custom_db}.raw_ref_f550101_{var_client_name} AS ff         ON ip.business_unit = LTRIM(RTRIM(PTMCU)) AND PTY55JROLE='PM' AND ip.client_nbr=ff.PTCO 
 LEFT JOIN {var_raw_db}.ref_f0101                     AS zz         ON ff.PTAN8         = zz.ABAN8   
 LEFT JOIN _invstselements                                          AS invstatus  ON ip.client_nbr    = invstatus.VHCO      AND ip.document_number = invstatus.RPDOC 
                                                                                              AND ip.pay_item = invstatus.RPSFX 
 LEFT JOIN _funding                                                 AS funding    ON invstatus.VHDOC1 = funding.ODDOCO  
;   """.format(var_client_custom_db=var_client_custom_db, var_client_name=var_client_name, var_raw_db=var_raw_db))