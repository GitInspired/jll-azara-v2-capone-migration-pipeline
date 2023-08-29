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

# DBTITLE 1,fn_ConvertJulianDay
spark.sql("""
CREATE OR REPLACE FUNCTION {var_client_custom_db}.fn_ConvertJulianDay(in_str STRING)
RETURNS TIMESTAMP
RETURN
SELECT 
CASE WHEN 
    in_str IS NOT NULL  
    AND in_str <> ''  
    AND LEN(in_str) > 4  
    THEN 
    DATEADD(DAY,CAST(RIGHT(in_str,3) as INT)-1,DATEADD(YEAR,CAST(LEFT(in_str,LEN(in_str)-3) as INT),'1900-01-01')) 
END
""".format(var_client_custom_db=var_client_custom_db))

# COMMAND ----------

# DBTITLE 1,fn_ProcessHTMLText
spark.sql("""
CREATE OR REPLACE FUNCTION {var_client_custom_db}.fn_ProcessHTMLText(HTMLText STRING)
RETURNS STRING
RETURN
SELECT LTRIM(RTRIM(
 replace
(replace
(replace
(replace  
(replace
(replace
(replace
(replace  
(replace
  (replace  
   (replace    
    (replace  
     (replace  
      (replace  
       (replace  
        (replace    
         (replace  
          (replace  
           (replace  
            (replace  
             (replace  
              (replace  
               (replace  
                (replace  
                 (replace  
                  (replace   
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace  
                   (replace
                   (replace(HTMLText,
                   '&#39;',''''), 
                   '&#162;','¢'),  
                   '&#171;','«'),  
                   '&#187;','»'),  
                   '&#212;','Ô'),  
                   '&#186;','º'),  
                   '&#214;','Ö'),  
                   '&#241;','ñ'),  
                   '&#225;','á'),  
                   '&#243;','ó'),  
                   '&#234;','ê'),  
                   '&#237;','í'),  
                   '&#235;','ë'),  
                   '&#180;','´'),  
                   '&#176;','°'),  
                   '&#196;','Ä'),  
                   '&#238;','î'),  
                   '&#224;','à'),  
                   '&#220;','Ü'),  
                   '&#251;','û'),  
                  '&#231;','ç'),  
                 '&#172;','¬'),  
                '&#178;','²'),  
               '&#226;','â'),  
              '&amp;','&'),    
             '&quot;','"'),  
            '&gt;','>'),   
           '&lt;','<'),   
          '&#233;', 'é'),  
         '&#239;', 'ï'),  
        '&#232;', 'è'),  
       '&#244;', 'ô'),  
      '&#160;', ' '),  
     '&#252;','ü'),  
    '&#223;','ß'),  
   '&#246;','ö'),  
  '&#228;','ä'),  
 '&quot;',''''),
 '&#8211;','-'),  
 '&#8203;',''),
 '&#8364;','€'),  
 '&#8230;','… '),
 '&#160;', ''),
 '&#8217;','\′'),
 '&#8221;','”'),
 '&#64257;','fi')))
""".format(var_client_custom_db=var_client_custom_db))
