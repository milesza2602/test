--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_610U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_610U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2017
--  Author:      Alastair de Wet
--               Create FND_CUSTOMER_MASTER dimention table in the foundation layer
--               with input ex staging table from SVOC  .
--  Tables:      Input  - stg_svoc_customer_master_cpy
--               Output - fnd_customer_master
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   
--
--   
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
g_physical_updated   integer       :=  0;

g_master_subscriber_key        stg_svoc_customer_master_cpy.master_subscriber_key%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_610U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CUTOMER MASTER EX SVOC';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_svoc_customer_master_cpy
where (master_subscriber_key)
in
(select master_subscriber_key
from stg_svoc_customer_master_cpy 
group by master_subscriber_key 
having count(*) > 1) 
order by master_subscriber_key,
sys_source_batch_id desc ,sys_source_sequence_no desc;

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_master_subscriber_key   := 0; 
 

for dupp_record in stg_dup
   loop

    if  dupp_record.master_subscriber_key    = g_master_subscriber_key  then
        update stg_svoc_customer_master_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_master_subscriber_key    := dupp_record.master_subscriber_key; 
 

   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin
--/*+ first_rows parallel(fnd) parallel(upd_rec) */
 
   MERGE  /*+ parallel(fnd,8) */ INTO fnd_customer_master fnd 
   USING (
         select /*+ FULL(cpy)  parallel(cpy,8) */  
              cpy.*
      from    stg_svoc_customer_master_cpy cpy
      where   cpy.sys_process_code      = 'N'
      order by   sys_source_batch_id,sys_source_sequence_no      
         ) mer_rec
   ON    (  fnd.	master_subscriber_key	  =	mer_rec.	master_subscriber_key )
   WHEN MATCHED THEN 
   UPDATE SET
            fnd.	IDENTITY_DOCUMENT_NO	          =	mer_rec.	IDENTITY_DOCUMENT_NO	,
            fnd.	IDENTITY_DOCUMENT_NO_CREATE_DT	=	mer_rec.	IDENTITY_DOCUMENT_NO_CREATE_DT	,
            fnd.	PASSPORT_NO	                    =	mer_rec.	PASSPORT_NO	,
            fnd.	PASSPORT_COUNTRY_CODE	          =	mer_rec.	PASSPORT_COUNTRY_CODE	,
            fnd.	PASSPORT_NO_CREATE_DATE	        =	mer_rec.	PASSPORT_NO_CREATE_DATE	,
            fnd.	CREATE_DATE	                    =	mer_rec.	CREATE_DATE	,
            fnd.	TITLE	                          =	mer_rec.	TITLE	,
            fnd.	INITIALS	                      =	mer_rec.	INITIALS	,
            fnd.	FIRST_NAME	                    =	mer_rec.	FIRST_NAME	,
            fnd.	LAST_NAME	                      =	mer_rec.	LAST_NAME	,
            fnd.	BIRTH_DATE	                    =	mer_rec.	BIRTH_DATE	,
            fnd.	GENDER	                        =	mer_rec.	GENDER	,
            fnd.	PREFERRED_LANGUAGE	            =	mer_rec.	PREFERRED_LANGUAGE	,
            fnd.	POPULATED_EMAIL_ADDRESS       	=	mer_rec.	POPULATED_EMAIL_ADDRESS	,
            fnd.	HOME_EMAIL_ADDRESS	            =	mer_rec.	HOME_EMAIL_ADDRESS	,
            fnd.	HOME_EMAIL_LAST_UPDATE_DT	      =	mer_rec.	HOME_EMAIL_LAST_UPDATE_DT	,
            fnd.	HOME_EMAIL_FAILURE_COUNT	      =	mer_rec.	HOME_EMAIL_FAILURE_COUNT	,
            fnd.	WORK_EMAIL_ADDRESS	            =	mer_rec.	WORK_EMAIL_ADDRESS	,
            fnd.	WORK_EMAIL_LAST_UPDATE_DT	      =	mer_rec.	WORK_EMAIL_LAST_UPDATE_DT	,
            fnd.	WORK_EMAIL_FAILURE_COUNT	      =	mer_rec.	WORK_EMAIL_FAILURE_COUNT	,
            fnd.	STATEMENT_EMAIL_ADDRESS	        =	mer_rec.	STATEMENT_EMAIL_ADDRESS	,
            fnd.	STATEMENT_EMAIL_LAST_UPDATE_DT	=	mer_rec.	STATEMENT_EMAIL_LAST_UPDATE_DT	,
            fnd.	STATEMENT_EMAIL_FAILURE_COUNT	  =	mer_rec.	STATEMENT_EMAIL_FAILURE_COUNT	,
            fnd.	ECOMMERCE_EMAIL_ADDRESS	        =	mer_rec.	ECOMMERCE_EMAIL_ADDRESS	,
            fnd.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	=	mer_rec.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	,
            fnd.	ECOMMERCE_EMAIL_FAILURE_COUNT	  =	mer_rec.	ECOMMERCE_EMAIL_FAILURE_COUNT	,
            fnd.	POPULATED_CELL_NO	              =	mer_rec.	POPULATED_CELL_NO	,
            fnd.	HOME_CELL_NO	                  =	mer_rec.	HOME_CELL_NO	,
            fnd.	HOME_CELL_LAST_UPDATED_DATE	    =	mer_rec.	HOME_CELL_LAST_UPDATED_DATE	,
            fnd.	HOME_CELL_FAILURE_COUNT	        =	mer_rec.	HOME_CELL_FAILURE_COUNT	,
            fnd.	WORK_CELL_NO	                  =	mer_rec.	WORK_CELL_NO	,
            fnd.	WORK_CELL_LAST_UPDATED_DATE	    =	mer_rec.	WORK_CELL_LAST_UPDATED_DATE	,
            fnd.	WORK_CELL_FAILURE_COUNT	        =	mer_rec.	WORK_CELL_FAILURE_COUNT	,
            fnd.	HOME_PHONE_NO	                  =	mer_rec.	HOME_PHONE_NO	,
            fnd.	HOME_PHONE_LAST_UPDATED_DATE	  =	mer_rec.	HOME_PHONE_LAST_UPDATED_DATE	,
            fnd.	HOME_PHONE_FAILURE_COUNT	      =	mer_rec.	HOME_PHONE_FAILURE_COUNT	,
            fnd.	WORK_PHONE_NO                 	=	mer_rec.	WORK_PHONE_NO	,
            fnd.	WORK_PHONE_LAST_UPDATED_DATE	  =	mer_rec.	WORK_PHONE_LAST_UPDATED_DATE	,
            fnd.	WORK_PHONE_FAILURE_COUNT	      =	mer_rec.	WORK_PHONE_FAILURE_COUNT	,
            fnd.	POSTAL_ADDRESS_LINE_1	          =	mer_rec.	POSTAL_ADDRESS_LINE_1	,
            fnd.	POSTAL_ADDRESS_LINE_2	          =	mer_rec.	POSTAL_ADDRESS_LINE_2	,
            fnd.	POSTAL_SUBURB_NAME	            =	mer_rec.	POSTAL_SUBURB_NAME	,
            fnd.	POST_POSTAL_CODE	              =	mer_rec.	POST_POSTAL_CODE	,
            fnd.	POSTAL_CITY_NAME	              =	mer_rec.	POSTAL_CITY_NAME	,
            fnd.	POSTAL_PROVINCE_NAME	          =	mer_rec.	POSTAL_PROVINCE_NAME	,
            fnd.	POSTAL_ADDRESS_UPDATE_DATE	    =	mer_rec.	POSTAL_ADDRESS_UPDATE_DATE	,
            fnd.	PHYSICAL_ADDRESS_LINE_1	        =	mer_rec.	PHYSICAL_ADDRESS_LINE_1	,
            fnd.	PHYSICAL_ADDRESS_LINE_2	        =	mer_rec.	PHYSICAL_ADDRESS_LINE_2	,
            fnd.	PHYSICAL_SUBURB_NAME	          =	mer_rec.	PHYSICAL_SUBURB_NAME	,
            fnd.	PHYSICAL_POSTAL_CODE	          =	mer_rec.	PHYSICAL_POSTAL_CODE	,
            fnd.	PHYSICAL_CITY_NAME	            =	mer_rec.	PHYSICAL_CITY_NAME	,
            fnd.	PHYSICAL_PROVINCE_NAME	        =	mer_rec.	PHYSICAL_PROVINCE_NAME	,
            fnd.	PHYSICAL_ADDRESS_UPDATE_DATE	  =	mer_rec.	PHYSICAL_ADDRESS_UPDATE_DATE	,
            fnd.	SHIPPING_ADDRESS_LINE_1	        =	mer_rec.	SHIPPING_ADDRESS_LINE_1	,
            fnd.	SHIPPING_ADDRESS_LINE_2	        =	mer_rec.	SHIPPING_ADDRESS_LINE_2	,
            fnd.	SHIPPING_SUBURB_NAME	          =	mer_rec.	SHIPPING_SUBURB_NAME	,
            fnd.	POST_SHIPPING_CODE	            =	mer_rec.	POST_SHIPPING_CODE	,
            fnd.	SHIPPING_CITY_NAME	            =	mer_rec.	SHIPPING_CITY_NAME	,
            fnd.	SHIPPING_PROVINCE_NAME	        =	mer_rec.	SHIPPING_PROVINCE_NAME	,
            fnd.	SHIPPING_ADDRESS_UPDATE_DATE  	=	mer_rec.	SHIPPING_ADDRESS_UPDATE_DATE	,
            fnd.	BILLING_ADDRESS_LINE_1	        =	mer_rec.	BILLING_ADDRESS_LINE_1	,
            fnd.	BILLING_ADDRESS_LINE_2	        =	mer_rec.	BILLING_ADDRESS_LINE_2	,
            fnd.	BILLING_SUBURB_NAME	            =	mer_rec.	BILLING_SUBURB_NAME	,
            fnd.	POST_BILLING_CODE	              =	mer_rec.	POST_BILLING_CODE	,
            fnd.	BILLING_CITY_NAME	              =	mer_rec.	BILLING_CITY_NAME	,
            fnd.	BILLING_PROVINCE_NAME	          =	mer_rec.	BILLING_PROVINCE_NAME	,
            fnd.	BILLING_ADDRESS_UPDATE_DATE	    =	mer_rec.	BILLING_ADDRESS_UPDATE_DATE	,
            fnd.	WW_DM_SMS_OPT_OUT_IND	          =	mer_rec.	WW_DM_SMS_OPT_OUT_IND	,
            fnd.	WW_DM_EMAIL_OPT_OUT_IND	        =	mer_rec.	WW_DM_EMAIL_OPT_OUT_IND	,
            fnd.	WW_DM_POST_OPT_OUT_IND	        =	mer_rec.	WW_DM_POST_OPT_OUT_IND	,
            fnd.	WW_DM_PHONE_OPT_OUT_IND	        =	mer_rec.	WW_DM_PHONE_OPT_OUT_IND	,
            fnd.	WW_MAN_SMS_OPT_OUT_IND	        =	mer_rec.	WW_MAN_SMS_OPT_OUT_IND	,
            fnd.	WW_MAN_EMAIL_OPT_OUT_IND	      =	mer_rec.	WW_MAN_EMAIL_OPT_OUT_IND	,
            fnd.	WW_MAN_POST_OPT_OUT_IND	        =	mer_rec.	WW_MAN_POST_OPT_OUT_IND	,
            fnd.	WW_MAN_PHONE_OPT_OUT_IND	      =	mer_rec.	WW_MAN_PHONE_OPT_OUT_IND	,
            fnd.	WFS_DM_SMS_OPT_OUT_IND	        =	mer_rec.	WFS_DM_SMS_OPT_OUT_IND	,
            fnd.	WFS_DM_EMAIL_OPT_OUT_IND	      =	mer_rec.	WFS_DM_EMAIL_OPT_OUT_IND	,
            fnd.	WFS_DM_POST_OPT_OUT_IND	        =	mer_rec.	WFS_DM_POST_OPT_OUT_IND	,
            fnd.	WFS_DM_PHONE_OPT_OUT_IND	      =	mer_rec.	WFS_DM_PHONE_OPT_OUT_IND	,
            fnd.	WFS_CON_SMS_OPT_OUT_IND	        =	mer_rec.	WFS_CON_SMS_OPT_OUT_IND	,
            fnd.	WFS_CON_EMAIL_OPT_OUT_IND	      =	mer_rec.	WFS_CON_EMAIL_OPT_OUT_IND	,
            fnd.	WFS_CON_POST_OPT_OUT_IND	      =	mer_rec.	WFS_CON_POST_OPT_OUT_IND	,
            fnd.	WFS_CON_PHONE_OPT_OUT_IND	      =	mer_rec.	WFS_CON_PHONE_OPT_OUT_IND	,
            fnd.	PREFERENCE_1_IND	=	mer_rec.	PREFERENCE_1_IND	,
            fnd.	PREFERENCE_2_IND	=	mer_rec.	PREFERENCE_2_IND	,
            fnd.	PREFERENCE_3_IND	=	mer_rec.	PREFERENCE_3_IND	,
            fnd.	PREFERENCE_4_IND	=	mer_rec.	PREFERENCE_4_IND	,
            fnd.	PREFERENCE_5_IND	=	mer_rec.	PREFERENCE_5_IND	,
            fnd.	PREFERENCE_6_IND	=	mer_rec.	PREFERENCE_6_IND	,
            fnd.	PREFERENCE_7_IND	=	mer_rec.	PREFERENCE_7_IND	,
            fnd.	SWIPE_DEVICE_IND	=	mer_rec.	SWIPE_DEVICE_IND	,
            fnd.	LAST_SWIPE_DATE	                =	mer_rec.	LAST_SWIPE_DATE	,
            fnd.	FACEBOOK_ID	                    =	mer_rec.	FACEBOOK_ID	,
            fnd.	FACEBOOK_ID_LAST_UPDATED_DATE	  =	mer_rec.	FACEBOOK_ID_LAST_UPDATED_DATE	,
            fnd.	INSTAGRAM_ID	                  =	mer_rec.	INSTAGRAM_ID	,
            fnd.	INSTAGRAM_ID_LAST_UPDATED_DATE	=	mer_rec.	INSTAGRAM_ID_LAST_UPDATED_DATE	,
            fnd.	TWITTER_ID	                    =	mer_rec.	TWITTER_ID	,
            fnd.	TWITTER_ID_LAST_UPDATED_DATE	  =	mer_rec.	TWITTER_ID_LAST_UPDATED_DATE	,
            fnd.	LINKEDIN_ID	                    =	mer_rec.	LINKEDIN_ID	,
            fnd.	LINKEDIN_ID_LAST_UPDATED_DATE	  =	mer_rec.	LINKEDIN_ID_LAST_UPDATED_DATE	,
            fnd.	ROA_MARKETING_CONSENT_IND     	=	mer_rec.	ROA_MARKETING_CONSENT_IND	,
            fnd.	ROA_REGISTERED_LOCATION_NO	    =	mer_rec.	ROA_REGISTERED_LOCATION_NO	,
            fnd.	ROA_COUNTRY_CODE	              =	mer_rec.	ROA_COUNTRY_CODE	,
            fnd.	ROA_ACTIVE_PRODUCT_IND	        =	mer_rec.	ROA_ACTIVE_PRODUCT_IND	,
            fnd.	ROA_LOYALTY_CARD_NO	            =	mer_rec.	ROA_LOYALTY_CARD_NO	,
            fnd.	ACCOUNT_LAST_MODIFIED_DATE    	=	mer_rec.	ACCOUNT_LAST_MODIFIED_DATE	,
            fnd.  last_updated_date             = g_date
   WHEN NOT MATCHED THEN
   INSERT
          (         
          MASTER_SUBSCRIBER_KEY,
          IDENTITY_DOCUMENT_NO,
          IDENTITY_DOCUMENT_NO_CREATE_DT,
          PASSPORT_NO,
          PASSPORT_COUNTRY_CODE,
          PASSPORT_NO_CREATE_DATE,
          CREATE_DATE,
          TITLE,
          INITIALS,
          FIRST_NAME,
          LAST_NAME,
          BIRTH_DATE,
          GENDER,
          PREFERRED_LANGUAGE,
          POPULATED_EMAIL_ADDRESS,
          HOME_EMAIL_ADDRESS,
          HOME_EMAIL_LAST_UPDATE_DT,
          HOME_EMAIL_FAILURE_COUNT,
          WORK_EMAIL_ADDRESS,
          WORK_EMAIL_LAST_UPDATE_DT,
          WORK_EMAIL_FAILURE_COUNT,
          STATEMENT_EMAIL_ADDRESS,
          STATEMENT_EMAIL_LAST_UPDATE_DT,
          STATEMENT_EMAIL_FAILURE_COUNT,
          ECOMMERCE_EMAIL_ADDRESS,
          ECOMMERCE_EMAIL_LAST_UPDATE_DT,
          ECOMMERCE_EMAIL_FAILURE_COUNT,
          POPULATED_CELL_NO,
          HOME_CELL_NO,
          HOME_CELL_LAST_UPDATED_DATE,
          HOME_CELL_FAILURE_COUNT,
          WORK_CELL_NO,
          WORK_CELL_LAST_UPDATED_DATE,
          WORK_CELL_FAILURE_COUNT,
          HOME_PHONE_NO,
          HOME_PHONE_LAST_UPDATED_DATE,
          HOME_PHONE_FAILURE_COUNT,
          WORK_PHONE_NO,
          WORK_PHONE_LAST_UPDATED_DATE,
          WORK_PHONE_FAILURE_COUNT,
          POSTAL_ADDRESS_LINE_1,
          POSTAL_ADDRESS_LINE_2,
          POSTAL_SUBURB_NAME,
          POST_POSTAL_CODE,
          POSTAL_CITY_NAME,
          POSTAL_PROVINCE_NAME,
          POSTAL_ADDRESS_UPDATE_DATE,
          PHYSICAL_ADDRESS_LINE_1,
          PHYSICAL_ADDRESS_LINE_2,
          PHYSICAL_SUBURB_NAME,
          PHYSICAL_POSTAL_CODE,
          PHYSICAL_CITY_NAME,
          PHYSICAL_PROVINCE_NAME,
          PHYSICAL_ADDRESS_UPDATE_DATE,
          SHIPPING_ADDRESS_LINE_1,
          SHIPPING_ADDRESS_LINE_2,
          SHIPPING_SUBURB_NAME,
          POST_SHIPPING_CODE,
          SHIPPING_CITY_NAME,
          SHIPPING_PROVINCE_NAME,
          SHIPPING_ADDRESS_UPDATE_DATE,
          BILLING_ADDRESS_LINE_1,
          BILLING_ADDRESS_LINE_2,
          BILLING_SUBURB_NAME,
          POST_BILLING_CODE,
          BILLING_CITY_NAME,
          BILLING_PROVINCE_NAME,
          BILLING_ADDRESS_UPDATE_DATE,
          WW_DM_SMS_OPT_OUT_IND,
          WW_DM_EMAIL_OPT_OUT_IND,
          WW_DM_POST_OPT_OUT_IND,
          WW_DM_PHONE_OPT_OUT_IND,
          WW_MAN_SMS_OPT_OUT_IND,
          WW_MAN_EMAIL_OPT_OUT_IND,
          WW_MAN_POST_OPT_OUT_IND,
          WW_MAN_PHONE_OPT_OUT_IND,
          WFS_DM_SMS_OPT_OUT_IND,
          WFS_DM_EMAIL_OPT_OUT_IND,
          WFS_DM_POST_OPT_OUT_IND,
          WFS_DM_PHONE_OPT_OUT_IND,
          WFS_CON_SMS_OPT_OUT_IND,
          WFS_CON_EMAIL_OPT_OUT_IND,
          WFS_CON_POST_OPT_OUT_IND,
          WFS_CON_PHONE_OPT_OUT_IND,
          PREFERENCE_1_IND,
          PREFERENCE_2_IND,
          PREFERENCE_3_IND,
          PREFERENCE_4_IND,
          PREFERENCE_5_IND,
          PREFERENCE_6_IND,
          PREFERENCE_7_IND,
          SWIPE_DEVICE_IND,
          LAST_SWIPE_DATE,
          FACEBOOK_ID,
          FACEBOOK_ID_LAST_UPDATED_DATE,
          INSTAGRAM_ID,
          INSTAGRAM_ID_LAST_UPDATED_DATE,
          TWITTER_ID,
          TWITTER_ID_LAST_UPDATED_DATE,
          LINKEDIN_ID,
          LINKEDIN_ID_LAST_UPDATED_DATE,
          ROA_MARKETING_CONSENT_IND,
          ROA_REGISTERED_LOCATION_NO,
          ROA_COUNTRY_CODE,
          ROA_ACTIVE_PRODUCT_IND,
          ROA_LOYALTY_CARD_NO,
          ACCOUNT_LAST_MODIFIED_DATE,
          LAST_UPDATED_DATE
          )
  values
          (         
          mer_rec.	MASTER_SUBSCRIBER_KEY,
          mer_rec.	IDENTITY_DOCUMENT_NO,
          mer_rec.	IDENTITY_DOCUMENT_NO_CREATE_DT	,
          mer_rec.	PASSPORT_NO	,
          mer_rec.	PASSPORT_COUNTRY_CODE	,
          mer_rec.	PASSPORT_NO_CREATE_DATE	,
          mer_rec.	CREATE_DATE	,
          mer_rec.	TITLE	,
          mer_rec.	INITIALS	,
          mer_rec.	FIRST_NAME	,
          mer_rec.	LAST_NAME	,
          mer_rec.	BIRTH_DATE	,
          mer_rec.	GENDER	,
          mer_rec.	PREFERRED_LANGUAGE	,
          mer_rec.	POPULATED_EMAIL_ADDRESS	,
          mer_rec.	HOME_EMAIL_ADDRESS	,
          mer_rec.	HOME_EMAIL_LAST_UPDATE_DT	,
          mer_rec.	HOME_EMAIL_FAILURE_COUNT	,
          mer_rec.	WORK_EMAIL_ADDRESS	,
          mer_rec.	WORK_EMAIL_LAST_UPDATE_DT	,
          mer_rec.	WORK_EMAIL_FAILURE_COUNT	,
          mer_rec.	STATEMENT_EMAIL_ADDRESS	,
          mer_rec.	STATEMENT_EMAIL_LAST_UPDATE_DT	,
          mer_rec.	STATEMENT_EMAIL_FAILURE_COUNT	,
          mer_rec.	ECOMMERCE_EMAIL_ADDRESS	,
          mer_rec.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	,
          mer_rec.	ECOMMERCE_EMAIL_FAILURE_COUNT	,
          mer_rec.	POPULATED_CELL_NO	,
          mer_rec.	HOME_CELL_NO	,
          mer_rec.	HOME_CELL_LAST_UPDATED_DATE	,
          mer_rec.	HOME_CELL_FAILURE_COUNT	,
          mer_rec.	WORK_CELL_NO	,
          mer_rec.	WORK_CELL_LAST_UPDATED_DATE	,
          mer_rec.	WORK_CELL_FAILURE_COUNT	,
          mer_rec.	HOME_PHONE_NO	,
          mer_rec.	HOME_PHONE_LAST_UPDATED_DATE	,
          mer_rec.	HOME_PHONE_FAILURE_COUNT	,
          mer_rec.	WORK_PHONE_NO	,
          mer_rec.	WORK_PHONE_LAST_UPDATED_DATE	,
          mer_rec.	WORK_PHONE_FAILURE_COUNT	,
          mer_rec.	POSTAL_ADDRESS_LINE_1	,
          mer_rec.	POSTAL_ADDRESS_LINE_2	,
          mer_rec.	POSTAL_SUBURB_NAME	,
          mer_rec.	POST_POSTAL_CODE	,
          mer_rec.	POSTAL_CITY_NAME	,
          mer_rec.	POSTAL_PROVINCE_NAME	,
          mer_rec.	POSTAL_ADDRESS_UPDATE_DATE	,
          mer_rec.	PHYSICAL_ADDRESS_LINE_1	,
          mer_rec.	PHYSICAL_ADDRESS_LINE_2	,
          mer_rec.	PHYSICAL_SUBURB_NAME	,
          mer_rec.	PHYSICAL_POSTAL_CODE	,
          mer_rec.	PHYSICAL_CITY_NAME	,
          mer_rec.	PHYSICAL_PROVINCE_NAME	,
          mer_rec.	PHYSICAL_ADDRESS_UPDATE_DATE	,
          mer_rec.	SHIPPING_ADDRESS_LINE_1	,
          mer_rec.	SHIPPING_ADDRESS_LINE_2	,
          mer_rec.	SHIPPING_SUBURB_NAME	,
          mer_rec.	POST_SHIPPING_CODE	,
          mer_rec.	SHIPPING_CITY_NAME	,
          mer_rec.	SHIPPING_PROVINCE_NAME	,
          mer_rec.	SHIPPING_ADDRESS_UPDATE_DATE	,
          mer_rec.	BILLING_ADDRESS_LINE_1	,
          mer_rec.	BILLING_ADDRESS_LINE_2	,
          mer_rec.	BILLING_SUBURB_NAME	,
          mer_rec.	POST_BILLING_CODE	,
          mer_rec.	BILLING_CITY_NAME	,
          mer_rec.	BILLING_PROVINCE_NAME	,
          mer_rec.	BILLING_ADDRESS_UPDATE_DATE	,
          mer_rec.	WW_DM_SMS_OPT_OUT_IND	,
          mer_rec.	WW_DM_EMAIL_OPT_OUT_IND	,
          mer_rec.	WW_DM_POST_OPT_OUT_IND	,
          mer_rec.	WW_DM_PHONE_OPT_OUT_IND	,
          mer_rec.	WW_MAN_SMS_OPT_OUT_IND	,
          mer_rec.	WW_MAN_EMAIL_OPT_OUT_IND	,
          mer_rec.	WW_MAN_POST_OPT_OUT_IND	,
          mer_rec.	WW_MAN_PHONE_OPT_OUT_IND	,
          mer_rec.	WFS_DM_SMS_OPT_OUT_IND	,
          mer_rec.	WFS_DM_EMAIL_OPT_OUT_IND	,
          mer_rec.	WFS_DM_POST_OPT_OUT_IND	,
          mer_rec.	WFS_DM_PHONE_OPT_OUT_IND	,
          mer_rec.	WFS_CON_SMS_OPT_OUT_IND	,
          mer_rec.	WFS_CON_EMAIL_OPT_OUT_IND	,
          mer_rec.	WFS_CON_POST_OPT_OUT_IND	,
          mer_rec.	WFS_CON_PHONE_OPT_OUT_IND	,
          mer_rec.	PREFERENCE_1_IND	,
          mer_rec.	PREFERENCE_2_IND	,
          mer_rec.	PREFERENCE_3_IND	,
          mer_rec.	PREFERENCE_4_IND	,
          mer_rec.	PREFERENCE_5_IND	,
          mer_rec.	PREFERENCE_6_IND	,
          mer_rec.	PREFERENCE_7_IND	,
          mer_rec.	SWIPE_DEVICE_IND	,
          mer_rec.	LAST_SWIPE_DATE	,
          mer_rec.	FACEBOOK_ID	,
          mer_rec.	FACEBOOK_ID_LAST_UPDATED_DATE	,
          mer_rec.	INSTAGRAM_ID	,
          mer_rec.	INSTAGRAM_ID_LAST_UPDATED_DATE	,
          mer_rec.	TWITTER_ID	,
          mer_rec.	TWITTER_ID_LAST_UPDATED_DATE	,
          mer_rec.	LINKEDIN_ID	,
          mer_rec.	LINKEDIN_ID_LAST_UPDATED_DATE	,
          mer_rec.	ROA_MARKETING_CONSENT_IND	,
          mer_rec.	ROA_REGISTERED_LOCATION_NO	,
          mer_rec.	ROA_COUNTRY_CODE	,
          mer_rec.	ROA_ACTIVE_PRODUCT_IND	,
          mer_rec.	ROA_LOYALTY_CARD_NO	,
          mer_rec.	ACCOUNT_LAST_MODIFIED_DATE	,
          g_date
          )           
          ;  
             
      g_recs_updated := g_recs_updated +  sql%rowcount;       



      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
 

end flagged_records_update;

--************************************************************************************************** 
-- Send records to hospital where not valid
-- /*+ APPEND parallel (hsp,2) */
-- /*+ FULL(cpy)  parallel (cpy,2) */
--**************************************************************************************************
/*
procedure flagged_records_hospital as
begin
     
      insert  into stg_svoc_customer_master_hsp hsp
      select  
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'Invalid field',
              cpy.master_subscriber_key,
              cpy..........
       from   stg_svoc_customer_master_cpy cpy
      where  
         cpy........ is null 

         AND sys_process_code = 'N';
         

g_recs_hospital := g_recs_hospital + sql%rowcount;
      
      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG HOSPITAL - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG HOSPITAL - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end flagged_records_hospital;

*/        


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';

 
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Call the bulk routines 
--**************************************************************************************************
    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    remove_duplicates;
    
    
    select count(*)
    into   g_recs_read
    from   stg_svoc_customer_master_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;
    

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'PHYSICAL UPDATES ACTUALLY DONE '||g_physical_updated;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

    commit;
    p_success := true;
  exception

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       RAISE;
end wh_fnd_cust_610u;
