--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_620FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_620FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2017
--  Author:      Alastair de Wet
--               Create FND_all_bean_MASTER dimention table in the foundation layer
--               with input ex staging table from SVOC  .
--  Tables:      Input  - stg_svoc_ALL_BEAN_master_cpy1
--               Output - fnd_ALL_BEAN_master
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

g_BEAN_ID        stg_svoc_all_bean_master_cpy1.BEAN_ID%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_620U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ALL BEAN MASTER EX SVOC';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin
--/*+ first_rows parallel(fnd) parallel(upd_rec) */
 
   merge  /*+ PARALLEL(fnd,8) */ into fnd_all_bean_master fnd 
   using (
   
    SELECT /*+ PARALLEL(CPY1,8) */ CPY1.*
    FROM STG_SVOC_ALL_BEAN_master_cpy1 CPY1
    WHERE (SYS_SOURCE_BATCH_ID, SYS_SOURCE_SEQUENCE_NO,BEAN_ID) IN
          (SELECT /*+ PARALLEL(8) */ SYS_SOURCE_BATCH_ID, SYS_SOURCE_SEQUENCE_NO,BEAN_ID
           FROM (SELECT /*+ PARALLEL(tmp,8) */ tmp.*,
                 RANK ()
                    OVER (PARTITION BY BEAN_ID ORDER BY SYS_SOURCE_BATCH_ID DESC, SYS_SOURCE_SEQUENCE_NO DESC)
                    AS RANK
                 FROM STG_SVOC_ALL_BEAN_master_cpy1 tmp
           )
           WHERE RANK = 1)
    ORDER BY SYS_SOURCE_BATCH_ID DESC, SYS_SOURCE_SEQUENCE_NO

         ) mer_rec
   on    (  fnd.	BEAN_ID	          =	mer_rec.	BEAN_ID )
   when matched then 
   update set
            fnd.	SUBSCRIBER_KEY                  =	MER_REC.	SUBSCRIBER_KEY	,
            fnd.	MASTER_SUBSCRIBER_KEY	          =	MER_REC.	MASTER_SUBSCRIBER_KEY	,
            fnd.	MASTER_BEAN_ID	                =	MER_REC.	MASTER_BEAN_ID	,
            fnd.	SOURCE	                        =	MER_REC.	SOURCE	,
            fnd.	SOURCE_KEY	                    =	MER_REC.	SOURCE_KEY	,
            fnd.	VISION_CUSTOMER_NO	            =	MER_REC.	VISION_CUSTOMER_NO	,
            fnd.	IDENTITY_DOCUMENT_NO	          =	MER_REC.	IDENTITY_DOCUMENT_NO	,
            fnd.	IDENTITY_DOCUMENT_NO_CREATE_DT	=	MER_REC.	IDENTITY_DOCUMENT_NO_CREATE_DT	,
            fnd.	PASSPORT_NO	                    =	MER_REC.	PASSPORT_NO	,
            fnd.	PASSPORT_COUNTRY_CODE	          =	MER_REC.	PASSPORT_COUNTRY_CODE	,
            fnd.	PASSPORT_NO_CREATE_DATE	        =	MER_REC.	PASSPORT_NO_CREATE_DATE	,
            fnd.	CREATE_DATE	                    =	MER_REC.	CREATE_DATE	,
            fnd.	TITLE	                          =	MER_REC.	TITLE	,
            fnd.	INITIALS	                      =	MER_REC.	INITIALS	,
            fnd.	FIRST_NAME	                    =	MER_REC.	FIRST_NAME	,
            fnd.	LAST_NAME	                      =	MER_REC.	LAST_NAME	,
            fnd.	BIRTH_DATE	                    =	MER_REC.	BIRTH_DATE	,
            fnd.	GENDER	                        =	MER_REC.	GENDER	,
            fnd.	PREFERRED_LANGUAGE	            =	MER_REC.	PREFERRED_LANGUAGE	,
            fnd.	POPULATED_EMAIL_ADDRESS	        =	MER_REC.	POPULATED_EMAIL_ADDRESS	,
            fnd.	HOME_EMAIL_ADDRESS	            =	MER_REC.	HOME_EMAIL_ADDRESS	,
            fnd.	HOME_EMAIL_ADDRESS_CLEANSED	    =	MER_REC.	HOME_EMAIL_ADDRESS_CLEANSED	,
            fnd.	HOME_EMAIL_ADDRESS_VALIDATED	  =	MER_REC.	HOME_EMAIL_ADDRESS_VALIDATED	,
            fnd.	HOME_EMAIL_LAST_UPDATE_DT	      =	MER_REC.	HOME_EMAIL_LAST_UPDATE_DT	,
            fnd.	HOME_EMAIL_FAILURE_COUNT	      =	MER_REC.	HOME_EMAIL_FAILURE_COUNT	,
            fnd.	WORK_EMAIL_ADDRESS	            =	MER_REC.	WORK_EMAIL_ADDRESS	,
            fnd.	WORK_EMAIL_ADDRESS_CLEANSED	    =	MER_REC.	WORK_EMAIL_ADDRESS_CLEANSED	,
            fnd.	WORK_EMAIL_ADDRESS_VALIDATED	  =	MER_REC.	WORK_EMAIL_ADDRESS_VALIDATED	,
            fnd.	WORK_EMAIL_LAST_UPDATE_DT	      =	MER_REC.	WORK_EMAIL_LAST_UPDATE_DT	,
            fnd.	WORK_EMAIL_FAILURE_COUNT	      =	MER_REC.	WORK_EMAIL_FAILURE_COUNT	,
            fnd.	STATEMENT_EMAIL_ADDRESS	        =	MER_REC.	STATEMENT_EMAIL_ADDRESS	,
            fnd.	STATEMENT_EMAIL_ADDRESS_CLEAN	  =	MER_REC.	STATEMENT_EMAIL_ADDRESS_CLEAN	,
            fnd.	STATEMENT_EMAIL_ADDRESS_VALID	  =	MER_REC.	STATEMENT_EMAIL_ADDRESS_VALID	,
            fnd.	STATEMENT_EMAIL_LAST_UPDATE_DT	=	MER_REC.	STATEMENT_EMAIL_LAST_UPDATE_DT	,
            fnd.	STATEMENT_EMAIL_FAILURE_COUNT	  =	MER_REC.	STATEMENT_EMAIL_FAILURE_COUNT	,
            fnd.	ECOMMERCE_EMAIL_ADDRESS	        =	MER_REC.	ECOMMERCE_EMAIL_ADDRESS	,
            fnd.	ECOMMERCE_EMAIL_ADDRESS_CLEAN	  =	MER_REC.	ECOMMERCE_EMAIL_ADDRESS_CLEAN	,
            fnd.	ECOMMERCE_EMAIL_ADDRESS_VALID	  =	MER_REC.	ECOMMERCE_EMAIL_ADDRESS_VALID	,
            fnd.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	=	MER_REC.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	,
            fnd.	ECOMMERCE_EMAIL_FAILURE_COUNT	  =	MER_REC.	ECOMMERCE_EMAIL_FAILURE_COUNT	,
            fnd.	POPULATED_CELL_NO	              =	MER_REC.	POPULATED_CELL_NO	,
            fnd.	HOME_CELL_NO	                  =	MER_REC.	HOME_CELL_NO	,
            fnd.	HOME_CELL_NO_CLEANSED	          =	MER_REC.	HOME_CELL_NO_CLEANSED	,
            fnd.	HOME_CELL_LAST_UPDATED_DATE	    =	MER_REC.	HOME_CELL_LAST_UPDATED_DATE	,
            fnd.	HOME_CELL_FAILURE_COUNT	        =	MER_REC.	HOME_CELL_FAILURE_COUNT	,
            fnd.	WORK_CELL_NO	                  =	MER_REC.	WORK_CELL_NO	,
            fnd.	WORK_CELL_NO_CLEANSED	          =	MER_REC.	WORK_CELL_NO_CLEANSED	,
            fnd.	WORK_CELL_LAST_UPDATED_DATE	    =	MER_REC.	WORK_CELL_LAST_UPDATED_DATE	,
            fnd.	WORK_CELL_FAILURE_COUNT	        =	MER_REC.	WORK_CELL_FAILURE_COUNT	,
            fnd.	HOME_PHONE_NO	                  =	MER_REC.	HOME_PHONE_NO	,
            fnd.	HOME_PHONE_NO_CLEANSED	        =	MER_REC.	HOME_PHONE_NO_CLEANSED	,
            fnd.	HOME_PHONE_LAST_UPDATED_DATE	  =	MER_REC.	HOME_PHONE_LAST_UPDATED_DATE	,
            fnd.	HOME_PHONE_FAILURE_COUNT	      =	MER_REC.	HOME_PHONE_FAILURE_COUNT	,
            fnd.	WORK_PHONE_NO	                  =	MER_REC.	WORK_PHONE_NO	,
            fnd.	WORK_PHONE_NO_CLEANSED	        =	MER_REC.	WORK_PHONE_NO_CLEANSED	,
            fnd.	WORK_PHONE_LAST_UPDATED_DATE	  =	MER_REC.	WORK_PHONE_LAST_UPDATED_DATE	,
            fnd.	WORK_PHONE_FAILURE_COUNT	      =	MER_REC.	WORK_PHONE_FAILURE_COUNT	,
            fnd.	POSTAL_ADDRESS_LINE_1	          =	MER_REC.	POSTAL_ADDRESS_LINE_1	,
            fnd.	POSTAL_ADDRESS_LINE_2	          =	MER_REC.	POSTAL_ADDRESS_LINE_2	,
            fnd.	POSTAL_SUBURB_NAME	            =	MER_REC.	POSTAL_SUBURB_NAME	,
            fnd.	POST_POSTAL_CODE	              =	MER_REC.	POST_POSTAL_CODE	,
            fnd.	POSTAL_CITY_NAME	              =	MER_REC.	POSTAL_CITY_NAME	,
            fnd.	POSTAL_PROVINCE_NAME	          =	MER_REC.	POSTAL_PROVINCE_NAME	,
            fnd.	POSTAL_ADDRESS_UPDATE_DATE	    =	MER_REC.	POSTAL_ADDRESS_UPDATE_DATE	,
            fnd.	PHYSICAL_ADDRESS_LINE_1	        =	MER_REC.	PHYSICAL_ADDRESS_LINE_1	,
            fnd.	PHYSICAL_ADDRESS_LINE_2	        =	MER_REC.	PHYSICAL_ADDRESS_LINE_2	,
            fnd.	PHYSICAL_SUBURB_NAME	          =	MER_REC.	PHYSICAL_SUBURB_NAME	,
            fnd.	PHYSICAL_POSTAL_CODE	          =	MER_REC.	PHYSICAL_POSTAL_CODE	,
            fnd.	PHYSICAL_CITY_NAME	            =	MER_REC.	PHYSICAL_CITY_NAME	,
            fnd.	PHYSICAL_PROVINCE_NAME	        =	MER_REC.	PHYSICAL_PROVINCE_NAME	,
            fnd.	PHYSICAL_ADDRESS_UPDATE_DATE	  =	MER_REC.	PHYSICAL_ADDRESS_UPDATE_DATE	,
            fnd.	SHIPPING_ADDRESS_LINE_1	        =	MER_REC.	SHIPPING_ADDRESS_LINE_1	,
            fnd.	SHIPPING_ADDRESS_LINE_2	        =	MER_REC.	SHIPPING_ADDRESS_LINE_2	,
            fnd.	SHIPPING_SUBURB_NAME	          =	MER_REC.	SHIPPING_SUBURB_NAME	,
            fnd.	POST_SHIPPING_CODE	            =	MER_REC.	POST_SHIPPING_CODE	,
            fnd.	SHIPPING_CITY_NAME	            =	MER_REC.	SHIPPING_CITY_NAME	,
            fnd.	SHIPPING_PROVINCE_NAME	        =	MER_REC.	SHIPPING_PROVINCE_NAME	,
            fnd.	SHIPPING_ADDRESS_UPDATE_DATE	  =	MER_REC.	SHIPPING_ADDRESS_UPDATE_DATE	,
            fnd.	BILLING_ADDRESS_LINE_1	        =	MER_REC.	BILLING_ADDRESS_LINE_1	,
            fnd.	BILLING_ADDRESS_LINE_2	        =	MER_REC.	BILLING_ADDRESS_LINE_2	,
            fnd.	BILLING_SUBURB_NAME	            =	MER_REC.	BILLING_SUBURB_NAME	,
            fnd.	POST_BILLING_CODE	              =	MER_REC.	POST_BILLING_CODE	,
            fnd.	BILLING_CITY_NAME	              =	MER_REC.	BILLING_CITY_NAME	,
            fnd.	BILLING_PROVINCE_NAME	          =	MER_REC.	BILLING_PROVINCE_NAME	,
            fnd.	BILLING_ADDRESS_UPDATE_DATE	    =	MER_REC.	BILLING_ADDRESS_UPDATE_DATE	,
            fnd.	WW_DM_SMS_OPT_OUT_IND	          =	MER_REC.	WW_DM_SMS_OPT_OUT_IND	,
            fnd.	WW_DM_EMAIL_OPT_OUT_IND	        =	MER_REC.	WW_DM_EMAIL_OPT_OUT_IND	,
            fnd.	WW_DM_POST_OPT_OUT_IND	        =	MER_REC.	WW_DM_POST_OPT_OUT_IND	,
            fnd.	WW_DM_PHONE_OPT_OUT_IND	        =	MER_REC.	WW_DM_PHONE_OPT_OUT_IND	,
            fnd.	WW_MAN_SMS_OPT_OUT_IND	        =	MER_REC.	WW_MAN_SMS_OPT_OUT_IND	,
            fnd.	WW_MAN_EMAIL_OPT_OUT_IND	      =	MER_REC.	WW_MAN_EMAIL_OPT_OUT_IND	,
            fnd.	WW_MAN_POST_OPT_OUT_IND	        =	MER_REC.	WW_MAN_POST_OPT_OUT_IND	,
            fnd.	WW_MAN_PHONE_OPT_OUT_IND	      =	MER_REC.	WW_MAN_PHONE_OPT_OUT_IND	,
            fnd.	WFS_DM_SMS_OPT_OUT_IND	        =	MER_REC.	WFS_DM_SMS_OPT_OUT_IND	,
            fnd.	WFS_DM_EMAIL_OPT_OUT_IND	      =	MER_REC.	WFS_DM_EMAIL_OPT_OUT_IND	,
            fnd.	WFS_DM_POST_OPT_OUT_IND	        =	MER_REC.	WFS_DM_POST_OPT_OUT_IND	,
            fnd.	WFS_DM_PHONE_OPT_OUT_IND	      =	MER_REC.	WFS_DM_PHONE_OPT_OUT_IND	,
            fnd.	WFS_CON_SMS_OPT_OUT_IND	        =	MER_REC.	WFS_CON_SMS_OPT_OUT_IND	,
            fnd.	WFS_CON_EMAIL_OPT_OUT_IND	      =	MER_REC.	WFS_CON_EMAIL_OPT_OUT_IND	,
            fnd.	WFS_CON_POST_OPT_OUT_IND	      =	MER_REC.	WFS_CON_POST_OPT_OUT_IND	,
            fnd.	WFS_CON_PHONE_OPT_OUT_IND	      =	MER_REC.	WFS_CON_PHONE_OPT_OUT_IND	,
            fnd.	PREFERENCE_1_IND	              =	MER_REC.	PREFERENCE_1_IND	,
            fnd.	PREFERENCE_2_IND              	=	MER_REC.	PREFERENCE_2_IND	,
            fnd.	PREFERENCE_3_IND	              =	MER_REC.	PREFERENCE_3_IND	,
            fnd.	PREFERENCE_4_IND              	=	MER_REC.	PREFERENCE_4_IND	,
            fnd.	PREFERENCE_5_IND              	=	MER_REC.	PREFERENCE_5_IND	,
            fnd.	PREFERENCE_6_IND              	=	MER_REC.	PREFERENCE_6_IND	,
            fnd.	PREFERENCE_7_IND	              =	MER_REC.	PREFERENCE_7_IND	,
            fnd.	SWIPE_DEVICE_IND	              =	MER_REC.	SWIPE_DEVICE_IND	,
            fnd.	LAST_SWIPE_DATE	                =	MER_REC.	LAST_SWIPE_DATE	,
            fnd.	FACEBOOK_ID	                    =	MER_REC.	FACEBOOK_ID	,
            fnd.	FACEBOOK_ID_LAST_UPDATED_DATE	  =	MER_REC.	FACEBOOK_ID_LAST_UPDATED_DATE	,
            fnd.	INSTAGRAM_ID	                  =	MER_REC.	INSTAGRAM_ID	,
            fnd.	INSTAGRAM_ID_LAST_UPDATED_DATE	=	MER_REC.	INSTAGRAM_ID_LAST_UPDATED_DATE	,
            fnd.	TWITTER_ID	                    =	MER_REC.	TWITTER_ID	,
            fnd.	TWITTER_ID_LAST_UPDATED_DATE	  =	MER_REC.	TWITTER_ID_LAST_UPDATED_DATE	,
            fnd.	LINKEDIN_ID	                    =	MER_REC.	LINKEDIN_ID	,
            fnd.	LINKEDIN_ID_LAST_UPDATED_DATE	  =	MER_REC.	LINKEDIN_ID_LAST_UPDATED_DATE	,
            fnd.	ROA_MARKETING_CONSENT_IND	      =	MER_REC.	ROA_MARKETING_CONSENT_IND	,
            fnd.	ROA_REGISTERED_LOCATION_NO	    =	MER_REC.	ROA_REGISTERED_LOCATION_NO	,
            fnd.	ROA_COUNTRY_CODE	              =	MER_REC.	ROA_COUNTRY_CODE	,
            fnd.	ROA_ACTIVE_PRODUCT_IND	        =	MER_REC.	ROA_ACTIVE_PRODUCT_IND	,
            fnd.	ROA_LOYALTY_CARD_NO 	          =	MER_REC.	ROA_LOYALTY_CARD_NO 	,
            fnd.	DELETED_IND	                    =	MER_REC.	DELETED_IND	,
            fnd.	OLD_MASTER_SUBSCRIBER_KEY	      =	MER_REC.	OLD_MASTER_SUBSCRIBER_KEY	,
            fnd.	CC360_SEGMENT	                  =	MER_REC.	CC360_SEGMENT	,
            fnd.	CC360_CUSTOM_FIELD_1	=	MER_REC.	CC360_CUSTOM_FIELD_1	,
            fnd.	CC360_CUSTOM_FIELD_2	=	MER_REC.	CC360_CUSTOM_FIELD_2	,
            fnd.	CC360_CUSTOM_FIELD_3	=	MER_REC.	CC360_CUSTOM_FIELD_3	,
            fnd.	CC360_CUSTOM_FIELD_4	=	MER_REC.	CC360_CUSTOM_FIELD_4	,
            fnd.	CC360_CUSTOM_FIELD_5	=	MER_REC.	CC360_CUSTOM_FIELD_5	,
            fnd.	CC360_CUSTOM_FIELD_6	=	MER_REC.	CC360_CUSTOM_FIELD_6	,
            fnd.	CC360_CUSTOM_FIELD_7	=	MER_REC.	CC360_CUSTOM_FIELD_7	,
            fnd.	CC360_CUSTOM_FIELD_8	=	MER_REC.	CC360_CUSTOM_FIELD_8	,
            fnd.	CC360_CUSTOM_FIELD_9	=	MER_REC.	CC360_CUSTOM_FIELD_9	,
            fnd.	CC360_CUSTOM_FIELD_10	=	MER_REC.	CC360_CUSTOM_FIELD_10	,
            fnd.	CC360_CUSTOM_FIELD_11	=	MER_REC.	CC360_CUSTOM_FIELD_11	,
            fnd.	CC360_CUSTOM_FIELD_12	=	MER_REC.	CC360_CUSTOM_FIELD_12	,
            fnd.	CC360_CUSTOM_FIELD_13	=	MER_REC.	CC360_CUSTOM_FIELD_13	,
            fnd.	CC360_CUSTOM_FIELD_14	=	MER_REC.	CC360_CUSTOM_FIELD_14	,
            fnd.	CC360_CUSTOM_FIELD_15	=	MER_REC.	CC360_CUSTOM_FIELD_15	,
            fnd.	CC360_CUSTOM_FIELD_16	=	MER_REC.	CC360_CUSTOM_FIELD_16	,
            fnd.	CC360_CUSTOM_FIELD_17	=	MER_REC.	CC360_CUSTOM_FIELD_17	,
            fnd.	CC360_CUSTOM_FIELD_18	=	MER_REC.	CC360_CUSTOM_FIELD_18	,
            fnd.	CC360_CUSTOM_FIELD_19	=	MER_REC.	CC360_CUSTOM_FIELD_19	,
            fnd.	CC360_CUSTOM_FIELD_20	=	MER_REC.	CC360_CUSTOM_FIELD_20	,
            fnd.	BEAN_LAST_MODIFIED_DATE 	      =	MER_REC.	BEAN_LAST_MODIFIED_DATE 	,
            fnd.  last_updated_date               = g_date
   WHERE    NVL(fnd.	SUBSCRIBER_KEY                  ,'0')  <>	MER_REC.	SUBSCRIBER_KEY	 OR
            NVL(fnd.	MASTER_SUBSCRIBER_KEY	          ,'0')  <>	MER_REC.	MASTER_SUBSCRIBER_KEY	 OR
            NVL(fnd.	MASTER_BEAN_ID	                ,'0')  <>	MER_REC.	MASTER_BEAN_ID	 OR
            NVL(fnd.	SOURCE	                        ,'0')  <>	MER_REC.	SOURCE	 OR
            NVL(fnd.	SOURCE_KEY	                    ,'0')  <>	MER_REC.	SOURCE_KEY	 OR
            NVL(fnd.	VISION_CUSTOMER_NO	            ,0)  <>	MER_REC.	VISION_CUSTOMER_NO	 OR
            NVL(fnd.	IDENTITY_DOCUMENT_NO	          ,0)  <>	MER_REC.	IDENTITY_DOCUMENT_NO	 OR
            NVL(fnd.	IDENTITY_DOCUMENT_NO_CREATE_DT	,'01 JAN 2000')  <>	MER_REC.	IDENTITY_DOCUMENT_NO_CREATE_DT	 OR
            NVL(fnd.	PASSPORT_NO	                    ,'0')  <>	MER_REC.	PASSPORT_NO	 OR
            NVL(fnd.	PASSPORT_COUNTRY_CODE	          ,'0')  <>	MER_REC.	PASSPORT_COUNTRY_CODE	 OR
            NVL(fnd.	PASSPORT_NO_CREATE_DATE	        ,'01 JAN 2000')  <>	MER_REC.	PASSPORT_NO_CREATE_DATE	 OR
            NVL(fnd.	CREATE_DATE	                    ,'01 JAN 2000')  <>	MER_REC.	CREATE_DATE	 OR
            NVL(fnd.	TITLE	                          ,'0')  <>	MER_REC.	TITLE	 OR
            NVL(fnd.	INITIALS	                      ,'0')  <>	MER_REC.	INITIALS	 OR
            NVL(fnd.	FIRST_NAME	                    ,'0')  <>	MER_REC.	FIRST_NAME	 OR
            NVL(fnd.	LAST_NAME	                      ,'0')  <>	MER_REC.	LAST_NAME	 OR
            NVL(fnd.	BIRTH_DATE	                    ,'01 JAN 2000')  <>	MER_REC.	BIRTH_DATE	 OR
            NVL(fnd.	GENDER	                        ,'0')  <>	MER_REC.	GENDER	 OR
            NVL(fnd.	PREFERRED_LANGUAGE	            ,'0')  <>	MER_REC.	PREFERRED_LANGUAGE	 OR
            NVL(fnd.	POPULATED_EMAIL_ADDRESS	        ,'0')  <>	MER_REC.	POPULATED_EMAIL_ADDRESS	 OR
            NVL(fnd.	HOME_EMAIL_ADDRESS	            ,'0')  <>	MER_REC.	HOME_EMAIL_ADDRESS	 OR
            NVL(fnd.	HOME_EMAIL_ADDRESS_CLEANSED	    ,'0')  <>	MER_REC.	HOME_EMAIL_ADDRESS_CLEANSED	 OR
            NVL(fnd.	HOME_EMAIL_ADDRESS_VALIDATED	  ,'0')  <>	MER_REC.	HOME_EMAIL_ADDRESS_VALIDATED	 OR
            NVL(fnd.	HOME_EMAIL_LAST_UPDATE_DT	      ,'01 JAN 2000')  <>	MER_REC.	HOME_EMAIL_LAST_UPDATE_DT	 OR
            NVL(fnd.	HOME_EMAIL_FAILURE_COUNT	      ,0)  <>	MER_REC.	HOME_EMAIL_FAILURE_COUNT	 OR
            NVL(fnd.	WORK_EMAIL_ADDRESS	            ,'0')  <>	MER_REC.	WORK_EMAIL_ADDRESS	 OR
            NVL(fnd.	WORK_EMAIL_ADDRESS_CLEANSED	    ,'0')  <>	MER_REC.	WORK_EMAIL_ADDRESS_CLEANSED	 OR
            NVL(fnd.	WORK_EMAIL_ADDRESS_VALIDATED	  ,'0')  <>	MER_REC.	WORK_EMAIL_ADDRESS_VALIDATED	 OR
            NVL(fnd.	WORK_EMAIL_LAST_UPDATE_DT	      ,'01 JAN 2000')  <>	MER_REC.	WORK_EMAIL_LAST_UPDATE_DT	 OR
            NVL(fnd.	WORK_EMAIL_FAILURE_COUNT	      ,0)  <>	MER_REC.	WORK_EMAIL_FAILURE_COUNT	 OR
            NVL(fnd.	STATEMENT_EMAIL_ADDRESS	        ,'0')  <>	MER_REC.	STATEMENT_EMAIL_ADDRESS	 OR
            NVL(fnd.	STATEMENT_EMAIL_ADDRESS_CLEAN	  ,'0')  <>	MER_REC.	STATEMENT_EMAIL_ADDRESS_CLEAN	 OR
            NVL(fnd.	STATEMENT_EMAIL_ADDRESS_VALID	  ,'0')  <>	MER_REC.	STATEMENT_EMAIL_ADDRESS_VALID	 OR
            NVL(fnd.	STATEMENT_EMAIL_LAST_UPDATE_DT	,'01 JAN 2000')  <>	MER_REC.	STATEMENT_EMAIL_LAST_UPDATE_DT	 OR
            NVL(fnd.	STATEMENT_EMAIL_FAILURE_COUNT	  ,0)  <>	MER_REC.	STATEMENT_EMAIL_FAILURE_COUNT	 OR
            NVL(fnd.	ECOMMERCE_EMAIL_ADDRESS	        ,'0')  <>	MER_REC.	ECOMMERCE_EMAIL_ADDRESS	 OR
            NVL(fnd.	ECOMMERCE_EMAIL_ADDRESS_CLEAN	  ,'0')  <>	MER_REC.	ECOMMERCE_EMAIL_ADDRESS_CLEAN	 OR
            NVL(fnd.	ECOMMERCE_EMAIL_ADDRESS_VALID	  ,'0')  <>	MER_REC.	ECOMMERCE_EMAIL_ADDRESS_VALID	 OR
            NVL(fnd.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	,'01 JAN 2000')  <>	MER_REC.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	 OR
            NVL(fnd.	ECOMMERCE_EMAIL_FAILURE_COUNT	  ,0)  <>	MER_REC.	ECOMMERCE_EMAIL_FAILURE_COUNT	 OR
            NVL(fnd.	POPULATED_CELL_NO	              ,'0')  <>	MER_REC.	POPULATED_CELL_NO	 OR
            NVL(fnd.	HOME_CELL_NO	                  ,'0')  <>	MER_REC.	HOME_CELL_NO	 OR
            NVL(fnd.	HOME_CELL_NO_CLEANSED	          ,'0')  <>	MER_REC.	HOME_CELL_NO_CLEANSED	 OR
            NVL(fnd.	HOME_CELL_LAST_UPDATED_DATE	    ,'01 JAN 2000')  <>	MER_REC.	HOME_CELL_LAST_UPDATED_DATE	 OR
            NVL(fnd.	HOME_CELL_FAILURE_COUNT	        ,0)  <>	MER_REC.	HOME_CELL_FAILURE_COUNT	 OR
            NVL(fnd.	WORK_CELL_NO	                  ,'0')  <>	MER_REC.	WORK_CELL_NO	 OR
            NVL(fnd.	WORK_CELL_NO_CLEANSED	          ,'0')  <>	MER_REC.	WORK_CELL_NO_CLEANSED	 OR
            NVL(fnd.	WORK_CELL_LAST_UPDATED_DATE	    ,'01 JAN 2000')  <>	MER_REC.	WORK_CELL_LAST_UPDATED_DATE	 OR
            NVL(fnd.	WORK_CELL_FAILURE_COUNT	        ,0)  <>	MER_REC.	WORK_CELL_FAILURE_COUNT	 OR
            NVL(fnd.	HOME_PHONE_NO	                  ,'0')  <>	MER_REC.	HOME_PHONE_NO	 OR
            NVL(fnd.	HOME_PHONE_NO_CLEANSED	        ,'0')  <>	MER_REC.	HOME_PHONE_NO_CLEANSED	 OR
            NVL(fnd.	HOME_PHONE_LAST_UPDATED_DATE	  ,'01 JAN 2000')  <>	MER_REC.	HOME_PHONE_LAST_UPDATED_DATE	 OR
            NVL(fnd.	HOME_PHONE_FAILURE_COUNT	      ,0)  <>	MER_REC.	HOME_PHONE_FAILURE_COUNT	 OR
            NVL(fnd.	WORK_PHONE_NO	                  ,'0')  <>	MER_REC.	WORK_PHONE_NO	 OR
            NVL(fnd.	WORK_PHONE_NO_CLEANSED	        ,'0')  <>	MER_REC.	WORK_PHONE_NO_CLEANSED	 OR
            NVL(fnd.	WORK_PHONE_LAST_UPDATED_DATE	  ,'01 JAN 2000')  <>	MER_REC.	WORK_PHONE_LAST_UPDATED_DATE	 OR
            NVL(fnd.	WORK_PHONE_FAILURE_COUNT	      ,0)  <>	MER_REC.	WORK_PHONE_FAILURE_COUNT	 OR
            NVL(fnd.	POSTAL_ADDRESS_LINE_1	          ,'0')  <>	MER_REC.	POSTAL_ADDRESS_LINE_1	 OR
            NVL(fnd.	POSTAL_ADDRESS_LINE_2	          ,'0')  <>	MER_REC.	POSTAL_ADDRESS_LINE_2	 OR
            NVL(fnd.	POSTAL_SUBURB_NAME	            ,'0')  <>	MER_REC.	POSTAL_SUBURB_NAME	 OR
            NVL(fnd.	POST_POSTAL_CODE	              ,'0')  <>	MER_REC.	POST_POSTAL_CODE	 OR
            NVL(fnd.	POSTAL_CITY_NAME	              ,'0')  <>	MER_REC.	POSTAL_CITY_NAME	 OR
            NVL(fnd.	POSTAL_PROVINCE_NAME	          ,'0')  <>	MER_REC.	POSTAL_PROVINCE_NAME	 OR
            NVL(fnd.	POSTAL_ADDRESS_UPDATE_DATE	    ,'01 JAN 2000')  <>	MER_REC.	POSTAL_ADDRESS_UPDATE_DATE	 OR
            NVL(fnd.	PHYSICAL_ADDRESS_LINE_1	        ,'0')  <>	MER_REC.	PHYSICAL_ADDRESS_LINE_1	 OR
            NVL(fnd.	PHYSICAL_ADDRESS_LINE_2	        ,'0')  <>	MER_REC.	PHYSICAL_ADDRESS_LINE_2	 OR
            NVL(fnd.	PHYSICAL_SUBURB_NAME	          ,'0')  <>	MER_REC.	PHYSICAL_SUBURB_NAME	 OR
            NVL(fnd.	PHYSICAL_POSTAL_CODE	          ,'0')  <>	MER_REC.	PHYSICAL_POSTAL_CODE	 OR
            NVL(fnd.	PHYSICAL_CITY_NAME	            ,'0')  <>	MER_REC.	PHYSICAL_CITY_NAME	 OR
            NVL(fnd.	PHYSICAL_PROVINCE_NAME	        ,'0')  <>	MER_REC.	PHYSICAL_PROVINCE_NAME	 OR
            NVL(fnd.	PHYSICAL_ADDRESS_UPDATE_DATE	  ,'01 JAN 2000')  <>	MER_REC.	PHYSICAL_ADDRESS_UPDATE_DATE	 OR
            NVL(fnd.	SHIPPING_ADDRESS_LINE_1	        ,'0')  <>	MER_REC.	SHIPPING_ADDRESS_LINE_1	 OR
            NVL(fnd.	SHIPPING_ADDRESS_LINE_2	        ,'0')  <>	MER_REC.	SHIPPING_ADDRESS_LINE_2	 OR
            NVL(fnd.	SHIPPING_SUBURB_NAME	          ,'0')  <>	MER_REC.	SHIPPING_SUBURB_NAME	 OR
            NVL(fnd.	POST_SHIPPING_CODE	            ,'0')  <>	MER_REC.	POST_SHIPPING_CODE	 OR
            NVL(fnd.	SHIPPING_CITY_NAME	            ,'0')  <>	MER_REC.	SHIPPING_CITY_NAME	 OR
            NVL(fnd.	SHIPPING_PROVINCE_NAME	        ,'0')  <>	MER_REC.	SHIPPING_PROVINCE_NAME	 OR
            NVL(fnd.	SHIPPING_ADDRESS_UPDATE_DATE	  ,'01 JAN 2000')  <>	MER_REC.	SHIPPING_ADDRESS_UPDATE_DATE	 OR
            NVL(fnd.	BILLING_ADDRESS_LINE_1	        ,'0')  <>	MER_REC.	BILLING_ADDRESS_LINE_1	 OR
            NVL(fnd.	BILLING_ADDRESS_LINE_2	        ,'0')  <>	MER_REC.	BILLING_ADDRESS_LINE_2	 OR
            NVL(fnd.	BILLING_SUBURB_NAME	            ,'0')  <>	MER_REC.	BILLING_SUBURB_NAME	 OR
            NVL(fnd.	POST_BILLING_CODE	              ,'0')  <>	MER_REC.	POST_BILLING_CODE	 OR
            NVL(fnd.	BILLING_CITY_NAME	              ,'0')  <>	MER_REC.	BILLING_CITY_NAME	 OR
            NVL(fnd.	BILLING_PROVINCE_NAME	          ,'0')  <>	MER_REC.	BILLING_PROVINCE_NAME	 OR
            NVL(fnd.	BILLING_ADDRESS_UPDATE_DATE	    ,'01 JAN 2000')  <>	MER_REC.	BILLING_ADDRESS_UPDATE_DATE	 OR
            NVL(fnd.	WW_DM_SMS_OPT_OUT_IND	          ,9)  <>	MER_REC.	WW_DM_SMS_OPT_OUT_IND	 OR
            NVL(fnd.	WW_DM_EMAIL_OPT_OUT_IND	        ,9)  <>	MER_REC.	WW_DM_EMAIL_OPT_OUT_IND	 OR
            NVL(fnd.	WW_DM_POST_OPT_OUT_IND	        ,9)  <>	MER_REC.	WW_DM_POST_OPT_OUT_IND	 OR
            NVL(fnd.	WW_DM_PHONE_OPT_OUT_IND	        ,9)  <>	MER_REC.	WW_DM_PHONE_OPT_OUT_IND	 OR
            NVL(fnd.	WW_MAN_SMS_OPT_OUT_IND	        ,9)  <>	MER_REC.	WW_MAN_SMS_OPT_OUT_IND	 OR
            NVL(fnd.	WW_MAN_EMAIL_OPT_OUT_IND	      ,9)  <>	MER_REC.	WW_MAN_EMAIL_OPT_OUT_IND	 OR
            NVL(fnd.	WW_MAN_POST_OPT_OUT_IND	        ,9)  <>	MER_REC.	WW_MAN_POST_OPT_OUT_IND	 OR
            NVL(fnd.	WW_MAN_PHONE_OPT_OUT_IND	      ,9)  <>	MER_REC.	WW_MAN_PHONE_OPT_OUT_IND	 OR
            NVL(fnd.	WFS_DM_SMS_OPT_OUT_IND	        ,9)  <>	MER_REC.	WFS_DM_SMS_OPT_OUT_IND	 OR
            NVL(fnd.	WFS_DM_EMAIL_OPT_OUT_IND	      ,9)  <>	MER_REC.	WFS_DM_EMAIL_OPT_OUT_IND	 OR
            NVL(fnd.	WFS_DM_POST_OPT_OUT_IND	        ,9)  <>	MER_REC.	WFS_DM_POST_OPT_OUT_IND	 OR
            NVL(fnd.	WFS_DM_PHONE_OPT_OUT_IND	      ,9)  <>	MER_REC.	WFS_DM_PHONE_OPT_OUT_IND	 OR
            NVL(fnd.	WFS_CON_SMS_OPT_OUT_IND	        ,9)  <>	MER_REC.	WFS_CON_SMS_OPT_OUT_IND	 OR
            NVL(fnd.	WFS_CON_EMAIL_OPT_OUT_IND	      ,9)  <>	MER_REC.	WFS_CON_EMAIL_OPT_OUT_IND	 OR
            NVL(fnd.	WFS_CON_POST_OPT_OUT_IND	      ,9)  <>	MER_REC.	WFS_CON_POST_OPT_OUT_IND	 OR
            NVL(fnd.	WFS_CON_PHONE_OPT_OUT_IND	      ,9)  <>	MER_REC.	WFS_CON_PHONE_OPT_OUT_IND	 OR
            NVL(fnd.	PREFERENCE_1_IND	              ,0)  <>	MER_REC.	PREFERENCE_1_IND	 OR
            NVL(fnd.	PREFERENCE_2_IND              	,0)  <>	MER_REC.	PREFERENCE_2_IND	 OR
            NVL(fnd.	PREFERENCE_3_IND	              ,0)  <>	MER_REC.	PREFERENCE_3_IND	 OR
            NVL(fnd.	PREFERENCE_4_IND              	,0)  <>	MER_REC.	PREFERENCE_4_IND	 OR
            NVL(fnd.	PREFERENCE_5_IND              	,0)  <>	MER_REC.	PREFERENCE_5_IND	 OR
            NVL(fnd.	PREFERENCE_6_IND              	,0)  <>	MER_REC.	PREFERENCE_6_IND	 OR
            NVL(fnd.	PREFERENCE_7_IND	              ,0)  <>	MER_REC.	PREFERENCE_7_IND	 OR
            NVL(fnd.	SWIPE_DEVICE_IND	              ,0)  <>	MER_REC.	SWIPE_DEVICE_IND	 OR
            NVL(fnd.	LAST_SWIPE_DATE	                ,'01 JAN 2000')  <>	MER_REC.	LAST_SWIPE_DATE	 OR
            NVL(fnd.	FACEBOOK_ID	                    ,'0')  <>	MER_REC.	FACEBOOK_ID	 OR
            NVL(fnd.	FACEBOOK_ID_LAST_UPDATED_DATE	  ,'01 JAN 2000')  <>	MER_REC.	FACEBOOK_ID_LAST_UPDATED_DATE	 OR
            NVL(fnd.	INSTAGRAM_ID	                  ,'0')  <>	MER_REC.	INSTAGRAM_ID	 OR
            NVL(fnd.	INSTAGRAM_ID_LAST_UPDATED_DATE	,'01 JAN 2000')  <>	MER_REC.	INSTAGRAM_ID_LAST_UPDATED_DATE	 OR
            NVL(fnd.	TWITTER_ID	                    ,'0')  <>	MER_REC.	TWITTER_ID	 OR
            NVL(fnd.	TWITTER_ID_LAST_UPDATED_DATE	  ,'01 JAN 2000')  <>	MER_REC.	TWITTER_ID_LAST_UPDATED_DATE	 OR
            NVL(fnd.	LINKEDIN_ID	                    ,'0')  <>	MER_REC.	LINKEDIN_ID	 OR
            NVL(fnd.	LINKEDIN_ID_LAST_UPDATED_DATE	  ,'01 JAN 2000')  <>	MER_REC.	LINKEDIN_ID_LAST_UPDATED_DATE	 OR
            NVL(fnd.	ROA_MARKETING_CONSENT_IND	      ,'0')  <>	MER_REC.	ROA_MARKETING_CONSENT_IND	 OR
            NVL(fnd.	ROA_REGISTERED_LOCATION_NO	    ,0)  <>	MER_REC.	ROA_REGISTERED_LOCATION_NO	 OR
            NVL(fnd.	ROA_COUNTRY_CODE	              ,'0')  <>	MER_REC.	ROA_COUNTRY_CODE	 OR
            NVL(fnd.	ROA_ACTIVE_PRODUCT_IND	        ,0)  <>	MER_REC.	ROA_ACTIVE_PRODUCT_IND	 OR
            NVL(fnd.	ROA_LOYALTY_CARD_NO 	          ,0)  <>	MER_REC.	ROA_LOYALTY_CARD_NO 	 OR
            NVL(fnd.	DELETED_IND	                    ,0)  <>	MER_REC.	DELETED_IND	 OR
            NVL(fnd.	OLD_MASTER_SUBSCRIBER_KEY	      ,'0')  <>	MER_REC.	OLD_MASTER_SUBSCRIBER_KEY	 OR
            NVL(fnd.	CC360_SEGMENT	                  ,'0')  <>	MER_REC.	CC360_SEGMENT	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_1	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_1	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_2	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_2	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_3	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_3	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_4	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_4	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_5	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_5	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_6	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_6	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_7	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_7	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_8	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_8	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_9	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_9	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_10	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_10	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_11	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_11	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_12	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_12	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_13	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_13	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_14	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_14	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_15	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_15	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_16	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_16	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_17	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_17	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_18	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_18	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_19	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_19	 OR
            NVL(fnd.	CC360_CUSTOM_FIELD_20	,'0')  <>	MER_REC.	CC360_CUSTOM_FIELD_20	 OR
            NVL(fnd.	BEAN_LAST_MODIFIED_DATE 	      ,'01 JAN 2000')  <>	MER_REC.	BEAN_LAST_MODIFIED_DATE 	         
   when not matched then
   insert
          (         
          SUBSCRIBER_KEY,
          BEAN_ID,
          MASTER_SUBSCRIBER_KEY,
          MASTER_BEAN_ID,
          SOURCE,
          SOURCE_KEY,
          VISION_CUSTOMER_NO,
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
          HOME_EMAIL_ADDRESS_CLEANSED,
          HOME_EMAIL_ADDRESS_VALIDATED,
          HOME_EMAIL_LAST_UPDATE_DT,
          HOME_EMAIL_FAILURE_COUNT,
          WORK_EMAIL_ADDRESS,
          WORK_EMAIL_ADDRESS_CLEANSED,
          WORK_EMAIL_ADDRESS_VALIDATED,
          WORK_EMAIL_LAST_UPDATE_DT,
          WORK_EMAIL_FAILURE_COUNT,
          STATEMENT_EMAIL_ADDRESS,
          STATEMENT_EMAIL_ADDRESS_CLEAN,
          STATEMENT_EMAIL_ADDRESS_VALID,
          STATEMENT_EMAIL_LAST_UPDATE_DT,
          STATEMENT_EMAIL_FAILURE_COUNT,
          ECOMMERCE_EMAIL_ADDRESS,
          ECOMMERCE_EMAIL_ADDRESS_CLEAN,
          ECOMMERCE_EMAIL_ADDRESS_VALID,
          ECOMMERCE_EMAIL_LAST_UPDATE_DT,
          ECOMMERCE_EMAIL_FAILURE_COUNT,
          POPULATED_CELL_NO,
          HOME_CELL_NO,
          HOME_CELL_NO_CLEANSED,
          HOME_CELL_LAST_UPDATED_DATE,
          HOME_CELL_FAILURE_COUNT,
          WORK_CELL_NO,
          WORK_CELL_NO_CLEANSED,
          WORK_CELL_LAST_UPDATED_DATE,
          WORK_CELL_FAILURE_COUNT,
          HOME_PHONE_NO,
          HOME_PHONE_NO_CLEANSED,
          HOME_PHONE_LAST_UPDATED_DATE,
          HOME_PHONE_FAILURE_COUNT,
          WORK_PHONE_NO,
          WORK_PHONE_NO_CLEANSED,
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
          DELETED_IND,
          OLD_MASTER_SUBSCRIBER_KEY,
          CC360_SEGMENT,
          CC360_CUSTOM_FIELD_1,
          CC360_CUSTOM_FIELD_2,
          CC360_CUSTOM_FIELD_3,
          CC360_CUSTOM_FIELD_4,
          CC360_CUSTOM_FIELD_5,
          CC360_CUSTOM_FIELD_6,
          CC360_CUSTOM_FIELD_7,
          CC360_CUSTOM_FIELD_8,
          CC360_CUSTOM_FIELD_9,
          CC360_CUSTOM_FIELD_10,
          CC360_CUSTOM_FIELD_11,
          CC360_CUSTOM_FIELD_12,
          CC360_CUSTOM_FIELD_13,
          CC360_CUSTOM_FIELD_14,
          CC360_CUSTOM_FIELD_15,
          CC360_CUSTOM_FIELD_16,
          CC360_CUSTOM_FIELD_17,
          CC360_CUSTOM_FIELD_18,
          CC360_CUSTOM_FIELD_19,
          CC360_CUSTOM_FIELD_20,
          BEAN_LAST_MODIFIED_DATE,
          LAST_UPDATED_DATE
          )
  values
          (         
          MER_REC.	SUBSCRIBER_KEY	,
          MER_REC.	BEAN_ID	,
          MER_REC.	MASTER_SUBSCRIBER_KEY	,
          MER_REC.	MASTER_BEAN_ID	,
          MER_REC.	SOURCE	,
          MER_REC.	SOURCE_KEY	,
          MER_REC.	VISION_CUSTOMER_NO	,
          MER_REC.	IDENTITY_DOCUMENT_NO	,
          MER_REC.	IDENTITY_DOCUMENT_NO_CREATE_DT	,
          MER_REC.	PASSPORT_NO	,
          MER_REC.	PASSPORT_COUNTRY_CODE	,
          MER_REC.	PASSPORT_NO_CREATE_DATE	,
          MER_REC.	CREATE_DATE	,
          MER_REC.	TITLE	,
          MER_REC.	INITIALS	,
          MER_REC.	FIRST_NAME	,
          MER_REC.	LAST_NAME	,
          MER_REC.	BIRTH_DATE	,
          MER_REC.	GENDER	,
          MER_REC.	PREFERRED_LANGUAGE	,
          MER_REC.	POPULATED_EMAIL_ADDRESS	,
          MER_REC.	HOME_EMAIL_ADDRESS	,
          MER_REC.	HOME_EMAIL_ADDRESS_CLEANSED	,
          MER_REC.	HOME_EMAIL_ADDRESS_VALIDATED	,
          MER_REC.	HOME_EMAIL_LAST_UPDATE_DT	,
          MER_REC.	HOME_EMAIL_FAILURE_COUNT	,
          MER_REC.	WORK_EMAIL_ADDRESS	,
          MER_REC.	WORK_EMAIL_ADDRESS_CLEANSED	,
          MER_REC.	WORK_EMAIL_ADDRESS_VALIDATED	,
          MER_REC.	WORK_EMAIL_LAST_UPDATE_DT	,
          MER_REC.	WORK_EMAIL_FAILURE_COUNT	,
          MER_REC.	STATEMENT_EMAIL_ADDRESS	,
          MER_REC.	STATEMENT_EMAIL_ADDRESS_CLEAN	,
          MER_REC.	STATEMENT_EMAIL_ADDRESS_VALID	,
          MER_REC.	STATEMENT_EMAIL_LAST_UPDATE_DT	,
          MER_REC.	STATEMENT_EMAIL_FAILURE_COUNT	,
          MER_REC.	ECOMMERCE_EMAIL_ADDRESS	,
          MER_REC.	ECOMMERCE_EMAIL_ADDRESS_CLEAN	,
          MER_REC.	ECOMMERCE_EMAIL_ADDRESS_VALID	,
          MER_REC.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	,
          MER_REC.	ECOMMERCE_EMAIL_FAILURE_COUNT	,
          MER_REC.	POPULATED_CELL_NO	,
          MER_REC.	HOME_CELL_NO	,
          MER_REC.	HOME_CELL_NO_CLEANSED	,
          MER_REC.	HOME_CELL_LAST_UPDATED_DATE	,
          MER_REC.	HOME_CELL_FAILURE_COUNT	,
          MER_REC.	WORK_CELL_NO	,
          MER_REC.	WORK_CELL_NO_CLEANSED	,
          MER_REC.	WORK_CELL_LAST_UPDATED_DATE	,
          MER_REC.	WORK_CELL_FAILURE_COUNT	,
          MER_REC.	HOME_PHONE_NO	,
          MER_REC.	HOME_PHONE_NO_CLEANSED	,
          MER_REC.	HOME_PHONE_LAST_UPDATED_DATE	,
          MER_REC.	HOME_PHONE_FAILURE_COUNT	,
          MER_REC.	WORK_PHONE_NO	,
          MER_REC.	WORK_PHONE_NO_CLEANSED	,
          MER_REC.	WORK_PHONE_LAST_UPDATED_DATE	,
          MER_REC.	WORK_PHONE_FAILURE_COUNT	,
          MER_REC.	POSTAL_ADDRESS_LINE_1	,
          MER_REC.	POSTAL_ADDRESS_LINE_2	,
          MER_REC.	POSTAL_SUBURB_NAME	,
          MER_REC.	POST_POSTAL_CODE	,
          MER_REC.	POSTAL_CITY_NAME	,
          MER_REC.	POSTAL_PROVINCE_NAME	,
          MER_REC.	POSTAL_ADDRESS_UPDATE_DATE	,
          MER_REC.	PHYSICAL_ADDRESS_LINE_1	,
          MER_REC.	PHYSICAL_ADDRESS_LINE_2	,
          MER_REC.	PHYSICAL_SUBURB_NAME	,
          MER_REC.	PHYSICAL_POSTAL_CODE	,
          MER_REC.	PHYSICAL_CITY_NAME	,
          MER_REC.	PHYSICAL_PROVINCE_NAME	,
          MER_REC.	PHYSICAL_ADDRESS_UPDATE_DATE	,
          MER_REC.	SHIPPING_ADDRESS_LINE_1	,
          MER_REC.	SHIPPING_ADDRESS_LINE_2	,
          MER_REC.	SHIPPING_SUBURB_NAME	,
          MER_REC.	POST_SHIPPING_CODE	,
          MER_REC.	SHIPPING_CITY_NAME	,
          MER_REC.	SHIPPING_PROVINCE_NAME	,
          MER_REC.	SHIPPING_ADDRESS_UPDATE_DATE	,
          MER_REC.	BILLING_ADDRESS_LINE_1	,
          MER_REC.	BILLING_ADDRESS_LINE_2	,
          MER_REC.	BILLING_SUBURB_NAME	,
          MER_REC.	POST_BILLING_CODE	,
          MER_REC.	BILLING_CITY_NAME	,
          MER_REC.	BILLING_PROVINCE_NAME	,
          MER_REC.	BILLING_ADDRESS_UPDATE_DATE	,
          MER_REC.	WW_DM_SMS_OPT_OUT_IND	,
          MER_REC.	WW_DM_EMAIL_OPT_OUT_IND	,
          MER_REC.	WW_DM_POST_OPT_OUT_IND	,
          MER_REC.	WW_DM_PHONE_OPT_OUT_IND	,
          MER_REC.	WW_MAN_SMS_OPT_OUT_IND	,
          MER_REC.	WW_MAN_EMAIL_OPT_OUT_IND	,
          MER_REC.	WW_MAN_POST_OPT_OUT_IND	,
          MER_REC.	WW_MAN_PHONE_OPT_OUT_IND	,
          MER_REC.	WFS_DM_SMS_OPT_OUT_IND	,
          MER_REC.	WFS_DM_EMAIL_OPT_OUT_IND	,
          MER_REC.	WFS_DM_POST_OPT_OUT_IND	,
          MER_REC.	WFS_DM_PHONE_OPT_OUT_IND	,
          MER_REC.	WFS_CON_SMS_OPT_OUT_IND	,
          MER_REC.	WFS_CON_EMAIL_OPT_OUT_IND	,
          MER_REC.	WFS_CON_POST_OPT_OUT_IND	,
          MER_REC.	WFS_CON_PHONE_OPT_OUT_IND	,
          MER_REC.	PREFERENCE_1_IND	,
          MER_REC.	PREFERENCE_2_IND	,
          MER_REC.	PREFERENCE_3_IND	,
          MER_REC.	PREFERENCE_4_IND	,
          MER_REC.	PREFERENCE_5_IND	,
          MER_REC.	PREFERENCE_6_IND	,
          MER_REC.	PREFERENCE_7_IND	,
          MER_REC.	SWIPE_DEVICE_IND	,
          MER_REC.	LAST_SWIPE_DATE	,
          MER_REC.	FACEBOOK_ID	,
          MER_REC.	FACEBOOK_ID_LAST_UPDATED_DATE	,
          MER_REC.	INSTAGRAM_ID	,
          MER_REC.	INSTAGRAM_ID_LAST_UPDATED_DATE	,
          MER_REC.	TWITTER_ID	,
          MER_REC.	TWITTER_ID_LAST_UPDATED_DATE	,
          MER_REC.	LINKEDIN_ID	,
          MER_REC.	LINKEDIN_ID_LAST_UPDATED_DATE	,
          MER_REC.	ROA_MARKETING_CONSENT_IND	,
          MER_REC.	ROA_REGISTERED_LOCATION_NO	,
          MER_REC.	ROA_COUNTRY_CODE	,
          MER_REC.	ROA_ACTIVE_PRODUCT_IND	,
          MER_REC.	ROA_LOYALTY_CARD_NO 	,
          MER_REC.  DELETED_IND,
          MER_REC.	OLD_MASTER_SUBSCRIBER_KEY,
          MER_REC.	CC360_SEGMENT	,
          MER_REC.	CC360_CUSTOM_FIELD_1	,
          MER_REC.	CC360_CUSTOM_FIELD_2	,
          MER_REC.	CC360_CUSTOM_FIELD_3	,
          MER_REC.	CC360_CUSTOM_FIELD_4	,
          MER_REC.	CC360_CUSTOM_FIELD_5	,
          MER_REC.	CC360_CUSTOM_FIELD_6	,
          MER_REC.	CC360_CUSTOM_FIELD_7	,
          MER_REC.	CC360_CUSTOM_FIELD_8	,
          MER_REC.	CC360_CUSTOM_FIELD_9	,
          MER_REC.	CC360_CUSTOM_FIELD_10	,
          MER_REC.	CC360_CUSTOM_FIELD_11	,
          MER_REC.	CC360_CUSTOM_FIELD_12	,
          MER_REC.	CC360_CUSTOM_FIELD_13	,
          MER_REC.	CC360_CUSTOM_FIELD_14	,
          MER_REC.	CC360_CUSTOM_FIELD_15	,
          MER_REC.	CC360_CUSTOM_FIELD_16	,
          MER_REC.	CC360_CUSTOM_FIELD_17	,
          MER_REC.	CC360_CUSTOM_FIELD_18	,
          MER_REC.	CC360_CUSTOM_FIELD_19	,
          MER_REC.	CC360_CUSTOM_FIELD_20	,
          MER_REC.	BEAN_LAST_MODIFIED_DATE 	,
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
--    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
--    remove_duplicates;
    
    
    select count(*)
    into   g_recs_read
    from   stg_svoc_all_bean_master_cpy1
    where  sys_process_code = 'N';
    
    l_text := 'RECORD COUNT '||g_recs_read||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   

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
end wh_fnd_cust_620FIX;
