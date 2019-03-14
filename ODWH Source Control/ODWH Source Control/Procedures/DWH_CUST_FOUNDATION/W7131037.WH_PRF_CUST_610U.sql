-- ****** Object: Procedure W7131037.WH_PRF_CUST_610U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_610U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        JUN 2017
--  Author:      Alastair de Wet
--  Purpose:    CREATE CUSTOMER SVOC MASTER EX FOUNDATION TABLE
--  Tables:      Input  - fnd_customer_master
--               Output - dim_customer_master
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Naming conventions:
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
g_recs_last_read     integer       :=  0;
g_recs_race          integer       :=  0;
g_recs_age           integer       :=  0;
g_recs_hospital      integer       :=  0;


g_date               date          := trunc(sysdate);




l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_610U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD DIM_CUSTOMER_MASTER EX FND TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF DIM_CUSTOMER_MASTER EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd/MON/yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text      := 'Start of Merge to LOAD customer SVOC master ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


execute immediate 'alter session enable parallel dml';


--**************************************************************************************************
--Merge onto the dim customer master ex Founadtion layer fnd_customer_master
--**************************************************************************************************

   MERGE  /*+ parallel(4) */ INTO DIM_CUSTOMER_MASTER DIM USING
   (
    SELECT /*+  parallel (cm,4) */ *
    FROM   W7131037.FND_CUSTOMER_MASTER cm
    WHERE LAST_UPDATED_DATE = G_DATE
   ) MER_REC
    ON    (  dim.	master_subscriber_key	          =	mer_rec.	master_subscriber_key )
   WHEN MATCHED THEN
   UPDATE SET
            dim.	IDENTITY_DOCUMENT_NO	          =	mer_rec.	IDENTITY_DOCUMENT_NO	,
            dim.	IDENTITY_DOCUMENT_NO_CREATE_DT	=	mer_rec.	IDENTITY_DOCUMENT_NO_CREATE_DT	,
            dim.	PASSPORT_NO	                    =	mer_rec.	PASSPORT_NO	,
            dim.	PASSPORT_COUNTRY_CODE	          =	mer_rec.	PASSPORT_COUNTRY_CODE	,
            dim.	PASSPORT_NO_CREATE_DATE	        =	mer_rec.	PASSPORT_NO_CREATE_DATE	,
            dim.	CREATE_DATE	                    =	mer_rec.	CREATE_DATE	,
            dim.	TITLE	                          =	mer_rec.	TITLE	,
            dim.	INITIALS	                      =	mer_rec.	INITIALS	,
            dim.	FIRST_NAME	                    =	mer_rec.	FIRST_NAME	,
            dim.	LAST_NAME	                      =	mer_rec.	LAST_NAME	,
            dim.	BIRTH_DATE	                    =	mer_rec.	BIRTH_DATE	,
            dim.	GENDER	                        =	mer_rec.	GENDER	,
            dim.	PREFERRED_LANGUAGE	            =	mer_rec.	PREFERRED_LANGUAGE	,
            dim.	POPULATED_EMAIL_ADDRESS       	=	mer_rec.	POPULATED_EMAIL_ADDRESS	,
            dim.	HOME_EMAIL_ADDRESS	            =	mer_rec.	HOME_EMAIL_ADDRESS	,
            dim.	HOME_EMAIL_LAST_UPDATE_DT	      =	mer_rec.	HOME_EMAIL_LAST_UPDATE_DT	,
            dim.	HOME_EMAIL_FAILURE_COUNT	      =	mer_rec.	HOME_EMAIL_FAILURE_COUNT	,
            dim.	WORK_EMAIL_ADDRESS	            =	mer_rec.	WORK_EMAIL_ADDRESS	,
            dim.	WORK_EMAIL_LAST_UPDATE_DT	      =	mer_rec.	WORK_EMAIL_LAST_UPDATE_DT	,
            dim.	WORK_EMAIL_FAILURE_COUNT	      =	mer_rec.	WORK_EMAIL_FAILURE_COUNT	,
            dim.	STATEMENT_EMAIL_ADDRESS	        =	mer_rec.	STATEMENT_EMAIL_ADDRESS	,
            dim.	STATEMENT_EMAIL_LAST_UPDATE_DT	=	mer_rec.	STATEMENT_EMAIL_LAST_UPDATE_DT	,
            dim.	STATEMENT_EMAIL_FAILURE_COUNT	  =	mer_rec.	STATEMENT_EMAIL_FAILURE_COUNT	,
            dim.	ECOMMERCE_EMAIL_ADDRESS	        =	mer_rec.	ECOMMERCE_EMAIL_ADDRESS	,
            dim.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	=	mer_rec.	ECOMMERCE_EMAIL_LAST_UPDATE_DT	,
            dim.	ECOMMERCE_EMAIL_FAILURE_COUNT	  =	mer_rec.	ECOMMERCE_EMAIL_FAILURE_COUNT	,
            dim.	POPULATED_CELL_NO	              =	mer_rec.	POPULATED_CELL_NO	,
            dim.	HOME_CELL_NO	                  =	mer_rec.	HOME_CELL_NO	,
            dim.	HOME_CELL_LAST_UPDATED_DATE	    =	mer_rec.	HOME_CELL_LAST_UPDATED_DATE	,
            dim.	HOME_CELL_FAILURE_COUNT	        =	mer_rec.	HOME_CELL_FAILURE_COUNT	,
            dim.	WORK_CELL_NO	                  =	mer_rec.	WORK_CELL_NO	,
            dim.	WORK_CELL_LAST_UPDATED_DATE	    =	mer_rec.	WORK_CELL_LAST_UPDATED_DATE	,
            dim.	WORK_CELL_FAILURE_COUNT	        =	mer_rec.	WORK_CELL_FAILURE_COUNT	,
            dim.	HOME_PHONE_NO	                  =	mer_rec.	HOME_PHONE_NO	,
            dim.	HOME_PHONE_LAST_UPDATED_DATE	  =	mer_rec.	HOME_PHONE_LAST_UPDATED_DATE	,
            dim.	HOME_PHONE_FAILURE_COUNT	      =	mer_rec.	HOME_PHONE_FAILURE_COUNT	,
            dim.	WORK_PHONE_NO                 	=	mer_rec.	WORK_PHONE_NO	,
            dim.	WORK_PHONE_LAST_UPDATED_DATE	  =	mer_rec.	WORK_PHONE_LAST_UPDATED_DATE	,
            dim.	WORK_PHONE_FAILURE_COUNT	      =	mer_rec.	WORK_PHONE_FAILURE_COUNT	,
            dim.	POSTAL_ADDRESS_LINE_1	          =	mer_rec.	POSTAL_ADDRESS_LINE_1	,
            dim.	POSTAL_ADDRESS_LINE_2	          =	mer_rec.	POSTAL_ADDRESS_LINE_2	,
            dim.	POSTAL_SUBURB_NAME	            =	mer_rec.	POSTAL_SUBURB_NAME	,
            dim.	POST_POSTAL_CODE	              =	mer_rec.	POST_POSTAL_CODE	,
            dim.	POSTAL_CITY_NAME	              =	mer_rec.	POSTAL_CITY_NAME	,
            dim.	POSTAL_PROVINCE_NAME	          =	mer_rec.	POSTAL_PROVINCE_NAME	,
            dim.	POSTAL_ADDRESS_UPDATE_DATE	    =	mer_rec.	POSTAL_ADDRESS_UPDATE_DATE	,
            dim.	PHYSICAL_ADDRESS_LINE_1	        =	mer_rec.	PHYSICAL_ADDRESS_LINE_1	,
            dim.	PHYSICAL_ADDRESS_LINE_2	        =	mer_rec.	PHYSICAL_ADDRESS_LINE_2	,
            dim.	PHYSICAL_SUBURB_NAME	          =	mer_rec.	PHYSICAL_SUBURB_NAME	,
            dim.	PHYSICAL_POSTAL_CODE	          =	mer_rec.	PHYSICAL_POSTAL_CODE	,
            dim.	PHYSICAL_CITY_NAME	            =	mer_rec.	PHYSICAL_CITY_NAME	,
            dim.	PHYSICAL_PROVINCE_NAME	        =	mer_rec.	PHYSICAL_PROVINCE_NAME	,
            dim.	PHYSICAL_ADDRESS_UPDATE_DATE	  =	mer_rec.	PHYSICAL_ADDRESS_UPDATE_DATE	,
            dim.	SHIPPING_ADDRESS_LINE_1	        =	mer_rec.	SHIPPING_ADDRESS_LINE_1	,
            dim.	SHIPPING_ADDRESS_LINE_2	        =	mer_rec.	SHIPPING_ADDRESS_LINE_2	,
            dim.	SHIPPING_SUBURB_NAME	          =	mer_rec.	SHIPPING_SUBURB_NAME	,
            dim.	POST_SHIPPING_CODE	            =	mer_rec.	POST_SHIPPING_CODE	,
            dim.	SHIPPING_CITY_NAME	            =	mer_rec.	SHIPPING_CITY_NAME	,
            dim.	SHIPPING_PROVINCE_NAME	        =	mer_rec.	SHIPPING_PROVINCE_NAME	,
            dim.	SHIPPING_ADDRESS_UPDATE_DATE  	=	mer_rec.	SHIPPING_ADDRESS_UPDATE_DATE	,
            dim.	BILLING_ADDRESS_LINE_1	        =	mer_rec.	BILLING_ADDRESS_LINE_1	,
            dim.	BILLING_ADDRESS_LINE_2	        =	mer_rec.	BILLING_ADDRESS_LINE_2	,
            dim.	BILLING_SUBURB_NAME	            =	mer_rec.	BILLING_SUBURB_NAME	,
            dim.	POST_BILLING_CODE	              =	mer_rec.	POST_BILLING_CODE	,
            dim.	BILLING_CITY_NAME	              =	mer_rec.	BILLING_CITY_NAME	,
            dim.	BILLING_PROVINCE_NAME	          =	mer_rec.	BILLING_PROVINCE_NAME	,
            dim.	BILLING_ADDRESS_UPDATE_DATE	    =	mer_rec.	BILLING_ADDRESS_UPDATE_DATE	,
            dim.	WW_DM_SMS_OPT_OUT_IND	          =	mer_rec.	WW_DM_SMS_OPT_OUT_IND	,
            dim.	WW_DM_EMAIL_OPT_OUT_IND	        =	mer_rec.	WW_DM_EMAIL_OPT_OUT_IND	,
            dim.	WW_DM_POST_OPT_OUT_IND	        =	mer_rec.	WW_DM_POST_OPT_OUT_IND	,
            dim.	WW_DM_PHONE_OPT_OUT_IND	        =	mer_rec.	WW_DM_PHONE_OPT_OUT_IND	,
            dim.	WW_MAN_SMS_OPT_OUT_IND	        =	mer_rec.	WW_MAN_SMS_OPT_OUT_IND	,
            dim.	WW_MAN_EMAIL_OPT_OUT_IND	      =	mer_rec.	WW_MAN_EMAIL_OPT_OUT_IND	,
            dim.	WW_MAN_POST_OPT_OUT_IND	        =	mer_rec.	WW_MAN_POST_OPT_OUT_IND	,
            dim.	WW_MAN_PHONE_OPT_OUT_IND	      =	mer_rec.	WW_MAN_PHONE_OPT_OUT_IND	,
            dim.	WFS_DM_SMS_OPT_OUT_IND	        =	mer_rec.	WFS_DM_SMS_OPT_OUT_IND	,
            dim.	WFS_DM_EMAIL_OPT_OUT_IND	      =	mer_rec.	WFS_DM_EMAIL_OPT_OUT_IND	,
            dim.	WFS_DM_POST_OPT_OUT_IND	        =	mer_rec.	WFS_DM_POST_OPT_OUT_IND	,
            dim.	WFS_DM_PHONE_OPT_OUT_IND	      =	mer_rec.	WFS_DM_PHONE_OPT_OUT_IND	,
            dim.	WFS_CON_SMS_OPT_OUT_IND	        =	mer_rec.	WFS_CON_SMS_OPT_OUT_IND	,
            dim.	WFS_CON_EMAIL_OPT_OUT_IND	      =	mer_rec.	WFS_CON_EMAIL_OPT_OUT_IND	,
            dim.	WFS_CON_POST_OPT_OUT_IND	      =	mer_rec.	WFS_CON_POST_OPT_OUT_IND	,
            dim.	WFS_CON_PHONE_OPT_OUT_IND	      =	mer_rec.	WFS_CON_PHONE_OPT_OUT_IND	,
            dim.	PREFERENCE_1_IND	=	mer_rec.	PREFERENCE_1_IND	,
            dim.	PREFERENCE_2_IND	=	mer_rec.	PREFERENCE_2_IND	,
            dim.	PREFERENCE_3_IND	=	mer_rec.	PREFERENCE_3_IND	,
            dim.	PREFERENCE_4_IND	=	mer_rec.	PREFERENCE_4_IND	,
            dim.	PREFERENCE_5_IND	=	mer_rec.	PREFERENCE_5_IND	,
            dim.	PREFERENCE_6_IND	=	mer_rec.	PREFERENCE_6_IND	,
            dim.	PREFERENCE_7_IND	=	mer_rec.	PREFERENCE_7_IND	,
            dim.	SWIPE_DEVICE_IND	=	mer_rec.	SWIPE_DEVICE_IND	,
            dim.	LAST_SWIPE_DATE	                =	mer_rec.	LAST_SWIPE_DATE	,
            dim.	FACEBOOK_ID	                    =	mer_rec.	FACEBOOK_ID	,
            dim.	FACEBOOK_ID_LAST_UPDATED_DATE	  =	mer_rec.	FACEBOOK_ID_LAST_UPDATED_DATE	,
            dim.	INSTAGRAM_ID	                  =	mer_rec.	INSTAGRAM_ID	,
            dim.	INSTAGRAM_ID_LAST_UPDATED_DATE	=	mer_rec.	INSTAGRAM_ID_LAST_UPDATED_DATE	,
            dim.	TWITTER_ID	                    =	mer_rec.	TWITTER_ID	,
            dim.	TWITTER_ID_LAST_UPDATED_DATE	  =	mer_rec.	TWITTER_ID_LAST_UPDATED_DATE	,
            dim.	LINKEDIN_ID	                    =	mer_rec.	LINKEDIN_ID	,
            dim.	LINKEDIN_ID_LAST_UPDATED_DATE	  =	mer_rec.	LINKEDIN_ID_LAST_UPDATED_DATE	,
            dim.	ROA_MARKETING_CONSENT_IND     	=	mer_rec.	ROA_MARKETING_CONSENT_IND	,
            dim.	ROA_REGISTERED_LOCATION_NO	    =	mer_rec.	ROA_REGISTERED_LOCATION_NO	,
            dim.	ROA_COUNTRY_CODE	              =	mer_rec.	ROA_COUNTRY_CODE	,
            dim.	ROA_ACTIVE_PRODUCT_IND	        =	mer_rec.	ROA_ACTIVE_PRODUCT_IND	,
            dim.	ROA_LOYALTY_CARD_NO	            =	mer_rec.	ROA_LOYALTY_CARD_NO	,
            dim.	ACCOUNT_LAST_MODIFIED_DATE    	=	mer_rec.	ACCOUNT_LAST_MODIFIED_DATE	,
            dim.  last_updated_date               = g_date
   WHEN NOT MATCHED THEN
   INSERT
          (
          MASTER_SUBSCRIBER_KEY,
          SK1_SUBSCRIBER_KEY,
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
          cust_svoc_seq.nextval,
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
          );


g_recs_read:=g_recs_read+SQL%ROWCOUNT;

commit;

--*****************************************************************************************************
-- Calculate the AGE of the account holder for inclusion on the dim_customer_master
--*****************************************************************************************************

MERGE  /*+ parallel (DC,4) */ INTO DIM_CUSTOMER_MASTER DC USING
(
   SELECT /*+ FULL(CST) parallel(CST,12) */
          MASTER_SUBSCRIBER_KEY,
          BIRTH_DATE,
          NVL(AGE,0) AGE,
          FLOOR(MONTHS_BETWEEN(SYSDATE,BIRTH_DATE) / 12) CALC_AGE
   FROM   DIM_CUSTOMER_MASTER CST
   WHERE  BIRTH_DATE     IS NOT NULL
   AND    FLOOR(MONTHS_BETWEEN(SYSDATE,BIRTH_DATE) / 12) <> NVL(AGE,0)
)  AGE
ON  ( DC.MASTER_SUBSCRIBER_KEY       = AGE.MASTER_SUBSCRIBER_KEY )
WHEN MATCHED THEN
UPDATE     SET     AGE = CALC_AGE;


g_recs_age  := g_recs_age   + sql%rowcount;

l_text :=  'RECORDS WHERE AGE WAS CALCULATED '||g_recs_age;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

commit;

--*****************************************************************************************************
-- Determine the RACE of the account holder for inclusion on the dim_customer_master
--*****************************************************************************************************
MERGE  /*+ parallel (DC,4) */ INTO W7131037.DIM_CUSTOMER_MASTER DC USING
(
   SELECT /*+ FULL(CST) parallel(CST,12) */
          CST.MASTER_SUBSCRIBER_KEY,
          CST.DERIVED_RACE,
          RR.RACE
   FROM   W7131037.DIM_CUSTOMER_MASTER CST
   JOIN   W7131037.FND_CUST_RACE_REF RR
   ON     RR.SURNAME = CST.LAST_NAME
   WHERE  NVL(CST.DERIVED_RACE,'X') <> RR.RACE
)  RACE
ON  ( DC.MASTER_SUBSCRIBER_KEY       = RACE.MASTER_SUBSCRIBER_KEY )
WHEN MATCHED THEN
UPDATE     SET     DC.DERIVED_RACE = RACE.RACE
;

g_recs_race  := g_recs_race   + sql%rowcount;

l_text :=  'RECORDS WHERE RACE WAS DETERMINED '||g_recs_race;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

commit;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS MERGED '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;


END "WH_PRF_CUST_610U";
