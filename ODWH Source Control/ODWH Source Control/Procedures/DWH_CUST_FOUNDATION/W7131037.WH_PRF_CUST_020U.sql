-- ****** Object: Procedure W7131037.WH_PRF_CUST_020U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_020U"      (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create CUSTOMER MASTER fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_customer
--               Output - dim_customer
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  11 Feb  2016 - N Chauhan - added 4 fields for SOI/SOF compliance.
--  23 Feb  2016 - N Chauhan - added 2 more fields for SOI/SOF compliance.
--
--
--
-- Note: This version Attempts to do a bulk insert / update
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
g_truncate_count     integer       :=  0;



g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_020U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUSTOMER MASTER EX C2';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_fnd_customer is
select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*
      from    fnd_customer fnd,
              dim_customer prf
      where   fnd.customer_no       = prf.customer_no
      and     fnd.last_updated_date = g_date
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.customer_no;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

       insert /*+ APPEND parallel (prf,2) */ into dim_customer prf
       select /*+ FULL(fnd)  parallel (fnd,2) */
             	fnd.	CUSTOMER_NO,
              fnd.	WFS_CUSTOMER_NO,
              fnd.	IDENTITY_DOCUMENT_CODE,
              fnd.	IDENTITY_DOCUMENT_TYPE,
              fnd.	PASSPORT_NO,
              fnd.	PASSPORT_EXPIRY_DATE,
              fnd.	PASSPORT_ISSUE_COUNTRY_CODE,
              fnd.	INDIVIDUAL_IND,
              fnd.	CUSTOMER_STATUS,
              fnd.	OPT_IN_IND,
              fnd.	GLID_IND,
              fnd.	ITC_IND,
              fnd.	FICA_STATUS,
              fnd.	NO_MARKETING_VIA_PHONE_IND,
              fnd.	NO_MARKETING_VIA_SMS_IND,
              fnd.	NO_SHARE_MY_DETAILS_IND,
              fnd.	C2_CREATE_DATE,
              fnd.	LAST_DETAIL_CONFIRM_DATE,
              fnd.	LAST_WEB_ACCESS_DATE,
              fnd.	FICA_CHANGE_DATE,
              fnd.	LAST_ITC_QUERY_DATE,
              fnd.	TITLE_CODE,
              fnd.	FIRST_MIDDLE_NAME_INITIAL,
              fnd.	FIRST_NAME,
              fnd.	PREFERRED_NAME,
              fnd.	LAST_NAME,
              fnd.	MAIDEN_NAMe,
              fnd.	BIRTH_DATE,
              fnd.	GENDER_CODE,
              fnd.	MARITAL_STATUS,
              fnd.	MARITAL_CONTRACT_TYPE,
              fnd.	NUM_MINOR,
              fnd.	PREFERRED_LANGUAGE,
              fnd.	CUSTOMER_HOME_LANGUAGE,
              fnd.	RESIDENTIAL_COUNTRY_CODE,
              fnd.	PRIMARY_COM_MEDIUM,
              fnd.	PRIMARY_COM_LANGUAGE,
              fnd.	SECONDARY_COM_MEDIUM,
              fnd.	SECONDARY_COM_LANGUAGE,
              fnd.	POSTAL_ADDRESS_LINE_1,
              fnd.	POSTAL_ADDRESS_LINE_2,
              fnd.	POSTAL_ADDRESS_LINE_3,
              fnd.	POSTAL_CODE,
              fnd.	POSTAL_CITY_NAME,
              fnd.	POSTAL_PROVINCE_NAME,
              fnd.	POSTAL_COUNTRY_CODE,
              fnd.	POSTAL_ADDRESS_OCCUPATION_DATE,
              fnd.	POSTAL_NUM_RETURNED_MAIL,
              fnd.	PHYSICAL_ADDRESS_LINE_1,
              fnd.	PHYSICAL_ADDRESS_LINE2,
              fnd.	PHYSICAL_SUBURB_NAME,
              fnd.	PHYSICAL_POSTAL_CODE,
              fnd.	PHYSICAL_CITY_NAME,
              fnd.	PHYSICAL_PROVINCE_NAME,
              fnd.	PHYSICAL_COUNTRY_CODE,
              fnd.	PHYSICAL_ADDRESS_OCCUPTN_DATE,
              fnd.	PHYSICAL_NUM_RETURNED_MAIL,
              fnd.	HOME_PHONE_COUNTRY_CODE,
              fnd.	HOME_PHONE_AREA_CODE,
              fnd.	HOME_PHONE_NO,
              fnd.	HOME_PHONE_EXTENSION_NO,
              fnd.	HOME_FAX_COUNTRY_CODE,
              fnd.	HOME_FAX_AREA_CODE,
              fnd.	HOME_FAX_NO,
              fnd.	HOME_CELL_COUNTRY_CODE,
              fnd.	HOME_CELL_AREA_CODE,
              fnd.	HOME_CELL_NO,
              fnd.	HOME_EMAIL_ADDRESS,
              fnd.	EMPLOYMENT_STATUS_IND,
              fnd.	COMPANY_NAME,
              fnd.	COMPANY_TYPE,
              fnd.	EMPLOYEE_NO,
              fnd.	EMPLOYEE_DEPT,
              fnd.	EMPLOYEE_JOB_TITLE,
              fnd.	WORK_PHONE_COUNTRY_CODE,
              fnd.	WORK_PHONE_AREA_CODE,
              fnd.	WORK_PHONE_NO,
              fnd.	WORK_PHONE_EXTENSION_NO,
              fnd.	WORK_FAX_COUNTRY_CODE,
              fnd.	WORK_FAX_AREA_CODE,
              fnd.	WORK_FAX_NO,
              fnd.	WORK_CELL_COUNTRY_CODE,
              fnd.	WORK_CELL_AREA_CODE,
              fnd.	WORK_CELL_NO,
              fnd.	WORK_EMAIL_ADDRESS,
              fnd.	HOME_CELL_FAILURE_IND,
              fnd.	HOME_CELL_DATE_LAST_UPDATED,
              fnd.	HOME_EMAIL_FAILURE_IND,
              fnd.	HOME_EMAIL_DATE_LAST_UPDATED,
              fnd.	HOME_PHONE_FAILURE_IND,
              fnd.	HOME_PHONE_DATE_LAST_UPDATED,
              fnd.	NO_MARKETING_VIA_EMAIL_IND,
              fnd.	NO_MARKETING_VIA_POST_IND,
              fnd.	POST_ADDR_DATE_LAST_UPDATED,
              fnd.	WFS_CUSTOMER_NO_TXT_VER,
              fnd.	WORK_CELL_FAILURE_IND,
              fnd.	WORK_CELL_DATE_LAST_UPDATED,
              fnd.	WORK_EMAIL_FAILURE_IND,
              fnd.	WORK_EMAIL_DATE_LAST_UPDATED,
              fnd.	WORK_PHONE_FAILURE_IND,
              fnd.	WORK_PHONE_DATE_LAST_UPDATED,
              fnd.	WW_ONLINE_CUSTOMER_NO,
              fnd.	LEGAL_LANGUAGE_DESCRIPTION,
              fnd.	ESTATEMENT_EMAIL,
              fnd.	ESTATEMENT_DATE_LAST_UPDATED,
              fnd.	ESTATEMENT_EMAIL_FAILURE_IND,
              fnd.	LAST_UPDATED_DATE,
              fnd.	RACE,
              fnd.	BILLING_CYCLE,
              fnd.	CREDIT_BURO_SCORE,
              fnd.	CREDIT_BURO_DATE,
              fnd.	CUSTOMER_TYPE,
              fnd.	AGE_ACC_HOLDER,
              '',
              fnd.	WW_DM_SMS_OPT_OUT_IND,
              fnd.	WW_DM_EMAIL_OPT_OUT_IND,
              fnd.	WW_DM_POST_OPT_OUT_IND,
              fnd.	WW_DM_PHONE_OPT_OUT_IND,
              fnd.	WW_MAN_SMS_OPT_OUT_IND,
              fnd.	WW_MAN_EMAIL_OPT_OUT_IND,
              fnd.	WW_MAN_POST_OPT_OUT_IND,
              fnd.	WW_MAN_PHONE_OPT_OUT_IND,
              fnd.	WFS_DM_SMS_OPT_OUT_IND,
              fnd.	WFS_DM_EMAIL_OPT_OUT_IND,
              fnd.	WFS_DM_POST_OPT_OUT_IND,
              fnd.	WFS_DM_PHONE_OPT_OUT_IND,
              fnd.	WFS_CON_SMS_OPT_OUT_IND,
              fnd.	WFS_CON_EMAIL_OPT_OUT_IND,
              fnd.	WFS_CON_POST_OPT_OUT_IND,
              fnd.	WFS_CON_PHONE_OPT_OUT_IND,
              fnd.	PREFERENCE_1_IND,
              fnd.	PREFERENCE_1_NO,
              fnd.	PREFERENCE_2_IND,
              fnd.	PREFERENCE_2_NO,
              fnd.	PREFERENCE_3_IND,
              fnd.	PREFERENCE_3_NO,
              fnd.	PREFERENCE_4_IND,
              fnd.	PREFERENCE_4_NO,
              fnd.	PREFERENCE_5_IND,
              fnd.	PREFERENCE_5_NO,
              fnd.	PREFERENCE_6_IND,
              fnd.	PREFERENCE_6_NO,
              fnd.	PREFERENCE_7_IND,
              fnd.	PREFERENCE_7_NO,
              fnd.	SOURCE_OF_INCOME_ID,
              fnd.	SOURCE_OF_INCOME_DESC,
              fnd.	OCCUPATION_ID,
              fnd.	OCCUPATION_DESC,
              fnd.	EMPLOYMENT_STATUS_ID,
              fnd.	EMPLOYMENT_STATUS_DESC,
              fnd.  EA_CODE,
              fnd.  X_COORDINATE,
              fnd.  Y_COORDINATE,
              '','',
              FND.  SUBSCRIBER_KEY,
              '',''
       from  fnd_customer fnd
       where fnd.last_updated_date = g_date    and
       not exists
      (select /*+ nl_aj */ * from dim_customer
       where  customer_no    = fnd.customer_no
       )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
       ;


      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin



FOR upd_rec IN c_fnd_customer
   loop
     update dim_customer prf
     set    prf.	wfs_customer_no	=	upd_rec.	wfs_customer_no	,
            prf.	identity_document_code	=	upd_rec.	identity_document_code	,
            prf.	identity_document_type	=	upd_rec.	identity_document_type	,
            prf.	passport_no	=	upd_rec.	passport_no	,
            prf.	passport_expiry_date	=	upd_rec.	passport_expiry_date	,
            prf.	passport_issue_country_code	=	upd_rec.	passport_issue_country_code	,
            prf.	individual_ind	=	upd_rec.	individual_ind	,
            prf.	customer_status	=	upd_rec.	customer_status	,
            prf.	opt_in_ind	=	upd_rec.	opt_in_ind	,
            prf.	glid_ind	=	upd_rec.	glid_ind	,
            prf.	itc_ind	=	upd_rec.	itc_ind	,
            prf.	fica_status	=	upd_rec.	fica_status	,
            prf.	no_marketing_via_phone_ind	=	upd_rec.	no_marketing_via_phone_ind	,
            prf.	no_marketing_via_sms_ind	=	upd_rec.	no_marketing_via_sms_ind	,
            prf.	no_share_my_details_ind	=	upd_rec.	no_share_my_details_ind	,
            prf.	c2_create_date	=	upd_rec.	c2_create_date	,
            prf.	last_detail_confirm_date	=	upd_rec.	last_detail_confirm_date	,
            prf.	last_web_access_date	=	upd_rec.	last_web_access_date	,
            prf.	fica_change_date	=	upd_rec.	fica_change_date	,
            prf.	last_itc_query_date	=	upd_rec.	last_itc_query_date	,
            prf.	title_code	=	upd_rec.	title_code	,
            prf.	first_middle_name_initial	=	upd_rec.	first_middle_name_initial	,
            prf.	first_name	=	upd_rec.	first_name	,
            prf.	preferred_name	=	upd_rec.	preferred_name	,
            prf.	last_name	=	upd_rec.	last_name	,
            prf.	maiden_name	=	upd_rec.	maiden_name	,
            prf.	birth_date	=	upd_rec.	birth_date	,
            prf.	gender_code	=	upd_rec.	gender_code	,
            prf.	marital_status	=	upd_rec.	marital_status	,
            prf.	marital_contract_type	=	upd_rec.	marital_contract_type	,
            prf.	num_minor	=	upd_rec.	num_minor	,
            prf.	preferred_language	=	upd_rec.	preferred_language	,
            prf.	customer_home_language	=	upd_rec.	customer_home_language	,
            prf.	residential_country_code	=	upd_rec.	residential_country_code	,
            prf.	primary_com_medium	=	upd_rec.	primary_com_medium	,
            prf.	primary_com_language	=	upd_rec.	primary_com_language	,
            prf.	secondary_com_medium	=	upd_rec.	secondary_com_medium	,
            prf.	secondary_com_language	=	upd_rec.	secondary_com_language	,
            prf.	postal_address_line_1	=	upd_rec.	postal_address_line_1	,
            prf.	postal_address_line_2	=	upd_rec.	postal_address_line_2	,
            prf.	postal_address_line_3	=	upd_rec.	postal_address_line_3	,
            prf.	postal_code	=	upd_rec.	postal_code	,
            prf.	postal_city_name	=	upd_rec.	postal_city_name	,
            prf.	postal_province_name	=	upd_rec.	postal_province_name	,
            prf.	postal_country_code	=	upd_rec.	postal_country_code	,
            prf.	postal_address_occupation_date	=	upd_rec.	postal_address_occupation_date	,
            prf.	postal_num_returned_mail	=	upd_rec.	postal_num_returned_mail	,
            prf.	physical_address_line_1	=	upd_rec.	physical_address_line_1	,
            prf.	physical_address_line2	=	upd_rec.	physical_address_line2	,
            prf.	physical_suburb_name	=	upd_rec.	physical_suburb_name	,
            prf.	physical_postal_code	=	upd_rec.	physical_postal_code	,
            prf.	physical_city_name	=	upd_rec.	physical_city_name	,
            prf.	physical_province_name	=	upd_rec.	physical_province_name	,
            prf.	physical_country_code	=	upd_rec.	physical_country_code	,
            prf.	physical_address_occuptn_date	=	upd_rec.	physical_address_occuptn_date	,
            prf.	physical_num_returned_mail	=	upd_rec.	physical_num_returned_mail	,
            prf.	home_phone_country_code	=	upd_rec.	home_phone_country_code	,
            prf.	home_phone_area_code	=	upd_rec.	home_phone_area_code	,
            prf.	home_phone_no	=	upd_rec.	home_phone_no	,
            prf.	home_phone_extension_no	=	upd_rec.	home_phone_extension_no	,
            prf.	home_fax_country_code	=	upd_rec.	home_fax_country_code	,
            prf.	home_fax_area_code	=	upd_rec.	home_fax_area_code	,
            prf.	home_fax_no	=	upd_rec.	home_fax_no	,
            prf.	home_cell_country_code	=	upd_rec.	home_cell_country_code	,
            prf.	home_cell_area_code	=	upd_rec.	home_cell_area_code	,
            prf.	home_cell_no	=	upd_rec.	home_cell_no	,
            prf.	home_email_address	=	upd_rec.	home_email_address	,
            prf.	employment_status_ind	=	upd_rec.	employment_status_ind	,
            prf.	company_name	=	upd_rec.	company_name	,
            prf.	company_type	=	upd_rec.	company_type	,
            prf.	employee_no	=	upd_rec.	employee_no	,
            prf.	employee_dept	=	upd_rec.	employee_dept	,
            prf.	employee_job_title	=	upd_rec.	employee_job_title	,
            prf.	work_phone_country_code	=	upd_rec.	work_phone_country_code	,
            prf.	work_phone_area_code	=	upd_rec.	work_phone_area_code	,
            prf.	work_phone_no	=	upd_rec.	work_phone_no	,
            prf.	work_phone_extension_no	=	upd_rec.	work_phone_extension_no	,
            prf.	work_fax_country_code	=	upd_rec.	work_fax_country_code	,
            prf.	work_fax_area_code	=	upd_rec.	work_fax_area_code	,
            prf.	work_fax_no	=	upd_rec.	work_fax_no	,
            prf.	work_cell_country_code	=	upd_rec.	work_cell_country_code	,
            prf.	work_cell_area_code	=	upd_rec.	work_cell_area_code	,
            prf.	work_cell_no	=	upd_rec.	work_cell_no	,
            prf.	work_email_address	=	upd_rec.	work_email_address	,
            prf.	home_cell_failure_ind	=	upd_rec.	home_cell_failure_ind	,
            prf.	home_cell_date_last_updated	=	upd_rec.	home_cell_date_last_updated	,
            prf.	home_email_failure_ind	=	upd_rec.	home_email_failure_ind	,
            prf.	home_email_date_last_updated	=	upd_rec.	home_email_date_last_updated	,
            prf.	home_phone_failure_ind	=	upd_rec.	home_phone_failure_ind	,
            prf.	home_phone_date_last_updated	=	upd_rec.	home_phone_date_last_updated	,
            prf.	no_marketing_via_email_ind	=	upd_rec.	no_marketing_via_email_ind	,
            prf.	no_marketing_via_post_ind	=	upd_rec.	no_marketing_via_post_ind	,
            prf.	post_addr_date_last_updated	=	upd_rec.	post_addr_date_last_updated	,
            prf.	wfs_customer_no_txt_ver	=	upd_rec.	wfs_customer_no_txt_ver	,
            prf.	work_cell_failure_ind	=	upd_rec.	work_cell_failure_ind	,
            prf.	work_cell_date_last_updated	=	upd_rec.	work_cell_date_last_updated	,
            prf.	work_email_failure_ind	=	upd_rec.	work_email_failure_ind	,
            prf.	work_email_date_last_updated	=	upd_rec.	work_email_date_last_updated	,
            prf.	work_phone_failure_ind	=	upd_rec.	work_phone_failure_ind	,
            prf.	work_phone_date_last_updated	=	upd_rec.	work_phone_date_last_updated	,
            prf.	ww_online_customer_no	=	upd_rec.	ww_online_customer_no	,
            prf.	legal_language_description	=	upd_rec.	legal_language_description	,
            prf.	estatement_email	=	upd_rec.	estatement_email	,
            prf.	estatement_date_last_updated	=	upd_rec.	estatement_date_last_updated	,
            prf.	estatement_email_failure_ind	=	upd_rec.	estatement_email_failure_ind	,
            prf.	last_updated_date	=	g_date	,
            prf.	race	=	upd_rec.	race	,
            prf.	billing_cycle	=	upd_rec.	billing_cycle	,
            prf.	credit_buro_score	=	upd_rec.	credit_buro_score	,
            prf.	credit_buro_date	=	upd_rec.	credit_buro_date	,
            prf.	customer_type	=	upd_rec.	customer_type	,
            prf.	age_acc_holder	=	upd_rec.	age_acc_holder,
            prf.	ww_dm_sms_opt_out_ind	=	upd_rec.	ww_dm_sms_opt_out_ind	,
            prf.	ww_dm_email_opt_out_ind	=	upd_rec.	ww_dm_email_opt_out_ind	,
            prf.	ww_dm_post_opt_out_ind	=	upd_rec.	ww_dm_post_opt_out_ind	,
            prf.	ww_dm_phone_opt_out_ind	=	upd_rec.	ww_dm_phone_opt_out_ind	,
            prf.	ww_man_sms_opt_out_ind	=	upd_rec.	ww_man_sms_opt_out_ind	,
            prf.	ww_man_email_opt_out_ind	=	upd_rec.	ww_man_email_opt_out_ind	,
            prf.	ww_man_post_opt_out_ind	=	upd_rec.	ww_man_post_opt_out_ind	,
            prf.	ww_man_phone_opt_out_ind	=	upd_rec.	ww_man_phone_opt_out_ind	,
            prf.	wfs_dm_sms_opt_out_ind	=	upd_rec.	wfs_dm_sms_opt_out_ind	,
            prf.	wfs_dm_email_opt_out_ind	=	upd_rec.	wfs_dm_email_opt_out_ind	,
            prf.	wfs_dm_post_opt_out_ind	=	upd_rec.	wfs_dm_post_opt_out_ind	,
            prf.	wfs_dm_phone_opt_out_ind	=	upd_rec.	wfs_dm_phone_opt_out_ind	,
            prf.	wfs_con_sms_opt_out_ind	=	upd_rec.	wfs_con_sms_opt_out_ind	,
            prf.	wfs_con_email_opt_out_ind	=	upd_rec.	wfs_con_email_opt_out_ind	,
            prf.	wfs_con_post_opt_out_ind	=	upd_rec.	wfs_con_post_opt_out_ind	,
            prf.	wfs_con_phone_opt_out_ind	=	upd_rec.	wfs_con_phone_opt_out_ind	,
            prf.	preference_1_ind	=	upd_rec.	preference_1_ind	,
            prf.	preference_1_no	=	upd_rec.	preference_1_no	,
            prf.	preference_2_ind	=	upd_rec.	preference_2_ind	,
            prf.	preference_2_no	=	upd_rec.	preference_2_no	,
            prf.	preference_3_ind	=	upd_rec.	preference_3_ind	,
            prf.	preference_3_no	=	upd_rec.	preference_3_no	,
            prf.	preference_4_ind	=	upd_rec.	preference_4_ind	,
            prf.	preference_4_no	=	upd_rec.	preference_4_no	,
            prf.	preference_5_ind	=	upd_rec.	preference_5_ind	,
            prf.	preference_5_no	=	upd_rec.	preference_5_no	,
            prf.	preference_6_ind	=	upd_rec.	preference_6_ind	,
            prf.	preference_6_no	=	upd_rec.	preference_6_no	,
            prf.	preference_7_ind	=	upd_rec.	preference_7_ind	,
            prf.	preference_7_no	=	upd_rec.	preference_7_no  ,
            prf.	SOURCE_OF_INCOME_ID	=	upd_rec.	SOURCE_OF_INCOME_ID	,
            prf.	SOURCE_OF_INCOME_DESC	=	upd_rec.	SOURCE_OF_INCOME_DESC	,
            prf.	OCCUPATION_ID	=	upd_rec.	OCCUPATION_ID	,
            prf.	OCCUPATION_DESC	=	upd_rec.	OCCUPATION_DESC,
            prf.	employment_status_id	=	upd_rec.	employment_status_id	,
            prf.	employment_status_desc	=	upd_rec.	employment_status_desc,
            prf.	EA_CODE	      =	upd_rec.	EA_CODE	,
            prf.	X_COORDINATE	=	upd_rec.	X_COORDINATE	,
            prf.	Y_COORDINATE	=	upd_rec.	Y_COORDINATE	 ,
            prf.	SUBSCRIBER_KEY	=	upd_rec.	SUBSCRIBER_KEY
     where  prf.	customer_no	      =	upd_rec.	customer_no ;

      g_recs_updated := g_recs_updated + 1;
   end loop;


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


    select count(*)
    into   g_recs_read
    from   fnd_customer
    where  last_updated_date = g_date;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated  then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;


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
       raise;
end wh_prf_cust_020u;
