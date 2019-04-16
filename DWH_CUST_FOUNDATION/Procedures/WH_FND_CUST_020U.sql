--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_020U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_020U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        MAR 2015
--  Author:      Alastair de Wet
--               Create Dim _customer dimention table in the foundation layer
--               with input ex staging table from Customer Central.
--  Tables:      Input  - stg_c2_customer_cpy
--               Output - fnd_customer
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  10 Feb  2016 - N Chauhan - added 4 fields for SOI/SOF compliance.
--  23 Feb  2016 - N Chauhan - added 2 more fields for SOI/SOF compliance.
--  23 MAR  2017 - A DE WET  - ADD SUBSCRIBER KEY
--  
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

g_customer_no        stg_c2_customer_cpy.customer_no%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_020U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CUTOMER DIM EX CUSTOMER CENTRAL';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_c2_customer_cpy
where (customer_no)
in
(select customer_no
from stg_c2_customer_cpy
group by customer_no
having count(*) > 1)
order by customer_no,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_c2_customer is
select /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.*
      from    stg_c2_customer_cpy cpy,
              fnd_customer fnd
      where   cpy.customer_no       = fnd.customer_no and
              cpy.sys_process_code = 'N' and
             (
             nvl(fnd.	IDENTITY_DOCUMENT_CODE	,0) <>	cpy.	IDENTITY_DOCUMENT_CODE	or
             nvl(fnd.	IDENTITY_DOCUMENT_TYPE	,0) <>	cpy.	IDENTITY_DOCUMENT_TYPE	or
             nvl(fnd.	PASSPORT_NO	,0) <>	cpy.	PASSPORT_NO	or
             nvl(fnd.	PASSPORT_EXPIRY_DATE	,'1 Jan 1900') <>	cpy.	PASSPORT_EXPIRY_DATE	or
             nvl(fnd.	PASSPORT_ISSUE_COUNTRY_CODE	,0) <>	cpy.	PASSPORT_ISSUE_COUNTRY_CODE	or
             nvl(fnd.	INDIVIDUAL_IND	,0) <>	cpy.	INDIVIDUAL_IND	or
             nvl(fnd.	CUSTOMER_STATUS	,0) <>	cpy.	CUSTOMER_STATUS	or
             nvl(fnd.	OPT_IN_IND	,0) <>	cpy.	OPT_IN_IND	or
             nvl(fnd.	GLID_IND	,0) <>	cpy.	GLID_IND	or
             nvl(fnd.	ITC_IND	,0) <>	cpy.	ITC_IND	or
             nvl(fnd.	FICA_STATUS	,0) <>	cpy.	FICA_STATUS	or
             nvl(fnd.	NO_MARKETING_VIA_PHONE_IND	,0) <>	cpy.	NO_MARKETING_VIA_PHONE_IND	or
             nvl(fnd.	NO_MARKETING_VIA_SMS_IND	,0) <>	cpy.	NO_MARKETING_VIA_SMS_IND	or
             nvl(fnd.	NO_SHARE_MY_DETAILS_IND	,0) <>	cpy.	NO_SHARE_MY_DETAILS_IND	or
             nvl(fnd.	C2_CREATE_DATE	,'1 Jan 1900') <>	cpy.	C2_CREATE_DATE	or
             nvl(fnd.	LAST_DETAIL_CONFIRM_DATE	,'1 Jan 1900') <>	cpy.	LAST_DETAIL_CONFIRM_DATE	or
             nvl(fnd.	LAST_WEB_ACCESS_DATE	,'1 Jan 1900') <>	cpy.	LAST_WEB_ACCESS_DATE	or
             nvl(fnd.	FICA_CHANGE_DATE	,'1 Jan 1900') <>	cpy.	FICA_CHANGE_DATE	or
             nvl(fnd.	LAST_ITC_QUERY_DATE	,'1 Jan 1900') <>	cpy.	LAST_ITC_QUERY_DATE	or
             nvl(fnd.	TITLE_CODE	,0) <>	cpy.	TITLE_CODE	or
             nvl(fnd.	FIRST_MIDDLE_NAME_INITIAL	,0) <>	cpy.	FIRST_MIDDLE_NAME_INITIAL	or
             nvl(fnd.	FIRST_NAME	,0) <>	cpy.	FIRST_NAME	or
             nvl(fnd.	PREFERRED_NAME	,0) <>	cpy.	PREFERRED_NAME	or
             nvl(fnd.	LAST_NAME	,0) <>	cpy.	LAST_NAME	or
             nvl(fnd.	MAIDEN_NAME	,0) <>	cpy.	MAIDEN_NAME	or
             nvl(fnd.	BIRTH_DATE	,'1 Jan 1900') <>	cpy.	BIRTH_DATE	or
             nvl(fnd.	GENDER_CODE	,0) <>	cpy.	GENDER_CODE	or
             nvl(fnd.	MARITAL_STATUS	,0) <>	cpy.	MARITAL_STATUS	or
             nvl(fnd.	MARITAL_CONTRACT_TYPE	,0) <>	cpy.	MARITAL_CONTRACT_TYPE	or
             nvl(fnd.	NUM_MINOR	,0) <>	cpy.	NUM_MINOR	or
             nvl(fnd.	PREFERRED_LANGUAGE	,0) <>	cpy.	PREFERRED_LANGUAGE	or
             nvl(fnd.	CUSTOMER_HOME_LANGUAGE	,0) <>	cpy.	CUSTOMER_HOME_LANGUAGE	or
             nvl(fnd.	RESIDENTIAL_COUNTRY_CODE	,0) <>	cpy.	RESIDENTIAL_COUNTRY_CODE	or
             nvl(fnd.	PRIMARY_COM_MEDIUM	,0) <>	cpy.	PRIMARY_COM_MEDIUM	or
             nvl(fnd.	PRIMARY_COM_LANGUAGE	,0) <>	cpy.	PRIMARY_COM_LANGUAGE	or
             nvl(fnd.	SECONDARY_COM_MEDIUM	,0) <>	cpy.	SECONDARY_COM_MEDIUM	or
             nvl(fnd.	SECONDARY_COM_LANGUAGE	,0) <>	cpy.	SECONDARY_COM_LANGUAGE	or
             nvl(fnd.	POSTAL_ADDRESS_LINE_1	,0) <>	cpy.	POSTAL_ADDRESS_LINE_1	or
             nvl(fnd.	POSTAL_ADDRESS_LINE_2	,0) <>	cpy.	POSTAL_ADDRESS_LINE_2	or
             nvl(fnd.	POSTAL_ADDRESS_LINE_3	,0) <>	cpy.	POSTAL_ADDRESS_LINE_3	or
             nvl(fnd.	POSTAL_CODE	,0) <>	cpy.	POSTAL_CODE	or
             nvl(fnd.	POSTAL_CITY_NAME	,0) <>	cpy.	POSTAL_CITY_NAME	or
             nvl(fnd.	POSTAL_PROVINCE_NAME	,0) <>	cpy.	POSTAL_PROVINCE_NAME	or
             nvl(fnd.	POSTAL_COUNTRY_CODE	,0) <>	cpy.	POSTAL_COUNTRY_CODE	or
             nvl(fnd.	POSTAL_ADDRESS_OCCUPATION_DATE	,'1 Jan 1900') <>	cpy.	POSTAL_ADDRESS_OCCUPATION_DATE	or
             nvl(fnd.	POSTAL_NUM_RETURNED_MAIL	,0) <>	cpy.	POSTAL_NUM_RETURNED_MAIL	or
             nvl(fnd.	PHYSICAL_ADDRESS_LINE_1	,0) <>	cpy.	PHYSICAL_ADDRESS_LINE_1	or
             nvl(fnd.	PHYSICAL_ADDRESS_LINE2	,0) <>	cpy.	PHYSICAL_ADDRESS_LINE2	or
             nvl(fnd.	PHYSICAL_SUBURB_NAME	,0) <>	cpy.	PHYSICAL_SUBURB_NAME	or
             nvl(fnd.	PHYSICAL_POSTAL_CODE	,0) <>	cpy.	PHYSICAL_POSTAL_CODE	or
             nvl(fnd.	PHYSICAL_CITY_NAME	,0) <>	cpy.	PHYSICAL_CITY_NAME	or
             nvl(fnd.	PHYSICAL_PROVINCE_NAME	,0) <>	cpy.	PHYSICAL_PROVINCE_NAME	or
             nvl(fnd.	PHYSICAL_COUNTRY_CODE	,0) <>	cpy.	PHYSICAL_COUNTRY_CODE	or
             nvl(fnd.	PHYSICAL_ADDRESS_OCCUPTN_DATE	,'1 Jan 1900') <>	cpy.	PHYSICAL_ADDRESS_OCCUPTN_DATE	or
             nvl(fnd.	PHYSICAL_NUM_RETURNED_MAIL	,0) <>	cpy.	PHYSICAL_NUM_RETURNED_MAIL	or
             nvl(fnd.	HOME_PHONE_COUNTRY_CODE	,0) <>	cpy.	HOME_PHONE_COUNTRY_CODE	or
             nvl(fnd.	HOME_PHONE_AREA_CODE	,0) <>	cpy.	HOME_PHONE_AREA_CODE	or
             nvl(fnd.	HOME_PHONE_NO	,0) <>	cpy.	HOME_PHONE_NO	or
             nvl(fnd.	HOME_PHONE_EXTENSION_NO	,0) <>	cpy.	HOME_PHONE_EXTENSION_NO	or
             nvl(fnd.	HOME_FAX_COUNTRY_CODE	,0) <>	cpy.	HOME_FAX_COUNTRY_CODE	or
             nvl(fnd.	HOME_FAX_AREA_CODE	,0) <>	cpy.	HOME_FAX_AREA_CODE	or
             nvl(fnd.	HOME_FAX_NO	,0) <>	cpy.	HOME_FAX_NO	or
             nvl(fnd.	HOME_CELL_COUNTRY_CODE	,0) <>	cpy.	HOME_CELL_COUNTRY_CODE	or
             nvl(fnd.	HOME_CELL_AREA_CODE	,0) <>	cpy.	HOME_CELL_AREA_CODE	or
             nvl(fnd.	HOME_CELL_NO	,0) <>	cpy.	HOME_CELL_NO	or
             nvl(fnd.	HOME_EMAIL_ADDRESS	,0) <>	cpy.	HOME_EMAIL_ADDRESS	or
             nvl(fnd.	EMPLOYMENT_STATUS_IND	,0) <>	cpy.	EMPLOYMENT_STATUS_IND	or
             nvl(fnd.	COMPANY_NAME	,0) <>	cpy.	COMPANY_NAME	or
             nvl(fnd.	COMPANY_TYPE	,0) <>	cpy.	COMPANY_TYPE	or
             nvl(fnd.	EMPLOYEE_NO	,0) <>	cpy.	EMPLOYEE_NO	or
             nvl(fnd.	EMPLOYEE_DEPT	,0) <>	cpy.	EMPLOYEE_DEPT	or
             nvl(fnd.	EMPLOYEE_JOB_TITLE	,0) <>	cpy.	EMPLOYEE_JOB_TITLE	or
             nvl(fnd.	WORK_PHONE_COUNTRY_CODE	,0) <>	cpy.	WORK_PHONE_COUNTRY_CODE	or
             nvl(fnd.	WORK_PHONE_AREA_CODE	,0) <>	cpy.	WORK_PHONE_AREA_CODE	or
             nvl(fnd.	WORK_PHONE_NO	,0) <>	cpy.	WORK_PHONE_NO	or
             nvl(fnd.	WORK_PHONE_EXTENSION_NO	,0) <>	cpy.	WORK_PHONE_EXTENSION_NO	or
             nvl(fnd.	WORK_FAX_COUNTRY_CODE	,0) <>	cpy.	WORK_FAX_COUNTRY_CODE	or
             nvl(fnd.	WORK_FAX_AREA_CODE	,0) <>	cpy.	WORK_FAX_AREA_CODE	or
             nvl(fnd.	WORK_FAX_NO	,0) <>	cpy.	WORK_FAX_NO	or
             nvl(fnd.	WORK_CELL_COUNTRY_CODE	,0) <>	cpy.	WORK_CELL_COUNTRY_CODE	or
             nvl(fnd.	WORK_CELL_AREA_CODE	,0) <>	cpy.	WORK_CELL_AREA_CODE	or
             nvl(fnd.	WORK_CELL_NO	,0) <>	cpy.	WORK_CELL_NO	or
             nvl(fnd.	WORK_EMAIL_ADDRESS	,0) <>	cpy.	WORK_EMAIL_ADDRESS	or
             nvl(fnd.	HOME_CELL_FAILURE_IND	,0) <>	cpy.	HOME_CELL_FAILURE_IND	or
             nvl(fnd.	HOME_CELL_DATE_LAST_UPDATED	,'1 Jan 1900') <>	cpy.	HOME_CELL_DATE_LAST_UPDATED	or
             nvl(fnd.	HOME_EMAIL_FAILURE_IND	,0) <>	cpy.	HOME_EMAIL_FAILURE_IND	or
             nvl(fnd.	HOME_EMAIL_DATE_LAST_UPDATED	,'1 Jan 1900') <>	cpy.	HOME_EMAIL_DATE_LAST_UPDATED	or
             nvl(fnd.	HOME_PHONE_FAILURE_IND	,0) <>	cpy.	HOME_PHONE_FAILURE_IND	or
             nvl(fnd.	HOME_PHONE_DATE_LAST_UPDATED	,'1 Jan 1900') <>	cpy.	HOME_PHONE_DATE_LAST_UPDATED	or
             nvl(fnd.	NO_MARKETING_VIA_EMAIL_IND	,0) <>	cpy.	NO_MARKETING_VIA_EMAIL_IND	or
             nvl(fnd.	NO_MARKETING_VIA_POST_IND	,0) <>	cpy.	NO_MARKETING_VIA_POST_IND	or
             nvl(fnd.	POST_ADDR_DATE_LAST_UPDATED	,'1 Jan 1900') <>	cpy.	POST_ADDR_DATE_LAST_UPDATED	or
             nvl(fnd.	WFS_CUSTOMER_NO_TXT_VER	,0) <>	cpy.	WFS_CUSTOMER_NO_TXT_VER	or
             nvl(fnd.	WORK_CELL_FAILURE_IND	,0) <>	cpy.	WORK_CELL_FAILURE_IND	or
             nvl(fnd.	WORK_CELL_DATE_LAST_UPDATED	,'1 Jan 1900') <>	cpy.	WORK_CELL_DATE_LAST_UPDATED	or
             nvl(fnd.	WORK_EMAIL_FAILURE_IND	,0) <>	cpy.	WORK_EMAIL_FAILURE_IND	or
             nvl(fnd.	WORK_EMAIL_DATE_LAST_UPDATED	,'1 Jan 1900') <>	cpy.	WORK_EMAIL_DATE_LAST_UPDATED	or
             nvl(fnd.	WORK_PHONE_FAILURE_IND	,0) <>	cpy.	WORK_PHONE_FAILURE_IND	or
             nvl(fnd.	WORK_PHONE_DATE_LAST_UPDATED	,'1 Jan 1900') <>	cpy.	WORK_PHONE_DATE_LAST_UPDATED	or
             nvl(fnd.	WW_ONLINE_CUSTOMER_NO	,0) <>	cpy.	WW_ONLINE_CUSTOMER_NO	or
             nvl(fnd.	LEGAL_LANGUAGE_DESCRIPTION	,0) <>	cpy.	LEGAL_LANGUAGE_DESCRIPTION	or
             nvl(fnd.	ESTATEMENT_EMAIL	,0) <>	cpy.	ESTATEMENT_EMAIL	or
             nvl(fnd.	ESTATEMENT_DATE_LAST_UPDATED	,'1 Jan 1900') <>	cpy.	ESTATEMENT_DATE_LAST_UPDATED	or
             nvl(fnd.	ESTATEMENT_EMAIL_FAILURE_IND	,0) <>	cpy.	ESTATEMENT_EMAIL_FAILURE_IND or
             nvl(fnd.	SOURCE_OF_INCOME_ID	,0) <>	cpy.	SOURCE_OF_INCOME_ID or
             nvl(fnd.	SOURCE_OF_INCOME_DESC	,0) <>	cpy.	SOURCE_OF_INCOME_DESC or
             nvl(fnd.	OCCUPATION_ID	,0) <>	cpy.	OCCUPATION_ID or
             nvl(fnd.	OCCUPATION_DESC	,0) <>	cpy.	OCCUPATION_DESC or
             nvl(fnd.	EMPLOYMENT_STATUS_ID	,0) <>	cpy.	EMPLOYMENT_STATUS_ID	or
             nvl(fnd.	EMPLOYMENT_STATUS_DESC	,0) <>	cpy.	EMPLOYMENT_STATUS_DESC or
             nvl(fnd.	SUBSCRIBER_KEY	,0) <>	cpy.	SUBSCRIBER_KEY
             )

-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              cpy.customer_no,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_customer_no   := 0;


for dupp_record in stg_dup
   loop

    if  dupp_record.customer_no    = g_customer_no  then
        update stg_c2_customer_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_customer_no    := dupp_record.customer_no;


   end loop;

   commit;

   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end remove_duplicates;

--**************************************************************************************************
-- Insert dummy m aster records to ensure RI
--**************************************************************************************************
procedure create_dummy_masters as
begin

--******************************************************************************

--      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_card fnd
--      select /*+ FULL(cpy)  parallel (cpy,2) */
--             distinct
--             cpy.loyalty_ww_swipe_no,
--             0,0,0,1,
--             g_date,
--             1
--      from   stg_c2_customer_cpy cpy

--       where not exists
--      (select /*+ nl_aj */ * from fnd_customer_card
--       where  card_no         = cpy.loyalty_ww_swipe_no )
--       and    sys_process_code    = 'N'
--       and    cpy.loyalty_ww_swipe_no is not null;

--       g_recs_dummy := g_recs_dummy + sql%rowcount;
       commit;

--******************************************************************************


  exception
      when dwh_errors.e_insert_error then
       l_message := 'DUMMY INS - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'DUMMY INS  - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end create_dummy_masters;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	CUSTOMER_NO	,
              cpy.	WFS_CUSTOMER_NO	,
            	cpy.	IDENTITY_DOCUMENT_CODE	,
            	cpy.	IDENTITY_DOCUMENT_TYPE	,
            	cpy.	PASSPORT_NO	,
            	cpy.	PASSPORT_EXPIRY_DATE	,
            	cpy.	PASSPORT_ISSUE_COUNTRY_CODE	,
            	cpy.	INDIVIDUAL_IND	,
            	cpy.	CUSTOMER_STATUS	,
            	cpy.	OPT_IN_IND	,
            	cpy.	GLID_IND	,
            	cpy.	ITC_IND	,
            	cpy.	FICA_STATUS	,
            	cpy.	NO_MARKETING_VIA_PHONE_IND	,
            	cpy.	NO_MARKETING_VIA_SMS_IND	,
            	cpy.	NO_SHARE_MY_DETAILS_IND	,
            	cpy.	C2_CREATE_DATE	,
            	cpy.	LAST_DETAIL_CONFIRM_DATE	,
            	cpy.	LAST_WEB_ACCESS_DATE	,
            	cpy.	FICA_CHANGE_DATE	,
            	cpy.	LAST_ITC_QUERY_DATE	,
            	cpy.	TITLE_CODE	,
            	cpy.	FIRST_MIDDLE_NAME_INITIAL	,
            	cpy.	FIRST_NAME	,
            	cpy.	PREFERRED_NAME	,
            	cpy.	LAST_NAME	,
            	cpy.	MAIDEN_NAME	,
            	cpy.	BIRTH_DATE	,
            	cpy.	GENDER_CODE	,
            	cpy.	MARITAL_STATUS	,
            	cpy.	MARITAL_CONTRACT_TYPE	,
            	cpy.	NUM_MINOR	,
            	cpy.	PREFERRED_LANGUAGE	,
            	cpy.	CUSTOMER_HOME_LANGUAGE	,
            	cpy.	RESIDENTIAL_COUNTRY_CODE	,
            	cpy.	PRIMARY_COM_MEDIUM	,
            	cpy.	PRIMARY_COM_LANGUAGE	,
            	cpy.	SECONDARY_COM_MEDIUM	,
            	cpy.	SECONDARY_COM_LANGUAGE	,
            	cpy.	POSTAL_ADDRESS_LINE_1	,
            	cpy.	POSTAL_ADDRESS_LINE_2	,
            	cpy.	POSTAL_ADDRESS_LINE_3	,
            	cpy.	POSTAL_CODE	,
            	cpy.	POSTAL_CITY_NAME	,
            	cpy.	POSTAL_PROVINCE_NAME	,
            	cpy.	POSTAL_COUNTRY_CODE	,
            	cpy.	POSTAL_ADDRESS_OCCUPATION_DATE	,
            	cpy.	POSTAL_NUM_RETURNED_MAIL	,
            	cpy.	PHYSICAL_ADDRESS_LINE_1	,
            	cpy.	PHYSICAL_ADDRESS_LINE2	,
            	cpy.	PHYSICAL_SUBURB_NAME	,
            	cpy.	PHYSICAL_POSTAL_CODE	,
            	cpy.	PHYSICAL_CITY_NAME	,
            	cpy.	PHYSICAL_PROVINCE_NAME	,
            	cpy.	PHYSICAL_COUNTRY_CODE	,
            	cpy.	PHYSICAL_ADDRESS_OCCUPTN_DATE	,
            	cpy.	PHYSICAL_NUM_RETURNED_MAIL	,
            	cpy.	HOME_PHONE_COUNTRY_CODE	,
            	cpy.	HOME_PHONE_AREA_CODE	,
            	cpy.	HOME_PHONE_NO	,
            	cpy.	HOME_PHONE_EXTENSION_NO	,
            	cpy.	HOME_FAX_COUNTRY_CODE	,
            	cpy.	HOME_FAX_AREA_CODE	,
            	cpy.	HOME_FAX_NO	,
            	cpy.	HOME_CELL_COUNTRY_CODE	,
            	cpy.	HOME_CELL_AREA_CODE	,
            	cpy.	HOME_CELL_NO	,
            	cpy.	HOME_EMAIL_ADDRESS	,
            	cpy.	EMPLOYMENT_STATUS_IND	,
            	cpy.	COMPANY_NAME	,
            	cpy.	COMPANY_TYPE	,
            	cpy.	EMPLOYEE_NO	,
            	cpy.	EMPLOYEE_DEPT	,
            	cpy.	EMPLOYEE_JOB_TITLE	,
            	cpy.	WORK_PHONE_COUNTRY_CODE	,
            	cpy.	WORK_PHONE_AREA_CODE	,
            	cpy.	WORK_PHONE_NO	,
            	cpy.	WORK_PHONE_EXTENSION_NO	,
            	cpy.	WORK_FAX_COUNTRY_CODE	,
            	cpy.	WORK_FAX_AREA_CODE	,
            	cpy.	WORK_FAX_NO	,
            	cpy.	WORK_CELL_COUNTRY_CODE	,
            	cpy.	WORK_CELL_AREA_CODE	,
            	cpy.	WORK_CELL_NO	,
            	cpy.	WORK_EMAIL_ADDRESS	,
            	cpy.	HOME_CELL_FAILURE_IND	,
            	cpy.	HOME_CELL_DATE_LAST_UPDATED	,
            	cpy.	HOME_EMAIL_FAILURE_IND	,
            	cpy.	HOME_EMAIL_DATE_LAST_UPDATED	,
            	cpy.	HOME_PHONE_FAILURE_IND	,
            	cpy.	HOME_PHONE_DATE_LAST_UPDATED	,
            	cpy.	NO_MARKETING_VIA_EMAIL_IND	,
            	cpy.	NO_MARKETING_VIA_POST_IND	,
            	cpy.	POST_ADDR_DATE_LAST_UPDATED	,
            	cpy.	WFS_CUSTOMER_NO_TXT_VER	,
            	cpy.	WORK_CELL_FAILURE_IND	,
            	cpy.	WORK_CELL_DATE_LAST_UPDATED	,
            	cpy.	WORK_EMAIL_FAILURE_IND	,
            	cpy.	WORK_EMAIL_DATE_LAST_UPDATED	,
            	cpy.	WORK_PHONE_FAILURE_IND	,
            	cpy.	WORK_PHONE_DATE_LAST_UPDATED	,
            	cpy.	WW_ONLINE_CUSTOMER_NO	,
            	cpy.	LEGAL_LANGUAGE_DESCRIPTION	,
            	cpy.	ESTATEMENT_EMAIL	,
            	cpy.	ESTATEMENT_DATE_LAST_UPDATED	,
            	cpy.	ESTATEMENT_EMAIL_FAILURE_IND	,
              g_date as last_updated_date,
              '','','','','','',
              '','','','','','','','','','','','','','','','','','','','','','','','','','','','','','',
            	cpy.	SOURCE_OF_INCOME_ID  ,
              cpy.	SOURCE_OF_INCOME_DESC  ,
              cpy.	OCCUPATION_ID  ,
              cpy.	OCCUPATION_DESC ,
            	cpy.	EMPLOYMENT_STATUS_ID	,
            	cpy.	EMPLOYMENT_STATUS_DESC,
              '','','',
              nvl(cpy.	SUBSCRIBER_KEY,0) SUBSCRIBER_KEY
              
       from  stg_c2_customer_cpy cpy
       where
       not exists
      (select /*+ nl_aj */ * from fnd_customer
       where  customer_no     = cpy.customer_no  )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
       and sys_process_code = 'N';


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



FOR upd_rec IN c_stg_c2_customer
   loop
     update fnd_customer fnd
     set    fnd.	IDENTITY_DOCUMENT_CODE	=	upd_rec.	IDENTITY_DOCUMENT_CODE	,
            fnd.	IDENTITY_DOCUMENT_TYPE	=	upd_rec.	IDENTITY_DOCUMENT_TYPE	,
            fnd.	PASSPORT_NO	=	upd_rec.	PASSPORT_NO	,
            fnd.	PASSPORT_EXPIRY_DATE	=	upd_rec.	PASSPORT_EXPIRY_DATE	,
            fnd.	PASSPORT_ISSUE_COUNTRY_CODE	=	upd_rec.	PASSPORT_ISSUE_COUNTRY_CODE	,
            fnd.	INDIVIDUAL_IND	=	upd_rec.	INDIVIDUAL_IND	,
            fnd.	CUSTOMER_STATUS	=	upd_rec.	CUSTOMER_STATUS	,
            fnd.	OPT_IN_IND	=	upd_rec.	OPT_IN_IND	,
            fnd.	GLID_IND	=	upd_rec.	GLID_IND	,
            fnd.	ITC_IND	=	upd_rec.	ITC_IND	,
            fnd.	FICA_STATUS	=	upd_rec.	FICA_STATUS	,
            fnd.	NO_MARKETING_VIA_PHONE_IND	=	upd_rec.	NO_MARKETING_VIA_PHONE_IND	,
            fnd.	NO_MARKETING_VIA_SMS_IND	=	upd_rec.	NO_MARKETING_VIA_SMS_IND	,
            fnd.	NO_SHARE_MY_DETAILS_IND	=	upd_rec.	NO_SHARE_MY_DETAILS_IND	,
            fnd.	C2_CREATE_DATE	=	upd_rec.	C2_CREATE_DATE	,
            fnd.	LAST_DETAIL_CONFIRM_DATE	=	upd_rec.	LAST_DETAIL_CONFIRM_DATE	,
            fnd.	LAST_WEB_ACCESS_DATE	=	upd_rec.	LAST_WEB_ACCESS_DATE	,
            fnd.	FICA_CHANGE_DATE	=	upd_rec.	FICA_CHANGE_DATE	,
            fnd.	LAST_ITC_QUERY_DATE	=	upd_rec.	LAST_ITC_QUERY_DATE	,
            fnd.	TITLE_CODE	=	upd_rec.	TITLE_CODE	,
            fnd.	FIRST_MIDDLE_NAME_INITIAL	=	upd_rec.	FIRST_MIDDLE_NAME_INITIAL	,
            fnd.	FIRST_NAME	=	upd_rec.	FIRST_NAME	,
            fnd.	PREFERRED_NAME	=	upd_rec.	PREFERRED_NAME	,
            fnd.	LAST_NAME	=	upd_rec.	LAST_NAME	,
            fnd.	MAIDEN_NAME	=	upd_rec.	MAIDEN_NAME	,
            fnd.	BIRTH_DATE	=	upd_rec.	BIRTH_DATE	,
            fnd.	GENDER_CODE	=	upd_rec.	GENDER_CODE	,
            fnd.	MARITAL_STATUS	=	upd_rec.	MARITAL_STATUS	,
            fnd.	MARITAL_CONTRACT_TYPE	=	upd_rec.	MARITAL_CONTRACT_TYPE	,
            fnd.	NUM_MINOR	=	upd_rec.	NUM_MINOR	,
            fnd.	PREFERRED_LANGUAGE	=	upd_rec.	PREFERRED_LANGUAGE	,
            fnd.	CUSTOMER_HOME_LANGUAGE	=	upd_rec.	CUSTOMER_HOME_LANGUAGE	,
            fnd.	RESIDENTIAL_COUNTRY_CODE	=	upd_rec.	RESIDENTIAL_COUNTRY_CODE	,
            fnd.	PRIMARY_COM_MEDIUM	=	upd_rec.	PRIMARY_COM_MEDIUM	,
            fnd.	PRIMARY_COM_LANGUAGE	=	upd_rec.	PRIMARY_COM_LANGUAGE	,
            fnd.	SECONDARY_COM_MEDIUM	=	upd_rec.	SECONDARY_COM_MEDIUM	,
            fnd.	SECONDARY_COM_LANGUAGE	=	upd_rec.	SECONDARY_COM_LANGUAGE	,
            fnd.	POSTAL_ADDRESS_LINE_1	=	upd_rec.	POSTAL_ADDRESS_LINE_1	,
            fnd.	POSTAL_ADDRESS_LINE_2	=	upd_rec.	POSTAL_ADDRESS_LINE_2	,
            fnd.	POSTAL_ADDRESS_LINE_3	=	upd_rec.	POSTAL_ADDRESS_LINE_3	,
            fnd.	POSTAL_CODE	=	upd_rec.	POSTAL_CODE	,
            fnd.	POSTAL_CITY_NAME	=	upd_rec.	POSTAL_CITY_NAME	,
            fnd.	POSTAL_PROVINCE_NAME	=	upd_rec.	POSTAL_PROVINCE_NAME	,
            fnd.	POSTAL_COUNTRY_CODE	=	upd_rec.	POSTAL_COUNTRY_CODE	,
            fnd.	POSTAL_ADDRESS_OCCUPATION_DATE	=	upd_rec.	POSTAL_ADDRESS_OCCUPATION_DATE	,
            fnd.	POSTAL_NUM_RETURNED_MAIL	=	upd_rec.	POSTAL_NUM_RETURNED_MAIL	,
            fnd.	PHYSICAL_ADDRESS_LINE_1	=	upd_rec.	PHYSICAL_ADDRESS_LINE_1	,
            fnd.	PHYSICAL_ADDRESS_LINE2	=	upd_rec.	PHYSICAL_ADDRESS_LINE2	,
            fnd.	PHYSICAL_SUBURB_NAME	=	upd_rec.	PHYSICAL_SUBURB_NAME	,
            fnd.	PHYSICAL_POSTAL_CODE	=	upd_rec.	PHYSICAL_POSTAL_CODE	,
            fnd.	PHYSICAL_CITY_NAME	=	upd_rec.	PHYSICAL_CITY_NAME	,
            fnd.	PHYSICAL_PROVINCE_NAME	=	upd_rec.	PHYSICAL_PROVINCE_NAME	,
            fnd.	PHYSICAL_COUNTRY_CODE	=	upd_rec.	PHYSICAL_COUNTRY_CODE	,
            fnd.	PHYSICAL_ADDRESS_OCCUPTN_DATE	=	upd_rec.	PHYSICAL_ADDRESS_OCCUPTN_DATE	,
            fnd.	PHYSICAL_NUM_RETURNED_MAIL	=	upd_rec.	PHYSICAL_NUM_RETURNED_MAIL	,
            fnd.	HOME_PHONE_COUNTRY_CODE	=	upd_rec.	HOME_PHONE_COUNTRY_CODE	,
            fnd.	HOME_PHONE_AREA_CODE	=	upd_rec.	HOME_PHONE_AREA_CODE	,
            fnd.	HOME_PHONE_NO	=	upd_rec.	HOME_PHONE_NO	,
            fnd.	HOME_PHONE_EXTENSION_NO	=	upd_rec.	HOME_PHONE_EXTENSION_NO	,
            fnd.	HOME_FAX_COUNTRY_CODE	=	upd_rec.	HOME_FAX_COUNTRY_CODE	,
            fnd.	HOME_FAX_AREA_CODE	=	upd_rec.	HOME_FAX_AREA_CODE	,
            fnd.	HOME_FAX_NO	=	upd_rec.	HOME_FAX_NO	,
            fnd.	HOME_CELL_COUNTRY_CODE	=	upd_rec.	HOME_CELL_COUNTRY_CODE	,
            fnd.	HOME_CELL_AREA_CODE	=	upd_rec.	HOME_CELL_AREA_CODE	,
            fnd.	HOME_CELL_NO	=	upd_rec.	HOME_CELL_NO	,
            fnd.	HOME_EMAIL_ADDRESS	=	upd_rec.	HOME_EMAIL_ADDRESS	,
            fnd.	EMPLOYMENT_STATUS_IND	=	upd_rec.	EMPLOYMENT_STATUS_IND	,
            fnd.	COMPANY_NAME	=	upd_rec.	COMPANY_NAME	,
            fnd.	COMPANY_TYPE	=	upd_rec.	COMPANY_TYPE	,
            fnd.	EMPLOYEE_NO	=	upd_rec.	EMPLOYEE_NO	,
            fnd.	EMPLOYEE_DEPT	=	upd_rec.	EMPLOYEE_DEPT	,
            fnd.	EMPLOYEE_JOB_TITLE	=	upd_rec.	EMPLOYEE_JOB_TITLE	,
            fnd.	WORK_PHONE_COUNTRY_CODE	=	upd_rec.	WORK_PHONE_COUNTRY_CODE	,
            fnd.	WORK_PHONE_AREA_CODE	=	upd_rec.	WORK_PHONE_AREA_CODE	,
            fnd.	WORK_PHONE_NO	=	upd_rec.	WORK_PHONE_NO	,
            fnd.	WORK_PHONE_EXTENSION_NO	=	upd_rec.	WORK_PHONE_EXTENSION_NO	,
            fnd.	WORK_FAX_COUNTRY_CODE	=	upd_rec.	WORK_FAX_COUNTRY_CODE	,
            fnd.	WORK_FAX_AREA_CODE	=	upd_rec.	WORK_FAX_AREA_CODE	,
            fnd.	WORK_FAX_NO	=	upd_rec.	WORK_FAX_NO	,
            fnd.	WORK_CELL_COUNTRY_CODE	=	upd_rec.	WORK_CELL_COUNTRY_CODE	,
            fnd.	WORK_CELL_AREA_CODE	=	upd_rec.	WORK_CELL_AREA_CODE	,
            fnd.	WORK_CELL_NO	=	upd_rec.	WORK_CELL_NO	,
            fnd.	WORK_EMAIL_ADDRESS	=	upd_rec.	WORK_EMAIL_ADDRESS	,
            fnd.	HOME_CELL_FAILURE_IND	=	upd_rec.	HOME_CELL_FAILURE_IND	,
            fnd.	HOME_CELL_DATE_LAST_UPDATED	=	upd_rec.	HOME_CELL_DATE_LAST_UPDATED	,
            fnd.	HOME_EMAIL_FAILURE_IND	=	upd_rec.	HOME_EMAIL_FAILURE_IND	,
            fnd.	HOME_EMAIL_DATE_LAST_UPDATED	=	upd_rec.	HOME_EMAIL_DATE_LAST_UPDATED	,
            fnd.	HOME_PHONE_FAILURE_IND	=	upd_rec.	HOME_PHONE_FAILURE_IND	,
            fnd.	HOME_PHONE_DATE_LAST_UPDATED	=	upd_rec.	HOME_PHONE_DATE_LAST_UPDATED	,
            fnd.	NO_MARKETING_VIA_EMAIL_IND	=	upd_rec.	NO_MARKETING_VIA_EMAIL_IND	,
            fnd.	NO_MARKETING_VIA_POST_IND	=	upd_rec.	NO_MARKETING_VIA_POST_IND	,
            fnd.	POST_ADDR_DATE_LAST_UPDATED	=	upd_rec.	POST_ADDR_DATE_LAST_UPDATED	,
            fnd.	WFS_CUSTOMER_NO_TXT_VER	=	upd_rec.	WFS_CUSTOMER_NO_TXT_VER	,
            fnd.	WORK_CELL_FAILURE_IND	=	upd_rec.	WORK_CELL_FAILURE_IND	,
            fnd.	WORK_CELL_DATE_LAST_UPDATED	=	upd_rec.	WORK_CELL_DATE_LAST_UPDATED	,
            fnd.	WORK_EMAIL_FAILURE_IND	=	upd_rec.	WORK_EMAIL_FAILURE_IND	,
            fnd.	WORK_EMAIL_DATE_LAST_UPDATED	=	upd_rec.	WORK_EMAIL_DATE_LAST_UPDATED	,
            fnd.	WORK_PHONE_FAILURE_IND	=	upd_rec.	WORK_PHONE_FAILURE_IND	,
            fnd.	WORK_PHONE_DATE_LAST_UPDATED	=	upd_rec.	WORK_PHONE_DATE_LAST_UPDATED	,
            fnd.	WW_ONLINE_CUSTOMER_NO	=	upd_rec.	WW_ONLINE_CUSTOMER_NO	,
            fnd.	LEGAL_LANGUAGE_DESCRIPTION	=	upd_rec.	LEGAL_LANGUAGE_DESCRIPTION	,
            fnd.	ESTATEMENT_EMAIL	=	upd_rec.	ESTATEMENT_EMAIL	,
            fnd.	ESTATEMENT_DATE_LAST_UPDATED	=	upd_rec.	ESTATEMENT_DATE_LAST_UPDATED	,
            fnd.	ESTATEMENT_EMAIL_FAILURE_IND	=	upd_rec.	ESTATEMENT_EMAIL_FAILURE_IND	,
            fnd.  last_updated_date = g_date  ,
            fnd.	SOURCE_OF_INCOME_ID	=	upd_rec.	SOURCE_OF_INCOME_ID	,
            fnd.	SOURCE_OF_INCOME_DESC	=	upd_rec.	SOURCE_OF_INCOME_DESC	,
            fnd.	OCCUPATION_ID	=	upd_rec.	OCCUPATION_ID	,
            fnd.	OCCUPATION_DESC	=	upd_rec.	OCCUPATION_DESC ,
            fnd.	EMPLOYMENT_STATUS_ID	=	upd_rec.	EMPLOYMENT_STATUS_ID	,
            fnd.	EMPLOYMENT_STATUS_DESC	=	upd_rec.	EMPLOYMENT_STATUS_DESC,
--            fnd.	SUBSCRIBER_KEY	=	nvl(upd_rec.	SUBSCRIBER_KEY,0)
            fnd.	SUBSCRIBER_KEY	=	
                  case     when nvl(upd_rec.SUBSCRIBER_KEY,'0')  = '0' then nvl(fnd.SUBSCRIBER_KEY,'0') 
                           else upd_rec.SUBSCRIBER_KEY
                  end     
     where  fnd.	customer_no	      =	upd_rec.	customer_no   ;

      g_recs_updated     := g_recs_updated + 1;
      g_physical_updated := g_physical_updated + sql%rowcount;
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
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin



--g_recs_hospital := g_recs_hospital + sql%rowcount;

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

--    l_text := 'CREATION OF DUMMY MASTER RECORDS STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
--    create_dummy_masters;

    select count(*)
    into   g_recs_read
    from   stg_c2_customer_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    flagged_records_hospital;



--    Taken out for better performance --------------------
--    update stg_c2_customer_cpy
--    set    sys_process_code = 'Y';





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
/*
   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital  then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;

*/
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
end wh_fnd_cust_020u;
