-- ****** Object: Procedure W7131037.WH_PRF_CUST_020TO Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_020TO" (p_forall_limit in integer,p_success out boolean) as

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
--      and     fnd.last_updated_date = g_date
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
             	fnd.	*
       from  fnd_customer fnd
       where --fnd.last_updated_date = g_date    and
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
            prf.	age_acc_holder	=	upd_rec.	age_acc_holder
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
 --   where  last_updated_date = g_date
 ;

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
end wh_prf_cust_020to;
