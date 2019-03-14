-- ****** Object: Procedure W7131037.WH_PRF_CUST_064U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_064U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create WOD STATISTICS fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_wod_application
--               Output - dim_wod_application
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

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_064U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD APPLICATONI MASTER EX WOD';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_fnd_wod_application is
select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*
      from    fnd_wod_application fnd,
              dim_wod_application prf
      where   fnd.application_id       = prf.application_id
      and     fnd.last_updated_date = g_date
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.application_id;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

       insert /*+ APPEND parallel (prf,2) */ into dim_wod_application prf
       SELECT /*+ FULL(fnd)  parallel (fnd,2) */
             	            fnd.*
       from   fnd_wod_application fnd
       where  fnd.last_updated_date = g_date    and
       not exists
      (select /*+ nl_aj */ * from dim_wod_application
       where  application_id    = fnd.application_id
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



FOR upd_rec IN c_fnd_wod_application
   loop
     update dim_wod_application prf
     set    prf.address_line1	=	upd_rec.	address_line1	,
            prf.address_line2	=	upd_rec.	address_line2	,
            prf.application_date	=	upd_rec.	application_date	,
            prf.application_status	=	upd_rec.	application_status	,
            prf.application_status_desc	=	upd_rec.	application_status_desc	,
            prf.birth_date	=	upd_rec.	birth_date	,
            prf.cell_phone_no	=	upd_rec.	cell_phone_no	,
            prf.channel	=	upd_rec.	channel	,
            prf.child1_birthdate	=	upd_rec.	child1_birthdate	,
            prf.child1_gender	=	upd_rec.	child1_gender	,
            prf.child1_name	=	upd_rec.	child1_name	,
            prf.child1_surname	=	upd_rec.	child1_surname	,
            prf.child2_birthdate	=	upd_rec.	child2_birthdate	,
            prf.child2_gender	=	upd_rec.	child2_gender	,
            prf.child2_name	=	upd_rec.	child2_name	,
            prf.child2_surname	=	upd_rec.	child2_surname	,
            prf.child3_birthdte	=	upd_rec.	child3_birthdte	,
            prf.child3_gender	=	upd_rec.	child3_gender	,
            prf.child3_name	=	upd_rec.	child3_name	,
            prf.child3_surname	=	upd_rec.	child3_surname	,
            prf.child4_birthdate	=	upd_rec.	child4_birthdate	,
            prf.child4_gender	=	upd_rec.	child4_gender	,
            prf.child4_name	=	upd_rec.	child4_name	,
            prf.child4_surname	=	upd_rec.	child4_surname	,
            prf.child5_birthdate	=	upd_rec.	child5_birthdate	,
            prf.child5_gender	=	upd_rec.	child5_gender	,
            prf.child5_name	=	upd_rec.	child5_name	,
            prf.child5_surname	=	upd_rec.	child5_surname	,
            prf.child6_birthdate	=	upd_rec.	child6_birthdate	,
            prf.child6_gender	=	upd_rec.	child6_gender	,
            prf.child6_name	=	upd_rec.	child6_name	,
            prf.child6_surname	=	upd_rec.	child6_surname	,
            prf.country_desc	=	upd_rec.	country_desc	,
            prf.created_myschool	=	upd_rec.	created_myschool	,
            prf.customer_no	=	upd_rec.	customer_no	,
            prf.pregnancy_due_date	=	upd_rec.	pregnancy_due_date	,
            prf.email_address	=	upd_rec.	email_address	,
            prf.error_status	=	upd_rec.	error_status	,
            prf.cust_first_name	=	upd_rec.	cust_first_name	,
            prf.gender	=	upd_rec.	gender	,
            prf.home_phone_no	=	upd_rec.	home_phone_no	,
            prf.identity_no	=	upd_rec.	identity_no	,
            prf.cust_initials	=	upd_rec.	cust_initials	,
            prf.littleworld	=	upd_rec.	littleworld	,
            prf.myschool_card_no	=	upd_rec.	myschool_card_no	,
            prf.myschool	=	upd_rec.	myschool	,
            prf.cust_surname	=	upd_rec.	cust_surname	,
            prf.passport_no	=	upd_rec.	passport_no	,
            prf.permission_3rd_party	=	upd_rec.	permission_3rd_party	,
            prf.permission_email	=	upd_rec.	permission_email	,
            prf.permission_phone	=	upd_rec.	permission_phone	,
            prf.permission_pos	=	upd_rec.	permission_pos	,
            prf.permission_post	=	upd_rec.	permission_post	,
            prf.permission_sms	=	upd_rec.	permission_sms	,
            prf.popout_card_no	=	upd_rec.	popout_card_no	,
            prf.postal_code	=	upd_rec.	postal_code	,
            prf.preferred_language_desc	=	upd_rec.	preferred_language_desc	,
            prf.appl_processed	=	upd_rec.	appl_processed	,
            prf.relationship_id	=	upd_rec.	relationship_id	,
            prf.referral_desc	=	upd_rec.	referral_desc	,
            prf.suburb	=	upd_rec.	suburb	,
            prf.terms_conditions_wod	=	upd_rec.	terms_conditions_wod	,
            prf.terms_conditions_lw	=	upd_rec.	terms_conditions_lw	,
            prf.title_desc	=	upd_rec.	title_desc	,
            prf.work_phone_no	=	upd_rec.	work_phone_no	,
            prf.receive_card_post	=	upd_rec.	receive_card_post	,
            prf.receive_card_collect	=	upd_rec.	receive_card_collect	,
            prf.application_customer	=	upd_rec.	application_customer	,
            prf.twitter_handle	      =	upd_rec.	twitter_handle	,
            prf.	last_updated_date 	=	g_date
     where  prf.	application_id	          =	upd_rec.	application_id ;

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
    from   fnd_wod_application
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
end wh_prf_cust_064u;
