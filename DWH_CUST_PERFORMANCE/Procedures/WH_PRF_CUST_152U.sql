--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_152U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_152U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create cust interaction fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_cust_interaction
--               Output - cust_interaction
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_152U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUSTOMER INTERACTION  EX FND C2';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

/*+ FULL(fnd)  parallel (4) */

cursor c_fnd_cust_interaction is
select /*+ parallel (4) */
              fnd.*
      from    fnd_cust_interaction fnd,
              cust_interaction prf
      where   fnd.inquiry_id = prf.inquiry_id  and
              fnd.last_updated_date = g_date
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
               fnd.inquiry_id;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
--/*+ FULL(fnd)  parallel (fnd,2) */

--      insert /*+ APPEND parallel (prf,2) */ into cust_interaction prf
      insert  into cust_interaction prf
      select /*+ parallel (fnd,4) */
             	fnd.*
       from   fnd_cust_interaction fnd
       where  fnd.last_updated_date = g_date    and
       not exists
      (select /*+ nl_aj */ * from cust_interaction
       where  inquiry_id    = fnd.inquiry_id
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

FOR upd_rec IN c_fnd_cust_interaction
   loop
     update cust_interaction prf
     set    prf.account_contact_id	=	upd_rec.	account_contact_id	,
            prf.account_id	=	upd_rec.	account_id	,
            prf.unresolved_cust_id	=	upd_rec.	unresolved_cust_id	,
            prf.customer_int_id	=	upd_rec.	customer_int_id	,
            prf.interaction_date	=	upd_rec.	interaction_date	,
            prf.cust_int_closed	=	upd_rec.	cust_int_closed	,
            prf.cust_int_chng_date	=	upd_rec.	cust_int_chng_date	,
            prf.cust_int_chng_by	=	upd_rec.	cust_int_chng_by	,
            prf.inquiry_type_desc	=	upd_rec.	inquiry_type_desc	,
            prf.front_end_desc	=	upd_rec.	front_end_desc	,
            prf.front_end_appl_id	=	upd_rec.	front_end_appl_id	,
            prf.category1_desc	=	upd_rec.	category1_desc	,
            prf.category2_desc	=	upd_rec.	category2_desc	,
            prf.status_desc	=	upd_rec.	status_desc	,
            prf.state_desc	=	upd_rec.	state_desc	,
            prf.priority_desc	=	upd_rec.	priority_desc	,
            prf.channel_desc	=	upd_rec.	channel_desc	,
            prf.business_area_desc	=	upd_rec.	business_area_desc	,
            prf.inq_chng_date	=	upd_rec.	inq_chng_date	,
            prf.inq_chng_by	=	upd_rec.	inq_chng_by	,
            prf.inquiry_txt	=	upd_rec.	inquiry_txt	,
            prf.inq_desc_chng_date	=	upd_rec.	inq_desc_chng_date	,
            prf.inq_desc_chng_by	=	upd_rec.	inq_desc_chng_by	,
            prf.logged_date	=	upd_rec.	logged_date	,
            prf.owner_user_name	=	upd_rec.	owner_user_name	,
            prf.owner_grp_name	=	upd_rec.	owner_grp_name	,
            prf.transfer_user_name	=	upd_rec.	transfer_user_name	,
            prf.transfer_grp_name	=	upd_rec.	transfer_grp_name	,
            prf.inq_closed_date	=	upd_rec.	inq_closed_date	,
            prf.closed_by_name	=	upd_rec.	closed_by_name	,
            prf.logged_by_name	=	upd_rec.	logged_by_name	,
            prf.inq_det_chng_date	=	upd_rec.	inq_det_chng_date	,
            prf.inq_det_chng_by	=	upd_rec.	inq_det_chng_by	,
            prf.order_id	=	upd_rec.	order_id	,
            prf.order_prod_offer	=	upd_rec.	order_prod_offer	,
            prf.solution_txt	=	upd_rec.	solution_txt	,
            prf.inq_sol_chng_date	=	upd_rec.	inq_sol_chng_date	,
            prf.inq_sol_chng_by	=	upd_rec.	inq_sol_chng_by	,
            prf.staff_name	=	upd_rec.	staff_name	,
            prf.staff_no	=	upd_rec.	staff_no	,
            prf.inq_stff_chng_date	=	upd_rec.	inq_stff_chng_date	,
            prf.inq_stff_chng_by	=	upd_rec.	inq_stff_chng_by	,
            prf.classified_date	=	upd_rec.	classified_date	,
            prf.respond_date	=	upd_rec.	respond_date	,
            prf.return_date	=	upd_rec.	return_date	,
            prf.trans_accept_date	=	upd_rec.	trans_accept_date	,
            prf.transfer_date	=	upd_rec.	transfer_date	,
            prf.qa_ind	=	upd_rec.	qa_ind	,
            prf.receipt_ack_ind	=	upd_rec.	receipt_ack_ind	,
            prf.return_ind	=	upd_rec.	return_ind	,
            prf.assign_date	=	upd_rec.	assign_date	,
            prf.feedback_requ_ind	=	upd_rec.	feedback_requ_ind	,
            prf.callback_date	=	upd_rec.	callback_date	,
            prf.transfer_ind	=	upd_rec.	transfer_ind	,
            prf.inq_trck_chng_date	=	upd_rec.	inq_trck_chng_date	,
            prf.inq_trck_chng_by	=	upd_rec.	inq_trck_chng_by	,
            prf.itb_external_no	=	upd_rec.	itb_external_no	,
            prf.offer_no	=	upd_rec.	offer_no	,
            prf.camp_id	=	upd_rec.	camp_id	,
            prf.uncontactable_no	=	upd_rec.	uncontactable_no	,
            prf.creative_id	=	upd_rec.	creative_id	,
            prf.sales_amount_no	=	upd_rec.	sales_amount_no	,
            prf.customer_experience_no	=	upd_rec.	customer_experience_no	,
            prf.customer_experience_desc	=	upd_rec.	customer_experience_desc	,
            prf.sla_resolve_by_date	=	upd_rec.	sla_resolve_by_date	,
            prf.sla_resolve_no	=	upd_rec.	sla_resolve_no	,
            prf.report_ind	=	upd_rec.	report_ind	,
            prf.inquiry_type_id	=	upd_rec.	inquiry_type_id	,
            prf.logged_datetime	=	upd_rec.	logged_datetime	,
            prf.closed_datetime	=	upd_rec.	closed_datetime	,
            prf.last_updated_date = g_date
     where  prf.inquiry_id	      =	upd_rec.inquiry_id ;

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
--    execute immediate 'alter session enable parallel dml';


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
    from   fnd_cust_interaction
    where  last_updated_date = g_date
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
end wh_prf_cust_152u;
