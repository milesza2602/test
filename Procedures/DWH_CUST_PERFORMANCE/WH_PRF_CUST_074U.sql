--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_074U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_074U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create WOD LCM CUST CARD fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_wod_lcm_cust_card
--               Output - dim_wod_lcm_cust_card
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_074U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD LCM CUSTOMER_CARD MASTER EX WOD';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_fnd_wod_lcm_cust_card is
select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*
      from    fnd_wod_lcm_cust_card fnd,
              dim_wod_lcm_cust_card prf
      where   fnd.card_no       = prf.card_no
      and     fnd.last_updated_date = g_date
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.card_no;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

       insert /*+ APPEND parallel (prf,2) */ into dim_wod_lcm_cust_card prf
       select /*+ FULL(fnd)  parallel (fnd,2) */
             	fnd.	*
       from   fnd_wod_lcm_cust_card fnd
       where  fnd.last_updated_date = g_date    and
       not exists
      (select /*+ nl_aj */ * from dim_wod_lcm_cust_card
       where  card_no    = fnd.card_no
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



FOR upd_rec IN c_fnd_wod_lcm_cust_card
   loop
     update dim_wod_lcm_cust_card prf
     set    prf.card_no2	            =	upd_rec.	card_no2	,
            prf.c2_customer_no	      =	upd_rec.	c2_customer_no	,
            prf.c2_customer_no2	      =	upd_rec.	c2_customer_no2	,
            prf.primary_account_no	  =	upd_rec.	primary_account_no	,
            prf.primary_account_no2	  =	upd_rec.	primary_account_no2	,
            prf.first_swipe_date	    =	upd_rec.	first_swipe_date	,
            prf.last_swipe_date	      =	upd_rec.	last_swipe_date	,
            prf.swipe_counter	        =	upd_rec.	swipe_counter	,
            prf.myschool_customer_id	=	upd_rec.	myschool_customer_id	,
            prf.linked_date	          =	upd_rec.	linked_date	,
            prf.myschool_linked_date	=	upd_rec.	myschool_linked_date	,
            prf.first_warning_date	  =	upd_rec.	first_warning_date	,
            prf.warning_counter	      =	upd_rec.	warning_counter	,
            prf.last_warning_date	    =	upd_rec.	last_warning_date	,
            prf.last_store	          =	upd_rec.	last_store	,
            prf.vmp_opt_in	          =	upd_rec.	vmp_opt_in	,
            prf.backdate_option	      =	upd_rec.	backdate_option	,
            prf.backdate_from_date	  =	upd_rec.	backdate_from_date	,
            prf.backdated	            =	upd_rec.	backdated	,
            prf.	last_updated_date 	=	g_date
     where  prf.	card_no	            =	upd_rec.	card_no ;

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
    from   fnd_wod_lcm_cust_card
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
end wh_prf_cust_074u;
