--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_156U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_156U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create cust talk2me fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_talk2me
--               Output - cust_talk2me
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
g_run_date           date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_156U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD TALK2ME  EX FND C2';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_fnd_talk2me is
select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*
      from    fnd_talk2me fnd,
              cust_talk2me prf
      where   fnd.customer_no = prf.customer_no   AND
              fnd.run_date    = prf.run_date 
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
               fnd.customer_no,fnd.run_date;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (prf,2) */ into cust_talk2me prf
      select /*+ FULL(fnd)  parallel (fnd,2) */
             	fnd.*
       from   fnd_talk2me fnd
       where  
       not exists
      (select /*+ nl_aj */ * from cust_talk2me
       where  customer_no    = fnd.customer_no and
              run_date       = fnd.run_date
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

FOR upd_rec IN c_fnd_talk2me
   loop
     update cust_talk2me prf
     set    prf.wfs_customer_no	=	upd_rec.	wfs_customer_no	,
            prf.wfs_account_no	=	upd_rec.	wfs_account_no	,
            prf.closed_ind	=	upd_rec.	closed_ind	,
            prf.deceased_ind	=	upd_rec.	deceased_ind	,
            prf.fraud_ind	=	upd_rec.	fraud_ind	,
            prf.jailed_ind	=	upd_rec.	jailed_ind	,
            prf.delinquent_ind	=	upd_rec.	delinquent_ind	,
            prf.title_code	=	upd_rec.	title_code	,
            prf.first_middle_name_initial	=	upd_rec.	first_middle_name_initial	,
            prf.first_name	=	upd_rec.	first_name	,
            prf.last_name	=	upd_rec.	last_name	,
            prf.preferred_language	=	upd_rec.	preferred_language	,
            prf.email_address	=	upd_rec.	email_address	,
            prf.cell_no	=	upd_rec.	cell_no	,
            prf.postal_address_line_1	=	upd_rec.	postal_address_line_1	,
            prf.postal_address_line_2	=	upd_rec.	postal_address_line_2	,
            prf.postal_address_line_3	=	upd_rec.	postal_address_line_3	,
            prf.postal_code	=	upd_rec.	postal_code	,
            prf.home_phone_no	=	upd_rec.	home_phone_no	,
            prf.work_phone_no	=	upd_rec.	work_phone_no	,
            prf.storcard_ind	=	upd_rec.	storcard_ind	,
            prf.creditcard_ind	=	upd_rec.	creditcard_ind	,
            prf.differencecard_ind	=	upd_rec.	differencecard_ind	,
            prf.myschool_ind	=	upd_rec.	myschool_ind	,
            prf.littleworld_ind	=	upd_rec.	littleworld_ind	,
            prf.max_tran_date	=	upd_rec.	max_tran_date	,
            prf.storecard_otb	=	upd_rec.	storecard_otb	,
            prf.creditcard_otb	=	upd_rec.	creditcard_otb	,
            prf.birthday_month	=	upd_rec.	birthday_month	,
            prf.account_holder_age	=	upd_rec.	account_holder_age	,
            prf.gender_code	=	upd_rec.	gender_code	,
            prf.lsm	=	upd_rec.	lsm	,
            prf.non_food_life_seg_code	=	upd_rec.	non_food_life_seg_code	,
            prf.food_life_seg_code	=	upd_rec.	food_life_seg_code	,
            prf.nfshv_current_seg	=	upd_rec.	nfshv_current_seg	,
            prf.fshv_current_seg	=	upd_rec.	fshv_current_seg	,
            prf.csm_shopping_habit_segment_no	=	upd_rec.	csm_shopping_habit_segment_no	,
            prf.csm_preferred_store	=	upd_rec.	csm_preferred_store	,
            prf.start_tier	=	upd_rec.	start_tier	,
            prf.month_tier	=	upd_rec.	month_tier	,
            prf.month_spend	=	upd_rec.	month_spend	,
            prf.ytd_spend	=	upd_rec.	ytd_spend	,
            prf.last_updated_date = g_date,
            prf.debt_review_ind	  =	upd_rec.	debt_review_ind	,
            prf.charged_off_ind	  =	upd_rec.	charged_off_ind	,
            prf.vitality_ind	    =	upd_rec.	vitality_ind,
            prf.month_discount  	=	upd_rec.	month_discount	,
            prf.month_green_value	=	upd_rec.	month_green_value	,
            prf.month_tier_value	=	upd_rec.	month_tier_value	,
            prf.ytd_discount	    =	upd_rec.	ytd_discount	,
            prf.ytd_green_value	  =	upd_rec.	ytd_green_value	,
            prf.ytd_tier_value	  =	upd_rec.	ytd_tier_value

     where  prf.customer_no	      =	upd_rec.customer_no and
            prf.run_date          = upd_rec.run_date;

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




    select this_mn_end_date into g_run_date from dim_calendar where calendar_date = g_date;

 
    if g_run_date = g_date then
       l_text := 'PROCESSING MONTH END SNAPSHOT '||
       to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       select count(*)
       into   g_recs_read
       from   fnd_talk2me
       ;

       l_text := 'BULK UPDATE STARTED AT '||
       to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       flagged_records_update;

       l_text := 'BULK INSERT STARTED AT '||
       to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       flagged_records_insert;

    else

       l_text := 'NO RUN TODAY - NOT MONTH END '||
       to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    end if;


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
end wh_prf_cust_156u;
