--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_102U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_102U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_basket_tender fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_cust_basket_tender
--               Output - cust_basket_tender
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_102U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD BASKET TENDER EX POS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- FULL(fnd)
cursor c_fnd_cust_basket_tender is
select /*+  parallel (fnd,2) */
              fnd.*
      from    fnd_cust_basket_tender fnd,
              cust_basket_tender prf
      where   fnd.location_no       = prf.location_no and
              fnd.tran_date         = prf.tran_date   and
              fnd.till_no           = prf.till_no     and
              fnd.tran_no           = prf.tran_no     and
              fnd.tender_type_code  = prf.tender_type_code  and
              fnd.tender_seq_no     = prf.tender_seq_no     and
              fnd.last_updated_date = g_date and
              fnd.tran_date         > g_date - 42
              -- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.location_no,
              fnd.tran_date,
              fnd.till_no,fnd.tran_no,fnd.tender_type_code,fnd.tender_seq_no;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
 --     /*+ APPEND parallel (prf,2) */
 
      insert /*+ APPEND */ into cust_basket_tender prf
      select /*+ FULL(fnd) parallel (fnd,4) */
             	fnd.	location_no,
              fnd.	till_no,
              fnd.	tran_no,
              fnd.	tender_type_code,
              fnd.	tender_seq_no,
              fnd.	tran_date,
              fnd.	tran_time,
              fnd.	tender_type_detail_code,
              fnd.	tender_no,
              fnd.	tender_selling,
              fnd.	change_tender_type,
              fnd.	change_selling,
              fnd.	change_gift_no,
              fnd.	card_seq_no,
              fnd.	card_authorisation_id,
              fnd.	tender_valid_offline_ind,
              fnd.	payment_acc_type,
              fnd.	payment_acc_type_dtl_code,
              fnd.	payment_account_no,
              fnd.	loyalty_ww_swipe_no,
              fnd.	card_eps_no,
              fnd.	customer_no,
              fnd.	vitality_cust_ind,
              0,
              g_date as last_updated_date,
              fnd.	EMPLOYEE_ID,
              fnd.	COMPANY_CODE,
              fnd.	TENDER_SELLING_LOCAL,
              fnd.	CHANGE_SELLING_LOCAL,
              fnd.	FORM_FACTOR_IND_NO,
              fnd.	PAN_ENTRY_MODE_NO 
       from  fnd_cust_basket_tender fnd
       where fnd.last_updated_date = g_date    and
       not exists
      (select /*+ nl_aj */ * from cust_basket_tender
       where  location_no       = fnd.location_no and
              tran_date         = fnd.tran_date   and
              till_no           = fnd.till_no     and
              tran_no           = fnd.tran_no     and
              tender_type_code  = fnd.tender_type_code  and
              tender_seq_no     = fnd.tender_seq_no
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



FOR upd_rec IN c_fnd_cust_basket_tender
   loop
     update cust_basket_tender prf
     set    prf.	tran_time	                =	upd_rec.	tran_time,
            prf.	tender_type_detail_code	  =	upd_rec.	tender_type_detail_code,
            prf.	tender_no	                =	upd_rec.	tender_no,
            prf.	tender_selling	          =	upd_rec.	tender_selling,
            prf.	change_tender_type	      =	upd_rec.	change_tender_type,
            prf.	change_selling	          =	upd_rec.	change_selling,
            prf.	change_gift_no	          =	upd_rec.	change_gift_no,
            prf.	card_seq_no	              =	upd_rec.	card_seq_no,
            prf.	card_authorisation_id	    =	upd_rec.	card_authorisation_id,
            prf.	tender_valid_offline_ind	=	upd_rec.	tender_valid_offline_ind,
            prf.	payment_acc_type	        =	upd_rec.	payment_acc_type,
            prf.	payment_acc_type_dtl_code	=	upd_rec.	payment_acc_type_dtl_code,
            prf.	payment_account_no	      =	upd_rec.	payment_account_no,
            prf.	loyalty_ww_swipe_no	      =	upd_rec.	loyalty_ww_swipe_no,
            prf.	card_eps_no	              =	upd_rec.	card_eps_no,
            prf.	customer_no	              =	upd_rec.	customer_no,
            prf.	vitality_cust_ind	        =	upd_rec.	vitality_cust_ind,
            prf.	EMPLOYEE_ID	              =	upd_rec.	EMPLOYEE_ID	,
            prf.	COMPANY_CODE              =	upd_rec.	COMPANY_CODE	,
            prf.	TENDER_SELLING_LOCAL      =	upd_rec.	TENDER_SELLING_LOCAL	,
            prf.	CHANGE_SELLING_LOCAL	    =	upd_rec.	CHANGE_SELLING_LOCAL	,
            prf.	FORM_FACTOR_IND_NO        =	upd_rec.	FORM_FACTOR_IND_NO	,
            prf.	PAN_ENTRY_MODE_NO	        =	upd_rec.	PAN_ENTRY_MODE_NO	,
            prf.  last_updated_date         = g_date
     where  prf.	location_no	      =	upd_rec.	location_no and
            prf.	tran_date	        =	upd_rec.	tran_date	  and
            prf.	till_no	          =	upd_rec.	till_no	    and
            prf.	tran_no	          =	upd_rec.	tran_no	    and
            prf.	tender_type_code	=	upd_rec.	tender_type_code and
            prf.	tender_seq_no	    =	upd_rec.	tender_seq_no and
            (
            nvl(prf.tran_time	                ,0) <>	upd_rec.	tran_time or
            nvl(prf.tender_type_detail_code 	,0) <>	upd_rec.	tender_type_detail_code or
            nvl(prf.tender_no	                ,0) <>	upd_rec.	tender_no or
            nvl(prf.tender_selling	          ,0) <>	upd_rec.	tender_selling or
            nvl(prf.change_tender_type	      ,0) <>	upd_rec.	change_tender_type or
            nvl(prf.change_selling	          ,0) <>	upd_rec.	change_selling or
            nvl(prf.change_gift_no	          ,0) <>	upd_rec.	change_gift_no or
            nvl(prf.card_seq_no             	,0) <>	upd_rec.	card_seq_no or
            nvl(prf.card_authorisation_id	    ,0) <>	upd_rec.	card_authorisation_id or
            nvl(prf.tender_valid_offline_ind	,0) <>	upd_rec.	tender_valid_offline_ind or
            nvl(prf.payment_acc_type	        ,0) <>	upd_rec.	payment_acc_type or
            nvl(prf.payment_acc_type_dtl_code	,0) <>	upd_rec.	payment_acc_type_dtl_code or
            nvl(prf.payment_account_no	      ,0) <>	upd_rec.	payment_account_no or
            nvl(prf.loyalty_ww_swipe_no	      ,0) <>	upd_rec.	loyalty_ww_swipe_no or
            nvl(prf.card_eps_no	              ,0) <>	upd_rec.	card_eps_no or
            nvl(prf.customer_no	              ,0) <>	upd_rec.	customer_no or
            nvl(prf.vitality_cust_ind	        ,0) <>	upd_rec.	vitality_cust_ind or
            nvl(prf.EMPLOYEE_ID              	,0) <>	upd_rec.	EMPLOYEE_ID or
            nvl(prf.COMPANY_CODE     	        ,0) <>	upd_rec.	COMPANY_CODE or
            nvl(prf.FORM_FACTOR_IND_NO       	,0) <>	upd_rec.	FORM_FACTOR_IND_NO or
            nvl(prf.PAN_ENTRY_MODE_NO         ,0) <>	upd_rec.	PAN_ENTRY_MODE_NO 
            );

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
    from   fnd_cust_basket_tender
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

--   if g_recs_read <> g_recs_inserted + g_recs_updated  then
--      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      p_success := false;
--      l_message := 'ERROR - Record counts do not balance see log file';
--      dwh_log.record_error(l_module_name,sqlcode,l_message);
--      raise_application_error (-20246,'Record count error - see log files');
--   end if;


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
end wh_prf_cust_102u;
