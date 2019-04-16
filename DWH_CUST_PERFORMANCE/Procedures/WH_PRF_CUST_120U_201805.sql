--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_120U_201805
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_120U_201805" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_fv_voucher fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_fv_voucher
--               Output - cust_fv_voucher
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_120U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD FLEXIBLE VOUCHER EX FND';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_fnd_fv_voucher is
select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*
      from    fnd_fv_voucher fnd,
              cust_fv_voucher prf
      where   fnd.voucher_no        = prf.voucher_no and
              fnd.last_updated_date = g_date
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.voucher_no;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

       insert /*+ APPEND parallel (prf,4) */ into cust_fv_voucher prf
       select /*+ FULL(fnd)  parallel (fnd,4) parallel (crd,4)*/
             	fnd.*,
              (case
                when fnd.primary_account_no is null then 
                     900
                when crd.product_no is null then 
                     fnd.primary_account_no
                when crd.customer_no = 0 then 
                     fnd.primary_account_no     
                else 
                     crd.customer_no
                end ) primary_customer_identifier 
        from  fnd_fv_voucher fnd,
              fnd_customer_product crd
       where  fnd.last_updated_date  = g_date    and
              nvl(fnd.primary_account_no,0) = crd.product_no(+) and
       not exists
      (select /*+ nl_aj */ * from cust_fv_voucher
       where  voucher_no     = fnd.voucher_no
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



FOR upd_rec IN c_fnd_fv_voucher
   loop
     update cust_fv_voucher prf
     set    prf.voucher_no	=	upd_rec.	voucher_no	,
            prf.campaign_no	=	upd_rec.	campaign_no	,
            prf.promotion_no	=	upd_rec.	promotion_no	,
            prf.voucher_type_no	=	upd_rec.	voucher_type_no	,
            prf.voucher_status_no	=	upd_rec.	voucher_status_no	,
            prf.voucher_status_description	=	upd_rec.	voucher_status_description	,
            prf.primary_account_no	=	upd_rec.	primary_account_no	,
            prf.location_no	=	upd_rec.	location_no	,
            prf.create_date	=	upd_rec.	create_date	,
            prf.start_date	=	upd_rec.	start_date	,
            prf.expiry_date	=	upd_rec.	expiry_date	,
            prf.delivery_method_no	=	upd_rec.	delivery_method_no	,
            prf.delivery_method_description	=	upd_rec.	delivery_method_description	,
            prf.previous_voucher_no	=	upd_rec.	previous_voucher_no	,
            prf.redeemed_primary_account_no	=	upd_rec.	redeemed_primary_account_no	,
            prf.redeemed_date	=	upd_rec.	redeemed_date	,
            prf.redeemed_tender_swipe_no	=	upd_rec.	redeemed_tender_swipe_no	,
            prf.redeemed_tender_seq_no	=	upd_rec.	redeemed_tender_seq_no	,
            prf.redeemed_loyalty_swipe_no	=	upd_rec.	redeemed_loyalty_swipe_no	,
            prf.redeemed_store	=	upd_rec.	redeemed_store	,
            prf.redeemed_till_no	=	upd_rec.	redeemed_till_no	,
            prf.redeemed_operator_no	=	upd_rec.	redeemed_operator_no	,
            prf.redeemed_tran_no	=	upd_rec.	redeemed_tran_no	,
            prf.redeemed_amount	=	upd_rec.	redeemed_amount	,
            prf.post_card_offline_ind	=	upd_rec.	post_card_offline_ind	,
            prf.change_user_id	=	upd_rec.	change_user_id	,
            prf.change_date	=	upd_rec.	change_date	,
            prf.voucher_amount	=	upd_rec.	voucher_amount	,
            prf.  last_updated_date = g_date
     where  prf.	voucher_no	      =	upd_rec.	voucher_no and
            (
             nvl(prf.CAMPAIGN_NO	,0) <>	upd_rec.	CAMPAIGN_NO	or
             nvl(prf.PROMOTION_NO	,0) <>	upd_rec.	PROMOTION_NO	or
             nvl(prf.VOUCHER_TYPE_NO	,0) <>	upd_rec.	VOUCHER_TYPE_NO	or
             nvl(prf.VOUCHER_STATUS_NO	,0) <>	upd_rec.	VOUCHER_STATUS_NO	or
             nvl(prf.VOUCHER_STATUS_DESCRIPTION	,0) <>	upd_rec.	VOUCHER_STATUS_DESCRIPTION	or
             nvl(prf.PRIMARY_ACCOUNT_NO	,0) <>	upd_rec.	PRIMARY_ACCOUNT_NO	or
             nvl(prf.LOCATION_NO	,0) <>	upd_rec.	LOCATION_NO	or
             nvl(prf.CREATE_DATE	,'1 Jan 1900') <>	upd_rec.	CREATE_DATE	or
             nvl(prf.START_DATE	,'1 Jan 1900') <>	upd_rec.	START_DATE	or
             nvl(prf.EXPIRY_DATE	,'1 Jan 1900') <>	upd_rec.	EXPIRY_DATE	or
             nvl(prf.DELIVERY_METHOD_NO	,0) <>	upd_rec.	DELIVERY_METHOD_NO	or
             nvl(prf.DELIVERY_METHOD_DESCRIPTION	,0) <>	upd_rec.	DELIVERY_METHOD_DESCRIPTION	or
             nvl(prf.PREVIOUS_VOUCHER_NO	,0) <>	upd_rec.	PREVIOUS_VOUCHER_NO	or
             nvl(prf.REDEEMED_PRIMARY_ACCOUNT_NO	,0) <>	upd_rec.	REDEEMED_PRIMARY_ACCOUNT_NO	or
             nvl(prf.REDEEMED_DATE	,'1 Jan 1900') <>	upd_rec.	REDEEMED_DATE	or
             nvl(prf.REDEEMED_TENDER_SWIPE_NO	,0) <>	upd_rec.	REDEEMED_TENDER_SWIPE_NO	or
             nvl(prf.REDEEMED_TENDER_SEQ_NO	,0) <>	upd_rec.	REDEEMED_TENDER_SEQ_NO	or
             nvl(prf.REDEEMED_LOYALTY_SWIPE_NO	,0) <>	upd_rec.	REDEEMED_LOYALTY_SWIPE_NO	or
             nvl(prf.REDEEMED_STORE	,0) <>	upd_rec.	REDEEMED_STORE	or
             nvl(prf.REDEEMED_TILL_NO	,0) <>	upd_rec.	REDEEMED_TILL_NO	or
             nvl(prf.REDEEMED_OPERATOR_NO	,0) <>	upd_rec.	REDEEMED_OPERATOR_NO	or
             nvl(prf.REDEEMED_TRAN_NO	,0) <>	upd_rec.	REDEEMED_TRAN_NO	or
             nvl(prf.REDEEMED_AMOUNT	,0) <>	upd_rec.	REDEEMED_AMOUNT	or
             nvl(prf.POST_CARD_OFFLINE_IND	,0) <>	upd_rec.	POST_CARD_OFFLINE_IND	or
             nvl(prf.change_user_id	,0) <>	upd_rec.	change_user_id	or
             nvl(prf.CHANGE_DATE	,'1 Jan 1900') <>	upd_rec.	CHANGE_DATE	or
             nvl(prf.VOUCHER_AMOUNT	,0) <>	upd_rec.	VOUCHER_AMOUNT
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
    from   fnd_fv_voucher
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
end wh_prf_cust_120u_201805;
