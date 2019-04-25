--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_610U_20171108
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_610U_20171108" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Description  WFS Statements - Credit Cards
--  Date:        2016-11-11
--  Author:      Buntu Qwela
--  Purpose:     Update  WFS_STMT_CR_CRD fact table in the performance layer
--               with input ex 
--                    FND_WFS_CRD_ACC_DLY
--                    APEX_WFS_STMT_ABSA_BILL_CYC

--               for Credit Cards
--  
--               THIS JOB RUNS DAILY 
--  Tables:      Input  - 
--                    FND_WFS_CRD_ACC_DLY
--                    APEX_WFS_STMT_ABSA_BILL_CYC
--                    
--               Output - WFS_STMT_CR_CRD
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2016-11-11 Buntu Qwela - created - based on WH_PRF_WFS_604U
--  2016-11-17 N Chauhan - reviewed and queries optimised.
--  2017-05-19 N Chauhan - Hints added for performance.
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            WFS_STMT_CR_CRD%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);

g_start_week         number;
g_end_week           number;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_stmt               varchar2(300);
g_yr_00              number;
g_qt_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_610U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS STATEMENTS update for Credit Cards';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'WFS STATEMENTS (cr_crd) update STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
-- Main loop
--**************************************************************************************************

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
  execute immediate 'alter session enable parallel dml';

      merge /*+ append parallel(visa_stmt,4) */
      into dwh_wfs_performance.wfs_stmt_cr_crd visa_stmt using (
       select 
                    t2.last_billing_date 				as statement_date
                   ,t1.information_date 				as information_date 
                   ,t2.cal_year_month_no 				as cycle6                        
                   ,t1.account_number  					as account_number                           
                   ,t1.card_account_status_code			as card_account_status_code 
                   ,t1.application_credit_score			as application_credit_score 
                   ,t1.behaviour_score 					as behaviour_score
                   ,t1.budget_limit_amount 				as budget_limit_amount	
                   ,t1.card_account_type_code			as card_account_type_code 
                   ,t1.card_closed_reason_code			as card_closed_reason_code 
                   ,t1.card_cycle_code					as card_cycle_code 
                   ,t1.card_production_brand			as card_production_brand 
                   ,t1.closed_date						as closed_date 
                   ,t1.card_collector_code				as card_collector_code 
                   ,t1.credit_bureau_score				as credit_bureau_score 
                   ,t1.customer_key						as customer_key                               
                   ,t1.day_5_delinquent_amt				as day_5_delinquent_amt 
                   ,t1.day_30_delinquent_amt			as day_30_delinquent_amt 
                   ,t1.day_60_delinquent_amt			as day_60_delinquent_amt 
                   ,t1.day_90_delinquent_amt			as day_90_delinquent_amt 
                   ,t1.day_120_delinquent_amt			as day_120_delinquent_amt 
                   ,t1.day_150_delinquent_amt			as day_150_delinquent_amt 
                   ,t1.day_180_delinquent_amt			as day_180_delinquent_amt 
                   ,t1.day_210_delinquent_amt			as day_210_delinquent_amt 
                   ,t1.delinquent_amount				as delinquent_amount 
                   ,t1.delinquent_cycles_count			as delinquent_cycles_count 
                   ,t1.delinquent_date 					as delinquent_date 
                   ,t1.delinquent_ind					as delinquent_ind	 
                   ,t1.manual_credit_line				as manual_credit_line 
                   ,t1.maturity_date 					as maturity_date
                   ,t1.ocs_processing_ind				as ocs_processing_ind 
                   ,t1.ocs_reason_code					as ocs_reason_code 
                   ,t1.open_date 						as open_date 
                   ,t1.prev_card_account_status_code	as prev_card_account_status_code 
                   ,t1.purchase_limit_amt 				as purchase_limit_amt
                   ,t1.site_code_card_div				as site_code_card_div 
                   ,t1.status_date 						as status_date 
                   ,t1.system_credit_line				as system_credit_line 
                   ,t1.total_budget_balance_amt			as total_budget_balance_amt 
                   ,t1.transfer_account_number			as transfer_account_number 
                   ,t1.random_number_digit				as random_number_digit 
                   ,t1.triad_sub_product_code			as triad_sub_product_code 
                   ,t1.last_client_dt_txn_date 			as last_client_dt_txn_date 
                   ,t1.last_client_dt_txn_amt			as last_client_dt_txn_amt 
                   ,t1.last_client_dt_txt_category		as last_client_dt_txt_category 
                   ,t1.last_transaction_code			as last_transaction_code 
                   ,t1.last_monetary_transaction_date 	as last_monetary_transaction_date 
                   ,t1.last_monetary_transaction_amt	as last_monetary_transaction_amt 
                   ,t1.transfer_to_legal_orig_date 		as transfer_to_legal_orig_date 
                   ,t1.last_client_ct_pmt_date 			as last_client_ct_pmt_date 
                   ,t1.last_client_ct_pmt_txn_code		as last_client_ct_pmt_txn_code 
                   ,t1.last_client_ct_pmt_amt			as last_client_ct_pmt_amt 
                   ,t1.card_acct_status_code			as card_acct_status_code 
                   ,t1.account_balance 					as account_balance
                   ,t1.derived_card_acct_status_code	as derived_card_acct_status_code 
                   ,t1.sub_product_code					as sub_product_code 
                   ,trunc(sysdate) 						as last_updated_date
    		from 
    				dwh_wfs_foundation.fnd_wfs_crd_acc_dly t1,(
    				select 
    						bc.* 
    				from 
    						apex_app_wfs_01.apex_wfs_stmt_absa_bill_cyc bc
    				where 
    						bc.cal_year_month_no = to_number(to_char(sysdate,'yyyymm'))
    			) t2               
    	where   
    				t1.information_date = t2.last_billing_date
    		) new_stmt 
    		
    	on (
    		new_stmt.cycle6 = visa_stmt.cycle_6
    		and new_stmt.account_number = visa_stmt.account_number
    		and new_stmt.customer_key = visa_stmt.customer_key
    		)
    when matched then update 
    set
                visa_stmt.statement_date	               	= new_stmt.statement_date
              , visa_stmt.information_date	       			= new_stmt.information_date
              , visa_stmt.card_account_status_code			= new_stmt.card_account_status_code
              , visa_stmt.application_credit_score			= new_stmt.application_credit_score
              , visa_stmt.behaviour_score	               	= new_stmt.behaviour_score
              , visa_stmt.budget_limit_amount	       		= new_stmt.budget_limit_amount
              , visa_stmt.card_account_type_code	       	= new_stmt.card_account_type_code
              , visa_stmt.card_closed_reason_code	       	= new_stmt.card_closed_reason_code
              , visa_stmt.card_cycle_code	               	= new_stmt.card_cycle_code
              , visa_stmt.card_production_brand	       		= new_stmt.card_production_brand
              , visa_stmt.closed_date	               		= new_stmt.closed_date
              , visa_stmt.card_collector_code	    		= new_stmt.card_collector_code
              , visa_stmt.credit_bureau_score	       		= new_stmt.credit_bureau_score
              , visa_stmt.day_5_delinquent_amt	       		= new_stmt.day_5_delinquent_amt
              , visa_stmt.day_30_delinquent_amt	       		= new_stmt.day_30_delinquent_amt
              , visa_stmt.day_60_delinquent_amt	       		= new_stmt.day_60_delinquent_amt
              , visa_stmt.day_90_delinquent_amt	       		= new_stmt.day_90_delinquent_amt
              , visa_stmt.day_120_delinquent_amt	       	= new_stmt.day_120_delinquent_amt
              , visa_stmt.day_150_delinquent_amt	       	= new_stmt.day_150_delinquent_amt
              , visa_stmt.day_180_delinquent_amt	       	= new_stmt.day_180_delinquent_amt
              , visa_stmt.day_210_delinquent_amt	       	= new_stmt.day_210_delinquent_amt
              , visa_stmt.delinquent_amount	       			= new_stmt.delinquent_amount
              , visa_stmt.delinquent_cycles_count	       	= new_stmt.delinquent_cycles_count
              , visa_stmt.delinquent_date	               	= new_stmt.delinquent_date
              , visa_stmt.delinquent_ind	               	= new_stmt.delinquent_ind
              , visa_stmt.manual_credit_line	       		= new_stmt.manual_credit_line
              , visa_stmt.maturity_date	               		= new_stmt.maturity_date
              , visa_stmt.ocs_processing_ind	       		= new_stmt.ocs_processing_ind
              , visa_stmt.ocs_reason_code	               	= new_stmt.ocs_reason_code
              , visa_stmt.open_date	              	 		= new_stmt.open_date
              , visa_stmt.prev_card_account_status_code		= new_stmt.prev_card_account_status_code
              , visa_stmt.purchase_limit_amt	       		= new_stmt.purchase_limit_amt
              , visa_stmt.site_code_card_div	       		= new_stmt.site_code_card_div
              , visa_stmt.status_date	               		= new_stmt.status_date
              , visa_stmt.system_credit_line	       		= new_stmt.system_credit_line
              , visa_stmt.total_budget_balance_amt			= new_stmt.total_budget_balance_amt
              , visa_stmt.transfer_account_number	       	= new_stmt.transfer_account_number
              , visa_stmt.random_number_digit	       		= new_stmt.random_number_digit
              , visa_stmt.triad_sub_product_code	       	= new_stmt.triad_sub_product_code
              , visa_stmt.last_client_dt_txn_date	       	= new_stmt.last_client_dt_txn_date
              , visa_stmt.last_client_dt_txn_amt	       	= new_stmt.last_client_dt_txn_amt
              , visa_stmt.last_client_dt_txt_category		= new_stmt.last_client_dt_txt_category
              , visa_stmt.last_transaction_code	       		= new_stmt.last_transaction_code
              , visa_stmt.last_monetary_transaction_date	= new_stmt.last_monetary_transaction_date
              , visa_stmt.last_monetary_transaction_amt		= new_stmt.last_monetary_transaction_amt
              , visa_stmt.transfer_to_legal_orig_date		= new_stmt.transfer_to_legal_orig_date
              , visa_stmt.last_client_ct_pmt_date	       	= new_stmt.last_client_ct_pmt_date
              , visa_stmt.last_client_ct_pmt_txn_code		= new_stmt.last_client_ct_pmt_txn_code
              , visa_stmt.last_client_ct_pmt_amt	       	= new_stmt.last_client_ct_pmt_amt
              , visa_stmt.card_acct_status_code	       		= new_stmt.card_acct_status_code
              , visa_stmt.account_balance	               	= new_stmt.account_balance
              , visa_stmt.derived_card_acct_status_code		= new_stmt.derived_card_acct_status_code
              , visa_stmt.sub_product_code	       			= new_stmt.sub_product_code
              , visa_stmt.last_updated_date	       			= new_stmt.last_updated_date
    
    when not matched then insert (
               visa_stmt.statement_date
             , visa_stmt.information_date
             , visa_stmt.cycle_6
             , visa_stmt.account_number
             , visa_stmt.customer_key
             , visa_stmt.card_account_status_code
             , visa_stmt.application_credit_score
             , visa_stmt.behaviour_score
             , visa_stmt.budget_limit_amount
             , visa_stmt.card_account_type_code
             , visa_stmt.card_closed_reason_code
             , visa_stmt.card_cycle_code
             , visa_stmt.card_production_brand
             , visa_stmt.closed_date
             , visa_stmt.card_collector_code
             , visa_stmt.credit_bureau_score
             , visa_stmt.day_5_delinquent_amt
             , visa_stmt.day_30_delinquent_amt
             , visa_stmt.day_60_delinquent_amt
             , visa_stmt.day_90_delinquent_amt
             , visa_stmt.day_120_delinquent_amt
             , visa_stmt.day_150_delinquent_amt
             , visa_stmt.day_180_delinquent_amt
             , visa_stmt.day_210_delinquent_amt
             , visa_stmt.delinquent_amount
             , visa_stmt.delinquent_cycles_count
             , visa_stmt.delinquent_date
             , visa_stmt.delinquent_ind
             , visa_stmt.manual_credit_line
             , visa_stmt.maturity_date
             , visa_stmt.ocs_processing_ind
             , visa_stmt.ocs_reason_code
             , visa_stmt.open_date
             , visa_stmt.prev_card_account_status_code
             , visa_stmt.purchase_limit_amt
             , visa_stmt.site_code_card_div
             , visa_stmt.status_date
             , visa_stmt.system_credit_line
             , visa_stmt.total_budget_balance_amt
             , visa_stmt.transfer_account_number
             , visa_stmt.random_number_digit
             , visa_stmt.triad_sub_product_code
             , visa_stmt.last_client_dt_txn_date
             , visa_stmt.last_client_dt_txn_amt
             , visa_stmt.last_client_dt_txt_category
             , visa_stmt.last_transaction_code
             , visa_stmt.last_monetary_transaction_date
             , visa_stmt.last_monetary_transaction_amt
             , visa_stmt.transfer_to_legal_orig_date
             , visa_stmt.last_client_ct_pmt_date
             , visa_stmt.last_client_ct_pmt_txn_code
             , visa_stmt.last_client_ct_pmt_amt
             , visa_stmt.card_acct_status_code
             , visa_stmt.account_balance
             , visa_stmt.derived_card_acct_status_code
             , visa_stmt.sub_product_code
             , visa_stmt.last_updated_date
      ) 
    
    values (
               new_stmt.statement_date
             , new_stmt.information_date
             , new_stmt.cycle6
             , new_stmt.account_number
             , new_stmt.customer_key
             , new_stmt.card_account_status_code
             , new_stmt.application_credit_score
             , new_stmt.behaviour_score
             , new_stmt.budget_limit_amount
             , new_stmt.card_account_type_code
             , new_stmt.card_closed_reason_code
             , new_stmt.card_cycle_code
             , new_stmt.card_production_brand
             , new_stmt.closed_date
             , new_stmt.card_collector_code
             , new_stmt.credit_bureau_score
             , new_stmt.day_5_delinquent_amt
             , new_stmt.day_30_delinquent_amt
             , new_stmt.day_60_delinquent_amt
             , new_stmt.day_90_delinquent_amt
             , new_stmt.day_120_delinquent_amt
             , new_stmt.day_150_delinquent_amt
             , new_stmt.day_180_delinquent_amt
             , new_stmt.day_210_delinquent_amt
             , new_stmt.delinquent_amount
             , new_stmt.delinquent_cycles_count
             , new_stmt.delinquent_date
             , new_stmt.delinquent_ind
             , new_stmt.manual_credit_line
             , new_stmt.maturity_date
             , new_stmt.ocs_processing_ind
             , new_stmt.ocs_reason_code
             , new_stmt.open_date
             , new_stmt.prev_card_account_status_code
             , new_stmt.purchase_limit_amt
             , new_stmt.site_code_card_div
             , new_stmt.status_date
             , new_stmt.system_credit_line
             , new_stmt.total_budget_balance_amt
             , new_stmt.transfer_account_number
             , new_stmt.random_number_digit
             , new_stmt.triad_sub_product_code
             , new_stmt.last_client_dt_txn_date
             , new_stmt.last_client_dt_txn_amt
             , new_stmt.last_client_dt_txt_category
             , new_stmt.last_transaction_code
             , new_stmt.last_monetary_transaction_date
             , new_stmt.last_monetary_transaction_amt
             , new_stmt.transfer_to_legal_orig_date
             , new_stmt.last_client_ct_pmt_date
             , new_stmt.last_client_ct_pmt_txn_code
             , new_stmt.last_client_ct_pmt_amt
             , new_stmt.card_acct_status_code
             , new_stmt.account_balance
             , new_stmt.derived_card_acct_status_code
             , new_stmt.sub_product_code
             , new_stmt.last_updated_date
    );
 
  

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


  commit;


  g_recs_deleted     :=  g_recs_deleted + SQL%ROWCOUNT;
    
  commit;
    

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
--    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    l_text :=  'RECORDS MERGED '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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



END WH_PRF_WFS_610U_20171108;
