--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_608U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_608U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Description  WFS Statements - Store cards and Personal Loans
--  Date:        2016-11-11
--  Author:      Buntu Qwela
--  Purpose:     Update  wfs_stmt_st_crd_ploan fact table in the performance layer
--               with input ex 
--                    FND_WFS_CUST_MTH_STMT
--               for Store cards and Personal Loans
--  
--               THIS JOB RUNS DAILY 
--  Tables:      Input  - 
--                    FND_WFS_CUST_MTH_STMT
--                    
--               Output - wfs_stmt_st_crd_ploan
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2016-11-11 Buntu Qwela - created - based on WH_PRF_WFS_602U
--  2016-11-16 N Chauhan - reviewed and queries optimised.
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
g_rec_out            wfs_stmt_st_crd_ploan%rowtype;
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_608U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'WFS STATEMENTS update for Store cards and Personal Loans';
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

    l_text := 'WFS STATEMENTS (sc & pl) update STARTED AT '||
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

    merge into dwh_wfs_performance.wfs_stmt_st_crd_ploan scpl_stmt
        using ( 
            with pl_stmt as (
          select  /*+ parallel(mst, 4) */
                     wfs_customer_no                    as wfs_customer_no
                    ,product_code_no                    as PRODUCT_CODE_NO
                    ,statement_date                     as statement_date
                    ,to_number(to_char(statement_date,'yyyymm'))    as cycle_6
                    ,wfs_account_no                     as wfs_account_no    
                    ,account_status                     as account_status
                    ,sm_open_bal                        as sm_open_bal
                    ,sm_end_bal                         as sm_end_bal 
                    ,sm_credit_limit                    as sm_credit_limit
                    ,sm_recov_fee_val                   as sm_recov_fee_val
                    ,sm_coll_fee_val                    as sm_coll_fee_val
                    ,sm_purchases_val                   as sm_purchases_val 
                    ,sm_payments_val                    as sm_payments_val
                    ,sm_arrear_val                      as sm_arrear_val
                    ,curr_due_val                       as curr_due_val
                    ,sm_interest_raise                  as sm_interest_raise
                    ,delinquency_cycle                  as delinquency_cycle
                    ,tot_past_due_val                   as tot_past_due_val
                    ,sm_cbp_fee_val                     as sm_cbp_fee_val
                    ,sm_lcp_fee_val                     as sm_lcp_fee_val                                  
                    ,sm_nsf_pay_val                     as sm_nsf_pay_val 
                    ,block_code_1                       as block_code_1 
                    ,sm_returns_value                   as sm_returns_val       
                    ,behaviour_score                    as behaviour_score
                    ,bureau_score                       as bureau_score
           from    
                    dwh_wfs_foundation.fnd_wfs_cust_mth_stmt mst
          where 
                    statement_date >= to_date(to_char(sysdate,'yyyymon') || '01','yyyymondd') 
            and 
                    statement_date >= to_date(to_char(statement_date,'yyyymon') || '01','yyyymondd')   
            and 
                    statement_date <= to_date(to_char(statement_date,'yyyymon') || '20','yyyymondd')
            ),
      stmt_cnt as (
           select /*+ parallel(st, 4) */
                     distinct
                     st.wfs_account_no,
                     st.PRODUCT_CODE_NO,
                     st.cycle_6,
                     count(*) as prod_count,
                     max(st.statement_date) as max_statement_date
             from 
                     pl_stmt st
         group by   
                     st.wfs_account_no, st.PRODUCT_CODE_NO, st.cycle_6
            )
          select /* full(s) full(c) parallel(s,4) parallel(c,4) */
              s.*
            from
              pl_stmt s, stmt_cnt c
            where 
              s.wfs_account_no  = c.wfs_account_no 
              and s.PRODUCT_CODE_NO     = c.PRODUCT_CODE_NO
              and s.cycle_6      	= c.cycle_6
              and s.statement_date  = c.max_statement_date       
        ) new_scpl_stmt
        on   
        (
            scpl_stmt.wfs_account_no  =  new_scpl_stmt.wfs_account_no
            and scpl_stmt.cycle_6      =  new_scpl_stmt.cycle_6
            and scpl_stmt.PRODUCT_CODE_NO = new_scpl_stmt.PRODUCT_CODE_NO
        )
        when matched then 
            update 
                set  
                     scpl_stmt.wfs_customer_no    =  new_scpl_stmt.wfs_customer_no                            
                    ,scpl_stmt.account_status     =  new_scpl_stmt.account_status           
                    ,scpl_stmt.sm_open_bal        =  new_scpl_stmt.sm_open_bal      
                    ,scpl_stmt.sm_end_bal         =  new_scpl_stmt.sm_end_bal       
                    ,scpl_stmt.sm_credit_limit    =  new_scpl_stmt.sm_credit_limit     
                    ,scpl_stmt.sm_recov_fee_val   =  new_scpl_stmt.sm_recov_fee_val    
                    ,scpl_stmt.sm_coll_fee_val    =  new_scpl_stmt.sm_coll_fee_val     
                    ,scpl_stmt.sm_purchases_val   =  new_scpl_stmt.sm_purchases_val 
                    ,scpl_stmt.sm_payments_val    =  new_scpl_stmt.sm_payments_val  
                    ,scpl_stmt.sm_arrear_val      =  new_scpl_stmt.sm_arrear_val    
                    ,scpl_stmt.curr_due_val       =  new_scpl_stmt.curr_due_val     
                    ,scpl_stmt.sm_interest_raise  =  new_scpl_stmt.sm_interest_raise
                    ,scpl_stmt.delinquency_cycle  =  new_scpl_stmt.delinquency_cycle
                    ,scpl_stmt.tot_past_due_val   =  new_scpl_stmt.tot_past_due_val 
                    ,scpl_stmt.sm_cbp_fee_val     =  new_scpl_stmt.sm_cbp_fee_val   
                    ,scpl_stmt.sm_lcp_fee_val     =  new_scpl_stmt.sm_lcp_fee_val   
                    ,scpl_stmt.sm_nsf_pay_val     =  new_scpl_stmt.sm_nsf_pay_val   
                    ,scpl_stmt.block_code_1       =  new_scpl_stmt.block_code_1     
                    ,scpl_stmt.sm_returns_val     =  new_scpl_stmt.sm_returns_val   
                    ,scpl_stmt.behaviour_score    =  new_scpl_stmt.behaviour_score  
                    ,scpl_stmt.bureau_score =  new_scpl_stmt.bureau_score
                    ,scpl_stmt.last_updated_date    =  trunc(sysdate)   
        when not matched then 
            insert ( 
                 scpl_stmt.wfs_customer_no      
                ,scpl_stmt.PRODUCT_CODE_NO      
                ,scpl_stmt.statement_date   
                ,scpl_stmt.cycle_6           
                ,scpl_stmt.wfs_account_no   
                ,scpl_stmt.account_status           
                ,scpl_stmt.sm_open_bal      
                ,scpl_stmt.sm_end_bal       
                ,scpl_stmt.sm_credit_limit     
                ,scpl_stmt.sm_recov_fee_val  
                ,scpl_stmt.sm_coll_fee_val     
                ,scpl_stmt.sm_purchases_val 
                ,scpl_stmt.sm_payments_val  
                ,scpl_stmt.sm_arrear_val    
                ,scpl_stmt.curr_due_val     
                ,scpl_stmt.sm_interest_raise
                ,scpl_stmt.delinquency_cycle
                ,scpl_stmt.tot_past_due_val 
                ,scpl_stmt.sm_cbp_fee_val   
                ,scpl_stmt.sm_lcp_fee_val   
                ,scpl_stmt.sm_nsf_pay_val   
                ,scpl_stmt.block_code_1     
                ,scpl_stmt.sm_returns_val   
                ,scpl_stmt.behaviour_score  
                ,scpl_stmt.bureau_score
                ,scpl_stmt.last_updated_date
            )
            values (
                 new_scpl_stmt.wfs_customer_no      
                ,new_scpl_stmt.PRODUCT_CODE_NO      
                ,new_scpl_stmt.statement_date   
                ,new_scpl_stmt.cycle_6           
                ,new_scpl_stmt.wfs_account_no   
                ,new_scpl_stmt.account_status
                ,new_scpl_stmt.sm_open_bal      
                ,new_scpl_stmt.sm_end_bal       
                ,new_scpl_stmt.sm_credit_limit     
                ,new_scpl_stmt.sm_recov_fee_val    
                ,new_scpl_stmt.sm_coll_fee_val      
                ,new_scpl_stmt.sm_purchases_val 
                ,new_scpl_stmt.sm_payments_val  
                ,new_scpl_stmt.sm_arrear_val    
                ,new_scpl_stmt.curr_due_val     
                ,new_scpl_stmt.sm_interest_raise
                ,new_scpl_stmt.delinquency_cycle
                ,new_scpl_stmt.tot_past_due_val 
                ,new_scpl_stmt.sm_cbp_fee_val   
                ,new_scpl_stmt.sm_lcp_fee_val   
                ,new_scpl_stmt.sm_nsf_pay_val   
                ,new_scpl_stmt.block_code_1     
                ,new_scpl_stmt.sm_returns_val   
                ,new_scpl_stmt.behaviour_score  
                ,new_scpl_stmt.bureau_score
                ,trunc(sysdate)
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

end wh_prf_wfs_608u;
