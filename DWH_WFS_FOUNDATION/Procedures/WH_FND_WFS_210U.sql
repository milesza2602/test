--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_210U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_210U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_mth_stmt fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_cust_mth_stmt_cpy
--               Output - fnd_wfs_cust_mth_stmt
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 Mar 2013 - Change to a BULK Insert/update load to speed up 10x
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


g_wfs_customer_no    stg_vsn_cust_mth_stmt_cpy.wfs_customer_no%type; 
g_wfs_account_no     stg_vsn_cust_mth_stmt_cpy.wfs_account_no%type; 
g_product_code_no    stg_vsn_cust_mth_stmt_cpy.product_code_no%TYPE; 
g_statement_date     stg_vsn_cust_mth_stmt_cpy.statement_date%type; 

   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_210U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_CUST_STMT_MONTH EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_cust_mth_stmt_cpy
where (wfs_customer_no,
wfs_account_no,
product_code_no,
statement_date)
in
(select wfs_customer_no,
wfs_account_no,
product_code_no,
statement_date
from stg_vsn_cust_mth_stmt_cpy 
group by wfs_customer_no,
wfs_account_no,
product_code_no,
statement_date
having count(*) > 1) 
order by wfs_customer_no,
wfs_account_no,
product_code_no,
statement_date,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_cust_mth_stmt is
select /*+ FULL(cpy)  parallel (4) */  
              cpy.*
      from    stg_vsn_cust_mth_stmt_cpy cpy,
              fnd_wfs_cust_mth_stmt fnd
      where   cpy.wfs_customer_no       = fnd.wfs_customer_no and    
              cpy.wfs_account_no        = fnd.wfs_account_no and    
              cpy.product_code_no       = fnd.product_code_no    and   
              cpy.statement_date           = fnd.statement_date    and 
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.wfs_customer_no,
              cpy.wfs_account_no,
              cpy.product_code_no,
              cpy.statement_date,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no    := 0; 
   g_wfs_account_no     := 0;
   g_product_code_no    := 0;
   g_statement_date     := '1 Jan 2000';


for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no   = g_wfs_customer_no and
        dupp_record.wfs_account_no    = g_wfs_account_no and
        dupp_record.product_code_no   = g_product_code_no and
        dupp_record.statement_date    = g_statement_date then
        update stg_vsn_cust_mth_stmt_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_wfs_customer_no    := dupp_record.wfs_customer_no; 
    g_wfs_account_no     := dupp_record.wfs_account_no; 
    g_product_code_no    := dupp_record.product_code_no;
    g_statement_date        := dupp_record.statement_date;

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

      insert /*+ APPEND parallel (pcd,2) */ into fnd_wfs_product pcd 
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.	product_code_no	,
             'Dummy wh_fnd_wfs_210U',
             0	,
             ' ',
             g_date,
             1
      from   stg_vsn_cust_mth_stmt_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_wfs_product
       where 	product_code_no     = cpy.product_code_no )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
       commit;

--******************************************************************************

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_product fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.wfs_customer_no	,
             0,
             1,
             g_date,
             1
      from   stg_vsn_cust_mth_stmt_cpy cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_customer_product 
       where  product_no              = cpy.wfs_customer_no )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
       commit;
       
--******************************************************************************

      insert /*+ APPEND parallel (fnd,2) */ into fnd_customer_product fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             distinct
             cpy.wfs_account_no	,
             0,
             1,
             g_date,
             1
      from   stg_vsn_cust_mth_stmt_cpy  cpy
 
       where not exists 
      (select /*+ nl_aj */ * from fnd_customer_product 
       where  product_no              = cpy.wfs_account_no )
       and    sys_process_code    = 'N';
       
       g_recs_dummy := g_recs_dummy + sql%rowcount;
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_cust_mth_stmt fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             	cpy.	wfs_customer_no	,
            	cpy.	product_code_no	,
            	cpy.	statement_date	,
            	cpy.	statement_month	,
            	cpy.	statement_year	,
            	cpy.	wfs_account_no	,
            	cpy.	sm_cycle_due	,
            	cpy.	sm_attr_score1	,
            	cpy.	sm_credit_limit	,
            	cpy.	sm_open_bal	,
            	cpy.	sm_end_bal	,
            	cpy.	sm_total_due_val	,
            	cpy.	sm_returns_qty	,
            	cpy.	sm_returns_value	,
            	cpy.	sm_recov_fee_val	,
            	cpy.	sm_coll_fee_val	,
            	cpy.	sm_purchases_qty	,
            	cpy.	sm_purchases_val	,
            	cpy.	sm_payments_qty	,
            	cpy.	sm_payments_val	,
            	cpy.	sm_arrear_val	,
            	cpy.	sm_nsf_pay_qty	,
            	cpy.	sm_nsf_pay_val	,
            	cpy.	sm_lcp_fee_val	,
            	cpy.	sm_cbp_fee_val	,
            	cpy.	sm_interest_raise	,
            	cpy.	block_code_1	,
            	cpy.	delinquency_cycle	,
            	cpy.	account_status	,
            	cpy.	coll_bnp	,
            	cpy.	coll_bnp_chgoff	,
            	cpy.	agency_debit	,
            	cpy.	agency_credit	,
            	cpy.	block_code_2	,
            	cpy.	billing_cycle	,
            	cpy.	stmt_flag	,
            	cpy.	dispu_items_no	,
            	cpy.	dispu_items_val	,
            	cpy.	recency_pmt	,
            	cpy.	fixed_pmt_amt	,
            	cpy.	curr_due_val	,
            	cpy.	tot_past_due_val	,
            	cpy.	last_act_date	,
            	cpy.	delq_cnt_curr	,
            	cpy.	delq_cnt_30	,
            	cpy.	delq_cnt_60	,
            	cpy.	delq_cnt_90	,
            	cpy.	delq_cnt_120	,
            	cpy.	delq_cnt_150	,
            	cpy.	delq_cnt_180	,
            	cpy.	delq_cnt_210	,
            	cpy.	purch_food_amt	,
            	cpy.	purch_txt_amt	,
            	cpy.	purch_food_no	,
            	cpy.	purch_txt_no	,
            	cpy.	return_food_amt	,
            	cpy.	return_txt_amt	,
            	cpy.	return_food_no	,
            	cpy.	return_txt_no	,
            	cpy.	reversal_amt	,
            	cpy.	reversal_no	,
            	cpy.	behaviour_score	,
            	cpy.	bureau_score	,
            	cpy.	viking_code	,
            	cpy.	viking_amt	,
            	cpy.	stmt_ind	,
             g_date as last_updated_date
       from  stg_vsn_cust_mth_stmt_cpy cpy
       where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_cust_mth_stmt 
       where  wfs_customer_no    = cpy.wfs_customer_no and
              wfs_account_no     = cpy.wfs_account_no and
              product_code_no    = cpy.product_code_no and
              statement_date        = cpy.statement_date)
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



FOR upd_rec IN c_stg_vsn_cust_mth_stmt
   loop
     UPDATE fnd_wfs_cust_mth_stmt fnd 
     SET    fnd.	statement_month	  =	upd_rec.	statement_month	,
            fnd.	statement_year	  =	upd_rec.	statement_year	,
            fnd.	sm_cycle_due	    =	upd_rec.	sm_cycle_due	,
            fnd.	sm_attr_score1  	=	upd_rec.	sm_attr_score1	,
            fnd.	sm_credit_limit	  =	upd_rec.	sm_credit_limit	,
            fnd.	sm_open_bal	      =	upd_rec.	sm_open_bal	,
            fnd.	sm_end_bal	      =	upd_rec.	sm_end_bal	,
            fnd.	sm_total_due_val	=	upd_rec.	sm_total_due_val	,
            fnd.	sm_returns_qty	  =	upd_rec.	sm_returns_qty	,
            fnd.	sm_returns_value	=	upd_rec.	sm_returns_value	,
            fnd.	sm_recov_fee_val	=	upd_rec.	sm_recov_fee_val	,
            fnd.	sm_coll_fee_val	  =	upd_rec.	sm_coll_fee_val	,
            fnd.	sm_purchases_qty	=	upd_rec.	sm_purchases_qty	,
            fnd.	sm_purchases_val	=	upd_rec.	sm_purchases_val	,
            fnd.	sm_payments_qty	  =	upd_rec.	sm_payments_qty	,
            fnd.	sm_payments_val	  =	upd_rec.	sm_payments_val	,
            fnd.	sm_arrear_val	    =	upd_rec.	sm_arrear_val	,
            fnd.	sm_nsf_pay_qty	  =	upd_rec.	sm_nsf_pay_qty	,
            fnd.	sm_nsf_pay_val	  =	upd_rec.	sm_nsf_pay_val	,
            fnd.	sm_lcp_fee_val	  =	upd_rec.	sm_lcp_fee_val	,
            fnd.	sm_cbp_fee_val	  =	upd_rec.	sm_cbp_fee_val	,
            fnd.	sm_interest_raise	=	upd_rec.	sm_interest_raise	,
            fnd.	block_code_1	    =	upd_rec.	block_code_1	,
            fnd.	delinquency_cycle	=	upd_rec.	delinquency_cycle	,
            fnd.	account_status	  =	upd_rec.	account_status	,
            fnd.	coll_bnp	        =	upd_rec.	coll_bnp	,
            fnd.	coll_bnp_chgoff   =	upd_rec.	coll_bnp_chgoff	,
            fnd.	agency_debit	    =	upd_rec.	agency_debit	,
            fnd.	agency_credit	    =	upd_rec.	agency_credit	,
            fnd.	block_code_2	    =	upd_rec.	block_code_2	,
            fnd.	billing_cycle   	=	upd_rec.	billing_cycle	,
            fnd.	stmt_flag	        =	upd_rec.	stmt_flag	,
            fnd.	dispu_items_no	  =	upd_rec.	dispu_items_no	,
            fnd.	dispu_items_val 	=	upd_rec.	dispu_items_val	,
            fnd.	recency_pmt	      =	upd_rec.	recency_pmt	,
            fnd.	fixed_pmt_amt   	=	upd_rec.	fixed_pmt_amt	,
            fnd.	curr_due_val	    =	upd_rec.	curr_due_val	,
            fnd.	tot_past_due_val	=	upd_rec.	tot_past_due_val	,
            fnd.	last_act_date	    =	upd_rec.	last_act_date	,
            fnd.	delq_cnt_curr   	=	upd_rec.	delq_cnt_curr	,
            fnd.	delq_cnt_30	      =	upd_rec.	delq_cnt_30	,
            fnd.	delq_cnt_60	      =	upd_rec.	delq_cnt_60	,
            fnd.	delq_cnt_90	      =	upd_rec.	delq_cnt_90	,
            fnd.	delq_cnt_120    	=	upd_rec.	delq_cnt_120	,
            fnd.	delq_cnt_150	    =	upd_rec.	delq_cnt_150	,
            fnd.	delq_cnt_180	    =	upd_rec.	delq_cnt_180	,
            fnd.	delq_cnt_210	    =	upd_rec.	delq_cnt_210	,
            fnd.	purch_food_amt  	=	upd_rec.	purch_food_amt	,
            fnd.	purch_txt_amt   	=	upd_rec.	purch_txt_amt	,
            fnd.	purch_food_no   	=	upd_rec.	purch_food_no	,
            fnd.	purch_txt_no	    =	upd_rec.	purch_txt_no	,
             fnd.	return_food_amt 	=	upd_rec.	return_food_amt	,
            fnd.	return_txt_amt  	=	upd_rec.	return_txt_amt	,
            fnd.	return_food_no	  =	upd_rec.	return_food_no	,
            fnd.	return_txt_no	    =	upd_rec.	return_txt_no	,
            fnd.	reversal_amt	    =	upd_rec.	reversal_amt	,
            fnd.	reversal_no	      =	upd_rec.	reversal_no	,
            fnd.	behaviour_score	  =	upd_rec.	behaviour_score	,
            fnd.	bureau_score	    =	upd_rec.	bureau_score	,
            fnd.	viking_code	      =	upd_rec.	viking_code	,
            fnd.	viking_amt	      =	upd_rec.	viking_amt	,
            fnd.	stmt_ind	        =	upd_rec.	stmt_ind	,

            fnd.  last_updated_date     = g_date
     WHERE  fnd.	wfs_customer_no	      =	upd_rec.	wfs_customer_no AND
            fnd.	wfs_account_no	      =	upd_rec.	wfs_account_no AND
            fnd.	product_code_no	      =	upd_rec.	product_code_no	AND
            fnd.	statement_date	          =	upd_rec.	statement_date	AND
            ( 
            nvl(fnd.statement_month	,0) <>	upd_rec.	statement_month	OR
            nvl(fnd.statement_year	,0) <>	upd_rec.	statement_year	OR
            nvl(fnd.sm_cycle_due	,0) <>	upd_rec.	sm_cycle_due	OR
            nvl(fnd.sm_attr_score1	,0) <>	upd_rec.	sm_attr_score1	OR
            nvl(fnd.sm_credit_limit	,0) <>	upd_rec.	sm_credit_limit	OR
            nvl(fnd.sm_open_bal	,0) <>	upd_rec.	sm_open_bal	OR
            nvl(fnd.sm_end_bal	,0) <>	upd_rec.	sm_end_bal	OR
            nvl(fnd.sm_total_due_val	,0) <>	upd_rec.	sm_total_due_val	OR
            nvl(fnd.sm_returns_qty	,0) <>	upd_rec.	sm_returns_qty	OR
            nvl(fnd.sm_returns_value	,0) <>	upd_rec.	sm_returns_value	OR
            nvl(fnd.sm_recov_fee_val	,0) <>	upd_rec.	sm_recov_fee_val	OR
            nvl(fnd.sm_coll_fee_val	,0) <>	upd_rec.	sm_coll_fee_val	OR
            nvl(fnd.sm_purchases_qty	,0) <>	upd_rec.	sm_purchases_qty	OR
            nvl(fnd.sm_purchases_val	,0) <>	upd_rec.	sm_purchases_val	OR
            nvl(fnd.sm_payments_qty	,0) <>	upd_rec.	sm_payments_qty	OR
            nvl(fnd.sm_payments_val	,0) <>	upd_rec.	sm_payments_val	OR
            nvl(fnd.sm_arrear_val	,0) <>	upd_rec.	sm_arrear_val	OR
            nvl(fnd.sm_nsf_pay_qty	,0) <>	upd_rec.	sm_nsf_pay_qty	OR
            nvl(fnd.sm_nsf_pay_val	,0) <>	upd_rec.	sm_nsf_pay_val	OR
            nvl(fnd.sm_lcp_fee_val	,0) <>	upd_rec.	sm_lcp_fee_val	OR
            nvl(fnd.sm_cbp_fee_val	,0) <>	upd_rec.	sm_cbp_fee_val	OR
            nvl(fnd.sm_interest_raise	,0) <>	upd_rec.	sm_interest_raise	OR
            nvl(fnd.block_code_1	,0) <>	upd_rec.	block_code_1	OR
            nvl(fnd.delinquency_cycle	,0) <>	upd_rec.	delinquency_cycle	OR
            nvl(fnd.account_status	,0) <>	upd_rec.	account_status	OR
            nvl(fnd.coll_bnp	,0) <>	upd_rec.	coll_bnp	OR
            nvl(fnd.coll_bnp_chgoff	,0) <>	upd_rec.	coll_bnp_chgoff	OR
            nvl(fnd.agency_debit	,0) <>	upd_rec.	agency_debit	OR
            nvl(fnd.agency_credit	,0) <>	upd_rec.	agency_credit	OR
            nvl(fnd.block_code_2	,0) <>	upd_rec.	block_code_2	OR
            nvl(fnd.billing_cycle	,0) <>	upd_rec.	billing_cycle	OR
            nvl(fnd.stmt_flag	,0) <>	upd_rec.	stmt_flag	OR
            nvl(fnd.dispu_items_no	,0) <>	upd_rec.	dispu_items_no	OR
            nvl(fnd.dispu_items_val	,0) <>	upd_rec.	dispu_items_val	OR
            nvl(fnd.recency_pmt	,0) <>	upd_rec.	recency_pmt	OR
            nvl(fnd.fixed_pmt_amt	,0) <>	upd_rec.	fixed_pmt_amt	OR
            nvl(fnd.curr_due_val	,0) <>	upd_rec.	curr_due_val	OR
            nvl(fnd.tot_past_due_val	,0) <>	upd_rec.	tot_past_due_val	or
            nvl(fnd.last_act_date	,'1 Jan 1900') <>	upd_rec.	last_act_date	OR
            nvl(fnd.delq_cnt_curr	,0) <>	upd_rec.	delq_cnt_curr	OR
            nvl(fnd.delq_cnt_30	,0) <>	upd_rec.	delq_cnt_30	OR
            nvl(fnd.delq_cnt_60	,0) <>	upd_rec.	delq_cnt_60	OR
            nvl(fnd.delq_cnt_90	,0) <>	upd_rec.	delq_cnt_90	OR
            nvl(fnd.delq_cnt_120	,0) <>	upd_rec.	delq_cnt_120	OR
            nvl(fnd.delq_cnt_150	,0) <>	upd_rec.	delq_cnt_150	OR
            nvl(fnd.delq_cnt_180	,0) <>	upd_rec.	delq_cnt_180	OR
            nvl(fnd.delq_cnt_210	,0) <>	upd_rec.	delq_cnt_210	OR
            nvl(fnd.purch_food_amt	,0) <>	upd_rec.	purch_food_amt	OR
            nvl(fnd.purch_txt_amt	,0) <>	upd_rec.	purch_txt_amt	OR
            nvl(fnd.purch_food_no	,0) <>	upd_rec.	purch_food_no	OR
            nvl(fnd.purch_txt_no	,0) <>	upd_rec.	purch_txt_no	OR
            nvl(fnd.return_food_amt	,0) <>	upd_rec.	return_food_amt	OR
            nvl(fnd.return_txt_amt	,0) <>	upd_rec.	return_txt_amt	OR
            nvl(fnd.return_food_no	,0) <>	upd_rec.	return_food_no	OR
            nvl(fnd.return_txt_no	,0) <>	upd_rec.	return_txt_no	OR
            nvl(fnd.reversal_amt	,0) <>	upd_rec.	reversal_amt	OR
            nvl(fnd.reversal_no	,0) <>	upd_rec.	reversal_no	OR
            nvl(fnd.behaviour_score	,0) <>	upd_rec.	behaviour_score	OR
            nvl(fnd.bureau_score	,0) <>	upd_rec.	bureau_score	OR
            nvl(fnd.viking_code	,0) <>	upd_rec.	viking_code	OR
            nvl(fnd.viking_amt	,0) <>	upd_rec.	viking_amt	OR
            nvl(fnd.stmt_ind	,0) <>	upd_rec.	stmt_ind
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
-- Send records to hospital where not valid
--**************************************************************************************************
procedure flagged_records_hospital as
begin
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_cust_mth_stmt_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
            	cpy.	wfs_customer_no	,
            	cpy.	product_code_no	,
            	cpy.	statement_date	,
            	cpy.	statement_month	,
            	cpy.	statement_year	,
            	cpy.	wfs_account_no	,
            	cpy.	sm_cycle_due	,
            	cpy.	sm_attr_score1	,
            	cpy.	sm_credit_limit	,
            	cpy.	sm_open_bal	,
            	cpy.	sm_end_bal	,
            	cpy.	sm_total_due_val	,
            	cpy.	sm_returns_qty	,
            	cpy.	sm_returns_value	,
            	cpy.	sm_recov_fee_val	,
            	cpy.	sm_coll_fee_val	,
            	cpy.	sm_purchases_qty	,
            	cpy.	sm_purchases_val	,
            	cpy.	sm_payments_qty	,
            	cpy.	sm_payments_val	,
            	cpy.	sm_arrear_val	,
            	cpy.	sm_nsf_pay_qty	,
            	cpy.	sm_nsf_pay_val	,
            	cpy.	sm_lcp_fee_val	,
            	cpy.	sm_cbp_fee_val	,
            	cpy.	sm_interest_raise	,
            	cpy.	block_code_1	,
            	cpy.	delinquency_cycle	,
            	cpy.	account_status	,
            	cpy.	coll_bnp	,
            	cpy.	coll_bnp_chgoff	,
            	cpy.	agency_debit	,
            	cpy.	agency_credit	,
            	cpy.	block_code_2	,
            	cpy.	billing_cycle	,
            	cpy.	stmt_flag	,
            	cpy.	dispu_items_no	,
            	cpy.	dispu_items_val	,
            	cpy.	recency_pmt	,
            	cpy.	fixed_pmt_amt	,
            	cpy.	curr_due_val	,
            	cpy.	tot_past_due_val	,
            	cpy.	last_act_date	,
            	cpy.	delq_cnt_curr	,
            	cpy.	delq_cnt_30	,
            	cpy.	delq_cnt_60	,
            	cpy.	delq_cnt_90	,
            	cpy.	delq_cnt_120	,
            	cpy.	delq_cnt_150	,
            	cpy.	delq_cnt_180	,
            	cpy.	delq_cnt_210	,
            	cpy.	purch_food_amt	,
            	cpy.	purch_txt_amt	,
            	cpy.	purch_food_no	,
            	cpy.	purch_txt_no	,
            	cpy.	return_food_amt	,
            	cpy.	return_txt_amt	,
            	cpy.	return_food_no	,
            	cpy.	return_txt_no	,
            	cpy.	reversal_amt	,
            	cpy.	reversal_no	,
            	cpy.	behaviour_score	,
            	cpy.	bureau_score	,
            	cpy.	viking_code	,
            	cpy.	viking_amt	,
            	cpy.	stmt_ind	
      FROM   stg_vsn_cust_mth_stmt_cpy cpy
      where  
      ( 1 =   
        (SELECT dummy_ind  FROM  fnd_customer_product cust
         where  cpy.wfs_customer_no       = cust.product_no and cust.customer_no = 0 ) or
        1 =   
        (select dummy_ind  from  fnd_customer_product cust
         where  cpy.wfs_account_no       = cust.product_no and cust.customer_no = 0 ) or
        1 = 
        (select dummy_ind from  fnd_wfs_product pcd
         where  cpy.product_code_no       = pcd.product_code_no ) 
      ) 
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---        
      and sys_process_code = 'N';
         

g_recs_hospital := g_recs_hospital + sql%rowcount;
      
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
    
    l_text := 'CREATION OF DUMMY MASTER RECORDS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    create_dummy_masters;
    
    select count(*)
    into   g_recs_read
    from   stg_vsn_cust_mth_stmt_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;
    
    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_vsn_cust_mth_stmt_cpy
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
       RAISE;
end wh_fnd_wfs_210u;
