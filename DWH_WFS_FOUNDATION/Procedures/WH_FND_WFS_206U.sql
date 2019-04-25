--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_206U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_206U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_perf_day fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_cust_perf_day_cpy
--               Output - fnd_wfs_cust_perf_day
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


g_wfs_customer_no    stg_vsn_cust_perf_day_cpy.wfs_customer_no%type; 
g_product_code_no    stg_vsn_cust_perf_day_cpy.product_code_no%type; 
g_run_date           stg_vsn_cust_perf_day_cpy.run_date%type; 

   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_206U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_CUST_PERF_DAY EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_cust_perf_day_cpy
where (wfs_customer_no,
product_code_no,
run_date)
in
(select wfs_customer_no,
product_code_no,
run_date
from stg_vsn_cust_perf_day_cpy 
group by wfs_customer_no,
product_code_no,
run_date
having count(*) > 1) 
order by wfs_customer_no,
product_code_no,
run_date,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_cust_perf_day is
select /*+ FULL(cpy)  parallel (cpy,2) */  
              cpy.*
      from    stg_vsn_cust_perf_day_cpy cpy,
              fnd_wfs_cust_perf_day fnd
      where   cpy.wfs_customer_no       = fnd.wfs_customer_no and    
              cpy.product_code_no       = fnd.product_code_no    and   
              cpy.run_date     = fnd.run_date    and 
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.wfs_customer_no,
              cpy.product_code_no,
              cpy.run_date,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no    := 0; 
   g_product_code_no    := 0;
   g_run_date           := '1 Jan 2000';


for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no   = g_wfs_customer_no and
        dupp_record.product_code_no   = g_product_code_no and
        dupp_record.run_date          = g_run_date then
        update stg_vsn_cust_perf_day_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_wfs_customer_no    := dupp_record.wfs_customer_no; 
    g_product_code_no    := dupp_record.product_code_no;
    g_run_date           := dupp_record.run_date;

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
             'Dummy wh_fnd_wfs_206U',
             0	,
             ' ',
             g_date,
             1
      from   stg_vsn_cust_perf_day_cpy cpy
 
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
      from   stg_vsn_cust_perf_day_cpy cpy
 
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
      from   stg_vsn_cust_perf_day_cpy  cpy
 
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_cust_perf_day fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	wfs_customer_no	,
            	cpy.	product_code_no	,
            	cpy.	run_date	,
            	cpy.	wfs_account_no	,
            	cpy.	auth_value_1	,
            	cpy.	auth_value_2	,
            	cpy.	auth_value_3	,
            	cpy.	auth_value_4	,
            	cpy.	auth_value_5	,
            	cpy.	auth_value_6	,
            	cpy.	auth_value_7	,
            	cpy.	auth_val_today_1	,
            	cpy.	auth_val_today_2	,
            	cpy.	auth_val_today_3	,
            	cpy.	auth_val_today_4	,
            	cpy.	auth_val_today_5	,
            	cpy.	auth_val_today_6	,
            	cpy.	auth_val_today_7	,
            	cpy.	cash_val_ctd	,
            	cpy.	coll_fees_val_ctd	,
            	cpy.	ins_fees_val_ctd	,
            	cpy.	late_fees_val_ctd	,
            	cpy.	memb_fees_val_ctd	,
            	cpy.	nsf_fees_val_ctd	,
            	cpy.	ovlm_fees_val_ctd	,
            	cpy.	decl_val_today_1	,
            	cpy.	decl_val_today_2	,
            	cpy.	decl_val_today_3	,
            	cpy.	decl_val_today_4	,
            	cpy.	decl_val_today_5	,
            	cpy.	decl_val_today_6	,
            	cpy.	decl_val_today_7	,
            	cpy.	last_debit_val	,
            	cpy.	last_pchs_val	,
            	cpy.	memo_credit_val	,
            	cpy.	memo_debit_val	,
            	cpy.	late_chg_val_ytd	,
            	cpy.	card_usage_flag	,
            	cpy.	cash_auth_val_tdy1	,
            	cpy.	cash_auth_val_tdy2	,
            	cpy.	cash_auth_val_tdy3	,
            	cpy.	cash_auth_val_tdy4	,
            	cpy.	cash_auth_val_tdy5	,
            	cpy.	cash_auth_val_tdy6	,
            	cpy.	cash_auth_val_tdy7	,
            	cpy.	cash_chgoff_val	,
            	cpy.	cash_decl_val_tdy1	,
            	cpy.	cash_decl_val_tdy2	,
            	cpy.	cash_decl_val_tdy3	,
            	cpy.	cash_decl_val_tdy4	,
            	cpy.	cash_decl_val_tdy5	,
            	cpy.	cash_decl_val_tdy6	,
            	cpy.	cash_decl_val_tdy7	,
            	cpy.	cash_dispu_itm_val	,
            	cpy.	cash_outstauth_val	,
            	cpy.	cash_avail_cr_val	,
            	cpy.	cash_avail_credit	,
            	cpy.	cash_avail_cr_flag	,
            	cpy.	cash_avail_cr_pcnt	,
            	cpy.	cash_balance	,
            	cpy.	cash_credit_limit	,
            	cpy.	cash_qty_auth_tdy1	,
            	cpy.	cash_qty_auth_tdy2	,
            	cpy.	cash_qty_auth_tdy3	,
            	cpy.	cash_qty_auth_tdy4	,
            	cpy.	cash_qty_auth_tdy5	,
            	cpy.	cash_qty_auth_tdy6	,
            	cpy.	cash_qty_auth_tdy7	,
            	cpy.	cash_qty_decl_tdy1	,
            	cpy.	cash_qty_decl_tdy2	,
            	cpy.	cash_qty_decl_tdy3	,
            	cpy.	cash_qty_decl_tdy4	,
            	cpy.	cash_qty_decl_tdy5	,
            	cpy.	cash_qty_decl_tdy6	,
            	cpy.	cash_qty_decl_tdy7	,
            	cpy.	cash_dispu_itm_qty	,
            	cpy.	cash_outstauth_qty	,
            	cpy.	cash_plan_qty	,
            	cpy.	cr_bal_refund_days	,
            	cpy.	cr_bal_refund_flag	,
            	cpy.	cash_adv_svchg_ctd	,
            	cpy.	service_charge_ctd	,
            	cpy.	curr_collector	,
            	cpy.	curr_late_chg_val	,
            	cpy.	curr_int_paid_ytd	,
            	cpy.	fixed_pmt_pct	,
            	cpy.	fraud_reprtng_flag	,
            	cpy.	finance_charge_mtd	,
            	cpy.	insurance_prem_mtd	,
            	cpy.	service_charge_mtd	,
            	cpy.	auth_qty_1	,
            	cpy.	auth_qty_2	,
            	cpy.	auth_qty_3	,
            	cpy.	auth_qty_4	,
            	cpy.	auth_qty_5	,
            	cpy.	auth_qty_6	,
            	cpy.	auth_qty_7	,
            	cpy.	auth_today_qty_1	,
            	cpy.	auth_today_qty_2	,
            	cpy.	auth_today_qty_3	,
            	cpy.	auth_today_qty_4	,
            	cpy.	auth_today_qty_5	,
            	cpy.	auth_today_qty_6	,
            	cpy.	auth_today_qty_7	,
            	cpy.	cash_qty_ctd	,
            	cpy.	late_fees_qty_ctd	,
            	cpy.	decl_qty_tdy_1	,
            	cpy.	decl_qty_tdy_2	,
            	cpy.	decl_qty_tdy_3	,
            	cpy.	decl_qty_tdy_4	,
            	cpy.	decl_qty_tdy_5	,
            	cpy.	decl_qty_tdy_6	,
            	cpy.	decl_qty_tdy_7	,
            	cpy.	memo_cr_qty	,
            	cpy.	plans_qty	,
            	cpy.	unblked_cards_qty	,
            	cpy.	late_chg_qty_ytd	,
            	cpy.	pmt_120days	,
            	cpy.	pmt_120days_qty	,
            	cpy.	pmt_150days	,
            	cpy.	pmt_150days_qty	,
            	cpy.	pmt_180days	,
            	cpy.	pmt_180days_qty	,
            	cpy.	pmt_210days	,
            	cpy.	pmt_210days_qty	,
            	cpy.	pmt_30days	,
            	cpy.	pmt_30days_qty	,
            	cpy.	pmt_60days	,
            	cpy.	pmt_60days_qty	,
            	cpy.	pmt_90days	,
            	cpy.	pmt_90days_qty	,
            	cpy.	pmt_ach_debit_qty	,
            	cpy.	pmt_ach_debit_type	,
            	cpy.	pmt_ach_flag	,
            	cpy.	pmt_ach_rt_qty	,
            	cpy.	pmt_consec	,
            	cpy.	pmt_days_delq	,
            	cpy.	pmt_delq_paid_1	,
            	cpy.	pmt_delq_paid_2	,
            	cpy.	pmt_delq_paid_3	,
            	cpy.	pmt_delq_paid_4	,
            	cpy.	pmt_delq_paid_5	,
            	cpy.	pmt_delq_paid_6	,
            	cpy.	pmt_delq_paid_7	,
            	cpy.	pmt_delq_paid_8	,
            	cpy.	pmt_delq_paid_9	,
            	cpy.	pmt_grace_days	,
            	cpy.	pmt_last_requested	,
            	cpy.	pmt_repaid_qty	,
            	cpy.	pmt_past_ctr	,
            	cpy.	pmt_potent_indic	,
            	cpy.	pmt_prior_recency	,
            	cpy.	pmt_ratios_1	,
            	cpy.	pmt_ratios_2	,
            	cpy.	pmt_ratios_3	,
            	cpy.	pmt_ratios_4	,
            	cpy.	pmt_ratios_5	,
            	cpy.	pmt_ratios_6	,
            	cpy.	pmt_ratios_7	,
            	cpy.	pmt_ratios_8	,
            	cpy.	pmt_ratios_9	,
            	cpy.	pmt_ratios_10	,
            	cpy.	pmt_ratios_11	,
            	cpy.	pmt_ratios_12	,
            	cpy.	pmt_reaged_by	,
            	cpy.	pmt_recency	,
            	cpy.	pmt_skip_flag	,
            	cpy.	pmt_tms_man_reage	,
            	cpy.	pmt_times_reaged	,
            	cpy.	pmt_total_due_val	,
            	cpy.	pmt_tot_forgvn_val	,
            	cpy.	prior_late_chg_val	,
            	cpy.	waiv_cash_avail_cr	,
            	cpy.	waive_intr_chg	,
            	cpy.	waive_late_chg	,
            	cpy.	waive_late_notice	,
            	cpy.	waive_nsf_fee_ind	,
            	cpy.	waive_ovlm	,
            	cpy.	waive_ovlm_notc	,
             	cpy.	waive_svc_chg	,
            	cpy.	ovlm_chg_ytd	,
              g_date as last_updated_date
       from  stg_vsn_cust_perf_day_cpy cpy
       where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_cust_perf_day 
       where  wfs_customer_no    = cpy.wfs_customer_no and
              product_code_no    = cpy.product_code_no and
              run_date           = cpy.run_date)
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



FOR upd_rec IN c_stg_vsn_cust_perf_day
   loop
     update fnd_wfs_cust_perf_day fnd 
     set    fnd.	wfs_customer_no	    =	upd_rec.	wfs_customer_no	,
            fnd.	product_code_no	    =	upd_rec.	product_code_no	,
            fnd.	run_date	          =	upd_rec.	run_date	,
            fnd.	wfs_account_no	    =	upd_rec.	wfs_account_no	,
            fnd.	auth_value_1	      =	upd_rec.	auth_value_1	,
            fnd.	auth_value_2      	=	upd_rec.	auth_value_2	,
            fnd.	auth_value_3	      =	upd_rec.	auth_value_3	,
            fnd.	auth_value_4	      =	upd_rec.	auth_value_4	,
            fnd.	auth_value_5	      =	upd_rec.	auth_value_5	,
            fnd.	auth_value_6      	=	upd_rec.	auth_value_6	,
            fnd.	auth_value_7	      =	upd_rec.	auth_value_7	,
            fnd.	auth_val_today_1	  =	upd_rec.	auth_val_today_1	,
            fnd.	auth_val_today_2	  =	upd_rec.	auth_val_today_2	,
            fnd.	auth_val_today_3	  =	upd_rec.	auth_val_today_3	,
            fnd.	auth_val_today_4	  =	upd_rec.	auth_val_today_4	,
            fnd.	auth_val_today_5  	=	upd_rec.	auth_val_today_5	,
            fnd.	auth_val_today_6	  =	upd_rec.	auth_val_today_6	,
            fnd.	auth_val_today_7	  =	upd_rec.	auth_val_today_7	,
            fnd.	cash_val_ctd	      =	upd_rec.	cash_val_ctd	,
            fnd.	coll_fees_val_ctd	  =	upd_rec.	coll_fees_val_ctd	,
            fnd.	ins_fees_val_ctd	  =	upd_rec.	ins_fees_val_ctd	,
            fnd.	late_fees_val_ctd	  =	upd_rec.	late_fees_val_ctd	,
            fnd.	memb_fees_val_ctd 	=	upd_rec.	memb_fees_val_ctd	,
            fnd.	nsf_fees_val_ctd	  =	upd_rec.	nsf_fees_val_ctd	,
            fnd.	ovlm_fees_val_ctd	  =	upd_rec.	ovlm_fees_val_ctd	,
            fnd.	decl_val_today_1	  =	upd_rec.	decl_val_today_1	,
            fnd.	decl_val_today_2	  =	upd_rec.	decl_val_today_2	,
            fnd.	decl_val_today_3	  =	upd_rec.	decl_val_today_3	,
            fnd.	decl_val_today_4	  =	upd_rec.	decl_val_today_4	,
            fnd.	decl_val_today_5	  =	upd_rec.	decl_val_today_5	,
            fnd.	decl_val_today_6	  =	upd_rec.	decl_val_today_6	,
            fnd.	decl_val_today_7	  =	upd_rec.	decl_val_today_7	,
            fnd.	last_debit_val	    =	upd_rec.	last_debit_val	,
            fnd.	last_pchs_val	      =	upd_rec.	last_pchs_val	,
            fnd.	memo_credit_val	    =	upd_rec.	memo_credit_val	,
            fnd.	memo_debit_val	    =	upd_rec.	memo_debit_val	,
            fnd.	late_chg_val_ytd	  =	upd_rec.	late_chg_val_ytd	,
            fnd.	card_usage_flag	    =	upd_rec.	card_usage_flag	,
            fnd.	cash_auth_val_tdy1	=	upd_rec.	cash_auth_val_tdy1	,
            fnd.	cash_auth_val_tdy2	=	upd_rec.	cash_auth_val_tdy2	,
            fnd.	cash_auth_val_tdy3	=	upd_rec.	cash_auth_val_tdy3	,
            fnd.	cash_auth_val_tdy4	=	upd_rec.	cash_auth_val_tdy4	,
            fnd.	cash_auth_val_tdy5	=	upd_rec.	cash_auth_val_tdy5	,
            fnd.	cash_auth_val_tdy6	=	upd_rec.	cash_auth_val_tdy6	,
            fnd.	cash_auth_val_tdy7	=	upd_rec.	cash_auth_val_tdy7	,
            fnd.	cash_chgoff_val   	=	upd_rec.	cash_chgoff_val	,
            fnd.	cash_decl_val_tdy1	=	upd_rec.	cash_decl_val_tdy1	,
            fnd.	cash_decl_val_tdy2	=	upd_rec.	cash_decl_val_tdy2	,
            fnd.	cash_decl_val_tdy3	=	upd_rec.	cash_decl_val_tdy3	,
            fnd.	cash_decl_val_tdy4	=	upd_rec.	cash_decl_val_tdy4	,
            fnd.	cash_decl_val_tdy5	=	upd_rec.	cash_decl_val_tdy5	,
            fnd.	cash_decl_val_tdy6	=	upd_rec.	cash_decl_val_tdy6	,
            fnd.	cash_decl_val_tdy7	=	upd_rec.	cash_decl_val_tdy7	,
            fnd.	cash_dispu_itm_val	=	upd_rec.	cash_dispu_itm_val	,
            fnd.	cash_outstauth_val	=	upd_rec.	cash_outstauth_val	,
            fnd.	cash_avail_cr_val 	=	upd_rec.	cash_avail_cr_val	,
            fnd.	cash_avail_credit	  =	upd_rec.	cash_avail_credit	,
            fnd.	cash_avail_cr_flag	=	upd_rec.	cash_avail_cr_flag	,
            fnd.	cash_avail_cr_pcnt	=	upd_rec.	cash_avail_cr_pcnt	,
            fnd.	cash_balance	      =	upd_rec.	cash_balance	,
            fnd.	cash_credit_limit	  =	upd_rec.	cash_credit_limit	,
            fnd.	cash_qty_auth_tdy1	=	upd_rec.	cash_qty_auth_tdy1	,
            fnd.	cash_qty_auth_tdy2	=	upd_rec.	cash_qty_auth_tdy2	,
            fnd.	cash_qty_auth_tdy3	=	upd_rec.	cash_qty_auth_tdy3	,
            fnd.	cash_qty_auth_tdy4	=	upd_rec.	cash_qty_auth_tdy4	,
            fnd.	cash_qty_auth_tdy5	=	upd_rec.	cash_qty_auth_tdy5	,
            fnd.	cash_qty_auth_tdy6	=	upd_rec.	cash_qty_auth_tdy6	,
            fnd.	cash_qty_auth_tdy7	=	upd_rec.	cash_qty_auth_tdy7	,
            fnd.	cash_qty_decl_tdy1	=	upd_rec.	cash_qty_decl_tdy1	,
            fnd.	cash_qty_decl_tdy2	=	upd_rec.	cash_qty_decl_tdy2	,
            fnd.	cash_qty_decl_tdy3	=	upd_rec.	cash_qty_decl_tdy3	,
            fnd.	cash_qty_decl_tdy4	=	upd_rec.	cash_qty_decl_tdy4	,
            fnd.	cash_qty_decl_tdy5	=	upd_rec.	cash_qty_decl_tdy5	,
            fnd.	cash_qty_decl_tdy6	=	upd_rec.	cash_qty_decl_tdy6	,
            fnd.	cash_qty_decl_tdy7	=	upd_rec.	cash_qty_decl_tdy7	,
            fnd.	cash_dispu_itm_qty	=	upd_rec.	cash_dispu_itm_qty	,
            fnd.	cash_outstauth_qty	=	upd_rec.	cash_outstauth_qty	,
            fnd.	cash_plan_qty	      =	upd_rec.	cash_plan_qty	,
            fnd.	cr_bal_refund_days	=	upd_rec.	cr_bal_refund_days	,
            fnd.	cr_bal_refund_flag	=	upd_rec.	cr_bal_refund_flag	,
            fnd.	cash_adv_svchg_ctd	=	upd_rec.	cash_adv_svchg_ctd	,
            fnd.	service_charge_ctd	=	upd_rec.	service_charge_ctd	,
            fnd.	curr_collector	    =	upd_rec.	curr_collector	,
            fnd.	curr_late_chg_val	  =	upd_rec.	curr_late_chg_val	,
            fnd.	curr_int_paid_ytd	  =	upd_rec.	curr_int_paid_ytd	,
            fnd.	fixed_pmt_pct	      =	upd_rec.	fixed_pmt_pct	,
            fnd.	fraud_reprtng_flag	=	upd_rec.	fraud_reprtng_flag	,
            fnd.	finance_charge_mtd	=	upd_rec.	finance_charge_mtd	,
            fnd.	insurance_prem_mtd	=	upd_rec.	insurance_prem_mtd	,
            fnd.	service_charge_mtd	=	upd_rec.	service_charge_mtd	,
            fnd.	auth_qty_1	        =	upd_rec.	auth_qty_1	,
            fnd.	auth_qty_2	        =	upd_rec.	auth_qty_2	,
            fnd.	auth_qty_3	        =	upd_rec.	auth_qty_3	,
            fnd.	auth_qty_4	        =	upd_rec.	auth_qty_4	,
            fnd.	auth_qty_5	        =	upd_rec.	auth_qty_5	,
            fnd.	auth_qty_6	        =	upd_rec.	auth_qty_6	,
            fnd.	auth_qty_7        	=	upd_rec.	auth_qty_7	,
            fnd.	auth_today_qty_1	  =	upd_rec.	auth_today_qty_1	,
            fnd.	auth_today_qty_2	  =	upd_rec.	auth_today_qty_2	,
            fnd.	auth_today_qty_3	  =	upd_rec.	auth_today_qty_3	,
            fnd.	auth_today_qty_4  	=	upd_rec.	auth_today_qty_4	,
            fnd.	auth_today_qty_5  	=	upd_rec.	auth_today_qty_5	,
            fnd.	auth_today_qty_6	  =	upd_rec.	auth_today_qty_6	,
            fnd.	auth_today_qty_7  	=	upd_rec.	auth_today_qty_7	,
            fnd.	cash_qty_ctd	      =	upd_rec.	cash_qty_ctd	,
            fnd.	late_fees_qty_ctd 	=	upd_rec.	late_fees_qty_ctd	,
            fnd.	decl_qty_tdy_1	    =	upd_rec.	decl_qty_tdy_1	,
            fnd.	decl_qty_tdy_2	    =	upd_rec.	decl_qty_tdy_2	,
            fnd.	decl_qty_tdy_3    	=	upd_rec.	decl_qty_tdy_3	,
            fnd.	decl_qty_tdy_4    	=	upd_rec.	decl_qty_tdy_4	,
            fnd.	decl_qty_tdy_5    	=	upd_rec.	decl_qty_tdy_5	,
            fnd.	decl_qty_tdy_6	    =	upd_rec.	decl_qty_tdy_6	,
            fnd.	decl_qty_tdy_7	    =	upd_rec.	decl_qty_tdy_7	,
            fnd.	memo_cr_qty       	=	upd_rec.	memo_cr_qty	,
            fnd.	plans_qty         	=	upd_rec.	plans_qty	,
            fnd.	unblked_cards_qty	  =	upd_rec.	unblked_cards_qty	,
            fnd.	late_chg_qty_ytd	  =	upd_rec.	late_chg_qty_ytd	,
            fnd.	pmt_120days	        =	upd_rec.	pmt_120days	,
            fnd.	pmt_120days_qty	    =	upd_rec.	pmt_120days_qty	,
            fnd.	pmt_150days	        =	upd_rec.	pmt_150days	,
            fnd.	pmt_150days_qty	    =	upd_rec.	pmt_150days_qty	,
            fnd.	pmt_180days	        =	upd_rec.	pmt_180days	,
            fnd.	pmt_180days_qty	    =	upd_rec.	pmt_180days_qty	,
            fnd.	pmt_210days	        =	upd_rec.	pmt_210days	,
            fnd.	pmt_210days_qty	    =	upd_rec.	pmt_210days_qty	,
            fnd.	pmt_30days	        =	upd_rec.	pmt_30days	,
            fnd.	pmt_30days_qty	    =	upd_rec.	pmt_30days_qty	,
            fnd.	pmt_60days	        =	upd_rec.	pmt_60days	,
            fnd.	pmt_60days_qty	    =	upd_rec.	pmt_60days_qty	,
            fnd.	pmt_90days	        =	upd_rec.	pmt_90days	,
            fnd.	pmt_90days_qty	    =	upd_rec.	pmt_90days_qty	,
            fnd.	pmt_ach_debit_qty 	=	upd_rec.	pmt_ach_debit_qty	,
            fnd.	pmt_ach_debit_type	=	upd_rec.	pmt_ach_debit_type	,
            fnd.	pmt_ach_flag	      =	upd_rec.	pmt_ach_flag	,
            fnd.	pmt_ach_rt_qty	    =	upd_rec.	pmt_ach_rt_qty	,
            fnd.	pmt_consec	        =	upd_rec.	pmt_consec	,
            fnd.	pmt_days_delq	      =	upd_rec.	pmt_days_delq	,
            fnd.	pmt_delq_paid_1	    =	upd_rec.	pmt_delq_paid_1	,
            fnd.	pmt_delq_paid_2	    =	upd_rec.	pmt_delq_paid_2	,
            fnd.	pmt_delq_paid_3	    =	upd_rec.	pmt_delq_paid_3	,
            fnd.	pmt_delq_paid_4	    =	upd_rec.	pmt_delq_paid_4	,
            fnd.	pmt_delq_paid_5	    =	upd_rec.	pmt_delq_paid_5	,
            fnd.	pmt_delq_paid_6   	=	upd_rec.	pmt_delq_paid_6	,
            fnd.	pmt_delq_paid_7	    =	upd_rec.	pmt_delq_paid_7	,
            fnd.	pmt_delq_paid_8   	=	upd_rec.	pmt_delq_paid_8	,
            fnd.	pmt_delq_paid_9	    =	upd_rec.	pmt_delq_paid_9	,
            fnd.	pmt_grace_days	    =	upd_rec.	pmt_grace_days	,
            fnd.	pmt_last_requested	=	upd_rec.	pmt_last_requested	,
            fnd.	pmt_repaid_qty	    =	upd_rec.	pmt_repaid_qty	,
            fnd.	pmt_past_ctr	      =	upd_rec.	pmt_past_ctr	,
            fnd.	pmt_potent_indic	  =	upd_rec.	pmt_potent_indic	,
            fnd.	pmt_prior_recency	  =	upd_rec.	pmt_prior_recency	,
            fnd.	pmt_ratios_1	      =	upd_rec.	pmt_ratios_1	,
            fnd.	pmt_ratios_2	      =	upd_rec.	pmt_ratios_2	,
            fnd.	pmt_ratios_3	      =	upd_rec.	pmt_ratios_3	,
            fnd.	pmt_ratios_4	      =	upd_rec.	pmt_ratios_4	,
            fnd.	pmt_ratios_5	      =	upd_rec.	pmt_ratios_5	,
            fnd.	pmt_ratios_6	      =	upd_rec.	pmt_ratios_6	,
            fnd.	pmt_ratios_7	      =	upd_rec.	pmt_ratios_7	,
            fnd.	pmt_ratios_8	      =	upd_rec.	pmt_ratios_8	,
            fnd.	pmt_ratios_9	      =	upd_rec.	pmt_ratios_9	,
            fnd.	pmt_ratios_10	      =	upd_rec.	pmt_ratios_10	,
            fnd.	pmt_ratios_11	      =	upd_rec.	pmt_ratios_11	,
            fnd.	pmt_ratios_12	      =	upd_rec.	pmt_ratios_12	,
            fnd.	pmt_reaged_by	      =	upd_rec.	pmt_reaged_by	,
            fnd.	pmt_recency	        =	upd_rec.	pmt_recency	,
            fnd.	pmt_skip_flag	      =	upd_rec.	pmt_skip_flag	,
            fnd.	pmt_tms_man_reage	  =	upd_rec.	pmt_tms_man_reage	,
            fnd.	pmt_times_reaged	  =	upd_rec.	pmt_times_reaged	,
            fnd.	pmt_total_due_val	  =	upd_rec.	pmt_total_due_val	,
            fnd.	pmt_tot_forgvn_val	=	upd_rec.	pmt_tot_forgvn_val	,
            fnd.	prior_late_chg_val	=	upd_rec.	prior_late_chg_val	,
            fnd.	waiv_cash_avail_cr	=	upd_rec.	waiv_cash_avail_cr	,
            fnd.	waive_intr_chg	    =	upd_rec.	waive_intr_chg	,
            fnd.	waive_late_chg	    =	upd_rec.	waive_late_chg	,
            fnd.	waive_late_notice	  =	upd_rec.	waive_late_notice	,
            fnd.	waive_nsf_fee_ind	  =	upd_rec.	waive_nsf_fee_ind	,
            fnd.	waive_ovlm	        =	upd_rec.	waive_ovlm	,
            fnd.	waive_ovlm_notc	    =	upd_rec.	waive_ovlm_notc	,
            fnd.	waive_svc_chg	      =	upd_rec.	waive_svc_chg	,
            fnd.	ovlm_chg_ytd	      =	upd_rec.	ovlm_chg_ytd	,
            fnd.  last_updated_date   = g_date
     where  fnd.	wfs_customer_no	      =	upd_rec.	wfs_customer_no and
            fnd.	product_code_no	      =	upd_rec.	product_code_no	and
            fnd.	run_date	    =	upd_rec.	run_date	and
            (             
            nvl(fnd.wfs_account_no	,0) <>	upd_rec.	wfs_account_no	or
            nvl(fnd.auth_value_1	,0) <>	upd_rec.	auth_value_1	or
            nvl(fnd.auth_value_2	,0) <>	upd_rec.	auth_value_2	or
            nvl(fnd.auth_value_3	,0) <>	upd_rec.	auth_value_3	or
            nvl(fnd.auth_value_4	,0) <>	upd_rec.	auth_value_4	or
            nvl(fnd.auth_value_5	,0) <>	upd_rec.	auth_value_5	or
            nvl(fnd.auth_value_6	,0) <>	upd_rec.	auth_value_6	or
            nvl(fnd.auth_value_7	,0) <>	upd_rec.	auth_value_7	or
            nvl(fnd.auth_val_today_1	,0) <>	upd_rec.	auth_val_today_1	or
            nvl(fnd.auth_val_today_2	,0) <>	upd_rec.	auth_val_today_2	or
            nvl(fnd.auth_val_today_3	,0) <>	upd_rec.	auth_val_today_3	or
            nvl(fnd.auth_val_today_4	,0) <>	upd_rec.	auth_val_today_4	or
            nvl(fnd.auth_val_today_5	,0) <>	upd_rec.	auth_val_today_5	or
            nvl(fnd.auth_val_today_6	,0) <>	upd_rec.	auth_val_today_6	or
            nvl(fnd.auth_val_today_7	,0) <>	upd_rec.	auth_val_today_7	or
            nvl(fnd.cash_val_ctd	,0) <>	upd_rec.	cash_val_ctd	or
            nvl(fnd.coll_fees_val_ctd	,0) <>	upd_rec.	coll_fees_val_ctd	or
            nvl(fnd.ins_fees_val_ctd	,0) <>	upd_rec.	ins_fees_val_ctd	or
            nvl(fnd.late_fees_val_ctd	,0) <>	upd_rec.	late_fees_val_ctd	or
            nvl(fnd.memb_fees_val_ctd	,0) <>	upd_rec.	memb_fees_val_ctd	or
            nvl(fnd.nsf_fees_val_ctd	,0) <>	upd_rec.	nsf_fees_val_ctd	or
            nvl(fnd.ovlm_fees_val_ctd	,0) <>	upd_rec.	ovlm_fees_val_ctd	or
            nvl(fnd.decl_val_today_1	,0) <>	upd_rec.	decl_val_today_1	or
            nvl(fnd.decl_val_today_2	,0) <>	upd_rec.	decl_val_today_2	or
            nvl(fnd.decl_val_today_3	,0) <>	upd_rec.	decl_val_today_3	or
            nvl(fnd.decl_val_today_4	,0) <>	upd_rec.	decl_val_today_4	or
            nvl(fnd.decl_val_today_5	,0) <>	upd_rec.	decl_val_today_5	or
            nvl(fnd.decl_val_today_6	,0) <>	upd_rec.	decl_val_today_6	or
            nvl(fnd.decl_val_today_7	,0) <>	upd_rec.	decl_val_today_7	or
            nvl(fnd.last_debit_val	,0) <>	upd_rec.	last_debit_val	or
            nvl(fnd.last_pchs_val	,0) <>	upd_rec.	last_pchs_val	or
            nvl(fnd.memo_credit_val	,0) <>	upd_rec.	memo_credit_val	or
            nvl(fnd.memo_debit_val	,0) <>	upd_rec.	memo_debit_val	or
            nvl(fnd.late_chg_val_ytd	,0) <>	upd_rec.	late_chg_val_ytd	or
            nvl(fnd.card_usage_flag	,0) <>	upd_rec.	card_usage_flag	or
            nvl(fnd.cash_auth_val_tdy1	,0) <>	upd_rec.	cash_auth_val_tdy1	or
            nvl(fnd.cash_auth_val_tdy2	,0) <>	upd_rec.	cash_auth_val_tdy2	or
            nvl(fnd.cash_auth_val_tdy3	,0) <>	upd_rec.	cash_auth_val_tdy3	or
            nvl(fnd.cash_auth_val_tdy4	,0) <>	upd_rec.	cash_auth_val_tdy4	or
            nvl(fnd.cash_auth_val_tdy5	,0) <>	upd_rec.	cash_auth_val_tdy5	or
            nvl(fnd.cash_auth_val_tdy6	,0) <>	upd_rec.	cash_auth_val_tdy6	or
            nvl(fnd.cash_auth_val_tdy7	,0) <>	upd_rec.	cash_auth_val_tdy7	or
            nvl(fnd.cash_chgoff_val	,0) <>	upd_rec.	cash_chgoff_val	or
            nvl(fnd.cash_decl_val_tdy1	,0) <>	upd_rec.	cash_decl_val_tdy1	or
            nvl(fnd.cash_decl_val_tdy2	,0) <>	upd_rec.	cash_decl_val_tdy2	or
            nvl(fnd.cash_decl_val_tdy3	,0) <>	upd_rec.	cash_decl_val_tdy3	or
            nvl(fnd.cash_decl_val_tdy4	,0) <>	upd_rec.	cash_decl_val_tdy4	or
            nvl(fnd.cash_decl_val_tdy5	,0) <>	upd_rec.	cash_decl_val_tdy5	or
            nvl(fnd.cash_decl_val_tdy6	,0) <>	upd_rec.	cash_decl_val_tdy6	or
            nvl(fnd.cash_decl_val_tdy7	,0) <>	upd_rec.	cash_decl_val_tdy7	or
            nvl(fnd.cash_dispu_itm_val	,0) <>	upd_rec.	cash_dispu_itm_val	or
            nvl(fnd.cash_outstauth_val	,0) <>	upd_rec.	cash_outstauth_val	or
            nvl(fnd.cash_avail_cr_val	,0) <>	upd_rec.	cash_avail_cr_val	or
            nvl(fnd.cash_avail_credit	,0) <>	upd_rec.	cash_avail_credit	or
            nvl(fnd.cash_avail_cr_flag	,0) <>	upd_rec.	cash_avail_cr_flag	or
            nvl(fnd.cash_avail_cr_pcnt	,0) <>	upd_rec.	cash_avail_cr_pcnt	or
            nvl(fnd.cash_balance	,0) <>	upd_rec.	cash_balance	or
            nvl(fnd.cash_credit_limit	,0) <>	upd_rec.	cash_credit_limit	or
            nvl(fnd.cash_qty_auth_tdy1	,0) <>	upd_rec.	cash_qty_auth_tdy1	or
            nvl(fnd.cash_qty_auth_tdy2	,0) <>	upd_rec.	cash_qty_auth_tdy2	or
            nvl(fnd.cash_qty_auth_tdy3	,0) <>	upd_rec.	cash_qty_auth_tdy3	or
            nvl(fnd.cash_qty_auth_tdy4	,0) <>	upd_rec.	cash_qty_auth_tdy4	or
            nvl(fnd.cash_qty_auth_tdy5	,0) <>	upd_rec.	cash_qty_auth_tdy5	or
            nvl(fnd.cash_qty_auth_tdy6	,0) <>	upd_rec.	cash_qty_auth_tdy6	or
            nvl(fnd.cash_qty_auth_tdy7	,0) <>	upd_rec.	cash_qty_auth_tdy7	or
            nvl(fnd.cash_qty_decl_tdy1	,0) <>	upd_rec.	cash_qty_decl_tdy1	or
            nvl(fnd.cash_qty_decl_tdy2	,0) <>	upd_rec.	cash_qty_decl_tdy2	or
            nvl(fnd.cash_qty_decl_tdy3	,0) <>	upd_rec.	cash_qty_decl_tdy3	or
            nvl(fnd.cash_qty_decl_tdy4	,0) <>	upd_rec.	cash_qty_decl_tdy4	or
            nvl(fnd.cash_qty_decl_tdy5	,0) <>	upd_rec.	cash_qty_decl_tdy5	or
            nvl(fnd.cash_qty_decl_tdy6	,0) <>	upd_rec.	cash_qty_decl_tdy6	or
            nvl(fnd.cash_qty_decl_tdy7	,0) <>	upd_rec.	cash_qty_decl_tdy7	or
            nvl(fnd.cash_dispu_itm_qty	,0) <>	upd_rec.	cash_dispu_itm_qty	or
            nvl(fnd.cash_outstauth_qty	,0) <>	upd_rec.	cash_outstauth_qty	or
            nvl(fnd.cash_plan_qty	,0) <>	upd_rec.	cash_plan_qty	or
            nvl(fnd.cr_bal_refund_days	,0) <>	upd_rec.	cr_bal_refund_days	or
            nvl(fnd.cr_bal_refund_flag	,0) <>	upd_rec.	cr_bal_refund_flag	or
            nvl(fnd.cash_adv_svchg_ctd	,0) <>	upd_rec.	cash_adv_svchg_ctd	or
            nvl(fnd.service_charge_ctd	,0) <>	upd_rec.	service_charge_ctd	or
            nvl(fnd.curr_collector	,0) <>	upd_rec.	curr_collector	or
            nvl(fnd.curr_late_chg_val	,0) <>	upd_rec.	curr_late_chg_val	or
            nvl(fnd.curr_int_paid_ytd	,0) <>	upd_rec.	curr_int_paid_ytd	or
            nvl(fnd.fixed_pmt_pct	,0) <>	upd_rec.	fixed_pmt_pct	or
            nvl(fnd.fraud_reprtng_flag	,0) <>	upd_rec.	fraud_reprtng_flag	or
            nvl(fnd.finance_charge_mtd	,0) <>	upd_rec.	finance_charge_mtd	or
            nvl(fnd.insurance_prem_mtd	,0) <>	upd_rec.	insurance_prem_mtd	or
            nvl(fnd.service_charge_mtd	,0) <>	upd_rec.	service_charge_mtd	or
            nvl(fnd.auth_qty_1	,0) <>	upd_rec.	auth_qty_1	or
            nvl(fnd.auth_qty_2	,0) <>	upd_rec.	auth_qty_2	or
            nvl(fnd.auth_qty_3	,0) <>	upd_rec.	auth_qty_3	or
            nvl(fnd.auth_qty_4	,0) <>	upd_rec.	auth_qty_4	or
            nvl(fnd.auth_qty_5	,0) <>	upd_rec.	auth_qty_5	or
            nvl(fnd.auth_qty_6	,0) <>	upd_rec.	auth_qty_6	or
            nvl(fnd.auth_qty_7	,0) <>	upd_rec.	auth_qty_7	or
            nvl(fnd.auth_today_qty_1	,0) <>	upd_rec.	auth_today_qty_1	or
            nvl(fnd.auth_today_qty_2	,0) <>	upd_rec.	auth_today_qty_2	or
            nvl(fnd.auth_today_qty_3	,0) <>	upd_rec.	auth_today_qty_3	or
            nvl(fnd.auth_today_qty_4	,0) <>	upd_rec.	auth_today_qty_4	or
            nvl(fnd.auth_today_qty_5	,0) <>	upd_rec.	auth_today_qty_5	or
            nvl(fnd.auth_today_qty_6	,0) <>	upd_rec.	auth_today_qty_6	or
            nvl(fnd.auth_today_qty_7	,0) <>	upd_rec.	auth_today_qty_7	or
            nvl(fnd.cash_qty_ctd	,0) <>	upd_rec.	cash_qty_ctd	or
            nvl(fnd.late_fees_qty_ctd	,0) <>	upd_rec.	late_fees_qty_ctd	or
            nvl(fnd.decl_qty_tdy_1	,0) <>	upd_rec.	decl_qty_tdy_1	or
            nvl(fnd.decl_qty_tdy_2	,0) <>	upd_rec.	decl_qty_tdy_2	or
            nvl(fnd.decl_qty_tdy_3	,0) <>	upd_rec.	decl_qty_tdy_3	or
            nvl(fnd.decl_qty_tdy_4	,0) <>	upd_rec.	decl_qty_tdy_4	or
            nvl(fnd.decl_qty_tdy_5	,0) <>	upd_rec.	decl_qty_tdy_5	or
            nvl(fnd.decl_qty_tdy_6	,0) <>	upd_rec.	decl_qty_tdy_6	or
            nvl(fnd.decl_qty_tdy_7	,0) <>	upd_rec.	decl_qty_tdy_7	or
            nvl(fnd.memo_cr_qty	,0) <>	upd_rec.	memo_cr_qty	or
            nvl(fnd.plans_qty	,0) <>	upd_rec.	plans_qty	or
            nvl(fnd.unblked_cards_qty	,0) <>	upd_rec.	unblked_cards_qty	or
            nvl(fnd.late_chg_qty_ytd	,0) <>	upd_rec.	late_chg_qty_ytd	or
            nvl(fnd.pmt_120days	,0) <>	upd_rec.	pmt_120days	or
            nvl(fnd.pmt_120days_qty	,0) <>	upd_rec.	pmt_120days_qty	or
            nvl(fnd.pmt_150days	,0) <>	upd_rec.	pmt_150days	or
            nvl(fnd.pmt_150days_qty	,0) <>	upd_rec.	pmt_150days_qty	or
            nvl(fnd.pmt_180days	,0) <>	upd_rec.	pmt_180days	or
            nvl(fnd.pmt_180days_qty	,0) <>	upd_rec.	pmt_180days_qty	or
            nvl(fnd.pmt_210days	,0) <>	upd_rec.	pmt_210days	or
            nvl(fnd.pmt_210days_qty	,0) <>	upd_rec.	pmt_210days_qty	or
            nvl(fnd.pmt_30days	,0) <>	upd_rec.	pmt_30days	or
            nvl(fnd.pmt_30days_qty	,0) <>	upd_rec.	pmt_30days_qty	or
            nvl(fnd.pmt_60days	,0) <>	upd_rec.	pmt_60days	or
            nvl(fnd.pmt_60days_qty	,0) <>	upd_rec.	pmt_60days_qty	or
            nvl(fnd.pmt_90days	,0) <>	upd_rec.	pmt_90days	or
            nvl(fnd.pmt_90days_qty	,0) <>	upd_rec.	pmt_90days_qty	or
            nvl(fnd.pmt_ach_debit_qty	,0) <>	upd_rec.	pmt_ach_debit_qty	or
            nvl(fnd.pmt_ach_debit_type	,0) <>	upd_rec.	pmt_ach_debit_type	or
            nvl(fnd.pmt_ach_flag	,0) <>	upd_rec.	pmt_ach_flag	or
            nvl(fnd.pmt_ach_rt_qty	,0) <>	upd_rec.	pmt_ach_rt_qty	or
            nvl(fnd.pmt_consec	,0) <>	upd_rec.	pmt_consec	or
            nvl(fnd.pmt_days_delq	,0) <>	upd_rec.	pmt_days_delq	or
            nvl(fnd.pmt_delq_paid_1	,0) <>	upd_rec.	pmt_delq_paid_1	or
            nvl(fnd.pmt_delq_paid_2	,0) <>	upd_rec.	pmt_delq_paid_2	or
            nvl(fnd.pmt_delq_paid_3	,0) <>	upd_rec.	pmt_delq_paid_3	or
            nvl(fnd.pmt_delq_paid_4	,0) <>	upd_rec.	pmt_delq_paid_4	or
            nvl(fnd.pmt_delq_paid_5	,0) <>	upd_rec.	pmt_delq_paid_5	or
            nvl(fnd.pmt_delq_paid_6	,0) <>	upd_rec.	pmt_delq_paid_6	or
            nvl(fnd.pmt_delq_paid_7	,0) <>	upd_rec.	pmt_delq_paid_7	or
            nvl(fnd.pmt_delq_paid_8	,0) <>	upd_rec.	pmt_delq_paid_8	or
            nvl(fnd.pmt_delq_paid_9	,0) <>	upd_rec.	pmt_delq_paid_9	or
            nvl(fnd.pmt_grace_days	,0) <>	upd_rec.	pmt_grace_days	or
            nvl(fnd.pmt_last_requested	,0) <>	upd_rec.	pmt_last_requested	or
            nvl(fnd.pmt_repaid_qty	,0) <>	upd_rec.	pmt_repaid_qty	or
            nvl(fnd.pmt_past_ctr	,0) <>	upd_rec.	pmt_past_ctr	or
            nvl(fnd.pmt_potent_indic	,0) <>	upd_rec.	pmt_potent_indic	or
            nvl(fnd.pmt_prior_recency	,0) <>	upd_rec.	pmt_prior_recency	or
            nvl(fnd.pmt_ratios_1	,0) <>	upd_rec.	pmt_ratios_1	or
            nvl(fnd.pmt_ratios_2	,0) <>	upd_rec.	pmt_ratios_2	or
            nvl(fnd.pmt_ratios_3	,0) <>	upd_rec.	pmt_ratios_3	or
            nvl(fnd.pmt_ratios_4	,0) <>	upd_rec.	pmt_ratios_4	or
            nvl(fnd.pmt_ratios_5	,0) <>	upd_rec.	pmt_ratios_5	or
            nvl(fnd.pmt_ratios_6	,0) <>	upd_rec.	pmt_ratios_6	or
            nvl(fnd.pmt_ratios_7	,0) <>	upd_rec.	pmt_ratios_7	or
            nvl(fnd.pmt_ratios_8	,0) <>	upd_rec.	pmt_ratios_8	or
            nvl(fnd.pmt_ratios_9	,0) <>	upd_rec.	pmt_ratios_9	or
            nvl(fnd.pmt_ratios_10	,0) <>	upd_rec.	pmt_ratios_10	or
            nvl(fnd.pmt_ratios_11	,0) <>	upd_rec.	pmt_ratios_11	or
            nvl(fnd.pmt_ratios_12	,0) <>	upd_rec.	pmt_ratios_12	or
            nvl(fnd.pmt_reaged_by	,0) <>	upd_rec.	pmt_reaged_by	or
            nvl(fnd.pmt_recency	,0) <>	upd_rec.	pmt_recency	or
            nvl(fnd.pmt_skip_flag	,0) <>	upd_rec.	pmt_skip_flag	or
            nvl(fnd.pmt_tms_man_reage	,0) <>	upd_rec.	pmt_tms_man_reage	or
            nvl(fnd.pmt_times_reaged	,0) <>	upd_rec.	pmt_times_reaged	or
            nvl(fnd.pmt_total_due_val	,0) <>	upd_rec.	pmt_total_due_val	or
            nvl(fnd.pmt_tot_forgvn_val	,0) <>	upd_rec.	pmt_tot_forgvn_val	or
            nvl(fnd.prior_late_chg_val	,0) <>	upd_rec.	prior_late_chg_val	or
            nvl(fnd.waiv_cash_avail_cr	,0) <>	upd_rec.	waiv_cash_avail_cr	or
            nvl(fnd.waive_intr_chg	,0) <>	upd_rec.	waive_intr_chg	or
            nvl(fnd.waive_late_chg	,0) <>	upd_rec.	waive_late_chg	or
            nvl(fnd.waive_late_notice	,0) <>	upd_rec.	waive_late_notice	or
            nvl(fnd.waive_nsf_fee_ind	,0) <>	upd_rec.	waive_nsf_fee_ind	or
            nvl(fnd.waive_ovlm	,0) <>	upd_rec.	waive_ovlm	or
            nvl(fnd.waive_ovlm_notc	,0) <>	upd_rec.	waive_ovlm_notc	or
            nvl(fnd.waive_svc_chg	,0) <>	upd_rec.	waive_svc_chg	or
            nvl(fnd.ovlm_chg_ytd	,0) <>	upd_rec.	ovlm_chg_ytd	
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_cust_perf_day_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
            	cpy.	wfs_customer_no	,
            	cpy.	product_code_no	,
            	cpy.	run_date	,
            	cpy.	wfs_account_no	,
            	cpy.	auth_value_1	,
            	cpy.	auth_value_2	,
            	cpy.	auth_value_3	,
            	cpy.	auth_value_4	,
            	cpy.	auth_value_5	,
            	cpy.	auth_value_6	,
            	cpy.	auth_value_7	,
            	cpy.	auth_val_today_1	,
            	cpy.	auth_val_today_2	,
            	cpy.	auth_val_today_3	,
            	cpy.	auth_val_today_4	,
            	cpy.	auth_val_today_5	,
            	cpy.	auth_val_today_6	,
            	cpy.	auth_val_today_7	,
            	cpy.	cash_val_ctd	,
            	cpy.	coll_fees_val_ctd	,
            	cpy.	ins_fees_val_ctd	,
            	cpy.	late_fees_val_ctd	,
            	cpy.	memb_fees_val_ctd	,
            	cpy.	nsf_fees_val_ctd	,
            	cpy.	ovlm_fees_val_ctd	,
            	cpy.	decl_val_today_1	,
            	cpy.	decl_val_today_2	,
            	cpy.	decl_val_today_3	,
            	cpy.	decl_val_today_4	,
            	cpy.	decl_val_today_5	,
            	cpy.	decl_val_today_6	,
            	cpy.	decl_val_today_7	,
            	cpy.	last_debit_val	,
            	cpy.	last_pchs_val	,
            	cpy.	memo_credit_val	,
            	cpy.	memo_debit_val	,
            	cpy.	late_chg_val_ytd	,
            	cpy.	card_usage_flag	,
            	cpy.	cash_auth_val_tdy1	,
            	cpy.	cash_auth_val_tdy2	,
            	cpy.	cash_auth_val_tdy3	,
            	cpy.	cash_auth_val_tdy4	,
            	cpy.	cash_auth_val_tdy5	,
            	cpy.	cash_auth_val_tdy6	,
            	cpy.	cash_auth_val_tdy7	,
            	cpy.	cash_chgoff_val	,
            	cpy.	cash_decl_val_tdy1	,
            	cpy.	cash_decl_val_tdy2	,
            	cpy.	cash_decl_val_tdy3	,
            	cpy.	cash_decl_val_tdy4	,
            	cpy.	cash_decl_val_tdy5	,
            	cpy.	cash_decl_val_tdy6	,
            	cpy.	cash_decl_val_tdy7	,
            	cpy.	cash_dispu_itm_val	,
            	cpy.	cash_outstauth_val	,
            	cpy.	cash_avail_cr_val	,
            	cpy.	cash_avail_credit	,
            	cpy.	cash_avail_cr_flag	,
            	cpy.	cash_avail_cr_pcnt	,
            	cpy.	cash_balance	,
            	cpy.	cash_credit_limit	,
            	cpy.	cash_qty_auth_tdy1	,
            	cpy.	cash_qty_auth_tdy2	,
            	cpy.	cash_qty_auth_tdy3	,
            	cpy.	cash_qty_auth_tdy4	,
            	cpy.	cash_qty_auth_tdy5	,
            	cpy.	cash_qty_auth_tdy6	,
            	cpy.	cash_qty_auth_tdy7	,
            	cpy.	cash_qty_decl_tdy1	,
            	cpy.	cash_qty_decl_tdy2	,
            	cpy.	cash_qty_decl_tdy3	,
            	cpy.	cash_qty_decl_tdy4	,
            	cpy.	cash_qty_decl_tdy5	,
            	cpy.	cash_qty_decl_tdy6	,
            	cpy.	cash_qty_decl_tdy7	,
            	cpy.	cash_dispu_itm_qty	,
            	cpy.	cash_outstauth_qty	,
            	cpy.	cash_plan_qty	,
            	cpy.	cr_bal_refund_days	,
            	cpy.	cr_bal_refund_flag	,
            	cpy.	cash_adv_svchg_ctd	,
            	cpy.	service_charge_ctd	,
            	cpy.	curr_collector	,
            	cpy.	curr_late_chg_val	,
            	cpy.	curr_int_paid_ytd	,
            	cpy.	fixed_pmt_pct	,
            	cpy.	fraud_reprtng_flag	,
            	cpy.	finance_charge_mtd	,
            	cpy.	insurance_prem_mtd	,
            	cpy.	service_charge_mtd	,
            	cpy.	auth_qty_1	,
            	cpy.	auth_qty_2	,
            	cpy.	auth_qty_3	,
            	cpy.	auth_qty_4	,
            	cpy.	auth_qty_5	,
            	cpy.	auth_qty_6	,
            	cpy.	auth_qty_7	,
            	cpy.	auth_today_qty_1	,
            	cpy.	auth_today_qty_2	,
            	cpy.	auth_today_qty_3	,
            	cpy.	auth_today_qty_4	,
            	cpy.	auth_today_qty_5	,
            	cpy.	auth_today_qty_6	,
            	cpy.	auth_today_qty_7	,
            	cpy.	cash_qty_ctd	,
            	cpy.	late_fees_qty_ctd	,
            	cpy.	decl_qty_tdy_1	,
            	cpy.	decl_qty_tdy_2	,
            	cpy.	decl_qty_tdy_3	,
            	cpy.	decl_qty_tdy_4	,
            	cpy.	decl_qty_tdy_5	,
            	cpy.	decl_qty_tdy_6	,
            	cpy.	decl_qty_tdy_7	,
            	cpy.	memo_cr_qty	,
            	cpy.	plans_qty	,
            	cpy.	unblked_cards_qty	,
            	cpy.	late_chg_qty_ytd	,
            	cpy.	pmt_120days	,
            	cpy.	pmt_120days_qty	,
            	cpy.	pmt_150days	,
            	cpy.	pmt_150days_qty	,
            	cpy.	pmt_180days	,
            	cpy.	pmt_180days_qty	,
            	cpy.	pmt_210days	,
            	cpy.	pmt_210days_qty	,
            	cpy.	pmt_30days	,
            	cpy.	pmt_30days_qty	,
            	cpy.	pmt_60days	,
            	cpy.	pmt_60days_qty	,
            	cpy.	pmt_90days	,
            	cpy.	pmt_90days_qty	,
            	cpy.	pmt_ach_debit_qty	,
            	cpy.	pmt_ach_debit_type	,
            	cpy.	pmt_ach_flag	,
            	cpy.	pmt_ach_rt_qty	,
            	cpy.	pmt_consec	,
            	cpy.	pmt_days_delq	,
            	cpy.	pmt_delq_paid_1	,
            	cpy.	pmt_delq_paid_2	,
            	cpy.	pmt_delq_paid_3	,
            	cpy.	pmt_delq_paid_4	,
            	cpy.	pmt_delq_paid_5	,
            	cpy.	pmt_delq_paid_6	,
            	cpy.	pmt_delq_paid_7	,
            	cpy.	pmt_delq_paid_8	,
            	cpy.	pmt_delq_paid_9	,
            	cpy.	pmt_grace_days	,
            	cpy.	pmt_last_requested	,
            	cpy.	pmt_repaid_qty	,
            	cpy.	pmt_past_ctr	,
            	cpy.	pmt_potent_indic	,
            	cpy.	pmt_prior_recency	,
            	cpy.	pmt_ratios_1	,
            	cpy.	pmt_ratios_2	,
            	cpy.	pmt_ratios_3	,
            	cpy.	pmt_ratios_4	,
            	cpy.	pmt_ratios_5	,
            	cpy.	pmt_ratios_6	,
            	cpy.	pmt_ratios_7	,
            	cpy.	pmt_ratios_8	,
            	cpy.	pmt_ratios_9	,
            	cpy.	pmt_ratios_10	,
            	cpy.	pmt_ratios_11	,
            	cpy.	pmt_ratios_12	,
            	cpy.	pmt_reaged_by	,
            	cpy.	pmt_recency	,
            	cpy.	pmt_skip_flag	,
            	cpy.	pmt_tms_man_reage	,
            	cpy.	pmt_times_reaged	,
            	cpy.	pmt_total_due_val	,
            	cpy.	pmt_tot_forgvn_val	,
            	cpy.	prior_late_chg_val	,
            	cpy.	waiv_cash_avail_cr	,
            	cpy.	waive_intr_chg	,
            	cpy.	waive_late_chg	,
            	cpy.	waive_late_notice	,
            	cpy.	waive_nsf_fee_ind	,
            	cpy.	waive_ovlm	,
            	cpy.	waive_ovlm_notc,
             	cpy.	waive_svc_chg	,
            	cpy.	ovlm_chg_ytd	 
      FROM   stg_vsn_cust_perf_day_cpy cpy
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
    from   stg_vsn_cust_perf_day_cpy
    where  sys_process_code = 'N';

    if g_recs_read > 300000 then
       l_text := 'TRUNCATE CUST_PERF_DAY STARTED AT '||
       to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       execute immediate 'truncate table dwh_wfs_foundation.fnd_wfs_cust_perf_day';
    end if;

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
end wh_fnd_wfs_206u;
