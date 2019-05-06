--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_204HSP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_204HSP" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_perf_60dy fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_cust_perf_60dy
--               Output - fnd_wfs_cust_perf_60dy
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


g_wfs_customer_no    stg_vsn_cust_perf_60dy.wfs_customer_no%type; 
g_product_code_no    stg_vsn_cust_perf_60dy.product_code_no%type; 
g_run_date           stg_vsn_cust_perf_60dy.run_date%type; 
g_wfs_account_no     stg_vsn_cust_perf_60dy.wfs_account_no%type;

   
g_date               date          := trunc(sysdate);
g_min_run_date       date          := trunc(sysdate);
g_max_run_date       date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_204U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_CUST_PERF_60DAY EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_cust_perf_60dy
where (wfs_customer_no,wfs_account_no,
product_code_no,
run_date)
IN
(select wfs_customer_no,wfs_account_no,
product_code_no,
run_date
from stg_vsn_cust_perf_60dy 
group by wfs_customer_no,wfs_account_no,
product_code_no,
run_date
HAVING COUNT(*) > 1) 
order by wfs_customer_no,wfs_account_no,
product_code_no,
run_date,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_cust_perf_60dy is
select /*+ FULL(cpy)  parallel (4) */  
              cpy.*
      from    stg_vsn_cust_perf_60dy cpy,
              fnd_wfs_cust_perf_60dy fnd
      where   cpy.wfs_customer_no       = fnd.wfs_customer_no and 
              cpy.wfs_account_no        = fnd.wfs_account_no  and   
              cpy.product_code_no       = fnd.product_code_no and   
              cpy.run_date     = fnd.run_date    and 
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.wfs_customer_no,cpy.wfs_account_no ,
              cpy.product_code_no,
              cpy.run_date,
              cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no    := 0; 
   g_wfs_account_no     := 0;
   g_product_code_no    := 0;
   g_run_date           := '1 Jan 2000';


for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no   = g_wfs_customer_no and
        dupp_record.wfs_account_no    = g_wfs_account_no and
        dupp_record.product_code_no   = g_product_code_no and
        dupp_record.run_date          = g_run_date then
        update stg_vsn_cust_perf_60dy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_wfs_customer_no    := dupp_record.wfs_customer_no; 
    g_wfs_account_no     := dupp_record.wfs_account_no; 
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
             'Dummy wh_fnd_wfs_204U',
             0	,
             ' ',
             g_date,
             1
      from   stg_vsn_cust_perf_60dy cpy
 
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
      from   stg_vsn_cust_perf_60dy cpy
 
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
      from   stg_vsn_cust_perf_60dy  cpy
 
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
      
      insert /*+ APPEND parallel (fnd,4) */ into fnd_wfs_cust_perf_60dy fnd
      select /*+ FULL(cpy)  parallel (cpy,4) */
            	cpy.	wfs_customer_no	,
            	cpy.	product_code_no	,
            	cpy.	run_date	,
            	cpy.	wfs_account_no	,
            	cpy.	cycle_due	,
            	cpy.	delinquency_cycle	,
            	cpy.	billing_cycle	,
            	cpy.	account_status	,
            	'N',   --cpy.	pending_chgoff_ind	,
            	cpy.	chgoff_date	,
            	cpy.	block_code_1	,
            	cpy.	block_code_2	,
            	cpy.	date_block_code_1	,
            	cpy.	date_block_code_2	,
            	cpy.	curr_beh_score	,
            	'' ,   --cpy.	curr_beh_scr_band	,
            	'0',   --cpy.	collector_id	,
            	'0',   --cpy.	cta_class	,
            	cpy.	cms_class	,
            	cpy.	triad_class	,
            	cpy.	triad_class_date	,
            	'0',   --cpy.	pend_chgoff_code	,
            	cpy.	pend_pct	,
            	cpy.	pend_pct_eff_date	,
            	cpy.	pend_pct_end_date	,
            	cpy.	credit_limit	,
            	cpy.	current_balance	,
            	cpy.	deposit_required	,
            	cpy.	nsf_val_ctd	,
            	cpy.	pchs_val_ctd	,
            	cpy.	recov_fees_val_ctd	,
            	cpy.	returns_val_ctd	,
            	cpy.	dispu_items_val	,
            	cpy.	cash_val_ctd	,
            	cpy.	int_paid_val_ctd	,
            	cpy.	pmts_val_ltd	,
            	cpy.	purchases_ltd_val	,
            	cpy.	returns_val_ltd	,
            	cpy.	cash_val_ytd	,
            	cpy.	pmts_val_ytd	,
            	cpy.	purchases_val_ytd	,
            	cpy.	returns_val_ytd	,
            	cpy.	mth_bal	,
            	cpy.	mth_purch	,
            	cpy.	nsf_qty_ctd	,
            	cpy.	purchases_qty_ctd	,
            	cpy.	returns_qty_ctd	,
            	cpy.	dispu_items_qty	,
            	cpy.	cash_qty_ltd	,
            	cpy.	pmts_qty_ltd	,
            	cpy.	purchases_qty_ltd	,
            	cpy.	returns_qty_ltd	,
            	cpy.	nsf_qty	,
            	cpy.	paid_out_qty	,
            	cpy.	cash_qty_ytd	,
            	cpy.	pmts_qty_ytd	,
            	cpy.	purchases_qty_ytd	,
            	cpy.	returns_qty_ytd	,
            	cpy.	open_to_buy	,
            	cpy.	pmt_val_prepaid	,
            	cpy.	pmt_calc_pre_adj	,
            	cpy.	pmt_ctd	,
            	cpy.	pmt_qty_ctd	,
            	cpy.	pmt_curr_due	,
            	cpy.	pmt_past_due	,
            	cpy.	psc_curr_bal	,
            	cpy.	collecting_agency	,
            	cpy.	cash_budget_bal	,
            	cpy.	write_off_ind	,
            	cpy.	write_off_date	,
            	cpy.	write_off_value	,
              g_date as last_updated_date
       from  stg_vsn_cust_perf_60dy cpy
       where  not exists 
      (select * from fnd_wfs_cust_perf_60dy 
       where  wfs_customer_no    = cpy.wfs_customer_no and
              wfs_account_no     = cpy.wfs_account_no  and
              product_code_no    = cpy.product_code_no and
              run_date           = cpy.run_date and
              run_date           between g_min_run_date and g_max_run_date)  
         and  sys_process_code   = 'N';
 

      g_recs_inserted := g_recs_inserted + sql%rowcount;
      
      commit;
      
--/*+ nl_aj */ 

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



FOR upd_rec IN c_stg_vsn_cust_perf_60dy
   loop
     update fnd_wfs_cust_perf_60dy fnd 
     set    fnd.	cycle_due	=	upd_rec.	cycle_due	,
            fnd.	delinquency_cycle	=	upd_rec.	delinquency_cycle	,
            fnd.	billing_cycle	=	upd_rec.	billing_cycle	,
            fnd.	account_status	=	upd_rec.	account_status	,
--            fnd.	pending_chgoff_ind	=	upd_rec.	pending_chgoff_ind	,
            fnd.	chgoff_date	=	upd_rec.	chgoff_date	,
            fnd.	block_code_1	=	upd_rec.	block_code_1	,
            fnd.	block_code_2	=	upd_rec.	block_code_2	,
            fnd.	date_block_code_1	=	upd_rec.	date_block_code_1	,
            fnd.	date_block_code_2	=	upd_rec.	date_block_code_2	,
            fnd.	curr_beh_score	=	upd_rec.	curr_beh_score	,
--            fnd.	curr_beh_scr_band	=	upd_rec.	curr_beh_scr_band	,
--            fnd.	collector_id	=	upd_rec.	collector_id	,
--            fnd.	cta_class	=	upd_rec.	cta_class	,
            fnd.	cms_class	=	upd_rec.	cms_class	,
            fnd.	triad_class	=	upd_rec.	triad_class	,
            fnd.	triad_class_date	=	upd_rec.	triad_class_date	,
--            fnd.	pend_chgoff_code	=	upd_rec.	pend_chgoff_code	,
            fnd.	pend_pct	=	upd_rec.	pend_pct	,
            fnd.	pend_pct_eff_date	=	upd_rec.	pend_pct_eff_date	,
            fnd.	pend_pct_end_date	=	upd_rec.	pend_pct_end_date	,
            fnd.	credit_limit	=	upd_rec.	credit_limit	,
            fnd.	current_balance	=	upd_rec.	current_balance	,
            fnd.	deposit_required	=	upd_rec.	deposit_required	,
            fnd.	nsf_val_ctd	=	upd_rec.	nsf_val_ctd	,
            fnd.	pchs_val_ctd	=	upd_rec.	pchs_val_ctd	,
            fnd.	recov_fees_val_ctd	=	upd_rec.	recov_fees_val_ctd	,
            fnd.	returns_val_ctd	=	upd_rec.	returns_val_ctd	,
            fnd.	dispu_items_val	=	upd_rec.	dispu_items_val	,
            fnd.	cash_val_ctd	=	upd_rec.	cash_val_ctd	,
            fnd.	int_paid_val_ctd	=	upd_rec.	int_paid_val_ctd	,
            fnd.	pmts_val_ltd	=	upd_rec.	pmts_val_ltd	,
            fnd.	purchases_ltd_val	=	upd_rec.	purchases_ltd_val	,
            fnd.	returns_val_ltd	=	upd_rec.	returns_val_ltd	,
            fnd.	cash_val_ytd	=	upd_rec.	cash_val_ytd	,
            fnd.	pmts_val_ytd	=	upd_rec.	pmts_val_ytd	,
            fnd.	purchases_val_ytd	=	upd_rec.	purchases_val_ytd	,
            fnd.	returns_val_ytd	=	upd_rec.	returns_val_ytd	,
            fnd.	mth_bal	=	upd_rec.	mth_bal	,
            fnd.	mth_purch	=	upd_rec.	mth_purch	,
            fnd.	nsf_qty_ctd	=	upd_rec.	nsf_qty_ctd	,
            fnd.	purchases_qty_ctd	=	upd_rec.	purchases_qty_ctd	,
            fnd.	returns_qty_ctd	=	upd_rec.	returns_qty_ctd	,
            fnd.	dispu_items_qty	=	upd_rec.	dispu_items_qty	,
            fnd.	cash_qty_ltd	=	upd_rec.	cash_qty_ltd	,
            fnd.	pmts_qty_ltd	=	upd_rec.	pmts_qty_ltd	,
            fnd.	purchases_qty_ltd	=	upd_rec.	purchases_qty_ltd	,
            fnd.	returns_qty_ltd	=	upd_rec.	returns_qty_ltd	,
            fnd.	nsf_qty	=	upd_rec.	nsf_qty	,
            fnd.	paid_out_qty	=	upd_rec.	paid_out_qty	,
            fnd.	cash_qty_ytd	=	upd_rec.	cash_qty_ytd	,
            fnd.	pmts_qty_ytd	=	upd_rec.	pmts_qty_ytd	,
            fnd.	purchases_qty_ytd	=	upd_rec.	purchases_qty_ytd	,
            fnd.	returns_qty_ytd	=	upd_rec.	returns_qty_ytd	,
            fnd.	open_to_buy	=	upd_rec.	open_to_buy	,
            fnd.	pmt_val_prepaid	=	upd_rec.	pmt_val_prepaid	,
            fnd.	pmt_calc_pre_adj	=	upd_rec.	pmt_calc_pre_adj	,
            fnd.	pmt_ctd	=	upd_rec.	pmt_ctd	,
            fnd.	pmt_qty_ctd	=	upd_rec.	pmt_qty_ctd	,
            fnd.	pmt_curr_due	=	upd_rec.	pmt_curr_due	,
            fnd.	pmt_past_due	=	upd_rec.	pmt_past_due	,
            fnd.	psc_curr_bal	=	upd_rec.	psc_curr_bal	,
            fnd.	collecting_agency	=	upd_rec.	collecting_agency	,
            fnd.	cash_budget_bal	=	upd_rec.	cash_budget_bal	,
            fnd.	write_off_ind	=	upd_rec.	write_off_ind	,
            fnd.	write_off_date	=	upd_rec.	write_off_date	,
            fnd.	write_off_value	=	upd_rec.	write_off_value	,
            fnd.  last_updated_date   = g_date
     where  fnd.	wfs_customer_no	      =	upd_rec.	wfs_customer_no and
            fnd.	wfs_account_no	      =	upd_rec.	wfs_account_no  and
            fnd.	product_code_no	      =	upd_rec.	product_code_no	and
            fnd.	run_date	    =	upd_rec.	run_date	and
            FND.LAST_UPDATED_DATE < UPD_REC.SYS_LOAD_DATE AND
            (             

            nvl(fnd.	cycle_due	,0) <>	upd_rec.	cycle_due	or
            nvl(fnd.delinquency_cycle	,0) <>	upd_rec.	delinquency_cycle	or
            nvl(fnd.billing_cycle	,0) <>	upd_rec.	billing_cycle	or
            nvl(fnd.account_status	,0) <>	upd_rec.	account_status	or
--            nvl(fnd.pending_chgoff_ind	,0) <>	upd_rec.	pending_chgoff_ind	or
            nvl(fnd.chgoff_date	,'1 Jan 1900') <>	upd_rec.	chgoff_date	or
            nvl(fnd.block_code_1	,0) <>	upd_rec.	block_code_1	or
            nvl(fnd.block_code_2	,0) <>	upd_rec.	block_code_2	or
            nvl(fnd.date_block_code_1	,'1 Jan 1900') <>	upd_rec.	date_block_code_1	or
            nvl(fnd.date_block_code_2	,'1 Jan 1900') <>	upd_rec.	date_block_code_2	or
            nvl(fnd.curr_beh_score	,0) <>	upd_rec.	curr_beh_score	or
--            nvl(fnd.curr_beh_scr_band	,0) <>	upd_rec.	curr_beh_scr_band	or
--            nvl(fnd.collector_id	,0) <>	upd_rec.	collector_id	or
--            nvl(fnd.cta_class	,0) <>	upd_rec.	cta_class	or
            nvl(fnd.cms_class	,0) <>	upd_rec.	cms_class	or
            nvl(fnd.triad_class	,0) <>	upd_rec.	triad_class	or
            nvl(fnd.triad_class_date	,'1 Jan 1900') <>	upd_rec.	triad_class_date	or
--            nvl(fnd.pend_chgoff_code	,0) <>	upd_rec.	pend_chgoff_code	or
            nvl(fnd.pend_pct	,0) <>	upd_rec.	pend_pct	or
            nvl(fnd.pend_pct_eff_date	,'1 Jan 1900') <>	upd_rec.	pend_pct_eff_date	or
            nvl(fnd.pend_pct_end_date	,'1 Jan 1900') <>	upd_rec.	pend_pct_end_date	or
            nvl(fnd.credit_limit	,0) <>	upd_rec.	credit_limit	or
            nvl(fnd.current_balance	,0) <>	upd_rec.	current_balance	or
            nvl(fnd.deposit_required	,0) <>	upd_rec.	deposit_required	or
            nvl(fnd.nsf_val_ctd	,0) <>	upd_rec.	nsf_val_ctd	or
            nvl(fnd.pchs_val_ctd	,0) <>	upd_rec.	pchs_val_ctd	or
            nvl(fnd.recov_fees_val_ctd	,0) <>	upd_rec.	recov_fees_val_ctd	or
            nvl(fnd.returns_val_ctd	,0) <>	upd_rec.	returns_val_ctd	or
            nvl(fnd.dispu_items_val	,0) <>	upd_rec.	dispu_items_val	or
            nvl(fnd.cash_val_ctd	,0) <>	upd_rec.	cash_val_ctd	or
            nvl(fnd.int_paid_val_ctd	,0) <>	upd_rec.	int_paid_val_ctd	or
            nvl(fnd.pmts_val_ltd	,0) <>	upd_rec.	pmts_val_ltd	or
            nvl(fnd.purchases_ltd_val	,0) <>	upd_rec.	purchases_ltd_val	or
            nvl(fnd.returns_val_ltd	,0) <>	upd_rec.	returns_val_ltd	or
            nvl(fnd.cash_val_ytd	,0) <>	upd_rec.	cash_val_ytd	or
            nvl(fnd.pmts_val_ytd	,0) <>	upd_rec.	pmts_val_ytd	or
            nvl(fnd.purchases_val_ytd	,0) <>	upd_rec.	purchases_val_ytd	or
            nvl(fnd.returns_val_ytd	,0) <>	upd_rec.	returns_val_ytd	or
            nvl(fnd.mth_bal	,0) <>	upd_rec.	mth_bal	or
            nvl(fnd.mth_purch	,0) <>	upd_rec.	mth_purch	or
            nvl(fnd.nsf_qty_ctd	,0) <>	upd_rec.	nsf_qty_ctd	or
            nvl(fnd.purchases_qty_ctd	,0) <>	upd_rec.	purchases_qty_ctd	or
            nvl(fnd.returns_qty_ctd	,0) <>	upd_rec.	returns_qty_ctd	or
            nvl(fnd.dispu_items_qty	,0) <>	upd_rec.	dispu_items_qty	or
            nvl(fnd.cash_qty_ltd	,0) <>	upd_rec.	cash_qty_ltd	or
            nvl(fnd.pmts_qty_ltd	,0) <>	upd_rec.	pmts_qty_ltd	or
            nvl(fnd.purchases_qty_ltd	,0) <>	upd_rec.	purchases_qty_ltd	or
            nvl(fnd.returns_qty_ltd	,0) <>	upd_rec.	returns_qty_ltd	or
            nvl(fnd.nsf_qty	,0) <>	upd_rec.	nsf_qty	or
            nvl(fnd.paid_out_qty	,0) <>	upd_rec.	paid_out_qty	or
            nvl(fnd.cash_qty_ytd	,0) <>	upd_rec.	cash_qty_ytd	or
            nvl(fnd.pmts_qty_ytd	,0) <>	upd_rec.	pmts_qty_ytd	or
            nvl(fnd.purchases_qty_ytd	,0) <>	upd_rec.	purchases_qty_ytd	or
            nvl(fnd.returns_qty_ytd	,0) <>	upd_rec.	returns_qty_ytd	or
            nvl(fnd.open_to_buy	,0) <>	upd_rec.	open_to_buy	or
            nvl(fnd.pmt_val_prepaid	,0) <>	upd_rec.	pmt_val_prepaid	or
            nvl(fnd.pmt_calc_pre_adj	,0) <>	upd_rec.	pmt_calc_pre_adj	or
            nvl(fnd.pmt_ctd	,0) <>	upd_rec.	pmt_ctd	or
            nvl(fnd.pmt_qty_ctd	,0) <>	upd_rec.	pmt_qty_ctd	or
            nvl(fnd.pmt_curr_due	,0) <>	upd_rec.	pmt_curr_due	or
            nvl(fnd.pmt_past_due	,0) <>	upd_rec.	pmt_past_due	or
            nvl(fnd.psc_curr_bal	,0) <>	upd_rec.	psc_curr_bal	or
            nvl(fnd.collecting_agency	,0) <>	upd_rec.	collecting_agency	or
            nvl(fnd.cash_budget_bal	,0) <>	upd_rec.	cash_budget_bal	or
            nvl(fnd.write_off_ind	,0) <>	upd_rec.	write_off_ind	or
            nvl(fnd.write_off_date	,'1 Jan 1900') <>	upd_rec.	write_off_date	or
            nvl(fnd.write_off_value	,0) <>	upd_rec.	write_off_value 
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_cust_perf_60dy_hsp hsp
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
            	cpy.	cycle_due	,
            	cpy.	delinquency_cycle	,
            	cpy.	billing_cycle	,
            	cpy.	account_status	,
            	cpy.	pending_chgoff_ind	,
            	cpy.	chgoff_date	,
            	cpy.	block_code_1	,
            	cpy.	block_code_2	,
            	cpy.	date_block_code_1	,
            	cpy.	date_block_code_2	,
            	cpy.	curr_beh_score	,
            	cpy.	curr_beh_scr_band	,
            	cpy.	collector_id	,
            	cpy.	cta_class	,
            	cpy.	cms_class	,
            	cpy.	triad_class	,
            	cpy.	triad_class_date	,
            	cpy.	pend_chgoff_code	,
            	cpy.	pend_pct	,
            	cpy.	pend_pct_eff_date	,
            	cpy.	pend_pct_end_date	,
            	cpy.	credit_limit	,
            	cpy.	current_balance	,
            	cpy.	deposit_required	,
            	cpy.	nsf_val_ctd	,
            	cpy.	pchs_val_ctd	,
            	cpy.	recov_fees_val_ctd	,
            	cpy.	returns_val_ctd	,
            	cpy.	dispu_items_val	,
            	cpy.	cash_val_ctd	,
            	cpy.	int_paid_val_ctd	,
            	cpy.	pmts_val_ltd	,
            	cpy.	purchases_ltd_val	,
            	cpy.	returns_val_ltd	,
            	cpy.	cash_val_ytd	,
            	cpy.	pmts_val_ytd	,
            	cpy.	purchases_val_ytd	,
            	cpy.	returns_val_ytd	,
            	cpy.	mth_bal	,
            	cpy.	mth_purch	,
            	cpy.	nsf_qty_ctd	,
            	cpy.	purchases_qty_ctd	,
            	cpy.	returns_qty_ctd	,
            	cpy.	dispu_items_qty	,
            	cpy.	cash_qty_ltd	,
            	cpy.	pmts_qty_ltd	,
            	cpy.	purchases_qty_ltd	,
            	cpy.	returns_qty_ltd	,
            	cpy.	nsf_qty	,
            	cpy.	paid_out_qty	,
            	cpy.	cash_qty_ytd	,
            	cpy.	pmts_qty_ytd	,
            	cpy.	purchases_qty_ytd	,
            	cpy.	returns_qty_ytd	,
            	cpy.	open_to_buy	,
            	cpy.	pmt_val_prepaid	,
            	cpy.	pmt_calc_pre_adj	,
            	cpy.	pmt_ctd	,
            	cpy.	pmt_qty_ctd	,
            	cpy.	pmt_curr_due	,
            	cpy.	pmt_past_due	,
            	cpy.	psc_curr_bal	,
            	cpy.	collecting_agency	,
            	cpy.	cash_budget_bal	,
            	cpy.	write_off_ind	,
            	cpy.	write_off_date	,
            	cpy.	write_off_value	 
      FROM   stg_vsn_cust_perf_60dy cpy
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
    from   stg_vsn_cust_perf_60dy
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
end wh_fnd_wfs_204HSP;
