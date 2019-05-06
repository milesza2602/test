--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_232U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_232U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_tran_plan_seg fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_tran_plan_seg_cpy
--               Output - fnd_wfs_tran_plan_seg
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


g_wfs_customer_no    stg_vsn_tran_plan_seg_cpy.wfs_customer_no%type;  
g_product_code_no    stg_vsn_tran_plan_seg_cpy.product_code_no%type; 
g_plan_no            stg_vsn_tran_plan_seg_cpy.plan_no%type;  
g_plan_date          stg_vsn_tran_plan_seg_cpy.plan_date%type; 
g_sequence_no        stg_vsn_tran_plan_seg_cpy.sequence_no%type;  
   
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_232U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_TRAN_PLAN_SEG EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_tran_plan_seg_cpy
where (wfs_customer_no,
product_code_no,
plan_no,
plan_date,
sequence_no)
in
(select wfs_customer_no,
product_code_no,
plan_no,
plan_date,
sequence_no
from stg_vsn_tran_plan_seg_cpy 
group by wfs_customer_no,
product_code_no,
plan_no,
plan_date,
sequence_no 
having count(*) > 1) 
order by wfs_customer_no,
product_code_no,
plan_no,
plan_date,
sequence_no ,sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_tran_plan_seg is
select /*+ FULL(cpy)  parallel (cpy,2) */  
              cpy.*
      from    stg_vsn_tran_plan_seg_cpy cpy,
              fnd_wfs_tran_plan_seg fnd
      where   cpy.wfs_customer_no       = fnd.wfs_customer_no and             
              cpy.product_code_no       = fnd.product_code_no    and   
              cpy.plan_no               = fnd.plan_no    and 
              cpy.plan_date             = fnd.plan_date    and 
              cpy.sequence_no           = fnd.sequence_no    and 
              cpy.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              cpy.wfs_customer_no,
              cpy.product_code_no,
              cpy.plan_no,
              cpy.plan_date,
              cpy.sequence_no,cpy.sys_source_batch_id,cpy.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_wfs_customer_no    := 0; 
   g_product_code_no    := 0;
   g_plan_no            := 0; 
   g_plan_date          := '1 Jan 2000';
   g_sequence_no        := 0;    

for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no   = g_wfs_customer_no and
        dupp_record.product_code_no   = g_product_code_no and
        dupp_record.plan_no           = g_plan_no and
        dupp_record.plan_date         = g_plan_date and
        dupp_record.sequence_no       = g_sequence_no     then
        update stg_vsn_tran_plan_seg_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_wfs_customer_no    := dupp_record.wfs_customer_no; 
    g_product_code_no    := dupp_record.product_code_no;
    g_plan_no            := dupp_record.plan_no; 
    g_plan_date          := dupp_record.plan_date;
    g_sequence_no        := dupp_record.sequence_no ;      
 

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
             'Dummy wh_fnd_wfs_232U',
             0	,
             ' ',
             g_date,
             1
      from   stg_vsn_tran_plan_seg_cpy cpy
 
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
      from   stg_vsn_tran_plan_seg_cpy cpy
 
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
      from   stg_vsn_tran_plan_seg_cpy  cpy
 
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_tran_plan_seg fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             	cpy.	product_code_no	,
            	cpy.	plan_no	,
            	cpy.	plan_date	,
            	cpy.	wfs_customer_no	,
            	cpy.	sequence_no	,
            	cpy.	accr_held_chgoff	,
            	cpy.	wfs_account_no	,
            	cpy.	acrd_int	,
            	cpy.	add_on_flag	,
            	cpy.	adj_rate1	,
            	cpy.	adj_rate2	,
            	cpy.	adj_rate3	,
            	cpy.	adj_rate4	,
            	cpy.	aggr_bal	,
            	cpy.	amt_cr	,
            	cpy.	amt_db	,
            	cpy.	amt_last_pmt	,
            	cpy.	aprbal_subjchg	,
            	cpy.	aprctd_cashadv_pnd	,
            	cpy.	aprctd_svcchg_pend	,
            	cpy.	apr_svc_chg_tot	,
            	cpy.	bal_xfr_mth_rem	,
            	cpy.	base_rate1	,
            	cpy.	base_rate2	,
            	cpy.	base_rate3	,
            	cpy.	base_rate4	,
            	cpy.	beg_bal	,
            	cpy.	bs_var1	,
            	cpy.	bs_var2	,
            	cpy.	bs_var3	,
            	cpy.	bs_var4	,
            	cpy.	calc_rate1	,
            	cpy.	calc_rate2	,
            	cpy.	calc_rate3	,
            	cpy.	calc_rate4	,
            	cpy.	cap_adj_rate1	,
            	cpy.	cap_adj_rate2	,
            	cpy.	cap_adj_rate3	,
            	cpy.	cap_adj_rate4	,
            	cpy.	coll_bnp	,
            	cpy.	coll_bnp_chgoff	,
            	cpy.	consol_pmt_flag	,
            	cpy.	consol_stmt_flag	,
            	cpy.	ctl_plan	,
            	cpy.	curr_bal	,
            	cpy.	curr_mth_db	,
            	cpy.	cust_db_amt	,
            	cpy.	cust_db_nbr	,
            	cpy.	date_beg_billing	,
            	cpy.	date_beg_int	,
            	cpy.	date_beg_pmt	,
            	cpy.	date_last_maint	,
            	cpy.	date_last_pmt	,
            	cpy.	date_orig_pmt_due	,
            	cpy.	date_paid_out	,
            	cpy.	def_agg_bal	,
            	cpy.	def_agg_days	,
            	cpy.	deferred_int	,
            	cpy.	dispu_bal	,
            	cpy.	dsclsr_days	,
            	cpy.	eff_date	,
            	cpy.	eff_rate_chg_date	,
            	cpy.	fixed_pmt_amt_amps/100	,
            	cpy.	gl_bal	,
            	cpy.	grace_day_flag	,
            	cpy.	hi_bal_amps	,
            	cpy.	ins_bnp	,
            	cpy.	int_accr_method	,
            	cpy.	int_bnp	,
            	cpy.	amps_insbnp_chgoff	,
            	cpy.	int_bnp_chgoff	,
            	cpy.	int_bnp_last_stmt	,
            	cpy.	int_rate	,
            	cpy.	intr_st	,
            	cpy.	interest_status	,
            	cpy.	int_table_nbr	,
            	cpy.	int_tbl_var1	,
            	cpy.	int_tbl_var2	,
            	cpy.	int_tbl_var3	,
            	cpy.	int_tbl_var4	,
            	cpy.	last_int_tbl_used	,
            	cpy.	late_chg_bnp	,
            	cpy.	late_chgbnp_chgoff	,
            	cpy.	limit_indctr	,
            	cpy.	limit1	,
            	cpy.	limit2	,
            	cpy.	limit3	,
            	cpy.	limit4	,
            	cpy.	loan_amount	,
            	cpy.	member_bnp	,
            	cpy.	member_bnp_chgoff	,
            	cpy.	misc_ctd	,
            	cpy.	nbr_cr	,
            	cpy.	nbr_db	,
            	cpy.	nbr_hist	,
            	cpy.	new_db_sw	,
            	cpy.	nsf_bnp	,
            	cpy.	nsf_bnp_chgoff	,
            	cpy.	oib_appl_payment	,
            	cpy.	oib_appl_reversal	,
            	cpy.	ovlm_bnp	,
            	cpy.	ovlm_bnp_chgoff	,
            	cpy.	pct_level	,
            	cpy.	plan_type	,
            	cpy.	pmt_ctd_amps	,
            	cpy.	pmt_flag	,
            	cpy.	pmt_flagmaint_date	,
            	cpy.	pmt_last_rqtd	,
            	cpy.	pmt_tbl_hi_bal	,
            	cpy.	prin_bal	,
            	cpy.	prin_bal_chgoff	,
            	cpy.	prior_mth_db	,
            	cpy.	prior_mth_def_int	,
            	cpy.	rate_table_occ_ind	,
            	cpy.	rate_type	,
            	cpy.	rec_type	,
            	cpy.	recv_bnp	,
            	cpy.	recv_bnp_chgoff	,
            	cpy.	rit_nbr	,
            	cpy.	rtfctn_int_tbl	,
            	cpy.	rtfctn_level	,
            	cpy.	rtfctn_method	,
            	cpy.	si_nbr	,
            	cpy.	si_org	,
            	cpy.	sit_pct_level	,
            	cpy.	spc_int_tbl	,
            	cpy.	svc_bnp	,
            	cpy.	svc_bnp_chgoff	,
            	cpy.	tot_due	,
            	cpy.	usury_adj_rate1	,
            	cpy.	usury_adj_rate2	,
            	cpy.	usury_adj_rate3	,
            	cpy.	usury_adj_rate4	,
            	cpy.	var_pct	,
            	cpy.	ytd_int	,
             g_date as last_updated_date
       from  stg_vsn_tran_plan_seg_cpy cpy
       where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_tran_plan_seg 
       where  wfs_customer_no     = cpy.wfs_customer_no and
              product_code_no     = cpy.product_code_no and
              plan_no             = cpy.plan_no and
              plan_date   = cpy.plan_date and
              sequence_no           = cpy.sequence_no )
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



for upd_rec in c_stg_vsn_tran_plan_seg
   loop
     update fnd_wfs_tran_plan_seg fnd 
     set    fnd.	accr_held_chgoff	  =	upd_rec.	accr_held_chgoff	,
            fnd.	wfs_account_no      =	upd_rec.	wfs_account_no	,
            fnd.	acrd_int          	=	upd_rec.	acrd_int	,
            fnd.	add_on_flag	        =	upd_rec.	add_on_flag	,
            fnd.	adj_rate1         	=	upd_rec.	adj_rate1	,
            fnd.	adj_rate2	          =	upd_rec.	adj_rate2	,
            fnd.	adj_rate3	          =	upd_rec.	adj_rate3	,
            fnd.	adj_rate4          	=	upd_rec.	adj_rate4	,
            fnd.	aggr_bal	          =	upd_rec.	aggr_bal	,
            fnd.	amt_cr	            =	upd_rec.	amt_cr	,
            fnd.	amt_db	            =	upd_rec.	amt_db	,
            fnd.	amt_last_pmt	      =	upd_rec.	amt_last_pmt	,
            fnd.	aprbal_subjchg	    =	upd_rec.	aprbal_subjchg	,
            fnd.	aprctd_cashadv_pnd	=	upd_rec.	aprctd_cashadv_pnd	,
            fnd.	aprctd_svcchg_pend	=	upd_rec.	aprctd_svcchg_pend	,
            fnd.	apr_svc_chg_tot   	=	upd_rec.	apr_svc_chg_tot	,
            fnd.	bal_xfr_mth_rem	    =	upd_rec.	bal_xfr_mth_rem	,
            fnd.	base_rate1	        =	upd_rec.	base_rate1	,
            fnd.	base_rate2	        =	upd_rec.	base_rate2	,
            fnd.	base_rate3        	=	upd_rec.	base_rate3	,
            fnd.	base_rate4        	=	upd_rec.	base_rate4	,
            fnd.	beg_bal	            =	upd_rec.	beg_bal	,
            fnd.	bs_var1            	=	upd_rec.	bs_var1	,
            fnd.	bs_var2	            =	upd_rec.	bs_var2	,
            fnd.	bs_var3	            =	upd_rec.	bs_var3	,
            fnd.	bs_var4	            =	upd_rec.	bs_var4	,
            fnd.	calc_rate1	        =	upd_rec.	calc_rate1	,
            fnd.	calc_rate2	        =	upd_rec.	calc_rate2	,
            fnd.	calc_rate3        	=	upd_rec.	calc_rate3	,
            fnd.	calc_rate4        	=	upd_rec.	calc_rate4	,
            fnd.	cap_adj_rate1      	=	upd_rec.	cap_adj_rate1	,
            fnd.	cap_adj_rate2      	=	upd_rec.	cap_adj_rate2	,
            fnd.	cap_adj_rate3      	=	upd_rec.	cap_adj_rate3	,
            fnd.	cap_adj_rate4      	=	upd_rec.	cap_adj_rate4	,
            fnd.	coll_bnp	          =	upd_rec.	coll_bnp	,
            fnd.	coll_bnp_chgoff    	=	upd_rec.	coll_bnp_chgoff	,
            fnd.	consol_pmt_flag   	=	upd_rec.	consol_pmt_flag	,
            fnd.	consol_stmt_flag  	=	upd_rec.	consol_stmt_flag	,
            fnd.	ctl_plan	          =	upd_rec.	ctl_plan	,
            fnd.	curr_bal           	=	upd_rec.	curr_bal	,
            fnd.	curr_mth_db        	=	upd_rec.	curr_mth_db	,
            fnd.	cust_db_amt       	=	upd_rec.	cust_db_amt	,
            fnd.	cust_db_nbr       	=	upd_rec.	cust_db_nbr	,
            fnd.	date_beg_billing	  =	upd_rec.	date_beg_billing	,
            fnd.	date_beg_int      	=	upd_rec.	date_beg_int	,
            fnd.	date_beg_pmt	      =	upd_rec.	date_beg_pmt	,
            fnd.	date_last_maint     =	upd_rec.	date_last_maint	,
            fnd.	date_last_pmt	      =	upd_rec.	date_last_pmt	,
            fnd.	date_orig_pmt_due  	=	upd_rec.	date_orig_pmt_due	,
            fnd.	date_paid_out	      =	upd_rec.	date_paid_out	,
            fnd.	def_agg_bal       	=	upd_rec.	def_agg_bal	,
            fnd.	def_agg_days	      =	upd_rec.	def_agg_days	,
            fnd.	deferred_int      	=	upd_rec.	deferred_int	,
            fnd.	dispu_bal	          =	upd_rec.	dispu_bal	,
            fnd.	dsclsr_days       	=	upd_rec.	dsclsr_days	,
            fnd.	eff_date	          =	upd_rec.	eff_date	,
            fnd.	eff_rate_chg_date	  =	upd_rec.	eff_rate_chg_date	,
            fnd.	fixed_pmt_amt_amps	=	upd_rec.	fixed_pmt_amt_amps/100	,
            fnd.	gl_bal	            =	upd_rec.	gl_bal	,
            fnd.	grace_day_flag	    =	upd_rec.	grace_day_flag	,
            fnd.	hi_bal_amps        	=	upd_rec.	hi_bal_amps	,
            fnd.	ins_bnp	            =	upd_rec.	ins_bnp	,
            fnd.	int_accr_method	    =	upd_rec.	int_accr_method	,
            fnd.	int_bnp           	=	upd_rec.	int_bnp	,
            fnd.	amps_insbnp_chgoff	=	upd_rec.	amps_insbnp_chgoff	,
            fnd.	int_bnp_chgoff    	=	upd_rec.	int_bnp_chgoff	,
            fnd.	int_bnp_last_stmt 	=	upd_rec.	int_bnp_last_stmt	,
            fnd.	int_rate	          =	upd_rec.	int_rate	,
            fnd.	intr_st	            =	upd_rec.	intr_st	,
            fnd.	interest_status   	=	upd_rec.	interest_status	,
            fnd.	int_table_nbr	      =	upd_rec.	int_table_nbr	,
            fnd.	int_tbl_var1	      =	upd_rec.	int_tbl_var1	,
            fnd.	int_tbl_var2	      =	upd_rec.	int_tbl_var2	,
            fnd.	int_tbl_var3	      =	upd_rec.	int_tbl_var3	,
            fnd.	int_tbl_var4	      =	upd_rec.	int_tbl_var4	,
            fnd.	last_int_tbl_used	  =	upd_rec.	last_int_tbl_used	,
            fnd.	late_chg_bnp	      =	upd_rec.	late_chg_bnp	,
            fnd.	late_chgbnp_chgoff	=	upd_rec.	late_chgbnp_chgoff	,
            fnd.	limit_indctr      	=	upd_rec.	limit_indctr	,
            fnd.	limit1	            =	upd_rec.	limit1	,
            fnd.	limit2	            =	upd_rec.	limit2	,
            fnd.	limit3	            =	upd_rec.	limit3	,
            fnd.	limit4	            =	upd_rec.	limit4	,
            fnd.	loan_amount	        =	upd_rec.	loan_amount	,
            fnd.	member_bnp	        =	upd_rec.	member_bnp	,
            fnd.	member_bnp_chgoff 	=	upd_rec.	member_bnp_chgoff	,
            fnd.	misc_ctd          	=	upd_rec.	misc_ctd	,
            fnd.	nbr_cr	            =	upd_rec.	nbr_cr	,
            fnd.	nbr_db	            =	upd_rec.	nbr_db	,
            fnd.	nbr_hist           	=	upd_rec.	nbr_hist	,
            fnd.	new_db_sw         	=	upd_rec.	new_db_sw	,
            fnd.	nsf_bnp           	=	upd_rec.	nsf_bnp	,
            fnd.	nsf_bnp_chgoff    	=	upd_rec.	nsf_bnp_chgoff	,
            fnd.	oib_appl_payment	  =	upd_rec.	oib_appl_payment	,
            fnd.	oib_appl_reversal	  =	upd_rec.	oib_appl_reversal	,
            fnd.	ovlm_bnp	          =	upd_rec.	ovlm_bnp	,
            fnd.	ovlm_bnp_chgoff	    =	upd_rec.	ovlm_bnp_chgoff	,
            fnd.	pct_level	          =	upd_rec.	pct_level	,
            fnd.	plan_type	          =	upd_rec.	plan_type	,
            fnd.	pmt_ctd_amps	      =	upd_rec.	pmt_ctd_amps	,
            fnd.	pmt_flag	          =	upd_rec.	pmt_flag	,
            fnd.	pmt_flagmaint_date	=	upd_rec.	pmt_flagmaint_date	,
            fnd.	pmt_last_rqtd	      =	upd_rec.	pmt_last_rqtd	,
            fnd.	pmt_tbl_hi_bal	    =	upd_rec.	pmt_tbl_hi_bal	,
            fnd.	prin_bal	          =	upd_rec.	prin_bal	,
            fnd.	prin_bal_chgoff   	=	upd_rec.	prin_bal_chgoff	,
            fnd.	prior_mth_db	      =	upd_rec.	prior_mth_db	,
            fnd.	prior_mth_def_int	  =	upd_rec.	prior_mth_def_int	,
            fnd.	rate_table_occ_ind	=	upd_rec.	rate_table_occ_ind	,
            fnd.	rate_type	          =	upd_rec.	rate_type	,
            fnd.	rec_type	          =	upd_rec.	rec_type	,
            fnd.	recv_bnp	          =	upd_rec.	recv_bnp	,
            fnd.	recv_bnp_chgoff	    =	upd_rec.	recv_bnp_chgoff	,
            fnd.	rit_nbr	            =	upd_rec.	rit_nbr	,
            fnd.	rtfctn_int_tbl    	=	upd_rec.	rtfctn_int_tbl	,
            fnd.	rtfctn_level	      =	upd_rec.	rtfctn_level	,
            fnd.	rtfctn_method	      =	upd_rec.	rtfctn_method	,
            fnd.	si_nbr	            =	upd_rec.	si_nbr	,
            fnd.	si_org	            =	upd_rec.	si_org	,
            fnd.	sit_pct_level     	=	upd_rec.	sit_pct_level	,
            fnd.	spc_int_tbl       	=	upd_rec.	spc_int_tbl	,
            fnd.	svc_bnp	            =	upd_rec.	svc_bnp	,
            fnd.	svc_bnp_chgoff    	=	upd_rec.	svc_bnp_chgoff	,
            fnd.	tot_due           	=	upd_rec.	tot_due	,
            fnd.	usury_adj_rate1   	=	upd_rec.	usury_adj_rate1	,
            fnd.	usury_adj_rate2	    =	upd_rec.	usury_adj_rate2	,
            fnd.	usury_adj_rate3   	=	upd_rec.	usury_adj_rate3	,
            fnd.	usury_adj_rate4	    =	upd_rec.	usury_adj_rate4	,
            fnd.	var_pct	            =	upd_rec.	var_pct	,
            fnd.	ytd_int	            =	upd_rec.	ytd_int	,
            fnd.  last_updated_date   = g_date
     where  fnd.	wfs_customer_no	    =	upd_rec.	wfs_customer_no and
            fnd.	product_code_no	    =	upd_rec.	product_code_no	and
            fnd.	plan_no	            =	upd_rec.	plan_no	and
            fnd.	plan_date	          =	upd_rec.	plan_date	and
            fnd.	sequence_no       	=	upd_rec.	sequence_no	and
            ( 
            nvl(fnd.accr_held_chgoff	,0) <>	upd_rec.	accr_held_chgoff	or
            nvl(fnd.wfs_account_no	,0) <>	upd_rec.	wfs_account_no	or
            nvl(fnd.acrd_int	,0) <>	upd_rec.	acrd_int	or
            nvl(fnd.add_on_flag	,0) <>	upd_rec.	add_on_flag	or
            nvl(fnd.adj_rate1	,0) <>	upd_rec.	adj_rate1	or
            nvl(fnd.adj_rate2	,0) <>	upd_rec.	adj_rate2	or
            nvl(fnd.adj_rate3	,0) <>	upd_rec.	adj_rate3	or
            nvl(fnd.adj_rate4	,0) <>	upd_rec.	adj_rate4	or
            nvl(fnd.aggr_bal	,0) <>	upd_rec.	aggr_bal	or
            nvl(fnd.amt_cr	,0) <>	upd_rec.	amt_cr	or
            nvl(fnd.amt_db	,0) <>	upd_rec.	amt_db	or
            nvl(fnd.amt_last_pmt	,0) <>	upd_rec.	amt_last_pmt	or
            nvl(fnd.aprbal_subjchg	,0) <>	upd_rec.	aprbal_subjchg	or
            nvl(fnd.aprctd_cashadv_pnd	,0) <>	upd_rec.	aprctd_cashadv_pnd	or
            nvl(fnd.aprctd_svcchg_pend	,0) <>	upd_rec.	aprctd_svcchg_pend	or
            nvl(fnd.apr_svc_chg_tot	,0) <>	upd_rec.	apr_svc_chg_tot	or
            nvl(fnd.bal_xfr_mth_rem	,0) <>	upd_rec.	bal_xfr_mth_rem	or
            nvl(fnd.base_rate1	,0) <>	upd_rec.	base_rate1	or
            nvl(fnd.base_rate2	,0) <>	upd_rec.	base_rate2	or
            nvl(fnd.base_rate3	,0) <>	upd_rec.	base_rate3	or
            nvl(fnd.base_rate4	,0) <>	upd_rec.	base_rate4	or
            nvl(fnd.beg_bal	,0) <>	upd_rec.	beg_bal	or
            nvl(fnd.bs_var1	,0) <>	upd_rec.	bs_var1	or
            nvl(fnd.bs_var2	,0) <>	upd_rec.	bs_var2	or
            nvl(fnd.bs_var3	,0) <>	upd_rec.	bs_var3	or
            nvl(fnd.bs_var4	,0) <>	upd_rec.	bs_var4	or
            nvl(fnd.calc_rate1	,0) <>	upd_rec.	calc_rate1	or
            nvl(fnd.calc_rate2	,0) <>	upd_rec.	calc_rate2	or
            nvl(fnd.calc_rate3	,0) <>	upd_rec.	calc_rate3	or
            nvl(fnd.calc_rate4	,0) <>	upd_rec.	calc_rate4	or
            nvl(fnd.cap_adj_rate1	,0) <>	upd_rec.	cap_adj_rate1	or
            nvl(fnd.cap_adj_rate2	,0) <>	upd_rec.	cap_adj_rate2	or
            nvl(fnd.cap_adj_rate3	,0) <>	upd_rec.	cap_adj_rate3	or
            nvl(fnd.cap_adj_rate4	,0) <>	upd_rec.	cap_adj_rate4	or
            nvl(fnd.coll_bnp	,0) <>	upd_rec.	coll_bnp	or
            nvl(fnd.coll_bnp_chgoff	,0) <>	upd_rec.	coll_bnp_chgoff	or
            nvl(fnd.consol_pmt_flag	,0) <>	upd_rec.	consol_pmt_flag	or
            nvl(fnd.consol_stmt_flag	,0) <>	upd_rec.	consol_stmt_flag	or
            nvl(fnd.ctl_plan	,0) <>	upd_rec.	ctl_plan	or
            nvl(fnd.curr_bal	,0) <>	upd_rec.	curr_bal	or
            nvl(fnd.curr_mth_db	,0) <>	upd_rec.	curr_mth_db	or
            nvl(fnd.cust_db_amt	,0) <>	upd_rec.	cust_db_amt	or
            nvl(fnd.cust_db_nbr	,0) <>	upd_rec.	cust_db_nbr	or
            nvl(fnd.date_beg_billing	,0) <>	upd_rec.	date_beg_billing	or
            nvl(fnd.date_beg_int	,0) <>	upd_rec.	date_beg_int	or
            nvl(fnd.date_beg_pmt	,0) <>	upd_rec.	date_beg_pmt	or
            nvl(fnd.date_last_maint	,0) <>	upd_rec.	date_last_maint	or
            nvl(fnd.date_last_pmt	,0) <>	upd_rec.	date_last_pmt	or
            nvl(fnd.date_orig_pmt_due	,0) <>	upd_rec.	date_orig_pmt_due	or
            nvl(fnd.date_paid_out	,0) <>	upd_rec.	date_paid_out	or
            nvl(fnd.def_agg_bal	,0) <>	upd_rec.	def_agg_bal	or
            nvl(fnd.def_agg_days	,0) <>	upd_rec.	def_agg_days	or
            nvl(fnd.deferred_int	,0) <>	upd_rec.	deferred_int	or
            nvl(fnd.dispu_bal	,0) <>	upd_rec.	dispu_bal	or
            nvl(fnd.dsclsr_days	,0) <>	upd_rec.	dsclsr_days	or
            nvl(fnd.eff_date	,0) <>	upd_rec.	eff_date	or
            nvl(fnd.eff_rate_chg_date	,0) <>	upd_rec.	eff_rate_chg_date	or
            nvl(fnd.fixed_pmt_amt_amps	,0) <>	upd_rec.	fixed_pmt_amt_amps	or
            nvl(fnd.gl_bal	,0) <>	upd_rec.	gl_bal	or
            nvl(fnd.grace_day_flag	,0) <>	upd_rec.	grace_day_flag	or
            nvl(fnd.hi_bal_amps	,0) <>	upd_rec.	hi_bal_amps	or
            nvl(fnd.ins_bnp	,0) <>	upd_rec.	ins_bnp	or
            nvl(fnd.int_accr_method	,0) <>	upd_rec.	int_accr_method	or
            nvl(fnd.int_bnp	,0) <>	upd_rec.	int_bnp	or
            nvl(fnd.amps_insbnp_chgoff	,0) <>	upd_rec.	amps_insbnp_chgoff	or
            nvl(fnd.int_bnp_chgoff	,0) <>	upd_rec.	int_bnp_chgoff	or
            nvl(fnd.int_bnp_last_stmt	,0) <>	upd_rec.	int_bnp_last_stmt	or
            nvl(fnd.int_rate	,0) <>	upd_rec.	int_rate	or
            nvl(fnd.intr_st	,0) <>	upd_rec.	intr_st	or
            nvl(fnd.interest_status	,0) <>	upd_rec.	interest_status	or
            nvl(fnd.int_table_nbr	,0) <>	upd_rec.	int_table_nbr	or
            nvl(fnd.int_tbl_var1	,0) <>	upd_rec.	int_tbl_var1	or
            nvl(fnd.int_tbl_var2	,0) <>	upd_rec.	int_tbl_var2	or
            nvl(fnd.int_tbl_var3	,0) <>	upd_rec.	int_tbl_var3	or
            nvl(fnd.int_tbl_var4	,0) <>	upd_rec.	int_tbl_var4	or
            nvl(fnd.last_int_tbl_used	,0) <>	upd_rec.	last_int_tbl_used	or
            nvl(fnd.late_chg_bnp	,0) <>	upd_rec.	late_chg_bnp	or
            nvl(fnd.late_chgbnp_chgoff	,0) <>	upd_rec.	late_chgbnp_chgoff	or
            nvl(fnd.limit_indctr	,0) <>	upd_rec.	limit_indctr	or
            nvl(fnd.limit1	,0) <>	upd_rec.	limit1	or
            nvl(fnd.limit2	,0) <>	upd_rec.	limit2	or
            nvl(fnd.limit3	,0) <>	upd_rec.	limit3	or
            nvl(fnd.limit4	,0) <>	upd_rec.	limit4	or
            nvl(fnd.loan_amount	,0) <>	upd_rec.	loan_amount	or
            nvl(fnd.member_bnp	,0) <>	upd_rec.	member_bnp	or
            nvl(fnd.member_bnp_chgoff	,0) <>	upd_rec.	member_bnp_chgoff	or
            nvl(fnd.misc_ctd	,0) <>	upd_rec.	misc_ctd	or
            nvl(fnd.nbr_cr	,0) <>	upd_rec.	nbr_cr	or
            nvl(fnd.nbr_db	,0) <>	upd_rec.	nbr_db	or
            nvl(fnd.nbr_hist	,0) <>	upd_rec.	nbr_hist	or
            nvl(fnd.new_db_sw	,0) <>	upd_rec.	new_db_sw	or
            nvl(fnd.nsf_bnp	,0) <>	upd_rec.	nsf_bnp	or
            nvl(fnd.nsf_bnp_chgoff	,0) <>	upd_rec.	nsf_bnp_chgoff	or
            nvl(fnd.oib_appl_payment	,0) <>	upd_rec.	oib_appl_payment	or
            nvl(fnd.oib_appl_reversal	,0) <>	upd_rec.	oib_appl_reversal	or
            nvl(fnd.ovlm_bnp	,0) <>	upd_rec.	ovlm_bnp	or
            nvl(fnd.ovlm_bnp_chgoff	,0) <>	upd_rec.	ovlm_bnp_chgoff	or
            nvl(fnd.pct_level	,0) <>	upd_rec.	pct_level	or
            nvl(fnd.plan_type	,0) <>	upd_rec.	plan_type	or
            nvl(fnd.pmt_ctd_amps	,0) <>	upd_rec.	pmt_ctd_amps	or
            nvl(fnd.pmt_flag	,0) <>	upd_rec.	pmt_flag	or
            nvl(fnd.pmt_flagmaint_date	,0) <>	upd_rec.	pmt_flagmaint_date	or
            nvl(fnd.pmt_last_rqtd	,0) <>	upd_rec.	pmt_last_rqtd	or
            nvl(fnd.pmt_tbl_hi_bal	,0) <>	upd_rec.	pmt_tbl_hi_bal	or
            nvl(fnd.prin_bal	,0) <>	upd_rec.	prin_bal	or
            nvl(fnd.prin_bal_chgoff	,0) <>	upd_rec.	prin_bal_chgoff	or
            nvl(fnd.prior_mth_db	,0) <>	upd_rec.	prior_mth_db	or
            nvl(fnd.prior_mth_def_int	,0) <>	upd_rec.	prior_mth_def_int	or
            nvl(fnd.rate_table_occ_ind	,0) <>	upd_rec.	rate_table_occ_ind	or
            nvl(fnd.rate_type	,0) <>	upd_rec.	rate_type	or
            nvl(fnd.rec_type	,0) <>	upd_rec.	rec_type	or
            nvl(fnd.recv_bnp	,0) <>	upd_rec.	recv_bnp	or
            nvl(fnd.recv_bnp_chgoff	,0) <>	upd_rec.	recv_bnp_chgoff	or
            nvl(fnd.rit_nbr	,0) <>	upd_rec.	rit_nbr	or
            nvl(fnd.rtfctn_int_tbl	,0) <>	upd_rec.	rtfctn_int_tbl	or
            nvl(fnd.rtfctn_level	,0) <>	upd_rec.	rtfctn_level	or
            nvl(fnd.rtfctn_method	,0) <>	upd_rec.	rtfctn_method	or
            nvl(fnd.si_nbr	,0) <>	upd_rec.	si_nbr	or
            nvl(fnd.si_org	,0) <>	upd_rec.	si_org	or
            nvl(fnd.sit_pct_level	,0) <>	upd_rec.	sit_pct_level	or
            nvl(fnd.spc_int_tbl	,0) <>	upd_rec.	spc_int_tbl	or
            nvl(fnd.svc_bnp	,0) <>	upd_rec.	svc_bnp	or
            nvl(fnd.svc_bnp_chgoff	,0) <>	upd_rec.	svc_bnp_chgoff	or
            nvl(fnd.tot_due	,0) <>	upd_rec.	tot_due	or
            nvl(fnd.usury_adj_rate1	,0) <>	upd_rec.	usury_adj_rate1	or
            nvl(fnd.usury_adj_rate2	,0) <>	upd_rec.	usury_adj_rate2	or
            nvl(fnd.usury_adj_rate3	,0) <>	upd_rec.	usury_adj_rate3	or
            nvl(fnd.usury_adj_rate4	,0) <>	upd_rec.	usury_adj_rate4	or
            nvl(fnd.var_pct	,0) <>	upd_rec.	var_pct	or
            nvl(fnd.ytd_int	,0) <>	upd_rec.	ytd_int 
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_tran_plan_seg_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'A DUMMY MASTER CREATE WAS DETECTED - LOAD CORRECT MASTER DETAIL',
            	cpy.	product_code_no	,
            	cpy.	plan_no	,
            	cpy.	plan_date	,
            	cpy.	wfs_customer_no	,
            	cpy.	sequence_no	,
            	cpy.	accr_held_chgoff	,
            	cpy.	wfs_account_no	,
            	cpy.	acrd_int	,
            	cpy.	add_on_flag	,
            	cpy.	adj_rate1	,
            	cpy.	adj_rate2	,
            	cpy.	adj_rate3	,
            	cpy.	adj_rate4	,
            	cpy.	aggr_bal	,
            	cpy.	amt_cr	,
            	cpy.	amt_db	,
            	cpy.	amt_last_pmt	,
            	cpy.	aprbal_subjchg	,
            	cpy.	aprctd_cashadv_pnd	,
            	cpy.	aprctd_svcchg_pend	,
            	cpy.	apr_svc_chg_tot	,
            	cpy.	bal_xfr_mth_rem	,
            	cpy.	base_rate1	,
            	cpy.	base_rate2	,
            	cpy.	base_rate3	,
            	cpy.	base_rate4	,
            	cpy.	beg_bal	,
            	cpy.	bs_var1	,
            	cpy.	bs_var2	,
            	cpy.	bs_var3	,
            	cpy.	bs_var4	,
            	cpy.	calc_rate1	,
            	cpy.	calc_rate2	,
            	cpy.	calc_rate3	,
            	cpy.	calc_rate4	,
            	cpy.	cap_adj_rate1	,
            	cpy.	cap_adj_rate2	,
            	cpy.	cap_adj_rate3	,
            	cpy.	cap_adj_rate4	,
            	cpy.	coll_bnp	,
            	cpy.	coll_bnp_chgoff	,
            	cpy.	consol_pmt_flag	,
            	cpy.	consol_stmt_flag	,
            	cpy.	ctl_plan	,
            	cpy.	curr_bal	,
            	cpy.	curr_mth_db	,
            	cpy.	cust_db_amt	,
            	cpy.	cust_db_nbr	,
            	cpy.	date_beg_billing	,
            	cpy.	date_beg_int	,
            	cpy.	date_beg_pmt	,
            	cpy.	date_last_maint	,
            	cpy.	date_last_pmt	,
            	cpy.	date_orig_pmt_due	,
            	cpy.	date_paid_out	,
            	cpy.	def_agg_bal	,
            	cpy.	def_agg_days	,
            	cpy.	deferred_int	,
            	cpy.	dispu_bal	,
            	cpy.	dsclsr_days	,
            	cpy.	eff_date	,
            	cpy.	eff_rate_chg_date	,
            	cpy.	fixed_pmt_amt_amps	,
            	cpy.	gl_bal	,
            	cpy.	grace_day_flag	,
            	cpy.	hi_bal_amps	,
            	cpy.	ins_bnp	,
            	cpy.	int_accr_method	,
            	cpy.	int_bnp	,
            	cpy.	amps_insbnp_chgoff	,
            	cpy.	int_bnp_chgoff	,
            	cpy.	int_bnp_last_stmt	,
            	cpy.	int_rate	,
            	cpy.	intr_st	,
            	cpy.	interest_status	,
            	cpy.	int_table_nbr	,
            	cpy.	int_tbl_var1	,
            	cpy.	int_tbl_var2	,
            	cpy.	int_tbl_var3	,
            	cpy.	int_tbl_var4	,
            	cpy.	last_int_tbl_used	,
            	cpy.	late_chg_bnp	,
            	cpy.	late_chgbnp_chgoff	,
            	cpy.	limit_indctr	,
            	cpy.	limit1	,
            	cpy.	limit2	,
            	cpy.	limit3	,
            	cpy.	limit4	,
            	cpy.	loan_amount	,
            	cpy.	member_bnp	,
            	cpy.	member_bnp_chgoff	,
            	cpy.	misc_ctd	,
            	cpy.	nbr_cr	,
            	cpy.	nbr_db	,
            	cpy.	nbr_hist	,
            	cpy.	new_db_sw	,
            	cpy.	nsf_bnp	,
            	cpy.	nsf_bnp_chgoff	,
            	cpy.	oib_appl_payment	,
            	cpy.	oib_appl_reversal	,
            	cpy.	ovlm_bnp	,
            	cpy.	ovlm_bnp_chgoff	,
            	cpy.	pct_level	,
            	cpy.	plan_type	,
            	cpy.	pmt_ctd_amps	,
            	cpy.	pmt_flag	,
            	cpy.	pmt_flagmaint_date	,
            	cpy.	pmt_last_rqtd	,
            	cpy.	pmt_tbl_hi_bal	,
            	cpy.	prin_bal	,
            	cpy.	prin_bal_chgoff	,
            	cpy.	prior_mth_db	,
            	cpy.	prior_mth_def_int	,
            	cpy.	rate_table_occ_ind	,
            	cpy.	rate_type	,
            	cpy.	rec_type	,
            	cpy.	recv_bnp	,
            	cpy.	recv_bnp_chgoff	,
            	cpy.	rit_nbr	,
            	cpy.	rtfctn_int_tbl	,
            	cpy.	rtfctn_level	,
            	cpy.	rtfctn_method	,
            	cpy.	si_nbr	,
            	cpy.	si_org	,
            	cpy.	sit_pct_level	,
            	cpy.	spc_int_tbl	,
            	cpy.	svc_bnp	,
            	cpy.	svc_bnp_chgoff	,
            	cpy.	tot_due	,
            	cpy.	usury_adj_rate1	,
            	cpy.	usury_adj_rate2	,
            	cpy.	usury_adj_rate3	,
            	cpy.	usury_adj_rate4	,
            	cpy.	var_pct	,
            	cpy.	ytd_int	 
      from   stg_vsn_tran_plan_seg_cpy cpy
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
    from   stg_vsn_tran_plan_seg_cpy
    where  sys_process_code = 'N';

    if g_recs_read > 0 then
    
    l_text := 'TRUNCATE TABLE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table fnd_wfs_tran_plan_seg';
    
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

--    Taken out for better performance --------------------
--    update stg_vsn_tran_plan_seg_cpy
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
       raise;
end wh_fnd_wfs_232u;
