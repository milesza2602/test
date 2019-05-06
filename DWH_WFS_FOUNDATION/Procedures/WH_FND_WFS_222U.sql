--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_222U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_222U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_fi_perf_day fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_fi_perf_day_cpy
--               Output - fnd_wfs_fi_perf_day
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


g_wfs_customer_no    stg_vsn_fi_perf_day_cpy.wfs_customer_no%type;  
g_product_code_no    stg_vsn_fi_perf_day_cpy.product_code_no%type; 
g_run_date           stg_vsn_fi_perf_day_cpy.run_date%type; 
   
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_222U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS_FI_PERF_DAY EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_vsn_fi_perf_day_cpy
where (wfs_customer_no,
product_code_no,
run_date)
in
(select wfs_customer_no,
product_code_no,
run_date
from stg_vsn_fi_perf_day_cpy 
group by wfs_customer_no,
product_code_no,
run_date
having count(*) > 1) 
order by wfs_customer_no,
product_code_no,
run_date,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_vsn_fi_perf_day is
select /*+ FULL(cpy)  parallel (cpy,2) */  
              cpy.*
      from    stg_vsn_fi_perf_day_cpy cpy,
              fnd_wfs_fi_perf_day fnd
      where   cpy.wfs_customer_no       =             fnd.wfs_customer_no and             
              cpy.product_code_no       = fnd.product_code_no    and   
              cpy.run_date              = fnd.run_date    and 
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
   g_run_date          := '1 Jan 2000';
 

for dupp_record in stg_dup
   loop

    if  dupp_record.wfs_customer_no   = g_wfs_customer_no and
        dupp_record.product_code_no   = g_product_code_no and
        dupp_record.run_date         = g_run_date      then
        update stg_vsn_fi_perf_day_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_wfs_customer_no    := dupp_record.wfs_customer_no; 
    g_product_code_no    := dupp_record.product_code_no;
    g_run_date          := dupp_record.run_date;
 

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
             'Dummy wh_fnd_wfs_222U',
             0	,
             ' ',
             g_date,
             1
      from   stg_vsn_fi_perf_day_cpy cpy
 
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
      from   stg_vsn_fi_perf_day_cpy cpy
 
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
      from   stg_vsn_fi_perf_day_cpy  cpy
 
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_fi_perf_day fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	wfs_customer_no	,
            	cpy.	product_code_no	,
            	cpy.	run_date	,
            	cpy.	wfs_account_no	,
            	cpy.	fi_amt_at_rsk_fctr	,
            	cpy.	fi_auth_aprv_code1	,
            	cpy.	fi_auth_aprv_code2	,
            	cpy.	fi_auth_aprv_code3	,
            	cpy.	fi_auth_aprv_code4	,
            	cpy.	fi_auth_aprv_code5	,
            	cpy.	fi_auth_ctrl_ind	,
            	cpy.	fi_auth_scnrio_id	,
            	cpy.	fi_auth_strat_id	,
            	cpy.	fi_block_code_1	,
            	cpy.	fi_block_code_2	,
            	cpy.	fi_coll_class	,
            	cpy.	fi_coll_ind	,
            	cpy.	fi_comm_ctrl_ind	,
            	cpy.	fi_comm_scnrio_id	,
            	cpy.	fi_comm_strat_id	,
            	cpy.	fi_crlim_ctrl_ind	,
            	cpy.	fi_crlim_source	,
            	cpy.	fi_crlim_scnrio_id	,
            	cpy.	fi_crlim_strat_id	,
            	cpy.	fi_curr_behav_scr	,
            	cpy.	fi_cust_score_1	,
            	cpy.	fi_cust_score_2	,
            	cpy.	fi_cust_score_3	,
            	cpy.	fi_cust_score_4	,
            	cpy.	fi_cust_score_5	,
            	cpy.	fi_cust_score_6	,
            	cpy.	fi_cust_score_7	,
            	cpy.	fi_date_last_comm	,
            	cpy.	fi_date_promo_1	,
            	cpy.	fi_date_promo_2	,
            	cpy.	fi_date_promo_3	,
            	cpy.	fi_date_promo_4	,
            	cpy.	fi_delq_action_cnt	,
            	cpy.	fi_delq_ctrl_ind	,
            	cpy.	fi_delq_next_call	,
            	cpy.	fi_delq_scnrio_id	,
            	cpy.	fi_delq_strat_id	,
            	cpy.	fi_fraud_score_1	,
            	cpy.	fi_fraud_score_2	,
            	cpy.	fi_fraud_score_3	,
            	cpy.	fi_fraud_score_4	,
            	cpy.	fi_fraud_score_5	,
            	cpy.	fi_fraud_score_6	,
            	cpy.	fi_fraud_score_7	,
            	cpy.	fi_lang_ind	,
            	cpy.	fi_notify_ind	,
            	cpy.	fi_ovlm_ctrl_ind	,
            	cpy.	fi_ovlm_feewaivctr	,
            	cpy.	fi_ovlm_scnrio_id	,
            	cpy.	fi_ovlm_strat_id	,
            	cpy.	fi_phone_ind	,
            	cpy.	fi_pre_post_delq	,
            	cpy.	fi_pre_post_ptp	,
            	cpy.	fi_promo_code_1	,
            	cpy.	fi_promo_code_2	,
            	cpy.	fi_promo_code_3	,
            	cpy.	fi_promo_code_4	,
            	cpy.	fi_ra_char_no_1	,
            	cpy.	fi_ra_char_no_2	,
            	cpy.	fi_ra_char_no_3	,
            	cpy.	fi_ra_char_no_4	,
            	cpy.	fi_ra_point_diff_1	,
            	cpy.	fi_ra_point_diff_2	,
            	cpy.	fi_ra_point_diff_3	,
            	cpy.	fi_ra_point_diff_4	,
            	cpy.	fi_reis_ctrl_ind	,
            	cpy.	fi_reis_scnrio_id	,
            	cpy.	fi_reis_strat_id	,
            	cpy.	fi_repr_ctrl_ind	,
            	cpy.	fi_repr_dtlst_repr	,
            	cpy.	fi_repr_eff_date	,
            	cpy.	fi_repr_end_date	,
            	cpy.	fi_repr_int_varnc	,
            	cpy.	fi_repr_pct_id	,
            	cpy.	fi_repr_pct_level	,
            	cpy.	fi_repr_scnrio_id	,
            	cpy.	fi_repr_status_ind	,
            	cpy.	fi_repr_strat_id	,
            	cpy.	fi_resp_score_1_1	,
            	cpy.	fi_resp_score_1_2	,
            	cpy.	fi_resp_score_1_3	,
            	cpy.	fi_resp_score_1_4	,
            	cpy.	fi_resp_score_1_5	,
            	cpy.	fi_resp_score_1_6	,
            	cpy.	fi_resp_score_1_7	,
            	cpy.	fi_resp_score_2_1	,
            	cpy.	fi_resp_score_2_2	,
            	cpy.	fi_resp_score_2_3	,
            	cpy.	fi_resp_score_2_4	,
            	cpy.	fi_resp_score_2_5	,
            	cpy.	fi_resp_score_2_6	,
            	cpy.	fi_resp_score_2_7	,
            	cpy.	fi_resp_score_3_1	,
            	cpy.	fi_resp_score_3_2	,
            	cpy.	fi_resp_score_3_3	,
            	cpy.	fi_resp_score_3_4	,
            	cpy.	fi_resp_score_3_5	,
            	cpy.	fi_resp_score_3_6	,
            	cpy.	fi_resp_score_3_7	,
            	cpy.	fi_revnu_score_1	,
            	cpy.	fi_revnu_score_2	,
            	cpy.	fi_revnu_score_3	,
            	cpy.	fi_revnu_score_4	,
            	cpy.	fi_revnu_score_5	,
            	cpy.	fi_revnu_score_6	,
            	cpy.	fi_revnu_score_7	,
            	cpy.	fi_revnu_score_1_1	,
            	cpy.	fi_revnu_score_1_2	,
            	cpy.	fi_revnu_score_1_3	,
            	cpy.	fi_revnu_score_1_4	,
            	cpy.	fi_revnu_score_1_5	,
            	cpy.	fi_revnu_score_1_6	,
            	cpy.	fi_revnu_score_1_7	,
            	cpy.	fi_revnu_score_2_1	,
            	cpy.	fi_revnu_score_2_2	,
            	cpy.	fi_revnu_score_2_3	,
            	cpy.	fi_revnu_score_2_4	,
            	cpy.	fi_revnu_score_2_5	,
            	cpy.	fi_revnu_score_2_6	,
            	cpy.	fi_revnu_score_2_7	,
            	cpy.	fi_specl_handling	,
            	cpy.	fi_spid	,
            	cpy.	fi_amt_at_risk	,

             g_date as last_updated_date
       from  stg_vsn_fi_perf_day_cpy cpy
       where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_fi_perf_day 
       where  wfs_customer_no     = cpy.wfs_customer_no and
              product_code_no     = cpy.product_code_no and
              run_date   = cpy.run_date  )
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



for upd_rec in c_stg_vsn_fi_perf_day
   loop
     update fnd_wfs_fi_perf_day fnd 
     set    fnd.	wfs_account_no	=	upd_rec.	wfs_account_no	,
            fnd.	fi_amt_at_rsk_fctr	=	upd_rec.	fi_amt_at_rsk_fctr	,
            fnd.	fi_auth_aprv_code1	=	upd_rec.	fi_auth_aprv_code1	,
            fnd.	fi_auth_aprv_code2	=	upd_rec.	fi_auth_aprv_code2	,
            fnd.	fi_auth_aprv_code3	=	upd_rec.	fi_auth_aprv_code3	,
            fnd.	fi_auth_aprv_code4	=	upd_rec.	fi_auth_aprv_code4	,
            fnd.	fi_auth_aprv_code5	=	upd_rec.	fi_auth_aprv_code5	,
            fnd.	fi_auth_ctrl_ind	=	upd_rec.	fi_auth_ctrl_ind	,
            fnd.	fi_auth_scnrio_id	=	upd_rec.	fi_auth_scnrio_id	,
            fnd.	fi_auth_strat_id	=	upd_rec.	fi_auth_strat_id	,
            fnd.	fi_block_code_1	=	upd_rec.	fi_block_code_1	,
            fnd.	fi_block_code_2	=	upd_rec.	fi_block_code_2	,
            fnd.	fi_coll_class	=	upd_rec.	fi_coll_class	,
            fnd.	fi_coll_ind	=	upd_rec.	fi_coll_ind	,
            fnd.	fi_comm_ctrl_ind	=	upd_rec.	fi_comm_ctrl_ind	,
            fnd.	fi_comm_scnrio_id	=	upd_rec.	fi_comm_scnrio_id	,
            fnd.	fi_comm_strat_id	=	upd_rec.	fi_comm_strat_id	,
            fnd.	fi_crlim_ctrl_ind	=	upd_rec.	fi_crlim_ctrl_ind	,
            fnd.	fi_crlim_source	=	upd_rec.	fi_crlim_source	,
            fnd.	fi_crlim_scnrio_id	=	upd_rec.	fi_crlim_scnrio_id	,
            fnd.	fi_crlim_strat_id	=	upd_rec.	fi_crlim_strat_id	,
            fnd.	fi_curr_behav_scr	=	upd_rec.	fi_curr_behav_scr	,
            fnd.	fi_cust_score_1	=	upd_rec.	fi_cust_score_1	,
            fnd.	fi_cust_score_2	=	upd_rec.	fi_cust_score_2	,
            fnd.	fi_cust_score_3	=	upd_rec.	fi_cust_score_3	,
            fnd.	fi_cust_score_4	=	upd_rec.	fi_cust_score_4	,
            fnd.	fi_cust_score_5	=	upd_rec.	fi_cust_score_5	,
            fnd.	fi_cust_score_6	=	upd_rec.	fi_cust_score_6	,
            fnd.	fi_cust_score_7	=	upd_rec.	fi_cust_score_7	,
            fnd.	fi_date_last_comm	=	upd_rec.	fi_date_last_comm	,
            fnd.	fi_date_promo_1	=	upd_rec.	fi_date_promo_1	,
            fnd.	fi_date_promo_2	=	upd_rec.	fi_date_promo_2	,
            fnd.	fi_date_promo_3	=	upd_rec.	fi_date_promo_3	,
            fnd.	fi_date_promo_4	=	upd_rec.	fi_date_promo_4	,
            fnd.	fi_delq_action_cnt	=	upd_rec.	fi_delq_action_cnt	,
            fnd.	fi_delq_ctrl_ind	=	upd_rec.	fi_delq_ctrl_ind	,
            fnd.	fi_delq_next_call	=	upd_rec.	fi_delq_next_call	,
            fnd.	fi_delq_scnrio_id	=	upd_rec.	fi_delq_scnrio_id	,
            fnd.	fi_delq_strat_id	=	upd_rec.	fi_delq_strat_id	,
            fnd.	fi_fraud_score_1	=	upd_rec.	fi_fraud_score_1	,
            fnd.	fi_fraud_score_2	=	upd_rec.	fi_fraud_score_2	,
            fnd.	fi_fraud_score_3	=	upd_rec.	fi_fraud_score_3	,
            fnd.	fi_fraud_score_4	=	upd_rec.	fi_fraud_score_4	,
            fnd.	fi_fraud_score_5	=	upd_rec.	fi_fraud_score_5	,
            fnd.	fi_fraud_score_6	=	upd_rec.	fi_fraud_score_6	,
            fnd.	fi_fraud_score_7	=	upd_rec.	fi_fraud_score_7	,
            fnd.	fi_lang_ind	=	upd_rec.	fi_lang_ind	,
            fnd.	fi_notify_ind	=	upd_rec.	fi_notify_ind	,
            fnd.	fi_ovlm_ctrl_ind	=	upd_rec.	fi_ovlm_ctrl_ind	,
            fnd.	fi_ovlm_feewaivctr	=	upd_rec.	fi_ovlm_feewaivctr	,
            fnd.	fi_ovlm_scnrio_id	=	upd_rec.	fi_ovlm_scnrio_id	,
            fnd.	fi_ovlm_strat_id	=	upd_rec.	fi_ovlm_strat_id	,
            fnd.	fi_phone_ind	=	upd_rec.	fi_phone_ind	,
            fnd.	fi_pre_post_delq	=	upd_rec.	fi_pre_post_delq	,
            fnd.	fi_pre_post_ptp	=	upd_rec.	fi_pre_post_ptp	,
            fnd.	fi_promo_code_1	=	upd_rec.	fi_promo_code_1	,
            fnd.	fi_promo_code_2	=	upd_rec.	fi_promo_code_2	,
            fnd.	fi_promo_code_3	=	upd_rec.	fi_promo_code_3	,
            fnd.	fi_promo_code_4	=	upd_rec.	fi_promo_code_4	,
            fnd.	fi_ra_char_no_1	=	upd_rec.	fi_ra_char_no_1	,
            fnd.	fi_ra_char_no_2	=	upd_rec.	fi_ra_char_no_2	,
            fnd.	fi_ra_char_no_3	=	upd_rec.	fi_ra_char_no_3	,
            fnd.	fi_ra_char_no_4	=	upd_rec.	fi_ra_char_no_4	,
            fnd.	fi_ra_point_diff_1	=	upd_rec.	fi_ra_point_diff_1	,
            fnd.	fi_ra_point_diff_2	=	upd_rec.	fi_ra_point_diff_2	,
            fnd.	fi_ra_point_diff_3	=	upd_rec.	fi_ra_point_diff_3	,
            fnd.	fi_ra_point_diff_4	=	upd_rec.	fi_ra_point_diff_4	,
            fnd.	fi_reis_ctrl_ind	=	upd_rec.	fi_reis_ctrl_ind	,
            fnd.	fi_reis_scnrio_id	=	upd_rec.	fi_reis_scnrio_id	,
            fnd.	fi_reis_strat_id	=	upd_rec.	fi_reis_strat_id	,
            fnd.	fi_repr_ctrl_ind	=	upd_rec.	fi_repr_ctrl_ind	,
            fnd.	fi_repr_dtlst_repr	=	upd_rec.	fi_repr_dtlst_repr	,
            fnd.	fi_repr_eff_date	=	upd_rec.	fi_repr_eff_date	,
            fnd.	fi_repr_end_date	=	upd_rec.	fi_repr_end_date	,
            fnd.	fi_repr_int_varnc	=	upd_rec.	fi_repr_int_varnc	,
            fnd.	fi_repr_pct_id	=	upd_rec.	fi_repr_pct_id	,
            fnd.	fi_repr_pct_level	=	upd_rec.	fi_repr_pct_level	,
            fnd.	fi_repr_scnrio_id	=	upd_rec.	fi_repr_scnrio_id	,
            fnd.	fi_repr_status_ind	=	upd_rec.	fi_repr_status_ind	,
            fnd.	fi_repr_strat_id	=	upd_rec.	fi_repr_strat_id	,
            fnd.	fi_resp_score_1_1	=	upd_rec.	fi_resp_score_1_1	,
            fnd.	fi_resp_score_1_2	=	upd_rec.	fi_resp_score_1_2	,
            fnd.	fi_resp_score_1_3	=	upd_rec.	fi_resp_score_1_3	,
            fnd.	fi_resp_score_1_4	=	upd_rec.	fi_resp_score_1_4	,
            fnd.	fi_resp_score_1_5	=	upd_rec.	fi_resp_score_1_5	,
            fnd.	fi_resp_score_1_6	=	upd_rec.	fi_resp_score_1_6	,
            fnd.	fi_resp_score_1_7	=	upd_rec.	fi_resp_score_1_7	,
            fnd.	fi_resp_score_2_1	=	upd_rec.	fi_resp_score_2_1	,
            fnd.	fi_resp_score_2_2	=	upd_rec.	fi_resp_score_2_2	,
            fnd.	fi_resp_score_2_3	=	upd_rec.	fi_resp_score_2_3	,
            fnd.	fi_resp_score_2_4	=	upd_rec.	fi_resp_score_2_4	,
            fnd.	fi_resp_score_2_5	=	upd_rec.	fi_resp_score_2_5	,
            fnd.	fi_resp_score_2_6	=	upd_rec.	fi_resp_score_2_6	,
            fnd.	fi_resp_score_2_7	=	upd_rec.	fi_resp_score_2_7	,
            fnd.	fi_resp_score_3_1	=	upd_rec.	fi_resp_score_3_1	,
            fnd.	fi_resp_score_3_2	=	upd_rec.	fi_resp_score_3_2	,
            fnd.	fi_resp_score_3_3	=	upd_rec.	fi_resp_score_3_3	,
            fnd.	fi_resp_score_3_4	=	upd_rec.	fi_resp_score_3_4	,
            fnd.	fi_resp_score_3_5	=	upd_rec.	fi_resp_score_3_5	,
            fnd.	fi_resp_score_3_6	=	upd_rec.	fi_resp_score_3_6	,
            fnd.	fi_resp_score_3_7	=	upd_rec.	fi_resp_score_3_7	,
            fnd.	fi_revnu_score_1	=	upd_rec.	fi_revnu_score_1	,
            fnd.	fi_revnu_score_2	=	upd_rec.	fi_revnu_score_2	,
            fnd.	fi_revnu_score_3	=	upd_rec.	fi_revnu_score_3	,
            fnd.	fi_revnu_score_4	=	upd_rec.	fi_revnu_score_4	,
            fnd.	fi_revnu_score_5	=	upd_rec.	fi_revnu_score_5	,
            fnd.	fi_revnu_score_6	=	upd_rec.	fi_revnu_score_6	,
            fnd.	fi_revnu_score_7	=	upd_rec.	fi_revnu_score_7	,
            fnd.	fi_revnu_score_1_1	=	upd_rec.	fi_revnu_score_1_1	,
            fnd.	fi_revnu_score_1_2	=	upd_rec.	fi_revnu_score_1_2	,
            fnd.	fi_revnu_score_1_3	=	upd_rec.	fi_revnu_score_1_3	,
            fnd.	fi_revnu_score_1_4	=	upd_rec.	fi_revnu_score_1_4	,
            fnd.	fi_revnu_score_1_5	=	upd_rec.	fi_revnu_score_1_5	,
            fnd.	fi_revnu_score_1_6	=	upd_rec.	fi_revnu_score_1_6	,
            fnd.	fi_revnu_score_1_7	=	upd_rec.	fi_revnu_score_1_7	,
            fnd.	fi_revnu_score_2_1	=	upd_rec.	fi_revnu_score_2_1	,
            fnd.	fi_revnu_score_2_2	=	upd_rec.	fi_revnu_score_2_2	,
            fnd.	fi_revnu_score_2_3	=	upd_rec.	fi_revnu_score_2_3	,
            fnd.	fi_revnu_score_2_4	=	upd_rec.	fi_revnu_score_2_4	,
            fnd.	fi_revnu_score_2_5	=	upd_rec.	fi_revnu_score_2_5	,
            fnd.	fi_revnu_score_2_6	=	upd_rec.	fi_revnu_score_2_6	,
            fnd.	fi_revnu_score_2_7	=	upd_rec.	fi_revnu_score_2_7	,
            fnd.	fi_specl_handling	=	upd_rec.	fi_specl_handling	,
            fnd.	fi_spid	=	upd_rec.	fi_spid	,
            fnd.	fi_amt_at_risk	=	upd_rec.	fi_amt_at_risk	,
            fnd.  last_updated_date   = g_date
     where  fnd.	wfs_customer_no	    =	upd_rec.	wfs_customer_no and
            fnd.	product_code_no	    =	upd_rec.	product_code_no	and
            fnd.	run_date	          =	upd_rec.	run_date	and
            ( 
            nvl(fnd.wfs_account_no	,0) <>	upd_rec.	wfs_account_no	or
            nvl(fnd.fi_amt_at_rsk_fctr	,0) <>	upd_rec.	fi_amt_at_rsk_fctr	or
            nvl(fnd.fi_auth_aprv_code1	,0) <>	upd_rec.	fi_auth_aprv_code1	or
            nvl(fnd.fi_auth_aprv_code2	,0) <>	upd_rec.	fi_auth_aprv_code2	or
            nvl(fnd.fi_auth_aprv_code3	,0) <>	upd_rec.	fi_auth_aprv_code3	or
            nvl(fnd.fi_auth_aprv_code4	,0) <>	upd_rec.	fi_auth_aprv_code4	or
            nvl(fnd.fi_auth_aprv_code5	,0) <>	upd_rec.	fi_auth_aprv_code5	or
            nvl(fnd.fi_auth_ctrl_ind	,0) <>	upd_rec.	fi_auth_ctrl_ind	or
            nvl(fnd.fi_auth_scnrio_id	,0) <>	upd_rec.	fi_auth_scnrio_id	or
            nvl(fnd.fi_auth_strat_id	,0) <>	upd_rec.	fi_auth_strat_id	or
            nvl(fnd.fi_block_code_1	,0) <>	upd_rec.	fi_block_code_1	or
            nvl(fnd.fi_block_code_2	,0) <>	upd_rec.	fi_block_code_2	or
            nvl(fnd.fi_coll_class	,0) <>	upd_rec.	fi_coll_class	or
            nvl(fnd.fi_coll_ind	,0) <>	upd_rec.	fi_coll_ind	or
            nvl(fnd.fi_comm_ctrl_ind	,0) <>	upd_rec.	fi_comm_ctrl_ind	or
            nvl(fnd.fi_comm_scnrio_id	,0) <>	upd_rec.	fi_comm_scnrio_id	or
            nvl(fnd.fi_comm_strat_id	,0) <>	upd_rec.	fi_comm_strat_id	or
            nvl(fnd.fi_crlim_ctrl_ind	,0) <>	upd_rec.	fi_crlim_ctrl_ind	or
            nvl(fnd.fi_crlim_source	,0) <>	upd_rec.	fi_crlim_source	or
            nvl(fnd.fi_crlim_scnrio_id	,0) <>	upd_rec.	fi_crlim_scnrio_id	or
            nvl(fnd.fi_crlim_strat_id	,0) <>	upd_rec.	fi_crlim_strat_id	or
            nvl(fnd.fi_curr_behav_scr	,0) <>	upd_rec.	fi_curr_behav_scr	or
            nvl(fnd.fi_cust_score_1	,0) <>	upd_rec.	fi_cust_score_1	or
            nvl(fnd.fi_cust_score_2	,0) <>	upd_rec.	fi_cust_score_2	or
            nvl(fnd.fi_cust_score_3	,0) <>	upd_rec.	fi_cust_score_3	or
            nvl(fnd.fi_cust_score_4	,0) <>	upd_rec.	fi_cust_score_4	or
            nvl(fnd.fi_cust_score_5	,0) <>	upd_rec.	fi_cust_score_5	or
            nvl(fnd.fi_cust_score_6	,0) <>	upd_rec.	fi_cust_score_6	or
            nvl(fnd.fi_cust_score_7	,0) <>	upd_rec.	fi_cust_score_7	or
            nvl(fnd.fi_date_last_comm	,'1 Jan 1900') <>	upd_rec.	fi_date_last_comm	or
            nvl(fnd.fi_date_promo_1	,'1 Jan 1900') <>	upd_rec.	fi_date_promo_1	or
            nvl(fnd.fi_date_promo_2	,'1 Jan 1900') <>	upd_rec.	fi_date_promo_2	or
            nvl(fnd.fi_date_promo_3	,'1 Jan 1900') <>	upd_rec.	fi_date_promo_3	or
            nvl(fnd.fi_date_promo_4	,'1 Jan 1900') <>	upd_rec.	fi_date_promo_4	or
            nvl(fnd.fi_delq_action_cnt	,0) <>	upd_rec.	fi_delq_action_cnt	or
            nvl(fnd.fi_delq_ctrl_ind	,0) <>	upd_rec.	fi_delq_ctrl_ind	or
            nvl(fnd.fi_delq_next_call	,0) <>	upd_rec.	fi_delq_next_call	or
            nvl(fnd.fi_delq_scnrio_id	,0) <>	upd_rec.	fi_delq_scnrio_id	or
            nvl(fnd.fi_delq_strat_id	,0) <>	upd_rec.	fi_delq_strat_id	or
            nvl(fnd.fi_fraud_score_1	,0) <>	upd_rec.	fi_fraud_score_1	or
            nvl(fnd.fi_fraud_score_2	,0) <>	upd_rec.	fi_fraud_score_2	or
            nvl(fnd.fi_fraud_score_3	,0) <>	upd_rec.	fi_fraud_score_3	or
            nvl(fnd.fi_fraud_score_4	,0) <>	upd_rec.	fi_fraud_score_4	or
            nvl(fnd.fi_fraud_score_5	,0) <>	upd_rec.	fi_fraud_score_5	or
            nvl(fnd.fi_fraud_score_6	,0) <>	upd_rec.	fi_fraud_score_6	or
            nvl(fnd.fi_fraud_score_7	,0) <>	upd_rec.	fi_fraud_score_7	or
            nvl(fnd.fi_lang_ind	,0) <>	upd_rec.	fi_lang_ind	or
            nvl(fnd.fi_notify_ind	,0) <>	upd_rec.	fi_notify_ind	or
            nvl(fnd.fi_ovlm_ctrl_ind	,0) <>	upd_rec.	fi_ovlm_ctrl_ind	or
            nvl(fnd.fi_ovlm_feewaivctr	,0) <>	upd_rec.	fi_ovlm_feewaivctr	or
            nvl(fnd.fi_ovlm_scnrio_id	,0) <>	upd_rec.	fi_ovlm_scnrio_id	or
            nvl(fnd.fi_ovlm_strat_id	,0) <>	upd_rec.	fi_ovlm_strat_id	or
            nvl(fnd.fi_phone_ind	,0) <>	upd_rec.	fi_phone_ind	or
            nvl(fnd.fi_pre_post_delq	,0) <>	upd_rec.	fi_pre_post_delq	or
            nvl(fnd.fi_pre_post_ptp	,0) <>	upd_rec.	fi_pre_post_ptp	or
            nvl(fnd.fi_promo_code_1	,0) <>	upd_rec.	fi_promo_code_1	or
            nvl(fnd.fi_promo_code_2	,0) <>	upd_rec.	fi_promo_code_2	or
            nvl(fnd.fi_promo_code_3	,0) <>	upd_rec.	fi_promo_code_3	or
            nvl(fnd.fi_promo_code_4	,0) <>	upd_rec.	fi_promo_code_4	or
            nvl(fnd.fi_ra_char_no_1	,0) <>	upd_rec.	fi_ra_char_no_1	or
            nvl(fnd.fi_ra_char_no_2	,0) <>	upd_rec.	fi_ra_char_no_2	or
            nvl(fnd.fi_ra_char_no_3	,0) <>	upd_rec.	fi_ra_char_no_3	or
            nvl(fnd.fi_ra_char_no_4	,0) <>	upd_rec.	fi_ra_char_no_4	or
            nvl(fnd.fi_ra_point_diff_1	,0) <>	upd_rec.	fi_ra_point_diff_1	or
            nvl(fnd.fi_ra_point_diff_2	,0) <>	upd_rec.	fi_ra_point_diff_2	or
            nvl(fnd.fi_ra_point_diff_3	,0) <>	upd_rec.	fi_ra_point_diff_3	or
            nvl(fnd.fi_ra_point_diff_4	,0) <>	upd_rec.	fi_ra_point_diff_4	or
            nvl(fnd.fi_reis_ctrl_ind	,0) <>	upd_rec.	fi_reis_ctrl_ind	or
            nvl(fnd.fi_reis_scnrio_id	,0) <>	upd_rec.	fi_reis_scnrio_id	or
            nvl(fnd.fi_reis_strat_id	,0) <>	upd_rec.	fi_reis_strat_id	or
            nvl(fnd.fi_repr_ctrl_ind	,0) <>	upd_rec.	fi_repr_ctrl_ind	or
            nvl(fnd.fi_repr_dtlst_repr	,0) <>	upd_rec.	fi_repr_dtlst_repr	or
            nvl(fnd.fi_repr_eff_date	,0) <>	upd_rec.	fi_repr_eff_date	or
            nvl(fnd.fi_repr_end_date	,0) <>	upd_rec.	fi_repr_end_date	or
            nvl(fnd.fi_repr_int_varnc	,0) <>	upd_rec.	fi_repr_int_varnc	or
            nvl(fnd.fi_repr_pct_id	,0) <>	upd_rec.	fi_repr_pct_id	or
            nvl(fnd.fi_repr_pct_level	,0) <>	upd_rec.	fi_repr_pct_level	or
            nvl(fnd.fi_repr_scnrio_id	,0) <>	upd_rec.	fi_repr_scnrio_id	or
            nvl(fnd.fi_repr_status_ind	,0) <>	upd_rec.	fi_repr_status_ind	or
            nvl(fnd.fi_repr_strat_id	,0) <>	upd_rec.	fi_repr_strat_id	or
            nvl(fnd.fi_resp_score_1_1	,0) <>	upd_rec.	fi_resp_score_1_1	or
            nvl(fnd.fi_resp_score_1_2	,0) <>	upd_rec.	fi_resp_score_1_2	or
            nvl(fnd.fi_resp_score_1_3	,0) <>	upd_rec.	fi_resp_score_1_3	or
            nvl(fnd.fi_resp_score_1_4	,0) <>	upd_rec.	fi_resp_score_1_4	or
            nvl(fnd.fi_resp_score_1_5	,0) <>	upd_rec.	fi_resp_score_1_5	or
            nvl(fnd.fi_resp_score_1_6	,0) <>	upd_rec.	fi_resp_score_1_6	or
            nvl(fnd.fi_resp_score_1_7	,0) <>	upd_rec.	fi_resp_score_1_7	or
            nvl(fnd.fi_resp_score_2_1	,0) <>	upd_rec.	fi_resp_score_2_1	or
            nvl(fnd.fi_resp_score_2_2	,0) <>	upd_rec.	fi_resp_score_2_2	or
            nvl(fnd.fi_resp_score_2_3	,0) <>	upd_rec.	fi_resp_score_2_3	or
            nvl(fnd.fi_resp_score_2_4	,0) <>	upd_rec.	fi_resp_score_2_4	or
            nvl(fnd.fi_resp_score_2_5	,0) <>	upd_rec.	fi_resp_score_2_5	or
            nvl(fnd.fi_resp_score_2_6	,0) <>	upd_rec.	fi_resp_score_2_6	or
            nvl(fnd.fi_resp_score_2_7	,0) <>	upd_rec.	fi_resp_score_2_7	or
            nvl(fnd.fi_resp_score_3_1	,0) <>	upd_rec.	fi_resp_score_3_1	or
            nvl(fnd.fi_resp_score_3_2	,0) <>	upd_rec.	fi_resp_score_3_2	or
            nvl(fnd.fi_resp_score_3_3	,0) <>	upd_rec.	fi_resp_score_3_3	or
            nvl(fnd.fi_resp_score_3_4	,0) <>	upd_rec.	fi_resp_score_3_4	or
            nvl(fnd.fi_resp_score_3_5	,0) <>	upd_rec.	fi_resp_score_3_5	or
            nvl(fnd.fi_resp_score_3_6	,0) <>	upd_rec.	fi_resp_score_3_6	or
            nvl(fnd.fi_resp_score_3_7	,0) <>	upd_rec.	fi_resp_score_3_7	or
            nvl(fnd.fi_revnu_score_1	,0) <>	upd_rec.	fi_revnu_score_1	or
            nvl(fnd.fi_revnu_score_2	,0) <>	upd_rec.	fi_revnu_score_2	or
            nvl(fnd.fi_revnu_score_3	,0) <>	upd_rec.	fi_revnu_score_3	or
            nvl(fnd.fi_revnu_score_4	,0) <>	upd_rec.	fi_revnu_score_4	or
            nvl(fnd.fi_revnu_score_5	,0) <>	upd_rec.	fi_revnu_score_5	or
            nvl(fnd.fi_revnu_score_6	,0) <>	upd_rec.	fi_revnu_score_6	or
            nvl(fnd.fi_revnu_score_7	,0) <>	upd_rec.	fi_revnu_score_7	or
            nvl(fnd.fi_revnu_score_1_1	,0) <>	upd_rec.	fi_revnu_score_1_1	or
            nvl(fnd.fi_revnu_score_1_2	,0) <>	upd_rec.	fi_revnu_score_1_2	or
            nvl(fnd.fi_revnu_score_1_3	,0) <>	upd_rec.	fi_revnu_score_1_3	or
            nvl(fnd.fi_revnu_score_1_4	,0) <>	upd_rec.	fi_revnu_score_1_4	or
            nvl(fnd.fi_revnu_score_1_5	,0) <>	upd_rec.	fi_revnu_score_1_5	or
            nvl(fnd.fi_revnu_score_1_6	,0) <>	upd_rec.	fi_revnu_score_1_6	or
            nvl(fnd.fi_revnu_score_1_7	,0) <>	upd_rec.	fi_revnu_score_1_7	or
            nvl(fnd.fi_revnu_score_2_1	,0) <>	upd_rec.	fi_revnu_score_2_1	or
            nvl(fnd.fi_revnu_score_2_2	,0) <>	upd_rec.	fi_revnu_score_2_2	or
            nvl(fnd.fi_revnu_score_2_3	,0) <>	upd_rec.	fi_revnu_score_2_3	or
            nvl(fnd.fi_revnu_score_2_4	,0) <>	upd_rec.	fi_revnu_score_2_4	or
            nvl(fnd.fi_revnu_score_2_5	,0) <>	upd_rec.	fi_revnu_score_2_5	or
            nvl(fnd.fi_revnu_score_2_6	,0) <>	upd_rec.	fi_revnu_score_2_6	or
            nvl(fnd.fi_revnu_score_2_7	,0) <>	upd_rec.	fi_revnu_score_2_7	or
            nvl(fnd.fi_specl_handling	,0) <>	upd_rec.	fi_specl_handling	or
            nvl(fnd.fi_spid	,0) <>	upd_rec.	fi_spid	or
            nvl(fnd.fi_amt_at_risk	,0) <>	upd_rec.	fi_amt_at_risk	 

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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_vsn_fi_perf_day_hsp hsp
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
            	cpy.	fi_amt_at_rsk_fctr	,
            	cpy.	fi_auth_aprv_code1	,
            	cpy.	fi_auth_aprv_code2	,
            	cpy.	fi_auth_aprv_code3	,
            	cpy.	fi_auth_aprv_code4	,
            	cpy.	fi_auth_aprv_code5	,
            	cpy.	fi_auth_ctrl_ind	,
            	cpy.	fi_auth_scnrio_id	,
            	cpy.	fi_auth_strat_id	,
            	cpy.	fi_block_code_1	,
            	cpy.	fi_block_code_2	,
            	cpy.	fi_coll_class	,
            	cpy.	fi_coll_ind	,
            	cpy.	fi_comm_ctrl_ind	,
            	cpy.	fi_comm_scnrio_id	,
            	cpy.	fi_comm_strat_id	,
            	cpy.	fi_crlim_ctrl_ind	,
            	cpy.	fi_crlim_source	,
            	cpy.	fi_crlim_scnrio_id	,
            	cpy.	fi_crlim_strat_id	,
            	cpy.	fi_curr_behav_scr	,
            	cpy.	fi_cust_score_1	,
            	cpy.	fi_cust_score_2	,
            	cpy.	fi_cust_score_3	,
            	cpy.	fi_cust_score_4	,
            	cpy.	fi_cust_score_5	,
            	cpy.	fi_cust_score_6	,
            	cpy.	fi_cust_score_7	,
            	cpy.	fi_date_last_comm	,
            	cpy.	fi_date_promo_1	,
            	cpy.	fi_date_promo_2	,
            	cpy.	fi_date_promo_3	,
            	cpy.	fi_date_promo_4	,
            	cpy.	fi_delq_action_cnt	,
            	cpy.	fi_delq_ctrl_ind	,
            	cpy.	fi_delq_next_call	,
            	cpy.	fi_delq_scnrio_id	,
            	cpy.	fi_delq_strat_id	,
            	cpy.	fi_fraud_score_1	,
            	cpy.	fi_fraud_score_2	,
            	cpy.	fi_fraud_score_3	,
            	cpy.	fi_fraud_score_4	,
            	cpy.	fi_fraud_score_5	,
            	cpy.	fi_fraud_score_6	,
            	cpy.	fi_fraud_score_7	,
            	cpy.	fi_lang_ind	,
            	cpy.	fi_notify_ind	,
            	cpy.	fi_ovlm_ctrl_ind	,
            	cpy.	fi_ovlm_feewaivctr	,
            	cpy.	fi_ovlm_scnrio_id	,
            	cpy.	fi_ovlm_strat_id	,
            	cpy.	fi_phone_ind	,
            	cpy.	fi_pre_post_delq	,
            	cpy.	fi_pre_post_ptp	,
            	cpy.	fi_promo_code_1	,
            	cpy.	fi_promo_code_2	,
            	cpy.	fi_promo_code_3	,
            	cpy.	fi_promo_code_4	,
            	cpy.	fi_ra_char_no_1	,
            	cpy.	fi_ra_char_no_2	,
            	cpy.	fi_ra_char_no_3	,
            	cpy.	fi_ra_char_no_4	,
            	cpy.	fi_ra_point_diff_1	,
            	cpy.	fi_ra_point_diff_2	,
            	cpy.	fi_ra_point_diff_3	,
            	cpy.	fi_ra_point_diff_4	,
            	cpy.	fi_reis_ctrl_ind	,
            	cpy.	fi_reis_scnrio_id	,
            	cpy.	fi_reis_strat_id	,
            	cpy.	fi_repr_ctrl_ind	,
            	cpy.	fi_repr_dtlst_repr	,
            	cpy.	fi_repr_eff_date	,
            	cpy.	fi_repr_end_date	,
            	cpy.	fi_repr_int_varnc	,
            	cpy.	fi_repr_pct_id	,
            	cpy.	fi_repr_pct_level	,
            	cpy.	fi_repr_scnrio_id	,
            	cpy.	fi_repr_status_ind	,
            	cpy.	fi_repr_strat_id	,
            	cpy.	fi_resp_score_1_1	,
            	cpy.	fi_resp_score_1_2	,
            	cpy.	fi_resp_score_1_3	,
            	cpy.	fi_resp_score_1_4	,
            	cpy.	fi_resp_score_1_5	,
            	cpy.	fi_resp_score_1_6	,
            	cpy.	fi_resp_score_1_7	,
            	cpy.	fi_resp_score_2_1	,
            	cpy.	fi_resp_score_2_2	,
            	cpy.	fi_resp_score_2_3	,
            	cpy.	fi_resp_score_2_4	,
            	cpy.	fi_resp_score_2_5	,
            	cpy.	fi_resp_score_2_6	,
            	cpy.	fi_resp_score_2_7	,
            	cpy.	fi_resp_score_3_1	,
            	cpy.	fi_resp_score_3_2	,
            	cpy.	fi_resp_score_3_3	,
            	cpy.	fi_resp_score_3_4	,
            	cpy.	fi_resp_score_3_5	,
            	cpy.	fi_resp_score_3_6	,
            	cpy.	fi_resp_score_3_7	,
            	cpy.	fi_revnu_score_1	,
            	cpy.	fi_revnu_score_2	,
            	cpy.	fi_revnu_score_3	,
            	cpy.	fi_revnu_score_4	,
            	cpy.	fi_revnu_score_5	,
            	cpy.	fi_revnu_score_6	,
            	cpy.	fi_revnu_score_7	,
            	cpy.	fi_revnu_score_1_1	,
            	cpy.	fi_revnu_score_1_2	,
            	cpy.	fi_revnu_score_1_3	,
            	cpy.	fi_revnu_score_1_4	,
            	cpy.	fi_revnu_score_1_5	,
            	cpy.	fi_revnu_score_1_6	,
            	cpy.	fi_revnu_score_1_7	,
            	cpy.	fi_revnu_score_2_1	,
            	cpy.	fi_revnu_score_2_2	,
            	cpy.	fi_revnu_score_2_3	,
            	cpy.	fi_revnu_score_2_4	,
            	cpy.	fi_revnu_score_2_5	,
            	cpy.	fi_revnu_score_2_6	,
            	cpy.	fi_revnu_score_2_7	,
            	cpy.	fi_specl_handling	,
            	cpy.	fi_spid	,
            	cpy.	fi_amt_at_risk 

      from   stg_vsn_fi_perf_day_cpy cpy
      where  
      ( 1 =   
        (SELECT dummy_ind  FROM  fnd_customer_product cust
         where  cpy.wfs_customer_no       = cust.product_no  and cust.customer_no = 0 ) or
        1 =   
        (select dummy_ind  from  fnd_customer_product cust
         where  cpy.wfs_account_no       = cust.product_no  and cust.customer_no = 0 ) or
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
    from   stg_vsn_fi_perf_day_cpy
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
--    update stg_vsn_fi_perf_day_cpy
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
end wh_fnd_wfs_222u;
