--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_322U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_322U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_ocs_card_daily fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_ocs_card_daily_cpy
--               Output - fnd_wfs_ocs_card_daily
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
g_truncate_count     integer       :=  0;


g_information_date                 stg_absa_ocs_card_daily_cpy.information_date%type;  
g_account_number                   stg_absa_ocs_card_daily_cpy.account_number%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_322U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS OCS CRD DLY EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_absa_ocs_card_daily_cpy
where (information_date,
account_number)
in
(select information_date,
account_number
from stg_absa_ocs_card_daily_cpy 
group by information_date,
account_number
having count(*) > 1) 
order by information_date,
account_number,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_absa_ocs_card_daily is
select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    stg_absa_ocs_card_daily_cpy stg,
              fnd_wfs_ocs_card_daily fnd
      where   stg.information_date         = fnd.information_date  and             
              stg.account_number           = fnd.account_number    and   
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.information_date,
              stg.account_number,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_information_date        := '1 Jan 2000'; 
   g_account_number          := '0';
 
for dupp_record in stg_dup
   loop

    if  dupp_record.information_date        = g_information_date and
        dupp_record.account_number          = g_account_number  then
        update stg_absa_ocs_card_daily_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_information_date         := dupp_record.information_date; 
    g_account_number           := dupp_record.account_number;

   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_ocs_card_daily fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
            	cpy.	information_date	,
            	cpy.	account_number	,
            	cpy.	last_payment_amt	,
            	cpy.	last_txn_amt	,
            	cpy.	financial_inst_code	,
            	cpy.	secondary_account_number	,
            	cpy.	product_code	,
            	cpy.	sub_product_code	,
            	cpy.	customer_key	,
            	cpy.	ocs_txn_type_code	,
            	cpy.	active_ind	,
            	cpy.	account_status_code	,
            	cpy.	last_money_txn_code	,
            	cpy.	language	,
            	cpy.	account_type_code	,
            	cpy.	cycle_code	,
            	cpy.	employee_ind	,
            	cpy.	expiry_date	,
            	cpy.	closed_date	,
            	cpy.	last_state_date	,
            	cpy.	last_payment_date	,
            	cpy.	last_money_txn_date	,
            	cpy.	account_delinquent_date	,
            	cpy.	number_days_past_due	,
            	cpy.	billing_cycle_day	,
            	cpy.	number_times_in_collections	,
            	cpy.	account_balance	,
            	cpy.	billing_date_req_pay_amt	,
            	cpy.	open_date	,
            	cpy.	number_cards_outstand	,
            	cpy.	total_tenants	,
            	cpy.	number_plastic_comments	,
            	cpy.	months_with_balance	,
            	cpy.	last_status_change_date	,
            	cpy.	last_txn_date	,
            	cpy.	last_dt_activity_date	,
            	cpy.	high_balance_amt	,
            	cpy.	calc_min_pay_amt	,
            	cpy.	delinquent_amt	,
            	cpy.	next_pay_due_date	,
            	cpy.	credit_limit	,
            	cpy.	current_avail_ct_limit	,
            	cpy.	over_limit_amt	,
            	cpy.	reverse_code	,
            	cpy.	bad_payment_ind	,
            	cpy.	behaviour_score	,
            	cpy.	last_txn_type_code	,
            	cpy.	budget_limit_amt	,
            	cpy.	letter_ind	,
            	cpy.	balance_at_risk	,
            	cpy.	returned_mail_ind	,
            	cpy.	payment_month_1	,
            	cpy.	delinquent_hist_1	,
            	cpy.	credit_line_use_hist_1	,
            	cpy.	times_delinquent_1	,
            	cpy.	delinquent_cat_1	,
            	cpy.	delinquent_amt_1	,
            	cpy.	transaction_desc_1	,
            	cpy.	transaction_amt_1	,
            	cpy.	transaction_date_1	,
             g_date as last_updated_date
      from   stg_absa_ocs_card_daily_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_ocs_card_daily 
       where  information_date         = cpy.information_date and
              account_number           = cpy.account_number  )
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



for upd_rec in c_stg_absa_ocs_card_daily
   loop
     update fnd_wfs_ocs_card_daily fnd 
     set    fnd.	last_payment_amt	=	upd_rec.	last_payment_amt	,
            fnd.	last_txn_amt	=	upd_rec.	last_txn_amt	,
            fnd.	financial_inst_code	=	upd_rec.	financial_inst_code	,
            fnd.	secondary_account_number	=	upd_rec.	secondary_account_number	,
            fnd.	product_code	=	upd_rec.	product_code	,
            fnd.	sub_product_code	=	upd_rec.	sub_product_code	,
            fnd.	customer_key	=	upd_rec.	customer_key	,
            fnd.	ocs_txn_type_code	=	upd_rec.	ocs_txn_type_code	,
            fnd.	active_ind	=	upd_rec.	active_ind	,
            fnd.	account_status_code	=	upd_rec.	account_status_code	,
            fnd.	last_money_txn_code	=	upd_rec.	last_money_txn_code	,
            fnd.	language	=	upd_rec.	language	,
            fnd.	account_type_code	=	upd_rec.	account_type_code	,
            fnd.	cycle_code	=	upd_rec.	cycle_code	,
            fnd.	employee_ind	=	upd_rec.	employee_ind	,
            fnd.	expiry_date	=	upd_rec.	expiry_date	,
            fnd.	closed_date	=	upd_rec.	closed_date	,
            fnd.	last_state_date	=	upd_rec.	last_state_date	,
            fnd.	last_payment_date	=	upd_rec.	last_payment_date	,
            fnd.	last_money_txn_date	=	upd_rec.	last_money_txn_date	,
            fnd.	account_delinquent_date	=	upd_rec.	account_delinquent_date	,
            fnd.	number_days_past_due	=	upd_rec.	number_days_past_due	,
            fnd.	billing_cycle_day	=	upd_rec.	billing_cycle_day	,
            fnd.	number_times_in_collections	=	upd_rec.	number_times_in_collections	,
            fnd.	account_balance	=	upd_rec.	account_balance	,
            fnd.	billing_date_req_pay_amt	=	upd_rec.	billing_date_req_pay_amt	,
            fnd.	open_date	=	upd_rec.	open_date	,
            fnd.	number_cards_outstand	=	upd_rec.	number_cards_outstand	,
            fnd.	total_tenants	=	upd_rec.	total_tenants	,
            fnd.	number_plastic_comments	=	upd_rec.	number_plastic_comments	,
            fnd.	months_with_balance	=	upd_rec.	months_with_balance	,
            fnd.	last_status_change_date	=	upd_rec.	last_status_change_date	,
            fnd.	last_txn_date	=	upd_rec.	last_txn_date	,
            fnd.	last_dt_activity_date	=	upd_rec.	last_dt_activity_date	,
            fnd.	high_balance_amt	=	upd_rec.	high_balance_amt	,
            fnd.	calc_min_pay_amt	=	upd_rec.	calc_min_pay_amt	,
            fnd.	delinquent_amt	=	upd_rec.	delinquent_amt	,
            fnd.	next_pay_due_date	=	upd_rec.	next_pay_due_date	,
            fnd.	credit_limit	=	upd_rec.	credit_limit	,
            fnd.	current_avail_ct_limit	=	upd_rec.	current_avail_ct_limit	,
            fnd.	over_limit_amt	=	upd_rec.	over_limit_amt	,
            fnd.	reverse_code	=	upd_rec.	reverse_code	,
            fnd.	bad_payment_ind	=	upd_rec.	bad_payment_ind	,
            fnd.	behaviour_score	=	upd_rec.	behaviour_score	,
            fnd.	last_txn_type_code	=	upd_rec.	last_txn_type_code	,
            fnd.	budget_limit_amt	=	upd_rec.	budget_limit_amt	,
            fnd.	letter_ind	=	upd_rec.	letter_ind	,
            fnd.	balance_at_risk	=	upd_rec.	balance_at_risk	,
            fnd.	returned_mail_ind	=	upd_rec.	returned_mail_ind	,
            fnd.	payment_month_1	=	upd_rec.	payment_month_1	,
            fnd.	delinquent_hist_1	=	upd_rec.	delinquent_hist_1	,
            fnd.	credit_line_use_hist_1	=	upd_rec.	credit_line_use_hist_1	,
            fnd.	times_delinquent_1	=	upd_rec.	times_delinquent_1	,
            fnd.	delinquent_cat_1	=	upd_rec.	delinquent_cat_1	,
            fnd.	delinquent_amt_1	=	upd_rec.	delinquent_amt_1	,
            fnd.	transaction_desc_1	=	upd_rec.	transaction_desc_1	,
            fnd.	transaction_amt_1	=	upd_rec.	transaction_amt_1	,
            fnd.	transaction_date_1	=	upd_rec.	transaction_date_1	,
            fnd.  last_updated_date               = g_date
     where  fnd.	information_date                =	upd_rec.	information_date and
            fnd.	account_number                  =	upd_rec.	account_number   and
            ( 
            nvl(fnd.last_payment_amt	,0) <>	upd_rec.	last_payment_amt	or
            nvl(fnd.last_txn_amt	,0) <>	upd_rec.	last_txn_amt	or
            nvl(fnd.financial_inst_code	,0) <>	upd_rec.	financial_inst_code	or
            nvl(fnd.secondary_account_number	,0) <>	upd_rec.	secondary_account_number	or
            nvl(fnd.product_code	,0) <>	upd_rec.	product_code	or
            nvl(fnd.sub_product_code	,0) <>	upd_rec.	sub_product_code	or
            nvl(fnd.customer_key	,0) <>	upd_rec.	customer_key	or
            nvl(fnd.ocs_txn_type_code	,0) <>	upd_rec.	ocs_txn_type_code	or
            nvl(fnd.active_ind	,0) <>	upd_rec.	active_ind	or
            nvl(fnd.account_status_code	,0) <>	upd_rec.	account_status_code	or
            nvl(fnd.last_money_txn_code	,0) <>	upd_rec.	last_money_txn_code	or
            nvl(fnd.language	,0) <>	upd_rec.	language	or
            nvl(fnd.account_type_code	,0) <>	upd_rec.	account_type_code	or
            nvl(fnd.cycle_code	,0) <>	upd_rec.	cycle_code	or
            nvl(fnd.employee_ind	,0) <>	upd_rec.	employee_ind	or
            nvl(fnd.expiry_date	,'1 Jan 1900') <>	upd_rec.	expiry_date	or
            nvl(fnd.closed_date	,'1 Jan 1900') <>	upd_rec.	closed_date	or
            nvl(fnd.last_state_date	,'1 Jan 1900') <>	upd_rec.	last_state_date	or
            nvl(fnd.last_payment_date	,'1 Jan 1900') <>	upd_rec.	last_payment_date	or
            nvl(fnd.last_money_txn_date	,'1 Jan 1900') <>	upd_rec.	last_money_txn_date	or
            nvl(fnd.account_delinquent_date	,'1 Jan 1900') <>	upd_rec.	account_delinquent_date	or
            nvl(fnd.number_days_past_due	,0) <>	upd_rec.	number_days_past_due	or
            nvl(fnd.billing_cycle_day	,'1 Jan 1900') <>	upd_rec.	billing_cycle_day	or
            nvl(fnd.number_times_in_collections	,0) <>	upd_rec.	number_times_in_collections	or
            nvl(fnd.account_balance	,0) <>	upd_rec.	account_balance	or
            nvl(fnd.billing_date_req_pay_amt	,0) <>	upd_rec.	billing_date_req_pay_amt	or
            nvl(fnd.open_date	,'1 Jan 1900') <>	upd_rec.	open_date	or
            nvl(fnd.number_cards_outstand	,0) <>	upd_rec.	number_cards_outstand	or
            nvl(fnd.total_tenants	,0) <>	upd_rec.	total_tenants	or
            nvl(fnd.number_plastic_comments	,0) <>	upd_rec.	number_plastic_comments	or
            nvl(fnd.months_with_balance	,0) <>	upd_rec.	months_with_balance	or
            nvl(fnd.last_status_change_date	,'1 Jan 1900') <>	upd_rec.	last_status_change_date	or
            nvl(fnd.last_txn_date	,'1 Jan 1900') <>	upd_rec.	last_txn_date	or
            nvl(fnd.last_dt_activity_date	,'1 Jan 1900') <>	upd_rec.	last_dt_activity_date	or
            nvl(fnd.high_balance_amt	,0) <>	upd_rec.	high_balance_amt	or
            nvl(fnd.calc_min_pay_amt	,0) <>	upd_rec.	calc_min_pay_amt	or
            nvl(fnd.delinquent_amt	,0) <>	upd_rec.	delinquent_amt	or
            nvl(fnd.next_pay_due_date	,'1 Jan 1900') <>	upd_rec.	next_pay_due_date	or
            nvl(fnd.credit_limit	,0) <>	upd_rec.	credit_limit	or
            nvl(fnd.current_avail_ct_limit	,0) <>	upd_rec.	current_avail_ct_limit	or
            nvl(fnd.over_limit_amt	,0) <>	upd_rec.	over_limit_amt	or
            nvl(fnd.reverse_code	,0) <>	upd_rec.	reverse_code	or
            nvl(fnd.bad_payment_ind	,0) <>	upd_rec.	bad_payment_ind	or
            nvl(fnd.behaviour_score	,0) <>	upd_rec.	behaviour_score	or
            nvl(fnd.last_txn_type_code	,0) <>	upd_rec.	last_txn_type_code	or
            nvl(fnd.budget_limit_amt	,0) <>	upd_rec.	budget_limit_amt	or
            nvl(fnd.letter_ind	,0) <>	upd_rec.	letter_ind	or
            nvl(fnd.balance_at_risk	,0) <>	upd_rec.	balance_at_risk	or
            nvl(fnd.returned_mail_ind	,0) <>	upd_rec.	returned_mail_ind	or
            nvl(fnd.payment_month_1	,0) <>	upd_rec.	payment_month_1	or
            nvl(fnd.delinquent_hist_1	,0) <>	upd_rec.	delinquent_hist_1	or
            nvl(fnd.credit_line_use_hist_1	,0) <>	upd_rec.	credit_line_use_hist_1	or
            nvl(fnd.times_delinquent_1	,0) <>	upd_rec.	times_delinquent_1	or
            nvl(fnd.delinquent_cat_1	,0) <>	upd_rec.	delinquent_cat_1	or
            nvl(fnd.delinquent_amt_1	,0) <>	upd_rec.	delinquent_amt_1	or
            nvl(fnd.transaction_desc_1	,0) <>	upd_rec.	transaction_desc_1	or
            nvl(fnd.transaction_amt_1	,0) <>	upd_rec.	transaction_amt_1	or
            nvl(fnd.transaction_date_1	,'1 Jan 1900') <>	upd_rec.	transaction_date_1 

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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_ocs_card_daily_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
            	cpy.	information_date	,
            	cpy.	account_number	,
            	cpy.	last_payment_amt	,
            	cpy.	last_txn_amt	,
            	cpy.	financial_inst_code	,
            	cpy.	secondary_account_number	,
            	cpy.	product_code	,
            	cpy.	sub_product_code	,
            	cpy.	customer_key	,
            	cpy.	ocs_txn_type_code	,
            	cpy.	active_ind	,
            	cpy.	account_status_code	,
            	cpy.	last_money_txn_code	,
            	cpy.	language	,
            	cpy.	account_type_code	,
            	cpy.	cycle_code	,
            	cpy.	employee_ind	,
            	cpy.	expiry_date	,
            	cpy.	closed_date	,
            	cpy.	last_state_date	,
            	cpy.	last_payment_date	,
            	cpy.	last_money_txn_date	,
            	cpy.	account_delinquent_date	,
            	cpy.	number_days_past_due	,
            	cpy.	billing_cycle_day	,
            	cpy.	number_times_in_collections	,
            	cpy.	account_balance	,
            	cpy.	billing_date_req_pay_amt	,
            	cpy.	open_date	,
            	cpy.	number_cards_outstand	,
            	cpy.	total_tenants	,
            	cpy.	number_plastic_comments	,
            	cpy.	months_with_balance	,
            	cpy.	last_status_change_date	,
            	cpy.	last_txn_date	,
            	cpy.	last_dt_activity_date	,
            	cpy.	high_balance_amt	,
            	cpy.	calc_min_pay_amt	,
            	cpy.	delinquent_amt	,
            	cpy.	next_pay_due_date	,
            	cpy.	credit_limit	,
            	cpy.	current_avail_ct_limit	,
            	cpy.	over_limit_amt	,
            	cpy.	reverse_code	,
            	cpy.	bad_payment_ind	,
            	cpy.	behaviour_score	,
            	cpy.	last_txn_type_code	,
            	cpy.	budget_limit_amt	,
            	cpy.	letter_ind	,
            	cpy.	balance_at_risk	,
            	cpy.	returned_mail_ind	,
            	cpy.	payment_month_1	,
            	cpy.	delinquent_hist_1	,
            	cpy.	credit_line_use_hist_1	,
            	cpy.	times_delinquent_1	,
            	cpy.	delinquent_cat_1	,
            	cpy.	delinquent_amt_1	,
            	cpy.	transaction_desc_1	,
            	cpy.	transaction_amt_1	,
            	cpy.	transaction_date_1	 
      from   stg_absa_ocs_card_daily_cpy cpy
      where  
--      (    
--      NOT EXISTS 
--        (SELECT * FROM  dim_table dim
--         where  cpy.xxx       = dim.xxx ) or
--      not exists 
--        (select * from  dim_table dim1
--         where  cpy.xxx    = dim1.xxx ) 
--      ) and 
-- Any further validation goes in here - like or xxx.ind not in (0,1) ---        
        sys_process_code = 'N';
         

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
    
    select count(*)
    into   g_recs_read
    from   stg_absa_ocs_card_daily_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;


--********** REMOVED AS THERE IS NO VALIDATION AND THUS NOT RECORDS GO TO HOSPITAL ******************    
--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_absa_ocs_card_daily_cpy
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
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
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
end wh_fnd_wfs_322u;
