--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_304U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_304U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_crd_acc_wly fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_crd_acc_wly_cpy
--               Output - fnd_wfs_crd_acc_wly
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 Mar 2013 - Change to a BULK Insert/update load to speed up 10x
--  03 Nov 2016 - N Chauhan - 2 fields added for statements
--  03 Nov 2016 - N Chauhan - 2 fields added for FICA -- Removed to ABSA Customer 10 Nov 2016
--  10 Nov 2016 - N Chauhan - 2 FICA fields removed again, to be incorporated in ABSA Customer 
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


g_information_date                 stg_absa_crd_acc_wly_cpy.information_date%type;  
g_account_number                   stg_absa_crd_acc_wly_cpy.account_number%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_304U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS CARD ACC WLY EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_absa_crd_acc_wly_cpy
where (information_date,
account_number)
in
(select information_date,
account_number
from stg_absa_crd_acc_wly_cpy 
group by information_date,
account_number
having count(*) > 1) 
order by information_date,
account_number,
sys_source_batch_id desc ,sys_source_sequence_no desc;

/*+ FULL(stg) parallel (stg,4)  full(fnd) parallel (fnd,4)  */

cursor c_stg_absa_crd_acc_wly is
select /*+ FULL(stg)  parallel (stg,4) FULL(fnd)  parallel (fnd,4) */  
              stg.*
      from    stg_absa_crd_acc_wly_cpy stg,
              fnd_wfs_crd_acc_wly fnd
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
        update stg_absa_crd_acc_wly_cpy stg
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
      
      insert /*+ APPEND parallel (fnd,4) */ into fnd_wfs_crd_acc_wly fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,4) */
             	cpy.	account_number	,
            	cpy.	information_date	,
            	cpy.	account_balance	,
            	cpy.	act_date	,
            	cpy.	act_type_code	,
            	cpy.	application_credit_score	,
            	cpy.	automatic_pay_ind	,
            	cpy.	automatic_payment_fixed_amt	,
            	cpy.	automatic_payment_ind	,
            	cpy.	automatic_payment_trigger_date	,
            	cpy.	avg_credit_balance	,
            	cpy.	avg_debit_balance	,
            	cpy.	behaviour_score	,
            	cpy.	behaviour_score_ind	,
            	cpy.	brand_short_name	,
            	cpy.	budget_interest_rate	,
            	cpy.	budget_limit_amount	,
            	cpy.	card_account_status_code	,
            	cpy.	card_account_type_code	,
            	cpy.	card_closed_reason_code	,
            	cpy.	card_collector_code	,
            	cpy.	card_cycle_code	,
            	cpy.	card_insurance_type_code	,
            	cpy.	card_product_type_code	,
            	cpy.	card_production_brand	,
            	cpy.	cards_outstanding	,
            	cpy.	closed_date	,
            	cpy.	collector_code	,
            	cpy.	cr_life_policy_bal_insured_amt	,
            	cpy.	credit_bureau_score	,
            	cpy.	credit_bureau_score_date	,
            	cpy.	customer_key	,
            	cpy.	customer_value_score	,
            	cpy.	cycle_begin_date	,
            	cpy.	date_exceeded_purchase_limit	,
            	cpy.	day_5_delinquent_amt	,
            	cpy.	day_30_delinquent_amt	,
            	cpy.	day_60_delinquent_amt	,
            	cpy.	day_90_delinquent_amt	,
            	cpy.	day_120_delinquent_amt	,
            	cpy.	day_150_delinquent_amt	,
            	cpy.	day_180_delinquent_amt	,
            	cpy.	day_210_delinquent_amt	,
            	cpy.	debt_counsel_consent_ind	,
            	cpy.	debt_counsel_date	,
            	cpy.	debt_counsel_hold_ind	,
            	cpy.	delinquent_amount	,
            	cpy.	delinquent_cycles_count	,
            	cpy.	delinquent_date	,
            	cpy.	delinquent_ind	,
            	cpy.	derived_card_acct_status_code	,
            	cpy.	electronic_mail_code	,
            	cpy.	except_nca_usury_act_ind	,
            	cpy.	first_active_date	,
            	cpy.	first_date_card_issued	,
            	cpy.	last_client_ct_pmt_amt	,
            	cpy.	last_client_ct_pmt_date	,
            	cpy.	last_client_ct_pmt_txn_code	,
            	cpy.	last_date_account_maintenance	,
            	cpy.	last_date_cr_score_calculation	,
            	cpy.	last_date_cr_score_reviewed	,
            	cpy.	last_date_fraud_activity	,
            	cpy.	last_date_memship_fee_charged	,
            	cpy.	last_date_monetary_txn	,
            	cpy.	last_date_purchase_limit_chang	,
            	cpy.	last_date_reported_cr_bureau	,
            	cpy.	last_db_txn_by_cardh_amt	,
            	cpy.	last_db_txn_by_cardh_catg_code	,
            	cpy.	last_db_txn_by_cardh_date	,
            	cpy.	last_txn_on_acct_amt	,
            	cpy.	last_txn_on_acct_code	,
            	cpy.	limit_decrease_ind	,
            	cpy.	limit_increase_ind	,
            	cpy.	lost_card_protection_ind	,
            	cpy.	manual_credit_line	,
            	cpy.	maturity_date	,
            	cpy.	memship_fee_to_be_waived_ind	,
            	cpy.	next_date_memship_fee_charge	,
            	cpy.	num_times_exceed_purchase_lim	,
            	cpy.	number_purchase_limit_increase	,
            	cpy.	ocs_processing_ind	,
            	cpy.	ocs_reason_code	,
            	cpy.	open_date	,
            	cpy.	over_limit_amt	,
            	cpy.	possible_close_code	,
            	cpy.	prev_card_account_status_code	,
            	cpy.	product_code	,
            	cpy.	purchase_limit_amt	,
            	cpy.	random_number	,
            	cpy.	random_number_digit	,
            	cpy.	reason_applic_cr_score_overrid	,
            	cpy.	reiss_ind	,
            	cpy.	related_comm_bank_acct_number	,
            	cpy.	site_code	,
            	cpy.	site_code_card_div	,
            	cpy.	skip_payment_ind	,
            	cpy.	status_date	,
            	cpy.	sub_product_code	,
            	cpy.	system_active_ind	,
            	cpy.	system_credit_line	,
            	cpy.	total_budget_balance_amt	,
            	cpy.	total_credit_score	,
            	cpy.	transfer_account_number	,
            	cpy.	transfer_acct_reason_code	,
            	cpy.	transfer_to_legal_orig_date	,
            	cpy.	triad_sub_product_code	,
              g_date as last_updated_date ,

              cpy. statement_payment_amt ,
              cpy. next_payment_due_date  
              
              
      from   stg_absa_crd_acc_wly_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_crd_acc_wly 
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



for upd_rec in c_stg_absa_crd_acc_wly
   loop
     update fnd_wfs_crd_acc_wly fnd 
     set    fnd.	account_number	=	upd_rec.	account_number	,
            fnd.	information_date	=	upd_rec.	information_date	,
            fnd.	account_balance	=	upd_rec.	account_balance	,
            fnd.	act_date	=	upd_rec.	act_date	,
            fnd.	act_type_code	=	upd_rec.	act_type_code	,
            fnd.	application_credit_score	=	upd_rec.	application_credit_score	,
            fnd.	automatic_pay_ind	=	upd_rec.	automatic_pay_ind	,
            fnd.	automatic_payment_fixed_amt	=	upd_rec.	automatic_payment_fixed_amt	,
            fnd.	automatic_payment_ind	=	upd_rec.	automatic_payment_ind	,
            fnd.	automatic_payment_trigger_date	=	upd_rec.	automatic_payment_trigger_date	,
            fnd.	avg_credit_balance	=	upd_rec.	avg_credit_balance	,
            fnd.	avg_debit_balance	=	upd_rec.	avg_debit_balance	,
            fnd.	behaviour_score	=	upd_rec.	behaviour_score	,
            fnd.	behaviour_score_ind	=	upd_rec.	behaviour_score_ind	,
            fnd.	brand_short_name	=	upd_rec.	brand_short_name	,
            fnd.	budget_interest_rate	=	upd_rec.	budget_interest_rate	,
            fnd.	budget_limit_amount	=	upd_rec.	budget_limit_amount	,
            fnd.	card_account_status_code	=	upd_rec.	card_account_status_code	,
            fnd.	card_account_type_code	=	upd_rec.	card_account_type_code	,
            fnd.	card_closed_reason_code	=	upd_rec.	card_closed_reason_code	,
            fnd.	card_collector_code	=	upd_rec.	card_collector_code	,
            fnd.	card_cycle_code	=	upd_rec.	card_cycle_code	,
            fnd.	card_insurance_type_code	=	upd_rec.	card_insurance_type_code	,
            fnd.	card_product_type_code	=	upd_rec.	card_product_type_code	,
            fnd.	card_production_brand	=	upd_rec.	card_production_brand	,
            fnd.	cards_outstanding	=	upd_rec.	cards_outstanding	,
            fnd.	closed_date	=	upd_rec.	closed_date	,
            fnd.	collector_code	=	upd_rec.	collector_code	,
            fnd.	cr_life_policy_bal_insured_amt	=	upd_rec.	cr_life_policy_bal_insured_amt	,
            fnd.	credit_bureau_score	=	upd_rec.	credit_bureau_score	,
            fnd.	credit_bureau_score_date	=	upd_rec.	credit_bureau_score_date	,
            fnd.	customer_key	=	upd_rec.	customer_key	,
            fnd.	customer_value_score	=	upd_rec.	customer_value_score	,
            fnd.	cycle_begin_date	=	upd_rec.	cycle_begin_date	,
            fnd.	date_exceeded_purchase_limit	=	upd_rec.	date_exceeded_purchase_limit	,
            fnd.	day_5_delinquent_amt	=	upd_rec.	day_5_delinquent_amt	,
            fnd.	day_30_delinquent_amt	=	upd_rec.	day_30_delinquent_amt	,
            fnd.	day_60_delinquent_amt	=	upd_rec.	day_60_delinquent_amt	,
            fnd.	day_90_delinquent_amt	=	upd_rec.	day_90_delinquent_amt	,
            fnd.	day_120_delinquent_amt	=	upd_rec.	day_120_delinquent_amt	,
            fnd.	day_150_delinquent_amt	=	upd_rec.	day_150_delinquent_amt	,
            fnd.	day_180_delinquent_amt	=	upd_rec.	day_180_delinquent_amt	,
            fnd.	day_210_delinquent_amt	=	upd_rec.	day_210_delinquent_amt	,
            fnd.	debt_counsel_consent_ind	=	upd_rec.	debt_counsel_consent_ind	,
            fnd.	debt_counsel_date	=	upd_rec.	debt_counsel_date	,
            fnd.	debt_counsel_hold_ind	=	upd_rec.	debt_counsel_hold_ind	,
            fnd.	delinquent_amount	=	upd_rec.	delinquent_amount	,
            fnd.	delinquent_cycles_count	=	upd_rec.	delinquent_cycles_count	,
            fnd.	delinquent_date	=	upd_rec.	delinquent_date	,
            fnd.	delinquent_ind	=	upd_rec.	delinquent_ind	,
            fnd.	derived_card_acct_status_code	=	upd_rec.	derived_card_acct_status_code	,
            fnd.	electronic_mail_code	=	upd_rec.	electronic_mail_code	,
            fnd.	except_nca_usury_act_ind	=	upd_rec.	except_nca_usury_act_ind	,
            fnd.	first_active_date	=	upd_rec.	first_active_date	,
            fnd.	first_date_card_issued	=	upd_rec.	first_date_card_issued	,
            fnd.	last_client_ct_pmt_amt	=	upd_rec.	last_client_ct_pmt_amt	,
            fnd.	last_client_ct_pmt_date	=	upd_rec.	last_client_ct_pmt_date	,
            fnd.	last_client_ct_pmt_txn_code	=	upd_rec.	last_client_ct_pmt_txn_code	,
            fnd.	last_date_account_maintenance	=	upd_rec.	last_date_account_maintenance	,
            fnd.	last_date_cr_score_calculation	=	upd_rec.	last_date_cr_score_calculation	,
            fnd.	last_date_cr_score_reviewed	=	upd_rec.	last_date_cr_score_reviewed	,
            fnd.	last_date_fraud_activity	=	upd_rec.	last_date_fraud_activity	,
            fnd.	last_date_memship_fee_charged	=	upd_rec.	last_date_memship_fee_charged	,
            fnd.	last_date_monetary_txn	=	upd_rec.	last_date_monetary_txn	,
            fnd.	last_date_purchase_limit_chang	=	upd_rec.	last_date_purchase_limit_chang	,
            fnd.	last_date_reported_cr_bureau	=	upd_rec.	last_date_reported_cr_bureau	,
            fnd.	last_db_txn_by_cardh_amt	=	upd_rec.	last_db_txn_by_cardh_amt	,
            fnd.	last_db_txn_by_cardh_catg_code	=	upd_rec.	last_db_txn_by_cardh_catg_code	,
            fnd.	last_db_txn_by_cardh_date	=	upd_rec.	last_db_txn_by_cardh_date	,
            fnd.	last_txn_on_acct_amt	=	upd_rec.	last_txn_on_acct_amt	,
            fnd.	last_txn_on_acct_code	=	upd_rec.	last_txn_on_acct_code	,
            fnd.	limit_decrease_ind	=	upd_rec.	limit_decrease_ind	,
            fnd.	limit_increase_ind	=	upd_rec.	limit_increase_ind	,
            fnd.	lost_card_protection_ind	=	upd_rec.	lost_card_protection_ind	,
            fnd.	manual_credit_line	=	upd_rec.	manual_credit_line	,
            fnd.	maturity_date	=	upd_rec.	maturity_date	,
            fnd.	memship_fee_to_be_waived_ind	=	upd_rec.	memship_fee_to_be_waived_ind	,
            fnd.	next_date_memship_fee_charge	=	upd_rec.	next_date_memship_fee_charge	,
            fnd.	num_times_exceed_purchase_lim	=	upd_rec.	num_times_exceed_purchase_lim	,
            fnd.	number_purchase_limit_increase	=	upd_rec.	number_purchase_limit_increase	,
            fnd.	ocs_processing_ind	=	upd_rec.	ocs_processing_ind	,
            fnd.	ocs_reason_code	=	upd_rec.	ocs_reason_code	,
            fnd.	open_date	=	upd_rec.	open_date	,
            fnd.	over_limit_amt	=	upd_rec.	over_limit_amt	,
            fnd.	possible_close_code	=	upd_rec.	possible_close_code	,
            fnd.	prev_card_account_status_code	=	upd_rec.	prev_card_account_status_code	,
            fnd.	product_code	=	upd_rec.	product_code	,
            fnd.	purchase_limit_amt	=	upd_rec.	purchase_limit_amt	,
            fnd.	random_number	=	upd_rec.	random_number	,
            fnd.	random_number_digit	=	upd_rec.	random_number_digit	,
            fnd.	reason_applic_cr_score_overrid	=	upd_rec.	reason_applic_cr_score_overrid	,
            fnd.	reiss_ind	=	upd_rec.	reiss_ind	,
            fnd.	related_comm_bank_acct_number	=	upd_rec.	related_comm_bank_acct_number	,
            fnd.	site_code	=	upd_rec.	site_code	,
            fnd.	site_code_card_div	=	upd_rec.	site_code_card_div	,
            fnd.	skip_payment_ind	=	upd_rec.	skip_payment_ind	,
            fnd.	status_date	=	upd_rec.	status_date	,
            fnd.	sub_product_code	=	upd_rec.	sub_product_code	,
            fnd.	system_active_ind	=	upd_rec.	system_active_ind	,
            fnd.	system_credit_line	=	upd_rec.	system_credit_line	,
            fnd.	total_budget_balance_amt	=	upd_rec.	total_budget_balance_amt	,
            fnd.	total_credit_score	=	upd_rec.	total_credit_score	,
            fnd.	transfer_account_number	=	upd_rec.	transfer_account_number	,
            fnd.	transfer_acct_reason_code	=	upd_rec.	transfer_acct_reason_code	,
            fnd.	transfer_to_legal_orig_date	=	upd_rec.	transfer_to_legal_orig_date	,
            fnd.	triad_sub_product_code	=	upd_rec.	triad_sub_product_code	,

            fnd.  last_updated_date               = g_date ,
            
            fnd. statement_payment_amt = upd_rec. statement_payment_amt ,
            fnd. next_payment_due_date = upd_rec. next_payment_due_date 
--            fnd. fica_blocked_date = upd_rec. fica_blocked_date ,
--            fnd. fica_blocked_by = upd_rec. fica_blocked_by
  
            
     where  fnd.	information_date                =	upd_rec.	information_date and
            fnd.	account_number                  =	upd_rec.	account_number   
/*            and
            ( 
            nvl(fnd.account_number	,0) <>	upd_rec.	account_number	or
            nvl(fnd.information_date	,'1 Jan 1900') <>	upd_rec.	information_date	or
            nvl(fnd.account_balance	,0) <>	upd_rec.	account_balance	or
            nvl(fnd.act_date	,'1 Jan 1900') <>	upd_rec.	act_date	or
            nvl(fnd.act_type_code	,0) <>	upd_rec.	act_type_code	or
            nvl(fnd.application_credit_score	,0) <>	upd_rec.	application_credit_score	or
            nvl(fnd.automatic_pay_ind	,0) <>	upd_rec.	automatic_pay_ind	or
            nvl(fnd.automatic_payment_fixed_amt	,0) <>	upd_rec.	automatic_payment_fixed_amt	or
            nvl(fnd.automatic_payment_ind	,0) <>	upd_rec.	automatic_payment_ind	or
            nvl(fnd.automatic_payment_trigger_date	,'1 Jan 1900') <>	upd_rec.	automatic_payment_trigger_date	or
            nvl(fnd.avg_credit_balance	,0) <>	upd_rec.	avg_credit_balance	or
            nvl(fnd.avg_debit_balance	,0) <>	upd_rec.	avg_debit_balance	or
            nvl(fnd.behaviour_score	,0) <>	upd_rec.	behaviour_score	or
            nvl(fnd.behaviour_score_ind	,0) <>	upd_rec.	behaviour_score_ind	or
            nvl(fnd.brand_short_name	,0) <>	upd_rec.	brand_short_name	or
            nvl(fnd.budget_interest_rate	,0) <>	upd_rec.	budget_interest_rate	or
            nvl(fnd.budget_limit_amount	,0) <>	upd_rec.	budget_limit_amount	or
            nvl(fnd.card_account_status_code	,0) <>	upd_rec.	card_account_status_code	or
            nvl(fnd.card_account_type_code	,0) <>	upd_rec.	card_account_type_code	or
            nvl(fnd.card_closed_reason_code	,0) <>	upd_rec.	card_closed_reason_code	or
            nvl(fnd.card_collector_code	,0) <>	upd_rec.	card_collector_code	or
            nvl(fnd.card_cycle_code	,0) <>	upd_rec.	card_cycle_code	or
            nvl(fnd.card_insurance_type_code	,0) <>	upd_rec.	card_insurance_type_code	or
            nvl(fnd.card_product_type_code	,0) <>	upd_rec.	card_product_type_code	or
            nvl(fnd.card_production_brand	,0) <>	upd_rec.	card_production_brand	or
            nvl(fnd.cards_outstanding	,0) <>	upd_rec.	cards_outstanding	or
            nvl(fnd.closed_date	,'1 Jan 1900') <>	upd_rec.	closed_date	or
            nvl(fnd.collector_code	,0) <>	upd_rec.	collector_code	or
            nvl(fnd.cr_life_policy_bal_insured_amt	,0) <>	upd_rec.	cr_life_policy_bal_insured_amt	or
            nvl(fnd.credit_bureau_score	,0) <>	upd_rec.	credit_bureau_score	or
            nvl(fnd.credit_bureau_score_date	,'1 Jan 1900') <>	upd_rec.	credit_bureau_score_date	or
            nvl(fnd.customer_key	,0) <>	upd_rec.	customer_key	or
            nvl(fnd.customer_value_score	,0) <>	upd_rec.	customer_value_score	or
            nvl(fnd.cycle_begin_date	,'1 Jan 1900') <>	upd_rec.	cycle_begin_date	or
            nvl(fnd.date_exceeded_purchase_limit	,'1 Jan 1900') <>	upd_rec.	date_exceeded_purchase_limit	or
            nvl(fnd.day_5_delinquent_amt	,0) <>	upd_rec.	day_5_delinquent_amt	or
            nvl(fnd.day_30_delinquent_amt	,0) <>	upd_rec.	day_30_delinquent_amt	or
            nvl(fnd.day_60_delinquent_amt	,0) <>	upd_rec.	day_60_delinquent_amt	or
            nvl(fnd.day_90_delinquent_amt	,0) <>	upd_rec.	day_90_delinquent_amt	or
            nvl(fnd.day_120_delinquent_amt	,0) <>	upd_rec.	day_120_delinquent_amt	or
            nvl(fnd.day_150_delinquent_amt	,0) <>	upd_rec.	day_150_delinquent_amt	or
            nvl(fnd.day_180_delinquent_amt	,0) <>	upd_rec.	day_180_delinquent_amt	or
            nvl(fnd.day_210_delinquent_amt	,0) <>	upd_rec.	day_210_delinquent_amt	or
            nvl(fnd.debt_counsel_consent_ind	,0) <>	upd_rec.	debt_counsel_consent_ind	or
            nvl(fnd.debt_counsel_date	,'1 Jan 1900') <>	upd_rec.	debt_counsel_date	or
            nvl(fnd.debt_counsel_hold_ind	,0) <>	upd_rec.	debt_counsel_hold_ind	or
            nvl(fnd.delinquent_amount	,0) <>	upd_rec.	delinquent_amount	or
            nvl(fnd.delinquent_cycles_count	,0) <>	upd_rec.	delinquent_cycles_count	or
            nvl(fnd.delinquent_date	,'1 Jan 1900') <>	upd_rec.	delinquent_date	or
            nvl(fnd.delinquent_ind	,0) <>	upd_rec.	delinquent_ind	or
            nvl(fnd.derived_card_acct_status_code	,0) <>	upd_rec.	derived_card_acct_status_code	or
            nvl(fnd.electronic_mail_code	,0) <>	upd_rec.	electronic_mail_code	or
            nvl(fnd.except_nca_usury_act_ind	,0) <>	upd_rec.	except_nca_usury_act_ind	or
            nvl(fnd.first_active_date	,'1 Jan 1900') <>	upd_rec.	first_active_date	or
            nvl(fnd.first_date_card_issued	,'1 Jan 1900') <>	upd_rec.	first_date_card_issued	or
            nvl(fnd.last_client_ct_pmt_amt	,0) <>	upd_rec.	last_client_ct_pmt_amt	or
            nvl(fnd.last_client_ct_pmt_date	,'1 Jan 1900') <>	upd_rec.	last_client_ct_pmt_date	or
            nvl(fnd.last_client_ct_pmt_txn_code	,0) <>	upd_rec.	last_client_ct_pmt_txn_code	or
            nvl(fnd.last_date_account_maintenance	,'1 Jan 1900') <>	upd_rec.	last_date_account_maintenance	or
            nvl(fnd.last_date_cr_score_calculation	,'1 Jan 1900') <>	upd_rec.	last_date_cr_score_calculation	or
            nvl(fnd.last_date_cr_score_reviewed	,'1 Jan 1900') <>	upd_rec.	last_date_cr_score_reviewed	or
            nvl(fnd.last_date_fraud_activity	,'1 Jan 1900') <>	upd_rec.	last_date_fraud_activity	or
            nvl(fnd.last_date_memship_fee_charged	,'1 Jan 1900') <>	upd_rec.	last_date_memship_fee_charged	or
            nvl(fnd.last_date_monetary_txn	,'1 Jan 1900') <>	upd_rec.	last_date_monetary_txn	or
            nvl(fnd.last_date_purchase_limit_chang	,'1 Jan 1900') <>	upd_rec.	last_date_purchase_limit_chang	or
            nvl(fnd.last_date_reported_cr_bureau	,'1 Jan 1900') <>	upd_rec.	last_date_reported_cr_bureau	or
            nvl(fnd.last_db_txn_by_cardh_amt	,0) <>	upd_rec.	last_db_txn_by_cardh_amt	or
            nvl(fnd.last_db_txn_by_cardh_catg_code	,0) <>	upd_rec.	last_db_txn_by_cardh_catg_code	or
            nvl(fnd.last_db_txn_by_cardh_date	,'1 Jan 1900') <>	upd_rec.	last_db_txn_by_cardh_date	or
            nvl(fnd.last_txn_on_acct_amt	,0) <>	upd_rec.	last_txn_on_acct_amt	or
            nvl(fnd.last_txn_on_acct_code	,0) <>	upd_rec.	last_txn_on_acct_code	or
            nvl(fnd.limit_decrease_ind	,0) <>	upd_rec.	limit_decrease_ind	or
            nvl(fnd.limit_increase_ind	,0) <>	upd_rec.	limit_increase_ind	or
            nvl(fnd.lost_card_protection_ind	,0) <>	upd_rec.	lost_card_protection_ind	or
            nvl(fnd.manual_credit_line	,0) <>	upd_rec.	manual_credit_line	or
            nvl(fnd.maturity_date	,'1 Jan 1900') <>	upd_rec.	maturity_date	or
            nvl(fnd.memship_fee_to_be_waived_ind	,0) <>	upd_rec.	memship_fee_to_be_waived_ind	or
            nvl(fnd.next_date_memship_fee_charge	,'1 Jan 1900') <>	upd_rec.	next_date_memship_fee_charge	or
            nvl(fnd.num_times_exceed_purchase_lim	,0) <>	upd_rec.	num_times_exceed_purchase_lim	or
            nvl(fnd.number_purchase_limit_increase	,0) <>	upd_rec.	number_purchase_limit_increase	or
            nvl(fnd.ocs_processing_ind	,0) <>	upd_rec.	ocs_processing_ind	or
            nvl(fnd.ocs_reason_code	,0) <>	upd_rec.	ocs_reason_code	or
            nvl(fnd.open_date	,'1 Jan 1900') <>	upd_rec.	open_date	or
            nvl(fnd.over_limit_amt	,0) <>	upd_rec.	over_limit_amt	or
            nvl(fnd.possible_close_code	,0) <>	upd_rec.	possible_close_code	or
            nvl(fnd.prev_card_account_status_code	,0) <>	upd_rec.	prev_card_account_status_code	or
            nvl(fnd.product_code	,0) <>	upd_rec.	product_code	or
            nvl(fnd.purchase_limit_amt	,0) <>	upd_rec.	purchase_limit_amt	or
            nvl(fnd.random_number	,0) <>	upd_rec.	random_number	or
            nvl(fnd.random_number_digit	,0) <>	upd_rec.	random_number_digit	or
            nvl(fnd.reason_applic_cr_score_overrid	,0) <>	upd_rec.	reason_applic_cr_score_overrid	or
            nvl(fnd.reiss_ind	,0) <>	upd_rec.	reiss_ind	or
            nvl(fnd.related_comm_bank_acct_number	,0) <>	upd_rec.	related_comm_bank_acct_number	or
            nvl(fnd.site_code	,0) <>	upd_rec.	site_code	or
            nvl(fnd.site_code_card_div	,0) <>	upd_rec.	site_code_card_div	or
            nvl(fnd.skip_payment_ind	,0) <>	upd_rec.	skip_payment_ind	or
            nvl(fnd.status_date	,'1 Jan 1900') <>	upd_rec.	status_date	or
            nvl(fnd.sub_product_code	,0) <>	upd_rec.	sub_product_code	or
            nvl(fnd.system_active_ind	,0) <>	upd_rec.	system_active_ind	or
            nvl(fnd.system_credit_line	,0) <>	upd_rec.	system_credit_line	or
            nvl(fnd.total_budget_balance_amt	,0) <>	upd_rec.	total_budget_balance_amt	or
            nvl(fnd.total_credit_score	,0) <>	upd_rec.	total_credit_score	or
            nvl(fnd.transfer_account_number	,0) <>	upd_rec.	transfer_account_number	or
            nvl(fnd.transfer_acct_reason_code	,0) <>	upd_rec.	transfer_acct_reason_code	or
            nvl(fnd.transfer_to_legal_orig_date	,'1 Jan 1900') <>	upd_rec.	transfer_to_legal_orig_date	or
            nvl(fnd.triad_sub_product_code	,0) <>	upd_rec.	triad_sub_product_code	) or

            nvl(fnd. statement_payment_amt, 0) <> upd_rec. statement_payment_amt OR
            nvl(fnd. next_payment_due_date, '01 JAN 1900') <> upd_rec. next_payment_due_date 

*/       
            ;         
             
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_crd_acc_wly_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
              cpy.sys_source_batch_id,
              cpy.sys_source_sequence_no,
              sysdate,'Y','DWH',
              cpy.sys_middleware_batch_id,
              'VALIDATION FAIL - REFERENCIAL ERROR',
            	cpy.	account_number	,
            	cpy.	information_date	,
            	cpy.	account_balance	,
            	cpy.	act_date	,
            	cpy.	act_type_code	,
            	cpy.	application_credit_score	,
            	cpy.	automatic_pay_ind	,
            	cpy.	automatic_payment_fixed_amt	,
            	cpy.	automatic_payment_ind	,
            	cpy.	automatic_payment_trigger_date	,
            	cpy.	avg_credit_balance	,
            	cpy.	avg_debit_balance	,
            	cpy.	behaviour_score	,
            	cpy.	behaviour_score_ind	,
            	cpy.	brand_short_name	,
            	cpy.	budget_interest_rate	,
            	cpy.	budget_limit_amount	,
            	cpy.	card_account_status_code	,
            	cpy.	card_account_type_code	,
            	cpy.	card_closed_reason_code	,
            	cpy.	card_collector_code	,
            	cpy.	card_cycle_code	,
            	cpy.	card_insurance_type_code	,
            	cpy.	card_product_type_code	,
            	cpy.	card_production_brand	,
            	cpy.	cards_outstanding	,
            	cpy.	closed_date	,
            	cpy.	collector_code	,
            	cpy.	cr_life_policy_bal_insured_amt	,
            	cpy.	credit_bureau_score	,
            	cpy.	credit_bureau_score_date	,
            	cpy.	customer_key	,
            	cpy.	customer_value_score	,
            	cpy.	cycle_begin_date	,
            	cpy.	date_exceeded_purchase_limit	,
            	cpy.	day_5_delinquent_amt	,
            	cpy.	day_30_delinquent_amt	,
            	cpy.	day_60_delinquent_amt	,
            	cpy.	day_90_delinquent_amt	,
            	cpy.	day_120_delinquent_amt	,
            	cpy.	day_150_delinquent_amt	,
            	cpy.	day_180_delinquent_amt	,
            	cpy.	day_210_delinquent_amt	,
            	cpy.	debt_counsel_consent_ind	,
            	cpy.	debt_counsel_date	,
            	cpy.	debt_counsel_hold_ind	,
            	cpy.	delinquent_amount	,
            	cpy.	delinquent_cycles_count	,
            	cpy.	delinquent_date	,
            	cpy.	delinquent_ind	,
            	cpy.	derived_card_acct_status_code	,
            	cpy.	electronic_mail_code	,
            	cpy.	except_nca_usury_act_ind	,
            	cpy.	first_active_date	,
            	cpy.	first_date_card_issued	,
            	cpy.	last_client_ct_pmt_amt	,
            	cpy.	last_client_ct_pmt_date	,
            	cpy.	last_client_ct_pmt_txn_code	,
            	cpy.	last_date_account_maintenance	,
            	cpy.	last_date_cr_score_calculation	,
            	cpy.	last_date_cr_score_reviewed	,
            	cpy.	last_date_fraud_activity	,
            	cpy.	last_date_memship_fee_charged	,
            	cpy.	last_date_monetary_txn	,
            	cpy.	last_date_purchase_limit_chang	,
            	cpy.	last_date_reported_cr_bureau	,
            	cpy.	last_db_txn_by_cardh_amt	,
            	cpy.	last_db_txn_by_cardh_catg_code	,
            	cpy.	last_db_txn_by_cardh_date	,
            	cpy.	last_txn_on_acct_amt	,
            	cpy.	last_txn_on_acct_code	,
            	cpy.	limit_decrease_ind	,
            	cpy.	limit_increase_ind	,
            	cpy.	lost_card_protection_ind	,
            	cpy.	manual_credit_line	,
            	cpy.	maturity_date	,
            	cpy.	memship_fee_to_be_waived_ind	,
            	cpy.	next_date_memship_fee_charge	,
            	cpy.	num_times_exceed_purchase_lim	,
            	cpy.	number_purchase_limit_increase	,
            	cpy.	ocs_processing_ind	,
            	cpy.	ocs_reason_code	,
            	cpy.	open_date	,
            	cpy.	over_limit_amt	,
            	cpy.	possible_close_code	,
            	cpy.	prev_card_account_status_code	,
            	cpy.	product_code	,
            	cpy.	purchase_limit_amt	,
            	cpy.	random_number	,
            	cpy.	random_number_digit	,
            	cpy.	reason_applic_cr_score_overrid	,
            	cpy.	reiss_ind	,
            	cpy.	related_comm_bank_acct_number	,
            	cpy.	site_code	,
            	cpy.	site_code_card_div	,
            	cpy.	skip_payment_ind	,
            	cpy.	status_date	,
            	cpy.	sub_product_code	,
            	cpy.	system_active_ind	,
            	cpy.	system_credit_line	,
            	cpy.	total_budget_balance_amt	,
            	cpy.	total_credit_score	,
            	cpy.	transfer_account_number	,
            	cpy.	transfer_acct_reason_code	,
            	cpy.	transfer_to_legal_orig_date	,
            	cpy.	triad_sub_product_code	,
              
              cpy. statement_payment_amt ,
              cpy. next_payment_due_date
--              cpy. fica_blocked_date ,
--              cpy. fica_blocked_by

      from    stg_absa_crd_acc_wly_cpy cpy
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
    from   stg_absa_crd_acc_wly_cpy
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
--    update stg_absa_crd_acc_wly_cpy
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
end wh_fnd_wfs_304u;
