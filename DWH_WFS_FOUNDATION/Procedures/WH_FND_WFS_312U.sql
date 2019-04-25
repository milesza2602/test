--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_312U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_312U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_crd_st_mly fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_crd_st_mly_cpy
--               Output - fnd_wfs_crd_st_mly
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


g_information_date                 stg_absa_crd_st_mly_cpy.information_date%type;  
g_account_number                   stg_absa_crd_st_mly_cpy.account_number%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_312U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS CARD ST MLY EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_absa_crd_st_mly_cpy
where (information_date,
account_number)
in
(select information_date,
account_number
from stg_absa_crd_st_mly_cpy 
group by information_date,
account_number
having count(*) > 1) 
order by information_date,
account_number,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_absa_crd_st_mly is
select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    stg_absa_crd_st_mly_cpy stg,
              fnd_wfs_crd_st_mly fnd
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
        update stg_absa_crd_st_mly_cpy stg
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_crd_st_mly fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.	information_date	,
             cpy.	account_number	,
             cpy.	customer_key	,
             cpy.	account_cash_balance_amt	,
             cpy.	sub_product_code	,
             cpy.	apr_card_payment_type_code	,
             cpy.	apr_cycle_cr_limit_used_ind	,
             cpy.	atm_cash_withdrawal_txn_amt	,
             cpy.	aug_card_payment_type_code	,
             cpy.	aug_cycle_cr_limit_used_ind	,
             cpy.	authorised_skip_payment_ind	,
             cpy.	card_cycle_code	,
             cpy.	card_cycle_end_date	,
             cpy.	credit_limit_available_amt	,
             cpy.	cycle_delinquent_amt	,
             cpy.	cycle_to_date_payment_amt	,
             cpy.	date_acct_bal_exceed_purch_lim	,
             cpy.	days_cycle_length	,
             cpy.	dec_card_payment_type_code	,
             cpy.	dec_cycle_cr_limit_used_ind	,
             cpy.	eligible_skip_paymnt_plan_ind	,
             cpy.	feb_card_payment_type_code	,
             cpy.	feb_cycle_cr_limit_used_ind	,
             cpy.	jan_card_payment_type_code	,
             cpy.	jan_cycle_cr_limit_used_ind	,
             cpy.	jul_card_payment_type_code	,
             cpy.	jul_cycle_cr_limit_used_ind	,
             cpy.	jun_card_payment_type_code	,
             cpy.	jun_cycle_cr_limit_used_ind	,
             cpy.	last_payment_date	,
             cpy.	mar_card_payment_type_code	,
             cpy.	mar_cycle_cr_limit_used_ind	,
             cpy.	may_card_payment_type_code	,
             cpy.	may_cycle_cr_limit_used_ind	,
             cpy.	next_payment_due_date	,
             cpy.	nov_card_payment_type_code	,
             cpy.	nov_cycle_cr_limit_used_ind	,
             cpy.	oct_card_payment_type_code	,
             cpy.	oct_cycle_cr_limit_used_ind	,
             cpy.	payments_recvd_since_prev_stmt	,
             cpy.	prev_cycle_begin_date	,
             cpy.	prev_statemnt_acct_bal	,
             cpy.	prev_statemnt_total_bal	,
             cpy.	sep_card_payment_type_code	,
             cpy.	sep_cycle_cr_limit_used_ind	,
             cpy.	statemnt_card_acct_status_code	,
             cpy.	statemnt_min_payment_due_date	,
             cpy.	statemnt_petrol_bal_amt	,
             cpy.	date_last_updated	,
             cpy.	atm_cash_fee_txn_amt	,
             cpy.	cash_withdrawal_amt	,
             cpy.	credit_interest_income_amt	,
             cpy.	credit_vouchers_amt	,
             cpy.	fin_charges_on_cash_bal_amt	,
             cpy.	fin_charges_on_purchase_bal	,
             cpy.	manual_cash_fee_txn_amt	,
             cpy.	manual_cash_withdrawal_txn_amt	,
             cpy.	manual_purchase_txn_amt	,
             cpy.	over_limit_amt	,
             cpy.	pos_purchase_txn_amt	,
             cpy.	statement_payment_amt	,
             g_date as last_updated_date
      from   stg_absa_crd_st_mly_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_crd_st_mly 
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



for upd_rec in c_stg_absa_crd_st_mly
   loop
     update fnd_wfs_crd_st_mly fnd 
     set    fnd.	customer_key	                  =	upd_rec.	customer_key	,
            fnd.	account_cash_balance_amt	      =	upd_rec.	account_cash_balance_amt	,
            fnd.	sub_product_code	              =	upd_rec.	sub_product_code	,
            fnd.	apr_card_payment_type_code	    =	upd_rec.	apr_card_payment_type_code	,
            fnd.	apr_cycle_cr_limit_used_ind   	=	upd_rec.	apr_cycle_cr_limit_used_ind	,
            fnd.	atm_cash_withdrawal_txn_amt   	=	upd_rec.	atm_cash_withdrawal_txn_amt	,
            fnd.	aug_card_payment_type_code	    =	upd_rec.	aug_card_payment_type_code	,
            fnd.	aug_cycle_cr_limit_used_ind	    =	upd_rec.	aug_cycle_cr_limit_used_ind	,
            fnd.	authorised_skip_payment_ind	    =	upd_rec.	authorised_skip_payment_ind	,
            fnd.	card_cycle_code	                =	upd_rec.	card_cycle_code	,
            fnd.	card_cycle_end_date	            =	upd_rec.	card_cycle_end_date	,
            fnd.	credit_limit_available_amt	    =	upd_rec.	credit_limit_available_amt	,
            fnd.	cycle_delinquent_amt	          =	upd_rec.	cycle_delinquent_amt	,
            fnd.	cycle_to_date_payment_amt	      =	upd_rec.	cycle_to_date_payment_amt	,
            fnd.	date_acct_bal_exceed_purch_lim	=	upd_rec.	date_acct_bal_exceed_purch_lim	,
            fnd.	days_cycle_length	              =	upd_rec.	days_cycle_length	,
            fnd.	dec_card_payment_type_code	    =	upd_rec.	dec_card_payment_type_code	,
            fnd.	dec_cycle_cr_limit_used_ind	    =	upd_rec.	dec_cycle_cr_limit_used_ind	,
            fnd.	eligible_skip_paymnt_plan_ind	  =	upd_rec.	eligible_skip_paymnt_plan_ind	,
            fnd.	feb_card_payment_type_code	    =	upd_rec.	feb_card_payment_type_code	,
            fnd.	feb_cycle_cr_limit_used_ind	    =	upd_rec.	feb_cycle_cr_limit_used_ind	,
            fnd.	jan_card_payment_type_code	    =	upd_rec.	jan_card_payment_type_code	,
            fnd.	jan_cycle_cr_limit_used_ind	    =	upd_rec.	jan_cycle_cr_limit_used_ind	,
            fnd.	jul_card_payment_type_code	    =	upd_rec.	jul_card_payment_type_code	,
            fnd.	jul_cycle_cr_limit_used_ind	    =	upd_rec.	jul_cycle_cr_limit_used_ind	,
            fnd.	jun_card_payment_type_code	    =	upd_rec.	jun_card_payment_type_code	,
            fnd.	jun_cycle_cr_limit_used_ind	    =	upd_rec.	jun_cycle_cr_limit_used_ind	,
            fnd.	last_payment_date             	=	upd_rec.	last_payment_date	,
            fnd.	mar_card_payment_type_code	    =	upd_rec.	mar_card_payment_type_code	,
            fnd.	mar_cycle_cr_limit_used_ind	    =	upd_rec.	mar_cycle_cr_limit_used_ind	,
            fnd.	may_card_payment_type_code	    =	upd_rec.	may_card_payment_type_code	,
            fnd.	may_cycle_cr_limit_used_ind	    =	upd_rec.	may_cycle_cr_limit_used_ind	,
            fnd.	next_payment_due_date	          =	upd_rec.	next_payment_due_date	,
            fnd.	nov_card_payment_type_code	    =	upd_rec.	nov_card_payment_type_code	,
            fnd.	nov_cycle_cr_limit_used_ind	    =	upd_rec.	nov_cycle_cr_limit_used_ind	,
            fnd.	oct_card_payment_type_code	    =	upd_rec.	oct_card_payment_type_code	,
            fnd.	oct_cycle_cr_limit_used_ind	    =	upd_rec.	oct_cycle_cr_limit_used_ind	,
            fnd.	payments_recvd_since_prev_stmt	=	upd_rec.	payments_recvd_since_prev_stmt	,
            fnd.	prev_cycle_begin_date	          =	upd_rec.	prev_cycle_begin_date	,
            fnd.	prev_statemnt_acct_bal	        =	upd_rec.	prev_statemnt_acct_bal	,
            fnd.	prev_statemnt_total_bal	        =	upd_rec.	prev_statemnt_total_bal	,
            fnd.	sep_card_payment_type_code	    =	upd_rec.	sep_card_payment_type_code	,
            fnd.	sep_cycle_cr_limit_used_ind	    =	upd_rec.	sep_cycle_cr_limit_used_ind	,
            fnd.	statemnt_card_acct_status_code	=	upd_rec.	statemnt_card_acct_status_code	,
            fnd.	statemnt_min_payment_due_date 	=	upd_rec.	statemnt_min_payment_due_date	,
            fnd.	statemnt_petrol_bal_amt	        =	upd_rec.	statemnt_petrol_bal_amt	,
            fnd.	date_last_updated	              =	upd_rec.	date_last_updated	,
            fnd.	atm_cash_fee_txn_amt	          =	upd_rec.	atm_cash_fee_txn_amt	,
            fnd.	cash_withdrawal_amt	            =	upd_rec.	cash_withdrawal_amt	,
            fnd.	credit_interest_income_amt	    =	upd_rec.	credit_interest_income_amt	,
            fnd.	credit_vouchers_amt	            =	upd_rec.	credit_vouchers_amt	,
            fnd.	fin_charges_on_cash_bal_amt	    =	upd_rec.	fin_charges_on_cash_bal_amt	,
            fnd.	fin_charges_on_purchase_bal	    =	upd_rec.	fin_charges_on_purchase_bal	,
            fnd.	manual_cash_fee_txn_amt       	=	upd_rec.	manual_cash_fee_txn_amt	,
            fnd.	manual_cash_withdrawal_txn_amt	=	upd_rec.	manual_cash_withdrawal_txn_amt	,
            fnd.	manual_purchase_txn_amt	        =	upd_rec.	manual_purchase_txn_amt	,
            fnd.	over_limit_amt	                =	upd_rec.	over_limit_amt	,
            fnd.	pos_purchase_txn_amt	          =	upd_rec.	pos_purchase_txn_amt	,
            fnd.	statement_payment_amt	          =	upd_rec.	statement_payment_amt	,
            fnd.  last_updated_date               = g_date
     where  fnd.	information_date                =	upd_rec.	information_date and
            fnd.	account_number                  =	upd_rec.	account_number   and
            ( 
            nvl(fnd.information_date	              ,'1 Jan 1900') <>	upd_rec.	information_date	or
            nvl(fnd.account_number	                ,0) <>	upd_rec.	account_number	or
            nvl(fnd.customer_key	                  ,0) <>	upd_rec.	customer_key	or
            nvl(fnd.account_cash_balance_amt	      ,0) <>	upd_rec.	account_cash_balance_amt	or
            nvl(fnd.sub_product_code	              ,0) <>	upd_rec.	sub_product_code	or
            nvl(fnd.apr_card_payment_type_code	    ,0) <>	upd_rec.	apr_card_payment_type_code	or
            nvl(fnd.apr_cycle_cr_limit_used_ind	    ,0) <>	upd_rec.	apr_cycle_cr_limit_used_ind	or
            nvl(fnd.atm_cash_withdrawal_txn_amt	    ,0) <>	upd_rec.	atm_cash_withdrawal_txn_amt	or
            nvl(fnd.aug_card_payment_type_code	    ,0) <>	upd_rec.	aug_card_payment_type_code	or
            nvl(fnd.aug_cycle_cr_limit_used_ind	    ,0) <>	upd_rec.	aug_cycle_cr_limit_used_ind	or
            nvl(fnd.authorised_skip_payment_ind   	,0) <>	upd_rec.	authorised_skip_payment_ind	or
            nvl(fnd.card_cycle_code	                ,0) <>	upd_rec.	card_cycle_code	or
            nvl(fnd.card_cycle_end_date	            ,'1 Jan 1900') <>	upd_rec.	card_cycle_end_date	or
            nvl(fnd.credit_limit_available_amt	    ,0) <>	upd_rec.	credit_limit_available_amt	or
            nvl(fnd.cycle_delinquent_amt	          ,0) <>	upd_rec.	cycle_delinquent_amt	or
            nvl(fnd.cycle_to_date_payment_amt     	,0) <>	upd_rec.	cycle_to_date_payment_amt	or
            nvl(fnd.date_acct_bal_exceed_purch_lim	,'1 Jan 1900') <>	upd_rec.	date_acct_bal_exceed_purch_lim	or
            nvl(fnd.days_cycle_length	              ,0) <>	upd_rec.	days_cycle_length	or
            nvl(fnd.dec_card_payment_type_code	    ,0) <>	upd_rec.	dec_card_payment_type_code	or
            nvl(fnd.dec_cycle_cr_limit_used_ind   	,0) <>	upd_rec.	dec_cycle_cr_limit_used_ind	or
            nvl(fnd.eligible_skip_paymnt_plan_ind	  ,0) <>	upd_rec.	eligible_skip_paymnt_plan_ind	or
            nvl(fnd.feb_card_payment_type_code	    ,0) <>	upd_rec.	feb_card_payment_type_code	or
            nvl(fnd.feb_cycle_cr_limit_used_ind	    ,0) <>	upd_rec.	feb_cycle_cr_limit_used_ind	or
            nvl(fnd.jan_card_payment_type_code	    ,0) <>	upd_rec.	jan_card_payment_type_code	or
            nvl(fnd.jan_cycle_cr_limit_used_ind	    ,0) <>	upd_rec.	jan_cycle_cr_limit_used_ind	or
            nvl(fnd.jul_card_payment_type_code	    ,0) <>	upd_rec.	jul_card_payment_type_code	or
            nvl(fnd.jul_cycle_cr_limit_used_ind	    ,0) <>	upd_rec.	jul_cycle_cr_limit_used_ind	or
            nvl(fnd.jun_card_payment_type_code	    ,0) <>	upd_rec.	jun_card_payment_type_code	or
            nvl(fnd.jun_cycle_cr_limit_used_ind   	,0) <>	upd_rec.	jun_cycle_cr_limit_used_ind	or
            nvl(fnd.last_payment_date	              ,'1 Jan 1900') <>	upd_rec.	last_payment_date	or
            nvl(fnd.mar_card_payment_type_code	    ,0) <>	upd_rec.	mar_card_payment_type_code	or
            nvl(fnd.mar_cycle_cr_limit_used_ind	    ,0) <>	upd_rec.	mar_cycle_cr_limit_used_ind	or
            nvl(fnd.may_card_payment_type_code	    ,0) <>	upd_rec.	may_card_payment_type_code	or
            nvl(fnd.may_cycle_cr_limit_used_ind	    ,0) <>	upd_rec.	may_cycle_cr_limit_used_ind	or
            nvl(fnd.next_payment_due_date	          ,'1 Jan 1900') <>	upd_rec.	next_payment_due_date	or
            nvl(fnd.nov_card_payment_type_code	    ,0) <>	upd_rec.	nov_card_payment_type_code	or
            nvl(fnd.nov_cycle_cr_limit_used_ind	    ,0) <>	upd_rec.	nov_cycle_cr_limit_used_ind	or
            nvl(fnd.oct_card_payment_type_code	    ,0) <>	upd_rec.	oct_card_payment_type_code	or
            nvl(fnd.oct_cycle_cr_limit_used_ind	    ,0) <>	upd_rec.	oct_cycle_cr_limit_used_ind	or
            nvl(fnd.payments_recvd_since_prev_stmt	,0) <>	upd_rec.	payments_recvd_since_prev_stmt	or
            nvl(fnd.prev_cycle_begin_date	          ,'1 Jan 1900') <>	upd_rec.	prev_cycle_begin_date	or
            nvl(fnd.prev_statemnt_acct_bal	        ,0) <>	upd_rec.	prev_statemnt_acct_bal	or
            nvl(fnd.prev_statemnt_total_bal       	,0) <>	upd_rec.	prev_statemnt_total_bal	or
            nvl(fnd.sep_card_payment_type_code	    ,0) <>	upd_rec.	sep_card_payment_type_code	or
            nvl(fnd.sep_cycle_cr_limit_used_ind     ,0) <>	upd_rec.	sep_cycle_cr_limit_used_ind	or
            nvl(fnd.statemnt_card_acct_status_code	,0) <>	upd_rec.	statemnt_card_acct_status_code	or
            nvl(fnd.statemnt_min_payment_due_date	  ,'1 Jan 1900') <>	upd_rec.	statemnt_min_payment_due_date	or
            nvl(fnd.statemnt_petrol_bal_amt	        ,0) <>	upd_rec.	statemnt_petrol_bal_amt	or
            nvl(fnd.date_last_updated	              ,'1 Jan 1900') <>	upd_rec.	date_last_updated	or
            nvl(fnd.atm_cash_fee_txn_amt	          ,0) <>	upd_rec.	atm_cash_fee_txn_amt	or
            nvl(fnd.cash_withdrawal_amt	            ,0) <>	upd_rec.	cash_withdrawal_amt	or
            nvl(fnd.credit_interest_income_amt	    ,0) <>	upd_rec.	credit_interest_income_amt	or
            nvl(fnd.credit_vouchers_amt	            ,0) <>	upd_rec.	credit_vouchers_amt	or
            nvl(fnd.fin_charges_on_cash_bal_amt	    ,0) <>	upd_rec.	fin_charges_on_cash_bal_amt	or
            nvl(fnd.fin_charges_on_purchase_bal	    ,0) <>	upd_rec.	fin_charges_on_purchase_bal	or
            nvl(fnd.manual_cash_fee_txn_amt	        ,0) <>	upd_rec.	manual_cash_fee_txn_amt	or
            nvl(fnd.manual_cash_withdrawal_txn_amt	,0) <>	upd_rec.	manual_cash_withdrawal_txn_amt	or
            nvl(fnd.manual_purchase_txn_amt	        ,0) <>	upd_rec.	manual_purchase_txn_amt	or
            nvl(fnd.over_limit_amt	                ,0) <>	upd_rec.	over_limit_amt	or
            nvl(fnd.pos_purchase_txn_amt	          ,0) <>	upd_rec.	pos_purchase_txn_amt	or
            nvl(fnd.statement_payment_amt	          ,0) <>	upd_rec.	statement_payment_amt	
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_crd_st_mly_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
             cpy.	information_date	,
             cpy.	account_number	,
             cpy.	customer_key	,
             cpy.	account_cash_balance_amt	,
             cpy.	sub_product_code	,
             cpy.	apr_card_payment_type_code	,
             cpy.	apr_cycle_cr_limit_used_ind	,
             cpy.	atm_cash_withdrawal_txn_amt	,
             cpy.	aug_card_payment_type_code	,
             cpy.	aug_cycle_cr_limit_used_ind	,
             cpy.	authorised_skip_payment_ind	,
             cpy.	card_cycle_code	,
             cpy.	card_cycle_end_date	,
             cpy.	credit_limit_available_amt	,
             cpy.	cycle_delinquent_amt	,
             cpy.	cycle_to_date_payment_amt	,
             cpy.	date_acct_bal_exceed_purch_lim	,
             cpy.	days_cycle_length	,
             cpy.	dec_card_payment_type_code	,
             cpy.	dec_cycle_cr_limit_used_ind	,
             cpy.	eligible_skip_paymnt_plan_ind	,
             cpy.	feb_card_payment_type_code	,
             cpy.	feb_cycle_cr_limit_used_ind	,
             cpy.	jan_card_payment_type_code	,
             cpy.	jan_cycle_cr_limit_used_ind	,
             cpy.	jul_card_payment_type_code	,
             cpy.	jul_cycle_cr_limit_used_ind	,
             cpy.	jun_card_payment_type_code	,
             cpy.	jun_cycle_cr_limit_used_ind	,
             cpy.	last_payment_date	,
             cpy.	mar_card_payment_type_code	,
             cpy.	mar_cycle_cr_limit_used_ind	,
             cpy.	may_card_payment_type_code	,
             cpy.	may_cycle_cr_limit_used_ind	,
             cpy.	next_payment_due_date	,
             cpy.	nov_card_payment_type_code	,
             cpy.	nov_cycle_cr_limit_used_ind	,
             cpy.	oct_card_payment_type_code	,
             cpy.	oct_cycle_cr_limit_used_ind	,
             cpy.	payments_recvd_since_prev_stmt	,
             cpy.	prev_cycle_begin_date	,
             cpy.	prev_statemnt_acct_bal	,
             cpy.	prev_statemnt_total_bal	,
             cpy.	sep_card_payment_type_code	,
             cpy.	sep_cycle_cr_limit_used_ind	,
             cpy.	statemnt_card_acct_status_code	,
             cpy.	statemnt_min_payment_due_date	,
             cpy.	statemnt_petrol_bal_amt	,
             cpy.	date_last_updated	,
             cpy.	atm_cash_fee_txn_amt	,
             cpy.	cash_withdrawal_amt	,
             cpy.	credit_interest_income_amt	,
             cpy.	credit_vouchers_amt	,
             cpy.	fin_charges_on_cash_bal_amt	,
             cpy.	fin_charges_on_purchase_bal	,
             cpy.	manual_cash_fee_txn_amt	,
             cpy.	manual_cash_withdrawal_txn_amt	,
             cpy.	manual_purchase_txn_amt	,
             cpy.	over_limit_amt	,
             cpy.	pos_purchase_txn_amt	,
             cpy.	statement_payment_amt	
      from   stg_absa_crd_st_mly_cpy cpy
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
    from   stg_absa_crd_st_mly_cpy
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
--    update stg_absa_crd_st_mly_cpy
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
end wh_fnd_wfs_312u;
