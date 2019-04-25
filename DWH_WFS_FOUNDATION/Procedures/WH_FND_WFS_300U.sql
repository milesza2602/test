--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_300U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_300U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_crd_acc_dly fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_crd_acc_dly_cpy
--               Output - fnd_wfs_crd_acc_dly
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


g_information_date                 stg_absa_crd_acc_dly_cpy.information_date%type;  
g_account_number                   stg_absa_crd_acc_dly_cpy.account_number%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_300U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS CARD ACC DLY EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_absa_crd_acc_dly_cpy
where (information_date,
account_number)
in
(select information_date,
account_number
from stg_absa_crd_acc_dly_cpy 
group by information_date,
account_number
having count(*) > 1) 
order by information_date,
account_number,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_absa_crd_acc_dly is
select /*+ FULL(stg) parallel (stg,4)  full(fnd) parallel (fnd,4)  */
              stg.*
      from    stg_absa_crd_acc_dly_cpy stg,
              fnd_wfs_crd_acc_dly fnd
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
        update stg_absa_crd_acc_dly_cpy stg
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
      
      insert /*+ APPEND parallel (fnd,4) */ into fnd_wfs_crd_acc_dly fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,4) */
           	cpy.	information_date	,
           	cpy.	account_number	,
           	cpy.	card_account_status_code	,
           	cpy.	application_credit_score	,
           	cpy.	behaviour_score	,
           	cpy.	budget_limit_amount	,
           	cpy.	card_account_type_code	,
           	cpy.	card_closed_reason_code	,
           	cpy.	card_cycle_code	,
           	cpy.	card_production_brand	,
           	cpy.	closed_date	,
           	cpy.	card_collector_code	,
           	cpy.	credit_bureau_score	,
           	cpy.	customer_key	,
           	cpy.	day_5_delinquent_amt	,
           	cpy.	day_30_delinquent_amt	,
           	cpy.	day_60_delinquent_amt	,
           	cpy.	day_90_delinquent_amt	,
           	cpy.	day_120_delinquent_amt	,
           	cpy.	day_150_delinquent_amt	,
           	cpy.	day_180_delinquent_amt	,
           	cpy.	day_210_delinquent_amt	,
           	cpy.	delinquent_amount	,
           	cpy.	delinquent_cycles_count	,
           	cpy.	delinquent_date	,
           	cpy.	delinquent_ind	,
           	cpy.	manual_credit_line	,
           	cpy.	maturity_date	,
           	cpy.	ocs_processing_ind	,
           	cpy.	ocs_reason_code	,
           	cpy.	open_date	,
           	cpy.	prev_card_account_status_code	,
           	cpy.	purchase_limit_amt	,
           	cpy.	site_code_card_div	,
           	cpy.	status_date	,
           	cpy.	system_credit_line	,
           	cpy.	total_budget_balance_amt	,
           	cpy.	transfer_account_number	,
           	cpy.	random_number_digit	,
           	cpy.	triad_sub_product_code	,
           	cpy.	last_client_dt_txn_date	,
           	cpy.	last_client_dt_txn_amt	,
           	cpy.	last_client_dt_txt_category	,
           	cpy.	last_transaction_code	,
           	cpy.	last_monetary_transaction_date	,
           	cpy.	last_monetary_transaction_amt	,
           	cpy.	transfer_to_legal_orig_date	,
           	cpy.	last_client_ct_pmt_date	,
           	cpy.	last_client_ct_pmt_txn_code	,
           	cpy.	last_client_ct_pmt_amt	,
           	cpy.	card_acct_status_code	,
           	cpy.	account_balance	,
           	cpy.	derived_card_acct_status_code	,
           	cpy.	sub_product_code	,
            g_date as last_updated_date
      from   stg_absa_crd_acc_dly_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_crd_acc_dly 
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



for upd_rec in c_stg_absa_crd_acc_dly
   loop
     update fnd_wfs_crd_acc_dly fnd 
     set    fnd.	card_account_status_code      	=	upd_rec.	card_account_status_code	,
            fnd.	application_credit_score      	=	upd_rec.	application_credit_score	,
            fnd.	behaviour_score	                =	upd_rec.	behaviour_score	,
            fnd.	budget_limit_amount	            =	upd_rec.	budget_limit_amount	,
            fnd.	card_account_type_code	        =	upd_rec.	card_account_type_code	,
            fnd.	card_closed_reason_code	        =	upd_rec.	card_closed_reason_code	,
            fnd.	card_cycle_code               	=	upd_rec.	card_cycle_code	,
            fnd.	card_production_brand	          =	upd_rec.	card_production_brand	,
            fnd.	closed_date	                    =	upd_rec.	closed_date	,
            fnd.	card_collector_code           	=	upd_rec.	card_collector_code	,
            fnd.	credit_bureau_score	            =	upd_rec.	credit_bureau_score	,
            fnd.	customer_key	                  =	upd_rec.	customer_key	,
            fnd.	day_5_delinquent_amt	          =	upd_rec.	day_5_delinquent_amt	,
            fnd.	day_30_delinquent_amt	          =	upd_rec.	day_30_delinquent_amt	,
            fnd.	day_60_delinquent_amt	          =	upd_rec.	day_60_delinquent_amt	,
            fnd.	day_90_delinquent_amt	          =	upd_rec.	day_90_delinquent_amt	,
            fnd.	day_120_delinquent_amt        	=	upd_rec.	day_120_delinquent_amt	,
            fnd.	day_150_delinquent_amt	        =	upd_rec.	day_150_delinquent_amt	,
            fnd.	day_180_delinquent_amt	        =	upd_rec.	day_180_delinquent_amt	,
            fnd.	day_210_delinquent_amt	        =	upd_rec.	day_210_delinquent_amt	,
            fnd.	delinquent_amount             	=	upd_rec.	delinquent_amount	,
            fnd.	delinquent_cycles_count	        =	upd_rec.	delinquent_cycles_count	,
            fnd.	delinquent_date               	=	upd_rec.	delinquent_date	,
            fnd.	delinquent_ind	                =	upd_rec.	delinquent_ind	,
            fnd.	manual_credit_line            	=	upd_rec.	manual_credit_line	,
            fnd.	maturity_date                 	=	upd_rec.	maturity_date	,
            fnd.	ocs_processing_ind	            =	upd_rec.	ocs_processing_ind	,
            fnd.	ocs_reason_code	                =	upd_rec.	ocs_reason_code	,
            fnd.	open_date                     	=	upd_rec.	open_date	,
            fnd.	prev_card_account_status_code	  =	upd_rec.	prev_card_account_status_code	,
            fnd.	purchase_limit_amt	            =	upd_rec.	purchase_limit_amt	,
            fnd.	site_code_card_div	            =	upd_rec.	site_code_card_div	,
            fnd.	status_date                   	=	upd_rec.	status_date	,
            fnd.	system_credit_line	            =	upd_rec.	system_credit_line	,
            fnd.	total_budget_balance_amt      	=	upd_rec.	total_budget_balance_amt	,
            fnd.	transfer_account_number       	=	upd_rec.	transfer_account_number	,
            fnd.	random_number_digit	            =	upd_rec.	random_number_digit	,
            fnd.	triad_sub_product_code	        =	upd_rec.	triad_sub_product_code	,
            fnd.	last_client_dt_txn_date	        =	upd_rec.	last_client_dt_txn_date	,
            fnd.	last_client_dt_txn_amt	        =	upd_rec.	last_client_dt_txn_amt	,
            fnd.	last_client_dt_txt_category	    =	upd_rec.	last_client_dt_txt_category	,
            fnd.	last_transaction_code	          =	upd_rec.	last_transaction_code	,
            fnd.	last_monetary_transaction_date	=	upd_rec.	last_monetary_transaction_date	,
            fnd.	last_monetary_transaction_amt	  =	upd_rec.	last_monetary_transaction_amt	,
            fnd.	transfer_to_legal_orig_date	    =	upd_rec.	transfer_to_legal_orig_date	,
            fnd.	last_client_ct_pmt_date	        =	upd_rec.	last_client_ct_pmt_date	,
            fnd.	last_client_ct_pmt_txn_code	    =	upd_rec.	last_client_ct_pmt_txn_code	,
            fnd.	last_client_ct_pmt_amt	        =	upd_rec.	last_client_ct_pmt_amt	,
            fnd.	card_acct_status_code	          =	upd_rec.	card_acct_status_code	,
            fnd.	account_balance	                =	upd_rec.	account_balance	,
            fnd.	derived_card_acct_status_code 	=	upd_rec.	derived_card_acct_status_code	,
            fnd.	sub_product_code              	=	upd_rec.	sub_product_code	,

            fnd.  last_updated_date               = g_date
     where  fnd.	information_date                =	upd_rec.	information_date and
            fnd.	account_number                  =	upd_rec.	account_number   and
            ( 
            nvl(fnd.card_account_status_code      	,0) <>	upd_rec.	card_account_status_code	or
            nvl(fnd.application_credit_score	      ,0) <>	upd_rec.	application_credit_score	or
            nvl(fnd.behaviour_score	                ,0) <>	upd_rec.	behaviour_score	or
            nvl(fnd.budget_limit_amount           	,0) <>	upd_rec.	budget_limit_amount	or
            nvl(fnd.card_account_type_code	        ,0) <>	upd_rec.	card_account_type_code	or
            nvl(fnd.card_closed_reason_code	        ,0) <>	upd_rec.	card_closed_reason_code	or
            nvl(fnd.card_cycle_code               	,0) <>	upd_rec.	card_cycle_code	or
            nvl(fnd.card_production_brand         	,0) <>	upd_rec.	card_production_brand	or
            nvl(fnd.closed_date	                    ,'1 Jan 1900') <>	upd_rec.	closed_date	or
            nvl(fnd.card_collector_code	            ,0) <>	upd_rec.	card_collector_code	or
            nvl(fnd.credit_bureau_score	            ,0) <>	upd_rec.	credit_bureau_score	or
            nvl(fnd.customer_key                  	,0) <>	upd_rec.	customer_key	or
            nvl(fnd.day_5_delinquent_amt          	,0) <>	upd_rec.	day_5_delinquent_amt	or
            nvl(fnd.day_30_delinquent_amt         	,0) <>	upd_rec.	day_30_delinquent_amt	or
            nvl(fnd.day_60_delinquent_amt	          ,0) <>	upd_rec.	day_60_delinquent_amt	or
            nvl(fnd.day_90_delinquent_amt         	,0) <>	upd_rec.	day_90_delinquent_amt	or
            nvl(fnd.day_120_delinquent_amt	        ,0) <>	upd_rec.	day_120_delinquent_amt	or
            nvl(fnd.day_150_delinquent_amt	        ,0) <>	upd_rec.	day_150_delinquent_amt	or
            nvl(fnd.day_180_delinquent_amt	        ,0) <>	upd_rec.	day_180_delinquent_amt	or
            nvl(fnd.day_210_delinquent_amt        	,0) <>	upd_rec.	day_210_delinquent_amt	or
            nvl(fnd.delinquent_amount             	,0) <>	upd_rec.	delinquent_amount	or
            nvl(fnd.delinquent_cycles_count	        ,0) <>	upd_rec.	delinquent_cycles_count	or
            nvl(fnd.delinquent_date	                ,'1 Jan 1900') <>	upd_rec.	delinquent_date	or
            nvl(fnd.delinquent_ind	                ,0) <>	upd_rec.	delinquent_ind	or
            nvl(fnd.manual_credit_line            	,0) <>	upd_rec.	manual_credit_line	or
            nvl(fnd.maturity_date	                  ,'1 Jan 1900') <>	upd_rec.	maturity_date	or
            nvl(fnd.ocs_processing_ind	            ,0) <>	upd_rec.	ocs_processing_ind	or
            nvl(fnd.ocs_reason_code	                ,0) <>	upd_rec.	ocs_reason_code	or
            nvl(fnd.open_date	                      ,'1 Jan 1900') <>	upd_rec.	open_date	or
            nvl(fnd.prev_card_account_status_code   ,0) <>	upd_rec.	prev_card_account_status_code	or
            nvl(fnd.purchase_limit_amt	            ,0) <>	upd_rec.	purchase_limit_amt	or
            nvl(fnd.site_code_card_div	            ,0) <>	upd_rec.	site_code_card_div	or
            nvl(fnd.status_date                   	,'1 Jan 1900') <>	upd_rec.	status_date	or
            nvl(fnd.system_credit_line	            ,0) <>	upd_rec.	system_credit_line	or
            nvl(fnd.total_budget_balance_amt	      ,0) <>	upd_rec.	total_budget_balance_amt	or
            nvl(fnd.transfer_account_number	        ,0) <>	upd_rec.	transfer_account_number	or
            nvl(fnd.random_number_digit	            ,0) <>	upd_rec.	random_number_digit	or
            nvl(fnd.triad_sub_product_code	        ,0) <>	upd_rec.	triad_sub_product_code	or
            nvl(fnd.last_client_dt_txn_date	        ,'1 Jan 1900') <>	upd_rec.	last_client_dt_txn_date	or
            nvl(fnd.last_client_dt_txn_amt	        ,0) <>	upd_rec.	last_client_dt_txn_amt	or
            nvl(fnd.last_client_dt_txt_category	    ,0) <>	upd_rec.	last_client_dt_txt_category	or
            nvl(fnd.last_transaction_code	          ,0) <>	upd_rec.	last_transaction_code	or
            nvl(fnd.last_monetary_transaction_date	,'1 Jan 1900') <>	upd_rec.	last_monetary_transaction_date	or
            nvl(fnd.last_monetary_transaction_amt 	,0) <>	upd_rec.	last_monetary_transaction_amt	or
            nvl(fnd.transfer_to_legal_orig_date	    ,'1 Jan 1900') <>	upd_rec.	transfer_to_legal_orig_date	or
            nvl(fnd.last_client_ct_pmt_date	        ,'1 Jan 1900') <>	upd_rec.	last_client_ct_pmt_date	or
            nvl(fnd.last_client_ct_pmt_txn_code   	,0) <>	upd_rec.	last_client_ct_pmt_txn_code	or
            nvl(fnd.last_client_ct_pmt_amt	        ,0) <>	upd_rec.	last_client_ct_pmt_amt	or
            nvl(fnd.card_acct_status_code	          ,0) <>	upd_rec.	card_acct_status_code	or
            nvl(fnd.account_balance	                ,0) <>	upd_rec.	account_balance	or
            nvl(fnd.derived_card_acct_status_code 	,0) <>	upd_rec.	derived_card_acct_status_code	or
            nvl(fnd.sub_product_code	              ,0) <>	upd_rec.	sub_product_code	 
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_crd_acc_dly_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
              cpy.sys_source_batch_id,
              cpy.sys_source_sequence_no,
              sysdate,'Y','DWH',
              cpy.sys_middleware_batch_id,
              'VALIDATION FAIL - REFERENCIAL ERROR',
            	cpy.	information_date	,
            	cpy.	account_number	,
            	cpy.	card_account_status_code	,
            	cpy.	application_credit_score	,
            	cpy.	behaviour_score	,
            	cpy.	budget_limit_amount	,
            	cpy.	card_account_type_code	,
            	cpy.	card_closed_reason_code	,
            	cpy.	card_cycle_code	,
            	cpy.	card_production_brand	,
            	cpy.	closed_date	,
            	cpy.	card_collector_code	,
            	cpy.	credit_bureau_score	,
            	cpy.	customer_key	,
            	cpy.	day_5_delinquent_amt	,
            	cpy.	day_30_delinquent_amt	,
            	cpy.	day_60_delinquent_amt	,
            	cpy.	day_90_delinquent_amt	,
            	cpy.	day_120_delinquent_amt	,
            	cpy.	day_150_delinquent_amt	,
            	cpy.	day_180_delinquent_amt	,
            	cpy.	day_210_delinquent_amt	,
            	cpy.	delinquent_amount	,
            	cpy.	delinquent_cycles_count	,
            	cpy.	delinquent_date	,
            	cpy.	delinquent_ind	,
            	cpy.	manual_credit_line	,
            	cpy.	maturity_date	,
            	cpy.	ocs_processing_ind	,
            	cpy.	ocs_reason_code	,
            	cpy.	open_date	,
            	cpy.	prev_card_account_status_code	,
            	cpy.	purchase_limit_amt	,
            	cpy.	site_code_card_div	,
            	cpy.	status_date	,
            	cpy.	system_credit_line	,
            	cpy.	total_budget_balance_amt	,
            	cpy.	transfer_account_number	,
            	cpy.	random_number_digit	,
            	cpy.	triad_sub_product_code	,
            	cpy.	last_client_dt_txn_date	,
            	cpy.	last_client_dt_txn_amt	,
            	cpy.	last_client_dt_txt_category	,
            	cpy.	last_transaction_code	,
            	cpy.	last_monetary_transaction_date	,
            	cpy.	last_monetary_transaction_amt	,
            	cpy.	transfer_to_legal_orig_date	,
            	cpy.	last_client_ct_pmt_date	,
            	cpy.	last_client_ct_pmt_txn_code	,
            	cpy.	last_client_ct_pmt_amt	,
            	cpy.	card_acct_status_code	,
            	cpy.	account_balance	,
            	cpy.	derived_card_acct_status_code	,
            	cpy.	sub_product_code	 
      from   stg_absa_crd_acc_dly_cpy cpy
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
    from   stg_absa_crd_acc_dly_cpy
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
--    update stg_absa_crd_acc_dly_cpy
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
end wh_fnd_wfs_300u;
