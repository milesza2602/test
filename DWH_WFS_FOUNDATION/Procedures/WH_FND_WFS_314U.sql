--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_314U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_314U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_crd_tover_mly fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_crd_tover_mly_cpy
--               Output - fnd_wfs_crd_tover_mly
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


g_information_date                 stg_absa_crd_tover_mly_cpy.information_date%type;  
g_account_number                   stg_absa_crd_tover_mly_cpy.account_number%type; 
   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_314U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS CARD TURNOVER MLY EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_absa_crd_tover_mly_cpy
where (information_date,
account_number)
in
(select information_date,
account_number
from stg_absa_crd_tover_mly_cpy 
group by information_date,
account_number
having count(*) > 1) 
order by information_date,
account_number,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_absa_crd_tover_mly is
select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    stg_absa_crd_tover_mly_cpy stg,
              fnd_wfs_crd_tover_mly fnd
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
        update stg_absa_crd_tover_mly_cpy stg
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_crd_tover_mly fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.	INFORMATION_DATE	,
             cpy.	ACCOUNT_NUMBER	,
             cpy.	ACCRUED_CREDIT_INT_INC_AMT	,
             cpy.	ACCRUED_FIN_CHRG_BUDGET_AMT	,
             cpy.	ACCRUED_FIN_CHRG_CASH_AMT	,
             cpy.	ACCRUED_FIN_CHRG_PETROL_AMT	,
             cpy.	ACCRUED_FIN_CHRG_PURCH_AMT	,
             cpy.	AVG_BUDGET_BALANCE	,
             cpy.	AVG_CARD_LIFE_BALANCE	,
             cpy.	AVG_CASH_WITHDRAWAL_BALANCE	,
             cpy.	AVG_CREDIT_BALANCE	,
             cpy.	AVG_DEBIT_BALANCE	,
             cpy.	AVG_PETROL_BALANCE	,
             cpy.	AVG_PURCHASE_BALANCE	,
             cpy.	CARD_LIFE_BAL_DAYS_PER_MONTH	,
             cpy.	TOTAL_AVG_BALANCE	,
             cpy.	TOTAL_BAL_BUDGET	,
             cpy.	TOTAL_BAL_CASH	,
             cpy.	TOTAL_BAL_PETROL	,
             cpy.	TOTAL_BAL_PURCHASE_CURRENT	,
             cpy.	TOTAL_BAL_PURCHASE_OLD	,
             cpy.	TOTAL_BAL_PURCHASE_PRIOR	,
             cpy.	TOTAL_CARD_LIFE_BALANCE	,
             cpy.	TOTAL_CREDIT_BALANCE	,
             cpy.	TOTAL_DEBIT_BALANCE	,
             cpy.	TOTAL_FIN_CHRG_CASH_AMT	,
             cpy.	TOTAL_FIN_CHRG_PETROL_AMT	,
             cpy.	TOTAL_FIN_CHRG_PURCH_AMT	,
             g_date as last_updated_date
      from   stg_absa_crd_tover_mly_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_crd_tover_mly 
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



for upd_rec in c_stg_absa_crd_tover_mly
   loop
     update fnd_wfs_crd_tover_mly fnd 
     set    fnd.	accrued_credit_int_inc_amt	      =	upd_rec.	accrued_credit_int_inc_amt	,	
            fnd.	accrued_fin_chrg_budget_amt	      =	upd_rec.	accrued_fin_chrg_budget_amt	,	
            fnd.	accrued_fin_chrg_cash_amt	        =	upd_rec.	accrued_fin_chrg_cash_amt	,	
            fnd.	accrued_fin_chrg_petrol_amt	      =	upd_rec.	accrued_fin_chrg_petrol_amt	,	
            fnd.	accrued_fin_chrg_purch_amt	      =	upd_rec.	accrued_fin_chrg_purch_amt	,	
            fnd.	avg_budget_balance	              =	upd_rec.	avg_budget_balance	,	
            fnd.	avg_card_life_balance	            =	upd_rec.	avg_card_life_balance	,	
            fnd.	avg_cash_withdrawal_balance	      =	upd_rec.	avg_cash_withdrawal_balance	,	
            fnd.	avg_credit_balance	              =	upd_rec.	avg_credit_balance	,	
            fnd.	avg_debit_balance	                =	upd_rec.	avg_debit_balance	,	
            fnd.	avg_petrol_balance	              =	upd_rec.	avg_petrol_balance	,	
            fnd.	avg_purchase_balance	            =	upd_rec.	avg_purchase_balance	,	
            fnd.	card_life_bal_days_per_month	    =	upd_rec.	card_life_bal_days_per_month	,	
            fnd.	total_avg_balance	                =	upd_rec.	total_avg_balance	,	
            fnd.	total_bal_budget	                =	upd_rec.	total_bal_budget	,	
            fnd.	total_bal_cash	                  =	upd_rec.	total_bal_cash	,	
            fnd.	total_bal_petrol	                =	upd_rec.	total_bal_petrol	,	
            fnd.	total_bal_purchase_current	      =	upd_rec.	total_bal_purchase_current	,	
            fnd.	total_bal_purchase_old	          =	upd_rec.	total_bal_purchase_old	,	
            fnd.	total_bal_purchase_prior	        =	upd_rec.	total_bal_purchase_prior	,	
            fnd.	total_card_life_balance         	=	upd_rec.	total_card_life_balance	,	
            fnd.	total_credit_balance	            =	upd_rec.	total_credit_balance	,	
            fnd.	total_debit_balance	              =	upd_rec.	total_debit_balance	,	
            fnd.	total_fin_chrg_cash_amt	          =	upd_rec.	total_fin_chrg_cash_amt	,	
            fnd.	total_fin_chrg_petrol_amt	        =	upd_rec.	total_fin_chrg_petrol_amt	,	
            fnd.	total_fin_chrg_purch_amt	        =	upd_rec.	total_fin_chrg_purch_amt	,	
            fnd.  last_updated_date                 = g_date
     where  fnd.	information_date                  =	upd_rec.	information_date and
            fnd.	account_number                	  =	upd_rec.	account_number   and
            ( 
            nvl(fnd.accrued_credit_int_inc_amt	  ,0) <>	upd_rec.	accrued_credit_int_inc_amt	or	
            nvl(fnd.accrued_fin_chrg_budget_amt	  ,0) <>	upd_rec.	accrued_fin_chrg_budget_amt	or	
            nvl(fnd.accrued_fin_chrg_cash_amt	    ,0) <>	upd_rec.	accrued_fin_chrg_cash_amt	or	
            nvl(fnd.accrued_fin_chrg_petrol_amt	  ,0) <>	upd_rec.	accrued_fin_chrg_petrol_amt	or	
            nvl(fnd.accrued_fin_chrg_purch_amt	  ,0) <>	upd_rec.	accrued_fin_chrg_purch_amt	or	
            nvl(fnd.avg_budget_balance	          ,0) <>	upd_rec.	avg_budget_balance	or	
            nvl(fnd.avg_card_life_balance	        ,0) <>	upd_rec.	avg_card_life_balance	or	
            nvl(fnd.avg_cash_withdrawal_balance	  ,0) <>	upd_rec.	avg_cash_withdrawal_balance	or	
            nvl(fnd.avg_credit_balance	          ,0) <>	upd_rec.	avg_credit_balance	or	
            nvl(fnd.avg_debit_balance	            ,0) <>	upd_rec.	avg_debit_balance	or	
            nvl(fnd.avg_petrol_balance	          ,0) <>	upd_rec.	avg_petrol_balance	or	
            nvl(fnd.avg_purchase_balance	        ,0) <>	upd_rec.	avg_purchase_balance	or	
            nvl(fnd.card_life_bal_days_per_month	,0) <>	upd_rec.	card_life_bal_days_per_month	or	
            nvl(fnd.total_avg_balance	            ,0) <>	upd_rec.	total_avg_balance	or	
            nvl(fnd.total_bal_budget	            ,0) <>	upd_rec.	total_bal_budget	or	
            nvl(fnd.total_bal_cash            	  ,0) <>	upd_rec.	total_bal_cash	or	
            nvl(fnd.total_bal_petrol	            ,0) <>	upd_rec.	total_bal_petrol	or	
            nvl(fnd.total_bal_purchase_current	  ,0) <>	upd_rec.	total_bal_purchase_current	or	
            nvl(fnd.total_bal_purchase_old	      ,0) <>	upd_rec.	total_bal_purchase_old	or	
            nvl(fnd.total_bal_purchase_prior	    ,0) <>	upd_rec.	total_bal_purchase_prior	or	
            nvl(fnd.total_card_life_balance	      ,0) <>	upd_rec.	total_card_life_balance	or	
            nvl(fnd.total_credit_balance	        ,0) <>	upd_rec.	total_credit_balance	or	
            nvl(fnd.total_debit_balance	          ,0) <>	upd_rec.	total_debit_balance	or	
            nvl(fnd.total_fin_chrg_cash_amt   	  ,0) <>	upd_rec.	total_fin_chrg_cash_amt	or	
            nvl(fnd.total_fin_chrg_petrol_amt 	  ,0) <>	upd_rec.	total_fin_chrg_petrol_amt	or	
            nvl(fnd.total_fin_chrg_purch_amt	    ,0) <>	upd_rec.	total_fin_chrg_purch_amt	
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_crd_tover_mly_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
             cpy.	INFORMATION_DATE	,
             cpy.	ACCOUNT_NUMBER	,
             cpy.	ACCRUED_CREDIT_INT_INC_AMT	,
             cpy.	ACCRUED_FIN_CHRG_BUDGET_AMT	,
             cpy.	ACCRUED_FIN_CHRG_CASH_AMT	,
             cpy.	ACCRUED_FIN_CHRG_PETROL_AMT	,
             cpy.	ACCRUED_FIN_CHRG_PURCH_AMT	,
             cpy.	AVG_BUDGET_BALANCE	,
             cpy.	AVG_CARD_LIFE_BALANCE	,
             cpy.	AVG_CASH_WITHDRAWAL_BALANCE	,
             cpy.	AVG_CREDIT_BALANCE	,
             cpy.	AVG_DEBIT_BALANCE	,
             cpy.	AVG_PETROL_BALANCE	,
             cpy.	AVG_PURCHASE_BALANCE	,
             cpy.	CARD_LIFE_BAL_DAYS_PER_MONTH	,
             cpy.	TOTAL_AVG_BALANCE	,
             cpy.	TOTAL_BAL_BUDGET	,
             cpy.	TOTAL_BAL_CASH	,
             cpy.	TOTAL_BAL_PETROL	,
             cpy.	TOTAL_BAL_PURCHASE_CURRENT	,
             cpy.	TOTAL_BAL_PURCHASE_OLD	,
             cpy.	TOTAL_BAL_PURCHASE_PRIOR	,
             cpy.	TOTAL_CARD_LIFE_BALANCE	,
             cpy.	TOTAL_CREDIT_BALANCE	,
             cpy.	TOTAL_DEBIT_BALANCE	,
             cpy.	TOTAL_FIN_CHRG_CASH_AMT	,
             cpy.	TOTAL_FIN_CHRG_PETROL_AMT	,
             cpy.	TOTAL_FIN_CHRG_PURCH_AMT
      from   stg_absa_crd_tover_mly_cpy cpy
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
    from   stg_absa_crd_tover_mly_cpy
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
--    update stg_absa_crd_tover_mly_cpy
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
       raise;
end wh_fnd_wfs_314u;
