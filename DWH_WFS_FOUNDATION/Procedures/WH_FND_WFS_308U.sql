--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_308U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_308U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_crd_budg_mly fact table in the foundation layer
--               with input ex staging table from ABSA.
--  Tables:      Input  - stg_absa_crd_budg_mly_cpy
--               Output - fnd_wfs_crd_budg_mly
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


g_information_date                 stg_absa_crd_budg_mly_cpy.information_date%type;  
g_account_number                   stg_absa_crd_budg_mly_cpy.account_number%type; 
g_budget_reference_number          stg_absa_crd_budg_mly_cpy.budget_reference_number%type;  

   
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_308U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS CARD BUDGET MLY EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_absa_crd_budg_mly_cpy
where (information_date,
account_number,
budget_reference_number)
in
(select information_date,
account_number,
budget_reference_number 
from stg_absa_crd_budg_mly_cpy 
group by information_date,
account_number,
budget_reference_number
having count(*) > 1) 
order by information_date,
account_number,
budget_reference_number,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_absa_crd_budg_mly is
select /*+ FULL(stg) parallel (stg,4)  full(fnd) parallel (fnd,4)  */  
              stg.*
      from    stg_absa_crd_budg_mly_cpy stg,
              fnd_wfs_crd_budg_mly fnd
      where   stg.information_date         = fnd.information_date  and             
              stg.account_number           = fnd.account_number    and   
              stg.budget_reference_number  = fnd.budget_reference_number       and 
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.information_date,
              stg.account_number,
              stg.budget_reference_number,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin


   g_information_date        := '1 Jan 2000'; 
   g_account_number          := '0';
   g_budget_reference_number := '0'; 
 
for dupp_record in stg_dup
   loop

    if  dupp_record.information_date        = g_information_date and
        dupp_record.account_number          = g_account_number and
        dupp_record.budget_reference_number = g_budget_reference_number  then
        update stg_absa_crd_budg_mly_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_information_date         := dupp_record.information_date; 
    g_account_number           := dupp_record.account_number;
    g_budget_reference_number  := dupp_record.budget_reference_number; 

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
      
      insert /*+ APPEND parallel (fnd,4) */ into fnd_wfs_crd_budg_mly fnd
      select /*+ FULL(cpy)  parallel (cpy,4) */
             cpy.	account_number	,
             cpy.	budget_reference_number	,
             cpy.	information_date	,
             cpy.	budget_balance_amt	,
             cpy.	statemnt_budget_balance_amt	,
             cpy.	statemnt_other_direct_payments	,
             cpy.	budget_original_purchase_amt	,
             cpy.	budget_payment_amt	,
             cpy.	statemnt_instalment_amt	,
             cpy.	budget_fin_charges_amt	,
             cpy.	budget_fin_charges_for_cycle	,
             cpy.	budget_merchant_name	,
             cpy.	budget_term	,
             cpy.	budget_interest_rate	,
             cpy.	budget_purchase_date	,
             cpy.	budget_paid_off_date	,
             cpy.	budget_plan_type_code	,
             g_date as last_updated_date
      from   stg_absa_crd_budg_mly_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_crd_budg_mly 
       where  information_date         = cpy.information_date and
              account_number           = cpy.account_number and
              budget_reference_number  = cpy.budget_reference_number )
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



for upd_rec in c_stg_absa_crd_budg_mly
   loop
     update fnd_wfs_crd_budg_mly fnd 
     set    fnd.	budget_balance_amt            	=	upd_rec.	budget_balance_amt	,
            fnd.	statemnt_budget_balance_amt   	=	upd_rec.	statemnt_budget_balance_amt	,
            fnd.	statemnt_other_direct_payments	=	upd_rec.	statemnt_other_direct_payments	,
            fnd.	budget_original_purchase_amt  	=	upd_rec.	budget_original_purchase_amt	,
            fnd.	budget_payment_amt            	=	upd_rec.	budget_payment_amt	,
            fnd.	statemnt_instalment_amt       	=	upd_rec.	statemnt_instalment_amt	,
            fnd.	budget_fin_charges_amt	        =	upd_rec.	budget_fin_charges_amt	,
            fnd.	budget_fin_charges_for_cycle  	=	upd_rec.	budget_fin_charges_for_cycle	,
            fnd.	budget_merchant_name           	=	upd_rec.	budget_merchant_name	,
            fnd.	budget_term	                    =	upd_rec.	budget_term	,
            fnd.	budget_interest_rate	          =	upd_rec.	budget_interest_rate	,
            fnd.	budget_purchase_date	          =	upd_rec.	budget_purchase_date	,
            fnd.	budget_paid_off_date	          =	upd_rec.	budget_paid_off_date	,
            fnd.	budget_plan_type_code	          =	upd_rec.	budget_plan_type_code	,
            fnd.  last_updated_date               = g_date
     where  fnd.	information_date                =	upd_rec.	information_date and
            fnd.	account_number                	=	upd_rec.	account_number   and
            fnd.	budget_reference_number        	=	upd_rec.	budget_reference_number	     and
            ( 
            nvl(fnd.budget_balance_amt	            ,0) <>	upd_rec.	budget_balance_amt	or
            nvl(fnd.statemnt_budget_balance_amt	    ,0) <>	upd_rec.	statemnt_budget_balance_amt	or
            nvl(fnd.statemnt_budget_balance_amt	    ,0) <>	upd_rec.	statemnt_budget_balance_amt	or
            nvl(fnd.statemnt_other_direct_payments  ,0) <>	upd_rec.	statemnt_other_direct_payments	or
            nvl(fnd.budget_original_purchase_amt	  ,0) <>	upd_rec.	budget_original_purchase_amt	or
            nvl(fnd.budget_payment_amt            	,0) <>	upd_rec.	budget_payment_amt	or
            nvl(fnd.statemnt_instalment_amt       	,0) <>	upd_rec.	statemnt_instalment_amt	or
            nvl(fnd.budget_fin_charges_amt	        ,0) <>	upd_rec.	budget_fin_charges_amt	or
            nvl(fnd.budget_fin_charges_for_cycle  	,0) <>	upd_rec.	budget_fin_charges_for_cycle	or
            nvl(fnd.budget_merchant_name	          ,0) <>	upd_rec.	budget_merchant_name	or
            nvl(fnd.budget_term	                    ,0) <>	upd_rec.	budget_term	or
            nvl(fnd.budget_interest_rate	          ,0) <>	upd_rec.	budget_interest_rate	or
            nvl(fnd.budget_purchase_date	          ,'1 Jan 1900') <>	upd_rec.	budget_purchase_date	or
            nvl(fnd.budget_paid_off_date	          ,'1 Jan 1900') <>	upd_rec.	budget_paid_off_date	or
            nvl(fnd.budget_plan_type_code	          ,0) <>	upd_rec.	budget_plan_type_code	 

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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_absa_crd_budg_mly_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
             cpy.	account_number	,
             cpy.	budget_reference_number	,
             cpy.	information_date	,
             cpy.	budget_balance_amt	,
             cpy.	statemnt_budget_balance_amt	,
             cpy.	statemnt_other_direct_payments	,
             cpy.	budget_original_purchase_amt	,
             cpy.	budget_payment_amt	,
             cpy.	statemnt_instalment_amt	,
             cpy.	budget_fin_charges_amt	,
             cpy.	budget_fin_charges_for_cycle	,
             cpy.	budget_merchant_name	,
             cpy.	budget_term	,
             cpy.	budget_interest_rate	,
             cpy.	budget_purchase_date	,
             cpy.	budget_paid_off_date	,
             cpy.	budget_plan_type_code 

      from   stg_absa_crd_budg_mly_cpy cpy
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
    from   stg_absa_crd_budg_mly_cpy
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
--    update stg_absa_crd_budg_mly_cpy
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
end wh_fnd_wfs_308u;
