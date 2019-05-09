--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_413U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_413U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: Load Credit Limit Increase (CLI) data
-- Tran_type: ODWH: ��CLIPPP   AIT: �CLIPPP
--
-- Date:       2016-06-14
-- Author:      Naresh Chauhan
-- Purpose:     update table FND_WFS_CLI_PROD_PMNT_PROFL in the foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - STG_C2_CLI_PROD_PMNT_PROFL_CPY
--              Output - FND_WFS_CLI_PROD_PMNT_PROFL
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2016-06-14 N Chauhan - created - based on WH_FND_WFS_410U

--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster than on the original template.
--  Naming conventions
--  g_ -  Global variable
--  l_ -  Log table variable
--  a_ -  Array variable
--  v_ -  Local variable as found in packages
--  p_ -  Parameter
--  c_ -  Prefix to cursor
--**************************************************************************************************




g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_nochange      integer       :=  0;
g_recs_duplicate     integer       :=  0;   
g_truncate_count     integer       :=  0;


g_unique_key_field_val       dwh_wfs_foundation.stg_c2_cli_prod_pmnt_profl_cpy.cli_product_payment_profile_no%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_413U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CREDIT LIMIT INCREASE (CLI) DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
   select * from  dwh_wfs_foundation.stg_c2_cli_prod_pmnt_profl_cpy
   where (cli_product_payment_profile_no)
   in
   (select cli_product_payment_profile_no 
    from dwh_wfs_foundation.stg_c2_cli_prod_pmnt_profl_cpy
    group by  cli_product_payment_profile_no
    having count(*) > 1) 
   order by
    cli_product_payment_profile_no,
    sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg is
   select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    dwh_wfs_foundation.stg_c2_cli_prod_pmnt_profl_cpy stg,
              dwh_wfs_foundation.fnd_wfs_cli_prod_pmnt_profl fnd
--              dwh_wfs_foundation.fnd_wfs_cli_offr dep
      where   stg.cli_product_payment_profile_no   = fnd.cli_product_payment_profile_no     -- only ones existing in fnd
--              and stg.cli_offer_no  =  dep.cli_offer_no       -- only those existing in depended table
              and stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.cli_product_payment_profile_no,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very 'rare' occasion they may be present
--**************************************************************************************************

procedure remove_duplicates as
begin

   g_unique_key_field_val   := 0;

   for dupp_record in stg_dup
    loop

       if  dupp_record.cli_product_payment_profile_no  = g_unique_key_field_val then
        update dwh_wfs_foundation.stg_c2_cli_prod_pmnt_profl_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
       end if;           

       g_unique_key_field_val   := dupp_record.cli_product_payment_profile_no;
    
    end loop;
   
   commit;
 
exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all NEW record in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
      
      insert /*+ append parallel (fnd,2) */ into fnd_wfs_cli_prod_pmnt_profl fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
         cpy. cli_product_payment_profile_no ,
         cpy. cli_offer_no ,
         cpy. account_opened_date ,
         cpy. last_limit_change_date ,
         cpy. delinquent_ind ,
         cpy. payment_profile ,
         cpy. current_delinquency ,
         cpy. current_credit_limit ,
         cpy. current_balance ,
         cpy. current_installment ,
         cpy. behaviour_score ,
         cpy. bureau_score ,
         cpy. create_date ,
         cpy. created_by ,
         cpy. product_category ,
         cpy. product_offering_no ,
         cpy. account_status ,
         cpy. block_code

         ,
         g_date as last_updated_date 
              
      from  dwh_wfs_foundation.stg_c2_cli_prod_pmnt_profl_cpy cpy
--         inner join dwh_wfs_foundation.fnd_wfs_cli_offr dep  on dep.cli_offer_no = cpy.cli_offer_no
         left outer join dwh_wfs_foundation.fnd_wfs_cli_prod_pmnt_profl fnd on fnd.cli_product_payment_profile_no = cpy.cli_product_payment_profile_no
      where fnd.cli_product_payment_profile_no is null
          
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
-- Updates existing records in the staging table into foundation if there are changes
--**************************************************************************************************

procedure flagged_records_update as
begin


for upd_rec in c_stg
   loop
     update fnd_wfs_cli_prod_pmnt_profl fnd 
     set    
         fnd. cli_product_payment_profile_no = upd_rec. cli_product_payment_profile_no ,
         fnd. cli_offer_no = upd_rec. cli_offer_no ,
         fnd. account_opened_date = upd_rec. account_opened_date ,
         fnd. last_limit_change_date = upd_rec. last_limit_change_date ,
         fnd. delinquent_ind = upd_rec. delinquent_ind ,
         fnd. payment_profile = upd_rec. payment_profile ,
         fnd. current_delinquency = upd_rec. current_delinquency ,
         fnd. current_credit_limit = upd_rec. current_credit_limit ,
         fnd. current_balance = upd_rec. current_balance ,
         fnd. current_installment = upd_rec. current_installment ,
         fnd. behaviour_score = upd_rec. behaviour_score ,
         fnd. bureau_score = upd_rec. bureau_score ,
         fnd. create_date = upd_rec. create_date ,
         fnd. created_by = upd_rec. created_by ,
         fnd. product_category = upd_rec. product_category ,
         fnd. product_offering_no = upd_rec. product_offering_no ,
         fnd. account_status = upd_rec. account_status ,
         fnd. block_code = upd_rec. block_code

         , 
         fnd.last_updated_date          = g_date 
            
            
     where  fnd.cli_product_payment_profile_no = upd_rec.cli_product_payment_profile_no and
        ( 
         nvl(fnd. cli_product_payment_profile_no, 0) <> upd_rec. cli_product_payment_profile_no OR
         nvl(fnd. cli_offer_no, 0) <> upd_rec. cli_offer_no OR
         nvl(fnd. account_opened_date, '01 JAN 1900') <> upd_rec. account_opened_date OR
         nvl(fnd. last_limit_change_date, '01 JAN 1900') <> upd_rec. last_limit_change_date OR
         nvl(fnd. delinquent_ind, 0) <> upd_rec. delinquent_ind OR
         nvl(fnd. payment_profile, 0) <> upd_rec. payment_profile OR
         nvl(fnd. current_delinquency, 0) <> upd_rec. current_delinquency OR
         nvl(fnd. current_credit_limit, 0) <> upd_rec. current_credit_limit OR
         nvl(fnd. current_balance, 0) <> upd_rec. current_balance OR
         nvl(fnd. current_installment, 0) <> upd_rec. current_installment OR
         nvl(fnd. behaviour_score, 0) <> upd_rec. behaviour_score OR
         nvl(fnd. bureau_score, 0) <> upd_rec. bureau_score OR
         nvl(fnd. create_date, '01 JAN 1900') <> upd_rec. create_date OR
         nvl(fnd. created_by, 0) <> upd_rec. created_by OR
         nvl(fnd. product_category, 0) <> upd_rec. product_category OR
         nvl(fnd. product_offering_no, 0) <> upd_rec. product_offering_no OR
         nvl(fnd. account_status, 0) <> upd_rec. account_status OR
         nvl(fnd. block_code, 0) <> upd_rec. block_code

         );         


   if sql%rowcount = 0 then
        g_recs_nochange:= g_recs_nochange + 1;
   else
        g_recs_updated := g_recs_updated + 1;  
   end if;

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
     
      insert /*+ append parallel (hsp,2) */ into dwh_wfs_foundation.stg_c2_cli_prod_pmnt_profl_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
         cpy.sys_source_batch_id,
         cpy.sys_source_sequence_no,
         sysdate,'Y','DWH',
         cpy.sys_middleware_batch_id,
         'VALIDATION FAIL - REFERENTIAL ERROR with CLI_OFFER_NO' ,

         cpy. cli_product_payment_profile_no ,
         cpy. cli_offer_no ,
         cpy. account_opened_date ,
         cpy. last_limit_change_date ,
         cpy. delinquent_ind ,
         cpy. payment_profile ,
         cpy. current_delinquency ,
         cpy. current_credit_limit ,
         cpy. current_balance ,
         cpy. current_installment ,
         cpy. behaviour_score ,
         cpy. bureau_score ,
         cpy. create_date ,
         cpy. created_by ,
         cpy. product_category ,
         cpy. product_offering_no ,
         cpy. account_status ,
         cpy. block_code

              
      from   dwh_wfs_foundation.stg_c2_cli_prod_pmnt_profl_cpy cpy
       left outer join dwh_wfs_foundation.fnd_wfs_cli_offr dep on dep.cli_offer_no = cpy.cli_offer_no

      where 
         dep.cli_offer_no  is null
      
      
--      ) and 

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
    
    select count(*)
    into   g_recs_read
    from   dwh_wfs_foundation.stg_c2_cli_prod_pmnt_profl_cpy
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
--    update stg_...._cpy
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
    l_text := 'NO CHANGE RECORDS '||g_recs_nochange;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital + g_recs_nochange then
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

END WH_FND_WFS_413U;