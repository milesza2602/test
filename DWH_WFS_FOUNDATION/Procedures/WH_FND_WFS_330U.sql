--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_330U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_330U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Purpose:     Create fnd_wfs_triad_strategy_outcome fact table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_absa_triad_strategy_cpy
--               Output - fnd_wfs_triad_strategy_outcome
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
g_count              integer       :=  0;


g_wfs_account_no   stg_absa_triad_strategy_cpy.wfs_account_no%type;  

   
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_330U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD fnd_wfs_triad_strategy_outcome EX ABSA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--cursor stg_dup is
--select  * from stg_absa_triad_strategy_cpy
--where 
--        (wfs_account_no
--        )
--in
--(select wfs_account_no
--from    stg_absa_triad_strategy_cpy 
--group by wfs_account_no
--having count(*) > 1) 
--order by wfs_account_no,
--         sys_source_batch_id desc ,sys_source_sequence_no desc;


--cursor c_stg_absa_triad_strategy is
--select /*+ FULL(stg)  parallel (stg,2) */  
               --fields to update --
--      from    stg_absa_triad_strategy_cpy cpy,
--              fnd_wfs_triad_strategy_outcome fnd
--      where   cpy.wfs_account_no      = fnd.wfs_account_no and             
--              cpy.sys_process_code     = 'N'  
--      order by
--              cpy.wfs_account_no,
--              cpy.sys_source_batch_id,cpy.sys_source_sequence_no; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
/*
procedure remove_duplicates as
begin


   g_ww_pan_token_no   := 0; 

   
for dupp_record in stg_dup
   loop

    if  dupp_record.ww_pan_token_no  = g_ww_pan_token_no      then
        update stg_absa_triad_strategy_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_ww_pan_token_no   := dupp_record.ww_pan_token_no; 



   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;

*/

--************************************************************************************************** 
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_triad_strategy_outcome fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
--   BELOW 2 LINES REPLACED ABOVE 2 WHEN AN INTERNAL ERROR OCCURED AFTER A DB CRASH 30 NOV 2016      
--      insert  into fnd_wfs_triad_strategy_outcome fnd
--      select 
              CPY.WFS_ACCOUNT_NO,
              CPY.RANDOM_DIGIT_1,
              CPY.RANDOM_DIGIT_2,
              CPY.RANDOM_DIGIT_3,
              CPY.RANDOM_DIGIT_4,
              CPY.CREDIT_FACILITY_STRATEGY_ID,
              CPY.CREDIT_FACILITY_SCENARIO_ID,
              CPY.CREDIT_FACILITY_TRIAD_LTTR_CDE,
              CPY.AUTHORISATION_STRATEGY_ID_1,
              CPY.AUTHORISATION_SCENARIO_ID_1,
              CPY.AUTHORISATION_TRIAD_LTTR_CDE,
              CPY.AUTHORISATION_STRATEGY_ID_2,
              CPY.AUTHORISATION_SCENARIO_ID_2,
              CPY.AUTHORISATION_STRATEGY_ID_3,
              CPY.AUTHORISATION_SCENARIO_ID_3,
              CPY.AUTHORISATION_STRATEGY_ID_4,
              CPY.AUTHORISATION_SCENARIO_ID_4,
              CPY.COLLECTIONS_STRATEGY_ID,
              CPY.COLLECTIONS_SCENARIO_ID,
              CPY.COLLECTIONS_TRIAD_LTTR_CDE,
              CPY.COLLECTIONS_CLASS,
              CPY.MARKETING_COMMS_STRATEGY_ID_1,
              CPY.MARKETING_COMMS_SCENARIO_ID_1,
              CPY.MARKETING_COMMS_STRATEGY_ID_2,
              CPY.MARKETING_COMMS_SCENARIO_ID_2,
              CPY.MARKETING_COMMS_STRATEGY_ID_3,
              CPY.MARKETING_COMMS_SCENARIO_ID_3,
              CPY.MARKETING_COMMS_STRATEGY_ID_4,
              CPY.MARKETING_COMMS_SCENARIO_ID_4,
              CPY.MARKETING_COMMS_STRATEGY_ID_5,
              CPY.MARKETING_COMMS_SCENARIO_ID_5,
              g_date as last_updated_date
      from    stg_absa_triad_strategy_cpy cpy;
--      where   not exists 
--      (select /*+ nl_aj */ * from fnd_wfs_triad_strategy_outcome 
--       where  ww_pan_token_no    = cpy.ww_pan_token_no )
--       and    sys_process_code = 'N';
 

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
/*
procedure flagged_records_update as
begin


for upd_rec in c_stg_absa_triad_strategy
   loop
     update fnd_wfs_triad_strategy_outcome fnd 
     set    
            fnd.	absa_acc_token_no   =	upd_rec.	absa_acc_token_no          	,
            fnd.	absa_pan_token_no  	=	upd_rec.	absa_pan_token_no          	,
            fnd.  last_updated_date   = g_date
     where  fnd.	ww_pan_token_no     =	upd_rec.	ww_pan_token_no ;

         
             
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

*/    
--************************************************************************************************** 
-- Send records to hospital where not valid
--**************************************************************************************************
/*
procedure flagged_records_hospital as
begin
     
 
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

*/    

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
    
    

 /*   
    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    remove_duplicates;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;
*/

    select count(*)
    into   g_recs_read
    from   stg_absa_triad_strategy_cpy
    where  sys_process_code = 'N';
    
    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_insert;
    
--    l_text := 'BULK HOSPITALIZATION STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--   flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_absa_triad_strategy_cpy
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
end wh_fnd_wfs_330u;
