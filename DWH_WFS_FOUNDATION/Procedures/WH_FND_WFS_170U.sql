--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_170U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_170U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Date:        2016-03-14
-- Author:      Naresh Chauhan
-- Purpose:     update table FND_WFS_OM4_CUST_INCOME in the foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - STG_OM4_CUST_INCOME_CPY
--              Output - FND_WFS_OM4_CUST_INCOME
-- Packages:    constants, dwh_log, dwh_valid
--
-- Maintenance:
--  2016-03-14 N Chauhan - created - based on WH_FND_WFS_150U
--  2017-03-30 N Chauhan - Additional fields for Data Revitalisation project.
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster than on the original template.
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

-- NB.
-- ADW advised that no referential checks are required due to 2 reasons:
-- 1.
-- WFS wants all data to be saved, irrespective of whether referential checks are satisfied or not,
-- and should not be hospitalised.
-- 2. 
-- OM4 data comes in 1 xml file split up at AIT for the different OM4 tables.
-- So, as long as the transmission from AIT to DWH is secure, there is no need for referential integrity checking.
-- RI checks are more important when data from source come via different channels.



g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;   
g_truncate_count     integer       :=  0;


g_application_no       stg_om4_cust_income_cpy.credit_applications_id%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_170U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS OMINC DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

CURSOR STG_DUP IS
SELECT * FROM STG_OM4_CUST_INCOME_CPY
where (credit_applications_id)
in
(SELECT CREDIT_APPLICATIONS_ID
FROM STG_OM4_CUST_INCOME_CPY 
group by credit_applications_id
having count(*) > 1) 
order by credit_applications_id, sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_wfs_om4_omapp_dly is
select /*+ FULL(stg)  parallel (stg,2) */  
              STG.*
      FROM    STG_OM4_CUST_INCOME_CPY STG,
              FND_WFS_OM4_CUST_INCOME fnd
      where   stg.credit_applications_id        = fnd.credit_applications_id   and             
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.credit_applications_id,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin

g_application_no   := 0;

for dupp_record in stg_dup
   loop

    IF  DUPP_RECORD.CREDIT_APPLICATIONS_ID       = G_APPLICATION_NO THEN
        UPDATE STG_OM4_CUST_INCOME_CPY STG
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

    g_application_no   := dupp_record.credit_applications_id;
    
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
      
      insert /*+ APPEND parallel (fnd,2) */ into FND_WFS_OM4_CUST_INCOME fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
         cpy. credit_applications_id ,
         cpy. net_monthly_income_type ,
         cpy. net_monthly_income_month1 ,
         cpy. net_monthly_income_month2 ,
         cpy. net_monthly_income_month3 ,
         cpy. net_monthly_income_total ,
         cpy. net_monthly_income_avg ,
         cpy. add_monthly_income1_type ,
         cpy. add_monthly_income1_month1 ,
         cpy. add_monthly_income1_month2 ,
         cpy. add_monthly_income1_month3 ,
         cpy. add_monthly_income1_total ,
         cpy. add_monthly_income1_avg ,
         cpy. add_monthly_income2_type ,
         cpy. add_monthly_income2_month1 ,
         cpy. add_monthly_income2_month2 ,
         cpy. add_monthly_income2_month3 ,
         cpy. add_monthly_income2_total ,
         cpy. add_monthly_income2_avg ,
         cpy. add_monthly_income3_type ,
         cpy. add_monthly_income3_month1 ,
         cpy. add_monthly_income3_month2 ,
         cpy. add_monthly_income3_month3 ,
         cpy. add_monthly_income3_total ,
         cpy. add_monthly_income3_avg ,
         cpy. add_monthly_income_tot_type ,
         cpy. add_monthly_income_tot_month1 ,
         cpy. add_monthly_income_tot_month2 ,
         cpy. add_monthly_income_tot_month3 ,
         cpy. add_monthly_income__tot_total ,
         cpy. add_monthly_income_tot_avg ,
         cpy. tot_monthly_income_type ,
         cpy. tot_monthly_income_month1 ,
         cpy. tot_monthly_income_month2 ,
         cpy. tot_monthly_income_month3 ,
         cpy. tot_monthly_income_total ,
         cpy. tot_monthly_income_avg ,
         g_date as last_updated_date ,
         cpy. add_monthly_income_doc_source ,
         cpy. net_monthly_income_doc_source
              
      FROM    STG_OM4_CUST_INCOME_CPY CPY
       where  not exists 
      (select /*+ nl_aj */ * from FND_WFS_OM4_CUST_INCOME 
       where  credit_applications_id           = cpy.credit_applications_id)
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


for upd_rec in c_stg_wfs_om4_omapp_dly
   loop
     update FND_WFS_OM4_CUST_INCOME fnd 
     set    
         fnd. credit_applications_id = upd_rec. credit_applications_id ,
         fnd. net_monthly_income_type = upd_rec. net_monthly_income_type ,
         fnd. net_monthly_income_month1 = upd_rec. net_monthly_income_month1 ,
         fnd. net_monthly_income_month2 = upd_rec. net_monthly_income_month2 ,
         fnd. net_monthly_income_month3 = upd_rec. net_monthly_income_month3 ,
         fnd. net_monthly_income_total = upd_rec. net_monthly_income_total ,
         fnd. net_monthly_income_avg = upd_rec. net_monthly_income_avg ,
         fnd. add_monthly_income1_type = upd_rec. add_monthly_income1_type ,
         fnd. add_monthly_income1_month1 = upd_rec. add_monthly_income1_month1 ,
         fnd. add_monthly_income1_month2 = upd_rec. add_monthly_income1_month2 ,
         fnd. add_monthly_income1_month3 = upd_rec. add_monthly_income1_month3 ,
         fnd. add_monthly_income1_total = upd_rec. add_monthly_income1_total ,
         fnd. add_monthly_income1_avg = upd_rec. add_monthly_income1_avg ,
         fnd. add_monthly_income2_type = upd_rec. add_monthly_income2_type ,
         fnd. add_monthly_income2_month1 = upd_rec. add_monthly_income2_month1 ,
         fnd. add_monthly_income2_month2 = upd_rec. add_monthly_income2_month2 ,
         fnd. add_monthly_income2_month3 = upd_rec. add_monthly_income2_month3 ,
         fnd. add_monthly_income2_total = upd_rec. add_monthly_income2_total ,
         fnd. add_monthly_income2_avg = upd_rec. add_monthly_income2_avg ,
         fnd. add_monthly_income3_type = upd_rec. add_monthly_income3_type ,
         fnd. add_monthly_income3_month1 = upd_rec. add_monthly_income3_month1 ,
         fnd. add_monthly_income3_month2 = upd_rec. add_monthly_income3_month2 ,
         fnd. add_monthly_income3_month3 = upd_rec. add_monthly_income3_month3 ,
         fnd. add_monthly_income3_total = upd_rec. add_monthly_income3_total ,
         fnd. add_monthly_income3_avg = upd_rec. add_monthly_income3_avg ,
         fnd. add_monthly_income_tot_type = upd_rec. add_monthly_income_tot_type ,
         fnd. add_monthly_income_tot_month1 = upd_rec. add_monthly_income_tot_month1 ,
         fnd. add_monthly_income_tot_month2 = upd_rec. add_monthly_income_tot_month2 ,
         fnd. add_monthly_income_tot_month3 = upd_rec. add_monthly_income_tot_month3 ,
         fnd. add_monthly_income__tot_total = upd_rec. add_monthly_income__tot_total ,
         fnd. add_monthly_income_tot_avg = upd_rec. add_monthly_income_tot_avg ,
         fnd. tot_monthly_income_type = upd_rec. tot_monthly_income_type ,
         fnd. tot_monthly_income_month1 = upd_rec. tot_monthly_income_month1 ,
         fnd. tot_monthly_income_month2 = upd_rec. tot_monthly_income_month2 ,
         fnd. tot_monthly_income_month3 = upd_rec. tot_monthly_income_month3 ,
         fnd. tot_monthly_income_total = upd_rec. tot_monthly_income_total ,
         fnd. tot_monthly_income_avg = upd_rec. tot_monthly_income_avg ,
         fnd.last_updated_date          = g_date ,
         fnd. add_monthly_income_doc_source = upd_rec. add_monthly_income_doc_source ,
         fnd. net_monthly_income_doc_source = upd_rec. net_monthly_income_doc_source
         
            
            
     where  fnd.credit_applications_id     = upd_rec.credit_applications_id and
            ( 
             nvl(fnd. credit_applications_id,0) <> upd_rec. credit_applications_id OR
             nvl(fnd. net_monthly_income_type,0) <> upd_rec. net_monthly_income_type OR
             nvl(fnd. net_monthly_income_month1,0) <> upd_rec. net_monthly_income_month1 OR
             nvl(fnd. net_monthly_income_month2,0) <> upd_rec. net_monthly_income_month2 OR
             nvl(fnd. net_monthly_income_month3,0) <> upd_rec. net_monthly_income_month3 OR
             nvl(fnd. net_monthly_income_total,0) <> upd_rec. net_monthly_income_total OR
             nvl(fnd. net_monthly_income_avg,0) <> upd_rec. net_monthly_income_avg OR
             nvl(fnd. add_monthly_income1_type,0) <> upd_rec. add_monthly_income1_type OR
             nvl(fnd. add_monthly_income1_month1,0) <> upd_rec. add_monthly_income1_month1 OR
             nvl(fnd. add_monthly_income1_month2,0) <> upd_rec. add_monthly_income1_month2 OR
             nvl(fnd. add_monthly_income1_month3,0) <> upd_rec. add_monthly_income1_month3 OR
             nvl(fnd. add_monthly_income1_total,0) <> upd_rec. add_monthly_income1_total OR
             nvl(fnd. add_monthly_income1_avg,0) <> upd_rec. add_monthly_income1_avg OR
             nvl(fnd. add_monthly_income2_type,0) <> upd_rec. add_monthly_income2_type OR
             nvl(fnd. add_monthly_income2_month1,0) <> upd_rec. add_monthly_income2_month1 OR
             nvl(fnd. add_monthly_income2_month2,0) <> upd_rec. add_monthly_income2_month2 OR
             nvl(fnd. add_monthly_income2_month3,0) <> upd_rec. add_monthly_income2_month3 OR
             nvl(fnd. add_monthly_income2_total,0) <> upd_rec. add_monthly_income2_total OR
             nvl(fnd. add_monthly_income2_avg,0) <> upd_rec. add_monthly_income2_avg OR
             nvl(fnd. add_monthly_income3_type,0) <> upd_rec. add_monthly_income3_type OR
             nvl(fnd. add_monthly_income3_month1,0) <> upd_rec. add_monthly_income3_month1 OR
             nvl(fnd. add_monthly_income3_month2,0) <> upd_rec. add_monthly_income3_month2 OR
             nvl(fnd. add_monthly_income3_month3,0) <> upd_rec. add_monthly_income3_month3 OR
             nvl(fnd. add_monthly_income3_total,0) <> upd_rec. add_monthly_income3_total OR
             nvl(fnd. add_monthly_income3_avg,0) <> upd_rec. add_monthly_income3_avg OR
             nvl(fnd. add_monthly_income_tot_type,0) <> upd_rec. add_monthly_income_tot_type OR
             nvl(fnd. add_monthly_income_tot_month1,0) <> upd_rec. add_monthly_income_tot_month1 OR
             nvl(fnd. add_monthly_income_tot_month2,0) <> upd_rec. add_monthly_income_tot_month2 OR
             nvl(fnd. add_monthly_income_tot_month3,0) <> upd_rec. add_monthly_income_tot_month3 OR
             nvl(fnd. add_monthly_income__tot_total,0) <> upd_rec. add_monthly_income__tot_total OR
             nvl(fnd. add_monthly_income_tot_avg,0) <> upd_rec. add_monthly_income_tot_avg OR
             nvl(fnd. tot_monthly_income_type,0) <> upd_rec. tot_monthly_income_type OR
             nvl(fnd. tot_monthly_income_month1,0) <> upd_rec. tot_monthly_income_month1 OR
             nvl(fnd. tot_monthly_income_month2,0) <> upd_rec. tot_monthly_income_month2 OR
             nvl(fnd. tot_monthly_income_month3,0) <> upd_rec. tot_monthly_income_month3 OR
             nvl(fnd. tot_monthly_income_total,0) <> upd_rec. tot_monthly_income_total OR
             nvl(fnd. tot_monthly_income_avg,0) <> upd_rec. tot_monthly_income_avg or
             nvl(fnd. add_monthly_income_doc_source, 0) <> upd_rec. add_monthly_income_doc_source OR
             nvl(fnd. net_monthly_income_doc_source, 0) <> upd_rec. net_monthly_income_doc_source
             
            );         
             
      g_recs_updated := g_recs_updated  + sql%rowcount;        
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
     
      insert /*+ APPEND parallel (hsp,2) */ into STG_OM4_CUST_INCOME_HSP hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
              cpy.sys_source_batch_id,
              cpy.sys_source_sequence_no,
              sysdate,'Y','DWH',
              cpy.sys_middleware_batch_id,
              'VALIDATION FAIL - REFERENTIAL ERROR',
             cpy. credit_applications_id ,
             cpy. net_monthly_income_type ,
             cpy. net_monthly_income_month1 ,
             cpy. net_monthly_income_month2 ,
             cpy. net_monthly_income_month3 ,
             cpy. net_monthly_income_total ,
             cpy. net_monthly_income_avg ,
             cpy. add_monthly_income1_type ,
             cpy. add_monthly_income1_month1 ,
             cpy. add_monthly_income1_month2 ,
             cpy. add_monthly_income1_month3 ,
             cpy. add_monthly_income1_total ,
             cpy. add_monthly_income1_avg ,
             cpy. add_monthly_income2_type ,
             cpy. add_monthly_income2_month1 ,
             cpy. add_monthly_income2_month2 ,
             cpy. add_monthly_income2_month3 ,
             cpy. add_monthly_income2_total ,
             cpy. add_monthly_income2_avg ,
             cpy. add_monthly_income3_type ,
             cpy. add_monthly_income3_month1 ,
             cpy. add_monthly_income3_month2 ,
             cpy. add_monthly_income3_month3 ,
             cpy. add_monthly_income3_total ,
             cpy. add_monthly_income3_avg ,
             cpy. add_monthly_income_tot_type ,
             cpy. add_monthly_income_tot_month1 ,
             cpy. add_monthly_income_tot_month2 ,
             cpy. add_monthly_income_tot_month3 ,
             cpy. add_monthly_income__tot_total ,
             cpy. add_monthly_income_tot_avg ,
             cpy. tot_monthly_income_type ,
             cpy. tot_monthly_income_month1 ,
             cpy. tot_monthly_income_month2 ,
             cpy. tot_monthly_income_month3 ,
             cpy. tot_monthly_income_total ,
             cpy. tot_monthly_income_avg ,
             cpy. add_monthly_income_doc_source ,
             cpy. net_monthly_income_doc_source
             
              
      FROM   STG_OM4_CUST_INCOME_CPY CPY
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
    INTO   G_RECS_READ
    FROM   STG_OM4_CUST_INCOME_CPY
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

--   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
--      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--      p_success := false;
--      l_message := 'ERROR - Record counts do not balance see log file';
--      dwh_log.record_error(l_module_name,sqlcode,l_message);
--      raise_application_error (-20246,'Record count error - see log files');
--   end if;  


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
end WH_FND_WFS_170U;
