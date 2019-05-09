--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_178U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_178U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Date:        December 2014
-- Author:      Jerome Appollis
-- Purpose:     Create fnd_wfs_om4_status table in the foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - stg_om4_status_cpy
--              Output - fnd_wfs_om4_status
-- Packages:    constants, dwh_log, dwh_valid
--
-- Maintenance:
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


g_application_no       stg_om4_status_cpy.application_no%type;
g_product_type         stg_om4_status_cpy.product_type%type;
g_activity             stg_om4_status_cpy.activity%type;  
g_activity_timestamp   stg_om4_status_cpy.activity_timestamp%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_178U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WFS OMSTAT DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * from stg_om4_status_cpy
where (application_no,product_type,activity, activity_timestamp)
in
(select application_no,product_type,activity, activity_timestamp
from stg_om4_status_cpy 
group by application_no,product_type,activity, activity_timestamp
having count(*) > 1) 
order by application_no,product_type,activity, activity_timestamp,
sys_source_batch_id desc ,sys_source_sequence_no desc;


cursor c_stg_wfs_om4_omstat_dly is
select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    stg_om4_status_cpy stg,
              fnd_wfs_om4_status fnd
      where   stg.application_no        = fnd.application_no   and             
              stg.product_type          = fnd.product_type    and   
              stg.activity              = fnd.activity      and   
              stg.activity_timestamp    = fnd.activity_timestamp      and                 
              stg.sys_process_code      = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.application_no,
              stg.product_type,
              stg.activity, stg.activity_timestamp, 
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin

g_application_no       := 0;
g_product_type         := ' ';
g_activity             := ' ';  
g_activity_timestamp   := '1 Jan 1900';




for dupp_record in stg_dup
   loop

    if  dupp_record.application_no       = g_application_no and
        dupp_record.product_type        = g_product_type  and
        dupp_record.activity          = g_activity  and
        dupp_record.activity_timestamp          = g_activity_timestamp  then
        update stg_om4_status_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

 
    g_application_no       := dupp_record.application_no;
    g_product_type         := dupp_record.product_type;
    g_activity             := dupp_record.activity; 
    g_activity_timestamp   := dupp_record.activity_timestamp;    
        
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
      
      insert /*+ APPEND parallel (fnd,2) */ into fnd_wfs_om4_status fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
              cpy.      application_no  ,
              cpy.      operator_id     ,
              cpy.      activity        ,
              cpy.      activity_timestamp      ,
              cpy.      decision        ,
              cpy.      status  ,
              cpy.      event   ,
              cpy.      isproduct       ,
              cpy.      product_type    ,
              g_date as date_last_udpated
      from    stg_om4_status_cpy cpy
      where  not exists 
      (select /*+ nl_aj */ * from fnd_wfs_om4_status 
       where  application_no      = cpy.application_no and
              product_type        = cpy.product_type and
              activity            = cpy.activity and              
              activity_timestamp  = cpy.activity_timestamp )
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



for upd_rec in c_stg_wfs_om4_omstat_dly
   loop
     update fnd_wfs_om4_status fnd 
     set    fnd.operator_id     =       upd_rec.operator_id     ,
            fnd.decision        =       upd_rec.decision        ,
            fnd.status  =       upd_rec.status  ,
            fnd.event   =       upd_rec.event   ,
            fnd.isproduct       =       upd_rec.isproduct       ,
            fnd.last_updated_date               = g_date
     where  fnd.application_no                  = upd_rec.application_no and
            fnd.product_type                   =        upd_rec.product_type and
            fnd.activity                     =  upd_rec.activity and
            fnd.activity_timestamp            = upd_rec.activity_timestamp and            
            ( 
            nvl(fnd.operator_id ,0) <>  upd_rec.operator_id     or
            nvl(fnd.decision    ,0) <>  upd_rec.decision        or
            nvl(fnd.status      ,0) <>  upd_rec.status  or
            nvl(fnd.event       ,0) <>  upd_rec.event   or
            nvl(fnd.isproduct   ,0) <>  upd_rec.isproduct       
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
     
      insert /*+ APPEND parallel (hsp,2) */ into stg_om4_status_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
              cpy.sys_source_batch_id
              ,cpy.sys_source_sequence_no
              ,sysdate,'Y','DWH'
              ,cpy.sys_middleware_batch_id
              ,'VALIDATION FAIL - REFERENCIAL ERROR'
              ,cpy.application_no       
              ,cpy.operator_id
              ,cpy.activity
              ,cpy.activity_timestamp
              ,cpy.decision
              ,cpy.status
              ,cpy.event
              ,cpy.isproduct
              ,cpy.product_type
      from   stg_om4_status_cpy cpy
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
    from   stg_om4_status_cpy
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
end WH_FND_WFS_178U;