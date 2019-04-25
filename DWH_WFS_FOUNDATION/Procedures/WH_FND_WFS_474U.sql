--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_474U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_474U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: Call Data - Load WFS Call Plan data
-- Tran_type: ODWH: IVRSDRPT   AIT: IVRSDRPT
--
-- Date:        2018-09-05
-- Author:      Safiyya Ismail
-- Purpose:     update table FND_WFS_CALL_PLAN in the Foundation layer
--              with input ex staging table from WFS.
--
-- Tables:      Input  - STG_VRNT_CALL_PLAN_CPY
--              Output - FND_WFS_CALL_PLAN
--
-- Packages:    constants, dwh_log
--
-- Maintenance:
--   2018-09-05 S Ismail - created.
--   2018-10-14 S Petersen - Renamed ACTUAL_PHONE_CALLS_ANSWERED to SERVICE_LEVEL_ACHIEVED_PERC 
--                           and FORECASTED_PHNE_CALLS_ANSWERED to SERVICE_LEVEL_FORECASTED_PERC.
--   2018-11-09 N Chauhan - removed dependency.
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


g_unique_key1_field_val  DWH_WFS_FOUNDATION.STG_VRNT_CALL_PLAN_CPY.QUEUE_ID%type;
g_unique_key2_field_val  DWH_WFS_FOUNDATION.STG_VRNT_CALL_PLAN_CPY.START_TIME%type;
--
--
--

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_474U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'VERINT DATA - LOAD WFS CALL PLAN DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_stg_dup is
   select * from  dwh_wfs_foundation.stg_vrnt_call_plan_cpy
   where (    queue_id, start_time  )
   in
   (select     queue_id, start_time 
    from dwh_wfs_foundation.stg_vrnt_call_plan_cpy
    group by      queue_id, start_time
    having count(*) > 1) 
   order by
    queue_id, start_time
    ,sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg is
   select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    dwh_wfs_foundation.stg_vrnt_call_plan_cpy stg
              ,dwh_wfs_foundation.fnd_wfs_call_plan fnd
     where
              fnd.queue_id   = stg.queue_id and      -- only ones existing in fnd
              fnd.start_time   = stg.start_time and      -- only ones existing in fnd

              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.queue_id,
              stg.start_time,

              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very 'rare' occasion they may be present
--**************************************************************************************************

procedure remove_duplicates as
begin

   g_unique_key1_field_val   := 0;
   g_unique_key2_field_val   := '01 JAN 1900';

   for dupp_record in c_stg_dup
    loop
       if 
               dupp_record.queue_id  = g_unique_key1_field_val
           and dupp_record.start_time  = g_unique_key2_field_val

       then 
        update dwh_wfs_foundation.stg_vrnt_call_plan_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;       
       end if;           

       g_unique_key1_field_val   := dupp_record.queue_id;
       g_unique_key2_field_val :=  dupp_record.start_time;

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

      insert /*+ append parallel (fnd,2) */ into fnd_wfs_call_plan fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
         cpy. queue_id ,
         cpy. start_time ,
         cpy. queue_name ,
         cpy. interval ,
         cpy. wfs_business_unit ,
         cpy. wfs_organisation ,
         cpy. actual_call_volume ,
         cpy. forecasted_call_volume ,
         cpy. service_level_achieved_perc ,
         cpy. service_level_forecasted_perc ,
         cpy. actual_average_handling_times ,
         cpy. forecasted_avg_handling_times ,
         cpy. actual_average_speed_of_answer ,
         cpy. forecasted_avg_speed_of_answer ,
         cpy. actual_staffing
        ,
         g_date as last_updated_date 

      from  dwh_wfs_foundation.stg_vrnt_call_plan_cpy cpy
         left outer join dwh_wfs_foundation.fnd_wfs_call_plan fnd on (
                 fnd.queue_id  = cpy.queue_id
             and fnd.start_time  = cpy.start_time

             )
      where fnd.queue_id is null

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
     update fnd_wfs_call_plan fnd 
     set    
         fnd. queue_id = upd_rec. queue_id ,
         fnd. start_time = upd_rec. start_time ,
         fnd. queue_name = upd_rec. queue_name ,
         fnd. interval = upd_rec. interval ,
         fnd. wfs_business_unit = upd_rec. wfs_business_unit ,
         fnd. wfs_organisation = upd_rec. wfs_organisation ,
         fnd. actual_call_volume = upd_rec. actual_call_volume ,
         fnd. forecasted_call_volume = upd_rec. forecasted_call_volume ,
         fnd. service_level_achieved_perc = upd_rec. service_level_achieved_perc ,
         fnd. service_level_forecasted_perc = upd_rec. service_level_forecasted_perc ,
         fnd. actual_average_handling_times = upd_rec. actual_average_handling_times ,
         fnd. forecasted_avg_handling_times = upd_rec. forecasted_avg_handling_times ,
         fnd. actual_average_speed_of_answer = upd_rec. actual_average_speed_of_answer ,
         fnd. forecasted_avg_speed_of_answer = upd_rec. forecasted_avg_speed_of_answer ,
         fnd. actual_staffing = upd_rec. actual_staffing
       , 
         fnd.last_updated_date          = g_date 


     where 
              fnd.queue_id  = upd_rec.queue_id and
              fnd.start_time  = upd_rec.start_time and

        ( 
         nvl(fnd. queue_id, 0) <> upd_rec. queue_id OR
         nvl(fnd. start_time, '01 JAN 1900') <> upd_rec. start_time OR
         nvl(fnd. queue_name, 0) <> upd_rec. queue_name OR
         nvl(fnd. interval, '01 JAN 1900') <> upd_rec. interval OR
         nvl(fnd. wfs_business_unit, 0) <> upd_rec. wfs_business_unit OR
         nvl(fnd. wfs_organisation, 0) <> upd_rec. wfs_organisation OR
         nvl(fnd. actual_call_volume, 0) <> upd_rec. actual_call_volume OR
         nvl(fnd. forecasted_call_volume, 0) <> upd_rec. forecasted_call_volume OR
         nvl(fnd. service_level_achieved_perc, 0) <> upd_rec. service_level_achieved_perc OR
         nvl(fnd. service_level_forecasted_perc, 0) <> upd_rec. service_level_forecasted_perc OR
         nvl(fnd. actual_average_handling_times, 0) <> upd_rec. actual_average_handling_times OR
         nvl(fnd. forecasted_avg_handling_times, 0) <> upd_rec. forecasted_avg_handling_times OR
         nvl(fnd. actual_average_speed_of_answer, 0) <> upd_rec. actual_average_speed_of_answer OR
         nvl(fnd. forecasted_avg_speed_of_answer, 0) <> upd_rec. forecasted_avg_speed_of_answer OR
         nvl(fnd. actual_staffing, 0) <> upd_rec. actual_staffing
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

-- ***  There is no dependency, so this proc is not called.
procedure flagged_records_hospital as
begin

      insert /*+ append parallel (hsp,2) */ into dwh_wfs_foundation.stg_vrnt_call_plan_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
         cpy.sys_source_batch_id,
         cpy.sys_source_sequence_no,
         sysdate,'Y','DWH',
         cpy.sys_middleware_batch_id,
         'VALIDATION FAIL - REFERENTIAL ERROR with QUEUE_ID' ,
         cpy. queue_id ,
         cpy. start_time ,
         cpy. queue_name ,
         cpy. interval ,
         cpy. wfs_business_unit ,
         cpy. wfs_organisation ,
         cpy. actual_call_volume ,
         cpy. forecasted_call_volume ,
         cpy. service_level_achieved_perc ,
         cpy. service_level_forecasted_perc ,
         cpy. actual_average_handling_times ,
         cpy. forecasted_avg_handling_times ,
         cpy. actual_average_speed_of_answer ,
         cpy. forecasted_avg_speed_of_answer ,
         cpy. actual_staffing
     from   dwh_wfs_foundation.stg_vrnt_call_plan_cpy cpy
       left outer join dwh_wfs_foundation.fnd_wfs_call_error dep on dep.evaluation_key = cpy.queue_id
      where 
         dep.evaluation_key  is null and


--      …       and 

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

    l_text := 'LOAD TABLE: '||'FND_WFS_CALL_PLAN' ;
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
    from   dwh_wfs_foundation.stg_vrnt_call_plan_cpy
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
       RAISE;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       RAISE;

end WH_FND_WFS_474U;
