--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_065U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_065U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        January 2016
--  Author:      Lwazi Ntloko
--  Purpose:     Create iterative procedure to reprocess WRNC schedule data for S4S
--  Tables:      Input  - STG_S4S_SCHED_XCPTN_EMP_DY_CPY
--               Output - FND_S4S_SCHED_XCPTN_EMP_DY
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:    Chg-41308 
--
--                  Standard insert/update, and write to hospital of invalid records.
--                  Delete process deletes from the day after batch date. As per request from Business. (Mojeed' call) 
--                  Procedure processes multiple number of batches, regardless of how many are sent
--
--
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;
g_run_date           date          := trunc(sysdate);
g_date               date;
G_MINEXCPTDATE       DATE;
g_sys_load_date      date; 

g_SYS_SOURCE_BATCH_ID STG_S4S_SCHED_XCPTN_EMP_DY.SYS_SOURCE_BATCH_ID%TYPE;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_065U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD Exceptions';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
--******---------------------------------------------------------------------------------------------------------*****--
--       Insert values per batch 
--******---------------------------------------------------------------------------------------------------------*****--
procedure fnd_insert as
begin
    g_recs := 0;
    
    insert into dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DY
    with selq as
    (select /*+ full(b1) parallel(d1,6) */ distinct 
           d1.EMPLOYEE_ID, d1.EXCEPTION_TYPE_ID, d1.EXCEPTION_DATE, d1.LOCATION_NO, d1.JOB_ID
      from dwh_foundation.STG_S4S_SCHED_XCPTN_EMP_DY_CPY d1
      where d1.sys_source_batch_id = g_SYS_SOURCE_BATCH_ID
      minus
     select /*+ full(f1) parallel(g1,6) */ distinct 
           f1.EMPLOYEE_ID, f1.EXCEPTION_TYPE_ID, f1.EXCEPTION_DATE, f1.LOCATION_NO, f1.JOB_ID
     from dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DY f1
     where exists 
            (select distinct g1.EMPLOYEE_ID, g1.EXCEPTION_TYPE_ID, g1.EXCEPTION_DATE, g1.LOCATION_NO, g1.JOB_ID
             from dwh_foundation.STG_S4S_SCHED_XCPTN_EMP_DY_CPY g1
             where g1.sys_source_batch_id = g_SYS_SOURCE_BATCH_ID and g1.EMPLOYEE_ID = f1.EMPLOYEE_ID 
             and g1.EXCEPTION_TYPE_ID = f1.EXCEPTION_TYPE_ID
             and g1.EXCEPTION_DATE = f1.EXCEPTION_DATE and g1.LOCATION_NO = f1.LOCATION_NO and g1.JOB_ID = f1.JOB_ID
             )
    )
      select s.*,'','',trunc(sysdate)
      from selq s;
        
        g_recs := sql%rowcount;
        
        commit;
        
        g_recs_inserted := g_recs;
        l_text := 'Inserted recs ='||g_recs_inserted;
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

end fnd_insert;
--******---------------------------------------------------------------------------------------------------------*****--
--       Update values per batch 
--******---------------------------------------------------------------------------------------------------------*****--
procedure fnd_update as
begin
      g_recs := 0;
      update dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DY b1
      set last_updated_date = trunc(sysdate)
      where exists (select distinct d1.EMPLOYEE_ID, d1.EXCEPTION_TYPE_ID, d1.EXCEPTION_DATE, d1.LOCATION_NO, d1.JOB_ID
              from dwh_foundation.STG_S4S_SCHED_XCPTN_EMP_DY_CPY d1
              where d1.EMPLOYEE_ID = d1.EMPLOYEE_ID and d1.EXCEPTION_TYPE_ID = b1.EXCEPTION_TYPE_ID
              and d1.EXCEPTION_DATE = b1.EXCEPTION_DATE and d1.LOCATION_NO = b1.LOCATION_NO and d1.JOB_ID = b1.JOB_ID);
      
      g_recs := sql%rowcount;     
      
      commit;  
      
      g_recs_updated := g_recs;
      l_text := 'updated recs ='||g_recs_updated;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      
end fnd_update;
--******---------------------------------------------------------------------------------------------------------*****--
-- Insert into hospital
--******---------------------------------------------------------------------------------------------------------*****--
procedure fnd_hospital as
begin
  g_recs := 0;
  INSERT /*+ append */ into dwh_foundation.STG_S4S_SCHED_XCPTN_EMP_DY_HSP 
        with selstg as 
                              ( SELECT STG.SYS_SOURCE_BATCH_ID,
                                        STG.SYS_SOURCE_SEQUENCE_NO,
                                        STG.SYS_LOAD_DATE,
                                        STG.SYS_PROCESS_CODE,
                                        STG.SYS_LOAD_SYSTEM_NAME,
                                        STG.SYS_MIDDLEWARE_BATCH_ID,
                                        STG.SOURCE_DATA_STATUS_CODE,
                                        SYS_PROCESS_MSG,
                                        STG.LOCATION_NO LOCATION_NO,
                                        STG.EXCEPTION_TYPE_ID EXCEPTION_TYPE_ID,
                                        STG.JOB_ID JOB_ID,
                                        STG.EMPLOYEE_ID EMPLOYEE_ID,
                                        STG.EXCEPTION_DATE ,
                                        STG.EXCEPTION_START_TIME,
                                        STG.EXCEPTION_END_TIME,
                                        DE.EMPLOYEE_ID DE_EMPLOYEE_ID,
                                        DJ.JOB_ID DJ_JOB_ID,
                                        FE.EXCEPTION_TYPE_ID FE_EXCEPTION_ID,
                                        fl.LOCATION_NO fl_LOCATION_NO,
                                        dc.calendar_date dc_exception_date
                         FROM DWH_FOUNDATION.STG_S4S_SCHED_XCPTN_EMP_DY_CPY stg,
                                  fnd_location fl,
                                  FND_S4S_EXCEPTION_TYPE fe,
                                  dim_calendar dc,
                                  DWH_HR_PERFORMANCE.DIM_EMPLOYEE DE,
                                  DIM_JOB DJ
                                WHERE stg.SYS_SOURCE_BATCH_ID = g_SYS_SOURCE_BATCH_ID
                                and  stg.location_NO      = fl.location_no(+)
                                AND   stg.exception_type_id = fe.exception_type_id(+)
                                and STG.EXCEPTION_DATE = dc.calendar_date(+)
                                and STG.EMPLOYEE_ID = DE.EMPLOYEE_ID(+)
                                and STG.JOB_ID = DJ.JOB_ID(+)                            
                             )
      select 
              SYS_SOURCE_BATCH_ID ,
              SYS_SOURCE_SEQUENCE_NO ,
              sysdate SYS_LOAD_DATE ,
              'Y' SYS_PROCESS_CODE ,
              'DWH' SYS_LOAD_SYSTEM_NAME ,
              SYS_MIDDLEWARE_BATCH_ID ,
              'error with loc/excpt_id/excpt_date/start_end_times' SYS_PROCESS_MSG ,
              SOURCE_DATA_STATUS_CODE ,
              EMPLOYEE_ID,
              EXCEPTION_TYPE_ID,
              EXCEPTION_DATE,
              LOCATION_NO,
              JOB_ID,
              EXCEPTION_START_TIME,
              EXCEPTION_END_TIME
        from selstg
        where FL_LOCATION_NO is null
        or FE_EXCEPTION_ID  is null
        or DE_EMPLOYEE_ID  is null
        or DJ_JOB_ID  is null
        or EXCEPTION_DATE  is null
        or dc_exception_date is null
        or (EXCEPTION_END_TIME IS NOT NULL AND  EXCEPTION_END_TIME < EXCEPTION_START_TIME)
        ;
      
      g_recs :=SQL%ROWCOUNT;
   commit;
      g_recs_hospital := g_recs;  
      l_text := 'hospital recs ='||g_recs_hospital;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  
end fnd_hospital;
--******---------------------------------------------------------------------------------------------------------*****--
-- Delete
--******---------------------------------------------------------------------------------------------------------*****--
procedure delete_fnd as
begin 
     l_text := 'DELETE_FND started';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text); 
      
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','FND_S4S_SCHED_XCPTN_EMP_DEL', DEGREE => 8);

      select max(run_seq_no)+1 into g_run_seq_no
      from dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DEL;
       
      If g_run_seq_no is null
      then 
         g_run_seq_no := 1;
      end if;
      
      g_run_date := trunc(sysdate);
      
    begin

--      coding below commented out due to decision made 15 april 2015 that deletes are not based upon location_no and exception_date
        insert /*+ append */ into dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DEL
        select /*+ full(f) parallel(f,6) */ distinct g_run_date, g_date, g_run_seq_no, f.*
        from DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_EMP_DY F
        where  trunc(f.exception_date) > G_DATE;
                
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        g_recs_inserted := g_recs;

     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','FND_S4S_SCHED_XCPTN_EMP_DEL', DEGREE => 8);
                
     l_text := 'Insert into FND_S4S_SCHED_XCPTN_EMP_DEL recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

     delete from DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_EMP_DY b
     where exists   (select distinct employee_id, trunc(exception_date) 
                      from dwh_foundation.FND_S4S_SCHED_XCPTN_EMP_DEL a
                       where run_seq_no = g_run_seq_no
                       and a.employee_id = B.employee_id
                       and trunc(exception_date) = trunc(B.exception_date));
     
      g_recs :=SQL%ROWCOUNT ;
      COMMIT;
      g_recs_deleted := g_recs;
                
      l_text := 'Deleted from FND_S4S_SCHED_XCPTN_EMP_DY recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      
    exception
         when no_data_found then
                l_text := 'No deletions done for DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_EMP_DY ';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    end;          

    g_recs_inserted  :=0;
    g_recs_deleted := 0;    
   
     exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end delete_fnd;
------------------------------------------------------------------------------------------------------------------------
--MAIN
------------------------------------------------------------------------------------------------------------------------
BEGIN
     for v_cur in (
                select  sys_source_batch_id,sys_load_date, MIN(TRUNC(EXCEPTION_DATE)) MINEXCPTDATE
                from DWH_FOUNDATION.STG_S4S_SCHED_XCPTN_EMP_DY_CPY
                --where sys_source_batch_id = 1273 
                GROUP BY sys_source_batch_id,sys_load_date
                order by sys_source_batch_id
                )
	  loop
        g_SYS_SOURCE_BATCH_ID := v_cur.SYS_SOURCE_BATCH_ID;
        g_sys_load_date := v_cur.sys_load_date;
        g_minexcptdate := v_cur.minexcptdate;
        l_text := '------------------------------------|-------------------------------------';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
        select batch_date 
        into g_date
        from awx_job_status 
        where table_name = 'STG_S4S_SCHED_XCPTN_EMP_DY' 
        and sys_source_batch_id = g_SYS_SOURCE_BATCH_ID;
   
        l_text := ' Batch_id being processed is: '|| g_sys_source_batch_id;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text := ' Batch is loaded on the date: '|| g_sys_load_date;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text := ' Batch being processed was sent on the date: '|| g_date;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text := ' Exceptions for batch '||g_sys_source_batch_id||' begin on the: '|| g_minexcptdate;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text := '------------------------------------|-------------------------------------';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
        delete_fnd;
        fnd_hospital;
        fnd_update;
        fnd_insert;
        
 end loop;
    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCHED_XCPTN_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--***********----------------------------------------------------------------------------------------------------********
-- Write final log data
--**********-----------------------------------------------------------------------------------------------------*******
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
  
END WH_FND_S4S_065U;
