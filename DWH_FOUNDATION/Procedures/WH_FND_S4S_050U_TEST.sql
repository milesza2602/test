--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_050U_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_050U_TEST" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  Date:        July 2014
  --  Author:      Wendy lyttle
  --  Purpose:     LOAD Employee job schedule information for Scheduling for Staff(S4S)
--
--               Delete process :
--                 Due to changes which can be made, we have to drop the current data and load the new data
--                        based upon employee_id and trunc(shift_clock_in)
--
--                The delete lists are used in the rollups as well.
--                ie. FND_S4S_SCHLOCEMPJBDY_del_list
--
--
  --  Tables:      AIT load - STG_S4S_EMP_LOC_JOB_SHED
  --               Input    - STG_S4S_EMP_LOC_JOB_SHED_CPY
  --               Output   - FND_S4S_SCH_LOC_EMP_JB_DY
  --  Packages:    dwh_constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  20 feb 2015 - w lyttle - change to allow for rolling forward
  --                ie. for each sys_source_batch, we delete from foundation all records for that employee
  --                     where FND.trunc(shift_clock_in) >= STG.trunc(shift_clock_in)
  --                     this is to accomodate changes to the schedule which can include dropping a day for an employee
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
          g_forall_limit        INTEGER := 10000;
          g_recs_read           INTEGER := 0;
          g_recs_updated        INTEGER := 0;
          g_recs_inserted       INTEGER := 0;
          g_recs_hospital       INTEGER := 0;
          g_error_count         NUMBER  := 0;
          g_error_index         NUMBER  := 0;
          g_hospital            CHAR(1) := 'N';
          g_sys_source_batch_id NUMBER  := 0;
          g_found        BOOLEAN;
          g_valid        BOOLEAN;
          g_date         DATE    := TRUNC(sysdate);
          g_run_date     DATE    := TRUNC(sysdate);
          g_run_seq_no   NUMBER  := 0;
          g_recs         NUMBER  := 0;
          g_recs_deleted INTEGER := 0;
          
          g_hospital_text DWH_FOUNDATION.STG_S4S_EMP_LOC_JOB_SHED_hsp.sys_process_msg%type;
          g_rec_out DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY%rowtype;

          l_message sys_dwh_errlog.log_text%type;
          l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_S4S_050U';
          l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
          l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
          l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
          l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
          l_text sys_dwh_log.log_text%type ;
          l_description sys_dwh_log_summary.log_description%type   := 'LOAD Employee job schedule data ex S4S';
          l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Delete from Foundation
  --                ie. for each sys_source_batch, we delete from foundation all records for that employee
  --                     where FND.trunc(shift_clock_in) >= STG.trunc(shift_clock_in)
  --                     this is to accomodate changes to the schedule which can include dropping a day for an employee
--**************************************************************************************************
procedure delete_fnd as
begin 
 
      g_recs_inserted := 0;

      l_text := 'truncate table dwh_foundation.FND_S4S_SCHLOCEMPJBDY_del_list';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text); 
      
      execute immediate('truncate table dwh_foundation.FND_S4S_SCHLOCEMPJBDY_del_list');
      
      DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_SCHLOCEMPJBDY_DEL_LIST', DEGREE => 8);

      select max(run_seq_no)+1 into g_run_seq_no
      from dwh_foundation.FND_S4S_SCHLOCEMPJBDY_del_list;
       
      If g_run_seq_no is null
     then 
         g_run_seq_no := 1;
      end if;
      
      g_run_date := trunc(sysdate);
      
begin
     insert /*+ append */ into dwh_foundation.FND_S4S_SCHLOCEMPJBDY_del_list
    
     with selstg
            as (select /*+ full(a) parallel(a,6) */ 
            distinct employee_id, trunc(SHIFT_CLOCK_IN) SHIFT_CLOCK_IN 
            from STG_S4S_EMP_LOC_JOB_SHED_cpy a 
            where sys_source_batch_id = g_sys_source_batch_id)
     select /*+ full(f) parallel(f,6) */ g_run_date
             , g_date
             , g_run_seq_no
             , f.*
     from DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY f, selstg s
     where f.employee_id = s.employee_id
     and trunc(f.SHIFT_CLOCK_IN) >= s.SHIFT_CLOCK_IN
                ;
                
     g_recs :=SQL%ROWCOUNT ;
     COMMIT;
     g_recs_inserted := g_recs;

     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_SCHLOCEMPJBDY_DEL_LIST', DEGREE => 8);
                
     l_text := 'Insert into FND_S4S_SCHLOCEMPJBDY_del_list recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

     delete from DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY b
     where exists   (select distinct employee_id, trunc(SHIFT_CLOCK_IN) 
                      from dwh_foundation.FND_S4S_SCHLOCEMPJBDY_del_list a
                       where run_seq_no = g_run_seq_no
                       and a.employee_id = b.employee_id
                       and trunc(A.SHIFT_CLOCK_IN) = trunc(B.shift_clock_in));
     
      g_recs :=SQL%ROWCOUNT ;
      COMMIT;
      g_recs_deleted := g_recs;
                
      l_text := 'Deleted from FND_S4S_SCH_LOC_EMP_JB_DY recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      
    exception
         when no_data_found then
                l_text := 'No deletions done for DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY ';
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

--**************************************************************************************************
-- Main process
--**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > 1000 THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD Employee job schedule data ex S4S STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- preparation for processing
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);

-- hardcoding batch_date for testing
--g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  --**************************************************************************************************
  -- The rule is that for an employee, location_no, employee_id, job_id, trunc(shift_clockin)
  --  we will receive a set of data in one sys_source_batch_id
  --  Any further updates will be sent per sys_source_batch_id
  --  ie. we don't need to check sys_source_sequence_no
  --
  -- Primary key on fnd = LOCATION_NO,EMPLOYEE_ID,JOB_ID,SHIFT_CLOCK_IN
  --**************************************************************************************************
for v_cur in (
select distinct sys_source_batch_id
from DWH_FOUNDATION.STG_S4S_EMP_LOC_JOB_SHED_cpy
where sys_source_batch_id = 9999991000
order by sys_source_batch_id)
loop
 
   
   g_SYS_SOURCE_BATCH_ID := v_cur.SYS_SOURCE_BATCH_ID;
   l_text := '------------------------------------------------------------------------';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   l_text := ' g_sys_source_batch_id =  '|| g_sys_source_batch_id;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
--   delete_fnd;
   DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_SCH_LOC_EMP_JB_DY', DEGREE => 8);
 
   g_recs_inserted := null;
   g_recs := null;
  
   insert /*+ append */
   into DWH_FOUNDATION.FND_S4S_SCH_LOC_EMP_JB_DY
                           SELECT /*+ full(sa) parallel(sa,6) full(stg) parallel(stg,6) */
                           distinct 
                                  stg.LOCATION_NO
                                ,stg.EMPLOYEE_ID
                                ,stg.JOB_ID
                                ,stg.SHIFT_CLOCK_IN
                                ,stg.SHIFT_CLOCK_OUT
                                ,stg.MEAL_BREAK_MINUTES
                                ,stg.TEA_BREAK_MINUTES
                                ,g_date LAST_UPDATED_DATE
                          FROM DWH_FOUNDATION.STG_S4S_EMP_LOC_JOB_SHED_cpy stg,
                               fnd_location fl,
                               DWH_HR_PERFORMANCE.DIM_EMPLOYEE fe,
                               fnd_S4S_job fJ
                          
                          WHERE stg.SYS_SOURCE_BATCH_ID = g_SYS_SOURCE_BATCH_ID
                          and  stg.location_NO      = fl.location_no
                          AND stg.EMPLOYEE_ID        = fe.employee_id
                          AND stg.JOB_ID             = fj.JOB_ID
                          and stg.SHIFT_CLOCK_IN is not null
                          and stg.SHIFT_CLOCK_OUT  is not null
                   ;
  
   g_recs :=SQL%ROWCOUNT ;
   COMMIT;
   g_recs_inserted := g_recs;
                      
   l_text := 'recs inserted ='||g_recs_inserted;
   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCH_LOC_EMP_JB_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_SCH_LOC_EMP_JB_DY', DEGREE => 8);
  
 
 end loop;
    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCH_LOC_EMP_JB_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Insert into hospital
  --**************************************************************************************************
  INSERT /*+ append */ 
  into dwh_foundation.STG_S4S_EMP_LOC_JOB_SHED_hsp 
        with selstg as 
                              ( SELECT STG.SYS_SOURCE_BATCH_ID,
                                        STG.SYS_SOURCE_SEQUENCE_NO,
                                        STG.SYS_LOAD_DATE,
                                        STG.SYS_PROCESS_CODE,
                                        STG.SYS_LOAD_SYSTEM_NAME,
                                        STG.SYS_MIDDLEWARE_BATCH_ID,
                                        STG.SOURCE_DATA_STATUS_CODE,
                                        SYS_PROCESS_MSG,
                                        STG.LOCATION_no stg_LOCATION_NO,
                                        STG.EMPLOYEE_ID stg_EMPLOYEE_ID,
                                        STG.JOB_ID stg_JOB_ID,
                                        STG.SHIFT_CLOCK_IN stg_SHIFT_CLOCK_IN,
                                        STG.SHIFT_CLOCK_OUT stg_SHIFT_CLOCK_OUT,
                                        STG.MEAL_BREAK_MINUTES stg_MEAL_BREAK_MINUTES,
                                        STG.TEA_BREAK_MINUTES stg_TEA_BREAK_MINUTES,
                                        fl.LOCATION_NO fl_LOCATION_NO,
                                        fe.EMPloyee_id fe_EMPLOYEE_ID,
                                        fJ.JOB_ID fJ_JOB_ID
                                FROM DWH_FOUNDATION.STG_S4S_EMP_LOC_JOB_SHED_cpy stg,
                                  fnd_location fl,
                                  DWH_HR_PERFORMANCE.DIM_EMPLOYEE fe,
                                  fnd_S4S_job fJ
                                WHERE stg.location_NO      = fl.location_no(+)
                                AND stg.EMPLOYEE_ID        = fe.employee_id(+)
                                AND stg.JOB_ID             = fj.JOB_ID(+)
                                and sys_source_batch_id = 9999991000
                             )
      select 
              SYS_SOURCE_BATCH_ID ,
              SYS_SOURCE_SEQUENCE_NO ,
              sysdate SYS_LOAD_DATE ,
              'Y' SYS_PROCESS_CODE ,
              'DWH' SYS_LOAD_SYSTEM_NAME ,
              SYS_MIDDLEWARE_BATCH_ID ,
              'error with loc/job/employee/ shift_clock_in/out' SYS_PROCESS_MSG ,
              SOURCE_DATA_STATUS_CODE ,
              stg_EMPLOYEE_ID ,
              stg_LOCATION_NO ,
              stg_JOB_ID ,
              stg_SHIFT_CLOCK_IN ,
              stg_SHIFT_CLOCK_OUT,
              stg_MEAL_BREAK_MINUTES ,
              stg_TEA_BREAK_MINUTES 
        from selstg
        where fl_LOCATION_NO is null
        or fl_LOCATION_NO  is null
        or fe_EMPLOYEE_ID  is null
        or fJ_JOB_ID  is null
        or STG_SHIFT_CLOCK_IN IS NULL 
        or STG_SHIFT_CLOCK_OUT IS NULL;
        
      g_recs_hospital := g_recs_hospital + sql%rowcount; 
   
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
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


END WH_FND_S4S_050U_TEST;
