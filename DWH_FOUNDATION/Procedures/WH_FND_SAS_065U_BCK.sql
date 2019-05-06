--------------------------------------------------------
--  DDL for Procedure WH_FND_SAS_065U_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_SAS_065U_BCK" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  PLEASE NOTE THE FOLLOWING IF DOING A HISTORY RELOAD :
-----------------------------------------------------------
--   1.	Current live BI process will run using the new logic of batch_date for deletes but will not load anything for the BI Sunday night batch
--      until Raph’s fix has been released to PRD - THIS SHOULD BE DURING THE WEEK OF 20TH APRIL 2015
--   2.	We will clear and reload all of the data on the staging table in the following manner :
--      Each batch will be processed one at a time but using the min(exception_date) as the delete date.
--      Any batch where the min(exception_date) is a Sunday will be excluded.
--         NB> but when live the batch_date will be used instead of MIN)(exception_date)
--      The 2 batches with incorrect values, ie. 107, 108 will be resent today – but only for the 12th and 13th April .
--      Until this is sent, the data will be reprocessed up until and including batch_id=106.
--   3.	A resend procedure will be created with comments in it. The same comments will also be added to the current live proc to enable any future reloads.

--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Update schedule exception EMPLOYEE day information for Scheduling for Staff(S4S)
--
--  Tables:      AIT load - STG_S4S_SCHED_XCPTN_EMP_DY 
--               Input    - STG_S4S_SCHED_XCPTN_EMP_DY_cpy
--               Output   - FND_S4S_SCHED_XCPTN_EMP_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  w lyttle 24 feb 2015 - SRS from Teddy
--                        a.	S4S SP: usp_Stg_Outbound_Exception_Schedule_Emp 
--                        b.	Trans Type:  SCRSEE
--                        c.	BI Interface: 
--                        d.	BI Logic:
--                            i.	Take-On: Clear all records and load as per payload received from source
--                            ii.	Normal run: 
--                            i.e more than 1 batch a day: Clear records greater than getdate() and  load payload received from source
--                            i.e. where batch is loaded on Sunday or a Monday: Clear records greater than getdate() and load payload received from source
--                            Insert data based on primary key and update if the record already exists.
--
--  w lyttle 16 apr 2015 - change delete to reflect where excpetion_date >= batch_date(ie. g_date) regardless of employee
--                       - Note that until the extract via AIT has been fixed, we will NOT be loading any data
--                               where the batch_date is a Sunday Night.

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_sys_source_batch_id NUMBER  := 0;
g_sys_load_date      date;
g_hospital_text      DWH_FOUNDATION.STG_S4S_SCHED_XCPTN_EMP_DY_hsp.sys_process_msg%type;
g_rec_out            DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_EMP_DY%rowtype;

g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_065U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE schedule exception employee day  data ex S4S';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Delete from Foundation
  --                ie. for each sys_source_batch, we delete from foundation all records for that employee
  --                     where FND.trunc(shift_clock_in) >= STG.trunc(shift_clock_in)
  --                     this is to accomodate changes to the schedule which can include dropping a day for an employee
--**************************************************************************************************
procedure delete_fnd as
begin 
 
      g_recs_inserted := 0;

 --     l_text := 'truncate table dwh_foundation.FND_S4S_SCHED_XCPTN_emp_DEL';
 --     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text); 
      
 --     execute immediate('truncate table dwh_foundation.FND_S4S_SCHED_XCPTN_emp_DEL');
      
      DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_SCHED_XCPTN_emp_DEL', DEGREE => 8);

      select max(run_seq_no)+1 into g_run_seq_no
      from dwh_foundation.FND_S4S_SCHED_XCPTN_emp_DEL;
       
      If g_run_seq_no is null
     then 
         g_run_seq_no := 1;
      end if;
      
      g_run_date := trunc(sysdate);
      
begin


--
--
-- coding below commented out due to decision made 15 april 2015 that deletes are not based upon location_no and exception_date
--
     insert /*+ append */ into dwh_foundation.FND_S4S_SCHED_XCPTN_emp_DEL
    
     select /*+ full(f) parallel(f,6) */ distinct g_run_date
             , g_date
             , g_run_seq_no
             , f.*
     from DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_emp_DY F
     where  trunc(f.exception_date) >= g_date
                ;
                
--     insert /*+ append */ into dwh_foundation.FND_S4S_SCHED_XCPTN_emp_DEL
--    
--     with selstg
--            as (select /*+ full(a) parallel(a,6) */ 
--           distinct employee_id, trunc(exception_date) exception_date
--            from STG_S4S_SCHED_XCPTN_emp_DY_cpy a 
--            where sys_source_batch_id = g_sys_source_batch_id)
--     select /*+ full(f) parallel(f,6) */ distinct g_run_date
--             , g_date
---             , g_run_seq_no
--             , f.*
--     from DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_emp_DY F, SELSTG S
--     where f.employee_id = s.employee_id
 --    and trunc(f.exception_date) >= s.exception_date
 --               ;
                
     g_recs :=SQL%ROWCOUNT ;
     COMMIT;
     g_recs_inserted := g_recs;

     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_SCHED_XCPTN_emp_DEL', DEGREE => 8);
                
     l_text := 'Insert into FND_S4S_SCHED_XCPTN_emp_DEL recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

     delete from DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_emp_DY b
     where exists   (select distinct employee_id, trunc(exception_date) 
                      from dwh_foundation.FND_S4S_SCHED_XCPTN_emp_DEL a
                       where run_seq_no = g_run_seq_no
                       and a.employee_id = B.employee_id
                       and trunc(exception_date) = trunc(B.exception_date));
     
      g_recs :=SQL%ROWCOUNT ;
      COMMIT;
      g_recs_deleted := g_recs;
                
      l_text := 'Deleted from FND_S4S_SCHED_XCPTN_emp_DY recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      
    exception
         when no_data_found then
                l_text := 'No deletions done for DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_emp_DY ';
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
  l_text := 'LOAD Schedule Location Exceptions data ex S4S STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- preparation for processing
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);

-- hardcoding batch_date for testing
-- g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
------------------------------------------------------------------------------------
-- while waiting for code fix in AIT, we must noyt process any sunday_night batch_dates
-------------------------------------------------------------------------------------------
  if TO_CHAR(G_DATE,'DY') = 'SUN'
  THEN 
     l_text := 'SUNDAY NIGHT BATCH NOT BEING PROCESSED';
   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  ELSE
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';

  --**************************************************************************************************
  -- The rule is that for an location_no, Exception, Exception date
  -- We will receive a set of data in one sys_source_batch_id
  -- Any further updates will be sent per sys_source_batch_id
  -- ie. we don't need to check sys_source_sequence_no
  --
  -- Primary key on fnd = LOCATION_NO,EXCEPTION_TYPE_ID,EXCEPTION_DATE
  --**************************************************************************************************
  
  for v_cur in (
	select distinct sys_source_batch_id, sys_load_date
	from DWH_FOUNDATION.STG_S4S_SCHED_XCPTN_EMP_DY_CPY
--  where sys_source_batch_id >= 68
	order by sys_source_batch_id)
	loop
   
   g_SYS_SOURCE_BATCH_ID := v_cur.SYS_SOURCE_BATCH_ID;
   g_sys_load_date := v_cur.sys_load_date;
   l_text := '------------------------------------|-------------------------------------';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   l_text := ' g_sys_source_batch_id =  '|| g_sys_source_batch_id;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    l_text := ' g_sys_load_date =  '|| g_sys_load_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   delete_fnd;
   DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','FND_S4S_SCHED_XCPTN_EMP_DY', DEGREE => 8);
 
   g_recs_inserted := null;
   g_recs := null;
 --------------------------------  
      insert /*+ append */ into DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_EMP_DY
               select /*+ full(sa) parallel(sa,6) full(stg) parallel(stg,6) */ distinct 
							STG.EMPLOYEE_ID,
              STG.EXCEPTION_TYPE_ID,
              STG.EXCEPTION_DATE,
              STG.LOCATION_NO,
              STG.JOB_ID,
              STG.EXCEPTION_START_TIME,
              STG.EXCEPTION_END_TIME,
              G_DATE
					     from DWH_FOUNDATION.STG_S4S_SCHED_XCPTN_EMP_DY_cpy stg,
							  dwh_performance.dim_calendar f,
							  dwh_performance.dim_exception_type e,
							  dwh_HR_performance.dim_EMPLOYEE De,
							  dwh_performance.DIM_JOB DJ,
							  dwh_foundation.fnd_location d
						where stg.SYS_SOURCE_BATCH_ID = g_SYS_SOURCE_BATCH_ID
            and stg.LOCATION_NO = d.LOCATION_NO
						and stg.EXCEPTION_TYPE_ID  = e.EXCEPTION_TYPE_ID
						and stg.EMPLOYEE_ID  = De.EMPLOYEE_ID
						and stg.JOB_ID  = DJ.JOB_ID
						and stg.EXCEPTION_DATE = f.calendar_date
                     ;
  
   g_recs :=SQL%ROWCOUNT ;
   COMMIT;
   g_recs_inserted := g_recs;
                      
   l_text := 'recs inserted ='||g_recs_inserted;
   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCHED_XCPTN_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','FND_S4S_SCHED_XCPTN_EMP_DY', DEGREE => 8);
 --**************************************************************************************************
  -- Insert into hospital
--**************************************************************************************************
  INSERT /*+ append */ into dwh_foundation.STG_S4S_SCHED_XCPTN_EMP_DY_HSP 
        with selstg as 
                              ( SELECT DISTINCT STG.SYS_SOURCE_BATCH_ID,
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
              DISTINCT SYS_SOURCE_BATCH_ID ,
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
        or (EXCEPTION_END_TIME IS NOT NULL AND  EXCEPTION_END_TIME < EXCEPTION_START_TIME);
        
      g_recs_hospital := 0;  
      g_recs_hospital := g_recs_hospital + sql%rowcount; 
      l_text := 'hospital recs ='||g_recs_hospital;
   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text); 
 
 end loop;
    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCHED_XCPTN_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   
END IF;	  
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


END WH_FND_SAS_065U_BCK;
