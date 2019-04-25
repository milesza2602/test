--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_065U_XFIX1DC2015
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_065U_XFIX1DC2015" (p_forall_limit in integer,p_success out boolean) as


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

G_MINEXCPTDATE        DATE;
--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_065U_XFIX1DC2015';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE schedule exception employee day  data ex S4S';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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
  
  
 --------------------------------  
      insert /*+ append */ into DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_EMP_DY
                  WITH SELEXT AS
            (SELECT /*+ PARALLEL(A,8) FULL(A) */   EMPLOYEE_ID,EXCEPTION_DATE,EXCEPTION_TYPE_ID,LOCATION_NO,JOB_ID, MAX(SYS_SOURCE_BATCH_ID) MAXBAT
             FROM DWH_FOUNDATION.STG_S4S_SCHED_XCPTN_EMP_DY_CPY A
             GROUP BY EMPLOYEE_ID,EXCEPTION_DATE,EXCEPTION_TYPE_ID,LOCATION_NO,JOB_ID)
               select /*+ full(sa) parallel(sa,6) full(stg) parallel(stg,6) */ distinct 
							STG.EMPLOYEE_ID,
              STG.EXCEPTION_TYPE_ID,
              STG.EXCEPTION_DATE,
              STG.LOCATION_NO,
              STG.JOB_ID,
              STG.EXCEPTION_START_TIME,
              STG.EXCEPTION_END_TIME,
              G_DATE
					     from DWH_FOUNDATION.STG_S4S_SCHED_XCPTN_EMP_DY_cpy stg, selext se,
							  dwh_performance.dim_calendar f,
							  dwh_performance.dim_exception_type e,
							  dwh_HR_performance.dim_EMPLOYEE De,
							  dwh_performance.DIM_JOB DJ,
							  dwh_foundation.fnd_location d
						where  stg.sys_source_batch_id = SE.MAXBAT
  AND stg.Exception_TYPE_ID = SE.Exception_TYPE_ID
  AND stg.Exception_DATE = SE.Exception_DATE
  AND stg.LOCATION_NO = SE.LOCATION_NO
 	and stg.EMPLOYEE_ID  = se.EMPLOYEE_ID
	and stg.JOB_ID  = se.JOB_ID
  --
  and stg.LOCATION_NO = d.LOCATION_NO
	and stg.EXCEPTION_TYPE_ID  = e.EXCEPTION_TYPE_ID
	and stg.EXCEPTION_DATE = f.calendar_date
  and stg.EMPLOYEE_ID  = De.EMPLOYEE_ID
	and stg.JOB_ID  = DJ.JOB_ID
                     ;
  
   g_recs :=SQL%ROWCOUNT ;
   COMMIT;
   g_recs_inserted := g_recs;
                      
   l_text := 'batch='||g_SYS_SOURCE_BATCH_ID||'  **recs inserted ='||g_recs_inserted;
   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);


    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCHED_XCPTN_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','FND_S4S_SCHED_XCPTN_EMP_DY', DEGREE => 8);
   


  --**************************************************************************************************
  -- The rule is that for an location_no, Exception, Exception date
  -- We will receive a set of data in one sys_source_batch_id
  -- Any further updates will be sent per sys_source_batch_id
  -- ie. we don't need to check sys_source_sequence_no
  --
  -- Primary key on fnd = LOCATION_NO,EXCEPTION_TYPE_ID,EXCEPTION_DATE
  --**************************************************************************************************
  
  INSERT /*+ APPEND */ INTO DWH_FOUNDATION.FND_S4S_SCHED_XCPTN_EMP_DY
  WITH SELEXT AS
            (SELECT /*+ PARALLEL(A,8) FULL(A) */ location_no, Exception_TYPE_ID, Exception_date, MAX(SYS_SOURCE_BATCH_ID) MAXBAT
             FROM DWH_FOUNDATION.STG_S4S_SCHED_XCPTN_EMP_DY_CPY A
             GROUP BY location_no, Exception_TYPE_ID, Exception_date)
	select /*+ full(sa) parallel(sa,6) full(stg) parallel(stg,6) */ distinct 
							STG.EMPLOYEE_ID,
              STG.EXCEPTION_TYPE_ID,
              STG.EXCEPTION_DATE,
              STG.LOCATION_NO,
              STG.JOB_ID,
              STG.EXCEPTION_START_TIME,
              STG.EXCEPTION_END_TIME,
              G_DATE
	from DWH_FOUNDATION.STG_S4S_SCHED_XCPTN_EMP_DY_CPY STG, SELEXT SE,
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
  AND STG.sys_source_batch_id = SE.MAXBAT
  AND STG.Exception_TYPE_ID = SE.Exception_TYPE_ID
  AND STG.Exception_DATE = SE.Exception_DATE
  ;
  
   g_recs :=SQL%ROWCOUNT ;
   COMMIT;
   g_recs_inserted := g_recs;
                      
   l_text := 'batch='||g_SYS_SOURCE_BATCH_ID||'  **recs inserted ='||g_recs_inserted;
   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_SCHED_XCPTN_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   DBMS_STATS.gather_table_stats ('DWH_FOUNDATION','FND_S4S_SCHED_XCPTN_EMP_DY', DEGREE => 8);
 
	  
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


END WH_FND_S4S_065U_XFIX1DC2015;
