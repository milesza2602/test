--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_043U_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_043U_TAKEON" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load ABSENCE_EMPLOYEE_WEEK information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - RTL_ABSENCE_EMP_DY
--               Output   - RTL_ABSENCE_EMP_WK
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            RTL_ABSENCE_EMP_WK%rowtype;
g_found              boolean;
g_date               date;

g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_043U_TAKEON';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ABSENCE_EMPLOYEE_WEEK data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF ABSENCE_EMPLOYEE_WEEK  EX DAILY STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);


-- hardcoding batch_date for testing
--   g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
 --**************************************************************************************************
-- Delete records from Performance
-- based on employee_id and absence_start_date
-- before loading from staging
--**************************************************************************************************

      g_recs_inserted := 0;

      select max(run_seq_no) into g_run_seq_no
      from dwh_foundation.FND_S4S_ABSNCE_EMP_DY_DEL_LIST;
      
      If g_run_seq_no is null
      then g_run_seq_no := 1;
      end if;
      g_run_date := trunc(sysdate);

    BEGIN
           delete from DWH_PERFORMANCE.RTL_ABSENCE_EMP_wk
           where (SK1_employee_id, FIN_YEAR_NO, FIN_WEEK_NO) in (select distinct SK1_employee_id, FIN_YEAR_NO, FIN_WEEK_NO from DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY RTL,
           DIM_CALENDAR DC
           WHERE rtl.last_updated_date = g_date
           AND RTL.absence_date = DC.CALENDAR_DATE);
       
            g_recs :=SQL%ROWCOUNT ;
            COMMIT;
            g_recs_deleted := g_recs;
                  
        l_text := 'Deleted from DWH_PERFORMANCE.RTL_ABSENCE_EMP_WK recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
    exception
           when no_data_found then
                  l_text := 'No deletions done for DWH_PERFORMANCE.RTL_ABSENCE_EMP_WK ';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
     end;          

   g_recs_inserted  :=0;
   g_recs_deleted := 0;
  --*** ADD CONSTRAINT_DATE - NEED EXTRA GEN RECS INBETWEEN
-- might need to remove CONSTRAINT_DATE, no of weeks from table

 INSERT /*+ APPEND */
INTO DWH_PERFORMANCE.RTL_ABSENCE_EMP_WK
    SELECT
      /*+ FULL(JD) PARALEL(JD,4) */
             SK1_EMPLOYEE_ID
              ,FIN_YEAR_NO
              ,FIN_WEEK_NO
              ,sk1_ABSENCE_TYPE_id
               ,sk1_LEAVE_TYPE_ID
              ,sum(nvl(ABSENCE_HOURS,0))
              ,g_date LAST_UPDATED_DATE
     FROM DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY  JD,
        DIM_CALENDAR DC
    WHERE jd.absence_date         = DC.CALENDAR_DATE
    GROUP BY SK1_EMPLOYEE_ID
              ,FIN_YEAR_NO
              ,FIN_WEEK_NO
              ,sk1_ABSENCE_TYPE_id
               ,sk1_LEAVE_TYPE_ID;

   g_recs_read:=SQL%ROWCOUNT;
   g_recs_inserted:=SQL%ROWCOUNT;
   commit;



    l_text := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_ABSENCE_EMP_WK', DEGREE => 8);



--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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


END WH_PRF_S4S_043U_TAKEON;
