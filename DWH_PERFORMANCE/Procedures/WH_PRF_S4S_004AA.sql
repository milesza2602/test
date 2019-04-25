--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004AA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004AA" (
   p_forall_limit   IN     INTEGER,
   p_success           OUT BOOLEAN)
AS
----------------------------------------------------------------------------------------
   --  Date:        Aug 2016
   --  Author:      Wendy lyttle
   --  Purpose:     Determine employee range split for processing
   --
   --  Tables:      Input    - dwh_foundation.FND_S4S_emp_avail_DY
   --               Output   - dwh_PERFORMANCE.TEMP_S4S_EMP_SPLIT
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
   g_forall_limit     INTEGER := dwh_constants.vc_forall_limit;
   g_recs_read        INTEGER := 0;
   g_recs_inserted    INTEGER := 0;
   g_recs_updated     INTEGER := 0;
   g_recs             INTEGER := 0;
   g_recs_tbc         INTEGER := 0;
   g_error_count      NUMBER := 0;
   g_error_index      NUMBER := 0;
   g_count            NUMBER := 0;
   g_rec_out          RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE;
   g_found            BOOLEAN;
   g_date             DATE;
   g_SUB              NUMBER := 0;
   g_end_date         DATE;
   g_run_period_end_date date;
   g_start_date       date;
   g_end_sub          number;
   g_NAME             VARCHAR2(40);


      l_message sys_dwh_errlog.log_text%TYPE;
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004AA';
      l_name sys_dwh_log.log_name%TYPE                     := dwh_constants.vc_log_name_rtl_md;
      l_system_name sys_dwh_log.log_system_name%TYPE       := dwh_constants.vc_log_system_name_rtl_prf;
      l_script_name sys_dwh_log.log_script_name%TYPE       := dwh_constants.vc_log_script_rtl_prf_md;
      l_procedure_name sys_dwh_log.log_procedure_name%TYPE := l_module_name;
      l_text sys_dwh_log.log_text%TYPE;
      l_description sys_dwh_log_summary.log_description%TYPE   := 'LOAD TEMP_S4S_EMP_SPLIT';
      l_process_type sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;
      -- For output arrays into bulk load forall statements --
      TYPE tbl_array_i
      IS
        TABLE OF RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE INDEX BY BINARY_INTEGER;
      TYPE tbl_array_u
      IS
        TABLE OF RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE INDEX BY BINARY_INTEGER;
        a_tbl_insert tbl_array_i;
        a_tbl_update tbl_array_u;
        a_empty_set_i tbl_array_i;
        a_empty_set_u tbl_array_u;
        a_count   INTEGER := 0;
        a_count_i INTEGER := 0;
        a_count_u INTEGER := 0;
 
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
    l_text := 'LOAD OF TEMP_S4S_EMP_SPLIT STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select distinct this_week_start_date + 20
        into g_end_date
   from dim_calendar where calendar_date = g_date;
   g_run_period_end_date := G_END_DATE;

    l_text := 'END DATE BEING PROCESSED IS:- '||g_END_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Start Processing
--************************************************************************************************** 
     l_text := '------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
                  g_recs_inserted := 0;
                  g_recs_updated := 0;
                  G_RECS := 0;
        
     l_text := 'TRUNCATE TABLE  dwh_performance.TEMP_S4S_EMP_SPLIT';
     DWh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
     EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.TEMP_S4S_EMP_SPLIT');
     
     l_text := '------------------------------------------------';
     dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

/*'1002140'
,'1002158'
,'1002194'
,'1002236'
,'1002238'
,'1002247'
,'1002258'
,'1002289'
,'1002355'
,'1002421'
,'1002436'
,'1002440'
,'1002446'
*/
insert /*= append */ into dwh_performance.TEMP_S4S_EMP_SPLIT
with 
          selext as(
                    select /*+ full(fnd) parallel(fnd,6) */ distinct employee_id 
                    from dwh_foundation.FND_S4S_emp_avail_DY fnd
                             , dim_calendar dc                             
                    where calendar_date between FND.CYCLE_START_DATE AND g_end_date 
                    and employee_id in ('1002140'
,'1002158'
,'1002194'
,'1002236'
,'1002238'
,'1002247'
,'1002258'
,'1002289'
,'1002355'
,'1002421'
,'1002436'
,'1002440'
,'1002446'
)
                    --35158 - 3mins
                    )
                    ,
           selrnk as(
                    select  employee_id,
                    row_number() OVER (order BY employee_id ) RANK
                    from selext 
                    )
                    ,
           selmax as(
                    select  count(*) maxrank
                    from selext ),
          sel1 as (
                    select  1 rnk_start_1, trunc(maxrank/3) rnk_end_1,
                            min(employee_id) emp_start_1, max(employee_id) emp_end_1
                    from selrnk sr, selmax sm
                    where rank = 1 or rank = trunc(maxrank / 3)
                    group by 1 , trunc(maxrank/3)
                    ),
          sel2 as (
                    select  trunc((maxrank/3)+1) rnk_start_2, trunc((maxrank/3)*2) rnk_end_2,
                            min(employee_id) emp_start_2, max(employee_id) emp_end_2
                    from selrnk sr, selmax sm
                    where rank = trunc((maxrank/3)+1) or rank = trunc((maxrank/3)*2)
                    group by trunc((maxrank/3)+1) , trunc((maxrank/3)*2)
                    ),
          sel3 as (
                    select  trunc(((maxrank/3)*2)+1) rnk_start_3, maxrank rnk_end_3,
                            min(employee_id) emp_start_3, max(employee_id) emp_end_3
                    from selrnk sr, selmax sm
                    where rank = trunc(((maxrank/3)*2)+1) or rank = maxrank
                    group by trunc(((maxrank/3)*2)+1), maxrank
                    )
select *
from sel1 se1,
sel2 se2,
sel3 se3;
commit;
    
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
      dwh_log.update_log_summary (l_name, l_system_name, l_script_name, l_procedure_name, l_description, l_process_type,
      dwh_constants.vc_log_ended, g_recs_read, g_recs_inserted, g_recs_updated, '','');
      l_text := dwh_constants.vc_log_time_completed || TO_CHAR (SYSDATE, ('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_records_read || g_recs_read;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_records_inserted || g_recs_inserted;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_records_updated || g_recs_updated;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_run_completed || SYSDATE;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := dwh_constants.vc_log_draw_line;
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
      l_text := ' ';
      dwh_log.write_log (l_name, l_system_name, l_script_name, l_procedure_name, l_text);
   COMMIT;
   p_success := TRUE;
EXCEPTION
   WHEN dwh_errors.e_insert_error
   THEN
      l_message := dwh_constants.vc_err_mm_insert || SQLCODE || ' ' || SQLERRM;
      dwh_log.record_error (l_module_name, SQLCODE, l_message);
      dwh_log.update_log_summary (l_name, l_system_name, l_script_name, l_procedure_name, l_description, l_process_type, dwh_constants.vc_log_aborted, '', '', '', '', '');
      ROLLBACK;
      p_success := FALSE;
      RAISE;
WHEN OTHERS THEN
      l_message := dwh_constants.vc_err_mm_other || SQLCODE || ' ' || SQLERRM;
      dwh_log.record_error (l_module_name, SQLCODE, l_message);
      dwh_log.update_log_summary (l_name, l_system_name, l_script_name, l_procedure_name, l_description, l_process_type, dwh_constants.vc_log_aborted, '', '', '', '', '');
      ROLLBACK;
      p_success := FALSE;
      RAISE;


END WH_PRF_S4S_004AA;
