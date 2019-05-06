--------------------------------------------------------
--  DDL for Procedure WH_PRF_DIM_CALENDAR_ROLL_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_DIM_CALENDAR_ROLL_LOAD" (p_forall_limit IN INTEGER,p_success OUT BOOLEAN,P_end_date in date)
AS
--**************************************************************************************************
--  Date:        march 2009
--  Author:      Wendy Lyttle
--  Purpose:     Create a DIM_CALENDAR table loaded with
--               rolling 24ths of date data to be used for
--               various day and week sets of data
--  Tables:      Input  - dim_calendar, dim_control
--               Output - dim_calendar_roll
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
  g_forall_limit  INTEGER := 10000;
  g_recs_read     INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_recs_integ    INTEGER := 0;
  g_recs_rejected INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_cnt           NUMBER  := 0;
  g_found BOOLEAN;
  g_valid BOOLEAN;
  g_date DATE := TRUNC(sysdate);
  g_date_min_15days  date;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_DIM_CALENDAR_ROLL_LOAD';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD OF DIM_CALENDAR_ROLL';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > 100 THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF DIM_CALENDAR_ROLL ex DIM_CALENDAR STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  --**************************************************************************************************
  -- LOOK UP BATCH DATE FROM DIM_CONTROL
  -- DWH_LOG.WRITE_LOG   : g_end_date
  --**************************************************************************************************
    if p_end_date is null then
       dwh_lookup.dim_control(g_date);
   else
       g_date := p_end_date;
   end if;
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- Clear STG_PROD_HIER_HDCDE
  --**************************************************************************************************
  dbms_output.put_line(l_text);
  g_date_min_15days := g_date-15;
   SELECT COUNT(*) INTO G_CNT
   FROM DBA_TABLES
   WHERE
   OWNER = 'DWH_PERFORMANCE'
   and TABLE_NAME = 'DIM_CALENDAR_ROLL';
   IF g_CNT = 1
   THEN
    dbms_output.put_line('DELETE FROM dwh_performance.dim_calendar_roll');
        EXECUTE IMMEDIATE ('DELETE FROM dwh_performance.dim_calendar_roll');
        COMMIT;
   dbms_output.put_line('INSERT INTO dwh_performance.dim_calendar_roll
                            select dc.*
                            from dwh_performance.dim_calendar dc
                            where dc.calendar_date <= '''||G_DATE||
                            ''' and dc.calendar_date  >=  trunc(ADD_MONTHS('''||G_DATE||''',-12))');
        EXECUTE IMMEDIATE ('INSERT INTO dwh_performance.dim_calendar_roll
                            select dc.*
                            from dwh_performance.dim_calendar dc
                            where dc.calendar_date <= '''||G_DATE||
                            ''' and dc.calendar_date  >=  trunc(ADD_MONTHS('''||G_DATE||''',-12))');
        COMMIT;
  ELSE
  dbms_output.put_line('create table dwh_performance.dim_calendar_roll
                      as
                      select dc.*
                      from dwh_performance.dim_calendar dc
                      where dc.calendar_date <= TO_DATE('||G_DATE||
                      ''' AND dc.calendar_date  >=  trunc(ADD_MONTHS('''||G_DATE||''',-12))');
  EXECUTE IMMEDIATE ('create table dwh_performance.dim_calendar_roll
                      as
                      select dc.*
                      from dwh_performance.dim_calendar dc
                      where dc.calendar_date <= TO_DATE('||G_DATE||
                      ''' AND dc.calendar_date  >=  trunc(ADD_MONTHS('''||G_DATE||''',-12))');
                      COMMIT;
  END IF;
 dbms_output.put_line('insert into dwh_performance.dim_calendar_roll
                      (select dc.*
                      from dwh_performance.dim_calendar dc, dwh_performance.dim_calendar_roll dct
                      where dc.calendar_date = dct.ly_calendar_date)');
  EXECUTE IMMEDIATE ('insert into dwh_performance.dim_calendar_roll
                      (select dc.*
                      from dwh_performance.dim_calendar dc, dwh_performance.dim_calendar_roll dct
                      where dc.calendar_date = dct.ly_calendar_date)');
  COMMIT;
  EXECUTE IMMEDIATE ('update dwh_performance.dim_calendar_roll
                         set order_by_seq_no = 0');
  commit;
  --
  -- order_by_seq_no = 1 = calendar_days to be used at daily level
  --
  DBMS_OUTPUT.PUT_LINE('update dwh_performance.dim_calendar_roll
                       set order_by_seq_no = 1
                       where  calendar_date >=  (select min(calendar_date)
                                                 from dwh_performance.dim_calendar_roll dc
                                                 where dc.calendar_date > '''||g_date_min_15days||'''
                                                 and dc.calendar_date <= '''||G_DATE||''' and dc.fin_day_no = 1)');
  EXECUTE IMMEDIATE ('update dwh_performance.dim_calendar_roll
                       set order_by_seq_no = 1
                       where  calendar_date >=  (select min(calendar_date)
                                                 from dwh_performance.dim_calendar_roll dc
                                                 where dc.calendar_date > '''||g_date_min_15days||'''
                                                 and dc.calendar_date <= '''||G_DATE||''' and dc.fin_day_no = 1)');
  commit;
  DBMS_OUTPUT.PUT_LINE('update dwh_performance.dim_calendar_roll
                       set order_by_seq_no = 1
                       where  calendar_date in (select ly_calendar_date
                                                 from dwh_performance.dim_calendar_roll
                                                 where order_by_seq_no = 1)');
  EXECUTE IMMEDIATE ('update dwh_performance.dim_calendar_roll
                       set order_by_seq_no = 1
                       where  calendar_date in (select ly_calendar_date
                                                 from dwh_performance.dim_calendar_roll
                                                 where order_by_seq_no = 1)');
  commit;
  select count(*) into g_recs_inserted
  from dwh_performance.dim_calendar_roll;
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
END WH_PRF_DIM_CALENDAR_ROLL_LOAD;
