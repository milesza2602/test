--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_248U_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_248U_BCK" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- LIKE-4-LIKE rerun of data from beginning fin_year=2013
  -- WENDY LYTTLE SEPTEMBER 2013
  -- testing location_no = 3043 - fin_year_no = 2013 - 0 to 1
  -- testing location_no = 230 - fin_year_no = 2012 - 1 to 0
  -- testing location_no = 103 - fin_year_no = 2013 - 1
  -- testing location_no = 105 - fin_year_no = 2013 - 0 to 1
  --**************************************************************************************************
  --  Date:        July 2009
  --  Author:      Wendy Lyttle
  --  Purpose:     Load like4like ind table in the foundation layer
  --               with input ex staging table from an Excel SS ex finance.
  --  Tables:      Input  - dwh_datafix.TMP_2013_L4L
  --               Output - dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  08 July 2009 - defect 2017 - Add field LIKE_FOR_LIKE_ADJ_IND to tables
  --                               dwh_datafix.tmp_fnd_rtl_loc_dy_l4l and RTL_LOC_DY
  --  14 August 2009 - defect 2252 - Ensure that check for valid location_no is
  --                                 done in FND and not PRF for Like4Like
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
  g_recs_updated1  INTEGER := 0;
  g_recs_updated2  INTEGER := 0;
    g_recs_updated3  INTEGER := 0;
      g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_inserted2 INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_recs_zeroised INTEGER := 0;
  g_count         NUMBER  := 0;
  g_hospital      CHAR(1) := 'N';
  g_hospital_text stg_excel_like_4_like_hsp.sys_process_msg%type;
  g_found BOOLEAN;
  g_date DATE          := TRUNC(sysdate);
  g_fin_year_no    NUMBER := 0;
  g_fin_week_no    NUMBER := 0;
  g_fin_day_no     NUMBER := 0;
  g_ly_fin_year_no NUMBER := 0;
  g_ly_fin_week_no NUMBER := 0;
  g_week1 NUMBER := 0;
  g_week2 NUMBER := 0;
  g_ly_calendar_date DATE;
  v_like_for_like_ind     NUMBER(1);
  v_like_for_like_adj_ind NUMBER(1);
  g_sub                   NUMBER := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_248U_BCK';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD LIKE FOR LIKE TRANSACTION EX FINANCE SPREADSHEET';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For input bulk collect --

  --**************************************************************************************************
  --                           M A I N     P R O C E S S
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > 1000 THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF dwh_datafix.tmp_fnd_rtl_loc_dy_l4l EX POS STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- SETUP TABLES FOR PROCESSING
  --**************************************************************************************************
 
 
    FOR V_CUR IN
    (SELECT /*+ parallel (a,4) */ LOCATION_NO
,A.CALENDAR_DATE
,LIKE_FOR_LIKE_IND
,A.SOURCE_DATA_STATUS_CODE
,A.LAST_UPDATED_DATE
,LIKE_FOR_LIKE_ADJ_IND
, LY_CALENDAR_DATE LY_CALENDAR_DATE
, FIN_WEEK_NO
, LY_FIN_WEEK_NO
    FROM
      dwh_datafix.tmp_fnd_rtl_loc_dy_l4l a,
      dim_calendar c
    WHERE c.CALENDAR_DATE = A.CALENDAR_DATE
    AND c.fin_year_no   = 2013
    AND FIN_WEEK_NO = 1
    )
    LOOP

       update  dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
       set    like_for_like_ind            = v_CUR.like_for_like_ind
       where  location_no                = v_cur.location_no
         and  calendar_date              = v_cur.LY_CALENDAR_DATE;
         commit;
        g_recs_updated  := g_recs_updated + 1;

--    dbms_output.put_line(g_recs_read||' '||g_recs_inserted||' '||g_recs_updated||' '||g_recs_hospital);
 end loop;


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
END WH_FND_CORP_248U_bck;
