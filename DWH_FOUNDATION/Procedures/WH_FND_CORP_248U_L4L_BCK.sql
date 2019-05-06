--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_248U_L4L_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_248U_L4L_BCK" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- LIKE-4-LIKE rerun of data from beginning fin_year=2013
  -- WENDY LYTTLE SEPTEMBER 2013
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
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
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
  g_ly_calendar_date DATE;
  v_like_for_like_ind     NUMBER(1);
  v_like_for_like_adj_ind NUMBER(1);
  g_sub                   NUMBER := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_248U_L4L_BCK';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD LIKE FOR LIKE TRANSACTION EX FINANCE SPREADSHEET';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For input bulk collect --
type stg_array
IS
  TABLE OF dwh_datafix.TMP_2013_L4L%rowtype;
  a_stg_input stg_array;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
IS
  TABLE OF dwh_datafix.tmp_fnd_rtl_loc_dy_l4l%rowtype INDEX BY binary_integer;
type tbl_array_u
IS
  TABLE OF dwh_datafix.tmp_fnd_rtl_loc_dy_l4l%rowtype INDEX BY binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count     INTEGER := 0;
  a_count_i   INTEGER := 0;
  a_count_u   INTEGER := 0;
  a_count_stg INTEGER := 0;
  
  CURSOR c_stg_excel_like_4_like
  IS
    SELECT a.LOCATION_NO
      --,a.LOCATION_NAME
      ,
      a.fin_year_no c_fin_year_no ,
      a.fin_week_no c_fin_week_no ,
      c.fin_day_no c_fin_day_no ,
      c.calendar_date c_calendar_date ,
      c.ly_calendar_date c_ly_calendar_date ,
      c.ly_fin_year_no c_ly_fin_year_no ,
      c.ly_fin_week_no c_ly_fin_week_no ,
      a.LIKE_FOR_LIKE_IND c_like_for_like_ind ,
      C.ly_calendar_date g_ly_calendar_date,
      C.fin_year_no g_fin_year_no,
      C.fin_week_no g_fin_week_no,
      C.fin_day_no g_fin_day_no,
      C.ly_fin_year_no g_ly_fin_year_no,
      C.ly_fin_week_no g_ly_fin_week_no
    FROM
      --dwh_foundation.STG_EXCEL_LIKE_4_LIKE_arc a,
      dwh_datafix.TMP_2013_L4L a,
      dim_calendar c
    WHERE c.fin_year_no = a.fin_year_no
    AND c.fin_week_no   = a.fin_week_no
    AND c.fin_year_no   = 2013
      --    and C.FIN_WEEK_NO = 1
    AND location_no IN (3043)
--        AND location_no IN (3043,230,103,105)
    ORDER BY a.LOCATION_NO ,
      a.FIN_YEAR_NO ,
      a.FIN_WEEK_NO ,
      c.FIN_DAY_NO;
  -- testing location_no = 3043 - fin_year_no = 2013 - 0 to 1
  -- testing location_no = 230 - fin_year_no = 2012 - 1 to 0
  -- testing location_no = 103 - fin_year_no = 2013 - 1
  -- testing location_no = 105 - fin_year_no = 2013 - 0 to 1
  g_rec_in c_stg_excel_like_4_like%rowtype;
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
  EXECUTE immediate ('truncate table dwh_datafix.tmp_fnd_rtl_loc_dy_l4l');
  COMMIT;
  l_text := 'TRUNCATED table dwh_datafix.tmp_fnd_rtl_loc_dy_l4l';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  insert into dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
select * from fnd_rtl_loc_dy_like_4_like
where location_no in( 3043,230,103,105);
g_recs_read := SQL%ROWCOUNT;
commit;
  l_text := 'Load table dwh_datafix.tmp_fnd_rtl_loc_dy_l4l - recs='||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
g_recs_read := 0;
  
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
 
  --**************************************************************************************************
  -- M A I N   S E C T I O N
  --**************************************************************************************************
  OPEN c_stg_excel_like_4_like;
  LOOP
    FETCH c_stg_excel_like_4_like INTO g_rec_in;
    EXIT
  WHEN c_stg_excel_like_4_like%notfound;
  
   g_date := g_rec_in.c_calendar_date - 7;
  --  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  IF g_rec_in.g_ly_calendar_date IS NULL THEN
    SELECT MIN(ly_calendar_date) INTO g_ly_calendar_date FROM DIM_CALENDAR;
    l_text := 'LY BATCH DATE BEING PROCESSED not found , defaulted to - '||g_ly_calendar_date;
  ELSE
    l_text := 'LY BATCH DATE BEING PROCESSED  - '||g_ly_calendar_date;
  END IF;
  
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  -------------------------------------------------------------------------------
  --  DESIGN :
  --
  --> At the start of each Fin Year (Fin Week 1 Fin Day 1)
  --              the values in the L4L Adj column
  --              for matching Fin Week and Fin Day of the "Last Year",
  --              for the entire ("Last Year") year must be set to zero ("0").
  -------------------------------------------------------------------------------
  IF g_fin_week_no = 1 AND g_fin_day_no = 1 THEN
          l_text          := 'if1a';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    UPDATE dwh_datafix.tmp_fnd_rtl_loc_dy_l4l rld
    SET rld.like_for_like_adj_ind = 0,
      last_updated_date           = g_date
    WHERE rld.calendar_date      IN
      (SELECT dc2.calendar_date
      FROM dim_calendar dc2
      WHERE dc2.ly_fin_year_no = g_ly_fin_year_no
      );
    g_recs_zeroised := SQL%ROWCOUNT;
    l_text          := 'LY fin_year zeroised at beginning of year : RECORDS UPDATED = '||g_recs_zeroised;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    COMMIT;
  END IF;
  
  
    g_recs_read := g_recs_read+ 1;
    -------------------------------------------------------------------------------
    --  DESIGN :
    --
    --> If loading data for the current Fin Year
    --       then populate both columns
    --       and the L4L Adj column
    --          for the same Fin Week, Fin Day "Last Year"
    -------------------------------------------------------------------------------
    IF g_rec_in.c_fin_year_no  = g_fin_year_no THEN
        l_text          := 'if1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      v_like_for_like_ind     := g_rec_in.c_like_for_like_ind;
      v_like_for_like_adj_ind := g_rec_in.c_like_for_like_ind;
      BEGIN
        UPDATE dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
        SET like_for_like_adj_ind = g_rec_in.c_like_for_like_ind,
          last_updated_date       = g_date
        WHERE calendar_date       = g_rec_in.c_ly_calendar_date
        AND location_no           = g_rec_in.location_no;
        COMMIT;
        g_recs_updated := g_recs_updated+ 1;
      EXCEPTION
      WHEN no_data_found THEN
        l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||sqlerrm;
        dwh_log.record_error(l_module_name,SQLCODE,l_message);
      END;
    ELSE
      -------------------------------------------------------------------------------
      --  DESIGN :
      --
      --> If loading data for a previous Fin Year
      --     update only the L4L column
      --            for that date
      --      and also the L4L Adj column
      --             for the same Fin Week, Fin Day "Last Year"
      --> For historical records, updates only no inserts for L4L Adj process
      -------------------------------------------------------------------------------
              l_text          := 'if2';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      IF g_rec_in.c_fin_year_no < g_fin_year_no THEN
              l_text          := 'if3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        v_like_for_like_ind    := g_rec_in.c_like_for_like_ind;
        BEGIN
          UPDATE dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
          SET like_for_like_adj_ind = g_rec_in.c_like_for_like_ind,
            last_updated_date       = g_date
          WHERE calendar_date       = g_rec_in.c_ly_calendar_date
          AND location_no           = g_rec_in.location_no;
          COMMIT;
          g_recs_updated := g_recs_updated+ 1;
        EXCEPTION
        WHEN no_data_found THEN
          l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||sqlerrm;
          dwh_log.record_error(l_module_name,SQLCODE,l_message);
        END;
      END IF;
    END IF;
    --**************************************************************************************************
    -- Check to see if item is present on table
    --      and update/insert accordingly Fin Week, Fin Day "This Year"
    --**************************************************************************************************
    g_count := NULL;
    SELECT COUNT(1)
    INTO g_count
    FROM dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
    WHERE location_no = g_rec_in.location_no
    AND calendar_date = g_rec_in.c_calendar_date;
            l_text          := 'if4';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    IF g_count        = 1 THEN
            l_text          := 'if5';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      UPDATE dwh_datafix.tmp_fnd_rtl_loc_dy_l4l
      SET like_for_like_ind   = v_like_for_like_ind,
        like_for_like_adj_ind =
        CASE
          WHEN g_rec_in.c_fin_year_no < g_fin_year_no
          THEN g_rec_in.c_like_for_like_ind
          ELSE like_for_like_adj_ind
        END,
        last_updated_date = g_date
      WHERE location_no   = g_rec_in.location_no
      AND calendar_date   = g_rec_in.c_calendar_date;
      COMMIT;
      g_recs_updated := g_recs_updated+ 1;
    ELSE
            l_text          := 'if6';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      IF g_rec_in.c_fin_year_no = g_fin_year_no THEN
              l_text          := 'if7';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        INSERT
        INTO dwh_datafix.tmp_fnd_rtl_loc_dy_l4l VALUES
          (
            g_rec_in.LOCATION_NO ,
            g_rec_in.c_CALENDAR_DATE ,
            g_rec_in.c_like_for_like_ind ,
            '' ,
            g_date ,
            g_rec_in.c_like_for_like_ind
          );
        COMMIT;
        g_recs_inserted := g_recs_inserted + 1;
      END IF;
    END IF;
    --    dbms_output.put_line(g_recs_read||' '||g_recs_inserted||' '||g_recs_updated||' '||g_recs_hospital);
    EXIT
  WHEN c_stg_excel_like_4_like%notfound;
  END LOOP;
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

END WH_FND_CORP_248U_L4L_BCK;
