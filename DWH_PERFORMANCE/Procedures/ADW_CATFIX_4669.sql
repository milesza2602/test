--------------------------------------------------------
--  DDL for Procedure ADW_CATFIX_4669
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."ADW_CATFIX_4669" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  G_Count         Number  := 0;
  G_INS         Number  := 0;
  G_Fin_Day_No         Number  := 0;
  G_Fin_Day_No1         Number  := 0;
  G_Run_Date Date  := '18 march 2012';
  G_Run_Date1 DATE := '18 march 2012';
  g_date DATE      := TRUNC(sysdate);
  G_Yesterday Date := Trunc(Sysdate) - 1;
  
  g_rec_out rtl_loc_item_dy_catalog%rowtype;
  g_found BOOLEAN;

  L_Message Sys_Dwh_Errlog.Log_Text%Type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'ADW_CATFIX_4669';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'FIX CATALOG';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For output arrays into bulk load forall statements --
  
type tbl_array_i
IS
  Table Of Rtl_Loc_Item_Dy_Catalog%Rowtype Index By Binary_Integer;
  
type tbl_array_u
IS
  Table Of Rtl_Loc_Item_Dy_Catalog%Rowtype Index By Binary_Integer;
  
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   INTEGER := 0;
  a_count_i INTEGER := 0;
  A_Count_U Integer := 0;
  
  CURSOR c_catalog
  IS
    SELECT a.SK1_ITEM_NO,
      A.SK1_LOCATION_NO,
      A.CALENDAR_DATE
    FROM rtl_loc_item_dy_catalog a,
      dim_item i,
      dim_location l
    WHERE a.sk1_item_no     = i.sk1_item_no
    AND A.Sk1_Location_No   = L.Sk1_Location_No
    AND calendar_Date       = g_run_date
    AND this_wk_catalog_ind = '1'
    AND NOT EXISTS
      (SELECT *
      FROM stg_om_location_item_arc b
      WHERE i.item_no   = b.item_no
      AND L.Location_No = B.Location_No
      AND sys_load_date = g_run_date1
      );
      
  G_Rec_In C_Catalog%Rowtype;
  
  -- For input bulk collect --
type stg_array
IS
  TABLE OF c_catalog%rowtype;
  A_Stg_Input Stg_Array;
  
  --**************************************************************************************************
  -- Process, transform and validate the data read from the input interface
  --**************************************************************************************************
PROCEDURE local_address_variables
AS
BEGIN
  g_rec_out.calendar_date   := g_rec_in.calendar_date;
  g_rec_out.sk1_item_no     := g_rec_in.sk1_item_no;
  g_rec_out.sk1_location_no := g_rec_in.sk1_location_no;
EXCEPTION
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_av_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
End Local_Address_Variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
PROCEDURE local_bulk_update
AS
BEGIN
  forall i IN a_tbl_update.first .. a_tbl_update.last
  SAVE exceptions
  UPDATE rtl_loc_item_dy_catalog
  SET last_updated_date          = '01 may 1950',
    THIS_WK_CATALOG_IND          = 0,
    NEXT_WK_CATALOG_IND          = 0,
    FD_NUM_AVAIL_DAYS            = 0,
    FD_NUM_AVAIL_DAYS_ADJ        = 0,
    FD_NUM_CATLG_DAYS            = 0,
    FD_NUM_CATLG_DAYS_ADJ        = 0,
    FD_SOD_NUM_AVAIL_DAYS        = 0,
    Fd_Sod_Num_Avail_Days_Adj    = 0,
    MIN_SHELF_LIFE               = NULL,
    WEIGHTED_AVAIL_SALES         = NULL,
    WEIGHTED_AVAIL_SALES_QTY     = NULL,
    WEIGHTED_ADJ_AVAIL_SALES_QTY = NULL,
    WEIGHTED_ADJ_AVAIL_SALES     = NULL
  WHERE sk1_location_no          = a_tbl_update(i).sk1_location_no
  AND sk1_item_no                = a_tbl_update(i).sk1_item_no
  AND calendar_date              = a_tbl_update(i).calendar_date;
  g_recs_updated                := g_recs_updated + a_tbl_update.count;
EXCEPTION
WHEN OTHERS THEN
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_update(g_error_index).sk1_location_no|| ' '||a_tbl_update(g_error_index).sk1_item_no|| ' '||a_tbl_update(g_error_index).calendar_date;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
  END LOOP;
  raise;
End Local_Bulk_Update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
PROCEDURE local_write_output
AS
BEGIN
  a_count_u               := a_count_u + 1;
  a_tbl_update(a_count_u) := g_rec_out;
  a_count                 := a_count + 1;
  --**************************************************************************************************
  -- Bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  IF a_count > g_forall_limit THEN
    local_bulk_update;
    a_tbl_insert := a_empty_set_i;
    a_tbl_update := a_empty_set_u;
    a_count_i    := 0;
    a_count_u    := 0;
    a_count      := 0;
    COMMIT;
  END IF;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_lw_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_lw_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
End Local_Write_Output;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  End If;
  
  Dbms_Output.Put_Line('Bulk write limit '||P_Forall_Limit||' '||G_Forall_Limit);
  
  P_Success := False;
  
  l_text    := dwh_constants.vc_log_draw_line;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
  l_text := 'FIX CATALOG STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
--Update /*+ PARALLEL(a 4) */ Rtl_Loc_Item_Dy_Catalog A
--Set Min_Shelf_Life = Null
--Where Calendar_Date Between '19-Mar-12' And '22-Apr-12'
--AND last_updated_date = '01 may 1950';---
--
--  G_Ins    := Sql%Rowcount;
--  
--    L_Text := 'DATAFIX - MIN_SHELF_LIFE :- '||G_Ins;
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
--  Commit;
--G_INS :=0;
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  L_Text := 'RUN DATES BEING PROCESSED ARE:- '||g_run_date||' - '||g_run_date1;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
  SELECT FIN_DAY_NO
  INTO G_FIN_DAY_NO
  FROM Dim_Calendar
  Where Calendar_Date = G_Run_Date;
  
  SELECT FIN_DAY_NO
  INTO G_FIN_DAY_NO1
  FROM Dim_Calendar
  Where Calendar_Date = G_Run_Date1;
  
  IF G_Fin_Day_No     = 7 AND G_Fin_Day_No1 <> 7 THEN
    L_Text           := 'PROBLEM ****  DATES WRONG FOR SUNDAY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  ELSE
    INSERT INTO Dwh_Performance.Tmp_Loc_Item_Dy_Cat_4669
    SELECT a.*
    FROM rtl_loc_item_dy_catalog a,
      dim_item i,
      dim_location l
    WHERE a.sk1_item_no     = i.sk1_item_no
    AND A.Sk1_Location_No   = L.Sk1_Location_No
    AND calendar_Date       = g_run_date
    AND this_wk_catalog_ind = '1'
    AND NOT EXISTS
      (SELECT *
      FROM stg_om_location_item_arc b
      WHERE i.item_no   = b.item_no
      AND L.Location_No = B.Location_No
      AND Sys_Load_Date = G_Run_Date1
      );
      
    G_Ins    := Sql%Rowcount;
    
    IF G_Ins  < 1 OR G_Ins IS NULL OR G_Ins > 65000 THEN
      L_Text := 'PROBLEM ****  Rows backed_up = '||G_Ins;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    ELSE
      --**************************************************************************************************
      -- Bulk fetch loop controlling main program execution
      --**************************************************************************************************
      Commit;
      L_Text := 'Rows backed_up TO Tmp_Loc_Item_Dy_Cat_4669 = '||G_Ins;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      OPEN c_catalog;
      FETCH c_catalog bulk collect INTO a_stg_input limit g_forall_limit;
      WHILE a_stg_input.count > 0
      LOOP
        FOR i IN 1 .. a_stg_input.count
        LOOP
          g_recs_read              := g_recs_read + 1;
          IF g_recs_read mod 100000 = 0 THEN
            l_text                 := dwh_constants.vc_log_records_processed|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          END IF;
          g_rec_in := a_stg_input(i);
          local_address_variables;
          local_write_output;
        END LOOP;
        FETCH c_catalog bulk collect INTO a_stg_input limit g_forall_limit;
      END LOOP;
      CLOSE c_catalog;
      --**************************************************************************************************
      -- At end write out what remains in the arrays at end of program
      --**************************************************************************************************
      local_bulk_update;
    END IF;
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

END ADW_CATFIX_4669;
