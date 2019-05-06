--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_248U_UPD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_248U_UPD" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- UPDATE PRD FND TABLE for a store for this fin_year, last_year
   --**************************************************************************************************
  --  Date:         october 2012
  --  Author:       W Lyttle
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --  testing
  --**************************************************************************************************
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  G_Recs_Inserted INTEGER := 0;
  g_recs_created  INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_deleted  INTEGER := 0;
  g_recs_dlet_cnt INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  G_Count         NUMBER  := 0;
  G_Cnt           NUMBER  := 0;
  G_Recs_Cnt      NUMBER  := 0;
  G_Recs_Calc1    NUMBER  := 0;
  G_Recs_Calc2    NUMBER  := 0;
  g_recs_calc3    NUMBER  := 0;
  g_found         BOOLEAN;
  g_param_ind     NUMBER := 0;
  g_TY_START_DATE        DATE;
  g_TY_END_DATE        DATE;
  g_LY_START_DATE        DATE;
  g_LY_END_DATE        DATE;  
  g_max_fin_week_no  NUMBER :=0;
  G_LOC_NO NUMBER :=0;
  G_LIKE_FOR_LIKE_IND NUMBER :=0;
  g_date DATE            := TRUNC(sysdate);
  g_rec_out rtl_po_supchain_loc_item_dy%rowtype;
  g_fill_fd_po_grn_qty          NUMBER := 0;
  g_fill_fd_latest_po_qty       NUMBER := 0;
  g_fill_fd_po_grn_qty_import   NUMBER := 0;
  g_fill_fd_latest_po_qty_imprt NUMBER := 0;
  g_fill_fd_po_grn_qty_local    NUMBER := 0;
  g_fill_fd_latest_po_qty_local NUMBER := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_248U_UPD';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_apps;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_apps;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  L_Text Sys_Dwh_Log.Log_Text%Type ;
  l_description sys_dwh_log_summary.log_description%type   := 'DATAFIX FOR CLEARANCE DATA';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For output arrays into bulk load forall statements --
  a_count   INTEGER := 0;
  a_count_i INTEGER := 0;
  a_count_u INTEGER := 0;
  --**************************************************************************************************
  -- Main process loop
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  End If;
  
  P_Success := False;
 
   --qc4799 - loc=404 and set to non-comparable
 
--FIN_YR 2013 
-- 175 -  1 FOR  1-23, 0 FOR 24-26
--260 -  1 FOR 1-22, 0 FOR 23-26
--362 -  1 FOR  1-23, 0 FOR 24-26
--562 -  1 FOR 1-23, 0 FOR 24-26
 
  g_loc_no := 465;
  g_like_for_like_ind := 0;
  
  --
  -- Get This_year dates
  select min(calendar_date), max(calendar_date), max(fin_week_no)
  into g_ty_start_date, g_ty_end_date, g_max_fin_week_no
  from dim_calendar
  where fin_year_no = 2013
  and calendar_date <= (select max(calendar_date) from fnd_rtl_loc_dy_like_4_like);
 
  --******NOTE  !!!!! years with 53 weeks *****
  --
  -- Get Last_year dates
  --
  select min(this_week_start_date), max(this_week_end_date)
  into 
  g_ly_start_date, g_ly_end_date
  from dim_calendar
  where fin_year_no = 2012
  and fin_week_no between 1 and g_max_fin_week_no;
    
  --
  -- Update FND table
  --
  g_recs_updated := 0;
  update fnd_rtl_loc_dy_like_4_like fnd
  set fnd.like_for_like_ind = g_like_for_like_ind,
  fnd.like_for_like_adj_ind = g_like_for_like_ind
  where ((fnd.calendar_date between g_ly_start_date and g_ly_end_date)
  or (fnd.calendar_date between g_ty_start_date and g_ty_end_date))
  and fnd.location_no = g_loc_no;
  
  g_recs_updated := sql%rowcount;
  
  commit;
  
  L_Text         := 'update fnd_rtl_loc_dy_like_4_like for loc_no='||g_loc_no||' ind='||g_like_for_like_ind||' recs='||g_recs_updated;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);

  --
  -- Update PRFtable
  --
  g_recs_updated := 0;
  update rtl_loc_dy fnd
  set fnd.like_for_like_ind = g_like_for_like_ind,
  fnd.like_for_like_adj_ind = g_like_for_like_ind
  where ((fnd.post_date between g_ly_start_date and g_ly_end_date)
  or (fnd.post_date between g_ty_start_date and g_ty_end_date))
  and fnd.sk1_location_no = (select dl.sk1_location_no from dim_location dl where dl.location_no = g_loc_no);
  
  g_recs_updated := sql%rowcount;
  
  commit;
  
  L_Text         := 'update rtl_loc_dy for loc_no='||g_loc_no||' ind='||g_like_for_like_ind||' recs='||g_recs_updated;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
 
  
  
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,g_recs_deleted,'');
l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_read||g_recs_read;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_deleted||g_recs_deleted;
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
  Raise;

END WH_FND_CORP_248U_UPD;
