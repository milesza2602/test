--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_136U_BCK_4616
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_136U_BCK_4616" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- qc4616 - cbackups for datafix- active_date
  --
  --**************************************************************************************************
  --  Date:         February 2012
  --  Author:       W Lyttle
  --  9 Feb 2012 - qc4616 - wendy lyttle
  --               active-date for clerance changed to 6 march 2012
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
  g_date DATE            := TRUNC(sysdate);
  g_rec_out rtl_po_supchain_loc_item_dy%rowtype;
  g_fill_fd_po_grn_qty          NUMBER := 0;
  g_fill_fd_latest_po_qty       NUMBER := 0;
  g_fill_fd_po_grn_qty_import   NUMBER := 0;
  g_fill_fd_latest_po_qty_imprt NUMBER := 0;
  g_fill_fd_po_grn_qty_local    NUMBER := 0;
  g_fill_fd_latest_po_qty_local NUMBER := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_136U_FIX_4616';
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
  
  l_text    := dwh_constants.vc_log_draw_line;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
  l_text := 'Datafix for Clearance Data  STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
 
--   Execute Immediate('Create Table DWH_FOUNDATION.Fnd_Rtl_Clearance_4616 As Select * From Dwh_FOUNDATION.FND_Rtl_Clearance Where ITEM_NO < 0');
--  Commit;
--    Execute Immediate('Create Table DWH_FOUNDATION.stg_rms_rtl_clearance_arc_4616 As Select * From Dwh_FOUNDATION.STG_rms_rtl_clearance_arc Where ITEM_NO < 0');
--  Commit;
--    Execute Immediate('Create Table DWH_FOUNDATION.stg_rms_rtl_clearance_CPY_4616 As Select * From Dwh_FOUNDATION.STG_rms_rtl_clearance_CPY Where ITEM_NO < 0');
--  Commit;
--      Execute Immediate('Create Table DWH_FOUNDATION.stg_rms_rtl_clearance_HSP_4616 As Select * From Dwh_FOUNDATION.STG_rms_rtl_clearance_HSP Where ITEM_NO < 0');
--  Commit;
--    Execute Immediate('Create Table DWH_FOUNDATION.stg_rms_rtl_clearance_4616 As Select * From Dwh_FOUNDATION.STG_rms_rtl_clearance Where ITEM_NO < 0');
--  COMMIT;
 


   Execute Immediate('Truncate Table DWH_FOUNDATION.Fnd_Rtl_Clearance_4616');
  Commit;
    L_Text         := 'Truncated - DWH_FOUNDATION.Fnd_Rtl_Clearance_4616';
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
    Execute Immediate('Truncate Table DWH_FOUNDATION.stg_rms_rtl_clearance_ARC_4616');
  Commit;
      L_Text         := 'Truncated - DWH_FOUNDATION.stg_rms_rtl_clearance_ARC_4616';
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);

    Execute Immediate('Truncate Table DWH_FOUNDATION.stg_rms_rtl_clearance_HSP_4616');
  Commit;
      L_Text         := 'Truncated - DWH_FOUNDATION.stg_rms_rtl_clearance_HSP_4616';
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
    Execute Immediate('Truncate Table DWH_FOUNDATION.stg_rms_rtl_clearance_CPY_4616');
  Commit;
      L_Text         := 'Truncated - DWH_FOUNDATION.stg_rms_rtl_clearance_CPY_4616';
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
      Execute Immediate('Truncate Table DWH_FOUNDATION.stg_rms_rtl_clearance_4616');
  Commit;
      L_Text         := 'Truncated - DWH_FOUNDATION.stg_rms_rtl_clearance_4616';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   COMMIT;
  
  
  Insert Into Dwh_FOUNDATION.Fnd_Rtl_Clearance_4616 
  Select * From Dwh_FOUNDATION.FND_Rtl_Clearance Where Active_Date = '06/MAR/2012';
  COMMIT;
  g_cnt := 0;
  SELECT COUNT(*) INTO G_Cnt FROM DWH_FOUNDATION.Fnd_Rtl_Clearance_4616;
  G_Recs_Created := G_Cnt ;
  L_Text         := 'DWH_FOUNDATION.Fnd_Rtl_Clearance_4616         - No. of recs backed-up = '|| G_Recs_Created;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
  
  G_Recs_Updated := 0;
  Insert Into Dwh_FOUNDATION.Stg_Rms_Rtl_Clearance_ARC_4616 
  Select * From Dwh_FOUNDATION.STG_rms_rtl_clearance_arc Where Active_Date = '06/MAR/2012';
  COMMIT;
  g_cnt := 0;
  SELECT COUNT(*) INTO G_Cnt FROM DWH_FOUNDATION.stg_rms_rtl_clearance_arc_4616;
  G_Recs_Created := G_Cnt ;
  L_Text         := 'DWH_FOUNDATION.stg_rms_rtl_clearance_ARC_4616 - No. of recs backed-up = '|| G_Recs_Created;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
 
  
  G_Recs_Updated := 0;
  Insert Into Dwh_FOUNDATION.Stg_Rms_Rtl_Clearance_Cpy_4616 
  Select * From Dwh_FOUNDATION.STG_rms_rtl_clearance_CPY Where Active_Date = '06/MAR/2012';
  COMMIT;
  g_cnt := 0;
  SELECT COUNT(*) INTO G_Cnt FROM DWH_FOUNDATION.stg_rms_rtl_clearance_CPY_4616;
  G_Recs_Created := G_Cnt ;
  L_Text         := 'DWH_FOUNDATION.stg_rms_rtl_clearance_CPY_4616 - No. of recs backed-up = '|| G_Recs_Created;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
  
  G_Recs_Updated := 0;
  Insert Into Dwh_FOUNDATION.Stg_Rms_Rtl_Clearance_Hsp_4616 
  Select * From Dwh_FOUNDATION.STG_rms_rtl_clearance_HSP Where Active_Date = '06/MAR/2012';
  COMMIT;
  g_cnt := 0;
  SELECT COUNT(*) INTO G_Cnt FROM DWH_FOUNDATION.stg_rms_rtl_clearance_HSP_4616;
  G_Recs_Created := G_Cnt ;
  L_Text         := 'DWH_FOUNDATION.stg_rms_rtl_clearance_HSP_4616 - No. of recs backed-up = '|| G_Recs_Created;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
  
  G_Recs_Updated := 0;
  Insert Into Dwh_FOUNDATION.Stg_Rms_Rtl_Clearance_4616 
  Select * From Dwh_FOUNDATION.STG_rms_rtl_clearance Where Active_Date = '06/MAR/2012';
  COMMIT;
  g_cnt := 0;
  SELECT COUNT(*) INTO G_Cnt FROM DWH_FOUNDATION.stg_rms_rtl_clearance_4616;
  G_Recs_Created := G_Cnt ;
  L_Text         := 'DWH_FOUNDATION.stg_rms_rtl_clearance_4616     - No. of recs backed-up = '|| G_Recs_Created;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
--     Execute Immediate('Delete from  DWH_FOUNDATION.Fnd_Rtl_Clearance where Active_Date = ''06/Mar/2012'' and sys_load_date = ''11/Feb/2012''');
--  Commit;
--    L_Text         := 'Deleted from  - DWH_FOUNDATION.Fnd_Rtl_Clearance';
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
--  
--    Execute Immediate('Delete from  DWH_FOUNDATION.stg_rms_rtl_clearance_ARC  where Active_Date = ''06/Mar/2012'' and sys_load_date = ''11/Feb/2012''');
--  Commit;
--      L_Text         := 'Deleted from - DWH_FOUNDATION.stg_rms_rtl_clearance_ARC';
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);--

--    Execute Immediate('Delete from  DWH_FOUNDATION.stg_rms_rtl_clearance_HSP where Active_Date = ''06/Mar/2012'' and sys_load_date = ''11/Feb/2012''');
--  Commit;
--      L_Text         := 'Deleted from - DWH_FOUNDATION.stg_rms_rtl_clearance_HSP';
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
--    Execute Immediate('Delete from  DWH_FOUNDATION.stg_rms_rtl_clearance_CPY where Active_Date = ''06/Mar/2012'' and sys_load_date = ''11/Feb/2012''');
--  Commit;
--      L_Text         := 'Deleted from - DWH_FOUNDATION.stg_rms_rtl_clearance_CPY';
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  
--      Execute Immediate('Delete from  DWH_FOUNDATION.stg_rms_rtl_clearance where Active_Date = ''06/Mar/2012'' and sys_load_date = ''11/Feb/2012''');
--  Commit;
--      L_Text         := 'Deleted from - DWH_FOUNDATION.stg_rms_rtl_clearance';
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
--   COMMIT;
  
  
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

END WH_FND_CORP_136U_BCK_4616;
