--------------------------------------------------------
--  DDL for Procedure WH_FND_COMPOSITES_4605
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_COMPOSITES_4605" 
    (P_Forall_Limit IN INTEGER,
    P_Success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  DATAFIX
  --**************************************************************************************************
  --  Date:        March 2012
  --  Author:      Wendy lyttle
  --  Purpose:     Datafix for composites - qc4600 thru to 4621
  --  Tables:      fnd_location_item
  --               fnd_rtl_loc_item_dy_om_st_ord
  --               fnd_zone_item_om  (product_region)
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
  g_recs_reset    INTEGER := 0;
  g_stg_count     INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;

  g_found BOOLEAN;
  G_Valid BOOLEAN;
  G_Date_End DATE;
  G_Start_Date DATE;
  G_End_Date DATE;
  G_Recs_Updated1 INTEGER := 0;
  G_Recs_Updated2 INTEGER := 0;
  G_Recs_Updated3 INTEGER := 0;
  G_sub_end       NUMBER  :=0;
  G_Pack_Type_ind NUMBER  := 0;
  --g_date              date          := to_char(sysdate,('dd mon yyyy'));
  G_Date DATE  := TRUNC(Sysdate);
  G_Cnt Number := 0;
  
  L_Message Sys_Dwh_Errlog.Log_Text%Type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_COMPOSITES_4605';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  L_Text Sys_Dwh_Log.Log_Text%Type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD THE LOCATION_ITEM MASTERDATA EX OM';
  L_Process_Type Sys_Dwh_Log_Summary.Log_Process_Type%Type := Dwh_Constants.Vc_Log_Process_Type_N;
  

  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > 1000 THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  l_text := 'DATAFIX - foundation composites STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  G_Date := trunc(sysdate);
  L_Text := 'Last_updated_date changed to:- '||G_Date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  L_Text := '==================================';
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
--  --**************************************************************************************************
--  -- FND_RTL_LOCATION
--  --**************************************************************************************************
--  UPDATE /*+ PARALLEL(F1 2) */ Dwh_Foundation.Fnd_Location_Item F1
--  SET F1.Num_Units_Per_Tray  = 1,
--    Last_Updated_Date        = '15 mar 2013'
--  WHERE F1.Item_No          IN (6009173634306,6009173468154)
--  And F1.Num_Units_Per_Tray Is Not Null;
--  G_Recs_Updated            := Sql%Rowcount;
--  l_text                    := 'NO OF Fnd_location_item records UPT set to 1 =  '||g_recs_updated;
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
--  L_Text := '==================================';
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
--  --**************************************************************************************************
--  -- FND_ZONE_ITEM_OM
--  --**************************************************************************************************
--  Update /*+ PARALLEL(F1 2) */ Dwh_Foundation.Fnd_Zone_Item_Om F1
--  SET F1.Num_Units_Per_Tray  = 1,
--    Last_Updated_Date        = '15 mar 2013'
--  Where F1.Item_No          In (6009173634306,6009173468154)
--  AND F1.Num_Units_Per_Tray IS NOT NULL;
--  G_Recs_Updated            := Sql%Rowcount;
--  
--  L_Text                    := 'NO OF fnd_zone_item_om records UPT set to 1 =  '||G_Recs_Updated;
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
--  L_Text := '==================================';
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  --**************************************************************************************************
  -- FND_RTL_LOC_ITEM_DY_OM_ST_ORD
  --**************************************************************************************************
--  G_Recs_Updated  := 0;
--  G_Sub_End       := 0;
--  G_End_Date      := Null;
--  G_Start_Date    := '3 oct 2011';
--  
--  SELECT MAX(Post_Date)
--  INTO G_End_Date
--  From Dwh_Foundation.Fnd_Rtl_Loc_Item_Dy_Om_St_Ord;
--  
--  Select Distinct This_Week_End_Date Into G_Date
--  From Dim_Calendar
--  Where calendar_Date = G_End_Date;
-- 
--  SELECT COUNT(DISTINCT This_Week_Start_Date) - 1
--  INTO G_Sub_End
--  FROM Dim_Calendar
--  Where This_Week_Start_Date Between G_Start_Date And G_End_Date;
--  
--  L_Text := 'PERIOD/WEEKS : fnd_rtl_loc_item_dy_om_st_ord ='||G_Start_Date||'-'||G_Date||'**wks='||G_Sub_End;
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
--  L_Text := '==================================';
--  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
-- 
-- 
--  FOR g_sub IN 0..g_sub_end
--  Loop
--  
--    SELECT
--      this_week_start_date,
--      this_week_end_date
--    INTO 
--      G_Start_Date,
--      G_End_Date
--    From Dim_Calendar
--    Where Calendar_Date = G_Date - (G_Sub * 7) ;
--  L_Text := 'RANGE : fnd_rtl_loc_item_dy_om_st_ord ='||G_Start_Date||' to '||G_End_Date||' sub='||g_sub;
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);-
--
--  FOR V_Cur IN
--  (
--SELECT  post_date,
--  item_no,
--  Location_No,
--  0 Boh_2_Qty ,
--  Decode(Num_Units_Per_Tray,0,Null,1) Num_Units_Per_Tray,
--  decode(num_units_per_tray2,0,null,1) Num_Units_Per_Tray2
--From 
--  Dwh_Foundation.Fnd_Rtl_Loc_Item_Dy_Om_St_Ord 
-- Where Post_Date Between G_Start_Date And G_End_Date
--    And Item_No          In (6009173634306,6009173468154)
--  )
--  Loop
--  UPDATE Dwh_foundation.Fnd_Rtl_Loc_Item_Dy_Om_St_Ord F1
--    SET F1.Num_Units_Per_Tray = V_Cur.Num_Units_Per_Tray,
--      F1.Num_Units_Per_Tray2  = V_Cur.Num_Units_Per_Tray2,
--      F1.Boh_2_Qty            = V_Cur.Boh_2_Qty
--    WHERE F1.Item_No      = V_Cur.Item_No
--    AND F1.Location_No    = V_Cur.Location_No
--    AND f1.post_date          = v_cur.post_date;
--    G_Recs_Updated             := G_Recs_Updated + to_number(TO_CHAR(sql%rowcount));
--    Commit-;
---  END LOOP;-
--
--  L_Text := 'NO OF fnd_rtl_loc_item_dy_om_st_ord records UPT2 set to 1  =  '||G_Recs_Updated;
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
--  L_Text := '==================================';
--  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
--    
--  End Loop;
-- 
  --**************************************************************************************************
  -- FND_RTL_LOC_ITEM_DY_OM_ST_ORD
  --**************************************************************************************************
  G_Recs_Updated  := 0;
  G_Sub_End       := 0;
  G_End_Date      := Null;
  G_Start_Date    := '3 oct 2011';
  
  SELECT MAX(Post_Date)
  INTO G_End_Date
  From Dwh_Foundation.Fnd_Rtl_Loc_Item_Dy_Om_St_Ord;
  
  Select Distinct This_Week_End_Date Into G_Date
  From Dim_Calendar
  Where calendar_Date = G_End_Date;
 
  SELECT COUNT(DISTINCT This_Week_Start_Date) - 1
  INTO G_Sub_End
  FROM Dim_Calendar
  Where This_Week_Start_Date Between G_Start_Date And G_End_Date;
  
  L_Text := 'PERIOD/WEEKS : fnd_rtl_loc_item_dy_om_st_ord ='||G_Start_Date||'-'||G_Date||'**wks='||G_Sub_End;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  L_Text := '==================================';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
 
  FOR g_sub IN 0..g_sub_end
  Loop
  
    SELECT
      this_week_start_date,
      this_week_end_date
    INTO 
      G_Start_Date,
      G_End_Date
    From Dim_Calendar
    Where Calendar_Date = G_Date - (G_Sub * 7) ;
  L_Text := 'RANGE : fnd_rtl_loc_item_dy_om_st_ord ='||G_Start_Date||' to '||G_End_Date||' sub='||g_sub;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);

  FOR V_Cur IN
  (
SELECT  post_date,
  item_no,
  Location_NO
  From 
  Dwh_Foundation.Fnd_Rtl_Loc_Item_Dy_Om_St_Ord 
 Where Post_Date Between G_Start_Date And G_End_Date
    And Item_No          In (6009173634306,6009173468154)
  )
  Loop
  UPDATE Dwh_foundation.Fnd_Rtl_Loc_Item_Dy_Om_St_Ord F1
    SET F1.LAST_UPDATED_DATE = '25 MAR 2012'
    WHERE F1.Item_No      = V_Cur.Item_No
    AND F1.Location_No    = V_Cur.Location_No
    AND f1.post_date          = v_cur.post_date;
    G_Recs_Updated             := G_Recs_Updated + to_number(TO_CHAR(sql%rowcount));
    Commit;
  END LOOP;

  L_Text := 'NO OF fnd_rtl_loc_item_dy_om_st_ord records UPT2 set to 1  =  '||G_Recs_Updated;
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
  L_Text := '==================================';
  Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
    
  End Loop; 

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
  Raise;

END WH_FND_COMPOSITES_4605;
