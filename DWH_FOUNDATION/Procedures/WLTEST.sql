--------------------------------------------------------
--  DDL for Procedure WLTEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WLTEST" (
    P_FORALL_LIMIT in integer,
    P_SUCCESS OUT BOOLEAN)
as
G_PROC_DATE date;
G_SUB number;
G_RECS number;
G_DATE date;
G_SUBPART_NAME              varchar2(32);
G_SQL VARCHAR2(4000);
 g_start_deal NUMBER := 0;
  g_end_deal   NUMBER := 0;
 g_start_deal_no NUMBER := 0;
  g_updated_recs  NUMBER := 0;
   g_inserted_recs  NUMBER := 0;
  g_end_deal_no   NUMBER := 0;
  L_Message Sys_Dwh_Errlog.Log_Text%Type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WLTEST';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_tran;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_tran;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  L_Text Sys_Dwh_Log.Log_Text%Type ;
  l_description sys_dwh_log_summary.log_description%type   := 'WLTEST';
  L_PROCESS_TYPE SYS_DWH_LOG_SUMMARY.LOG_PROCESS_TYPE%type := DWH_CONSTANTS.VC_LOG_PROCESS_TYPE_N;
Begin
  P_SUCCESS := false;
/*G_DATE := '2 april 2013';

  FOR g_sub IN 0..10
     LOOP
   G_PROC_DATE := g_date + g_sub;


   insert into STG_AST_LOC_ITEM_DY_CATLG_ARC
   select * from DWH_DATAFIX.STG_AST_LOC_ITEM_DY_CATLG_WL
   where POST_DATE = G_PROC_DATE;
   
      G_RECS          := sql%ROWCOUNT;

      commit;

   L_TEXT := 'recs read='||g_recs;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
   end loop;
--****************************************************************************
G_DATE := '13 april 2013';

  FOR g_sub IN 0..3
     LOOP
   G_PROC_DATE := g_date + g_sub;


   insert into STG_AST_LOC_ITEM_DY_CATLG_ARC
   select * from STG_AST_LOC_ITEM_DY_CATLG
   where POST_DATE = G_PROC_DATE;
   
      G_RECS          := sql%ROWCOUNT;

      commit;

   L_TEXT := 'recs read='||G_RECS;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
   end LOOP;
--****************************************************************************
*/
EXECUTE IMMEDIATE('
  CREATE TABLE DWH_FOUNDATION.TEMP_L4L 
   (	LOCATION_NO NUMBER(10,0), 
	TY_CALENDAR_DATE DATE, 
	TY_FIN_YEAR_NO NUMBER(4,0), 
	LIKE_FOR_LIKE_IND NUMBER(1,0)
   ) 
  TABLESPACE FND_MASTER ');
 



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
END WLTEST;
