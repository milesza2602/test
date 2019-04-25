--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_034C_WLTST_OTHER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_034C_WLTST_OTHER" 
(P_FORALL_LIMIT in integer,P_SUCCESS OUT BOOLEAN)
as

--**************************************************************************************************
--
-- New version of WH_PRF_AST_034U - write to MART_CH_AST_CHN_GRD_SCWK_WL (old version mart_ch_ast_chn_grd_sc_wk)
--
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Wendy Lyttle
--
--  Purpose:     Create Style-colour datamart for C&GM
--
--
--  Tables:      Input  - dwh_performance.wl_rtl_chn_gg_sc_wk_ast_act (ACTUAL AND PRE-ACTUAL)
--                        dwh_performance.wl_rtl_chain_sc_wk_ast_pln_wl (CHAIN_MARGIN)
--                        temp_rtl_ast_sc_6wk_034b (6WK SALES AND STOCK)
--                        dwh_performance.wl_rtl_chn_gg_sc_wk_ast_act (GEO SEASON SALES AND STOCK)
--                        dwh_performance.wl_rtl_chain_sc_wk_ast_pln_wl (CHAIN SEASON SALES AND STOCK)
--                        dwh_performance.wl_RTL_LOC_SC_WK_ast_CATLG (NUM_AVAIL_DAYS, NUM_CATLG_DAYS)
--               Output - mart_ch_ast_chn_grd_scwk_wl
--mart_ch_ast_chn_grd_sc_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  May 2013 - wendy - new version of wh_prf_ast_034u
--                   - write to mart_ch_ast_chn_grd_sc_wk
--  june 2013 - wendy - fin year_end rollover
--            - VERSION 18 JUNE 2013
--  21 June 2013 - release to prd
--  November 11 2013 - 4878 -Enhancement:Style Card - Filter on Chain 10 for the Availability measure
--                           where area_no not in (9965, 8800, 9978, 9979, 9953) AND CHAIN_NO = 10
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************


g_forall_limit           integer       :=  dwh_constants.vc_forall_limit;
g_recs_read              integer       :=  0;
g_recs_inserted          integer       :=  0;

g_date                    date;
g_6wk_bck_start_date      date;
g_curr_season_start_date  date;
g_curr_season_end_date  date;
g_curr_half_wk_start_date date;
g_date_minus_84           date;
G_WEEK_NO NUMBER;
g_fin_year_no             number;
g_fin_half_no             number;

g_fin_year_no_new            number;
g_fin_half_no_new            number;

g_start_fin_week_no       number;
g_end_fin_week_no         number;
g_curr_season_fin_week_no number;
g_start_fin_week_date     date;
g_this_week_start_date    date;

g_new_season_start_date date;
g_new_season_fin_week_no number;
g_new_half_wk_start_date date;
g_new_season_end_date date;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_034C_OTHER';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DATAMART - CHN LEVEL';
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
    L_TEXT := 'LOAD OF mart_ch_ast_chn_grd_sc_wk STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DWH_LOG.INSERT_LOG_SUMMARY(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_DESCRIPTION,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session enable parallel dml';
 --   EXECUTE IMMEDIATE ('TRUNCATE TABLE DWH_PERFORMANCE.mart_ch_ast_chn_grd_sc_wk');
 --   COMMIT;
 --   l_text := 'TRUNCATE TABLE DWH_PERFORMANCE.mart_ch_ast_chn_grd_sc_wk';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
--
-- S T E P 1 : setup dates required for processing
--
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--      g_date := g_date + 1;
--      l_text := 'BATCH DATE BEING PROCESSED - '||G_DATE;
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  

    EXECUTE IMMEDIATE 'alter session enable parallel dml';
       G_DATE := '30 OCT 2016';
      g_date := g_date + 1;
    l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date||' THRU 30 OCT 2016';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    G_WEEK_NO := 0;
FOR g_sub IN 0..11
  LOOP
    g_recs_inserted := 0;
    G_WEEK_NO := G_WEEK_NO + 1;


    insert /*+append */ into  dwh_performance.Wmart_ch_ast_chn_grd_sc_wk mART
    SELECT  /*+ FULL(CH) PARALLEL(CH,8) */
         CH.* FROM dwh_performance.mart_ch_ast_chn_grd_sc_wk CH, DWH_PERFORMANCE.WLSTYLE_COLOUR B
    WHERE CH.SK1_STYLE_COLOUR_NO = B.SK1_STYLE_COLOUR_NO
    AND FIN_YEAR_NO = 2017 AND FIN_WEEK_NO = G_WEEK_NO;
        

    g_recs_inserted := sql%rowcount;
    L_TEXT := 'Recs inserted into Wmart_ch_ast_chn_grd_sc_wk = '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;

  END LOOP;
  
    L_TEXT := 'Recs inserted into Wmart_ch_ast_chn_grd_sc_wk = '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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

END WH_PRF_AST_034c_WLTST_OTHER;
