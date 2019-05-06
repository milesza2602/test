--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_034C_CHKDATE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_034C_CHKDATE" 
(P_FORALL_LIMIT in integer,P_SUCCESS OUT BOOLEAN)
as

--**************************************************************************************************
--
-- Fix version of WH_PRF_AST_034C - fix actuals values -- just for checking derived dates
--
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Wendy Lyttle
--
--  Purpose:     Create Style-colour datamart for C&GM
--
--
--  Tables:      Input  - dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act (ACTUAL AND PRE-ACTUAL)
--                        dwh_performance.mart_ch_ast_chn_grd_sc_wk (Current Mart)
--                        dwh_performance.rtl_chn_geo_grd_sc_wk_ast_act (GEO SEASON SALES AND STOCK)
--               Output - mart_ch_ast_chn_grd_sc_wk_dtfx
--
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

g_sub                 number;
g_sub_end                   number;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_034C_CHKDATE';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DATAMART - DATAFIX';
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

--**************************************************************************************************
--
-- S T E P 1 : setup dates required for processing
--
--**************************************************************************************************

-- For this datafix, we need to set the G_DATE = what would have been the batch_date
-- eg.
--29 DEC 2013
--5 JAN 2014
--12 JAN 2014
--19 JAN 2014
--26 JAN 2014
-- 2 feb 2014
-- 9 feb 2014
--16 feb 2014
-- 23 feb 2014
--2 MARCH 2014

G_DATE := '23 feb 2014';
    l_text := 'BATCH RUN DATE  - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := '---------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


-- The loop will start at the g_date but roll forward
-- we need to set the no. of loops.
-- eg.
-- If we want it to run for six weeks then G_SUB_END = 6 -1

g_sub_end := 1;
g_date := g_date + 1;

for g_sub in 0..G_SUB_END loop
  
    g_date := g_date + (7 *g_sub);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_date_minus_84 := g_date - (7 * 12);

    SELECT THIS_WEEK_START_DATE,    THIS_WEEK_START_DATE - 42,     FIN_WEEK_NO
    into   g_this_week_start_date,  g_6wk_bck_start_date,          g_end_fin_week_no
    from   dim_calendar
    where  calendar_date = g_date;

    select fin_half_no,       fin_year_no
    into   g_fin_half_no,     g_fin_year_no
    from   DIM_CALENDAR
    where  calendar_date = g_6wk_bck_start_date;

    select distinct min(SEASON_START_DATE),     min(FIN_WEEK_NO),             min(this_week_start_date), min(fin_half_end_date)
    into  g_curr_season_start_date,             g_curr_season_fin_week_no,    g_curr_half_wk_start_date, g_curr_season_end_date
    from  dim_calendar
    where fin_half_no = g_fin_half_no
    and   fin_year_no = g_fin_year_no;

--
-- This part caters for the situatrion at season roll-over when we straddle 2 seasons for 6 weeks until we
--  are in week=7 of the new season
--
    select fin_half_no,       fin_year_no
    into   g_fin_half_no_new,     g_fin_year_no_new
    from   DIM_CALENDAR
    where  calendar_date = g_date;

    select distinct min(SEASON_START_DATE),     min(FIN_WEEK_NO),             min(this_week_start_date), min(fin_half_end_date)
    into  g_new_season_start_date,             g_new_season_fin_week_no,    g_new_half_wk_start_date, g_new_season_end_date
    from  dim_calendar
    where fin_half_no = g_fin_half_no_new
    and   fin_year_no = g_fin_year_no_new;


    SELECT THIS_WEEK_START_DATE,         FIN_WEEK_NO
     into  g_start_fin_week_date,        g_start_fin_week_no
    from   DIM_CALENDAR
    where  CALENDAR_DATE = case when g_6wk_bck_start_date < G_CURR_SEASON_START_DATE
                                          then G_CURR_SEASON_START_DATE
                                else
                                          g_6wk_bck_start_date
                                end;


    L_TEXT := 'g_this_week_start_date = '||G_THIS_WEEK_START_DATE||' g_6wk_bck_start_date = '||g_6wk_bck_start_date;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_end_fin_week_no = '||G_END_FIN_WEEK_NO||' g_fin_half_no = '||g_fin_half_no||' g_fin_year_no = '||g_fin_year_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_curr_season_start_date = '||g_curr_season_start_date||' g_curr_season_fin_week_no = '||g_curr_season_fin_week_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   L_TEXT := 'g_curr_half_wk_start_date = '||g_curr_half_wk_start_date||' g_start_fin_week_date = '||G_START_FIN_WEEK_DATE;
       DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_start_fin_week_no = '||G_START_FIN_WEEK_NO;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    L_TEXT := 'g_new_season_start_date = '||g_new_season_start_date||' g_new_season_fin_week_no = '||g_new_season_fin_week_no;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


end loop;
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



END WH_PRF_AST_034C_CHKDATE;
