--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_032U_WLLOD311016
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_032U_WLLOD311016" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2012
--  Author:      A Joshua
--  Purpose:     Load Assort Attribute/Product View (Approved, Original, Actualised and Pre-Actualised Plan) from performance level.
--  Tables:      Input  - rtl_chn_geo_grd_sc_wk_ast_pln, rtl_chn_geo_grd_sc_wk_ast_act and rtl_chain_sc_wk_ast_pln
--               Output - mart_ch_ast_chn_sc_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit          integer       :=  dwh_constants.vc_forall_limit;
g_recs_read             integer       :=  0;
g_recs_inserted         integer       :=  0;
g_date                  date;
g_season_start_date     date;
g_season_end_date       date;
g_ly_season_start_date  date;
g_ly_season_end_date    date;
g_calendar_end_date_ly  date;
/*
g_6wk_bck_start_date date             := '20 feb 12';
g_74wk_fwd_end_date   date            := '17 mar 13';
g_prev_week_end_date   date;
g_6wk_bck_start_date_ly date          := '27 jun 11';
g_max_end_date_ly   date              := '18 mar 12';
*/
g_6wk_bck_start_date    date;
g_74wk_fwd_end_date     date;
g_prev_week_end_date    date;
g_6wk_bck_start_date_ly date;
g_max_end_date_ly       date;

g_fin_week_no           dim_calendar.fin_week_no%type;
g_fin_year_no           dim_calendar.fin_year_no%type;
g_fin_half_no           dim_calendar.fin_half_no%type;
g_fin_year_no_ly        dim_calendar.fin_year_no%type;
g_fin_week_no_ly        dim_calendar.fin_week_no%type;

G_THIS_WEEK_START_DATE_ly  date;
G_THIS_WEEK_END_DATE_ly  date;

G_THIS_WEEK_START_DATE  date;
G_THIS_WEEK_END_DATE  date;
g_start_week         integer       :=  0;
g_start_year         integer       :=  0;
g_start_month        integer       :=  0;
g_fin_week_code      varchar2(7);



g_PARTITION_name       varchar2(40);
g_sql_trunc_partition  varchar2(120);


l_message               sys_dwh_errlog.log_text%type;
l_module_name           sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_032U_WLLOD311016';
l_name                  sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name           sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name           sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name        sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text                  sys_dwh_log.log_text%type ;
l_description           sys_dwh_log_summary.log_description%type  := 'LOAD THE ATRRIBUTE/PRODUCT VIEW DATAMART - CHN LEVEL';
l_process_type          sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
   l_text := 'LOAD OF mart_ch_ast_chn_sc_wk STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
--g_date := '5 SEP 2016';
g_date := '27 JUNE 2016';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

FOR g_sub IN 0..17
  LOOP
    g_recs_inserted := 0;
    select fin_year_no, fin_week_no, this_week_start_date, this_week_end_date, fin_week_code, fin_month_no
    into   g_start_year, g_start_week, g_this_week_start_date, g_this_week_end_date, g_fin_week_code, g_start_month
    from   dim_calendar
    WHERE calendar_date = g_date + (g_sub * 7);
    
   
--MRT_CACSW_M20132_7
          g_partition_name :=   'MRT_CACSW_M'||g_START_YEAR||g_START_month||'_'||g_START_week;    
          g_sql_trunc_partition := 'alter table dwh_performance.MART_CH_AST_CHN_SC_WK truncate SUBPARTITION '||g_partition_name;
      
          l_text := 'Truncate partition ='||g_partition_name;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                              
          EXECUTE IMMEDIATE g_sql_trunc_partition;
          commit; 
          
      insert /*+ APPEND parallel (mart,4) */ into dwh_performance.mart_ch_ast_chn_sc_wk mart
      SELECT *FROM DWH_PERFORMANCE.mart_chastchnscwk_WLFIX311016
  WHERE FIN_YEAR_NO = G_START_YEAR AND FIN_WEEK_NO =  G_START_WEEK;


g_recs_read := g_recs_read + SQL%ROWCOUNT;
   g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

  l_text := 'Insert NEW:- RECS =  '||g_recs_inserted||' '||g_this_week_start_date||'  To '||g_this_week_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       commit;
  l_text := ' ==================  ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
          
END LOOP;






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

end wh_prf_ast_032u_WLLOD311016;
