--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_030U_ADHOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_030U_ADHOC" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2012
--  Author:      A Joshua
--  Purpose:     Load Assort Location View (Approved, Actuals and Pre-Actuals Plan) from performance level.
--  Tables:      Input  - rtl_chn_geo_grd_sc_wk_ast_pln and rtl_chn_geo_grd_sc_wk_ast_act
--               Output - aj_ch_ast_chn_geo_grd_sc_wk13
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
g_forall_limit         integer       :=  dwh_constants.vc_forall_limit;
g_recs_read            integer       :=  0;
g_recs_inserted        integer       :=  0;
g_date                 date;
g_season_start_date    date;
g_season_end_date      date          := '29 sep 13';
g_6wk_bck_start_date   date          := '01 jul 13';
g_fin_week_no          dim_calendar.fin_week_no%type;
g_fin_year_no          dim_calendar.fin_year_no%type;
g_fin_half_no          dim_calendar.fin_half_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_030U_ADHOC';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION VIEW DATAMART - CHN/GEO/GRD LEVEL';
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
   l_text := 'LOAD OF aj_ch_ast_chn_geo_grd_sc_wk13 STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select (this_week_end_date - 6) - (7*6)
   into  g_6wk_bck_start_date
   from  dim_calendar
   where calendar_date = (select to_date(sysdate, 'dd mon yy') from dual);

   select fin_half_no, fin_year_no
   into g_fin_half_no, g_fin_year_no
   from dim_calendar
   where calendar_date = g_date;

   select distinct min(season_start_date), max(fin_half_end_date)
   into g_season_start_date, g_season_end_date
   from dim_calendar
   where fin_half_no = g_fin_half_no
   and   fin_year_no = g_fin_year_no;

   l_text := 'Data extract from '||g_6wk_bck_start_date|| ' to '||g_season_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   execute immediate 'truncate table dwh_performance.aj_ch_ast_chn_geo_grd_sc_wk13';

   execute immediate 'alter session enable parallel dml';
--   begin
      insert /*+ APPEND parallel (ast,2) */ into dwh_datafix.aj_ch_ast_chn_geo_grd_sc_wk13 ast
      with aplan_data as (
-- Approved Plan
      select /*+ full (apln) parallel (apln,2) */
             apln.sk1_chain_no,
             apln.sk1_geography_no,
             apln.sk1_grade_no,
             apln.sk1_style_colour_no,
             apln.fin_year_no,
             apln.fin_week_no,
             apln.fin_week_code,
             apln.time_on_offer_ind,
             apln.store_count,
             apln.sales_qty,
             apln.sales,
             apln.target_stock_selling
      from   rtl_chn_geo_grd_sc_wk_ast_pln apln,
             dim_calendar cal
      where  apln.sk1_plan_type_no = 60
       and   apln.fin_year_no      = cal.fin_year_no
       and   apln.fin_week_no      = cal.fin_week_no
       and   cal.this_week_start_date between '25 jun 12' and '30 jun 13'
       and   cal.fin_day_no        = 3),
      actual_data as (
-- Actualised Plan
      select /*+ full (act) parallel (act,4) */
             act.sk1_chain_no,
             act.sk1_geography_no,
             act.sk1_grade_no,
             act.sk1_style_colour_no,
             act.fin_year_no,
             act.fin_week_no,
             act.fin_week_code,
             act.time_on_offer_ind,
             act.store_count,
             act.sales_qty,
             act.sales,
             act.target_stock_selling
      from   rtl_chn_geo_grd_sc_wk_ast_act act,
             dim_calendar cal
      where  act.sk1_plan_type_no = 63
       and   act.fin_year_no      = cal.fin_year_no
       and   act.fin_week_no      = cal.fin_week_no
       and   cal.this_week_start_date between '25 jun 12' and '30 jun 13'
       and   cal.fin_day_no        = 3),
      preact_data as (
-- Pre-Actualised Plan
      select /*+ full (preact) parallel (preact,4) */
             preact.sk1_chain_no,
             preact.sk1_geography_no,
             preact.sk1_grade_no,
             preact.sk1_style_colour_no,
             preact.fin_year_no,
             preact.fin_week_no,
             preact.fin_week_code,
             preact.time_on_offer_ind,
             preact.store_count,
             preact.sales_qty,
             preact.sales,
             preact.target_stock_selling
      from   rtl_chn_geo_grd_sc_wk_ast_act preact,
             dim_calendar cal
      where  preact.sk1_plan_type_no = 64
       and   preact.fin_year_no      = cal.fin_year_no
       and   preact.fin_week_no      = cal.fin_week_no
       and   cal.this_week_start_date between '25 jun 12' and '30 jun 13'
       and   cal.fin_day_no       = 3)
-- collapsing of data from respective tables (aplan_data, actual_data, preact_data)
      select sk1_chain_no,
             sk1_geography_no,
             sk1_grade_no,
             sk1_style_colour_no,
             fin_year_no,
             fin_week_no,
             fin_week_code,
             sum(time_on_offer_ind_apln)      as time_on_offer_ind_apln,
             sum(time_on_offer_ind_act)       as time_on_offer_ind_act,
             sum(time_on_offer_ind_preact)    as time_on_offer_ind_preact,
             sum(store_count_apln)            as store_count_apln,
             sum(store_count_act)             as store_count_act,
             sum(store_count_preact)          as store_count_preact,
             sum(sales_qty_apln)              as sales_qty_apln,
             sum(sales_qty_act)               as sales_qty_act,
             sum(sales_qty_preact)            as sales_qty_preact,
             sum(sales_apln)                  as sales_apln,
             sum(sales_act)                   as sales_act,
             sum(sales_preact)                as sales_preact,
             sum(target_stock_selling_apln)   as target_stock_selling_apln,
             sum(target_stock_selling_act)    as target_stock_selling_act,
             sum(target_stock_selling_preact) as target_stock_selling_preact,
             g_date
      from (
      select /*+ full (extr1,extr2,extr3) parallel (extr1,4) */
             nvl(nvl(extr1.sk1_chain_no,extr2.sk1_chain_no),extr3.sk1_chain_no) as sk1_chain_no,
             nvl(nvl(extr1.sk1_geography_no,extr2.sk1_geography_no),extr3.sk1_geography_no) as sk1_geography_no,
             nvl(nvl(extr1.sk1_grade_no,extr2.sk1_grade_no),extr3.sk1_grade_no) as sk1_grade_no,
             nvl(nvl(extr1.sk1_style_colour_no,extr2.sk1_style_colour_no),extr3.sk1_style_colour_no) as sk1_style_colour_no,
             nvl(nvl(extr1.fin_year_no,extr2.fin_year_no),extr3.fin_year_no) as fin_year_no,
             nvl(nvl(extr1.fin_week_no,extr2.fin_week_no),extr3.fin_week_no) as fin_week_no,
             nvl(nvl(extr1.fin_week_code,extr2.fin_week_code),extr3.fin_week_code) as fin_week_code,
             extr1.time_on_offer_ind    as time_on_offer_ind_apln,
             extr2.time_on_offer_ind    as time_on_offer_ind_act,
             extr3.time_on_offer_ind    as time_on_offer_ind_preact,
             extr1.store_count          as store_count_apln,
             extr2.store_count          as store_count_act,
             extr3.store_count          as store_count_preact,
             extr1.sales_qty            as sales_qty_apln,
             extr2.sales_qty            as sales_qty_act,
             extr3.sales_qty            as sales_qty_preact,
             extr1.sales                as sales_apln,
             extr2.sales                as sales_act,
             extr3.sales                as sales_preact,
             extr1.target_stock_selling as target_stock_selling_apln,
             extr2.target_stock_selling as target_stock_selling_act,
             extr3.target_stock_selling as target_stock_selling_preact,
             g_date
      from   aplan_data extr1
      full outer join actual_data extr2 on
             extr1.sk1_chain_no        = extr2.sk1_chain_no
       and   extr1.sk1_geography_no    = extr2.sk1_geography_no
       and   extr1.sk1_grade_no        = extr2.sk1_grade_no
       and   extr1.sk1_style_colour_no = extr2.sk1_style_colour_no
       and   extr1.fin_year_no         = extr2.fin_year_no
       and   extr1.fin_week_no         = extr2.fin_week_no
--
      full outer join preact_data extr3 on
             extr1.sk1_chain_no        = extr3.sk1_chain_no
       and   extr1.sk1_geography_no    = extr3.sk1_geography_no
       and   extr1.sk1_grade_no        = extr3.sk1_grade_no
       and   extr1.sk1_style_colour_no = extr3.sk1_style_colour_no
       and   extr1.fin_year_no         = extr3.fin_year_no
       and   extr1.fin_week_no         = extr3.fin_week_no)

      group by sk1_chain_no, sk1_geography_no, sk1_grade_no, sk1_style_colour_no, fin_year_no, fin_week_no, fin_week_code, g_date;
/*
      EXCEPTION
        WHEN OTHERS
          THEN
            DBMS_OUTPUT.put_line (DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);

   end;
*/
      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

   commit;

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

end wh_prf_ast_030u_adhoc;
