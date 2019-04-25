--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_016U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_016U_FIX" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        November 2012
--  Author:      A Joshua
--  Purpose:     Load Assort Location Actual A-Plan from foundation level.
--  Tables:      Input  - fnd_ast_loc_sc_wk_act
--               Output - rtl_loc_geo_grd_sc_wk_ast_act
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_chain_corporate    integer       :=  0;
g_chain_franchise    integer       :=  0;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_ly_fin_week_no     dim_calendar.fin_week_no%type;
g_ly_fin_year_no     dim_calendar.fin_year_no%type;
g_lcw_fin_week_no    dim_calendar.fin_week_no%type;
g_lcw_fin_year_no    dim_calendar.fin_year_no%type;
g_date               date;
g_start_date         date;
g_end_date           date;
g_ly_start_date      date;
g_ly_end_date        date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_016U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ACTUALS ex AST FACT APLAN';
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
   l_text := 'LOAD OF RTL_LOC_GEO_GRD_SC_WK_AST_ACT STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   g_date := '14 dec 14';
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select (this_week_end_date - 6) - (7*6), this_week_end_date - 7
   into g_start_date, g_end_date
   from  dim_calendar
   where calendar_date = (select to_date(sysdate, 'dd mon yy') from dual);

   select min(ly_calendar_date), ly_fin_year_no , ly_fin_week_no, fin_week_no, fin_year_no
   into g_ly_start_date, g_ly_fin_year_no, g_ly_fin_week_no, g_lcw_fin_week_no, g_lcw_fin_year_no
   from dim_calendar
   where this_week_start_date = g_start_date
   group by ly_fin_year_no, ly_fin_week_no, fin_week_no, fin_year_no;
   
   select max(ly_calendar_date), ly_fin_year_no , ly_fin_week_no, fin_week_no, fin_year_no
   into g_ly_end_date, g_ly_fin_year_no, g_ly_fin_week_no, g_lcw_fin_week_no, g_lcw_fin_year_no
   from dim_calendar
   where this_week_end_date = g_end_date
   group by ly_fin_year_no, ly_fin_week_no, fin_week_no, fin_year_no;

   l_text := 'Current Data extract from '||g_start_date|| ' to '||g_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'LY Data extract from '||g_ly_start_date|| ' to '||g_ly_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'LY Fin Year '||g_ly_fin_year_no|| ' LY Fin Week '||g_ly_fin_week_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'Last Completed Year '||g_lcw_fin_year_no|| ' Last Completed Week '||g_lcw_fin_week_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';

      insert /*+ APPEND parallel (ast,6) */ into rtl_loc_geo_grd_sc_wk_ast_act ast

      with curr_data as (
      select /*+ full (apln) full (lsc) parallel (apln,4) parallel (lsc,4) */
             loc.sk1_location_no,
             geo.sk1_geography_no,
             nvl(lsc.grade_no,999) as sk1_grade_no,
             sc.sk1_style_colour_no,
             apln.fin_year_no,
             apln.fin_week_no,
             pln.sk1_plan_type_no,
             'W'||apln.fin_year_no||apln.fin_week_no as fin_week_code,
             apln.store_count,
             apln.sales_qty,
             apln.sales_cost,
             apln.sales,
             apln.target_stock_qty,
             apln.target_stock_cost,
             apln.target_stock_selling,
             g_date as last_updated_date
      from   fnd_ast_loc_sc_wk_act apln,
             fnd_ast_loc_sc_wk lsc,
             fnd_ast_location floc,
             dim_ast_lev1_diff1 sc,
             dim_location loc,
             dim_plan_type pln,
             dim_ast_geography geo,
             dim_calendar cal
      where apln.style_colour_no   = sc.style_colour_no
       and  apln.style_colour_no   = lsc.style_colour_no (+)
       and  apln.location_no       = lsc.location_no     (+)
       and  apln.fin_year_no       = lsc.fin_year_no     (+)
       and  apln.fin_week_no       = lsc.fin_week_no     (+)
       and  apln.location_no       = loc.location_no
       and  apln.location_no       = floc.location_no
       and  floc.geography_no      = geo.geography_no
       and  apln.plan_type_no      = pln.plan_type_no
       and  apln.fin_year_no       = cal.fin_year_no
       and  apln.fin_week_no       = cal.fin_week_no
       and  cal.fin_day_no         = 3
       and  apln.last_updated_date = g_date),
--      order by apln.fin_year_no, apln.fin_week_no) ,

      selcal as (
      select distinct fin_year_no, fin_week_no, this_week_start_date
      from dim_calendar dc
      where dc.this_week_start_date between g_ly_start_date and g_ly_end_date),
      
      ly_data as (
      select /*+ full (apln) full (lsc) parallel (apln,4) parallel (lsc,4) */
             loc.sk1_location_no,
             geo.sk1_geography_no,
             nvl(lsc.grade_no,999)                                           as sk1_grade_no,
             sc.sk1_style_colour_no,
             cal.fin_year_no                                                 as fin_year_no,
             cal.fin_week_no                                                 as fin_week_no,
             pln.sk1_plan_type_no,
             'W'||(cal.fin_year_no)||cal.fin_week_no                         as fin_week_code,
             apln.store_count,
             apln.sales_qty,
             apln.sales_cost,
             apln.sales,
             apln.target_stock_qty,
             apln.target_stock_cost,
             apln.target_stock_selling,
             g_date                                                          as last_updated_date
      from   fnd_ast_loc_sc_wk_act apln,
             fnd_ast_loc_sc_wk lsc,
             fnd_ast_location floc,
             dim_ast_lev1_diff1 sc,
             dim_location loc,
             dim_plan_type pln,
             dim_ast_geography geo,
             dim_calendar cal,
             selcal sc
      where apln.style_colour_no   = sc.style_colour_no
       and  apln.style_colour_no   = lsc.style_colour_no (+)
       and  apln.location_no       = lsc.location_no     (+)
       and  apln.fin_year_no       = lsc.fin_year_no     (+)
       and  apln.fin_week_no       = lsc.fin_week_no     (+)
       and  apln.location_no       = loc.location_no
       and  apln.location_no       = floc.location_no
       and  floc.geography_no      = geo.geography_no
       and  apln.plan_type_no      = pln.plan_type_no
       and  apln.fin_year_no       = cal.ly_fin_year_no
       and  apln.fin_week_no       = cal.ly_fin_week_no
       and  cal.fin_day_no         = 3
       and  apln.fin_year_no       = sc.fin_year_no
       and  apln.fin_week_no       = sc.fin_week_no)
--       and  cal.this_week_start_date between g_ly_start_date and g_ly_end_date
--      order by apln.fin_year_no, apln.fin_week_no)

      select sk1_location_no,
             sk1_geography_no,
             sk1_grade_no,
             sk1_style_colour_no,
             fin_year_no,
             fin_week_no,
             sk1_plan_type_no,
             fin_week_code,
             sum(store_count)             store_count,
             sum(sales_qty)               sales_qty,
             sum(sales_cost)              sales_cost,
             sum(sales)                   sales,
             sum(sales_qty_ly)            sales_qty_ly,
             sum(sales_cost_ly)           sales_cost_ly,
             sum(sales_ly)                sales_ly,
             sum(target_stock_qty)        target_stock_qty,
             sum(target_stock_cost)       target_stock_cost,
             sum(target_stock_selling)    target_stock_selling,
             sum(target_stock_qty_ly)     target_stock_qty_ly,
             sum(target_stock_cost_ly)    target_stock_cost_ly,
             sum(target_stock_selling_ly) target_stock_selling_ly,
             g_date as last_updated_date
      from   (
         select /*+ full (extr1) full (extr2) parallel (extr1,4) parallel (extr2,4) */ 
             nvl(extr1.sk1_location_no,extr2.sk1_location_no)         as sk1_location_no,
             nvl(extr1.sk1_geography_no,extr2.sk1_geography_no)       as sk1_geography_no,
             nvl(extr1.sk1_grade_no,extr2.sk1_grade_no)               as sk1_grade_no,
             nvl(extr1.sk1_style_colour_no,extr2.sk1_style_colour_no) as sk1_style_colour_no,
             nvl(extr1.fin_year_no,extr2.fin_year_no)                 as fin_year_no,
             nvl(extr1.fin_week_no,extr2.fin_week_no)                 as fin_week_no,
             nvl(extr1.sk1_plan_type_no,extr2.sk1_plan_type_no)       as sk1_plan_type_no,
             nvl(extr1.fin_week_code,extr2.fin_week_code)             as fin_week_code,
             extr1.store_count                                        as store_count,
             extr1.sales_qty                                          as sales_qty,
             extr1.sales_cost                                         as sales_cost,
             extr1.sales                                              as sales,
             extr2.sales_qty                                          as sales_qty_ly,
             extr2.sales_cost                                         as sales_cost_ly,
             extr2.sales                                              as sales_ly,
             extr1.target_stock_qty                                   as target_stock_qty,
             extr1.target_stock_cost                                  as target_stock_cost,
             extr1.target_stock_selling                               as target_stock_selling,
             extr2.target_stock_qty                                   as target_stock_qty_ly,
             extr2.target_stock_cost                                  as target_stock_cost_ly,
             extr2.target_stock_selling                               as target_stock_selling_ly,
             g_date                                                   as last_updated_date
         from   curr_data extr1
         full outer join
             ly_data extr2 on
             extr1.sk1_location_no     = extr2.sk1_location_no
         and extr1.sk1_geography_no    = extr2.sk1_geography_no
         and extr1.sk1_grade_no        = extr2.sk1_grade_no
         and extr1.sk1_style_colour_no = extr2.sk1_style_colour_no
         and extr1.fin_year_no         = extr2.fin_year_no
         and extr1.fin_week_no         = extr2.fin_week_no )

         group by sk1_location_no, sk1_geography_no, sk1_grade_no, sk1_style_colour_no, fin_year_no, fin_week_no, sk1_plan_type_no, fin_week_code, g_date;

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

end wh_prf_ast_016u_fix;
