--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_022U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_022U_FIX" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2012
--  Author:      A Joshua
--  Purpose:     Load Actual Plan (last 6 weeks) from foundation level.
--  Tables:      Input  - fnd_ast_geo_grd_sc_wk_act
--               Output - rtl_chn_geo_grd_sc_wk_ast_act
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
g_date               date;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_chain_corporate    integer       :=  0;
g_chain_franchise    integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_022U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ASSORT FACT ACTUAL PLANS';
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
   l_text := 'LOAD OF RTL_CHN_GEO_GRD_SC_WK_AST_ACT STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   DWH_LOOKUP.DIM_CONTROL(G_DATE);
   g_date := '14 dec 14';
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select sk1_chain_no
   into g_chain_corporate
   from dim_chain
   where chain_no = 10;

   select sk1_chain_no
   into g_chain_franchise
   from dim_chain
   where chain_no = 20;

      execute immediate 'alter session enable parallel dml';

      insert /*+ APPEND parallel (ast,2) */ into rtl_chn_geo_grd_sc_wk_ast_act ast
      select /*+ full (apln) parallel (apln,2) */
             (CASE WHEN apln.grade_no in (14)  THEN g_chain_franchise
                 ELSE g_chain_corporate
              END) sk1_chain_no,
             geo.sk1_geography_no,
             grd.sk1_grade_no,
             sc.sk1_style_colour_no,
             apln.fin_year_no,
             apln.fin_week_no,
             pln.sk1_plan_type_no,
             'W'||apln.fin_year_no||apln.fin_week_no as fin_week_code,
             apln.sales_qty,
             apln.sales_cost,
             apln.sales,
             apln.clearance_cost,
             apln.prom_cost,
             apln.store_intake_qty,
             apln.store_intake_cost,
             apln.store_intake_selling,
             apln.target_stock_qty,
             apln.target_stock_cost,
             apln.target_stock_selling,
             apln.pres_min,
             apln.store_count,
             apln.time_on_offer_ind,
             apln.facings,
             apln.sit_qty,
             apln.sales_qty as aps_sales_qty,
             apln.sales - apln.sales_cost as sales_margin,
             apln.store_intake_selling - apln.store_intake_cost as store_intake_margin,
             apln.target_stock_selling - apln.target_stock_cost as target_stock_margin,
             apln.selling_price,
             apln.cost_price,
--             apln.prom_reg_qty, -- prom_sales  + reg_sales
--             apln.prom_reg_cost, -- prom_sales  + reg_sales
--             apln.prom_reg_selling, -- prom_sales  + reg_sales
             apln.prom_sales_qty + apln.reg_sales_qty as prom_reg_qty,
             apln.prom_sales_cost + apln.reg_sales_cost as prom_reg_cost,
             apln.prom_sales_selling + apln.reg_sales_selling as prom_reg_selling,
             apln.clear_sales_qty,
             apln.clear_sales_cost,
             apln.clear_sales_selling,
             apln.prom_sales_qty,
             apln.prom_sales_cost,
             apln.prom_sales_selling,
             apln.reg_sales_qty,
             apln.reg_sales_cost,
             apln.reg_sales_selling,
             g_date as last_updated_date
      from   fnd_ast_geo_grd_sc_wk_act apln,
             dim_ast_lev1_diff1 sc,
             dim_ast_geography geo,
             dim_ast_grade grd,
             dim_plan_type pln
      where apln.style_colour_no   = sc.style_colour_no
       and  apln.geography_no      = geo.geography_no
       and  apln.grade_no          = grd.grade_no
       and  apln.plan_type_no      = pln.plan_type_no
       and  apln.last_updated_date = g_date;

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

end WH_PRF_AST_022U_FIX;
