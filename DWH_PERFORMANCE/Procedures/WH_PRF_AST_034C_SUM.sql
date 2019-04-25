--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_034C_SUM
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_034C_SUM" 
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_034C_SUM';
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
   dwh_lookup.dim_control(g_date);
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

      insert /*+ APPEND parallel (ast,2) */ into dwh_datafix.mart_ch_ast_chn_grd_sc_wk_dtfx ast
      select /*+ full (a) parallel (a,2) */             
             a.sk1_chain_no,
             a.sk1_grade_no,
             a.sk1_style_colour_no,
             a.fin_year_no,
             a.fin_week_no,
             a.this_week_start_date,
             max(a.reg_rsp_excl_vat),
             sum(a.sales_act),
             max(a.cost_price_act),
             max(a.selling_rsp_act),
             sum(a.chain_intake_margin_act),                                 
             sum(a.sales_qty_act),
             sum(a.sales_planned_qty_pre_act),
             sum(a.target_stock_qty_act),
             sum(a.aps_sales_qty_act),
             sum(a.sales_std_act),
             sum(a.sales_margin_std_act),
             sum(a.store_intake_selling_std_act),
             sum(a.sales_qty_std_act),
             sum(a.target_stock_qty_act),
             sum(a.sales_qty_6wk_act),
             sum(a.sales_6wk_act),
             sum(a.target_stock_selling_6wk_act),
             sum(a.store_count_act),
             sum(a.prom_sales_qty_act) ,
             sum(a.prom_sales_selling_act),
             sum(a.clear_sales_qty_act),
             sum(a.clear_sales_selling_act),
             sum(a.target_stock_selling_act),
             sum(a.reg_sales_qty_act),
             sum(a.reg_sales_selling_act),
             a.last_updated_date,
             sum(a.num_avail_days_std_act),
             sum(a.num_catlg_days_std_act),
             sum(a.chain_intake_selling_act),
             sum(a.chain_intake_selling_std_act),
             sum(a.chain_intake_margin_std_act)
      from   dwh_performance.mart_ch_ast_chn_grd_sc_wk_dtfx a
      where  a.fin_year_no   = 2014
       and   a.fin_week_no   between 28 and 48
      group by
             a.sk1_chain_no,
             a.sk1_grade_no,
             a.sk1_style_colour_no,
             a.fin_year_no,
             a.fin_week_no,
             a.this_week_start_date,
             a.last_updated_date     ;

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

end wh_prf_ast_034c_sum;
