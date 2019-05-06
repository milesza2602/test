--------------------------------------------------------
--  DDL for Procedure WH_FND_AST_018U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_AST_018U" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2012
--  Author:      A Joshua
--  Purpose:     Load Assort Approved Plan from foundation level.
--  Tables:      Input  - stg_ast_geo_grd_sc_fpln_wk_cpy
--               Output - fnd_ast_geo_grd_sc_wk_fpln
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
g_sub                integer       :=  0;
g_prev_week_end_date date;
g_6wk_bck_start_date date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_AST_018U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ASSORT APPROVED FACT PLAN';
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
   l_text := 'LOAD OF FND_AST_GEO_GRD_SC_WK_FPLN STARTED AT '||
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

   select (this_week_end_date - 6) - (7*6), this_week_end_date - 7
   into  g_6wk_bck_start_date , g_prev_week_end_date
   from  dim_calendar
   where calendar_date = (select to_date(sysdate, 'dd mon yy') from dual);

      execute immediate 'alter session enable parallel dml';

      insert /*+ APPEND parallel (ast,2) */ into fnd_ast_geo_grd_sc_wk_fpln ast
      select /*+ full (fpln) parallel (fpln,2) */
             fpln.style_colour_no,
             fpln.geography_no,
             fpln.grade_no,
             fpln.fin_year_no,
             fpln.fin_week_no,
             fpln.plan_type_no,
             fpln.sales_qty,
             fpln.sales_cost,
             fpln.sales,
             fpln.pres_min,
             fpln.store_intake_qty,
             fpln.store_intake_cost,
             fpln.store_intake_selling,
             fpln.target_stock_qty,
             fpln.target_stock_cost,
             fpln.target_stock_selling,
             fpln.adjusted_sales,
             fpln.adjusted_sales_qty,
             fpln.adjusted_sales_cost,
             g_date as last_updated_date
      from   stg_ast_geo_grd_sc_fpln_wk_cpy fpln,
             fnd_ast_lev1_diff1 sc,
             fnd_ast_lev1_diff1_real scr,
             fnd_ast_geography geo,
             fnd_ast_grade grd,
             fnd_plan_type pln,
             dim_calendar_wk cal
      where  fpln.style_colour_no                        = sc.style_colour_no (+)
       and   fpln.style_colour_no                        = scr.style_colour_no (+)
       and   fpln.geography_no                           = geo.geography_no (+)
       and   fpln.grade_no                               = grd.grade_no (+)
       and   fpln.plan_type_no                           = pln.plan_type_no (+)
       and  (case when substr(fpln.style_colour_no,1,2)  = 99 and sc.style_colour_no is not null then 1 else 0 end +
             case when substr(fpln.style_colour_no,1,2) <> 99 and scr.style_colour_no is not null then 1 else 0 end) > 0
       and  (case when geo.geography_no is not null then 1 else 0 end) > 0
       and  (case when grd.grade_no     is not null then 1 else 0 end) > 0
       and  (case when pln.plan_type_no is not null then 1 else 0 end) > 0
       and   fpln.fin_year_no = cal.fin_year_no
       and   fpln.fin_week_no = cal.fin_week_no
       and (((substr(fpln.style_colour_no,1,2) <> 99)
       and   cal.this_week_start_date between g_6wk_bck_start_date and g_prev_week_end_date)
        or  (cal.this_week_start_date > g_prev_week_end_date));

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

end wh_fnd_ast_018u;
