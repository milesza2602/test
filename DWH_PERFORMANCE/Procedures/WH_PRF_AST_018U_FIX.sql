--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_018U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_018U_FIX" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        August 2013
--  Author:      A Joshua
--  Purpose:     Load Assort APS (ex MP cleansed and JDA Location Actuals)
--  Tables:      Input  - fnd_ast_loc_sc_wk_act and rtl_loc_sc_wk_mp
--               Output - rtl_loc_sc_wk_aps
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_018U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE APS measures ex AST and MP';
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
   l_text := 'LOAD OF RTL_LOC_SC_WK_APS STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   DWH_LOOKUP.DIM_CONTROL(G_DATE);
   g_date := '14 dec 14';
   L_TEXT := 'BATCH DATE BEING PROCESSED - '||G_DATE;   
   
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';

      insert /*+ APPEND parallel (aps,2) */ into rtl_loc_sc_wk_aps aps

      with aps_assort_data as (
      select /*+ parallel (act,2) */
             loc.sk1_location_no,
             sc.sk1_style_colour_no,
             act.fin_year_no,
             act.fin_week_no,
             cal.fin_week_code,
             act.store_count,
             act.sales_qty,
             g_date as last_updated_date
      from   fnd_ast_loc_sc_wk_act act,
             dim_ast_lev1_diff1 sc,
             dim_location loc,
             dim_calendar cal
      where  act.style_colour_no   = sc.style_colour_no
       and   act.location_no       = loc.location_no
       and   act.fin_year_no       = cal.fin_year_no
       and   act.fin_week_no       = cal.fin_week_no
       and   CAL.FIN_DAY_NO        = 3
       and   act.last_updated_date = g_date
      order by act.fin_year_no, act.fin_week_no) ,

      aps_cleansed_data as (
      select /*+ full (apln) parallel (apln,2) */
             mp.sk1_location_no,
             mp.sk1_style_colour_no,
             mp.fin_year_no,
             mp.fin_week_no,
             cal.fin_week_code,
             mp.adjusted_store_count,
             mp.aps_sales_qty,
             g_date as last_updated_date
      from   rtl_loc_sc_wk_mp mp,
             dim_calendar cal
      where  mp.fin_year_no       = cal.fin_year_no
       and   mp.fin_week_no       = cal.fin_week_no
       and   CAL.FIN_DAY_NO       = 3
       and   mp.last_updated_date = g_date
      order by mp.fin_year_no, mp.fin_week_no)

      select sk1_location_no,
             sk1_style_colour_no,
             fin_year_no,
             fin_week_no,
             fin_week_code,
             sum(store_count_cleansed)    store_count_cleansed,
             sum(aps_sales_qty_cleansed)  aps_sales_qty_cleansed,
             sum(store_count)             store_count,
             sum(aps_sales_qty)           aps_sales_qty,
             g_date                       last_updated_date
      from   (
         select /*+ full (extr1,extr2) parallel (extr1,2) */
             nvl(extr1.sk1_location_no,extr2.sk1_location_no)         as sk1_location_no,
             nvl(extr1.sk1_style_colour_no,extr2.sk1_style_colour_no) as sk1_style_colour_no,
             nvl(extr1.fin_year_no,extr2.fin_year_no)                 as fin_year_no,
             nvl(extr1.fin_week_no,extr2.fin_week_no)                 as fin_week_no,
             nvl(extr1.fin_week_code,extr2.fin_week_code)             as fin_week_code,
             extr2.adjusted_store_count                               as store_count_cleansed,
             extr2.aps_sales_qty                                      as aps_sales_qty_cleansed,
             extr1.store_count                                        as store_count,
             extr1.sales_qty                                          as aps_sales_qty,
             g_date                                                   as last_updated_date
         from   aps_assort_data extr1
         full outer join aps_cleansed_data extr2 on
             extr1.sk1_location_no     = extr2.sk1_location_no
         and extr1.sk1_style_colour_no = extr2.sk1_style_colour_no
         and extr1.fin_year_no         = extr2.fin_year_no
         and extr1.fin_week_no         = extr2.fin_week_no )

         group by sk1_location_no, sk1_style_colour_no, fin_year_no, fin_week_no, fin_week_code, g_date;

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

end wh_prf_ast_018u_fix;
