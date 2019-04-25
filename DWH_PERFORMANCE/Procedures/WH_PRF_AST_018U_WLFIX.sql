--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_018U_WLFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_018U_WLFIX" 
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
G_THIS_WEEK_START_DATE  date;
G_THIS_WEEK_END_DATE  date;
g_start_week         integer       :=  0;
g_start_year         integer       :=  0;
g_start_month        integer       :=  0;
g_fin_week_code      varchar2(7);



g_PARTITION_name       varchar2(32);
g_sql_trunc_partition  varchar2(100);


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_018U_WLFIX';
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
   dwh_lookup.dim_control(g_date);

   execute immediate 'alter session enable parallel dml';
g_date := '5 SEP 2016';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



FOR g_sub IN 0..10
  LOOP
    g_recs_inserted := 0;
    select fin_year_no, fin_week_no, this_week_start_date, this_week_end_date, fin_week_code, fin_month_no
    into   g_start_year, g_start_week, g_this_week_start_date, g_this_week_end_date, g_fin_week_code, g_start_month
    from   dim_calendar
    WHERE calendar_date = g_date - (g_sub * 7);
--RTL_LSWA_M20173_11
          g_partition_name :=   'RTL_LSWA_M'||g_START_YEAR||g_START_month||'_'||g_START_week;    
          g_sql_trunc_partition := 'alter table dwh_performance.RTL_LOC_SC_WK_APS truncate SUBPARTITION '||g_partition_name;
      
          l_text := 'Truncate partition ='||g_partition_name;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                              
          EXECUTE IMMEDIATE g_sql_trunc_partition;
          commit; 
      insert /*+ APPEND parallel (aps,2) */ into rtl_loc_sc_wk_aps aps

      with aps_assort_data as (
      select /*+ parallel (act,2) */
             loc.sk1_location_no,
             sc.sk1_style_colour_no,
             act.fin_year_no,
             act.fin_week_no,
             g_fin_week_code fin_week_code,
             act.store_count,
             act.sales_qty,
             g_date as last_updated_date
      from   fnd_ast_loc_sc_wk_act act,
             dim_ast_lev1_diff1 sc,
             dim_location loc
      where  act.style_colour_no   = sc.style_colour_no
       and   act.location_no       = loc.location_no
       and   act.fin_year_no       = g_start_year
       and   act.fin_week_no       = g_start_WEEK
    --   and   CAL.FIN_DAY_NO        = 3
      -- and   act.last_updated_date = g_date
      order by act.fin_year_no, act.fin_week_no) ,

      aps_cleansed_data as (
      select /*+ full (apln) parallel (apln,2) */
             mp.sk1_location_no,
             mp.sk1_style_colour_no,
             mp.fin_year_no,
             mp.fin_week_no,
             G_fin_week_code fin_week_code,
             mp.adjusted_store_count,
             mp.aps_sales_qty,
             g_date as last_updated_date
      from   rtl_loc_sc_wk_mp mp
      where  mp.fin_year_no       = g_start_year
       and   mp.fin_week_no       = g_start_WEEK
    --   and   CAL.FIN_DAY_NO       = 3
    --   and   mp.last_updated_date = g_date
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
g_recs_read := g_recs_read + SQL%ROWCOUNT;
   g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

  l_text := 'Insert NEW:- RECS =  '||g_recs_inserted||' '||g_this_week_start_date||'  To '||g_this_week_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       commit;
  l_text := ' ==================  ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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

end wh_prf_ast_018u_WLFIX;
