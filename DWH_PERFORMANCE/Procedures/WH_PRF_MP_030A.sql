--------------------------------------------------------
--  DDL for Procedure WH_PRF_MP_030A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MP_030A" 
                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2011
--  Author:      A Joshua
--  Purpose:     Load MP APS/Stock counts from temp_loc_sc_wk_mp to rtl_loc_sc_wk_mp.
--  Tables:      Input  - fnd_mp_loc_sc_wk
--               Output - temp_loc_sc_wk_mp
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_max_week           number        :=  0;
g_fin_start_date     Date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MP_030A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE MP LOC/SC/WK EX MP APS DATA SWING ';
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
   l_text := 'LOAD OF TEMP_LOC_SC_WK_MP STARTED AT '||
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

   select (this_week_start_date - (6 * 7))
   into  g_fin_start_date
   from  dim_calendar
   Where calendar_date = (select to_date(sysdate, 'dd mon yy') from dual);

   Select Fin_week_no, Fin_year_no
   into g_fin_week_no, g_fin_year_no
   from dim_calendar
   Where Calendar_date = g_fin_start_date;

   select max(fin_week_no) + 1
   into   g_max_week
   from   dim_calendar
   where  fin_year_no =
          (select fin_year_no
           from   dim_calendar
           where  calendar_date = g_date
          );

   execute immediate 'truncate table dwh_performance.temp_loc_sc_wk_mp';
    l_text := 'TABLE TEMP_LOC_SC_WK_MP TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--      execute immediate 'alter session enable parallel dml';

      insert /*+ APPEND */ into dwh_performance.temp_loc_sc_wk_mp
      select location_no,
              style_colour_no,
--              fin_year_no,
--              fin_week_no,
              (
                case
                  when (g_fin_week_no + To_number(Substr(Syscol,4,2))) > g_max_week Then
                     g_fin_year_no + 1
                  else
                     g_fin_year_no
                end
              ) fin_year_no,

              (
                case
                  when (g_fin_week_no + to_number(substr(syscol,4,2))) > g_max_week then
                    g_fin_week_no + (to_number(substr(syscol,4,2))- g_max_week)
                  else
                    g_fin_week_no + (to_number(substr(syscol,4,2))-1)
                end
               ) fin_week_no,

               adjusted_store_count,
               aps_sales_qty,
               last_updated_date

      from fnd_mp_loc_sc_wk
             unpivot include nulls ((adjusted_store_count,aps_sales_qty)
                                                                for syscol in (
                                                                               (wk_01_adjusted_store_count,wk_01_aps_sales_qty),
                                                                               (wk_02_adjusted_store_count,wk_02_aps_sales_qty),
                                                                               (wk_03_adjusted_store_count,wk_03_aps_sales_qty),
                                                                               (wk_04_adjusted_store_count,wk_04_aps_sales_qty),
                                                                               (wk_05_adjusted_store_count,wk_05_aps_sales_qty),
                                                                               (wk_06_adjusted_store_count,wk_06_aps_sales_qty)
                                                                               ))
      where last_updated_date = g_date;

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

end wh_prf_mp_030a;
