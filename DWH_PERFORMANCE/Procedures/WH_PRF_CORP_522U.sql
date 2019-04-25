--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_522U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_522U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        SEP 2010
--  Author:      A. de Wet
--  Purpose:     Rollup from rtl_loc_item_wk_rms_dense to rtl_zone_item_wk_rms_dense.
--  Tables:      Input  - rtl_loc_item_wk_rms_dense
--               Output - rtl_zone_item_wk_rms_dense
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  1 july 2011 - w lyttle - add gather stats after truncate
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

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_522U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP RMS DENSE FROM WEEK TO ZONE ITEM';
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
   l_text := 'ROLLUP OF rtl_zone_item_wk_rms_dense EX WEEK LEVEL STARTED AT '||
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
   l_text := 'Truncate table - rtl_zone_item_wk_rms_dense '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  --Truncate of RTL_ZONE_ITEM_WK_RMS_DENSE
  --**************************************************************************************************
   execute immediate 'truncate table dwh_performance.rtl_zone_item_wk_rms_dense';

   l_text := 'Truncate table - rtl_zone_item_wk_rms_dense completed '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  --Gather Stats On RTL_ZONE_ITEM_WK_RMS_DENSE
  --**************************************************************************************************

  dbms_stats.gather_table_stats ('DWH_PERFORMANCE', 'RTL_ZONE_ITEM_WK_RMS_DENSE', degree => 8);
  commit;

  l_text := 'Gather stats on DWH_PERFORMANCE.RTL_ZONE_ITEM_WK_RMS_DENSE '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Start main loop
  --**************************************************************************************************

   for g_sub in 0..2 loop

      select fin_week_no,   fin_year_no
      into   g_fin_week_no, g_fin_year_no
      from   dim_calendar
      where  calendar_date = g_date ;

      g_fin_year_no := g_fin_year_no - g_sub;

      if g_sub in (0,1) then
         g_fin_week_no := 1;
      end if;

      insert /*+ APPEND */  into dwh_performance.rtl_zone_item_wk_rms_dense rtl_ziw
      select   /*+ FULL(liw) */
               dl.sk1_fd_zone_group_zone_no as sk1_zone_group_zone_no,
               liw.sk1_item_no as sk1_item_no,
               liw.fin_year_no as fin_year_no,
               liw.fin_week_no as fin_week_no,
               max(liw.fin_week_code)  fin_week_code,
               max(liw.this_week_start_date) as this_week_start_date,
               max(liw.sk2_item_no) sk2_item_no,
               sum(liw.sales_qty) sales_qty,
               sum(liw.sales) sales,
               g_date last_updated_date
      from     rtl_loc_item_wk_rms_dense liw
      join     dim_item di
      on       liw.sk1_item_no  = di.sk1_item_no
      join     dim_location dl
      on       liw.sk1_location_no = dl.sk1_location_no
      where    liw.fin_year_no  =  g_fin_year_no
      and      liw.fin_week_no  >= g_fin_week_no
      and      di.business_unit_no = 50
      group by liw.fin_year_no,
               liw.fin_week_no,
               dl.sk1_fd_zone_group_zone_no,
               liw.sk1_item_no;

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      l_text := 'ROLLED UP YEAR  - '||g_fin_year_no||' '|| ' at '||
                to_char(sysdate,('hh24:mi:ss'))||' records '||sql%rowcount||' total '||g_recs_inserted;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      commit;

      dbms_stats.gather_table_stats ('DWH_PERFORMANCE', 'RTL_ZONE_ITEM_WK_RMS_DENSE', degree => 8);
      commit;

      l_text := 'Gather stats on DWH_PERFORMANCE.RTL_ZONE_ITEM_WK_RMS_DENSE '||g_date;
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

end wh_prf_corp_522u;
