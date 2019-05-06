--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_169U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_169U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLANNING data to JDAFF fact table in the performance layer
--               with input ex JDAFF FND_JDAFF_ST_PLAN_ANALYSIS_WK table from foundation layer.
--
--  Tables:      Input  - FND_JDAFF_ST_PLAN_ANALYSIS_WK
--               Output - dwh_performance.rtl_jdaff_st_plan_analysis_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_169U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STORE WEEK PLANNING ANALYSIS DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
g_start_date         date;
g_end_date           date;
g_min_post           date;
g_max_post           date;

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
   l_text := 'LOAD OF RTL_JDAFF_ST_PLAN_ANALYSIS_WK STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   --g_date := '11/JAN/15';
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';

   -- Determine the start and end date for the record to be processed.  This must tie up
   -- with the partitions that were truncated prior to this program being run else the program will fail.
   -- The partitions that would have been dropped would be for todays date going 20 days forward.
   
   select /*+ parallel(fnd,4) full(fnd) */ min(fnd.trading_date), max(fnd.trading_date), min(post_date), max(post_date)
     into g_start_date, g_end_date, g_min_post, g_max_post
     from dwh_foundation.FND_JDAFF_ST_PLAN_ANALYSIS_WK fnd
    where fnd.last_updated_date = g_date ;
    
    

   l_text := 'START TRADING DATE BEING PROCESSED - '||g_start_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'END TRADING DATE BEING PROCESSED - '||g_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   l_text := 'START POST_DATE BEING PROCESSED - '||g_min_post;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'END POST DATE BEING PROCESSED - '||g_max_post;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'Running GATHER_TABLE_STATS ON rtl_jdaff_st_plan_analysis_WK - Needed due to the truncate that preceeded this job';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   --DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'rtl_jdaff_st_plan_analysis_WK', DEGREE => 32);

   l_text := 'First GATHER_TABLE_STATS ON rtl_jdaff_st_plan_analysis_WK completed';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'Processing..';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  INSERT /*+ APPEND parallel(jdast,4)  */
   INTO  DWH_PERFORMANCE.rtl_jdaff_st_plan_analysis_WK jdast
   SELECT /*+ full(jdaff) parallel(jdaff,4) */
            di.sk1_item_no,
            dl.sk1_location_no,
            jdaff.trading_date,
            jdaff.POST_DATE,
            jdaff.TOTAL_DEMAND_UNIT,
            jdaff.INVENTORY_UNIT,
            jdaff.PLANNED_ARRIVALS_UNIT,
            jdaff.PLANNED_ARRIVALS_CASE,
            jdaff.REC_ARRIVAL_UNIT,
            jdaff.REC_ARRIVAL_CASE,
            jdaff.IN_TRANSIT_UNIT,
            jdaff.IN_TRANSIT_CASE,
            jdaff.CONSTRAINT_POH_UNIT,
            jdaff.SAFETY_STOCK_UNIT,
            jdaff.CONSTRAINED_EXPIRED_STOCK,
            jdaff.constr_store_cover_day ,
            g_date as last_updated_date,
            jdaff.alt_constraint_unused_soh_unit,
            jdaff.alt_constraint_poh_unit,
            jdaff.constraint_unmet_demand_unit,
            jdaff.constraint_unused_soh_unit,
            jdaff.expired_soh_unit,
            jdaff.ignored_demand_unit,
            jdaff.projected_stock_available_unit

   from     dwh_foundation.FND_JDAFF_ST_PLAN_ANALYSIS_WK jdaff,
            dim_item di,
            dim_location dl
   where jdaff.item_no            = di.item_no
     and jdaff.location_no        = dl.location_no
     and jdaff.trading_date between g_start_date and g_end_date
     and jdaff.last_updated_date  = g_date
 --    and jdaff.post_date    between g_min_post and g_max_post
  -- order by  di.sk1_item_no, dl.sk1_location_no, jdaff.trading_date
  ;


      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;


      commit;

   if g_recs_inserted > 0 then

      l_text := 'Running GATHER_TABLE_STATS ON rtl_jdaff_st_plan_analysis_WK';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('dwh_performance', 'rtl_jdaff_st_plan_analysis_WK', DEGREE => 32);

      l_text := 'Second GATHER_TABLE_STATS ON rtl_jdaff_st_plan_analysis_WK completed';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

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
end wh_prf_corp_169u;
