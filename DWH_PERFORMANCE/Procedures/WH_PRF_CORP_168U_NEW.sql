--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_168U_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_168U_NEW" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLANNING data to JDAFF fact table in the performance layer
--               with input ex JDAFF FND_JDAFF_ST_PLAN_ANALYSIS_DY table from foundation layer.
--
--  Tables:      Input  - FND_JDAFF_ST_PLAN_ANALYSIS_DY
--               Output - dwh_performance.rtl_jdaff_st_plan_analysis_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance: S Samsodien Sept 2015
--               QC 37776 add additional column to convert constr_store_cover_day into DDHHMM format
--               BK - adjust stats collection on very large partitioned tables for 12c upgrade (old way is very slow)           Ref: 23Jan2019
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_168U_NEW';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STORE PLANNING ANALYSIS DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
g_start_date         date;
g_end_date           date;

g_min_post           date;
g_max_post           date;

-- required for gather schema stats - Ref: 23Jan2019
filter_lst  DBMS_STATS.OBJECTTAB := DBMS_STATS.OBJECTTAB();

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
   l_text := 'LOAD OF RTL_JDAFF_ST_PLAN_ANALYSIS_DY STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
--   g_date := '18/JAN/19';
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';

   -- Determine the start and end date for the record to be processed.  This must tie up
   -- with the partitions that were truncated prior to this program being run else the program will fail.
   -- The partitions that would have been dropped would be for todays date going 20 days forward.
   select /*+ parallel (t,8)  */ min(trading_date), max(trading_date), min(post_date), max(post_date)
     into g_start_date, g_end_date, g_min_post, g_max_post
     from dwh_foundation.FND_JDAFF_ST_PLAN_ANALYSIS_DY t
    where last_updated_date = g_date ;

   l_text := 'START TRADING DATE BEING PROCESSED - '||g_start_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'END TRADING DATE BEING PROCESSED - '||g_end_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   l_text := 'START POST_DATE BEING PROCESSED - '||g_min_post;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'END POST DATE BEING PROCESSED - '||g_max_post;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   --l_text := 'Running GATHER_TABLE_STATS ON rtl_jdaff_st_plan_analysis_dy - Needed due to the truncate that preceeded this job';
   --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   --DBMS_STATS.gather_table_stats ('dwh_performance', 'rtl_jdaff_st_plan_analysis_dy', DEGREE => 32);

   --l_text := 'First GATHER_TABLE_STATS ON rtl_jdaff_st_plan_analysis_dy completed';
   --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   l_text := 'Processing..';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  INSERT /*+ APPEND parallel(jdast,8) */
   INTO  DWH_PERFORMANCE.rtl_jdaff_st_plan_analysis_dy jdast
   SELECT /*+ full(jdaff) parallel(jdaff,8) */
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
            jdaff.constrained_expired_stock,
            jdaff.constr_store_cover_day ,
            g_date as last_updated_date,
            jdaff.alt_constraint_unused_soh_unit, 
            jdaff.alt_constraint_poh_unit,	     
            jdaff.constraint_unmet_demand_unit,   
            jdaff.constraint_unused_soh_unit,    
            jdaff.expired_soh_unit,               
            jdaff.ignored_demand_unit,            
            jdaff.projected_stock_available_unit,
-- QC 37776 add additional column to convert constr_store_cover_day into DDHHMM format
            to_char(extract(day from(numtodsinterval(jdaff.constr_store_cover_day, 'minute'))) )|| 'D' ||
            to_char(extract(hour from(numtodsinterval(jdaff.constr_store_cover_day, 'minute'))),'fm00' )  || 'H' ||
            to_char(extract(minute from(numtodsinterval(jdaff.constr_store_cover_day, 'minute'))),'fm00' ) || 'M' as constr_store_cover
   from     dwh_foundation.FND_JDAFF_ST_PLAN_ANALYSIS_DY jdaff,
            dim_item di,
            dim_location dl
   where jdaff.item_no            = di.item_no
     and jdaff.location_no        = dl.location_no
     and jdaff.trading_date between g_start_date and g_end_date
     and jdaff.last_updated_date  = g_date
     and jdaff.post_date          = g_start_date
  --   and jdaff.post_date    between g_min_post and g_max_post
--   order by  di.sk1_item_no, dl.sk1_location_no, jdaff.trading_date
;


      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;

     if g_recs_inserted > 0 then
         l_text := 'Running GATHER_TABLE_STATS ON rtl_jdaff_st_plan_analysis_dy';
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--         DBMS_STATS.gather_table_stats ('dwh_performance', 'rtl_jdaff_st_plan_analysis_dy', estimate_percent => dbms_stats.auto_sample_size); -- Ref: 23Jan2019
         
         -- below is the new code for stats collection in 12c - Ref: 23Jan2019
         filter_lst.extend(1);
         filter_lst(1).ownname := 'DWH_PERFORMANCE';
         filter_lst(1).objname := 'RTL_JDAFF_ST_PLAN_ANALYSIS_DY';
         DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>'DWH_PERFORMANCE',obj_filter_list=>filter_lst,options=>'gather auto',granularity=>'PARTITION');

         l_text := 'Second GATHER_TABLE_STATS ON rtl_jdaff_st_plan_analysis_dy completed';
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
end WH_PRF_CORP_168U_NEW;
