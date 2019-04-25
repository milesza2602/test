--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_808U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_808U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2018
--  Author:      Alfonso Joshua
--  Purpose:     Rollup to location planogram using the Planogram Structure
--  Tables:      Input  - rtl_loc_item_wk_plan_disp_grp
--               Output - rtl_loc_display_wk
--
--  Maintenance:
--  99/99/99 - Chg-9999 - Name and desc of change
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_reset         integer       :=  0;
g_rec_out            rtl_loc_display_wk%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date   := trunc(sysdate);
g_max_date           date   := trunc(sysdate);
g_cnt                number := 0;
g_fin_year_no        number := 0;
g_fin_week_no        number := 0;
g_this_week_start_date   date   := trunc(sysdate + 1);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_808U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DISPLAY PLANOGRAM EX PLANOGRAM STRUCTURE ';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure do_merge as
begin

    merge /*+ parallel (lp,4) */ into rtl_loc_display_wk lp using (
       select /*+ full (a) parallel (a,6) */ distinct  
            sk1_location_no, 
            display_area_name,
            fin_year_no,
            fin_week_no,
            sk1_floorplan_key,
--            fixture_no,
            this_week_start_date,
            floorplan_live_from_date,
            floorplan_live_to_date,
            display_area,
            fixture_count,
            fixture_linear,
            fixture_area,
            fixture_density,
            section_count_no,
            section_linear,
            section_area,
            section_density,
            foods_selling_sqm,
            market_type,
            market_size,
            trading_sqm,
            store_profile,
            node,
            province,
            fsa_indicator,
            longitude,
            latitude,
--            planogram_channel,
            ww_catchment_area
       from rtl_loc_item_wk_plan_disp_grp a
       where fin_year_no = g_fin_year_no 
        and  fin_week_no = g_fin_week_no
/*       group by 
            sk1_location_no, 
            display_area_name,
            fin_year_no,
            fin_week_no,
            sk1_floorplan_key,
--            fixture_no,
            this_week_start_date,
            floorplan_live_from_date,
            floorplan_live_to_date,
            display_area,
            market_type,
            market_size,
            trading_sqm,
            store_profile,
            node,
            province,
            fsa_indicator,
            longitude,
            latitude,
--            planogram_channel,
            ww_catchment_area */
  ) mer_mart
  
  on (lp.sk1_location_no      = mer_mart.sk1_location_no      and
      lp.fin_year_no          = mer_mart.fin_year_no          and
      lp.fin_week_no          = mer_mart.fin_week_no          and
      lp.sk1_floorplan_key    = mer_mart.sk1_floorplan_key    and
      lp.display_area_name    = mer_mart.display_area_name    
--      lp.fixture_no           = mer_mart.fixture_no
     )

  when matched then
     update 
       set this_week_start_date         = mer_mart.this_week_start_date,
           floorplan_live_from_date     = mer_mart.floorplan_live_from_date,
           floorplan_live_to_date       = mer_mart.floorplan_live_to_date,
           display_area                 = mer_mart.display_area,
           market_type                  = mer_mart.market_type,
           market_size                  = mer_mart.market_size,
           trading_sqm                  = mer_mart.trading_sqm,
           store_profile                = mer_mart.store_profile,
           node                         = mer_mart.node,
           province                     = mer_mart.province,
           fsa_indicator                = mer_mart.fsa_indicator,
           longitude                    = mer_mart.longitude,
           latitude                     = mer_mart.latitude,
--           planogram_channel            = mer_mart.planogram_channel,
           ww_catchment_area            = mer_mart.ww_catchment_area,
           fixture_count                = mer_mart.fixture_count,
           fixture_linear               = mer_mart.fixture_linear,
           fixture_area                 = mer_mart.fixture_area,
           fixture_density              = mer_mart.fixture_density,
           section_count_no             = mer_mart.section_count_no,
           section_linear               = mer_mart.section_linear,
           section_area                 = mer_mart.section_area,
           section_density              = mer_mart.section_density,
           foods_selling_sqm            = mer_mart.foods_selling_sqm,
           last_updated_date            = g_date
     
when not matched then
  insert (
           sk1_location_no, 
           display_area_name,
           fin_year_no,
           fin_week_no,
           sk1_floorplan_key,
--           fixture_no,
           this_week_start_date,
           floorplan_live_from_date,
           floorplan_live_to_date,
           display_area,
           fixture_count,
           fixture_linear,
           fixture_area,
           fixture_density,
           section_count_no,
           section_linear,
           section_area,
           section_density,
           foods_selling_sqm,
           market_type,
           market_size,
           trading_sqm,
           store_profile,
           node,
           province,
           fsa_indicator,
           longitude,
           latitude,
--           planogram_channel,
           ww_catchment_area,
           last_updated_date            
         )
  values (
           mer_mart.sk1_location_no, 
           mer_mart.display_area_name,
           mer_mart.fin_year_no,
           mer_mart.fin_week_no,
           mer_mart.sk1_floorplan_key,
--           mer_mart.fixture_no,
           mer_mart.this_week_start_date,
           mer_mart.floorplan_live_from_date,
           mer_mart.floorplan_live_to_date,
           mer_mart.display_area,
           mer_mart.fixture_count,
           mer_mart.fixture_linear,
           mer_mart.fixture_area,
           mer_mart.fixture_density,
           mer_mart.section_count_no,
           mer_mart.section_linear,
           mer_mart.section_area,
           mer_mart.section_density,
           mer_mart.foods_selling_sqm,
           mer_mart.market_type,
           mer_mart.market_size,
           mer_mart.trading_sqm,
           mer_mart.store_profile,
           mer_mart.node,
           mer_mart.province,
           mer_mart.fsa_indicator,
           mer_mart.longitude,
           mer_mart.latitude,
--           mer_mart.planogram_channel,
           mer_mart.ww_catchment_area,
           g_date
          )  
  ;
  
  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

  commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'MAIN MERGE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'MAIN MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end do_merge;
  
--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin 

    l_text := 'LOAD OF RTL_LOC_DISPLAY_WK EX INTACTIX STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
   
--    g_date := '26 aug 18';
    select this_week_start_date, fin_year_no, fin_week_no
    into g_this_week_start_date, g_fin_year_no, g_fin_week_no
    from dim_calendar
    where calendar_date = g_date + 1;
    
  --**************************************************************************************************
  -- De Duplication of the staging table to avoid Bulk insert failures
  --************************************************************************************************** 
      
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    do_merge;
      
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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
end wh_prf_corp_808u;
