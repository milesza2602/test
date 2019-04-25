--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_806U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_806U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2018
--  Author:      Alfonso Joshua
--  Purpose:     Rollup to location planogram using the Planogram Structure
--  Tables:      Input  - rtl_loc_item_wk_plan_disp_grp
--               Output - rtl_loc_plan_wk
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
g_rec_out            rtl_loc_plan_wk%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date   := trunc(sysdate);
g_max_date           date   := trunc(sysdate);
g_cnt                number := 0;
g_fin_year_no        number := 0;
g_fin_week_no        number := 0;
g_this_week_start_date   date   := trunc(sysdate + 1);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_806U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION PLANOGRAM EX PLANOGRAM STRUCTURE ';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure do_merge as
begin

    merge /*+ parallel (lp,4) */ into rtl_loc_plan_wk lp using (
       select /*+ full (a) parallel (a,6) */ distinct  
            sk1_location_no, 
            sk1_planogram_id, 
            fin_year_no,
            fin_week_no,
            sk1_planogram_key,
            sk1_fixture_seq_no,
            sk1_floorplan_key,
--            fixture_no,
            this_week_start_date,
            planogram_live_from_date,
            max(planogram_live_to_date) planogram_live_to_date,
            display_area_name         as planogram_department,
            company_store_no,
            planogram_fam_key,
            xcoordinate_distance,
            ycoordinate_distance,
            segment_start_no,
            segment_end_no,
            segment_linear,
            segment_square_space,
            segment_cubic_space,
            store_sqm,
            number_of_segments,
            planogram_group_name,
            planogram_subgroup_name,
            planogram_supercategory,
            planogram_category,
            planogram_subcategory,
            planogram_cluster,
            planogram_region,
            planogram_equip_type,
            planogram_channel,
            planogram_name            
       from rtl_loc_item_wk_plan_disp_grp a
       where fin_year_no = g_fin_year_no 
        and  fin_week_no = g_fin_week_no 
       group by 
            sk1_location_no, 
            sk1_planogram_id, 
            fin_year_no,
            fin_week_no,
            sk1_planogram_key,
            sk1_fixture_seq_no,
            sk1_floorplan_key,
--            fixture_no,
            this_week_start_date,
            planogram_live_from_date,
--            max(planogram_live_to_date) planogram_live_to_date,
            display_area_name,
            company_store_no,
            planogram_fam_key,
            xcoordinate_distance,
            ycoordinate_distance,
            segment_start_no,
            segment_end_no,
            segment_linear,
            segment_square_space,
            segment_cubic_space,
            store_sqm,
            number_of_segments,
            planogram_group_name,
            planogram_subgroup_name,
            planogram_supercategory,
            planogram_category,
            planogram_subcategory,
            planogram_cluster,
            planogram_region,
            planogram_equip_type,
            planogram_channel,
            planogram_name  
  ) mer_mart
  
  on (lp.sk1_location_no      = mer_mart.sk1_location_no      and
      lp.sk1_planogram_id     = mer_mart.sk1_planogram_id     and
      lp.fin_year_no          = mer_mart.fin_year_no          and
      lp.fin_week_no          = mer_mart.fin_week_no          and
      lp.sk1_floorplan_key    = mer_mart.sk1_floorplan_key    and
      lp.sk1_fixture_seq_no   = mer_mart.sk1_fixture_seq_no   and
      lp.sk1_planogram_key    = mer_mart.sk1_planogram_key
--      lp.fixture_no           = mer_mart.fixture_no
     )

  when matched then
     update 
       set this_week_start_date         = mer_mart.this_week_start_date,
           planogram_live_from_date     = mer_mart.planogram_live_from_date,
           planogram_live_to_date       = mer_mart.planogram_live_to_date,
           planogram_department         = mer_mart.planogram_department,
           company_store_no             = mer_mart.company_store_no,
--           fixture_no                   = mer_mart.fixture_no,
           planogram_fam_key            = mer_mart.planogram_fam_key,
           xcoordinate_distance         = mer_mart.xcoordinate_distance,
           ycoordinate_distance         = mer_mart.ycoordinate_distance,
           segment_start_no             = mer_mart.segment_start_no,
           segment_end_no               = mer_mart.segment_end_no,
           segment_linear               = mer_mart.segment_linear,
           segment_square_space         = mer_mart.segment_square_space,
           segment_cubic_space          = mer_mart.segment_cubic_space,
           store_sqm                    = mer_mart.store_sqm,
           number_of_segments           = mer_mart.number_of_segments,
           planogram_group_name         = mer_mart.planogram_group_name,
           planogram_subgroup_name      = mer_mart.planogram_subgroup_name,
           planogram_supercategory      = mer_mart.planogram_supercategory,
           planogram_category           = mer_mart.planogram_category,
           planogram_subcategory        = mer_mart.planogram_subcategory,
           planogram_cluster            = mer_mart.planogram_cluster,
           planogram_region             = mer_mart.planogram_region,
           planogram_equip_type         = mer_mart.planogram_equip_type,
           planogram_channel            = mer_mart.planogram_channel,
           planogram_name               = mer_mart.planogram_name,
           last_updated_date            = g_date
     
when not matched then
  insert (
           sk1_location_no, 
           sk1_planogram_id,
           fin_year_no,
           fin_week_no,
           sk1_planogram_key,
           sk1_fixture_seq_no,
           sk1_floorplan_key,
--           fixture_no,
           this_week_start_date,
           planogram_live_from_date,
           planogram_live_to_date,
           planogram_department, 
           company_store_no,
           planogram_fam_key,
           xcoordinate_distance,
           ycoordinate_distance,
           segment_start_no,
           segment_end_no,
           segment_linear,
           segment_square_space,
           segment_cubic_space,
           store_sqm,
           number_of_segments,
           planogram_group_name,
           planogram_subgroup_name,
           planogram_supercategory,
           planogram_category,
           planogram_subcategory,
           planogram_cluster,
           planogram_region,
           planogram_equip_type,
           planogram_channel,
           planogram_name,
           last_updated_date            
         )
  values (
           mer_mart.sk1_location_no, 
           mer_mart.sk1_planogram_id,
           mer_mart.fin_year_no,
           mer_mart.fin_week_no,
           mer_mart.sk1_planogram_key,
           mer_mart.sk1_fixture_seq_no,
           mer_mart.sk1_floorplan_key,
--           mer_mart.fixture_no,
           mer_mart.this_week_start_date,
           mer_mart.planogram_live_from_date,
           mer_mart.planogram_live_to_date,
           mer_mart.planogram_department, 
           mer_mart.company_store_no,
           mer_mart.planogram_fam_key,
           mer_mart.xcoordinate_distance,
           mer_mart.ycoordinate_distance,
           mer_mart.segment_start_no,
           mer_mart.segment_end_no,
           mer_mart.segment_linear,
           mer_mart.segment_square_space,
           mer_mart.segment_cubic_space,
           mer_mart.store_sqm,
           mer_mart.number_of_segments,
           mer_mart.planogram_group_name,
           mer_mart.planogram_subgroup_name,
           mer_mart.planogram_supercategory,
           mer_mart.planogram_category,
           mer_mart.planogram_subcategory,
           mer_mart.planogram_cluster,
           mer_mart.planogram_region,
           mer_mart.planogram_equip_type,
           mer_mart.planogram_channel,
           mer_mart.planogram_name,
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

    l_text := 'LOAD OF RTL_LOC_PLAN_WK EX INTACTIX STARTED AT '||
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
end wh_prf_corp_806u;
