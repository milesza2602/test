--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_804U_TST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_804U_TST" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2018
--  Author:      Alfonso Joshua
--  Purpose:     Create single view Planogram Structure for Display Group data ex Intactix(CKM) Non Transactional Tables
--  Tables:      Input  - fnd_loc_planogram_wk
--                        fnd_loc_display_wk
--                        fnd_planogram_wk_prod
--               Output - rtl_loc_item_wk_plan_disp_grp
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
g_rec_out            rtl_loc_item_wk_plan_disp_grp%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date   := trunc(sysdate);
g_max_date           date   := trunc(sysdate);
g_cnt                number := 0;
g_fin_year_no        number := 0;
g_fin_week_no        number := 0;
g_this_week_start_date   date   := trunc(sysdate + 1);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_804U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE PLANOGRAM DISPLAY GROUP DATA EX INTACTIX ';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure do_merge as
begin

    merge /*+ parallel (ilp,4) */ into rtl_loc_item_wk_plan_disp_grp ilp using (
       select /*+ full (a) parallel (a,6) full (b) parallel (b,6) full (c) parallel (c,6) full (d) full (e) */ 
            distinct  
            d.sk1_item_no,
            e.sk1_location_no, 
            g_fin_year_no              as fin_year_no,
            g_fin_week_no              as fin_week_no,
            g_this_week_start_date     as this_week_start_date,
            a.planogram_id             as sk1_planogram_id, 
            c.planogram_db_key         as sk1_planogram_key,
            a.fixture_seq_no           as sk1_fixture_seq_no,
            a.floorplan_key            as sk1_floorplan_key,
            b.display_area_name,  
            c.fixture_no,
            c.product_seq_no,
            c.planogram_live_from_date,
            c.planogram_live_to_date,
            a.floorplan_live_from_date,
            a.floorplan_live_to_date,
            c.group_name               as planogram_group_name,
            c.subgroup_name            as planogram_subgroup_name,
            c.planogram_supercategory,
            c.planogram_category,
            c.planogram_subcategory,
            c.planogram_cluster,
            c.planogram_region,
            c.planogram_equip_type,
            c.planogram_channel,
            a.planogram_name,
            c.fixture_type,
            b.market_type,
            b.market_size,
            b.trading_sqm,
            b.store_profile,
            b.node,
            b.ww_catchment_area,
            b.province,
            b.fsa_indicator,
            b.longitude,
            b.latitude,
            c.position_merch_style,
            a.company_store_no,
            a.planogram_fam_key,
            c.segment_no,
            c.facings_total,
            c.capacity_total,
            c.capacity_total_cases,
            c.position_seq_number,
            c.position_facings_width,
            c.position_facings_height,
            c.position_facings_depth,
            c.position_facings_unit_width,
            c.position_facings_unit_height,
            c.position_facings_unit_depth,
            c.position_capacity,
            c.position_case_capacity,
            c.position_max_capacity,
            c.width,
            c.height,
            c.depth,
            c.planogram_width_linear,
            c.shelf_width,
            c.number_of_fixtures,
            a.segment_start_no,
            a.segment_end_no,
            a.segment_linear,
            a.segment_square_space,
            a.segment_cubic_space,
            a.number_of_segments,
            sum(b.display_area) display_area,
            sum(b.fixture_count) fixture_count,
            sum(b.fixture_linear) fixture_linear,
            sum(b.fixture_area) fixture_area,
            sum(b.section_count_no) section_count_no,
            sum(b.section_linear) section_linear,
            sum(b.section_area) section_area,
            sum(b.fixture_density) fixture_density,
            sum(b.section_density) section_density,
            sum(b.foods_selling_sqm) foods_selling_sqm,
            a.xcoordinate_distance,
            a.ycoordinate_distance,
            a.store_sqm,
            a.floorplan_display_method,
            a.spaceplan_display_method,
            a.size_break,
            a.floorplan_seq_no
       from dwh_foundation.fnd_loc_planogram_wk a,
            dwh_foundation.fnd_loc_display_wk b,
            dwh_foundation.fnd_planogram_wk_prod c, 
            dim_item d,
            dim_location e
       where --a.location_no = 103 and
            a.fin_year_no          = g_fin_year_no
        and a.fin_week_no          = g_fin_week_no
        and a.fin_year_no          = b.fin_year_no
        and a.fin_week_no          = b.fin_week_no
        and a.fin_year_no          = c.fin_year_no
        and a.fin_week_no          = c.fin_week_no
        and a.location_no          = e.location_no 
        and a.location_no          = b.location_no
        and a.floorplan_key        = b.floorplan_key
        and a.planogram_department = b.display_area_name 
        and a.planogram_id         = c.planogram_id
        and a.planogram_db_key     = c.planogram_db_key
        and c.item_no              = d.item_no
        and d.business_unit_no     = 50
       group by 
            d.sk1_item_no,
            e.sk1_location_no, 
            g_fin_year_no,
            g_fin_week_no,
            g_this_week_start_date,
            a.planogram_id, 
            c.planogram_db_key,
            a.fixture_seq_no,
            a.floorplan_key,
            b.display_area_name,  
            c.fixture_no,
            c.product_seq_no,
            c.planogram_live_from_date,
            c.planogram_live_to_date,
            a.floorplan_live_from_date,
            a.floorplan_live_to_date,
            c.group_name,
            c.subgroup_name,
            c.planogram_supercategory,
            c.planogram_category,
            c.planogram_subcategory,
            c.planogram_cluster,
            c.planogram_region,
            c.planogram_equip_type,
            c.planogram_channel,
            a.planogram_name,
            c.fixture_type,
            b.market_type,
            b.market_size,
            b.trading_sqm,
            b.store_profile,
            b.node,
            b.ww_catchment_area,
            b.province,
            b.fsa_indicator,
            b.longitude,
            b.latitude,
            c.position_merch_style,
            a.company_store_no,
            a.planogram_fam_key,
            c.segment_no,
            c.facings_total,
            c.capacity_total,
            c.capacity_total_cases,
            c.position_seq_number,
            c.position_facings_width,
            c.position_facings_height,
            c.position_facings_depth,
            c.position_facings_unit_width,
            c.position_facings_unit_height,
            c.position_facings_unit_depth,
            c.position_capacity,
            c.position_case_capacity,
            c.position_max_capacity,
            c.width,
            c.height,
            c.depth,
            c.planogram_width_linear,
            c.shelf_width,
            c.number_of_fixtures,
            a.segment_start_no,
            a.segment_end_no,
            a.segment_linear,
            a.segment_square_space,
            a.segment_cubic_space,
            a.number_of_segments,
            a.xcoordinate_distance,
            a.ycoordinate_distance,
            a.store_sqm,
            a.floorplan_display_method,
            a.spaceplan_display_method,
            a.size_break,
            a.floorplan_seq_no
  ) mer_mart

  on (ilp.sk1_item_no          = mer_mart.sk1_item_no          and
      ilp.sk1_location_no      = mer_mart.sk1_location_no      and
      ilp.fin_year_no          = mer_mart.fin_year_no          and
      ilp.fin_week_no          = mer_mart.fin_week_no          and
      ilp.sk1_planogram_id     = mer_mart.sk1_planogram_id     and 
      ilp.sk1_planogram_key    = mer_mart.sk1_planogram_key    and
      ilp.sk1_fixture_seq_no   = mer_mart.sk1_fixture_seq_no   and
      ilp.sk1_floorplan_key    = mer_mart.sk1_floorplan_key    and
      ilp.display_area_name    = mer_mart.display_area_name    and
      ilp.fixture_no           = mer_mart.fixture_no           and
      ilp.product_seq_no       = mer_mart.product_seq_no    
     )

  when matched then
     update 
       set this_week_start_date         = mer_mart.this_week_start_date,
           planogram_live_from_date     = mer_mart.planogram_live_from_date,
           planogram_live_to_date       = mer_mart.planogram_live_to_date,
           floorplan_live_from_date     = mer_mart.floorplan_live_from_date,
           floorplan_live_to_date       = mer_mart.floorplan_live_to_date,
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
           fixture_type                 = mer_mart.fixture_type,
           market_type                  = mer_mart.market_type,
           market_size                  = mer_mart.market_size,
           trading_sqm                  = mer_mart.trading_sqm,
           store_profile                = mer_mart.store_profile,
           node                         = mer_mart.node,
           ww_catchment_area            = mer_mart.ww_catchment_area,
           province                     = mer_mart.province,
           fsa_indicator                = mer_mart.fsa_indicator,
           longitude                    = mer_mart.longitude,
           latitude                     = mer_mart.latitude,
           position_merch_style         = mer_mart.position_merch_style,
           segment_no                   = mer_mart.segment_no,
           facings_total                = mer_mart.facings_total,
           capacity_total               = mer_mart.capacity_total,
           capacity_total_cases         = mer_mart.capacity_total_cases,
           position_seq_number          = mer_mart.position_seq_number,
           position_facings_width       = mer_mart.position_facings_width,
           position_facings_height      = mer_mart.position_facings_height,
           position_facings_depth       = mer_mart.position_facings_depth,
           position_facings_unit_width  = mer_mart.position_facings_unit_width,
           position_facings_unit_height = mer_mart.position_facings_unit_height,
           position_facings_unit_depth  = mer_mart.position_facings_unit_depth,
           position_capacity            = mer_mart.position_capacity,
           position_case_capacity       = mer_mart.position_case_capacity,
           position_max_capacity        = mer_mart.position_max_capacity,
           width                        = mer_mart.width,
           height                       = mer_mart.height,
           depth                        = mer_mart.depth,
           planogram_width_linear       = mer_mart.planogram_width_linear,
           shelf_width                  = mer_mart.shelf_width,
           number_of_fixtures           = mer_mart.number_of_fixtures,
           segment_start_no             = mer_mart.segment_start_no,
           segment_end_no               = mer_mart.segment_end_no,
           segment_linear               = mer_mart.segment_linear,
           segment_square_space         = mer_mart.segment_square_space,
           segment_cubic_space          = mer_mart.segment_cubic_space,
           number_of_segments           = mer_mart.number_of_segments,
           display_area                 = mer_mart.display_area,
           fixture_count                = mer_mart.fixture_count,
           fixture_linear               = mer_mart.fixture_linear,
           fixture_area                 = mer_mart.fixture_area,
           section_count_no             = mer_mart.section_count_no,
           section_linear               = mer_mart.section_linear,
           section_area                 = mer_mart.section_area,
           fixture_density              = mer_mart.fixture_density,
           section_density              = mer_mart.section_density,
           foods_selling_sqm            = mer_mart.foods_selling_sqm,
           company_store_no             = mer_mart.company_store_no,
           planogram_fam_key            = mer_mart.planogram_fam_key,
           xcoordinate_distance         = mer_mart.xcoordinate_distance,
           ycoordinate_distance         = mer_mart.ycoordinate_distance,
           store_sqm                    = mer_mart.store_sqm,
           last_updated_date            = g_date,
           floorplan_display_method     = mer_mart.floorplan_display_method,
           spaceplan_display_method     = mer_mart.spaceplan_display_method,
           size_break                   = mer_mart.size_break,
           floorplan_seq_no             = mer_mart.floorplan_seq_no

when not matched then
  insert (           
           sk1_location_no,         
           sk1_item_no,
           fin_year_no,
           fin_week_no,
           sk1_planogram_id, 
           sk1_planogram_key,
           sk1_fixture_seq_no,
           sk1_floorplan_key,
           display_area_name, 
           fixture_no,
           product_seq_no,
           this_week_start_date,
           planogram_live_from_date,
           planogram_live_to_date,
           floorplan_live_from_date,
           floorplan_live_to_date,
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
           fixture_type,
           market_type,
           market_size,
           trading_sqm,
           store_profile,
           node,
           ww_catchment_area,
           province,
           fsa_indicator,
           longitude,
           latitude,
           position_merch_style,
           segment_no,
           facings_total,
           capacity_total,
           capacity_total_cases,
           position_seq_number,
           position_facings_width,
           position_facings_height,
           position_facings_depth,
           position_facings_unit_width,
           position_facings_unit_height,
           position_facings_unit_depth,
           position_capacity,
           position_case_capacity,
           position_max_capacity,
           width,
           height,
           depth,
           planogram_width_linear,
           shelf_width,
           number_of_fixtures,
           segment_start_no,
           segment_end_no,
           segment_linear,
           segment_square_space,
           segment_cubic_space,
           number_of_segments,
           display_area,
           fixture_count,
           fixture_linear,
           fixture_area,
           section_count_no,
           section_linear,
           section_area,
           fixture_density,
           section_density,
           foods_selling_sqm,
           company_store_no,
           xcoordinate_distance,
           ycoordinate_distance,
           planogram_fam_key,
           store_sqm,
           last_updated_date,
           floorplan_display_method,
           spaceplan_display_method,
           size_break,
           floorplan_seq_no
         )
  values (
           mer_mart.sk1_location_no,
           mer_mart.sk1_item_no,
           mer_mart.fin_year_no,
           mer_mart.fin_week_no,
           mer_mart.sk1_planogram_id,
           mer_mart.sk1_planogram_key,
           mer_mart.sk1_fixture_seq_no,
           mer_mart.sk1_floorplan_key,
           mer_mart.display_area_name,
           mer_mart.fixture_no,
           mer_mart.product_seq_no,
           mer_mart.this_week_start_date,
           mer_mart.planogram_live_from_date,
           mer_mart.planogram_live_to_date,
           mer_mart.floorplan_live_from_date,
           mer_mart.floorplan_live_to_date,
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
           mer_mart.fixture_type,
           mer_mart.market_type,
           mer_mart.market_size,
           mer_mart.trading_sqm,
           mer_mart.store_profile,
           mer_mart.node,
           mer_mart.ww_catchment_area,
           mer_mart.province,
           mer_mart.fsa_indicator,
           mer_mart.longitude,
           mer_mart.latitude,
           mer_mart.position_merch_style,
           mer_mart.segment_no,
           mer_mart.facings_total,
           mer_mart.capacity_total,
           mer_mart.capacity_total_cases,
           mer_mart.position_seq_number,
           mer_mart.position_facings_width,
           mer_mart.position_facings_height,
           mer_mart.position_facings_depth,
           mer_mart.position_facings_unit_width,
           mer_mart.position_facings_unit_height,
           mer_mart.position_facings_unit_depth,
           mer_mart.position_capacity,
           mer_mart.position_case_capacity,
           mer_mart.position_max_capacity,
           mer_mart.width,
           mer_mart.height,
           mer_mart.depth,
           mer_mart.planogram_width_linear,
           mer_mart.shelf_width,
           mer_mart.number_of_fixtures,
           mer_mart.segment_start_no,
           mer_mart.segment_end_no,
           mer_mart.segment_linear,
           mer_mart.segment_square_space,
           mer_mart.segment_cubic_space,
           mer_mart.number_of_segments,
           mer_mart.display_area,
           mer_mart.fixture_count,
           mer_mart.fixture_linear,
           mer_mart.fixture_area,
           mer_mart.section_count_no,
           mer_mart.section_linear,
           mer_mart.section_area,
           mer_mart.fixture_density,
           mer_mart.section_density,
           mer_mart.foods_selling_sqm, 
           mer_mart.company_store_no,
           mer_mart.xcoordinate_distance,
           mer_mart.ycoordinate_distance,
           mer_mart.planogram_fam_key,
           mer_mart.store_sqm,
           g_date,
           mer_mart.floorplan_display_method,
           mer_mart.spaceplan_display_method,
           mer_mart.size_break,
           mer_mart.floorplan_seq_no
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

    l_text := 'LOAD OF RTL_LOC_ITEM_WK_PLAN_DISP_GRP EX INTACTIX STARTED AT '||
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
    g_date := '3 mar 19';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  

    select this_week_start_date, fin_year_no, fin_week_no
    into g_this_week_start_date, g_fin_year_no, g_fin_week_no
    from dim_calendar
    where calendar_date = g_date + 1;  --(data load for future/current week of planogram datasend)
--    where calendar_date = g_date; --remove

    l_text := 'Load Data = '||g_fin_year_no||' - '||g_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
end wh_prf_corp_804u_tst;
