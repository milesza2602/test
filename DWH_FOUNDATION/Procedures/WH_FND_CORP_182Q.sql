--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_182Q
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_182Q" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2017
--  Author:      Alfonso Joshua
--  Purpose:     Create Planogram Product data with input ex Intactix (foods)
--  Tables:      Input  - stg_intactix_pln_product
--               Output - w6005682.FND_PLANOGRAM_DY_PROD_Q
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_reset         integer       :=  0;
g_stg_count          integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_intactix_pln_product_hsp.sys_process_msg%type;
g_rec_out            w6005682.FND_PLANOGRAM_DY_PROD_Q%rowtype;
g_rec_in             stg_intactix_pln_product%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date          := trunc(sysdate);
g_cnt                number := 0;

g_planogram_id       stg_intactix_pln_product.planogram_id%type;
g_segment_no         stg_intactix_pln_product.segment_no%type;
g_fixture_no         stg_intactix_pln_product.fixture_no%type;
g_product_seq_no     stg_intactix_pln_product.product_seq_no%type;
g_item_no            stg_intactix_pln_product.item_no%type;
g_live_date          stg_intactix_pln_product.live_date%type;
g_db_status          stg_intactix_pln_product.db_status%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_182Q';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE PLANOGRAM PRODUCT DATA EX INTACTIX';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- order by only where sequencing is essential to the correct loading of data

--=======================================================================================
procedure do_merge as 
begin

  merge /*+ parallel (fli,4) */ into w6005682.FND_PLANOGRAM_DY_PROD_Q fli using (
     select /*+ PARALLEL(a,4) FULL(a) */
            planogram_id,
            segment_no,
            fixture_no,
            product_seq_no,
            a.item_no,
            live_date,
            db_status,
            planogram_status,
            legacy_ind,
            group_name,
            subgroup_name,
            planogram_supercategory,
            planogram_category,
            planogram_traffic_flow,
            planogram_channel,
            planogram_cluster,
            planogram_region,
            planogram_equip_type,
            segment_name,
            fixture_type,
            floor_height,
            facings_total,
            capacity_total,
            capacity_total_cases,
            replenishment_min,
            replenishment_max,
            position_seq_number,
            position_merch_style,
            position_facings_width,
            position_facings_unit_width,
            position_facings_height,
            position_facings_unit_height,
            position_facings_depth,
            position_facings_unit_depth,
            position_capacity,
            position_case_capacity,
            position_max_capacity,
            replenishment_min_position,
            replenishment_max_position,
            date_exported,
            g_date as last_updated_date
      from  stg_intactix_pln_product_cpy a, fnd_item b
      where sys_process_code = 'N'
       and  a.item_no = b.item_no
       
  ) mer_mart

  on (fli.planogram_id     = mer_mart.planogram_id
  and fli.segment_no       = mer_mart.segment_no
  and fli.fixture_no       = mer_mart.fixture_no
  and fli.product_seq_no   = mer_mart.product_seq_no
  and fli.item_no          = mer_mart.item_no
  and fli.live_date        = mer_mart.live_date
  and fli.db_status        = mer_mart.db_status
  )

when matched then
  update set
           planogram_status             = mer_mart.planogram_status,
           legacy_ind                   = mer_mart.legacy_ind,
           group_name                   = mer_mart.group_name,
           subgroup_name                = mer_mart.subgroup_name,
           planogram_supercategory      = mer_mart.planogram_supercategory,
           planogram_category           = mer_mart.planogram_category,
           planogram_traffic_flow       = mer_mart.planogram_traffic_flow,
           planogram_channel            = mer_mart.planogram_channel,
           planogram_cluster            = mer_mart.planogram_cluster,
           planogram_region             = mer_mart.planogram_region,
           planogram_equip_type         = mer_mart.planogram_equip_type,
           segment_name                 = mer_mart.segment_name,
           fixture_type                 = mer_mart.fixture_type,
           floor_height                 = mer_mart.floor_height,
           facings_total                = mer_mart.facings_total,
           capacity_total               = mer_mart.capacity_total,
           capacity_total_cases         = mer_mart.capacity_total_cases,
           replenishment_min            = mer_mart.replenishment_min,
           replenishment_max            = mer_mart.replenishment_max,
           position_seq_number          = mer_mart.position_seq_number,
           position_merch_style         = mer_mart.position_merch_style,
           position_facings_width       = mer_mart.position_facings_width,
           position_facings_unit_width  = mer_mart.position_facings_unit_width,
           position_facings_height      = mer_mart.position_facings_height,
           position_facings_unit_height = mer_mart.position_facings_unit_height,
           position_facings_depth       = mer_mart.position_facings_depth,
           position_facings_unit_depth  = mer_mart.position_facings_unit_depth,
           position_capacity            = mer_mart.position_capacity,
           position_case_capacity       = mer_mart.position_case_capacity,
           position_max_capacity        = mer_mart.position_max_capacity,
           replenishment_min_position   = mer_mart.replenishment_min_position,
           replenishment_max_position   = mer_mart.replenishment_max_position,
           date_exported                = mer_mart.date_exported,
           last_updated_date            = g_date

when not matched then
  insert (
          planogram_id,
          segment_no,
          fixture_no,
          product_seq_no,
          item_no,
          live_date,
          db_status,
          planogram_status,
          legacy_ind,
          group_name,
          subgroup_name,
          planogram_supercategory,
          planogram_category,
          planogram_traffic_flow,
          planogram_channel,
          planogram_cluster,
          planogram_region,
          planogram_equip_type,
          segment_name,
          fixture_type,
          floor_height,
          facings_total,
          capacity_total,
          capacity_total_cases,
          replenishment_min,
          replenishment_max,
          position_seq_number,
          position_merch_style,
          position_facings_width,
          position_facings_unit_width,
          position_facings_height,
          position_facings_unit_height,
          position_facings_depth,
          position_facings_unit_depth,
          position_capacity,
          position_case_capacity,
          position_max_capacity,
          replenishment_min_position,
          replenishment_max_position,
          date_exported,
          last_updated_date
         )
  values (
          mer_mart.planogram_id,
          mer_mart.segment_no,
          mer_mart.fixture_no,
          mer_mart.product_seq_no,
          mer_mart.item_no,
          mer_mart.live_date,
          mer_mart.db_status,
          mer_mart.planogram_status,
          mer_mart.legacy_ind,
          mer_mart.group_name,
          mer_mart.subgroup_name,
          mer_mart.planogram_supercategory,
          mer_mart.planogram_category,
          mer_mart.planogram_traffic_flow,
          mer_mart.planogram_channel,
          mer_mart.planogram_cluster,
          mer_mart.planogram_region,
          mer_mart.planogram_equip_type,
          mer_mart.segment_name,
          mer_mart.fixture_type,
          mer_mart.floor_height,
          mer_mart.facings_total,
          mer_mart.capacity_total,
          mer_mart.capacity_total_cases,
          mer_mart.replenishment_min,
          mer_mart.replenishment_max,
          mer_mart.position_seq_number,
          mer_mart.position_merch_style,
          mer_mart.position_facings_width,
          mer_mart.position_facings_unit_width,
          mer_mart.position_facings_height,
          mer_mart.position_facings_unit_height,
          mer_mart.position_facings_depth,
          mer_mart.position_facings_unit_depth,
          mer_mart.position_capacity,
          mer_mart.position_case_capacity,
          mer_mart.position_max_capacity,
          mer_mart.replenishment_min_position,
          mer_mart.replenishment_max_position,
          mer_mart.date_exported,
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
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF w6005682.FND_PLANOGRAM_DY_PROD_Q EX OM STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- De Duplication of the staging table to avoid Bulk insert failures
  --**************************************************************************************************

   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';
   
   l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   do_merge;
 
   l_text := 'MERGE DONE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
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
end wh_fnd_corp_182Q;
