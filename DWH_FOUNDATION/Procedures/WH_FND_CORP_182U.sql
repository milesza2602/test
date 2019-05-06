--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_182U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_182U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2017
--  Author:      Alfonso Joshua
--  Purpose:     Create Planogram Product data with input ex Intactix (foods)
--  Tables:      Input  - stg_intactix_pln_product_cpy
--               Output - fnd_planogram_wk_prod
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

-- 12C UPGRADE CHANGES MDM
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
g_rec_out            fnd_planogram_wk_prod%rowtype;
g_rec_in             stg_intactix_pln_product%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date          := trunc(sysdate);
g_load_date          date          := trunc(sysdate);
g_cnt                number := 0;

g_planogram_id       stg_intactix_pln_product.planogram_id%type;
g_segment_no         stg_intactix_pln_product.segment_no%type;
g_fixture_no         stg_intactix_pln_product.fixture_no%type;
g_product_seq_no     stg_intactix_pln_product.product_seq_no%type;
g_item_no            stg_intactix_pln_product.item_no%type;
g_live_date          stg_intactix_pln_product.live_date%type;
g_db_status          stg_intactix_pln_product.db_status%type;
g_planogram_db_key   stg_intactix_pln_product.planogram_key%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_182U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE PLANOGRAM PRODUCT DATA EX INTACTIX';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
      select * from stg_intactix_pln_product_cpy
      where (planogram_id, segment_no, fixture_no, product_seq_no, item_no, live_date, db_status, planogram_key)
      in
     (select planogram_id, segment_no, fixture_no, product_seq_no, item_no, live_date, db_status, planogram_key
      from stg_intactix_pln_product_cpy
      where sys_process_code = 'N'
      group by planogram_id, segment_no, fixture_no, product_seq_no, item_no, live_date, db_status, planogram_key
      having count(*) > 1)
      order by planogram_id, segment_no, fixture_no, product_seq_no, item_no, live_date, db_status, planogram_key,
      sys_source_batch_id desc ,sys_source_sequence_no desc;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_bulk_merge as
begin

  merge /*+ parallel (fli,4) */ into fnd_planogram_wk_prod fli using (
--  merge /*+ parallel (fli,4) */ into dwh_datafix.aj_fnd_planogram_dy_prod fli using (
     select /*+ PARALLEL(a,4) FULL(a) */
            planogram_id,
            c.fin_year_no,
            c.fin_week_no,
            segment_no,
            fixture_no,
            product_seq_no,
            a.item_no,
            live_date,
            db_status,
            planogram_key  as planogram_db_key,
            live_date      as planogram_live_from_date,
            planogram_status,
            legacy_ind,
            group_name,
            subgroup_name,
            planogram_supercategory,
            planogram_category,
            planogram_subcategory,
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
            width,
            height,
            depth,
            planogram_width_linear,
            number_of_fixtures,
            shelf_width,
            g_date      as last_updated_date
      from  stg_intactix_pln_product_cpy a, fnd_item b, dim_calendar c
      where a.item_no        = b.item_no
       and  c.calendar_date  = g_load_date
       and  a.sys_process_code = 'N'
       and planogram_key is not null -- fix SS 27/01/2019 @ 9:17pm
  ) mer_mart

  on (fli.planogram_id     = mer_mart.planogram_id
  and fli.fin_year_no      = mer_mart.fin_year_no
  and fli.fin_week_no      = mer_mart.fin_week_no
  and fli.segment_no       = mer_mart.segment_no
  and fli.fixture_no       = mer_mart.fixture_no
  and fli.product_seq_no   = mer_mart.product_seq_no
  and fli.item_no          = mer_mart.item_no
  and fli.live_date        = mer_mart.live_date
  and fli.db_status        = mer_mart.db_status
  and fli.planogram_db_key = mer_mart.planogram_db_key
  )

when matched then
  update set
           planogram_live_from_date     = mer_mart.planogram_live_from_date,
           planogram_status             = mer_mart.planogram_status,
           legacy_ind                   = mer_mart.legacy_ind,
           group_name                   = mer_mart.group_name,
           subgroup_name                = mer_mart.subgroup_name,
           planogram_supercategory      = mer_mart.planogram_supercategory,
           planogram_category           = mer_mart.planogram_category,
           planogram_subcategory        = mer_mart.planogram_subcategory,
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
           width                        = mer_mart.width,
           height                       = mer_mart.height,
           depth                        = mer_mart.depth,
           planogram_width_linear       = mer_mart.planogram_width_linear,
           number_of_fixtures           = mer_mart.number_of_fixtures,
           shelf_width                  = mer_mart.shelf_width,
           last_updated_date            = g_date

when not matched then
  insert (
          planogram_id,
          fin_year_no,
          fin_week_no,
          segment_no,
          fixture_no,
          product_seq_no,
          item_no,
          live_date,
          db_status,
          planogram_db_key,
          planogram_live_from_date,
          planogram_live_to_date,
          planogram_status,
          legacy_ind,
          group_name,
          subgroup_name,
          planogram_supercategory,
          planogram_category,
          planogram_subcategory,
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
          width,
          height,
          depth,
          planogram_width_linear,
          number_of_fixtures,
          shelf_width,
          last_updated_date
         )
  values (
          mer_mart.planogram_id,
          mer_mart.fin_year_no,
          mer_mart.fin_week_no,
          mer_mart.segment_no,
          mer_mart.fixture_no,
          mer_mart.product_seq_no,
          mer_mart.item_no,
          mer_mart.live_date,
          mer_mart.db_status,
          mer_mart.planogram_db_key,
          mer_mart.planogram_live_from_date,
          '31 DEC 3999',
          mer_mart.planogram_status,
          mer_mart.legacy_ind,
          mer_mart.group_name,
          mer_mart.subgroup_name,
          mer_mart.planogram_supercategory,
          mer_mart.planogram_category,
          mer_mart.planogram_subcategory,
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
          mer_mart.width,
          mer_mart.height,
          mer_mart.depth,
          mer_mart.planogram_width_linear,
          mer_mart.number_of_fixtures,
          mer_mart.shelf_width,
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

end local_bulk_merge;

--**************************************************************************************************
-- Validate active live dates
--**************************************************************************************************
procedure local_bulk_active_update as
begin

  merge into fnd_planogram_wk_prod a                                                        
          using ( 
            with 
                minrec as (
                   select distinct
                      planogram_id,
                      segment_no,
                      fixture_no,
                      product_seq_no,
                      item_no,
                      live_date,
                      db_status,
                      min(planogram_db_key)         as planogram_db_key,
                      min(planogram_live_from_date) as planogram_live_from_date                      
                   from
                      fnd_planogram_wk_prod 
                   where planogram_live_to_date = '31/DEC/3999'                   
                   group by 
                      planogram_id,
                      segment_no,
                      fixture_no,
                      product_seq_no,
                      item_no,
                      live_date,
                      db_status
                    ),
                maxrec as (
                   select distinct
                      planogram_id,
                      segment_no,
                      fixture_no,
                      product_seq_no,
                      item_no,
                      live_date,
                      db_status,
                      max(planogram_db_key)         as planogram_db_key,
                      max(planogram_live_from_date) as planogram_live_from_date                      
                   from
                      fnd_planogram_wk_prod
                    where planogram_live_to_date = '31/DEC/3999'
                    group by 
                       planogram_id,
                       segment_no,
                       fixture_no,
                       product_seq_no,
                       item_no,
                       live_date,
                       db_status
                    )
                    select mins.planogram_id,
                           mins.segment_no,
                           mins.fixture_no,
                           mins.product_seq_no,
                           mins.item_no,
                           mins.live_date,
                           mins.db_status,
                           mins.planogram_db_key,
                           maxs.planogram_live_from_date  as maxplan_date
                    from maxrec maxs,
                         minrec mins
                    where maxs.item_no           = mins.item_no
                      and maxs.planogram_id      = mins.planogram_id
                      and maxs.segment_no        = mins.segment_no
                      and maxs.fixture_no        = mins.fixture_no
                      and maxs.product_seq_no    = mins.product_seq_no
                      and maxs.live_date         = mins.live_date
                      and maxs.db_status         = mins.db_status
                      and maxs.planogram_db_key <> mins.planogram_db_key
                )b
            on    
               (a.item_no		       = b.item_no        and 
                a.planogram_id	   = b.planogram_id   and
                a.segment_no	     = b.segment_no     and
                a.fixture_no	     = b.fixture_no     and
                a.product_seq_no   = b.product_seq_no and
                a.live_date        = b.live_date      and
                a.db_status        = b.db_status      and
                a.planogram_db_key = b.planogram_db_key
                 )            
            when matched then 
               update set 
                   a.planogram_live_to_date = b.maxplan_date - 1;
                  
                   
  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + sql%rowcount;
  
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

end local_bulk_active_update;

--**************************************************************************************************
-- Process error handling
--**************************************************************************************************
procedure local_bulk_error_handling as
begin

  insert /*+ APPEND parallel (hsp,2) */ into stg_intactix_pln_product_hsp hsp
   select /*+ FULL(TMP) */  tmp.sys_source_batch_id,
                            tmp.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            tmp.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM',
                            tmp.planogram_id,
                            tmp.segment_no,
                            tmp.fixture_no,
                            tmp.product_seq_no,
                            tmp.item_no,
                            tmp.live_date,
                            tmp.db_status,
                            tmp.planogram_status,
                            tmp.legacy_ind,
                            tmp.group_name,
                            tmp.subgroup_name,
                            tmp.planogram_supercategory,
                            tmp.planogram_category,
                            tmp.planogram_traffic_flow,
                            tmp.planogram_channel,
                            tmp.planogram_cluster,
                            tmp.planogram_region,
                            tmp.planogram_equip_type,
                            tmp.segment_name,
                            tmp.fixture_type,
                            tmp.floor_height,
                            tmp.facings_total,
                            tmp.capacity_total,
                            tmp.capacity_total_cases,
                            tmp.replenishment_min,
                            tmp.replenishment_max,
                            tmp.position_seq_number,
                            tmp.position_merch_style,
                            tmp.position_facings_width,
                            tmp.position_facings_unit_width,
                            tmp.position_facings_height,
                            tmp.position_facings_unit_height,
                            tmp.position_facings_depth,
                            tmp.position_facings_unit_depth,
                            tmp.position_capacity,
                            tmp.position_case_capacity,
                            tmp.position_max_capacity,
                            tmp.replenishment_min_position,
                            tmp.replenishment_max_position,
                            tmp.date_exported,
                            tmp.width,
                            tmp.height,
                            tmp.depth,
                            tmp.planogram_width_linear,
                            tmp.number_of_fixtures,
                            tmp.shelf_width,
                            tmp.planogram_subcategory,
                            tmp.planogram_key

    from  stg_intactix_pln_product_cpy  tmp
    where (
         not exists
           (select *
            from   fnd_item di
            where  tmp.item_no       = di.item_no )
         )
          and sys_process_code = 'N'
         ;

    g_recs_hospital := g_recs_hospital + sql%rowcount;

    commit;

    l_text := 'HOSPITALISATION CHECKS ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'Hospital Records - '||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end local_bulk_error_handling;

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

    l_text := 'LOAD OF FND_PLANOGRAM_DY_PROD EX OM STARTED AT '||
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
  -- A&S is future week data - data loaded as next week (g_date + 1)
  --**************************************************************************************************
  
    g_load_date := g_date + 1;
   
  --**************************************************************************************************
  -- De Duplication of the staging table to avoid Bulk insert failures
  --**************************************************************************************************
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   g_planogram_id       := 0;
   g_segment_no         := 0;
   g_fixture_no         := 0;
   g_product_seq_no     := 0;
   g_item_no            := 0;
   g_live_date          := '';
   g_db_status          := 0;

    for dupp_record in stg_dup
       loop

        if  dupp_record.planogram_id       = g_planogram_id and
            dupp_record.segment_no         = g_segment_no and
            dupp_record.fixture_no         = g_fixture_no and
            dupp_record.product_seq_no     = g_product_seq_no and
            dupp_record.item_no            = g_item_no and
            dupp_record.live_date          = g_live_date and
            dupp_record.db_status          = g_db_status then
            update stg_intactix_pln_product_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;

            g_recs_duplicate  := g_recs_duplicate  + 1;
        end if;

        g_planogram_id      := dupp_record.planogram_id;
        g_segment_no        := dupp_record.segment_no;
        g_fixture_no        := dupp_record.fixture_no;
        g_product_seq_no    := dupp_record.product_seq_no;
        g_item_no           := dupp_record.item_no;
        g_live_date         := dupp_record.live_date;
        g_db_status         := dupp_record.db_status;

    end loop;

    commit;

    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'Duplicate Records - '||g_recs_duplicate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';
   
   l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   local_bulk_merge;
   
   l_text := 'MERGE ENDING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   l_text := 'MERGE DATE UPDATES STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   local_bulk_active_update;
   
   l_text := 'MERGE DATE UPDATES ENDING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   l_text := 'STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   local_bulk_error_handling;

   l_text := 'ENDING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
end wh_fnd_corp_182u;
