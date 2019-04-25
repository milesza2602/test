--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_184U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_184U_FIX" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2017
--  Author:      Alfonso Joshua
--  Purpose:     Create Planogram Store (PLAN STORE) data with input ex Intactix (foods)
--  Tables:      Input  - stg_intactix_pln_store_cpy
--               Output - fnd_loc_planogram_wk
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
g_hospital_text      stg_intactix_pln_store_hsp.sys_process_msg%type;
g_rec_out            fnd_loc_planogram_wk%rowtype;
g_rec_in             stg_intactix_pln_store_cpy%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date          := trunc(sysdate);
g_load_date          date          := trunc(sysdate);
g_cnt                number := 0;
g_location_no           stg_intactix_pln_store_cpy.location_no%type;
g_planogram_id          stg_intactix_pln_store_cpy.planogram_id%type;
g_planogram_status      stg_intactix_pln_store_cpy.planogram_status%type;
g_live_date             stg_intactix_pln_store_cpy.live_date%type;
g_floorplan_key         stg_intactix_pln_store_cpy.floorplan_key%type;
g_planogram_department  stg_intactix_pln_store_cpy.planogram_department%type;
g_fixture_seq_no        stg_intactix_pln_store_cpy.fixture_seq_no%type;
g_planogram_db_key      stg_intactix_pln_store_cpy.planogram_db_key%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_184U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE PLANOGRAM STORE DATA EX INTACTIX';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
      select * from stg_intactix_pln_store_arc
      where  (location_no, planogram_id, planogram_status, live_date, floorplan_key ,planogram_department, 
             fixture_seq_no, planogram_db_key)
      in
     (select location_no, planogram_id, planogram_status, live_date, floorplan_key, planogram_department, 
             fixture_seq_no, planogram_db_key
      from stg_intactix_pln_store_arc
      where sys_process_code = 'N' 
      group by location_no, planogram_id, planogram_status, live_date, floorplan_key, planogram_department, 
               fixture_seq_no, planogram_db_key
      having count(*) > 1)
      order by location_no, planogram_id, planogram_status, live_date, floorplan_key, planogram_department, 
               fixture_seq_no, planogram_db_key,
               sys_source_batch_id desc ,sys_source_sequence_no desc;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_bulk_merge as
begin

  merge /*+ parallel (fli,4) */ into dwh_foundation.fnd_loc_planogram_wk fli using (
     select /*+ PARALLEL(a,4) FULL(a) */
            a.location_no,
            planogram_id,
            c.fin_year_no,
            c.fin_week_no,
            planogram_status,
            live_date,
            floorplan_key, 
            nvl(planogram_department,'UNALLOCATED DEPARTMENT') planogram_department,
            fixture_seq_no,
            planogram_db_key,
            live_date      as floorplan_live_from_date,
            company_store_no,
            xcoordinate_distance,
            ycoordinate_distance,
            floorplan_seq_no,
            planogram_fam_key,
            planogram_name,
            planogram_legacy_ind,
            group_name,
            subgroup_name,
            planogram_supercategory,
            planogram_category,
            planogram_traffic_flow,
            planogram_channel,
            planogram_cluster,
            planogram_region,
            planogram_equip_type,
            segment_start_no,
            segment_end_no,
            segment_linear,
            segment_square_space,
            segment_cubic_space,
            store_sqm,
            date_exported,
            number_of_segments,
            planogram_subcategory,
            g_date as last_updated_date
      from  stg_intactix_pln_store_arc a, fnd_location b, dim_calendar c
      where a.sys_process_code = 'N'
       and  a.location_no    = b.location_no
       and  c.calendar_date  = g_load_date
--       and  a.planogram_department is not null
  ) mer_mart

  on (fli.location_no           = mer_mart.location_no
  and fli.planogram_id          = mer_mart.planogram_id
  and fli.fin_year_no           = mer_mart.fin_year_no
  and fli.fin_week_no           = mer_mart.fin_week_no
  and fli.planogram_status      = mer_mart.planogram_status
  and fli.live_date             = mer_mart.live_date
  and fli.floorplan_key         = mer_mart.floorplan_key
  and fli.planogram_department  = mer_mart.planogram_department
  and fli.fixture_seq_no        = mer_mart.fixture_seq_no
  and fli.planogram_db_key      = mer_mart.planogram_db_key
  )

when matched then
  update set
           floorplan_live_from_date = mer_mart.floorplan_live_from_date,
           company_store_no         = mer_mart.company_store_no,
           xcoordinate_distance     = mer_mart.xcoordinate_distance,
           ycoordinate_distance     = mer_mart.ycoordinate_distance,
           floorplan_seq_no         = mer_mart.floorplan_seq_no,
           planogram_fam_key        = mer_mart.planogram_fam_key,
           planogram_name           = mer_mart.planogram_name,
           planogram_legacy_ind     = mer_mart.planogram_legacy_ind,
           group_name               = mer_mart.group_name,
           subgroup_name            = mer_mart.subgroup_name,
           planogram_supercategory  = mer_mart.planogram_supercategory,
           planogram_category       = mer_mart.planogram_category,
           planogram_traffic_flow   = mer_mart.planogram_traffic_flow,
           planogram_channel        = mer_mart.planogram_channel,
           planogram_cluster        = mer_mart.planogram_cluster,
           planogram_region         = mer_mart.planogram_region,
           planogram_equip_type     = mer_mart.planogram_equip_type,
           segment_start_no         = mer_mart.segment_start_no,
           segment_end_no           = mer_mart.segment_end_no,
           segment_linear           = mer_mart.segment_linear,
           segment_square_space     = mer_mart.segment_square_space,
           segment_cubic_space      = mer_mart.segment_cubic_space,
           store_sqm                = mer_mart.store_sqm,
           date_exported            = mer_mart.date_exported,
           number_of_segments       = mer_mart.number_of_segments,
           planogram_subcategory    = mer_mart.planogram_subcategory,
           last_updated_date        = g_date

when not matched then
  insert (location_no,
          planogram_id,
          fin_year_no,
          fin_week_no,
          planogram_status,
          live_date,
          floorplan_key,
          planogram_department,
          fixture_seq_no,
          planogram_db_key,
          floorplan_live_from_date,
          floorplan_live_to_date,
          company_store_no,
          xcoordinate_distance,
          ycoordinate_distance,
          floorplan_seq_no,
          planogram_fam_key,
          planogram_name,
          planogram_legacy_ind,
          group_name,
          subgroup_name,
          planogram_supercategory,
          planogram_category,
          planogram_traffic_flow,
          planogram_channel,
          planogram_cluster,
          planogram_region,
          planogram_equip_type,
          segment_start_no,
          segment_end_no,
          segment_linear,
          segment_square_space,
          segment_cubic_space,
          store_sqm,
          date_exported,
          number_of_segments,
          planogram_subcategory,
          last_updated_date
         )
  values (mer_mart.location_no,
          mer_mart.planogram_id,
          mer_mart.fin_year_no,
          mer_mart.fin_week_no,
          mer_mart.planogram_status,
          mer_mart.live_date,
          mer_mart.floorplan_key,
          mer_mart.planogram_department,
          mer_mart.fixture_seq_no,
          mer_mart.planogram_db_key,
          mer_mart.floorplan_live_from_date,
          '31 DEC 3999',
          mer_mart.company_store_no,
          mer_mart.xcoordinate_distance,
          mer_mart.ycoordinate_distance,
          mer_mart.floorplan_seq_no,
          mer_mart.planogram_fam_key,
          mer_mart.planogram_name,
          mer_mart.planogram_legacy_ind,
          mer_mart.group_name,
          mer_mart.subgroup_name,
          mer_mart.planogram_supercategory,
          mer_mart.planogram_category,
          mer_mart.planogram_traffic_flow,
          mer_mart.planogram_channel,
          mer_mart.planogram_cluster,
          mer_mart.planogram_region,
          mer_mart.planogram_equip_type,
          mer_mart.segment_start_no,
          mer_mart.segment_end_no,
          mer_mart.segment_linear,
          mer_mart.segment_square_space,
          mer_mart.segment_cubic_space,
          mer_mart.store_sqm,
          mer_mart.date_exported,
          mer_mart.number_of_segments,
          mer_mart.planogram_subcategory,
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

  merge into fnd_loc_planogram_wk a                                                        
          using ( 
            with 
                minrec as (
                   select distinct
                      location_no,
                      planogram_id,
                      planogram_status,
                      live_date,
--                      floorplan_key,
                      planogram_department,
                      fixture_seq_no,
--                      min(planogram_db_key)         as planogram_db_key,
                      min(floorplan_key)            as floorplan_key,
                      min(floorplan_live_from_date) as floorplan_live_from_date                      
                   from
                      fnd_loc_planogram_wk 
                   where floorplan_live_to_date = '31/DEC/3999'                   
                   group by 
                      location_no,
                      planogram_id,
                      planogram_status,
                      live_date,
--                      floorplan_key,
                      planogram_department,
                      fixture_seq_no
                    ),
                maxrec as (
                   select distinct
                      location_no,
                      planogram_id,
                      planogram_status,
                      live_date,
--                      floorplan_key,
                      planogram_department,
                      fixture_seq_no,
--                      max(planogram_db_key)         as planogram_db_key,
                      max(floorplan_key)            as floorplan_key,
                      max(floorplan_live_from_date) as floorplan_live_from_date                      
                   from
                      fnd_loc_planogram_wk 
                   where floorplan_live_to_date = '31/DEC/3999'                   
                   group by 
                      location_no,
                      planogram_id,
                      planogram_status,
                      live_date,
--                      floorplan_key,
                      planogram_department,
                      fixture_seq_no
                    )
                    select mins.location_no,
                           mins.planogram_id,
                           mins.planogram_status,
                           mins.live_date,
                           mins.floorplan_key,
                           mins.planogram_department,
                           mins.fixture_seq_no,
--                           mins.planogram_db_key,
                           maxs.floorplan_live_from_date  as maxplan_date
                    from maxrec maxs,
                         minrec mins
                    where maxs.location_no           = mins.location_no
                      and maxs.planogram_id          = mins.planogram_id
                      and maxs.planogram_status      = mins.planogram_status
                      and maxs.live_date             = mins.live_date
                      and maxs.planogram_department  = mins.planogram_department
                      and maxs.fixture_seq_no        = mins.fixture_seq_no
                      and maxs.floorplan_key        <> mins.floorplan_key
--                      and maxs.planogram_db_key     <> mins.planogram_db_key
                )b
            on    
               (a.location_no		        = b.location_no          and 
                a.planogram_id	        = b.planogram_id         and
                a.planogram_status	    = b.planogram_status     and
                a.live_date             = b.live_date            and
                a.floorplan_key	        = b.floorplan_key        and
                a.planogram_department  = b.planogram_department and
                a.fixture_seq_no        = b.fixture_seq_no       
--                a.planogram_db_key      = b.planogram_db_key
                 )            
            when matched then 
               update set 
                   a.floorplan_live_to_date = b.maxplan_date - 1;

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

    l_text := 'LOAD OF FND_LOC_PLANOGRAM_DY_WK EX INTACTIX STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := '26 aug 18';
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

   g_location_no          := 0;
   g_planogram_id         := 0;
   g_planogram_status     := 0;
   g_live_date            := '';
   g_planogram_department := '';
   g_floorplan_key        := 0;
   g_fixture_seq_no       := 0;
   g_planogram_db_key     := 0;

    for dupp_record in stg_dup
       loop

        if  dupp_record.location_no          = g_location_no and
            dupp_record.planogram_id         = g_planogram_id and
            dupp_record.planogram_status     = g_planogram_status and
            dupp_record.live_date            = g_live_date and
            dupp_record.planogram_department = g_planogram_department and
            dupp_record.floorplan_key        = g_floorplan_key  and
            dupp_record.fixture_seq_no       = g_fixture_seq_no and 
            dupp_record.planogram_db_key     = g_planogram_db_key then
            update stg_intactix_pln_store_arc stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;

            g_recs_duplicate  := g_recs_duplicate  + 1;
        end if;

        g_location_no          := dupp_record.location_no;
        g_planogram_id         := dupp_record.planogram_id;
        g_planogram_status     := dupp_record.planogram_status;
        g_live_date            := dupp_record.live_date;
        g_planogram_department := dupp_record.planogram_department;
        g_floorplan_key        := dupp_record.floorplan_key;
        g_fixture_seq_no       := dupp_record.fixture_seq_no;
        g_planogram_db_key     := dupp_record.planogram_db_key;

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
   
   local_bulk_active_update;

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into stg_intactix_pln_store_hsp hsp
   select /*+ FULL(TMP) */  tmp.sys_source_batch_id,
                            tmp.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            tmp.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON INDICATORS / ITEM / LOCATION',
                            tmp.location_no,
                            tmp.planogram_id,
                            tmp.planogram_status,
                            tmp.live_date,
                            tmp.planogram_department,
                            tmp.fixture_seq_no,
                            tmp.planogram_db_key,
                            tmp.company_store_no,
                            tmp.xcoordinate_distance,
                            tmp.ycoordinate_distance,
                            tmp.floorplan_seq_no,
                            tmp.planogram_fam_key,
                            tmp.planogram_name,
                            tmp.planogram_legacy_ind,
                            tmp.group_name,
                            tmp.subgroup_name,
                            tmp.planogram_supercategory,
                            tmp.planogram_category,
                            tmp.planogram_traffic_flow,
                            tmp.planogram_channel,
                            tmp.planogram_cluster,
                            tmp.planogram_region,
                            tmp.planogram_equip_type,
                            tmp.segment_start_no,
                            tmp.segment_end_no,
                            tmp.segment_linear,
                            tmp.segment_square_space,
                            tmp.segment_cubic_space,
                            tmp.store_sqm,
                            tmp.date_exported,
                            tmp.number_of_segments,
                            tmp.planogram_subcategory,
                            tmp.floorplan_key

    from  stg_intactix_pln_store_arc  tmp
    where
         not exists
           (select *
            from   fnd_location dl
            where  tmp.location_no       = dl.location_no )

          and sys_process_code = 'N'
          ;

    g_recs_hospital := g_recs_hospital + sql%rowcount;

    commit;

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
end wh_fnd_corp_184u_fix;
