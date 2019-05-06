--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_186U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_186U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2017
--  Author:      Alfonso Joshua
--  Purpose:     Create Department Store data with input ex Intactix (foods)
--               Department seen as display area by the business
--  Tables:      Input  - stg_intactix_dept_store_cpy
--               Output - fnd_loc_display_wk
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
g_hospital_text      stg_intactix_dept_store_hsp.sys_process_msg%type;
g_rec_out            fnd_loc_display_wk%rowtype;
g_rec_in             stg_intactix_dept_store_cpy%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date          := trunc(sysdate);
g_load_date          date          := trunc(sysdate);
g_cnt                number := 0;
g_location_no        stg_intactix_dept_store_cpy.location_no%type;
g_display_id         stg_intactix_dept_store_cpy.display_id%type;
g_live_date          stg_intactix_dept_store_cpy.live_date%type;
g_floorplan_key      stg_intactix_dept_store_cpy.floorplan_key%type;
g_display_area_name  stg_intactix_dept_store_cpy.display_area_name%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_186U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DEPARTMENT (DISPLAY AREA) STORE DATA EX INTACTIX';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
      select * from stg_intactix_dept_store_cpy
      where (location_no, display_id, live_date, floorplan_key, display_area_name)
      in
     (select location_no, display_id, live_date, floorplan_key, display_area_name
      from stg_intactix_dept_store_cpy
      where sys_process_code = 'N'
      group by location_no, display_id, live_date, floorplan_key, display_area_name
      having count(*) > 1)
      order by location_no, display_id, live_date, floorplan_key, display_area_name,
      sys_source_batch_id desc ,sys_source_sequence_no desc;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure do_merge as
begin

  merge /*+ parallel (fli,4) */ into fnd_loc_display_wk fli using (
     select /*+ PARALLEL(a,4) FULL(a) full(b) */
            a.location_no,
            display_id,
            c.fin_year_no,
            c.fin_week_no,
            live_date,
            floorplan_key,
            nvl(display_area_name,'UNALLOCATED DEPARTMENT') display_area_name,
            display_area,
            floorplan_status,
            market_type,
            market_size,
            trading_sqm,
            store_profile,
            node,
            fixture_count,
            fixture_linear,
            fixture_area,
            section_count_no,
            section_linear,
            section_area,
            fixture_density,
            section_density,
            date_exported,
            ww_catchment_area,
            province,
            fsa_indicator,
            foods_selling_sqm,
            longitude,
            latitude,
            g_date as last_updated_date
      from  stg_intactix_dept_store_cpy a, fnd_location b, dim_calendar c
      where a.sys_process_code = 'N'
       and  a.location_no    = b.location_no
       and  c.calendar_date  = g_load_date
--       and  a.display_area_name is not null
  ) mer_mart

  on (fli.location_no       = mer_mart.location_no
  and fli.display_id        = mer_mart.display_id
  and fli.fin_year_no       = mer_mart.fin_year_no
  and fli.fin_week_no       = mer_mart.fin_week_no
  and fli.live_date         = mer_mart.live_date
  and fli.floorplan_key     = mer_mart.floorplan_key
  and fli.display_area_name = mer_mart.display_area_name
  )

when matched then
  update
       set display_area       = mer_mart.display_area,
           floorplan_status   = mer_mart.floorplan_status,
           market_type        = mer_mart.market_type,
           market_size        = mer_mart.market_size,
           trading_sqm        = mer_mart.trading_sqm,
           store_profile      = mer_mart.store_profile,
           node               = mer_mart.node,
           fixture_count      = mer_mart.fixture_count,
           fixture_linear     = mer_mart.fixture_linear,
           fixture_area       = mer_mart.fixture_area,
           section_count_no   = mer_mart.section_count_no,
           section_linear     = mer_mart.section_linear,
           section_area       = mer_mart.section_area,
           fixture_density    = mer_mart.fixture_density,
           section_density    = mer_mart.section_density,
           date_exported      = mer_mart.date_exported,
           ww_catchment_area  = mer_mart.ww_catchment_area,
           province           = mer_mart.province,
           fsa_indicator      = mer_mart.fsa_indicator,
           foods_selling_sqm  = mer_mart.foods_selling_sqm,
           longitude          = mer_mart.longitude,
           latitude           = mer_mart.latitude,
           last_updated_date  = g_date

when not matched then
  insert (
          location_no,
          display_id,
          fin_year_no,
          fin_week_no,
          live_date,
          floorplan_key,
          display_area_name,
          display_area,
          floorplan_status,
          market_type,
          market_size,
          trading_sqm,
          store_profile,
          node,
          fixture_count,
          fixture_linear,
          fixture_area,
          section_count_no,
          section_linear,
          section_area,
          fixture_density,
          section_density,
          date_exported,
          ww_catchment_area,
          province,
          fsa_indicator,
          foods_selling_sqm,
          longitude,
          latitude,
          last_updated_date
         )
  values (
          mer_mart.location_no,
          mer_mart.display_id,
          mer_mart.fin_year_no,
          mer_mart.fin_week_no,
          mer_mart.live_date,
          mer_mart.floorplan_key,
          mer_mart.display_area_name,
          mer_mart.display_area,
          mer_mart.floorplan_status,
          mer_mart.market_type,
          mer_mart.market_size,
          mer_mart.trading_sqm,
          mer_mart.store_profile,
          mer_mart.node,
          mer_mart.fixture_count,
          mer_mart.fixture_linear,
          mer_mart.fixture_area,
          mer_mart.section_count_no,
          mer_mart.section_linear,
          mer_mart.section_area,
          mer_mart.fixture_density,
          mer_mart.section_density,
          mer_mart.date_exported,
          mer_mart.ww_catchment_area,
          mer_mart.province,
          mer_mart.fsa_indicator,
          mer_mart.foods_selling_sqm,
          mer_mart.longitude,
          mer_mart.latitude,
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

    l_text := 'LOAD OF FND_LOC_DY_PLANO EX INTACTIX STARTED AT '||
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
    
    g_load_date := g_date + 1;

  --**************************************************************************************************
  -- De Duplication of the staging table to avoid Bulk insert failures
  --**************************************************************************************************
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   g_location_no        := 0;
   g_display_id         := 0;
   g_live_date          := '';
   g_floorplan_key      := 0;
   g_display_area_name  := '';

    for dupp_record in stg_dup
       loop

        if  dupp_record.location_no        = g_location_no and
            dupp_record.display_id         = g_display_id and
            dupp_record.live_date          = g_live_date and
            dupp_record.floorplan_key      = g_floorplan_key and
            dupp_record.display_area_name  = g_display_area_name then
            update stg_intactix_dept_store_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;

            g_recs_duplicate  := g_recs_duplicate  + 1;
        end if;

        g_location_no         := dupp_record.location_no;
        g_display_id          := dupp_record.display_id;
        g_live_date           := dupp_record.live_date;
        g_floorplan_key       := dupp_record.floorplan_key;
        g_display_area_name   := dupp_record.display_area_name;

    end loop;

    commit;

    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'Duplicate Records - '||g_recs_duplicate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';
   
   l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   do_merge;
   
   l_text := 'MERGE ENDING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
   l_text := 'STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into stg_intactix_dept_store_hsp hsp
   select /*+ FULL(TMP) */  tmp.sys_source_batch_id,
                            tmp.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            tmp.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON LOCATION',
                            tmp.location_no,
                            tmp.display_id,
                            tmp.live_date,
                            tmp.display_area,
                            tmp.floorplan_status,
                            tmp.display_area_name,
                            tmp.market_type,
                            tmp.market_size,
                            tmp.trading_sqm,
                            tmp.store_profile,
                            tmp.node,
                            tmp.fixture_count,
                            tmp.fixture_linear,
                            tmp.fixture_area,
                            tmp.section_count_no,
                            tmp.section_linear,
                            tmp.section_area,
                            tmp.fixture_density,
                            tmp.section_density,
                            tmp.date_exported,
                            tmp.ww_catchment_area,
                            tmp.province,
                            tmp.fsa_indicator,
                            tmp.foods_selling_sqm,
                            tmp.longitude,
                            tmp.latitude,
                            tmp.floorplan_key

    from  stg_intactix_dept_store_cpy  tmp
    where (
         not exists
           (select *
            from   fnd_location dl
            where  tmp.location_no       = dl.location_no )
         )
          and sys_process_code = 'N'
          ;

    g_recs_hospital := g_recs_hospital + sql%rowcount;
    
    l_text := 'ENDING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
end wh_fnd_corp_186u;
