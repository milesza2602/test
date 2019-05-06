--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_187U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_187U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        July 2018
--  Author:      Alfonso Joshua
--  Purpose:     Load Future Catalogue data
--  Tables:      Input  - stg_catman_loc_item_catlog_cpy
--               Output - fnd_loc_item_dy_fut_catlog
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
g_hospital_text      stg_catman_loc_item_catlog_hsp.sys_process_msg%type;
g_rec_out            fnd_loc_item_dy_fut_catlog%rowtype;
g_rec_in             stg_catman_loc_item_catlog_cpy%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date   := trunc(sysdate);
g_cnt                number := 0;
g_location_no        stg_catman_loc_item_catlog_cpy.location_no%type; 
g_item_no            stg_catman_loc_item_catlog_cpy.item_no%type;
g_effective_date     stg_catman_loc_item_catlog_cpy.effective_date%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_187U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE FUTURE CATALOG DATA EX CATMAN';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
      select * from stg_catman_loc_item_catlog_cpy
      where (location_no, item_no, effective_date)
      in
     (select location_no, item_no, effective_date
      from stg_catman_loc_item_catlog_cpy 
      group by location_no, item_no, effective_date
      having count(*) > 1) 
      order by location_no, item_no, effective_date, sys_source_batch_id desc ,sys_source_sequence_no desc;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure do_merge as
begin

  merge /*+ parallel (fli,4) */ into fnd_loc_item_dy_fut_catlog fli using (
     select /*+ PARALLEL(a,4) FULL(a) */
            a.location_no,
            a.item_no,
            a.effective_date,
            a.catalog_ind,
            a.catalog_type,
            g_date as last_updated_date
      from  stg_catman_loc_item_catlog_cpy a, 
            fnd_item b,
            fnd_location c
      where sys_process_code = 'N'
       and  a.item_no        = b.item_no
       and  a.location_no    = c.location_no
       
  ) mer_mart
  
  on (fli.location_no    = mer_mart.location_no and
      fli.item_no        = mer_mart.item_no     and
      fli.effective_date = mer_mart.effective_date
  )

when matched then
  update 
       set catalog_ind       = mer_mart.catalog_ind,
           catalog_type      = mer_mart.catalog_type,
           last_updated_date = g_date
     
when not matched then
  insert (
           location_no,
           item_no,
           effective_date,
           catalog_ind,
           catalog_type,
           last_updated_date
         )
  values (
          mer_mart.location_no,
          mer_mart.item_no,                
          mer_mart.effective_date,
          mer_mart.catalog_ind,
          mer_mart.catalog_type,
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

    l_text := 'LOAD OF FND_LOC_ITEM_DY_FUT_CATLOG EX INTACTIX STARTED AT '||
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
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no    := 0; 
   g_item_no        := 0;
   g_effective_date := '';
 
    for dupp_record in stg_dup
       loop
    
        if  dupp_record.location_no       = g_location_no and 
            dupp_record.item_no           = g_item_no     and 
            dupp_record.effective_date    = g_effective_date then
            update stg_catman_loc_item_catlog_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no     := dupp_record.location_no; 
        g_item_no         := dupp_record.item_no;
        g_effective_date  := dupp_record.effective_date;
        
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
   
    l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    insert /*+ APPEND parallel (hsp,2) */ into stg_catman_loc_item_catlog_hsp hsp 
    select /*+ FULL(TMP) */ tmp.sys_source_batch_id,
                            tmp.sys_source_sequence_no,
                            sysdate,
                            'Y',
                            'DWH',
                            tmp.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON INDICATORS / ITEM / LOCATION',
                            tmp.source_data_status_code,
                            tmp.location_no,                
                            tmp.item_no,           
                            tmp.effective_date,
                            tmp.catalog_ind,
                            tmp.catalog_type            
    from  stg_catman_loc_item_catlog_cpy  tmp
    where (
         not exists
           (select *
            from   fnd_item di
            where  tmp.item_no       = di.item_no )
         )  and
          (
         not exists
           (select *
            from   fnd_location dl
            where  tmp.location_no   = dl.location_no )
         )  
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
end wh_fnd_corp_187u;
