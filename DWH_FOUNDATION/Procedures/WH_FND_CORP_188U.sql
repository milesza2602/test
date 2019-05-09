--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_188U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_188U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2017
--  Author:      Alfonso Joshua
--  Purpose:     Create Item Display Group data with input ex Intactix (foods)
--  Tables:      Input  - stg_intactix_display_group_cpy
--               Output - fnd_item_display_grp
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
g_hospital_text      stg_intactix_display_group_hsp.sys_process_msg%type;
g_rec_out            fnd_item_display_grp%rowtype;
g_rec_in             stg_intactix_display_group_cpy%rowtype;
g_found              boolean;
g_valid              boolean;
g_date               date   := trunc(sysdate);
g_cnt                number := 0;
g_item_no            stg_intactix_display_group_cpy.item_no%type; 

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_188U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ITEM DISPLAY GROUP DATA EX INTACTIX';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
      select * from stg_intactix_display_group_cpy
      where (item_no)
      in
     (select item_no
      from stg_intactix_display_group_cpy 
      group by item_no
      having count(*) > 1) 
      order by item_no, sys_source_batch_id desc ,sys_source_sequence_no desc;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure do_merge as
begin

  merge /*+ parallel (fli,4) */ into fnd_item_display_grp fli using (
     select /*+ PARALLEL(a,4) FULL(a) */
            a.item_no,
            a.display_group1_name,
            a.display_group2_name,
            a.display_group3_name,
            a.display_group4_name,
            a.display_group5_name,
            a.cdt1_name,
            a.cdt2_name,
            a.cdt3_name,
            a.cdt4_name,
            a.cdt5_name,
            g_date as last_updated_date
      from  stg_intactix_display_group_cpy a, fnd_item b
      where sys_process_code = 'N'
       and  a.item_no = b.item_no
       
  ) mer_mart
  
  on (fli.item_no      = mer_mart.item_no
  )

when matched then
  update 
       set display_group1_name     = mer_mart.display_group1_name,
           display_group2_name     = mer_mart.display_group2_name,
           display_group3_name     = mer_mart.display_group3_name,
           display_group4_name     = mer_mart.display_group4_name,
           display_group5_name     = mer_mart.display_group5_name,
           cdt1_name               = mer_mart.cdt1_name,
           cdt2_name               = mer_mart.cdt2_name,
           cdt3_name               = mer_mart.cdt3_name,
           cdt4_name               = mer_mart.cdt4_name,
           cdt5_name               = mer_mart.cdt5_name,
           last_updated_date       = g_date
     
when not matched then
  insert (
          item_no,
          display_group1_name,
          display_group2_name,
          display_group3_name,
          display_group4_name,
          display_group5_name,
          cdt1_name,
          cdt2_name,
          cdt3_name,
          cdt4_name,
          cdt5_name,
          last_updated_date
         )
  values (
          mer_mart.item_no,                
          mer_mart.display_group1_name,            
          mer_mart.display_group2_name,
          mer_mart.display_group3_name,
          mer_mart.display_group4_name,
          mer_mart.display_group5_name,
          mer_mart.cdt1_name,
          mer_mart.cdt2_name,
          mer_mart.cdt3_name,
          mer_mart.cdt4_name,
          mer_mart.cdt5_name,
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

    l_text := 'LOAD OF FND_ITEM_DISPLAY_GRP EX INTACTIX STARTED AT '||
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
   
   g_item_no      := 0; 
 
    for dupp_record in stg_dup
       loop
    
        if  dupp_record.item_no           = g_item_no then
            update stg_intactix_display_group_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_item_no         := dupp_record.item_no; 
        
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
   
    insert /*+ APPEND parallel (hsp,2) */ into stg_intactix_display_group_hsp hsp 
    select /*+ FULL(TMP) */ tmp.sys_source_batch_id,
                            tmp.sys_source_sequence_no,
                            sysdate,
                            'Y',
                            'DWH',
                            tmp.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON INDICATORS / ITEM / LOCATION',
                            tmp.source_data_status_code,
                            tmp.item_no,                
                            tmp.display_group1_name,            
                            tmp.display_group2_name,
                            tmp.display_group3_name,
                            tmp.display_group4_name,
                            tmp.display_group5_name,
                            tmp.cdt1_name,
                            tmp.cdt2_name,
                            tmp.cdt3_name,
                            tmp.cdt4_name,
                            tmp.cdt5_name
                            
    from  stg_intactix_display_group_cpy  tmp
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
end wh_fnd_corp_188u;