--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_155U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_155U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2013
--  Author:      Quentin Smit
--  Purpose:     Create zone_item dimention table in the foundation layer
--               with input ex staging table from JDAFF.
--  Tables:      Input  - stg_catman_location_item
--               Output - fnd_location_item
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  Oct 2015    - rewritten as a single merge
--  08 Sep 2016 - A Joshua Chg-202 -- Remove table fnd_jdaff_dept_rollout from selection criteria

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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_recs_duplicate     integer       :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_catman_location_item_hsp.sys_process_msg%type;
g_rec_out            fnd_location_item%rowtype;
g_rec_in             stg_catman_location_item%rowtype;
g_found              boolean;
g_valid              boolean;
g_count              integer       :=  0;
--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_155U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ZONE_ITEM MASTERDATA EX AMOS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


g_location_no       stg_catman_location_item_cpy.location_no%type; 
g_item_no           stg_catman_location_item_cpy.item_no%TYPE; 


 cursor stg_dup is
/*     select a.* from stg_catman_location_item_cpy a, fnd_jdaff_dept_rollout b, dim_item c
      where a.item_no = c.item_no
        and b.department_no = c.department_no
        and b.department_live_ind = 'Y'
        and (a.location_no, a.item_no)
          in (select location_no,item_no
               from stg_catman_location_item_cpy 
           group by location_no, item_no
           having count(*) > 1) 
   order by a.location_no, a.item_no, sys_source_batch_id  desc,sys_source_sequence_no desc; */
   
      select a.* from stg_catman_location_item_cpy a
      where 
         (a.location_no, a.item_no)
          in (select location_no,item_no
               from stg_catman_location_item_cpy 
           group by location_no, item_no
           having count(*) > 1) 
   order by a.location_no, a.item_no, sys_source_batch_id  desc,sys_source_sequence_no desc;

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

    l_text := 'LOAD OF fnd_location_item EX CATMAN STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    
    --g_date := g_date - 1;
    
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

    select count(*) into g_recs_read 
      from stg_catman_location_item_cpy
     where sys_process_code = 'N';
     
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no    := 0; 
   g_item_no        := 0;

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.location_no   = g_location_no and
            dupp_record.item_no       = g_item_no then
            update stg_catman_location_item_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no    := dupp_record.location_no; 
        g_item_no        := dupp_record.item_no;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

--**************************************************************************************************
-- Bulk Merge controlling main program execution
--**************************************************************************************************
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   

merge  /*+ parallel(fnd_mart,6) */
    into fnd_location_item fnd_mart 
    using (   
       select /*+ parallel(a,6) */  a.*
         from stg_catman_location_item_cpy a
--         , fnd_jdaff_dept_rollout b, dim_item c
        where sys_process_code = 'N'
          and (a.source_data_status_code in ('U', 'I', 'D', 'P') or a.source_data_status_code is null)
--          and a.item_no = c.item_no
--          and b.department_no = c.department_no
--          and b.department_live_ind = 'Y'
   
      ) mer_mart
  
on  (mer_mart.item_no        = fnd_mart.item_no
and  mer_mart.location_no    = fnd_mart.location_no
    )
when matched then
update
set       this_wk_catalog_ind         = mer_mart.this_wk_catalog_ind,
          NEXT_wk_catalog_ind         = mer_mart.next_wk_catalog_ind,
          source_data_status_code     = mer_mart.source_data_status_code,
          last_updated_date           = g_date
          
WHEN NOT MATCHED THEN
INSERT
(         ITEM_NO,
          LOCATION_NO,
          THIS_WK_CATALOG_IND,
          NEXT_WK_CATALOG_IND,
          SOURCE_DATA_STATUS_CODE,
          LAST_UPDATED_DATE
          )
  values
(         mer_mart.ITEM_NO,
          mer_mart.LOCATION_NO,
          mer_mart.THIS_WK_CATALOG_IND,
          mer_mart.NEXT_WK_CATALOG_IND,
          mer_mart.SOURCE_DATA_STATUS_CODE,
          g_date
          )  
  ;
  
  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
  
  commit;
  

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into stg_catman_location_item_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM, LOC',
                            TMP.ITEM_NO,
                            TMP.LOCATION_NO,
                            TMP.THIS_WK_CATALOG_IND,
                            TMP.NEXT_WK_CATALOG_IND,
                            TMP.SOURCE_DATA_STATUS_CODE,
                            g_date
    from  stg_catman_location_item_cpy  TMP 
    where ( tmp.source_data_status_code not in ('U', 'I', 'D', 'P') 
        or
         not exists
          (select *
           from   fnd_item di
           where  tmp.item_no   = di.item_no )  
        or
         not exists
           (select *
           from   fnd_location dl
           where  tmp.location_no       = dl.location_no )
         
          )  
          and sys_process_code = 'N'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

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
end wh_fnd_corp_155u;
