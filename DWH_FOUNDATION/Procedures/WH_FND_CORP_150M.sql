--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_150M
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_150M" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Christie Koorts
--  Purpose:     Create item vat rate dimension table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_item_vat_rate_cpy
--               Output - fnd_item_vat_rate
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_active_upd    integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_item_vat_rate_hsp.sys_process_msg%type;
g_rec_out            W6005682.fnd_item_vat_rateQ%rowtype;
g_rec_in             stg_rms_item_vat_rate_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_150M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ITEM VAT RATE MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_vat_region_no       stg_rms_item_vat_rate_cpy.vat_region_no%type; 
g_item_no             stg_rms_item_vat_rate_cpy.item_no%TYPE; 
g_vat_code            stg_rms_item_vat_rate_cpy.vat_code%type;
g_active_from_date    stg_rms_item_vat_rate_cpy.active_from_date%type;
g_mod_cnt             integer;


cursor stg_dup is
 select * from stg_rms_item_vat_rate_cpy
  where (ITEM_NO, VAT_REGION_NO, VAT_CODE, ACTIVE_FROM_DATE)
      in
      (select ITEM_NO, VAT_REGION_NO, VAT_CODE, ACTIVE_FROM_DATE
         from stg_rms_item_vat_rate_cpy 
     group by ITEM_NO, VAT_REGION_NO, VAT_CODE, ACTIVE_FROM_DATE
      having count(*) > 1) 
      order by ITEM_NO, VAT_REGION_NO, VAT_CODE, ACTIVE_FROM_DATE, sys_source_batch_id desc ,sys_source_sequence_no desc;
      
      
cursor fnd_upd is
    with aa as (
select item_no, vat_region_no, vat_code, max(active_from_date) max_active_from_date
from w6005682.fnd_item_vat_rateq 
where active_from_date < g_date
group by item_no, vat_region_no, vat_code
) 
select b.item_no, b.vat_region_no, b.vat_code, b.active_from_date 
  from w6005682.fnd_item_vat_rateq b , aa
              where aa.item_no = b.item_no
                and aa.vat_region_no = b.vat_region_no
                and aa.vat_code = b.vat_code
                and aa.max_active_from_date = b.active_from_date
;

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

    l_text := 'LOAD OF FND_ITEM_VAT_RATE EX RMS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
      execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
  select count(*)
    into   g_recs_read
    from   stg_rms_item_vat_rate_cpy                                                                                                                                                                                               
    where  sys_process_code = 'Y';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_item_no              := 0; 
   g_vat_region_no        := 0;
   g_vat_code             := 0;
   g_active_from_date     := '01/JAN/1900';

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.item_no           = g_item_no and
            dupp_record.vat_region_no     = g_vat_region_no and
            dupp_record.vat_code          = g_vat_code and
            dupp_record.active_from_date  = g_active_from_date    then
            
            update stg_rms_item_vat_rate_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_vat_region_no     := dupp_record.vat_region_no; 
        g_item_no           := dupp_record.item_no;
        g_vat_code          := dupp_record.vat_code;
        g_active_from_date  := dupp_record.active_from_date;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
-- Bulk Merge controlling main program execution
--**************************************************************************************************
    l_text := 'MERGE STARTING (including setting all active_ind to 0) - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

merge /*+ parallel(fnd_mart,6) */ 
    into w6005682.FND_ITEM_VAT_RATEq fnd_mart 
    using (
    select /*+ FULL(TMP) */ tmp.*
    from stg_rms_item_vat_rate_cpy  tmp 
    join fnd_item di    on tmp.item_no = di.item_no 
    
    where tmp.sys_process_code = 'Y'
      and (tmp.source_data_status_code in ('U', 'I', 'D', 'P')
        or tmp.source_data_status_code is null)
        
     ) mer_mart
  
on  (mer_mart.item_no           = fnd_mart.item_no
and  mer_mart.vat_region_no     = fnd_mart.vat_region_no
and  mer_mart.vat_code          = fnd_mart.vat_code
and  mer_mart.active_from_date  = fnd_mart.active_from_date
    )
when matched then
update
set       VAT_TYPE                  = mer_mart.vat_type,
          VAT_CODE_DESC             = mer_mart.vat_code_desc,
          VAT_RATE_PERC             = mer_mart.vat_rate_perc,
          VAT_REGION_TYPE           = mer_mart.vat_region_type,
          VAT_REGION_NAME           = mer_mart.vat_region_name,
          SOURCE_DATA_STATUS_CODE   = mer_mart.source_data_status_code,
          LAST_UPDATED_DATE         = g_date,
          ACTIVE_IND                = 0
         
WHEN NOT MATCHED THEN
INSERT
(         ITEM_NO,
          VAT_REGION_NO,
          VAT_CODE,
          ACTIVE_FROM_DATE,
          VAT_TYPE,
          VAT_CODE_DESC,
          VAT_RATE_PERC,
          VAT_REGION_TYPE,
          VAT_REGION_NAME,
          SOURCE_DATA_STATUS_CODE,
          LAST_UPDATED_DATE,
          active_ind
          )
  values
(         mer_mart.ITEM_NO,
          mer_mart.VAT_REGION_NO,
          mer_mart.VAT_CODE,
          mer_mart.ACTIVE_FROM_DATE,
          mer_mart.VAT_TYPE,
          mer_mart.VAT_CODE_DESC,
          mer_mart.VAT_RATE_PERC,
          mer_mart.VAT_REGION_TYPE,
          mer_mart.VAT_REGION_NAME,
          mer_mart.SOURCE_DATA_STATUS_CODE,
          g_date,
          0
          )  
  ;
  
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into stg_rms_item_vat_rate_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM,VAT_REGION,VAT_CODE',
                            TMP.ITEM_NO,
                            TMP.VAT_REGION_NO,
                            TMP.VAT_CODE,
                            TMP.ACTIVE_FROM_DATE,
                            TMP.VAT_TYPE,
                            TMP.VAT_CODE_DESC,
                            TMP.VAT_RATE_PERC,
                            TMP.VAT_REGION_TYPE,
                            TMP.VAT_REGION_NAME,
                            TMP.SOURCE_DATA_STATUS_CODE
                            
    from  stg_rms_item_vat_rate_cpy  TMP 
    where ( tmp.source_data_status_code in ('U', 'I', 'D', 'P')
        or
         not exists
          (select *
           from   fnd_item di
           where  tmp.item_no   = di.item_no )  
          )
        and sys_process_code = 'Y'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
    
   
   l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

   l_text := 'MARKING ACTIVE RECORDS START';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   g_mod_cnt := 150000;
   
   l_text := 'SETTING ALL ACTIVE INDICATORS = 0';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   update /*+ parallel(fnd,8) full(fnd) */ w6005682.fnd_item_vat_rateq fnd
     set active_ind = 0;
     
   commit;
   
   l_text := 'SETTING ALL ACTIVE INDICATORS = 0 - COMPLETE, STARTING ACTIVE CHECK MERGE';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   merge /*+ parallel (fnd_mart,6) */ 
    into w6005682.FND_ITEM_VAT_RATEq fnd_mart 
    using (
         with aa as (
            select item_no, vat_region_no,  max(active_from_date) max_active_from_date
            from w6005682.fnd_item_vat_rateq 
            where active_from_date <= g_date
            group by item_no, vat_region_no
            ) 
            select b.item_no, b.vat_region_no,  b.active_from_date 
              from w6005682.fnd_item_vat_rateq b , aa
                          where aa.item_no = b.item_no
                            and aa.vat_region_no = b.vat_region_no
                           and aa.max_active_from_date = b.active_from_date
          ) mer_mart
  
    on  (mer_mart.item_no           = fnd_mart.item_no
    and  mer_mart.vat_region_no     = fnd_mart.vat_region_no
  --  and  mer_mart.vat_code          = fnd_mart.vat_code
    and  mer_mart.active_from_date  = fnd_mart.active_from_date
        )
    when matched then
    update
    set       ACTIVE_IND                = 1
    ;   
    
    g_recs_active_upd := SQL%ROWCOUNT;
    
    commit;
    
    l_text := 'MARKING ACTIVE RECORDS END - '|| g_recs_active_upd || ' - records marked as active';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'Gather stats on w6005682.FND_ITEM_VAT_RATEq starting';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dbms_stats.gather_table_stats ('w6005682', 'FND_ITEM_VAT_RATEq', degree => 32);
    commit;
    l_text := 'Gather stats on w6005682.FND_ITEM_VAT_RATEq complete';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;          
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
       
end wh_fnd_corp_150m;
