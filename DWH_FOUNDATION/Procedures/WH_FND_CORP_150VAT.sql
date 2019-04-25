--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_150VAT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_150VAT" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Christie Koorts
--  Purpose:     Create item vat rate dimension table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_item_vat_rate
--               Output - W6005682.FND_ITEM_VAT_RATE_01APR
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  25 Sep 2015 : Chg 38403 - rewrote module to be a bulk merge to optimise AND added logic for a new 
--                            field added to the table - active_ind - which is used to indicate the 
--                            most current item / vate region record.
--
--  Theo Filander
--  02 Mar 2018 : VAT Fix   - Added additional functionality to cater for change in VAT Rate
--                            This is the FLASH version which is the latest.
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
g_recs_vat_fix       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_item_vat_rate_hsp.sys_process_msg%type;
g_rec_out            W6005682.FND_ITEM_VAT_RATE_01APR%rowtype;
g_rec_in             stg_rms_item_vat_rate%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_150VAT';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ITEM VAT RATE MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_vat_region_no       stg_rms_item_vat_rate.vat_region_no%type; 
g_item_no             stg_rms_item_vat_rate.item_no%TYPE; 
g_vat_code            stg_rms_item_vat_rate.vat_code%type;
g_active_from_date    stg_rms_item_vat_rate.active_from_date%type;

g_active_to_date      W6005682.FND_ITEM_VAT_RATE_01APR.active_to_date%type;
g_active_ind          W6005682.FND_ITEM_VAT_RATE_01APR.active_ind%type;
g_mod_cnt             integer;


cursor stg_dup is
 select * from stg_rms_item_vat_rate
  where (item_no, vat_region_no, vat_code, active_from_date)
      in
      (select item_no, vat_region_no, vat_code, active_from_date
         from stg_rms_item_vat_rate 
     group by item_no, vat_region_no, vat_code, active_from_date
      having count(*) > 1) 
      order by item_no, vat_region_no, vat_code, active_from_date, sys_source_batch_id desc ,sys_source_sequence_no desc;
      
cursor stg_vat_fix is
 select /*+ Parallel(cpy1,12)  Parallel(fi,12)*/ cpy1.*,max(fi.active_from_date) old_active_f_date
  from stg_rms_item_vat_rate cpy1
  inner join W6005682.FND_ITEM_VAT_RATE_01APR fi 
          on (cpy1.item_no            =  fi.item_no and
              cpy1.vat_region_no      =  fi.vat_region_no and
              cpy1.active_from_date   >  fi.active_from_date)
 where (sys_source_batch_id, sys_source_sequence_no,cpy1.item_no,cpy1.vat_region_no,cpy1.active_from_date) in
           (select /*+ PARALLEL(12) */ sys_source_batch_id, sys_source_sequence_no,item_no,vat_region_no,active_from_date
              from (select /*+ PARALLEL(t,12) */ t.*,
                           rank ()
                              over (partition by item_no,vat_region_no,active_from_date order by sys_source_batch_id desc, sys_source_sequence_no desc)
                              as rank
                      from stg_rms_item_vat_rate t
                   )
             where rank = 1)
 group by cpy1.sys_source_batch_id,
          cpy1.sys_source_sequence_no,
          cpy1.sys_load_date,
          cpy1.sys_process_code,
          cpy1.sys_load_system_name,
          cpy1.sys_middleware_batch_id,
          cpy1.sys_process_msg,
          cpy1.item_no,
          cpy1.vat_region_no,
          cpy1.vat_code,
          cpy1.active_from_date,
          cpy1.vat_type,
          cpy1.vat_code_desc,
          cpy1.vat_rate_perc,
          cpy1.vat_region_type,
          cpy1.vat_region_name,
          cpy1.source_data_status_code
 order by sys_source_batch_id desc, sys_source_sequence_no;
        
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

    l_text := 'LOAD OF W6005682.FND_ITEM_VAT_RATE_01APR EX RMS STARTED AT '||
    to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
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
    g_date := g_date +1;
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
  select count(*)
    into   g_recs_read
    from   stg_rms_item_vat_rate                                                                                                                                                                                               
    where  sys_process_code = 'N';
    
---**************************************************************************************************
----- De Duplication of the staging table to avoid Bulk insert failures
----************************************************************************************************** 
--   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
--   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--   
--   g_item_no              := 0; 
--   g_vat_region_no        := 0;
--   g_vat_code             := 0;
--   g_active_from_date     := '01/JAN/1900';

--    for dupp_record in stg_dup
--       loop
    
--        if  dupp_record.item_no           = g_item_no and
--            dupp_record.vat_region_no     = g_vat_region_no and
--            dupp_record.vat_code          = g_vat_code and
--            dupp_record.active_from_date  = g_active_from_date    then
--            
--            update stg_rms_item_vat_rate stg
--            set    sys_process_code = 'D'
--            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
--                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
--             
--            g_recs_duplicate  := g_recs_duplicate  + 1;       
--        end if;           
    
--        g_vat_region_no     := dupp_record.vat_region_no; 
--        g_item_no           := dupp_record.item_no;
--        g_vat_code          := dupp_record.vat_code;
--        g_active_from_date  := dupp_record.active_from_date;
--        commit;
--    end loop;
       
--    commit;
    
--    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Change in active_from_date
--************************************************************************************************** 
   l_text := 'NEW ACTIVE FROM DATE UPDATE STARTING - '||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_active_ind         := 0;
   g_active_to_date     := TO_DATE('01/JAN/3001');

    for close_vat in stg_vat_fix
    loop
    update W6005682.FND_ITEM_VAT_RATE_01APR fnd
       set fnd.active_to_date   = close_vat.active_from_date - 1,
           fnd.last_updated_date= g_date
     where fnd.item_no          = close_vat.item_no
       and fnd.vat_region_no    = close_vat.vat_region_no
--       and fnd.vat_code         = close_vat.vat_code
       and fnd.active_from_date = close_vat.old_active_f_date;
    
    
    g_recs_vat_fix := g_recs_vat_fix + SQL%ROWCOUNT;
    
    end loop;
    commit;
    
    l_text := 'CLOSE OFF ACTIVE_TO DATES : '||g_recs_vat_fix||' - '|| to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
             
    
--**************************************************************************************************
-- Bulk Merge controlling main program execution
--**************************************************************************************************
    l_text := 'MERGE STARTING (including setting all active_ind to 0) - '||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge /*+ parallel(fnd_mart,6) */ 
        into W6005682.FND_ITEM_VAT_RATE_01APR fnd_mart 
        using (
                select /*+ PARALLEL(cpy1,12) */ cpy1.*
                  from stg_rms_item_vat_rate cpy1
                 where (sys_source_batch_id, sys_source_sequence_no,item_no,vat_region_no,active_from_date) in
                       (select /*+ PARALLEL(12) */ sys_source_batch_id, sys_source_sequence_no,item_no,vat_region_no,active_from_date
                          from (select /*+ PARALLEL(t,12) */ t.*,
                                       rank ()
                                            over (partition by item_no,vat_region_no,active_from_date order by sys_source_batch_id desc, sys_source_sequence_no desc)
                                            as rank
                                  from stg_rms_item_vat_rate t
                                      )
                         where rank = 1
                       )
                 order by sys_source_batch_id desc, sys_source_sequence_no
              )  mer_mart
        on  (mer_mart.item_no           = fnd_mart.item_no
        and  mer_mart.vat_region_no     = fnd_mart.vat_region_no
        and  mer_mart.vat_code          = fnd_mart.vat_code
        and  mer_mart.active_from_date  = fnd_mart.active_from_date
            )
    when matched then
    update
    set       vat_type                  = mer_mart.vat_type,
              vat_code_desc             = mer_mart.vat_code_desc,
              vat_rate_perc             = mer_mart.vat_rate_perc,
              vat_region_type           = mer_mart.vat_region_type,
              vat_region_name           = mer_mart.vat_region_name,
              source_data_status_code   = mer_mart.source_data_status_code,
              last_updated_date         = g_date,
              active_ind                = g_active_ind
              
             
    when not matched then
    insert
    (         item_no,
              vat_region_no,
              vat_code,
              active_from_date,
              vat_type,
              vat_code_desc,
              vat_rate_perc,
              vat_region_type,
              vat_region_name,
              source_data_status_code,
              last_updated_date,
              active_ind,
              active_to_date
              )
      values
    (         mer_mart.item_no,
              mer_mart.vat_region_no,
              mer_mart.vat_code,
              mer_mart.active_from_date,
              mer_mart.vat_type,
              mer_mart.vat_code_desc,
              mer_mart.vat_rate_perc,
              mer_mart.vat_region_type,
              mer_mart.vat_region_name,
              mer_mart.source_data_status_code,
              g_date,
              g_active_ind,
              g_active_to_date
              )  
      ;
      
      g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

--   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
--   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
--   insert /*+ APPEND parallel (hsp,2) */ into stg_rms_item_vat_rate_hsp hsp 
--   select /*+ FULL(TMP) */  tmp.sys_source_batch_id,
--                            tmp.sys_source_sequence_no,
--                            sysdate,'Y','DWH',
--                            tmp.sys_middleware_batch_id,
--                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM,VAT_REGION,VAT_CODE',
--                            tmp.item_no,
--                            tmp.vat_region_no,
--                            tmp.vat_code,
--                            tmp.active_from_date,
--                            tmp.vat_type,
--                            tmp.vat_code_desc,
--                            tmp.vat_rate_perc,
--                            tmp.vat_region_type,
--                            tmp.vat_region_name,
--                            tmp.source_data_status_code
--                            
--    FROM  stg_rms_item_vat_rate  tmp 
--    WHERE ( tmp.source_data_status_code NOT IN ('U', 'I', 'D', 'P')
--        or
--         not exists
--          (select *
--           from   fnd_item di
--           where  tmp.item_no   = di.item_no )  
--          )
--        and sys_process_code = 'N'  
--          ;
--           
--    g_recs_hospital := g_recs_hospital + sql%rowcount;
--      
--    commit;
    
   
   l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

   l_text := 'MARKING ACTIVE RECORDS START';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   g_mod_cnt := 150000;
   
   l_text := 'SETTING ALL ACTIVE INDICATORS = 0';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   update /*+ parallel(fnd,8) full(fnd) */ W6005682.FND_ITEM_VAT_RATE_01APR fnd
     set active_ind = 0;
     
   commit;
   
   l_text := 'SETTING ALL ACTIVE INDICATORS = 0 - COMPLETE, STARTING ACTIVE CHECK MERGE';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   merge /*+ parallel (fnd_mart,6) */ 
    into W6005682.FND_ITEM_VAT_RATE_01APR fnd_mart 
    using (
         with aa as (
            select item_no, vat_region_no,  max(active_from_date) max_active_from_date
            from W6005682.FND_ITEM_VAT_RATE_01APR 
            where active_from_date <= g_date
            group by item_no, vat_region_no
            ) 
            select b.item_no, b.vat_region_no,  b.active_from_date 
              from W6005682.FND_ITEM_VAT_RATE_01APR b , aa
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
    
    l_text := 'Gather stats on W6005682.FND_ITEM_VAT_RATE_01APR starting';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dbms_stats.gather_table_stats ('DWH_FOUNDATION', 'W6005682.FND_ITEM_VAT_RATE_01APR', degree => 32);
    commit;
    l_text := 'Gather stats on W6005682.FND_ITEM_VAT_RATE_01APR complete';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd MON yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;                              --Duplicate check no longer being executed. Main query now excludes duplicates.
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    l_text :=  'NEW ACTIVE FROM DATE RECORS CHANGED '||g_recs_vat_fix;
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
       
end WH_FND_CORP_150VAT;
