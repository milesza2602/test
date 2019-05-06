--------------------------------------------------------
--  DDL for Procedure WH_FND_MC_246U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MC_246U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        November 2017
--  Author:      Bhavesh Valodia
--  Purpose:     Update STG_RMS_MC_WAC table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_mc_wac_cpy
--               Output - fnd_mc_loc_item_dy_rms_wac
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  
--  

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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      dwh_foundation.stg_rms_mc_wac_hsp.sys_process_msg%type;
mer_mart             dwh_foundation.stg_rms_mc_wac_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MC_246U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE WAC MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no     stg_rms_mc_wac_cpy.location_no%type; 
g_item_no           stg_rms_mc_wac_cpy.item_no%type; 
g_tran_date           stg_rms_mc_wac_cpy.tran_date%type;


   cursor stg_dup is
      select * 
        from dwh_foundation.stg_rms_mc_wac_cpy
       where (location_no, item_no, tran_date)
          in
     (select location_no, item_no, tran_date
        from stg_rms_mc_wac_cpy 
    group by location_no, item_no, tran_date
      having count(*) > 1) 
    order by location_no, item_no, tran_date, sys_source_batch_id desc ,sys_source_sequence_no desc ;
    

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

    l_text := 'LOAD OF MC_WAC EX RMS STARTED AT '||
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
    from   DWH_FOUNDATION.STG_RMS_MC_WAC_CPY                                                                                                                                                                                               
    where  sys_process_code = 'N';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no  := 0; 
   g_item_no      := 0;
   g_tran_date    := '1 Jan 2000';

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.location_no = g_location_no and
            dupp_record.item_no     = g_item_no and 
            dupp_record.tran_date   = g_tran_date
            then
            update stg_rms_mc_wac_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no   := dupp_record.location_no; 
        g_item_no       := dupp_record.item_no;
        g_tran_date     := dupp_record.tran_date;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
-- Bulk Merge controlling main program execution
--**************************************************************************************************
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


merge /*+ parallel (fnd_mart,6) */ 
    into DWH_FOUNDATION.FND_MC_LOC_ITEM_DY_RMS_WAC fnd_mart 
    using (
    select /*+ FULL(TMP) */ tmp.*
    from DWH_FOUNDATION.STG_RMS_MC_WAC_CPY  tmp 
    where tmp.sys_process_code = 'N'
    and tmp.item_no     in (select item_no     from fnd_item itm     where itm.item_no = tmp.item_no) 
    and tmp.location_no in (select location_no from fnd_location loc where loc.location_no = tmp.location_no)      
     
     ) mer_mart
  
on  (mer_mart.location_no   = fnd_mart.location_no
and  mer_mart.item_no       = fnd_mart.item_no
and  mer_mart.tran_date     = fnd_mart.tran_date
    )
when matched then
update
set           SOURCE_DATA_STATUS_CODE     = mer_mart.source_data_status_code,
              LAST_UPDATED_DATE           = g_date,
              WAC_LOCAL                   = mer_mart.wac_local,
              WAC_OPR                     = mer_mart.wac_opr 



          
WHEN NOT MATCHED THEN
INSERT
(         LOCATION_NO,
          ITEM_NO,
          TRAN_DATE,
          SOURCE_DATA_STATUS_CODE,
          LAST_UPDATED_DATE,
          WAC_LOCAL,
          WAC_OPR


          )
  values
(         mer_mart.LOCATION_NO,
          mer_mart.ITEM_NO,
          mer_mart.TRAN_DATE,
          mer_mart.SOURCE_DATA_STATUS_CODE,
          g_date,
          mer_mart.WAC_LOCAL,
          mer_mart.WAC_OPR

          )  
  ;
  
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into DWH_FOUNDATION.STG_RMS_MC_WAC_HSP hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,
                            'Y',
                            'DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON  LOCATION_NO, ITEM_NO, TRAN_DATE',
                            TMP.LOCATION_NO,
                            TMP.ITEM_NO,
                            TMP.TRAN_DATE,
                            TMP.SOURCE_DATA_STATUS_CODE,
                            TMP.WAC_LOCAL,
                            TMP.WAC_OPR

   from  DWH_FOUNDATION.STG_RMS_MC_WAC_CPY  TMP 
   where tmp.item_no     not in (select item_no     from fnd_item itm     where itm.item_no = tmp.item_no) 
   or    tmp.location_no not in (select location_no from fnd_location loc where loc.location_no = tmp.location_no)   
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

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
end wh_fnd_mc_246u;
