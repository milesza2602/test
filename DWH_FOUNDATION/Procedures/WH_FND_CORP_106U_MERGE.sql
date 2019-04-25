--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_106U_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_106U_MERGE" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alastair de Wet
--  Purpose:     Create zone_item dimention table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_zone_item_cpy
--               Output - fnd_zone_item
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  August 2015 - Rewritten to be a single MERGE to improve performance
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
g_hospital_text      stg_rms_location_item_hsp.sys_process_msg%type;
mer_mart             stg_rms_zone_item_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_106M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION_ITEM MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_zone_group_no     stg_rms_zone_item_cpy.zone_group_no%type; 
g_item_no           stg_rms_zone_item_cpy.item_no%TYPE; 
g_zone_no           stg_rms_zone_item_cpy.zone_no%type;


   cursor stg_dup is
      select * 
        from stg_rms_zone_item_cpy
       where (zone_group_no, zone_no, item_no)
          in
     (select zone_group_no, zone_no, item_no
        from stg_rms_zone_item_cpy 
    group by zone_group_no, zone_no, item_no
      having count(*) > 1) 
    order by zone_group_no, zone_no, item_no, sys_source_batch_id desc ,sys_source_sequence_no desc;
    

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

    l_text := 'LOAD OF FND_LOCATION_ITEM EX RMS STARTED AT '||
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
    from   stg_rms_zone_item_cpy                                                                                                                                                                                               
    where  sys_process_code = 'Y';
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_zone_group_no  := 0; 
   g_item_no        := 0;

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.zone_group_no = g_zone_group_no and
            dupp_record.zone_no       = g_zone_no and 
            dupp_record.item_no       = g_item_no then
            update stg_rms_zone_item_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_zone_group_no   := dupp_record.zone_group_no; 
        g_zone_no         := dupp_record.zone_no;
        g_item_no         := dupp_record.item_no;
    
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
    into w6005682.fnd_zone_item_qs fnd_mart 
    using (
    select /*+ FULL(TMP) */ tmp.*
    from stg_rms_zone_item_cpy  tmp 
    join dim_item di          on tmp.item_no        = di.item_no   
    join fnd_zone dz          on tmp.zone_group_no  = dz.zone_group_no 
                             and tmp.zone_no        = dz.zone_no
    
    where tmp.sys_process_code = 'Y'
      and tmp.base_retail_ind in (0,1)
      and (tmp.source_data_status_code in ('U', 'I', 'D', 'P') or tmp.source_data_status_code is null)
     
     ) mer_mart
  
on  (mer_mart.zone_group_no   = fnd_mart.zone_group_no
and  mer_mart.zone_no         = fnd_mart.zone_no
and  mer_mart.item_no         = fnd_mart.item_no
    )
when matched then
update
set       base_retail_ind           = mer_mart.base_retail_ind,
          reg_rsp                   = mer_mart.reg_rsp,
          selling_uom_code          = mer_mart.selling_uom_code,
          market_basket_code        = mer_mart.market_basket_code,
          item_zone_link_code       = mer_mart.item_zone_link_code,
          multi_selling_uom_code    = mer_mart.multi_selling_uom_code,
          selling_unit_rsp          = mer_mart.selling_unit_rsp,
          multi_unit_rsp            = mer_mart.multi_unit_rsp,
          multi_qty                 = mer_mart.multi_qty,
          source_data_status_code   = mer_mart.source_data_status_code,
          currency_code             = mer_mart.currency_code,
          currency_reg_rsp          = mer_mart.currency_reg_rsp,
          last_updated_date         = g_date
          
WHEN NOT MATCHED THEN
INSERT
(         zone_group_no,
          zone_no,
          item_no,
          base_retail_ind,
          reg_rsp,
          selling_uom_code,
          market_basket_code,
          item_zone_link_code,
          multi_selling_uom_code,
          selling_unit_rsp,
          multi_unit_rsp,
          multi_qty,
          source_data_status_code,
          currency_code,
          currency_reg_rsp,
          last_updated_date
          )
  values
(         mer_mart.zone_group_no,
          mer_mart.zone_no,
          mer_mart.item_no,
          mer_mart.base_retail_ind,
          mer_mart.reg_rsp,
          mer_mart.selling_uom_code,
          mer_mart.market_basket_code,
          mer_mart.item_zone_link_code,
          mer_mart.multi_selling_uom_code,
          mer_mart.selling_unit_rsp,
          mer_mart.multi_unit_rsp,
          mer_mart.multi_qty,
          mer_mart.source_data_status_code,
          mer_mart.currency_code,
          mer_mart.currency_reg_rsp,
          
          g_date
          )  
  ;
  
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into w6005682.stg_rms_zne_item_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM, ZONE_GROUP, ZONE',
                            TMP.zone_group_no,
                            TMP.zone_no,
                            TMP.item_no,
                            TMP.base_retail_ind,
                            TMP.reg_rsp,
                            TMP.selling_uom_code,
                            TMP.market_basket_code,
                            TMP.item_zone_link_code,
                            TMP.multi_selling_uom_code,
                            TMP.selling_unit_rsp,
                            TMP.multi_unit_rsp,
                            TMP.multi_qty,
                            TMP.source_data_status_code,
                            TMP.currency_code,
                            TMP.currency_reg_rsp
    from  stg_rms_zone_item_cpy  TMP 
   where ( tmp.base_retail_ind not in (0,1)
      or (tmp.source_data_status_code is not null and tmp.source_data_status_code not in ('U', 'I', 'D', 'P'))
      or not exists
         (select *
            from dim_item di
           where tmp.item_no   = di.item_no )  
      or not exists
         (select *
            from fnd_zone fz
           where tmp.zone_group_no = fz.zone_group_no 
             and tmp.zone_no       = fz.zone_no)
           )  
          and sys_process_code = 'Y'  
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
end wh_fnd_corp_106u_merge;
