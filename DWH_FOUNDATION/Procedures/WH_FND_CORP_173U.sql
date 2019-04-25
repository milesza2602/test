--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_173U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_173U" (p_forall_limit in integer,
                                                p_success      out boolean) as
--**************************************************************************************************
--  Date:        November 2016
--  Author:      Alfonso Joshua
--  Purpose:     Create item_supplier dc plan orders fact table in the foundation layer
--               with input ex staging table from JDAFF.
--  Tables:      Input  - stg_jdaff_dc_plan_sup_ord
--               Output - fnd_jdaff_dc_plan_sup_ord_dy
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

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
g_hospital_text      stg_jdaff_dc_plan_sup_ord_hsp.sys_process_msg%type;
mer_mart             stg_jdaff_dc_plan_sup_ord_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_173U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ITEM_SUPPLIER DC PLAN ORDERS EX JDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_supplier_no               stg_jdaff_dc_plan_sup_ord_cpy.supplier_no%type; 
g_item_no                   stg_jdaff_dc_plan_sup_ord_cpy.item_no%type; 
g_deliver_to_dc_region      stg_jdaff_dc_plan_sup_ord_cpy.deliver_to_dc_region%type;
g_distribute_from_dc_region stg_jdaff_dc_plan_sup_ord_cpy.distribute_from_dc_region%type;
g_post_date           date;
g_trading_date        date;

   cursor stg_dup is
      select * from stg_jdaff_dc_plan_sup_ord_cpy
      where (item_no, supplier_no, trading_date, post_date, deliver_to_dc_region, distribute_from_dc_region)
         in
      (select item_no, supplier_no, trading_date, post_date, deliver_to_dc_region, distribute_from_dc_region
       from stg_jdaff_dc_plan_sup_ord_cpy 
      group by item_no, supplier_no, trading_date, post_date, deliver_to_dc_region, distribute_from_dc_region
      having count(*) > 1) 
      order by item_no, supplier_no, trading_date, post_date, deliver_to_dc_region, distribute_from_dc_region, 
               sys_source_batch_id desc ,sys_source_sequence_no desc;
    
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

    l_text := 'LOAD OF FND_JDAFF_ITEM_SUPP_DY_DC_ORD EX JDA STARTED AT '||
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

-- BELOW IS DONE TO OVERCOME THE JOB BEING A SPINNER IN APPWORX   
 
    l_text := 'STARTING MAIN MERGE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    
 
    select count(*)
    into   g_recs_read
    from   stg_jdaff_dc_plan_sup_ord_cpy                                                                                                                                                                                               
    where  sys_process_code = 'N'
   ;
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
   g_item_no                   := 0;
   g_supplier_no               := 0; 
   g_trading_date              := '01/JAN/1900';
   g_post_date                 := '01/JAN/1900';
   g_deliver_to_dc_region      := 0;
   g_distribute_from_dc_region := 0;

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.item_no                   = g_item_no and
            dupp_record.supplier_no               = g_supplier_no and
            dupp_record.trading_date              = g_trading_date and
            dupp_record.post_date                 = g_post_date and
            dupp_record.deliver_to_dc_region      = g_deliver_to_dc_region and
            dupp_record.distribute_from_dc_region = g_distribute_from_dc_region then
            
            update stg_jdaff_dc_plan_sup_ord_cpy stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_supplier_no               := dupp_record.supplier_no; 
        g_item_no                   := dupp_record.item_no;
        g_trading_date              := dupp_record.trading_date;
        g_post_date                 := dupp_record.post_date;
        g_deliver_to_dc_region      := dupp_record.deliver_to_dc_region;
        g_distribute_from_dc_region := dupp_record.distribute_from_dc_region;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
-- Bulk Merge controlling main program execution
--**************************************************************************************************
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    merge /*+ parallel(fnd_mart,4) */ 
    into fnd_jdaff_dc_plan_sup_ord_dy fnd_mart 
    using (
    select /*+ FULL(TMP) */ tmp.*
      from stg_jdaff_dc_plan_sup_ord_cpy  tmp 
      join fnd_item di        on tmp.item_no = di.item_no 
      join fnd_supplier ds    on tmp.supplier_no = ds.supplier_no
      join fnd_zone dzt       on tmp.deliver_to_dc_region = dzt.zone_no 
      join fnd_zone dzf       on tmp.distribute_from_dc_region = dzf.zone_no
    where tmp.sys_process_code = 'N'
   
     ) mer_mart
  
     on  (mer_mart.item_no                   = fnd_mart.item_no
     and  mer_mart.supplier_no               = fnd_mart.supplier_no
     and  mer_mart.trading_date              = fnd_mart.trading_date
     and  mer_mart.post_date                 = fnd_mart.post_date
     and  mer_mart.deliver_to_dc_region      = fnd_mart.deliver_to_dc_region
     and  mer_mart.distribute_from_dc_region = fnd_mart.distribute_from_dc_region
    )
    when matched then
    update
    set   tombag_factor                  = mer_mart.tombag_factor,
          deliver_to_dc_reg_date         = mer_mart.deliver_to_dc_reg_date,
          deliver_to_dc_region_cases     = mer_mart.deliver_to_dc_region_cases,
          num_units_per_tray             = mer_mart.num_units_per_tray,
          distribute_from_dc_region_date = mer_mart.distribute_from_dc_region_date
           
    when not matched then
    insert
   (      trading_date,
          post_date,
          item_no,
          supplier_no,          
          deliver_to_dc_region,
          distribute_from_dc_region,
          tombag_factor,
          deliver_to_dc_reg_date,
          deliver_to_dc_region_cases,
          num_units_per_tray,
          distribute_from_dc_region_date,
          last_updated_date
          )
    values
  (       mer_mart.trading_date,
          mer_mart.post_date,
          mer_mart.item_no,
          mer_mart.supplier_no,          
          mer_mart.deliver_to_dc_region,
          mer_mart.distribute_from_dc_region,
          mer_mart.tombag_factor,
          mer_mart.deliver_to_dc_reg_date,
          mer_mart.deliver_to_dc_region_cases,
          mer_mart.num_units_per_tray,
          mer_mart.distribute_from_dc_region_date,
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

   insert /*+ APPEND parallel (hsp,2) */ into stg_jdaff_dc_plan_sup_ord_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM,LOC',
                            tmp.source_data_status_code,
                            tmp.trading_date,
                            tmp.post_date,
                            tmp.item_no,
                            tmp.supplier_no,
                            tmp.deliver_to_dc_region,
                            tmp.distribute_from_dc_region,
                            tmp.tombag_factor,
                            tmp.deliver_to_dc_reg_date,
                            tmp.deliver_to_dc_region_cases,
                            tmp.num_units_per_tray,
                            tmp.distribute_from_dc_region_date
    from  stg_jdaff_dc_plan_sup_ord_cpy  TMP 
    where not exists
          (select *
           from   fnd_item di
           where  tmp.item_no = di.item_no )  
           or
           not exists
          (select *
           from   fnd_supplier ds
           where  tmp.supplier_no = ds.supplier_no )
           or
           not exists
          (select *
           from   fnd_zone dz
           where  tmp.deliver_to_dc_region = dz.zone_no )
           or
           not exists
          (select *
           from   fnd_zone dzf
           where  tmp.distribute_from_dc_region = dzf.zone_no )
            and   sys_process_code = 'N'  
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
end WH_FND_CORP_173U;
