--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_764U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_764U" (p_forall_limit in integer,p_success out boolean) as
-- ******************************************************************************************
--  Date:        June 2016
--  Author:      Alfonso Joshua
--  Purpose:     Create item_supplier dc plan orders fact table in the performance layer
--               with input ex staging table from JDAFF.
--  Tables:      Input  - fnd_jdaff_dc_plan_sup_ord_dy
--               Output - rtl_jdaff_dc_plan_sup_ord_dy
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            fnd_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_764U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ITEM_SUPPLIER DC PLAN ORDERS EX JDA FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Bulk merge
--**************************************************************************************************
procedure local_bulk_merge as
begin
   
  merge /*+ parallel(iso,4) */ into rtl_jdaff_dc_plan_sup_ord_dy iso using 
  (
   select /*+ parallel(jda,4) full (jda) full(ds) full (di) */ 
          jda.trading_date,
          jda.post_date,
          di.sk1_item_no,
          ds.sk1_supplier_no,
          dzt.sk1_zone_group_zone_no as sk1_deliver_to_dc_region,
          dzf.sk1_zone_group_zone_no as sk1_distribute_from_dc_region,
          jda.tombag_factor,
          jda.deliver_to_dc_reg_date,
          jda.deliver_to_dc_region_cases,
          jda.num_units_per_tray,
          jda.distribute_from_dc_region_date,
          g_date as last_updated_date
   from   fnd_jdaff_dc_plan_sup_ord_dy jda
    join  dim_item di        on jda.item_no = di.item_no 
    join  dim_supplier ds    on jda.supplier_no = ds.supplier_no
    join  dim_zone dzt       on jda.deliver_to_dc_region = dzt.zone_no 
    join  dim_zone dzf       on jda.distribute_from_dc_region = dzf.zone_no 
   where  jda.last_updated_date   = g_date
 
    ) mer_mart
          
     on  (mer_mart.sk1_item_no                   = iso.sk1_item_no
     and  mer_mart.sk1_supplier_no               = iso.sk1_supplier_no
     and  mer_mart.trading_date                  = iso.trading_date
     and  mer_mart.post_date                     = iso.post_date
     and  mer_mart.sk1_deliver_to_dc_region      = iso.sk1_deliver_to_dc_region
     and  mer_mart.sk1_distribute_from_dc_region = iso.sk1_distribute_from_dc_region
      )
        
 when matched then
  update 
     set  tombag_factor                          = mer_mart.tombag_factor,
          deliver_to_dc_reg_date                 = mer_mart.deliver_to_dc_reg_date,
          deliver_to_dc_region_cases             = mer_mart.deliver_to_dc_region_cases,
          num_units_per_tray                     = mer_mart.num_units_per_tray,
          distribute_from_dc_region_date         = mer_mart.distribute_from_dc_region_date
          
 when not matched then
  insert 
     (    trading_date,
          post_date,
          sk1_item_no,
          sk1_supplier_no,          
          sk1_deliver_to_dc_region,
          sk1_distribute_from_dc_region,
          tombag_factor,
          deliver_to_dc_reg_date,
          deliver_to_dc_region_cases,
          num_units_per_tray,
          distribute_from_dc_region_date,
          last_updated_date
    )
    values
    (     mer_mart.trading_date,
          mer_mart.post_date,
          mer_mart.sk1_item_no,
          mer_mart.sk1_supplier_no,          
          mer_mart.sk1_deliver_to_dc_region,
          mer_mart.sk1_distribute_from_dc_region,
          mer_mart.tombag_factor,
          mer_mart.deliver_to_dc_reg_date,
          mer_mart.deliver_to_dc_region_cases,
          mer_mart.num_units_per_tray,
          mer_mart.distribute_from_dc_region_date,
          g_date
    )
     ;
       
   g_recs_read      :=  g_recs_read + SQL%ROWCOUNT;
   g_recs_inserted  :=  g_recs_inserted + SQL%ROWCOUNT;
   g_recs_updated   :=  g_recs_updated + SQL%ROWCOUNT;

  COMMIT;

   exception
     when dwh_errors.e_insert_error then
       l_message := 'BULK MERGE - INSERT / UPDATE ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
     when others then
       l_message := 'BULK MERG - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;    
               

end local_bulk_merge;

--************************************************************************************************** 
-- Main process 
--**************************************************************************************************
begin 
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;   
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF RTL_JDAFF_ITEM_SUPP_DY_DC_ORD EX FOUNDATION STARTED AT '||
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
    
    execute immediate 'alter session enable parallel dml';

    local_bulk_merge;
       
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
end wh_prf_corp_764u;
