--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_084U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_084U" (p_forall_limit in integer,p_success out boolean) as
-- ******************************************************************************************
--  Date:        Sept 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Warehouse locations on fnd_location_item 
--               with input ex OM fnd_zone_item_om table from foundation layer.
--  Tables:      Input  - fnd_zone_item_om
--               Output - fnd_location_item
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
-- 29 May 2009 - defect636    - Measures with a data type of text are causing issues in SSAS
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_084U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD location_item DC Location EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Bulk merge
--**************************************************************************************************
procedure local_bulk_merge as
begin
   
  merge /*+ parallel(fli,6) */ into fnd_location_item fli using 
  (
   select /*+ parallel(fiz,6) full(fiz) */ 
          fiz.*,
          location_no
   from   fnd_zone_item_om fiz,
          dim_location dl 
   where  fiz.zone_no             = dl.wh_fd_zone_no 
     and  fiz.zone_group_no       = dl.wh_fd_zone_group_no 
     and  dl.loc_type = 'W'     
 
    ) mer_mart
          
   on (fli.location_no                     = mer_mart.location_no 
   and fli.item_no                         = mer_mart.item_no 
      )
        
 when matched then
  update 
     set next_wk_deliv_pattern_code      = mer_mart.reg_delivery_pattern_code,
         this_wk_deliv_pattern_code      = mer_mart.reg_delivery_pattern_code,
         num_shelf_life_days             = mer_mart.num_shelf_life_days,
         num_units_per_tray              = mer_mart.num_units_per_tray,
         weigh_ind                       = mer_mart.weigh_ind,
         last_updated_date               = mer_mart.last_updated_date
      where (
            next_wk_deliv_pattern_code      <> mer_mart.reg_delivery_pattern_code or
            this_wk_deliv_pattern_code      <> mer_mart.reg_delivery_pattern_code or
            num_shelf_life_days             <> mer_mart.num_shelf_life_days or
            num_units_per_tray              <> mer_mart.num_units_per_tray or
            next_wk_deliv_pattern_code      is null or
            this_wk_deliv_pattern_code      is null or
            num_shelf_life_days             is null or
            num_units_per_tray              is null 
          )
   
 when not matched then
  insert 
     (
        item_no,
        location_no,
        next_wk_deliv_pattern_code,
        this_wk_deliv_pattern_code,
        num_shelf_life_days,
        num_units_per_tray,
        weigh_ind ,
        last_updated_date,
        primary_supplier_no,
        primary_country_code 
    )
    values
    (
        mer_mart.item_no,
        mer_mart.location_no,
        mer_mart.reg_delivery_pattern_code,
        mer_mart.reg_delivery_pattern_code,
        mer_mart.num_shelf_life_days,
        mer_mart.num_units_per_tray,
        mer_mart.weigh_ind,
        g_date,
        0,
        'ZA'
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
    
    l_text := 'LOAD OF fnd_location_item EX FOUNDATION STARTED AT '||
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
end wh_fnd_corp_084u;
