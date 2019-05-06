--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_085J
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_085J" (p_forall_limit in integer,p_success out boolean) as
-- ******************************************************************************************
--  Date:        March 2014
--  Author:      Quentin Smit
--  Purpose:     Update UPT on fnd_location_item
--               with input ex OM fnd_zone_item table from foundation layer.
--  Tables:      Input  - fnd_zone_item
--               Output - fnd_location_item
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
-- 29 May 2009 - defect636    - Measures with a data type of text are causing issues in SSAS
-- 25 Jun 2011 - QC 2981      - Add new field num_shelf_life_days ex Zone Item to update location item
--
-- March 2014                 - New version of WH_FND_CORP_085U to cater for Foods Renewal where the existing
--                              foundation table (fnd_zone_item_om) is going to be replaced by fnd_zone_item
--
-- Feb 2015                   - removed redundant code from cursor that was not needed and at the same time
--                              excluding data that should have been returned (BI defect 141)

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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_085J';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE location_item ex zone_item EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure bulk_merge as
begin

  merge /*+ parallel(fli,4) */ into fnd_location_item fli using 
  (
  select  /*+ parallel(li,4) parallel(zi,4) */
          li.item_no,
          li.location_no,
          zi.num_units_per_tray,
          zi.num_shelf_life_days,
          zi.zone_no, zi.supplier_no
   from   fnd_location_item li,
          dim_location dl,
          dim_item di,
          fnd_zone_item zi
   where  li.item_no              = di.item_no  
          and li.location_no      = dl.location_no 
          and li.item_no          = zi.item_no 
          and zi.zone_no          = dl.wh_fd_zone_no 
          and zi.zone_group_no    = dl.wh_fd_zone_group_no 
          and di.business_unit_no = 50  
          and (
           li.num_units_per_tray          is null or
           li.num_shelf_life_days         is null or
           li.num_shelf_life_days        <> zi.num_shelf_life_days or
           li.num_units_per_tray         <> zi.num_units_per_tray)
   ) mer_mart
   
 on (fli.item_no      = mer_mart.item_no
 and fli.location_no  = mer_mart.location_no)
 
 when matched then
   update 
      set num_units_per_tray              = mer_mart.num_units_per_tray,
          num_shelf_life_days             = mer_mart.num_shelf_life_days,
          last_updated_date               = g_date
  
  when not matched then 
    insert (ITEM_NO,                
            LOCATION_NO,            
            NUM_UNITS_PER_TRAY,     
            NUM_SHELF_LIFE_DAYS,    
            LAST_UPDATED_DATE
            )
    values (mer_mart.ITEM_NO,                
            mer_mart.LOCATION_NO,            
            mer_mart.NUM_UNITS_PER_TRAY,     
            mer_mart.NUM_SHELF_LIFE_DAYS,    
            g_date
            )
       ;

       g_recs_read      :=  g_recs_read + SQL%ROWCOUNT;
       g_recs_inserted  :=  g_recs_inserted + SQL%ROWCOUNT;
       g_recs_updated   :=  g_recs_updated + SQL%ROWCOUNT;

    COMMIT;

   exception
     when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
     when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end bulk_merge;

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
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    bulk_merge;
    
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
       
end wh_fnd_corp_085j;
