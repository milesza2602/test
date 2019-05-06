--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_620U_LOADM
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_620U_LOADM" (p_forall_limit in integer,p_success out boolean) as

--*************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Allocation fact table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - W6005682.STG_ALLOCATIONS
--               Output - fnd_rtl_allocation
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  29 april 2015 wendy lyttle  DAVID JONES - add chain_code derived from to_loc_no
--  14 may 2015 wendy lyttle  DAVID JONES add first_dc_no
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
--g_vat_rate_perc      dim_item.vat_rate_perc%type;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_rtl_allocation_hsp.sys_process_msg%type;
g_rec_out            fnd_rtl_allocation%rowtype;

g_found              boolean;
g_insert_rec         boolean;
--g_business_unit_no   dim_item.business_unit_no%type;
--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
g_start_date         date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_620U_LOAD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_tran;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_tran;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ALLOCATION FACTS EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_bulk_merge as
begin

  merge /*+ full(alloc) parallel(alloc,4) */ into fnd_rtl_allocation alloc using (
    select /*+ full(stg) parallel(stg,4) */ stg.*,
          -- NEW CODE TO DERIVE TAX PERCENTAGE FROM VARIOUS SOURCES
          -- If no location/item record is found, the vat region determines default vat rate 
          case when li.tax_perc is null then
              case when dl.vat_region_no = 1000 then
                  di.VAT_RATE_PERC
              else
                  dl.default_tax_region_no_perc
              end
          else 
              li.tax_perc
          end as vat_rate_perc,
          --  nvl(di.vat_rate_perc,14) vat_rate_perc,

          li.wac,
          nvl(li.reg_rsp,0)        reg_rsp    ,    
          
          CASE WHEN dl2.CHAIN_NO = 40 THEN 'DJ'
          WHEN dl2.CHAIN_no <> 40 THEN 'WW'
          ELSE
          NULL
          END CHAIN_CODE,
          dl.WH_PHYSICAL_WH_NO first_dc_no
          
    from W6005682.STG_ALLOC_2016_1 stg
        join dim_item di                     on stg.item_no    = di.item_no
        left outer join dim_location dl      on stg.wh_no      = dl.location_no
        left outer join dim_location dl2     on stg.to_loc_no  = dl2.location_no
        left outer join rtl_location_item li on di.sk1_item_no      = li.sk1_item_no  and
                                                dl2.sk1_location_no = li.sk1_location_no
                                                
    where sys_source_batch_id IN  (5657,5661)
 
                                                
   order by sys_source_batch_id,sys_source_sequence_no
   ) mer_mart

  on (alloc.alloc_no  = mer_mart.alloc_no
  and alloc.to_loc_no = mer_mart.to_loc_no)
  
  when matched then
    update
      set release_date                    = mer_mart.release_date,
          po_no                           = mer_mart.po_no,
          wh_no                           = mer_mart.wh_no,
          item_no                         = mer_mart.item_no,
          alloc_status_code               = mer_mart.alloc_status_code,
          to_loc_type                     = mer_mart.to_loc_type,
          sdn_qty                         = mer_mart.sdn_qty,
          alloc_qty                       = mer_mart.alloc_qty,
          dist_qty                        = mer_mart.dist_qty,
          apportion_qty                   = mer_mart.apportion_qty,
          alloc_cancel_qty                = mer_mart.alloc_cancel_qty,
          received_qty                    = mer_mart.received_qty,
          po_grn_qty                      = mer_mart.po_grn_qty,
          ext_ref_id                      = mer_mart.ext_ref_id,
          planned_into_loc_date           = mer_mart.planned_into_loc_date,
          into_loc_date                   = mer_mart.into_loc_date,
          scale_priority_code             = mer_mart.scale_priority_code,
          trunk_ind                       = mer_mart.trunk_ind,
          overstock_qty                   = mer_mart.overstock_qty,
          priority1_qty                   = mer_mart.priority1_qty,
          safety_qty                      = mer_mart.safety_qty,
          special_qty                     = mer_mart.special_qty,
          orig_alloc_qty                  = mer_mart.orig_alloc_qty,
          alloc_line_status_code          = mer_mart.alloc_line_status_code,
          --source_data_status_code         = mer_mart.source_data_status_code,
          --wac                             = mer_mart.wac,
          --reg_rsp_excl_vat                = mer_mart.reg_rsp_excl_vat,
          --last_updated_date               = mer_mart.last_updated_date,
          chain_code                      = mer_mart.chain_code,
          first_dc_no                     = mer_mart.first_dc_no ;


    g_recs_inserted :=  0;
    g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
    g_recs_read     :=  g_recs_read + SQL%ROWCOUNT; 
      
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

end local_bulk_merge;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_RTL_ALLOCTION EX OM STARTED AT '||
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
    l_text := 'LOCATION RANGE BEING PROCESSED - ALL!';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'STARTING MERGE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

    local_bulk_merge;
    
    l_text := 'MERGE COMPLETE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
       
end wh_fnd_corp_620u_loadm;
