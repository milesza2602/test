--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_118M
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_118M" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        JAN 2009
--  Author:      Alastair de Wet
--  Purpose:     Load Allocation fact table in performance layer
--               with input ex RMS Allocation table from foundation layer (Foods Only).
--  Tables:      Input  - fnd_rtl_allocation
--               Output - rtl_loc_item_dy_rms_alloc
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  29 april 2015 wendy lyttle  DAVID JONES - do not load where  chain_code = 'DJ'
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
g_rec_out            rtl_loc_item_dy_rms_alloc%rowtype;
g_wac                rtl_loc_item_dy_rms_price.wac%type                  := 0;
g_reg_rsp_excl_vat   rtl_loc_item_dy_rms_price.reg_rsp_excl_vat%type     := 0;
g_num_units_per_tray rtl_loc_item_dy_rms_price.num_units_per_tray%type   := 1;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          := trunc(sysdate) - 14;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_118M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP RMS ALLOC DATA EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_merge as
begin

  --merge /*+ parallel(alloc,4) */ into rtl_loc_item_dy_rms_alloc alloc using (
  
  merge /*+ parallel(alloc,4) */ into W6005682.RTL_LOC_ITEM_DY_RMS_ALLOCQ alloc using (
  
      select /*+ PARALLEL(alloc, 4) full(alloc) parallel(fnd_li,4) full(fnd_li) */
          sum(nvl(aloc.alloc_qty,0)) alloc_qty,
          sum(nvl(aloc.apportion_qty,0)) apportion_qty,
          sum(nvl(aloc.sdn_qty,0)) sdn_qty,
          sum(nvl(aloc.orig_alloc_qty,0)) orig_alloc_qty,
          sum(nvl(aloc.dist_qty,0)) dist_qty,
          sum(nvl(aloc.received_qty,0)) received_qty,
          sum(nvl(aloc.alloc_cancel_qty,0)) alloc_cancel_qty,
          sum(nvl(aloc.special_qty,0)) special_qty,
          sum(nvl(aloc.safety_qty,0)) safety_qty,
          sum(nvl(aloc.priority1_qty,0)) priority1_qty,
          sum(nvl(aloc.overstock_qty,0)) overstock_qty,
          max(nvl(reg_rsp_excl_vat,0)) reg_rsp_excl_vat,
          max(nvl(wac,0)) wac,
          trunc(aloc.into_loc_date) into_loc_date,
          di.sk1_item_no,
          dl.sk1_location_no,
          max(aloc.item_no) item_no,
          max(aloc.to_loc_no) location_no,
          max(di.standard_uom_code) standard_uom_code,
          max(nvl(di.static_mass,1)) static_mass,
          max(nvl(di.random_mass_ind,0)) random_mass_ind,
          max(nvl(di.vat_rate_perc,0)) vat_rate_perc,
          max(nvl(fnd_li.num_units_per_tray,1)) num_units_per_tray,
          max(dlh.sk2_location_no) sk2_location_no,
          max(dih.sk2_item_no) sk2_item_no,
          
          round(sum(nvl(aloc.alloc_qty,0))        / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_alloc_cases     ,         
          round(sum(nvl(aloc.apportion_qty,0))    / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_apportion_cases   ,
          round(sum(nvl(aloc.sdn_qty,0))          / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_sdn_cases         ,        
          round(sum(nvl(aloc.orig_alloc_qty,0))   / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_orig_alloc_cases  ,
          round(sum(nvl(aloc.dist_qty,0))         / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_dist_cases        ,
          round(sum(nvl(aloc.received_qty,0))     / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_received_cases    ,
          round(sum(nvl(aloc.alloc_cancel_qty,0)) / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_alloc_cancel_cases ,
          round(sum(nvl(aloc.special_qty,0))      / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_p1_picking_cases  ,
          round(sum(nvl(aloc.safety_qty,0))       / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_p2_picking_cases  ,
          round(sum(nvl(aloc.priority1_qty,0))    / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_p3_picking_cases  , 
          round(sum(nvl(aloc.overstock_qty,0))    / max(nvl(fnd_li.num_units_per_tray,1)),0) fd_p4_picking_cases  ,  
          
         
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.alloc_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.alloc_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          end as fd_alloc_selling,
        
         
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.apportion_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.apportion_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_apportion_selling,
         
         
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.sdn_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.sdn_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_sdn_selling,
         
         
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.orig_alloc_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.orig_alloc_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_orig_alloc_selling   ,      
         
         
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.dist_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.dist_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_dist_selling ,
          
          
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.received_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.received_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_received_selling,
          
          
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.alloc_cancel_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.alloc_cancel_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_alloc_cancel_selling,
       
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.special_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.special_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_p1_picking_selling,
        
          
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.safety_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.safety_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_p2_picking_selling,
             
           
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.priority1_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.priority1_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_p3_picking_selling       ,
     
          
          case when max(di.standard_uom_code) = 'EA' and max(nvl(di.random_mass_ind,0)) = 1 then
            round(sum(nvl(aloc.overstock_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) * max(nvl(di.static_mass,1)) 
          else
            round(sum(nvl(aloc.overstock_qty,0))) * max(nvl(reg_rsp_excl_vat,0)) 
          end as fd_p4_picking_selling  ,                 
          
          round(sum(nvl(aloc.alloc_qty,0)))         * max(nvl(wac,0))   as fd_alloc_cost           ,
          round(sum(nvl(aloc.apportion_qty,0)))     * max(nvl(wac,0))   as fd_apportion_cost       ,
          round(sum(nvl(aloc.sdn_qty,0)))           * max(nvl(wac,0))   as fd_sdn_cost             ,
          round(sum(nvl(aloc.orig_alloc_qty,0)))    * max(nvl(wac,0))   as fd_orig_alloc_cost      ,
          round(sum(nvl(aloc.dist_qty,0)))          * max(nvl(wac,0))   as fd_dist_cost            , 
          round(sum(nvl(aloc.received_qty,0)))      * max(nvl(wac,0))   as fd_received_cost        ,
          round(sum(nvl(aloc.alloc_cancel_qty,0)))  * max(nvl(wac,0))   as fd_alloc_cancel_cost    , 
          round(sum(nvl(aloc.special_qty,0)))       * max(nvl(wac,0))   as fd_p1_picking_cost      , 
          round(sum(nvl(aloc.safety_qty,0)))        * max(nvl(wac,0))   as fd_p2_picking_cost      , 
          round(sum(nvl(aloc.priority1_qty,0)))     * max(nvl(wac,0))   as fd_p3_picking_cost      ,   
          round(sum(nvl(aloc.overstock_qty,0)))     * max(nvl(wac,0))   as fd_p4_picking_cost            
          

   from   fnd_rtl_allocation aloc,
          fnd_location_item fnd_li,
          dim_item di,
          dim_item_hist dih,
          dim_location dl,
          dim_location_hist dlh
   where  aloc.item_no                = di.item_no          and
          aloc.item_no                = dih.item_no         and
          aloc.into_loc_date         between dih.sk2_active_from_date and dih.sk2_active_to_date and
          aloc.to_loc_no              = dl.location_no      and
          aloc.to_loc_no              = dlh.location_no     and
          aloc.into_loc_date         between dlh.sk2_active_from_date and dlh.sk2_active_to_date  and
          aloc.item_no                = fnd_li.item_no(+) and
          aloc.to_loc_no              = fnd_li.location_no(+) and
          aloc.into_loc_date         between g_start_date  and g_date and
          di.business_unit_no        = 50 and
          aloc.into_loc_date         IS NOT NULL and
          (
          nvl(aloc.alloc_qty,0) <> 0 or
          nvl(aloc.apportion_qty,0) <> 0 or
          nvl(aloc.sdn_qty,0) <> 0 or
          nvl(aloc.orig_alloc_qty,0) <> 0 or
          nvl(aloc.dist_qty,0) <> 0 or
          nvl(aloc.received_qty,0) <> 0 or
          nvl(aloc.alloc_cancel_qty,0) <> 0 or
          nvl(aloc.special_qty,0) <> 0 or
          nvl(aloc.safety_qty,0) <> 0 or
          nvl(aloc.priority1_qty,0) <> 0 or
          nvl(aloc.overstock_qty,0) <> 0
          )
          AND (CHAIN_CODE <> 'DJ' or chain_code is null)
   group by di.sk1_item_no, 
            dl.sk1_location_no,  
            trunc(aloc.into_loc_date)

  ) mer_mart
  
  on (alloc.sk1_location_no = mer_mart.sk1_location_no
  and alloc.sk1_item_no     = mer_mart.sk1_item_no
  and alloc.calendar_date   = mer_mart.into_loc_date)
  
  when matched then
    update 
       set    fd_alloc_selling                = mer_mart.fd_alloc_selling,
              fd_alloc_cost                   = mer_mart.fd_alloc_cost,
              fd_alloc_qty                    = mer_mart.alloc_qty,
              fd_alloc_cases                  = mer_mart.fd_alloc_cases,
              fd_apportion_selling            = mer_mart.fd_apportion_selling,
              fd_apportion_cost               = mer_mart.fd_apportion_cost,
              fd_apportion_qty                = mer_mart.apportion_qty,
              fd_apportion_cases              = mer_mart.fd_apportion_cases,
              fd_sdn_selling                  = mer_mart.fd_sdn_selling,
              fd_sdn_cost                     = mer_mart.fd_sdn_cost,
              fd_sdn_qty                      = mer_mart.sdn_qty,
              fd_sdn_cases                    = mer_mart.fd_sdn_cases,
              fd_orig_alloc_selling           = mer_mart.fd_orig_alloc_selling,
              fd_orig_alloc_cost              = mer_mart.fd_orig_alloc_cost,
              fd_orig_alloc_qty               = mer_mart.orig_alloc_qty,
              fd_orig_alloc_cases             = mer_mart.fd_orig_alloc_cases,
              fd_dist_selling                 = mer_mart.fd_dist_selling,
              fd_dist_cost                    = mer_mart.fd_dist_cost,
              fd_dist_qty                     = mer_mart.dist_qty,
              fd_dist_cases                   = mer_mart.fd_dist_cases,
              fd_received_selling             = mer_mart.fd_received_selling,
              fd_received_cost                = mer_mart.fd_received_cost,
              fd_received_qty                 = mer_mart.received_qty,
              fd_received_cases               = mer_mart.fd_received_cases,
              fd_alloc_cancel_selling         = mer_mart.fd_alloc_cancel_selling,
              fd_alloc_cancel_cost            = mer_mart.fd_alloc_cancel_cost,
              fd_alloc_cancel_qty             = mer_mart.alloc_cancel_qty,
              fd_alloc_cancel_cases           = mer_mart.fd_alloc_cancel_cases,
              fd_p1_picking_selling           = mer_mart.fd_p1_picking_selling,
              fd_p1_picking_cost              = mer_mart.fd_p1_picking_cost,
              fd_p1_picking_qty               = mer_mart.special_qty,
              fd_p1_picking_cases             = mer_mart.fd_p1_picking_cases,
              fd_p2_picking_selling           = mer_mart.fd_p2_picking_selling,
              fd_p2_picking_cost              = mer_mart.fd_p2_picking_cost,
              fd_p2_picking_qty               = mer_mart.safety_qty,
              fd_p2_picking_cases             = mer_mart.fd_p2_picking_cases,
              fd_p4_picking_selling           = mer_mart.fd_p4_picking_selling,
              fd_p3_picking_selling           = mer_mart.fd_p3_picking_selling,
              fd_p3_picking_cost              = mer_mart.fd_p3_picking_cost,
              fd_p4_picking_cost              = mer_mart.fd_p4_picking_cost,
              fd_p3_picking_qty               = mer_mart.priority1_qty,
              fd_p4_picking_qty               = mer_mart.OVERSTOCK_QTY,
              fd_p4_picking_cases             = mer_mart.fd_p4_picking_cases,
              fd_p3_picking_cases             = mer_mart.fd_p3_picking_cases,
              last_updated_date               = g_date
      
  when not matched then
    insert 
        (  
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              CALENDAR_DATE,
              SK2_LOCATION_NO,
              SK2_ITEM_NO,
              FD_ALLOC_SELLING,
              FD_ALLOC_COST,
              FD_ALLOC_QTY,
              FD_ALLOC_CASES,
              FD_APPORTION_SELLING,
              FD_APPORTION_COST,
              FD_APPORTION_QTY,
              FD_APPORTION_CASES,
              FD_SDN_SELLING,
              FD_SDN_COST,
              FD_SDN_QTY,
              FD_SDN_CASES,
              FD_ORIG_ALLOC_SELLING,
              FD_ORIG_ALLOC_COST,
              FD_ORIG_ALLOC_QTY,
              FD_ORIG_ALLOC_CASES,
              FD_DIST_SELLING,
              FD_DIST_COST,
              FD_DIST_QTY,
              FD_DIST_CASES,
              FD_RECEIVED_SELLING,
              FD_RECEIVED_COST,
              FD_RECEIVED_QTY,
              FD_RECEIVED_CASES,
              FD_ALLOC_CANCEL_SELLING,
              FD_ALLOC_CANCEL_COST,
              FD_ALLOC_CANCEL_QTY,
              FD_ALLOC_CANCEL_CASES,
              FD_P1_PICKING_SELLING,
              FD_P1_PICKING_COST,
              FD_P1_PICKING_QTY,
              FD_P1_PICKING_CASES,
              FD_P2_PICKING_SELLING,
              FD_P2_PICKING_COST,
              FD_P2_PICKING_QTY,
              FD_P2_PICKING_CASES,
              FD_P4_PICKING_SELLING,
              FD_P3_PICKING_SELLING,
              FD_P3_PICKING_COST,
              FD_P4_PICKING_COST,
              FD_P3_PICKING_QTY,
              FD_P4_PICKING_QTY,
              FD_P4_PICKING_CASES,
              FD_P3_PICKING_CASES,
              LAST_UPDATED_DATE
        )
      
    values
        (
              mer_mart.SK1_LOCATION_NO,
              mer_mart.SK1_ITEM_NO,
              mer_mart.into_loc_date,
              mer_mart.SK2_LOCATION_NO,
              mer_mart.SK2_ITEM_NO,
              mer_mart.FD_ALLOC_SELLING,
              mer_mart.FD_ALLOC_COST,
              mer_mart.ALLOC_QTY,
              mer_mart.FD_ALLOC_CASES,
              mer_mart.FD_APPORTION_SELLING,
              mer_mart.FD_APPORTION_COST,
              mer_mart.APPORTION_QTY,
              mer_mart.FD_APPORTION_CASES,
              mer_mart.FD_SDN_SELLING,
              mer_mart.FD_SDN_COST,
              mer_mart.SDN_QTY,
              mer_mart.FD_SDN_CASES,
              mer_mart.FD_ORIG_ALLOC_SELLING,
              mer_mart.FD_ORIG_ALLOC_COST,
              mer_mart.ORIG_ALLOC_QTY,
              mer_mart.FD_ORIG_ALLOC_CASES,
              mer_mart.FD_DIST_SELLING,
              mer_mart.FD_DIST_COST,
              mer_mart.DIST_QTY,
              mer_mart.FD_DIST_CASES,
              mer_mart.FD_RECEIVED_SELLING,
              mer_mart.FD_RECEIVED_COST,
              mer_mart.RECEIVED_QTY,
              mer_mart.FD_RECEIVED_CASES,
              mer_mart.FD_ALLOC_CANCEL_SELLING,
              mer_mart.FD_ALLOC_CANCEL_COST,
              mer_mart.alloc_cancel_qty,
              mer_mart.FD_ALLOC_CANCEL_CASES,
              mer_mart.FD_P1_PICKING_SELLING,
              mer_mart.FD_P1_PICKING_COST,
              mer_mart.special_qty,
              mer_mart.FD_P1_PICKING_CASES,
              mer_mart.FD_P2_PICKING_SELLING,
              mer_mart.FD_P2_PICKING_COST,
              mer_mart.safety_qty,
              mer_mart.FD_P2_PICKING_CASES,
              mer_mart.FD_P4_PICKING_SELLING,
              mer_mart.FD_P3_PICKING_SELLING,
              mer_mart.FD_P3_PICKING_COST,
              mer_mart.FD_P4_PICKING_COST,
              mer_mart.priority1_qty,
              mer_mart.OVERSTOCK_QTY,
              mer_mart.FD_P4_PICKING_CASES,
              mer_mart.FD_P3_PICKING_CASES,
              g_date
        
        );
        
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
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
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD rtl_loc_item_dy_rms_alloc EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'RANGE BEING PROCESSED - '||g_start_date||' Through '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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

end wh_prf_corp_118m;
