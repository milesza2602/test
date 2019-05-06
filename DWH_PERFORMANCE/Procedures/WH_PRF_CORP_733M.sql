--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_733M
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_733M" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2009
--  Author:      Alfonso Joshua
--  Purpose:     Update Depot item (DC catlg/avail) fact table in the performance layer
--               with input ex RMS location/item
--  Tables:      Input  - rtl_location_item
--               Output - rtl_depot_item_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--    01/02/2010 A. Joshua TD-2540  : Rectify ETL for adjusted catalogue/available days
--    20/10/2015 Q. Smit            : Rewritten as bulk merge
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
g_rec_out            rtl_depot_item_dy%rowtype;
g_found              boolean;
g_uda_value_no_or_text_or_date     varchar2(250) := 0;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_733M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DEPOT ITEM EX RMS LOC/ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure bulk_merge as
begin

  merge /*+ parallel(rtld,4) */ into w6005682.rtl_depot_item_dy_qs rtld using 
   (
     with loc_item as
       (select /*+ parallel(li,4) full(li) */ distinct
               li.sk1_item_no,
               di.item_no,
               dl.wh_fd_zone_no,
               di.fd_discipline_type
         from  rtl_location_item li,
               dim_location dl,
               dim_item di
         where li.sk1_location_no              = dl.sk1_location_no and
               li.sk1_item_no                  = di.sk1_item_no and
               li.this_wk_catalog_ind           = 1 and
               di.fd_discipline_type       in ('SA','SF')),
             
      item_uda as (select /*+ parallel(uda,4) full(uda) */ uda.item_no, uda.uda_value_no_or_text_or_date from fnd_item_uda uda where uda.uda_no = 542),
    
    ZONE_LOC as
       (select /*+ parallel(li,4) full(uda) full(dl) parallel(uda) full(uda) */
               dl.sk1_location_no,
               li.sk1_item_no,
               li.item_no,
               nvl(uda.uda_value_no_or_text_or_date,0)  uda_value_no_or_text_or_date
        from   dim_location dl
        join   loc_item li  on dl.wh_fd_zone_no      = li.wh_fd_zone_no 
                           and dl.wh_discipline_type = li.fd_discipline_type 
                           
        left join item_uda uda on li.item_no   = uda.item_no
       
        where  dl.loc_type           = 'W' 
          and  dl.stock_holding_ind  = 0 ),  -- select * from zone_loc;
    
    DEPOT as
        (select /*+ parallel(cat,4) full(cat) */ 
                sk1_item_no,
                sk1_location_no,
                post_date,
                nvl(stock_cases,0) stock_cases
         from   rtl_depot_item_dy
         where  last_updated_date = '19/OCT/15')
    
     select /*+ parallel(cat,4) parallel(zl,4) full(cat) full(zl) parallel(pck,4) full(pck) */
              unique cat.sk1_item_no,
              cat.sk1_location_no,
              cat.post_date,
              dep.stock_cases,
              nvl(pck.item_no,0) item_no,
              zl.item_no inline_item_no,
              
              -- FD_NUM_DC_AVAIL_DAYS
              case when dep.stock_cases > 0
                then 1
                else 0
              end as fd_num_dc_avail_days,
             
             
              -- FD_NUM_DC_CATLG_ADJ_DAYS
              case when nvl(pck.item_no,0) = 0 then
                  case when zl.uda_value_no_or_text_or_date = 1 then 1 else 0 end
                       else 0 
              end as fd_num_dc_catlg_adj_days,
                         
              
              -- FD_NUM_DC_AVAIL_ADJ_DAYS
                -- first check if fd_num_dc_avail_days = 1 or 0
              case when dep.stock_cases > 0 then 
                  case when nvl(pck.item_no,0) = 0 then
                    case when zl.uda_value_no_or_text_or_date = 1 then 1 else 0 end
                  else 0 end
                else 0
              end as fd_num_dc_avail_adj_days, 
              
              1 as fd_num_dc_catlg_days
            
       from   rtl_depot_item_dy cat,
              zone_loc zl,
              fnd_pack_item_detail pck,
              depot dep
       where  cat.post_date         = dep.post_date and
              cat.sk1_location_no   = dep.sk1_location_no and
              cat.sk1_item_no       = dep.sk1_item_no and
              cat.sk1_item_no       = zl.sk1_item_no and
              cat.sk1_location_no   = zl.sk1_location_no and
              zl.item_no            = pck.item_no(+)
                 
    ) mer_mart
    
    on (rtld.sk1_item_no      = mer_mart.sk1_item_no
   and  rtld.sk1_location_no  = mer_mart.sk1_location_no
   and  rtld.post_date        = mer_mart.post_date)
   
   when matched then
    update 
       set    fd_num_dc_catlg_days            = mer_mart.fd_num_dc_catlg_days,
              fd_num_dc_catlg_adj_days        = mer_mart.fd_num_dc_catlg_adj_days,
              fd_num_dc_avail_days            = mer_mart.fd_num_dc_avail_days,
              fd_num_dc_avail_adj_days        = mer_mart.fd_num_dc_avail_adj_days,
              last_updated_date               = g_date
       
      ;
     
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

    l_text := 'LOAD OF RTL_DEPOT_ITEM_DY FOODS EX LOC/ITEM STARTED AT '||
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

  --**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
  
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
end wh_prf_corp_733m;
