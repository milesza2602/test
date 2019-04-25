--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_120M
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_120M" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        SEPT 2008
--  Author:      Alastair de Wet
--  Purpose:     Create ACtual store rcpt fact table in the performance layer
--               with input ex RMS Shipment  table from foundation layer.
--  Tables:      Input  - fnd_rtl_shipment
--               Output - W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_rec_out            W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q%rowtype;
--g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_120M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS SHIPMENT DATA EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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

    l_text := 'MERGE INTO W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := '08/AUG/17';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- 'With' creates a sub query which is treated as a table called 'lid_list' and used in the from clause of the main query.
-- This option is known as subquery factoring and eliminates the need to create a temp table of the 1st result set.
--+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

merge /*+ parallel(rtl_dense,4) */ into W6005682.RTL_LOC_ITEM_DY_RMS_DENSE_Q rtl_dense
 using (
   with lid_list as
   (
   select /*+ parallel(shp,4) full(dl) */ item_no,to_loc_no,shp.actl_rcpt_date
   from   fnd_rtl_shipment shp,
          dwh_performance.dim_location dl
   where  shp.last_updated_date  = g_date and
       --   shipment_no between 2630000 and 2665000 and
          shp.to_loc_no          = dl.location_no and
          dl.loc_type            = 'S' and
          shp.actl_rcpt_date     is not null
 --         AND (CHAIN_CODE <> 'DJ' or chain_code is null)
   group by item_no,to_loc_no,shp.actl_rcpt_date
   )

   select /*+ parallel(shp,4) full(di) full(dih) full(dl) full(dlh) full(rli) parallel(rli,4) parallel(vr,4) */  sum(nvl(shp.received_qty,0)) as received_qty,
          
          --sum(nvl(shp.received_qty,0) * (shp.reg_rsp * 100 / (100 + di.vat_rate_perc))) as received_selling,
          -- NEW CODE TO DERIVE TAX PERCENTAGE FROM VARIOUS SOURCES
          sum(case when rli.tax_perc is null then
                case when dl.vat_region_no = 1000 then
                    nvl(shp.received_qty,0) * (shp.reg_rsp * 100 / (100 + di.VAT_RATE_PERC))
                else
                    nvl(shp.received_qty,0) * (shp.reg_rsp * 100 / (100 + dl.default_tax_region_no_perc))
                end
              else 
                nvl(shp.received_qty,0) * (shp.reg_rsp * 100 / (100 + rli.tax_perc  ))                               
              end) as received_selling,
           
          sum(nvl(shp.received_qty,0) * shp.cost_price) as received_cost,
          trunc(shp.actl_rcpt_date) as actl_rcpt_date,
          di.sk1_item_no,
          max(di.sk1_department_no) as sk1_department_no,
          dl.sk1_location_no,
          max(dl.chain_no) as chain_no,
          max(dl.sk1_fd_zone_group_zone_no) as sk1_fd_zone_group_zone_no ,
          max(dlh.sk2_location_no) as sk2_location_no,
          max(dih.sk2_item_no) as sk2_item_no,
          
          sum(case when dl.chain_no = 20 then
              ---         received_cost                 --                ---         received_cost                 --
              (nvl(shp.received_qty,0) * shp.cost_price) + round((nvl( (nvl(shp.received_qty,0) * shp.cost_price) ,0) * nvl(vr.debtors_commission_perc,0) / 100),2)
          else
              0
          end) as actl_store_rcpt_fr_cost
        
   from   fnd_rtl_shipment shp
          join lid_list  on shp.item_no                                 = lid_list.item_no
                        and shp.to_loc_no                               = lid_list.to_loc_no   
                        and shp.actl_rcpt_date                          = lid_list.actl_rcpt_date 
          join dim_item di on lid_list.item_no                          = di.item_no      
                                  
          join dim_item_hist dih on lid_list.item_no                    = dih.item_no  
          join dim_location dl on lid_list.to_loc_no                    = dl.location_no 
          join dim_location_hist dlh on lid_list.to_loc_no              = dlh.location_no     
          left outer join rtl_location_item rli on  rli.sk1_item_no     = di.sk1_item_no       
                                               and  rli.sk1_location_no = dl.sk1_location_no   
                                               
          left outer join dwh_performance.rtl_loc_dept_dy vr on dl.sk1_location_no    = vr.sk1_location_no
                                                            and di.sk1_department_no  = vr.sk1_department_no
                                                            and shp.actl_rcpt_date     = vr.post_date
 
   where  shp.received_qty            <> 0                      
     and  shp.received_qty            is not null               
     and  lid_list.actl_rcpt_date     between dih.sk2_active_from_date and dih.sk2_active_to_date 
     and  lid_list.actl_rcpt_date     between dlh.sk2_active_from_date and dlh.sk2_active_to_date 
          
   group by trunc(shp.actl_rcpt_date), di.sk1_item_no, dl.sk1_location_no 

  ) rtl_in

on (rtl_in.SK1_LOCATION_NO  = rtl_dense.SK1_LOCATION_NO
and rtl_in.SK1_ITEM_NO      = rtl_dense.SK1_ITEM_NO
and rtl_in.ACTL_RCPT_DATE   = rtl_dense.POST_DATE)
WHEN MATCHED
THEN
UPDATE
       set    actl_store_rcpt_qty             = rtl_in.received_qty,
              actl_store_rcpt_selling         = rtl_in.received_selling,
              actl_store_rcpt_cost            = rtl_in.received_cost,
              actl_store_rcpt_fr_cost         = rtl_in.actl_store_rcpt_fr_cost,
              last_updated_date               = g_date

WHEN NOT MATCHED
THEN
  insert 
     (
        sk1_location_no,
        sk1_item_no,
        post_date,
        
        actl_store_rcpt_qty,     
        actl_store_rcpt_selling,
        actl_store_rcpt_cost, 
        actl_store_rcpt_fr_cost,
        
        --sk1_department_no,
        --chain_no,
        --sk1_fd_zone_group_zone_no,
        sk2_location_no,
        sk2_item_no,
        
        last_updated_date
       )
  values 
       (rtl_in.sk1_location_no,
        rtl_in.sk1_item_no,
        rtl_in.ACTL_RCPT_DATE,
        
        rtl_in.received_qty,
        rtl_in.received_selling,
        rtl_in.received_cost,
        rtl_in.actl_store_rcpt_fr_cost,
        
        --rtl_in.sk1_department_no,
        --rtl_in.chain_no,
        --rtl_in.sk1_fd_zone_group_zone_no,
        rtl_in.sk2_location_no,
        rtl_in.sk2_item_no,
        
        g_date
       );

   g_recs_updated  := g_recs_updated  + sql%rowcount;

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
end wh_prf_corp_120m;
