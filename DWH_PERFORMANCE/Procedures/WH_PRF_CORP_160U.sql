--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_160U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_160U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Novembe3r 2017
--  Author:      Q. Smit
--  Purpose:     Finance JV Inventory Adjustment Mart
--               with input from various DWH tables
--
--  Tables:      Input  - FND_RTL_SHIPMENT
--                        FND_RTL_INVENTORY_ADJ
--                        RTL_LOC_ITEM_DY_OM_ORD
--                        RTL_INV_ADJ_SUMMARY
--               Output - dwh_performance.MART_FDS_LOC_ITEM_DY_IAR
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_today_day          number;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_160U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD STOCK MANAGEMENT REPORT MART';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
g_from_date          date;
g_to_date             date;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF DWH_PERFORMANCE.MART_FDS_LOC_ITEM_DY_IAR STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    g_from_date := g_date - 35;
--    g_from_date := g_date - 32;
    g_to_date   := g_date + 1;
    
    
    l_text := 'FROM DATE - '||g_from_date || ' // TO DATE - ' || g_to_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    --g_to_date := 'Moo';

    while g_from_date < g_to_date 
      loop
      
        l_text := 'DATE BEING PROCESSED - '||g_from_date ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        merge /*+ parallel(iar,4) */ into DWH_PERFORMANCE.MART_FDS_LOC_ITEM_DY_IAR iar using (
    
        with itemlist as (select item_no, sk1_item_no, sk1_supplier_no from dim_item where business_unit_no = 50 ),
         loclist as  (select location_no, sk1_location_no from dim_location where district_no = 9963)
    
        select /*+ parallel(a,4) parallel(d,4) */ 
        dl.sk1_location_no, 
        c.sk1_item_no, 
        a.sdn_no, 
        c.sk1_supplier_no, 
        max(a.receive_date) as receive_date,
        max(a.item_no) as item_no, 
        sum(a.sdn_qty) as sdn_qty, 
        sum(a.sdn_qty * a.cost_price) sdn_cost, 
        max(a.final_loc_no) as final_loc_no,  
        
        
        max(b.ref_id_1)         as ss_ref_id_1,
        max(b.inv_adj_type)     as ss_inv_adj_type ,    
        max(b.tran_date)        as ss_tran_date,           
        max(b.reason_code)      as ss_reason_code,       
        max(b.reason_desc)      as ss_reason_desc,       
        max(b.liability_code)   as ss_liability_code,     
        max(b.inv_adj_qty)      as ss_inv_adj_qty,      
        max(b.inv_adj_cost)     as ss_inv_adj_cost,
        
        max(d.ref_id_1)         as cl_ref_id_1,
        max(d.inv_adj_type)     as cl_inv_adj_type ,    
        max(d.tran_date)        as cl_tran_date,           
        max(d.reason_code)      as cl_reason_code,       
        max(d.reason_desc)      as cl_reason_desc,   
        max(d.liability_type)   as cl_liability_type,
        max(d.liability_code)   as cl_liability_code,     
        max(d.inv_adj_qty)      as cl_inv_adj_qty,      
        max(d.inv_adj_cost)     as cl_inv_adj_cost
                
        from  fnd_rtl_shipment a  
        join itemlist c on a.item_no = c.item_no
        join loclist dl on a.final_loc_no = dl.location_no
        left join fnd_rtl_inventory_adj b on b.post_date >= a.receive_date
                                         and b.item_no = a.item_no  
                                         and nvl(b.ref_id_1,0) = a.sdn_no
                                         and b.inv_adj_type = 'SS'
                                         and b.reason_code = 109
                                         and b.liability_code = 57004
                                         and b.location_no = a.final_loc_no
                                         
        left join fnd_rtl_inventory_adj d on d.post_date >= a.receive_date
                                         and d.item_no = a.item_no  
                                         and nvl(d.ref_id_1,0) = a.sdn_no
                                         and d.inv_adj_type = 'C'
                                         and d.location_no = a.final_loc_no
                                                                 
        
        where receive_date     = g_from_date
        group by dl.sk1_location_no, c.sk1_item_no, a.sdn_no, c.sk1_supplier_no
        order by 1, 2, 3, 4, 5
         
           ) mer_mart
             on (iar.sk1_item_no     = mer_mart.sk1_item_no
            and  iar.sk1_location_no = mer_mart.sk1_location_no
            and  iar.si_sdn_no       = mer_mart.sdn_no
            and  iar.sk1_supplier_no = mer_mart.sk1_supplier_no)
            
          when matched then
            update 
               set  SI_RECEIVE_DATE   = mer_mart.RECEIVE_DATE,
                    SI_ITEM_NO        = mer_mart.ITEM_NO,
                    SI_SDN_QTY        = mer_mart.SDN_QTY,
                    SI_SDN_COST       = mer_mart.SDN_COST,
                    SS_REF_ID_1       = mer_mart.SS_REF_ID_1,
                    SS_INV_ADJ_TYPE   = mer_mart.SS_INV_ADJ_TYPE,
                    SS_TRAN_DATE      = mer_mart.SS_TRAN_DATE,
                    SS_REASON_CODE    = mer_mart.SS_REASON_CODE,
                    SS_REASON_DESC    = mer_mart.SS_REASON_DESC,
                    SS_LIABILITY_CODE = mer_mart.SS_LIABILITY_CODE,
                    SS_INV_ADJ_QTY    = mer_mart.SS_INV_ADJ_QTY,
                    SS_INV_ADJ_COST   = mer_mart.SS_INV_ADJ_COST,
                    CL_REF_ID_1       = mer_mart.CL_REF_ID_1,
                    CL_INV_ADJ_TYPE   = mer_mart.CL_INV_ADJ_TYPE,
                    CL_TRAN_DATE      = mer_mart.CL_TRAN_DATE,
                    CL_REASON_CODE    = mer_mart.CL_REASON_CODE,
                    CL_REASON_DESC    = mer_mart.CL_REASON_DESC,
                    CL_LIABILITY_TYPE = mer_mart.CL_LIABILITY_TYPE,
                    CL_LIABILITY_CODE = mer_mart.CL_LIABILITY_CODE,
                    CL_INV_ADJ_QTY    = mer_mart.CL_INV_ADJ_QTY,
                    CL_INV_ADJ_COST   = mer_mart.CL_INV_ADJ_COST,
                    LAST_UPDATED_DATE = g_date
                    
          when not matched then       
            insert 
              ( SK1_LOCATION_NO,
                SK1_ITEM_NO,
                SI_SDN_NO,
                SK1_SUPPLIER_NO,
                SI_RECEIVE_DATE,
                SI_ITEM_NO,
                SI_SDN_QTY  ,
                SI_SDN_COST ,
                SS_REF_ID_1  ,
                SS_INV_ADJ_TYPE ,
                SS_TRAN_DATE ,
                SS_REASON_CODE,
                SS_REASON_DESC ,
                SS_LIABILITY_CODE,
                SS_INV_ADJ_QTY  ,
                SS_INV_ADJ_COST,
                CL_REF_ID_1    ,
                CL_INV_ADJ_TYPE ,
                CL_TRAN_DATE  ,
                CL_REASON_CODE ,
                CL_REASON_DESC,
                CL_LIABILITY_TYPE,
                CL_LIABILITY_CODE,
                CL_INV_ADJ_QTY ,
                CL_INV_ADJ_COST,
                LAST_UPDATED_DATE
               )
            values
               (mer_mart.SK1_LOCATION_NO,
                mer_mart.SK1_ITEM_NO,
                mer_mart.SDN_NO,
                mer_mart.SK1_SUPPLIER_NO,
                mer_mart.RECEIVE_DATE,
                mer_mart.ITEM_NO,
                mer_mart.SDN_QTY,
                mer_mart.SDN_COST,
                mer_mart.SS_REF_ID_1,
                mer_mart.SS_INV_ADJ_TYPE,
                mer_mart.SS_TRAN_DATE,
                mer_mart.SS_REASON_CODE,
                mer_mart.SS_REASON_DESC,
                mer_mart.SS_LIABILITY_CODE,
                mer_mart.SS_INV_ADJ_QTY,
                mer_mart.SS_INV_ADJ_COST,
                mer_mart.CL_REF_ID_1,
                mer_mart.CL_INV_ADJ_TYPE,
                mer_mart.CL_TRAN_DATE,
                mer_mart.CL_REASON_CODE,
                mer_mart.CL_REASON_DESC,
                mer_mart.CL_LIABILITY_TYPE,
                mer_mart.CL_LIABILITY_CODE,
                mer_mart.CL_INV_ADJ_QTY,
                mer_mart.CL_INV_ADJ_COST,
                g_date
              )
            ;  
            
         g_from_date := g_from_date + 1;
         
         g_recs_inserted  :=  g_recs_inserted + SQL%ROWCOUNT;
         g_recs_updated   :=  g_recs_updated + SQL%ROWCOUNT;
         g_recs_read      :=  g_recs_read + SQL%ROWCOUNT;
      
         commit;
      
      end loop;
     
      commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_corp_160u;
