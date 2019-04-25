--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_161U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_161U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        December 2017
--  Author:      Q. Smit
--  Purpose:     Finance JV Inventory Adjustment Mart
--               with input from various DWH tables
--
--  Tables:      Input  - FND_RTL_INVENTORY_ADJ
--                        DIM_LOCATION
--                        DIM_ITEM
--               Output - dwh_performance.MART_FDS_LOC_ITEM_DY_MAN_IA
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_161U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD STOCK JV MANUAL IA MART';
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
    l_text := 'LOAD OF DWH_PERFORMANCE.MART_FDS_LOC_ITEM_DY_MAN_IA STARTED '||
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
    g_to_date   := g_date + 1;
    
    l_text := 'FROM DATE - '||g_from_date || ' // TO DATE - ' || g_to_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    --g_to_date := 'Moo';

    while g_from_date < g_to_date 
      loop
      
        l_text := 'DATE BEING PROCESSED - '||g_from_date ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        merge /*+ parallel(iar,4) */ into DWH_PERFORMANCE.MART_FDS_LOC_ITEM_DY_MAN_IA iar using (
    
          with itemlist as (select item_no, sk1_item_no, department_no, sk1_supplier_no from dim_item where business_unit_no = 50 ),
               loclist as  (select location_no, sk1_location_no, linked_location_no from dim_location where district_no = 9963) 

          select /*+ parallel(a,4) */ dl.sk1_location_no, c.sk1_item_no, c.sk1_supplier_no, 
          a.tran_date               as tran_date,
          
          a.ref_id_1                as ref_id_1,
          max(a.ref_id_2)           as ref_id_2,
          
          sum(a.inv_adj_qty)        as inv_adj_qty,
          sum(a.inv_adj_cost)       as inv_adj_cost,
          a.inv_adj_type            as inv_adj_type,
          max(a.reason_code)        as reason_code,
          a.reason_desc             as reason_desc,
          max(a.liability_code)     as liability_code
          
          from  fnd_rtl_inventory_adj a  ,
                itemlist c, 
                loclist dl 
          
          where a.post_date = g_from_date
            and a.item_no = c.item_no
            and a.inv_adj_type = 'SS'
            and a.reason_code = 43
            and a.liability_code = 57004
            and a.location_no = dl.linked_location_no
            and a.ref_id_1 is not null
           
          group by sk1_location_no, sk1_item_no, sk1_supplier_no, tran_date, ref_id_1, inv_adj_type, reason_desc 
          order by 1, 2, 3, 4, 5
        ) mer_mart
    
             on (iar.sk1_item_no      = mer_mart.sk1_item_no
            and  iar.sk1_location_no  = mer_mart.sk1_location_no
            and  iar.sk1_supplier_no  = mer_mart.sk1_supplier_no
            and  iar.tran_date        = mer_mart.tran_date
            and  iar.ref_id_1         = mer_mart.ref_id_1)
            
          when matched then
            update 
               set  REF_ID_2          = mer_mart.REF_ID_2,
                    INV_ADJ_QTY       = mer_mart.INV_ADJ_QTY,
                    INV_ADJ_COST      = mer_mart.INV_ADJ_COST,
                    INV_ADJ_TYPE      = mer_mart.INV_ADJ_TYPE,
                    REASON_CODE       = mer_mart.REASON_CODE,
                    REASON_DESC       = mer_mart.REASON_DESC,
                    LIABILITY_CODE    = mer_mart.LIABILITY_CODE,
                    LAST_UPDATED_DATE = g_date
                    
          when not matched then       
            insert 
              ( SK1_LOCATION_NO,
                SK1_ITEM_NO,
                SK1_SUPPLIER_NO,
                TRAN_DATE,
                REF_ID_1,
                REF_ID_2,
                INV_ADJ_QTY,
                INV_ADJ_COST,
                INV_ADJ_TYPE,
                REASON_CODE,
                REASON_DESC,
                LIABILITY_CODE,
                LAST_UPDATED_DATE
               )
            values
               (mer_mart.SK1_LOCATION_NO,
                mer_mart.SK1_ITEM_NO,
                mer_mart.SK1_SUPPLIER_NO,
                mer_mart.TRAN_DATE,
                mer_mart.REF_ID_1,
                mer_mart.REF_ID_2,
                mer_mart.INV_ADJ_QTY,
                mer_mart.INV_ADJ_COST,
                mer_mart.INV_ADJ_TYPE,
                mer_mart.REASON_CODE,
                mer_mart.REASON_DESC,
                mer_mart.LIABILITY_CODE,
                g_date
              )
            ;  
            
         G_FROM_DATE := G_FROM_DATE + 1;
         
         g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
         g_recs_updated :=  g_recs_updated + SQL%ROWCOUNT;
         g_recs_read :=  g_recs_read + SQL%ROWCOUNT;
      
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

end wh_prf_corp_161u;
