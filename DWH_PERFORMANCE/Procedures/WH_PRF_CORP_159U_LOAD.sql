--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_159U_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_159U_LOAD" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        October 2017
--  Author:      Q. Smit
--  Purpose:     Load Foods Services data into the Stock Management Report mart
--               with input from various RTL tables
--
--  Tables:      Input  - RTL_LOC_ITEM_DY_RMS_DENSE
--                        RTL_LOC_ITEM_DY_ST_ORD
--                        RTL_LOC_ITEM_DY_OM_ORD
--                        RTL_INV_ADJ_SUMMARY
--               Output - dwh_performance.MART_FDS_LOC_ITEM_DY_SMR
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_159U_LOAD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD STOCK MANAGEMENT REPORT MART';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_to_date            date;


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
    l_text := 'LOAD OF DWH_PERFORMANCE.MART_FDS_LOC_ITEM_DY_SMR STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
   -- EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
   
   l_to_date := '07/DEC/17';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := '03/NOV/17';
    
    while g_date < l_to_date 
    loop
        l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        merge /*+ parallel(smr,4) */ into DWH_PERFORMANCE.MART_FDS_LOC_ITEM_DY_SMR smr using (
    
        with item_list as (select item_no, sk1_item_no from dim_item where business_unit_no = 50 ),
         loc_list as (select location_no, sk1_location_no from dim_location where district_no = 9963),
         
        dense_data as (
             select /*+ parallel(a,4) full(b) full(c) */ a.sk1_location_no, a.sk1_item_no, post_date, sum(sdn_in_qty) sdn_in_qty, sum(sdn_in_cost) sdn_in_cost
               from rtl_loc_item_dy_rms_dense a, item_list b, loc_list c
              where a.post_date = g_date
                and a.sk1_location_no = c.sk1_location_no
                and a.sk1_item_no = b.sk1_item_no
             group by a.sk1_location_no, a.sk1_item_no, post_date
             )  
             ,
        
        stord_data as (
             select /*+ parallel(a,4) full(b) full(c) */ a.sk1_location_no, a.sk1_item_no, post_date, sum(num_units_per_tray) num_units_per_tray
               from RTL_LOC_ITEM_DY_ST_ORD a, item_list b, loc_list c
              where a.post_date = g_date
                and a.sk1_location_no = c.sk1_location_no
                and a.sk1_item_no = b.sk1_item_no
             group by a.sk1_location_no, a.sk1_item_no, post_date
             ) 
        ,
        
        omord_data as (
             --select /*+ parallel(a,4) full(b) full(c) */ a.sk1_location_no, a.sk1_item_no, post_date, sum(roq_qty) roq_qty, sum(roq_cost) roq_cost
             select /*+ parallel(a,4) full(b) full(c) */ a.sk1_location_no, a.sk1_item_no, post_date, sum(scanned_order_qty) roq_qty, sum(scanned_order_cost) roq_cost
               from RTL_LOC_ITEM_DY_OM_ORD a, item_list b, loc_list c
              where a.post_date = g_date
                and a.sk1_location_no = c.sk1_location_no
                and a.sk1_item_no = b.sk1_item_no
                and (roq_qty is not null or roq_cost is not null)
             group by a.sk1_location_no, a.sk1_item_no, post_date
             )
             ,
             
        invadj_data as (
             select /*+ parallel(a,4) full(b) full(c) */ a.sk1_location_no, a.sk1_item_no, post_date, sum(inv_adj_qty) inv_adj_qty, sum(inv_adj_cost) inv_adj_cost
               from RTL_INV_ADJ_SUMMARY a, item_list b, loc_list c, DIM_IA_LIABILITY_CODE d, DIM_IA_REASON_CODE e
              where a.post_date = g_date
                and a.sk1_location_no = c.sk1_location_no
                and a.sk1_item_no = b.sk1_item_no
                and a.SK1_IA_LIABILITY_CODE = d.SK1_IA_LIABILITY_CODE
                and d.ia_liability_code = 57004
                and a.SK1_IA_REASON_CODE = e.SK1_IA_REASON_CODE
                and e.ia_reason_code in (43, 109)
             group by a.sk1_location_no, a.sk1_item_no, post_date
             ) 
        
        
        select  nvl(nvl(nvl(dense.sk1_location_no, omord.sk1_location_no), stord.sk1_location_no), invadj.sk1_location_no) as sk1_location_no,
                nvl(nvl(nvl(dense.sk1_item_no, omord.sk1_item_no), stord.sk1_item_no), invadj.sk1_item_no) as sk1_item_no, 
                nvl(nvl(nvl(dense.post_date, omord.post_date), stord.post_date), invadj.post_date) as post_date, 
        
                case when omord.roq_qty >0 then
                  (omord.roq_cost / omord.roq_qty) 
                else 
                   0
                end as cost_price,
                stord.num_units_per_tray as units_per_tray,
                omord.roq_qty,
                omord.roq_cost,
                dense.sdn_in_qty, 
                dense.sdn_in_cost,
                invadj.inv_adj_qty,
                invadj.inv_adj_cost,
                g_date as last_updated_date
                
        
        from dense_data dense
        
        full outer join stord_data stord on dense.sk1_location_no = stord.sk1_location_no     --post_date
                                                    and dense.sk1_item_no     = stord.sk1_item_no
                                                    and dense.post_date       = stord.post_date
        
        full outer join omord_data omord on nvl(dense.sk1_location_no, stord.sk1_location_no) = omord.sk1_location_no
                                                    and nvl(dense.sk1_item_no,     stord.sk1_item_no)     = omord.sk1_item_no
                                                    and nvl(dense.post_date,       stord.post_date)       = omord.post_date
        
        full outer join invadj_data  invadj  on nvl(nvl(dense.sk1_location_no, stord.sk1_location_no), omord.sk1_location_no) = invadj.sk1_location_no
                                            and nvl(nvl(dense.sk1_item_no,     stord.sk1_item_no),     omord.sk1_item_no)     = invadj.sk1_item_no
                                            and nvl(nvl(dense.post_date,           stord.post_date),       omord.post_date)       = invadj.post_date
     
       ) mer_mart
         on (smr.sk1_item_no     = mer_mart.sk1_item_no
        and  smr.sk1_location_no = mer_mart.sk1_location_no
        and  smr.post_date       = mer_mart.post_date)
        
      when matched then
        update 
           set  COST_PRICE        = mer_mart.COST_PRICE,
                UNITS_PER_TRAY    = mer_mart.UNITS_PER_TRAY,
                ROQ_QTY           = mer_mart.ROQ_QTY,
                ROQ_COST          = mer_mart.ROQ_COST,
                SDN_IN_QTY        = mer_mart.SDN_IN_QTY,
                SDN_IN_COST       = mer_mart.SDN_IN_COST,
                INV_ADJ_QTY       = mer_mart.INV_ADJ_QTY,
                INV_ADJ_COST      = mer_mart.INV_ADJ_COST
                --LAST_UPDATED_DATE = g_date
                
      
      when not matched then       
        insert 
          ( SK1_LOCATION_NO,
            SK1_ITEM_NO,
            POST_DATE,
            COST_PRICE,
            UNITS_PER_TRAY,
            ROQ_QTY,
            ROQ_COST,
            SDN_IN_QTY,
            SDN_IN_COST,
            INV_ADJ_QTY,
            INV_ADJ_COST,
            LAST_UPDATED_DATE
           )
        values
           (mer_mart.SK1_LOCATION_NO,
            mer_mart.SK1_ITEM_NO,
            mer_mart.POST_DATE,
            mer_mart.COST_PRICE,
            mer_mart.UNITS_PER_TRAY,
            mer_mart.ROQ_QTY,
            mer_mart.ROQ_COST,
            mer_mart.SDN_IN_QTY,
            mer_mart.SDN_IN_COST,
            mer_mart.INV_ADJ_QTY,
            mer_mart.INV_ADJ_COST,
            g_date
          )
        ;  
          g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
          g_recs_updated :=  g_recs_updated + SQL%ROWCOUNT;
          g_recs_read :=  g_recs_read + SQL%ROWCOUNT;
          
          commit;
          
    g_date := g_date + 1;
    
  end loop;



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

end wh_prf_corp_159u_load;
