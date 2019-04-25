--------------------------------------------------------
--  DDL for Procedure WH_PRF2_PO_DELETE_3JUN2015
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF2_PO_DELETE_3JUN2015" 
(p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Wendy lyttle
--  Purpose:     DELETE OF PO_NO'S as these are going to be re-used by RMS
--               Total no. of po's in dim_purchase_order selected = 815836
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dwh_performance.rtl_loc_item_wk_sales_index%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

g_recs_DELETED_FRSD   NUMBER := 0;
g_recs_DELETED_FRPO   NUMBER := 0;
g_recs_DELETED_FRA    NUMBER := 0;
g_recs_DELETED_FRIM   NUMBER := 0;
g_recs_DELETED_RSD    NUMBER := 0;
g_recs_DELETED_FRS    NUMBER := 0;
g_recs_DELETED_FDAD   NUMBER := 0;
g_recs_DELETED_RPSLID NUMBER := 0;
g_recs_DELETED_RPCSW  NUMBER := 0;
g_tot_recs  NUMBER := 0;
g_gtot_recs  NUMBER := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF2_PO_DELETE_3JUN2015';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'delete po range 1000000 to 2999999';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process loop
-- BACKUPS TAKEN INTO FOLLOING TABLES :
--                        DWH_FOUNDATION.FND_RTL_STOCKOWN_DCR_old                    17818058 rows created.
--                        DWH_FOUNDATION.FND_RTL_PURCHASE_ORDER_old                 5166339 rows created.
--                        DWH_FOUNDATION.FND_RTL_ALLOCATION_old                             25278620 rows created.
--                        DWH_FOUNDATION.FND_RTL_INVOICE_MATCHING_old            1112154 rows created.
--                        DWH_PERFORMANCE.RTL_STOCKOWN_DCR_old                            4993479 rows created.
--                        DWH_FOUNDATION.FND_RTL_SHIPMENT_old                                 39359505 rows created.
--                        DWH_FOUNDATION.FND_DEAL_ACTUAL_DETAIL_old                   164 rows created.
--                        DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DYold    979849 rows created.
--                        DWH_PERFORMANCE.RTL_PO_CHAIN_SC_WK_old                        3951 rows created

--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

  execute immediate 'alter session enable parallel dml';
  

 /*      l_text := 'Running GATHER_TABLE_STATS ON RTL_STOCKOWN_DCR';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                       'RTL_STOCKOWN_DCR', DEGREE => 8);

     
       l_text := 'Running GATHER_TABLE_STATS ON RTL_PO_SUPCHAIN_LOC_ITEM_DY';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                       'RTL_PO_SUPCHAIN_LOC_ITEM_DY', DEGREE => 8);

       l_text := 'Running GATHER_TABLE_STATS ON RTL_PO_CHAIN_SC_WK';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                       'RTL_PO_CHAIN_SC_WK', DEGREE => 8); 
*/
-------------------------------------------------------------------------------------------------------------------
g_recs_DELETED_FRSD    := 0;
g_recs_DELETED_FRPO    := 0;
g_recs_DELETED_FRA     := 0;
g_recs_DELETED_FRIM    := 0;
g_recs_DELETED_RSD     := 0;
g_recs_DELETED_FRS     := 0;
g_recs_DELETED_FDAD    := 0;
g_recs_DELETED_RPSLID  := 0;
g_recs_DELETED_RPCSW   := 0;
g_tot_recs  := 0;
g_gtot_recs  := 0;

l_text := 'STARTING DELETE ';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

FOR V_CUR IN
            (WITH selext AS
                          (SELECT PO_NO,
                                  SK1_PO_NO
                          FROM DIM_PURCHASE_ORDER
                          WHERE PO_NO BETWEEN 1000000 AND 2999999
                          AND PO_STATUS_CODE = 'C'
                          ORDER BY PO_NO
                          )
            SELECT /*+  parallel(a,4) */ DISTINCT b.PO_NO, b.sk1_PO_NO
            FROM DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DY A,
                  selext B
            WHERE a.SK1_PO_NO = b.SK1_PO_NO
              AND TRAN_DATE <= '17/FEB/09'
)
LOOP

 
      begin
            DELETE /*+  parallel(a,4) */ FROM DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DY a
              where a.SK1_PO_NO = V_CUR.SK1_PO_NO
              ;
              
              g_recs_DELETED_RPSLID := g_recs_DELETED_RPSLID + SQL%ROWCOUNT;
              commit;
              
                                  if g_recs_DELETED_RPSLID >= 100000
        then 
                  g_gtot_recs := g_gtot_recs + g_recs_DELETED_RPSLID;
                  l_text := 'recs deleted='||g_gtot_recs||' at po_no='||v_cur.po_no;
                  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
                  G_RECS_DELETED_RPSLID := 0;
      end if;
              
        exception
        when no_data_found then
          g_recs_DELETED_RPSLID := g_recs_DELETED_RPSLID ;
      end;      
 

END LOOP;



l_text := 'RTL_PO_SUPCHAIN_LOC_ITEM_DY recs deleted = '||g_recS_deleted_RPSLID;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 




--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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



END WH_PRF2_PO_DELETE_3JUN2015;
