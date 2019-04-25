--------------------------------------------------------
--  DDL for Procedure AAWL_RUN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."AAWL_RUN" 
(p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Wendy lyttle
--  Purpose:     Tests stats

--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs         integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dwh_performance.rtl_loc_item_wk_sales_index%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_SQL              VARCHAR2(4000);
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'AAWL_RUN';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'test stats';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.rtl_loc_item_wk_sales_index%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.rtl_loc_item_wk_sales_index%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_from_date         date := trunc(sysdate) - 1;
l_fin_year_no       number;
l_fin_week_no       number;
l_fin_year_month_no       number;
l_last_wk_start_date date;
l_last_wk_end_date    date;
l_last_wk_ly_start_date date;
l_last_wk_ly_end_date   date;

g_partition_name varchar2(40);

l_ly_fin_year_no    number;
l_ly_fin_week_no    number;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
-- ######################################################################################### --
-- The outer joins are needed as there are cases when there are no sales in dense for items  --
-- which must be included in order to show a zero sales index as these records will be       --
-- created when the outer joins to either dense LY or the item price records are found       --
-- ######################################################################################### --
DELETE FROM DWH_PERFORMANCE.RTL_STOCKOWN_DCR A
WHERE EXISTS(
SELECT /*+ FULL(B) PARALLEL(B,8) */ B.ASN_NO
,B.DU_ID
,B.DI_NO
,B.SK1_ITEM_NO
FROM DWH_DATAFIX.RTLSTKWNDCR_BCK19112014 B
WHERE A.ASN_NO = B.ASN_NO
AND A.DU_ID = B.DU_ID
AND A.DI_NO = B.DI_NO
AND A.SK1_ITEM_NO = B.SK1_ITEM_NO
);
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'RTL_STOCKOWN_DCR RECS DELETED='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
--******************************************************************************
DELETE FROM DWH_PERFORMANCE.RTL_LOC_ITEM_WK_STOCKOWN_DCR A
WHERE EXISTS(
SELECT /*+ FULL(B) PARALLEL(B,8) */ B.SK1_TO_LOCATION_NO
,B.SK1_ITEM_NO
,B.SK1_SUPPLIER_NO
,B.FIN_YEAR_NO
,B.FIN_WEEK_NO
FROM DWH_DATAFIX.RTL_LCITMWKSTKDCR_BCK19112014 B
WHERE A.SK1_TO_LOCATION_NO = B.SK1_TO_LOCATION_NO
AND A.FIN_YEAR_NO = B.FIN_YEAR_NO
AND A.FIN_WEEK_NO = B.FIN_WEEK_NO
AND A.SK1_ITEM_NO = B.SK1_ITEM_NO
AND A.SK1_SUPPLIER_NO = B.SK1_SUPPLIER_NO
);
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'RTL_LOC_ITEM_WK_STOCKOWN_DCR RECS DELETED='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
--******************************************************************************
DELETE FROM DWH_PERFORMANCE.RTL_LOC_SC_WK_STOCKOWN_DCR A
WHERE EXISTS(
SELECT /*+ FULL(B) PARALLEL(B,8) */ B.SK1_TO_LOCATION_NO
,B.SK1_STYLE_COLOUR_NO
,B.FIN_YEAR_NO
,B.FIN_WEEK_NO
FROM DWH_DATAFIX.RTL_LCSCWKSTKDCR_BCK19112014 B
WHERE A.SK1_TO_LOCATION_NO = B.SK1_TO_LOCATION_NO
AND A.FIN_YEAR_NO = B.FIN_YEAR_NO
AND A.FIN_WEEK_NO = B.FIN_WEEK_NO
AND A.SK1_STYLE_COLOUR_NO = B.SK1_STYLE_COLOUR_NO
);
G_RECS := sql%ROWCOUNT;
COMMIT;
L_TEXT := 'RTL_LOC_SC_WK_STOCKOWN_DCR RECS DELETED='||g_recs;
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


--*******************
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

END AAWL_RUN;
