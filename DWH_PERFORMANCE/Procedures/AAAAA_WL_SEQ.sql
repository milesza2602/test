--------------------------------------------------------
--  DDL for Procedure AAAAA_WL_SEQ
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."AAAAA_WL_SEQ" 
(p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Wendy lyttle
--  Purpose:     Tests stats

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
g_SQL              VARCHAR2(4000);
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'AAWL_2RUN';
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

g_start_date date;

g_partition_name varchar2(40);

l_ly_fin_year_no    number;
l_ly_fin_week_no    number;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

execute immediate('INSERT /*+ APPEND  parallel(ssn,6) */ INTO DWH_PERFORMANCE.MART_CHASTCHNGRDSCWK_SEASON  SSN
   select * from DWH_PERFORMANCE.MART_CH_AST_CHN_GRD_SC_WK');
  commit;

  --    execute immediate('CREATE SEQUENCE dwh_performance.temp_seq START WITH 1');
  --    commit;
     
 

--insert /*+ APPEND  */ into dwh_performance.temp_nielsen
/*    WITH SELEXT AS (
SELECT fin_year_no as fin_year_no,
fin_week_no as fin_week_no,
location_no AS location_no,
'19 oct 2013' as last_wk_end_date,
item_no AS item_no,
lid2.sk1_location_no AS sk1_location_no,
lid2.sk1_item_no AS sk1_item_no,
lid2.sales_qty AS sales_qty,
lid2.sales as sales,
di.vat_rate_perc AS vat_rate_perc,
base_rsp AS base_rsp
FROM
dwh_performance.rtl_loc_item_wk_rms_dense lid2,
dwh_performance.dim_item di,
dwh_performance.dim_location dl
--,
--dwh_performance.dim_control dc
WHERE lid2.sk1_item_no = di.sk1_item_no
AND lid2.sk1_location_no = dl.sk1_location_no
AND lid2.fin_year_no = 2014
AND lid2.fin_week_no = 16
AND business_unit_no = 50
AND DEPARTMENT_NO <> 62
AND loc_type = 'S'
)
SELECT max(lid.last_wk_end_date) as last_wk_end_date,
lid.location_no AS location_no,
lid.item_no AS item_no,
 (DECODE ( (SIGN (NVL (SUM (lid.sales_qty), 0))),-1,0,NVL (SUM (lid.sales_qty), 0))) AS sales_qty,
(DECODE ( (SIGN(NVL ( SUM(lid.sales + (lid.sales * NVL (lid.vat_rate_perc, 0) / 100)), 0 ))), -1, 0,
NVL ( SUM(lid.sales + (lid.sales * NVL (lid.vat_rate_perc, 0) / 100)), 0 ) ))  AS sales,
 (DECODE ( (SIGN (NVL (SUM (stk.soh_qty), 0))), -1, 0, NVL (SUM (stk.soh_qty), 0)))  AS soh,
MAX(lid.base_rsp) AS base_rsp,
MAX (catlg.num_units_per_tray) AS num_units_per_tray
FROM
SELEXT lid,
dwh_performance.rtl_loc_item_wk_rms_stock stk,
dwh_performance.rtl_loc_item_wk_catalog catlg
WHERE lid.sk1_item_no = catlg.sk1_item_no(+)
AND lid.sk1_location_no = catlg.sk1_location_no(+)
AND lid.fin_year_no = catlg.fin_year_no(+)
AND lid.fin_week_no = catlg.fin_week_no(+)
AND lid.sk1_item_no = stk.sk1_item_no(+)
AND lid.sk1_location_no = stk.sk1_location_no(+)
AND lid.fin_year_no = stk.fin_year_no(+)
AND lid.fin_week_no = stk.fin_week_no(+)
GROUP BY lid.location_no ,
lid.item_no;


                                commit;

      execute immediate('create index dwh_performance.i10_p_tmp_mrt_6wkavg_PRM_DTS on dwh_performance.temp_mart_6wkavg_prom_dates
                            (week_count, item_no) TABLESPACE PRF_MASTER') ;
    l_text := 'create index dwh_performance.i10_p_tmp_mrt_6wkavg_PRM_DTS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     execute immediate('create index dwh_performance.i20_p_tmp_mrt_6wkavg_PRM_DTS on dwh_performance.temp_mart_6wkavg_prom_dates
                            (week_count, sk1_prom_no) TABLESPACE PRF_MASTER') ;
    l_text := 'create index dwh_performance.i20_p_tmp_mrt_6wkavg_PRM_DTS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     execute immediate('create index dwh_performance.i30_p_tmp_mrt_6wkavg_PRM_DTS on dwh_performance.temp_mart_6wkavg_prom_dates
                            (item_no) TABLESPACE PRF_MASTER') ;
      l_text := 'create index dwh_performance.i30_p_tmp_mrt_6wkavg_PRM_DTS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        */   
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

END AAAAA_WL_SEQ;
