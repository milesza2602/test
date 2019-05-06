--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_532U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_532U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        OCT 2009
--  Author:      Alastair de Wet
--  Purpose:     Create LIWk OM_ORD rollup fact table in the performance layer
--               with input ex lid OM_ORD table from performance layer.
--  Tables:      Input  - rtl_loc_item_dy_om_ord
--               Output - rtl_loc_item_wk_om_ord
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  20 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
--  06 OCT 2009 - Replaced Merge with Insert into select from with a generic partition truncate prior to run.
--  10 Feb 2016 - Add new ISO and Cust Orders interface measures from source input daily table                Ref: BK10Feb2016
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            rtl_loc_item_wk_om_ord%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_532U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE OM ORD PERFORMANCE to WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

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

    l_text := 'ROLLUP OF rtl_loc_item_wk_om_ord EX DAY LEVEL STARTED AT '||
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

for g_sub in 0..1 loop
    select fin_day_no, this_week_start_date, this_week_end_date
    into   g_fin_day_no, g_start_date, g_end_date
    from   dim_calendar
    where  calendar_date = g_date - (g_sub * 7);

    l_text := 'ROLLUP RANGE IS:- '||g_start_date||'  to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

--INSERT /*+ APPEND */ INTO dwh_performance.rtl_loc_item_wk_om_ord
INSERT /*+ parallel(wk_om,5) */ INTO dwh_performance.rtl_loc_item_wk_om_ord wk_om
   select /*+ full(lid) parallel(lid,5) */ 
          lid.sk1_location_no sk1_location_no,
          lid.sk1_item_no sk1_item_no,
          dc.fin_year_no fin_year_no,
          dc.fin_week_no fin_week_no,
          max(dc.fin_week_code) fin_week_code,
          max(dc.this_week_start_date) this_week_start_date,
          max(lid.sk2_location_no) sk2_location_no,
          max(lid.sk2_item_no) sk2_item_no ,
          sum(nvl(	roq_qty	,0)) as 	roq_qty	,
          sum(nvl(	roq_cases	,0)) as 	roq_cases	,
          sum(nvl(	roq_selling	,0)) as 	roq_selling	,
          sum(nvl(	roq_cost	,0)) as 	roq_cost	,
          sum(nvl(	cust_order_qty	,0)) as 	cust_order_qty	,
          sum(nvl(	cust_order_cases	,0)) as 	cust_order_cases	,
          sum(nvl(	cust_order_selling	,0)) as 	cust_order_selling	,
          sum(nvl(	cust_order_cost	,0)) as 	cust_order_cost	,
          sum(nvl(	sod_boh_qty	,0)) as 	sod_boh_qty	,
          max(g_date) last_update_date,
          sum(nvl(	sod_boh_selling	,0)) as 	sod_boh_selling	,
          sum(nvl(	sod_boh_cost	,0)) as 	sod_boh_cost,

          -- Ref: BK10Feb2016 (start)
          sum(nvl(	EMERGENCY_ORDER_QTY	,0))      as 	EMERGENCY_order_qty	,
          sum(nvl(	EMERGENCY_order_cases	,0))    as 	EMERGENCY_order_cases	,
          sum(nvl(	EMERGENCY_order_selling	,0))  as 	EMERGENCY_order_selling	,
          sum(nvl(	EMERGENCY_order_cost	,0))    as 	EMERGENCY_order_cost	,

          sum(nvl(	IN_STORE_order_qty	,0))      as 	IN_STORE_order_qty	,
          sum(nvl(	IN_STORE_order_cases	,0))    as 	IN_STORE_order_cases	,
          sum(nvl(	IN_STORE_order_selling	,0))  as 	IN_STORE_order_selling	,
          sum(nvl(	IN_STORE_order_cost	,0))      as 	IN_STORE_order_cost	,

          sum(nvl(	ZERO_BOH_order_qty	,0))      as 	ZERO_BOH_order_qty	,
          sum(nvl(	ZERO_BOH_order_cases	,0))    as 	ZERO_BOH_order_cases	,
          sum(nvl(	ZERO_BOH_order_selling	,0))  as 	ZERO_BOH_order_selling	,
          sum(nvl(	ZERO_BOH_order_cost	,0))      as 	ZERO_BOH_order_cost	,

          sum(nvl(	SCANNED_order_qty	,0))        as 	SCANNED_order_qty	,
          sum(nvl(	SCANNED_order_cases	,0))      as 	SCANNED_order_cases	,
          sum(nvl(	SCANNED_order_selling	,0))    as 	SCANNED_order_selling	,
          sum(nvl(	SCANNED_order_cost	,0))      as 	SCANNED_order_cost
          -- Ref: BK10Feb2016 (end)
   from   rtl_loc_item_dy_om_ord lid,
          dim_calendar dc
   where  lid.post_date         = dc.calendar_date and
          lid.post_date          between g_start_date and g_end_date
   group by dc.fin_year_no,dc.fin_week_no,
            lid.sk1_item_no,lid.sk1_location_no;

  g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;


    commit;
end loop;
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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_corp_532u;
