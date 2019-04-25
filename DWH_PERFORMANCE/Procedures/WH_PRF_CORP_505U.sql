--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_505U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_505U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Jan 2009
--  Author:      Alastair de Wet
--  Purpose:     Create LIWk stock rollup fact table in the performance layer
--               with input ex lid stock table from performance layer.
--  Tables:      Input  - rtl_loc_item_dy_rms_stock
--               Output - rtl_loc_item_wk_rms_stock
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_rec_out            rtl_loc_item_wk_rms_stock%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_505U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS STOCK PERFORMANCE to WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_stock EX DAY LEVEL STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_start_date  := g_date;
    l_text := 'START DATE OF ROLLUP - '||g_start_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

   INSERT /*+ APPEND */ INTO dwh_performance.rtl_loc_item_wk_rms_stock rtl_liwrs 
   select lid.sk1_location_no  as   sk1_location_no, 
          lid.sk1_item_no  as   sk1_item_no,
          dc.fin_year_no as fin_year_no,
          dc.fin_week_no as fin_week_no,
          dc.fin_week_code as fin_week_code,
          dc.this_week_start_date as this_week_start_date,
          lid.sk2_location_no  as   sk2_location_no,
          lid.sk2_item_no  as   sk2_item_no,
          lid.com_flag_ind  as  num_com_flag_ind,
          lid.sit_qty  as   sit_qty,
          lid.sit_cases  as sit_cases,
          lid.sit_selling  as   sit_selling,
          lid.sit_cost  as  sit_cost,
          lid.sit_fr_cost  as   sit_fr_cost,
          lid.sit_margin  as    sit_margin,
          lid.non_sellable_qty  as  non_sellable_qty,
          lid.soh_qty  as   soh_qty,
          lid.soh_cases  as soh_cases,
          lid.soh_selling  as   soh_selling,
          lid.soh_cost  as  soh_cost,
          lid.soh_fr_cost  as   soh_fr_cost,
          lid.soh_margin  as    soh_margin,
          lid.franchise_soh_margin  as  franchise_soh_margin,
          lid.inbound_excl_cust_ord_qty  as inbound_excl_cust_ord_qty,
          lid.inbound_excl_cust_ord_selling  as inbound_excl_cust_ord_selling,
          lid.inbound_excl_cust_ord_cost  as    inbound_excl_cust_ord_cost,
          lid.inbound_incl_cust_ord_qty  as inbound_incl_cust_ord_qty,
          lid.inbound_incl_cust_ord_selling  as inbound_incl_cust_ord_selling,
          lid.inbound_incl_cust_ord_cost  as    inbound_incl_cust_ord_cost,
          lid.boh_qty  as   boh_qty,
          lid.boh_cases  as boh_cases,
          lid.boh_selling  as   boh_selling,
          lid.boh_cost  as  boh_cost,
          lid.boh_fr_cost  as   boh_fr_cost,
          lid.clear_soh_qty  as clear_soh_qty,
          lid.clear_soh_selling  as clear_soh_selling,
          lid.clear_soh_cost  as    clear_soh_cost,
          lid.clear_soh_fr_cost  as clear_soh_fr_cost,
          lid.reg_soh_qty  as   reg_soh_qty,
          lid.reg_soh_selling  as   reg_soh_selling,
          lid.reg_soh_cost  as  reg_soh_cost,
          lid.reg_soh_fr_cost  as   reg_soh_fr_cost,
          lid.last_updated_date  as last_updated_date,
          lid.clear_soh_margin  as  clear_soh_margin,
          lid.reg_soh_margin  as  reg_soh_margin,
          null as CH_OPENING_REG_STOCK_QTY, 
          null as CH_OPENING_REG_STOCK_SELLING, 
          null as AVAIL_REG_STOCK_QTY, 
          null as AVAIL_REG_STOCK_SELLING
   from   rtl_loc_item_dy_rms_stock lid,
          dim_calendar dc
   where  lid.post_date         =  dc.calendar_date and
          lid.post_date         =  g_start_date;


   g_recs_read:=SQL%ROWCOUNT;
   g_recs_inserted:=SQL%ROWCOUNT;
 
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
end wh_prf_corp_505u;
