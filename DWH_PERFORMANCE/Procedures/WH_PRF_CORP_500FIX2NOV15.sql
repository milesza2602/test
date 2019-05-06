--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_500FIX2NOV15
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_500FIX2NOV15" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
-- DAVID JONES DATAFIX
--**************************************************************************************************
--  Date:        2 NOV 2015
--  Author:       WENDY LYTTLE
--  Purpose:     Create LIWk Dense rollup fact table in the performance layer
--               with input ex lid dense table from performance layer.
--  Tables:      Input  - rtl_loc_item_dy_rms_dense
--               Output - rtl_loc_item_wk_rms_dense
--  Packages:    constants, dwh_log, dwh_valid
/*ROLLUP RANGE IS:- 27-APR-15  to 03-MAY-15
ROLLUP RANGE IS:- 04-MAY-15  to 10-MAY-15
ROLLUP RANGE IS:- 11-MAY-15  to 17-MAY-15
ROLLUP RANGE IS:- 18-MAY-15  to 24-MAY-15
ROLLUP RANGE IS:- 25-MAY-15  to 31-MAY-15
ROLLUP RANGE IS:- 01-JUN-15  to 07-JUN-15
ROLLUP RANGE IS:- 08-JUN-15  to 14-JUN-15
ROLLUP RANGE IS:- 15-JUN-15  to 21-JUN-15
ROLLUP RANGE IS:- 22-JUN-15  to 28-JUN-15
ROLLUP RANGE IS:- 29-JUN-15  to 05-JUL-15
ROLLUP RANGE IS:- 06-JUL-15  to 12-JUL-15
ROLLUP RANGE IS:- 13-JUL-15  to 19-JUL-15
ROLLUP RANGE IS:- 20-JUL-15  to 26-JUL-15
ROLLUP RANGE IS:- 27-JUL-15  to 02-AUG-15
ROLLUP RANGE IS:- 03-AUG-15  to 09-AUG-15
ROLLUP RANGE IS:- 10-AUG-15  to 16-AUG-15
ROLLUP RANGE IS:- 17-AUG-15  to 23-AUG-15
ROLLUP RANGE IS:- 24-AUG-15  to 30-AUG-15
*/

--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  20 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
--  06 Aug 2009 - Replaced Merge with Insert into select from with a generic partition truncate prior to run.
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
g_rec_out            rtl_loc_item_wk_rms_dense%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;
g_fin_year_no dim_calendar.fin_year_no%type;
g_fin_week_no dim_calendar.fin_week_no%type;
g_fin_month_no dim_calendar.fin_month_no%type;
g_stmt               varchar2(300);
g_part_period               varchar2(20);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_500FIX2NOV15';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS DENSE PERFORMANCE to WEEK';
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

    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_dense EX DAY LEVEL STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    g_date := '30 aug 2015';
 --   g_date := '10 may 2015';
    l_text := 'Start_date is '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
execute immediate 'alter session enable parallel dml';

--for g_sub in 0..17 loop
for g_sub in 0..17 loop

    select fin_day_no, this_week_start_date, this_week_end_date
    into   g_fin_day_no, g_start_date, g_end_date
    from   dim_calendar
    where  calendar_date = g_date - (g_sub * 7);

    select fin_day_no, fin_year_no, fin_month_no, fin_week_no
    into   g_fin_day_no, g_fin_year_no, g_fin_month_no, g_fin_week_no
    from   dim_calendar
    where  calendar_date = g_end_date ;

    l_text      := '-----------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'Rollup range is:- '||g_start_date||'  to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--RTL_LCITMRD_M20053_11

    g_part_period := 'M'||g_fin_year_no||g_fin_month_no||'_'||g_fin_week_no;

    l_text      := 'Truncate started - for subpartition RTL_LCITMRD_'||g_part_period;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    g_stmt      := 'Alter table  DWH_PERFORMANCE.RTL_LOC_ITEM_WK_RMS_DENSE truncate  subpartition RTL_LCITMRD_'||g_part_period;

    execute immediate g_stmt;

    commit;

    l_text      := 'Truncate completed - for subpartition RTL_LCITMRD_'||g_part_period;

    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

INSERT 
/*+ APPEND */ 
INTO dwh_performance.rtl_loc_item_wk_rms_dense rtl_liwrd
   select lid.sk1_location_no sk1_location_no,
          lid.sk1_item_no sk1_item_no,
          dc.fin_year_no fin_year_no,
          dc.fin_week_no fin_week_no,
          max(dc.fin_week_code) fin_week_code,
          max(dc.this_week_start_date) this_week_start_date,
          max(lid.sk2_location_no) sk2_location_no,
          max(lid.sk2_item_no) sk2_item_no ,
          sum(nvl(lid.sales_qty,0)) sales_qty,
          sum(nvl(lid.sales_cases,0)) sales_cases,
          sum(nvl(lid.sales,0)) sales,
          sum(nvl(lid.sales_incl_vat,0)) sales_incl_vat,
          sum(nvl(lid.sales_cost,0)) sales_cost,
          sum(nvl(lid.sales_fr_cost,0)) sales_fr_cost,
          sum(nvl(lid.sales_margin,0)) sales_margin,
          sum(nvl(lid.franchise_sales,0)) franchise_sales,
          sum(nvl(lid.franchise_sales_margin,0)) franchise_sales_margin,
          sum(nvl(lid.reg_sales_qty,0)) reg_sales_qty,
          sum(nvl(lid.reg_sales,0)) reg_sales,
          sum(nvl(lid.reg_sales_cost,0)) reg_sales_cost,
          sum(nvl(lid.reg_sales_fr_cost,0)) reg_sales_fr_cost,
          sum(nvl(lid.reg_sales_margin,0)) reg_sales_margin,
          sum(nvl(lid.franchise_reg_sales_margin,0)) franchise_reg_sales_margin,
          sum(nvl(lid.gross_sales_qty,0)) gross_sales_qty,
          sum(nvl(lid.gross_sales,0)) gross_sales,
          sum(nvl(lid.gross_sales_cost,0)) gross_sales_cost,
          sum(nvl(lid.gross_sales_fr_cost,0)) gross_sales_fr_cost,
          sum(nvl(lid.gross_reg_sales_qty,0)) gross_reg_sales_qty,
          sum(nvl(lid.gross_reg_sales,0)) gross_reg_sales,
          sum(nvl(lid.gross_reg_sales_cost,0)) gross_reg_sales_cost,
          sum(nvl(lid.gross_reg_sales_fr_cost,0)) gross_reg_sales_fr_cost,
          sum(nvl(lid.sdn_in_qty,0)) sdn_in_qty,
          sum(nvl(lid.sdn_in_selling,0)) sdn_in_selling,
          sum(nvl(lid.sdn_in_cost,0)) sdn_in_cost,
          sum(nvl(lid.sdn_in_fr_cost,0)) sdn_in_fr_cost,
          sum(nvl(lid.sdn_in_cases,0)) sdn_in_cases,
          sum(nvl(lid.actl_store_rcpt_qty,0)) actl_store_rcpt_qty,
          sum(nvl(lid.actl_store_rcpt_selling,0)) actl_store_rcpt_selling,
          sum(nvl(lid.actl_store_rcpt_cost,0)) actl_store_rcpt_cost,
          sum(nvl(lid.actl_store_rcpt_fr_cost,0)) actl_store_rcpt_fr_cost,
          sum(nvl(lid.store_deliv_selling,0)) store_deliv_selling,
          sum(nvl(lid.store_deliv_cost,0)) store_deliv_cost,
          sum(nvl(lid.store_deliv_fr_cost,0)) store_deliv_fr_cost,
          sum(nvl(lid.store_intake_qty,0)) store_intake_qty,
          sum(nvl(lid.store_intake_selling,0)) store_intake_selling,
          sum(nvl(lid.store_intake_cost,0)) store_intake_cost,
          sum(nvl(lid.store_intake_fr_cost,0)) store_intake_fr_cost,
          sum(nvl(lid.store_intake_margin,0)) store_intake_margin,
          sum(nvl(lid.sales_returns_qty,0)) sales_returns_qty,
          sum(nvl(lid.sales_returns_selling,0)) sales_returns_selling,
          sum(nvl(lid.sales_returns_cost,0)) sales_returns_cost,
          sum(nvl(lid.sales_returns_fr_cost,0)) sales_returns_fr_cost,
          sum(nvl(lid.reg_sales_returns_qty,0)) reg_sales_returns_qty,
          sum(nvl(lid.reg_sales_returns_selling,0)) reg_sales_returns_selling,
          sum(nvl(lid.reg_sales_returns_cost,0)) reg_sales_returns_cost,
          sum(nvl(lid.reg_sales_returns_fr_cost,0)) reg_sales_returns_fr_cost,
          sum(nvl(lid.clear_sales_returns_selling,0)) clear_sales_returns_selling,
          sum(nvl(lid.clear_sales_returns_cost,0)) clear_sales_returns_cost,
          sum(nvl(lid.clear_sales_returns_fr_cost,0)) clear_sales_returns_fr_cost,
          sum(nvl(lid.clear_sales_returns_qty,0)) clear_sales_returns_qty,
          max(g_date) last_update_date,
          sum(nvl(lid.store_deliv_qty,0)) store_deliv_qty,
          sum(nvl(lid.store_deliv_cases,0)) store_deliv_cases,
          sum(nvl(lid.eol_sales,0)) eol_sales,
          sum(nvl(lid.eol_sales_qty,0)) eol_sales_qty,
          sum(nvl(lid.eol_discount,0)) eol_discount
   from   rtl_loc_item_dy_rms_dense lid,
          dim_calendar dc
   where  lid.post_date         = dc.calendar_date and
          lid.post_date          between g_start_date and g_end_date
   group by dc.fin_year_no,dc.fin_week_no,
            lid.sk1_item_no,lid.sk1_location_no;

 g_recs_inserted :=   0;
  g_recs_inserted :=   SQL%ROWCOUNT;


    commit;

    l_text := 'recs inserted =  '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end loop;

    l_text      := '-----------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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

end wh_prf_corp_500FIX2NOV15;
