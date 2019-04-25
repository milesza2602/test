--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_256U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_256U" (p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Quentin Smit
--  Purpose:     Foods Renewal datacheck comparison extract 2
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   foods_renewal_extract3_1
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            DWH_PERFORMANCE.foods_renewal_extract3_1%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_256U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FOODS RENEWAL DATACHECK COMPARISON EXTRACT 3 PART 1 - ORDERS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_PERFORMANCE.foods_renewal_extract3_1%rowtype index by binary_integer;
type tbl_array_u is table of DWH_PERFORMANCE.foods_renewal_extract3_1%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_today_date        date := trunc(sysdate) - 1;


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

    l_text := 'LOAD OF foods_renewal_extract_3_1 started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table DWH_PERFORMANCE.foods_renewal_extract3_1');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';
-- ######################################################################################### --
-- The outer joins are needed as there are cases when there are no sales in dense for items  --
-- which must be included in order to show a zero sales index as these records will be       --
-- created when the outer joins to either dense LY or the item price records are found       --
-- ######################################################################################### --

  l_text := 'Date being processed B4 lookup: ' || l_today_date ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 select today_date
   into l_today_date
   from dim_control;


 l_text := 'Date being processed AFTER lookup: ' || l_today_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


-- l_today_date := 'Moo';

INSERT /*+ APPEND PARALLEL (mart,4) */ INTO DWH_PERFORMANCE.foods_renewal_extract3_1 mart
with
 item_list as (select di.item_no, di.sk1_item_no, di.item_desc, di.sk1_supplier_no, dd.department_no, dd.department_name, di.subclass_no, di.subclass_name
                from dim_item di, dim_department dd
               --where dd.department_no in (44, 88, 41, 47, 68, 75, 85, 46, 51)  --(42,46,59,66,83,86,89)   --(41,58,73,81,88,96)   --,44,45,47,50,55,58,72,73,75,76,77,81,87,88,90,93,95,96,97,98,99)
               --where dd.department_no in (97,88,83,59,73,91,13,53,95,93,66,89,90,80,58,16,34,42,52,62,85)   --(88, 96, 73, 58, 47,55, 13, 44, 68, 81, 85, 75, 41, 40)
               --where dd.department_no in (15,20,41,43,48,64,71,78,99)
               --  where dd.department_no in (49,65,87,88)
--               where dd.department_no in (13,44,68,73,83,10,11,45,55,58,64,82,96)
--               where dd.department_no in (45,64,22,26,27,32,33,67,69,70,97,58,96)
--               where dd.department_no in (12,28,29,36,37,53,57,59,66,76,77,79,86,89,91,95,98)
               where dd.department_no in (14,17,18,19,21,23,24,30,31,35,39,50,56,60,63,72,80,84,90,93)
                 and di.department_no = dd.department_no),
 loc_list as (select location_no, location_name, sk1_location_no, wh_fd_zone_no from dim_location),
 supp_list as (select sk1_supplier_no, supplier_no, supplier_name from dim_supplier),

aa as (
select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.tran_date,
       b.location_no,
       a.item_no,
       e.supplier_no,
       max(not_before_date) not_before_date,
       sum(nvl(ORIGINAL_PO_QTY,0)) original_po_qty,
       sum(nvl(AMENDED_PO_QTY,0)) amended_po_qty,
       sum(nvl(CANCEL_PO_QTY,0)) cancel_po_qty,
       sum(nvl(PO_GRN_QTY,0)) po_grn_qty
from item_list a, loc_list b, RTL_PO_SUPCHAIN_LOC_ITEM_DY c, dim_calendar d, supp_list e
where c.tran_date =  l_today_date    --and '21/JUL/13'
and c.tran_date = d.calendar_date
and a.sk1_supplier_no = e.sk1_supplier_no
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.tran_date, b.location_no, a.item_no, e.supplier_no
),   -- select count(*) from aa;  --6951

bb as (
select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.post_date,
       b.location_no,
       a.item_no,
       e.supplier_no,
       sum(nvl(c.roq_qty,0)) roq_units,
       sum(nvl(c.cust_order_cases,0)) cust_order_cases,
       sum(nvl(c.cust_order_qty,0)) cust_order_qty,
       sum(nvl(c.cust_order_cost,0)) cust_order_cost
from item_list a, loc_list b, rtl_loc_item_dy_om_ord c, dim_calendar d, supp_list e
--from dim_item a, dim_location b, rtl_loc_item_dy_om_ord c
where c.post_date = l_today_date    --and '21/JUL/13'
and c.post_date = d.calendar_date
and a.sk1_supplier_no = e.sk1_supplier_no
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, b.location_no, a.item_no, e.supplier_no
),  --    select count(*)  from bb;    --22633  804931

cc as (
select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.tran_date,
       b.location_no,
       a.item_no,
       e.supplier_no,
       sum(nvl(c.FILLRATE_FD_PO_GRN_QTY_IMPORT,0)) FILLRATE_FD_PO_GRN_QTY_IMPORT,
       sum(nvl(c.FILLRATE_FD_PO_GRN_QTY_IMPORT,0)) fillrate_supp_import_qty,
       sum(nvl(c.FILLRTE_FD_LATEST_PO_QTY_IMPRT,0)) fillrte_fd_latest_po_qty_imprt
from item_list a, loc_list b, RTL_SUPCHAIN_LOC_ITEM_DY c, dim_calendar d, supp_list e
--from   dim_item a, dim_location b, RTL_SUPCHAIN_LOC_ITEM_DY c
where c.tran_date = l_today_date --and '21/JUL/13'
and c.tran_date = d.calendar_date
and a.sk1_supplier_no = e.sk1_supplier_no
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.tran_date, b.location_no, a.item_no, e.supplier_no
) ,  -- select count(*) from cc;    --9545   6951

dd as (
select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.post_date,
       b.location_no,
       a.item_no,
       e.supplier_no,
       max(dp.prom_type) prom_type
from item_list a, loc_list b, rtl_prom_loc_item_dy c, dim_calendar d, supp_list e, dim_prom dp
where c.post_date = l_today_date --and '21/JUL/13'
and c.post_date = d.calendar_date
and a.sk1_supplier_no = e.sk1_supplier_no
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no
and c.sk1_prom_no = dp.sk1_prom_no
and c.sk1_prom_period_no = 9749661
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, b.location_no, a.item_no, e.supplier_no
),   --  select count(*) from dd;   -- where item_no = 4953869001236;    --9545  82770

ee as (
select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.post_date,
       b.location_no,
       a.item_no,
       sum(nvl(sales_dly_app_fcst_qty,0)) sales_dly_app_fcst_qty
--from item_list a, loc_list b, rtl_loc_item_dy_rdf_fcst c, dim_calendar d, supp_list e     --RDF L1/L2 remapping change
from item_list a, loc_list b, RTL_LOC_ITEM_RDF_DYFCST_L2 c, dim_calendar d, supp_list e
where c.post_date = l_today_date --and '21/JUL/13'
and c.post_date = d.calendar_date
and a.sk1_supplier_no = e.sk1_supplier_no
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no
and sales_dly_app_fcst_qty > 0
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, b.location_no, a.item_no
),  --  select count(*) from ee;  -- where item_no = 4953869001236;    --9545   750397

ff as (
select /*+ PARALLEL(c,4) FULL(c) */
       d.fin_year_no,
       d.fin_week_no,
       d.fin_day_no,
       b.wh_fd_zone_no,
       c.post_date,
       b.location_no,
       a.item_no,
       sum(nvl(num_units_per_tray,0)) num_units_per_tray
from item_list a, loc_list b, rtl_loc_item_dy_st_ord c, dim_calendar d, supp_list e
where c.post_date = l_today_date --and '21/JUL/13'
and c.post_date = d.calendar_date
and a.sk1_supplier_no = e.sk1_supplier_no
and a.sk1_item_no = c.sk1_item_no
and b.sk1_location_no = c.sk1_location_no
group by d.fin_year_no, d.fin_week_no, d.fin_day_no, b.wh_fd_zone_no, c.post_date, b.location_no, a.item_no
),  --  select count(*) from ee;  -- where item_no = 4953869001236;    --9545   750397


xx as (
select nvl(nvl(nvl(nvl(nvl(a.fin_year_no, b.fin_year_no), c.fin_year_no), d.fin_year_no), e.fin_year_no), f.fin_year_no) fin_year_no,
       nvl(nvl(nvl(nvl(nvl(a.fin_week_no, b.fin_week_no), c.fin_week_no), d.fin_week_no), e.fin_week_no), f.fin_week_no) fin_week_no,
       nvl(nvl(nvl(nvl(nvl(a.fin_day_no, b.fin_day_no), c.fin_day_no), d.fin_day_no), e.fin_day_no), f.fin_day_no) fin_day_no,
       nvl(nvl(nvl(nvl(nvl(a.location_no, b.location_no), c.location_no), d.location_no), e.location_no), f.location_no) location_no,
       nvl(nvl(nvl(nvl(nvl(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no), e.wh_fd_zone_no), f.wh_fd_zone_no) wh_fd_zone_no,
       nvl(nvl(nvl(nvl(nvl(a.item_no, b.item_no), c.item_no), d.item_no), e.item_no), f.item_no) item_no,
       nvl(nvl(nvl(a.supplier_no, b.supplier_no), c.supplier_no), d.supplier_no) supplier_no,
       nvl(a.original_po_qty,0) original_po_qty,
       nvl(a.amended_po_qty,0) amended_po_qty,
       nvl(a.cancel_po_qty,0) cancel_po_qty,
       nvl(a.po_grn_qty,0) po_grn_qty,
       nvl(b.roq_units,0) roq_units,
       nvl(b.cust_order_cases,0) cust_order_cases,
       nvl(b.cust_order_qty,0) cust_order_qty,
       nvl(b.cust_order_cost,0) cust_order_cost,
       nvl(c.FILLRATE_FD_PO_GRN_QTY_IMPORT,0) fillrate_fd_po_grn_qty_import,
       nvl(c.fillrate_supp_import_qty,0)  fillrate_supp_import_qty,
       nvl(c.fillrte_fd_latest_po_qty_imprt,0) fillrte_fd_latest_po_qty_imprt,
       nvl(d.prom_type, '') prom_type,
       nvl(sales_dly_app_fcst_qty,0) forecast_qty,
       nvl(not_before_date, g_date) not_before_date,
       nvl(f.num_units_per_tray,0) num_units_per_tray

from aa a
full outer join bb b on a.item_no       = b.item_no
                    and a.location_no   = b.location_no
                    and a.wh_fd_zone_no = b.wh_fd_zone_no
                    and a.tran_date     = b.post_date

full outer join cc c on nvl(a.item_no, b.item_no)             = c.item_no
                    and nvl(a.location_no, b.location_no)     = c.location_no
                    and nvl(a.wh_fd_zone_no, b.wh_fd_zone_no) = c.wh_fd_zone_no
                    and nvl(a.tran_date, b.post_date)         = c.tran_date

full outer join dd d on nvl(nvl(a.item_no, b.item_no), c.item_no)                   = d.item_no
                    and nvl(nvl(a.location_no, b.location_no), c.location_no)       = d.location_no
                    and nvl(nvl(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no) = d.wh_fd_zone_no
                    and nvl(nvl(a.tran_date, b.post_date), c.tran_date)             = d.post_date

full outer join ee e on nvl(nvl(nvl(a.item_no, b.item_no), c.item_no), d.item_no)                         = e.item_no
                    and nvl(nvl(nvl(a.location_no, b.location_no), c.location_no), d.location_no)         = e.location_no
                    and nvl(nvl(nvl(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no) = e.wh_fd_zone_no
                    and nvl(nvl(nvl(a.tran_date, b.post_date), c.tran_date), d.post_date)                 = e.post_date

full outer join ff f on nvl(nvl(nvl(nvl(a.item_no, b.item_no), c.item_no), d.item_no), e.item_no)                               = f.item_no
                    and nvl(nvl(nvl(nvl(a.location_no, b.location_no), c.location_no), d.location_no), e.location_no)           = f.location_no
                    and nvl(nvl(nvl(nvl(a.wh_fd_zone_no, b.wh_fd_zone_no), c.wh_fd_zone_no), d.wh_fd_zone_no), e.wh_fd_zone_no) = f.wh_fd_zone_no
                    and nvl(nvl(nvl(nvl(a.tran_date, b.post_date), c.tran_date), d.post_date), e.post_date)                     = f.post_date
)

select xx.fin_year_no,
          xx.fin_week_no,
          xx.fin_day_no,
          xx.wh_fd_zone_no dc_region,
          xx.item_no,
          item_list.item_desc,
          xx.location_no,
          loc_list.location_name,
          item_list.department_no,
          item_list.department_name,
          item_list.subclass_no,
          item_list.subclass_name,
          supp_list.supplier_no,
          supp_list.supplier_name,
          --xx.app_fcst_selling,
          xx.prom_type,
          xx.original_po_qty,
          xx.amended_po_qty,
          xx.cancel_po_qty,
          xx.po_grn_qty,
          xx.roq_units,
          xx.cust_order_cases,
          xx.cust_order_qty,
          xx.cust_order_cost,
          xx.fillrate_fd_po_grn_qty_import,
          xx.fillrte_fd_latest_po_qty_imprt,
          xx.fillrate_supp_import_qty,
          xx.forecast_qty,
          xx.not_before_date,
          xx.num_units_per_tray
   from xx,
        item_list,
        supp_list,
        loc_list
   where xx.item_no = item_list.item_no
     and xx.supplier_no = supp_list.supplier_no
     and xx.location_no = loc_list.location_no
     --and xx.forecast_qty > 0
   order by item_no, location_no
 ;

g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;

commit;

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

end wh_prf_corp_256u;
