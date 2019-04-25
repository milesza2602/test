--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_249U_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_249U_NEW" (p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        Reweritten May 2013
--  Author:      Quentin Smit
--  Purpose:     Derive Sales Index (Inflation)
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   rtl_loc_item_wk_sales_index FOR CURRENT WEEK TO DATE
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
g_rec_out            dwh_performance.rtl_loc_item_wk_sales_index%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_249U_NEW';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT SALES INDEX AT WEEK LEVEL FOR CURRENT WEEK TO DATE';
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

begin
   execute immediate 'alter session set optimizer_features_enable=''11.1.0.7''';
end;

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF rtl_loc_item_wk_sales_index started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session enable parallel dml';
-- ######################################################################################### --
-- The outer joins are needed as there are cases when there are no sales in dense for items  --
-- which must be included in order to show a zero sales index as these records will be       --
-- created when the outer joins to either dense LY or the item price records are found       --
-- ######################################################################################### --

 select today_fin_year_no,
          today_fin_week_no,
          this_wk_start_date,
          this_wk_end_date,
          TODAY_FIN_YEAR_NO||TODAY_FIN_MONTH_NO
     into l_fin_year_no,
          l_fin_week_no,
          l_last_wk_start_date,
          l_last_wk_end_date,
          L_FIN_YEAR_MONTH_NO
     from dim_control_report;

l_text := 'l_fin_year_no : ' || l_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_fin_week_no : ' || l_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select unique ly_fin_year_no, ly_fin_week_no                    --WK 53
      into l_ly_fin_year_no, l_ly_fin_week_no                       --WK 53
     from dim_calendar                                              --WK 53
     where fin_year_no = l_fin_year_no                              --WK 53
       and fin_week_no = l_fin_week_no;                             --WK 53

l_text := 'l_ly_fin_year_no : ' || l_ly_fin_year_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := 'l_ly_fin_week_no : ' || l_ly_fin_week_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     select this_week_start_date, this_week_end_date
       into l_last_wk_ly_start_date, l_last_wk_ly_end_date
       from dim_calendar_wk
      where fin_year_no = l_ly_fin_year_no    --l_fin_year_no - 1   --WK 53
        and fin_week_no = l_ly_fin_week_no;                         --WK 53

--     select this_week_start_date, this_week_end_date
--       into l_last_wk_ly_start_date, l_last_wk_ly_end_date
--       from dim_calendar_wk
--      where fin_year_no = l_fin_year_no - 1
--        and fin_week_no = l_fin_week_no;

     l_text := 'Year and week being processed : ' || l_fin_year_no || ' ' || l_fin_week_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_text := 'Start date for week being processed = ' || l_last_wk_start_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_text := 'End date for week being processed = ' || l_last_wk_end_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_text := 'Start date for LY week being processed = ' || l_last_wk_ly_start_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_text := 'End date for LY week being processed = ' || l_last_wk_ly_end_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     --l_from_date := 'Moo';

--dbms_stats.gather_table_stats (,,partname=>'TP_RTL_LIDRMSPRC_M20141')

--**------------------------------------------------
-- Partition Stats
--**------------------------------------------------

g_partition_name := 'TP_RTL_LIDRMSPRC_M'||L_FIN_YEAR_MONTH_NO;

dbms_stats.gather_table_stats ('DWH_PERFORMANCE',
                                'RTL_LOC_ITEM_DY_RMS_PRICE',
                                        g_partition_name,
                                        granularity => 'PARTITION',
                                        degree => 4);
commit;

     l_text := 'Partition_stats on  = ' ||g_partition_name;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**------------------------------------------------
-- Insert
--**------------------------------------------------
INSERT /*+ APPEND PARALLEL (mart,6) */ INTO dwh_performance.rtl_loc_item_wk_sales_index mart
with rms_dense_measures_ty as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       sl.wh_fd_zone_no,
       f.sales sales,
       f.sales_qty sales_qty,
       nvl(f.sales_margin,0) sales_margin
from   rtl_loc_item_wk_rms_dense f, dim_item il, dim_location sl
where f.sk1_item_no       = il.sk1_item_no
and f.sk1_location_no     = sl.sk1_location_no
and f.fin_year_no         = l_fin_year_no  -- current week fin year
and f.fin_week_no         = l_fin_week_no  -- current week to date
)  ,

sales_ly_by_region as (
select /*+ PARALLEL(f,6) FULL(f) */
       sl.wh_fd_zone_no,
       f.sk1_item_no,
       sum(f.sales) sales_ly_zone
from   rtl_loc_item_wk_rms_dense f, dim_item il, dim_location sl
where f.sk1_item_no       = il.sk1_item_no
and f.sk1_location_no     = sl.sk1_location_no
and f.fin_year_no         = l_ly_fin_year_no   --l_fin_year_no - 1  -- current week last fin year
and f.fin_week_no         = l_ly_fin_week_no   --l_fin_week_no    -- current week to date
group by sl.wh_fd_zone_no, f.sk1_item_no
),

rms_dense_measures_ly as (
select /*+ PARALLEL(f,6) FULL(f) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sales sales_ly,
       f.sales_qty sales_qty_ly,
       nvl(f.sales_margin,0) sales_margin_ly
from   rtl_loc_item_wk_rms_dense f, dim_item il, dim_location sl
where f.sk1_item_no       = il.sk1_item_no
and f.sk1_location_no     = sl.sk1_location_no
and f.fin_year_no         = l_ly_fin_year_no   --l_fin_year_no - 1  -- current week last fin year
and f.fin_week_no         = l_ly_fin_week_no   --l_fin_week_no  -- current week to date
) ,

aa_ly as (select /*+ PARALLEL(pr,6) FULL(pr) */
       pr.sk1_location_no,
       pr.sk1_item_no,
       max(pr.calendar_date) last_prom_date
from rtl_loc_item_dy_rms_price pr,
     dim_item il,
     dim_location sl
where pr.sk1_location_no = sl.sk1_location_no
and pr.sk1_item_no = il.sk1_item_no
and pr.calendar_date between l_last_wk_ly_start_date and l_last_wk_ly_end_date                   -- l_fin_year_no - 1 l_fin_week_no
and pr.prom_rsp is not null
and il.business_unit_no = 50
group by pr.sk1_location_no, pr.sk1_item_no
),  -- select * from aa;
bb_ly as (
select /*+ PARALLEL(pr,6) FULL(pr) */
       unique pr.sk1_location_no,
       pr.sk1_item_no,
       pr.prom_rsp,
       aa_ly.last_prom_date prom_date
from rtl_loc_item_dy_rms_price pr
     left join aa_ly on aa_ly.sk1_location_no = pr.sk1_location_no
                       and aa_ly.sk1_item_no     = pr.sk1_item_no
                       and aa_ly.last_prom_date  = pr.calendar_date
                       and aa_ly.last_prom_date is not null
where pr.calendar_date between l_last_wk_ly_start_date and l_last_wk_ly_end_date                   -- l_fin_year_no - 1 l_fin_week_no

), cc_ly as ( select * from bb_ly where prom_date is not null),

item_price_day7_ly as (
select /*+ PARALLEL(pr,6) FULL(pr) */
       pr.sk1_location_no,
       pr.sk1_item_no,
       max(pr.reg_rsp) rsp_day7_ly,
       max(lpr.prom_rsp) prom_rsp_ly
from rtl_loc_item_dy_rms_price pr
     join dim_item il on pr.sk1_item_no = il.sk1_item_no
     join dim_location sl on pr.sk1_location_no = sl.sk1_location_no
     left join cc_ly lpr on pr.calendar_date = lpr.prom_date
                     and pr.sk1_location_no = lpr.sk1_location_no
                     and pr.sk1_item_no     = lpr.sk1_item_no
where pr.calendar_date between l_last_wk_ly_start_date and l_last_wk_ly_end_date                   -- l_fin_year_no - 1 l_fin_week_no
and (pr.reg_rsp is not null or pr.prom_rsp is not null)
group by pr.sk1_location_no, pr.sk1_item_no
),

aa as (select /*+ PARALLEL(pr,6) FULL(pr) */
       pr.sk1_location_no,
       pr.sk1_item_no,
       max(pr.calendar_date) last_prom_date
from rtl_loc_item_dy_rms_price pr,
     dim_item il,
     dim_location sl
where pr.sk1_location_no = sl.sk1_location_no
and pr.sk1_item_no = il.sk1_item_no
and pr.calendar_date between l_last_wk_start_date and l_last_wk_end_date
and pr.prom_rsp is not null
group by pr.sk1_location_no, pr.sk1_item_no
),  -- select * from aa;
bb as (
select /*+ PARALLEL(pr,6) FULL(pr) */
       unique pr.sk1_location_no,
       pr.sk1_item_no,
       pr.prom_rsp,
       aa.last_prom_date prom_date
from rtl_loc_item_dy_rms_price pr
    left join aa on aa.sk1_location_no = pr.sk1_location_no
                       and aa.sk1_item_no     = pr.sk1_item_no
                       and aa.last_prom_date  = pr.calendar_date
                       and aa.last_prom_date is not null
where pr.calendar_date between l_last_wk_start_date and l_last_wk_end_date

), cc as ( select * from bb where prom_date is not null),   --  select * from cc;

item_price_day7 as (
select /*+ PARALLEL(pr,6) FULL(pr) */
       pr.sk1_location_no,
       pr.sk1_item_no,
       max(pr.reg_rsp) rsp_day7,
       max(lpr.prom_rsp) prom_rsp_ty
from rtl_loc_item_dy_rms_price pr
     join dim_item il on pr.sk1_item_no = il.sk1_item_no
     join dim_location sl on pr.sk1_location_no = sl.sk1_location_no
     left join cc lpr on pr.calendar_date = lpr.prom_date
                     and pr.sk1_location_no = lpr.sk1_location_no
                     and pr.sk1_item_no     = lpr.sk1_item_no
where pr.calendar_date between l_last_wk_start_date and l_last_wk_end_date
and (pr.reg_rsp is not null or pr.prom_rsp is not null)
group by pr.sk1_location_no, pr.sk1_item_no
) ,

sales_index as (
select nvl(nvl(nvl(dense_ty.sk1_location_no, dense_ly.sk1_location_no), ip.sk1_location_no), ip_ly.sk1_location_no) sk1_location_no,
       nvl(nvl(nvl(dense_ty.sk1_item_no, dense_ly.sk1_item_no), ip.sk1_item_no), ip_ly.sk1_item_no) sk1_item_no,
       dense_ty.sales sales,
       dense_ty.sales_qty sales_qty,
       nvl(dense_ty.sales_margin,0) sales_margin,
       dense_ly.sales_ly sales_ly,
       dense_ly.sales_qty_ly sales_qty_ly,
       nvl(dense_ly.sales_margin_ly,0) sales_margin_ly,
       ip.prom_rsp_ty,
       ip_ly.prom_rsp_ly,
       ip.rsp_day7,
       ip_ly.rsp_day7_ly,
       nvl(reg.sales_ly_zone,0) sales_ly_zone
  from rms_dense_measures_ty dense_ty
  full outer join rms_dense_measures_ly dense_ly  on dense_ty.sk1_location_no = dense_ly.sk1_location_no
                                                 and dense_ty.sk1_item_no     = dense_ly.sk1_item_no
  full outer join item_price_day7 ip on  nvl(dense_ty.sk1_item_no, dense_ly.sk1_item_no) = ip.sk1_item_no
                                     and nvl(dense_ty.sk1_location_no, dense_ly.sk1_location_no) = ip.sk1_location_no

  full outer join item_price_day7_ly ip_ly on nvl(nvl(dense_ty.sk1_item_no, dense_ly.sk1_item_no), ip.sk1_item_no) = ip_ly.sk1_item_no
                                          and nvl(nvl(dense_ty.sk1_location_no, dense_ly.sk1_location_no),ip.sk1_location_no) = ip_ly.sk1_location_no

  left outer join sales_ly_by_region reg on dense_ty.sk1_item_no = reg.sk1_item_no
                                        and dense_ty.wh_fd_zone_no    = reg.wh_fd_zone_no


) ,

all_together as (
  select sk1_location_no, sk1_item_no,
         l_fin_year_no,
         l_fin_week_no,
         max(sales) sales,
         max(sales_qty) sales_qty,
         max(sales_margin) sales_margin,
         max(sales_ly) sales_ly,
         max(sales_qty_ly) sales_qty_ly,
         max(sales_margin_ly) sales_margin_ly,
         max(prom_rsp_ty) prom_rsp_ty,
         max(prom_rsp_ly) prom_rsp_ly,
         max(rsp_day7) rsp_day7,
         max(rsp_day7_ly) rsp_day7_ly,
         --max(sales_ly_zone) sales_ly_zone,
         --max(
         --sum((nvl(sales,0)) * (1 +
         sum((sales) * (1 +
            (case
              when nvl(sales,0) = 0 then null
              --when nvl(sales_ly,0) = 0 then nvl(sales_ly_zone,0)   --null
              when nvl(sales_ly_zone,0) = 0 then null
              when nvl(prom_rsp_ty,0) != 0 And nvl(prom_rsp_ly,0) != 0 And prom_rsp_ty < 4 * prom_rsp_ly  Then nvl(prom_rsp_ty / prom_rsp_ly - 1,0) -- With promotions for TY and LY:    = Prom Price TY / Prom Price LY - 1
              when nvl(prom_rsp_ty,0) != 0 And nvl(rsp_day7_ly,0) != 0 And prom_rsp_ty < 4 * rsp_day7_ly Then nvl(prom_rsp_ty / rsp_day7_ly - 1,0)  -- With promotions for TY:           = Prom Price TY / Reg Selling Price LY - 1
              when nvl(prom_rsp_ly,0) != 0 And rsp_day7 < 4 * prom_rsp_ly Then nvl(rsp_day7 / prom_rsp_ly - 1,0)                                    -- With promotions for LY:           = Reg Selling Price TY / Prom Price LY ¿ 1

              when nvl(prom_rsp_ty,0) = 0 And nvl(prom_rsp_ly,0) = 0 Then
                      case when nvl(rsp_day7_ly,0) <> 0 And rsp_day7 < 4 * rsp_day7_ly  then nvl(rsp_day7 / rsp_day7_ly - 1,0)                      -- With no promotions for TY and LY: = Reg Selling Price TY / Reg Selling Price LY - 1
                           else null
                      end
               else null
             end
           ))) as Sales_Index,
           g_date
    from sales_index
    group by sk1_location_no, sk1_item_no, l_fin_year_no, l_fin_week_no, g_date
)
select * from all_together ;

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

end WH_PRF_CORP_249U_NEW;
