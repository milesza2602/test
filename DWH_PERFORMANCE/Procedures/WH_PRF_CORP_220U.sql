--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_220U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_220U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Sep 2011
--  Author:      Alastair de Wet
--  Purpose:     Create location_item Mart table in the performance layer
--               with input ex lid dense/sparse/catalog/rdf fcst table from performance layer.
--               For Foods management guide
--  Tables:      Input  - rtl_loc_item_dy_rms_dense
--               Output - tmp_mart_foods_mngmnt_guide
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  09 Nov 2016 - A Joshua - Level1 rename (chg-2218)
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

g_rec_out            tmp_mart_foods_mngmnt_guide%rowtype;

g_date               date          := trunc(sysdate);
g_last_week          date          := trunc(sysdate) - 7;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_220U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Mart table for Foods management guide';
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

    l_text := 'Create Mart table for Foods management guide STARTED AT '||
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


    l_text := 'Week being processed:- '||g_last_week ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table dwh_performance.tmp_mart_foods_mngmnt_guide');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

/*----------------------------------------------------------------------------------------------------
    l_text := 'Disable Index Begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('alter index B01_MRT_FDS_MNGMNT_GD unusable');
    EXECUTE IMMEDIATE('alter index B02_MRT_FDS_MNGMNT_GD unusable');
    EXECUTE IMMEDIATE('alter index B03_MRT_FDS_MNGMNT_GD unusable');
    EXECUTE IMMEDIATE('alter index B04_MRT_FDS_MNGMNT_GD unusable');
    EXECUTE IMMEDIATE('alter index B05_MRT_FDS_MNGMNT_GD unusable');
    EXECUTE IMMEDIATE('alter index B06_MRT_FDS_MNGMNT_GD unusable');
    l_text := 'Disable Index Completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

*/----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------


INSERT /*+ APPEND */ INTO dwh_performance.tmp_mart_foods_mngmnt_guide mart
--list of items for Foods departments
with
item_list as (
select sk1_item_no
from   dim_item
where  business_unit_no = 50
),
store_list as (
select sk1_location_no,
       location_no,
       location_name,
       region_no,
       region_name,
       area_no,
       area_name
from   dim_location
where  active_store_ind = 1
and    loc_type         = 'S'
),
calendar_week as (
select fin_year_no,
       fin_week_no
from   dim_calendar
where  calendar_date    = TRUNC(sysdate)-7
),

--list of year to date weeks for last year
week_list as (
select dcw.fin_year_no,
       dcw.fin_week_no
from   dim_calendar_wk dcw
inner join calendar_week cw on dcw.fin_year_no   = cw.fin_year_no
                           and dcw.fin_week_no  <= cw.fin_week_no
),
--List of items, product class and catalog indicator for selected department
item_pclass as (
select i.sk1_item_no,
       i.item_no,
       SUBSTR(i.item_desc,1,40) AS item_desc,
       i.department_no,
       i.department_name,
       i.subclass_no,
       i.subclass_name,
       uda.product_class_desc_507
from   dim_item i
inner join item_list il     on i.sk1_item_no=il.sk1_item_no
inner join dim_item_uda uda on il.sk1_item_no=uda.sk1_item_no
),
--Forecast Sales at loc item week level
rdf_fcst_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sales_wk_app_fcst sales_fcst
--from rtl_loc_item_wk_rdf_fcst  f    
from RTL_LOC_ITEM_RDF_WKFCST_L1  f                                      --chg-2218  RDF L1/L2 remapping change
inner join item_list il     on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl    on f.sk1_location_no = sl.sk1_location_no
inner join calendar_week cw on f.fin_year_no     = cw.fin_year_no
                           and f.fin_week_no     = cw.fin_week_no
where f.sales_wk_app_fcst is not null
),
--Sales at loc item week level
rms_dense_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sales
from   rtl_loc_item_wk_rms_dense f
inner join item_list il     on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl    on f.sk1_location_no = sl.sk1_location_no
inner join calendar_week cw on f.fin_year_no     = cw.fin_year_no
                           and f.fin_week_no     = cw.fin_week_no
where f.sales  is not null
),
--Sales LY at loc item week level. Used to calculate Sales LY %. Sales LY not shown in report.
rms_dense_ly_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sales sales_ly
from   rtl_loc_item_wk_rms_dense f
inner join item_list il     on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl    on f.sk1_location_no = sl.sk1_location_no
inner join calendar_week cw on f.fin_year_no     = cw.fin_year_no-1
                           and f.fin_week_no     = cw.fin_week_no
where f.sales is not null
),

--Sales LY NW at loc item week level
rms_dense_ly_nw_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sales sales_ly_nw
from   rtl_loc_item_wk_rms_dense f
inner join item_list il     on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl    on f.sk1_location_no = sl.sk1_location_no
inner join calendar_week cw on f.fin_year_no     = cw.fin_year_no-1
                           and f.fin_week_no     = cw.fin_week_no+1
where f.sales  is not null
),
--Forecast Sales NW at loc item week level
rdf_fcst_nw_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.sales_wk_app_fcst sales_fcst_nw
--from   rtl_loc_item_wk_rdf_fcst  f       
from   RTL_LOC_ITEM_RDF_WKFCST_L1  f                                      --chg-2218  RDF L1/L2 remapping change
inner join item_list il     on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl    on f.sk1_location_no = sl.sk1_location_no
inner join calendar_week cw on f.fin_year_no     = cw.fin_year_no
                           and f.fin_week_no     = cw.fin_week_no+1
where f.sales_wk_app_fcst is not null
),
--Available Days, Catalogue Days and Avg BOH Adjusted Selling at loc item week level. Available Days, Catalogue Days used to calculate Availability %, but not shown in report. Avg BOH Adjusted Selling used to calculate Stock Accuracy %, but not shown in report.
catalog_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.fd_num_avail_days,
       f.fd_num_catlg_days,
       f.avg_boh_adj_selling,
       f.boh_adj_selling,
       f.this_wk_catalog_ind
from   rtl_loc_item_wk_catalog f
inner join item_list il     on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl    on f.sk1_location_no = sl.sk1_location_no
inner join calendar_week cw on f.fin_year_no     = cw.fin_year_no
                           and f.fin_week_no     = cw.fin_week_no
where f.boh_adj_selling <> 0 or f.fd_num_avail_days <> 0
                                    or f.fd_num_catlg_days  <> 0
                                    or f.this_wk_catalog_ind <> 0
),
--Waste Cost and Abs Shrinkage Selling at loc item week level. Waste Cost used to calculate Waste Cost %, but not shown in report. Abs Shrinkage Selling used to calculate Stock Accuracy %, but not shown in report.
rms_sparse_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       f.waste_cost,
       f.abs_shrinkage_selling
from   rtl_loc_item_wk_rms_sparse f
inner join item_list il     on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl    on f.sk1_location_no = sl.sk1_location_no
inner join calendar_week cw on f.fin_year_no     = cw.fin_year_no
                           and f.fin_week_no     = cw.fin_week_no
where f.waste_cost is not null or f.abs_shrinkage_selling is not null
),

--Sales YTD at loc item week level. Used to calculate Sales YTD %. Sales YTD not shown in report.
rms_dense_ytd_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(f.sales) sales_ytd
from   rtl_loc_item_wk_rms_dense f
inner join item_list il      on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl     on f.sk1_location_no = sl.sk1_location_no
inner join week_list wl      on f.fin_year_no     = wl.fin_year_no
                            and f.fin_week_no     = wl.fin_week_no
where f.sales is not null and f.sales <> 0

group by   f.sk1_location_no,
           f.sk1_item_no

),
--Sales LY YTD at loc item week level. Used to calculate Sales LY YTD %. Sales LY YTD not shown in report.
rms_dense_ly_ytd_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(f.sales) sales_ly_ytd
from   rtl_loc_item_wk_rms_dense f
inner join item_list il      on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl     on f.sk1_location_no = sl.sk1_location_no
inner join week_list wl      on f.fin_year_no     = wl.fin_year_no-1
                            and f.fin_week_no     = wl.fin_week_no
where f.sales is not null and f.sales <> 0

group by   f.sk1_location_no,
           f.sk1_item_no
),
--Sales LY YTD at loc item week level. Used to calculate Sales LY YTD %. Sales LY YTD not shown in report.
rdf_fcst_ytd_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(f.sales_wk_app_fcst) sales_fcst_ytd
--from   rtl_loc_item_wk_rdf_fcst  f
from   RTL_LOC_ITEM_RDF_WKFCST_L1  f                                      --chg-2218  RDF L1/L2 remapping change
inner join item_list il      on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl     on f.sk1_location_no = sl.sk1_location_no
inner join week_list wl      on f.fin_year_no     = wl.fin_year_no
                            and f.fin_week_no     = wl.fin_week_no
where  f.sales_wk_app_fcst is not null and  f.sales_wk_app_fcst <> 0

group by   f.sk1_location_no,
           f.sk1_item_no
),
rms_sparse_ytd_measures as (
select /*+ PARALLEL(f,6) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(f.waste_cost) waste_cost_ytd
from   rtl_loc_item_wk_rms_sparse f
inner join item_list il     on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl    on f.sk1_location_no = sl.sk1_location_no
inner join week_list wl     on f.fin_year_no     = wl.fin_year_no
                           and f.fin_week_no     = wl.fin_week_no
where f.waste_cost is not null
group by   f.sk1_location_no,
           f.sk1_item_no
),
all_together as (
--Joining all temp data sets into final result set
select /*+ PARALLEL(f0,6) */
nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                           f2.sk1_location_no),
                                           f3.sk1_location_no),
                                           f4.sk1_location_no),
                                           f5.sk1_location_no),
                                           f6.sk1_location_no),
                                           fy0.sk1_location_no),
                                           fy1.sk1_location_no),
                                           fy2.sk1_location_no),
                                           fy3.sk1_location_no)
sk1_location_no,
nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                       f2.sk1_item_no),
                                       f3.sk1_item_no),
                                       f4.sk1_item_no),
                                       f5.sk1_item_no),
                                       f6.sk1_item_no),
                                       fy0.sk1_item_no),
                                       fy1.sk1_item_no),
                                       fy2.sk1_item_no),
                                       fy3.sk1_item_no)
sk1_item_no,
decode(f0.this_wk_catalog_ind,1,'Yes','No') catalog_ind,
f1.sales_fcst,
f5.sales_fcst_nw,
f2.sales,
f3.sales_ly,
f4.sales_ly_nw,
f0.fd_num_avail_days,
f0.fd_num_catlg_days,
f6.waste_cost,
f6.abs_shrinkage_selling,
f0.boh_adj_selling,
f0.avg_boh_adj_selling,
fy0.sales_ytd,
fy1.sales_ly_ytd,
fy2.sales_fcst_ytd,
fy3.waste_cost_ytd
from catalog_measures f0
full outer join rdf_fcst_measures        f1 on f0.sk1_item_no     = f1.sk1_item_no
                                           and f0.sk1_location_no = f1.sk1_location_no
full outer join rms_dense_measures       f2 on nvl(f0.sk1_item_no,f1.sk1_item_no)              = f2.sk1_item_no
                                           and nvl(f0.sk1_location_no,f1.sk1_location_no)      = f2.sk1_location_no
full outer join rms_dense_ly_measures    f3 on nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                      f2.sk1_item_no)          = f3.sk1_item_no
                                           and nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                          f2.sk1_location_no)  = f3.sk1_location_no
full outer join rms_dense_ly_nw_measures f4 on nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                          f2.sk1_item_no),
                                                                          f3.sk1_item_no)         = f4.sk1_item_no
                                           and nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                              f2.sk1_location_no),
                                                                              f3.sk1_location_no) = f4.sk1_location_no
full outer join rdf_fcst_nw_measures     f5 on nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                              f2.sk1_item_no),
                                                                              f3.sk1_item_no),
                                                                              f4.sk1_item_no)         = f5.sk1_item_no
                                           and nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                  f2.sk1_location_no),
                                                                                  f3.sk1_location_no),
                                                                                  f4.sk1_location_no) = f5.sk1_location_no
full outer join rms_sparse_measures      f6 on nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                  f2.sk1_item_no),
                                                                                  f3.sk1_item_no),
                                                                                  f4.sk1_item_no),
                                                                                  f5.sk1_item_no)          = f6.sk1_item_no
                                           and nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                      f2.sk1_location_no),
                                                                                      f3.sk1_location_no),
                                                                                      f4.sk1_location_no),
                                                                                      f5.sk1_location_no)  = f6.sk1_location_no

full outer join rms_dense_ytd_measures  fy0 on nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f5.sk1_item_no),
                                                                                   f6.sk1_item_no)          = fy0.sk1_item_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f5.sk1_location_no),
                                                                                   f6.sk1_location_no)      = fy0.sk1_location_no
full outer join rms_dense_ly_ytd_measures fy1 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f5.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no)         = fy1.sk1_item_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f5.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no)     = fy1.sk1_location_no
full outer join rdf_fcst_ytd_measures  fy2  on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f5.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no)         = fy2.sk1_item_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f5.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no)     = fy2.sk1_location_no
full outer join rms_sparse_ytd_measures fy3 on nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                                   f2.sk1_item_no),
                                                                                   f3.sk1_item_no),
                                                                                   f4.sk1_item_no),
                                                                                   f5.sk1_item_no),
                                                                                   f6.sk1_item_no),
                                                                                   fy0.sk1_item_no),
                                                                                   fy1.sk1_item_no),
                                                                                   fy2.sk1_item_no)          = fy3.sk1_item_no
                                           and nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                                   f2.sk1_location_no),
                                                                                   f3.sk1_location_no),
                                                                                   f4.sk1_location_no),
                                                                                   f5.sk1_location_no),
                                                                                   f6.sk1_location_no),
                                                                                   fy0.sk1_location_no),
                                                                                   fy1.sk1_location_no),
                                                                                   fy2.sk1_location_no)      = fy3.sk1_location_no
)
select /*+ PARALLEL(atg,6) */
atg.sk1_location_no,
sl.location_no,
sl.location_no||' - '||sl.location_name location_no_and_name,
sl.region_no,
sl.region_no||' - '||sl.region_name region_no_and_name,
sl.area_no,
sl.area_no||' - '||sl.area_name area_no_and_name,
atg.sk1_item_no,
itm.item_no,
itm.item_no||' - '||itm.item_desc item_no_and_name,
itm.department_no,
itm.department_no||' - '||itm.department_name department_no_and_name,
itm.subclass_no,
itm.subclass_no||' - '||itm.subclass_name subclass_no_and_name,
itm.product_class_desc_507,
atg.catalog_ind,
atg.sales_fcst,
atg.sales_fcst_nw,
atg.sales_fcst_ytd,
atg.sales,
atg.sales_ly,
atg.sales_ly_nw,
atg.sales_ytd,
atg.sales_ly_ytd,
atg.fd_num_avail_days,
atg.fd_num_catlg_days,
atg.waste_cost,
atg.waste_cost_ytd,
atg.abs_shrinkage_selling,
atg.boh_adj_selling,
atg.avg_boh_adj_selling,
0,
g_date
from all_together atg
inner join  item_pclass itm  on atg.sk1_item_no     = itm.sk1_item_no
inner join  store_list sl    on atg.sk1_location_no = sl.sk1_location_no

where atg.sales                  is not null or
      atg.waste_cost             is not null or
      atg.sales_fcst             is not null or
      atg.catalog_ind            = 'Yes' or
      atg.sales_fcst_nw          is not null or
      atg.sales_ly               is not null or
      atg.sales_ly_nw            is not null or
      atg.abs_shrinkage_selling  is not null or
      atg.boh_adj_selling        is not null


;

  g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;


    commit;

/*----------------------------------------------------------------------------------------------------
    l_text := 'Rebuild Index Begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('alter index B01_MRT_FDS_MNGMNT_GD rebuild');
    EXECUTE IMMEDIATE('alter index B02_MRT_FDS_MNGMNT_GD rebuild');
    EXECUTE IMMEDIATE('alter index B03_MRT_FDS_MNGMNT_GD rebuild');
    EXECUTE IMMEDIATE('alter index B04_MRT_FDS_MNGMNT_GD rebuild');
    EXECUTE IMMEDIATE('alter index B05_MRT_FDS_MNGMNT_GD rebuild');
    EXECUTE IMMEDIATE('alter index B06_MRT_FDS_MNGMNT_GD rebuild');
    l_text := 'Rebuild Index Completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    l_text := 'GATHER STATS on dwh_performance.mart_foods_management_guide';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'mart_foods_management_guide', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS  - Completed';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

*/----------------------------------------------------------------------------------------------------

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

end wh_prf_corp_220u;
