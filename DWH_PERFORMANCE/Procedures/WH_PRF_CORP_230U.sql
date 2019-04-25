--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_230U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_230U" (p_forall_limit in integer, p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2012
--  Author:      Quentin Smit
--  Purpose:     Extract rate of sale data for previous day - ROS day is NOT an average in this program
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   dwh_performance.rtl_loc_item_dy_rate_of_sale
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
g_rec_out            dwh_performance.rtl_loc_item_dy_rate_of_sale%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_230U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT RATE OF SALE AT ITEM LOCATION DAY LEVEL FOR 6 WEEKS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.rtl_loc_item_dy_rate_of_sale%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.rtl_loc_item_dy_rate_of_sale%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_from_date         date := trunc(sysdate) - 43;
l_to_date           date;   -- := g_date;    --l_from_date +13;    --13
--in_batch            integer  :=1;


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

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session enable parallel dml';


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_to_date := g_date;
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************

    l_text := 'Date range being processed : ' || l_from_date || ' - ' || l_to_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF rtl_loc_item_dy_rate_of_sale_t for a week started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_to_date := 'Mooo';

 INSERT /*+ APPEND PARALLEL (mart,6) */ INTO dwh_performance.rtl_loc_item_dy_rate_of_sale mart
with item_list as (
select di.sk1_item_no
from   dim_item di,
       dim_department dd
where  di.business_unit_no = 50
  and  di.pack_item_ind <> 1
  and  di.sk1_department_no = dd.sk1_department_no
),

uda_3502_item_list as (
  select uda_3502.calendar_date, uda_3502.sk1_item_no
  from rtl_item_uda_dy uda_3502, item_list il
   where uda_3502.calendar_date between l_from_date and l_to_date
   and uda_3502.sk1_item_no = il.sk1_item_no
   and uda_3502.uda_no = 3502
   and uda_3502.uda_value_no_or_text_or_date <> 1
   and uda_3502.uda_value_no_or_text_or_date <> 3
   order by uda_3502.calendar_date, uda_3502.sk1_item_no
),

uda_item_list as (
select uda.sk1_item_no
  from dim_item_uda uda, item_list il
  where uda.sk1_item_no = il.sk1_item_no
   and uda.PRODUCT_CLASS_DESC_507 <> 'H'
),
store_list_area as (
select sk1_location_no, location_no
from   dim_location
where  area_no = 9951
--and location_no between P_FROM_LOC and P_TO_LOC
) ,
catalog_stuff as (
select  /*+ PARALLEL(cat,6) FULL(cat) */
cat.sk1_location_no,
cat.sk1_item_no, cat.calendar_date,
sum(cat.fd_num_catlg_days) fd_num_catlg_days,
sum(cat.fd_num_avail_days) fd_num_avail_days
from rtl_loc_item_dy_catalog cat,
     item_list di,
     store_list_area dl,
     uda_3502_item_list uda_3502,
     uda_item_list uda
where cat.calendar_date between l_from_date and l_to_date
  and cat.fd_num_avail_days > 0
  and cat.fd_num_catlg_days > 0
  and cat.product_status_1_code <> 4
  and cat.product_status_1_code <> 26
  and cat.sk1_item_no = di.sk1_item_no
  and cat.sk1_location_no = dl.sk1_location_no
  and cat.sk1_item_no = uda_3502.sk1_item_no
  and cat.calendar_date = uda_3502.calendar_date
  and cat.sk1_item_no = uda.sk1_item_no
  --and dl.location_no between 0 and 350
  group by cat.sk1_location_no, cat.sk1_item_no, cat.calendar_date
  order by calendar_date asc
),

dense_stuff as (
select /*+ PARALLEL(dense,6) FULL(dense) */
     dense.sk1_location_no, dense.sk1_item_no, dense.post_date, sum(nvl(sales_qty,0)) sales_qty
from rtl_loc_item_dy_rms_dense dense,
     store_list_area dl,
     item_list di,
     uda_3502_item_list uda_3502,
     uda_item_list uda
where dense.post_date between l_from_date and l_to_date
  and dense.sk1_item_no     = di.sk1_item_no
  and dense.sk1_location_no = dl.sk1_location_no
  and dense.sk1_item_no     = uda_3502.sk1_item_no
  and dense.post_date       = uda_3502.calendar_date
  and dense.sk1_item_no     = uda.sk1_item_no
  and dense.sales_qty >= 0
  --and dl.location_no between 0 and 350
  group by dense.sk1_location_no, dense.sk1_item_no, dense.post_date
),

ros_dy as (

select nvl(a.sk1_location_no,b.sk1_location_no) sk1_location_no,
nvl(a.sk1_item_no, b.sk1_item_no) sk1_item_no,
nvl(a.calendar_date, b.post_date) calendar_date,
nvl(b.sales_qty,0) sales_qty,
nvl(fd_num_catlg_days,0) fd_num_catlg_days,
nvl(fd_num_avail_days,0) fd_num_avail_days,
nvl((b.sales_qty / a.fd_num_catlg_days),0) units_per_day,
g_date
from catalog_stuff a
left outer join dense_stuff b on a.sk1_item_no = b.sk1_item_no
                            and a.sk1_location_no = b.sk1_location_no
                            and a.calendar_date = b.post_date
) select * from ros_dy;
   --where sales_qty >= 0;

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
end wh_prf_corp_230u;
