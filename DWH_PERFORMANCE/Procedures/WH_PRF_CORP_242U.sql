--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_242U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_242U" (p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        June 2012
--  Author:      Quentin Smit
--  Purpose:     Extract rate of sale data for previous day
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_242U';
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
l_from_date         date:= trunc(sysdate) - 10;   --41
--in_batch            integer  :=1;

cursor c_rate_of_sale is
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
   where uda_3502.calendar_date between l_from_date and g_date
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
where cat.calendar_date between l_from_date and g_date
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
where dense.post_date between l_from_date and g_date
  and dense.sk1_item_no     = di.sk1_item_no
  and dense.sk1_location_no = dl.sk1_location_no
  and dense.sk1_item_no     = uda_3502.sk1_item_no
  and dense.post_date       = uda_3502.calendar_date
  and dense.sk1_item_no     = uda.sk1_item_no
  --and dl.location_no between 0 and 350
  group by dense.sk1_location_no, dense.sk1_item_no, dense.post_date
),

aa as (
select nvl(a.sk1_location_no,b.sk1_location_no) sk1_location_no,
nvl(a.sk1_item_no, b.sk1_item_no) sk1_item_no,
nvl(a.calendar_date, b.post_date) calendar_date,
nvl(b.sales_qty,0) sales_qty,
nvl(fd_num_catlg_days,0) fd_num_catlg_days,
nvl(fd_num_avail_days,0) fd_num_avail_days
from catalog_stuff a
left outer join dense_stuff b on a.sk1_item_no = b.sk1_item_no
                            and a.sk1_location_no = b.sk1_location_no
                            and a.calendar_date = b.post_date
),

bb as (
select sk1_item_no, calendar_date, avg(sales_qty/fd_num_catlg_days) rate_of_sale_day
  from aa
 group by sk1_item_no, calendar_date
),

allofit as (
 select a.sk1_location_no, a.sk1_item_no, a.calendar_date, a.sales_qty, a.fd_num_catlg_days, a.fd_num_avail_days,
 b.rate_of_sale_day units_per_day, g_date
   from  aa a
   full outer join bb b on a.sk1_item_no = b.sk1_item_no
                      and a.calendar_date = b.calendar_date

) select * from allofit;

g_rec_in      c_rate_of_sale%rowtype;

-- For input bulk collect --
type stg_array is table of c_rate_of_sale%rowtype;
a_rate_of_sale      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_location_no              := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                  := g_rec_in.sk1_item_no;
   g_rec_out.calendar_date                := g_rec_in.calendar_date;
   g_rec_out.sales_qty                    := g_rec_in.sales_qty;
   g_rec_out.fd_num_catlg_days            := g_rec_in.fd_num_catlg_days;
   g_rec_out.fd_num_avail_days            := g_rec_in.fd_num_avail_days;
   g_rec_out.units_per_day                := g_rec_in.units_per_day;
   g_rec_out.last_updated_date            := g_date;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variable;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into dwh_performance.rtl_loc_item_dy_rate_of_sale values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update dwh_performance.rtl_loc_item_dy_rate_of_sale
      set    sales_qty                      = a_tbl_update(i).sales_qty,
             fd_num_catlg_days              = a_tbl_update(i).fd_num_catlg_days,
             fd_num_avail_days              = a_tbl_update(i).fd_num_avail_days,
             units_per_day                = a_tbl_update(i).units_per_day,
             last_updated_date              = a_tbl_update(i).last_updated_date
      where  sk1_location_no                = a_tbl_update(i).sk1_location_no
        and  sk1_item_no                    = a_tbl_update(i).sk1_item_no
        and  calendar_date                  = a_tbl_update(i).calendar_date;

      g_recs_updated := g_recs_updated + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
     into   g_count
     from   dwh_performance.rtl_loc_item_dy_rate_of_sale
    where sk1_location_no    = g_rec_out.sk1_location_no
      and sk1_item_no        = g_rec_out.sk1_item_no
      and calendar_date      = g_rec_out.calendar_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;

      commit;
   end if;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

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

    l_text := 'LOAD OF dwh_performance.rtl_loc_item_dy_rate_of_sale for 6 weeks started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --l_text := 'Location range being processed : '|| P_FROM_LOC || ' - ' || P_TO_LOC;
    --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************

    -- When processed in batch run for 3 days else process 6 weeks back outside of batch processing

    l_text := 'Date range being processed : ' || l_from_date || ' - ' || g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    open c_rate_of_sale;
    fetch c_rate_of_sale bulk collect into a_rate_of_sale limit g_forall_limit;
    while a_rate_of_sale.count > 0
    loop
      for i in 1 .. a_rate_of_sale.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_rate_of_sale(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_rate_of_sale bulk collect into a_rate_of_sale limit g_forall_limit;
    end loop;
    close c_rate_of_sale;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;


--**************************************************************************************************
-- At end write out log totals
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
end wh_prf_corp_242u;
