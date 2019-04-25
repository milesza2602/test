--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_222U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_222U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        Sep 2011
--  Author:      Alastair de Wet
--  Purpose:     Create Foods performance management mart table in the performance layer
--               with input ex RMS Food rtl_loc_item_wk_rms_dense table from performance layer (WBC).
--  Tables:      Input  - rtl_loc_item_wk_rms_dense
--               Output - tmp_mart_foods_mngmnt_guide
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  09 Nov 2016 - A Joshua - Level1 rename (chg-2218)
--
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;

g_rec_out            tmp_mart_foods_mngmnt_guide%rowtype;
g_found              boolean;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_eergister          date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_222U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE FOODS MANAGEMENT GUIDE MART';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of tmp_mart_foods_mngmnt_guide%rowtype index by binary_integer;
type tbl_array_u is table of tmp_mart_foods_mngmnt_guide%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;



cursor c_rtl_loc_item_wk_rms_dense is
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

--Sales YTD at loc item week level. Used to calculate Sales YTD %. Sales YTD not shown in report.
rms_dense_ytd_measures as (
select /*+ PARALLEL(f,8) */
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
select /*+ PARALLEL(f,8) */
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
select /*+ PARALLEL(f,8) */
       f.sk1_location_no,
       f.sk1_item_no,
       sum(f.sales_wk_app_fcst) sales_fcst_ytd
--from   rtl_loc_item_wk_rdf_fcst f       --RDF L1/L2 remapping change chg-2218
from   RTL_LOC_ITEM_RDF_WKFCST_L1 f
inner join item_list il      on f.sk1_item_no     = il.sk1_item_no
inner join store_list sl     on f.sk1_location_no = sl.sk1_location_no
inner join week_list wl      on f.fin_year_no     = wl.fin_year_no
                            and f.fin_week_no     = wl.fin_week_no
where  f.sales_wk_app_fcst is not null and  f.sales_wk_app_fcst <> 0

group by f.sk1_location_no, f.sk1_item_no
),
rms_sparse_ytd_measures as (
select /*+ PARALLEL(f,8) */
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
)
--Joining all data sets into final result set
select/*+ PARALLEL(f0,4) PARALLEL(f1,4) PARALLEL(f2,4) PARALLEL(f3,4)*/
nvl(nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                               f2.sk1_location_no),
                               f3.sk1_location_no)
sk1_location_no,
nvl(nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                           f2.sk1_item_no),
                           f3.sk1_item_no)
sk1_item_no,
f0.sales_ytd,
f1.sales_ly_ytd,
f2.sales_fcst_ytd,
f3.waste_cost_ytd
from            rms_dense_ytd_measures    f0
full outer join rms_dense_ly_ytd_measures f1 on f0.sk1_item_no     = f1.sk1_item_no
                                            and f0.sk1_location_no = f1.sk1_location_no
full outer join rdf_fcst_ytd_measures     f2 on nvl(f0.sk1_item_no,f1.sk1_item_no)              = f2.sk1_item_no
                                            and nvl(f0.sk1_location_no,f1.sk1_location_no)      = f2.sk1_location_no
full outer join rms_sparse_ytd_measures   f3 on nvl(nvl(f0.sk1_item_no,f1.sk1_item_no),
                                                                        f2.sk1_item_no)          = f3.sk1_item_no
                                            and nvl(nvl(f0.sk1_location_no,f1.sk1_location_no),
                                                                           f2.sk1_location_no)   = f3.sk1_location_no
order by sk1_location_no;

g_rec_in                   c_rtl_loc_item_wk_rms_dense%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_wk_rms_dense%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sales_ytd                       := g_rec_in.sales_ytd;
   g_rec_out.sales_ly_ytd                    := g_rec_in.sales_ly_ytd;
   g_rec_out.sales_fcst_ytd                  := g_rec_in.sales_fcst_ytd;
   g_rec_out.waste_cost_ytd                  := g_rec_in.waste_cost_ytd;
   g_rec_out.last_updated_date               := g_date;


   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;




--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin
--/*+ no_index (tmp_mart_foods_mngmnt_guide) */
    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update
              tmp_mart_foods_mngmnt_guide
       set    sales_ytd                       = a_tbl_update(i).sales_ytd,
              sales_ly_ytd                    = a_tbl_update(i).sales_ly_ytd,
              sales_fcst_ytd                  = a_tbl_update(i).sales_fcst_ytd,
              waste_cost_ytd                  = a_tbl_update(i).waste_cost_ytd,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no;


       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
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
   from   tmp_mart_foods_mngmnt_guide
   where  sk1_location_no    = g_rec_out.sk1_location_no  and
          sk1_item_no        = g_rec_out.sk1_item_no  ;

   if g_count = 1 then
      g_found := TRUE;
   end if;


-- Place data into and array for later writing to table in bulk
   if  g_found then
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
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
-- Main process
--**************************************************************************************************
begin

----------------------------------------------------------------------------------------------------
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
----------------------------------------------------------------------------------------------------
    l_text := 'NO LONGER EXECUTING THIS PROCESS - YTD COMBINED INTO 220U '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    p_success := true;
    return;
----------------------------------------------------------------------------------------------------
--++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
----------------------------------------------------------------------------------------------------
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    g_forall_limit := 1000;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF tmp_mart_foods_mngmnt_guide  FOODS EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------
    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    g_eergister := g_date - 1;
/*
    l_text := 'GATHER STATS on dwh_performance.tmp_mart_foods_mngmnt_guide';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'tmp_mart_foods_mngmnt_guide', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS  - Completed';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_rtl_loc_item_wk_rms_dense;
    fetch c_rtl_loc_item_wk_rms_dense bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_wk_rms_dense bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_wk_rms_dense;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************


    local_bulk_update;



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
end wh_prf_corp_222u;
