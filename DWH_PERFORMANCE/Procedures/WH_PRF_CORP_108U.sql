--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_108U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_108U" (
    p_forall_limit in integer,
    p_success out boolean,
    p_from_loc_no in integer,
    p_to_loc_no   in integer)
as
  --**************************************************************************************************
  --  date:        july 2011
  --  author:      wendy lyttle
  --  purpose:     create style_colour daily stock_count rollup fact table in the performance layer
  --               with input ex daily stock_count in the foundation layer
  --  tables:      input  - fnd_rtl_stock_count
  --               output - rtl_loc_sc_dy_rms_stock_count
  --  packages:    constants, dwh_log, dwh_valid
  --
  --  maintenance:
  --  11 Aug 2011 - testing in PRD - division by zero error fix
  --  23 aug 2011 - add check for values > threshold.
  --                calc'd columns will be made = 0
  --
  --  naming conventions
  --  g_  -  global variable
  --  l_  -  log table variable
  --  a_  -  array variable
  --  v_  -  local variable as found in packages
  --  p_  -  parameter
  --  c_  -  prefix to cursor
  --**************************************************************************************************
  g_forall_limit  integer := dwh_constants.vc_forall_limit;
  g_recs_read     integer := 0;
  g_recs_inserted integer := 0;
  g_recs_updated  integer := 0;
  g_error_count   number  := 0;
  g_error_index   number  := 0;
  g_count         number  := 0;
  g_rec_out rtl_loc_sc_dy_rms_stock_count%rowtype;
  g_found boolean;
  g_date date := trunc(sysdate);
  g_start_date date ;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_108U-'||p_from_loc_no;
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_roll;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_roll;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'ROLL UP THE RMS STOCK COUNT TO STYLE_COLOUR';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- for output arrays into bulk load forall statements --
type tbl_array_i
is
  table of rtl_loc_sc_dy_rms_stock_count%rowtype index by binary_integer;
type tbl_array_u
is
  table of rtl_loc_sc_dy_rms_stock_count%rowtype index by binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   integer := 0;
  a_count_i integer := 0;
  a_count_u integer := 0;
  cursor c_fnd_rtl_stock_count
  is
  with chgrecs as
    (select stocktake_date,
      location_no,
      item_no
    from fnd_rtl_stock_count
--    where (last_updated_date = '10 aug 2011'
--    or last_updated_date = '27 jun 2011')
--    and location_no = 105
   where last_updated_date = g_date
    and location_no between p_from_loc_no and p_to_loc_no
       --         and item_no in (20199197, 20201227, 2031580000003)
      --         and last_updated_date = '26/jan/2010'
      --         and p_from_loc_no = 103
      --         and p_to_loc_no = 103
    group by stocktake_date,
      location_no,
      item_no
    )
select dl.sk1_location_no                                                                                                                                                   as sk1_location_no,
  di.sk1_style_colour_no                                                                                                                                                    as sk1_style_colour_no,
  dlh.sk2_location_no                                                                                                                                                       as sk2_location_no,
  dih.sk1_style_colour_no                                                                                                                                                   as sk2_style_colour_no,
  fnd_lid.stocktake_date                                                                                                                                                    as stocktake_date,
  sum(nvl(fnd_lid.snapshot_on_hand_qty,0))                                                                                                                                  as snapshot_on_hand_qty,
  sum((nvl(fnd_lid.snapshot_unit_cost,0)) * (nvl(fnd_lid.snapshot_on_hand_qty,0)))                                                                                          as snapshot_on_hand_cost,
  sum(nvl(fnd_lid.physical_count_qty,0))                                                                                                                                    as physical_count_qty,
  sum((nvl(fnd_lid.snapshot_unit_cost,0))                                                   * (nvl(fnd_lid.physical_count_qty,0)))                                                                                       as physical_count_cost,
  sum(abs((nvl(fnd_lid.physical_count_qty,0))                                               - (nvl(fnd_lid.snapshot_on_hand_qty,0))))                                                                                    as gross_count_variance_qty,
  sum(abs(((nvl(fnd_lid.physical_count_qty,0))                                              * (nvl(fnd_lid.snapshot_on_hand_qty,0))) - ((nvl(fnd_lid.snapshot_on_hand_qty,0)) * (nvl(fnd_lid.snapshot_on_hand_qty,0))))) as gross_count_variance_cost,
  sum((decode (
  nvl(fnd_lid.snapshot_on_hand_qty,0),0,0,((
  nvl(fnd_lid.snapshot_on_hand_qty,0)
  )-(
  abs((
  nvl(fnd_lid.physical_count_qty,0)
  ) - (
  nvl(fnd_lid.snapshot_on_hand_qty,0)
  ))))/
  nvl(fnd_lid.snapshot_on_hand_qty,0)
  )))        as stock_accuracy_perc,
--Sum((Snapshot_on_hand_qty-Abs(Physical_count_qty-Snapshot_on_hand_qty))/Nullif(Snapshot_on_hand_qty,0)) Stock_accuracy_perc,
  sum((nvl(fnd_lid.physical_count_qty,0))                                                   - (nvl(fnd_lid.snapshot_on_hand_qty,0)))                                                                                     as net_count_variance_qty,
  sum(((nvl(fnd_lid.physical_count_qty,0))                                                  * (nvl(fnd_lid.snapshot_on_hand_qty,0))) - ((nvl(fnd_lid.snapshot_on_hand_qty,0)) * (nvl(fnd_lid.snapshot_on_hand_qty,0))))  as net_count_variance_cost
from chgrecs cr,
  fnd_rtl_stock_count fnd_lid,
  dim_item di,
  dim_location dl,
  dim_item_hist dih,
  dim_location_hist dlh
where cr.stocktake_date = fnd_lid.stocktake_date
and cr.location_no      = fnd_lid.location_no
and cr.item_no          = fnd_lid.item_no
and fnd_lid.item_no     = di.item_no
and fnd_lid.location_no = dl.location_no
and fnd_lid.item_no     = dih.item_no
and fnd_lid.stocktake_date between dih.sk2_active_from_date and dih.sk2_active_to_date
and fnd_lid.location_no = dlh.location_no
and fnd_lid.stocktake_date between dlh.sk2_active_from_date and dlh.sk2_active_to_date
--and fnd_lid.location_no = 105
--and fnd_lid.stocktake_date in ('27 Jun 2011', '10 aug 2011')
group by dl.sk1_location_no,
  di.sk1_style_colour_no ,
  dlh.sk2_location_no ,
  dih.sk1_style_colour_no ,
  fnd_lid.stocktake_date;
g_rec_in c_fnd_rtl_stock_count%rowtype;
-- for input bulk collect --
type stg_array
is
  table of c_fnd_rtl_stock_count%rowtype;
  a_stg_input stg_array;
  --**************************************************************************************************
  -- process, transform and validate the data read from the input interface
  --**************************************************************************************************
procedure local_address_variables
as
begin
  g_rec_out.sk1_location_no           := g_rec_in.sk1_location_no;
  g_rec_out.sk1_style_colour_no       := g_rec_in.sk1_style_colour_no;
  g_rec_out.sk2_location_no           := g_rec_in.sk2_location_no;
  g_rec_out.sk2_style_colour_no       := g_rec_in.sk2_style_colour_no;
  g_rec_out.stocktake_date            := g_rec_in.stocktake_date;
  g_rec_out.physical_count_qty        := g_rec_in.physical_count_qty;
  g_rec_out.snapshot_on_hand_qty      := g_rec_in.snapshot_on_hand_qty;
--
-- Due to some huge values being captured (eg. 20,000,000 items per location)
--  we need to zeroise the cost and variance calculated values.
--
  if g_rec_in.snapshot_on_hand_cost > 999999999999.99
  or g_rec_in.physical_count_cost > 999999999999.99
  or g_rec_in.gross_count_variance_qty > 999999999999.999
  or g_rec_in.gross_count_variance_cost > 999999999999.99
  or g_rec_in.net_count_variance_qty > 999999999999.999
  or g_rec_in.net_count_variance_cost > 999999999999.99
  or g_rec_in.stock_accuracy_perc > 9999999999.9999
  then
      g_rec_out.snapshot_on_hand_cost     := 0;
      g_rec_out.physical_count_cost       := 0;
      g_rec_out.gross_count_variance_qty  := 0;
      g_rec_out.gross_count_variance_cost := 0;
      g_rec_out.stock_accuracy_perc       := 0;
      g_rec_out.net_count_variance_qty    := 0;
      g_rec_out.net_count_variance_cost   := 0;
  else
      g_rec_out.physical_count_cost       := g_rec_in.physical_count_cost;
      g_rec_out.snapshot_on_hand_cost     := g_rec_in.snapshot_on_hand_cost;
      g_rec_out.gross_count_variance_qty  := g_rec_in.gross_count_variance_qty;
      g_rec_out.gross_count_variance_cost := g_rec_in.gross_count_variance_cost;
      g_rec_out.stock_accuracy_perc       := g_rec_in.stock_accuracy_perc;
      g_rec_out.net_count_variance_qty    := g_rec_in.net_count_variance_qty;
      g_rec_out.net_count_variance_cost   := g_rec_in.net_count_variance_cost;
 end if;
  g_rec_out.last_updated_date         := g_date;
--    l_text := 'rec='||g_rec_out.sk1_location_no||'-'||g_rec_out.sk1_style_colour_no||'-'||g_rec_out.stocktake_date||'-'||g_rec_out.last_updated_date;
--  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

exception
when others then
  l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  raise;
end local_address_variables;
--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert
as
begin
  forall i in a_tbl_insert.first .. a_tbl_insert.last
  SAVE exceptions
  insert into rtl_loc_sc_dy_rms_stock_count values a_tbl_insert
    (i
    );
  g_recs_inserted := g_recs_inserted + a_tbl_insert.count;
exception
when others then
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  for i in 1 .. g_error_count
  loop
    g_error_index := sql%bulk_exceptions
    (
      i
    )
    .error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_insert(g_error_index).sk1_location_no|| ' '||a_tbl_insert(g_error_index).sk1_style_colour_no|| ' '||a_tbl_insert(g_error_index).stocktake_date;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
  end loop;
  raise;
end local_bulk_insert;
--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk updates to output table
--**************************************************************************************************
procedure local_bulk_update
as
begin
  forall i in a_tbl_update.first .. a_tbl_update.last
  SAVE exceptions
  update rtl_loc_sc_dy_rms_stock_count
  SET snapshot_on_hand_qty    = a_tbl_update(i).snapshot_on_hand_qty,
    snapshot_on_hand_cost     = a_tbl_update(i).snapshot_on_hand_cost,
    physical_count_qty        = a_tbl_update(i).physical_count_qty,
    physical_count_cost       = a_tbl_update(i).physical_count_cost,
    gross_count_variance_qty  = a_tbl_update(i).gross_count_variance_qty,
    gross_count_variance_cost = a_tbl_update(i).gross_count_variance_cost,
    stock_accuracy_perc       = a_tbl_update(i).stock_accuracy_perc,
    net_count_variance_qty    = a_tbl_update(i).net_count_variance_qty,
    net_count_variance_cost   = a_tbl_update(i).net_count_variance_cost,
    last_updated_date         = a_tbl_update(i).last_updated_date
  where stocktake_date        = a_tbl_update(i).stocktake_date
  and sk1_style_colour_no     = a_tbl_update(i).sk1_style_colour_no
  and sk1_location_no         = a_tbl_update(i).sk1_location_no;
  g_recs_updated             := g_recs_updated + a_tbl_update.count;
exception
when others then
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  for i in 1 .. g_error_count
  loop
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_update(g_error_index).stocktake_date|| ' '||a_tbl_update(g_error_index).sk1_style_colour_no|| ' '||a_tbl_update(g_error_index).sk1_location_no;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
  end loop;
  raise;
end local_bulk_update;
--**************************************************************************************************
-- write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output
as
begin
  g_found := false;
  -- check to see if item is present on table and update/insert accordingly
  select count(1)
  into g_count
  from rtl_loc_sc_dy_rms_stock_count
  where stocktake_date    = g_rec_out.stocktake_date
  and sk1_style_colour_no = g_rec_out.sk1_style_colour_no
  and sk1_location_no     = g_rec_out.sk1_location_no;
  if g_count              = 1 then
    g_found              := true;
  end if;
  -- place data into and array for later writing to table in bulk
  if not g_found then
    a_count_i               := a_count_i + 1;
    a_tbl_insert(a_count_i) := g_rec_out;
  else
    a_count_u               := a_count_u + 1;
    a_tbl_update(a_count_u) := g_rec_out;
  end if;
  a_count := a_count + 1;
  --**************************************************************************************************
  -- bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  if a_count > g_forall_limit then
       local_bulk_insert;
    local_bulk_update;
    a_tbl_insert := a_empty_set_i;
    a_tbl_update := a_empty_set_u;
    a_count_i    := 0;
    a_count_u    := 0;
    a_count      := 0;
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
-- main process
--**************************************************************************************************
begin
  if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
    g_forall_limit  := p_forall_limit;
  end if;
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD rtl_loc_sc_dy_rms_stock_count EX FOUNDATION STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- execute immediate
  --**************************************************************************************************
 -- EXECUTE immediate 'alter session set workarea_size_policy=manual';
 -- EXECUTE immediate 'alter session set sort_area_size=100000000';
 -- EXECUTE immediate 'alter session enable parallel dml';
  --**************************************************************************************************
  -- bulk fetch loop controlling main program execution
  --**************************************************************************************************
  open c_fnd_rtl_stock_count;
  fetch c_fnd_rtl_stock_count bulk collect
  into a_stg_input limit g_forall_limit;
  while a_stg_input.count > 0
  loop
    for i in 1 .. a_stg_input.count
    loop
      g_recs_read              := g_recs_read + 1;
      if g_recs_read mod 500000 = 0 then
        l_text                 := dwh_constants.vc_log_records_processed|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
      g_rec_in := a_stg_input(i);
      local_address_variables;
      local_write_output;
    end loop;
    fetch c_fnd_rtl_stock_count bulk collect
    into a_stg_input limit g_forall_limit;
  end loop;
  close c_fnd_rtl_stock_count;
  --**************************************************************************************************
  -- at end write out what remains in the arrays at end of program
  --**************************************************************************************************
  local_bulk_insert;
  local_bulk_update;
  --**************************************************************************************************
  -- write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
  l_text := dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  commit;
  p_success := true;
exception
when dwh_errors.e_insert_error then
  l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;
when others then
  l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;
end wh_prf_corp_108u;
