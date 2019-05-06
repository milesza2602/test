--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_656U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_656U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:         July 2014
--  Author:       Alfonso Joshua
--  Purpose:      Identify future dated prices and update accordingly
--  Tables:       Input  - fnd_rtl_purchase_order, fnd_price_change
--                Output - dwh_datafix.ajtst_rtl_po_schn_loc_item_dy
--  Packages:     constants, dwh_log
--
--  Maintenance:
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_not_after_date     date          := g_date - 21;

g_rec_out            rtl_po_supchain_loc_item_dy%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_656U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATES FUTURE PRICES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_po_supchain_loc_item_dy%rowtype index by binary_integer;
type tbl_array_u is table of rtl_po_supchain_loc_item_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_prices is
   with itemsel as (
   select a.item_no, a.zone_no, a.zone_group_no,
          round((a.unit_retail * 100 / (100 + b.vat_rate_perc)),2) unit_retail,
          a.active_date
   from fnd_price_change a, dim_item b
   where  a.last_updated_date = g_date
    and   a.item_no = b.item_no
    and   a.zone_no = 1
    and   a.zone_group_no = 2
    and   a.price_change_status_code = 'A'),

   porder as (
   select b.item_no, b.location_no, b.not_after_date, b.not_before_date,
          b.po_no, a.unit_retail, a.zone_no, a.active_date, b.supply_chain_type,
          b.original_po_qty, b.reg_rsp, b.amended_po_qty, b.cancel_po_qty
   from itemsel a, fnd_rtl_purchase_order b, dim_location c, dim_zone d
   where a.item_no = b.item_no
     and b.not_after_date >= g_not_after_date
     and b.location_no = c.location_no
     and c.sk1_ch_zone_group_zone_no = d.sk1_zone_group_zone_no
--     and a.item_no = 6009184592886
--     and a.zone_no = d.zone_no
--     and a.zone_group_no = d.zone_group_no
     and b.po_status_code = 'A')

   select b.sk1_item_no, c.sk1_location_no, a.not_after_date, d.sk1_po_no, a.unit_retail, e.sk1_supply_chain_no,
          a.original_po_qty, a.amended_po_qty, a.not_before_date, a.active_date,
          b.vat_rate_perc, a.reg_rsp, f.bc_shipment_qty,
          round((a.original_po_qty * (a.unit_retail * 100 / (100 + b.vat_rate_perc))),2) original_po_selling,
          round((a.amended_po_qty *  (a.unit_retail * 100 / (100 + b.vat_rate_perc))),2) amended_po_selling,
          round((a.cancel_po_qty *   (a.unit_retail * 100 / (100 + b.vat_rate_perc))),2) cancel_po_selling
   from porder a, dim_item b, dim_location c, dim_purchase_order d,
        dim_supply_chain_type e, rtl_po_supchain_loc_item_dy f
   where a.item_no = b.item_no
    and  a.location_no = c.location_no
    and  a.po_no = d.po_no
    and  a.supply_chain_type = e.supply_chain_code
    and  a.not_before_date >= a.active_date
    and  d.sk1_po_no = f.sk1_po_no
    and  e.sk1_supply_chain_no = f.sk1_supply_chain_no
    and  c.sk1_location_no = f.sk1_location_no
    and  b.sk1_item_no = f.sk1_item_no
    and  a.not_before_date = f.tran_date;

-- join po_no and item_no to fnd_rtl_shipment for bc_shipment _qty (latest_po_selling)

g_rec_in             c_fnd_prices%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_prices%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                                     := null;

   g_rec_out.sk1_po_no			                     := g_rec_in.sk1_po_no;
   g_rec_out.sk1_supply_chain_no		             := g_rec_in.sk1_supply_chain_no;
   g_rec_out.sk1_location_no			               := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no			                   := g_rec_in.sk1_item_no;
   g_rec_out.tran_date			                     := g_rec_in.not_before_date;
   g_rec_out.original_po_selling		             := g_rec_in.original_po_selling;
   g_rec_out.amended_po_selling			             := g_rec_in.amended_po_selling;
   g_rec_out.cancel_po_selling			             := g_rec_in.cancel_po_selling;
   if g_rec_in.bc_shipment_qty is null then
      g_rec_out.latest_po_selling			           := g_rec_in.amended_po_selling;
   end if;
   g_rec_out.last_updated_date                   := '01 aug 14';

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;
/*
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_po_supchain_loc_item_dy values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_po_no||
                       ' '||a_tbl_insert(g_error_index).sk1_supply_chain_no||
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).tran_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;
*/
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update dwh_datafix.ajtst_rtl_po_schn_loc_item_dy
       set    amended_po_selling		          = a_tbl_update(i).amended_po_selling,
              original_po_selling			        = a_tbl_update(i).original_po_selling,
              cancel_po_selling			          = a_tbl_update(i).cancel_po_selling,
              latest_po_selling			          = a_tbl_update(i).latest_po_selling,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_po_no                       = a_tbl_update(i).sk1_po_no
       and    sk1_supply_chain_no             = a_tbl_update(i).sk1_supply_chain_no
       and    sk1_location_no                 = a_tbl_update(i).sk1_location_no
       and    sk1_item_no                     = a_tbl_update(i).sk1_item_no
       and    tran_date                       = a_tbl_update(i).tran_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_po_no||
                       ' '||a_tbl_update(g_error_index).sk1_supply_chain_no||
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).tran_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to output table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(*)
   into   g_count
   from   dwh_datafix.ajtst_rtl_po_schn_loc_item_dy
   where  sk1_po_no             = g_rec_out.sk1_po_no
   and    sk1_supply_chain_no   = g_rec_out.sk1_supply_chain_no
   and    sk1_location_no       = g_rec_out.sk1_location_no
   and    sk1_item_no           = g_rec_out.sk1_item_no
   and    tran_date             = g_rec_out.tran_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

--   dbms_output.put_line('Found Ind '||g_count||' '||g_rec_out.sk1_po_no||' '||g_rec_out.sk1_supply_chain_no||' '||g_rec_out.sk1_location_no||' '||g_rec_out.sk1_item_no||' '||g_rec_out.tran_date);

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
--      local_bulk_insert;
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
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF rtl_po_supchain_loc_item_dy EX FOUNDATION STARTED '||
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
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_prices;
    fetch c_fnd_prices bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in                := null;
         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_prices bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_prices;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
--    local_bulk_insert;
    local_bulk_update;

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

end wh_prf_corp_656U;
