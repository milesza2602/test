--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_720U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_720U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Stockown   fact table in the performance layer
--               with input ex Stockown DCR  table from foundation layer.
--  Tables:      Input  - fnd_rtl_stockown_dcr
--               Output - rtl_stockown_dcr
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_stockown_dcr%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_this_wk_end        date;
g_last_wk_start      date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_720U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD STOCKOWN DATA EX STOCKOWN DCR FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_stockown_dcr%rowtype index by binary_integer;
type tbl_array_u is table of rtl_stockown_dcr%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_stockown_dcr is
   select   so.*,
            nvl(so.expected_qty,0) * cost_price  as expected_cost,
            nvl(so.checked_qty,0) * cost_price as checked_cost,
            nvl(so.received_qty,0) * cost_price as received_cost,
            di.sk1_item_no,
            dl.sk1_location_no,
            dih.sk2_item_no ,
            dlh.sk2_location_no,
            ds.sk1_supplier_no
   from     fnd_rtl_stockown_dcr so,
            dim_item di,
            dim_location dl,
            dim_item_hist dih,
            dim_location_hist dlh,
            dim_supplier ds
   where    so.item_no                 = di.item_no  and
            so.to_location_no             = dl.location_no   and
            so.item_no                 = dih.item_no and
            so.create_date              between dih.sk2_active_from_date and dih.sk2_active_to_date and
            so.to_location_no             = dlh.location_no and
            so.create_date              between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
            so.supplier_no             = ds.supplier_no and
            so.create_date             between g_last_wk_start and g_this_wk_end ;


g_rec_in             c_fnd_rtl_stockown_dcr%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_stockown_dcr%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.asn_no                          := g_rec_in.asn_no;
   g_rec_out.du_id                           := g_rec_in.du_id;
   g_rec_out.di_no                           := g_rec_in.di_no;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.order_no                        := g_rec_in.order_no;
   g_rec_out.from_location_no                := g_rec_in.from_location_no;
   g_rec_out.sk1_supplier_no                 := g_rec_in.sk1_supplier_no;
   g_rec_out.sk1_to_location_no              := g_rec_in.sk1_location_no;
   g_rec_out.sk2_to_location_no              := g_rec_in.sk2_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.received_date                   := g_rec_in.received_date;
   g_rec_out.detail_check_date               := g_rec_in.detail_check_date;
   g_rec_out.detail_check_ind                := g_rec_in.detail_check_ind;
   g_rec_out.damage_ind                      := g_rec_in.damage_ind;
   g_rec_out.expected_qty                    := g_rec_in.expected_qty;
   g_rec_out.expected_cost                   := g_rec_in.expected_cost;
   g_rec_out.received_qty                    := g_rec_in.received_qty;
   g_rec_out.received_cost                   := g_rec_in.received_cost;
   g_rec_out.checked_qty                     := g_rec_in.checked_qty;
   g_rec_out.checked_cost                    := g_rec_in.checked_cost;
   g_rec_out.du_reason_code                  := g_rec_in.du_reason_code;
   g_rec_out.unit_reason_code                := g_rec_in.unit_reason_code;
   g_rec_out.cost_price                      := g_rec_in.cost_price;
   g_rec_out.selling_price                   := g_rec_in.selling_price;
   g_rec_out.create_date                     := g_rec_in.create_date;
--   g_rec_out.under_qty                       := g_rec_in.under_qty;
--   g_rec_out.over_qty                        := g_rec_in.over_qty;
--   g_rec_out.substitute_qty                  := g_rec_in.substitute_qty;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.last_updated_date               := g_date;

-- QC 2347
   g_rec_out.under_qty                       := 0;
   g_rec_out.under_cost                      := 0;
   g_rec_out.over_qty                        := 0;
   g_rec_out.over_cost                       := 0;
   g_rec_out.substitute_qty                  := 0;
   g_rec_out.substitute_cost                 := 0;
   g_rec_out.net_accuracy_qty                := 0;
   g_rec_out.net_accuracy_cost               := 0;
   g_rec_out.gross_accuracy_qty              := 0;
   g_rec_out.gross_accuracy_cost             := 0;

if g_rec_in.detail_check_ind = 1 then
   if g_rec_out.expected_qty > g_rec_out.checked_qty  then
      g_rec_out.under_qty    :=  g_rec_out.expected_qty - g_rec_out.checked_qty ;
      g_rec_out.under_cost   :=  g_rec_out.under_qty * g_rec_in.cost_price;
   end if;
   if g_rec_out.checked_qty  > g_rec_out.expected_qty and
      g_rec_out.expected_qty > 0                      then
      g_rec_out.over_qty    :=  g_rec_out.checked_qty - g_rec_out.expected_qty ;
      g_rec_out.over_cost   :=  g_rec_out.over_qty * g_rec_in.cost_price;
   end if;
   if g_rec_out.checked_qty  > g_rec_out.expected_qty and
      g_rec_out.expected_qty = 0                      then
      g_rec_out.substitute_qty    :=  g_rec_out.checked_qty ;
      g_rec_out.substitute_cost   :=  g_rec_out.substitute_qty * g_rec_in.cost_price;
   end if;
   g_rec_out.net_accuracy_qty                := nvl(g_rec_out.over_qty,0) + nvl(g_rec_out.substitute_qty,0) - nvl(g_rec_out.under_qty,0);
   g_rec_out.net_accuracy_cost               := nvl(g_rec_out.over_cost,0) + nvl(g_rec_out.substitute_cost,0) - nvl(g_rec_out.under_cost,0);
   g_rec_out.gross_accuracy_qty              := nvl(g_rec_out.over_qty,0) + nvl(g_rec_out.substitute_qty,0) + nvl(g_rec_out.under_qty,0);
   g_rec_out.gross_accuracy_cost             := nvl(g_rec_out.over_cost,0) + nvl(g_rec_out.substitute_cost,0) + nvl(g_rec_out.under_cost,0);
end if;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_stockown_dcr values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).asn_no||
                       ' '||a_tbl_insert(g_error_index).du_id||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).di_no;
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
       update rtl_stockown_dcr
       set    order_no                        = a_tbl_update(i).order_no,
              from_location_no                = a_tbl_update(i).from_location_no,
              sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no,
              sk1_to_location_no              = a_tbl_update(i).sk1_to_location_no,
              sk2_to_location_no              = a_tbl_update(i).sk2_to_location_no,
              sk2_item_no                     = a_tbl_update(i).sk2_item_no,
              received_date                   = a_tbl_update(i).received_date,
              detail_check_date               = a_tbl_update(i).detail_check_date,
              detail_check_ind                = a_tbl_update(i).detail_check_ind,
              damage_ind                      = a_tbl_update(i).damage_ind,
              expected_qty                    = a_tbl_update(i).expected_qty,
              expected_cost                   = a_tbl_update(i).expected_cost,
              received_qty                    = a_tbl_update(i).received_qty,
              received_cost                   = a_tbl_update(i).received_cost,
              checked_qty                     = a_tbl_update(i).checked_qty,
              checked_cost                    = a_tbl_update(i).checked_cost,
              du_reason_code                  = a_tbl_update(i).du_reason_code,
              unit_reason_code                = a_tbl_update(i).unit_reason_code,
              cost_price                      = a_tbl_update(i).cost_price,
              selling_price                   = a_tbl_update(i).selling_price,
              create_date                     = a_tbl_update(i).create_date,
              under_qty                       = a_tbl_update(i).under_qty,
              under_cost                      = a_tbl_update(i).under_cost,
              over_qty                        = a_tbl_update(i).over_qty,
              over_cost                       = a_tbl_update(i).over_cost,
              substitute_qty                  = a_tbl_update(i).substitute_qty,
              substitute_cost                 = a_tbl_update(i).substitute_cost,
              net_accuracy_qty                = a_tbl_update(i).net_accuracy_qty,
              net_accuracy_cost               = a_tbl_update(i).net_accuracy_cost,
              gross_accuracy_qty              = a_tbl_update(i).gross_accuracy_qty,
              gross_accuracy_cost             = a_tbl_update(i).gross_accuracy_cost,
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  asn_no                          = a_tbl_update(i).asn_no and
              du_id                           = a_tbl_update(i).du_id  and
              di_no                           = a_tbl_update(i).di_no  and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no ;

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
                       ' '||a_tbl_update(g_error_index).asn_no||
                       ' '||a_tbl_update(g_error_index).du_id||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).di_no;
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
   from   rtl_stockown_dcr
   where  asn_no             = g_rec_out.asn_no              and
          du_id              = g_rec_out.du_id               and
          sk1_item_no        = g_rec_out.sk1_item_no         and
          di_no              = g_rec_out.di_no     ;

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

    l_text := 'LOAD OF rtl_stockown_dcr EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    select this_wk_end_date, last_wk_start_date
    into   g_this_wk_end, g_last_wk_start
    from   dim_control;

--   g_last_wk_start := '19 jan 2009';   -- for testing

    l_text := 'DATE RANGE BEING PROCESSED = '||g_last_wk_start|| ' TO '||g_this_wk_end;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_stockown_dcr;
    fetch c_fnd_rtl_stockown_dcr bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_stockown_dcr bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_stockown_dcr;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
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

end wh_prf_corp_720u;
