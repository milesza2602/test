--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_722U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_722U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Stockown rollup fact table in the performance layer
--               with input ex Stockown DCR  table from performance layer.
--  Tables:      Input  - rtl_stockown_dcr
--               Output - rtl_loc_item_wk_stockown_dcr
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
g_rec_out            rtl_loc_item_wk_stockown_dcr%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_this_wk_end        date;
g_last_wk_start      date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_722U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS STOCKOWN DCR FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_wk_stockown_dcr%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_wk_stockown_dcr%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_rtl_stockown_dcr is
   select   max(from_location_no) as from_location_no,
            sum(nvl(so.expected_qty,0)) as expected_qty,
            sum(nvl(so.expected_cost,0)) as expected_cost,
            sum(nvl(so.checked_qty,0)) as checked_qty,
            sum(nvl(so.checked_cost,0)) as checked_cost,
            sum(nvl(so.under_qty,0)) as under_qty,
            sum(nvl(so.under_cost,0)) as under_cost,
            sum(nvl(so.over_qty,0)) as over_qty,
            sum(nvl(so.over_cost,0)) as over_cost,
            sum(nvl(so.substitute_qty,0)) as substitute_qty,
            sum(nvl(so.substitute_cost,0)) as substitute_cost,
            sum(nvl(so.net_accuracy_qty,0)) as net_accuracy_qty,
            sum(nvl(so.net_accuracy_cost,0)) as net_accuracy_cost,
            sum(nvl(so.gross_accuracy_qty,0)) as gross_accuracy_qty,
            sum(nvl(so.gross_accuracy_cost,0)) as gross_accuracy_cost,
            so.sk1_item_no,
            so.sk1_to_location_no,
            so.sk1_supplier_no,
            max(so.sk2_item_no) as  sk2_item_no,
            max(so.sk2_to_location_no) as sk2_to_location_no,
            dc.fin_year_no,
            dc.fin_week_no,
            dc.fin_week_code,
            dc.this_week_start_date
   from     rtl_stockown_dcr so,
            dim_calendar dc
   where    so.create_date             = dc.calendar_date and
            so.create_date             between g_last_wk_start and g_this_wk_end and
            so.detail_check_ind        = 1
   group by dc.fin_year_no,
            dc.fin_week_no,
            dc.fin_week_code,
            dc.this_week_start_date,
            so.sk1_item_no,
            so.sk1_to_location_no,
            so.sk1_supplier_no;

g_rec_in             c_rtl_stockown_dcr%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_stockown_dcr%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_to_location_no              := g_rec_in.sk1_to_location_no;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_supplier_no                 := g_rec_in.sk1_supplier_no;
   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.sk2_to_location_no              := g_rec_in.sk2_to_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.fin_week_code                   := g_rec_in.fin_week_code;
   g_rec_out.this_week_start_date            := g_rec_in.this_week_start_date;
   g_rec_out.from_location_no                := g_rec_in.from_location_no;
   g_rec_out.expected_qty                    := g_rec_in.expected_qty;
   g_rec_out.expected_cost                   := g_rec_in.expected_cost;
   g_rec_out.checked_qty                     := g_rec_in.checked_qty;
   g_rec_out.checked_cost                    := g_rec_in.checked_cost;
   g_rec_out.under_qty                       := g_rec_in.under_qty;
   g_rec_out.under_cost                      := g_rec_in.under_cost;
   g_rec_out.over_qty                        := g_rec_in.over_qty;
   g_rec_out.over_cost                       := g_rec_in.over_cost;
   g_rec_out.substitute_qty                  := g_rec_in.substitute_qty;
   g_rec_out.substitute_cost                 := g_rec_in.substitute_cost;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.net_accuracy_qty                := g_rec_in.net_accuracy_qty ;
   g_rec_out.net_accuracy_cost               := g_rec_in.net_accuracy_cost ;
   g_rec_out.gross_accuracy_qty              := g_rec_in.gross_accuracy_qty;
   g_rec_out.gross_accuracy_cost             := g_rec_in.gross_accuracy_cost ;


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
       insert into rtl_loc_item_wk_stockown_dcr values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_to_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).fin_week_no;
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
       update rtl_loc_item_wk_stockown_dcr
       set    fin_week_code                   = a_tbl_update(i).fin_week_code,
              this_week_start_date            = a_tbl_update(i).this_week_start_date,
              from_location_no                = a_tbl_update(i).from_location_no,
              expected_qty                    = a_tbl_update(i).expected_qty,
              expected_cost                   = a_tbl_update(i).expected_cost,
              checked_qty                     = a_tbl_update(i).checked_qty,
              checked_cost                    = a_tbl_update(i).checked_cost,
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
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_to_location_no              = a_tbl_update(i).sk1_to_location_no  and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no         and
              sk1_supplier_no                 = a_tbl_update(i).sk1_supplier_no     and
              fin_year_no                     = a_tbl_update(i).fin_year_no         and
              fin_week_no                     = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_to_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no;
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
   from   rtl_loc_item_wk_stockown_dcr
   where  sk1_to_location_no = g_rec_out.sk1_to_location_no  and
          sk1_item_no        = g_rec_out.sk1_item_no         and
          sk1_supplier_no    = g_rec_out.sk1_supplier_no     and
          fin_year_no        = g_rec_out.fin_year_no         and
          fin_week_no        = g_rec_out.fin_week_no;

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

    l_text := 'LOAD OF rtl_loc_item_wk_stockown_dcr EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    select this_wk_end_date, last_wk_start_date
    into   g_this_wk_end, g_last_wk_start
    from   dim_control;

--   g_last_wk_start := '22 Jun 2015';   -- for testing
    
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
    open c_rtl_stockown_dcr;
    fetch c_rtl_stockown_dcr bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_rtl_stockown_dcr bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_stockown_dcr;
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

end wh_prf_corp_722u;
