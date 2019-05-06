--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_687U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_687U" (p_forall_limit in integer,p_success out boolean) as

-- *************************************************************************************************
-- * Notes from 12.2 upgrade performance tuning
-- *************************************************************************************************
-- Date:   2019-03-22
-- Author: Paul Wakefield
-- Added hints to cursor c_po_chain
-- **************************************************************************************************

--**************************************************************************************************
--  Date:         February 2013
--  Author:       A Joshua
--  Purpose:      Rollup of PO data needed for JDA Assort Commitment
--  Tables:       Input  - rtl_po_supchain_loc_item_dy
--                Output - rtl_po_chain_sc_wk
--  Packages:     constants, dwh_log, dwh_valid
--  Maintenance:
--   24 july 2014 - WW-1591446  - add po_delete processing
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_rec_out            rtl_po_chain_sc_wk%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_687U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ADDS SHIPMENT INFO TO PO COMBINATION FACT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_po_chain_sc_wk%rowtype index by binary_integer;
type tbl_array_u is table of rtl_po_chain_sc_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

Cursor c_po_chain Is
   with chgrecs as (select   SK1_PO_NO,SK1_CHAIN_NO,SK1_STYLE_COLOUR_NO,FIN_YEAR_NO,FIN_WEEK_NO
                    from     rtl_po_supchain_loc_item_dy   rtl, dim_location dl, dim_item di, dim_calendar dc --rtl_supchain_loc_item_dy
                    where    rtl.last_updated_date = g_date
                    and dl.sk1_location_no = rtl.sk1_location_no
                    and di.sk1_item_no = rtl.sk1_item_no
                    and dc.calendar_date = rtl.tran_date
                    group by  SK1_PO_NO,SK1_CHAIN_NO,SK1_STYLE_COLOUR_NO,FIN_YEAR_NO,FIN_WEEK_NO
                    union
                    select    SK1_PO_NO,SK1_CHAIN_NO,SK1_STYLE_COLOUR_NO,FIN_YEAR_NO,FIN_WEEK_NO
                    from     temp_po_deletes po, dim_location dl
                    where po.sk1_location_no = dl.sk1_location_no
                    group by  SK1_PO_NO,SK1_CHAIN_NO,SK1_STYLE_COLOUR_NO,FIN_YEAR_NO,FIN_WEEK_NO)
    select /*+ PARALLEL(4) */
              po.sk1_po_no,
              dl.sk1_chain_no,
              di.sk1_style_colour_no,
              cal.fin_year_no,
              cal.fin_week_no,
              sum(po.original_po_qty)     original_po_qty,
              sum(po.original_po_selling) original_po_selling,
              sum(po.original_po_cost)    original_po_cost,
              sum(po.amended_po_qty)      amended_po_qty,
              sum(po.amended_po_selling)  amended_po_selling,
              sum(po.amended_po_cost)     amended_po_cost,
              sum(po.latest_po_qty)       latest_po_qty,
              sum(po.latest_po_selling)   latest_po_selling,
              sum(po.latest_po_cost)      latest_po_cost
    from      rtl_po_supchain_loc_item_dy po
    , dim_item di
    , dim_location dl
    , dim_calendar cal
    , dim_contract con
    , dim_supply_chain_type sct
    , chgrecs cr
    where     po.sk1_item_no     = di.sk1_item_no
     and      po.sk1_location_no = dl.sk1_location_no
     and      po.tran_date       = cal.calendar_date
     and      po.po_ind            = 1
    and      di.business_unit_no  <> 50
     and      po.sk1_contract_no   = con.sk1_contract_no
     and      con.sk1_contract_no  = 0
     and      po.sk1_supply_chain_no = sct.sk1_supply_chain_no
     and      sct.supply_chain_code  = 'WH'
     and      cr.SK1_PO_NO = po.sk1_po_no
     and      cr.SK1_CHAIN_NO = dl.sk1_chain_no
     and      cr.SK1_STYLE_COLOUR_NO = di.sk1_style_colour_no
     and      cr.FIN_YEAR_NO = cal.fin_year_no
     and      cr.FIN_WEEK_NO = cal.fin_week_no

    group by po.sk1_po_no, dl.sk1_chain_no, di.sk1_style_colour_no, cal.fin_year_no, cal.fin_week_no ;

g_rec_in             c_po_chain%rowtype;
-- For input bulk collect --
type stg_array is table of c_po_chain%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

--   g_rec_out                                     := null;

   g_rec_out.sk1_po_no			                     := g_rec_in.sk1_po_no;
   g_rec_out.sk1_chain_no       		             := g_rec_in.sk1_chain_no;
   g_rec_out.sk1_style_colour_no			           := g_rec_in.sk1_style_colour_no;
   g_rec_out.fin_year_no			                   := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no			                   := g_rec_in.fin_week_no;
   g_rec_out.original_po_qty		                 := g_rec_in.original_po_qty;
   g_rec_out.original_po_selling			           := g_rec_in.original_po_selling;
   g_rec_out.original_po_cost			               := g_rec_in.original_po_cost;
   g_rec_out.amended_po_qty			                 := g_rec_in.amended_po_qty;
   g_rec_out.amended_po_selling     			       := g_rec_in.amended_po_selling;
   g_rec_out.amended_po_cost			               := g_rec_in.amended_po_cost;
   g_rec_out.latest_po_qty			                 := g_rec_in.latest_po_qty;
   g_rec_out.latest_po_selling			             := g_rec_in.latest_po_selling;
   g_rec_out.latest_po_cost			                 := g_rec_in.latest_po_cost;
   g_rec_out.last_updated_date                   := g_date;

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
       insert into rtl_po_chain_sc_wk values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_chain_no||
                       ' '||a_tbl_insert(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_insert(g_error_index).fin_year_no||
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
       update rtl_po_chain_sc_wk
       set    original_po_qty		              = a_tbl_update(i).original_po_qty,
              original_po_selling			        = a_tbl_update(i).original_po_selling,
              original_po_cost			          = a_tbl_update(i).original_po_cost,
              amended_po_qty			            = a_tbl_update(i).amended_po_qty,
              amended_po_selling			        = a_tbl_update(i).amended_po_selling,
              amended_po_cost			            = a_tbl_update(i).amended_po_cost,
              latest_po_qty                   = a_tbl_update(i).latest_po_qty,
              latest_po_selling			          = a_tbl_update(i).latest_po_selling,
              latest_po_cost			            = a_tbl_update(i).latest_po_cost,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_po_no                       = a_tbl_update(i).sk1_po_no
       and    sk1_chain_no                    = a_tbl_update(i).sk1_chain_no
       and    sk1_style_colour_no             = a_tbl_update(i).sk1_style_colour_no
       and    fin_year_no                     = a_tbl_update(i).fin_year_no
       and    fin_week_no                     = a_tbl_update(i).fin_week_no;

--       g_recs_updated  := g_recs_updated  + a_tbl_update.count;
       g_recs_updated  := g_recs_updated  + sql%rowcount;

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
                       ' '||a_tbl_update(g_error_index).sk1_chain_no||
                       ' '||a_tbl_update(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_update(g_error_index).fin_year_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no;
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
   select count(1)
   into   g_count
   from   rtl_po_chain_sc_wk
   where  sk1_po_no             = g_rec_out.sk1_po_no
   and    sk1_chain_no          = g_rec_out.sk1_chain_no
   and    sk1_style_colour_no   = g_rec_out.sk1_style_colour_no
   and    fin_year_no           = g_rec_out.fin_year_no
   and    fin_week_no           = g_rec_out.fin_week_no;

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
-- Records on rtl_po_supchain_loc_item_dy will be deleted when the not_before_date of a PO changes.
-- Records on rtl_po_chain_sc_wk are a summary of rtl_supchain_loc_item_dy,
-- which is a summary of rtl_po_supchain_loc_item_dy.
-- Summary of rtl_po_chain_sc_wk per tran_date, sk1_location_no, sk1_item_no, sk1_supply_chain_no.
-- Therefore, when the not_before_date of a PO changes, it moves from one tran_date to another.
-- Records on rtl_po_chain_sc_wk which belongs to the
-- SK1_PO_NO, SK1_CHAIN_NO, SK1_STYLE_COLOUR_NO, FIN_YEAR_NO, FIN_WEEK_NO combination of
-- any of the records which have been deleted from rtl_po_supchain_loc_item_dy, have to be re-summarised.
-- Therefore, the summarised records are first deleted from rtl_po_chain_sc_wk.
--**************************************************************************************************
procedure delete_recs as
begin

    delete from rtl_po_chain_sc_wk r
    where  exists( select distinct r.SK1_PO_NO, r.SK1_CHAIN_NO, r.SK1_STYLE_COLOUR_NO, r.FIN_YEAR_NO, r.FIN_WEEK_NO
                  from   dwh_performance.temp_po_deletes d, dwh_performance.dim_location dl
                  where  d.fin_year_no           = r.fin_year_no
                  and  d.fin_week_no           = r.fin_week_no
                  and    d.sk1_location_no     = dl.sk1_location_no
                  and    dl.sk1_chain_no     = r.sk1_chain_no
                  and    d.SK1_STYLE_COLOUR_NO = r.SK1_STYLE_COLOUR_NO
                   );

    g_recs_deleted  := g_recs_deleted  + sql%rowcount;

    exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end delete_recs;

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
    l_text := 'LOAD OF RTL_PO_CHAIN_SC_WK EX FOUNDATION STARTED '||
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
--    g_date := '23 july 2014';
--**************************************************************************************************
    l_text := 'Delete starting ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    delete_recs;
    l_text := 'Delete finished - recs = '||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_po_chain;
    fetch c_po_chain bulk collect into a_stg_input limit g_forall_limit;
    l_text := 'a_stg_input.count = '||a_stg_input.count;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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
    fetch c_po_chain bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_po_chain;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
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

end wh_prf_corp_687U;
