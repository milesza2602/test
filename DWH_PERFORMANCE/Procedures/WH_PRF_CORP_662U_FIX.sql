--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_662U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_662U_FIX"                                                                                                 (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:         February 2009
--  Author:       M Munnik
--  Purpose:      Load PO combination fact table in performance layer
--                with input ex Purchase Order and Shipment RMS tables from foundation layer.
--                The combination fact table combines measures from PO's and Shipments.
--                The program wh_prf_corp_661u only loads the PO info to a record keyed with the
--                not_before_date/not_after_date of the PO.
--                This program loads the Shipment info to a record keyed with the actl_rcpt_date of the Shipment.
--                If the not_before_date/not_after_date and actl_rcpt_date are equal,
--                then the PO and Shipment measures will be contained on the same record.
--                The PO measures must NOT be repeated on the records where shipments were received
--                on other dates than the PO not_before_date/not_after_date.
--                However, the PO static data, like not_before_date, not_after_date, po_status_code and cancel_code
--                are carried on all the records for the PO.
--                The table temp_po_list contains the POs updated during the selected period(today or last 5 weeks)
--  Tables:       Input  - fnd_rtl_purchase_order, fnd_rtl_shipment
--                Output - rtl_po_supchain_loc_item_dy
--  Packages:     constants, dwh_log, dwh_valid
--
--  Maintenance:
--  09 Jul 2010 - M Munnik
--                The join between Purchase Orders and Shipments is changed to include location in the join criteria.
--                This will prevent the join to have duplicate records in the resultset.
--                The test to exclude PO's with more than one location, (causing duplicates), has been removed from wh_prf_corp_660u.
--                There are records on the PO table with a null supply_chain_type.
--                A list is created in wh_prf_corp_660u, to get the distinct not null supply_chain_type per PO.
--                However, there are PO's where all supply_chain_type's for the PO are null. In this case, a null is selected.
--                The distinct supply_chain_type is then used for all the records of the PO, regardless of the item and location (in wh_prf_corp_661u and _662UD).
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
g_rec_out            rtl_po_supchain_loc_item_dy%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_662U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ADDS SHIPMENT INFO TO PO COMBINATION FACT';
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

-- Before June 2007, it was allowed to completely cancel a Purchase Order
-- and then re-use the PO Number with a different location, not_before_date, etc. (therefore more than 1 location per PO).
-- The procedures joining Purchase Orders with Shipments, used to only join on po_no and item_no.
-- When doing this join on records where there are more than 1 location per PO, the resultset will contain duplicate records.
-- To prevent this, all PO's with more than one location (duplicate PO numbers), were excluded from all the wh_prf_corp_66* programs.
-- The join between Purchase Orders and Shipments were changed to include location in the join criteria.
-- First join to dim_location with the location_no on the PO. 
-- If the location on the PO is a store (loc_type 'S') then use location_no to join to Shipment else use wh_physical_wh_no to join to Shipment.
-- A decision was made and aggreed with Business, to only load PO's from Fin Year 2008 (starting at 25 June 2007) and onwards.

cursor c_fnd_po_ship is
   with
   shipment as
   (select      s.po_no, s.item_no, s.to_loc_no, s.shipment_status_code,
                s.actl_rcpt_date, s.received_qty, s.reg_rsp, s.cost_price, s.du_id
   from         fnd_rtl_shipment s
   join         temp_po_list tl
   on           s.po_no    = tl.po_no
   and          s.item_no  = tl.item_no
   where        s.shipment_status_code = 'R'
   and         (s.actl_rcpt_date is not null)),

   receipts as
   (select    s.po_no, s.item_no, s.to_loc_no, dl.sk1_location_no, dl.loc_type, dl.chain_no, s.actl_rcpt_date,
              count(distinct s.du_id) num_du,
              avg(nvl(ldd.debtors_commission_perc,0)) debtors_commission_perc, sum(s.received_qty) actl_grn_qty,
              sum(case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                    then (round((s.received_qty * (s.reg_rsp * 100 / (100 + di.vat_rate_perc)) * di.static_mass),2))
                    else (round((s.received_qty * (s.reg_rsp * 100 / (100 + di.vat_rate_perc))),2)) end) actl_grn_selling,
              sum(case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                    then (round((s.received_qty * s.cost_price * di.static_mass),2))
                    else (round((s.received_qty * s.cost_price),2)) end) actl_grn_cost
    from      shipment s
    join      dim_location dl           on  s.to_loc_no          = dl.location_no
    join      dim_item di               on  s.item_no            = di.item_no
    left join rtl_loc_dept_dy ldd       on  dl.sk1_location_no   = ldd.sk1_location_no
                                        and di.sk1_department_no = ldd.sk1_department_no
                                        and s.actl_rcpt_date     = ldd.post_date
    group by  s.po_no, s.item_no, s.to_loc_no, dl.sk1_location_no, dl.loc_type, dl.chain_no, s.actl_rcpt_date),

   purchord as
   (select    p.po_no, pl.supply_chain_type, p.item_no, pl.sk1_po_no, nvl(dsc.sk1_supply_chain_no,0) sk1_supply_chain_no,
              di.sk1_item_no, nvl(dc.sk1_contract_no, 0) sk1_contract_no, nvl(ds.sk1_supplier_no, 0) sk1_supplier_no,
              p.location_no, dlh.sk2_location_no, dih.sk2_item_no, di.fd_discipline_type, di.business_unit_no,
              p.not_before_date, p.not_after_date, p.po_status_code, p.cancel_code, p.cancel_po_qty,
              (decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray)) num_units_per_tray
    from      fnd_rtl_purchase_order p
    join      temp_po_list pl           on  p.po_no              = pl.po_no
                                        and p.item_no            = pl.item_no
    join      dim_item di               on  pl.item_no           = di.item_no
    join      dim_item_hist dih         on  pl.item_no           = dih.item_no
                                        and p.not_before_date    between dih.sk2_active_from_date and dih.sk2_active_to_date
    join      dim_location dl           on  p.location_no        = dl.location_no
    join      dim_location_hist dlh     on  p.location_no        = dlh.location_no
                                        and p.not_before_date    between dlh.sk2_active_from_date and dlh.sk2_active_to_date
    left join dim_supply_chain_type dsc on  pl.supply_chain_type = dsc.supply_chain_code
    left join dim_contract dc           on  p.contract_no        = dc.contract_no
    left join dim_supplier ds           on  p.supplier_no        = ds.supplier_no
--    left join rtl_loc_item_dy_catalog li on li.calendar_date     = (case di.fd_discipline_type 
--                                                                         when 'SA' then nvl(p.not_after_date,p.not_before_date)
--                                                                         when 'SF' then nvl(p.not_after_date,p.not_before_date)
--                                                                         else p.not_before_date end)
--                                         and di.sk1_item_no      = li.sk1_item_no
--                                         and dl.sk1_location_no  = li.sk1_location_no
    left join rtl_location_item li      on  li.sk1_item_no       = di.sk1_item_no
                                        and li.sk1_location_no   = dl.sk1_location_no
    )

    select    p.sk1_po_no, p.sk1_supply_chain_no, sr.sk1_location_no, p.sk1_item_no, sr.actl_rcpt_date tran_date,
              p.sk1_contract_no, p.sk1_supplier_no, p.sk2_location_no, p.sk2_item_no,
              p.supply_chain_type, p.not_before_date, p.not_after_date, p.po_status_code, p.cancel_code, 
              sr.chain_no, sr.debtors_commission_perc, 
              sr.num_du, sr.actl_grn_qty, sr.actl_grn_selling, sr.actl_grn_cost,
              case when p.business_unit_no = 50 then (sr.actl_grn_qty/p.num_units_per_tray) end actl_grn_cases,
              (case when sr.actl_rcpt_date is not null then
                    (case when p.fd_discipline_type in('SA','SF') and (p.not_after_date is not null)
                          then dwh_lookup.no_of_workdays(p.not_after_date, sr.actl_rcpt_date)
                          else dwh_lookup.no_of_workdays(p.not_before_date, sr.actl_rcpt_date) end)
               end) num_days_to_deliver_po
    from      purchord p
    join      dim_location l on p.location_no = l.location_no
    join      receipts sr
    on        p.po_no      = sr.po_no 
    and       p.item_no    = sr.item_no
    and       sr.to_loc_no = (case l.loc_type when 'S' then l.location_no else l.wh_physical_wh_no end);

g_rec_in             c_fnd_po_ship%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_po_ship%rowtype;
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
   g_rec_out.tran_date			                     := g_rec_in.tran_date;
   g_rec_out.sk1_contract_no		                 := g_rec_in.sk1_contract_no;
   g_rec_out.sk1_supplier_no			               := g_rec_in.sk1_supplier_no;
   g_rec_out.sk2_location_no			               := g_rec_in.sk2_location_no;
   g_rec_out.sk2_item_no			                   := g_rec_in.sk2_item_no;
   g_rec_out.po_ind     			                   := 0;
   g_rec_out.not_before_date			               := g_rec_in.not_before_date;
   g_rec_out.not_after_date			                 := g_rec_in.not_after_date;
   g_rec_out.po_status_code			                 := g_rec_in.po_status_code;
   g_rec_out.cancel_code			                   := g_rec_in.cancel_code;
   g_rec_out.actl_grn_qty			                   := g_rec_in.actl_grn_qty;
   g_rec_out.actl_grn_selling			               := g_rec_in.actl_grn_selling;
   g_rec_out.actl_grn_cost			                 := g_rec_in.actl_grn_cost;
   g_rec_out.actl_grn_cases			                 := round(g_rec_in.actl_grn_cases,0);
   g_rec_out.num_du			                         := g_rec_in.num_du;
   g_rec_out.num_days_to_deliver_po		           := g_rec_in.num_days_to_deliver_po;
-- Case quantities can not contain fractions, the case quantity has to be an integer value (ie. 976.0).

   if g_rec_in.chain_no = 20 then
      g_rec_out.actl_grn_fr_cost                 := round(g_rec_in.actl_grn_cost +
                                                   (g_rec_in.actl_grn_cost * (g_rec_in.debtors_commission_perc/100)),2);
   end if;

   if (g_rec_in.actl_grn_qty is null) then
      g_rec_out.num_days_to_deliver_po           := null;
   end if;

   if (g_rec_out.num_days_to_deliver_po is not null) then
      g_rec_out.num_weighted_days_to_deliver     := g_rec_out.num_days_to_deliver_po * g_rec_out.actl_grn_qty;
   end if;

   if (g_rec_out.actl_grn_selling is not null) or (g_rec_out.actl_grn_cost is not null) then
      g_rec_out.actl_grn_margin		               := nvl(g_rec_out.actl_grn_selling,0) - nvl(g_rec_out.actl_grn_cost,0);
   end if;

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

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update rtl_po_supchain_loc_item_dy
       set    
--              sk1_contract_no		              = a_tbl_update(i).sk1_contract_no,
--              sk1_supplier_no			            = a_tbl_update(i).sk1_supplier_no,
--              sk2_location_no			            = a_tbl_update(i).sk2_location_no,
--              sk2_item_no			                = a_tbl_update(i).sk2_item_no,
--              not_before_date			            = a_tbl_update(i).not_before_date,
--              not_after_date			            = a_tbl_update(i).not_after_date,
--              po_status_code                  = a_tbl_update(i).po_status_code,
--              cancel_code			                = a_tbl_update(i).cancel_code,
--              actl_grn_qty			              = a_tbl_update(i).actl_grn_qty,
              actl_grn_selling			          = a_tbl_update(i).actl_grn_selling,
--              actl_grn_cost			              = a_tbl_update(i).actl_grn_cost,
--              actl_grn_cases			            = a_tbl_update(i).actl_grn_cases,
--              actl_grn_fr_cost			          = a_tbl_update(i).actl_grn_fr_cost,
              actl_grn_margin			            = a_tbl_update(i).actl_grn_margin
--              num_du			                    = a_tbl_update(i).num_du,
--              num_days_to_deliver_po		      = a_tbl_update(i).num_days_to_deliver_po,
--              num_weighted_days_to_deliver    = a_tbl_update(i).num_weighted_days_to_deliver,
--              last_updated_date               = a_tbl_update(i).last_updated_date
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
   select count(1)
   into   g_count
   from   rtl_po_supchain_loc_item_dy
   where  sk1_po_no             = g_rec_out.sk1_po_no
   and    sk1_supply_chain_no   = g_rec_out.sk1_supply_chain_no
   and    sk1_location_no       = g_rec_out.sk1_location_no
   and    sk1_item_no           = g_rec_out.sk1_item_no
   and    tran_date             = g_rec_out.tran_date;

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
    open c_fnd_po_ship;
    fetch c_fnd_po_ship bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_po_ship bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_po_ship;
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

end wh_prf_corp_662U_FIX;
