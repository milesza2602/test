--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_664U_BK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_664U_BK" 
                                                      (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        February 2009
--  Author:      M Munnik
--  Purpose:     Rollup from rtl_po_supchain_loc_item_dy to rtl_loc_item_dy_po_supchain.
--               Rollup excluding po and supchain.
--  Tables:      Input  - rtl_po_supchain_loc_item_dy
--               Output - rtl_loc_item_dy_po_supchain
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
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_deleted       integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            rtl_loc_item_dy_po_supchain%rowtype;
g_found              boolean;
g_date               date;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_664U_BK';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP PO SUPCHAIN TO LOC_ITEM_DY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_po_supchain%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_po_supchain%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_rtl_loc_item_dy_po_supchain is
  with chgrecs as (select   tran_date, sk1_location_no, sk1_item_no 
                    from     w7104429.rtl_po_supchain_loc_item_dy
                    where    last_updated_date = g_date
                    and     sk1_item_no <> 25329804 
                    group by tran_date, sk1_location_no, sk1_item_no
                    union
                    select   tran_date, sk1_location_no, sk1_item_no 
                    from     temp_po_deletes
                    group by tran_date, sk1_location_no, sk1_item_no)

   select   lid.sk1_location_no,
            lid.sk1_item_no,
            lid.tran_date,
            max(lid.sk2_location_no) sk2_location_no,
            max(lid.sk2_item_no) sk2_item_no,
            sum(lid.original_po_qty) original_po_qty,
            sum(lid.original_po_selling) original_po_selling,
            sum(lid.original_po_cost) original_po_cost,
            sum(lid.original_po_cases) original_po_cases,
            sum(lid.amended_po_qty) amended_po_qty,
            sum(lid.amended_po_selling) amended_po_selling,
            sum(lid.amended_po_cost) amended_po_cost,
            sum(lid.amended_po_cases) amended_po_cases,
            sum(lid.buyer_edited_po_qty) buyer_edited_po_qty,
            sum(lid.cancel_po_qty) cancel_po_qty,
            sum(lid.cancel_po_selling) cancel_po_selling,
            sum(lid.cancel_po_cost) cancel_po_cost,
            sum(lid.cancel_po_cases) cancel_po_cases,
            sum(lid.rejection_qty) rejection_qty,
            sum(lid.rejection_selling) rejection_selling,
            sum(lid.rejection_cost) rejection_cost,
            sum(lid.rejected_cases) rejected_cases,
            sum(lid.po_grn_qty) po_grn_qty,
            sum(lid.po_grn_selling) po_grn_selling,
            sum(lid.po_grn_cost) po_grn_cost,
            sum(lid.po_grn_cases) po_grn_cases,
            sum(lid.po_grn_fr_cost) po_grn_fr_cost,
            sum(lid.shorts_qty) shorts_qty,
            sum(lid.shorts_selling) shorts_selling,
            sum(lid.shorts_cost) shorts_cost,
            sum(lid.shorts_cases) shorts_cases,
            sum(lid.bc_shipment_qty) bc_shipment_qty,
            sum(lid.bc_shipment_selling) bc_shipment_selling,
            sum(lid.bc_shipment_cost) bc_shipment_cost,
            sum(lid.actl_grn_qty) actl_grn_qty,
            sum(lid.actl_grn_selling) actl_grn_selling,
            sum(lid.actl_grn_cost) actl_grn_cost,
            sum(lid.actl_grn_cases) actl_grn_cases,
            sum(lid.actl_grn_fr_cost) actl_grn_fr_cost,
            sum(lid.actl_grn_margin) actl_grn_margin,
            sum(lid.fillrate_actl_grn_excl_wh_qty) fillrate_actl_grn_excl_wh_qty,
            sum(lid.fillrte_actl_grn_excl_wh_sell) fillrte_actl_grn_excl_wh_sell,
            sum(lid.latest_po_qty) latest_po_qty,
            sum(lid.latest_po_selling) latest_po_selling,
            sum(lid.latest_po_cost) latest_po_cost,
            max(lid.latest_po_qty_all_time) latest_po_qty_all_time,
            max(lid.latest_po_selling_all_time) latest_po_selling_all_time,
            max(lid.latest_po_cost_all_time) latest_po_cost_all_time,
            max(lid.avg_po_rsp_excl_vat_all_time) avg_po_rsp_excl_vat_all_time,
            max(lid.avg_po_cost_price_all_time) avg_po_cost_price_all_time,
            max(lid.avg_po_margin_perc_all_time) avg_po_margin_perc_all_time,
            (case when 1 = 2 then 0 end) num_du,   -- to force a null value
            sum(lid.num_weighted_days_to_deliver) num_weighted_days_to_deliver,
            sum(lid.fillrate_order_qty) fillrate_order_qty,
            sum(lid.fillrate_order_selling) fillrate_order_selling,
            sum(lid.fillrate_order_excl_wh_qty) fillrate_order_excl_wh_qty,
            sum(lid.fillrate_order_excl_wh_selling) fillrate_order_excl_wh_selling,
            g_date last_updated_date,
            sum(lid.fillrate_actl_grn_qty) fillrate_actl_grn_qty,
            sum(lid.fillrate_actl_grn_selling) fillrate_actl_grn_selling
   from     chgrecs cr
   join     w7104429.rtl_po_supchain_loc_item_dy lid     on  lid.tran_date       = cr.tran_date
                                                and lid.sk1_location_no = cr.sk1_location_no
                                                AND LID.SK1_ITEM_NO     = CR.SK1_ITEM_NO
--                                                and lid.tran_date < '06 oct 15'
-- this date is being commented out as it should not be here.
-- we think it was added to prevent loading into partitions which did not exist
--
   group by lid.tran_date, lid.sk1_location_no, lid.sk1_item_no;

-- Input record declared as cursor%rowtype
g_rec_in             c_rtl_loc_item_dy_po_supchain%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_rtl_loc_item_dy_po_supchain%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out   			                     := g_rec_in;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into w7104429.rtl_loc_item_dy_po_supchain values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).tran_date||
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update w7104429.rtl_loc_item_dy_po_supchain
      set    row                 = a_tbl_update(i)
      where  tran_date           = a_tbl_update(i).tran_date
      and    sk1_location_no     = a_tbl_update(i).sk1_location_no
      and    sk1_item_no         = a_tbl_update(i).sk1_item_no;

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
                       ' '||a_tbl_update(g_error_index).tran_date||
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to table
--**************************************************************************************************
procedure local_write_output as
begin

   g_found := FALSE;
-- Check to see if record is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   w7104429.rtl_loc_item_dy_po_supchain
   where  tran_date           = g_rec_out.tran_date
   and    sk1_location_no     = g_rec_out.sk1_location_no
   and    sk1_item_no         = g_rec_out.sk1_item_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place record into array for later bulk writing
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
-- Records on rtl_loc_item_dy_po_supchain are a summary of rtl_po_supchain_loc_item_dy per 
-- tran_date, sk1_location_no, sk1_item_no.
-- Therefore, when the not_before_date of a PO changes, it moves from one tran_date to another.
-- Records on rtl_loc_item_dy_po_supchain which belongs to the tran_date, sk1_location_no, sk1_item_no
-- combination of any of the records which have been deleted from rtl_po_supchain_loc_item_dy,
-- have to be re-summarised, if the tran_date, sk1_location_no, sk1_item_no combination
-- still exsts on rtl_po_supchain_loc_item_dy.
-- Therefore, the summarised records are first deleted from rtl_loc_item_dy_po_supchain.
--**************************************************************************************************
procedure delete_recs as
begin

    delete from w7104429.rtl_loc_item_dy_po_supchain r
    where  exists(select d.tran_date, d.sk1_location_no, d.sk1_item_no 
                  from   temp_po_deletes d
                  where  d.tran_date       = r.tran_date
                  and    d.sk1_location_no = r.sk1_location_no
                  and    d.sk1_item_no     = r.sk1_item_no);

    g_recs_deleted  := g_recs_deleted  + sql%rowcount;

    exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end delete_recs;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'ROLLUP OF rtl_loc_item_dy_po_supchain EX PO SUPCHAIN LEVEL STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    G_DATE := '22 Nov 2016';
 --   l_text := 'HARDCODE G_DATE='||G_DATE;
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
    delete_recs;

    open c_rtl_loc_item_dy_po_supchain;
    fetch c_rtl_loc_item_dy_po_supchain bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_dy_po_supchain bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_dy_po_supchain;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
      local_bulk_insert;
      local_bulk_update;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,g_recs_deleted,'');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_corp_664u_bk;
