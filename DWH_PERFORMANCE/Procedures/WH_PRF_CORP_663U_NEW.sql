--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_663U_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_663U_NEW"                                                                                                                                                                 (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2009
--  Author:      M Munnik
--  Purpose:     Copy sum of actl_grn_xxxxx measures from Shipment records (po_ind = 0) to PO records (po_ind = 1).
--  Tables:      Input  - rtl_po_supchain_loc_item_dy
--               Output - rtl_po_supchain_loc_item_dy
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_date               date          := trunc(sysdate);
g_rec_out            rtl_po_supchain_loc_item_dy%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'wh_prf_corp_663U_NEW';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type;
l_description        sys_dwh_log_summary.log_description%type  := 'COPY ACTL GRN MEASURES FROM SHIPMENT TO PO RECORDS ON PO SUPCHAIN';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_u is table of rtl_po_supchain_loc_item_dy%rowtype index by binary_integer;
a_tbl_update        tbl_array_u;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_temp_po_supchain_sum is
   select   *
   from     temp_po_supchain_sum;

g_rec_in          c_temp_po_supchain_sum%rowtype;
type stg_array is table of c_temp_po_supchain_sum%rowtype;
a_stg_input       stg_array;

--**************************************************************************************************
-- Load summary values into global temporary table.
-- Because this SP takes quite some time to run, it had to be changed to make use of a global temporary table.
-- If the cursor selects data from the same table, that the SP wants to update to,
-- it sometimes fails on the error : snapshot too old: rollback segment ..... too small
-- So the results of the select is first loaded into the gtt, read again and updated to the target table.
-- DML to a gtt is not logged, so the undo tablespace will not be used.
--**************************************************************************************************
procedure load_summary_values as
begin

   insert        into dwh_performance.temp_po_supchain_sum
   select        /*+ parallel (ps,4) full(ps) */ 
                 ps.sk1_po_no, ps.sk1_supply_chain_no, ps.sk1_location_no, ps.sk1_item_no, ps.tran_date,
                 clc.fillrate_actl_grn_qty, clc.fillrate_actl_grn_selling, clc.fillrate_actl_grn_excl_wh_qty, clc.fillrte_actl_grn_excl_wh_sell
   from
      (select    /*+ full(r) parallel (r,4)  */ tl.sk1_po_no,
                (case when l.loc_type = 'W' then lv.sk1_location_no else r.sk1_location_no end) sk1_location_no,
                 r.sk1_item_no,
                 sum(case when (r.po_status_code <> 'C' or sc.supply_chain_code in('HS','VMI','NSC')) then null
                          else r.actl_grn_qty end) fillrate_actl_grn_qty,
                 sum(case when (r.po_status_code <> 'C' or sc.supply_chain_code in('HS','VMI','NSC')) then null
                          else r.actl_grn_selling end) fillrate_actl_grn_selling,
                 sum(case when (r.po_status_code <> 'C' or sc.supply_chain_code in('HS','VMI','NSC','WH')) then null
                          else r.actl_grn_qty end) fillrate_actl_grn_excl_wh_qty,
                 sum(case when (r.po_status_code <> 'C' or sc.supply_chain_code in('HS','VMI','NSC','WH')) then null
                          else r.actl_grn_selling end) fillrte_actl_grn_excl_wh_sell
       from      rtl_po_supchain_loc_item_dy r
       join      temp_po_list tl          on  r.sk1_po_no                = tl.sk1_po_no
                                          and r.sk1_item_no              = tl.sk1_item_no
       join      dim_supply_chain_type sc on  r.sk1_supply_chain_no      = sc.sk1_supply_chain_no
       join      dim_item i               on  r.sk1_item_no              = i.sk1_item_no
       join      dim_location l           on  r.sk1_location_no          = l.sk1_location_no
       left join dim_location lv          on  l.wh_primary_virtual_wh_no = lv.location_no
       where     i.business_unit_no not in(50, 70)
       group by  tl.sk1_po_no, r.sk1_item_no, (case when l.loc_type = 'W' then lv.sk1_location_no else r.sk1_location_no end)) clc
   join          rtl_po_supchain_loc_item_dy ps
   on            clc.sk1_po_no            = ps.sk1_po_no
   and           clc.sk1_location_no      = ps.sk1_location_no
   and           clc.sk1_item_no          = ps.sk1_item_no
   where         ps.po_ind                = 1;

   g_recs_inserted  := sql%rowcount;
   l_text := 'Records inserted to temp_po_supchain_sum '||g_recs_inserted;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end load_summary_values;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                                     := null;

   g_rec_out.sk1_po_no                           := g_rec_in.sk1_po_no;
   g_rec_out.sk1_supply_chain_no                 := g_rec_in.sk1_supply_chain_no;
   g_rec_out.sk1_location_no                     := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                         := g_rec_in.sk1_item_no;
   g_rec_out.tran_date                           := g_rec_in.tran_date;
   g_rec_out.fillrate_actl_grn_qty               := g_rec_in.fillrate_actl_grn_qty;
   g_rec_out.fillrate_actl_grn_selling           := g_rec_in.fillrate_actl_grn_selling;
   g_rec_out.fillrate_actl_grn_excl_wh_qty       := g_rec_in.fillrate_actl_grn_excl_wh_qty;
   g_rec_out.fillrte_actl_grn_excl_wh_sell       := g_rec_in.fillrte_actl_grn_excl_wh_sell;

   g_rec_out.last_updated_date                   := g_date;

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

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update rtl_po_supchain_loc_item_dy
       set    fillrate_actl_grn_qty          = a_tbl_update(i).fillrate_actl_grn_qty,
              fillrate_actl_grn_selling      = a_tbl_update(i).fillrate_actl_grn_selling,
              fillrate_actl_grn_excl_wh_qty  = a_tbl_update(i).fillrate_actl_grn_excl_wh_qty,
              fillrte_actl_grn_excl_wh_sell  = a_tbl_update(i).fillrte_actl_grn_excl_wh_sell,
              last_updated_date              = a_tbl_update(i).last_updated_date
       where  sk1_po_no                      = a_tbl_update(i).sk1_po_no
       and    sk1_supply_chain_no            = a_tbl_update(i).sk1_supply_chain_no
       and    sk1_location_no                = a_tbl_update(i).sk1_location_no
       and    sk1_item_no                    = a_tbl_update(i).sk1_item_no
       and    tran_date                      = a_tbl_update(i).tran_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).tran_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the output table
--**************************************************************************************************
procedure local_write_output as
begin
-- Place data into and array for later writing to table in bulk
   a_count_u               := a_count_u + 1;
   a_tbl_update(a_count_u) := g_rec_out;
   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_update;
      a_tbl_update  := a_empty_set_u;
      a_count_u     := 0;
      a_count       := 0;
      commit;
   end if;

   exception
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
    l_text := 'COPY ACTL GRN MEASURES FROM SHIPMENT TO PO RECORDS ON PO SUPCHAIN STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--        G_DATE := '29 AUG 2016';
--    l_text := 'HARDCODE G_DATE='||G_DATE;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    load_summary_values;
    open c_temp_po_supchain_sum;
    fetch c_temp_po_supchain_sum bulk collect into a_stg_input limit g_forall_limit;
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

    fetch c_temp_po_supchain_sum bulk collect into a_stg_input limit g_forall_limit;

    end loop;

    close c_temp_po_supchain_sum;
--**************************************************************************************************
-- At end write out what remains in the array at end of program
--**************************************************************************************************
    local_bulk_update;
    commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
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
      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

end wh_prf_corp_663U_NEW;
