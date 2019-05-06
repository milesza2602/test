--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_154U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_154U"                                 (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2009
--  Author:      M Munnik
--  Purpose:     Loads location item day Quality Single RTV Reason Code facts in the performance layer with
--               added value ex foundation layer table for Single RTV Warehouse (location 218).
--               Quality Bulk RTV Reason Code facts loaded by seperate procedure - wh_prf_corp_155u.
--  Tables:      Input  -   fnd_rtl_sngl_rtv_wh_fault_dtl
--               Output -   rtl_loc_item_dy_supp_rsn_rtv
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            rtl_loc_item_dy_supp_rsn_rtv%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_154U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOADS RTL_LOC_ITEM_DY_SUPP_RSN_RTV EX FND_RTL_SNGL_RTV_WH_FAULT_DTL';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_supp_rsn_rtv%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_supp_rsn_rtv%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- Because the main SELECT has a GROUP BY clause, there could be for example 4 records 
-- belonging to the same group of key fields (item_no, fault_code, tran_date), 
-- but only 2 of these 4 has been updated on the selected date (g_date).
-- Therefore, the 'with' clause is needed to join back to fnd_rtl_sngl_rtv_wh_fault_dtl, to prevent 
-- doing the GROUP BY and excluding the records not being updated. 

cursor c_rtl_loc_item_dy_supp_rsn_rtv is
   with changed_srf as (select   item_no, fault_code, tran_date
                        from     fnd_rtl_sngl_rtv_wh_fault_dtl
                        where    last_updated_date = g_date
                        group by item_no, fault_code, tran_date)
   select   /*+ USE_HASH (di, dih, dl, dlh, srf) */
            dl.sk1_location_no,
            di.sk1_item_no,
            di.item_no,
            srf.tran_date calendar_date,
            di.sk1_supplier_no,
            drc.sk1_rtv_reason_code,
            max(dlh.sk2_location_no) sk2_location_no,
            max(dih.sk2_item_no) sk2_item_no,
            sum(srf.tsf_qty) quality_single_rtv_qty,
            sum(srf.tsf_qty * di.base_rsp_excl_vat) quality_single_rtv_selling
   from     changed_srf csrf
   join     fnd_rtl_sngl_rtv_wh_fault_dtl srf on  srf.item_no         = csrf.item_no
                                              and srf.fault_code      = csrf.fault_code
                                              and srf.tran_date       = csrf.tran_date
   join     dim_rtv_reason_code drc           on  drc.rtv_reason_code = srf.fault_code
   join     dim_location dl                   on  1                   = 1
   join     dim_location_hist dlh             on  dlh.location_no     = dl.location_no
                                              and srf.tran_date       between dlh.sk2_active_from_date and dlh.sk2_active_to_date
   join     dim_item di                       on  di.item_no          = srf.item_no
   join     dim_item_hist dih                 on  dih.item_no         = srf.item_no
                                              and srf.tran_date       between dih.sk2_active_from_date and dih.sk2_active_to_date
   where    dl.location_no                    =   218
   group by dl.sk1_location_no, di.sk1_item_no, di.item_no, srf.tran_date, di.sk1_supplier_no, drc.sk1_rtv_reason_code;

-- Input record declared as cursor%rowtype
g_rec_in             c_rtl_loc_item_dy_supp_rsn_rtv%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_rtl_loc_item_dy_supp_rsn_rtv%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out                            := null;
   g_rec_out.sk1_location_no            := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                := g_rec_in.sk1_item_no;
   g_rec_out.calendar_date              := g_rec_in.calendar_date;
   g_rec_out.sk1_supplier_no            := g_rec_in.sk1_supplier_no;
   g_rec_out.sk1_rtv_reason_code        := g_rec_in.sk1_rtv_reason_code;
   g_rec_out.sk2_location_no            := g_rec_in.sk2_location_no;
   g_rec_out.sk2_item_no                := g_rec_in.sk2_item_no;
   g_rec_out.quality_single_rtv_qty     := g_rec_in.quality_single_rtv_qty;
   g_rec_out.quality_single_rtv_selling := g_rec_in.quality_single_rtv_selling;
   g_rec_out.last_updated_date          := g_date;

   begin
      select wac1.wac * g_rec_out.quality_single_rtv_qty
      into   g_rec_out.quality_single_rtv_cost
      from   fnd_rtl_loc_item_dy_rms_wac wac1
      where  wac1.tran_date   = (select max(wac2.tran_date) from fnd_rtl_loc_item_dy_rms_wac wac2
                                 where  wac2.tran_date   <= g_rec_out.calendar_date
                                 and    wac2.item_no     =  g_rec_in.item_no
                                 and    wac2.location_no =  473)
      and    wac1.item_no     = g_rec_in.item_no
      and    wac1.location_no = 473;
      exception
         when no_data_found then
            g_rec_out.quality_single_rtv_cost := null;
   end;

-- location 218 is the single returns warehouse. 218 does not have entries on fnd_rtl_loc_item_dy_rms_wac. 
-- 473 is the wh_store_no for location 218.  473 must be used to get the wac values for 218, from fnd_rtl_loc_item_dy_rms_wac.
-- select wh_store_no from dim_location where location_no = 218
   
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
      insert into rtl_loc_item_dy_supp_rsn_rtv values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).calendar_date||
                       ' '||a_tbl_insert(g_error_index).sk1_supplier_no||
                       ' '||a_tbl_insert(g_error_index).sk1_rtv_reason_code;
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
      update rtl_loc_item_dy_supp_rsn_rtv
      set sk2_location_no             = a_tbl_update(i).sk2_location_no,
          sk2_item_no                 = a_tbl_update(i).sk2_item_no,
          quality_single_rtv_qty      = a_tbl_update(i).quality_single_rtv_qty,
          quality_single_rtv_selling  = a_tbl_update(i).quality_single_rtv_selling,
          quality_single_rtv_cost     = a_tbl_update(i).quality_single_rtv_cost,
          last_updated_date           = a_tbl_update(i).last_updated_date
      where sk1_location_no           = a_tbl_update(i).sk1_location_no
      and   sk1_item_no               = a_tbl_update(i).sk1_item_no
      and   calendar_date             = a_tbl_update(i).calendar_date
      and   sk1_supplier_no           = a_tbl_update(i).sk1_supplier_no
      and   sk1_rtv_reason_code       = a_tbl_update(i).sk1_rtv_reason_code;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).calendar_date||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no||
                       ' '||a_tbl_update(g_error_index).sk1_rtv_reason_code;
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
   into g_count
   from rtl_loc_item_dy_supp_rsn_rtv
   where sk1_location_no     = g_rec_out.sk1_location_no
   and   sk1_item_no         = g_rec_out.sk1_item_no
   and   calendar_date       = g_rec_out.calendar_date
   and   sk1_supplier_no     = g_rec_out.sk1_supplier_no
   and   sk1_rtv_reason_code = g_rec_out.sk1_rtv_reason_code;

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
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_LOC_ITEM_DY_SUPP_RSN_RTV EX FND_RTL_SNGL_RTV_WH_FAULT_DTL STARTED '||
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
    open c_rtl_loc_item_dy_supp_rsn_rtv;
    fetch c_rtl_loc_item_dy_supp_rsn_rtv bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 2500 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_dy_supp_rsn_rtv bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_dy_supp_rsn_rtv;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
      local_bulk_insert;
      local_bulk_update;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_corp_154u;
