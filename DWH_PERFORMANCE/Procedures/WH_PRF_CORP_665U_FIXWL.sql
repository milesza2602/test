--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_665U_FIXWL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_665U_FIXWL" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        February 2009
--  Author:      M Munnik
--  Purpose:     Rollup from rtl_loc_item_dy_po_supchain to rtl_loc_sc_wk_po_supchain.
--  Tables:      Input  - rtl_loc_item_dy_po_supchain
--               Output - rtl_loc_sc_wk_po_supchain
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
g_rec_out            rtl_loc_sc_wk_po_supchain%rowtype;
g_found              boolean;
g_date               date;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_665U_FIXWL';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL LOC_ITEM_DY_PO_SUPCHAIN TO LOC_SC_WK_PO_SUPCHAIN';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_sc_wk_po_supchain%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_sc_wk_po_supchain%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_rtl_loc_sc_wk_po_supchain is
   with chgrecs as (select   r.sk1_location_no, i.sk1_style_colour_no, c.fin_year_no, c.fin_week_no
                    from     rtl_loc_item_dy_po_supchain r
                    join     dim_item i           on r.sk1_item_no = i.sk1_item_no
                    join     dim_calendar c       on r.tran_date   = c.calendar_date
--                    where    r.last_updated_date = g_date  
                    where   r.last_updated_date = G_DATE
                    --r.last_updated_date between '29 jun 15' and '09 aug 15'
--                    and r.tran_date > '22/OCT/00'     ---REMOVE QST 17/oct/13
                    and c.calendar_date <> '18 may 04'
--                    and i.sk1_style_colour_no <> 23936026 --> partition failure
                    group by r.sk1_location_no, i.sk1_style_colour_no, c.fin_year_no, c.fin_week_no)
--                    union
--                    select   sk1_location_no, sk1_style_colour_no, fin_year_no, fin_week_no
--                    from     temp_po_deletes
--                    group by sk1_location_no, sk1_style_colour_no, fin_year_no, fin_week_no)
   select   lid.sk1_location_no,
            di.sk1_style_colour_no,
            dc.fin_year_no,
            dc.fin_week_no,
            max(dc.fin_week_code) fin_week_code,
            max(dc.this_week_start_date) this_week_start_date,
            max(lid.sk2_location_no) sk2_location_no,
            sum(lid.actl_grn_qty) actl_grn_qty,
            sum(lid.actl_grn_selling) actl_grn_selling,
            sum(lid.actl_grn_cost) actl_grn_cost,
            sum(lid.fillrate_order_qty) fillrate_order_qty,
            sum(lid.fillrate_order_selling) fillrate_order_selling,
            sum(lid.fillrate_order_excl_wh_qty) fillrate_order_excl_wh_qty,
            sum(lid.fillrate_order_excl_wh_selling) fillrate_order_excl_wh_selling,
            sum(lid.fillrate_actl_grn_excl_wh_qty) fillrate_actl_grn_excl_wh_qty,
            sum(lid.fillrte_actl_grn_excl_wh_sell) fillrte_actl_grn_excl_wh_sell,
            g_date last_updated_date,
            sum(lid.latest_po_qty) latest_po_qty,
            sum(lid.latest_po_selling) latest_po_selling,
            sum(lid.latest_po_cost) latest_po_cost,
            sum(lid.fillrate_actl_grn_qty) fillrate_actl_grn_qty,
            sum(lid.fillrate_actl_grn_selling) fillrate_actl_grn_selling
   from     rtl_loc_item_dy_po_supchain lid
   join     dim_item di                       on  lid.sk1_item_no         = di.sk1_item_no
   join     dim_calendar dc                   on  lid.tran_date           = dc.calendar_date
   join     chgrecs cr                        on  lid.sk1_location_no     = cr.sk1_location_no
                                              and di.sk1_style_colour_no  = cr.sk1_style_colour_no
                                              and dc.fin_year_no          = cr.fin_year_no
                                              and dc.fin_week_no          = cr.fin_week_no
   group by lid.sk1_location_no,
            di.sk1_style_colour_no,
            dc.fin_year_no,
            dc.fin_week_no;

-- Input record declared as cursor%rowtype
g_rec_in             c_rtl_loc_sc_wk_po_supchain%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_rtl_loc_sc_wk_po_supchain%rowtype;
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
      insert into rtl_loc_sc_wk_po_supchain values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_insert(g_error_index).fin_year_no||
                       ' '||a_tbl_insert(g_error_index).fin_week_no;
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
      update rtl_loc_sc_wk_po_supchain
--      set    row                 = a_tbl_update(i)
      set    latest_po_selling	              = a_tbl_update(i).latest_po_selling,              
             actl_grn_selling		              = a_tbl_update(i).actl_grn_selling,
             fillrate_order_selling           = a_tbl_update(i).fillrate_order_selling,
             fillrate_order_excl_wh_selling   = a_tbl_update(i).fillrate_order_excl_wh_selling,
             fillrte_actl_grn_excl_wh_sell    = a_tbl_update(i).fillrte_actl_grn_excl_wh_sell,              
             fillrate_actl_grn_selling        = a_tbl_update(i).fillrate_actl_grn_selling   ,
                    LAST_UPDATED_DATE   = a_tbl_update(i).LAST_UPDATED_DATE
      where  sk1_location_no                  = a_tbl_update(i).sk1_location_no
      and    sk1_style_colour_no              = a_tbl_update(i).sk1_style_colour_no
      and    fin_year_no                      = a_tbl_update(i).fin_year_no
      and    fin_week_no                      = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_update(g_error_index).fin_year_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no;
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
   from   rtl_loc_sc_wk_po_supchain
   where  sk1_location_no     = g_rec_out.sk1_location_no
   and    sk1_style_colour_no = g_rec_out.sk1_style_colour_no
   and    fin_year_no         = g_rec_out.fin_year_no
   and    fin_week_no         = g_rec_out.fin_week_no;

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
-- Records on rtl_po_supchain_loc_item_dy will be deleted when the not_before_date of a PO changes.
-- Records on rtl_loc_sc_wk_po_supchain are a summary of rtl_loc_item_dy_po_supchain per 
-- sk1_location_no, sk1_style_colour_no, fin_year_no, fin_week_no.
-- Therefore, when the not_before_date of a PO changes, it may move from one fin_year_no/fin_week_no to another fin_year_no/fin_week_no.
-- Records on rtl_loc_sc_wk_po_supchain which belongs to the sk1_location_no, sk1_style_colour_no, fin_year_no, fin_week_no
-- combination of any of the records which have been deleted from rtl_po_supchain_loc_item_dy,
-- have to be re-summarised, if the sk1_location_no, sk1_style_colour_no, fin_year_no, fin_week_no combination
-- still exists on rtl_po_supchain_loc_item_dy.
-- Therefore, the summarised records are first deleted from rtl_loc_sc_wk_po_supchain.
--**************************************************************************************************
procedure delete_recs as
begin

    delete from rtl_loc_sc_wk_po_supchain r
    where  exists(select d.sk1_location_no, d.sk1_style_colour_no, d.fin_year_no, d.fin_week_no 
                  from   temp_po_deletes d
                  where  d.sk1_location_no     = r.sk1_location_no
                  and    d.sk1_style_colour_no = r.sk1_style_colour_no
                  and    d.fin_year_no         = r.fin_year_no
                  and    d.fin_week_no         = r.fin_week_no);

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
    l_text := 'ROLLUP OF rtl_loc_sc_wk_po_supchain EX LOC ITEM DY LEVEL STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    G_DATE := G_DATE+100;
      l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
--    delete_recs;

    open c_rtl_loc_sc_wk_po_supchain;
    fetch c_rtl_loc_sc_wk_po_supchain bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_rtl_loc_sc_wk_po_supchain bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_sc_wk_po_supchain;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
--      local_bulk_insert;
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

end wh_prf_corp_665U_FIXWL;
