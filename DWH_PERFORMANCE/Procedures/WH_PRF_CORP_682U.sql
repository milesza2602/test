--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_682U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_682U"                                                 (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Feb 2009
--  Author:      M Munnik
--  Purpose:     Load Foods Waste and Availability Toolbox fact table from RMS Allocations.
--               There are three procedures to load Foods Waste and Avail Toolbox : 
--               681 - from rtl_loc_item_dy_rms_stock
--               682 - from rtl_loc_item_dy_rms_alloc
--               683 - from rtl_loc_item_dy_rdf_fcst and rtl_loc_item_dy_rdf_sale
--  Tables:      Input  - rtl_loc_item_dy_rms_alloc
--               Output - rtl_loc_item_dy_waste_avl_tbox
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
g_found              boolean;
g_date               date;
g_rec_out            rtl_loc_item_dy_waste_avl_tbox%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_682U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD FOODS WASTE and AVAILABILITY TOOLBOX from ALLOCATIONS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_waste_avl_tbox%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_waste_avl_tbox%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_rtl_loc_item_dy_waste_avail is
   with fd_items as
  (select sk1_item_no
   from   dim_item
   where  business_unit_no   = 50
   and    fd_discipline_type in('PA', 'PC'))

   select r.sk1_location_no, r.sk1_item_no, r.calendar_date, r.sk2_location_no, r.sk2_item_no,
          (case when nvl(r.fd_alloc_qty,0) > nvl(r.fd_sdn_qty,0)
            and nvl(r.fd_sdn_qty,0) = nvl(r.fd_apportion_qty,0)
              then 1 else 0 end) a10_under_del_scaling_ind,
          (case when nvl(r.fd_alloc_qty,0) = nvl(r.fd_apportion_qty,0)
            and nvl(r.fd_apportion_qty,0) > nvl(r.fd_sdn_qty,0)
              then 1 else 0 end) a11_under_del_picking_err_ind,
          (case when nvl(r.fd_alloc_qty,0) = nvl(r.fd_apportion_qty,0)
            and nvl(r.fd_apportion_qty,0) < nvl(r.fd_sdn_qty,0)
              then 1 else 0 end) a12_over_del_picking_err_ind,
          (case when nvl(r.fd_alloc_qty,0) < nvl(r.fd_sdn_qty,0)
            and nvl(r.fd_sdn_qty,0) = nvl(r.fd_apportion_qty,0)
              then 1 else 0 end) a13_over_del_scaling_ind
   from   rtl_loc_item_dy_rms_alloc r
   join   fd_items i on r.sk1_item_no = i.sk1_item_no
   where  r.last_updated_date = g_date;

g_rec_in             c_rtl_loc_item_dy_waste_avail%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_dy_waste_avail%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_location_no                := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                    := g_rec_in.sk1_item_no;
   g_rec_out.calendar_date                  := g_rec_in.calendar_date;
   g_rec_out.sk2_location_no                := g_rec_in.sk2_location_no;
   g_rec_out.sk2_item_no                    := g_rec_in.sk2_item_no;
   g_rec_out.a01_negative_soh_ind           := 0;
   g_rec_out.a02_low_over_fcst_ind          := 0;
   g_rec_out.a03_medium_over_fcst_ind       := 0;
   g_rec_out.a04_high_over_fcst_ind         := 0;
   g_rec_out.a05_low_under_fcst_ind         := 0;
   g_rec_out.a06_medium_under_fcst_ind      := 0;
   g_rec_out.a07_high_under_fcst_ind        := 0;
   g_rec_out.a10_under_del_scaling_ind      := g_rec_in.a10_under_del_scaling_ind;
   g_rec_out.a11_under_del_picking_err_ind  := g_rec_in.a11_under_del_picking_err_ind;
   g_rec_out.a12_over_del_picking_err_ind   := g_rec_in.a12_over_del_picking_err_ind;
   g_rec_out.a13_over_del_scaling_ind       := g_rec_in.a13_over_del_scaling_ind;
   g_rec_out.last_updated_date              := g_date;

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
       insert into rtl_loc_item_dy_waste_avl_tbox values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).calendar_date;
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
       update rtl_loc_item_dy_waste_avl_tbox
       set    sk2_location_no                  = a_tbl_update(i).sk2_location_no,
              sk2_item_no                      = a_tbl_update(i).sk2_item_no,
              a10_under_del_scaling_ind        = a_tbl_update(i).a10_under_del_scaling_ind,
              a11_under_del_picking_err_ind    = a_tbl_update(i).a11_under_del_picking_err_ind,
              a12_over_del_picking_err_ind     = a_tbl_update(i).a12_over_del_picking_err_ind,
              a13_over_del_scaling_ind         = a_tbl_update(i).a13_over_del_scaling_ind,
              last_updated_date                = a_tbl_update(i).last_updated_date
       where  calendar_date                    = a_tbl_update(i).calendar_date
       and    sk1_item_no                      = a_tbl_update(i).sk1_item_no
       and    sk1_location_no                  = a_tbl_update(i).sk1_location_no;

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
                       ' '||a_tbl_update(g_error_index).calendar_date;
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
   from   rtl_loc_item_dy_waste_avl_tbox
   where  calendar_date      = g_rec_out.calendar_date
   and    sk1_item_no        = g_rec_out.sk1_item_no
   and    sk1_location_no    = g_rec_out.sk1_location_no;

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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD RTL_LOC_ITEM_DY_WASTE_AVL_TBOX(ALLOCATIONS) STARTED '||
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
    open c_rtl_loc_item_dy_waste_avail;
    fetch c_rtl_loc_item_dy_waste_avail bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_dy_waste_avail bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_dy_waste_avail;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
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

end wh_prf_corp_682u;
