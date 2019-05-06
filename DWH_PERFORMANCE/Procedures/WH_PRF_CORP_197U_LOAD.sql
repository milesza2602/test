--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_197U_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_197U_LOAD" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        May 2009
--  Author:      Alfonso Joshua
--  Purpose:     Update RMS Foods Catalog (DC catlg/avail) fact table in the performance layer
--               with input ex RMS location/item
--  Tables:      Input  - rtl_location_item
--               Output - rtl_loc_item_dy_catalog
--  Packages:    constants, dwh_log, dwh_valid
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_item_dy_catalog%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_197U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE FOOD CATALOG EX RMS DC LOC/ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_catalog%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_catalog%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- This select gets all of the records, based on various filters, to be updated for todays catalog into a list.
-- It then uses the list to get all of yesterdays data that matches the list in order to update todays catalog with yesterdays
-- figures when today was not received (comflag <> 1)

cursor c_rtl_location_item is
   with loc_item as
 (select /*+ full(li)  */ distinct
         li.sk1_item_no,
         di.item_no,
         dl.wh_fd_zone_no,
         di.fd_discipline_type
   from  rtl_location_item li,
         dim_location dl,
         dim_item di
   where li.sk1_location_no              = dl.sk1_location_no and
         li.sk1_item_no                  = di.sk1_item_no and
         li.this_wk_catalog_ind           = 1 and
         di.fd_discipline_type       in ('SA','SF')),

      zone_loc as
 (select dl.sk1_location_no,
         li.sk1_item_no,
         li.item_no,
         uda_value_no_or_text_or_date
  from   dim_location dl,
         loc_item li,
         fnd_item_uda uda
  where  dl.wh_fd_zone_no      = li.wh_fd_zone_no and
         dl.wh_discipline_type = li.fd_discipline_type and
         dl.loc_type           = 'W' and
         dl.stock_holding_ind  = 1 and
         li.item_no            = uda.item_no(+) and
         uda.uda_no            = 542),

      stock as
  (select stk.sk1_item_no,
          stk.sk1_location_no,
          stk.post_date,
          nvl(boh_qty,0) boh_qty
   from   rtl_loc_item_dy_rms_stock stk,
          dim_item di
   where  stk.post_date       = g_date and
          stk.sk1_item_no     = di.sk1_item_no and
          di.business_unit_no = 50)

   select cat.sk1_item_no,
          cat.sk1_location_no,
          cat.calendar_date,
          stk.boh_qty,
          zl.uda_value_no_or_text_or_date,
          nvl(pck.item_no,0) item_no
   from   rtl_loc_item_dy_catalog cat,
          zone_loc zl,
          fnd_pack_item_detail pck,
          stock stk
   where  cat.calendar_date     = stk.post_date and
          cat.sk1_location_no   = stk.sk1_location_no and
          cat.sk1_item_no       = stk.sk1_item_no and
          cat.sk1_item_no       = zl.sk1_item_no and
          cat.sk1_location_no   = zl.sk1_location_no and
          zl.item_no            = pck.item_no(+);

g_rec_in                   c_rtl_location_item%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_location_item%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.calendar_date                   := g_rec_in.calendar_date;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.fd_num_dc_catlg_days            := 1;

   if g_rec_out.fd_num_dc_catlg_days = 1 and
      g_rec_in.boh_qty > 0 then
      g_rec_out.fd_num_dc_avail_days         := 1;
   else
      g_rec_out.fd_num_dc_avail_days         := 0;
   end if;

   if g_rec_out.fd_num_dc_catlg_days = 1 and
      g_rec_in.item_no = 0 and
      g_rec_in.uda_value_no_or_text_or_date = 1 then
      g_rec_out.fd_num_dc_catlg_adj_days     := 1;
   else
      g_rec_out.fd_num_dc_catlg_adj_days     := 0;
   end if;

   if g_rec_out.fd_num_dc_avail_days = 1 and
      g_rec_in.item_no = 0 and
      g_rec_in.uda_value_no_or_text_or_date = 1 then
      g_rec_out.fd_num_dc_avail_adj_days     := 1;
   else
      g_rec_out.fd_num_dc_avail_adj_days     := 0;
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
       insert into rtl_loc_item_dy_catalog values a_tbl_insert(i);

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
       update rtl_loc_item_dy_catalog
       set    fd_num_dc_catlg_days            = a_tbl_update(i).fd_num_dc_catlg_days,
              fd_num_dc_catlg_adj_days        = a_tbl_update(i).fd_num_dc_catlg_adj_days,
              fd_num_dc_avail_days            = a_tbl_update(i).fd_num_dc_avail_days,
              fd_num_dc_avail_adj_days        = a_tbl_update(i).fd_num_dc_avail_adj_days,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no      and
              calendar_date                   = a_tbl_update(i).calendar_date;

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
   g_found := TRUE;

-- Place data into and array for later writing to table in bulk
--   if not g_found then
--      a_count_i               := a_count_i + 1;
--      a_tbl_insert(a_count_i) := g_rec_out;
--   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
--   end if;

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

    l_text := 'LOAD OF rtl_loc_item_dy_catalog FOODS EX LOC/ITEM STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := g_date -1;   --XXREMOVE
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_rtl_location_item;
    fetch c_rtl_location_item bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_rtl_location_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_rtl_location_item;
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
end wh_prf_corp_197u_load;
