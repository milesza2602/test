--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_190U_LOAD2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_190U_LOAD2" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        Feb 2009
--  Author:      Alastair de Wet
--  Purpose:     Create RMS Food Catalog fact table in the performance layer
--               with input ex RMS Food loc/item table from foundation layer.
--  Tables:      Input  - rtl_location_item
--               Output - rtl_loc_item_dy_catalog
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
-- 29 May 2009 - defect636    - Measures with a data type of text are causing issues in SSAS
--                            - further change - removed redundant coding in update section
-- 10 June 2009 - defect636   - remove convert for product_class and product_status_code
--                              as being done for DIM_ITEM already
-- 13 June 2009 - defect 1797 - Mod to RTL_LOC_ITEM_DY_CATALOG due to
--                              performance constraint - linked to QC636
-- 08 June 2011 - defect 2981 - New load measure min_shelf_life
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
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_190U_LOAD2';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE FOOD CATALOG EX FOUNDATION';
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



cursor c_fnd_location_item is
   select di.sk1_item_no,
          dl.sk1_location_no,
          dih.sk2_item_no,
          dlh.sk2_location_no,
          fnd_li.this_wk_deliv_pattern_code as wk_delivery_pattern,
          nvl(fnd_li.num_units_per_tray,1) num_units_per_tray,
          nvl(fnd_li.this_wk_catalog_ind,0) this_wk_catalog_ind,
          nvl(fnd_li.next_wk_catalog_ind,0) next_wk_catalog_ind,
          nvl(fnd_li.num_shelf_life_days,0) num_shelf_life_days,
          nvl(fnd_li.product_status_code,0) product_status_code,
          nvl(fnd_li.product_status_1_code,0) product_status_1_code,
          nvl(fnd_li.min_shelf_life,0) min_shelf_life,
          di.product_class,
          di.handling_method_code,
          dl.st_open_date,
          dl.st_close_date
   from   rtl_location_item fnd_li,
          dim_item di,
          dim_location dl,
          dim_item_hist dih,
          dim_location_hist dlh
   where
          fnd_li.this_wk_catalog_ind     = 1      and
          di.business_unit_no            = 50     and
--          dl.loc_type                    = 'S'    and
--          dl.active_store_ind            = 1      and
          fnd_li.sk1_item_no             = di.sk1_item_no and
          fnd_li.sk1_location_no         = dl.sk1_location_no and
          di.item_no                     = dih.item_no and
          g_date                     between dih.sk2_active_from_date and dih.sk2_active_to_date and
          dl.location_no                 = dlh.location_no and
          g_date                     between dlh.sk2_active_from_date and dlh.sk2_active_to_date ;


g_rec_in                   c_fnd_location_item%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_location_item%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin


   g_rec_out.com_flag_adj_ind                := 2;
   g_rec_out.boh_adj_qty                     := 0;
   g_rec_out.boh_adj_selling                 := 0;
   g_rec_out.boh_adj_cost                    := 0;
   g_rec_out.soh_adj_qty                     := 0;
   g_rec_out.soh_adj_selling                 := 0;
   g_rec_out.soh_adj_cost                    := 0;
   g_rec_out.fd_num_catlg_days               := 1;
   g_rec_out.fd_num_avail_days               := 0;
   g_rec_out.fd_num_catlg_days_adj           := 0;
   g_rec_out.fd_num_avail_days_adj           := 0;
   g_rec_out.fd_sod_num_avail_days           := 0;



   g_rec_out.product_status_code             := g_rec_in.product_status_code ;
   g_rec_out.product_status_1_code           := g_rec_in.product_status_1_code;

   g_rec_out.product_class                   := g_rec_in.product_class ;
-- Remove convert for product_class and product_status_code
--  as being done for DIM_ITEM already
--  Case g_rec_in.product_class
--        when '1' then g_rec_out.product_class := 1 ;
--        when '2'  then g_rec_out.product_class := 2;
--        when '3'  then g_rec_out.product_class := 3;
--        when '4' then g_rec_out.product_class := 4;
--        when '5' then g_rec_out.product_class := 5;
--        else g_rec_out.product_class := 0;
--   end case;

    g_rec_out.wk_delivery_pattern        := g_rec_in.wk_delivery_pattern ;
--
-- Following code for WK_DELIVERY_PATTERN(QC636) has been commented out(qc1797)
-- until we are in a position to change the dat structure and do the translation
--
--   if g_rec_in.wk_delivery_pattern is null
--   or g_rec_in.wk_delivery_pattern = ' '
--   then
--    g_rec_out.wk_delivery_pattern := 2;
--   else
--    g_rec_out.wk_delivery_pattern := to_number(translate(g_rec_in.wk_delivery_pattern,'YNO0', '1222'));
--    end if;

   g_rec_out.this_wk_catalog_ind             := g_rec_in.this_wk_catalog_ind;
   g_rec_out.next_wk_catalog_ind             := g_rec_in.next_wk_catalog_ind;
   g_rec_out.num_units_per_tray              := g_rec_in.num_units_per_tray;

   g_rec_out.num_shelf_life_days             := g_rec_in.num_shelf_life_days;
   g_rec_out.min_shelf_life                  := g_rec_in.min_shelf_life;


   g_rec_out.calendar_date                   := g_date;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;

   if g_rec_in.st_close_date is not null then
      if g_rec_in.st_close_date < g_date then
         g_rec_out.fd_num_catlg_days               := 0;
      end if;
   end if;

   if g_rec_in.st_open_date is not null then
      if g_rec_in.handling_method_code  = 'P'  and
         g_rec_in.st_open_date > g_date then
         g_rec_out.fd_num_catlg_days               := 0;
      end if;
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

--   g_rec_out.product_class                   := g_rec_in.product_class ;
--   g_rec_out.wk_delivery_pattern            := g_rec_in.wk_delivery_pattern ;

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update rtl_loc_item_dy_catalog
       set    com_flag_adj_ind                = a_tbl_update(i).com_flag_adj_ind,
              boh_adj_qty                     = a_tbl_update(i).boh_adj_qty,
              boh_adj_selling                 = a_tbl_update(i).boh_adj_selling,
              boh_adj_cost                    = a_tbl_update(i).boh_adj_cost,
              soh_adj_qty                     = a_tbl_update(i).soh_adj_qty,
              soh_adj_selling                 = a_tbl_update(i).soh_adj_selling,
              soh_adj_cost                    = a_tbl_update(i).soh_adj_cost,
              fd_num_catlg_days               = a_tbl_update(i).fd_num_catlg_days,
              fd_num_avail_days               = a_tbl_update(i).fd_num_avail_days,
              fd_num_catlg_days_adj           = a_tbl_update(i).fd_num_catlg_days_adj,
              fd_num_avail_days_adj           = a_tbl_update(i).fd_num_avail_days_adj,
              fd_sod_num_avail_days           = a_tbl_update(i).fd_sod_num_avail_days,
              product_status_code             = a_tbl_update(i).product_status_code,
              product_class                   = a_tbl_update(i).product_class,
              wk_delivery_pattern             = a_tbl_update(i).wk_delivery_pattern,
              this_wk_catalog_ind             = a_tbl_update(i).this_wk_catalog_ind,
              next_wk_catalog_ind             = a_tbl_update(i).next_wk_catalog_ind,
              num_units_per_tray              = a_tbl_update(i).num_units_per_tray,
              product_status_1_code           = a_tbl_update(i).product_status_1_code,
              num_shelf_life_days             = a_tbl_update(i).num_shelf_life_days,
              min_shelf_life                  = a_tbl_update(i).min_shelf_life,
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
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_loc_item_dy_catalog
   where  sk1_location_no    = g_rec_out.sk1_location_no  and
          sk1_item_no        = g_rec_out.sk1_item_no      and
          calendar_date      = g_rec_out.calendar_date;

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

    l_text := 'LOAD OF rtl_loc_item_dy_catalog  FOODS EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    g_date := g_date - 3;   --XXREMOVE
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    g_date := 'Moo';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_location_item;
    fetch c_fnd_location_item bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_location_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_location_item;
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
end wh_prf_corp_190u_load2;
