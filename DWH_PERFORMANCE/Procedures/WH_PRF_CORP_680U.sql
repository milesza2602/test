--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_680U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_680U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Feb 2009
--  Author:      M Munnik
--  Purpose:     Load Foods Waste and Availability Toolbox fact table in the performance layer.
--  Tables:      Input  - rtl_loc_item_dy_rms_stock
--                        rtl_loc_item_dy_rdf_fcst
--                        rtl_loc_item_dy_rdf_sale
--                        rtl_loc_item_dy_rms_alloc
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
g_start_date         date;
g_end_date           date;
g_rec_out            rtl_loc_item_dy_waste_avl_tbox%rowtype;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_680U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD FOODS WASTE and AVAILABILITY TOOLBOX';
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
   select rmss.sk1_location_no, rmss.sk1_item_no, rmss.post_date, rmss.sk2_location_no, rmss.sk2_item_no,
          (case when rmss.soh_qty < 0 then 1 else 0 end) a01_negative_soh_ind,
          (case when nvl(rdfs.corr_sales_qty,0) > 0 and nvl(rdff.sales_dly_app_fcst_qty,0) > 0 then
             (case when (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) >= 105
               and (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) < 120
                 then 1 else 0 end)
           else 0 end) a02_low_over_fcst_ind,
          (case when nvl(rdfs.corr_sales_qty,0) > 0 and nvl(rdff.sales_dly_app_fcst_qty,0) > 0 then
             (case when (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) >= 120
                 and (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) < 151
              then 1 else 0 end)
           else 0 end) a03_medium_over_fcst_ind,
          (case when nvl(rdfs.corr_sales_qty,0) > 0 and nvl(rdff.sales_dly_app_fcst_qty,0) > 0 then
             (case when (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) > 150
              then 1 else 0 end)
           else 0 end) a04_high_over_fcst_ind,
          (case when nvl(rdfs.corr_sales_qty,0) > 0 and nvl(rdff.sales_dly_app_fcst_qty,0) > 0 then
             (case when (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) >= 82
               and (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) < 95
                 then 1 else 0 end)
           else 0 end) a05_low_under_fcst_ind,
          (case when nvl(rdfs.corr_sales_qty,0) > 0 and nvl(rdff.sales_dly_app_fcst_qty,0) > 0 then
             (case when (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) >= 65
               and (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) < 82
                 then 1 else 0 end)
           else 0 end) a06_medium_under_fcst_ind,
          (case when nvl(rdfs.corr_sales_qty,0) > 0 and nvl(rdff.sales_dly_app_fcst_qty,0) > 0 then
             (case when (((nvl(rdff.sales_dly_app_fcst_qty,0))/(nvl(rdfs.corr_sales_qty,0)))*100) < 65
                 then 1 else 0 end)
           else 0 end) a07_high_under_fcst_ind,
          (case when nvl(rmsa.fd_alloc_qty,0) > nvl(rmsa.fd_sdn_qty,0)
            and nvl(rmsa.fd_sdn_qty,0) = nvl(rmsa.fd_apportion_qty,0)
              then 1 else 0 end) a10_under_del_scalling_ind,
          (case when nvl(rmsa.fd_alloc_qty,0) = nvl(rmsa.fd_apportion_qty,0)
            and nvl(rmsa.fd_apportion_qty,0) > nvl(rmsa.fd_sdn_qty,0)
              then 1 else 0 end) a11_under_del_picking_err_ind,
          (case when nvl(rmsa.fd_alloc_qty,0) = nvl(rmsa.fd_apportion_qty,0)
            and nvl(rmsa.fd_apportion_qty,0) < nvl(rmsa.fd_sdn_qty,0)
              then 1 else 0 end) a12_over_del_picking_err_ind,
          (case when nvl(rmsa.fd_alloc_qty,0) < nvl(rmsa.fd_sdn_qty,0)
            and nvl(rmsa.fd_sdn_qty,0) = nvl(rmsa.fd_apportion_qty,0)
              then 1 else 0 end) a13_over_scaling_ind,
           trunc(sysdate) last_updated_date
   from   (select r.sk1_location_no, r.sk1_item_no, r.post_date, c.this_week_end_date, r.sk2_location_no, r.sk2_item_no, soh_qty
           from   rtl_loc_item_dy_rms_stock r join dim_calendar c on r.post_date = c.calendar_date
           where  r.post_date between g_start_date and g_end_date) rmss
   join    dim_item i
   on      rmss.sk1_item_no = i.sk1_item_no
   left join (select r.sk1_location_no, r.sk1_item_no, c.this_week_end_date, sum(r.sales_dly_app_fcst_qty) sales_dly_app_fcst_qty
              --from rtl_loc_item_dy_rdf_fcst r join dim_calendar c on r.post_date = c.calendar_date      --RDF L1/L2 remapping change
              from RTL_LOC_ITEM_RDF_DYFCST_L2 r join dim_calendar c on r.post_date = c.calendar_date
              where r.post_date between g_start_date and g_end_date
              group by r.sk1_location_no, r.sk1_item_no, c.this_week_end_date) rdff
   on         rmss.sk1_location_no    = rdff.sk1_location_no
   and        rmss.sk1_item_no        = rdff.sk1_item_no
   and        rmss.this_week_end_date = rdff.this_week_end_date
   left join (select r.sk1_location_no, r.sk1_item_no, c.this_week_end_date, sum(r.corr_sales_qty) corr_sales_qty
              from rtl_loc_item_dy_rdf_sale r join dim_calendar c on r.post_date = c.calendar_date
              where r.post_date between g_start_date and g_end_date
              group by r.sk1_location_no, r.sk1_item_no, c.this_week_end_date) rdfs
   on         rmss.sk1_location_no    = rdfs.sk1_location_no
   and        rmss.sk1_item_no        = rdfs.sk1_item_no
   and        rmss.this_week_end_date = rdfs.this_week_end_date
   left join (select sk1_location_no, sk1_item_no, calendar_date,
                     fd_sdn_qty, fd_alloc_qty, fd_apportion_qty
              from   rtl_loc_item_dy_rms_alloc
              where  calendar_date between g_start_date and g_end_date) rmsa
   on         rmss.sk1_location_no = rmsa.sk1_location_no
   and        rmss.sk1_item_no     = rmsa.sk1_item_no
   and        rmss.post_date       = rmsa.calendar_date
   where      i.business_unit_no   = 50
   and        i.fd_discipline_type in('PA', 'PC');

g_rec_in             c_rtl_loc_item_dy_waste_avail%rowtype;
-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_dy_waste_avail%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                   := g_rec_in;
   g_rec_out.last_updated_date := g_date;

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
       set    a01_negative_soh_ind             = a_tbl_update(i).a01_negative_soh_ind,
              a02_low_over_fcst_ind            = a_tbl_update(i).a02_low_over_fcst_ind,
              a03_medium_over_fcst_ind         = a_tbl_update(i).a03_medium_over_fcst_ind,
              a04_high_over_fcst_ind           = a_tbl_update(i).a04_high_over_fcst_ind,
              a05_low_under_fcst_ind           = a_tbl_update(i).a05_low_under_fcst_ind,
              a06_medium_under_fcst_ind        = a_tbl_update(i).a06_medium_under_fcst_ind,
              a07_high_under_fcst_ind          = a_tbl_update(i).a07_high_under_fcst_ind,
              a10_under_del_scaling_ind        = a_tbl_update(i).a10_under_del_scaling_ind,
              a11_under_del_picking_err_ind    = a_tbl_update(i).a11_under_del_picking_err_ind,
              a12_over_del_picking_err_ind     = a_tbl_update(i).a12_over_del_picking_err_ind,
              a13_over_del_scaling_ind         = a_tbl_update(i).a13_over_del_scaling_ind,
              last_updated_date                = a_tbl_update(i).last_updated_date
       where  sk1_location_no                  = a_tbl_update(i).sk1_location_no
       and    sk1_item_no                      = a_tbl_update(i).sk1_item_no
       and    calendar_date                    = a_tbl_update(i).calendar_date;

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
   where  sk1_location_no    = g_rec_out.sk1_location_no
   and    sk1_item_no        = g_rec_out.sk1_item_no
   and    calendar_date      = g_rec_out.calendar_date;

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
    l_text := 'LOAD rtl_loc_item_dy_waste_avl_tbox STARTED '||
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

    if trim(to_char(g_date,'day')) = 'sunday' then
       select this_week_start_date - 7, this_week_end_date
       into   g_start_date, g_end_date
       from   dim_calendar
       where  calendar_date = g_date;
    else
       select this_week_start_date - 14, this_week_start_date - 1
       into   g_start_date, g_end_date
       from   dim_calendar
       where  calendar_date = g_date;
    end if;

    l_text := 'UPDATE PERIOD - '||g_start_date||' to '||g_end_date;
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
         if g_recs_read mod 500000 = 0 then
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

end wh_prf_corp_680u;
