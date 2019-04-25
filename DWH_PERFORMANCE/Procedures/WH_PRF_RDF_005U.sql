--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_005U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_005U" (p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as

--**************************************************************************************************
--  Date:        October 2008
--  Author:      Alfonso Joshua
--  Purpose:     Create the swing weekly forecast temp table in the performance layer
--               with input ex RDF Sale table from foundation layer.
--  Tables:      Input  - temp_loc_item_wk_rdf_sysfcst
--                      - temp_loc_item_wk_rdf_appfcst -
--               Output - rtl_loc_item_wk_rdf_fcst
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_item_wk_rdf_fcst%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;


g_debtors_commission_perc rtl_loc_dept_dy.debtors_commission_perc%type   := 0;
g_wac                     rtl_loc_item_dy_rms_price.wac%type             := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_005U_'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RDF WEEKLY FCST FACTS EX TEMP TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_wk_rdf_fcst%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_wk_rdf_fcst%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

g_from_loc_no       number  := 0;
g_to_loc_no         number  := 0;

cursor c_tmp_rtl_loc_item_wk_wkfcst is
 select /*+ FULL(sys_ilv) */
          sys_ilv.location_no,
          sys_ilv.item_no,
          sys_ilv.fin_year_no,
          sys_ilv.fin_week_no,
          sys_ilv.sales_wkly_sys_fcst_qty,
          sys_ilv.sales_wkly_app_fcst_qty,
          di.sk1_item_no,
          dl.sk1_location_no,
          dih.sk2_item_no,
          dlh.sk2_location_no,
--          di.base_rsp_excl_vat,
--          di.vat_rate_perc,
          dc.this_week_start_date,
          zi.reg_rsp,
          di.standard_uom_code,
          di.random_mass_ind,
          di.static_mass
  from    temp_loc_item_wk_rdf_sysfcst sys_ilv /*
  join    temp_loc_item_wk_rdf_appfcst app_ilv on
          sys_ilv.location_no = app_ilv.location_no and
          sys_ilv.item_no     = app_ilv.item_no and
          sys_ilv.fin_year_no = app_ilv.fin_year_no and
          sys_ilv.fin_week_no = app_ilv.fin_week_no */
  join    dim_item di on
          sys_ilv.item_no     = di.item_no
  join    dim_location dl on
          sys_ilv.location_no = dl.location_no
  join    dim_calendar dc on
          sys_ilv.fin_year_no = dc.fin_year_no and
          sys_ilv.fin_week_no = dc.fin_week_no and
          dc.fin_day_no       = 4
  join    dim_location_hist dlh on
          sys_ilv.location_no = dlh.location_no and
          dc.calendar_date between dlh.sk2_active_from_date and dlh.sk2_active_to_date
  join    dim_item_hist dih on
          sys_ilv.item_no     = dih.item_no and
          dc.calendar_date between dih.sk2_active_from_date and dih.sk2_active_to_date
  join    fnd_zone_item zi on
          sys_ilv.location_no = dl.location_no and
          dl.wh_fd_zone_group_no = zi.zone_group_no and
          dl.wh_fd_zone_no    = zi.zone_no and
          sys_ilv.item_no     = zi.item_no
  --where   sys_ilv.location_no between p_from_loc_no and p_to_loc_no               --XX
  where   sys_ilv.location_no between g_from_loc_no and g_to_loc_no                 --XX
  order by sys_ilv.location_no, sys_ilv.item_no, sys_ilv.fin_year_no, sys_ilv.fin_week_no;

g_rec_in                   c_tmp_rtl_loc_item_wk_wkfcst%rowtype;
-- For input bulk collect --
type stg_array is table of c_tmp_rtl_loc_item_wk_wkfcst%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.fin_week_code                   := ('W'||g_rec_in.fin_year_no||g_rec_in.fin_week_no);
   g_rec_out.sales_wk_sys_fcst_qty           := g_rec_in.sales_wkly_sys_fcst_qty;
   g_rec_out.sales_wk_app_fcst_qty           := g_rec_in.sales_wkly_app_fcst_qty; /*
   g_rec_out.sales_wk_sys_fcst               := (((g_rec_in.sales_wkly_sys_fcst_qty * g_rec_in.base_rsp_excl_vat)
                                                       * 100 / (100 + g_rec_in.vat_rate_perc)) + 0.05);
   g_rec_out.sales_wk_app_fcst               := (((g_rec_in.sales_wkly_app_fcst_qty * g_rec_in.base_rsp_excl_vat)
                                                       * 100 / (100 + g_rec_in.vat_rate_perc)) + 0.05);  */

   if g_rec_in.standard_uom_code = 'EA' and
      g_rec_in.random_mass_ind   = 1 then
      g_rec_out.sales_wk_sys_fcst            := g_rec_in.sales_wkly_sys_fcst_qty * g_rec_in.reg_rsp * g_rec_in.static_mass;
      g_rec_out.sales_wk_app_fcst            := g_rec_in.sales_wkly_app_fcst_qty * g_rec_in.reg_rsp * g_rec_in.static_mass;
   else
      g_rec_out.sales_wk_sys_fcst            := g_rec_in.sales_wkly_sys_fcst_qty * g_rec_in.reg_rsp;
      g_rec_out.sales_wk_app_fcst            := g_rec_in.sales_wkly_app_fcst_qty * g_rec_in.reg_rsp;
   end if;

   g_rec_out.this_week_start_date            := g_rec_in.this_week_start_date;
   g_rec_out.last_updated_date               := g_date;

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
       insert into rtl_loc_item_wk_rdf_fcst values a_tbl_insert(i);

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
       update rtl_loc_item_wk_rdf_fcst
       set    sales_wk_sys_fcst_qty      = a_tbl_update(i).sales_wk_sys_fcst_qty,
              sales_wk_sys_fcst          = a_tbl_update(i).sales_wk_sys_fcst,
              sales_wk_app_fcst_qty      = a_tbl_update(i).sales_wk_app_fcst_qty,
              sales_wk_app_fcst          = a_tbl_update(i).sales_wk_app_fcst,
              sk2_item_no                = a_tbl_update(i).sk2_item_no,
              sk2_location_no            = a_tbl_update(i).sk2_location_no,
              fin_week_code              = a_tbl_update(i).fin_week_code,
              this_week_start_date       = a_tbl_update(i).this_week_start_date,
              last_updated_date          = a_tbl_update(i).last_updated_date
       where  sk1_location_no            = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                = a_tbl_update(i).sk1_item_no      and
              fin_year_no                = a_tbl_update(i).fin_year_no      and
              fin_week_no                = a_tbl_update(i).fin_week_no;

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
                       ' '||a_tbl_update(g_error_index).fin_year_no||
                       ' '||a_tbl_update(g_error_index).fin_week_no;
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
   from   rtl_loc_item_wk_rdf_fcst
   where  sk1_location_no    = g_rec_out.sk1_location_no  and
          sk1_item_no        = g_rec_out.sk1_item_no      and
          fin_year_no        = g_rec_out.fin_year_no      and
          fin_week_no        = g_rec_out.fin_week_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).sk1_location_no = g_rec_out.sk1_location_no and
            a_tbl_insert(i).sk1_item_no     = g_rec_out.sk1_item_no and
            a_tbl_insert(i).fin_year_no     = g_rec_out.fin_year_no and
            a_tbl_insert(i).fin_week_no     = g_rec_out.fin_week_no
             then
            g_found := TRUE;
         end if;
      end loop;
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

    l_text := 'LOAD OF RTL_LOC_ITEM_WK_RDF_FCST EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;      --XX
    --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);          --XX
    
    if p_from_loc_no = 0 then                 --XX start
       g_from_loc_no := 0;
       g_to_loc_no   := 530;
    end if;
    
    if p_from_loc_no  = 351 then
       g_from_loc_no := 531;
       g_to_loc_no   := 1200;
    end if;
    
    if p_from_loc_no  = 491 then
       g_from_loc_no := 1201;
       g_to_loc_no   := 99999;
    end if;                                   --XX end

    l_text := 'LOCATION RANGE BEING PROCESSED - '||g_from_loc_no||' to '||g_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_tmp_rtl_loc_item_wk_wkfcst;
    fetch c_tmp_rtl_loc_item_wk_wkfcst bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 500000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_tmp_rtl_loc_item_wk_wkfcst bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_tmp_rtl_loc_item_wk_wkfcst;
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
end wh_prf_rdf_005u;
