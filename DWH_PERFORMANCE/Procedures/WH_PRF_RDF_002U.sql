--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_002U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_002U" 
                                                                                                                                                                                                                                                                                                (p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as
--**************************************************************************************************
--  Date:        August 2010
--  Author:      Alfonso Joshua
--  Purpose:     Load daily forecast level 1 table in performance layer
--               with input ex RDF Forecast temp level 1 table from foundation layer.
--  Tables:      Input  - temp_loc_item_dy_rdf_sysfcstl1
--               Output - rtl_loc_item_dy_rdf_fcst_l1
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  16 Feb 2009 - A. Joshua : TD-390  - Include ETL to fcst_err_sls_dly_app_fcst_qty,
--                                                    fcst_err_sales_dly_app_fcst,
--                                                    fcst_err_sls_dly_sys_fcst_qty,
--                                                    fcst_err_sales_dly_sys_fcst_qty
--  29 Apr 2009 - A. Joshua : TD-1490 - Remove lookup to table rtl_loc_item_dy_catalog
--                                    - The fcst_err* measures are now catered for in wh_prf_rdf_001c
--
--  16 May 2011: Remove stats gathering on temp_loc_item_dy_rdf_sysfcstl1 tables main, since this
--               module is runned as a spinner with 3 slaves, all 3 do stats gathering before processing
--               I have removed it to WH_PRF_RDF_002A.
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
g_recs_ignored       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_today_fin_day_no   number        :=  0;
g_fd_num_catlg_days  number        :=  0;
g_sales_dly_app_fcst number(14,2)  :=  0;
g_sales_dly_sys_fcst number(14,2)  :=  0;
g_sales_dly_app_fcst_qty number(14,3) :=  0;
g_sales_dly_sys_fcst_qty number(14,3) :=  0;
g_rec_out            rtl_loc_item_dy_rdf_fcst_l1%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_002U_'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD RDF DAILY FCST LEVEL 1 FACTS EX TEMP TABLES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_rdf_fcst_l1%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_rdf_fcst_l1%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_item_wk_dyfcst is

--  select  /*+ FULL(sys_ilv) FULL(app_ilv) */
  select  /*+ USE_HASH (dih, dlh, di, dl, sys_ilv) */
          sys_ilv.location_no,
          sys_ilv.item_no,
          sys_ilv.post_date,
          sys_ilv.sales_dly_sys_fcst_qty,
          sys_ilv.sales_dly_app_fcst_qty,
          di.sk1_item_no,
          dl.sk1_location_no,
          dih.sk2_item_no,
          dlh.sk2_location_no,
          zi.reg_rsp,
          di.standard_uom_code,
          di.random_mass_ind,
          di.static_mass
  from    temp_loc_item_dy_rdf_sysfcstl1 sys_ilv
  join    dim_item di on
          sys_ilv.item_no     = di.item_no
  join    dim_location dl on
          sys_ilv.location_no = dl.location_no
  join    dim_item_hist dih on
          sys_ilv.item_no     = dih.item_no and
          sys_ilv.post_date between dih.sk2_active_from_date and dih.sk2_active_to_date
  join    dim_location_hist dlh on
          sys_ilv.location_no = dlh.location_no and
          sys_ilv.post_date between dlh.sk2_active_from_date and dlh.sk2_active_to_date
  join    fnd_zone_item zi on
          sys_ilv.location_no = dl.location_no and
          dl.wh_fd_zone_group_no = zi.zone_group_no and
          dl.wh_fd_zone_no    = zi.zone_no and
          sys_ilv.item_no     = zi.item_no
  where   sys_ilv.location_no between p_from_loc_no and p_to_loc_no ;

g_rec_in                   c_fnd_rtl_loc_item_wk_dyfcst%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_wk_dyfcst%rowtype;
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
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sales_dly_sys_fcst_qty          := g_rec_in.sales_dly_sys_fcst_qty;
   g_rec_out.sales_dly_app_fcst_qty          := g_rec_in.sales_dly_app_fcst_qty;

   if g_rec_in.standard_uom_code = 'EA' and
      g_rec_in.random_mass_ind   = 1 then
      g_rec_out.sales_dly_sys_fcst           := g_rec_in.sales_dly_sys_fcst_qty * g_rec_in.reg_rsp * g_rec_in.static_mass;
      g_rec_out.sales_dly_app_fcst           := g_rec_in.sales_dly_app_fcst_qty * g_rec_in.reg_rsp * g_rec_in.static_mass;
   else
      g_rec_out.sales_dly_sys_fcst           := g_rec_in.sales_dly_sys_fcst_qty * g_rec_in.reg_rsp;
      g_rec_out.sales_dly_app_fcst           := g_rec_in.sales_dly_app_fcst_qty * g_rec_in.reg_rsp;
   end if;

   g_rec_out.last_updated_date               := g_date;
--   g_rec_out.last_updated_date               := '07 jun 10'; data take on

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
       insert into rtl_loc_item_dy_rdf_fcst_l1 values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).post_date;
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
       update rtl_loc_item_dy_rdf_fcst_l1
       set    sales_dly_sys_fcst_qty        = a_tbl_update(i).sales_dly_sys_fcst_qty,
              sales_dly_sys_fcst            = a_tbl_update(i).sales_dly_sys_fcst,
              sales_dly_app_fcst_qty        = a_tbl_update(i).sales_dly_app_fcst_qty,
              sales_dly_app_fcst            = a_tbl_update(i).sales_dly_app_fcst,
              last_updated_date             = a_tbl_update(i).last_updated_date
       where  post_date                     = a_tbl_update(i).post_date and
              sk1_item_no                   = a_tbl_update(i).sk1_item_no      and
              sk1_location_no               = a_tbl_update(i).sk1_location_no; /*
             (sales_dly_sys_fcst_qty       <> a_tbl_update(i).sales_dly_sys_fcst_qty or
              sales_dly_sys_fcst           <> a_tbl_update(i).sales_dly_sys_fcst     or
              sales_dly_app_fcst_qty       <> a_tbl_update(i).sales_dly_app_fcst_qty or
              sales_dly_app_fcst           <> a_tbl_update(i).sales_dly_app_fcst); */

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;
--       g_recs_updated  := g_recs_updated  + sql%rowcount;

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
                       ' '||a_tbl_update(g_error_index).post_date;
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
   g_count :=0;
-- Check to see if item is present on table and update/insert accordingly
   select count(1), sum(sales_dly_sys_fcst_qty), sum(sales_dly_sys_fcst),
          sum(sales_dly_app_fcst_qty), sum(sales_dly_app_fcst)
   into   g_count, g_sales_dly_sys_fcst_qty, g_sales_dly_sys_fcst, g_sales_dly_app_fcst_qty, g_sales_dly_app_fcst
   from   rtl_loc_item_dy_rdf_fcst_l1
   where  post_date          = g_rec_out.post_date and
          sk1_item_no        = g_rec_out.sk1_item_no and
          sk1_location_no    = g_rec_out.sk1_location_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;
/*
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).sk1_location_no = g_rec_out.sk1_location_no and
             a_tbl_insert(i).sk1_item_no     = g_rec_out.sk1_item_no and
             a_tbl_insert(i).post_date       = g_rec_out.post_date
             then
            g_found := TRUE;
         end if;
      end loop;
   end if;
*/
-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
      a_count                 := a_count + 1;
   else
      if
        (g_sales_dly_sys_fcst_qty <> g_rec_out.sales_dly_sys_fcst_qty or
         g_sales_dly_sys_fcst     <> g_rec_out.sales_dly_sys_fcst or
         g_sales_dly_app_fcst_qty <> g_rec_out.sales_dly_app_fcst_qty or
         g_sales_dly_app_fcst     <> g_rec_out.sales_dly_app_fcst) then
         a_count_u                := a_count_u + 1;
         a_tbl_update(a_count_u)  := g_rec_out;
         a_count                  := a_count + 1;
      else
         g_recs_ignored           := g_recs_ignored + 1;
      end if;
   end if;

--   a_count := a_count + 1;
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
/*
    l_text := 'LOAD OF UPDATE STATS STARTED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'temp_loc_item_dy_rdf_sysfcstl1', DEGREE => 8);
  */
/*
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'temp_loc_item_dy_rdf_appfcst', DEGREE => 8);

    l_text := 'LOAD OF UPDATE STATS ENDED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD RTL_LOC_ITEM_DY_RDF_FCST_L1 EX TEMP TABLES STARTED '||
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
    l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_loc_item_wk_dyfcst;
    fetch c_fnd_rtl_loc_item_wk_dyfcst bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_rtl_loc_item_wk_dyfcst bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_wk_dyfcst;
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
    l_text :=  'RECORDS IGNORED '||g_recs_ignored;
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

end wh_prf_rdf_002u;
