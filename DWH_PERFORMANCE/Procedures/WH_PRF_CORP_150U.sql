--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_150U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_150U" 
                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2008
--  Author:      Christie Koorts
--  Purpose:     Create location week fact table in the performance layer with
--               added value ex foundation layer location week staffplanner table.
--  Tables:      Input  -   fnd_rtl_loc_wk_staffplan_fcst
--               Output -   rtl_loc_wk
--  Packages:    constants, dwh_log, dwh_valid
--  Comments:    Single DML could be considered for this program.
--
--  Maintenance:
--  03 Feb 2009 - A Joshua : TD-359  - Retrieve this_week_start_date from fnd_rtl_loc_wk_staffplan
--                                   - Also include filter on last_updated_date
--  25 Jun 2009 - A Joshua : TD-1901 - Remove fte_budget and fte_actual from ETL
--                                    (new module required to load these measures based on last completed work)
--  29 sep 2009 - W Lyttle : TD-2271 - The Num Store Open days is incorrect
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
g_rec_out            rtl_loc_wk%rowtype;
g_found              boolean;
g_date               date;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_150U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE RTL_LOC_WK EX FND_RTL_LOC_WK_STAFFPLAN_FCST';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_wk%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_wk_staffplan is
   select lws.*,
          dl.sk1_location_no,
          dl.sunday_store_trade_ind,
          dlh.sk2_location_no,
          fc.fin_week_code
   from fnd_rtl_loc_wk_staffplan_fcst lws,
        dim_location dl,
        dim_location_hist dlh,
        dim_calendar fc
   where lws.location_no = dl.location_no
   and   lws.location_no = dlh.location_no
   and   lws.fin_year_no = fc.fin_year_no
   and   lws.fin_week_no = fc.fin_week_no
   and   fc.fin_day_no = 4
   and   fc.calendar_date between dlh.sk2_active_from_date and dlh.sk2_active_to_date
   and   lws.last_updated_date = g_date;

  /* TD-359 */

-- Input record declared as cursor%rowtype
g_rec_in             c_fnd_rtl_loc_wk_staffplan%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_fnd_rtl_loc_wk_staffplan%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_location_no                := g_rec_in.sk1_location_no;
   g_rec_out.fin_year_no                    := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                    := g_rec_in.fin_week_no;
   g_rec_out.sk2_location_no                := g_rec_in.sk2_location_no;
   g_rec_out.fin_week_code                  := g_rec_in.fin_week_code;
   g_rec_out.this_week_start_date           := g_rec_in.this_week_start_date;
--   g_rec_out.fte_budget                     := g_rec_in.fte_budget;
--   g_rec_out.fte_actual                     := g_rec_in.fte_actual;
   g_rec_out.dy01_fd_sls_hl_st_fcst_xcl_vat := g_rec_in.dy01_fd_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy02_fd_sls_hl_st_fcst_xcl_vat := g_rec_in.dy02_fd_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy03_fd_sls_hl_st_fcst_xcl_vat := g_rec_in.dy03_fd_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy04_fd_sls_hl_st_fcst_xcl_vat := g_rec_in.dy04_fd_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy05_fd_sls_hl_st_fcst_xcl_vat := g_rec_in.dy05_fd_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy06_fd_sls_hl_st_fcst_xcl_vat := g_rec_in.dy06_fd_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy07_fd_sls_hl_st_fcst_xcl_vat := g_rec_in.dy07_fd_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy01_ch_sls_hl_st_fcst_xcl_vat := g_rec_in.dy01_ch_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy02_ch_sls_hl_st_fcst_xcl_vat := g_rec_in.dy02_ch_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy03_ch_sls_hl_st_fcst_xcl_vat := g_rec_in.dy03_ch_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy04_ch_sls_hl_st_fcst_xcl_vat := g_rec_in.dy04_ch_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy05_ch_sls_hl_st_fcst_xcl_vat := g_rec_in.dy05_ch_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy06_ch_sls_hl_st_fcst_xcl_vat := g_rec_in.dy06_ch_sls_hl_st_fcst_xcl_vat;
   g_rec_out.dy07_ch_sls_hl_st_fcst_xcl_vat := g_rec_in.dy07_ch_sls_hl_st_fcst_xcl_vat;


   If g_rec_in.sunday_store_trade_ind = 1
     then
         g_rec_out.num_st_open_days := 6;
     else
        g_rec_out.num_st_open_days := 7;
   end if;
--
--  OLD CODE - td 2271
--   g_rec_out.num_st_open_days := 7;
--   if g_rec_in.sunday_store_trade_ind = 0 then
--      g_rec_out.num_st_open_days := 6;
--   end if;
---

   g_rec_out.last_updated_date              := g_date;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into rtl_loc_wk values a_tbl_insert(i);
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
      update rtl_loc_wk
      set sk1_location_no                = a_tbl_update(i).sk1_location_no,
          fin_year_no                    = a_tbl_update(i).fin_year_no,
          fin_week_no                    = a_tbl_update(i).fin_week_no,
          this_week_start_date           = a_tbl_update(i).this_week_start_date,
          sk2_location_no                = a_tbl_update(i).sk2_location_no,
--          fte_budget                     = a_tbl_update(i).fte_budget,
          fin_week_code                  = a_tbl_update(i).fin_week_code ,
--          fte_actual                     = a_tbl_update(i).fte_actual,
          dy01_fd_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy01_fd_sls_hl_st_fcst_xcl_vat,
          dy02_fd_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy02_fd_sls_hl_st_fcst_xcl_vat,
          dy03_fd_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy03_fd_sls_hl_st_fcst_xcl_vat,
          dy04_fd_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy04_fd_sls_hl_st_fcst_xcl_vat,
          dy05_fd_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy05_fd_sls_hl_st_fcst_xcl_vat,
          dy06_fd_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy06_fd_sls_hl_st_fcst_xcl_vat,
          dy07_fd_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy07_fd_sls_hl_st_fcst_xcl_vat,
          dy01_ch_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy01_ch_sls_hl_st_fcst_xcl_vat,
          dy02_ch_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy02_ch_sls_hl_st_fcst_xcl_vat,
          dy03_ch_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy03_ch_sls_hl_st_fcst_xcl_vat,
          dy04_ch_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy04_ch_sls_hl_st_fcst_xcl_vat,
          dy05_ch_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy05_ch_sls_hl_st_fcst_xcl_vat,
          dy06_ch_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy06_ch_sls_hl_st_fcst_xcl_vat,
          dy07_ch_sls_hl_st_fcst_xcl_vat = a_tbl_update(i).dy07_ch_sls_hl_st_fcst_xcl_vat,
          num_st_open_days               = a_tbl_update(i).num_st_open_days,
          last_updated_date              = a_tbl_update(i).last_updated_date
      where sk1_location_no = a_tbl_update(i).sk1_location_no
      and   fin_year_no = a_tbl_update(i).fin_year_no
      and   fin_week_no = a_tbl_update(i).fin_week_no;

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
   into g_count
   from rtl_loc_wk
   where sk1_location_no = g_rec_out.sk1_location_no
   and   fin_year_no = g_rec_out.fin_year_no
   and   fin_week_no = g_rec_out.fin_week_no;

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
    l_text := 'LOAD OF RTL_LOC_WK EX FND_RTL_LOC_WK_STAFFPLAN_FCST STARTED '||
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
    open c_fnd_rtl_loc_wk_staffplan;
    fetch c_fnd_rtl_loc_wk_staffplan bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_loc_wk_staffplan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_wk_staffplan;
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

end wh_prf_corp_150u;