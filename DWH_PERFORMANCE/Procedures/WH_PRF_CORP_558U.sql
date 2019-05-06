--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_558U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_558U" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  date:        february 2009
--  author:      wendy lyttle
--  purpose:     create location department week fact table in the performance layer with
--               added value ex foundation layer location department week finance budget table.
--  tables:      input  -   fnd_rtl_loc_dept_wk_fin_budg
--               output -   rtl_loc_comp_dy
--  packages:    constants, dwh_log, dwh_valid
--  comments:    single dml could be considered for this program.
--
--  maintenance:
--
--  naming conventions:
--  g_  -  global variable
--  l_  -  log table variable
--  a_  -  array variable
--  v_  -  local variable as found in packages
--  p_  -  parameter
--  c_  -  prefix to cursor
--**************************************************************************************************
g_recs_read         integer := 0;
g_recs_inserted     integer := 0;
g_recs_updated      integer := 0;
g_forall_limit      integer := dwh_constants.vc_forall_limit;
g_error_count       number  := 0;
g_error_index       number  := 0;
g_rec_out rtl_loc_comp_dy%rowtype;
g_found boolean;
g_count number      := 0;
g_date date         := trunc(sysdate);

l_message sys_dwh_errlog.log_text%type;
l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_558U';
l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
l_text sys_dwh_log.log_text%type ;
l_description sys_dwh_log_summary.log_description%type   := 'CREATE rtl_loc_comp_dy EX fnd_rtl_loc_dept_wk_fin_budg';
l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
-- for output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_comp_dy%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_comp_dy%rowtype index by binary_integer;

a_tbl_insert tbl_array_i;
a_tbl_update tbl_array_u;
a_empty_set_i tbl_array_i;
a_empty_set_u tbl_array_u;
a_count   integer := 0;
a_count_i integer := 0;
a_count_u integer := 0;

cursor c_fnd_rtl_loc_dept_wk_fin_budg is
with chgrecs as
  (select flbu.location_no, dd.company_no, flbu.fin_year_no, flbu.fin_week_no
   from   fnd_rtl_loc_dept_wk_fin_budg flbu
   join   dim_department dd on dd.department_no = flbu.department_no
   where  flbu.last_updated_date = g_date
   group  by flbu.location_no, dd.company_no, flbu.fin_year_no, flbu.fin_week_no)

   select dl.sk1_location_no,
          dd.sk1_company_no,
          dc.this_week_start_date tran_date,
          max(dlh.sk2_location_no) sk2_location_no,
          sum(num_tran_999_budget) num_customers_budget
   from   fnd_rtl_loc_dept_wk_fin_budg flbu
   join   dim_department dd      on  dd.department_no        = flbu.department_no
   join   chgrecs cr             on  cr.location_no          = flbu.location_no
                                 and cr.company_no           = dd.company_no
                                 and cr.fin_year_no          = flbu.fin_year_no
                                 and cr.fin_week_no          = flbu.fin_week_no
   join   dim_calendar dc        on  dc.fin_year_no          = flbu.fin_year_no
                                 and dc.fin_week_no          = flbu.fin_week_no
   join   dim_location dl        on  dl.location_no          = flbu.location_no
   join   dim_location_hist dlh  on  dlh.location_no         = flbu.location_no
                                 and dc.this_week_start_date between dlh.sk2_active_from_date and dlh.sk2_active_to_date
   where dc.fin_day_no = 3
   group by dl.sk1_location_no, dd.sk1_company_no, dc.this_week_start_date;

g_rec_in c_fnd_rtl_loc_dept_wk_fin_budg%rowtype;
type stg_array is table of c_fnd_rtl_loc_dept_wk_fin_budg%rowtype;
a_stg_input stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

  g_rec_out.sk1_location_no         := g_rec_in.sk1_location_no;
  g_rec_out.sk1_company_no          := g_rec_in.sk1_company_no;
  g_rec_out.tran_date               := g_rec_in.tran_date;
  g_rec_out.sk2_location_no         := g_rec_in.sk2_location_no;
  g_rec_out.num_customers_budget    := g_rec_in.num_customers_budget;
  g_rec_out.last_updated_date       := g_date;

  exception
    when others then
      l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise;

end local_address_variable;

--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

  forall i in a_tbl_insert.first .. a_tbl_insert.last
  save exceptions
  insert into rtl_loc_comp_dy values a_tbl_insert(i);

  g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

  exception
    when others then
      g_error_count := sql%bulk_exceptions.count;
      l_message     := dwh_constants.vc_err_lb_insert||g_error_count||' '||sqlcode||' '||sqlerrm;
      dwh_log.record_error(l_module_name,sqlcode,l_message);

  for i in 1 .. g_error_count
  loop
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index
                 ||' '||sqlerrm(-sql%bulk_exceptions(i).error_code)
                 ||' '||a_tbl_insert(g_error_index).sk1_location_no
                 ||' '||a_tbl_insert(g_error_index).sk1_company_no
                 ||' '||a_tbl_insert(g_error_index).tran_date;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
  end loop;
  raise;

end local_bulk_insert;

--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

  forall i in a_tbl_update.first .. a_tbl_update.last
  save exceptions
  update rtl_loc_comp_dy
  set    sk2_location_no       = a_tbl_update(i).sk2_location_no,
         last_updated_date     = a_tbl_update(i).last_updated_date,
         num_customers_budget  = a_tbl_update(i).num_customers_budget
  where  sk1_location_no       = a_tbl_update(i).sk1_location_no
  and    sk1_company_no        = a_tbl_update(i).sk1_company_no
  and    tran_date             = a_tbl_update(i).tran_date;

  g_recs_updated := g_recs_updated + a_tbl_update.count;

  exception
    when others then
      g_error_count := sql%bulk_exceptions.count;
      l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      for i in 1 .. g_error_count
      loop
        g_error_index := sql%bulk_exceptions(i).error_index;
        l_message     := dwh_constants.vc_err_lb_loop||i||' '||g_error_index||
                         ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                         ' '||a_tbl_update(g_error_index).sk1_location_no||
                         ' '||a_tbl_update(g_error_index).sk1_company_no||
                         ' '||a_tbl_update(g_error_index).tran_date;
        dwh_log.record_error(l_module_name,sqlcode,l_message);
      end loop;
      raise;

end local_bulk_update;

--**************************************************************************************************
-- write valid data out to the table
--**************************************************************************************************
procedure local_write_output as
begin

  g_found := false;
  -- check to see if item is present on table and update/insert accordingly
  select count(1)
  into   g_count
  from   rtl_loc_comp_dy
  where  sk1_location_no   = g_rec_out.sk1_location_no
  and    sk1_company_no    = g_rec_out.sk1_company_no
  and    tran_date         = g_rec_out.tran_date;

  if g_count               = 1 then
    g_found                := true;
  end if;
  -- place record into array for later bulk writing
  if not g_found then
    a_count_i               := a_count_i + 1;
    a_tbl_insert(a_count_i) := g_rec_out;
  else
    a_count_u               := a_count_u + 1;
    a_tbl_update(a_count_u) := g_rec_out;
  end if;
  a_count := a_count + 1;
  --**************************************************************************************************
  -- bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  if a_count > g_forall_limit then
    local_bulk_insert;
    local_bulk_update;
    a_tbl_insert := a_empty_set_i;
    a_tbl_update := a_empty_set_u;
    a_count_i    := 0;
    a_count_u    := 0;
    a_count      := 0;
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
-- main process loop
--**************************************************************************************************
begin

  if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
    g_forall_limit  := p_forall_limit;
  end if;
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF rtl_loc_comp_dy EX fnd_rtl_loc_dept_wk_fin_budg STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
  l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- look up batch date from dim_control
--**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
  open c_fnd_rtl_loc_dept_wk_fin_budg;
  fetch c_fnd_rtl_loc_dept_wk_fin_budg bulk collect
     into a_stg_input limit g_forall_limit;

  while a_stg_input.count > 0
  loop
    for i in 1 .. a_stg_input.count
    loop
      g_recs_read             := g_recs_read + 1;
      if g_recs_read mod 10000 = 0 then
        l_text                := dwh_constants.vc_log_records_processed||
                                 to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
      g_rec_in := a_stg_input(i);
      local_address_variable;
      local_write_output;
    end loop;
    fetch c_fnd_rtl_loc_dept_wk_fin_budg bulk collect
       into a_stg_input limit g_forall_limit;
  end loop;
  close c_fnd_rtl_loc_dept_wk_fin_budg;
  --**************************************************************************************************
  -- at end write out what remains in the arrays
  --**************************************************************************************************
  local_bulk_insert;
  local_bulk_update;
  --**************************************************************************************************
  -- at end write out log totals
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
  l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
  l_text := dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
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

end wh_prf_corp_558u;
