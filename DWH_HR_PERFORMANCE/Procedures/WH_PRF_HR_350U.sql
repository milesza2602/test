--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_350U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_350U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        November 2011
--  Author:      Alastair de Wet
--  Purpose:     Create hr_bee_ee_employee_bu_dy fact table in the performance layer
--               with added value ex foundation layer dim_hr_employee.
--  Tables:      Input  - dim_hr_employee
--               Output - hr_bee_ee_employee_bu_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  10000;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            hr_bee_ee_employee_bu_dy%rowtype;

g_found              boolean;
g_this_mn_start_date date          ;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_350U';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE hr_bee_ee_employee_bu_dy EX EMP MASTER';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of hr_bee_ee_employee_bu_dy%rowtype index by binary_integer;
type tbl_array_u is table of hr_bee_ee_employee_bu_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_dim_hr_employee is
   select emp.*,bu.sk1_bee_business_unit_code
   from   dim_hr_employee emp,
          dim_hr_department bu
   where  emp.department_code  = bu.department_code and
          emp.company_code     = bu.company_code and
          emp.employee_status_code = 'A'  and
          emp.perm_temp_code       = 'P' and
          emp.company_code        in ('WW','UPN');

g_rec_in                   c_dim_hr_employee%rowtype;
-- For input bulk collect --
type stg_array is table of c_dim_hr_employee%rowtype;
a_stg_input      stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_employee_id             := g_rec_in.sk1_employee_id;
   g_rec_out.sk1_bee_business_unit_code  := g_rec_in.sk1_bee_business_unit_code;
--   g_rec_out.snapshot_date               := g_date;
   g_rec_out.employee_count              := 1;
   g_rec_out.last_updated_date           := g_date;

   select this_mn_end_date,this_mn_start_date
   into   g_rec_out.this_mn_end_date,g_this_mn_start_date
   from   dim_calendar
   where  calendar_date = g_date - 21;
  --***********************************************************************************************
  -- Add filter sk2_latest_record_ind =1 for take on
  --*********************************************************************************************************
   select sk2_employee_id
   into   g_rec_out.sk2_employee_id
   from   dim_hr_employee_hist deh
   where  g_rec_in.employee_id = deh.employee_id and
          g_rec_out.this_mn_end_date between deh.sk2_active_from_date and deh.sk2_active_to_date
--          and sk2_latest_record_ind=1
   ;

   exception
      when others then
       l_message := dwh_hr_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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
      insert into hr_bee_ee_employee_bu_dy values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).sk1_employee_id;
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
      update hr_bee_ee_employee_bu_dy
      set    employee_count              = a_tbl_update(i).employee_count,
             sk1_bee_business_unit_code  = a_tbl_update(i).sk1_bee_business_unit_code,
             last_updated_date           = a_tbl_update(i).last_updated_date
      where  sk1_employee_id             = a_tbl_update(i).sk1_employee_id and
             this_mn_end_date            = a_tbl_update(i).this_mn_end_date  ;


      g_recs_updated := g_recs_updated + a_tbl_update.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_employee_id;
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
   from   hr_bee_ee_employee_bu_dy
   where  sk1_employee_id       = g_rec_out.sk1_employee_id  and
          this_mn_end_date      = g_rec_out.this_mn_end_date;

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
       l_message := dwh_hr_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF hr_bee_ee_employee_bu_dy EX dim_hr_employee STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_dim_hr_employee;
    fetch c_dim_hr_employee bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_hr_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_dim_hr_employee bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_dim_hr_employee;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_hr_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_hr_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_hr_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_hr_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_hr_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_HR_350U;
