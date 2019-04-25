--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_250U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_250U" (p_forall_limit in integer,p_success out boolean) AS
--**************************************************************************************************
--  Date:        Apr 2012
--  Author:      Alastair de Wet
--  Purpose:     Generate the employee dimention  sk type 2 load program
--  Tables:      Input  - dim_hr_employee
--               Output - dim_hr_employee_hist
--  Packages:    constants, dwh_log,
--
--  Maintenance:
--      Date      : Jun 2014
--     Changed by : Kgomotso Lehabe
--     Purpose    Add Employee_class_code as a type 2 monitored field.
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor followed by table name
--**************************************************************************************************
g_recs_read         integer       :=  0;
g_recs_inserted     integer       :=  0;
g_recs_updated      integer       :=  0;

g_rec_out           dim_hr_employee_hist%rowtype;
g_count             integer       :=  0;
g_found             boolean;
g_insert_rec        boolean;
g_date              date          := trunc(sysdate);
g_this_mn_start_date date;
l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_250U';
l_name              sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name    sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'CREATE dim_hr_employee_hist EX employee mast';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;

cursor c_dim_hr_employee is
   select  *
   from   dim_hr_employee  ;

g_rec_in            c_dim_hr_employee%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin


   g_rec_out.employee_id                     := g_rec_in.employee_id;
   g_rec_out.first_name                      := g_rec_in.first_name;
   g_rec_out.last_name                       := g_rec_in.last_name;
   g_rec_out.full_name                       := g_rec_in.full_name;
   g_rec_out.employee_long_desc              := g_rec_in.employee_long_desc;
   g_rec_out.sk1_company_code                := g_rec_in.sk1_company_code;
   g_rec_out.company_code                    := g_rec_in.company_code;
   g_rec_out.sk1_department_code             := g_rec_in.sk1_department_code;
   g_rec_out.department_code                 := g_rec_in.department_code;
   g_rec_out.sk1_employee_status_code        := g_rec_in.sk1_employee_status_code;
   g_rec_out.employee_status_code            := g_rec_in.employee_status_code;
   g_rec_out.id_no                           := g_rec_in.id_no;
   g_rec_out.sk1_gender_code                 := g_rec_in.sk1_gender_code;
   g_rec_out.gender_code                     := g_rec_in.gender_code;
   g_rec_out.sk1_race_code                   := g_rec_in.sk1_race_code;
   g_rec_out.race_code                       := g_rec_in.race_code;
   g_rec_out.sk1_citizenship_status_code     := g_rec_in.sk1_citizenship_status_code;
   g_rec_out.citizenship_status_code         := g_rec_in.citizenship_status_code;
   g_rec_out.termination_date                := g_rec_in.termination_date;
   g_rec_out.perm_temp_code                  := g_rec_in.perm_temp_code;
   g_rec_out.grade_code                      := g_rec_in.grade_code;
   g_rec_out.sk1_occupation_level_code       := g_rec_in.sk1_occupation_level_code;
   g_rec_out.occupation_level_code           := g_rec_in.occupation_level_code;
   g_rec_out.management_control_ind          := g_rec_in.management_control_ind;
   g_rec_out.disabled_ind                    := g_rec_in.disabled_ind;
   g_rec_out.compensation_rate               := g_rec_in.compensation_rate;
   g_rec_out.sk1_employee_location_code      := g_rec_in.sk1_employee_location_code;
   g_rec_out.employee_location_code          := g_rec_in.employee_location_code;
   g_rec_out.active_ind                      := g_rec_in.active_ind;
   g_rec_out.board_member_ind                := g_rec_in.board_member_ind;
   g_rec_out.executive_director_ind          := g_rec_in.executive_director_ind;
   g_rec_out.independent_director_ind        := g_rec_in.independent_director_ind;
   g_rec_out.top_management_ind              := g_rec_in.top_management_ind;
   g_rec_out.other_top_management_ind        := g_rec_in.other_top_management_ind;
   g_rec_out.employee_class_code             := g_rec_in.employee_class_code;
   g_rec_out.sk1_employee_class_code         := g_rec_in.sk1_employee_class_code ;


   g_rec_out.last_updated_date               := g_date;


   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm||' '||g_rec_out.employee_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;

--**************************************************************************************************
-- Write valid data out to the employee master table
--**************************************************************************************************
procedure local_write_output as
begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
    from   dim_hr_employee_hist
    where  employee_id = g_rec_out.employee_id and
           sk2_active_to_date = dwh_constants.sk_to_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;



--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 -- Place record into array for later bulk writing

   dwh_hr_valid.did_employee_change
   (g_rec_out.employee_id,g_this_mn_start_date,
                          g_rec_out.gender_code,g_rec_out.race_code,g_rec_out.citizenship_status_code,
                          g_rec_out.occupation_level_code,g_rec_out.disabled_ind,g_rec_out.perm_temp_code, g_rec_out.employee_class_code,
                          g_insert_rec);

   if g_insert_rec then
      g_rec_out.sk2_employee_id        := hr_seq.nextval;
      g_rec_out.sk2_active_from_date   := g_this_mn_start_date;
      g_rec_out.sk2_active_to_date     := dwh_constants.sk_to_date;
      g_rec_out.sk2_latest_record_ind  := 1;
      if not g_found then
         g_rec_out.sk2_active_from_date   := '1 Jan 2005';
      end if;
      insert into dim_hr_employee_hist values g_rec_out;
      g_recs_inserted                  := g_recs_inserted + sql%rowcount;
   else
      update dim_hr_employee_hist
      set     first_name                      = g_rec_out.first_name,
              last_name                       = g_rec_out.last_name,
              full_name                       = g_rec_out.full_name,
              employee_long_desc              = g_rec_out.employee_long_desc,
              company_code                    = g_rec_out.company_code,
              department_code                 = g_rec_out.department_code,
              employee_status_code            = g_rec_out.employee_status_code,
              id_no                           = g_rec_out.id_no,
              gender_code                     = g_rec_out.gender_code,
              race_code                       = g_rec_out.race_code,
              citizenship_status_code         = g_rec_out.citizenship_status_code,
              termination_date                = g_rec_out.termination_date,
              perm_temp_code                  = g_rec_out.perm_temp_code,
              grade_code                      = g_rec_out.grade_code,
              occupation_level_code           = g_rec_out.occupation_level_code,
              management_control_ind          = g_rec_out.management_control_ind,
              disabled_ind                    = g_rec_out.disabled_ind,
              compensation_rate               = g_rec_out.compensation_rate,
              employee_location_code          = g_rec_out.employee_location_code,
              sk1_race_code                   = g_rec_out.sk1_race_code,
              sk1_gender_code                 = g_rec_out.sk1_gender_code,
              sk1_company_code                = g_rec_out.sk1_company_code,
              sk1_department_code             = g_rec_out.sk1_department_code,
              sk1_citizenship_status_code     = g_rec_out.sk1_citizenship_status_code,
              sk1_occupation_level_code       = g_rec_out.sk1_occupation_level_code,
              sk1_employee_status_code        = g_rec_out.sk1_employee_status_code,
              sk1_employee_location_code      = g_rec_out.sk1_employee_location_code,
              active_ind                      = g_rec_out.active_ind,
              board_member_ind                = g_rec_out.board_member_ind,
              executive_director_ind          = g_rec_out.executive_director_ind,
              independent_director_ind        = g_rec_out.independent_director_ind,
              top_management_ind              = g_rec_out.top_management_ind,
              other_top_management_ind        = g_rec_out.other_top_management_ind,
              employee_class_code             = g_rec_out.employee_class_code,
              sk1_employee_class_code         = g_rec_out.sk1_employee_class_code,
              last_updated_date               = g_date

      where  employee_id                     = g_rec_out.employee_id and
             sk2_active_to_date              = dwh_constants.sk_to_date;

      g_recs_updated              := g_recs_updated + sql%rowcount;
   end if;

-- *************************************************************************************************
-- Update old versions of the same employee with details not linked to SCD attributes
-- This avoids having different employee names for history employees and will be done as
-- required by the business
-- NOT REQUIRED BUSINESS SHOULD SEE HISTORY AS IT WAS
--   update dim_hr_employee_hist
--   set    employee_name            = g_rec_out.employee_name,
--          date_last_updated    = g_rec_out.date_last_updated
--   where  employee_no              = g_rec_out.employee_no and
--          sk2_active_to_date   <> dwh_constants.sk_to_date;
-- *************************************************************************************************

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm||' '||g_rec_out.employee_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm||' '||g_rec_out.employee_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF employee MASTER SK2 VERSION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

   select this_mn_start_date
   into   g_this_mn_start_date
   from   dim_calendar
   where  calendar_date = g_date - 15;

--**************************************************************************************************
    for v_dim_hr_employee in c_dim_hr_employee
    loop
      g_recs_read := g_recs_read + 1;

      if g_recs_read mod 10000 = 0 then
         l_text := dwh_constants.vc_log_records_processed||
         to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;

      g_rec_in := v_dim_hr_employee;
      local_address_variable;
      local_write_output;

    end loop;

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
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm||' '||g_rec_out.employee_id;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       rollback;
       p_success := false;
       raise;

end wh_prf_hr_250u;
