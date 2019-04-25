--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_001U_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_001U_TAKEON" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  the employee data from PeopleSoft needs to be sent to ODWH on a trickle-feed basis for S$S.
-- Currently runs for HR_BEE on a monthly basis
-- Until we recive a daily feed from People Soft, we will be using the S4S employee file
--**************************************************************************************************
--
--  Date:        November 2011
--  Author:      Alastair de Wet
--  Purpose:     Create dwh_performance.dim_employee dimention table in the performance layer
--               with added value ex foundation layer dwh_foundation.fnd_s4s_EMPLOYEE.
--  Tables:      Input  - dwh_foundation.fnd_s4s_EMPLOYEE
--               Output - dwh_performance.dim_employee
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  wENDY lYTTLE - S4S PROJECT - add news columns
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
g_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dwh_performance.dim_employee%rowtype;
g_rec_in             DWH_FOUNDATION.fnd_s4s_EMPLOYEE%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_001U_TAKEON';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE payment category data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



-- For input bulk collect --
type stg_array is table of DWH_FOUNDATION.fnd_s4s_EMPLOYEE%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.dim_employee%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.dim_employee%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_s4s_EMPLOYEE is
   select *
   from DWH_FOUNDATION.fnd_s4s_EMPLOYEE
---   where employee_status_code in ('A','L','P','S','W')
;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin


   g_rec_out.employee_id                     := g_rec_in.employee_id;
/*   g_rec_out.first_name                      := g_rec_in.first_name;
   g_rec_out.last_name                       := g_rec_in.last_name;
   g_rec_out.company_code                    := g_rec_in.company_code;
   g_rec_out.department_code                 := g_rec_in.department_code;
   g_rec_out.employee_status_code            := g_rec_in.employee_status_code;
   g_rec_out.id_no                           := g_rec_in.id_no;
   g_rec_out.gender_code                     := g_rec_in.gender_code;
   g_rec_out.race_code                       := g_rec_in.race_code;
   g_rec_out.citizenship_status_code         := g_rec_in.citizenship_status_code;
   g_rec_out.termination_date                := g_rec_in.termination_date;
   g_rec_out.perm_temp_code                   := g_rec_in.perm_temp_code;
   g_rec_out.grade_code                      := g_rec_in.grade_code;
   g_rec_out.occupation_level_code           := g_rec_in.occupation_level_code;
   g_rec_out.disabled_ind                    := g_rec_in.disabled_ind;
   g_rec_out.compensation_rate               := g_rec_in.compensation_rate;
   g_rec_out.employee_location_code          := g_rec_in.employee_location_code;
   g_rec_out.board_member_ind                := g_rec_in.board_member_ind;
   g_rec_out.executive_director_ind          := g_rec_in.executive_director_ind;
   g_rec_out.independent_director_ind        := g_rec_in.independent_director_ind;
   g_rec_out.top_management_ind              := g_rec_in.top_management_ind;
   g_rec_out.other_top_management_ind        := g_rec_in.other_top_management_ind;
 */  g_rec_out.last_updated_date               := g_date;
   --- s4s columns
   g_rec_out.s4s_EMPLOYEE_TYPE                    := g_rec_in.s4s_EMPLOYEE_TYPE;
   g_rec_out.s4s_EMPLOYEE_WORKstatus              := g_rec_in.s4s_EMPLOYEE_WORKstatus;
   --- s4s columns
/*   g_rec_out.full_name                      := g_rec_in.first_name||' '||g_rec_in.last_name;
   g_rec_out.employee_long_desc             := g_rec_in.employee_id||' - '||g_rec_in.first_name||' '||g_rec_in.last_name;
*/
--ADD INDICATOR VALUES
/*
  if g_rec_out.employee_status_code  in ('A','L','P','S','W') then
    g_rec_out.active_ind                      := 1;
    else
    g_rec_out.active_ind                      := 0;
  end if ;

  g_rec_out.management_control_ind      := 0;
  if g_rec_out.board_member_ind          = 1 or
     g_rec_out.executive_director_ind    = 1 or
     g_rec_out.independent_director_ind  = 1 or
     g_rec_out.top_management_ind        = 1 or
     g_rec_out.other_top_management_ind  = 1 then
     g_rec_out.management_control_ind   := 1;
  end if;

--ADD SK1 VALUES TO OUTPUT EMPLOYEE MASTER
   begin
     select sk1_race_code
     into   g_rec_out.sk1_race_code
     from   dim_race
     where  race_code          = g_rec_out.race_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_race_code := 0;
   end;
   begin
     select sk1_gender_code
     into   g_rec_out.sk1_gender_code
     from   dim_gender
     where  gender_code          = g_rec_out.gender_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_gender_code := 0;
   end;
   begin
     select sk1_company_code
     into   g_rec_out.sk1_company_code
     from   dim_company
     where  company_code          = g_rec_out.company_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_company_code := 0;
   end;
   begin
     select sk1_department_code
     into   g_rec_out.sk1_department_code
     from   dim_department
     where  department_code          = g_rec_out.department_code and
            company_code             = g_rec_out.company_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_department_code := 0;
   end;
   begin
     select sk1_employee_location_code
     into   g_rec_out.sk1_employee_location_code
     from   dwh_performance.dim_employee_location
     where  employee_location_code          = g_rec_out.employee_location_code and
            company_code                    = g_rec_out.company_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_employee_location_code := 0;
   end;
      begin
     select sk1_citizenship_status_code
     into   g_rec_out.sk1_citizenship_status_code
     from   dim_citizenship_status
     where  citizenship_status_code          = g_rec_out.citizenship_status_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_citizenship_status_code := 0;
   end;
   begin
     select sk1_employee_status_code
     into   g_rec_out.sk1_employee_status_code
     from   dwh_performance.dim_employee_status
     where  employee_status_code          = g_rec_out.employee_status_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_employee_status_code := 0;
   end;
   begin
     select sk1_occupation_level_code
     into   g_rec_out.sk1_occupation_level_code
     from   dim_occupation_level
     where  occupation_level_code          = g_rec_out.occupation_level_code ;

     exception
         when no_data_found then
              g_rec_out.sk1_occupation_level_code := 0;
   end;
   */

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
      insert into dwh_performance.dim_employee values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).employee_id;
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
      update  dwh_performance.dim_employee
       set   
       /*first_name                      = a_tbl_update(i).first_name,
              last_name                       = a_tbl_update(i).last_name,
              full_name                       = a_tbl_update(i).full_name,
              employee_long_desc              = a_tbl_update(i).employee_long_desc,
              company_code                    = a_tbl_update(i).company_code,
              department_code                 = a_tbl_update(i).department_code,
              employee_status_code            = a_tbl_update(i).employee_status_code,
              id_no                           = a_tbl_update(i).id_no,
              gender_code                     = a_tbl_update(i).gender_code,
              race_code                       = a_tbl_update(i).race_code,
              citizenship_status_code         = a_tbl_update(i).citizenship_status_code,
              termination_date                = a_tbl_update(i).termination_date,
              perm_temp_code                  = a_tbl_update(i).perm_temp_code,
              grade_code                      = a_tbl_update(i).grade_code,
              occupation_level_code           = a_tbl_update(i).occupation_level_code,
              management_control_ind          = a_tbl_update(i).management_control_ind,
              disabled_ind                    = a_tbl_update(i).disabled_ind,
              compensation_rate               = a_tbl_update(i).compensation_rate,
              employee_location_code          = a_tbl_update(i).employee_location_code,
              sk1_race_code                   = a_tbl_update(i).sk1_race_code,
              sk1_gender_code                 = a_tbl_update(i).sk1_gender_code,
              sk1_company_code                = a_tbl_update(i).sk1_company_code,
              sk1_department_code             = a_tbl_update(i).sk1_department_code,
              sk1_citizenship_status_code     = a_tbl_update(i).sk1_citizenship_status_code,
              sk1_occupation_level_code       = a_tbl_update(i).sk1_occupation_level_code,
              sk1_employee_status_code        = a_tbl_update(i).sk1_employee_status_code,
              sk1_employee_location_code      = a_tbl_update(i).sk1_employee_location_code,
              active_ind                      = a_tbl_update(i).active_ind,
              board_member_ind                = a_tbl_update(i).board_member_ind,
              executive_director_ind          = a_tbl_update(i).executive_director_ind,
              independent_director_ind        = a_tbl_update(i).independent_director_ind,
              top_management_ind              = a_tbl_update(i).top_management_ind,
              other_top_management_ind        = a_tbl_update(i).other_top_management_ind,
    */          last_updated_date               = a_tbl_update(i).last_updated_date,
              s4s_EMPLOYEE_TYPE                    =  a_tbl_update(i).s4s_EMPLOYEE_TYPE,
              s4s_EMPLOYEE_WORKstatus              =  a_tbl_update(i).s4s_EMPLOYEE_WORKstatus
       where  employee_id                     = a_tbl_update(i).employee_id ;

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
                       ' '||a_tbl_update(g_error_index).employee_id;
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
   from   dwh_performance.dim_employee
    where  EMPLOYEE_ID             = g_rec_out.EMPLOYEE_ID ;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_EMPLOYEE_ID   := LABOUR_HIERACHY_seq.nextval;
--      g_rec_out.sk_from_date  := g_date;
--      g_rec_out.sk_to_date    := dwh_constants.sk_to_date;
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

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF dwh_performance.dim_employee EX dwh_foundation.fnd_s4s_EMPLOYEE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_performance.DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_s4s_EMPLOYEE;
    fetch c_fnd_s4s_EMPLOYEE bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_s4s_EMPLOYEE bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_s4s_EMPLOYEE;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
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



END WH_PRF_S4S_001U_TAKEON;
