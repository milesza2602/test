set define off

CREATE OR REPLACE PROCEDURE "WH_FND_HR_070U" (p_forall_limit in integer,p_success out boolean) as

--************************************************************************************************
--  Date:        July 2014
--  Author:      Alastair/Wendy
--  Purpose:     Update HR employee information with PeopleSoft data for S4S ONLY AT THE MOMENT
--
--  Tables:      AIT load - STG_HR_PS_EMPLOYEE
--               Input    - STG_HR_PS_EMPLOYEE_CPY
--               Output   - FND_PS_EMPLOYEE
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  Lwazi Ntloko

-- Why won't changes merge
--  Added comments as a test
--  Added another change for comparison

-- Added a comment to test check-in

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      dwh_hr_foundation.STG_HR_PS_EMPLOYEE_HSP.sys_process_msg%type;
g_rec_out            dwh_hr_foundation.FND_PS_EMPLOYEE%rowtype;
g_found              boolean;
g_valid              boolean;

/*add to check 2 things. 1) Build gets all changes 2) deplys to Barry's schema*/

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_HR_070U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE Employee data ex PS for S4S';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  -- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_hr_foundation.FND_PS_EMPLOYEE%rowtype index by binary_integer;
type tbl_array_u is table of dwh_hr_foundation.FND_PS_EMPLOYEE%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of dwh_hr_foundation.STG_HR_PS_EMPLOYEE_CPY.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of dwh_hr_foundation.STG_HR_PS_EMPLOYEE_CPY.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_STG_S4S_EMPLOYEE is
   select SYS_SOURCE_BATCH_ID,
          SYS_SOURCE_SEQUENCE_NO,
          SYS_LOAD_DATE,
          SYS_PROCESS_CODE,
          SYS_LOAD_SYSTEM_NAME,
          SYS_MIDDLEWARE_BATCH_ID,
          SYS_PROCESS_MSG,
          SOURCE_DATA_STATUS_CODE,
          EMPLOYEE_ID,
          FIRST_NAME,
          LAST_NAME ,
          COMPANY_CODE,
          DEPARTMENT_CODE,
          EMPLOYEE_STATUS_CODE,
          ID_NO,
          GENDER_CODE ,
          RACE_CODE,
          CITIZENSHIP_STATUS_CODE,
          TERMINATION_DATE,
          PERM_TEMP_CODE,
          GRADE_CODE,
          OCCUPATION_LEVEL_CODE,
          DISABLED_IND,     
          EMPLOYEE_LOCATION_CODE,
          EMPLOYEE_CLASS_CODE,
          EFFECTIVE_DATE
   from dwh_hr_foundation.STG_HR_PS_EMPLOYEE_CPY 
   order by sys_source_batch_id,sys_source_sequence_no;

g_rec_in                   c_STG_S4S_EMPLOYEE%rowtype;
-- For input bulk collect --
type stg_array is table of c_STG_S4S_EMPLOYEE%rowtype;
a_stg_input      stg_array;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************

-- and so I added this comment for further testing --
procedure local_address_variables as
begin

   g_hospital                           := 'N';
   g_rec_out.employee_id                     := g_rec_in.EMPLOYEE_ID;
   g_rec_out.first_name                      := g_rec_in.first_name;
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
   g_rec_out.employee_location_code          := g_rec_in.employee_location_code;
   g_rec_out.employee_class_code             := g_rec_in.employee_class_code;
   g_rec_out.effective_date                 := g_rec_in.effective_date ;
   g_rec_out.last_updated_date               := g_date;


   if not dwh_valid.indicator_field(g_rec_out.disabled_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
     return;
   end if;

--   if not dwh_valid.indicator_field(g_rec_out.perm_temp_code) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_hr_constants.vc_invalid_indicator;
--     return;
--   end if;  

   if not dwh_hr_valid.fnd_hr_company(g_rec_out.company_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_company;
     l_text          := dwh_hr_constants.vc_invalid_company||g_rec_out.company_code ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          return;
   end if;

   if not dwh_hr_valid.fnd_hr_department(g_rec_out.department_code,g_rec_out.company_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_department;
     l_text          := dwh_hr_constants.vc_invalid_department||g_rec_out.department_code ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          return;
   end if;

   if not dwh_hr_valid.fnd_hr_employee_status(g_rec_out.employee_status_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_employee_status;
     l_text          := dwh_hr_constants.vc_invalid_employee_status||g_rec_out.employee_status_code ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          return;
   end if;

   if not dwh_hr_valid.fnd_hr_gender(g_rec_out.gender_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_gender;
     l_text          := dwh_hr_constants.vc_invalid_gender||g_rec_out.gender_code ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
   end if;

   if not dwh_hr_valid.fnd_hr_race(g_rec_out.race_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_race;
     l_text          := dwh_hr_constants.vc_invalid_race||g_rec_out.race_code ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
   end if;

   if not dwh_hr_valid.fnd_hr_citizenship_status(g_rec_out.citizenship_status_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_citizenship_status;
     l_text          := dwh_hr_constants.vc_invalid_citizenship_status||g_rec_out.citizenship_status_code ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
   end if;

   if not dwh_hr_valid.fnd_hr_occupation_level(g_rec_out.occupation_level_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_occupational_level;
     l_text          := dwh_hr_constants.vc_invalid_occupational_level||g_rec_out.occupation_level_code ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         return;
   end if;

   if not dwh_hr_valid.fnd_hr_employee_location(g_rec_out.employee_location_code,g_rec_out.company_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_employee_location;
     l_text          := dwh_hr_constants.vc_invalid_employee_location||g_rec_out.employee_location_code ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;   

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;
--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin

-- subprocedure to write to the hospital table --
   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

      insert into dwh_hr_foundation.STG_HR_PS_EMPLOYEE_HSP values
   ( g_rec_in.SYS_SOURCE_BATCH_ID
     ,g_rec_in.SYS_SOURCE_SEQUENCE_NO
     ,g_rec_in.SYS_LOAD_DATE
     ,g_rec_in.SYS_PROCESS_CODE
     ,g_rec_in.SYS_LOAD_SYSTEM_NAME
     ,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
     ,g_rec_in.SYS_PROCESS_MSG
     ,g_rec_in.SOURCE_DATA_STATUS_CODE
     ,g_rec_in.EMPLOYEE_ID
      ,g_rec_in.first_name
      ,g_rec_in.last_name
      ,g_rec_in.company_code
      ,g_rec_in.department_code
      ,g_rec_in.employee_status_code
      ,g_rec_in.id_no
      ,g_rec_in.gender_code
     ,g_rec_in.race_code
      ,g_rec_in.citizenship_status_code
      ,g_rec_in.termination_date
      ,g_rec_in.perm_temp_code
		,g_rec_in.grade_code
		,g_rec_in.occupation_level_code
		,g_rec_in.disabled_ind
		,g_rec_in.employee_location_code
    ,g_rec_in.employee_class_code
		,g_rec_in.effective_date 
    ); 

   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_hospital;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into dwh_hr_foundation.FND_PS_EMPLOYEE values a_tbl_insert(i);
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
                       'Emp-Id-'||a_tbl_insert(g_error_index).employee_id;
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
       update dwh_hr_foundation.FND_PS_EMPLOYEE
       set
          FIRST_NAME =  a_tbl_update(i).FIRST_NAME,
          LAST_NAME =  a_tbl_update(i).LAST_NAME,
          COMPANY_CODE =  a_tbl_update(i).COMPANY_CODE,
          DEPARTMENT_CODE =  a_tbl_update(i).DEPARTMENT_CODE,
          EMPLOYEE_STATUS_CODE =  a_tbl_update(i).EMPLOYEE_STATUS_CODE,
          ID_NO =  a_tbl_update(i).ID_NO,
          GENDER_CODE =  a_tbl_update(i).GENDER_CODE ,
          RACE_CODE =  a_tbl_update(i).RACE_CODE,
          CITIZENSHIP_STATUS_CODE =  a_tbl_update(i).CITIZENSHIP_STATUS_CODE,
          TERMINATION_DATE =  a_tbl_update(i).TERMINATION_DATE,
          PERM_TEMP_CODE =  a_tbl_update(i).PERM_TEMP_CODE,
          GRADE_CODE =  a_tbl_update(i).GRADE_CODE,
          OCCUPATION_LEVEL_CODE =  a_tbl_update(i).OCCUPATION_LEVEL_CODE,
          DISABLED_IND =  a_tbl_update(i).DISABLED_IND,     
          EMPLOYEE_LOCATION_CODE =  a_tbl_update(i).EMPLOYEE_LOCATION_CODE,
          EMPLOYEE_CLASS_CODE =  a_tbl_update(i).EMPLOYEE_CLASS_CODE,
          EFFECTIVE_DATE =  a_tbl_update(i).EFFECTIVE_DATE,
          LAST_UPDATED_DATE =  a_tbl_update(i).LAST_UPDATED_DATE
       where  EMPLOYEE_ID =  a_tbl_update(i).EMPLOYEE_ID;

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
		' '||a_tbl_update(g_error_index).EMPLOYEE_ID;

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := false;
    -- Check to see if Employee already present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   dwh_hr_foundation.FND_PS_EMPLOYEE
   where  EMPLOYEE_ID =  g_rec_out.EMPLOYEE_ID;

   if g_count = 1 then
      g_found := TRUE;
   end if;

  -- Check if insert of Employee_ID is already in insert array and change to put duplicate in update array
     if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).EMPLOYEE_ID = g_rec_out.EMPLOYEE_ID then
            g_found := TRUE;
         end if;
      end loop;
   end if;
  ---
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
--   if a_count > 1000 then
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;

      a_staging1    := a_empty_set_s1;
      a_staging2    := a_empty_set_s2;

      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      a_count_stg   := 0;

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
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Update hr_employee data ex PS for S4S STARTED AT '||
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


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_STG_S4S_EMPLOYEE;
    fetch c_STG_S4S_EMPLOYEE bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in                := a_stg_input(i);
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
         if g_hospital = 'Y' then
            local_write_hospital;
         else
            local_write_output;
         end if;
      end loop;
    fetch c_STG_S4S_EMPLOYEE bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_STG_S4S_EMPLOYEE;
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


END "WH_FND_HR_070U";
/
show errors