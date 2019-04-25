--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_070U_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_070U_NEW" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        November 2011
--  Author:      Alastair de Wet
--  Purpose:     Create dim_employee dimention table in the performance layer
--               with added value ex foundation layer fnd_hr_employee.
--  Tables:      Input  - fnd_hr_employee
--               Output - dim_employee
--  Packages:    constants, dwh_log, dwh_valid
--
--CREATE TABLE DWH_HR_PERFORMANCE.DIM_EMPLOYEE_BACKUP_WL
--AS SELECT * FROM DWH_HR_PERFORMANCE.DIM_EMPLOYEE
--  Maintenance:
--      Date      : Jun 2014
--     Changed by : Kgomotso Lehabe
--     Purpose     :Add columns to dim_employee
--                 Employee_class_code
--                 SK1_employee_class_code
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
g_rec_out            dim_employee%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_070U_NEW';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE dim_employee EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;


-- For input bulk collect --

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_employee%rowtype index by binary_integer;
type tbl_array_u is table of dim_employee%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


/*select emp.sk1_employee_id, xt.employee_id,
                    (CASE WHEN xt.employee_status_code in upper('a')THEN 1
                        WHEN xt.employee_status_code in upper('l') THEN 1
                        WHEN xt.employee_status_code in upper('p') THEN 1
                        WHEN xt.employee_status_code in upper('s') THEN 1
                        WHEN xt.employee_status_code in upper('w') THEN 1  
                        ELSE 0
                        END) active_ind, ss.s4s_employee_type, ss.s4s_employee_workstatus 
      from dwh_hr_foundation.fnd_ps_employee xt
      full outer join dwh_hr_performance.dim_employee emp on xt.employee_id = emp.employee_id
      full outer join dwh_foundation.fnd_s4s_employee ss on xt.employee_id = ss.employee_id     
 */ 
cursor c_fnd_hr_employee is
   select PS.EMPLOYEE_ID
        ,PS.FIRST_NAME
        ,PS.LAST_NAME
        ,PS.first_name||' '||PS.last_name FULL_NAME
        ,PS.employee_id||' - '||PS.first_name||' '||PS.last_name EMPLOYEE_LONG_DESC
        ,nvl(cc.SK1_COMPANY_CODE,0) SK1_COMPANY_CODE
        ,PS.COMPANY_CODE
        ,nvl(SK1_DEPARTMENT_CODE,0) SK1_DEPARTMENT_CODE
        ,PS.DEPARTMENT_CODE
        ,nvl(SK1_EMPLOYEE_STATUS_CODE,0)  SK1_EMPLOYEE_STATUS_CODE
        ,PS.EMPLOYEE_STATUS_CODE
        ,PS.ID_NO
        ,nvl(SK1_GENDER_CODE,0) SK1_GENDER_CODE
        ,PS.GENDER_CODE
        ,nvl(SK1_RACE_CODE,0) SK1_RACE_CODE
        ,PS.RACE_CODE
        ,nvl(SK1_CITIZENSHIP_STATUS_CODE,0) SK1_CITIZENSHIP_STATUS_CODE
        ,PS.CITIZENSHIP_STATUS_CODE
        ,PS.TERMINATION_DATE
        ,PS.PERM_TEMP_CODE
        ,PS.GRADE_CODE
        ,nvl(SK1_OCCUPATION_LEVEL_CODE,0) SK1_OCCUPATION_LEVEL_CODE
        ,PS.OCCUPATION_LEVEL_CODE
  --      ,CASE WHEN board_member_ind = 1 or executive_director_ind = 1 or independent_director_ind = 1 or top_management_ind = 1 or other_top_management_ind  = 1 
  --            then  1 ELSE 0  end MANAGEMENT_CONTROL_IND
         , NULL MANAGEMENT_CONTROL_IND
        ,PS.DISABLED_IND
        ,NULL COMPENSATION_RATE
        ,nvl(SK1_EMPLOYEE_LOCATION_CODE,0) SK1_EMPLOYEE_LOCATION_CODE
        ,PS.EMPLOYEE_LOCATION_CODE
  --      ,case when employee_status_code  in ('A','L','P','S','W') then   1    else 0  end ACTIVE_IND
        ,NULL ACTIVE_IND
        ,NULL BOARD_MEMBER_IND
        ,NULL EXECUTIVE_DIRECTOR_IND
        ,NULL INDEPENDENT_DIRECTOR_IND
        ,NULL TOP_MANAGEMENT_IND
        ,NULL OTHER_TOP_MANAGEMENT_IND
        ,G_DATE LAST_UPDATED_DATE
        ,nvl(SK1_EMPLOYEE_CLASS_CODE,0) SK1_EMPLOYEE_CLASS_CODE
        ,PS.EMPLOYEE_CLASS_CODE
     --   ,NULL S4S_EMPLOYEE_TYPE
    --    ,NULL S4S_EMPLOYEE_WORKSTATUS
    from dwh_hr_foundation.fnd_ps_employee ps 
		left outer join dwh_hr_performance.dim_hr_company cc 
       on ps.company_code = cc.company_code
		left outer join dwh_hr_performance.dim_hr_department dp 
       on ps.department_code = dp.department_code 
       and ps.company_code = dp.company_code
    left outer join dwh_hr_performance.dim_HR_employee_status esc 
        on ps.employee_status_code = esc.employee_status_code
		left outer join dwh_hr_performance.dim_hr_gender gc 
       on ps.gender_code = gc.gender_code
		left outer join dwh_hr_performance.dim_hr_race rc 
       on ps.race_code = rc.race_code
		left outer join dwh_hr_performance.dim_hr_citizenship_status csc 
       on ps.citizenship_status_code = csc.citizenship_status_code
		left outer join dwh_hr_performance.dim_hr_occupation_level olc 
       on ps.occupation_level_code = olc.occupation_level_code
		left outer join dwh_hr_performance.dim_HR_employee_location elc 
        on ps.employee_location_code = elc.employee_location_code and ps.company_code = elc.company_code 
    LEFT OUTER JOIN dwh_hr_performance.dim_hr_employee_class HEC
    ON   HEC.employee_class_code          = PS.employee_class_code 
    ;
  g_rec_in c_fnd_hr_employee%rowtype;
  -- For input bulk collect --
type stg_array
IS
  TABLE OF c_fnd_hr_employee%rowtype;
  a_stg_input stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

G_REC_OUT.EMPLOYEE_ID                 := G_REC_IN.EMPLOYEE_ID;
G_REC_OUT.FIRST_NAME                  := G_REC_IN.FIRST_NAME;
G_REC_OUT.LAST_NAME                   := G_REC_IN.LAST_NAME;
G_REC_OUT.FULL_NAME                   := G_REC_IN.FULL_NAME;
G_REC_OUT.EMPLOYEE_LONG_DESC          := G_REC_IN.EMPLOYEE_LONG_DESC;
G_REC_OUT.SK1_COMPANY_CODE            := G_REC_IN.SK1_COMPANY_CODE;
G_REC_OUT.COMPANY_CODE                := G_REC_IN.COMPANY_CODE;
G_REC_OUT.SK1_DEPARTMENT_CODE         := G_REC_IN.SK1_DEPARTMENT_CODE;
G_REC_OUT.DEPARTMENT_CODE             := G_REC_IN.DEPARTMENT_CODE;
G_REC_OUT.SK1_EMPLOYEE_STATUS_CODE    := G_REC_IN.SK1_EMPLOYEE_STATUS_CODE;
G_REC_OUT.EMPLOYEE_STATUS_CODE        := G_REC_IN.EMPLOYEE_STATUS_CODE;
G_REC_OUT.ID_NO                       := G_REC_IN.ID_NO;
G_REC_OUT.SK1_GENDER_CODE             := G_REC_IN.SK1_GENDER_CODE;
G_REC_OUT.GENDER_CODE                 := G_REC_IN.GENDER_CODE;
G_REC_OUT.SK1_RACE_CODE               := G_REC_IN.SK1_RACE_CODE;
G_REC_OUT.RACE_CODE                   := G_REC_IN.RACE_CODE;
G_REC_OUT.SK1_CITIZENSHIP_STATUS_CODE := G_REC_IN.SK1_CITIZENSHIP_STATUS_CODE;
G_REC_OUT.CITIZENSHIP_STATUS_CODE     := G_REC_IN.CITIZENSHIP_STATUS_CODE;
G_REC_OUT.TERMINATION_DATE            := G_REC_IN.TERMINATION_DATE;
G_REC_OUT.PERM_TEMP_CODE              := G_REC_IN.PERM_TEMP_CODE;
G_REC_OUT.GRADE_CODE                  := G_REC_IN.GRADE_CODE;
G_REC_OUT.SK1_OCCUPATION_LEVEL_CODE   := G_REC_IN.SK1_OCCUPATION_LEVEL_CODE;
G_REC_OUT.OCCUPATION_LEVEL_CODE       := G_REC_IN.OCCUPATION_LEVEL_CODE;
G_REC_OUT.MANAGEMENT_CONTROL_IND      := G_REC_IN.MANAGEMENT_CONTROL_IND;
G_REC_OUT.DISABLED_IND                := G_REC_IN.DISABLED_IND;
G_REC_OUT.COMPENSATION_RATE           := G_REC_IN.COMPENSATION_RATE;
G_REC_OUT.SK1_EMPLOYEE_LOCATION_CODE  := G_REC_IN.SK1_EMPLOYEE_LOCATION_CODE;
G_REC_OUT.EMPLOYEE_LOCATION_CODE      := G_REC_IN.EMPLOYEE_LOCATION_CODE;
G_REC_OUT.ACTIVE_IND                  := G_REC_IN.ACTIVE_IND;
G_REC_OUT.BOARD_MEMBER_IND            := G_REC_IN.BOARD_MEMBER_IND;
G_REC_OUT.EXECUTIVE_DIRECTOR_IND      := G_REC_IN.EXECUTIVE_DIRECTOR_IND;
G_REC_OUT.INDEPENDENT_DIRECTOR_IND    := G_REC_IN.INDEPENDENT_DIRECTOR_IND;
G_REC_OUT.TOP_MANAGEMENT_IND          := G_REC_IN.TOP_MANAGEMENT_IND;
G_REC_OUT.OTHER_TOP_MANAGEMENT_IND    := G_REC_IN.OTHER_TOP_MANAGEMENT_IND;
G_REC_OUT.LAST_UPDATED_DATE           := G_REC_IN.LAST_UPDATED_DATE;
G_REC_OUT.SK1_EMPLOYEE_CLASS_CODE     := G_REC_IN.SK1_EMPLOYEE_CLASS_CODE;
G_REC_OUT.EMPLOYEE_CLASS_CODE         := G_REC_IN.EMPLOYEE_CLASS_CODE;
--G_REC_OUT.S4S_EMPLOYEE_TYPE           := G_REC_IN.S4S_EMPLOYEE_TYPE;
--G_REC_OUT.S4S_EMPLOYEE_WORKSTATUS     := G_REC_IN.S4S_EMPLOYEE_WORKSTATUS;


--ADD SK1 VALUES TO OUTPUT EMPLOYEE MASTER


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
      insert into dim_employee values a_tbl_insert(i);
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
      update  dim_employee
       set    FIRST_NAME =  a_tbl_update(i).FIRST_NAME,
              LAST_NAME =  a_tbl_update(i).LAST_NAME,
              FULL_NAME =  a_tbl_update(i).FULL_NAME,
              EMPLOYEE_LONG_DESC =  a_tbl_update(i).EMPLOYEE_LONG_DESC,
              SK1_COMPANY_CODE =  a_tbl_update(i).SK1_COMPANY_CODE,
              COMPANY_CODE =  a_tbl_update(i).COMPANY_CODE,
              SK1_DEPARTMENT_CODE =  a_tbl_update(i).SK1_DEPARTMENT_CODE,
              DEPARTMENT_CODE =  a_tbl_update(i).DEPARTMENT_CODE,
              SK1_EMPLOYEE_STATUS_CODE =  a_tbl_update(i).SK1_EMPLOYEE_STATUS_CODE,
              EMPLOYEE_STATUS_CODE =  a_tbl_update(i).EMPLOYEE_STATUS_CODE,
              ID_NO =  a_tbl_update(i).ID_NO,
              SK1_GENDER_CODE =  a_tbl_update(i).SK1_GENDER_CODE,
              GENDER_CODE =  a_tbl_update(i).GENDER_CODE,
              SK1_RACE_CODE =  a_tbl_update(i).SK1_RACE_CODE,
              RACE_CODE =  a_tbl_update(i).RACE_CODE,
              SK1_CITIZENSHIP_STATUS_CODE =  a_tbl_update(i).SK1_CITIZENSHIP_STATUS_CODE,
              CITIZENSHIP_STATUS_CODE =  a_tbl_update(i).CITIZENSHIP_STATUS_CODE,
              TERMINATION_DATE =  a_tbl_update(i).TERMINATION_DATE,
              PERM_TEMP_CODE =  a_tbl_update(i).PERM_TEMP_CODE,
              GRADE_CODE =  a_tbl_update(i).GRADE_CODE,
              SK1_OCCUPATION_LEVEL_CODE =  a_tbl_update(i).SK1_OCCUPATION_LEVEL_CODE,
              OCCUPATION_LEVEL_CODE =  a_tbl_update(i).OCCUPATION_LEVEL_CODE,
              MANAGEMENT_CONTROL_IND =  a_tbl_update(i).MANAGEMENT_CONTROL_IND,
              DISABLED_IND =  a_tbl_update(i).DISABLED_IND,
              COMPENSATION_RATE =  a_tbl_update(i).COMPENSATION_RATE,
              SK1_EMPLOYEE_LOCATION_CODE =  a_tbl_update(i).SK1_EMPLOYEE_LOCATION_CODE,
              EMPLOYEE_LOCATION_CODE =  a_tbl_update(i).EMPLOYEE_LOCATION_CODE,
              ACTIVE_IND =  a_tbl_update(i).ACTIVE_IND,
              BOARD_MEMBER_IND =  a_tbl_update(i).BOARD_MEMBER_IND,
              EXECUTIVE_DIRECTOR_IND =  a_tbl_update(i).EXECUTIVE_DIRECTOR_IND,
              INDEPENDENT_DIRECTOR_IND =  a_tbl_update(i).INDEPENDENT_DIRECTOR_IND,
              TOP_MANAGEMENT_IND =  a_tbl_update(i).TOP_MANAGEMENT_IND,
              OTHER_TOP_MANAGEMENT_IND =  a_tbl_update(i).OTHER_TOP_MANAGEMENT_IND,
              LAST_UPDATED_DATE =  a_tbl_update(i).LAST_UPDATED_DATE,
              SK1_EMPLOYEE_CLASS_CODE =  a_tbl_update(i).SK1_EMPLOYEE_CLASS_CODE,
              EMPLOYEE_CLASS_CODE =  a_tbl_update(i).EMPLOYEE_CLASS_CODE
       where  employee_id                     = a_tbl_update(i).employee_id ;

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
   from   dwh_HR_performance.dim_employee
    where  EMPLOYEE_ID             = g_rec_out.EMPLOYEE_ID ;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_EMPLOYEE_ID   := DWH_PERFORMANCE.LABOUR_HIERACHY_seq.nextval;
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

    l_text := 'LOAD OF dim_employee EX fnd_hr_employee STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_performance.DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_hr_employee;
    fetch c_fnd_hr_employee bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_hr_employee bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_hr_employee;
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

END WH_PRF_HR_070U_NEW;
