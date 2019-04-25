--------------------------------------------------------
--  DDL for Procedure WH_FND_HR_304U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_FOUNDATION"."WH_FND_HR_304U" (p_forall_limit in integer,p_success out boolean) AS


--**************************************************************************************************
--  Date:        Jan 2012
--  Author:      Alastair de Wet
--  Purpose:     Create HR SKILLS DEV FACT table in the foundation layer
--               with input ex staging table from HR.
--  Tables:      Input  - stg_ps_skill_dev_spend_cpy
--               Output - fnd_hr_bee_skill_dev_spend
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

--      Date      : Jun 2014
--     Changed by : Kgomotso Lehabe
--     Purpose    :  Only load records that are in the Employee master because this interface combines Employees and POIs
--                : Add course start date to the join
--
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
g_hospital           char(1)       := 'N';
g_hospital_text      stg_ps_skill_dev_spend_hsp.sys_process_msg%type;
g_rec_out            fnd_hr_bee_skill_dev_spend%rowtype;
g_rec_in             stg_ps_skill_dev_spend_cpy%rowtype;
g_found              boolean;
g_count              number        :=  0;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_HR_304U';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_fnd;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE HR SKILL DEV EX PEOPLE SOFT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_ps_skill_dev_spend_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_hr_bee_skill_dev_spend%rowtype index by binary_integer;
type tbl_array_u is table of fnd_hr_bee_skill_dev_spend%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_ps_skill_dev_spend_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_ps_skill_dev_spend_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_ps_skill_dev_spend is
   select *
   from stg_ps_skill_dev_spend_cpy
   where sys_process_code = 'N'
    and employee_id in
      (select employee_id from dwh_hr_performance.dim_hr_employee)
     and employee_id not in
       (select poi_id from dwh_hr_performance.dim_hr_poi a,
         dwh_hr_performance.dim_hr_employee b
        where a.poi_id = b.employee_id
        and a.poi_status ='A'
        and b.employee_status_code <>'A')
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                := 'N';
   g_rec_out.employee_id                     := g_rec_in.employee_id;
   g_rec_out.course_code                     := g_rec_in.course_code;
   g_rec_out.course_start_date               := g_rec_in.course_start_date;
   g_rec_out.course_end_date                 := g_rec_in.course_end_date;
   g_rec_out.session_code                    := g_rec_in.session_code;
   g_rec_out.vendor_cost                     := g_rec_in.vendor_cost;
   g_rec_out.facillity_cost                  := g_rec_in.facillity_cost;
   g_rec_out.equipment_cost                  := g_rec_in.equipment_cost;
   g_rec_out.instruction_cost                := g_rec_in.instruction_cost;
   g_rec_out.expences_cost                   := g_rec_in.expences_cost;
   g_rec_out.accomodation_cost               := g_rec_in.accomodation_cost;
   g_rec_out.admin_cost                      := g_rec_in.admin_cost;
   g_rec_out.attachment_cost                 := g_rec_in.attachment_cost;
   g_rec_out.external_cost                   := g_rec_in.external_cost;
   g_rec_out.course_fees                     := g_rec_in.course_fees;
   g_rec_out.external_cater_cost             := g_rec_in.external_cater_cost;
   g_rec_out.external_venue_cost             := g_rec_in.external_venue_cost;
   g_rec_out.flight_acc                      := g_rec_in.flight_acc;
   g_rec_out.flight_cost                     := g_rec_in.flight_cost;
   g_rec_out.internal_cater_cost             := g_rec_in.internal_cater_cost;
   g_rec_out.internal_venue_cost             := g_rec_in.internal_venue_cost;
   g_rec_out.material_cost                   := g_rec_in.material_cost;
   g_rec_out.once_off_cost                   := g_rec_in.once_off_cost;
   g_rec_out.sign_language_cost              := g_rec_in.sign_language_cost;
   g_rec_out.workshop_cost                   := g_rec_in.workshop_cost;
   g_rec_out.other_cost                      := g_rec_in.other_cost;
   g_rec_out.effective_date                  := g_rec_in.effective_date;

   g_rec_out.last_updated_date          := g_date;



   if not dwh_hr_valid.fnd_hr_employee(g_rec_out.employee_id) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_employee_not_found;
     l_text          := dwh_hr_constants.vc_employee_not_found||g_rec_out.employee_id ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_hr_valid.fnd_hr_course(g_rec_out.course_code) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_hr_constants.vc_invalid_course;
     l_text          := dwh_hr_constants.vc_invalid_course||g_rec_out.course_code ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   exception
      when others then
       l_message := dwh_hr_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin

   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into stg_ps_skill_dev_spend_hsp values g_rec_in;
   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_hr_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
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
       insert into fnd_hr_bee_skill_dev_spend values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).employee_id ;
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
       update fnd_hr_bee_skill_dev_spend
       set    course_end_date                 = a_tbl_update(i).course_end_date,
              session_code                    = a_tbl_update(i).session_code,
              vendor_cost                     = a_tbl_update(i).vendor_cost,
              facillity_cost                  = a_tbl_update(i).facillity_cost,
              equipment_cost                  = a_tbl_update(i).equipment_cost,
              instruction_cost                = a_tbl_update(i).instruction_cost,
              expences_cost                   = a_tbl_update(i).expences_cost,
              accomodation_cost               = a_tbl_update(i).accomodation_cost,
              admin_cost                      = a_tbl_update(i).admin_cost,
              attachment_cost                 = a_tbl_update(i).attachment_cost,
              external_cost                   = a_tbl_update(i).external_cost,
              course_fees                     = a_tbl_update(i).course_fees,
              external_cater_cost             = a_tbl_update(i).external_cater_cost,
              external_venue_cost             = a_tbl_update(i).external_venue_cost,
              flight_acc                      = a_tbl_update(i).flight_acc,
              flight_cost                     = a_tbl_update(i).flight_cost,
              internal_cater_cost             = a_tbl_update(i).internal_cater_cost,
              internal_venue_cost             = a_tbl_update(i).internal_venue_cost,
              material_cost                   = a_tbl_update(i).material_cost,
              once_off_cost                   = a_tbl_update(i).once_off_cost,
              sign_language_cost              = a_tbl_update(i).sign_language_cost,
              workshop_cost                   = a_tbl_update(i).workshop_cost,
              other_cost                      = a_tbl_update(i).other_cost,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  employee_id                     = a_tbl_update(i).employee_id  and
              course_code                     = a_tbl_update(i).course_code and
              course_start_date               = a_tbl_update(i).course_start_date and
              effective_date                  = a_tbl_update(i).effective_date;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

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
                       ' '||a_tbl_update(g_error_index).employee_id ;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update stg_ps_skill_dev_spend_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_staging_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   fnd_hr_bee_skill_dev_spend
   where  employee_id          = g_rec_out.employee_id  and
          course_code          = g_rec_out.course_code   and
          course_start_date    = g_rec_out.course_start_date and
          effective_date       = g_rec_out.effective_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).employee_id          = g_rec_out.employee_id  and
            a_tbl_insert(i).course_code          = g_rec_out.course_code  and
            a_tbl_insert(i).course_start_date    = g_rec_out.course_start_date and
            a_tbl_insert(i).effective_date       = g_rec_out.effective_date then
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
--   if a_count > 1000 then
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;
      local_bulk_staging_update;

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
       l_message := dwh_hr_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
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
    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_hr_bee_skill_dev_spend EX hr STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_ps_skill_dev_spend;
    fetch c_stg_ps_skill_dev_spend bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_hr_constants.vc_log_records_processed||
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
    fetch c_stg_ps_skill_dev_spend bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_ps_skill_dev_spend;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_hr_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_run_completed ||sysdate;
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

END WH_FND_HR_304U;
