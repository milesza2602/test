--------------------------------------------------------
--  DDL for Procedure WH_PRF_NRT_002U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_NRT_002U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        May 2014
--  Author:      Alastair de Wet
--  Purpose:     Create Near real time schedule fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_nrt_staff_clocking
--               Output - cust_nrt_staff_time_mng
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_rec_out            cust_nrt_staff_time_mng%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_NRT_002U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD OF STAFF CLOCKING EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of cust_nrt_staff_time_mng%rowtype index by binary_integer;
type tbl_array_u is table of cust_nrt_staff_time_mng%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_nrt_staff_clocking_om is
     select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*,
              dl.location_name,
              de.first_name,
              de.last_name,
              dj.job_name
     from     dwh_cust_foundation.fnd_nrt_staff_clocking fnd,
              dim_location dl,
              dim_hr_employee de,
              dim_nrt_job dj
     where    processed_ind = 'N'   and
              to_char(fnd.employee_no) = de.employee_id(+)  and
              fnd.location_no          = dl.location_no(+)  and
              fnd.job_id               = dj.job_id(+)
     order by
              fnd.location_no,fnd.employee_no,fnd.clock_time;

-- order by only where sequencing is essential to the correct loading of data

-- For input bulk collect --
type stg_array is table of c_fnd_nrt_staff_clocking_om%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_nrt_staff_clocking_om%rowtype;



--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

--    g_rec_out.tran_date                       := trunc(sysdate);
    g_rec_out.tran_date                       := trunc( g_rec_in.clock_time_rounded);
    g_rec_out.location_no                     := g_rec_in.location_no;
    g_rec_out.employee_no                     := g_rec_in.employee_no;
    g_rec_out.workgroup_id                    := 1000000;
    g_rec_out.job_id                          := g_rec_in.job_id;
    g_rec_out.location_name                   := g_rec_in.location_name;
    g_rec_out.employee_first_name             := g_rec_in.first_name;
    g_rec_out.employee_last_name              := g_rec_in.last_name;
    g_rec_out.workgroup_name                  := 'UNKNOWN WORKGROUP';
    g_rec_out.job_name                        := g_rec_in.job_name;
    g_rec_out.shift_start                     := '';
    g_rec_out.shift_end                       := '';
    g_rec_out.last_updated_date               := g_date;



    case
       when g_rec_in.clock_type = 's' then
       g_rec_out.clock_time_in := g_rec_in.clock_time ;
       else g_rec_out.clock_time_in := null ;
    end case;
    case
       when g_rec_in.clock_type = 't'   then
       g_rec_out.clock_time_out := g_rec_in.clock_time ;
       else g_rec_out.clock_time_out := null ;
    end case;




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
       insert into cust_nrt_staff_time_mng values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).location_no||
                       ' '||a_tbl_insert(g_error_index).employee_no;
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

       update cust_nrt_staff_time_mng
       set    clock_time_in                   =
              case  when  a_tbl_update(i).clock_time_in   is not null and
--                          a_tbl_update(i).clock_time_in   < nvl(clock_time_in,to_date(a_tbl_update(i).clock_time_in, 'DD/MON/YYYY')|| ' 23:59:00')
                          a_tbl_update(i).clock_time_in   < nvl(clock_time_in,sysdate + 30)
                    then  a_tbl_update(i).clock_time_in else clock_time_in end,
              clock_time_out                  =
              case  when  a_tbl_update(i).clock_time_out   is not null and
--                          a_tbl_update(i).clock_time_out   > nvl(clock_time_out,to_date(a_tbl_update(i).clock_time_out, 'DD/MON/YYYY')|| ' 00:00:00')
                          a_tbl_update(i).clock_time_out   > nvl(clock_time_out,sysdate - 30)
                    then  a_tbl_update(i).clock_time_out else clock_time_out end,

              last_updated_date               = a_tbl_update(i).last_updated_date
       where  location_no                     = a_tbl_update(i).location_no  and
              employee_no                     = a_tbl_update(i).employee_no  and
              tran_date                       = a_tbl_update(i).tran_date ;


       g_recs_updated  := g_recs_updated  + a_tbl_update.count;


     commit;

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
                       ' '||a_tbl_update(g_error_index).location_no||
                       ' '||a_tbl_update(g_error_index).employee_no;
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
   from   cust_nrt_staff_time_mng
   where  tran_date        = g_rec_out.tran_date  and
          location_no      = g_rec_out.location_no  and
          employee_no      = g_rec_out.employee_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).tran_date    = g_rec_out.tran_date   and
            a_tbl_insert(i).location_no  = g_rec_out.location_no and
            a_tbl_insert(i).employee_no  = g_rec_out.employee_no then
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

    l_text := 'LOAD OF cust_nrt_staff_time_mng EX FOUNDATION STARTED AT '||
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
    open c_fnd_nrt_staff_clocking_om;
    fetch c_fnd_nrt_staff_clocking_om bulk collect into a_stg_input limit g_forall_limit;
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

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_nrt_staff_clocking_om bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_nrt_staff_clocking_om;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;

    l_text := 'UPDATE PROCESSED_IND STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    update dwh_cust_foundation.fnd_nrt_staff_clocking
    set    processed_ind = 'Y'
    where  processed_ind = 'N';

    awx_job_control.complete_job_status('FND_NRT_STAFF_CLOCKING');
    l_text := 'Set AWX_JOB_STATUS = Y on '||  'FND_NRT_STAFF_CLOCKING';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
end wh_prf_nrt_002u;
