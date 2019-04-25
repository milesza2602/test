--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_060U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_060U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        November 2014
--  Author:      Lwazi Ntloko
--  Purpose:     Load S4S employee exception information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EXCEPTION_CATEGORY
--               Output   - DIM_EXCEPTION_CATEGORY
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
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

g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;

g_rec_out            DIM_EXCEPTION_CATEGORY%rowtype;

g_found              boolean;
g_insert_rec         boolean;
g_invalid_plan_type_no boolean;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_DESC        sys_dwh_errlog.LOG_PROCEDURE_NAME%type    := 'WH_PRF_S4S_060U';
l_DESC               sys_dwh_log.LOG_NAME%type                 := dwh_constants.vc_LOG_NAME_bam_md;
l_system_DESC        sys_dwh_log.LOG_SYSTEM_NAME%type          := dwh_constants.vc_LOG_SYSTEM_NAME_pln_fnd;
l_script_DESC        sys_dwh_log.LOG_SCRIPT_NAME%type          := dwh_constants.vc_log_script_pln_fnd_md;
l_procedure_DESC     sys_dwh_log.LOG_PROCEDURE_NAME%type       := l_module_DESC;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.LOG_DESCRIPTION%type  := 'LOAD EXCEPTION_CATEGORY dimenstion Data';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DIM_EXCEPTION_CATEGORY%rowtype index by binary_integer;
type tbl_array_u is table of DIM_EXCEPTION_CATEGORY%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_EXCEPTION_CATEGORY is
select 
      EXCEPTION_CATEGORY_DESC,
      EXCEPTION_CATEGORY_ID
from DWH_FOUNDATION.FND_S4S_EXCEPTION_CATEGORY
;

g_rec_in    c_EXCEPTION_CATEGORY%rowtype;

-- For input bulk collect --
type stg_array is table of c_EXCEPTION_CATEGORY%rowtype;
a_stg_input      stg_array;

---------  SK1_EXCEPTION_CATEGORY_ID, SK1_EXCEPTION_CATEGORY_ID

-----------------------------------------------------------------------------------------------------
-- order by only where sequencing is essential to the correct loading of data
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

	g_rec_out.EXCEPTION_CATEGORY_ID   := g_rec_in.EXCEPTION_CATEGORY_ID;
	g_rec_out.EXCEPTION_CATEGORY_DESC  := g_rec_in.EXCEPTION_CATEGORY_DESC;
  g_rec_out.TOTAL := 'TOTAL';
  g_rec_out.TOTAL_DESC := 'ALL EXCEPTION_CATEGORY ID';
	g_rec_out.last_updated_date  := g_date;

  exception
    when others then
     l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
     dwh_log.record_error(l_module_DESC,sqlcode,l_message);
    raise;

end local_address_variables;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into DIM_EXCEPTION_CATEGORY values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_DESC,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                                     ' '||a_tbl_insert(g_error_index).EXCEPTION_CATEGORY_ID;
          dwh_log.record_error(l_module_DESC,sqlcode,l_message);
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
        update DIM_EXCEPTION_CATEGORY
            set EXCEPTION_CATEGORY_DESC = a_tbl_update(i).EXCEPTION_CATEGORY_DESC,
                TOTAL = a_tbl_update(i).TOTAL,
                TOTAL_DESC = a_tbl_update(i).TOTAL_DESC,
                --EXCEPTION_CATEGORY_ID = a_tbl_update(i).EXCEPTION_CATEGORY_ID,
                last_updated_date = a_tbl_update(i).LAST_UPDATED_DATE
        where EXCEPTION_CATEGORY_ID = a_tbl_update(i).EXCEPTION_CATEGORY_ID;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_DESC,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).SK1_EXCEPTION_CATEGORY_ID||
                       ' '||a_tbl_insert(g_error_index).EXCEPTION_CATEGORY_ID;
          dwh_log.record_error(l_module_DESC,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************

procedure local_write_output as
begin
   g_found := FALSE;
   -- Check to see if Business Unit is present on table and update/insert accordingly
   select count(1)
     into g_count
     from DIM_EXCEPTION_CATEGORY
    where EXCEPTION_CATEGORY_ID = g_rec_out.EXCEPTION_CATEGORY_ID;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of Business Unit number is already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).EXCEPTION_CATEGORY_ID = g_rec_out.EXCEPTION_CATEGORY_ID then
            g_found := TRUE;
         end if;
      end loop;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
	  g_rec_out.SK1_EXCEPTION_CATEGORY_ID := LABOUR_HIERACHY_SEQ.nextval;
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;
--~~~~~~~~~~`````````````````````****************************````````````````````````~~~~~~~~~~~~~~~~~~~
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
       dwh_log.record_error(l_module_DESC,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_DESC,sqlcode,l_message);
       raise;

end local_write_output;
--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
  if p_forall_limit is not null and p_forall_limit > 1000  then ------dwh_constants.vc_forall_minimum
       g_forall_limit := p_forall_limit;
    end if;

	dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);

    l_text := 'LOADING PRF S4S Business unit data'|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);

    dwh_log.insert_log_summary(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);


-- hardcoding batch_date for testing
--g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
   open c_EXCEPTION_CATEGORY;
    fetch c_EXCEPTION_CATEGORY bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 1000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

	   local_address_variables;
       local_write_output;

      end loop;
    fetch c_EXCEPTION_CATEGORY bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_EXCEPTION_CATEGORY;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

dwh_log.update_log_summary(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_text);
    commit;
    p_success := true;
  exception

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_DESC,sqlcode,l_message);
       dwh_log.update_log_summary(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_DESC,sqlcode,l_message);
       dwh_log.update_log_summary(l_DESC,l_system_DESC,l_script_DESC,l_procedure_DESC,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_S4S_060U;
