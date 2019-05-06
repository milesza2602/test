--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_014E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_014E" 
        (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        10 July 2014
--  Author:      Lwazi Ntloko
--  Purpose:     Create S4S labour role hierachy Data on the foundation layer
--               with input from the Labour hierachy staging tables.
--
--  DIM Tables:  Input  - fnd_s4s_LABOUR_HIERARCHY
--               Output - fnd_s4s_labour_role
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  5 September - WL - amended
--  20 November 2018 - S4S Redo (LK,SS,FDV)
--
--  Naming conventions
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            fnd_s4s_labour_role%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_014E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD S4S labour role ss';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_s4s_labour_role%rowtype index by binary_integer;
type tbl_array_u is table of fnd_s4s_labour_role%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
G_COUNT NUMBER;


cursor c_fnd_s4s_labour_hierarchy is
with lr_list as
(select 
    labour_role_id,
    labour_role_name,
    s4s_business_unit_no,
    s4s_business_unit_name,
    workgroup_id,
    workgroup_name,
    jobgroup_id,
    jobgroup_name,
    job_id,
    job_name,
    min(effective_from_date) effective_from_date , 
    max(effective_to_date)  date_candidate
from fnd_s4s_labour_hierarchy
group by labour_role_id,
    labour_role_name,
    s4s_business_unit_no,
    s4s_business_unit_name,
    workgroup_id,
    workgroup_name,
    jobgroup_id,
    jobgroup_name,
    job_id,
    job_name),
Newtable as (    
select ll.*, 
      LAG(effective_from_date-1, 1, date_candidate) OVER (PARTITION BY labour_role_id,job_id,effective_from_date ORDER BY labour_role_id,job_id, effective_from_date desc) effective_to_date
from lr_list ll
order by job_id, effective_from_date desc)
select labour_role_id,
    labour_role_name,
    s4s_business_unit_no,
    s4s_business_unit_name,
    workgroup_id,
    workgroup_name,
    jobgroup_id,
    jobgroup_name,
    job_id,
    job_name,
    effective_from_date,
    effective_to_date,
    g_date
from Newtable;

g_rec_in             c_fnd_s4s_labour_hierarchy%rowtype;

   -- for input bulk collect --
type stg_array is table of c_fnd_s4s_labour_hierarchy%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
   g_rec_out.labour_role_id          := g_rec_in.labour_role_id;
   g_rec_out.labour_role_name        := g_rec_in.labour_role_name;
   g_rec_out.jobgroup_id	         := g_rec_in.jobgroup_id;
   g_rec_out.jobgroup_name           := g_rec_in.jobgroup_name;
   g_rec_out.job_id	                 := g_rec_in.job_id;
   g_rec_out.job_name                := g_rec_in.job_name;
   g_rec_out.workgroup_id            := g_rec_in.workgroup_id;
   g_rec_out.workgroup_name	         := g_rec_in.workgroup_name;
   g_rec_out.s4s_business_unit_no	 := g_rec_in.s4s_business_unit_no;
   g_rec_out.s4s_business_unit_name	 := g_rec_in.s4s_business_unit_name;
   g_rec_out.effective_from_date	 := g_rec_in.effective_from_date;
   g_rec_out.effective_to_date	     := g_rec_in.effective_to_date;
   g_rec_out.last_updated_date       := g_date;

   exception
    when others then
     l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
     dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;

end local_address_variables;
--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into fnd_s4s_labour_role values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).labour_role_id ||
                       ' '||a_tbl_insert(g_error_index).job_id ||
                       ' '||a_tbl_insert(g_error_index).jobgroup_id ||
                       ' '||a_tbl_insert(g_error_index).workgroup_id ||
                       ' '||a_tbl_insert(g_error_index).s4s_business_unit_no ||                       
                       ' '||a_tbl_insert(g_error_index).effective_from_date || 
                       ' '||'INS';
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
        update fnd_s4s_labour_role
           set labour_role_name         = a_tbl_update(i).labour_role_name,
               job_name                 = a_tbl_update(i).job_name,
--               jobgroup_id              = a_tbl_update(i).jobgroup_id,
               jobgroup_name            = a_tbl_update(i).jobgroup_name,
--               workgroup_id             = a_tbl_update(i).workgroup_id,          
               workgroup_name           = a_tbl_update(i).workgroup_name,
--	           s4s_business_unit_no	    = a_tbl_update(i).s4s_business_unit_no,
	           s4s_business_unit_name   = a_tbl_update(i).s4s_business_unit_name,
--               effective_from_date      = a_tbl_update(i).effective_from_date,
               effective_to_date        = a_tbl_update(i).effective_to_date,
               last_updated_date        = a_tbl_update(i).last_updated_date
         where labour_role_id       = a_tbl_update(i).labour_role_id
           and job_id               = a_tbl_update(i).job_id
           and jobgroup_id          = a_tbl_update(i).jobgroup_id
           and workgroup_id         = a_tbl_update(i).workgroup_id
           and s4s_business_unit_no = a_tbl_update(i).s4s_business_unit_no
           and effective_from_date  = a_tbl_update(i).effective_from_date
         ;
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
                       ' '||a_tbl_update(g_error_index).labour_role_id ||
                       ' '||a_tbl_update(g_error_index).job_id ||
                       ' '||a_tbl_update(g_error_index).jobgroup_id ||
                       ' '||a_tbl_update(g_error_index).workgroup_id ||
                       ' '||a_tbl_update(g_error_index).s4s_business_unit_no ||    
                       ' '||a_tbl_update(g_error_index).effective_from_date ;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := false;
   -- check to see if workgroup_id is present on table and update/insert accordingly
   select count(1)
     into g_count
     from fnd_s4s_labour_role
    where labour_role_id        = g_rec_out.labour_role_id
      and job_id                = g_rec_out.job_id    
      and jobgroup_id           = g_rec_out.jobgroup_id
	    and workgroup_id          = g_rec_out.workgroup_id
	    and s4s_business_unit_no  = g_rec_out.s4s_business_unit_no
      and effective_from_date   = g_rec_out.effective_from_date
    ;

   if g_count = 1 then
      g_found := true;
   end if;

-- check if insert of workroup Id is lready in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).labour_role_id       = g_rec_out.labour_role_id
        and a_tbl_insert(i).job_id               = g_rec_out.job_id
        and a_tbl_insert(i).jobgroup_id          = g_rec_out.jobgroup_id
        and a_tbl_insert(i).workgroup_id         = g_rec_out.workgroup_id
        and a_tbl_insert(i).s4s_business_unit_no = g_rec_out.s4s_business_unit_no
        and a_tbl_insert(i).effective_from_date  = g_rec_out.effective_from_date      
       then
            g_found := true;
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
-- main process
--**************************************************************************************************
begin
  if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;

    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'loading S4S labour role data started at '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'batch date being processed is:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- bulk fetch loop controlling main program execution
--**************************************************************************************************
   open c_fnd_s4s_labour_hierarchy;
    fetch c_fnd_s4s_labour_hierarchy bulk collect into a_stg_input limit g_forall_limit;
       while a_stg_input.count > 0
    loop
        for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;


         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
         fetch c_fnd_s4s_labour_hierarchy bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_s4s_labour_hierarchy;
--**************************************************************************************************
-- at end write out what remains in the arrays at end of program
--**************************************************************************************************

   local_bulk_insert;   
   local_bulk_update;



--**************************************************************************************************
-- write final log data
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
       --dbms_output.put_line('MEssage 2 : '|| l_message);


       raise;


END WH_FND_S4S_014E;
