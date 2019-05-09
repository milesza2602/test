--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_014B
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_014B" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        10 July 2014
--  Author:      Lwazi Ntloko
--  Purpose:     Create S4S workgroup hierachy Data on the foundation layer
--               with input from the Labour hierachy staging tables.
--
--  DIM Tables:  Input  - fnd_s4s_LABOUR_HIERARCHY
--               Output - FND_S4S_workgroup
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  5 September - WL - amended
--  20 November 2018 - S4s Redo (LK,SS,FDV)
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
g_rec_out            FND_S4S_workgroup%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_014B';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD S4S workgroup';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of FND_S4S_workgroup%rowtype index by binary_integer;
type tbl_array_u is table of FND_S4S_workgroup%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
G_COUNT NUMBER;


cursor c_fnd_s4s_LABOUR_HIERARCHY is
with wg_list as
(select 
    s4s_business_unit_no,
    s4s_business_unit_name,
    workgroup_id,
    workgroup_name,
    workgroup_seq,
    min(effective_from_date) effective_from_date, 
    max(effective_to_date)   date_candidate
from fnd_s4s_labour_hierarchy
group by 
    s4s_business_unit_no,
    s4s_business_unit_name,
    workgroup_id,
    workgroup_name,
    workgroup_seq),
Newtable as (    
select wg.*, 
      LAG(effective_from_date-1, 1, date_candidate) OVER (PARTITION BY workgroup_id ORDER BY workgroup_id desc) effective_to_date
from wg_list wg
order by workgroup_id, effective_from_date desc)
select 
    s4s_business_unit_no,
    s4s_business_unit_name,
    workgroup_id,
    workgroup_name,
    workgroup_seq,
    effective_from_date,
    effective_to_date,
    case when effective_to_date > sysdate then 'Y' else 'N' end active_ind,
    g_date
from Newtable
;
g_rec_in             c_fnd_s4s_LABOUR_HIERARCHY%rowtype;

   -- for input bulk collect --
type stg_array is table of c_fnd_s4s_LABOUR_HIERARCHY%rowtype;
a_stg_input      stg_array;



--**************************************************************************************************
-- process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin


     g_rec_out.workgroup_id             := g_rec_in.workgroup_id;
	 g_rec_out.workgroup_name           := g_rec_in.workgroup_name;
	 g_rec_out.s4s_business_unit_no	    := g_rec_in.s4s_business_unit_no;
	 g_rec_out.s4s_business_unit_name	:= g_rec_in.s4s_business_unit_name;
     g_rec_out.workgroup_seq            := g_rec_in.workgroup_seq;
     g_rec_out.effective_from_date      := g_rec_in.effective_from_date;
     g_rec_out.effective_to_date        := g_rec_in.effective_to_date;
     g_rec_out.active_ind	            := g_rec_in.active_ind;
	 g_rec_out.last_updated_date        := g_date;


   exception
    when others then
     l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
     dwh_log.record_error(l_module_name,sqlcode,l_message);

    --dbms_output.put_line('Message XY'||l_message);

    raise;

end local_address_variables;

--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into FND_S4S_workgroup values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).workgroup_id ;
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
        update FND_S4S_workgroup
          set   workgroup_name           = a_tbl_update(i).workgroup_name,
                workgroup_seq            = a_tbl_update(i).workgroup_seq,
                s4s_business_unit_no     = a_tbl_update(i).s4s_business_unit_no,
                s4s_business_unit_name   = a_tbl_update(i).s4s_business_unit_name,
                effective_from_date      = a_tbl_update(i).effective_from_date,
                effective_to_date        = a_tbl_update(i).effective_to_date,
                active_ind	            = a_tbl_update(i).active_ind,
                last_updated_date        = a_tbl_update(i).last_updated_date
          where workgroup_id            = a_tbl_update(i).workgroup_id
            and effective_from_date     = a_tbl_update(i).effective_from_date
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
                       ' '||a_tbl_update(g_error_index).workgroup_id ;
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
   -- check to see if workgroup_seq is present on table and update/insert accordingly
   select count(1)
     into g_count
     from FND_S4S_workgroup
    where workgroup_id = g_rec_out.workgroup_id
      and effective_from_date = g_rec_out.effective_from_date
      ;

   if g_count = 1 then
      g_found := true;
   end if;

-- check if insert of workroup Id is lready in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).workgroup_id= g_rec_out.workgroup_id
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

    l_text := 'loading S4S workgroup data started at '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
   open c_fnd_s4s_LABOUR_HIERARCHY;
    fetch c_fnd_s4s_LABOUR_HIERARCHY bulk collect into a_stg_input limit g_forall_limit;
       while a_stg_input.count > 0
    loop
        for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 1000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;


         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
         fetch c_fnd_s4s_LABOUR_HIERARCHY bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_s4s_LABOUR_HIERARCHY;
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
       raise;


END WH_FND_S4S_014B;