--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_059U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_059U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        10 July 2014
--  Author:      Lwazi Ntloko
--  Purpose:     Load Exception_Type data to DIM_Exception_Type 
--
--  DIM Tables:  Input  - FND_s4s_Exception_Type
--               Output - DIM_Exception_Type
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  ************* - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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

g_rec_out            DIM_Exception_Type%rowtype;

g_found              boolean;
g_insert_rec         boolean;
g_invalid_plan_type_no boolean;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_desc        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_059U';
l_desc               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_desc        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_desc        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_pln_fnd_md;
l_procedure_desc     sys_dwh_log.log_procedure_name%type       := l_module_desc;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD Exception_Type dimension Data';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DIM_Exception_Type%rowtype index by binary_integer;
type tbl_array_u is table of DIM_Exception_Type%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_Exception_Type is
select et.Exception_Type_ID,
      et.Exception_Type_desc,
      ec.SK1_Exception_category_id,
      ec.Exception_category_desc,
      ec.Exception_category_id
from FND_s4s_Exception_Type et, 
     DIM_Exception_category ec,
     FND_S4S_XCPTN_CATEGORY_MAP fcm
where et.Exception_type_id = fcm.Exception_type_id
and fcm.Exception_category_id = ec.Exception_category_id
;

g_rec_in    c_Exception_Type%rowtype;

-- For input bulk collect --
type stg_array is table of c_Exception_Type%rowtype;
a_stg_input      stg_array;

---------  SK1_Exception_Type_ID, SK1_Exception_category_id

-----------------------------------------------------------------------------------------------------
-- order by only where sequencing is essential to the correct loading of data
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

g_rec_out.Exception_Type_ID         := g_rec_in.Exception_Type_ID;
g_rec_out.Exception_Type_desc       := g_rec_in.Exception_Type_desc;
g_rec_out.SK1_Exception_category_id := g_rec_in.SK1_Exception_category_id;
g_rec_out.Exception_category_id     := g_rec_in.Exception_category_id;
g_rec_out.Exception_category_desc   := g_rec_in.Exception_category_desc;
g_rec_out.TOTAL                     := 'TOTAL';
g_rec_out.TOTAL_DESC                := 'ALL Exception_Type ID';
g_rec_out.last_updated_date         := g_date;

  exception
    when others then
     l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
     dwh_log.record_error(l_module_desc,sqlcode,l_message);
    raise;

end local_address_variables;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into DIM_Exception_Type values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_desc,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).Exception_Type_ID||
                       ' '||a_tbl_insert(g_error_index).Exception_category_id;
          dwh_log.record_error(l_module_desc,sqlcode,l_message);
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
          UPDATE DIM_Exception_Type
          SET Exception_Type_desc       = a_tbl_update(i).Exception_Type_desc,
            SK1_Exception_category_id = a_tbl_update(i).SK1_Exception_category_id,
            Exception_category_desc   = a_tbl_update(i).Exception_category_desc,
            TOTAL                     = a_tbl_update(i).TOTAL,
            TOTAL_DESC                = a_tbl_update(i).TOTAL_DESC,
            Exception_category_id     = a_tbl_update(i).Exception_category_id,
            last_updated_date         = a_tbl_update(i).LAST_UPDATED_DATE
          WHERE Exception_Type_ID     = a_tbl_update(i).Exception_Type_ID;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_desc,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).Exception_Type_ID||
                       ' '||a_tbl_insert(g_error_index).Exception_category_id;
          dwh_log.record_error(l_module_desc,sqlcode,l_message);
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
     from DIM_Exception_Type
    where Exception_Type_ID = g_rec_out.Exception_Type_ID
	   and Exception_category_id = g_rec_out.Exception_category_id;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of Business Unit number is already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).Exception_Type_ID  = g_rec_out.Exception_Type_ID and
		 a_tbl_insert(i).Exception_category_id = g_rec_out.Exception_category_id then
            g_found := TRUE;
         end if;
      end loop;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
	  g_rec_out.SK1_Exception_Type_ID := LABOUR_HIERACHY_SEQ.nextval;
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
       dwh_log.record_error(l_module_desc,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_desc,sqlcode,l_message);
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
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);

    l_text := 'LOADING PRF S4S Business unit data'|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);

    dwh_log.insert_log_summary(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);


-- hardcoding batch_date for testing
--g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
   open c_Exception_Type;
    fetch c_Exception_Type bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 1000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

	   local_address_variables;
       local_write_output;

      end loop;
    fetch c_Exception_Type bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_Exception_Type;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

dwh_log.update_log_summary(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_text);
    commit;
    p_success := true;
  exception

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_desc,sqlcode,l_message);
       dwh_log.update_log_summary(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_desc,sqlcode,l_message);
       dwh_log.update_log_summary(l_desc,l_system_desc,l_script_desc,l_procedure_desc,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;
end WH_PRF_S4S_059U;
