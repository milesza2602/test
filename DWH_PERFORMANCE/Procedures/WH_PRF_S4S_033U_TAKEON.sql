--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_033U_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_033U_TAKEON" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load S4S_LEAVE_TYPE information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - dwh_foundation.FND_S4S_LEAVE_TYPE
--               Output   - DWH_PERFORMANCE.dim_LEAVE_TYPE
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_tbc           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            DWH_PERFORMANCE.dim_LEAVE_TYPE%rowtype;
g_found              boolean;
g_date               date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_033U_TAKEON';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE EMPLOYEE LEAVE_TYPE data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_LEAVE_TYPE%rowtype index by binary_integer;
type tbl_array_u is table of dim_LEAVE_TYPE%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        TBL_ARRAY_U;
a_empty_set_i       tbl_array_i;
a_empty_set_u       TBL_ARRAY_U;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_job is
   select
flr.LEAVE_TYPE_ID,
flr.LEAVE_TYPE_DESC,
CASE 
WHEN LEAVE_TYPE_ID in( 1000176,1000181)	
    THEN 75
WHEN LEAVE_TYPE_ID in( 1000177,1000229, 1000229, 1000212, 1000203, 1000179, 1000205	)
then 33
WHEN LEAVE_TYPE_ID in(1000201, 1000230	, 1000230, 1000213, 1000206, 1000180)
    THEN 50
ELSE 
    100
END  LEAVE_TYPE_PERCENT_OF_HOURS
   from dwh_foundation.FND_S4S_LEAVE_TYPE flr
;

type stg_array is table of c_fnd_job%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_job%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

g_rec_out.LEAVE_TYPE_ID := g_rec_in.LEAVE_TYPE_ID;
g_rec_out.LEAVE_TYPE_DESC := g_rec_in.LEAVE_TYPE_DESC;
g_rec_out.TOTAL := 'TOTAL';
g_rec_out.TOTAL_DESC := 'ALL LEAVE TYPE';
g_rec_out.LAST_UPDATED_DATE := G_DATE;

g_rec_out.LEAVE_TYPE_PERCENT_OF_HOURS := g_rec_in.LEAVE_TYPE_PERCENT_OF_HOURS;



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
       insert into dim_LEAVE_TYPE values a_tbl_insert(i);

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
                      ' '||a_tbl_INSERT(g_error_index).LEAVE_TYPE_ID;
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
          UPDATE dim_LEAVE_TYPE
          SET LEAVE_TYPE_DESC           = a_tbl_update(i).LEAVE_TYPE_DESC,
            TOTAL                       = a_tbl_update(i).TOTAL,
            TOTAL_DESC                  = a_tbl_update(i).TOTAL_DESC,
            LEAVE_TYPE_PERCENT_OF_HOURS = a_tbl_update(i).LEAVE_TYPE_PERCENT_OF_HOURS,
            LAST_UPDATED_DATE           = a_tbl_update(i).LAST_UPDATED_DATE
          WHERE LEAVE_TYPE_ID           = a_tbl_update(i).LEAVE_TYPE_ID ;


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
                      ' '||a_tbl_update(g_error_index).LEAVE_TYPE_ID;
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
   from   dim_LEAVE_TYPE
    where  LEAVE_TYPE_ID             = g_rec_out.LEAVE_TYPE_ID
  ;
IF g_count        = 1 THEN
    g_found        := TRUE;
  END IF;

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_LEAVE_TYPE_ID  := LABOUR_HIERACHY_seq.nextval;
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
      a_tbl_update  := a_empty_set_U;
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF dim_LEAVE_TYPE EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);


-- hardcoding batch_date for testing
--g_date := trunc(sysdate);

    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_job;
    fetch c_fnd_job bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

        local_address_variables;
        local_write_output;

      end loop;
    fetch c_fnd_job bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_job;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
   local_bulk_insert;
   local_bulk_update;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
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


END WH_PRF_S4S_033U_TAKEON;