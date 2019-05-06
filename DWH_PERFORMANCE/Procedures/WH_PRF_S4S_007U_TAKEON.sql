--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_007U_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_007U_TAKEON" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load ABSENCE FACT information for Scheduling for Staff(S4S)
--
--  Tables:      Input    - dwh_foundation.FND_S4S_ABSENCE_EMP_DY
--               Output   - DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY
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
g_rec_out            RTL_ABSENCE_EMP_DY%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_007U_TAKEON';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ABSENCE data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_ABSENCE_EMP_DY%rowtype index by binary_integer;
type tbl_array_u is table of RTL_ABSENCE_EMP_DY%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor C_ABSENCE is
with selext as 
(SELECT   
                   FLR.EMPLOYEE_ID
                  , FLR.absence_date
                  , FLR.S4S_leave_update_date
                  , FLR.ABSENCE_HOURS
                  , FLR.LEAVE_TYPE_ID
                  , DE.SK1_EMPLOYEE_ID
                  ,CASE WHEN FLR.absence_date > FLR.S4S_leave_update_date 
                    THEN 1  -- 'PLANNED'
                    ELSE 2  -- 'UNPLANNED' 
                    END ABSENCE_TYPE_id
                    , sk1_leave_type_id
          FROM dwh_foundation.FND_S4S_ABSENCE_EMP_DY flr,
                dim_employee DE,
                dim_leave_type dl
          WHERE FLR.EMPLOYEE_ID    = DE.EMPLOYEE_ID
          and flr.leave_type_id = dl.leave_type_id
       --    and flr.last_updated_date = g_date
           )
SELECT   
                    SE.absence_date
                  , SE.S4S_leave_update_date
                  , SE.ABSENCE_HOURS
                  , SE.SK1_LEAVE_TYPE_ID
                  , SE.SK1_EMPLOYEE_ID
                  , da.SK1_ABSENCE_TYPE_ID
                  , RTL.SK1_EMPLOYEE_ID RTL_EXISTS
          FROM selext se,
                dim_ABSENCE_TYPE da,
                 RTL_ABSENCE_EMP_DY rtl
          WHERE se.ABSENCE_TYPE_id = da.ABSENCE_TYPE_id
          and se.absence_date =  RTL.absence_date(+)
          AND SE.SK1_EMPLOYEE_ID = RTL.SK1_EMPLOYEE_ID(+)
;
type stg_array is table of C_ABSENCE%rowtype;
a_stg_input      stg_array;

g_rec_in             C_ABSENCE%rowtype;


--**************************************************************************************************
-- Delete records from Performance
-- based on employee_id and ABSENCE_start_date
-- before loading from staging
--**************************************************************************************************
procedure delete_prf as
begin

      g_recs_inserted := 0;

      select max(run_seq_no) into g_run_seq_no
      from dwh_foundation.FND_S4S_ABSNCE_EMP_DY_DEL_LIST;
      
      If g_run_seq_no is null
      then g_run_seq_no := 1;
      end if;
      g_run_date := trunc(sysdate);

BEGIN
         delete from DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY
         where (SK1_employee_id, ABSENCE_DATE) in (select distinct SK1_employee_id, ABSENCE_DATE from dwh_foundation.FND_S4S_ABSNCE_EMP_DY_DEL_LIST A, dim_employee DE
         where run_seq_no = g_run_seq_no
         AND A.EMPLOYEE_ID = DE.EMPLOYEE_ID);
     
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_deleted := g_recs;
                
      l_text := 'Deleted from DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  exception
         when no_data_found then
                l_text := 'No deletions done for DWH_PERFORMANCE.RTL_ABSENCE_EMP_DY ';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
   end;          

   g_recs_inserted  :=0;
   g_recs_deleted := 0;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end delete_prf;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
   g_rec_out.SK1_EMPLOYEE_ID          :=  g_rec_in.SK1_EMPLOYEE_ID;
   g_rec_out.absence_date             :=  g_rec_in.absence_date;
   g_rec_out.S4S_leave_update_date    :=  g_rec_in.S4S_leave_update_date ;
   g_rec_out.ABSENCE_HOURS        :=  g_rec_in.ABSENCE_HOURS;
   g_rec_out.SK1_LEAVE_TYPE_ID        :=  g_rec_in.SK1_LEAVE_TYPE_ID;
   g_rec_out.SK1_ABSENCE_TYPE_ID         :=  g_rec_in.SK1_ABSENCE_TYPE_ID;
   g_rec_out.LAST_UPDATED_DATE         :=  G_DATE;

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
       insert into RTL_ABSENCE_EMP_DY values a_tbl_insert(i);

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
                      -- ' '||a_tbl_INSERT(g_error_index).sk1_LOCATION_no||
                       ' '||a_tbl_INSERT(g_error_index).absence_date||
                       ' '||a_tbl_INSERT(g_error_index).sk1_EMPLOYEE_ID;
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
        update RTL_ABSENCE_EMP_DY
        set
            S4S_leave_update_date  =  a_tbl_update(i).S4S_leave_update_date ,
            ABSENCE_HOURS =  a_tbl_update(i).ABSENCE_HOURS,
            SK1_LEAVE_TYPE_ID =  a_tbl_update(i).SK1_LEAVE_TYPE_ID,
            SK1_ABSENCE_TYPE_ID =  a_tbl_update(i).SK1_ABSENCE_TYPE_ID,
            LAST_UPDATED_DATE =  a_tbl_update(i).LAST_UPDATED_DATE


       where  absence_date =  a_tbl_update(i).absence_date
         AND     SK1_EMPLOYEE_ID =  a_tbl_update(i).SK1_EMPLOYEE_ID
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
--                      ' '||a_tbl_update(g_error_index).sk1_LOCATION_no||
                      ' '||a_tbl_update(g_error_index).absence_date||
                       ' '||a_tbl_update(g_error_index).sk1_EMPLOYEE_ID;
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
  IF G_REC_IN.RTL_EXISTS IS NOT NULL
   THEN G_COUNT := 1;
   g_found := TRUE;
   END IF;
    if g_count = 1 then
      g_found := TRUE;
   end if;


-- Place record into array for later bulk writing
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_ABSENCE_EMP_DY   EX FOUNDATION STARTED '||
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
-- delete process
--**************************************************************************************************

 delete_prf;


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open C_ABSENCE;
    fetch C_ABSENCE bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 50000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

        local_address_variables;
        local_write_output;

      end loop;
    fetch C_ABSENCE bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close C_ABSENCE;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
   local_bulk_insert;
   local_bulk_update;

    l_text := 'Running GATHER_TABLE_STATS ON RTL_ABSENCE_EMP_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_ABSENCE_EMP_DY', DEGREE => 8);

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



END WH_PRF_S4S_007U_TAKEON;
