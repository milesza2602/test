--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_040U_TEMP_STEP2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_040U_TEMP_STEP2" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load EMPLOYEE STATUS DY FACT information for Scheduling for Staff(S4S)
--                NB> effective_start_date = MONDAY and effective_end_date = any day in week
--
--               Delete process :
--                 Due to changes which can be made, we have to drop the current data and load the new data
--                        based upon employee_id and effective_start_date
--
--                The delete lists are used in the rollups as well.
--                The delete lists were created in the STG to FND load  
--                ie. FND_S4S_EMPLOCSTATUS_del_list
--
--
--  Tables:      Input    - dwh_foundation.FND_S4S_EMP_LOC_STATUS
--               Output   - DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY
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
g_rec_out            RTL_EMP_LOC_STATUS_DY%rowtype;
g_found              boolean;
g_date               date;
G_THIS_WEEK_START_DATE date;
g_fin_days number;
g_eff_end_date  date;

g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_040U_TEMP_STEP2';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_EMP_LOC_STATUS_DY data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of RTL_EMP_LOC_STATUS_DY%rowtype index by binary_integer;
type tbl_array_u is table of RTL_EMP_LOC_STATUS_DY%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_LOCATION is
-- 
WITH
SELEXT1 AS (
            SELECT DISTINCT
                  /*+ full(flr) full(de) full(dl) */
                  de.SK1_EMPLOYEE_ID ,
                  dl.SK1_LOCATION_NO ,
                  flr.EMPLOYEE_STATUS ,
                  flr.EMPLOYEE_WORKSTATUS ,
                  flr.EFFECTIVE_START_DATE ,
                  flr.EFFECTIVE_END_DATE,
                    dc.calendar_date tran_date
            FROM FND_S4S_EMP_LOC_STATUS flr
            JOIN DWH_HR_PERFORMANCE.dim_employee DE
                  ON DE.EMPLOYEE_ID = FLR.EMPLOYEE_ID
            JOIN DIM_LOCATION DL
                  ON DL.LOCATION_NO = FLR.LOCATION_NO
            JOIN DIM_CALENDAR DC
                  ON DC.THIS_WEEK_START_DATE BETWEEN FLR.EFFECTIVE_START_DATE  AND  NVL(FLR.EFFECTIVE_END_DATE - 1, g_eff_end_date)
                  where 
                  flr.last_updated_date = g_date
                  and not exists (select distinct a.sk1_employee_id
                  from  dwh_datafix.wl_emp_loc_status_dups a
                  where a.sk1_employee_id = de.sk1_employee_id)
                --  and 
          --        flr.employee_id in ('1002436','7089864')
    --    and sk1_employee_id <> 1037600
                  ),
selext2 as (SELECT DISTINCT SK1_EMPLOYEE_ID ,
                  SK1_LOCATION_NO ,
                  EMPLOYEE_STATUS ,
                  tran_date,
                  EMPLOYEE_WORKSTATUS ,
                  EFFECTIVE_START_DATE ,
                  EFFECTIVE_END_DATE,
                   ( CASE
                        WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                        WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date   IS NULL      THEN SE1.effective_START_DATE
                        WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date    IS NOT NULL      THEN SE1.effective_START_DATE
                        ELSE NULL
                          --SE1.availability_start_DATE - 1
                      END) derive_start_date ,
                   (CASE
                        WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
                        WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date   IS NULL      THEN g_eff_end_date
                         WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND se1.effective_end_date    IS NOT NULL      THEN se1.effective_end_date - 1
                        ELSE NULL
                          --SE1.availability_END_DATE - 1
                      END) derive_end_date
  FROM selext1 SE1
   WHERE SE1.EMPLOYEE_STATUS        IN ('H','I','R', 'S')
   )  
select distinct 
se2.SK1_LOCATION_NO
,se2.SK1_EMPLOYEE_ID
,se2.TRAN_DATE
,se2.EMPLOYEE_STATUS
,se2.EMPLOYEE_WORKSTATUS
,se2.EFFECTIVE_START_DATE
,se2.EFFECTIVE_END_DATE
, rtl.sk1_employee_id rtl_exists
from selext2 se2, dwh_performance.RTL_EMP_LOC_STATUS_DY rtl
where se2.tran_DATE BETWEEN derive_start_date AND derive_end_date
and se2.SK1_LOCATION_NO =  rtl.SK1_LOCATION_NO(+)
and se2.SK1_EMPLOYEE_ID  =  rtl.SK1_EMPLOYEE_ID(+)
and se2.TRAN_DATE  =  rtl.TRAN_DATE(+)
order by se2.SK1_LOCATION_NO
,se2.SK1_EMPLOYEE_ID
,se2.TRAN_DATE
;



                 
type stg_array is table of c_fnd_LOCATION%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_LOCATION%rowtype;


--**************************************************************************************************
-- Delete records from Performance
-- based on employee_id and effective_start_date
-- before loading from staging
--**************************************************************************************************
procedure delete_prf as
begin

      g_recs_inserted := 0;

      select max(run_seq_no) into g_run_seq_no
      from dwh_foundation.FND_S4S_EMPLOCSTATUS_DEL_LIST
      where batch_date = g_date;
      
      If g_run_seq_no is null
      then select max(run_seq_no) into g_run_seq_no
      from dwh_foundation.FND_S4S_EMPLOCSTATUS_DEL_LIST;
      If g_run_seq_no is null
      then
      g_run_seq_no := 1;
      end if;
      end if;
      g_run_date := trunc(sysdate);

BEGIN
         delete from DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY B
         where EXISTS (select distinct SK1_employee_id, effective_start_date 
         from dwh_foundation.FND_S4S_EMPLOCSTATUS_DEL_LIST A, DWH_HR_PERFORMANCE.dim_employee DE
         where run_seq_no = g_run_seq_no
         AND A.EMPLOYEE_ID = DE.EMPLOYEE_ID
         AND B.SK1_employee_id = DE.SK1_EMPLOYEE_ID
         AND B.effective_start_date = A.EFFECTIVE_START_DATE);
     
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_deleted := g_recs;
                
      l_text := 'Deleted from DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  exception
         when no_data_found then
                l_text := 'No deletions done for DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY ';
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
    g_rec_out.SK1_EMPLOYEE_ID           := g_rec_in.SK1_EMPLOYEE_ID;
   g_rec_out.SK1_LOCATION_NO           := g_rec_in.SK1_LOCATION_NO;
    g_rec_out.TRAN_DATE                 := g_rec_in.TRAN_DATE;
    g_rec_out.EMPLOYEE_STATUS           := g_rec_in.EMPLOYEE_STATUS;
    g_rec_out.effective_START_DATE     := g_rec_in.effective_START_DATE;
    g_rec_out.effective_END_DATE       := g_rec_in.effective_END_DATE;
    G_REC_OUT.EMPLOYEE_workSTATUS   := G_REC_IN.EMPLOYEE_workSTATUS;
       g_rec_out.last_updated_date         := g_date;
    

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
       insert into dwh_performance.RTL_EMP_LOC_STATUS_DY  values a_tbl_insert(i);

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
                       ' '||a_tbl_INSERT(g_error_index).tran_date||
    ' '||a_tbl_INSERT(g_error_index).SK1_LOCATION_NO||
                       ' '||a_tbl_INSERT(g_error_index).SK1_EMPLOYEE_ID;
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
             UPDATE RTL_EMP_LOC_STATUS_DY
                    SET effective_START_DATE   = a_tbl_update(i).effective_START_DATE,
                    effective_END_DATE   = a_tbl_update(i).effective_END_DATE,
                      EMPLOYEE_workSTATUS       = a_tbl_update(i).EMPLOYEE_workSTATUS ,
                     EMPLOYEE_STATUS = a_tbl_update(i).EMPLOYEE_STATUS,
                      LAST_UPDATED_DATE       = a_tbl_update(i).LAST_UPDATED_DATE
                    WHERE SK1_EMPLOYEE_ID     = a_tbl_update(i).SK1_EMPLOYEE_ID
                    AND SK1_location_no     = a_tbl_update(i).SK1_location_no
                    AND tran_date = a_tbl_update(i).tran_date
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
                       ' '||a_tbl_update(g_error_index).tran_date||
    ' '||a_tbl_update(g_error_index).SK1_LOCATION_NO||
        ' '||a_tbl_update(g_error_index).SK1_EMPLOYEE_ID;
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
    l_text := 'LOAD OF RTL_EMP_LOC_STATUS_DY  EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
  -- g_date := '7 dec 2014';
   
    
    -- derivation of cobnstr_end_date for recs where null.
    --  = 21days+ g_date+ days for rest of week 
    SELECT distinct THIS_WEEK_END_DATE into g_eff_end_date
    FROM DIM_CALENDAR 
    WHERE CALENDAR_DATE = g_date + 20;

      
    l_text := 'Derived g_eff_end_date - '||g_eff_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_loc_status';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_EMP_loc_status', DEGREE => 8);


--**************************************************************************************************
-- delete process
--**************************************************************************************************

 delete_prf;



--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_LOCATION;
    fetch c_fnd_LOCATION bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_LOCATION bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_LOCATION;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
   local_bulk_insert;
   local_bulk_update;
    l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_LOC_STATUS_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_EMP_LOC_STATUS_DY', DEGREE => 8);

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



END WH_PRF_S4S_040U_TEMP_STEP2;
