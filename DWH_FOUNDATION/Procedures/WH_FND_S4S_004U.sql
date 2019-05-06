--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_004U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_004U" (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Update Location_employee_day information for Scheduling for Staff(S4S)
--
-- Comment :
-- 9 MARCH 2015 -- We create the delete list solely for recording purposes.
--              -- The deletes are taken from STG where the employee_id exists in the FNd table
--                 ie. when a change occurs at source, the entire employee history is resent.
--              --   When it comes to running the 'explode' from FND to PRF_DAY, 
--                        we do it for all records
--                 as the delete off the PRD_DAY table will take forever and be a huge number of records.
--              --   This is because we would need to process not only changed employess(coming from STG) 
--                but also any employees with availability_end_date = null on FND.

--                ie. FND_S4S_EMP_AVAIL_DY_del_list
--
--
--  Tables:      AIT load - STG_S4S_LOC_EMP_DY
--               Input    - STG_S4S_LOC_EMP_DY_CPY
--               Output   - FND_S4S_EMP_AVAIL_DY
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--   22 jan 2015 - w lyttle - coding added for open_ended processing when no update received :
--                 1. Records need to be appended to the 'delete list table' where the 'end_date' is null.
--                     This will ensure that from this point forward the performance records for these will be deleted and reloaded.
--                 2. The foundation fact table will have LAST_UPDATED_DATE =batch_date where any records have a null 'end_date'.
--                     This is to ensure that from this point onwards, forecasting will be applied.
--   1 MARCH 2015 - W LYTTLE  DO NOT LOAD THESE UNTIL ISSUES RESOLVED - 1 MARCH 2015
                      ------
                      --WHERE EMPLOYEE_ID IN (7084089
                      --,7053578
                      --,7090169)
--  4 march 2015 w lyttle - added data takeon option

                      ------------------------------
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
g_hospital_text      STG_S4S_LOC_EMP_DY_hsp.sys_process_msg%type;
g_rec_out            FND_S4S_EMP_AVAIL_DY%rowtype;
g_data_takeon       varchar2(1);
g_next_delete_batch_number  number        :=  0;

g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_004U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE Location_employee_day data ex S4S';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  -- For output arrays into bulk load forall statements --
type tbl_array_i is table of FND_S4S_EMP_AVAIL_DY%rowtype index by binary_integer;
type tbl_array_u is table of FND_S4S_EMP_AVAIL_DY%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of STG_S4S_LOC_EMP_DY_CPY.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of STG_S4S_LOC_EMP_DY_CPY.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


----**** nb WHEN PROCESSING MORE THAN ONE BATCH UNFORTUNATELY THE CODE BELOW DOES NOT WORK 100%
--***     UNLESS ALL OF THE BATCHES HAVE NO CHANGES TO THE KEY.
--**     tHEREFORE YOU HAVE TO RUN 1 BATCH AT A TIME TO LOAD.
--*****
cursor c_STG_S4S_LOC_EMP_DY is
 WITH selbat AS
  (SELECT /*+ full(stg) parallel(stg,6)  */ MAX(sys_source_batch_id) maxbat,
  employee_id,
    availability_start_date, cycle_start_date
  FROM dwh_foundation.STG_S4S_LOC_EMP_DY_CPY stg

--WHERE EMPLOYEE_ID NOT in ('7044026'
--,'7069869'
--,'7026830')
  GROUP BY employee_id,
    availability_start_date, cycle_start_date
  )
   ,
   selSEQ AS
  (SELECT /*+ full(stg) parallel(stg,6)  */ maxbat, MAX(SYS_SOURCE_SEQUENCE_NO) MAXSEQ,
  STG.employee_id,
    STG.availability_start_date, STG.cycle_start_date
  FROM dwh_foundation.STG_S4S_LOC_EMP_DY_CPY stg, SELBAT SB
  WHERE SB.MAXBAT = STG.SYS_SOURCE_BATCH_ID
  AND SB.EMPLOYEE_ID = STG.EMPLOYEE_ID
  AND SB.AVAILABILITY_START_DATE = STG.AVAILABILITY_START_DATE
  AND SB.CYCLE_START_DATE = STG.CYCLE_START_DATE
  --  where  employee_id = '7021874'
-- and sys_source_batch_id >= 148
  GROUP BY MAXBAT, STG.employee_id,
    STG.availability_start_date, STG.cycle_start_date
  )
   ,
      selALL AS
  (SELECT /*+ full(stg) parallel(stg,6)  */  DISTINCT SYS_SOURCE_BATCH_ID,  NO_OF_WEEKS,
  STG.employee_id,
    STG.availability_start_date,  STG.availability_END_date, STG.cycle_start_date
  FROM dwh_foundation.STG_S4S_LOC_EMP_DY_CPY stg, SELSEQ SS
  WHERE SS.MAXBAT = STG.SYS_SOURCE_BATCH_ID
  AND  SS.MAXSEQ = STG.SYS_SOURCE_SEQUENCE_NO
  AND SS.EMPLOYEE_ID = STG.EMPLOYEE_ID
  AND SS.AVAILABILITY_START_DATE = STG.AVAILABILITY_START_DATE
  AND SS.CYCLE_START_DATE = STG.CYCLE_START_DATE
 )
  
  SELECT /*+ full(stg) parallel(stg,6) full(fe) parallel(fe,6) full(sa) parallel(sa,6)*/ 
STG.SYS_SOURCE_BATCH_ID,
  STG.SYS_SOURCE_SEQUENCE_NO,
  STG.SYS_LOAD_DATE,
  STG.SYS_PROCESS_CODE,
  STG.SYS_LOAD_SYSTEM_NAME,
  STG.SYS_MIDDLEWARE_BATCH_ID,
  STG.SYS_PROCESS_MSG,
  STG.SOURCE_DATA_STATUS_CODE,
  STG.LOCATION_NO stg_LOCATION_NO,
  STG.EMPLOYEE_ID stg_EMPLOYEE_ID,
  STG.CYCLE_START_DATE stg_CYCLE_START_DATE,
  STG.AVAILABILITY_START_DATE stg_AVAILABILITY_START_DATE,
  STG.AVAILABILITY_END_DATE stg_AVAILABILITY_END_DATE,
  STG.FIXED_ROSTER_START_TIME stg_FIXED_ROSTER_START_TIME ,
  STG.FIXED_ROSTER_END_TIME stg_FIXED_ROSTER_END_TIME,
  STG.NO_OF_WEEKS stg_NO_OF_WEEKS,
  STG.WEEK_NUMBER stg_WEEK_NUMBER,
  STG.DAY_OF_WEEK stg_DAY_OF_WEEK,
  STG.MEAL_BREAK_MINUTES stg_MEAL_BREAK_MINUTES,
  fe.EMPloyee_id fe_EMPLOYEE_ID,
  dc.calendar_DATE dc_CYCLE_START_DATE ,
  dc2.calendar_date dc2_AVAILABILITY_START_DATE
FROM DWH_FOUNDATION.STG_S4S_LOC_EMP_DY_CPY stg,
  DWH_HR_PERFORMANCE.DIM_EMPLOYEE fe,
  dim_calendar dc,
  dim_calendar dc2,
  selall sa
WHERE stg.EMPLOYEE_ID           = fe.employee_id(+)
AND stg.CYCLE_START_DATE        = dc.calendar_date(+)
AND stg.AVAILABILITY_START_DATE = dc2.calendar_date(+)
AND stg.EMPLOYEE_ID             = sa.employee_id
AND stg.AVAILABILITY_START_DATE = sa.AVAILABILITY_START_DATE
AND stg.CYCLE_START_DATE = sa.CYCLE_START_DATE
------------------------------------------
---   The coding was ....AND stg.AVAILABILITY_END_DATE = sa.AVAILABILITY_END_DATE
--    but had to change to an arbitrary value when doing the join
--     as it won't join a null value to a null value 
--     which is quite a valid scenario for this extract
------------------------------------------
AND nvl(stg.AVAILABILITY_END_DATE, to_date('01/01/3099', 'dd/mm/yyyy')) = nvl(sa.AVAILABILITY_END_DATE, to_date('01/01/3099', 'dd/mm/yyyy'))
AND stg.SYS_SOURCE_BATCH_ID     = sa.SYS_SOURCE_BATCH_ID 
AND STG.NO_OF_WEEKS = SA.NO_OF_WEEKS
---------------- PRD ISSUE 4 JUNE 2015
             AND stg.EMPLOYEE_ID NOT IN ('7047656')
--------------- EMPLOYEE EXCLUDED UNTIL DATA CORRECTED
order by STG.SYS_SOURCE_BATCH_ID,
  STG.SYS_SOURCE_SEQUENCE_NO
;



g_rec_in                   c_STG_S4S_LOC_EMP_DY%rowtype;
-- For input bulk collect --
type stg_array is table of c_STG_S4S_LOC_EMP_DY%rowtype;
a_stg_input      stg_array;


--**************************************************************************************************
-- Delete records from Foundation
-- based on employee_id -- before loading from staging
-- IE. whole history for an employee is sent when any changes occur
--**************************************************************************************************
procedure delete_fnd as
begin

      g_recs_inserted := 0;

      select max(run_seq_no)+1 into g_run_seq_no
      from dwh_foundation.FND_S4S_EMP_AVAIL_DY_del_list;
      
     If g_run_seq_no is null
      then g_run_seq_no := 1;
      end if;
      
      g_run_date := trunc(sysdate);

     insert /*+ append */ into dwh_foundation.FND_S4S_EMP_AVAIL_DY_del_list
     with selstg
            as (select distinct employee_id from STG_S4S_LOC_EMP_DY_CPY)
     select g_run_date, g_date, g_run_seq_no, f.*
        from DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY f, selstg s
        where f.employee_id = s.employee_id;
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_inserted := g_recs;
                
          l_text := 'Insert into FND_S4S_EMP_AVAIL_DY_del_list recs='||g_recs_inserted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
          dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

         delete from DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY
         where employee_id in (select distinct employee_id from STG_S4S_LOC_EMP_DY_CPY);
     
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_deleted := g_recs;
                
      l_text := 'Deleted from DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

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

end delete_fnd;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';
   g_rec_out.EMPLOYEE_ID :=  g_rec_in.STG_EMPLOYEE_ID;
   g_rec_out.CYCLE_START_DATE :=  g_rec_in.STG_CYCLE_START_DATE;
   g_rec_out.AVAILABILITY_START_DATE :=  g_rec_in.STG_AVAILABILITY_START_DATE;
   g_rec_out.AVAILABILITY_END_DATE :=  g_rec_in.STG_AVAILABILITY_END_DATE;
   g_rec_out.FIXED_ROSTER_START_TIME  :=  g_rec_in.STG_FIXED_ROSTER_START_TIME ;
   g_rec_out.FIXED_ROSTER_END_TIME :=  g_rec_in.STG_FIXED_ROSTER_END_TIME;
   g_rec_out.NO_OF_WEEKS :=  g_rec_in.STG_NO_OF_WEEKS;
   g_rec_out.WEEK_NUMBER :=  g_rec_in.STG_WEEK_NUMBER;
   g_rec_out.DAY_OF_WEEK :=  g_rec_in.STG_DAY_OF_WEEK;
   g_rec_out.MEAL_BREAK_MINUTES :=  g_rec_in.STG_MEAL_BREAK_MINUTES;


   g_rec_out.last_updated_date               := g_date;


   if G_REC_IN.fe_EMPLOYEE_ID IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'EMPLOYEE_ID NOT FOUND';
     return;
   end if;
   if G_REC_IN.dc_CYCLE_START_DATE IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'CYCLE_START_DATE NOT FOUND';
     return;
   end if;
   if G_REC_IN.dc2_AVAILABILITY_START_DATE IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := 'AVAILABILITY_START_DATE NOT FOUND';
     return;
   end if;

      if G_REC_IN.STG_AVAILABILITY_END_DATE IS NOT NULL
      AND  G_REC_IN.STG_AVAILABILITY_END_DATE < G_REC_IN.STG_AVAILABILITY_START_DATE
      then
     g_hospital      := 'Y';
     g_hospital_text := 'AVAILABILITY_END_DATE < AVAILABILITY_START_DATE';
     return;
   end if;

      if G_REC_IN.STG_FIXED_ROSTER_END_TIME IS NOT NULL
      AND  G_REC_IN.STG_FIXED_ROSTER_END_TIME < G_REC_IN.STG_FIXED_ROSTER_START_TIME
      then
     g_hospital      := 'Y';
     g_hospital_text := 'FIXED_ROSTER_END_TIME < FIXED_ROSTER_START_TIME';
     return;
   end if;

   if G_REC_IN.stg_DAY_OF_WEEK IS NULL OR G_REC_IN.stg_DAY_OF_WEEK < 1 OR G_REC_IN.stg_DAY_OF_WEEK > 7 then
     g_hospital      := 'Y';
     g_hospital_text := 'DAY_OF_WEEK INVALID';
     return;
   end if;

   if G_REC_IN.stg_WEEK_NUMBER IS NULL OR G_REC_IN.stg_WEEK_NUMBER < 1  then
     g_hospital      := 'Y';
     g_hospital_text := 'WEEK_NUMBER INVALID';
     return;
   end if;


   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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

/* Formatted on 23/09/2014 01:48:55 PM (QP5 v5.185.11230.41888) */
INSERT INTO dwh_foundation.STG_S4S_LOC_EMP_DY_hsp
     VALUES (g_rec_in.SYS_SOURCE_BATCH_ID,
             g_rec_in.SYS_SOURCE_SEQUENCE_NO,
             g_rec_in.SYS_LOAD_DATE,
             g_rec_in.SYS_PROCESS_CODE,
             g_rec_in.SYS_LOAD_SYSTEM_NAME,
             g_rec_in.SYS_MIDDLEWARE_BATCH_ID,
             g_rec_in.SYS_PROCESS_MSG,
             g_rec_in.SOURCE_DATA_STATUS_CODE,
             g_rec_in.STG_LOCATION_NO,
             g_rec_in.STG_EMPLOYEE_ID,
             g_rec_in.STG_AVAILABILITY_START_DATE,
             g_rec_in.STG_NO_OF_WEEKS,
             g_rec_in.STG_DAY_OF_WEEK,
             g_rec_in.STG_CYCLE_START_DATE,
             g_rec_in.STG_WEEK_NUMBER,
             g_rec_in.STG_AVAILABILITY_END_DATE,
             g_rec_in.STG_FIXED_ROSTER_START_TIME,
             g_rec_in.STG_FIXED_ROSTER_END_TIME,
             g_rec_in.STG_MEAL_BREAK_MINUTES);


   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
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
       insert into FND_S4S_EMP_AVAIL_DY  values a_tbl_insert(i);

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
                      ' '||a_tbl_insert(g_error_index).EMPLOYEE_ID||
' '||a_tbl_insert(g_error_index).AVAILABILITY_START_DATE||
' '||a_tbl_insert(g_error_index).NO_OF_WEEKS||
' '||a_tbl_insert(g_error_index).week_number||
' '||a_tbl_insert(g_error_index).DAY_OF_WEEK;

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
       update FND_S4S_EMP_AVAIL_DY
       set
            AVAILABILITY_END_DATE =  a_tbl_update(i).AVAILABILITY_END_DATE,
            FIXED_ROSTER_START_TIME  =  a_tbl_update(i).FIXED_ROSTER_START_TIME ,
            FIXED_ROSTER_END_TIME =  a_tbl_update(i).FIXED_ROSTER_END_TIME,
            CYCLE_START_DATE =  a_tbl_update(i).CYCLE_START_DATE ,
            WEEK_NUMBER =  a_tbl_update(i).WEEK_NUMBER ,
            MEAL_BREAK_MINUTES =  a_tbl_update(i).MEAL_BREAK_MINUTES,
            LAST_UPDATED_DATE =  a_tbl_update(i).LAST_UPDATED_DATE


       where   EMPLOYEE_ID =  a_tbl_update(i).EMPLOYEE_ID
         AND     AVAILABILITY_START_DATE =  a_tbl_update(i).AVAILABILITY_START_DATE
         AND       NO_OF_WEEKS =  a_tbl_update(i).NO_OF_WEEKS
         AND       week_number =  a_tbl_update(i).week_number
         AND        DAY_OF_WEEK =  a_tbl_update(i).DAY_OF_WEEK;

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
' '||a_tbl_update(g_error_index).EMPLOYEE_ID||
' '||a_tbl_update(g_error_index).AVAILABILITY_START_DATE||
' '||a_tbl_update(g_error_index).NO_OF_WEEKS||
' '||a_tbl_update(g_error_index).week_number||
' '||a_tbl_update(g_error_index).DAY_OF_WEEK;

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

v_count integer := 0;

begin

   g_found := false;

   select count(1)
   into   v_count
   from   DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY
   where  EMPLOYEE_ID =  g_rec_out.EMPLOYEE_ID
          AND        week_number =  g_rec_out.week_number
         AND        NO_OF_WEEKS=  g_rec_out.NO_OF_WEEKS
         AND        DAY_OF_WEEK =  g_rec_out.DAY_OF_WEEK
             AND AVAILABILITY_START_DATE =  g_rec_out.AVAILABILITY_START_DATE;


   if v_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if   a_tbl_insert(i).EMPLOYEE_ID =  g_rec_out.EMPLOYEE_ID
           AND a_tbl_insert(i).AVAILABILITY_START_DATE =  g_rec_out.AVAILABILITY_START_DATE
          AND a_tbl_insert(i).NO_OF_WEEKS =  g_rec_out.NO_OF_WEEKS
           AND a_tbl_insert(i).week_number =  g_rec_out.week_number
         AND a_tbl_insert(i).DAY_OF_WEEK =  g_rec_out.DAY_OF_WEEK

           then
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
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD THE LOC_EMP_DY data ex S4S STARTED AT '||
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

    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Delete Process *** set g_data_takeon indicator
--**************************************************************************************************

  G_DATA_TAKEON := 'N';
  
  l_text := 'Data takeon = '||G_DATA_TAKEON;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   If g_data_takeon <> 'Y' OR g_data_takeon IS NULL
       THEN 
             delete_fnd;
   ELSE
        L_Text := 'TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_AVAIL_DY_DEL_LIST';
        Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
        Execute Immediate ('TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_AVAIL_DY_DEL_LIST ');
        L_Text := 'TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_AVAIL_DY_DEL';
        Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
        Execute Immediate ('TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_AVAIL_DY_DEL ');
        L_Text := 'TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_AVAIL_DY';
        Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
        Execute Immediate ('TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_AVAIL_DY');
        L_Text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_AVAIL_DY';
        Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
         Dbms_Stats.Gather_Table_Stats ('DWH_FOUNDATION',
                                       'FND_S4S_EMP_AVAIL_DY', Degree => 8);
        L_Text := 'TRUNCATE TABLE dwh_foundation.STG_S4S_LOC_EMP_DY_HSP';
        Dwh_Log.Write_Log(L_Name,L_System_Name,L_Script_Name,L_Procedure_Name,L_Text);
        Execute Immediate ('TRUNCATE TABLE dwh_foundation.STG_S4S_LOC_EMP_DY_HSP');
   END IF;


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_STG_S4S_LOC_EMP_DY;
    fetch c_STG_S4S_LOC_EMP_DY bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_STG_S4S_LOC_EMP_DY bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_STG_S4S_LOC_EMP_DY;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_AVAIL_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_EMP_AVAIL_DY', DEGREE => 8);
    --**************************************************************************************************
--
-- Setup records for reprocessing( change 22 jan 2015 )
-------------------------------------
-- 1. Records need to be appended to the 'delete list table' where the 'end_date' is null.
--    This will ensure that from this point forward the performance records for these will be deleted and reloaded.
-- 2. The foundation fact table will have LAST_UPDATED_DATE =batch_date where any records have a null 'end_date'.
--    This is to ensure that from this point onwards, forecasting will be applied.
--**************************************************************************************************
---- Write delete recs -----      
      BEGIN
          insert /*+ append */ 
                into dwh_foundation.FND_S4S_EMP_AVAIL_DY_del_list
         select g_run_date, g_date, g_run_seq_no, f.*
              from DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY f
              where f.AVAILABILITY_end_date is null
              and last_updated_date <> g_date;
            
            g_recs := 0;
            g_recs :=SQL%ROWCOUNT ;
            COMMIT;
                     
            l_text := 'Records added to DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY_del_list recs='||g_recs;
            dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

      
      exception
         when no_data_found then
                l_text := 'No extra records for DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY_del_list ';
                dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
   end;          
   
---- Update ----- 
      Update DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY
      set last_updated_date = g_date
      where AVAILABILITY_end_date is null 
      and last_updated_date <> g_date;
      g_recs := 0;
      g_recs :=SQL%ROWCOUNT ;
      COMMIT;
      
 --test
 --select count(*) into g_recs from DWH_FOUNDATION.FND_S4S_EMP_LOC_STATUS
 --     where effective_end_date is null 
 --     and last_updated_date <> g_date;
         
      l_text := 'Last_updated_date updated on DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY recs='||g_recs;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

--********************************************************************************************************
-- special delete
--************************************************************************************************************
      delete DWH_FOUNDATION.FND_S4S_EMP_AVAIL_DY
      where employee_id = '7033825'
      ;
      g_recs := 0;
      g_recs :=SQL%ROWCOUNT ;
      COMMIT;
      
      l_text := 'records delete from FND_S4S_EMP_AVAIL_DY until source fixed='||g_recs;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);


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


END WH_FND_S4S_004U;
