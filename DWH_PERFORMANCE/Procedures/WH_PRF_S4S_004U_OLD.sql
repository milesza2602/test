--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_004U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_004U_OLD" (
   p_forall_limit   IN     INTEGER,
   p_success           OUT BOOLEAN)
AS
   --**************************************************************************************************
    -- copy of wh_prf_s4s_004u - taken on 6/oct/2014 before code to change aviability to cycle
    --**************************************************************************************************
   --  Date:        July 2014
   --  Author:      Wendy lyttle
   --  Purpose:     Load EMPLOYEE_LOCATION_DAY  information for Scheduling for Staff(S4S)
--
   -- Comment  from FND:
-- 9 MARCH 2015 -- When a change occurs at source, the entire employee history is resent.
--              --   When it comes to running the 'explode' from FND to PRF_DAY, 
--                        we do it for all records
--                 as the delete off the PRD_DAY table will take forever and be a huge number of records.
--              --   This is because we would need to process not only changed employess(coming from STG) 
--                but also any employees with availability_end_date = null on FND.
  --               Note that eventhough we will only use a subset of this data (next proc to run wh_prf_s4s_4u)
   --                we generate all days within each period
--
   --**************************************************************************************************
   -- setup dates
   -- Each cycle_period has a certain no_of_weeks in which certain days apply(availability)
   -- We have to 'cycle' these weeks from the availability_start_date through to the beginning of the next date
   -- To do this we have to
   -- 1. derive the end_date for the availability period
   -- 2. generate the missing weeks during these periods
   -- 3. generate the missing weeks between periods
   --**************************************************************************************************
   --
   --  Tables:      Input    - dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3
   --               Output   - RTL_EMP_AVAIL_LOC_JOB_DY -- RTL_EMP_AVAIL_LOC_JOB_DY
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
   g_forall_limit     INTEGER := dwh_constants.vc_forall_limit;
   g_recs_read        INTEGER := 0;
   g_recs_inserted    INTEGER := 0;
   g_recs_updated     INTEGER := 0;
   g_recs_tbc         INTEGER := 0;
   g_error_count      NUMBER := 0;
   g_error_index      NUMBER := 0;
   g_count            NUMBER := 0;
   g_rec_out          RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE;
   g_found            BOOLEAN;
   g_date             DATE;
   g_AVAILABILITY_START_date             DATE;
   g_SUB      NUMBER := 0;
   g_end_date             DATE;
g_name varchar2(40);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;


      l_message sys_dwh_errlog.log_text%TYPE;
      l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_S4S_004U';
      l_name sys_dwh_log.log_name%TYPE                     := dwh_constants.vc_log_name_rtl_md;
      l_system_name sys_dwh_log.log_system_name%TYPE       := dwh_constants.vc_log_system_name_rtl_prf;
      l_script_name sys_dwh_log.log_script_name%TYPE       := dwh_constants.vc_log_script_rtl_prf_md;
      l_procedure_name sys_dwh_log.log_procedure_name%TYPE := l_module_name;
      l_text sys_dwh_log.log_text%TYPE;
      l_description sys_dwh_log_summary.log_description%TYPE   := 'LOAD THE RTL_EMP_AVAIL_LOC_JOB_DY data  EX FOUNDATION';
      l_process_type sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;
      -- For output arrays into bulk load forall statements --
      TYPE tbl_array_i
      IS
        TABLE OF RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE INDEX BY BINARY_INTEGER;
      TYPE tbl_array_u
      IS
        TABLE OF RTL_EMP_AVAIL_LOC_JOB_DY%ROWTYPE INDEX BY BINARY_INTEGER;
        a_tbl_insert tbl_array_i;
        a_tbl_update tbl_array_u;
        a_empty_set_i tbl_array_i;
        a_empty_set_u tbl_array_u;
        a_count   INTEGER := 0;
        a_count_i INTEGER := 0;
        a_count_u INTEGER := 0;
---
-- Reason for extra step (ie. with caluse) is that when we ran the history in we had no response
-- This 'step' approach seemed to work better
--
 
  --**************************************************************************************************
  -- Remove constraints and indexes
--  I30_RTL_EMP_AVL_LC_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	LAST_UPDATED_DATE
--  I40_RTL_EMP_AVL_LC_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
--  I50_RTL_EMP_AVL_LC_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	SK1_EMPLOYEE_ID, TRAN_DATE
--  K_RTL_EMP_AVL_LC_JB_DY	UNIQUE	VALID	NORMAL	N	NO		NO	SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
  --**************************************************************************************************
procedure a_remove_indexes as
BEGIN
     g_name := null; 
  BEGIN
    SELECT CONSTRAINT_NAME
    INTO G_name
    FROM DBA_CONSTRAINTS
    WHERE CONSTRAINT_NAME = 'PK_RTL_EMP_AVL_LC_JB_DY'
    AND TABLE_NAME        = 'RTL_EMP_AVAIL_LOC_JOB_DY';
    
    l_text               := 'alter table dwh_performance.RTL_EMP_AVAIL_LOC_JOB_DY drop constraint PK_RTL_EMP_AVL_LC_JB_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_performance.RTL_EMP_AVAIL_LOC_JOB_DY drop constraint PK_RTL_EMP_AVL_LC_JB_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_RTL_EMP_AVL_LC_JB_DY does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
     l_text               := 'done';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    g_name := null;

    
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I30_RTL_EMP_AVL_LC_JB_DY'
    AND TABLE_NAME        = 'RTL_EMP_AVAIL_LOC_JOB_DY';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I30_RTL_EMP_AVL_LC_JB_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I30_RTL_EMP_AVL_LC_JB_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'index PI30_RTL_EMP_AVL_LC_JB_DY does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;


      g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I40_RTL_EMP_AVL_LC_JB_DY'
    AND TABLE_NAME        = 'RTL_EMP_AVAIL_LOC_JOB_DY';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I40_RTL_EMP_AVL_LC_JB_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I40_RTL_EMP_AVL_LC_JB_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'index I40_RTL_EMP_AVL_LC_JB_DY does not exist';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;

      g_name := null;
  BEGIN
    SELECT index_NAME
    INTO G_name
    FROM DBA_indexes
    WHERE index_NAME = 'I50_RTL_EMP_AVL_LC_JB_DY'
    AND TABLE_NAME        = 'RTL_EMP_AVAIL_LOC_JOB_DY';
    
    l_text               := 'drop INDEX DWH_PERFORMANCE.I50_RTL_EMP_AVL_LC_JB_DY';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_PERFORMANCE.I50_RTL_EMP_AVL_LC_JB_DY');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
        l_text := 'index I50_RTL_EMP_AVL_LC_JB_DY does not exist';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_remove_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_remove_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end a_remove_indexes;
  
  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

  l_text := 'Insert into TEMP_S4S_AVAIL_004U ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
          insert /*+ append */
   into DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U

with selexta
 as
 (SELECT
    /*+ FULL(FC) FULL(FLR) parallel(flr,8) parallel(fc,8)  */
    distinct
            FLR.EMPLOYEE_ID ,
            FC.ORIG_CYCLE_START_DATE,
            fc.CYCLE_START_date,
            fc.CYCLE_end_date,
            fc.WEEK_NUMBER,
            FLR.DAY_OF_WEEK,
            FLR.AVAILABILITY_START_DATE,
            FLR.AVAILABILITY_END_DATE,
            FLR.FIXED_ROSTER_START_TIME,
            FLR.FIXED_ROSTER_END_TIME,
            FLR.NO_OF_WEEKS,
            FLR.MEAL_BREAK_MINUTES,
            de.sk1_employee_id,
            fc.this_week_start_date
  FROM dwh_foundation.FND_S4S_emp_avail_DY flr
  JOIN dwh_PERFORMANCE.TEMP_S4S_LOC_EMP_DY_part3 fc
        ON fc.employee_id             = flr.employee_id
        AND fc.availability_start_date = flr.availability_start_date
        AND fc.week_number            = FLR.WEEK_NUMBER
  join dwh_hr_performance.dim_employee de
  on DE.EMPLOYEE_ID = FC.EMPLOYEE_ID
--  where flr.employee_id not in  (
--'7078713','7052745','7071470','7070206 ','7005239','7075302','7042165','7051039','7053437','7090955'
--,'7080850','7074906','7072303','7095618','7095628','7096401','7096429',-
--'7033160','7070989','7043523','7039226')
  --SK1_EMPLOYEE_ID not in (1105841,1128052,1140310,1081138,1115483)
  ORDER BY de.sk1_employee_id,fc.CYCLE_START_date )
  ,
  selext1
  as 
 ( select  /*+ FULL(sea) FULL(dc) FULL(ej) FULL(el) parallel(sea,8) parallel(dc,8) parallel(ej,8) parallel(el,8) */
           sea.EMPLOYEE_ID ,
            sea.ORIG_CYCLE_START_DATE,
            sea.CYCLE_START_date,
            sea.CYCLE_end_date,
            sea.WEEK_NUMBER,
            sea.DAY_OF_WEEK,
            sea.AVAILABILITY_START_DATE,
            sea.AVAILABILITY_END_DATE,
            sea.FIXED_ROSTER_START_TIME,
            sea.FIXED_ROSTER_END_TIME,
            sea.NO_OF_WEEKS,
            sea.MEAL_BREAK_MINUTES,
            sea.sk1_employee_id,
            el.SK1_LOCATION_NO,
            ej.SK1_job_id,
            el.EMPLOYEE_STATUS,
            dc.calendar_date tran_date,
            el.EFFECTIVE_START_DATE,
            el.EFFECTIVE_END_DATE,
            ((( sea.fixed_roster_end_time - sea.fixed_roster_start_time) * 24 * 60) - sea.meal_break_minutes) / 60 FIXED_ROSTER_HRS
  FROM selexta sea
  JOIN DIM_CALENDAR DC
      ON DC.THIS_WEEK_START_DATE BETWEEN sea.CYCLE_START_DATE AND NVL(sea.CYCLE_END_DATE, NVL(sea.CYCLE_END_DATE, G_END_DATE))
       AND dc.this_week_start_date = sea.this_week_start_date
      AND dc.fin_day_no           = sea.DAY_OF_WEEK
  JOIN RTL_EMP_JOB_DY Ej
      ON Ej.SK1_EMPLOYEE_ID = sea.SK1_EMPLOYEE_ID
      AND Ej.TRAN_DATE      = dc.CALENDAR_DATE
  JOIN RTL_EMP_LOC_STATUS_DY El
        ON El.SK1_EMPLOYEE_ID = sea.SK1_EMPLOYEE_ID
        and EL.TRAN_DATE      = DC.CALENDAR_DATE
 where 
                                            -- SEA.last_updated_date = g_date 
                                             --                       or
                                (dc.calendar_date between SEA.cycle_start_date and g_end_date 
                                              --and availability_end_date is null
                                )
              )
  
  ,
  selext2 AS
  (
  SELECT DISTINCT se1.SK1_LOCATION_NO ,
                    se1.SK1_EMPLOYEE_ID ,
                    se1.SK1_JOB_ID ,
                    se1.TRAN_DATE ,
                    se1.AVAILABILITY_START_DATE ,
                    se1.NO_OF_WEEKS ,
                    se1.DAY_OF_WEEK ,
                    se1.ORIG_CYCLE_START_DATE ,
                    se1.CYCLE_START_DATE ,
                    se1.CYCLE_END_DATE ,
                    se1.WEEK_NUMBER ,
                    se1.AVAILABILITY_END_DATE ,
                    se1.FIXED_ROSTER_START_TIME ,
                    se1.FIXED_ROSTER_END_TIME ,
                    se1.MEAL_BREAK_MINUTES ,
                    se1.FIXED_ROSTER_HRS ,
                    --    rtl.sk1_employee_id rtl_exists ,
                    SE1.EMPLOYEE_STATUS ,
                    SE1.EFFECTIVE_START_DATE ,
                    SE1.EFFECTIVE_END_DATE ,
                    se1.employee_id ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NULL      THEN se1.ORIG_CYCLE_START_DATE
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NOT NULL      THEN se1.ORIG_CYCLE_START_DATE
      ELSE NULL
    END) derive_start_date ,
    (
    CASE
      WHEN SE1.EMPLOYEE_STATUS IN ('S')      THEN SE1.effective_START_DATE
      --   WHEN SE1.EMPLOYEE_STATUS IN ('H','I','R') AND SE1.availability_start_date >= se1.effective_start_date AND se1.availability_end_date IS NULL THEN to_date('19/10/2014','dd/mm/yyyy')
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NULL      THEN G_END_DATE
      WHEN SE1.EMPLOYEE_STATUS        IN ('H','I','R')      AND SE1.availability_start_date >= se1.effective_start_date      AND se1.availability_end_date   IS NOT NULL      THEN se1.availability_end_date
      ELSE NULL
      END) derive_end_date
  FROM selext1 SE1
  WHERE SE1.EMPLOYEE_STATUS        = 'S'
      OR ( SE1.EMPLOYEE_STATUS        IN ('H','I','R')
      AND SE1.availability_start_date >= se1.effective_start_date )
  ) 
  
SELECT /*+ full(se2) */ DISTINCT se2.SK1_LOCATION_NO ,
                se2.SK1_JOB_ID ,
                se2.SK1_EMPLOYEE_ID ,
                se2.TRAN_DATE ,
                se2.AVAILABILITY_START_DATE ,
                se2.NO_OF_WEEKS ,
                se2.DAY_OF_WEEK ,
                se2.ORIG_CYCLE_START_DATE ORIG_CYCLE_START_DATE ,
                SE2.CYCLE_START_DATE ,
                sE2.cycle_end_date ,
                se2.WEEK_NUMBER ,
                se2.AVAILABILITY_END_DATE ,
                se2.FIXED_ROSTER_START_TIME ,
                se2.FIXED_ROSTER_END_TIME ,
                se2.MEAL_BREAK_MINUTES ,
                se2.FIXED_ROSTER_HRS ,
                G_DATE LAST_UPDATED_DATE
FROM selext2 se2 
WHERE se2.TRAN_DATE BETWEEN derive_start_date AND derive_end_date
AND derive_start_date  IS NOT NULL
ORDER BY se2.SK1_LOCATION_NO
,se2.SK1_JOB_ID
,SE2.SK1_EMPLOYEE_ID
,se2.TRAN_DATE ;                  
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        L_TEXT := 'TEMP_S4S_AVAIL_004U : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        insert /*+ append */ into DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY
        with seldup as (select SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
        from DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U dup
        group by SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
        minus
        select SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
        from DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U dup
        group by SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
        having count(*) > 1)
        select tmp.* from DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U tmp, seldup sd
        where tmp.sk1_employee_id = sd.sk1_employee_id
        and tmp.sK1_LOCATION_NO = sd.sK1_LOCATION_NO
        and tmp.SK1_JOB_ID = sd.SK1_JOB_ID
        and tmp.TRAN_DATE = sd.tran_date;
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        L_TEXT := 'RTL_EMP_AVAIL_LOC_JOB_DY : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
----
--- these records delete dbecause employee has overlapping period
----

        insert /*+ append */ into dwh_performance.s4s_avail_004u_error_table
        select SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE, g_date
        from DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U dup
        group by SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
        having count(*) > 1;
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        L_TEXT := 'RTL_EMP_AVAIL_LOC_JOB_DY : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   exception
  WHEN no_data_found THEN
        l_text := 'no data found for insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end b_insert;

--**************************************************************************************************
-- create primary key and index
--  I30_RTL_EMP_AVL_LC_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	LAST_UPDATED_DATE
--  I40_RTL_EMP_AVL_LC_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
--  I50_RTL_EMP_AVL_LC_JB_DY	NONUNIQUE	VALID	NORMAL	N	NO		NO	SK1_EMPLOYEE_ID, TRAN_DATE
--  K_RTL_EMP_AVL_LC_JB_DY	UNIQUE	VALID	NORMAL	N	NO		NO	SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
--**************************************************************************************************
procedure c_add_indexes as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON RTL_EMP_AVAIL_LOC_JOB_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_AVAIL_LOC_JOB_DY', DEGREE => 8);

      l_text := 'create INDEX DWH_PERFORMANCE.I30_RTL_EMP_AVL_LC_JB_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I30_RTL_EMP_AVL_LC_JB_DY ON DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY (LAST_UPDATED_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I30_RTL_EMP_AVL_LC_JB_DY LOGGING NOPARALLEL') ;
      
      l_text := 'create INDEX DWH_PERFORMANCE.I40_RTL_EMP_AVL_LC_JB_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I40_RTL_EMP_AVL_LC_JB_DY ON DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY (SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I40_RTL_EMP_AVL_LC_JB_DY LOGGING NOPARALLEL') ;

      l_text := 'create INDEX DWH_PERFORMANCE.I50_RTL_EMP_AVL_LC_JB_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('CREATE INDEX DWH_PERFORMANCE.I50_RTL_EMP_AVL_LC_JB_DY ON DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY (SK1_EMPLOYEE_ID, TRAN_DATE)     
      TABLESPACE PRF_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_PERFORMANCE.I50_RTL_EMP_AVL_LC_JB_DY LOGGING NOPARALLEL') ;   

   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end c_add_indexes;


--**************************************************************************************************
-- create primary key and index
--  K_RTL_EMP_AVL_LC_JB_DY	UNIQUE	VALID	NORMAL	N	NO		NO	SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE
--**************************************************************************************************
procedure e_add_primary_key as
BEGIN
    l_text          := 'Running GATHER_TABLE_STATS ON RTL_EMP_AVAIL_LOC_JOB_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_AVAIL_LOC_JOB_DY', DEGREE => 8);

      l_text := 'alter table dwh_performance.RTL_EMP_AVAIL_LOC_JOB_DY add constraint PK_RTL_EMP_AVL_LC_JB_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_performance.RTL_EMP_AVAIL_LOC_JOB_DY add CONSTRAINT PK_RTL_EMP_AVL_LC_JB_DY PRIMARY KEY (SK1_LOCATION_NO, SK1_EMPLOYEE_ID, SK1_JOB_ID, TRAN_DATE)                    
      USING INDEX tABLESPACE PRF_MASTER  ENABLE');
  
   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in d_add_primary_key';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in d_add_primary_key';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end e_add_primary_key;

  --**************************************************************************************************
  --
  --
  --                    M  a  i  n    p  r  o  c  e  s  s
  --
  --
  --**************************************************************************************************
BEGIN
  IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
    g_forall_limit  := p_forall_limit;
  END IF;
  
  
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  l_text := 'LOAD OF RTL_EMP_JOB_DY  EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  
  --**************************************************************************************************
  -- Set dates
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  -- derivation of end_date for recs where null.
  --  = 21days+ g_date+ days for rest of week
    SELECT distinct THIS_WEEK_END_DATE into g_end_date
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE = g_date + 20;
    
  l_text             := 'Derived g_end_date - '||g_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  --**************************************************************************************************
  -- Prepare environment
  --**************************************************************************************************
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  
  l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_S4S_LOC_EMP_DY_PART3', DEGREE => 8);
   l_text := 'Completed GATHER_TABLE_STATS ON TEMP_S4S_LOC_EMP_DY_PART3';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  a_remove_indexes;

  l_text := 'Truncating RTL_EMP_AVAIL_LOC_JOB_DY';
  DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
  execute immediate ('truncate table DWH_PERFORMANCE.RTL_EMP_AVAIL_LOC_JOB_DY');
  
  l_text := 'Running GATHER_TABLE_STATS ON RTL_EMP_AVAIL_LOC_JOB_DY';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_EMP_AVAIL_LOC_JOB_DY', DEGREE => 8);

  l_text := 'Truncating TEMP_S4S_AVAIL_004U';
  DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
  execute immediate ('truncate table DWH_PERFORMANCE.TEMP_S4S_AVAIL_004U');


  b_insert;

   c_add_indexes;

 --  d_delete_prf;

   e_add_primary_key;
 
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




END WH_PRF_S4S_004U_OLD;
