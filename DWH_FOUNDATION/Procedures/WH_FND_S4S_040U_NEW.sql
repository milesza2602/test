--------------------------------------------------------
--  DDL for Procedure WH_FND_S4S_040U_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_S4S_040U_NEW" 
(p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Update employee_status information for Scheduling for Staff(S4S)
--               Tran_type = SCREPS
--
--  Tables:      AIT load - STG_S4S_EMP_LOC_STATUS
--               Input    - STG_S4S_EMP_LOC_STATUS_cpy
--               Output   - FND_S4S_EMP_LOC_STATUS
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--               General process :
--               ----------------   
--               NON-standard process
--               ie.
--               1. truncate performance day table (target table)
--               2. a_remove_indexes from performance day table (target table)
--               3. b_insert into performance day table (target table)
--               4. c_add_indexes back onto performance day table (target table)
--               5. e_add_primary_key back onto performance day table (target table)
--
--
--
--               Delete process :
--               ----------------   
--                 The records are written to an audit table before they are deleted from FOUNDATION.
--                 Any changes at source result in the employee's full history being sent.
--                          and therefore we have to delete the current records for the employee (based upon employee_id only)
--                              the current data and load the new data.
--                 The audit table = dwh_foundation.FND_S4S_EMPLOCSTATUS_del_list  -- NOT used anywhaere else.
--
--                Validation :
--                ------------
--                  Normal validation done on dimension data.
--                  Check for overlapping periods is done - NOTE that this is not based on the detail used in the performance roll-ups
--                                                   but a generic one.
--                  We should not be sent any records for an employee where the dates overlap for the effective periods.
--                  The effective_start_date can be any day but the effective_end_date will be sent as the effective_start_dtae of the next period 
--                     but we will subtract 1 day from it to derive the effective_end_date of the previous period.
--                  This all depends on the derivation criteria.
--                  eg. RECORD 1 : effective_start_date = '1 jan 2015'  effective_end_date = '12 january 2015'
--                      RECORD 2 : effective_start_date = '12 jan 2015'  effective_end_date = NULL
--                      therefore we process as ..........
--                            RECORD 1 : effective_start_date = '1 jan 2015'  effective_end_date = '11 january 2015' **** note changed end_date
--                            RECORD 2 : effective_start_date = '12 jan 2015'  effective_end_date = NULL
--
--                Multiple batches :
--                -----------------
--                  Regardless of how many batches are sent, we derive the latest sys_source_batch_id for an employee_id 
--                  and then process the latest sys_source_sequence_no for the employee_id, location_no, effective_start_date.
--                 
--
--                Data Takeon option :
--                -----------------
--                  The code in this procedure can be changed to allow for a data takeon.
--                    ie. TRUNCATE TABLE dwh_foundation.FND_S4S_EMPLOCSTATUS_del_list
--                        TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_LOC_STATUS
--                        TRUNCATE TABLE dwh_foundation.STG_S4S_EMP_LOC_STATUS_HSP
--
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--  Maintenance:
--   24 nov 2014 - w lyttle - coding added for overlapping period check
--   22 jan 2015 - w lyttle - coding added for open_ended processing when no update received :
--                 1. Records need to be appended to the 'delete list table' where the 'end_date' is null.
--                     This will ensure that from this point forward the performance records for these will be deleted and reloaded.
--                 2. The foundation fact table will have LAST_UPDATED_DATE =batch_date where any records have a null 'end_date'.
--                     This is to ensure that from this point onwards, forecasting will be applied.
--  4 march 2015 w lyttle - added data takeon option
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
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      DWH_FOUNDATION.STG_S4S_EMP_LOC_STATUS_hsp.sys_process_msg%type;
g_rec_out            DWH_FOUNDATION.FND_S4S_EMP_LOC_STATUS%rowtype;

g_found              boolean;
g_valid              boolean;

g_eff_end_date       date ;
g_date               date          := trunc(sysdate);
g_run_date           date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs               number        :=  0;
g_recs_deleted       integer       :=  0;
g_data_takeon       varchar2(1);


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_S4S_040U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE employee_status data ex S4S';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  -- For output arrays into bulk load forall statements --
type tbl_array_i is table of FND_S4S_EMP_LOC_STATUS%rowtype index by binary_integer;
type tbl_array_u is table of FND_S4S_EMP_LOC_STATUS%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of STG_S4S_EMP_LOC_STATUS_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of STG_S4S_EMP_LOC_STATUS_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;





--**************************************************************************************************
-- Delete records from Foundation
-- based on employee_id  as we will get full history for an employee each time from source
--**************************************************************************************************
procedure a_delete_fnd as
begin

      g_recs_inserted := 0;

      select max(run_seq_no)+1 into g_run_seq_no
      from dwh_foundation.FND_S4S_EMPLOCSTATUS_del_list;
      
      If g_run_seq_no is null
      then g_run_seq_no := 1;
      end if;
      
      g_run_date := trunc(sysdate);

     BEGIN
     insert /*+ append */ into dwh_foundation.FND_S4S_EMPLOCSTATUS_del_list
     with selstg
            as (select distinct employee_id from STG_S4S_EMP_LOC_STATUS_cpy)
     select g_run_date, g_date, g_run_seq_no, 
              f.EMPLOYEE_ID
            , f.LOCATION_NO
            , f.EMPLOYEE_STATUS
            , f.EMPLOYEE_WORKSTATUS
            , f.EFFECTIVE_START_DATE
            , f.EFFECTIVE_END_DATE
            , f.LAST_UPDATED_DATE
        from DWH_FOUNDATION.FND_S4S_EMP_LOC_STATUS f, selstg s
        where f.employee_id = s.employee_id;
        
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_inserted := g_recs;
                
          l_text := 'Insert into FND_S4S_EMPLOCSTATUS_del_list recs='||g_recs_inserted
          ||' - run_date='||g_run_date
          ||' - batch_date='||g_date
          ||' - run_seq_no='||g_run_seq_no;
          dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

         delete from DWH_FOUNDATION.FND_S4S_EMP_LOC_STATUS
         where employee_id IN (select distinct employee_id from dwh_foundation.STG_S4S_EMP_LOC_STATUS_cpy)
         ;
     
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_deleted := g_recs;
          l_text := 'Deleted from DWH_FOUNDATION.FND_S4S_EMP_LOC_STATUS recs='||g_recs_deleted
          ||' - run_date='||g_run_date
          ||' - batch_date='||g_date
          ||' - run_seq_no='||g_run_seq_no;
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      
      exception
         when no_data_found then
                l_text := 'No deletions done for DWH_FOUNDATION.FND_S4S_EMP_LOC_STATUS ';
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

end a_delete_fnd;


procedure b_insert_fnd as 
  --**************************************************************************************************
  -- Insert into foundation table
  --**************************************************************************************************

 
   g_recs_inserted := null;
   g_recs := null;
  
   insert /*+ append */
   into DWH_FOUNDATION.FND_S4S_EMP_LOC_STATUS
            WITH selbat AS
                          (SELECT
                            /*+ FULL(STG)  */
                                              MAX(sys_source_batch_id) maxbat,
                                              employee_id
                                        FROM dwh_foundation.STG_S4S_EMP_LOC_STATUS_cpy STG
                                        GROUP BY employee_id
                          ) ,
              selseq AS
                        (SELECT
                          /*+ FULL(STG)  */
                                                MAX(sys_source_sequence_no) maxseq,
                                                maxbat,
                                                STG.EMPLOYEE_ID ,
                                                stg.location_no,
                                                STG.EFFECTIVE_START_DATE
                                          FROM selbat sb,
                                                dwh_foundation.STG_S4S_EMP_LOC_STATUS_cpy stg
                                          WHERE stg.EMPLOYEE_ID = sb.employee_id
                                          AND SB.MAXBAT         = STG.sys_source_batch_id
                                          GROUP BY maxbat,
                                                  STG.EMPLOYEE_ID ,
                                                  stg.location_no ,
                                                  STG.EFFECTIVE_START_DATE
                        ) ,
              selall AS
                        (SELECT
                          /*+ FULL(STG)  */
                                                SYS_SOURCE_BATCH_ID,
                                                sys_source_sequence_no,
                                                STG.EMPLOYEE_ID ,
                                                stg.location_no,
                                                STG.EFFECTIVE_START_DATE ,
                                                STG.EFFECTIVE_end_DATE
                                          FROM selseq ss,
                                                 dwh_foundation.STG_S4S_EMP_LOC_STATUS_cpy stg
                                          WHERE stg.EMPLOYEE_ID        = sS.employee_id
                                          AND stg.location_no          = sS.location_no
                                          AND stg.EFFECTIVE_START_DATE = sS.EFFECTIVE_START_DATE
                                          AND SS.MAXBAT                = STG.sys_source_batch_id
                                          AND SS.MAXSEQ                = STG.sys_source_sequence_no
                                          GROUP BY SYS_SOURCE_BATCH_ID,
                                                    sys_source_sequence_no,
                                                    STG.EMPLOYEE_ID ,
                                                    stg.location_no,
                                                    STG.EFFECTIVE_START_DATE ,
                                                    STG.EFFECTIVE_end_DATE
                        ) ,
              selover AS
                        (SELECT
                          /*+ FULL(STG2) parallel(stg2,4)  */
                                                      DISTINCT stg2.employee_id,
                                                             stg2.effective_start_date
                                                FROM DWH_FOUNDATION.STG_S4S_EMP_LOC_STATUS_cpy stg2 ,
                                                      (SELECT
                                                        /*+ FULL(STG) parallel(stg,4)  */
                                                                          STG.EMPLOYEE_ID ,
                                                                          calendar_date,  
                                                                          COUNT(*)
                                                                    FROM DWH_FOUNDATION.STG_S4S_EMP_LOC_STATUS_cpy STG ,
                                                                          dim_calendar dc,
                                                                          selseq ss
                                                                    WHERE (dc.calendar_date BETWEEN stg.effective_start_date AND NVL(stg.EFFECTIVE_END_DATE - 1, g_eff_end_date)
                                                                                AND ss.maxseq               = stg.sys_source_sequence_no
                                                                                AND ss.maxbat               = stg.sys_source_batch_id
                                                                                AND ss.EMPLOYEE_ID          = stg.EMPLOYEE_ID
                                                                                AND ss.EFFECTIVE_START_DATE = stg.EFFECTIVE_START_DATE
                                                                                AND STG.EMPLOYEE_STATUS    IN ('H','I','R'))
                                                                    OR (dc.calendar_date        = stg.effective_start_date
                                                                              AND ss.maxseq               = stg.sys_source_sequence_no
                                                                              AND ss.maxbat               = stg.sys_source_batch_id
                                                                              AND ss.EMPLOYEE_ID          = stg.EMPLOYEE_ID
                                                                              AND ss.EFFECTIVE_START_DATE = stg.EFFECTIVE_START_DATE
                                                                              AND STG.EMPLOYEE_STATUS    IN ('S'))
                                                                    GROUP BY STG.EMPLOYEE_ID ,
                                                                              calendar_date
                                                                    HAVING COUNT(*) > 1
                                                      ) sel
                                                WHERE sel.employee_id = stg2.employee_id
                                                AND sel.calendar_date BETWEEN stg2.effective_start_date AND NVL(stg2.EFFECTIVE_END_DATE - 1, g_eff_end_date)
                                  ),
  seljoin as (          SELECT
              /*+ FULL(STG) */
                            stg.SYS_SOURCE_BATCH_ID,
                            stg.SYS_SOURCE_SEQUENCE_NO,
                            stg.SYS_LOAD_DATE,
                            stg.SYS_PROCESS_CODE,
                            stg.SYS_LOAD_SYSTEM_NAME,
                            stg.SYS_MIDDLEWARE_BATCH_ID,
                            stg.SYS_PROCESS_MSG,
                            STG.SOURCE_DATA_STATUS_CODE,
                            STG.LOCATION_no LOCATION_NO,
                            STG.EMPLOYEE_ID EMPLOYEE_ID,
                            dc2.calendar_date EFFECTIVE_START_DATE,
                            STG.EFFECTIVE_END_DATE EFFECTIVE_END_DATE,
                            STG.EMPLOYEE_WORKSTATUS EMPLOYEE_WORKSTATUS,
                            STG.EMPLOYEE_STATUS EMPLOYEE_STATUS,
                            se.employee_id overlap_employee_id
                  FROM DWH_FOUNDATION.STG_S4S_EMP_LOC_STATUS_cpy stg,
                            fnd_location fl,
                            dwh_hr_performance.DIM_EMPLOYEE fe,
                            dim_calendar dc2,
                            selover se,
                            SELALL SA
                  WHERE stg.location_no        = fl.location_no
                  AND stg.EMPLOYEE_ID          = fe.employee_id
                  AND stg.EFFECTIVE_START_DATE = dc2.calendar_date
                  AND stg.employee_id          = se.employee_id(+)
                  AND stg.effective_start_date = se.effective_start_date(+)
                  AND stg.EMPLOYEE_ID          = sa.employee_id
                  AND stg.EFFECTIVE_START_DATE = sa.EFFECTIVE_START_DATE
                    ------------------------------------------
                    ---   The coding was ....AND stg.effective_END_DATE = sa.effective_END_DATE
                    --    but had to change to an arbitrary value when doing the join
                    --     as it won't join a null value to a null value
                    --     which is quite a valid scenario for this extract
                    ------------------------------------------
                  AND NVL(stg.EFFECTIVE_END_DATE, to_date('01/01/3099', 'dd/mm/yyyy')) = NVL(sa.EFFECTIVE_END_DATE, to_date('01/01/3099', 'dd/mm/yyyy'))
                  AND stg.SYS_SOURCE_BATCH_ID                                          = sa.SYS_SOURCE_BATCH_ID
                  AND stg.sys_source_sequence_no                                       = sa.sys_source_sequence_no)
                  select        EMPLOYEE_ID,
                                LOCATION_NO,
                                EMPLOYEE_STATUS,
                                EMPLOYEE_WORKSTATUS,
                                EFFECTIVE_START_DATE,
                                EFFECTIVE_END_DATE,
                                g_date LAST_UPDATED_DATE,
                                null TEMP_EFFECTIVE_END_DATE,
                                null PREV_EFFECTIVE_END_DATE  
                  from seljoin
                  where sj.overlap_employee_id is null
                  ORDER BY sys_source_batch_id,
                    sys_source_sequence_no;
                    

   g_recs :=SQL%ROWCOUNT ;
   COMMIT;
   g_recs_inserted := g_recs;
                      
   l_text := 'recs inserted ='||g_recs_inserted;
   dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);

    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_LOC_STATUS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
       DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_EMP_LOC_STATUS', DEGREE => 8);  


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
                      ' '||a_tbl_insert(g_error_index).LOCATION_NO||
                      ' '||a_tbl_insert(g_error_index).EMPLOYEE_ID||
                         ' '||a_tbl_insert(g_error_index).EMPLOYEE_STATUS||
                      ' '||a_tbl_insert(g_error_index).EFFECTIVE_START_DATE;

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end b_insert_fnd;

procedure c_insert_hospital as 
  --**************************************************************************************************
  -- Insert into hospital
  --**************************************************************************************************
begin

  INSERT /*+ append */ 
  into DWH_FOUNDATION.STG_S4S_EMP_LOC_STATUS_hsp

            WITH selbat AS
                          (SELECT
                            /*+ FULL(STG)  */
                                              MAX(sys_source_batch_id) maxbat,
                                              employee_id
                                        FROM dwh_foundation.STG_S4S_EMP_LOC_STATUS_cpy STG
                                        GROUP BY employee_id
                          ) ,
              selseq AS
                        (SELECT
                          /*+ FULL(STG)  */
                                                MAX(sys_source_sequence_no) maxseq,
                                                maxbat,
                                                STG.EMPLOYEE_ID ,
                                                stg.location_no,
                                                STG.EFFECTIVE_START_DATE
                                          FROM selbat sb,
                                                dwh_foundation.STG_S4S_EMP_LOC_STATUS_cpy stg
                                          WHERE stg.EMPLOYEE_ID = sb.employee_id
                                          AND SB.MAXBAT         = STG.sys_source_batch_id
                                          GROUP BY maxbat,
                                                  STG.EMPLOYEE_ID ,
                                                  stg.location_no ,
                                                  STG.EFFECTIVE_START_DATE
                        ) ,
              selall AS
                        (SELECT
                          /*+ FULL(STG)  */
                                                SYS_SOURCE_BATCH_ID,
                                                sys_source_sequence_no,
                                                STG.EMPLOYEE_ID ,
                                                stg.location_no,
                                                STG.EFFECTIVE_START_DATE ,
                                                STG.EFFECTIVE_end_DATE
                                          FROM selseq ss,
                                                 dwh_foundation.STG_S4S_EMP_LOC_STATUS_cpy stg
                                          WHERE stg.EMPLOYEE_ID        = sS.employee_id
                                          AND stg.location_no          = sS.location_no
                                          AND stg.EFFECTIVE_START_DATE = sS.EFFECTIVE_START_DATE
                                          AND SS.MAXBAT                = STG.sys_source_batch_id
                                          AND SS.MAXSEQ                = STG.sys_source_sequence_no
                                          GROUP BY SYS_SOURCE_BATCH_ID,
                                                    sys_source_sequence_no,
                                                    STG.EMPLOYEE_ID ,
                                                    stg.location_no,
                                                    STG.EFFECTIVE_START_DATE ,
                                                    STG.EFFECTIVE_end_DATE
                        ) ,
              selover AS
                        (SELECT
                          /*+ FULL(STG2) parallel(stg2,4)  */
                                                      DISTINCT stg2.employee_id,
                                                             stg2.effective_start_date
                                                FROM DWH_FOUNDATION.STG_S4S_EMP_LOC_STATUS_cpy stg2 ,
                                                      (SELECT
                                                        /*+ FULL(STG) parallel(stg,4)  */
                                                                          STG.EMPLOYEE_ID ,
                                                                          calendar_date,  
                                                                          COUNT(*)
                                                                    FROM DWH_FOUNDATION.STG_S4S_EMP_LOC_STATUS_cpy STG ,
                                                                          dim_calendar dc,
                                                                          selseq ss
                                                                    WHERE (dc.calendar_date BETWEEN stg.effective_start_date AND NVL(stg.EFFECTIVE_END_DATE - 1, g_eff_end_date)
                                                                                AND ss.maxseq               = stg.sys_source_sequence_no
                                                                                AND ss.maxbat               = stg.sys_source_batch_id
                                                                                AND ss.EMPLOYEE_ID          = stg.EMPLOYEE_ID
                                                                                AND ss.EFFECTIVE_START_DATE = stg.EFFECTIVE_START_DATE
                                                                                AND STG.EMPLOYEE_STATUS    IN ('H','I','R'))
                                                                    OR (dc.calendar_date        = stg.effective_start_date
                                                                              AND ss.maxseq               = stg.sys_source_sequence_no
                                                                              AND ss.maxbat               = stg.sys_source_batch_id
                                                                              AND ss.EMPLOYEE_ID          = stg.EMPLOYEE_ID
                                                                              AND ss.EFFECTIVE_START_DATE = stg.EFFECTIVE_START_DATE
                                                                              AND STG.EMPLOYEE_STATUS    IN ('S'))
                                                                    GROUP BY STG.EMPLOYEE_ID ,
                                                                              calendar_date
                                                                    HAVING COUNT(*) > 1
                                                      ) sel
                                                WHERE sel.employee_id = stg2.employee_id
                                                AND sel.calendar_date BETWEEN stg2.effective_start_date AND NVL(stg2.EFFECTIVE_END_DATE - 1, g_eff_end_date)
                                  ),
  seljoin as (          SELECT
              /*+ FULL(STG) */
                        stg.SYS_SOURCE_BATCH_ID,
                        stg.SYS_SOURCE_SEQUENCE_NO,
                        stg.SYS_LOAD_DATE,
                        stg.SYS_PROCESS_CODE,
                        stg.SYS_LOAD_SYSTEM_NAME,
                        stg.SYS_MIDDLEWARE_BATCH_ID,
                        stg.SYS_PROCESS_MSG,
                        STG.SOURCE_DATA_STATUS_CODE,
                        STG.LOCATION_no stg_LOCATION_NO,
                        STG.EMPLOYEE_ID stg_EMPLOYEE_ID,
                        STG.EFFECTIVE_START_DATE stg_EFFECTIVE_START_DATE,
                        STG.EFFECTIVE_END_DATE stg_EFFECTIVE_END_DATE,
                        STG.EMPLOYEE_WORKSTATUS stg_EMPLOYEE_WORKSTATUS,
                        STG.EMPLOYEE_STATUS stg_EMPLOYEE_STATUS,
                        fl.LOCATION_NO fl_LOCATION_NO,
                        fe.EMPloyee_id fe_EMPLOYEE_ID,
                        dc2.calendar_date dc2_EFFECTIVE_START_DATE,
                        se.employee_id overlap_employee_id
   from DWH_FOUNDATION.STG_S4S_EMP_LOC_STATUS_cpy stg,
        fnd_location fl,
        dwh_hr_performance.DIM_EMPLOYEE fe,
        dim_calendar dc2,
        selover se,
        SELALL SA
   where stg.location_no = fl.location_no(+)
     and stg.EMPLOYEE_ID = fe.employee_id(+)
     and stg.EFFECTIVE_START_DATE = dc2.calendar_date(+)
     and stg.employee_id = se.employee_id(+)
     and stg.effective_start_date = se.effective_start_date(+)
 AND stg.EMPLOYEE_ID             = sa.employee_id
AND stg.EFFECTIVE_START_DATE = sa.EFFECTIVE_START_DATE
------------------------------------------
---   The coding was ....AND stg.effective_END_DATE = sa.effective_END_DATE
--    but had to change to an arbitrary value when doing the join
--     as it won't join a null value to a null value 
--     which is quite a valid scenario for this extract
------------------------------------------
AND nvl(stg.EFFECTIVE_END_DATE, to_date('01/01/3099', 'dd/mm/yyyy')) = nvl(sa.EFFECTIVE_END_DATE, to_date('01/01/3099', 'dd/mm/yyyy'))
AND stg.SYS_SOURCE_BATCH_ID     = sa.SYS_SOURCE_BATCH_ID 
AND stg.sys_source_sequence_no     = sa.sys_source_sequence_no)
select 

   order by sys_source_batch_id,sys_source_sequence_no;


        where fl_LOCATION_NO is null
        or fl_LOCATION_NO  is null
        or fe_EMPLOYEE_ID  is null
        or fJ_JOB_ID  is null
        or STG_SHIFT_CLOCK_IN IS NULL 
        or STG_SHIFT_CLOCK_OUT IS NULL;
        
      g_recs_hospital := g_recs_hospital + sql%rowcount;    
commit;

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
                      ' '||a_tbl_insert(g_error_index).LOCATION_NO||
                      ' '||a_tbl_insert(g_error_index).EMPLOYEE_ID||
                         ' '||a_tbl_insert(g_error_index).EMPLOYEE_STATUS||
                      ' '||a_tbl_insert(g_error_index).EFFECTIVE_START_DATE;

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end c_insert_hospital;


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

    l_text := 'LOAD THE LOC_EMP_status data ex S4S STARTED AT '||
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
   -- derivation of cobnstr_end_date for recs where null.
    --  = 21days+ g_date+ days for rest of week 
    
    SELECT distinct THIS_WEEK_END_DATE into g_eff_end_date
    FROM DIM_CALENDAR 
    WHERE CALENDAR_DATE = g_date + 20;
    l_text := 'g_eff_end_date= '||g_eff_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  
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
        l_text := 'TRUNCATE TABLE dwh_foundation.FND_S4S_EMPLOCSTATUS_del_list';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        EXECUTE IMMEDIATE ('TRUNCATE TABLE dwh_foundation.FND_S4S_EMPLOCSTATUS_del_list');
        l_text := 'TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_LOC_STATUS';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        EXECUTE IMMEDIATE ('TRUNCATE TABLE dwh_foundation.FND_S4S_EMP_LOC_STATUS');
        l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_LOC_STATUS';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                       'FND_S4S_EMP_LOC_STATUS', DEGREE => 8);
        l_text := 'TRUNCATE TABLE dwh_foundation.STG_S4S_EMP_LOC_STATUS_HSP';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        EXECUTE IMMEDIATE ('TRUNCATE TABLE dwh_foundation.STG_S4S_EMP_LOC_STATUS_HSP');
   END IF;
  
     a_delete_fnd;
     
     b_insert_fnd;
     
     c_insert_hospital;
     

 
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



END WH_FND_S4S_040U_NEW;
