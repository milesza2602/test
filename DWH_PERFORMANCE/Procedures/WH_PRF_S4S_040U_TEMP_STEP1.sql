--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_040U_TEMP_STEP1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_040U_TEMP_STEP1" (p_forall_limit in integer,p_success out boolean) as
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_040U_TEMP_STEP1';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_EMP_LOC_STATUS_DY data  EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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
--
-- Look up batch date from dim_control
--
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

--**************************************************************************************************
-- Set database runtime parameters
--**************************************************************************************************
    execute immediate 'alter session set workarea_size_policy=manual';
    execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
--
-- Prepare table for loading
--
--**************************************************************************************************
    l_text := 'Dropping and recreating temp table : TEMP_S4S_RTL_EMPLOCSTATUSDY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    EXECUTE IMMEDIATE('DROP TABLE dwh_performance.TEMP_S4S_RTL_EMPLOCSTATUSDY');
    COMMIT;
    
    EXECUTE IMMEDIATE('create table dwh_performance.TEMP_S4S_RTL_EMPLOCSTATUSDY
                       as select * from RTL_EMP_LOC_STATUS_DY
                       where sk1_location_no is null');
    COMMIT;
    
    l_text := 'Running GATHER_TABLE_STATS ON FND_S4S_EMP_loc_status';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
                                   'FND_S4S_EMP_loc_status', DEGREE => 8);

--**************************************************************************************************
--
-- Load expanded data into temp table for new/updated data
--
--**************************************************************************************************
    l_text := 'Load new/updated data into TEMP_S4S_RTL_EMPLOCSTATUSDY STARTED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

begin
    insert /*+ append */ into dwh_performance.TEMP_S4S_RTL_EMPLOCSTATUSDY
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
--                            AND flr.employee_id = '7089578'
--                              and flr.location_no = 118

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
          ,g_date
          from selext2 se2
          where se2.tran_DATE BETWEEN derive_start_date AND derive_end_date
          order by se2.SK1_LOCATION_NO
          ,se2.SK1_EMPLOYEE_ID
          ,se2.TRAN_DATE
          ;
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_inserted := g_recs;
                
    l_text := 'Load new/updated data into TEMP_S4S_RTL_EMPLOCSTATUSDY ENDED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'Load new/updated data into  TEMP_S4S_RTL_EMPLOCSTATUSDY recs='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  exception
         when no_data_found then
                l_text := 'No data to load';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
   end;          

   g_recs_inserted  :=0;

--**************************************************************************************************
--
-- Load expanded data into temp table for existing data to be projected
-- eventhough coding has some duplication am leaving for most part as per previous coded version
--
--**************************************************************************************************
    l_text := 'Load existing data to be projected into TEMP_S4S_RTL_EMPLOCSTATUSDY STARTED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

begin
    insert /*+ append */ into dwh_performance.TEMP_S4S_RTL_EMPLOCSTATUSDY
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
                            flr.last_updated_date <> g_date
                            and FLR.EFFECTIVE_END_DATE is null
                            and EMPLOYEE_STATUS        IN ('H','I','R', 'S')
                       --       AND flr.employee_id = '7089578'
                       --       and flr.location_no = 118
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
          , g_date
          from selext2 se2
          where se2.tran_DATE BETWEEN derive_start_date AND derive_end_date
          order by se2.SK1_LOCATION_NO
          ,se2.SK1_EMPLOYEE_ID
          ,se2.TRAN_DATE
          ;
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_inserted := g_recs;
                
    l_text := 'Load existing data to be projected into TEMP_S4S_RTL_EMPLOCSTATUSDY ENDED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'Load existing data to be projected into  TEMP_S4S_RTL_EMPLOCSTATUSDY recs='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  exception
         when no_data_found then
                l_text := 'No existing data to be projected data to load';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
   end;          

   g_recs_inserted  :=0;

--**************************************************************************************************
--
-- Gather stats on temp table
--
--**************************************************************************************************
    l_text := 'Running GATHER_TABLE_STATS ON TEMP_S4S_RTL_EMPLOCSTATUSDY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_S4S_RTL_EMPLOCSTATUSDY', DEGREE => 8);

--**************************************************************************************************
--
-- Delete records from Performance
-- based on employee_id and effective_start_date
-- before loading from staging
--
--**************************************************************************************************
      l_text := 'Starting delete of RTL_EMP_LOC_STATUS_DY';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      g_recs_inserted := 0;

      select max(run_seq_no) 
             into g_run_seq_no
      from dwh_foundation.FND_S4S_EMPLOCSTATUS_DEL_LIST
      where batch_date = g_date;
      
      If g_run_seq_no is null
      then 
                 select max(run_seq_no) 
                         into g_run_seq_no
                  from dwh_foundation.FND_S4S_EMPLOCSTATUS_DEL_LIST;
                  If g_run_seq_no is null
                  then
                       g_run_seq_no := 1;
                  end if;
      end if;
      g_run_date := trunc(sysdate);

      BEGIN
        DELETE
        FROM DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY B
        WHERE EXISTS
                    (SELECT DISTINCT SK1_employee_id,
                                     effective_start_date
                    FROM dwh_foundation.FND_S4S_EMPLOCSTATUS_DEL_LIST A,
                          DWH_HR_PERFORMANCE.dim_employee DE
                    WHERE run_seq_no           = g_run_seq_no
                    AND A.EMPLOYEE_ID          = DE.EMPLOYEE_ID
                    AND B.SK1_employee_id      = DE.SK1_EMPLOYEE_ID
                    AND B.effective_start_date = A.EFFECTIVE_START_DATE
                    )
        OR (EFFECTIVE_END_DATE IS NULL AND EMPLOYEE_STATUS        IN ('H','I','R', 'S'));
                    
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        g_recs_deleted := g_recs;
        l_text         := 'Deleted from DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      EXCEPTION
      WHEN no_data_found THEN
        l_text := 'No deletions done for DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY ';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
      END;
      
      g_recs_inserted :=0;
      g_recs_deleted  := 0;

--**************************************************************************************************
--
-- Load expanded data into temp table for existing data to be projected
--
--**************************************************************************************************
    l_text := 'Load TEMP_S4S_RTL_EMPLOCSTATUSDY into RTL_EMP_LOC_STATUS_DY STARTED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

begin
    insert /*+ append */ 
    into DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_DY
           with selext as 
           (             select distinct 
                                SK1_LOCATION_NO
                                ,SK1_EMPLOYEE_ID
                                ,max(EFFECTIVE_START_DATE) EFFECTIVE_START_DATE
                                ,tran_date
                          from dwh_performance.TEMP_S4S_RTL_EMPLOCSTATUSDY
                          group by SK1_LOCATION_NO
                                ,SK1_EMPLOYEE_ID
                                ,tran_date)
              select 
                  tmp.SK1_LOCATION_NO
                  ,tmp.SK1_EMPLOYEE_ID
                  ,tmp.TRAN_DATE
                  ,tmp.EMPLOYEE_STATUS
                  ,tmp.EMPLOYEE_WORKSTATUS
                  ,tmp.EFFECTIVE_START_DATE
                  ,tmp.EFFECTIVE_END_DATE
                  ,tmp.LAST_UPDATED_DATE
            from selext se,
                 dwh_performance.TEMP_S4S_RTL_EMPLOCSTATUSDY tmp
           where se.sk1_location_no = tmp.sk1_location_no
           and se.SK1_EMPLOYEE_ID = tmp.SK1_EMPLOYEE_ID
           AND se.tran_date = tmp.tran_date
           AND se.EFFECTIVE_START_DATE = tmp.EFFECTIVE_START_DATE
          ;
          g_recs :=SQL%ROWCOUNT ;
          COMMIT;
          g_recs_inserted := g_recs;
                
    l_text := 'Load TEMP_S4S_RTL_EMPLOCSTATUSDY into RTL_EMP_LOC_STATUS_DY ENDED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'Load TEMP_S4S_RTL_EMPLOCSTATUSDY into RTL_EMP_LOC_STATUS_DY recs='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  exception
         when no_data_found then
                l_text := 'No existing data to be projected data to load';
      dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
   end;          



--**************************************************************************************************
--
-- Write final log data
--
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


END WH_PRF_S4S_040U_TEMP_STEP1;
