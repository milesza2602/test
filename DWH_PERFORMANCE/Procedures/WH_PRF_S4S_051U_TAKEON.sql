--------------------------------------------------------
--  DDL for Procedure WH_PRF_S4S_051U_TAKEON
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_S4S_051U_TAKEON" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
  --*** ADD CONSTRAINT_DATE - NEED EXTRA GEN RECS INBETWEEN
-- might need to remove CONSTRAINT_DATE, no of weeks from table
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Wendy lyttle
--  Purpose:     Load Employee Schedule information for Scheduling for Staff(S4S)
--
--               Delete process :
--                 Due to changes which can be made, we have to drop the current data and load the new data
--                        based upon employee_id and trunc(shift_clock_in)
--
--                The delete lists are used in the rollups as well.
--                The delete lists were created in the STG to FND load  
--                ie. FND_S4S_SCHLOCEMPJBDY_del_list
--
--
--                Other Processing:
--                nb. we are working with SHIFT_CLOCK_IN and SHIFT_CLOCK_OUT
--                      which consist of date+time.
--                    this must be remmebered when doing joins to other tables as they
--                        use date only.
--
--               nb. FULL OUTER JOIN done between sumed(dwh_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY)
--                              and RTL_ABSENCE_EMP_WK
--                               is because someone can be anbent for a week 
--                                   but have no schedule
--
--
--  Tables:      Input    - dwh_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY
--               Output   - DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK  
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
g_rec_out            dwh_performance.RTL_SCH_LOC_EMP_JB_WK%rowtype;
g_found              boolean;
G_THIS_WEEK_START_DATE date;
g_fin_days number;
g_constr_end_date  date;


g_date               date          := trunc(sysdate);
g_run_date               date          := trunc(sysdate);
g_run_seq_no         number        :=  0;
g_recs         number        :=  0;
g_recs_deleted      integer       :=  0;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_S4S_051U_TAKEON';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RTL_SCH_LOC_EMP_JB_WK data EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.RTL_SCH_LOC_EMP_JB_WK%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.RTL_SCH_LOC_EMP_JB_WK%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_LOCATION is
WITH selext1a AS
  (SELECT
          /*+ full(flr) parallel(flr,6) */
          FLR.SK1_JOB_ID ,
          FLR.SK1_LOCATION_NO ,
          FLR.sk1_employee_ID ,
          DC.fin_year_no ,
          DC.fin_week_no ,
          EMPLOYEE_RATE,
          SUM(NVL(NETT_SCHEDULED_HOURS,0)) total_NETT_SCHEDULED_HOURS
  FROM dwh_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY flr
  JOIN dwh_PERFORMANCE.DIM_CALENDAR DC
         ON TRUNC(flr.shift_clock_in) = dc.calendar_date
  JOIN DWH_PERFORMANCE.RTL_EMP_JOB_WK RE
        ON RE.SK1_JOB_ID       = flr.SK1_JOB_ID
        AND RE.SK1_EMPLOYEE_ID = flr.SK1_EMPLOYEE_ID
        AND RE.FIN_YEAR_NO     = dc.FIN_YEAR_NO
        AND RE.FIN_WEEK_NO     = dc.FIN_WEEK_NO
       --    WHERE  flr.last_updated_date = g_date
  GROUP BY FLR.SK1_JOB_ID ,
            FLR.SK1_LOCATION_NO ,
            FLR.sk1_employee_ID ,
            DC.fin_year_no ,
            DC.fin_week_no,
            EMPLOYEE_RATE
  ) ,
  selext1b AS
          (SELECT DISTINCT ae.SK1_EMPLOYEE_ID ,
                        ae.FIN_YEAR_NO ,
                        ae.FIN_WEEK_NO ,
                        re.sk1_job_id ,
                        el.sk1_location_no
                        --   ,dai.ABSENCE_TYPE_ID
                        --      ,dli.LEAVE_TYPE_ID
                        --      ,Ae.aBSENCE_HOURS
                        ,
                        CASE
                          WHEN dli.LEAVE_TYPE_ID  = 1000185   AND DAI.ABSENCE_TYPE_ID = 1 THEN ABSENCE_HOURS
                          ELSE 0
                        END TRAINING_HOURS ,
                        employee_rate,
                        CASE
                          WHEN DAI.ABSENCE_TYPE_ID = 1 AND dli.LEAVE_TYPE_ID   IN( 1000228,1000173,1000176,1000177 ,1000178,1000229,1000230,1000175 ,1000183,1000194,1000184)
                                                              THEN absence_hours       * LEAVE_TYPE_PERCENT_OF_HOURS / 100
                          ELSE 0
                        END absence_hours_value
          FROM DWH_PERFORMANCE.RTL_ABSENCE_EMP_WK AE
          JOIN DIM_LEAVE_TYPE DLI
          ON AE.SK1_LEAVE_TYPE_ID = DLI.SK1_LEAVE_TYPE_ID
          JOIN DIM_ABSENCE_TYPE DAI
          ON AE.SK1_ABSENCE_TYPE_ID = DAI.SK1_ABSENCE_TYPE_ID
          JOIN DWH_PERFORMANCE.RTL_EMP_JOB_WK RE
          ON RE.SK1_EMPLOYEE_ID = ae.SK1_EMPLOYEE_ID
          AND RE.FIN_YEAR_NO    = ae.FIN_YEAR_NO
          AND RE.FIN_WEEK_NO    = ae.FIN_WEEK_NO
          JOIN DWH_PERFORMANCE.RTL_EMP_LOC_STATUS_WK el
          ON el.SK1_EMPLOYEE_ID      = ae.SK1_EMPLOYEE_ID
          AND el.FIN_YEAR_NO         = ae.FIN_YEAR_NO
          AND el.FIN_WEEK_NO         = ae.FIN_WEEK_NO
    --      WHERE AE.last_updated_date = g_date
          ),
  selext1 AS
          (SELECT NVL(se1a.SK1_JOB_ID, se1b.SK1_JOB_ID) SK1_JOB_ID,
                    NVL(se1a.SK1_LOCATION_NO, se1b.SK1_LOCATION_NO) SK1_LOCATION_NO,
                    NVL(se1a.sk1_employee_ID , se1b.sk1_employee_ID) sk1_employee_ID,
                    NVL(se1a.fin_year_no , se1b.fin_year_no) fin_year_no ,
                    NVL(se1a.fin_week_no , se1b.fin_week_no) fin_week_no ,
                    NVL(se1a.total_NETT_SCHEDULED_HOURS, NULL) total_NETT_SCHEDULED_HOURS,
                    --      nvl(se1b.ABSENCE_TYPE_ID, null) ABSENCE_TYPE_ID,
                    --       nvl(se1b.LEAVE_TYPE_ID, null) LEAVE_TYPE_ID,
                    --         nvl(se1b.ABSENCE_HOURS, null) ABSENCE_HOURS,
                    NVL(se1b.absence_hours_value, NULL) absence_hours_value,
                    NVL(se1b.TRAINING_HOURS, NULL) TRAINING_HOURS,
                    --        nvl(se1b.LEAVE_TYPE_PERCENT_OF_HOURS, null)  LEAVE_TYPE_PERCENT_OF_HOURS,
                    NVL(se1a.employee_rate, se1b.employee_rate) employee_rate
          FROM selext1a se1a
          FULL OUTER JOIN selext1b se1b
                ON se1b.SK1_EMPLOYEE_ID = SE1a.SK1_EMPLOYEE_ID
                AND se1b.FIN_YEAR_NO    = SE1a.FIN_YEAR_NO
                AND se1b.FIN_WEEK_NO    = SE1a.FIN_WEEK_NO
          ),
  selext3 AS
          (SELECT SE1.SK1_JOB_ID ,
                  SE1.SK1_LOCATION_NO ,
                  SE1.SK1_EMPLOYEE_ID ,
                  SE1.FIN_YEAR_NO ,
                  SE1.FIN_WEEK_NO ,
                  NVL(SE1.TOTAL_NETT_SCHEDULED_HOURS,0) TOTAL_NETT_SCHEDULED_HOURS,
                  SUM(NVL(TRAINING_HOURS,0)) TRAINING_HOURS ,
                  SUM(NVL(absence_hours_value,0)) absence_hours_value,
                  --      SUM(NVL(LEAVE_TYPE_PERCENT_OF_HOURS,0)) LEAVE_TYPE_PERCENT_OF_HOURS ,
                  MAX_HRS_PER_WK ,
                  EMPLOYEE_RATE
          FROM SELEXT1 SE1
          JOIN DWH_PERFORMANCE.RTL_EMP_CONSTR_LOC_JOB_WK EC
            -- must be changed back to straight join once data in --JOIN DWH_PERFORMANCE.RTL_EMP_CONSTR_LOC_JOB_WK EC
                ON EC.SK1_JOB_ID       = se1.SK1_JOB_ID
               AND EC.SK1_EMPLOYEE_ID = se1.SK1_EMPLOYEE_ID
               AND EC.FIN_YEAR_NO     = se1.FIN_YEAR_NO
               AND EC.FIN_WEEK_NO     = se1.FIN_WEEK_NO
          GROUP BY sE1.SK1_JOB_ID ,
                    SE1.SK1_LOCATION_NO ,
                    SE1.SK1_EMPLOYEE_ID ,
                    SE1.FIN_YEAR_NO ,
                    SE1.FIN_WEEK_NO ,
                    NVL(SE1.TOTAL_NETT_SCHEDULED_HOURS,0),
                    MAX_HRS_PER_WK ,
                    EMPLOYEE_RATE
          ORDER BY SE1.SK1_EMPLOYEE_ID
          ),
  selext4 AS
                ( SELECT DISTINCT SK1_JOB_ID ,
                          SK1_LOCATION_NO ,
                          SK1_EMPLOYEE_ID ,
                          FIN_YEAR_NO ,
                          FIN_WEEK_NO ,
                          employee_rate ,
                          training_hours,
                          CASE
                            WHEN TOTAL_NETT_SCHEDULED_HOURS+TRAINING_HOURS >= MAX_HRS_PER_WK
                            THEN TOTAL_NETT_SCHEDULED_HOURS+ TRAINING_HOURS
                            WHEN TOTAL_NETT_SCHEDULED_HOURS+TRAINING_HOURS+absence_hours_value < MAX_HRS_PER_WK
                            THEN TOTAL_NETT_SCHEDULED_HOURS+ TRAINING_HOURS+absence_hours_value
                            WHEN TOTAL_NETT_SCHEDULED_HOURS+TRAINING_HOURS+absence_hours_value >= MAX_HRS_PER_WK
                            THEN MAX_HRS_PER_WK
                            ELSE TOTAL_NETT_SCHEDULED_HOURS
                          END NETT_SCHEDULED_HOURS_WK,
                          TOTAL_NETT_SCHEDULED_HOURS
                FROM SELEXT3
                )
SELECT
          /*+ full(rtl) parallel(rtl,6) */
          DISTINCT se4.SK1_LOCATION_NO ,
          se4.SK1_JOB_ID ,
          se4.SK1_employee_ID ,
          se4.FIN_YEAR_NO ,
          se4.FIN_WEEK_NO ,
          se4.TOTAL_NETT_SCHEDULED_HOURS,
          se4.nett_scheduled_hours_WK ,
          se4.nett_scheduled_hours_WK / 40 SCHEDULED_FTE_WK ,
          se4.nett_scheduled_hours_WK * se4.EMPLOYEE_RATE SCHEDULED_COST_WK ,
          NULL RTL_EXISTS
FROM selext4 se4
ORDER BY se4.sk1_employee_id ,
        se4.SK1_LOCATION_NO ,
        se4.SK1_JOB_ID ,
        se4.FIN_YEAR_NO ,
        se4.FIN_WEEK_NO ;
type stg_array is table of c_fnd_LOCATION%rowtype;
a_stg_input      stg_array;

g_rec_in             c_fnd_LOCATION%rowtype;


--**************************************************************************************************
-- Delete records from Performance
-- based on employee_id and availability_start_date
-- before loading from staging
--**************************************************************************************************
procedure delete_prf as
begin
        g_recs_inserted := 0;
        
        SELECT MAX(run_seq_no)
        INTO g_run_seq_no
        FROM dwh_foundation.FND_S4S_SCHLOCEMPJBDY_DEL_LIST
      where batch_date = g_date;
      
      If g_run_seq_no is null
      then select max(run_seq_no) into g_run_seq_no
      from dwh_foundation.FND_S4S_SCHLOCEMPJBDY_DEL_LIST;
      If g_run_seq_no is null
      then
      g_run_seq_no := 1;
      end if;
      end if;
        g_run_date := TRUNC(sysdate);
        
        BEGIN
                     delete from DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK B
           where EXISTS  (select distinct SK1_employee_id,fin_year_no, fin_week_no from DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_DY RTL,
           dim_calendar dc
           WHERE  rtl.last_updated_date = g_date
           and dc.calendar_date        = TRUNC(RTL.SHIFT_CLOCK_IN)
           AND RTL.SK1_employee_id = B.SK1_EMPLOYEE_ID
           AND DC.fin_year_no = B.FIN_YEAR_NO
           AND DC.fin_week_no = B.FIN_WEEK_NO); 
           
           
                                                                   
          g_recs :=SQL%ROWCOUNT ;
          
          COMMIT;
          
          g_recs_deleted := g_recs;
          
          l_text         := 'Deleted from DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
          dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
          
        EXCEPTION
        WHEN no_data_found THEN
                    l_text := 'No deletions done for DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK ';
                    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        END;
        
        g_recs_inserted :=0;
        g_recs_deleted  := 0;
        
        BEGIN
          l_text := '***** SECOND DELETION ***** ';
          dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
          
          DELETE
                  FROM DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK b
    WHERE EXISTS (SELECT DISTINCT SK1_EMPLOYEE_ID, fin_year_no, fin_week_no FROM dwh_performance.rtl_ABSENCE_EMP_wk a
          WHERE a.SK1_EMPLOYEE_ID = b.SK1_EMPLOYEE_ID
          AND a.fin_year_no = B.fin_year_no
           AND a.fin_week_no = B.fin_week_no
          AND a.LAST_UPDATED_DATE = G_DATE)
          ;
          
              
                                                                        
          g_recs :=SQL%ROWCOUNT ;
          
          COMMIT;
          
          g_recs_deleted := g_recs;
          
          l_text         := 'abscenec data changes for Schedule - Deleted from DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK recs='||g_recs_deleted||' - run_date='||g_run_date||' - batch_date='||g_date||' - run_seq_no='||g_run_seq_no;
          dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
          
        EXCEPTION
                WHEN no_data_found THEN
                  l_text := 'No Schedule deletions done for DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK ';
                  dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        END;
        
        g_recs_inserted :=0;
        g_recs_deleted  := 0;
        
EXCEPTION
        WHEN dwh_errors.e_insert_error THEN
                    l_message := dwh_constants.vc_err_lw_insert||SQLCODE||' '||sqlerrm;
                    dwh_log.record_error(l_module_name,SQLCODE,l_message);
                    raise;
        WHEN OTHERS THEN
                    l_message := dwh_constants.vc_err_lw_other||SQLCODE||' '||sqlerrm;
                    dwh_log.record_error(l_module_name,SQLCODE,l_message);
                    raise;

end delete_prf;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

      g_rec_out.sk1_LOCATION_NO      := g_rec_in.sk1_LOCATION_NO;
      g_rec_out.sk1_JOB_ID           := g_rec_in.sk1_JOB_ID;
      g_rec_out.sk1_employee_id      := g_rec_in.sk1_employee_id;
      g_rec_out.FIN_YEAR_NO       := g_rec_in.FIN_YEAR_NO ;
      g_rec_out.FIN_week_NO       := g_rec_in.FIN_week_NO ;
      g_rec_out.TOTAL_NETT_SCHEDULED_HOURS := g_rec_in.TOTAL_NETT_SCHEDULED_HOURS;
      g_rec_out.NETT_scheduled_HOURS_WK := g_rec_in.NETT_scheduled_HOURS_WK;
   --   g_rec_out.NETT_fixed_HOURS    := g_rec_in.NETT_fixed_HOURS;
      g_rec_out.SCHEDULED_FTE_WK      := g_rec_in.SCHEDULED_FTE_WK;
      g_rec_out.SCHEDULED_COST_WK      := g_rec_in.SCHEDULED_COST_WK;
      g_rec_out.last_updated_date    := g_date;


   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variable;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into dwh_performance.RTL_SCH_LOC_EMP_JB_WK  values a_tbl_insert(i);

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
                       ' '||a_tbl_INSERT(g_error_index).sk1_employee_id||
                       ' '||a_tbl_INSERT(g_error_index).fin_year_no ||
                       ' '||a_tbl_INSERT(g_error_index).fin_week_no ||
                       ' '||a_tbl_INSERT(g_error_index).SK1_LOCATION_NO||
                       ' '||a_tbl_INSERT(g_error_index).SK1_JOB_ID;
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
     UPDATE dwh_performance.RTL_SCH_LOC_EMP_JB_WK
        SET 
        --NETT_FIXED_HOURS     = a_tbl_update(i).NETT_FIXED_HOURS ,
             TOTAL_NETT_SCHEDULED_HOURS = a_tbl_update(i).TOTAL_NETT_SCHEDULED_HOURS,
             NETT_scheduled_HOURS_WK = a_tbl_update(i).NETT_scheduled_HOURS_WK ,
              SCHEDULED_COST_WK       = a_tbl_update(i).SCHEDULED_COST_WK ,
              SCHEDULED_FTE_WK        = a_tbl_update(i).SCHEDULED_FTE_WK ,
              LAST_UPDATED_DATE      = a_tbl_update(i).LAST_UPDATED_DATE
        WHERE SK1_JOB_ID         = a_tbl_update(i).SK1_JOB_ID
        AND SK1_location_NO      = a_tbl_update(i).SK1_location_NO
        AND sk1_employee_id      = a_tbl_update(i).sk1_employee_id
        AND fin_year_no       = a_tbl_update(i).fin_year_no
         AND fin_week_no       = a_tbl_update(i).fin_week_no
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
                       ' '||a_tbl_update(g_error_index).sk1_employee_id||
                       ' '||a_tbl_update(g_error_index).fin_year_no ||
                       ' '||a_tbl_update(g_error_index).fin_week_no ||
                       ' '||a_tbl_update(g_error_index).SK1_LOCATION_NO||
                       ' '||a_tbl_update(g_error_index).SK1_JOB_ID;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
 procedure local_write_output as
begin
/*   g_found := FALSE;
   -- Check to see if Business Unit is present on table and update/insert accordingly
   select count(1)
     into g_count
     from DIM_JOB
    where
    JOB_ID = g_rec_out.JOB_ID
	   and JOBGROUP_ID = g_rec_out.JOBGROUP_ID
	   and WORKGROUP_ID = g_rec_out.WORKGROUP_ID
	   and JOB_ID = g_rec_out.JOB_ID;
*/
--   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
--     G_COUNT := 0;
 --  IF G_REC_IN.RTL_EXISTS IS NOT NULL
 --  THEN G_COUNT := 1;
--   g_found := TRUE;
--   END IF;

/*   l_text := 'G_COUNT='||g_COUNT||' -G_REC_IN.RTL_EXISTS='||G_REC_IN.RTL_EXISTS
   ||'-'||g_rec_out.SK1_JOB_ID
   ||'-'||g_rec_out.SK1_location_NO
   ||'-'||g_rec_out.sk1_employee_id
   ||'-'||g_rec_out.SHIFT_CLOCK_IN 
                    ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/
  
G_COUNT := 1;
g_found := FALSE;
-- Place data into and array for later writing to table in bulk
 --  if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
--   else
--      a_count_u               := a_count_u + 1;
--      a_tbl_update(a_count_u) := g_rec_out;
--   end if;

   a_count := a_count + 1;
--~~~~~~~~~~`````````````````````****************************````````````````````````~~~~~~~~~~~~~~~~~~~
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
   --   local_bulk_update;

      a_tbl_insert  := a_empty_set_i;
--     a_tbl_update  := a_empty_set_u;

      a_count_i     := 0;
  --    a_count_u     := 0;
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
    l_text := 'LOAD OF RTL_SCH_LOC_EMP_JB_WK  EX FOUNDATION STARTED '||
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

    l_text := 'Running GATHER_TABLE_STATS ON RTL_SCH_LOC_EMP_JB_DY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_SCH_LOC_EMP_JB_DY', DEGREE => 8);

        l_text := 'TRUNCATE TABLE DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_PERFORMANCE.RTL_SCH_LOC_EMP_JB_WK');

--**************************************************************************************************
-- delete process
--**************************************************************************************************

 --delete_prf;
 
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
   open c_FND_LOCATION;
    fetch c_FND_LOCATION bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 5000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_FND_LOCATION bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_FND_LOCATION;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
       l_text := 'AFTER CURSOR';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   local_bulk_insert;
       l_text := 'AFTER INS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--   local_bulk_update;
   l_text := 'Running GATHER_TABLE_STATS ON RTL_SCH_LOC_EMP_JB_WK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_SCH_LOC_EMP_JB_WK', DEGREE => 8);
/* pk
SK1_LOCATION_NO
SK1_EMPLOYEE_ID
SK1_JOB_ID
FIN_YEAR_NO
FIN_WEEK_NO*/
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



END WH_PRF_S4S_051U_takeon;
