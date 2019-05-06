--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_474U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_474U" (p_forall_limit IN INTEGER,p_success OUT BOOLEAN) AS

--**************************************************************************************************
-- Description: Call Data - Load WFS Call Queue Performance data
--
--
-- Date:        2018-10-05
-- Author:      Nhlaka Dlamini
-- Purpose:     update table wfs_call_q_performance in the Performance layer
--      
-- Tables: 
--              Input  - 
--                       FND_WFS_CALL_PLAN 
--                       
-- 
--              Output - wfs_call_q_performance
--                       dim_wfs_call_queue

--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2018-10-05 N Dlamini - created.
--  2018-11-08 N Dlamini - Loggging tidied up.
--  2018-11-08 N Dlamini - Table checks for start date fixed.
--
--
--  Naming conventions
--  g_ -  Global variable
--  l_ -  Log table variable
--  a_ -  Array variable
--  v_ -  Local variable as found in packages
--  p_ -  Parameter
--  c_ -  Prefix to cursor
--**************************************************************************************************




g_forall_limit       INTEGER       :=  dwh_constants.vc_forall_limit;
g_recs_read          INTEGER       :=  0;
g_recs_updated       INTEGER       :=  0;
g_recs_inserted      INTEGER       :=  0;
g_recs_hospital      INTEGER       :=  0;
g_recs_deleted       INTEGER       :=  0;
g_error_count        NUMBER        :=  0;
g_error_index        NUMBER        :=  0;
g_count              NUMBER        :=  0;
g_date               DATE          :=  TRUNC(sysdate);

g_year_no            INTEGER       :=  0;
g_month_no           INTEGER       :=  0;


l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%TYPE    := 'WH_PRF_WFS_474U';
l_name               sys_dwh_log.log_name%TYPE                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%TYPE          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%TYPE          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%TYPE       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%TYPE  := 'CALL QUEUE DATA - LOAD WFS CALL QUEUE PERFORMANCE';
l_process_type       sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;


g_job_desc VARCHAR2(200):= 'Load WFS Call Queue Performance Data';
g_success BOOLEAN:= TRUE;
g_source_rows_chk INTEGER :=0;

g_analysed_count INTEGER:=0;
g_analysed_success BOOLEAN:= FALSE;

g_check_date DATE := TRUNC(sysdate-1);
g_date_to_do DATE := TRUNC(sysdate-1);
g_date_done DATE := TRUNC(sysdate-7);
g_date_end  DATE := TRUNC(sysdate-1);
g_recs_cnt_day NUMBER := 0;

g_recs_call_q_performance NUMBER := 0;
g_recs_call_queue         NUMBER := 0;



/* load new data 16/08/2018 existing  */

PROCEDURE load_from_hist_day(p_day_to_do DATE) AS


BEGIN

   MERGE /*+ append  */
   INTO dwh_wfs_performance.wfs_call_q_performance tgt USING
      (
      WITH 
      dataset_1 AS (
         SELECT  /*+ parallel(act,4) full(act) */ 
            distinct
            start_time,
            queue_id,
            interval,
            actual_call_volume,
            forecasted_call_volume,
            service_level_achieved_perc,
            service_level_forecasted_perc,
            actual_average_handling_times,
            forecasted_avg_handling_times,
            actual_average_speed_of_answer,
            forecasted_avg_speed_of_answer,
            actual_staffing

         FROM
            dwh_wfs_foundation.fnd_wfs_call_plan call_plan

         WHERE 
            call_plan.start_time >= p_day_to_do AND 
            call_plan.start_time < p_day_to_do + 1 
           AND NOT EXISTS (SELECT START_TIME,
                                    ACTUAL_CALL_VOLUME
                            FROM dwh_wfs_performance.wfs_call_q_performance b
                            WHERE call_plan.START_TIME = b.START_TIME
                            AND call_plan.ACTUAL_CALL_VOLUME = b.ACTUAL_CALL_VOLUME
                        )            

      )

      SELECT  /*+ parallel(e,4) full(e) */ 
         *
      FROM
         dataset_1 E

   ) new_rec 

   ON (

                 new_rec.queue_id  = tgt.queue_id
             AND new_rec.start_time  = tgt.start_time

       )
   WHEN MATCHED THEN UPDATE 
    SET
            tgt.interval                         = new_rec.interval,
            tgt.actual_call_volume               = new_rec.actual_call_volume,
            tgt.forecasted_call_volume           = new_rec.forecasted_call_volume,
            tgt.service_level_achieved_perc      = new_rec.service_level_achieved_perc,
            tgt.service_level_forecasted_perc   = new_rec.service_level_forecasted_perc,
            tgt.actual_average_handling_times    = new_rec.actual_average_handling_times,
            tgt.forecasted_avg_handling_times    = new_rec.forecasted_avg_handling_times,
            tgt.actual_average_speed_of_answer   = new_rec.actual_average_speed_of_answer,
            tgt.forecasted_avg_speed_of_answer   = new_rec.forecasted_avg_speed_of_answer,
            tgt.actual_staffing                  = new_rec.actual_staffing,          
            tgt.last_updated_date                = trunc(g_date)

    WHEN NOT MATCHED THEN INSERT (

            tgt.start_time,
            tgt.queue_id,
            tgt.interval,
            tgt.actual_call_volume,
            tgt.forecasted_call_volume,
            tgt.service_level_achieved_perc,
            tgt.service_level_forecasted_perc,
            tgt.actual_average_handling_times,
            tgt.forecasted_avg_handling_times,
            tgt.actual_average_speed_of_answer,
            tgt.forecasted_avg_speed_of_answer,
            tgt.actual_staffing,
            tgt.last_updated_date

           ) 

    VALUES (

            new_rec.start_time,
            new_rec.queue_id,
            new_rec.interval,
            new_rec.actual_call_volume,
            new_rec.forecasted_call_volume,
            new_rec.service_level_achieved_perc,
            new_rec.service_level_forecasted_perc,
            new_rec.actual_average_handling_times,
            new_rec.forecasted_avg_handling_times,
            new_rec.actual_average_speed_of_answer,
            new_rec.forecasted_avg_speed_of_answer,
            new_rec.actual_staffing,
            trunc(g_date)
           ) 

   ;
--   g_recs_call_q_performance := g_recs_call_q_performance + SQL%rowcount;
--   COMMIT;
   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  l_description||' - LOAD_FROM_HIST_DAY sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      RAISE;

END load_from_hist_day;




PROCEDURE upd_dim_wfs_call_queue(p_day_to_do IN DATE, g_success OUT BOOLEAN) AS

BEGIN   

   MERGE /*+ append */
      INTO dwh_wfs_performance.dim_wfs_call_queue tgt USING
      (   -- get all new and changes
          SELECT /*+ parallel (act,4)   full(act)  */
             DISTINCT 
             call_plan.queue_id
            ,call_plan.queue_name
            ,trunc(g_date) AS last_updated_date
          FROM
             dwh_wfs_foundation.fnd_wfs_call_plan call_plan
             LEFT OUTER JOIN dwh_wfs_performance.dim_wfs_call_queue exst  
                ON ( call_plan.queue_id = exst.queue_id )
          WHERE  
             call_plan.start_time >= p_day_to_do AND 
             call_plan.start_time < p_day_to_do + 1 
             AND (  -- only if a change in any value
                      nvl(call_plan.queue_name, 0) <> nvl(exst.queue_name, 0) 
                 )

   ) rec_to_ins_or_upd 

   ON ( rec_to_ins_or_upd.queue_id  = tgt.queue_id )

   WHEN MATCHED THEN UPDATE 
    SET
            tgt.queue_name = rec_to_ins_or_upd.queue_name
           ,tgt.last_updated_date = rec_to_ins_or_upd.last_updated_date

    WHEN NOT MATCHED THEN INSERT (

             tgt.queue_id
            ,tgt.queue_name
            ,tgt.last_updated_date

           ) 

    VALUES (

             rec_to_ins_or_upd.queue_id
            ,rec_to_ins_or_upd.queue_name
            ,rec_to_ins_or_upd.last_updated_date

           ) 

   ;
   g_recs_call_queue := g_recs_call_queue + SQL%rowcount;
   COMMIT;

EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  'UPD_DIM_WFS_CALL_QUEUE sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      RAISE;

END upd_dim_wfs_call_queue;


PROCEDURE LOAD AS

BEGIN

   g_source_rows_chk := NULL;

   SELECT /*+ parallel(s,4) */  
      TRUNC(MAX( START_TIME ))                -- most recent login
        INTO g_date_done
   FROM dwh_wfs_performance.wfs_call_q_performance  S;
   IF g_date_done IS NOT NULL THEN                -- most recent login retrieved
     SELECT /*+ parallel(t,4) full(t) */
      COUNT(*) cnt                                -- check if there is source data since most recent login
      INTO g_source_rows_chk
     FROM  dwh_wfs_foundation.fnd_wfs_call_plan    T
     WHERE T.START_TIME >  g_date_done + 1  -- plus 1 'cos g_date_done is trunc'd 
       AND ROWNUM< 100;                            -- for performance  - no need to count all
   ELSE                                            -- target table was empty
     SELECT  /*+ parallel(a,4)  */ 
      TRUNC(MIN(A.START_TIME))          -- to start with earliest source date
        INTO g_date_done
     FROM dwh_wfs_foundation.fnd_wfs_call_plan  A;
   END IF;
   IF g_date_done IS NULL                          -- source table empty
    OR g_source_rows_chk = 0 THEN                  -- no new source data

       g_date_done := TRUNC(g_date);

       l_text      := 'Latest data not available in FND_WFS_CALL_PLAN.'; 
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');

   END IF;

   SELECT  /*+ parallel(a,4)  */ 
    TRUNC(MAX(A.START_TIME))          -- to end with latest source date
      INTO g_date_end
   FROM  dwh_wfs_foundation.fnd_wfs_call_plan A;

   g_date_to_do := g_date_done;    -- reprocess last day in case more added
   g_date_end := nvl(g_date_end, g_date);

   l_text :=  'Processing from day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  to '||to_char(g_date_end, 'YYYY-MM-DD');
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   WHILE g_date_to_do <= g_date_end AND g_success 
   LOOP     
      --  update dims with any new values ****************
      upd_dim_wfs_call_queue(g_date_to_do, g_success);

      IF g_success = FALSE THEN
        RETURN;
      END IF;


      -- ****** daily load *****************************
      load_from_hist_day(g_date_to_do);
      -- **********************************************


      g_recs_cnt_day  :=SQL%rowcount;
      g_recs_read     :=  g_recs_read + g_recs_cnt_day;
      g_recs_inserted :=  g_recs_inserted + g_recs_cnt_day;

      COMMIT;  -- NB. write_log already does a commit !

      l_text :=  'For day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  Merged:  '||g_recs_cnt_day;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


      g_date_to_do := g_date_to_do +1;

   END LOOP;

   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  'LOAD sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      RAISE;


END LOAD;








--##############################################################################################
-- Main process
--**********************************************************************************************

BEGIN

    IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
       g_forall_limit := p_forall_limit;
    END IF;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := FALSE;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'WFS_CALL_Q_PERFORMANCE load STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    EXECUTE IMMEDIATE 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************


    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    l_text := 'LOAD TABLE: WFS_CALL_Q_PERFORMANCE' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    -- ****** main load *************
    LOAD;
    -- ******************************


    COMMIT;  


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text :=  'RECORDS MERGED  '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text :=  'DIM_WFS_CALL_QUEUE RECORDS MERGED  '||g_recs_call_queue;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



    IF g_success THEN

        p_success := TRUE;
        COMMIT;

--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - ends');
    ELSE
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
--      || '   - load for day '||to_char(g_date_to_do,'yyyy-mm-dd') ||' fails');

        ROLLBACK;
        l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
                  || '   - load for day '||to_char(g_date_to_do,'yyyy-mm-dd')||'  fails';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');

        p_success := FALSE;

    END IF;

    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


EXCEPTION

    WHEN dwh_errors.e_insert_error THEN
       ROLLBACK;
       l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
       dwh_log.record_error(l_module_name,SQLCODE,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := FALSE;
       RAISE;

    WHEN OTHERS THEN
       ROLLBACK;
       l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
       dwh_log.record_error(l_module_name,SQLCODE,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := FALSE;
       RAISE;



END wh_prf_wfs_474u;
