--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_182U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_182U" (p_forall_limit IN INTEGER,p_success OUT BOOLEAN) AS

--**************************************************************************************************
-- Description: OneApp usage and navigations - Load event requests
--
--
-- Date:        2018-01-02
-- Author:      Naresh Chauhan
-- Purpose:     update table DIM_ONEAPP_EVENT_REQUEST in the Performance layer
--      
-- Tables: 
--              Input  - 
--                       FND_WFS_ONEAPP_API
--                       fnd_wfs_oneapp_gateway
-- 
--              Output - DIM_ONEAPP_EVENT_REQUEST
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2018-01-02 N Chauhan - created.
--  2018-01-24 N Chauhan - processing of past data done in daily batches.
--  2018-01-25 N Chauhan - strip out cmpNNN.. and cmpNN...  from event requests.
--  2018-01-25 N Chauhan - end processing when end of source data is reached.
--  2018-01-25 N Chauhan - strip out /N-...  from event requests.
--  2018-02-28 N Chauhan - retrieve event_request_category from apex_app_wfs_01.apex_oneapp_request_category.

--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster than on the original template.
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
l_module_name        sys_dwh_errlog.log_procedure_name%TYPE    := 'WH_PRF_WFS_182U';
l_name               sys_dwh_log.log_name%TYPE                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%TYPE          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%TYPE          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%TYPE       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%TYPE  := 'ONEAPP USAGE AND NAVIGATIONS - LOAD EVENT REQUESTS';
l_process_type       sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;


g_job_desc VARCHAR2(200):= 'Load event requests';
g_success BOOLEAN:= TRUE;
g_source_rows_chk INTEGER :=0;

g_analysed_count INTEGER:=0;
g_analysed_success BOOLEAN:= FALSE;

g_dormant_limit NUMBER(11)  := 30;
g_check_date DATE := TRUNC(sysdate-1);
g_date_to_do DATE := TRUNC(sysdate-1);
g_date_done DATE := TRUNC(sysdate-7);
g_date_end  DATE := TRUNC(sysdate-1);
g_recs_cnt_day NUMBER := 0;




/* load new event_requests & update existing event_requests */

PROCEDURE event_req_load_from_hist_day(p_day_to_do DATE) AS


BEGIN


   MERGE /*+ append parallel(tgt,4) */
   INTO dwh_wfs_performance.dim_oneapp_event_request tgt USING 
     (
      WITH 
      all_event_requests AS (
         SELECT  /*+ parallel(fnd,4) full(fnd) */ 
            'API' AS event_request_origin,
             regexp_replace(
                          regexp_replace( api_event_request, 
                            '/[[:digit:]+]+/'       -- replace numbers between slashes
                            ||'|/cat[[:digit:]]+/'  -- replace "/cat" and accompanying number
                            ||'|/cmp[[:digit:]]+/'  -- replace "/cmp" and accompanying number
                            ||'|/N-.+/'             -- replace sub field starting with "/N-
                            ,'/'),                  -- if not at end, replace with slash

                          '/[[:digit:]+]+$'         -- remove same if at end of text
                            ||'|/cat[[:digit:]]+$'
                            ||'|/cmp[[:digit:]]+$'
                            ||'|/N-.+$'
                          )
                        --inner regexp to replace numbers between delimiters
                        --outer regexp to replace numbers at end of string.

              AS event_request,
             api_event_create_date AS event_create_date
         FROM
             dwh_wfs_foundation.fnd_wfs_oneapp_api fnd
         WHERE 
          fnd.api_event_create_date >= p_day_to_do AND 
          fnd.api_event_create_date < p_day_to_do + 1 
         UNION
         SELECT  /*+ parallel(g,4) full(g) */ 
            'GATEWAY' AS event_request_origin,
             regexp_replace(
                          regexp_replace( gateway_event_request, 
                            '/[[:digit:]+]+/'       -- replace numbers between slashes
                            ||'|/cat[[:digit:]]+/'  -- replace "/cat" and accompanying number
                            ||'|/cmp[[:digit:]]+/'  -- replace "/cmp" and accompanying number
                            ||'|/N-.+/'             -- replace sub field starting with "/N-
                            ,'/'),                  -- if not at end, replace with slash

                          '/[[:digit:]+]+$'         -- remove same if at end of text
                            ||'|/cat[[:digit:]]+$'
                            ||'|/cmp[[:digit:]]+$'
                            ||'|/N-.+$'
                          )
             AS event_request,
                        --inner regexp to replace numbers between delimiters
                        --outer regexp to replace numbers at end of string.

             gateway_event_create_date AS event_create_date
         FROM
             dwh_wfs_foundation.fnd_wfs_oneapp_gateway G
         WHERE 
             G.gateway_event_create_date >= p_day_to_do AND 
             G.gateway_event_create_date < p_day_to_do + 1
      ),
      distinct_event_requests AS (
         SELECT  /*+ parallel(e,4) full(e) */ 
            event_request_origin,
            event_request,
            MIN(event_create_date) event_create_date,
            MAX(event_create_date) last_active_date
         FROM
            all_event_requests E
         GROUP BY event_request_origin, event_request
      )
      SELECT  /*+ parallel(e,4) full(e) */ 
         E.event_request_origin,
         E.event_request,
         c.event_request_category,
         CASE 
           WHEN g_check_date-last_active_date <= g_dormant_limit THEN
             1
           ELSE
             0
         END 
         AS active_ind,
         event_create_date ,
         last_active_date
       FROM distinct_event_requests E
       left outer join apex_app_wfs_01.apex_oneapp_request_category c
        on (c.EVENT_REQUEST = E.Event_Request)

   ) new_rec 

   ON (
        new_rec.event_request_origin = tgt.event_request_origin AND
        new_rec.event_request = tgt.event_request
 	    )
   WHEN MATCHED THEN UPDATE 
    SET
        tgt.event_request_category   = new_rec.event_request_category
       ,tgt.active_ind               = new_rec.active_ind
--       ,tgt.event_create_date        = new_rec.event_create_date
       ,tgt.last_active_date         =
           CASE 
           WHEN new_rec.last_active_date > tgt.last_active_date THEN
              new_rec.last_active_date
           ELSE
              tgt.last_active_date
           END             
       ,tgt.last_updated_date        = TRUNC(g_date)

    WHEN NOT MATCHED THEN INSERT (

             tgt.event_request_origin
            ,tgt.event_request
            ,tgt.event_request_category
            ,tgt.active_ind
            ,tgt.event_create_date
            ,tgt.last_active_date
            ,tgt.last_updated_date

           ) 

    VALUES (

             new_rec.event_request_origin
            ,new_rec.event_request
            ,new_rec.event_request_category
            ,new_rec.active_ind
            ,new_rec.event_create_date
            ,new_rec.last_active_date
            , TRUNC(g_date)
           ) 

   ;

   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text := 'ONEAPP - EVENT_REQ_LOAD_FROM_HIST_DAY sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
--      raise;

END event_req_load_from_hist_day;




PROCEDURE event_requests_load AS

BEGIN

   g_source_rows_chk := NULL;

   SELECT /*+ parallel(s,4) */  
      TRUNC(MAX(last_active_date))                -- most recent login
        INTO g_date_done
   FROM dwh_wfs_performance.dim_oneapp_event_request S;
   IF g_date_done IS NOT NULL THEN                -- most recent login retrieved
     SELECT /*+ parallel(t,4) full(t) */
      COUNT(*) cnt                                -- check if there is source data since most recent login
      INTO g_source_rows_chk
     FROM  dwh_wfs_foundation.fnd_wfs_oneapp_api   T
     WHERE T.api_event_create_date >  g_date_done + 1  -- plus 1 'cos g_date_done is trunc'd 
       AND ROWNUM< 100;                            -- for performance  - no need to count all
   ELSE                                            -- target table was empty
     SELECT  /*+ parallel(a,4)  */ 
      TRUNC(MIN(A.api_event_create_date))          -- to start with earliest source date
        INTO g_date_done
     FROM dwh_wfs_foundation.fnd_wfs_oneapp_api	A;
   END IF;
   IF g_date_done IS NULL                          -- source table empty
    OR g_source_rows_chk = 0 THEN                  -- no new source data

       g_date_done := TRUNC(g_date);

       l_text      := 'Latest data not available in FND_WFS_ONEAPP_API. Load abandoned.'; 
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');

   END IF;

   SELECT  /*+ parallel(a,4)  */ 
    TRUNC(Max(A.api_event_create_date))          -- to end with latest source date
      INTO g_date_end
   FROM dwh_wfs_foundation.fnd_wfs_oneapp_api	A;

   g_date_to_do := g_date_done;    -- reprocess last day in case more added
   g_date_end := nvl(g_date_end, g_date);

   l_text :=  'Processing from day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  to '||to_char(g_date_end, 'YYYY-MM-DD');
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   WHILE g_date_to_do <= g_date_end AND g_success 
   LOOP


      -- ****** daily load *****************************
      event_req_load_from_hist_day(g_date_to_do);
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
      l_text :=  'ONEAPP - EVENT_REQUESTS_LOAD sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
--      raise;


END event_requests_load;



/* update active_ind status  - mainly for existing unchanged records where dormancy has changed */ 
/* NB. This flag gets updated automtically when records are inserted or updated */

PROCEDURE active_ind_update AS

BEGIN

   MERGE /*+ append parallel(tgt,4) */
   INTO dwh_wfs_performance.dim_oneapp_event_request tgt USING (

      SELECT 
         S.event_request_origin ,
         S.event_request ,
         S.event_request_category ,
         0 AS active_ind        -- because of non-active selection criterion

      FROM dwh_wfs_performance.dim_oneapp_event_request S
      WHERE 
         g_check_date- S.last_active_date > g_dormant_limit --beyond active window
         AND active_ind <> 0
   ) upd_rec
   ON (
        upd_rec.event_request_origin = tgt.event_request_origin AND
        upd_rec.event_request = tgt.event_request
       )
   WHEN MATCHED THEN UPDATE 
      SET
         tgt.active_ind               = upd_rec.active_ind
        ,tgt.last_updated_date         = TRUNC(g_date)
   ;
   --commit;    -- don't commit as count is needed outside this proc
   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  'ONEAPP - ACTIVE_IND_UPDATE sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
--      raise;

END active_ind_update;




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

    l_text := 'WFS OneApp DIM_ONEAPP_EVENT_REQUEST load STARTED AT '||
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

    g_check_date := g_date;

    l_text := 'LOAD TABLE: '||'DIM_ONEAPP_EVENT_REQUEST' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



    -- ****** main load *************
    event_requests_load;
    -- ******************************

    IF g_success THEN

       l_text :=  'Event_requests load complete';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       active_ind_update;

    END IF;

    IF g_success THEN

       g_recs_updated   :=  SQL%rowcount;

       COMMIT;  

       l_text :=  'Active_ind update complete';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


       -- NB write_log does a commit !




   --**************************************************************************************************
   -- Write final log data
   --**************************************************************************************************

       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
       l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
       l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated||'   (Active_ind)';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   --    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
       l_text :=  'RECORDS MERGED   '||g_recs_inserted;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


        p_success := TRUE;
        COMMIT;

--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - ends');
    ELSE
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
--      || '   - load for day '||to_char(g_date_to_do,'yyyy-mm-dd') ||' fails');

        ROLLBACK;
        l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc||'  fails';
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




END wh_prf_wfs_182u;
