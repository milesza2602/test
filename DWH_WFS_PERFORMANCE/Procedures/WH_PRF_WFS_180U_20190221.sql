--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_180U_20190221
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_180U_20190221" (p_forall_limit IN INTEGER,p_success OUT BOOLEAN) AS

--**************************************************************************************************
-- Description: OneApp usage and navigations - Load/update subscriber's details
--
--
-- Date:        2017-12-21
-- Author:      Naresh Chauhan
-- Purpose:     load/update table DIM_ONEAPP_SUBSCRIBER in the Performance layer
--      
-- Tables: 
--              Input  - 
--                       FND_WFS_ONEAPP_API
-- 
--              Output - DIM_ONEAPP_SUBSCRIBER
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--              dwh_wfs_performance.dwh_stdlib_wfs
--
-- Maintenance:
--  2017-12-21 N Chauhan - created.
--  2018-01-16 N Chauhan - fix to reference username_extract function in dwh_stdlib_wfs.
--  2018-01-16 N Chauhan - fix to reference username_extract function in dwh_stdlib_wfs.
--  2018-01-22 N Chauhan - processing of past data done in daily batches.
--  2018-01-23 N Chauhan - force upper case for names, and trim spaces.
--  2018-01-25 N Chauhan - end processing when end of source data is reached.
--  2018-03-19 N Chauhan - add fields ATG_CUST_NO_FIRST_LINKED_DATE, CUSTOMER_NO_FIRST_LINKED_DATE
--  2018-04-10 N Chauhan - use api_event_create_date for linked_date fields, rather than g_date.
--  2018-04-10 N Chauhan - fixed first_..._linked_date check.
--  2018-04-10 N Chauhan - only update subscribers where dormancy changed.
--  2018-04-13 N Chauhan - fix first_..linked_date on initialisation.
--  2018-08-02 N Chauhan - add RAISE in sub procs exceptions to allow proc to fail, allowing alert to be generated.
--  2018-08-02 N Chauhan - Ignore source records where the subscriber_key is null.
--  2018-08-02 N Chauhan - Set g_check_date to p_date_to_do in loop.
--  2018-08-02 N Chauhan - Incorporate dormancy update in day loop.
--  2018-08-03 N Chauhan - Check that snapshot and summary of existing data has been saved before loading.
--  2018-12-06 N Chauhan - Limit g_date_end to g_date (for run-time change)
--
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
l_module_name        sys_dwh_errlog.log_procedure_name%TYPE    := 'WH_PRF_WFS_180U';
l_name               sys_dwh_log.log_name%TYPE                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%TYPE          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%TYPE          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%TYPE       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%TYPE  := 'ONEAPP USAGE AND NAVIGATIONS - LOAD/UPDATE SUBSCRIBER DETAILS';
l_process_type       sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;


g_job_desc VARCHAR2(200):= 'Load/update subscriber details';
g_success BOOLEAN:= TRUE;
g_source_rows_chk INTEGER := NULL;

g_analysed_count INTEGER:=0;
g_analysed_success BOOLEAN:= FALSE;

g_pre_dormant_limit NUMBER(11)  := 30;
g_dormant_limit NUMBER(11)      := 45;
g_check_date DATE := TRUNC(sysdate-1);
g_date_to_do DATE := TRUNC(sysdate-1);
g_date_done DATE := TRUNC(sysdate-7);
g_date_end  DATE := TRUNC(sysdate-1);
g_recs_cnt_day NUMBER := 0;
g_recs_dormcy_upd_day NUMBER := 0;
g_abandoned BOOLEAN:= FALSE;

g_last_snapshot_date DATE;
g_last_summary_date DATE;

/* load new subscriber & update existing subscribers */

PROCEDURE subscr_load_from_hist_day(p_day_to_do DATE) AS


BEGIN


   MERGE /*+ append parallel(tgt,4) */
   INTO dwh_wfs_performance.dim_oneapp_subscriber tgt USING 
     (
      WITH 
      decoded AS (
      SELECT  /*+ parallel(fnd,4) full(fnd) */ 
          oneapp_api_no,  
      --    tmp_base64_decode_to_char(tmp_substr_between(username,'.','.')) username_ch,
          substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'C2Id'),1,20) 
          AS customer_no,
          substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'sub') ,1,50)
          AS  oneapp_subscriber_key , 
          substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'AtgId'),1,40) 
          AS atg_customer_no,
          substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'sid'),1,100) 
          AS service_id,
          substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'preferred_username'),1,100) 
          AS preferred_username,
          substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'email'),1,100) 
          AS subscriber_email,
          substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'email_verified'),1,5) 
          AS email_address_verified,
          UPPER(TRIM(substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'name'),1,100))) 
          AS subscriber_first_name,
          UPPER(TRIM(substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'family_name'),1,100))) 
          AS subscriber_surname,
          api_event_create_date
      FROM
          dwh_wfs_foundation.fnd_wfs_oneapp_api fnd
      WHERE 
          fnd.api_event_create_date >= p_day_to_do AND 
          fnd.api_event_create_date < p_day_to_do + 1 AND 
          username_token IS NOT NULL

      ),
      latest AS (
      SELECT  /*+ parallel(t,4) full(t) */ 
         DISTINCT
         oneapp_subscriber_key,
         FIRST_VALUE(T.oneapp_api_no) 
           OVER (PARTITION BY T.oneapp_subscriber_key 
           ORDER BY T.api_event_create_date DESC) 
           AS oneapp_api_no,
         FIRST_VALUE(T.atg_customer_no) 
           OVER (PARTITION BY T.oneapp_subscriber_key 
           ORDER BY T.api_event_create_date DESC) 
           AS atg_customer_no,
         FIRST_VALUE(T.customer_no) 
           OVER (PARTITION BY T.oneapp_subscriber_key 
           ORDER BY T.api_event_create_date DESC) 
           AS customer_no,
         FIRST_VALUE(T.service_id) 
           OVER (PARTITION BY T.oneapp_subscriber_key 
           ORDER BY T.api_event_create_date DESC) 
           AS service_id,
         FIRST_VALUE(T.preferred_username) 
           OVER (PARTITION BY T.oneapp_subscriber_key 
           ORDER BY T.api_event_create_date DESC) 
           AS preferred_username,
         FIRST_VALUE(T.subscriber_email) 
           OVER (PARTITION BY T.oneapp_subscriber_key 
           ORDER BY T.api_event_create_date DESC) 
           AS subscriber_email,
         FIRST_VALUE(T.email_address_verified) 
           OVER (PARTITION BY T.oneapp_subscriber_key 
           ORDER BY T.api_event_create_date DESC) 
           AS email_address_verified,
         FIRST_VALUE(T.subscriber_first_name) 
           OVER (PARTITION BY T.oneapp_subscriber_key 
           ORDER BY T.api_event_create_date DESC) 
           AS subscriber_first_name,
         FIRST_VALUE(T.subscriber_surname) 
           OVER (PARTITION BY T.oneapp_subscriber_key 
           ORDER BY T.api_event_create_date DESC) 
           AS subscriber_surname,
         FIRST_VALUE(T.api_event_create_date)
           OVER (PARTITION BY T.oneapp_subscriber_key
           ORDER BY T.api_event_create_date ASC)
           AS first_subscr_login_date,    -- "first_"  for the day
         FIRST_VALUE(T.api_event_create_date)
           OVER (PARTITION BY T.oneapp_subscriber_key
           ORDER BY T.api_event_create_date DESC)
           AS last_subscr_login_date
      FROM
          decoded T
      where oneapp_subscriber_key is not null 
      )
      SELECT  /*+ parallel(t,4) full(t) */ 
         T.*,

         CASE      -- capture date when ATG_CUSTOMER_NO  is first encountered
           WHEN S.atg_cust_no_first_linked_date IS NULL 
            AND T.atg_customer_no IS NOT NULL THEN
             TRUNC(T.first_subscr_login_date)
           ELSE
             S.atg_cust_no_first_linked_date 
         END 
         AS atg_cust_no_first_linked_date, 
         CASE      -- capture date when CUSTOMER_NO  is first encountered
           WHEN S.customer_no_first_linked_date IS NULL
            AND T.customer_no IS NOT NULL THEN
             TRUNC(T.first_subscr_login_date)
           ELSE
             S.customer_no_first_linked_date
         END 
         AS customer_no_first_linked_date, 

         CASE 
           WHEN g_check_date-T.last_subscr_login_date <= g_pre_dormant_limit THEN
             1
           ELSE
             0
         END 
         AS active_ind,        -- because of non-active selection criterion
         CASE 
           WHEN g_check_date-T.last_subscr_login_date >= g_dormant_limit THEN
             1
           ELSE
             0
         END 
         AS dormant_ind,
         CASE 
           WHEN g_check_date-T.last_subscr_login_date > g_pre_dormant_limit
            AND g_check_date-T.last_subscr_login_date < g_dormant_limit THEN
             1
           ELSE 
             0
         END 
         AS pre_dormant_ind,
         CASE 
           WHEN g_check_date-T.last_subscr_login_date >= g_dormant_limit
           AND S.dormant_ind = 0 THEN
             1
           ELSE
             0
         END 
         AS inc_dormant_cnt,
         S.frequency_of_dormancy
      FROM latest T 
        LEFT OUTER JOIN dwh_wfs_performance.dim_oneapp_subscriber S
        ON (S.oneapp_subscriber_key = T.oneapp_subscriber_key)

 	  ) new_rec 

   ON (
        new_rec.oneapp_subscriber_key = tgt.oneapp_subscriber_key
 	    )
   WHEN MATCHED THEN UPDATE 
    SET
        tgt.oneapp_api_no             = new_rec.oneapp_api_no
       ,tgt.atg_customer_no           = new_rec.atg_customer_no
       ,tgt.atg_cust_no_first_linked_date  = new_rec.atg_cust_no_first_linked_date
       ,tgt.customer_no               = new_rec.customer_no
       ,tgt.customer_no_first_linked_date  = new_rec.customer_no_first_linked_date
       ,tgt.service_id                = new_rec.service_id
       ,tgt.preferred_username        = new_rec.preferred_username
       ,tgt.subscriber_email          = new_rec.subscriber_email
       ,tgt.email_address_verified    = new_rec.email_address_verified
       ,tgt.subscriber_first_name     = new_rec.subscriber_first_name
       ,tgt.subscriber_surname        = new_rec.subscriber_surname
--       ,tgt.FIRST_SUBSCR_LOGIN_DATE   = new_rec.FIRST_SUBSCR_LOGIN_DATE   -- only capure on first insert
       ,tgt.last_subscr_login_date    = new_rec.last_subscr_login_date
       ,tgt.active_ind                = new_rec.active_ind
       ,tgt.dormant_ind               = new_rec.dormant_ind
       ,tgt.pre_dormant_ind           = new_rec.pre_dormant_ind
       ,tgt.frequency_of_dormancy     = tgt.frequency_of_dormancy+new_rec.inc_dormant_cnt
       ,tgt.last_updated_date         = TRUNC(g_date)

    WHEN NOT MATCHED THEN INSERT (

             tgt.oneapp_subscriber_key
            ,tgt.oneapp_api_no
            ,tgt.atg_customer_no
            ,tgt.atg_cust_no_first_linked_date
            ,tgt.customer_no
            ,tgt.customer_no_first_linked_date
            ,tgt.service_id
            ,tgt.preferred_username
            ,tgt.subscriber_email
            ,tgt.email_address_verified
            ,tgt.subscriber_first_name
            ,tgt.subscriber_surname
            ,tgt.first_subscr_login_date
            ,tgt.last_subscr_login_date
            ,tgt.active_ind
            ,tgt.dormant_ind
            ,tgt.pre_dormant_ind
            ,tgt.frequency_of_dormancy
            ,tgt.last_updated_date
           ) 

    VALUES (

             new_rec.oneapp_subscriber_key
            ,new_rec.oneapp_api_no
            ,new_rec.atg_customer_no
            ,new_rec.atg_cust_no_first_linked_date
            ,new_rec.customer_no
            ,new_rec.customer_no_first_linked_date
            ,new_rec.service_id
            ,new_rec.preferred_username
            ,new_rec.subscriber_email
            ,new_rec.email_address_verified
            ,new_rec.subscriber_first_name
            ,new_rec.subscriber_surname
            ,new_rec.first_subscr_login_date
            ,new_rec.last_subscr_login_date
            , 1 -- ACTIVE_IND
            , 0 -- DORMANT_IND
            , 0 -- PRE_DORMANT_IND
            , 0 -- FREQUENCY_OF_DORMANCY
            , TRUNC(g_date)
           ) 

   ;

   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  'ONEAPP - SUBSCR_LOAD_FROM_HIST_DAY sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      raise;

END subscr_load_from_hist_day;


/* update dormancy statuses  */ 

PROCEDURE dormancy_update AS

BEGIN

   MERGE /*+ append parallel(tgt,4) */
   INTO dwh_wfs_performance.dim_oneapp_subscriber tgt USING (
      WITH
      all_dormancy AS (
         SELECT 
            S.oneapp_subscriber_key,
            0 AS active_ind,        -- because of non-active selection criterion
            CASE 
              WHEN g_check_date-last_subscr_login_date >= g_dormant_limit THEN
                1
              ELSE
                0
            END 
            AS dormant_ind,
            CASE 
              WHEN g_check_date-last_subscr_login_date > g_pre_dormant_limit
               AND g_check_date-last_subscr_login_date < g_dormant_limit THEN
                1
              ELSE 
                0
            END 
            AS pre_dormant_ind,
            CASE 
              WHEN g_check_date-last_subscr_login_date >= g_dormant_limit
              AND S.dormant_ind = 0 THEN
                1
              ELSE
                0
            END 
            AS inc_dormant_cnt,
            S.frequency_of_dormancy

         FROM dwh_wfs_performance.dim_oneapp_subscriber S
         WHERE 
            g_check_date-last_subscr_login_date > g_pre_dormant_limit --beyond active window
      )
      SELECT  /*+ parallel(t,4) full(t) */ 
          A.oneapp_subscriber_key
         ,A.active_ind
         ,A.dormant_ind
         ,A.pre_dormant_ind
         ,A.inc_dormant_cnt
         ,A.frequency_of_dormancy
      FROM all_dormancy A,
           dwh_wfs_performance.dim_oneapp_subscriber S
      WHERE S.oneapp_subscriber_key = A.oneapp_subscriber_key
        AND  ( S.active_ind  <> A.active_ind
               OR S.dormant_ind  <> A.dormant_ind
               OR S.pre_dormant_ind <> A.pre_dormant_ind
              )  -- only those that have changed
   ) upd_rec
   ON (
       	upd_rec.oneapp_subscriber_key = tgt.oneapp_subscriber_key
       )
   WHEN MATCHED THEN UPDATE 
      SET
         tgt.active_ind               = upd_rec.active_ind
        ,tgt.dormant_ind              = upd_rec.dormant_ind
        ,tgt.pre_dormant_ind          = upd_rec.pre_dormant_ind
        ,tgt.last_updated_date         = TRUNC(g_date)
        ,tgt.frequency_of_dormancy     = upd_rec.frequency_of_dormancy+inc_dormant_cnt

;
   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  'ONEAPP - DORMANCY_UPDATE sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      raise;

END dormancy_update;



PROCEDURE subscribers_load AS

BEGIN

   g_source_rows_chk := NULL;

   SELECT /*+ parallel(s,4) */  
      TRUNC(MAX(last_subscr_login_date))            -- most recent login
        INTO g_date_done
   FROM dwh_wfs_performance.dim_oneapp_subscriber S;
   IF g_date_done IS NOT NULL THEN                -- most recent login retrieved
     SELECT /*+ parallel(t,4) full(t) */
      COUNT(*) cnt                                  -- check if there is source data since most recent login
      INTO g_source_rows_chk
     FROM  dwh_wfs_foundation.fnd_wfs_oneapp_api   T
     WHERE T.api_event_create_date >  g_date_done + 1  -- plus 1 'cos g_date_done is trunc'd 
       AND ROWNUM< 100;                             -- for performance  - no need to count all
   ELSE                                     -- target table was empty
     SELECT  /*+ parallel(a,4)  */ 
      TRUNC(MIN(A.api_event_create_date))    -- to start with earliest source date
        INTO g_date_done
     FROM dwh_wfs_foundation.fnd_wfs_oneapp_api	A;
   END IF;
   IF g_date_done IS NULL                   -- source table empty
    OR g_source_rows_chk = 0 THEN           -- no new source data

       --g_date_done := TRUNC(g_date);

       l_text      := 'Latest data not available in FND_WFS_ONEAPP_API. Load abandoned.'; 
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       l_text :=  'Most recent login retrieved: '||to_char(g_date_done, 'YYYY-MM-DD');
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


       g_abandoned := TRUE;

--       p_success := true;
       return;    -- ************ EXIT **************

   END IF;


   SELECT  /*+ parallel(a,4)  */ 
    TRUNC(MAX(A.api_event_create_date))          -- to end with latest source date
      INTO g_date_end
   FROM dwh_wfs_foundation.fnd_wfs_oneapp_api	A;

   if g_date_end > g_date then      -- do not go beyond g_date
        g_date_end := g_date;
   end if;


   g_date_to_do := g_date_done;    -- reprocess last day in case more added
   g_date_end := nvl(g_date_end, g_date);

   l_text :=  'Processing from day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  to '||to_char(g_date_end, 'YYYY-MM-DD');
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   WHILE g_date_to_do <= g_date_end AND g_success 
   LOOP

      -- check if snapshot is up-to-date

      SELECT  /*+ parallel(t,4) full(t) */ 
         MAX(subscr_status_snapshot_date) 
         into g_last_snapshot_date

      FROM dwh_wfs_performance.wfs_oneapp_subscr_login_dly T;

      IF g_last_snapshot_date is not null and       -- proceed anyway if null   - new table
         g_last_snapshot_date < g_date_to_do - 1 THEN  -- snapshot for previous day not taken yet
         l_text := 'Snapshot of existing subscriber data not saved yet. Load for '||
                   to_char(g_date_to_do, 'YYYY-MM-DD')||' abandoned.'; 
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
         l_text := 'latest snapshot date: '||to_char(g_last_snapshot_date, 'YYYY-MM-DD');
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


         g_abandoned := TRUE;

--       p_success := true;
         return;    -- ************ EXIT **************

      END IF;


      -- check if summary is up-to-date

      SELECT  /*+ parallel(t,4) full(t) */ 
         MAX(SUBSCR_SUMMARY_DATE) AS last_summary_date
         into g_last_summary_date
      FROM dwh_wfs_performance.WFS_ONEAPP_SUBSCR_SUMMARY_DLY T;
      IF g_last_summary_date is not null and       -- proceed anyway if null   - new table
         g_last_summary_date < g_date_to_do - 1 THEN  -- summary for previous day not taken yet
         l_text := 'Summary of existing subscriber data not saved yet. Load for '|| 
                   to_char(g_date_to_do, 'YYYY-MM-DD')||' abandoned.'; 
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
         l_text := 'latest summary date: '||to_char(g_last_summary_date, 'YYYY-MM-DD');
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


         g_abandoned := TRUE;

--       p_success := true;
         return;    -- ************ EXIT **************

      END IF;


      g_check_date := g_date_to_do;

      -- ****** main load *****************************
      subscr_load_from_hist_day(g_date_to_do);
      -- **********************************************


      g_recs_cnt_day  :=SQL%rowcount;
      g_recs_read     :=  g_recs_read + g_recs_cnt_day;
      g_recs_inserted :=  g_recs_inserted + g_recs_cnt_day;

      COMMIT;  -- NB. write_log already does a commit !

      l_text :=  'For day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  Merged:  '||g_recs_cnt_day;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


      -- ****** update dormancy *****************************
      dormancy_update;
      -- **********************************************

      g_recs_dormcy_upd_day := SQL%rowcount;
      g_recs_updated   :=  g_recs_updated  + SQL%rowcount;

      COMMIT;  

      l_text :=  'For day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  Dormancy updated:  '||g_recs_dormcy_upd_day;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


      g_date_to_do := g_date_to_do + 1;

   END LOOP;

   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  'ONEAPP - subscribers_load sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      raise;


END subscribers_load;






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

    l_text := 'WFS OneApp DIM_ONEAPP_SUBSCRIBER load STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--p_success := true;
--return;    -- ************ EXIT ************** bypass until issues are fixed


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

    l_text := 'LOAD TABLE: '||'DIM_ONEAPP_SUBSCRIBER' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    -- ****** main load *************
    subscribers_load;
    -- ******************************

    if g_abandoned = TRUE then
       p_success := true;
       return;    -- ************ EXIT **************
    END IF;

    IF g_success THEN

       l_text :=  'Subscribers load complete';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--       dormancy_update;

--    END IF;

--    IF g_success THEN

--       g_recs_updated   :=  SQL%rowcount;

--       COMMIT;  

--       l_text :=  'Dormancy update complete';
--       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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
       l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated||'   (Dormancy)';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   --    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
       l_text :=  'RECORDS MERGED   '||g_recs_inserted;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   --    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
   --    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   --    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
   --    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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



end wh_prf_wfs_180u_20190221;
