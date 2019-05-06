--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_616U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_616U" 
  (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: OneApp usage and navigations - Load/update subscriber statuses
--
--
-- Date:        2019-03-07
-- Author:      Naresh Chauhan
-- Purpose:     update table WFS_ONEAPP_SUBSCR_STATUS_HIST in the Performance layer
--      
-- Tables: 
--              Input  - 
--                       FND_WFS_ONEAPP_API
-- 
--              Output - WFS_ONEAPP_SUBSCR_STATUS_HIST
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2019-03-08 N Chauhan - created.
--  2019-03-11 N Chauhan - TEMP dblink to prod
--  2019-03-18 N Chauhan - rectify record_active_from_date to be batch_date for new subscribers 
--  2019-03-18 N Chauhan - TEMP dblink to prod removed for implementation.

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




g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_date               date          :=  trunc(sysdate);

g_year_no            integer       :=  0;
g_month_no           integer       :=  0;


L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_616U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'ONEAPP USAGE AND NAVIGATIONS - LOAD/UPDATE SUBSCRIBER STATUSES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


g_job_desc varchar2(200):= 'Load/update subscriber statuses';
g_success boolean:= TRUE;
g_date_start date;
g_date_end date;
g_yr_mth_to_do integer :=  0;
g_date_to_do DATE := TRUNC(sysdate-1);
g_date_done DATE := TRUNC(sysdate-7);

g_run_date date;
g_2nd_of_month date;
g_today date;
g_sql_stmt varchar2(200);
g_source_rows_chk integer :=0;
g_recs_cnt_day NUMBER := 0;

g_idx_drop_success boolean:= false;
g_idx_existed  boolean:= false;
g_analysed_count integer:=0;
g_analysed_success boolean:= false;

g_pre_dormant_limit NUMBER(11)  := 30;
g_dormant_limit NUMBER(11)      := 45;

g_redo_del_count   integer       :=  0;
g_redo_del_newsub_count   integer       :=  0;
g_redo_upd_count   integer       :=  0;


PROCEDURE load_day(p_day_to_do DATE) AS

BEGIN


   -- First DELETE/UPDATE to revert to previous state to accommodate re-runs

   -- for existing subscribers  ------------
   -- delete 'new' records as at p_day_to_do, for a refresh.
   DELETE  /*+ parallel(t,4) full(t) */ 
   FROM DWH_WFS_PERFORMANCE.WFS_ONEAPP_SUBSCR_STATUS_HIST T
   WHERE record_active_from_date =  p_day_to_do + 1;
   
   g_redo_del_count:=SQL%rowcount;
   commit;

--      l_text :=  'For day '||to_char(p_day_to_do, 'YYYY-MM-DD')||'Existing subscribers - Deleted:  '||g_redo_del_count; 
--      SYS.dbms_output.put_line(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||l_text);

   -- for new subscribers  --------------
   -- delete 'new' records as at p_day_to_do, for a refresh.
   DELETE  /*+ parallel(t,4) full(t) */ 
   FROM DWH_WFS_PERFORMANCE.WFS_ONEAPP_SUBSCR_STATUS_HIST T
   WHERE record_active_from_date =  p_day_to_do
     and last_subscr_login_date = record_active_from_date;
   
   g_redo_del_newsub_count:=SQL%rowcount;
   commit;

--      l_text :=  'For day '||to_char(p_day_to_do, 'YYYY-MM-DD')||'New subscribers - Deleted:  '||g_redo_del_count; 
--      SYS.dbms_output.put_line(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||l_text);

 
   -- restore closed off recs back to 'open'
   UPDATE /*+ parallel(t) full(t) */  
   DWH_WFS_PERFORMANCE.WFS_ONEAPP_SUBSCR_STATUS_HIST T
   SET record_active_to_date = '01/jan/3000'
   WHERE record_active_to_date = p_day_to_do;

   g_redo_upd_count:=SQL%rowcount;
   commit;

--      l_text :=  'For day '||to_char(p_day_to_do, 'YYYY-MM-DD')||'  Re-opened:  '||g_redo_upd_count; 
--      SYS.dbms_output.put_line(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||l_text);

   l_text :=  'For day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  revert for re-do -'||
   '  Existing subscr del:  '||g_redo_del_count||';  New subscr del: '||g_redo_del_newsub_count||';  Updated:  '||g_redo_upd_count;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


  -- Update with new data
   
   MERGE /*+ append parallel(tgt) */
   INTO DWH_WFS_PERFORMANCE.WFS_ONEAPP_SUBSCR_STATUS_HIST tgt USING
     (
       WITH 

       -- get list of existing subscriber with valid records
        existing_subscrs AS (
          SELECT *  /*+ parallel(exst,4) full(exst) */ 
          FROM DWH_WFS_PERFORMANCE.WFS_ONEAPP_SUBSCR_STATUS_HIST exst
          WHERE record_active_to_date >  p_day_to_do
          
          -- for those existing at the time, this additioinal filter
          AND record_active_from_date <=  p_day_to_do
          -- delete subsequent data for a refresh?
        )
        ,
        -- new logins
        all_logins_this_day AS (
            SELECT  /*+ parallel(f,4) full(f) */ 
               substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(f.username_token,'sub') ,1,50) 
               AS oneapp_subscriber_key,
               trunc(f.API_EVENT_CREATE_DATE) as api_event_create_trunc_dt
            FROM DWH_WFS_FOUNDATION.FND_WFS_ONEAPP_API f
            where f.API_EVENT_CREATE_DATE >= p_day_to_do
              and f.API_EVENT_CREATE_DATE <  p_day_to_do + 1
        )
        ,
        logged_in_this_day AS (
            SELECT  /*+ parallel(t,4) full(t) */ 
               oneapp_subscriber_key,
               MAX(api_event_create_trunc_dt) AS last_subscr_login_date
            FROM all_logins_this_day T
            where oneapp_subscriber_key is not null
              and oneapp_subscriber_key <> '.'
            GROUP BY oneapp_subscriber_key
        )
        ,
        -- new subscribers, where we don't need to close off existing records
        new_subscr AS (
         SELECT  /*+ parallel(l,4) full(l) parallel(exst,4) full(exst) */ 
            DISTINCT
            L.oneapp_subscriber_key ,
--            p_day_to_do+1  AS record_active_from_date ,     -- batchdate plus one - next day onwards 
            p_day_to_do  AS record_active_from_date ,     -- batchdate, as this records is to be currently active 
            TO_DATE('01/jan/3000', 'DD/mon/YYYY') AS record_active_to_date ,  -- future date to indicate as the current valid record
            'ACTIVE' as status_desc
            ,p_day_to_do as last_subscr_login_date
         FROM
            logged_in_this_day  L
            LEFT OUTER JOIN existing_subscrs exst  ON ( L. oneapp_subscriber_key = exst.oneapp_subscriber_key )
         WHERE  exst.oneapp_subscriber_key IS NULL
        )
        ,
        -- new logins changing status for existing subscriber
        existing_subscr_new_logins AS (
         SELECT  /*+ parallel(l,4) full(l) parallel(exst,4) full(exst) */ 
            DISTINCT
            L.oneapp_subscriber_key ,
            exst.record_active_from_date as prev_from_dt,
            p_day_to_do+1  AS record_active_from_date ,     -- batchdate plus one - next day onwards
            TO_DATE('01/jan/3000', 'DD/mon/YYYY') AS record_active_to_date ,  -- future date to indicate as the current valid record
            'ACTIVE' as status_desc
            ,p_day_to_do as last_subscr_login_date
            ,exst.last_subscr_login_date as prev_login
         FROM
            logged_in_this_day  L
            inner JOIN existing_subscrs exst  ON ( L. oneapp_subscriber_key = exst.oneapp_subscriber_key )
         WHERE  exst.status_desc <> 'ACTIVE'
        )
        ,
        -- for update of last login where no status changed
        exst_subscr_new_login_no_chg as (
         SELECT  /*+ parallel(l,4) full(l) parallel(exst,4) full(exst) */ 
            L.oneapp_subscriber_key ,
            exst.record_active_from_date,
            exst.record_active_to_date,  -- future date to indicate as the current valid record
            exst.status_desc,
            p_day_to_do as last_subscr_login_date
         FROM
            logged_in_this_day  L
            inner JOIN existing_subscrs exst  ON ( L. oneapp_subscriber_key = exst.oneapp_subscriber_key )
         WHERE  exst.status_desc = 'ACTIVE'
        )        
       
        ,
        exst_subscr_became_pre_dorm AS (
            SELECT  /*+ parallel(n,4) full(n) parallel(exst,4) full(exst) */ 
               exst.oneapp_subscriber_key,
               exst.record_active_from_date as prev_from_dt,
               p_day_to_do+1  AS record_active_from_date ,     -- batchdate plus one - to align with JOB_REC_EFFECTIVE_DATE 
               TO_DATE('01/jan/3000', 'DD/mon/YYYY') AS record_active_to_date ,  -- future date to indicate as the current valid record
               'PRE-DORMANT' as status_desc 
               ,exst.last_subscr_login_date
            FROM existing_subscrs exst 
                 left outer join logged_in_this_day N    
                      on ( n.oneapp_subscriber_key = exst.oneapp_subscriber_key )
            where n.oneapp_subscriber_key is null             -- no new login
              and p_day_to_do - exst.last_subscr_login_date < g_dormant_limit
              and p_day_to_do - exst.last_subscr_login_date > g_pre_dormant_limit
              and exst.status_desc <> 'PRE-DORMANT'
        )
        ,
        exst_subscr_became_dorm AS (
            SELECT  /*+ parallel(n,4) full(n) parallel(exst,4) full(exst) */ 
               exst.oneapp_subscriber_key,
               exst.record_active_from_date as prev_from_dt,
               p_day_to_do+1  AS record_active_from_date ,     -- batchdate plus one - to align with JOB_REC_EFFECTIVE_DATE 
               TO_DATE('01/jan/3000', 'DD/mon/YYYY') AS record_active_to_date ,  -- future date to indicate as the current valid record
               'DORMANT' as status_desc
              ,exst.last_subscr_login_date
            FROM existing_subscrs exst 
                 left outer join logged_in_this_day N    
                      on ( n.oneapp_subscriber_key = exst.oneapp_subscriber_key )
            where n.oneapp_subscriber_key is null             -- no new login
              and p_day_to_do - exst.last_subscr_login_date >= g_dormant_limit
              and exst.status_desc <> 'DORMANT'
       )

       ,
       exst_recs_to_close_off as (
       
           SELECT  /*+ parallel(exst,4) full(exst) */ 
              exst.oneapp_subscriber_key ,
              exst.prev_from_dt as record_active_from_date,
              p_day_to_do AS record_active_to_date,   -- batchdate  - a day before opening day of new record 
              'DUMMY exst new lgin' as status_desc,
              exst.prev_login as last_subscr_login_date
           FROM existing_subscr_new_logins exst
           union
           SELECT  /*+ parallel(exst,4) full(exst) */ 
              exst.oneapp_subscriber_key ,
              exst.prev_from_dt  as record_active_from_date,
              p_day_to_do AS record_active_to_date,   -- batchdate  - a day before opening day of new record 
              'DUMMY exst predorm' as status_desc,
              exst.last_subscr_login_date
           FROM exst_subscr_became_pre_dorm exst
           union
           SELECT  /*+ parallel(exst,4) full(exst) */ 
              exst.oneapp_subscriber_key ,
              exst.prev_from_dt  as record_active_from_date,
              p_day_to_do AS record_active_to_date,   -- batchdate  - a day before opening day of new record 
              'DUMMY exst dorm' as status_desc,
              exst.last_subscr_login_date
           FROM exst_subscr_became_dorm exst
        )   


       -- new subscribers
       SELECT * FROM new_subscr

       UNION  

       -- updates to be added as new records
       SELECT 
            oneapp_subscriber_key,
            record_active_from_date,
            record_active_to_date,
            status_desc,
            last_subscr_login_date
       FROM existing_subscr_new_logins        
       UNION  
       SELECT
            oneapp_subscriber_key,
            record_active_from_date,
            record_active_to_date,
            status_desc,
            last_subscr_login_date
       FROM exst_subscr_became_pre_dorm        

       UNION  
       SELECT
            oneapp_subscriber_key,
            record_active_from_date,
            record_active_to_date,
            status_desc,
            last_subscr_login_date
       FROM exst_subscr_became_dorm        


       UNION

       -- existing records to be closed off
       SELECT 
            oneapp_subscriber_key,
            record_active_from_date,
            record_active_to_date,
            status_desc,
            last_subscr_login_date
       FROM exst_recs_to_close_off exst
      
       UNION
       
       -- newer logins for existing records that did not have a status change
       SELECT 
            oneapp_subscriber_key,
            record_active_from_date,
            record_active_to_date,
            status_desc,
            last_subscr_login_date
       from exst_subscr_new_login_no_chg
                  

   ) rec_to_ins_or_upd 

   ON (
          rec_to_ins_or_upd.oneapp_subscriber_key  = tgt.oneapp_subscriber_key AND 
          rec_to_ins_or_upd.record_active_from_date  = tgt.record_active_from_date
                 -- to get match for updating/closing off existing valid record
       )
       
   WHEN MATCHED THEN 
   
      UPDATE SET
              tgt.record_active_to_date = rec_to_ins_or_upd.record_active_to_date     -- close off with a day before active_to date of new record
             ,tgt.last_subscr_login_date = rec_to_ins_or_upd.last_subscr_login_date
             ,tgt.last_updated_date = TRUNC(p_day_to_do)
  
   WHEN NOT MATCHED THEN 
   
      INSERT (
              oneapp_subscriber_key ,
              record_active_from_date ,
              record_active_to_date ,
              status_desc ,
              last_subscr_login_date ,
              last_updated_date
             ) 
  
      VALUES (
              rec_to_ins_or_upd.oneapp_subscriber_key ,
              rec_to_ins_or_upd.record_active_from_date ,
              rec_to_ins_or_upd.record_active_to_date ,
              rec_to_ins_or_upd.status_desc ,
              rec_to_ins_or_upd.last_subscr_login_date 
              , TRUNC(p_day_to_do)
             ) 
   ;

   g_success := true;
    
    
exception

   when others then

      rollback;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  l_description||' - LOAD_DAY sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
      dwh_log.record_error(l_module_name,sqlcode,l_message);

      g_success := false;
      raise;

end load_day;




PROCEDURE LOAD AS

BEGIN

   g_source_rows_chk := NULL;

   SELECT /*+ parallel(s,4) */  
      TRUNC(MAX( record_active_from_date )) - 1     -- most recent 
        -- minus 1 as record_active_from_date is future dated by 1 day
        INTO g_date_done
   FROM DWH_WFS_PERFORMANCE.WFS_ONEAPP_SUBSCR_STATUS_HIST  S;
   IF g_date_done IS NOT NULL THEN                -- most recent retrieved
     SELECT /*+ parallel(t,4) full(t) */
      COUNT(*) cnt                                -- check if there is source data since most recent in perf table
      INTO g_source_rows_chk
     FROM  DWH_WFS_FOUNDATION.FND_WFS_ONEAPP_API   T

     WHERE T.API_EVENT_CREATE_DATE >  g_date_done 
       AND ROWNUM< 100;                            -- for performance  - no need to count all
   ELSE                                            -- target table was empty
     SELECT  /*+ parallel(a,4)  */ 
      TRUNC(MIN(A.API_EVENT_CREATE_DATE))          -- to start with earliest source date
        INTO g_date_done
     FROM DWH_WFS_FOUNDATION.FND_WFS_ONEAPP_API A;


   END IF;
   IF g_date_done IS NULL                          -- source table empty
    OR g_source_rows_chk = 0 THEN                  -- no new source data

       g_date_done := TRUNC(g_date);

       l_text      := 'Latest data not available in FND_WFS_ONEAPP_API.'; 
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   END IF;

   SELECT  /*+ parallel(a,4)  */ 
    TRUNC(MAX(A.API_EVENT_CREATE_DATE))          -- to end with latest source date
      INTO g_date_end
   FROM DWH_WFS_FOUNDATION.FND_WFS_ONEAPP_API A;

   g_date_to_do := g_date_done;    -- reprocess last day in case more added
   g_date_end := nvl(g_date_end, g_date);

   l_text :=  'Processing from day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  to '||to_char(g_date_end, 'YYYY-MM-DD');
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   WHILE g_date_to_do <= g_date_end AND g_success 
   LOOP

      -- ****** daily load *****************************
      load_day(g_date_to_do);
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
      l_message := substr(dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm,1,200);
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      raise;


END LOAD;




--##############################################################################################
-- Main process
--**********************************************************************************************
 
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
--    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Load/update subscriber statuses - STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************


    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    
    l_text := 'LOAD TABLE: '||'WFS_ONEAPP_SUBSCR_STATUS_HIST' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   

    -- ****** main load *************
    LOAD;
    -- ******************************


    commit;  

    
 

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    -- NB write_log does a commit !

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,g_recs_deleted,g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    l_text :=  'RECORDS MERGED   '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    if g_success then
   
        p_success := true;
        commit;

--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - ends');
    else
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
--      || '   - load for day '||to_char(g_date_to_do,'yyyy-mm-dd') ||' fails');
      
        rollback;
        l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc||'  fails';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
    
        p_success := false;

    end if;
   
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


exception

    when dwh_errors.e_insert_error then
       rollback;
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := false;
       raise;

    when others then
       rollback;
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := false;
       raise;



end WH_PRF_WFS_616U;
