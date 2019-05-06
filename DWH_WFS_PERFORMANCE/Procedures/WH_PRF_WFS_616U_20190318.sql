--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_616U_20190318
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_616U_20190318" 
(p_forall_limit IN INTEGER,p_success OUT BOOLEAN) AS

-- ++++  SUPERCEDED with saving only changes instead of snapshots

--**************************************************************************************************
-- Description: OneApp usage and navigations - Load subscriber daily status snapshots
--
-- WARNING:  *****  Snapshots are created for g_date. 
--           *****  To catch up snapshots for any previous days, the procedure will require alteration.
--
-- Date:        2018-03-19
-- Author:      Naresh Chauhan
-- Purpose:     update table WFS_ONEAPP_SUBSCR_LOGIN_DLY in the Performance layer
--      
-- Tables: 
--              Input  - 
--                       DIM_ONEAPP_SUBSCRIBER
-- 
--              Output - WFS_ONEAPP_SUBSCR_LOGIN_DLY
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2018-03-19 N Chauhan - created.
--  2018-04-24 N Chauhan - remove temp fixes used for testing.
--
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
l_module_name        sys_dwh_errlog.log_procedure_name%TYPE    := 'WH_PRF_WFS_616U';
l_name               sys_dwh_log.log_name%TYPE                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%TYPE          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%TYPE          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%TYPE       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%TYPE  := 'ONEAPP USAGE AND NAVIGATIONS - LOAD SUBSCRIBER DAILY STATUS SNAPSHOTS';
l_process_type       sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;


g_job_desc VARCHAR2(200):= 'Load subscriber daily status snapshots';
g_success BOOLEAN:= TRUE;
g_date_start DATE;
g_date_end DATE;
--g_date_to_do date;
g_yr_mth_to_do INTEGER :=  0;

g_run_date DATE;
g_2nd_of_month DATE;
g_today DATE;
g_sql_stmt VARCHAR2(200);
g_source_rows_chk INTEGER :=0;

g_idx_drop_success BOOLEAN:= FALSE;
g_idx_existed  BOOLEAN:= FALSE;
g_analysed_count INTEGER:=0;
g_analysed_success BOOLEAN:= FALSE;


PROCEDURE dly_load(g_success OUT BOOLEAN) AS


BEGIN

   MERGE /*+ append  */
   INTO dwh_wfs_performance.wfs_oneapp_subscr_login_dly tgt USING 
   (
      WITH 
      dates AS (
         SELECT  /*+ parallel(t,4) full(t) */ 
            nvl(MAX(subscr_status_snapshot_date),'01/jan/1900') AS prev_snapshot_date
         FROM dwh_wfs_performance.wfs_oneapp_subscr_login_dly T
         WHERE subscr_status_snapshot_date <  TRUNC(g_date)  -- in case already run for the day

      ),

      prev_snapshot AS (
         SELECT  /*+ parallel(t,4) full(t) */ 
            *
         FROM dwh_wfs_performance.wfs_oneapp_subscr_login_dly T
         INNER JOIN dates D ON (D.prev_snapshot_date = T.subscr_status_snapshot_date)
      )

      SELECT  /*+ parallel(S,4) full(S) parallel(P,4) full(P) */ 

         S.oneapp_subscriber_key
         ,g_date as SUBSCR_STATUS_SNAPSHOT_DATE
         ,CASE 
         WHEN P.oneapp_subscriber_key IS NULL THEN
          1
         ELSE
          0
         END 
         AS new_subscr_ind
         ,S.oneapp_api_no
         ,S.last_subscr_login_date
         ,(g_date - trunc(S.LAST_SUBSCR_LOGIN_DATE)) as  DAYS_ELAPSED_SINCE_LAST_LOGIN
         ,S.active_ind
         ,S.dormant_ind
         ,S.pre_dormant_ind
         ,CASE 
         WHEN nvl(P.dormant_ind,0) = 0 
          AND S.dormant_ind = 1 THEN
          1
         ELSE
          0
         END 
         AS became_dormant_ind 
         ,TRUNC(g_date) AS last_updated_date
      FROM dwh_wfs_performance.dim_oneapp_subscriber S
         LEFT OUTER JOIN prev_snapshot P
            ON ( P.oneapp_subscriber_key = S.oneapp_subscriber_key )


   ) new_rec 

   ON (
        new_rec.oneapp_subscriber_key = tgt.oneapp_subscriber_key AND 
        new_rec.subscr_status_snapshot_date = tgt.subscr_status_snapshot_date
 	    )


    WHEN NOT MATCHED THEN INSERT (


         tgt.oneapp_subscriber_key ,
         tgt.subscr_status_snapshot_date ,
         tgt.new_subscr_ind ,
         tgt.oneapp_api_no ,
         tgt.last_subscr_login_date ,
         tgt.days_elapsed_since_last_login ,
         tgt.active_ind ,
         tgt.pre_dormant_ind ,
         tgt.dormant_ind ,
         tgt.became_dormant_ind
        ,tgt.last_updated_date
           ) 

    VALUES (

         new_rec.oneapp_subscriber_key ,
         new_rec.subscr_status_snapshot_date ,
         new_rec.new_subscr_ind ,
         new_rec.oneapp_api_no ,
         new_rec.last_subscr_login_date ,
         new_rec.days_elapsed_since_last_login ,
         new_rec.active_ind ,
         new_rec.pre_dormant_ind ,
         new_rec.dormant_ind ,
         new_rec.became_dormant_ind
         ,new_rec.last_updated_date

           ) 
   ;


   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  l_description||' - DLY_LOAD sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := substr(dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm, 1, 200);
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
--      raise;

END dly_load;




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

    l_text := 'Load subscriber daily status snapshots - STARTED AT '||
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


    l_text := 'LOAD TABLE: '||'WFS_ONEAPP_SUBSCR_LOGIN_DLY' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



    -- ****** main load *************************
    dly_load(g_success);
    -- ******************************************

    g_recs_read     :=  SQL%rowcount;
    g_recs_inserted :=  SQL%rowcount;

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
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



    IF g_success THEN


--**************************************************************************************************
-- Retention maintenance    
--**************************************************************************************************

     -- excluded -  Managed separately  by Quentin  - copy specs also to Raamy and Laurencia





--**************************************************************************************************
-- gather statistics
--**************************************************************************************************


-- skip gather statistics - let DBA's maintenance task do it overnight
/* 
       l_text := 'gathering statistics ...';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 

--     DBMS_STATS.SET_TABLE_PREFS('DWH_WFS_PERFORMANCE','WFS_MART_SALES_RWDS_BU_MLY','INCREMENTAL','TRUE');  
--     done by dba, need only do once

    -- analyse all unanalysed partitions, one partition at a time
       DWH_DBA_WFS.stats_partitions_outstanding (
            'DWH_WFS_PERFORMANCE',
            'WFS_MART_SALES_RWDS_BU_MLY',
            g_analysed_count,
            g_analysed_success );

        if g_analysed_success = false then
           l_text := 'gather_table_stats failed';
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
        else 
           l_text := 'gather_table_stats : '||g_analysed_count||' partitions analysed' ;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name, l_text); 
        end if;   
*/

        p_success := TRUE;
        COMMIT;

--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - ends');
    ELSE
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
--      || '   - load for day '||to_char(g_date_to_do,'yyyy-mm-dd') ||' fails');

        ROLLBACK;
        l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
                  || '   - load for month '||g_yr_mth_to_do||'  fails';
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
       l_message := substr(dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm, 1,200);
       dwh_log.record_error(l_module_name,SQLCODE,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := FALSE;
       RAISE;




END WH_PRF_WFS_616U_20190318;
