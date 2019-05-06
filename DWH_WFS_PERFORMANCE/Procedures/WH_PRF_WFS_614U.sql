--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_614U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_614U" (p_forall_limit IN INTEGER,p_success OUT BOOLEAN) AS

--**************************************************************************************************
-- Description: OneApp usage and navigations - Load summary of app usage
--
--
-- Date:        2018-01-18
-- Author:      Naresh Chauhan
-- Purpose:     update table WFS_ONEAPP_SUBSCR_SUMMARY_DLY in the Performance layer
--      
-- Tables: 
--              Input  - 
--                       DIM_ONEAPP_SUBSCRIBER
-- 
--              Output - WFS_ONEAPP_SUBSCR_SUMMARY_DLY
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2018-01-18 N Chauhan - created.

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
l_module_name        sys_dwh_errlog.log_procedure_name%TYPE    := 'WH_PRF_WFS_614U';
l_name               sys_dwh_log.log_name%TYPE                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%TYPE          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%TYPE          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%TYPE       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%TYPE  := 'ONEAPP USAGE AND NAVIGATIONS - LOAD SUMMARY OF APP USAGE';
l_process_type       sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;


g_job_desc VARCHAR2(200):= 'Load summary of app usage';
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

   MERGE /*+ append parallel(tgt,4) */
   INTO dwh_wfs_performance.wfs_oneapp_subscr_summary_dly tgt USING 
   (
      WITH 
      subscr_status AS (
         SELECT  /*+ parallel(s,4) full(s) */ 

         1 AS is_subscr,
         CASE WHEN customer_no IS NOT NULL
          THEN 1 ELSE 0 END
         AS is_linked,
         CASE WHEN customer_no IS NULL
          THEN 1 ELSE 0 END
         AS is_unlinked,
         CASE WHEN TRUNC(g_date)-TRUNC(last_subscr_login_date) < 45 
          THEN 1 ELSE 0 END
         AS is_active ,
         CASE WHEN TRUNC(g_date)-TRUNC(last_subscr_login_date) >= 45 
          THEN 1 ELSE 0 END
         AS is_dormant ,
         CASE WHEN TRUNC(g_date)-TRUNC(last_subscr_login_date) = 45 
          THEN 1 ELSE 0 END
         AS is_new_dormant
               FROM dwh_wfs_performance.dim_oneapp_subscriber S
      )
      SELECT  /*+ parallel(s,4) full(s) */ 
         TRUNC(g_date) AS subscr_summary_date ,
         SUM(is_subscr) AS total_no_of_subscribers,
         SUM(is_linked) AS no_of_linked_customers,
         SUM(is_unlinked) AS no_of_unlinked_customers,
         SUM(is_active) AS no_of_active_subscribers ,
         SUM(is_dormant) AS no_of_dormant_subscribers ,
         SUM(is_new_dormant) AS no_of_new_dormant_subscribers ,
         TRUNC(g_date) AS last_updated_date

      FROM subscr_status S

   ) new_rec 

   ON (
        new_rec.subscr_summary_date = tgt.subscr_summary_date 
 	    )
   WHEN MATCHED THEN UPDATE 
    SET

--         tgt.subscr_summary_date = new_rec.subscr_summary_date ,
         tgt.total_no_of_subscribers = new_rec.total_no_of_subscribers ,
         tgt.no_of_linked_customers = new_rec.no_of_linked_customers ,
         tgt.no_of_unlinked_customers = new_rec.no_of_unlinked_customers ,
         tgt.no_of_active_subscribers = new_rec.no_of_active_subscribers ,
         tgt.no_of_dormant_subscribers = new_rec.no_of_dormant_subscribers ,
         tgt.no_of_new_dormant_subscribers = new_rec.no_of_new_dormant_subscribers
         ,tgt.last_updated_date        = TRUNC(g_date)

    WHEN NOT MATCHED THEN INSERT (

         tgt.subscr_summary_date ,
         tgt.total_no_of_subscribers ,
         tgt.no_of_linked_customers ,
         tgt.no_of_unlinked_customers ,
         tgt.no_of_active_subscribers ,
         tgt.no_of_dormant_subscribers ,
         tgt.no_of_new_dormant_subscribers
        ,tgt.last_updated_date
           ) 

    VALUES (

         new_rec.subscr_summary_date ,
         new_rec.total_no_of_subscribers ,
         new_rec.no_of_linked_customers ,
         new_rec.no_of_unlinked_customers ,
         new_rec.no_of_active_subscribers ,
         new_rec.no_of_dormant_subscribers ,
         new_rec.no_of_new_dormant_subscribers ,
         new_rec.last_updated_date

           ) 
   ;


   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  l_description||' - DLY_LOAD sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
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

    l_text :=  g_job_desc||'  - STARTED AT '||
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


    l_text := 'LOAD TABLE: '||'WFS_ONEAPP_SUBSCR_SUMMARY_DLY' ;
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
   --    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    l_text :=  'RECORDS MERGED   '||g_recs_inserted;
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
                  || '   - load fails';
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



END wh_prf_wfs_614u;
