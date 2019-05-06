--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_468U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_468U" (p_forall_limit IN INTEGER,p_success OUT BOOLEAN) AS

--**************************************************************************************************
-- Description: Card Track and Trace - Load DSV card-in-hand data in performance layer
--
--
-- Date:        2018-02-09
-- Author:      Naresh Chauhan
-- Purpose:     update table WFS_CARD_TRACKNTRACE in the Performance layer
--      
-- Tables: 
--              Input  - 
--                       FND_WFS_CARD_TRACKNTRACE
-- 
--              Output - WFS_CARD_TRACKNTRACE
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2018-02-09 N Chauhan - created.
--  2018-03-09 N Chauhan - fix check for end date of source data .
--  2018-05-16 N Chauhan - limit error string size to fit in error log.
--  2018-05-16 N Chauhan - Remove parallel hint in merge - does not work in prod.
--  2018-10-03 N Chauhan - fix check/logging for warning for no source data to load.

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
l_module_name        sys_dwh_errlog.log_procedure_name%TYPE    := 'WH_PRF_WFS_468U';
l_name               sys_dwh_log.log_name%TYPE                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%TYPE          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%TYPE          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%TYPE       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%TYPE  := 'CARD TRACK AND TRACE - LOAD DSV CARD-IN-HAND DATA IN PERFORMANCE LAYER';
l_process_type       sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;


g_job_desc VARCHAR2(200):= 'Load DSV card-in-hand data in performance layer';
g_success BOOLEAN:= TRUE;
g_source_rows_chk INTEGER :=0;

g_analysed_count INTEGER:=0;
g_analysed_success BOOLEAN:= FALSE;

g_check_date DATE := TRUNC(sysdate-1);
g_date_to_do DATE := TRUNC(sysdate-1);
g_date_done DATE := TRUNC(sysdate-7);
g_date_end  DATE := TRUNC(sysdate-1);
g_recs_cnt_day NUMBER := 0;



/* load new data & update existing  */

PROCEDURE load_from_hist_day(p_day_to_do DATE) AS


BEGIN


   MERGE /*+ append  */
   INTO dwh_wfs_performance.wfs_card_trackntrace tgt USING
     (
         SELECT  /*+ parallel(fnd,4) full(fnd)  */ 

            fnd.card_no ,
            fnd.dsv_unique_reference_no ,
            fnd.customer_id_no ,
            fnd.information_date ,
            fnd.customer_name ,
            fnd.card_type ,
            fnd.mobile_no ,
            fnd.office_phone_no ,
            fnd.home_phone_no ,
            fnd.card_embossed_date ,
            fnd.dsv_card_received_date ,
            fnd.days_elapsed_dsv_received ,
            fnd.delivered_date ,
            fnd.branch_code_and_name ,
            fnd.days_elapsed_dsv_delivered ,
            fnd.received_by_store_date ,
            fnd.days_elapsed_store_received ,
            fnd.cust_collect_at_store_datetime ,
            fnd.days_elapsed_store_collect ,
            fnd.pending_destruction_datetime ,
            fnd.days_elapsed_pending_destr ,
            fnd.dsv_collect_frm_store_datetime ,
            fnd.card_destroyed_datetime ,
            fnd.days_elapsed_card_destroy ,
            fnd.sms_datetime ,
            fnd.days_in_store_current ,
            fnd.days_in_store_no_longer ,
            fnd.safe_in_date ,
            fnd.thermal_in_date ,
            fnd.operator_no ,
            fnd.overseas_ind ,
            fnd.delivery_type ,
            fnd.wfs_product_desc ,
            fnd.fica_code ,
            fnd.fica_instructions ,
            fnd.fica_instructions_desc ,
            fnd.card_status

            ,TRUNC(g_date)

         FROM
             dwh_wfs_foundation.fnd_wfs_card_trackntrace  fnd
         WHERE 
          fnd.information_date >= p_day_to_do AND 
          fnd.information_date < p_day_to_do + 1 

   ) new_rec 

   ON (
                 new_rec.card_no  = tgt.card_no
             AND new_rec.dsv_unique_reference_no  = tgt.dsv_unique_reference_no
             AND new_rec.customer_id_no  = tgt.customer_id_no
             AND new_rec.information_date  = tgt.information_date

       )
   WHEN MATCHED THEN UPDATE 
    SET

            tgt. customer_name = new_rec. customer_name ,
            tgt. card_type = new_rec. card_type ,
            tgt. mobile_no = new_rec. mobile_no ,
            tgt. office_phone_no = new_rec. office_phone_no ,
            tgt. home_phone_no = new_rec. home_phone_no ,
            tgt. card_embossed_date = new_rec. card_embossed_date ,
            tgt. dsv_card_received_date = new_rec. dsv_card_received_date ,
            tgt. days_elapsed_dsv_received = new_rec. days_elapsed_dsv_received ,
            tgt. delivered_date = new_rec. delivered_date ,
            tgt. branch_code_and_name = new_rec. branch_code_and_name ,
            tgt. days_elapsed_dsv_delivered = new_rec. days_elapsed_dsv_delivered ,
            tgt. received_by_store_date = new_rec. received_by_store_date ,
            tgt. days_elapsed_store_received = new_rec. days_elapsed_store_received ,
            tgt. cust_collect_at_store_datetime = new_rec. cust_collect_at_store_datetime ,
            tgt. days_elapsed_store_collect = new_rec. days_elapsed_store_collect ,
            tgt. pending_destruction_datetime = new_rec. pending_destruction_datetime ,
            tgt. days_elapsed_pending_destr = new_rec. days_elapsed_pending_destr ,
            tgt. dsv_collect_frm_store_datetime = new_rec. dsv_collect_frm_store_datetime ,
            tgt. card_destroyed_datetime = new_rec. card_destroyed_datetime ,
            tgt. days_elapsed_card_destroy = new_rec. days_elapsed_card_destroy ,
            tgt. sms_datetime = new_rec. sms_datetime ,
            tgt. days_in_store_current = new_rec. days_in_store_current ,
            tgt. days_in_store_no_longer = new_rec. days_in_store_no_longer ,
            tgt. safe_in_date = new_rec. safe_in_date ,
            tgt. thermal_in_date = new_rec. thermal_in_date ,
            tgt. operator_no = new_rec. operator_no ,
            tgt. overseas_ind = new_rec. overseas_ind ,
            tgt. delivery_type = new_rec. delivery_type ,
            tgt. wfs_product_desc = new_rec. wfs_product_desc ,
            tgt. fica_code = new_rec. fica_code ,
            tgt. fica_instructions = new_rec. fica_instructions ,
            tgt. fica_instructions_desc = new_rec. fica_instructions_desc ,
            tgt. card_status = new_rec. card_status 
            ,tgt.last_updated_date        = TRUNC(g_date)

    WHEN NOT MATCHED THEN INSERT (

            tgt.card_no ,
            tgt.dsv_unique_reference_no ,
            tgt.customer_id_no ,
            tgt.information_date ,
            tgt.customer_name ,
            tgt.card_type ,
            tgt.mobile_no ,
            tgt.office_phone_no ,
            tgt.home_phone_no ,
            tgt.card_embossed_date ,
            tgt.dsv_card_received_date ,
            tgt.days_elapsed_dsv_received ,
            tgt.delivered_date ,
            tgt.branch_code_and_name ,
            tgt.days_elapsed_dsv_delivered ,
            tgt.received_by_store_date ,
            tgt.days_elapsed_store_received ,
            tgt.cust_collect_at_store_datetime ,
            tgt.days_elapsed_store_collect ,
            tgt.pending_destruction_datetime ,
            tgt.days_elapsed_pending_destr ,
            tgt.dsv_collect_frm_store_datetime ,
            tgt.card_destroyed_datetime ,
            tgt.days_elapsed_card_destroy ,
            tgt.sms_datetime ,
            tgt.days_in_store_current ,
            tgt.days_in_store_no_longer ,
            tgt.safe_in_date ,
            tgt.thermal_in_date ,
            tgt.operator_no ,
            tgt.overseas_ind ,
            tgt.delivery_type ,
            tgt.wfs_product_desc ,
            tgt.fica_code ,
            tgt.fica_instructions ,
            tgt.fica_instructions_desc ,
            tgt.card_status 
            ,tgt.last_updated_date

           ) 

    VALUES (

            new_rec.card_no ,
            new_rec.dsv_unique_reference_no ,
            new_rec.customer_id_no ,
            new_rec.information_date ,
            new_rec.customer_name ,
            new_rec.card_type ,
            new_rec.mobile_no ,
            new_rec.office_phone_no ,
            new_rec.home_phone_no ,
            new_rec.card_embossed_date ,
            new_rec.dsv_card_received_date ,
            new_rec.days_elapsed_dsv_received ,
            new_rec.delivered_date ,
            new_rec.branch_code_and_name ,
            new_rec.days_elapsed_dsv_delivered ,
            new_rec.received_by_store_date ,
            new_rec.days_elapsed_store_received ,
            new_rec.cust_collect_at_store_datetime ,
            new_rec.days_elapsed_store_collect ,
            new_rec.pending_destruction_datetime ,
            new_rec.days_elapsed_pending_destr ,
            new_rec.dsv_collect_frm_store_datetime ,
            new_rec.card_destroyed_datetime ,
            new_rec.days_elapsed_card_destroy ,
            new_rec.sms_datetime ,
            new_rec.days_in_store_current ,
            new_rec.days_in_store_no_longer ,
            new_rec.safe_in_date ,
            new_rec.thermal_in_date ,
            new_rec.operator_no ,
            new_rec.overseas_ind ,
            new_rec.delivery_type ,
            new_rec.wfs_product_desc ,
            new_rec.fica_code ,
            new_rec.fica_instructions ,
            new_rec.fica_instructions_desc ,
            new_rec.card_status
            , TRUNC(g_date)
           ) 

   ;

   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  'LOAD_FROM_HIST_DAY sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := substr(dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm,1,200);
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      raise;

END load_from_hist_day;




PROCEDURE LOAD AS

BEGIN

   g_source_rows_chk := NULL;

   SELECT /*+ parallel(s,4) */  
      TRUNC(MAX( information_date ))                -- most recent 
        INTO g_date_done
   FROM dwh_wfs_performance.wfs_card_trackntrace  S;
   IF g_date_done IS NOT NULL THEN                -- most recent retrieved
     SELECT /*+ parallel(t,4) full(t) */
      COUNT(*) cnt                                -- check if there is source data since most recent in perf table
      INTO g_source_rows_chk
     FROM  DWH_WFS_FOUNDATION.FND_WFS_CARD_TRACKNTRACE   T
     WHERE T.information_date >  g_date_done 
       AND ROWNUM< 100;                            -- for performance  - no need to count all
   ELSE                                            -- target table was empty
     SELECT  /*+ parallel(a,4)  */ 
      TRUNC(MIN(A.information_date))          -- to start with earliest source date
        INTO g_date_done
     FROM DWH_WFS_FOUNDATION.FND_WFS_CARD_TRACKNTRACE A;
   END IF;
   IF g_date_done IS NULL                          -- source table empty
    OR g_source_rows_chk = 0 THEN                  -- no new source data

       g_date_done := TRUNC(g_date);

       l_text      := 'Latest data not available in FND_WFS_CARD_TRACKNTRACE.'; 
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   END IF;

   SELECT  /*+ parallel(a,4)  */ 
    TRUNC(MAX(A.information_date))          -- to end with latest source date
      INTO g_date_end
   FROM dwh_wfs_foundation.FND_WFS_CARD_TRACKNTRACE A;

   g_date_to_do := g_date_done;    -- reprocess last day in case more added
   g_date_end := nvl(g_date_end, g_date);

   l_text :=  'Processing from day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  to '||to_char(g_date_end, 'YYYY-MM-DD');
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   WHILE g_date_to_do <= g_date_end AND g_success 
   LOOP


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
      l_message := substr(dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm,1,200);
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      raise;


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

    l_text := 'WFS_CARD_TRACKNTRACE load STARTED AT '||
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


    l_text := 'LOAD TABLE: WFS_CARD_TRACKNTRACE' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    -- ****** main load *************
    LOAD;
    -- ******************************


    COMMIT;  



--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

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
       l_message := substr(dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm,1,200);
       dwh_log.record_error(l_module_name,SQLCODE,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := FALSE;
       RAISE;

    WHEN OTHERS THEN
       ROLLBACK;
       l_message := substr(dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm,1,200);
       dwh_log.record_error(l_module_name,SQLCODE,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := FALSE;
       RAISE;



END wh_prf_wfs_468u;
