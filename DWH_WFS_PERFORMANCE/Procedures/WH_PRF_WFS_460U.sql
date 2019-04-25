--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_460U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_460U" (p_forall_limit IN INTEGER,p_success OUT BOOLEAN) AS

--**************************************************************************************************
-- Description: OneApp usage and navigations - Load subscriber activities and app interactions
--
--
-- Date:        2018-01-16
-- Author:      Naresh Chauhan
-- Purpose:     update table WFS_ONEAPP_ACTIVITY_TRAIL in the Performance layer
--      
-- Tables: 
--              Input  - 
--                       FND_WFS_ONEAPP_API
--                       FND_WFS_ONEAPP_GATEWAY
--                       DIM_ONEAPP_SUBSCRIBER
-- 
--              Output - WFS_ONEAPP_ACTIVITY_TRAIL
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2018-01-16 N Chauhan - created.
--  2018-01-26 N Chauhan - improve start/end day calc, and tune query.
--  2018-02-22 N Chauhan - tune query.
--  2018-02-27 N Chauhan - table structure changes for performance and tune query.
--  2018-02-27 N Chauhan - hints tuning adjusted for when large amount of data present.
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
l_module_name        sys_dwh_errlog.log_procedure_name%TYPE    := 'WH_PRF_WFS_460U';
l_name               sys_dwh_log.log_name%TYPE                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%TYPE          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%TYPE          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%TYPE       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%TYPE  := 'ONEAPP USAGE AND NAVIGATIONS - LOAD SUBSCRIBER ACTIVITIES AND APP INTERACTIONS';
l_process_type       sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;


g_job_desc VARCHAR2(200):= 'Load subscriber activities and app interactions';
g_success BOOLEAN:= TRUE;

g_source_rows_chk INTEGER :=0;


g_idx_drop_success BOOLEAN:= FALSE;
g_idx_existed  BOOLEAN:= FALSE;
g_analysed_count INTEGER:=0;
g_analysed_success BOOLEAN:= FALSE;

g_check_date DATE := TRUNC(sysdate-1);
g_date_to_do DATE := TRUNC(sysdate-1);
g_date_done DATE := TRUNC(sysdate-7);
g_date_end  DATE := TRUNC(sysdate-1);
g_recs_cnt_day NUMBER := 0;

/* load new event_requests & update existing event_requests */

PROCEDURE day_activities_load(  p_day_to_do DATE) AS

day_to_do_end date := to_date(to_char(p_day_to_do, 'dd/mon/yy')||' 23:59:29','dd/mon/yy hh24:mi:ss');

BEGIN


   MERGE /*+ parallel(tgt,4) append index(tgt PK_WFS_ONEAPP_ACTIV_TRL)  */
   INTO dwh_wfs_performance.wfs_oneapp_activity_trail tgt USING 
     (
        WITH 
        recent_api_recs AS (
           SELECT  /*+ parallel(A,4) full(A) */ 
               A.oneapp_api_no
              ,substr(dwh_wfs_performance.dwh_stdlib_wfs.username_extract(username_token,'sub') ,1,50) 
               AS oneapp_subscriber_key
              ,A.api_event_create_date
              ,TRUNC(api_event_create_date)
               AS api_event_create_trunc_dt
              ,A.device_os
              ,A.device_os_version
              ,A.user_device_interface
              ,A.user_device_interface_version
              ,A.device_model
              ,A.device_version
              ,A.mobile_network
              ,A.api_event_request
              ,A.api_event_status
              ,regexp_substr(
                         regexp_substr (api_event_request,'/[[:digit:]]+/products|products/[[:digit:]]+$'),
                        '[[:digit:]]+' )  -- extract number from event_request
              AS item_no
           FROM 
              dwh_wfs_foundation.fnd_wfs_oneapp_api	A
           WHERE A.api_event_create_date between p_day_to_do      -- already trunc'd 
              and day_to_do_end
        )

           SELECT  /*+ parallel(A,4)  parallel(G,4) parallel(S,4) full(S) parallel(I,4) index(G I20_FND_WFS_ONEAPP_GATEW) */ 

               A.oneapp_api_no
              ,nvl(G.oneapp_gateway_no,0) AS oneapp_gateway_no
              ,A.oneapp_subscriber_key
              ,S.atg_customer_no
              ,S.customer_no
              ,A.api_event_create_date
              ,A.api_event_create_trunc_dt
              ,A.device_os
              ,A.device_os_version
              ,A.user_device_interface
              ,A.user_device_interface_version
              ,A.device_model
              ,A.device_version
              ,A.mobile_network
              ,A.api_event_request
              ,A.api_event_status
              ,G.gateway_event_create_date
              ,G.gateway_event_request
              ,G.product_group_no
              ,CASE  G.product_group_no
                  WHEN 1 THEN 'Credit Card'
                  WHEN 2 THEN 'Personal Loan'
                  WHEN 3 THEN 'Personal Loan'
                  WHEN 4 THEN 'Store Card'
                  WHEN 5 THEN 'Personal Loan'
                  ELSE NULL
               END 
               AS product_name
              ,G.product_event_status
              ,CASE WHEN I.item_no IS NULL THEN 0 ELSE 1 END 
               AS item_ind
              ,I.item_no
           FROM 
              recent_api_recs A
              INNER JOIN dwh_wfs_performance.dim_oneapp_subscriber	S
                ON (S.oneapp_subscriber_key = A.oneapp_subscriber_key )
              LEFT OUTER JOIN dwh_performance.dim_item I
                ON ( I.item_no = A.item_no  ) 
              LEFT OUTER JOIN  dwh_wfs_foundation.fnd_wfs_oneapp_gateway	G 
                ON (G.oneapp_api_no = A.oneapp_api_no )
   ) new_rec 

   ON (
        new_rec.oneapp_api_no = tgt.oneapp_api_no AND
        new_rec.oneapp_gateway_no = tgt.oneapp_gateway_no and 
        new_rec.api_event_create_trunc_dt = tgt.api_event_create_trunc_dt 
 	    )
   WHEN MATCHED THEN UPDATE 
    SET

--         tgt.oneapp_api_no = new_rec.oneapp_api_no ,
--         tgt.oneapp_gateway_no = new_rec.oneapp_gateway_no ,
         tgt.oneapp_subscriber_key = new_rec.oneapp_subscriber_key ,
         tgt.atg_customer_no = new_rec.atg_customer_no ,
         tgt.customer_no = new_rec.customer_no ,
         tgt.api_event_create_date = new_rec.api_event_create_date ,
--         tgt.api_event_create_trunc_dt = new_rec.api_event_create_trunc_dt ,
         tgt.device_os = new_rec.device_os ,
         tgt.device_os_version = new_rec.device_os_version ,
         tgt.user_device_interface = new_rec.user_device_interface ,
         tgt.user_device_interface_version = new_rec.user_device_interface_version ,
         tgt.device_model = new_rec.device_model ,
         tgt.device_version = new_rec.device_version ,
         tgt.mobile_network = new_rec.mobile_network ,
         tgt.api_event_request = new_rec.api_event_request ,
         tgt.api_event_status = new_rec.api_event_status ,
         tgt.gateway_event_create_date = new_rec.gateway_event_create_date ,
         tgt.gateway_event_request = new_rec.gateway_event_request ,
         tgt.product_group_no = new_rec.product_group_no ,
         tgt.product_name = new_rec.product_name ,
         tgt.product_event_status = new_rec.product_event_status ,
         tgt.item_ind = new_rec.item_ind ,
         tgt.item_no = new_rec.item_no
         ,tgt.last_updated_date        = TRUNC(g_date)

    WHEN NOT MATCHED THEN INSERT (

         tgt.oneapp_api_no ,
         tgt.oneapp_gateway_no ,
         tgt.oneapp_subscriber_key ,
         tgt.atg_customer_no ,
         tgt.customer_no ,
         tgt.api_event_create_date ,
         tgt.api_event_create_trunc_dt ,
         tgt.device_os ,
         tgt.device_os_version ,
         tgt.user_device_interface ,
         tgt.user_device_interface_version ,
         tgt.device_model ,
         tgt.device_version ,
         tgt.mobile_network ,
         tgt.api_event_request ,
         tgt.api_event_status ,
         tgt.gateway_event_create_date ,
         tgt.gateway_event_request ,
         tgt.product_group_no ,
         tgt.product_name ,
         tgt.product_event_status ,
         tgt.item_ind ,
         tgt.item_no
        ,tgt.last_updated_date
           ) 

    VALUES (

         new_rec.oneapp_api_no ,
         new_rec.oneapp_gateway_no ,
         new_rec.oneapp_subscriber_key ,
         new_rec.atg_customer_no ,
         new_rec.customer_no ,
         new_rec.api_event_create_date ,
         new_rec.api_event_create_trunc_dt ,
         new_rec.device_os ,
         new_rec.device_os_version ,
         new_rec.user_device_interface ,
         new_rec.user_device_interface_version ,
         new_rec.device_model ,
         new_rec.device_version ,
         new_rec.mobile_network ,
         new_rec.api_event_request ,
         new_rec.api_event_status ,
         new_rec.gateway_event_create_date ,
         new_rec.gateway_event_request ,
         new_rec.product_group_no ,
         new_rec.product_name ,
         new_rec.product_event_status ,
         new_rec.item_ind ,
         new_rec.item_no
            , TRUNC(g_date)

           ) 
   ;

   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
--      l_text :=  l_description||' - day_activities_load sub proc fails';
      l_text :=  'ONEAPP - DAY_ACTIVITIES_LOAD sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
--      raise;

END day_activities_load;


PROCEDURE activities_load AS

BEGIN


   g_source_rows_chk := NULL;


   -- determine start and end days for load .

   SELECT /*+ parallel(s,4) */  
      TRUNC(MAX(api_event_create_date))                -- most recent login
        INTO g_date_done
   FROM dwh_wfs_performance.wfs_oneapp_activity_trail S;
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

       l_text      := 'No new data available in FND_WFS_ONEAPP_API.'; 
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');

   END IF;

   SELECT  /*+ parallel(a,4)  */ 
    TRUNC(Max(A.api_event_create_date))            -- to end with latest source date
      INTO g_date_end
   FROM dwh_wfs_foundation.fnd_wfs_oneapp_api	A;

   g_date_to_do := g_date_done;                     -- reprocess last day in case more added
   g_date_end := nvl(g_date_end, g_date);
   g_date_end := greatest (g_date_to_do, g_date_end);  

   l_text :=  'Processing from day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  to '||to_char(g_date_end, 'YYYY-MM-DD');
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   -- start processing for each day.

   WHILE g_date_to_do <= g_date_end AND g_success 
   LOOP


      -- ****** main load *****************************
      day_activities_load(g_date_to_do);
      -- **********************************************


      g_recs_cnt_day  :=  SQL%rowcount;
      g_recs_read     :=  g_recs_read + g_recs_cnt_day;
      g_recs_inserted :=  g_recs_inserted + g_recs_cnt_day;

      COMMIT;  -- NB. write_log already does a commit !

      l_text :=  'For day '||to_char(g_date_to_do, 'YYYY-MM-DD')||'  Merged:  '||g_recs_cnt_day;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


      g_date_to_do := g_date_to_do +1;

   END LOOP;



EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  'ONEAPP - ACTIVITIES_LOAD sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
--      raise;


END activities_load;





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

    g_check_date := g_date;

    l_text := 'LOAD TABLE: '||'WFS_ONEAPP_ACTIVITY_TRAIL' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    -- ****** main load *************
    activities_load;
    -- ******************************


    IF g_success THEN

      l_text :=  g_job_desc||'  - complete'; 
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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




END wh_prf_wfs_460u;
