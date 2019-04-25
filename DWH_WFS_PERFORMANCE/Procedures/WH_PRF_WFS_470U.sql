--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_470U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_470U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- Description: Evaluation Call Data - Load WFS Call Evaluation Data
  --
  --
  -- Date:        2018-10-30
  -- Author:      Nhlaka Dlamini
  -- Purpose:     update table wfs_call_evaluation in the Performance layer
  --
  -- Tables:
  --              Input  -
  --                       FND_WFS_CALL_EVALUATION
  --                       FND_WFS_CALL_ERROR
  --
  --
  --              Output - wfs_call_verint_evaluation
  --                       dim_wfs_call_evaluation
  --
  --              Dependency on  -   none
  -- Packages:    constants, dwh_log
  --
  -- Maintenance:
  --  2018-10-30 N Dlamini - created.
  --  2018-11-08 N Dlamini - Loggging tidied up.
  --  2018-11-08 N Dlamini - Table checks for start date fixed.
  --  2018-12-21 N Dlamini - Added a fix on the Evaluation scores calculation to match Verint Source data
  --  2018-12-23 N Dlamini - Changed the Proc to load using EVALUATION_DATE instead of CONTACT_START_TIME to cater for the manually incorrectly captured CONTACT_START_TIME 
  --  2019-03-11 N Dlamini - Correct the Keys on the join clause to reflect EVALUATION_DATE instead of CONTACT_START_TIME to syncronise with change to the source extract.
  --  
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
  
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_recs_deleted  INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_count         NUMBER  := 0;
  g_date          DATE    := TRUNC(sysdate);
  g_year_no       INTEGER := 0;
  g_month_no      INTEGER := 0;
  l_message sys_dwh_errlog.log_text%TYPE;
  l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_WFS_470U';
  l_name sys_dwh_log.log_name%TYPE                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%TYPE       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%TYPE       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%TYPE := l_module_name;
  l_text sys_dwh_log.log_text%TYPE ;
  l_description sys_dwh_log_summary.log_description%TYPE   := 'CALL EVALUATION DATA - LOAD WFS CALL EVALUATION PERFORMANCE';
  l_process_type sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;
  g_job_desc                   VARCHAR2(200)                                 := 'Load WFS Call Evaluation Data';
  g_success                    BOOLEAN                                       := TRUE;
  g_source_rows_chk            INTEGER                                       :=0;
  g_analysed_count             INTEGER                                       :=0;
  g_analysed_success           BOOLEAN                                       := FALSE;
  g_check_date                 DATE                                          := TRUNC(sysdate-1);
  g_date_to_do                 DATE                                          := TRUNC(sysdate-1);
  g_date_done                  DATE                                          := TRUNC(sysdate-7);
  g_date_end                   DATE                                          := TRUNC(sysdate-1);
  g_recs_cnt_day               NUMBER                                        := 0;
  g_recs_call_eval_performance NUMBER                                        := 0;
  g_recs_dim_eval              NUMBER                                        := 0;





  PROCEDURE load_from_hist_day(
      p_day_to_do DATE)
  AS
  BEGIN

    MERGE
    /*+ append  */
    INTO dwh_wfs_performance.wfs_call_verint_evaluation tgt USING
    (WITH 
         autofail_evaluations 
          AS
          (SELECT DISTINCT 
                  evaluation_key,
                  ROUND(SUM(evaluation_score)/SUM(max_possible_score)*100,2) evaluation_score
           FROM
           (SELECT evaluation_key,
                   evaluation_form,
                   category_name,
                   evaluation_score,
                   max_possible_score,
                   row_number() over (partition BY evaluation_key order by evaluation_score ASC nulls last, category_name)rn,
                   CASE
                      WHEN row_number() over (partition BY evaluation_key order by evaluation_score ASC nulls last, category_name) = 1
                      AND category_name                                                                                            = 'Compliance'
                      AND evaluation_form                                                                                         IN ('(QA) POI Audits 112018','(QA) POI Audit 112018 Training Sheet', '(QA) POI Audits 052018','(QA) POI Audit 052018 Training Sheet', '(QA) Telesales FSB','(QA) Acquisitions FSB','(QA) Acquisitions training FSB', '(QA) ACLI 112017','(QA) ACLI 032018')
                      AND evaluation_score                                                                                         = 0
                      THEN 0
                  END AS overall_score
          FROM dwh_wfs_foundation.fnd_wfs_call_error det)
          WHERE overall_score = 0
             AND evaluation_form IS NOT NULL
          GROUP BY evaluation_key),

         correct_evaluations 
          AS
          (SELECT DISTINCT 
                  evaluation_key,
                  ROUND(SUM(evaluation_score)/SUM(max_possible_score)*100,2) evaluation_score
           FROM
            (SELECT evaluation_key,
                    evaluation_form,
                    category_name,
                    evaluation_score,
                    max_possible_score,
                    row_number() over (partition BY evaluation_key order by evaluation_score ASC nulls last, category_name)rn,
                    CASE
                      WHEN row_number() over (partition BY evaluation_key order by evaluation_score ASC nulls last, category_name) = 1
                      AND category_name                                                                                            = 'Compliance'
                      AND evaluation_form                                                                                         IN ('(QA) POI Audits 112018','(QA) POI Audit 112018 Training Sheet', '(QA) POI Audits 052018','(QA) POI Audit 052018 Training Sheet', '(QA) Telesales FSB','(QA) Acquisitions FSB','(QA) Acquisitions training FSB', '(QA) ACLI 112017','(QA) ACLI 032018')
                      AND evaluation_score                                                                                         = 0
                      THEN 0
                    END AS overall_score
             FROM dwh_wfs_foundation.fnd_wfs_call_error det)
             WHERE overall_score    IS NULL
               AND evaluation_form IS NOT NULL
               AND evaluation_key NOT IN (SELECT DISTINCT evaluation_key FROM autofail_evaluations)
           GROUP BY evaluation_key),

          all_evalution_scores 
            AS
            (SELECT * FROM autofail_evaluations
                  UNION ALL
             SELECT * FROM correct_evaluations)

            SELECT  /*+ parallel(ev,4) full(ev) */
                DISTINCT ev.evaluation_key,
                         ev.call_id,
                         ev.contact_start_time,
                         ev.evaluation_date,
                         ev.handset_login,
                         ev.customer_line_identification,
                         er.evaluation_score,
                         sysdate AS last_updated_date
            FROM dwh_wfs_foundation.fnd_wfs_call_evaluation ev
            LEFT OUTER JOIN 
                 all_evalution_scores er
            ON 
                ev.evaluation_key       = er.evaluation_key
           WHERE ev.call_id          IS NOT NULL  -- Fix to ignore null values, as this is a Key on the Perofmance table N. Dlamini 07/11/2018
             AND ev.evaluation_date >= p_day_to_do
             AND ev.evaluation_date  < p_day_to_do + 1
    ) new_rec 
    ON 
    ( 
      new_rec.evaluation_key = tgt.evaluation_key AND new_rec.call_id = tgt.call_id AND new_rec.evaluation_date = tgt.evaluation_date
    )

  WHEN MATCHED THEN

    UPDATE
    SET 
      tgt.contact_start_time           = new_rec.contact_start_time,
      tgt.handset_login                = new_rec.handset_login,
      tgt.customer_line_identification = new_rec.customer_line_identification,
      tgt.evaluation_score             = new_rec.evaluation_score,
      tgt.last_updated_date            = TRUNC(g_date) 

    WHEN NOT MATCHED THEN

    INSERT
      (
        tgt.evaluation_key,
        tgt.call_id,
        tgt.contact_start_time,
        tgt.evaluation_date,
        tgt.handset_login,
        tgt.customer_line_identification,
        tgt.evaluation_score,
        tgt.last_updated_date
      )
      VALUES
      (
        new_rec.evaluation_key,
        new_rec.call_id,
        new_rec.contact_start_time,
        new_rec.evaluation_date,
        new_rec.handset_login,
        new_rec.customer_line_identification,
        new_rec.evaluation_score,
        TRUNC(g_date)
      ) ;
    g_success := TRUE;

  EXCEPTION

      WHEN OTHERS THEN
        ROLLBACK;

        l_text := l_description||' - LOAD_FROM_HIST_DAY sub proc fails';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
        dwh_log.record_error(l_module_name,SQLCODE,l_message);
        g_success := FALSE;
        RAISE;

  END load_from_hist_day;


  PROCEDURE upd_dim_wfs_call_evaluation
    (
      p_day_to_do IN DATE,
      g_success OUT BOOLEAN
    )
  AS
  BEGIN

    MERGE
    /*+ append */
    INTO dwh_wfs_performance.dim_wfs_call_evaluation tgt USING
    (SELECT
        /*+ parallel(ev,4) full(ev) */
        DISTINCT ev.evaluation_key,
        ev.call_id,
        p_day_to_do+1                         AS eval_rec_active_from_date,
        to_date('01/jan/3000', 'DD/mon/YYYY') AS eval_rec_active_to_date,
        ev.agent_id,
        ev.evaluator_name,
        ev.evaluation_form,
        ev.handset_login,
        ev.agent_name,
        ev.supervisor_name,
        ev.wfs_business_unit,
        ev.wfs_organization,
        TRUNC(g_date) AS last_updated_date
      FROM dwh_wfs_foundation.fnd_wfs_call_evaluation ev
      LEFT OUTER JOIN dwh_wfs_performance.dim_wfs_call_evaluation exst
      ON (ev.evaluation_key = exst.evaluation_key
      AND ev.call_id   = exst.call_id)
      WHERE ev.call_id IS NOT NULL -- Temporary Fix to ignore null values, as this is a Key on the Perofmance table (N. Dlamini 07/11/2018)
        AND ev.evaluation_date                     >=  p_day_to_do
        AND ev.evaluation_date                      <  p_day_to_do + 1
        AND ( NVL(ev.agent_id,0)                       <> NVL(exst.agent_id,0)
        OR NVL(ev.evaluator_name,0)                    <> NVL(exst.evaluator_name,0)
        OR NVL(ev.evaluation_form,0)                   <> NVL(exst.evaluation_form,0)
        OR NVL(ev.handset_login,0)                     <> NVL(exst.handset_login,0)
        OR NVL(ev.agent_name,0)                        <> NVL(exst.agent_name,0)
        OR NVL(ev.supervisor_name,0)                   <> NVL(exst.supervisor_name,0)
        OR NVL(ev.wfs_business_unit,0)                 <> NVL(exst.wfs_business_unit,0)
        OR NVL(ev.wfs_organization,0)                  <> NVL(exst.wfs_organization,0) )
    )
    rec_to_ins_or_upd 
    ON ( rec_to_ins_or_upd.evaluation_key = tgt.evaluation_key AND rec_to_ins_or_upd.call_id = tgt.call_id AND rec_to_ins_or_upd.eval_rec_active_from_date = tgt.eval_rec_active_from_date )

  WHEN MATCHED THEN

    UPDATE
    SET tgt.eval_rec_active_to_date = p_day_to_do ,
      tgt.last_updated_date             = TRUNC(g_date) 

  WHEN NOT MATCHED THEN

    INSERT
      (
        evaluation_key,
        call_id,
        eval_rec_active_from_date,
        eval_rec_active_to_date,
        agent_id,
        evaluator_name,
        evaluation_form,
        handset_login,
        agent_name,
        supervisor_name,
        wfs_business_unit,
        wfs_organization,
        last_updated_date
      )
      VALUES
      (
        rec_to_ins_or_upd.evaluation_key,
        rec_to_ins_or_upd.call_id,
        rec_to_ins_or_upd.eval_rec_active_from_date,
        rec_to_ins_or_upd.eval_rec_active_to_date,
        rec_to_ins_or_upd.agent_id,
        rec_to_ins_or_upd.evaluator_name,
        rec_to_ins_or_upd.evaluation_form,
        rec_to_ins_or_upd.handset_login,
        rec_to_ins_or_upd.agent_name,
        rec_to_ins_or_upd.supervisor_name,
        rec_to_ins_or_upd.wfs_business_unit,
        rec_to_ins_or_upd.wfs_organization,
        TRUNC(g_date)
      ) ;
    g_recs_dim_eval := g_recs_dim_eval + SQL%rowcount;
    COMMIT;

  EXCEPTION

      WHEN OTHERS THEN
        ROLLBACK;
        --      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
        l_text := 'DIM_WFS_CALL_EVALUATION_new_add sub proc fails';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
        dwh_log.record_error(l_module_name,SQLCODE,l_message);
        g_success := FALSE;
        RAISE;


  END upd_dim_wfs_call_evaluation;



  PROCEDURE LOAD
  AS
  BEGIN

    g_source_rows_chk := NULL;
    SELECT
      /*+ parallel(s,4) */
      TRUNC(MAX(EVALUATION_DATE)) -- most recent login
    INTO g_date_done
    FROM dwh_wfs_performance.wfs_call_verint_evaluation s;
    IF g_date_done IS NOT NULL THEN -- most recent login retrieved
      SELECT
        /*+ parallel(t,4) full(t) */
        COUNT(*) cnt -- check if there is source data since most recent login
      INTO g_source_rows_chk
      FROM dwh_wfs_foundation.fnd_wfs_call_evaluation t
      WHERE T.EVALUATION_DATE > g_date_done + 1 -- plus 1 'cos g_date_done is trunc'd
      AND ROWNUM         < 100;            -- for performance  - no need to count all
    ELSE                                   -- target table was empty
      SELECT
        /*+ parallel(a,4)  */
        TRUNC(MIN(A.EVALUATION_DATE)) -- to start with earliest source date
      INTO g_date_done
      FROM dwh_wfs_foundation.fnd_wfs_call_evaluation a;
    END IF;
    IF g_date_done        IS NULL   -- source table empty
      OR g_source_rows_chk = 0 THEN -- no new source data
      g_date_done         := TRUNC(g_date);
      l_text              := 'Latest data not available in fnd_wfs_call_evaluation.';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    END IF;
    SELECT
      /*+ parallel(a,4)  */
      TRUNC(MAX(A.EVALUATION_DATE)) -- to end with latest source date
    INTO g_date_end
    FROM dwh_wfs_foundation.fnd_wfs_call_evaluation a;
    g_date_to_do := g_date_done; -- reprocess last day in case more added
    g_date_end   := NVL(g_date_end, g_date);
    l_text       := 'Processing from day '||TO_CHAR(g_date_to_do, 'YYYY-MM-DD')||'  to '||TO_CHAR(g_date_end, 'YYYY-MM-DD');
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    WHILE g_date_to_do <= g_date_end AND g_success
    LOOP
      --  update dims with any new values ****************
      upd_dim_wfs_call_evaluation(g_date_to_do, g_success);
      IF g_success = FALSE THEN
        RETURN;
      END IF;
      -- ****** daily load *****************************
      load_from_hist_day(g_date_to_do);
      -- **********************************************
      g_recs_cnt_day  :=SQL%rowcount;
      g_recs_read     := g_recs_read     + g_recs_cnt_day;
      g_recs_inserted := g_recs_inserted + g_recs_cnt_day;
      COMMIT; -- NB. write_log already does a commit !
      l_text := 'For day '||TO_CHAR(g_date_to_do, 'YYYY-MM-DD')||'  Merged:  '||g_recs_cnt_day;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      g_date_to_do := g_date_to_do +1;
    END LOOP;

    g_success := TRUE;

  EXCEPTION

      WHEN OTHERS THEN
        ROLLBACK;
        --      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
        l_text := 'LOAD sub proc fails';
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
    g_forall_limit  := p_forall_limit;
  END IF;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := FALSE;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'WFS_CALL_VERINT_EVALUATION load STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  EXECUTE IMMEDIATE 'alter session enable parallel dml';
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD TABLE: WFS_CALL_VERINT_EVALUATION' ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  -- ****** main load *************
  LOAD;
  -- ******************************

  COMMIT;

  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
  --    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
  --    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'RECORDS MERGED  '||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'DIM_WFS_CALL_EVALUATION RECORDS MERGED  '||g_recs_dim_eval;
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
    l_text := TO_CHAR(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc || '   - load for day '||TO_CHAR(g_date_to_do,'yyyy-mm-dd')||'  fails';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
    p_success := FALSE;
  END IF;

  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


EXCEPTION

    WHEN dwh_errors.e_insert_error THEN
      ROLLBACK;
      l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);
      dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
      p_success := FALSE;
      RAISE;

    WHEN OTHERS THEN
      ROLLBACK;
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);
      dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
      p_success := FALSE;
      RAISE;


END wh_prf_wfs_470u;
