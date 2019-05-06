--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_478U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_478U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- Description: Call Data - Load WFS Call Verint Activity data
  --
  --
  -- Date:        2018-09-28
  -- Author:      N Dlamini
  -- Purpose:     update table WFS_CALL_VERINT_ACTIVITY in the Performance layer
  --
  -- Tables:
  --              Input  -
  --                       fnd_wfs_call_agent_actvty
  --                       FND_WFS_CALL_EVALUATION
  --                       FND_WFS__CALL_AGENT_SKILL,   *** For future dev
  --                       FND_WFS_CALL_ERROR
  --
  --              Output - WFS_CALL_VERINT_ACTIVITY
  --                       dim_wfs_agent_activity
  --                       dim_wfs_agent
  
  --              Dependency on  -   none
  -- Packages:    constants, dwh_log
  --
  -- Maintenance:
  --  2018-09-28 N Chauhan - created.
  --  2018-10-05 N Dlamini Added three procedures that load Dimensions that are used by this table.
  --  2018-11-08 N Dlamini Fixed table name for logging.
  --  2018-11-12 N Dlamini Fixed unique key violation on dim_wfs_agent
  --  2018-11-15 N Dlamini Fixed loading of dim_wfs_agent for re-runnabilty 
  --  2018-11-15 N Dlamini Join on full PK in merge of the WFS_CALL_VERINT_ACTIVITY
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
  l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_WFS_478U';
  l_name sys_dwh_log.log_name%TYPE                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%TYPE       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%TYPE       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%TYPE := l_module_name;
  l_text sys_dwh_log.log_text%TYPE ;
  l_description sys_dwh_log_summary.log_description%TYPE   := 'LOAD WFS CALL VERINT ACTIVITY DATA';
  l_process_type sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;
  g_job_desc                     VARCHAR2(200)                                 := 'Load WFS Call Verint Activity data';
  g_success                      BOOLEAN                                       := TRUE;
  g_source_rows_chk              INTEGER                                       :=0;
  g_analysed_count               INTEGER                                       :=0;
  g_analysed_success             BOOLEAN                                       := FALSE;
  g_check_date                   DATE                                          := TRUNC(sysdate-1);
  g_date_to_do                   DATE                                          := TRUNC(sysdate-1);
  g_date_done                    DATE                                          := TRUNC(sysdate-7);
  g_date_end                     DATE                                          := TRUNC(sysdate-1);
  g_recs_cnt_day                 NUMBER                                        := 0;
  g_recs_agent_activity_inserted NUMBER                                        := 0;
  g_recs_agents_inserted         NUMBER                                        := 0;
  g_recs_form_comp_inserted      NUMBER                                        := 0;
  g_recs_agent_skills_inserted   NUMBER                                        := 0;
  /* load new data 1 existing  */




PROCEDURE load_from_hist_day(
      p_day_to_do DATE)
  AS
  BEGIN
    MERGE
    /*+ append  */
    INTO dwh_wfs_performance.wfs_call_verint_activity tgt USING
    ( WITH dataset_1 AS
    (SELECT DISTINCT
      /*+ parallel(act,4) full(act) */
      act.activity_id ,
      act.activity_type,
      act.activity_starttime ,
      act.activity_endtime ,
      act.exception_id ,
      act.overtime_ind ,
      act.handset_login,
      CASE
        WHEN act.activity_name IN ('Sick','AWOL','Compassionate Leave','FRL','Intraday End Shift Early','Emergency Leave','Emergency Unpaid Leave','Unpaid Sick')
        THEN 1
        ELSE 0
      END AS absenteeism_ind ,
      CASE
        WHEN act.activity_name IN ('121', 'Annual Leave', 'AWOL', 'Coaching', 'Compassionate Leave', 'CSI', 'Emergency Leave', 'Emergency Unpaid Leave', 'FRL', 'General Absence', 'Grand Parade', 'Incentive Time', 'Intraday End Shift Early', 'Late', 'Learnership', 'Long Term Sick', 'LTM', 'LTW', 'Maternity Leave', 'Medical Apointment', 'Meeting', 'Moving Day', 'Paternity Leave', 'Public Holiday', 'QA Feedback', 'Resignation', 'Secondment', 'Sick', 'Study Leave', 'Suspended', 'Team Meeting', 'TL Instruction', 'Training', 'UAT', 'Unpaid Leave', 'Unpaid Sick', 'Unplanned Learnership', 'Unplanned Leave', 'Unplanned LTM', 'Unplanned Maternity', 'Unplanned Study Leave', 'Unplanned Training', 'Unplanned UAT', 'Vacation', 'Unplanned Sick Leave', 'Unplanned FRL' )
        THEN 1
        ELSE 0
      END AS shrinkage_ind,
      CASE
        WHEN act.activity_name IN ('CSI','Annual Leave','Grand Parade','Incentive Time','Learnership','Long Term Sick','LTM','LTW','Maternity Leave','Medical Apointment','Moving Day','Paternity Leave','Public Holiday','Secondment','Study Leave','Unpaid Leave')
        THEN 1
        ELSE 0
      END AS planned_shrinkage_ind,
      CASE
        WHEN act.activity_name IN ('Resignation','Unplanned leave','Unplanned Maternity','Suspended','Unplanned Learnership','Unplanned leave','Unplanned UAT','Unplanned Study Leave','Unplanned Sick Leave','Unplanned FRL')
        THEN 1
        ELSE 0
      END    AS unplanned_shrinkage_ind,
      g_date AS last_updated_date
    FROM dwh_wfs_foundation.fnd_wfs_call_agent_actvty act
    WHERE act.activity_starttime                               >= p_day_to_do
    AND act.activity_starttime                                  < p_day_to_do + 1
    AND act.activity_id                                        <> '-4001'
    AND (act.activity_endtime-act.activity_starttime)*24*60*60 <> 0
    )
  SELECT /*+ parallel(e,4) full(e) */
    * FROM dataset_1 E
  
    ) new_rec 
    ON ( new_rec.activity_id        = tgt.activity_id AND 
         new_rec.activity_starttime = tgt.activity_starttime AND 
         new_rec.handset_login      = tgt.handset_login AND 
         new_rec.activity_endtime   = tgt.activity_endtime AND
         new_rec.activity_type      = tgt.activity_type )


  WHEN MATCHED THEN
    UPDATE
    SET 
      tgt.exception_id            = new_rec.exception_id,
      tgt.overtime_ind            = new_rec.overtime_ind,
      tgt.absenteeism_ind         = new_rec.absenteeism_ind,
      tgt.shrinkage_ind           = new_rec.shrinkage_ind,
      tgt.planned_shrinkage_ind   = new_rec.planned_shrinkage_ind,
      tgt.unplanned_shrinkage_ind = new_rec.unplanned_shrinkage_ind,
      tgt.last_updated_date       = new_rec.last_updated_date 
  WHEN NOT MATCHED THEN
    INSERT
      (
        tgt.activity_starttime ,
        tgt.activity_endtime ,
        tgt.activity_id ,
        tgt.activity_type,
        tgt.handset_login ,
        tgt.exception_id ,
        tgt.overtime_ind ,
        tgt.absenteeism_ind ,
        tgt.shrinkage_ind ,
        tgt.planned_shrinkage_ind ,
        tgt.unplanned_shrinkage_ind ,
        tgt.last_updated_date
      )
      VALUES
      (
        new_rec.activity_starttime ,
        new_rec.activity_endtime ,
        new_rec.activity_id ,
        new_rec.activity_type,
        new_rec.handset_login ,
        new_rec.exception_id ,
        new_rec.overtime_ind ,
        new_rec.absenteeism_ind ,
        new_rec.shrinkage_ind ,
        new_rec.planned_shrinkage_ind ,
        new_rec.unplanned_shrinkage_ind ,
        new_rec.last_updated_date
      ) ;
    g_success := TRUE;

EXCEPTION

  WHEN OTHERS THEN
    ROLLBACK;
    --      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
    l_text := l_description||' - LOAD_FROM_HIST_DAY sub proc fails';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
    g_success := FALSE;
    RAISE;

END load_from_hist_day;


PROCEDURE upd_dim_wfs_agent_activity
    (
      p_day_to_do IN DATE,
      g_success OUT BOOLEAN
    )
  AS
  BEGIN
    MERGE
    /*+ append */
    INTO dwh_wfs_performance.dim_wfs_agent_activity tgt USING
    ( -- get all new and changes
      SELECT
        /*+ parallel (act,4)   full(act)  */
        DISTINCT act.activity_id ,
        act.activity_name ,
        TRUNC(g_date)        AS last_updated_date
      FROM dwh_wfs_foundation.fnd_wfs_call_agent_actvty act
      LEFT OUTER JOIN dwh_wfs_performance.dim_wfs_agent_activity exst
      ON ( act.activity_id          = exst.activity_id)
      WHERE act.activity_starttime >= p_day_to_do
      and not exists (select ACTIVITY_ID from dwh_wfs_performance.dim_wfs_agent_activity  a
                        where a.ACTIVITY_ID = act.ACTIVITY_ID)
      AND act.activity_starttime    < p_day_to_do + 1
      AND ( -- only if a change in any value
         NVL(act.activity_name, 0)      <> NVL(exst.activity_name, 0))
    )
    rec_to_ins_or_upd ON ( rec_to_ins_or_upd.activity_id = tgt.activity_id )
  WHEN MATCHED THEN
    UPDATE
    SET tgt.activity_name         = rec_to_ins_or_upd.activity_name ,
      tgt.last_updated_date       = rec_to_ins_or_upd.last_updated_date 
  WHEN NOT MATCHED THEN
    INSERT
      (
        tgt.activity_id ,
        tgt.activity_name ,
        tgt.last_updated_date
      )
      VALUES
      (
        rec_to_ins_or_upd.activity_id ,
        rec_to_ins_or_upd.activity_name ,
        rec_to_ins_or_upd.last_updated_date
      ) ;
    g_recs_agent_activity_inserted := g_recs_agent_activity_inserted + SQL%rowcount;
    COMMIT;
  EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    --      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
    l_text := 'UPD_DIM_WFS_AGENT_ACTIVITY sub proc fails';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
    g_success := FALSE;
    RAISE;
END upd_dim_wfs_agent_activity;


PROCEDURE upd_dim_wfs_agents
    (
      p_day_to_do IN DATE,
      g_success OUT BOOLEAN
    )
  AS
  BEGIN
    MERGE
/*+ append parallel(tgt,4) */
INTO dwh_wfs_performance.dim_wfs_agent tgt USING

( WITH
  -- get record list of existing emp Id's, including both, active and inactive
  existing_agents AS

  (SELECT *
    /*+ parallel(exst,4) full(exst) */
  FROM dwh_wfs_performance.dim_wfs_agent exst
  WHERE agent_record_active_to_date > p_day_to_do
  ),

  -- isolate new foundation records

    -- this subquery to get the "latest" record for each agent, to eliminate duplicates
    latest_activity as (
    select 
    EMPLOYEE_ID,
    max (ACTIVITY_STARTTIME) ACTIVITY_STARTTIME, count(*)
    from dwh_wfs_foundation.fnd_wfs_call_agent_actvty
    
    where 
          activity_starttime >= p_day_to_do and
          activity_starttime  < p_day_to_do + 1 
    
    group by 
     EMPLOYEE_ID
    ),
    
  latest_updates as(
    
   select
    distinct
    f.EMPLOYEE_ID
    ,f.STAFF_NUMBER
    ,f.HANDSET_LOGIN
    ,f.AGENT_NAME
    ,f.WFS_ORGANIZATION
    ,f.EMPLOYMENT_TYPE
    ,f.SUPERVISOR_NAME
    ,f.agent_termination_date
   from dwh_wfs_foundation.fnd_wfs_call_agent_actvty f,
    latest_activity l
   where 
    l.EMPLOYEE_ID = f.EMPLOYEE_ID and
    l.ACTIVITY_STARTTIME = f.ACTIVITY_STARTTIME and
    f.activity_starttime >= p_day_to_do and
    f.activity_starttime  < p_day_to_do + 1
    
  ),



  -- isolate new agents that do not exist on the dimension
  new_agents AS
  (SELECT
    /*+ parallel(fnd,4) full(fnd) parallel(exst,4) full(exst) */
    DISTINCT fnd.employee_id,
    fnd.staff_number,
    p_day_to_do+1                              AS agent_record_active_from_date , -- batchdate plus one - to align with JOB_REC_EFFECTIVE_DATE
    to_date('01/jan/3000', 'DD/mon/YYYY') AS agent_record_active_to_date ,         -- future date to indicate as the current valid record
    fnd.agent_name,
    fnd.handset_login,
    fnd.supervisor_name,
    fnd.employment_type,
    fnd.agent_termination_date,
    fnd.wfs_organization
  FROM latest_updates fnd
  LEFT OUTER JOIN existing_agents exst
  ON ( fnd.employee_id   = exst.employee_id 
  AND  fnd.handset_login = exst.handset_login)
  WHERE exst.employee_id IS NULL
  ),

  --get all the agents that have new changes
  updates_with_changes AS
  (SELECT
    /*+ parallel(fnd,4) full(fnd) parallel(exst,4) full(exst) */
    DISTINCT fnd.employee_id ,
    fnd.staff_number,
    p_day_to_do+1                              AS agent_record_active_from_date , -- batchdate plus one - to align with JOB_REC_EFFECTIVE_DATE
    to_date('01/jan/3000', 'DD/mon/YYYY') AS agent_record_active_to_date ,   -- future date to indicate as the current valid record
    fnd.agent_name,
    fnd.handset_login,
    fnd.supervisor_name,
    fnd.employment_type,
    fnd.agent_termination_date,
    fnd.wfs_organization
  FROM latest_updates fnd,
    existing_agents exst
  WHERE fnd.employee_id   = exst.employee_id
   AND  fnd.handset_login = exst.handset_login
  AND    ( 
     NVL(fnd.agent_name,0)                         <> NVL(exst.agent_name,0)
  OR NVL(fnd.supervisor_name,0)                    <> NVL(exst.supervisor_name,0)
  OR NVL(fnd.employment_type,0)                    <> NVL(exst.employment_type,0)
  OR NVL(fnd.staff_number,0)                       <> NVL(exst.staff_number,0)
  OR NVL(fnd.agent_termination_date,'01/jan/3000') <> NVL(exst.agent_termination_date,'01/jan/3000')
  OR NVL(fnd.wfs_organization,0)                   <> NVL(exst.wfs_organization,0) )

  and agent_record_active_from_date < p_day_to_do  -- for re-runability  - ignore emps with active_from >= p_day_to_do

  )


-- new agents
SELECT * FROM new_agents 
UNION
-- updates to be added as new records
SELECT * FROM updates_with_changes
UNION
-- existing records to be closed off
SELECT
  /*+ parallel(fnd,4) full(fnd) parallel(exst,4) full(exst) */
  exst.employee_id ,
  exst.staff_number,
  exst.agent_record_active_from_date ,
  p_day_to_do AS agent_record_active_to_date , -- batchdate  - a day before opening day of new record
  exst.agent_name ,
  exst.handset_login ,
  exst.supervisor_name ,
  exst.employment_type ,
  exst.agent_termination_date ,
  exst.wfs_organization
FROM updates_with_changes fnd
INNER JOIN existing_agents exst
ON ( fnd.employee_id   = exst.employee_id 
AND  fnd.handset_login = exst.handset_login)
) rec_to_ins_or_upd 

ON ( rec_to_ins_or_upd.employee_id = tgt.employee_id AND rec_to_ins_or_upd.agent_record_active_from_date = tgt.agent_record_active_from_date AND rec_to_ins_or_upd.handset_login = tgt.handset_login) -- to get match for updating/closing off existing valid record

WHEN MATCHED THEN
  UPDATE
  SET agent_record_active_to_date = p_day_to_do -- close off with a day before active_to date of new record
    ,
    tgt.last_updated_date = TRUNC(g_date) 

WHEN NOT MATCHED THEN
  INSERT
    (
      employee_id ,
      staff_number,
      agent_record_active_from_date ,
      agent_record_active_to_date ,
      agent_name ,
      handset_login ,
      supervisor_name ,
      employment_type ,
      agent_termination_date ,
      wfs_organization,
      last_updated_date
    )
    VALUES
    (
      rec_to_ins_or_upd.employee_id ,
      rec_to_ins_or_upd.staff_number ,
      rec_to_ins_or_upd.agent_record_active_from_date ,
      rec_to_ins_or_upd.agent_record_active_to_date ,
      rec_to_ins_or_upd.agent_name ,
      rec_to_ins_or_upd.handset_login ,
      rec_to_ins_or_upd.supervisor_name ,
      rec_to_ins_or_upd.employment_type ,
      rec_to_ins_or_upd.agent_termination_date ,
      rec_to_ins_or_upd.wfs_organization,
      TRUNC(g_date)
    ) ;
    g_recs_agents_inserted := g_recs_agents_inserted + SQL%rowcount;
    COMMIT;


  EXCEPTION

    WHEN OTHERS THEN
        ROLLBACK;
        --      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
        l_text := 'UPD_DIM_WFS_AGENTS sub proc fails';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
        dwh_log.record_error(l_module_name,SQLCODE,l_message);
        g_success := FALSE;
        RAISE;

END upd_dim_wfs_agents;



--PROCEDURE upd_dim_wfs_service_skills(
--    p_day_to_do IN DATE,
--    g_success OUT BOOLEAN )
--AS
--BEGIN
--  MERGE
--  /*+ append  */
--  INTO dwh_wfs_performance.dim_wfs_service_skills tgt USING
--  ( WITH latest_service AS
--  (SELECT DISTINCT 
--    service_id,
--    service_skill,
--    MAX(tran_log_date) tran_log_date
--  FROM dwh_wfs_foundation.fnd_wfs_call_agent_skill
--  GROUP BY service_id,
--           service_skill
--  )
--SELECT DISTINCT 
--  sk.service_id,
--  sk.service_name,
--  sk.service_skill,
--  TRUNC(g_date) AS last_updated_date
--FROM dwh_wfs_foundation.fnd_wfs_call_agent_skill sk
--INNER JOIN latest_service latest
--ON sk.service_id     = latest.service_id
--AND sk.tran_log_date = latest.tran_log_date
--LEFT JOIN dwh_wfs_performance.dim_wfs_service_skills exst
--ON ( sk.service_id      = exst.service_id)
--WHERE sk.tran_log_date >= p_day_to_do
--AND sk.tran_log_date    < p_day_to_do + 1
--AND ( -- only if a change in any value
--  NVL(sk.service_name, 0)                              <> NVL(exst.service_name, 0)
--OR NVL(sk.service_skill, 0)                            <> NVL(exst.service_skill, 0))
--  ) rec_to_ins_or_upd ON ( rec_to_ins_or_upd.service_id = tgt.service_id )
--WHEN MATCHED THEN
--  UPDATE
--  SET tgt.service_name    = rec_to_ins_or_upd.service_name ,
--    tgt.service_skill     = rec_to_ins_or_upd.service_skill ,
--    tgt.last_updated_date = rec_to_ins_or_upd.last_updated_date WHEN NOT MATCHED THEN
--  INSERT
--    (
--      tgt.service_id ,
--      tgt.service_name ,
--      tgt.service_skill ,
--      tgt.last_updated_date
--    )
--    VALUES
--    (
--      rec_to_ins_or_upd.service_id ,
--      rec_to_ins_or_upd.service_name ,
--      rec_to_ins_or_upd.service_skill ,
--      rec_to_ins_or_upd.last_updated_date
--    ) ;
--  g_recs_agents_inserted := g_recs_agents_inserted + SQL%rowcount;
--  COMMIT;
--  
--EXCEPTION
--
--    WHEN OTHERS THEN
--      ROLLBACK;
--      --      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
--      l_text := 'DIM_WFS_SERVICE_SKILLS_new_add sub proc fails';
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
--      dwh_log.record_error(l_module_name,SQLCODE,l_message);
--      g_success := FALSE;
--      RAISE;
--  
--END upd_dim_wfs_service_skills;



PROCEDURE LOAD
  AS
  BEGIN
    g_source_rows_chk := NULL;
    SELECT
      /*+ parallel(s,4) */
      TRUNC(MAX( activity_starttime )) -- most recent login
    INTO g_date_done
    FROM dwh_wfs_performance.wfs_call_verint_activity S;
    IF g_date_done IS NOT NULL THEN -- most recent login retrieved
      SELECT
        /*+ parallel(t,4) full(t) */
        COUNT(*) cnt -- check if there is source data since most recent login
      INTO g_source_rows_chk
      FROM dwh_wfs_foundation.fnd_wfs_call_agent_actvty T
      WHERE T.activity_starttime > g_date_done + 1 -- plus 1 'cos g_date_done is trunc'd
      AND ROWNUM                 < 100;            -- for performance  - no need to count all
    ELSE                                           -- target table was empty
      SELECT
        /*+ parallel(a,4)  */
        TRUNC(MIN(A.activity_starttime)) -- to start with earliest source date
      INTO g_date_done
      FROM dwh_wfs_foundation.fnd_wfs_call_agent_actvty A;
    END IF;
    IF g_date_done        IS NULL   -- source table empty
      OR g_source_rows_chk = 0 THEN -- no new source data
      g_date_done         := TRUNC(g_date);
      l_text              := 'Latest data not available in fnd_wfs_call_agent_actvty.';
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
    END IF;
    SELECT
      /*+ parallel(a,4)  */
      TRUNC(MAX(A.activity_starttime)) -- to end with latest source date
    INTO g_date_end
    FROM dwh_wfs_foundation.fnd_wfs_call_agent_actvty A;
    --dwh_wfs_performance.wfs_call_verint_activity A;
    g_date_to_do := g_date_done; -- reprocess last day in case more added
    g_date_end   := NVL(g_date_end, g_date);
    l_text       := 'Processing from day '||TO_CHAR(g_date_to_do, 'YYYY-MM-DD')||'  to '||TO_CHAR(g_date_end, 'YYYY-MM-DD');
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    WHILE g_date_to_do <= g_date_end AND g_success
    LOOP
      --  update dims with any new values ****************
      upd_dim_wfs_agent_activity(g_date_to_do, g_success);
      --      upd_dim_agent_skills(g_date_to_do, g_success);
      upd_dim_wfs_agents(g_date_to_do, g_success);
      --      upd_dim_evaluation_form_comp(g_date_to_do, g_success);
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
  l_text := 'WFS_CALL_VERINT_ACTIVITY load STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  EXECUTE IMMEDIATE 'alter session enable parallel dml';
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD TABLE: WFS_CALL_VERINT_ACTIVITY' ;
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

  l_text := 'DIM_WFS_AGENT_ACTIVITY RECORDS MERGED  '||g_recs_agent_activity_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text := 'DIM_WFS_AGENT RECORDS MERGED  '||g_recs_agents_inserted;
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


END wh_prf_wfs_478u;
