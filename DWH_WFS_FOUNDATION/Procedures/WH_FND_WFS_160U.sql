--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_160U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_160U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  -- Date:        June 2015
  -- Author:      Jerome Appollis
  -- Purpose:     Create fnd_wfs_om4_workflow table in the foundation layer
  --              with input ex staging table from WFS.
  -- Tables:      Input  - stg_om4_itc_workflow_cpy
  --              Output - fnd_wfs_om4_workflow
  -- Packages:    constants, dwh_log, dwh_valid
  --
  -- Maintenance:
  --
  -- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
  --       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
  --       Updates however are also a lot faster that on the original template.
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_recs_read      INTEGER := 0;
  g_recs_updated   INTEGER := 0;
  g_recs_inserted  INTEGER := 0;
  g_recs_hospital  INTEGER := 0;
  g_recs_duplicate INTEGER := 0;
  g_truncate_count INTEGER := 0;
  
  g_workflow_history_id stg_om4_itc_workflow_cpy.workflow_history_id%type;
  
  
  g_date DATE := TRUNC(sysdate);
  
  L_MESSAGE SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_WFS_160U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  L_TEXT SYS_DWH_LOG.LOG_TEXT%TYPE ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD WFS ITC WORKFLOW DATA';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  CURSOR stg_dup
  IS
    SELECT *
    FROM stg_om4_itc_workflow_cpy
    WHERE (workflow_history_id) IN
      (SELECT workflow_history_id
      FROM stg_om4_itc_workflow_cpy
      GROUP BY workflow_history_id
      HAVING COUNT(*) > 1
      )
  ORDER BY workflow_history_id,
    sys_source_batch_id DESC ,
    sys_source_sequence_no DESC;
  CURSOR c_stg_wfs_om4_workflow_dly
  IS
    SELECT
      /*+ FULL(stg)  parallel (stg,2) */
      stg.*
    FROM stg_om4_itc_workflow_cpy stg,
      fnd_wfs_om4_workflow fnd
    WHERE stg.workflow_history_id = fnd.workflow_history_id
    AND stg.sys_process_code    = 'N'
      -- Any further validation goes in here - like xxx.ind in (0,1) ---
    ORDER BY stg.workflow_history_id,
      stg.sys_source_batch_id,
      stg.sys_source_sequence_no ;
  --**************************************************************************************************
  -- Eliminate duplicates on the very rare occasion they may be present
  --**************************************************************************************************
PROCEDURE remove_duplicates
AS
BEGIN
  g_workflow_history_id := 0;
  FOR dupp_record IN stg_dup
  LOOP
    IF dupp_record.workflow_history_id = g_workflow_history_id THEN
      UPDATE stg_om4_itc_workflow_cpy stg
      SET sys_process_code       = 'D'
      WHERE sys_source_batch_id  = dupp_record.sys_source_batch_id
      AND sys_source_sequence_no = dupp_record.sys_source_sequence_no;
      g_recs_duplicate          := g_recs_duplicate + 1;
    END IF;
    g_workflow_history_id := dupp_record.workflow_history_id;
  END LOOP;
  COMMIT;
EXCEPTION
WHEN OTHERS THEN
  l_message := 'REMOVE DUPLICATES - OTHER ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END remove_duplicates;
--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
PROCEDURE flagged_records_insert
AS
BEGIN
  --     g_rec_out.last_updated_date         := g_date;
  INSERT
    /*+ APPEND parallel (fnd,2) */
  INTO fnd_wfs_om4_workflow fnd
  SELECT
    /*+ FULL(cpy)  parallel (cpy,2) */
    cpy.workflow_history_id	,
    cpy.credit_applications_id	,
    cpy.user1	,
    cpy.activity	,
    cpy.activity_timestamp	,
    cpy.status	,
    cpy.event	,
    cpy.is_product	,
    cpy.product_type	,
    cpy.decision	,
    g_date AS last_updated_date
  FROM stg_om4_itc_workflow_cpy cpy
  WHERE NOT EXISTS
    (SELECT
      /*+ nl_aj */
      *
    FROM fnd_wfs_om4_workflow
    WHERE workflow_history_id = cpy.workflow_history_id
    )
    -- Any further validation goes in here - like xxx.ind in (0,1) ---
  AND sys_process_code = 'N';
  g_recs_inserted     := g_recs_inserted + sql%rowcount;
  COMMIT;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := 'FLAG INSERT - INSERT ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := 'FLAG INSERT - OTHER ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END flagged_records_insert;
--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
PROCEDURE flagged_records_update
AS
BEGIN
  FOR upd_rec IN c_stg_wfs_om4_workflow_dly
  LOOP      
    UPDATE fnd_wfs_om4_workflow fnd
    SET fnd. credit_applications_id         = upd_rec. credit_applications_id ,
      fnd. user1                             = upd_rec. user1 ,      
      fnd. activity                         = upd_rec. activity ,    
      fnd. activity_timestamp               = upd_rec. activity_timestamp ,
      fnd. status                           = upd_rec. status ,
      fnd. event                            = upd_rec. event ,      
      fnd. is_product                       = upd_rec. is_product ,
      fnd. product_type                     = upd_rec. product_type ,      
      fnd. decision                         = upd_rec. decision ,          
      fnd.last_updated_date                 = g_date
    WHERE fnd.workflow_history_id      = upd_rec.workflow_history_id
    AND ( 
    NVL(fnd. credit_applications_id ,0)     <>upd_rec. credit_applications_id
    OR NVL(fnd. user1 ,0)                    <>upd_rec. user1
    OR NVL(fnd. activity ,0)                <>upd_rec. activity
    OR NVL(fnd. activity_timestamp ,NULL)    <>upd_rec. activity_timestamp
    OR NVL(fnd. status ,0)                  <>upd_rec. status
    OR NVL(fnd. event ,0)                   <>upd_rec. event    
    OR NVL(fnd. is_product ,0)              <>upd_rec. is_product
    OR NVL(fnd. product_type ,0)            <>upd_rec. product_type
    OR NVL(fnd. decision ,0)                <>upd_rec. decision);
    
    g_recs_updated                              := g_recs_updated + 1;
  END LOOP;
  COMMIT;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := 'FLAG UPDATE - INSERT ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := 'FLAG UPDATE - OTHER ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END flagged_records_update;
--**************************************************************************************************
-- Send records to hospital where not valid
--**************************************************************************************************
PROCEDURE flagged_records_hospital
AS
BEGIN
  INSERT /*+ APPEND parallel (hsp,2) */
  INTO stg_om4_itc_workflow_hsp hsp
  SELECT
    /*+ FULL(cpy)  parallel (cpy,2) */
    cpy.sys_source_batch_id,
    cpy.sys_source_sequence_no,
    sysdate,
    'Y',
    'DWH',
    cpy.sys_middleware_batch_id,
    'VALIDATION FAIL - REFERENCIAL ERROR',
    cpy.	workflow_history_id	,
    cpy.	credit_applications_id	,
    cpy.	user1	,
    cpy.	activity	,
    cpy.	activity_timestamp	,
    cpy.	status	,
    cpy.	event	,
    cpy.	is_product	,
    cpy.	product_type	,
    cpy.	decision	
  FROM stg_om4_itc_workflow_cpy cpy
  WHERE
    --      (
    --      NOT EXISTS
    --        (SELECT * FROM  dim_table dim
    --         where  cpy.xxx       = dim.xxx ) or
    --      not exists
    --        (select * from  dim_table dim1
    --         where  cpy.xxx    = dim1.xxx )
    --      ) and
    -- Any further validation goes in here - like or xxx.ind not in (0,1) ---
    sys_process_code = 'N';
  g_recs_hospital   := g_recs_hospital + sql%rowcount;
  COMMIT;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := 'FLAG HOSPITAL - INSERT ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
WHEN OTHERS THEN
  l_message := 'FLAG HOSPITAL - OTHER ERROR '||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  raise;
END flagged_records_hospital;
--**************************************************************************************************
-- Main process
--**************************************************************************************************
BEGIN
  EXECUTE immediate 'alter session enable parallel dml';
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- Call the bulk routines
  --**************************************************************************************************
  l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  remove_duplicates;
  SELECT COUNT(*)
  INTO g_recs_read
  FROM stg_om4_itc_workflow_cpy
  WHERE sys_process_code = 'N';
  l_text                := 'BULK UPDATE STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  flagged_records_update;
  l_text := 'BULK INSERT STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  flagged_records_insert;
  --********** REMOVED AS THERE IS NO VALIDATION AND THUS NOT RECORDS GO TO HOSPITAL ******************
  --    l_text := 'BULK HOSPITALIZATION STARTED AT '||
  --    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --    flagged_records_hospital;
  --    Taken out for better performance --------------------
  --    update stg_absa_crd_acc_dly_cpy
  --    set    sys_process_code = 'Y';
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'DUPLICATE REMOVED '||g_recs_duplicate;                              --Bulk load--
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); --Bulk Load--
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  IF g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital THEN
    l_text       := 'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    p_success := false;
    l_message := 'ERROR - Record counts do not balance see log file';
    dwh_log.record_error(l_module_name,SQLCODE,l_message);
    raise_application_error (-20246,'Record count error - see log files');
  END IF;
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  RAISE;
END WH_FND_WFS_160U;