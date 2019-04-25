--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_686U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_686U" (
    p_forall_limit in integer,
    p_success out boolean)
as
  --**************************************************************************************************
  --  Description  WFS New Business Report - POI (Proof Of Payment) of each application, the dates and statuses associated with each POI.
  --  Date:        2018-07-25
  --  Author:      Nhlaka dlamini
  --  Purpose:     Update  FND_WFS_NBR_BASE_FICA base temp table in the performance layer
  --               with input ex
  --                    FND_WFS_OM4_APPLICATION
  --                    FND_WFS_OM4_CR_DETAIL
  --                    FND_WFS_OM4_WORKFLOW
  --
  --               THIS JOB RUNS DAILY
  --  Tables:      Input  -
  --                   FND_WFS_OM4_APPLICATION
  --                    FND_WFS_OM4_CR_DETAIL
  --                    FND_WFS_OM4_WORKFLOW
  --               Output - FND_WFS_NBR_BASE_FICA
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  2018-07-27 N Dlamini - created.
  --  2018-08-03 N Dlamini - Added standard metadata on procedure.
  --
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  -- TEMP_TABLE_DDL  - Variable to hold the DDL of the temp table
  --**************************************************************************************************
  
  g_forall_limit  integer := dwh_constants.vc_forall_limit;
  g_recs_read     integer := 0;
  g_recs_updated  integer := 0;
  g_recs_inserted integer := 0;
  g_recs_hospital integer := 0;
  g_recs_deleted  integer := 0;
  g_error_count   number  := 0;
  g_error_index   number  := 0;
  g_count         number  := 0;
  g_sub           integer := 0;
  g_rec_out wfs_product_activation%rowtype;
  g_found      boolean;
  g_date       date := trunc(sysdate);
  g_start_week number ;
  g_end_week   number ;
  g_yesterday  date := trunc(sysdate) - 1;
  g_fin_day_no dim_calendar.fin_day_no%type;
  g_stmt  varchar2(300);
  g_yr_00 number;
  g_qt_00 number;
  v_ctas clob;
  table_exists integer;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_WFS_686U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'WFS NBR Base FICA';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  
  
  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************
  
begin
  if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
    g_forall_limit  := p_forall_limit;
  end if;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'WFS_NBR_TMP_BASE_FICA Load Started At '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD TABLE: '||'WFS_NBR_TMP_BASE_FICA' ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'ALTER SESSION ENABLE PARALLEL DML';
  
  execute immediate 'TRUNCATE TABLE DWH_WFS_PERFORMANCE.WFS_NBR_TMP_BASE_FICA';

  l_text := 'WFS_NBR_TMP_BASE_FICA Completed Truncate At '|| to_char(sysdate,('DD MON YYYY HH24:MI:SS'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  insert
    /*+ APPEND */
  into dwh_wfs_performance.wfs_nbr_tmp_base_fica a
    (
      credit_applications_id,
      product_type,
      first_fica_pending_date,
      first_fica_successful_date
    )
with 
  fnd_wfs_tmp_base_cred_appl as
  (select
    /*+ FULL(WF) PARALLEL(WF,4) PARALLEL (DETAIL,4) */
    distinct wf.credit_applications_id,
    product_type
  from dwh_wfs_foundation.fnd_wfs_om4_workflow wf
  inner join dwh_wfs_foundation.fnd_wfs_om4_cr_detail detail
  on detail.credit_applications_id = wf.credit_applications_id
    --WHERE CREDIT_APPLICATIONS_ID IN ('8aa4146359f5750e0159f868b9b3347f')
  where wf.credit_applications_id in
    (select
      /*+ FULL(OM4_APP) PARALLEL(OM4_APP,4) */
      distinct om4_app.credit_applications_id
    from dwh_wfs_foundation.fnd_wfs_om4_application om4_app
    where entered_time_stamp >= add_months (trunc (sysdate,'YEAR'), -48)-- Changed by Nhlaka Dlamini from ADD_MONTHS (TRUNC (SYSDATE,'YEAR'), -12) to include all the missing data
    )
  and product_type = 'CreditCard'
  ),
  fnd_wfs_tmp_first_fica as
  (select credit_applications_id,
    first_fica_pending_date
  from
    (select
      /*+ FULL(WF) PARALLEL(WF,4) */
      wf.credit_applications_id,
      trunc(activity_timestamp,'dd') as first_fica_pending_date,
      row_number() over (partition by wf.credit_applications_id order by activity_timestamp asc) rn
    from dwh_wfs_foundation.fnd_wfs_om4_workflow wf
    where activity = 'FICA Verification-CreditCard'
    )
  where rn = 1
  ),
  fnd_wfs_tmp_first_succ_fica as
  (select credit_applications_id,
    first_fica_successful_date
  from
    (select
      /*+ FULL(WF) PARALLEL(WF,4) */
      wf.credit_applications_id,
      trunc(activity_timestamp,'dd') as first_fica_successful_date,
      row_number() over (partition by wf.credit_applications_id order by activity_timestamp asc) rn
    from dwh_wfs_foundation.fnd_wfs_om4_workflow wf
    where activity = 'PREAGREEMENT_SENT'
    and decision   = 'CreditCard-WFS_FICA_TIER3'
    )
  where rn = 1
  ),
  fnd_wfs_tmp_base_fica as
  (select
    /*+ FULL(CRED_APPL) PARALLEL(CRED_APPL,4) PARALLEL(FIRST_FICA,4) PARALLEL(SUCC_FICA,4) */
    cred_appl.credit_applications_id ,   -- AS App Id ,
    cred_appl.product_type ,             --AS Product ,
    first_fica.first_fica_pending_date , --AS First_Fica Pending Date ,
    succ_fica.first_fica_successful_date --AS First_Fica Successful Date
  from fnd_wfs_tmp_base_cred_appl cred_appl
  left join fnd_wfs_tmp_first_fica first_fica
  on cred_appl.credit_applications_id = first_fica.credit_applications_id
  left join fnd_wfs_tmp_first_succ_fica succ_fica
  on cred_appl.credit_applications_id = succ_fica.credit_applications_id
  )
  
select
  /*+ FULL(FND_WFS_TMP_BASE_FICA) PARALLEL(FND_WFS_TMP_BASE_FICA,4) */
  credit_applications_id,
  product_type,
  first_fica_pending_date,
  first_fica_successful_date
from fnd_wfs_tmp_base_fica ;

g_recs_read     := g_recs_read     + sql%rowcount;
g_recs_inserted := g_recs_inserted + sql%rowcount;

commit;

l_text := 'WFS_NBR_TMP_BASE_FICA Insert completed at '||to_char(sysdate,('DD MON YYYY HH24:MI:SS'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
commit;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
l_text := dwh_constants.vc_log_time_completed ||to_char(sysdate,('DD MON YYYY  HH24:MI:SS'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_read||g_recs_read;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_deleted||g_recs_deleted;
l_text := l_text||'';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_run_completed ||sysdate;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_draw_line;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

commit;
p_success := true;


exception

when dwh_errors.e_insert_error then
  l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;
  
when others then
  l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;
  
  
end wh_prf_wfs_686u;
