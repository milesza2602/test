--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_688U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_688U" (
    p_forall_limit in integer,
    p_success out boolean)
as
  --**************************************************************************************************
  --  Description  WFS New Business Report - POI (Proof Of Payment) of each application, the dates and statuses associated with each POI.
  --  Date:        2018-07-25
  --  Author:      Nhlaka dlamini
  --  Purpose:     Update  FND_WFS_NBR_BOOKED_ACC base temp table in the performance layer
  --               with input ex
  --                    FND_WFS_OM4_APPLICATION
  --                    FND_WFS_OM4_WORKFLOW
  --
  --               THIS JOB RUNS DAILY
  --  Tables:      Input  -
  --                    FND_WFS_OM4_APPLICATION
  --                    FND_WFS_OM4_WORKFLOW
  --               Output - FND_WFS_NBR_BOOKED_ACC
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
  v_ctas_booked clob;
  table_exists integer;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_WFS_688U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'WFS NBR Booked Account';
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
  l_text := 'WFS_NBR_BOOKED_ACC Load Started At '|| to_char(sysdate,('DD MON YYYY HH24:MI:SS'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --  --**************************************************************************************************
  --  -- Look up batch date from dim_control
  --  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD TABLE: '||'WFS_NBR_TMP_BOOKED_ACC' ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'ALTER SESSION ENABLE PARALLEL DML';
  
  execute immediate 'TRUNCATE TABLE DWH_WFS_PERFORMANCE.WFS_NBR_TMP_BOOKED_ACC';

  l_text := 'WFS_NBR_TMP_BOOKED_ACC Completed Truncate At '|| to_char(sysdate,('DD MON YYYY HH24:MI:SS'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  
  insert
    /*+ APPEND */
  into dwh_wfs_performance.wfs_nbr_tmp_booked_acc a
    (
      credit_applications_id,
      product_type,
      booked_account_date,
      activity
    )
with 
  fnd_wfs_tmp_base_base_apps as
  (select distinct credit_applications_id
  from dwh_wfs_foundation.fnd_wfs_om4_application
  where entered_time_stamp >= to_date('20160101','YYYYMMDD')
  ),
  fnd_wfs_tmp_first_booked_acc as
  (select booked_acc.credit_applications_id,
    booked_acc.product_type,
    booked_acc.booked_account_date,
    booked_acc.activity
  from fnd_wfs_tmp_base_base_apps base_apps
  right outer join
    (select distinct credit_applications_id,
      product_type,
      trunc(activity_timestamp,'dd') as booked_account_date ,
      activity
    from dwh_wfs_foundation.fnd_wfs_om4_workflow wf
    where product_type is not null
    and activity       in ('PENDING CARD CREATION-CreditCard','CREATE PRODUCT-PersonalLoan','CREATE PRODUCT-StoreCard')
    ) booked_acc
  on booked_acc.credit_applications_id = base_apps.credit_applications_id
  )
  
select
  /*+ FULL(BOOKED_ACCOUNT) PARALLEL (BOOKED_ACCOUNT,4) */
  credit_applications_id,
  product_type,
  booked_account_date,
  activity
from fnd_wfs_tmp_first_booked_acc booked_account;  


g_recs_read     := g_recs_read     + sql%rowcount;  
g_recs_inserted := g_recs_inserted + sql%rowcount;  

commit;  

l_text := 'WFS_NBR_TMP_BOOKED_ACC Insert completed at '||to_char(sysdate,('DD MON YYYY HH24:MI:SS'));  
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
commit;   

--**************************************************************************************************  
-- Write final log data  
--**************************************************************************************************  
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);  
l_text := dwh_constants.vc_log_time_completed ||to_char(sysdate,('DD MON YYYY HH24:MI:SS'));  
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
l_message := dwh_constants.vc_err_mm_insert||sqlcode||''||sqlerrm;  
dwh_log.record_error(l_module_name,sqlcode,l_message);  
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');  
rollback;  
p_success := false;  
raise;

when others then  
l_message := dwh_constants.vc_err_mm_other||sqlcode||''||sqlerrm;  
dwh_log.record_error(l_module_name,sqlcode,l_message);  
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');  
rollback;  
p_success := false;  
raise;


end wh_prf_wfs_688u;
