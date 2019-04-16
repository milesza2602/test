--------------------------------------------------------
--  DDL for Procedure WH_PRF_NRT_002OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_NRT_002OLD" (p_forall_limit in integer,p_success out boolean) as
--***********************************************************************************************
--  Date:        May 2014
--  Author:      Alastair de Wet
--  Purpose:     Create Near real time schedule fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_nrt_staff_clocking
--               Output - cust_nrt_staff_time_mng
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
-- Note: This version Attempts to do a bulk insert / update
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

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_truncate_count     integer       :=  0;



g_date               date          := trunc(sysdate);
g_sysdate            date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_NRT_002U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD STAFF SCHEDULE EX FND CLOCKING';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_fnd_nrt_staff_clocking is
select --/*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*
      from    dwh_cust_foundation.fnd_nrt_staff_clocking fnd,
              cust_nrt_staff_time_mng prf
      where   fnd.location_no      = prf.location_no and
              fnd.employee_no      = prf.employee_no and
              trunc(SYSDATE)       = prf.tran_date
      and     fnd.processed_ind = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.location_no,fnd.employee_no,fnd.clock_time;


--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;
 --parallel (prf,2)
      insert /*+ APPEND  */ into cust_nrt_staff_time_mng prf
      select /*+ FULL(fnd)  parallel (fnd,2) */
             	trunc(sysdate) tran_date,
              fnd.location_no,
              fnd.employee_no,
              1000000,
              fnd.job_id,
              dl.location_name,
              de.first_name,
              de.last_name,
              'UNKNOWN WORKGROUP',
              dj.job_name,
              '','',
 --           	to_char(SYSDATE, 'DD/MON/YYYY')|| ' 23:59:00', to_char(SYSDATE, 'DD/MON/YYYY')|| ' 23:59:00',
              case
              when fnd.clock_type = 's' then fnd.clock_time else null end,
              case
              when fnd.clock_type = 't'   then fnd.clock_time else null end,
              g_date as last_updated_date
       from   dwh_cust_foundation.fnd_nrt_staff_clocking fnd,
              dim_location dl,
              dim_hr_employee de,
              dim_nrt_job dj
       where  processed_ind = 'N'   and
              to_char(fnd.employee_no)      = de.employee_id(+)  and
              fnd.location_no      = dl.location_no(+)  and
              fnd.job_id           = dj.job_id(+)       and
       not exists
      (select /*+ nl_aj */ * from cust_nrt_staff_time_mng
       where  location_no      = fnd.location_no and
              employee_no      = fnd.employee_no and
              tran_date        = trunc(sysdate)
       )
-- Any further validation goes in here - like xxx.ind in (0,1) ---
       ;


      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin

for upd_rec in c_fnd_nrt_staff_clocking
   loop
    if upd_rec.clock_type = 's' then
       update cust_nrt_staff_time_mng prf
       set    prf.clock_time_in         =	upd_rec.clock_time_rounded	,
              prf.last_updated_date     = g_date
       where  location_no               = upd_rec.location_no and
              employee_no               = upd_rec.employee_no and
              tran_date                 = trunc(sysdate)      and
              upd_rec.clock_time_rounded        < nvl(prf.clock_time_in,to_char(sysdate, 'DD/MON/YYYY')|| ' 23:59:00');
       g_recs_updated := g_recs_updated  + sql%rowcount;
    else
       update cust_nrt_staff_time_mng prf
       set    prf.clock_time_out        =	upd_rec.clock_time_rounded	,
              prf. last_updated_date    = g_date
       where  location_no               = upd_rec.location_no and
              employee_no               = upd_rec.employee_no and
              tran_date                 = trunc(sysdate)      and
              upd_rec.clock_time_rounded        > nvl(prf.clock_time_out,to_char(sysdate, 'DD/MON/YYYY')|| ' 00:00:00');
       g_recs_updated := g_recs_updated + sql%rowcount;
    end if;

      commit;


   end loop;


      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_update;




--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';


    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Call the bulk routines
--**************************************************************************************************


    select count(*)
    into   g_recs_read
    from   dwh_cust_foundation.fnd_nrt_staff_clocking
    where  processed_ind = 'N'
    ;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    update cust_nrt_staff_time_mng prf
    set    prf.clock_time_out        = null	,
           prf. last_updated_date    = g_date
    where  tran_date                 = trunc(sysdate)      and
           prf.clock_time_out        is not null and
           prf.clock_time_out        <= prf.clock_time_in;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    l_text := 'UPDATE PROCESSED_IND STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    update dwh_cust_foundation.fnd_nrt_staff_clocking
    set    processed_ind = 'Y'
    where  processed_ind = 'N';

--   l_text := 'DELETE HISTORY FROM CUST_NRT_STAFF_TIME_MNG STARTED AT '||
--   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   delete from cust_nrt_staff_time_mng
--   where  tran_date < g_date - 3 ;




--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated  then
      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := false;
      l_message := 'ERROR - Record counts do not balance see log file';
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      raise_application_error (-20246,'Record count error - see log files');
   end if;


    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    commit;
    p_success := true;
  exception

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       RAISE;
end wh_prf_nrt_002old;
