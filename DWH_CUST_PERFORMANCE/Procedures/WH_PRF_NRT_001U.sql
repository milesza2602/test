--------------------------------------------------------
--  DDL for Procedure WH_PRF_NRT_001U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_NRT_001U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2014
--  Author:      Alastair de Wet
--  Purpose:     Create Near real time schedule fact table in the performance layer
--               with input ex foundation layer.
--  Tables:      Input  - fnd_nrt_staff_schedule
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
g_recs_duplicate     integer       :=  0;
g_truncate_count     integer       :=  0;

g_location_no        number(10,0)  := 0;
g_employee_no        number(10,0)  := 0;
g_tran_date          date          := '1 Jan 2000';

g_date               date          := trunc(sysdate);
g_sysdate            date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_NRT_001U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD STAFF SCHEDULE EX FND SCHEDULE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor stg_dup is
select SS.*,ROWID 
from   dwh_cust_foundation.fnd_nrt_staff_schedule SS
where  SS.processed_ind = 'N' and
(location_no,employee_no,trunc(shift_in))
in
(select location_no,employee_no,trunc(shift_in) 
from dwh_cust_foundation.fnd_nrt_staff_schedule 
where  processed_ind = 'N' 
group by location_no,employee_no,trunc(shift_in) 
having count(*) > 1) 
order by location_no,employee_no,trunc(shift_in);

cursor c_fnd_nrt_staff_schedule is
select /*+ FULL(fnd)  parallel (fnd,2) */
              fnd.*,
              de.first_name,
              de.last_name,
              dl.location_name,
              dw.workgroup_name,
              dj.job_name
      from    dwh_cust_foundation.fnd_nrt_staff_schedule fnd,
              dim_hr_employee de,
              DWH_CUST_PERFORMANCE.cust_nrt_staff_time_mng prf,
              dim_location dl,
              DWH_CUST_PERFORMANCE.dim_nrt_workgroup dw,
              DWH_CUST_PERFORMANCE.dim_nrt_job dj
      where   fnd.location_no      = prf.location_no and
              fnd.employee_no      = prf.employee_no and
              trunc(fnd.shift_in)  = prf.tran_date   and
              TO_CHAR(fnd.employee_no)      = de.employee_id(+)  and
              fnd.location_no      = dl.location_no(+)  and
              fnd.workgroup_id     = dw.workgroup_id(+) and
              fnd.job_id           = dj.job_id(+)       and
              fnd.processed_ind = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
      order by
              fnd.location_no,fnd.employee_no;

--************************************************************************************************** 
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin

   g_location_no     := 0;
   g_employee_no     := 0;
   g_tran_date       := '1 Jan 2000';
   
for dupp_record in stg_dup
   loop

    if  dupp_record.location_no     = g_location_no and
        dupp_record.employee_no     = g_employee_no and
        trunc(dupp_record.shift_in) = g_tran_date   then
        update dwh_cust_foundation.fnd_nrt_staff_schedule stg
        set    processed_ind = 'D'
        where  rowid = dupp_record.rowid;
         
        g_recs_duplicate  := g_recs_duplicate  + 1;       
    end if;           

      
    g_location_no    := dupp_record.location_no;
    g_employee_no    := dupp_record.employee_no;
    g_tran_date      := trunc(dupp_record.shift_in);

   end loop;
   
   commit;
 
   exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;




--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ APPEND parallel (prf,2) */ into cust_nrt_staff_time_mng prf
      select /*+ FULL(fnd)  parallel (fnd,2) */
             	trunc(fnd.shift_in) tran_date,
              fnd.location_no,
              fnd.employee_no,
              fnd.workgroup_id,
              fnd.job_id,
              dl.location_name,
              de.first_name,
              de.last_name,
              dw.workgroup_name,
              dj.job_name,
              fnd.shift_in,
              fnd.shift_out,
              '','',
--            	to_char(fnd.shift_in, 'DD/MON/YYYY')|| ' 23:59:00',to_char(fnd.shift_in, 'DD/MON/YYYY')|| ' 23:59:00',
              g_date as last_updated_date
       from   dwh_cust_foundation.fnd_nrt_staff_schedule fnd,
              dim_location dl,
              dim_hr_employee de,
              dim_nrt_workgroup dw,
              dim_nrt_job dj
       where  processed_ind = 'N'   and
              to_char(fnd.employee_no)      = de.employee_id(+)  and
              fnd.location_no      = dl.location_no(+)  and
              fnd.workgroup_id     = dw.workgroup_id(+) and
              fnd.job_id           = dj.job_id(+)       and
       not exists
      (select /*+ nl_aj */ * from cust_nrt_staff_time_mng
       where  location_no      = fnd.location_no and
              employee_no      = fnd.employee_no and
              tran_date        = trunc(fnd.shift_in)
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

FOR upd_rec IN c_fnd_nrt_staff_schedule
   loop
     update cust_nrt_staff_time_mng prf
     set    prf.	workgroup_id  	    =	upd_rec.workgroup_id	,
            prf.	job_id	            =	upd_rec.job_id	,
            prf.	location_name       =	upd_rec.location_name	,
            prf.	employee_first_name	=	upd_rec.first_name	,
            prf.	employee_last_name	=	upd_rec.last_name	,
            prf.	workgroup_name	    =	upd_rec.workgroup_name	,
            prf.	job_name	          =	upd_rec.job_name	,
            prf.	shift_start	        =	upd_rec.shift_in	,
            prf.	shift_end	          =	upd_rec.shift_out	,
            prf.  last_updated_date   = g_date
     where  location_no               = upd_rec.location_no and
            employee_no               = upd_rec.employee_no and
            tran_date                 = trunc(upd_rec.shift_in);

      g_recs_updated := g_recs_updated + 1;
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

    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    remove_duplicates;

    select count(*)
    into   g_recs_read
    from   dwh_cust_foundation.fnd_nrt_staff_schedule
    where  processed_ind = 'N'
    ;

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    l_text := 'UPDATE PROCESSED_IND STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    update dwh_cust_foundation.fnd_nrt_staff_schedule
    set    processed_ind = 'Y'
    where  processed_ind = 'N';

    l_text := 'DELETE HISTORY FROM CUST_NRT_STAFF_TIME_MNG STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    delete from cust_nrt_staff_time_mng
    where  tran_date < g_date - 6 ;

    awx_job_control.complete_job_status('FND_NRT_STAFF_SCHEDULE');
    l_text := 'Set AWX_JOB_STATUS = Y on '||  'FND_NRT_STAFF_SCHEDULE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
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
end wh_prf_nrt_001u;
