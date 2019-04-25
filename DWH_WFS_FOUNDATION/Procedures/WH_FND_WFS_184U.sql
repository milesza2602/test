--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_184U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_184U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: HR Employee Data - Load WFS HR employee data
-- Tran_type: ODWH: 0   AIT: 0
--
-- Date:        2018-06-11
-- Author:      Naresh Chauhan
-- Purpose:     update table FND_WFS_HR_EMPLOYEE in the Foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - STG_PS_HR_EMPLOYEE_CPY
--              Output - FND_WFS_HR_EMPLOYEE
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--   2018-06-11 N Chauhan - created.
--   2018-06-20 N Chauhan - tidy up.
--   2018-06-20 N Chauhan - exclude extract_time in check for changes.

--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster than on the original template.
--  Naming conventions
--  g_ -  Global variable
--  l_ -  Log table variable
--  a_ -  Array variable
--  v_ -  Local variable as found in packages
--  p_ -  Parameter
--  c_ -  Prefix to cursor
--**************************************************************************************************




g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_nochange      integer       :=  0;
g_recs_duplicate     integer       :=  0;   
g_truncate_count     integer       :=  0;


g_unique_key1_field_val  DWH_WFS_FOUNDATION.STG_PS_HR_EMPLOYEE_CPY.EMPLOYEE_ID%type;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_184U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'HR EMPLOYEE DATA - LOAD WFS HR EMPLOYEE DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_stg_dup is
   select * from  dwh_wfs_foundation.stg_ps_hr_employee_cpy
   where (    employee_id  )
   in
   (select     employee_id 
    from dwh_wfs_foundation.stg_ps_hr_employee_cpy
    group by      employee_id
    having count(*) > 1) 
   order by
    employee_id
    ,sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg is
   select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    dwh_wfs_foundation.stg_ps_hr_employee_cpy stg
              ,dwh_wfs_foundation.fnd_wfs_hr_employee fnd
     where
              fnd.employee_id   = stg.employee_id and      -- only ones existing in fnd
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.employee_id,
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very 'rare' occasion they may be present
--**************************************************************************************************

procedure remove_duplicates as
begin

   g_unique_key1_field_val   := 0;

   for dupp_record in c_stg_dup
    loop
       if 
               dupp_record.employee_id  = g_unique_key1_field_val
       then 
        update dwh_wfs_foundation.stg_ps_hr_employee_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;       
       end if;           

       g_unique_key1_field_val   := dupp_record.employee_id;

    end loop;

   commit;

exception
      when others then
       l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;   

end remove_duplicates;



--************************************************************************************************** 
-- Insert all NEW record in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_insert as
begin
 --     g_rec_out.last_updated_date         := g_date;

      insert /*+ append parallel (fnd,2) */ into fnd_wfs_hr_employee fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
         cpy. employee_id ,
         cpy. job_rec_effective_date ,
         cpy. effective_date_latest_seq_no ,
         cpy. last_name ,
         cpy. first_name ,
         cpy. preferred_first_name ,
         cpy. company_code ,
         cpy. company_name ,
         cpy. org_hrchy_level_2_code ,
         cpy. org_hrchy_level_2_desc ,
         cpy. org_hrchy_level_3_code ,
         cpy. org_hrchy_level_3_desc ,
         cpy. org_hrchy_level_4_code ,
         cpy. org_hrchy_level_4_desc ,
         cpy. org_hrchy_level_5_code ,
         cpy. org_hrchy_level_5_desc ,
         cpy. org_hrchy_level_6_code ,
         cpy. org_hrchy_level_6_desc ,
         cpy. department_code ,
         cpy. department_desc ,
         cpy. employee_location_code ,
         cpy. employee_location_desc ,
         cpy. employee_class_code ,
         cpy. employee_class_desc ,
         cpy. employee_sub_class_code ,
         cpy. employee_sub_class_desc ,
         cpy. standard_hours_per_week ,
         cpy. job_code ,
         cpy. job_desc ,
         cpy. position_number ,
         cpy. position_desc ,
         cpy. reports_to_position_number ,
         cpy. reports_to_position_desc ,
         cpy. current_service_start_date ,
         cpy. department_manager_id ,
         cpy. department_manager_name ,
         cpy. employee_status_code ,
         cpy. employee_status_desc ,
         cpy. hr_status_code ,
         cpy. hr_status_desc ,
         cpy. termination_date ,
         cpy. extract_time



         ,
         g_date as last_updated_date 

      from  dwh_wfs_foundation.stg_ps_hr_employee_cpy cpy
         left outer join dwh_wfs_foundation.fnd_wfs_hr_employee fnd on (
                 fnd.employee_id  = cpy.employee_id
             )
      where fnd.employee_id is null

-- Any further validation goes in here - like xxx.ind in (0,1) ---  

       and sys_process_code = 'N'; 

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
-- Updates existing records in the staging table into foundation if there are changes
--**************************************************************************************************

procedure flagged_records_update as
begin


for upd_rec in c_stg
   loop
     update fnd_wfs_hr_employee fnd 
     set    
 --        fnd. employee_id = upd_rec. employee_id ,
         fnd. job_rec_effective_date = upd_rec. job_rec_effective_date ,
         fnd. effective_date_latest_seq_no = upd_rec. effective_date_latest_seq_no ,
         fnd. last_name = upd_rec. last_name ,
         fnd. first_name = upd_rec. first_name ,
         fnd. preferred_first_name = upd_rec. preferred_first_name ,
         fnd. company_code = upd_rec. company_code ,
         fnd. company_name = upd_rec. company_name ,
         fnd. org_hrchy_level_2_code = upd_rec. org_hrchy_level_2_code ,
         fnd. org_hrchy_level_2_desc = upd_rec. org_hrchy_level_2_desc ,
         fnd. org_hrchy_level_3_code = upd_rec. org_hrchy_level_3_code ,
         fnd. org_hrchy_level_3_desc = upd_rec. org_hrchy_level_3_desc ,
         fnd. org_hrchy_level_4_code = upd_rec. org_hrchy_level_4_code ,
         fnd. org_hrchy_level_4_desc = upd_rec. org_hrchy_level_4_desc ,
         fnd. org_hrchy_level_5_code = upd_rec. org_hrchy_level_5_code ,
         fnd. org_hrchy_level_5_desc = upd_rec. org_hrchy_level_5_desc ,
         fnd. org_hrchy_level_6_code = upd_rec. org_hrchy_level_6_code ,
         fnd. org_hrchy_level_6_desc = upd_rec. org_hrchy_level_6_desc ,
         fnd. department_code = upd_rec. department_code ,
         fnd. department_desc = upd_rec. department_desc ,
         fnd. employee_location_code = upd_rec. employee_location_code ,
         fnd. employee_location_desc = upd_rec. employee_location_desc ,
         fnd. employee_class_code = upd_rec. employee_class_code ,
         fnd. employee_class_desc = upd_rec. employee_class_desc ,
         fnd. employee_sub_class_code = upd_rec. employee_sub_class_code ,
         fnd. employee_sub_class_desc = upd_rec. employee_sub_class_desc ,
         fnd. standard_hours_per_week = upd_rec. standard_hours_per_week ,
         fnd. job_code = upd_rec. job_code ,
         fnd. job_desc = upd_rec. job_desc ,
         fnd. position_number = upd_rec. position_number ,
         fnd. position_desc = upd_rec. position_desc ,
         fnd. reports_to_position_number = upd_rec. reports_to_position_number ,
         fnd. reports_to_position_desc = upd_rec. reports_to_position_desc ,
         fnd. current_service_start_date = upd_rec. current_service_start_date ,
         fnd. department_manager_id = upd_rec. department_manager_id ,
         fnd. department_manager_name = upd_rec. department_manager_name ,
         fnd. employee_status_code = upd_rec. employee_status_code ,
         fnd. employee_status_desc = upd_rec. employee_status_desc ,
         fnd. hr_status_code = upd_rec. hr_status_code ,
         fnd. hr_status_desc = upd_rec. hr_status_desc ,
         fnd. termination_date = upd_rec. termination_date ,
         fnd. extract_time = upd_rec. extract_time
        , 
         fnd.last_updated_date          = g_date 


     where 
              fnd.employee_id  = upd_rec.employee_id and


        ( 
--         nvl(fnd. employee_id, 0) <> upd_rec. employee_id OR
         nvl(fnd. job_rec_effective_date, '01 JAN 1900') <> nvl(upd_rec. job_rec_effective_date, '01 JAN 1900') OR
         nvl(fnd. effective_date_latest_seq_no, 0) <> upd_rec. effective_date_latest_seq_no OR
         nvl(fnd. last_name, 0) <> upd_rec. last_name OR
         nvl(fnd. first_name, 0) <> upd_rec. first_name OR
         nvl(fnd. preferred_first_name, 0) <> upd_rec. preferred_first_name OR
         nvl(fnd. company_code, 0) <> upd_rec. company_code OR
         nvl(fnd. company_name, 0) <> upd_rec. company_name OR
         nvl(fnd. org_hrchy_level_2_code, 0) <> upd_rec. org_hrchy_level_2_code OR
         nvl(fnd. org_hrchy_level_2_desc, 0) <> upd_rec. org_hrchy_level_2_desc OR
         nvl(fnd. org_hrchy_level_3_code, 0) <> upd_rec. org_hrchy_level_3_code OR
         nvl(fnd. org_hrchy_level_3_desc, 0) <> upd_rec. org_hrchy_level_3_desc OR
         nvl(fnd. org_hrchy_level_4_code, 0) <> upd_rec. org_hrchy_level_4_code OR
         nvl(fnd. org_hrchy_level_4_desc, 0) <> upd_rec. org_hrchy_level_4_desc OR
         nvl(fnd. org_hrchy_level_5_code, 0) <> upd_rec. org_hrchy_level_5_code OR
         nvl(fnd. org_hrchy_level_5_desc, 0) <> upd_rec. org_hrchy_level_5_desc OR
         nvl(fnd. org_hrchy_level_6_code, 0) <> upd_rec. org_hrchy_level_6_code OR
         nvl(fnd. org_hrchy_level_6_desc, 0) <> upd_rec. org_hrchy_level_6_desc OR
         nvl(fnd. department_code, 0) <> upd_rec. department_code OR
         nvl(fnd. department_desc, 0) <> upd_rec. department_desc OR
         nvl(fnd. employee_location_code, 0) <> upd_rec. employee_location_code OR
         nvl(fnd. employee_location_desc, 0) <> upd_rec. employee_location_desc OR
         nvl(fnd. employee_class_code, 0) <> upd_rec. employee_class_code OR
         nvl(fnd. employee_class_desc, 0) <> upd_rec. employee_class_desc OR
         nvl(fnd. employee_sub_class_code, 0) <> upd_rec. employee_sub_class_code OR
         nvl(fnd. employee_sub_class_desc, 0) <> upd_rec. employee_sub_class_desc OR
         nvl(fnd. standard_hours_per_week, 0) <> upd_rec. standard_hours_per_week OR
         nvl(fnd. job_code, 0) <> upd_rec. job_code OR
         nvl(fnd. job_desc, 0) <> upd_rec. job_desc OR
         nvl(fnd. position_number, 0) <> upd_rec. position_number OR
         nvl(fnd. position_desc, 0) <> upd_rec. position_desc OR
         nvl(fnd. reports_to_position_number, 0) <> upd_rec. reports_to_position_number OR
         nvl(fnd. reports_to_position_desc, 0) <> upd_rec. reports_to_position_desc OR
         nvl(fnd. current_service_start_date, '01 JAN 1900') <> nvl(upd_rec. current_service_start_date, '01 JAN 1900') OR
         nvl(fnd. department_manager_id, 0) <> upd_rec. department_manager_id OR
         nvl(fnd. department_manager_name, 0) <> upd_rec. department_manager_name OR
         nvl(fnd. employee_status_code, 0) <> upd_rec. employee_status_code OR
         nvl(fnd. employee_status_desc, 0) <> upd_rec. employee_status_desc OR
         nvl(fnd. hr_status_code, 0) <> upd_rec. hr_status_code OR
         nvl(fnd. hr_status_desc, 0) <> upd_rec. hr_status_desc OR
         nvl(fnd. termination_date, '01 JAN 1900') <> nvl(upd_rec. termination_date, '01 JAN 1900') 
--OR
--         nvl(fnd. extract_time, '01 JAN 1900') <> nvl(upd_rec. extract_time, '01 JAN 1900')

         );         


   if sql%rowcount = 0 then
        g_recs_nochange:= g_recs_nochange + 1;
   else
        g_recs_updated := g_recs_updated + 1;  
   end if;

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
-- Send records to hospital where not valid
--**************************************************************************************************
-- ***** Not applicable, as there is no dependency on some other table *****
procedure flagged_records_hospital as
begin

      insert /*+ append parallel (hsp,2) */ into dwh_wfs_foundation.stg_ps_hr_employee_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */ 
         cpy.sys_source_batch_id,
         cpy.sys_source_sequence_no,
         sysdate,'Y','DWH',
         cpy.sys_middleware_batch_id,
         'VALIDATION FAIL - REFERENTIAL ERROR with ' ,

         cpy. employee_id ,
         cpy. job_rec_effective_date ,
         cpy. effective_date_latest_seq_no ,
         cpy. last_name ,
         cpy. first_name ,
         cpy. preferred_first_name ,
         cpy. company_code ,
         cpy. company_name ,
         cpy. org_hrchy_level_2_code ,
         cpy. org_hrchy_level_2_desc ,
         cpy. org_hrchy_level_3_code ,
         cpy. org_hrchy_level_3_desc ,
         cpy. org_hrchy_level_4_code ,
         cpy. org_hrchy_level_4_desc ,
         cpy. org_hrchy_level_5_code ,
         cpy. org_hrchy_level_5_desc ,
         cpy. org_hrchy_level_6_code ,
         cpy. org_hrchy_level_6_desc ,
         cpy. department_code ,
         cpy. department_desc ,
         cpy. employee_location_code ,
         cpy. employee_location_desc ,
         cpy. employee_class_code ,
         cpy. employee_class_desc ,
         cpy. employee_sub_class_code ,
         cpy. employee_sub_class_desc ,
         cpy. standard_hours_per_week ,
         cpy. job_code ,
         cpy. job_desc ,
         cpy. position_number ,
         cpy. position_desc ,
         cpy. reports_to_position_number ,
         cpy. reports_to_position_desc ,
         cpy. current_service_start_date ,
         cpy. department_manager_id ,
         cpy. department_manager_name ,
         cpy. employee_status_code ,
         cpy. employee_status_desc ,
         cpy. hr_status_code ,
         cpy. hr_status_desc ,
         cpy. termination_date ,
         cpy. extract_time


      from   dwh_wfs_foundation.stg_ps_hr_employee_cpy cpy
--  no dependency table applicable
      where 
--

--      …       and 

-- Any further validation goes in here - like or xxx.ind not in (0,1) ---    

       sys_process_code = 'N';


      g_recs_hospital := g_recs_hospital + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG HOSPITAL - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG HOSPITAL - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end flagged_records_hospital;



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

    l_text := 'LOAD TABLE: '||'FND_WFS_HR_EMPLOYEE' ;
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
    from   dwh_wfs_foundation.stg_ps_hr_employee_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

/*   no referential integrity checks required
    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_hospital;
*/

--    Taken out for better performance --------------------
--    update stg_...._cpy
--    set    sys_process_code = 'Y';





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
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'NO CHANGE RECORDS '||g_recs_nochange;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital + g_recs_nochange then
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
       RAISE;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       RAISE;

end WH_FND_WFS_184U;
