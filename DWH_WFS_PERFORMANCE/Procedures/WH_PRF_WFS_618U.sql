--------------------------------------------------------
--  DDL for Procedure WH_PRF_WFS_618U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_PERFORMANCE"."WH_PRF_WFS_618U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: HR Employee Data - Load WFS HR data into history
--
--
-- Date:        2018-06-15
-- Author:      Naresh Chauhan
-- Purpose:     update table WFS_HR_EMPLOYEE in the Performance layer
--      
-- Tables: 
--              Input  - 
--                       FND_WFS_HR_EMPLOYEE
-- 
--              Output - WFS_HR_EMPLOYEE
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2018-06-15 N Chauhan - created.
--  2018-06-20 N Chauhan - exclude check on extract time for changes.
--  2018-06-21 N Chauhan - include active and inactive employees in "existing" list.
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




g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_date               date          :=  trunc(sysdate);

g_year_no            integer       :=  0;
g_month_no           integer       :=  0;


L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_WFS_618U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'HR EMPLOYEE DATA - LOAD WFS HR DATA INTO HISTORY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_job_desc varchar2(200):= 'Load WFS HR data into history';
g_success BOOLEAN:= TRUE;

g_analysed_count INTEGER:=0;
g_analysed_success BOOLEAN:= FALSE;


/* load new data & update existing  */

PROCEDURE load AS


BEGIN


   MERGE /*+ append  */
   INTO DWH_WFS_PERFORMANCE.WFS_HR_EMPLOYEE tgt USING
     (
       With 

       -- get record list of existing emp Id's, including both, active and inactive
       existing_emps as (
          select *  /*+ parallel(exst,4) full(exst) */ 
          from DWH_WFS_PERFORMANCE.WFS_HR_EMPLOYEE exst
          where record_active_to_date >  g_date
       ),

       -- isolate new foundation records 
       latest_updates as (
         select   /*+ parallel(fnd,4) */
         *
         FROM
            DWH_WFS_FOUNDATION.FND_WFS_HR_EMPLOYEE  fnd
         where fnd.last_updated_date = g_date    -- only changed records will have new information_date
       ),        

       new_emps as (

         SELECT  /*+ parallel(fnd,4) full(fnd) parallel(exst,4) full(exst) */ 
            distinct
            fnd.employee_id ,

            g_date+1  as record_active_from_date ,     -- batchdate plus one - to align with JOB_REC_EFFECTIVE_DATE 

            to_date('01/jan/3000', 'DD/mon/YYYY') as record_active_to_date ,  -- future date to indicate as the current valid record

            fnd.job_rec_effective_date ,
            fnd.effective_date_latest_seq_no ,
            fnd.last_name ,
            fnd.first_name ,
            fnd.preferred_first_name ,
            fnd.company_code ,
            fnd.company_name ,
            fnd.org_hrchy_level_2_code ,
            fnd.org_hrchy_level_2_desc ,
            fnd.org_hrchy_level_3_code ,
            fnd.org_hrchy_level_3_desc ,
            fnd.org_hrchy_level_4_code ,
            fnd.org_hrchy_level_4_desc ,
            fnd.org_hrchy_level_5_code ,
            fnd.org_hrchy_level_5_desc ,
            fnd.org_hrchy_level_6_code ,
            fnd.org_hrchy_level_6_desc ,
            fnd.department_code ,
            fnd.department_desc ,
            fnd.employee_location_code ,
            fnd.employee_location_desc ,
            fnd.employee_class_code ,
            fnd.employee_class_desc ,
            fnd.employee_sub_class_code ,
            fnd.employee_sub_class_desc ,
            fnd.standard_hours_per_week ,
            fnd.job_code ,
            fnd.job_desc ,
            fnd.position_number ,
            fnd.position_desc ,
            fnd.reports_to_position_number ,
            fnd.reports_to_position_desc ,
            fnd.current_service_start_date ,
 -- virtual           fnd.current_service_no_of_years ,
            fnd.department_manager_id ,
            fnd.department_manager_name ,
            fnd.employee_status_code ,
            fnd.employee_status_desc ,
            fnd.hr_status_code ,
            fnd.hr_status_desc ,
            fnd.termination_date ,
            fnd.extract_time

         FROM
            latest_updates  fnd
            left outer join existing_emps exst  on ( fnd. employee_id = exst.employee_id )
         where  exst.employee_id is null
       ),        


       updates_with_changes as (

         SELECT  /*+ parallel(fnd,4) full(fnd) parallel(exst,4) full(exst) */ 
            distinct
            fnd.employee_id ,

            g_date+1  as record_active_from_date ,     -- batchdate plus one - to align with JOB_REC_EFFECTIVE_DATE 

            to_date('01/jan/3000', 'DD/mon/YYYY') as record_active_to_date ,  -- future date to indicate as the current valid record

            fnd.job_rec_effective_date ,
            fnd.effective_date_latest_seq_no ,
            fnd.last_name ,
            fnd.first_name ,
            fnd.preferred_first_name ,
            fnd.company_code ,
            fnd.company_name ,
            fnd.org_hrchy_level_2_code ,
            fnd.org_hrchy_level_2_desc ,
            fnd.org_hrchy_level_3_code ,
            fnd.org_hrchy_level_3_desc ,
            fnd.org_hrchy_level_4_code ,
            fnd.org_hrchy_level_4_desc ,
            fnd.org_hrchy_level_5_code ,
            fnd.org_hrchy_level_5_desc ,
            fnd.org_hrchy_level_6_code ,
            fnd.org_hrchy_level_6_desc ,
            fnd.department_code ,
            fnd.department_desc ,
            fnd.employee_location_code ,
            fnd.employee_location_desc ,
            fnd.employee_class_code ,
            fnd.employee_class_desc ,
            fnd.employee_sub_class_code ,
            fnd.employee_sub_class_desc ,
            fnd.standard_hours_per_week ,
            fnd.job_code ,
            fnd.job_desc ,
            fnd.position_number ,
            fnd.position_desc ,
            fnd.reports_to_position_number ,
            fnd.reports_to_position_desc ,
            fnd.current_service_start_date ,
 -- virtual           fnd.current_service_no_of_years ,
            fnd.department_manager_id ,
            fnd.department_manager_name ,
            fnd.employee_status_code ,
            fnd.employee_status_desc ,
            fnd.hr_status_code ,
            fnd.hr_status_desc ,
            fnd.termination_date ,
            fnd.extract_time

         FROM
            latest_updates  fnd,
            existing_emps exst
         where  fnd. employee_id = exst.employee_id
            and (

            --  pk       nvl(fnd. employee_id, 0) <> exst.employee_id OR
            --  pk       nvl(fnd. record_active_from_date, '01 JAN 1900') <> nvl(exst.record_active_from_date, '01 JAN 1900') OR
            --  generated:      to_date('01/jan/3000', 'DD/mon/YYYY') <> nvl(exst.record_active_to_date, '01 JAN 1900') OR
                     nvl(fnd. job_rec_effective_date, '01 JAN 1900') <> nvl(exst.job_rec_effective_date, '01 JAN 1900') OR
                     nvl(fnd. effective_date_latest_seq_no, 0) <> exst.effective_date_latest_seq_no OR
                     nvl(fnd. last_name, 0) <> exst.last_name OR
                     nvl(fnd. first_name, 0) <> exst.first_name OR
                     nvl(fnd. preferred_first_name, 0) <> exst.preferred_first_name OR
                     nvl(fnd. company_code, 0) <> exst.company_code OR
                     nvl(fnd. company_name, 0) <> exst.company_name OR
                     nvl(fnd. org_hrchy_level_2_code, 0) <> exst.org_hrchy_level_2_code OR
                     nvl(fnd. org_hrchy_level_2_desc, 0) <> exst.org_hrchy_level_2_desc OR
                     nvl(fnd. org_hrchy_level_3_code, 0) <> exst.org_hrchy_level_3_code OR
                     nvl(fnd. org_hrchy_level_3_desc, 0) <> exst.org_hrchy_level_3_desc OR
                     nvl(fnd. org_hrchy_level_4_code, 0) <> exst.org_hrchy_level_4_code OR
                     nvl(fnd. org_hrchy_level_4_desc, 0) <> exst.org_hrchy_level_4_desc OR
                     nvl(fnd. org_hrchy_level_5_code, 0) <> exst.org_hrchy_level_5_code OR
                     nvl(fnd. org_hrchy_level_5_desc, 0) <> exst.org_hrchy_level_5_desc OR
                     nvl(fnd. org_hrchy_level_6_code, 0) <> exst.org_hrchy_level_6_code OR
                     nvl(fnd. org_hrchy_level_6_desc, 0) <> exst.org_hrchy_level_6_desc OR
                     nvl(fnd. department_code, 0) <> exst.department_code OR
                     nvl(fnd. department_desc, 0) <> exst.department_desc OR
                     nvl(fnd. employee_location_code, 0) <> exst.employee_location_code OR
                     nvl(fnd. employee_location_desc, 0) <> exst.employee_location_desc OR
                     nvl(fnd. employee_class_code, 0) <> exst.employee_class_code OR
                     nvl(fnd. employee_class_desc, 0) <> exst.employee_class_desc OR
                     nvl(fnd. employee_sub_class_code, 0) <> exst.employee_sub_class_code OR
                     nvl(fnd. employee_sub_class_desc, 0) <> exst.employee_sub_class_desc OR
                     nvl(fnd. standard_hours_per_week, 0) <> exst.standard_hours_per_week OR
                     nvl(fnd. job_code, 0) <> exst.job_code OR
                     nvl(fnd. job_desc, 0) <> exst.job_desc OR
                     nvl(fnd. position_number, 0) <> exst.position_number OR
                     nvl(fnd. position_desc, 0) <> exst.position_desc OR
                     nvl(fnd. reports_to_position_number, 0) <> exst.reports_to_position_number OR
                     nvl(fnd. reports_to_position_desc, 0) <> exst.reports_to_position_desc OR
                     nvl(fnd. current_service_start_date, '01 JAN 1900') <> nvl(exst.current_service_start_date, '01 JAN 1900') OR
           -- virtual col      nvl(fnd. current_service_no_of_years, 0) <> exst.current_service_no_of_years OR
                     nvl(fnd. department_manager_id, 0) <> exst.department_manager_id OR
                     nvl(fnd. department_manager_name, 0) <> exst.department_manager_name OR
                     nvl(fnd. employee_status_code, 0) <> exst.employee_status_code OR
                     nvl(fnd. employee_status_desc, 0) <> exst.employee_status_desc OR
                     nvl(fnd. hr_status_code, 0) <> exst.hr_status_code OR
                     nvl(fnd. hr_status_desc, 0) <> exst.hr_status_desc OR
                     nvl(fnd. termination_date, '01 JAN 1900') <> nvl(exst.termination_date, '01 JAN 1900') 
          --    OR   nvl(fnd. extract_time, '01 JAN 1900') <> nvl(exst.extract_time, '01 JAN 1900')
          -- exclude extract_time, as this changes for each extract

            )
       )


         -- new employees
         SELECT * from new_emps

         UNION  

         -- updates to be added as new records
         SELECT * from updates_with_changes        

         UNION

         -- existing records to be closed off
         SELECT  /*+ parallel(fnd,4) full(fnd) parallel(exst,4) full(exst) */ 

            exst.employee_id ,
            exst.record_active_from_date ,
            g_date as record_active_to_date ,  -- batchdate  - a day before opening day of new record 

            exst.job_rec_effective_date ,
            exst.effective_date_latest_seq_no ,
            exst.last_name ,
            exst.first_name ,
            exst.preferred_first_name ,
            exst.company_code ,
            exst.company_name ,
            exst.org_hrchy_level_2_code ,
            exst.org_hrchy_level_2_desc ,
            exst.org_hrchy_level_3_code ,
            exst.org_hrchy_level_3_desc ,
            exst.org_hrchy_level_4_code ,
            exst.org_hrchy_level_4_desc ,
            exst.org_hrchy_level_5_code ,
            exst.org_hrchy_level_5_desc ,
            exst.org_hrchy_level_6_code ,
            exst.org_hrchy_level_6_desc ,
            exst.department_code ,
            exst.department_desc ,
            exst.employee_location_code ,
            exst.employee_location_desc ,
            exst.employee_class_code ,
            exst.employee_class_desc ,
            exst.employee_sub_class_code ,
            exst.employee_sub_class_desc ,
            exst.standard_hours_per_week ,
            exst.job_code ,
            exst.job_desc ,
            exst.position_number ,
            exst.position_desc ,
            exst.reports_to_position_number ,
            exst.reports_to_position_desc ,
            exst.current_service_start_date ,
 -- virtual col           exst.current_service_no_of_years ,
            exst.department_manager_id ,
            exst.department_manager_name ,
            exst.employee_status_code ,
            exst.employee_status_desc ,
            exst.hr_status_code ,
            exst.hr_status_desc ,
            exst.termination_date ,
            exst.extract_time

         FROM
            updates_with_changes fnd
            inner join  existing_emps exst 
               on (  fnd. employee_id = exst.employee_id )

   ) rec_to_ins_or_upd 

   ON (
                 rec_to_ins_or_upd.employee_id  = tgt.employee_id
             and rec_to_ins_or_upd.record_active_from_date  = tgt.record_active_from_date
                 -- to get match for updating/closing off existing valid record
       )
   WHEN MATCHED THEN UPDATE 
    SET
             record_active_to_date = g_date    -- close off with a day before active_to date of new record

            ,tgt.last_updated_date = TRUNC(g_date)

    WHEN NOT MATCHED THEN INSERT (

            employee_id ,
            record_active_from_date ,
            record_active_to_date ,
            job_rec_effective_date ,
            effective_date_latest_seq_no ,
            last_name ,
            first_name ,
            preferred_first_name ,
            company_code ,
            company_name ,
            org_hrchy_level_2_code ,
            org_hrchy_level_2_desc ,
            org_hrchy_level_3_code ,
            org_hrchy_level_3_desc ,
            org_hrchy_level_4_code ,
            org_hrchy_level_4_desc ,
            org_hrchy_level_5_code ,
            org_hrchy_level_5_desc ,
            org_hrchy_level_6_code ,
            org_hrchy_level_6_desc ,
            department_code ,
            department_desc ,
            employee_location_code ,
            employee_location_desc ,
            employee_class_code ,
            employee_class_desc ,
            employee_sub_class_code ,
            employee_sub_class_desc ,
            standard_hours_per_week ,
            job_code ,
            job_desc ,
            position_number ,
            position_desc ,
            reports_to_position_number ,
            reports_to_position_desc ,
            current_service_start_date ,
--virtual            current_service_no_of_years ,
            department_manager_id ,
            department_manager_name ,
            employee_status_code ,
            employee_status_desc ,
            hr_status_code ,
            hr_status_desc ,
            termination_date ,
            extract_time ,
            last_updated_date

           ) 

    VALUES (

            rec_to_ins_or_upd.employee_id ,
            rec_to_ins_or_upd.record_active_from_date ,
            rec_to_ins_or_upd.record_active_to_date ,
            rec_to_ins_or_upd.job_rec_effective_date ,
            rec_to_ins_or_upd.effective_date_latest_seq_no ,
            rec_to_ins_or_upd.last_name ,
            rec_to_ins_or_upd.first_name ,
            rec_to_ins_or_upd.preferred_first_name ,
            rec_to_ins_or_upd.company_code ,
            rec_to_ins_or_upd.company_name ,
            rec_to_ins_or_upd.org_hrchy_level_2_code ,
            rec_to_ins_or_upd.org_hrchy_level_2_desc ,
            rec_to_ins_or_upd.org_hrchy_level_3_code ,
            rec_to_ins_or_upd.org_hrchy_level_3_desc ,
            rec_to_ins_or_upd.org_hrchy_level_4_code ,
            rec_to_ins_or_upd.org_hrchy_level_4_desc ,
            rec_to_ins_or_upd.org_hrchy_level_5_code ,
            rec_to_ins_or_upd.org_hrchy_level_5_desc ,
            rec_to_ins_or_upd.org_hrchy_level_6_code ,
            rec_to_ins_or_upd.org_hrchy_level_6_desc ,
            rec_to_ins_or_upd.department_code ,
            rec_to_ins_or_upd.department_desc ,
            rec_to_ins_or_upd.employee_location_code ,
            rec_to_ins_or_upd.employee_location_desc ,
            rec_to_ins_or_upd.employee_class_code ,
            rec_to_ins_or_upd.employee_class_desc ,
            rec_to_ins_or_upd.employee_sub_class_code ,
            rec_to_ins_or_upd.employee_sub_class_desc ,
            rec_to_ins_or_upd.standard_hours_per_week ,
            rec_to_ins_or_upd.job_code ,
            rec_to_ins_or_upd.job_desc ,
            rec_to_ins_or_upd.position_number ,
            rec_to_ins_or_upd.position_desc ,
            rec_to_ins_or_upd.reports_to_position_number ,
            rec_to_ins_or_upd.reports_to_position_desc ,
            rec_to_ins_or_upd.current_service_start_date ,
--virtual            rec_to_ins_or_upd.current_service_no_of_years ,
            rec_to_ins_or_upd.department_manager_id ,
            rec_to_ins_or_upd.department_manager_name ,
            rec_to_ins_or_upd.employee_status_code ,
            rec_to_ins_or_upd.employee_status_desc ,
            rec_to_ins_or_upd.hr_status_code ,
            rec_to_ins_or_upd.hr_status_desc ,
            rec_to_ins_or_upd.termination_date ,
            rec_to_ins_or_upd.extract_time

            , TRUNC(g_date)
           ) 

   ;

   g_success := TRUE;


EXCEPTION

   WHEN OTHERS THEN

      ROLLBACK;
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||'Error '||sqlcode||' '||sqlerrm );
      l_text :=  l_description||' - LOAD sub proc fails';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
      dwh_log.record_error(l_module_name,SQLCODE,l_message);

      g_success := FALSE;
      raise;

END load;







--##############################################################################################
-- Main process
--**********************************************************************************************

begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'WFS_HR_EMPLOYEE load STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************


    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    l_text := 'LOAD TABLE: WFS_HR_EMPLOYEE' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    -- ****** main load *************
    LOAD;
    -- ******************************

    g_recs_read     :=  SQL%ROWCOUNT;
    g_recs_inserted :=  SQL%ROWCOUNT;

    commit;  




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
    l_text :=  'Merge count includes inserts for new employees, ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'inserts of updated records for existing employees, as well as, ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'close-off updates to existing records that are being superceded.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



    if g_success then


        p_success := true;
        commit;

--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc|| '   - ends');
    else
--      SYS.DBMS_OUTPUT.PUT_LINE(to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
--      || '   - load for day '||to_char(g_date_to_do,'yyyy-mm-dd') ||' fails');

        rollback;
        l_text := to_char(sysdate,'yyyy-mm-dd hh24:mi:ss  ')||g_job_desc
                  || '   - load for '||g_date||'  fails';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');

        p_success := false;

    end if;

    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


exception

    when dwh_errors.e_insert_error then
       rollback;
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := false;
       raise;

    when others then
       rollback;
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := false;
       raise;


end WH_PRF_WFS_618U;
