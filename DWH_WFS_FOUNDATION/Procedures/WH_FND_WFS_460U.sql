--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_460U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_460U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- Description: OneApp usage and navigations - Load API data
-- Tran_type: ODWH: MBAPI   AIT: MBAPI
--
-- Date:       2017-10-04
-- Author:      Naresh Chauhan
-- Purpose:     update table FND_WFS_ONEAPP_API in the Foundation layer
--              with input ex staging table from WFS.
-- Tables:      Input  - STG_WGRP_ONEAPP_API_CPY
--              Output - FND_WFS_ONEAPP_API
--              Dependency on  -   none
-- Packages:    constants, dwh_log
--
-- Maintenance:
--  2017-10-04 N Chauhan - created.
--  2018-01-17 N Chauhan - disable update of api_event_create_date - can't update partition column

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


g_unique_key1_field_val  DWH_WFS_FOUNDATION.STG_WGRP_ONEAPP_API_CPY.ONEAPP_API_NO%type;
--
--
--
--

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_WFS_460U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'ONEAPP USAGE AND NAVIGATIONS - LOAD API DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_stg_dup is
   select * from  dwh_wfs_foundation.stg_wgrp_oneapp_api_cpy
   where (    oneapp_api_no  )
   in
   (select     oneapp_api_no 
    from dwh_wfs_foundation.stg_wgrp_oneapp_api_cpy
    group by      oneapp_api_no
    having count(*) > 1) 
   order by
    oneapp_api_no
    ,sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg is
   select /*+ FULL(stg)  parallel (stg,2) */  
              stg.*
      from    dwh_wfs_foundation.stg_wgrp_oneapp_api_cpy stg
              ,dwh_wfs_foundation.fnd_wfs_oneapp_api fnd
--
     where
              fnd.oneapp_api_no   = stg.oneapp_api_no and      -- only ones existing in fnd
--
--
--
--
--
              stg.sys_process_code         = 'N'  
-- Any further validation goes in here - like xxx.ind in (0,1) ---              
      order by
              stg.oneapp_api_no,
--
--
--
--
              stg.sys_source_batch_id,stg.sys_source_sequence_no ; 

--************************************************************************************************** 
-- Eliminate duplicates on the very 'rare' occasion they may be present
--**************************************************************************************************

procedure remove_duplicates as
begin

   g_unique_key1_field_val   := 0;
--
   --
   --
   --

   for dupp_record in c_stg_dup
    loop
       if 
               dupp_record.oneapp_api_no  = g_unique_key1_field_val
          --
          --
          --
          --
       then 
        update dwh_wfs_foundation.stg_wgrp_oneapp_api_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;       
       end if;           

       g_unique_key1_field_val   := dupp_record.oneapp_api_no;
       --
       --
       --
       --

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

      insert /*+ append parallel (fnd,2) */ into fnd_wfs_oneapp_api fnd
      SELECT /*+ FULL(cpy)  parallel (cpy,2) */
         cpy. oneapp_api_no ,
         cpy. device_type_desc ,
         cpy. device_os ,
         cpy. device_os_version ,
         cpy. user_device_interface ,
         cpy. user_device_interface_version ,
         cpy. username_token ,
         cpy. device_model ,
         cpy. device_version ,
         cpy. mobile_network ,
         cpy. api_event_request ,
         cpy. api_event_create_date ,
         cpy. api_event_status ,
         cpy. etl_run_no
         ,
         g_date as last_updated_date 

      from  dwh_wfs_foundation.stg_wgrp_oneapp_api_cpy cpy
--
         left outer join dwh_wfs_foundation.fnd_wfs_oneapp_api fnd on (
                 fnd.oneapp_api_no  = cpy.oneapp_api_no
--
--
--
--
             )
      where fnd.oneapp_api_no is null

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
     update fnd_wfs_oneapp_api fnd 
     set    
         fnd. oneapp_api_no = upd_rec. oneapp_api_no ,
         fnd. device_type_desc = upd_rec. device_type_desc ,
         fnd. device_os = upd_rec. device_os ,
         fnd. device_os_version = upd_rec. device_os_version ,
         fnd. user_device_interface = upd_rec. user_device_interface ,
         fnd. user_device_interface_version = upd_rec. user_device_interface_version ,
         fnd. username_token = upd_rec. username_token ,
         fnd. device_model = upd_rec. device_model ,
         fnd. device_version = upd_rec. device_version ,
         fnd. mobile_network = upd_rec. mobile_network ,
         fnd. api_event_request = upd_rec. api_event_request ,
--         fnd. api_event_create_date = upd_rec. api_event_create_date ,
         fnd. api_event_status = upd_rec. api_event_status ,
         fnd. etl_run_no = upd_rec. etl_run_no
         , 
         fnd.last_updated_date          = g_date 


     where 
              fnd.oneapp_api_no  = upd_rec.oneapp_api_no and

        ( 
         nvl(fnd. oneapp_api_no, 0) <> upd_rec. oneapp_api_no OR
         nvl(fnd. device_type_desc, 0) <> upd_rec. device_type_desc OR
         nvl(fnd. device_os, 0) <> upd_rec. device_os OR
         nvl(fnd. device_os_version, 0) <> upd_rec. device_os_version OR
         nvl(fnd. user_device_interface, 0) <> upd_rec. user_device_interface OR
         nvl(fnd. user_device_interface_version, 0) <> upd_rec. user_device_interface_version OR
         nvl(fnd. username_token, 0) <> upd_rec. username_token OR
         nvl(fnd. device_model, 0) <> upd_rec. device_model OR
         nvl(fnd. device_version, 0) <> upd_rec. device_version OR
         nvl(fnd. mobile_network, 0) <> upd_rec. mobile_network OR
         nvl(fnd. api_event_request, 0) <> upd_rec. api_event_request OR
--         nvl(fnd. api_event_create_date, '01 JAN 1900') <> upd_rec. api_event_create_date OR
         nvl(fnd. api_event_status, 0) <> upd_rec. api_event_status OR
         nvl(fnd. etl_run_no, 0) <> upd_rec. etl_run_no
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
--procedure flagged_records_hospital as
--begin

--      insert /*+ append parallel (hsp,2) */ into dwh_wfs_foundation.stg_wgrp_oneapp_api_hsp hsp
--      select /*+ FULL(cpy)  parallel (cpy,2) */ 
/*
         cpy.sys_source_batch_id,
         cpy.sys_source_sequence_no,
         sysdate,'Y','DWH',
         cpy.sys_middleware_batch_id,
         'VALIDATION FAIL - REFERENTIAL ERROR with ' ,

         cpy. oneapp_api_no ,
         cpy. device_type_desc ,
         cpy. device_os ,
         cpy. device_os_version ,
         cpy. user_device_interface ,
         cpy. user_device_interface_version ,
         cpy. username_token ,
         cpy. device_model ,
         cpy. device_version ,
         cpy. mobile_network ,
         cpy. api_event_request ,
         cpy. api_event_create_date ,
         cpy. api_event_status ,
         cpy. etl_run_no


      from   dwh_wfs_foundation.stg_wgrp_oneapp_api_cpy cpy
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

*/

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
    from   dwh_wfs_foundation.stg_wgrp_oneapp_api_cpy
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

end WH_FND_WFS_460U;
