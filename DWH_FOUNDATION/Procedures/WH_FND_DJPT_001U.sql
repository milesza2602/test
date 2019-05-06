--------------------------------------------------------
--  DDL for Procedure WH_FND_DJPT_001U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_DJPT_001U" (p_forall_limit in integer,p_success out boolean) as
-- **************************************************************************************************
--  Date:        Jun 2017
--  Author:      Mariska Matthee
--  Purpose:     Load DJ Tier data from the DJ Parameter Tool into fnd_location
--
--  Tables:      Input  - stg_djpt_location_cpy
--               Output - fnd_location
--
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_date               date          := trunc(sysdate);
g_hospital_text      stg_djpt_location_hsp.sys_process_msg%type;
g_location_no        stg_djpt_location_cpy.location_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_DJPT_001U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_mp;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_mp;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD DJ PARAMETER TOOL LOCATION MASTERDATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * 
  from dwh_foundation.stg_djpt_location_cpy
 where (location_no) in (select location_no
                           from dwh_foundation.stg_djpt_location_cpy
                          group by location_no
                         having count(*) > 1)
 order by location_no,
          sys_source_batch_id desc,
          sys_source_sequence_no desc;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin
  g_location_no := 0;

  for dupp_record in stg_dup
  loop

    if dupp_record.location_no = g_location_no then
        update dwh_foundation.stg_djpt_location_cpy stg
           set sys_process_code = 'D'
         where sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_location_no := dupp_record.location_no;
  end loop;

  commit;

exception
  when others then
    l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end remove_duplicates;

--**************************************************************************************************
-- Hospital data checks
--**************************************************************************************************
procedure local_bulk_insert_hsp as
begin
  -- check valid locations
  g_hospital_text := dwh_constants.vc_location_not_found;
  
  insert into dwh_foundation.stg_djpt_location_hsp stgh
    (SYS_SOURCE_BATCH_ID,SYS_SOURCE_SEQUENCE_NO,SYS_LOAD_DATE,SYS_PROCESS_CODE,SYS_LOAD_SYSTEM_NAME,
     SYS_MIDDLEWARE_BATCH_ID,SYS_PROCESS_MSG,LOCATION_NO,STORE_TIER_NO)
  select SYS_SOURCE_BATCH_ID,
         SYS_SOURCE_SEQUENCE_NO,
         trunc(sysdate)SYS_LOAD_DATE,
         'Y' SYS_PROCESS_CODE,
         'DWH' SYS_LOAD_SYSTEM_NAME,
         SYS_MIDDLEWARE_BATCH_ID,
         g_hospital_text SYS_PROCESS_MSG,
         LOCATION_NO,
         STORE_TIER_NO
    from dwh_foundation.stg_djpt_location_cpy stg
   where stg.sys_process_code = 'N'
     and stg.location_no not in (select location_no
                                   from fnd_location);
  commit;

  g_recs_hospital := g_recs_hospital + sql%rowcount;

  if g_recs_hospital > 0 then
    l_text := g_hospital_text;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  end if;
exception
  when others then
    l_message := 'HSP INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end local_bulk_insert_hsp;

--**************************************************************************************************
-- Bulk updates
--**************************************************************************************************
procedure local_bulk_update as
begin
  update dwh_foundation.fnd_location fnd
     set fnd.store_tier_no = (select nvl(store_tier_no,-1) store_tier_no
                                from dwh_foundation.stg_djpt_location_cpy stg
                               where stg.location_no = fnd.location_no
                                 and stg.sys_process_code = 'N')
   where exists (select store_tier_no
                   from dwh_foundation.stg_djpt_location_cpy stg
                  where stg.location_no = fnd.location_no
                    and stg.sys_process_code = 'N');

  g_recs_updated := g_recs_updated + sql%rowcount;
  
  commit;
exception
  when others then
    l_message := 'UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end local_bulk_update;

procedure local_bulk_staging_update as
begin
  update dwh_foundation.stg_djpt_location_cpy stg
     set stg.sys_process_code = 'Y'
   where stg.sys_process_code = 'N';
  commit;

  g_recs_updated := g_recs_updated + sql%rowcount;
exception
  when others then
    l_message := 'STAGING UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end local_bulk_staging_update;

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
    into g_recs_read
    from dwh_foundation.stg_djpt_location_cpy
   where sys_process_code = 'N';

  l_text := 'BULK HOSPITAL INSERT STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  local_bulk_insert_hsp;

  l_text := 'BULK UPDATE STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  local_bulk_update;
  
  l_text := 'BULK STAGING UPDATE STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  local_bulk_staging_update;

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
  l_text :=  'DUPLICATES REMOVED '||g_recs_duplicate;           
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
  l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  
  if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital  then
    l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    /*
    Because the location data is a main load, allow the batch to continue
    
    p_success := false;
    l_message := 'ERROR - Record counts do not balance see log file';
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise_application_error (-20246,'Record count error - see log files');
    */
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
    raise;
end WH_FND_DJPT_001U;
