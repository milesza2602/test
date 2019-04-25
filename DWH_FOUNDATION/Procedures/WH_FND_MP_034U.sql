--------------------------------------------------------
--  DDL for Procedure WH_FND_MP_034U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_MP_034U" (p_forall_limit in integer,p_success out boolean) as
-- **************************************************************************************************
--  Date:        Jun 2017
--  Author:      Mariska Matthee
--  Purpose:     Create a fnd table for location store tier information from the DJ Parameter Tool
--
--  Tables:      Input  - stg_mp_store_tier_cpy
--               Output - fnd_store_tier
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
g_store_tier_no      stg_mp_store_tier_cpy.store_tier_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_MP_034U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_mp;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_mp;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD MP STORE TIER FND MASTERDATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select * 
  from dwh_foundation.stg_mp_store_tier_cpy
 where (store_tier_no) in (select store_tier_no
                             from dwh_foundation.stg_mp_store_tier_cpy
                            group by store_tier_no
                           having count(*) > 1)
 order by store_tier_no,
          sys_source_batch_id desc,
          sys_source_sequence_no desc;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin
  g_store_tier_no := 0;

  for dupp_record in stg_dup
  loop
    if dupp_record.store_tier_no = g_store_tier_no then
        update dwh_foundation.stg_mp_store_tier_cpy stg
           set sys_process_code = 'D'
         where sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_store_tier_no := dupp_record.store_tier_no;
  end loop;

  commit;

exception
  when others then
    l_message := 'REMOVE DUPLICATES - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end remove_duplicates;

--**************************************************************************************************
-- Data merge
--**************************************************************************************************
procedure local_bulk_merge as

begin
  merge into dwh_foundation.fnd_store_tier fnd
   USING (
         select /*+ FULL(cpy)  parallel (4) */  
              stg.*
       from  dwh_foundation.stg_mp_store_tier_cpy stg
      where  stg.sys_process_code      = 'N'
 ) mp_tier
   ON    (  fnd.store_tier_no = mp_tier.store_tier_no )
   when matched then
     update
        set fnd.store_tier_desc   = mp_tier.store_tier_desc,
            fnd.last_updated_date = g_date
   when not matched then
    insert (fnd.store_tier_no, fnd.store_tier_desc, fnd.last_updated_date)
     values (mp_tier.store_tier_no, mp_tier.store_tier_desc, g_date);
 
  g_recs_updated := g_recs_updated + sql%rowcount;
  
  commit;
  
exception
  when others then
    l_message := 'DATA MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end local_bulk_merge;

--**************************************************************************************************
-- Bulk update
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
  update dwh_foundation.stg_mp_store_tier_cpy stg
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
    from dwh_foundation.stg_mp_store_tier_cpy
   where sys_process_code = 'N';

  l_text := 'MERGE STARTED AT '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  local_bulk_merge;
  
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
end WH_FND_MP_034U;
