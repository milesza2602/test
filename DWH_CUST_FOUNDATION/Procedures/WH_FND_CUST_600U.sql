--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_600U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_600U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2016
--  Author:      Mariska Matthee
--  Purpose:     Load customer subscriber information into a fact table in the foundation layer
--               with input staging table.
--  Tables:      Input  - stg_svoc_mapping_cpy
--               Output - fnd_svoc_mapping
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
-- 1
-- 2
-- 3
-- 4
-- 5
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

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
g_date               date          := trunc(sysdate);
g_stmt               varchar(500);

g_subscriber_key     stg_svoc_mapping_cpy.subscriber_key%type;
g_source             stg_svoc_mapping_cpy.source%type;
g_source_key         stg_svoc_mapping_cpy.source_key%type;

l_message            sys_dwh_errlog.log_text%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_600U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST SUBSCRIBER KEY INFORMATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor stg_dup is
select *
  from dwh_cust_foundation.stg_svoc_mapping_cpy
 where (subscriber_key,source,source_key) in (select subscriber_key,source,source_key
                                                from dwh_cust_foundation.stg_svoc_mapping_cpy
                                               group by subscriber_key,source,source_key
                                              having count(*) > 1)
 order by subscriber_key,
          source,
          source_key,
          sys_source_batch_id desc,
          sys_source_sequence_no desc;

cursor c_stg_svoc_mapping is
select /*+ FULL(cpy)  parallel (cpy,2) */
       cpy.*
  from dwh_cust_foundation.stg_svoc_mapping_cpy cpy,
       dwh_cust_foundation.fnd_svoc_mapping fnd
 where cpy.subscriber_key  = fnd.subscriber_key and
       cpy.source = fnd.source and
       cpy.source_key = fnd.source_key and
       cpy.sys_process_code = 'N'
-- Any further validation goes in here - like xxx.ind in (0,1) ---
 order by cpy.subscriber_key,
          cpy.source,
          cpy.source_key,
          cpy.sys_source_batch_id,
          cpy.sys_source_sequence_no;

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin
  g_subscriber_key := '';
  g_source := '';
  g_source_key := '';

  for dupp_record in stg_dup
  loop

    if dupp_record.subscriber_key = g_subscriber_key and
       dupp_record.source = g_source and
       dupp_record.source_key = g_source_key then

        update dwh_cust_foundation.stg_svoc_mapping_cpy stg
           set sys_process_code = 'D'
         where sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;

    g_subscriber_key := dupp_record.subscriber_key;
    g_source := dupp_record.source;
    g_source_key := dupp_record.source_key;
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

  insert /*+ APPEND parallel (fnd,2) */ into dwh_cust_foundation.fnd_svoc_mapping fnd
  select /*+ FULL(cpy)  parallel (cpy,2) */
         cpy.subscriber_key,
         cpy.source,
         cpy.source_key,
         g_date as last_updated_date
    from dwh_cust_foundation.stg_svoc_mapping_cpy cpy
   where not exists (select /*+ nl_aj */ *
                       from dwh_cust_foundation.fnd_svoc_mapping
                      where subscriber_key = cpy.subscriber_key
                        and source = cpy.source
                        and source_key = cpy.source_key)
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
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_update as
begin
  for upd_rec in c_stg_svoc_mapping
  loop
    update dwh_cust_foundation.fnd_svoc_mapping fnd
       set fnd.last_updated_date = g_date
     where fnd.subscriber_key =	upd_rec.subscriber_key
       and fnd.source         =	upd_rec.source
       and fnd.source_key     =	upd_rec.source_key;

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
    into g_recs_read
    from dwh_cust_foundation.stg_svoc_mapping_cpy
   where sys_process_code = 'N';

  -- if the stg cpy table has no data, do not truncate the foundation table
  -- every day a full extract off data will be received, if no file lands keep the existing data
  if g_recs_read = 0 then
    l_text := 'NO STAGING DATA LOADED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    p_success := true;
    return;
  end if;

  l_text := 'TRUNCATE TABLE DWH_CUST_FOUNDATION.FND_SVOC_MAPPING';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  g_stmt := 'TRUNCATE TABLE DWH_CUST_FOUNDATION.FND_SVOC_MAPPING';
  execute immediate g_stmt;

  l_text := 'UPDATE STATS ON DWH_CUST_FOUNDATION.FND_SVOC_MAPPING';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dbms_stats.gather_table_stats ('DWH_CUST_FOUNDATION','FND_SVOC_MAPPING',estimate_percent=>1, DEGREE => 32);
  commit;

  --l_text := 'BULK UPDATE STARTED AT '||
  --to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --flagged_records_update;

  l_text := 'BULK INSERT STARTED AT '||
  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  flagged_records_insert;

--    Taken out for better performance --------------------
--    update dwh_cust_foundation.stg_svoc_mapping_cpy
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
  l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
  l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
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
    raise;
end wh_fnd_cust_600u;
/
show errors