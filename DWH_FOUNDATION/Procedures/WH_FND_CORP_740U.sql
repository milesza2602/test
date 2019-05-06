--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_740U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_740U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2013
--  Author:      Alastair de Wet
--  Create DC PLAN DATA in the foundation layer
--               with input ex staging table from OM.
--  Tables:      Input  - stg_om_wh_plan_cpy
--               Output - fnd_loc_item_om_wh_plan
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  20 Mar 2013 - Change to a BULK Insert/update load to speed up 5x
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.

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
g_recs_duplicate     integer       :=  0;  --Bulk Load--
g_truncate_count     integer       :=  0;

g_location_no        stg_om_wh_plan_cpy.location_no%type; --Bulk Load--
g_item_no            stg_om_wh_plan_cpy.item_no%type;     --Bulk Load--

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_740U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD DC PLAN DATA EX OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

/*
cursor stg_dup is
select * from stg_om_wh_plan_cpy
where (location_no,item_no)
in
(select location_no,item_no
from stg_om_wh_plan_cpy
group by location_no,item_no
having count(*) > 1)
order by location_no,item_no,sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg_om_wh_plan is
select        stg.location_no,
              stg.item_no,
              stg.week_1_day_1_cases,
              stg.week_1_day_2_cases,
              stg.week_1_day_3_cases,
              stg.week_1_day_4_cases,
              stg.week_1_day_5_cases,
              stg.week_1_day_6_cases,
              stg.week_1_day_7_cases,
              stg.week_2_day_1_cases,
              stg.week_2_day_2_cases,
              stg.week_2_day_3_cases,
              stg.week_2_day_4_cases,
              stg.week_2_day_5_cases,
              stg.week_2_day_6_cases,
              stg.week_2_day_7_cases,
              stg.week_3_day_1_cases,
              stg.week_3_day_2_cases,
              stg.week_3_day_3_cases,
              stg.week_3_day_4_cases,
              stg.week_3_day_5_cases,
              stg.week_3_day_6_cases,
              stg.week_3_day_7_cases,
              stg.source_data_status_code
      from    stg_om_wh_plan_cpy stg,
              fnd_loc_item_om_wh_plan fnd
      where   stg.location_no  = fnd.location_no and
              stg.item_no      = fnd.item_no     and
              stg.sys_process_code = 'N'
      order by
              stg.location_no,stg.item_no,stg.sys_source_batch_id,stg.sys_source_sequence_no ;
*/

cursor stg_dup is
select * from stg_om_wh_plan_cpy 
where (location_no, item_no)
in
(select a.location_no, a.item_no
from stg_om_wh_plan_cpy a, dim_item b, fnd_jdaff_dept_rollout c
  where a.item_no = b.item_no
    and b.department_no = c.department_no
    and c.department_live_ind = 'N'
group by a.location_no,a.item_no
having count(*) > 1)
order by  location_no,  item_no, sys_source_batch_id desc ,sys_source_sequence_no desc;

cursor c_stg_om_wh_plan is
select        stg.location_no,
              stg.item_no,
              stg.week_1_day_1_cases,
              stg.week_1_day_2_cases,
              stg.week_1_day_3_cases,
              stg.week_1_day_4_cases,
              stg.week_1_day_5_cases,
              stg.week_1_day_6_cases,
              stg.week_1_day_7_cases,
              stg.week_2_day_1_cases,
              stg.week_2_day_2_cases,
              stg.week_2_day_3_cases,
              stg.week_2_day_4_cases,
              stg.week_2_day_5_cases,
              stg.week_2_day_6_cases,
              stg.week_2_day_7_cases,
              stg.week_3_day_1_cases,
              stg.week_3_day_2_cases,
              stg.week_3_day_3_cases,
              stg.week_3_day_4_cases,
              stg.week_3_day_5_cases,
              stg.week_3_day_6_cases,
              stg.week_3_day_7_cases,
              stg.source_data_status_code
      from    stg_om_wh_plan_cpy stg,
              fnd_loc_item_om_wh_plan fnd,
              dim_item b, 
              fnd_jdaff_dept_rollout c
      where   stg.location_no  = fnd.location_no and
              stg.item_no      = fnd.item_no     and
              stg.sys_process_code = 'N'
              and stg.item_no = b.item_no
              and b.department_no = c.department_no
              and c.department_live_ind = 'N'
      order by
              stg.location_no,stg.item_no,stg.sys_source_batch_id,stg.sys_source_sequence_no ;
 

--**************************************************************************************************
-- Eliminate duplicates on the very rare occasion they may be present
--**************************************************************************************************
procedure remove_duplicates as
begin

   g_location_no    := 0;
   g_item_no        := 0;

for dupp_record in stg_dup
   loop

    if  dupp_record.location_no   = g_location_no and
        dupp_record.item_no       = g_item_no     then
        update stg_om_wh_plan_cpy stg
        set    sys_process_code = 'D'
        where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
               sys_source_sequence_no = dupp_record.sys_source_sequence_no;

        g_recs_duplicate  := g_recs_duplicate  + 1;
    end if;


    g_location_no    := dupp_record.location_no;
    g_item_no        := dupp_record.item_no;

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

      insert /*+ APPEND parallel (fnd,2) */ into fnd_loc_item_om_wh_plan fnd
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.location_no ,
             cpy.item_no ,
             cpy.week_1_day_1_cases,
             cpy.week_1_day_2_cases,
             cpy.week_1_day_3_cases,
             cpy.week_1_day_4_cases,
             cpy.week_1_day_5_cases,
             cpy.week_1_day_6_cases,
             cpy.week_1_day_7_cases,
             cpy.week_2_day_1_cases,
             cpy.week_2_day_2_cases,
             cpy.week_2_day_3_cases,
             cpy.week_2_day_4_cases,
             cpy.week_2_day_5_cases,
             cpy.week_2_day_6_cases,
             cpy.week_2_day_7_cases,
             cpy.week_3_day_1_cases,
             cpy.week_3_day_2_cases,
             cpy.week_3_day_3_cases,
             cpy.week_3_day_4_cases,
             cpy.week_3_day_5_cases,
             cpy.week_3_day_6_cases,
             cpy.week_3_day_7_cases,
             cpy.source_data_status_code,
             g_date as last_updated_date
      from   stg_om_wh_plan_cpy cpy,
             fnd_item itm,
             fnd_location loc
       where cpy.location_no    = loc.location_no
       and   cpy.item_no        = itm.item_no
       and   not exists
      (select * from fnd_loc_item_om_wh_plan
       where  location_no   = cpy.location_no and
              item_no       = cpy.item_no )
       and sys_process_code = 'N';

--      g_recs_read     := g_recs_read     + sql%rowcount;
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



for upd_rec in c_stg_om_wh_plan
   loop
     update fnd_loc_item_om_wh_plan fnd
     set    fnd.week_1_day_1_cases      = upd_rec.week_1_day_1_cases,
            fnd.week_1_day_2_cases      = upd_rec.week_1_day_2_cases,
            fnd.week_1_day_3_cases      = upd_rec.week_1_day_3_cases,
            fnd.week_1_day_4_cases      = upd_rec.week_1_day_4_cases,
            fnd.week_1_day_5_cases      = upd_rec.week_1_day_5_cases,
            fnd.week_1_day_6_cases      = upd_rec.week_1_day_6_cases,
            fnd.week_1_day_7_cases      = upd_rec.week_1_day_7_cases,
            fnd.week_2_day_1_cases      = upd_rec.week_2_day_1_cases,
            fnd.week_2_day_2_cases      = upd_rec.week_2_day_2_cases,
            fnd.week_2_day_3_cases      = upd_rec.week_2_day_3_cases,
            fnd.week_2_day_4_cases      = upd_rec.week_2_day_4_cases,
            fnd.week_2_day_5_cases      = upd_rec.week_2_day_5_cases,
            fnd.week_2_day_6_cases      = upd_rec.week_2_day_6_cases,
            fnd.week_2_day_7_cases      = upd_rec.week_2_day_7_cases,
            fnd.week_3_day_1_cases      = upd_rec.week_3_day_1_cases,
            fnd.week_3_day_2_cases      = upd_rec.week_3_day_2_cases,
            fnd.week_3_day_3_cases      = upd_rec.week_3_day_3_cases,
            fnd.week_3_day_4_cases      = upd_rec.week_3_day_4_cases,
            fnd.week_3_day_5_cases      = upd_rec.week_3_day_5_cases,
            fnd.week_3_day_6_cases      = upd_rec.week_3_day_6_cases,
            fnd.week_3_day_7_cases      = upd_rec.week_3_day_7_cases,
            fnd.source_data_status_code = upd_rec.source_data_status_code,
            fnd.last_updated_date       = g_date
     where  fnd.location_no       = upd_rec.location_no and
            fnd.item_no           = upd_rec.item_no;

     g_recs_updated := g_recs_updated + sql%rowcount;
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
procedure flagged_records_hospital as
begin
 --     g_rec_out.last_updated_date         := g_date;


      insert /*+ APPEND parallel (hsp,2) */ into stg_om_wh_plan_hsp hsp
      select /*+ FULL(cpy)  parallel (cpy,2) */
             cpy.sys_source_batch_id,
             cpy.sys_source_sequence_no,
             sysdate,'Y','DWH',
             cpy.sys_middleware_batch_id,
             'VALIDATION FAIL - REFERENCIAL ERROR',
             cpy.location_no ,
             cpy.item_no ,
             cpy.week_1_day_1_cases,
             cpy.week_1_day_2_cases,
             cpy.week_1_day_3_cases,
             cpy.week_1_day_4_cases,
             cpy.week_1_day_5_cases,
             cpy.week_1_day_6_cases,
             cpy.week_1_day_7_cases,
             cpy.week_2_day_1_cases,
             cpy.week_2_day_2_cases,
             cpy.week_2_day_3_cases,
             cpy.week_2_day_4_cases,
             cpy.week_2_day_5_cases,
             cpy.week_2_day_6_cases,
             cpy.week_2_day_7_cases,
             cpy.week_3_day_1_cases,
             cpy.week_3_day_2_cases,
             cpy.week_3_day_3_cases,
             cpy.week_3_day_4_cases,
             cpy.week_3_day_5_cases,
             cpy.week_3_day_6_cases,
             cpy.week_3_day_7_cases,
             cpy.source_data_status_code
      from   stg_om_wh_plan_cpy cpy
      where
      (
      not exists
        (select * from fnd_item itm
         where  cpy.item_no       = itm.item_no ) or
      not exists
        (select * from fnd_location loc
         where  cpy.location_no       = loc.location_no )
      )
      and sys_process_code = 'N';

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


-- DETERMINE IF RECORDS EXIST IN FOUNDATION LAYER BEFORE TRUNCATING PERFORMANCE LAYER TABLE
    select count(1) into g_truncate_count
    from   stg_om_wh_plan_cpy;

    if g_truncate_count > 0 then
    begin
        l_text := 'TRUNCATED TABLE fnd_loc_item_om_wh_plan '||
        to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        execute immediate 'truncate table fnd_loc_item_om_wh_plan';
    end;
    else
        l_text := 'TABLE fnd_loc_item_om_wh_plan NOT TRUNCATED - no data in staging  '||
        to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;

--**************************************************************************************************
-- Call the bulk routines
--**************************************************************************************************


    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    remove_duplicates;

    select count(*)
    into   g_recs_read
    from   stg_om_wh_plan_cpy
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_update;

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_hospital;

--    Taken out for better performance --------------------
--    update stg_om_wh_plan_cpy
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
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_recs_read <> g_recs_inserted + g_recs_updated + g_recs_hospital then
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
end wh_fnd_corp_740u;
