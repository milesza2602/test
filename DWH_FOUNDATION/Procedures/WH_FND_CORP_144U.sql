--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_144U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_144U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        February 2013
--  Author:      Quentin Smit
--  Purpose:     Create zone_item_supp dimention table in the foundation layer
--               with input ex staging table from OM.
--  Tables:      Input  - stg_jdaff_wh_plan
--               Output - FND_LOC_ITEM_JDAFF_WH_PLAN
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  16 May 2016 - Changed key on destination table, removed dc_loc_no and added calendar_date
--                Added zero-checking for the first 3 days when loading the data
--  05 Jul 2016 - Removed all zero-checking on week days when loading data. Replaced with one-to-one 
--                mapping from source STG table (previously day 2 mapping)          Ref: BK05Jul2016
--  August 2016 - Changed to bulk merge to improve performance                    
--              - Quentuin Smit

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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_jdaff_wh_plan_hsp.sys_process_msg%type;
mer_mart             stg_jdaff_wh_plan_CPY%rowtype;
g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_144U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE WHAREHOUSE PLANNING DATA FROM JDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_location_no       stg_jdaff_wh_plan_CPY.location_no%type; 
g_item_no           stg_jdaff_wh_plan_CPY.item_no%TYPE; 
g_calendar_date     stg_jdaff_wh_plan_CPY.calendar_date%TYPE; 

l_date              date;
l_day_no            integer;

   cursor stg_dup is
      select * from stg_jdaff_wh_plan_CPY
      where (location_no, item_no, calendar_date)
      in
      (select location_no,item_no, calendar_date
      from stg_jdaff_wh_plan_CPY 
      group by location_no, item_no, calendar_date
      having count(*) > 1) 
      order by location_no,
      item_no,
      sys_source_batch_id desc ,sys_source_sequence_no desc;
    

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_LOC_ITEM_JDAFF_WH_PLAN EX RMS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
      execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    select count(*)
      into g_recs_read
      from stg_jdaff_wh_plan_CPY                                                                                                                                                                                               
     where sys_process_code = 'N'
   ;
    
    select max(calendar_date) 
      into l_date
      from stg_jdaff_wh_plan_cpy;
     
    l_text := 'Calendar date from JDA - ' || l_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    select fin_day_no 
     into l_day_no
     from dim_calendar 
    where calendar_date = l_date;
    
    case l_day_no
       when 1 then
          l_text := 'Day of week = 1';
       when 2 then
          l_text := 'Day of week = 2';
       when 3 then
          l_text := 'Day of week = 3';
       when 4 then
          l_text := 'Day of week = 4';
       when 5 then
          l_text := 'Day of week = 5';
       when 6 then
          l_text := 'Day of week = 6';
       when 7 then
          l_text := 'Day of week = 7';
    end case;
                    
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
-- De Duplication of the staging table to avoid Bulk insert failures
--************************************************************************************************** 
   l_text := 'DEDUP STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   g_location_no    := 0; 
   g_item_no        := 0;
   g_calendar_date  := '01/JAN/1900';

    for dupp_record in stg_dup
       loop
    
        if  dupp_record.location_no   = g_location_no and
            dupp_record.item_no       = g_item_no  and
            dupp_record.calendar_date = g_calendar_date then
            
            update stg_jdaff_wh_plan_CPY stg
            set    sys_process_code = 'D'
            where  sys_source_batch_id    = dupp_record.sys_source_batch_id and
                   sys_source_sequence_no = dupp_record.sys_source_sequence_no;
             
            g_recs_duplicate  := g_recs_duplicate  + 1;       
        end if;           
    
        g_location_no    := dupp_record.location_no; 
        g_item_no        := dupp_record.item_no;
        g_calendar_date  := dupp_record.calendar_date;
    
    end loop;
       
    commit;
    
    l_text := 'DEDUP ENDED - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--**************************************************************************************************
-- Bulk Merge controlling main program execution
--**************************************************************************************************
    l_text := 'MERGE STARTING - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


  merge /*+ parallel (fnd_mart,6) */ 
    into DWH_FOUNDATION.FND_LOC_ITEM_JDAFF_WH_PLAN fnd_mart 
    using (
    select /*+ FULL(TMP) */ 
        tmp.LOCATION_NO,
        tmp.ITEM_NO,
        tmp.DC_LOC_NO,
        tmp.WEEK_1_DAY_1_CASES,
        tmp.WEEK_1_DAY_2_CASES,
        tmp.WEEK_1_DAY_3_CASES,
        tmp.WEEK_1_DAY_4_CASES,
        tmp.WEEK_1_DAY_5_CASES,
        tmp.WEEK_1_DAY_6_CASES,
        tmp.WEEK_1_DAY_7_CASES,
        tmp.WEEK_2_DAY_1_CASES,
        tmp.WEEK_2_DAY_2_CASES,
        tmp.WEEK_2_DAY_3_CASES,
        tmp.WEEK_2_DAY_4_CASES,
        tmp.WEEK_2_DAY_5_CASES,
        tmp.WEEK_2_DAY_6_CASES,
        tmp.WEEK_2_DAY_7_CASES,
        tmp.WEEK_3_DAY_1_CASES,
        tmp.WEEK_3_DAY_2_CASES,
        tmp.WEEK_3_DAY_3_CASES,
        tmp.WEEK_3_DAY_4_CASES,
        tmp.WEEK_3_DAY_5_CASES,
        tmp.WEEK_3_DAY_6_CASES,
        tmp.WEEK_3_DAY_7_CASES,
        tmp.CALENDAR_DATE,
        tmp.SOURCE_DATA_STATUS_CODE,
        tmp.LAST_UPDATED_DATE
        
    from stg_jdaff_wh_plan_CPY  tmp 
    join fnd_item di           on tmp.item_no     = di.item_no 
    join fnd_location dl       on tmp.location_no = dl.location_no 
   where tmp.sys_process_code = 'N'
  
     ) mer_mart
  
on  (mer_mart.item_no         = fnd_mart.item_no
and  mer_mart.location_no     = fnd_mart.location_no
and  mer_mart.calendar_date   = fnd_mart.calendar_date
    )
when matched then
update
set       dc_loc_no               = mer_mart.dc_loc_no,
          week_1_day_1_cases      = mer_mart.week_1_day_1_cases,
          week_1_day_2_cases      = mer_mart.week_1_day_2_cases,
          week_1_day_3_cases      = mer_mart.week_1_day_3_cases,
          week_1_day_4_cases      = mer_mart.week_1_day_4_cases,
          week_1_day_5_cases      = mer_mart.week_1_day_5_cases,
          week_1_day_6_cases      = mer_mart.week_1_day_6_cases,
          week_1_day_7_cases      = mer_mart.week_1_day_7_cases,
          week_2_day_1_cases      = mer_mart.week_2_day_1_cases,
          week_2_day_2_cases      = mer_mart.week_2_day_2_cases,
          week_2_day_3_cases      = mer_mart.week_2_day_3_cases,
          week_2_day_4_cases      = mer_mart.week_2_day_4_cases,
          week_2_day_5_cases      = mer_mart.week_2_day_5_cases,
          week_2_day_6_cases      = mer_mart.week_2_day_6_cases,
          week_2_day_7_cases      = mer_mart.week_2_day_7_cases,
          week_3_day_1_cases      = mer_mart.week_3_day_1_cases,
          week_3_day_2_cases      = mer_mart.week_3_day_2_cases,
          week_3_day_3_cases      = mer_mart.week_3_day_3_cases,
          week_3_day_4_cases      = mer_mart.week_3_day_4_cases,
          week_3_day_5_cases      = mer_mart.week_3_day_5_cases,
          week_3_day_6_cases      = mer_mart.week_3_day_6_cases,
          week_3_day_7_cases      = mer_mart.week_3_day_7_cases,
          last_updated_date       = g_date,
          source_data_status_code = mer_mart.source_data_status_code
          
WHEN NOT MATCHED THEN
INSERT
(         location_no,
          item_no,
          dc_loc_no,
          week_1_day_1_cases,
          week_1_day_2_cases,
          week_1_day_3_cases,
          week_1_day_4_cases,
          week_1_day_5_cases,
          week_1_day_6_cases,
          week_1_day_7_cases,
          week_2_day_1_cases,
          week_2_day_2_cases,
          week_2_day_3_cases,
          week_2_day_4_cases,
          week_2_day_5_cases,
          week_2_day_6_cases,
          week_2_day_7_cases,
          week_3_day_1_cases,
          week_3_day_2_cases,
          week_3_day_3_cases,
          week_3_day_4_cases,
          week_3_day_5_cases,
          week_3_day_6_cases,
          week_3_day_7_cases,
          calendar_date,
          last_updated_date,
          source_data_status_code
          )
  values
(         mer_mart.location_no,
          mer_mart.item_no,
          mer_mart.dc_loc_no,
          mer_mart.week_1_day_1_cases,
          mer_mart.week_1_day_2_cases,
          mer_mart.week_1_day_3_cases,
          mer_mart.week_1_day_4_cases,
          mer_mart.week_1_day_5_cases,
          mer_mart.week_1_day_6_cases,
          mer_mart.week_1_day_7_cases,
          mer_mart.week_2_day_1_cases,
          mer_mart.week_2_day_2_cases,
          mer_mart.week_2_day_3_cases,
          mer_mart.week_2_day_4_cases,
          mer_mart.week_2_day_5_cases,
          mer_mart.week_2_day_6_cases,
          mer_mart.week_2_day_7_cases,
          mer_mart.week_3_day_1_cases,
          mer_mart.week_3_day_2_cases,
          mer_mart.week_3_day_3_cases,
          mer_mart.week_3_day_4_cases,
          mer_mart.week_3_day_5_cases,
          mer_mart.week_3_day_6_cases,
          mer_mart.week_3_day_7_cases,
          mer_mart.calendar_date,
          g_date,
          mer_mart.source_data_status_code
          )  
  ;
  
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

   l_text := 'MERGE DONE, STARTING HOSPITALISATION CHECKS - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (hsp,2) */ into stg_jdaff_wh_plan_hsp hsp 
   select /*+ FULL(TMP) */  TMP.sys_source_batch_id,
                            TMP.sys_source_sequence_no,
                            sysdate,'Y','DWH',
                            TMP.sys_middleware_batch_id,
                            'INVALID INDICATOR OR REFERENCIAL ERROR ON ITEM or LOC',
                            TMP.ITEM_NO,
                            TMP.LOCATION_NO,
                            TMP.dc_loc_no,
                            TMP.week_1_day_1_cases,
                            TMP.week_1_day_2_cases,
                            TMP.week_1_day_3_cases,
                            TMP.week_1_day_4_cases,
                            TMP.week_1_day_5_cases,
                            TMP.week_1_day_6_cases,
                            TMP.week_1_day_7_cases,
                            TMP.week_2_day_1_cases,
                            TMP.week_2_day_2_cases,
                            TMP.week_2_day_3_cases,
                            TMP.week_2_day_4_cases,
                            TMP.week_2_day_5_cases,
                            TMP.week_2_day_6_cases,
                            TMP.week_2_day_7_cases,
                            TMP.week_3_day_1_cases,
                            TMP.week_3_day_2_cases,
                            TMP.week_3_day_3_cases,
                            TMP.week_3_day_4_cases,
                            TMP.week_3_day_5_cases,
                            TMP.week_3_day_6_cases,
                            TMP.week_3_day_7_cases,
                            TMP.calendar_date,
                            TMP.last_updated_date,
                            TMP.SOURCE_DATA_STATUS_CODE
    from  stg_jdaff_wh_plan_CPY  TMP 
    where not exists
          (select *
           from   fnd_item di
           where  tmp.item_no     = di.item_no )  
         or
         not exists
           (select *
           from   fnd_location dl
           where  tmp.location_no  = dl.location_no )
         
          and sys_process_code = 'N'  
          ;
           
    g_recs_hospital := g_recs_hospital + sql%rowcount;
      
    commit;
           
    l_text := 'HOSPITALISATION CHECKS COMPLETE - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --------------------------------------------------------------------------------------------------------

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;          
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
end wh_fnd_corp_144u;
