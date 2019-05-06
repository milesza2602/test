--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_731U_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_731U_MERGE" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLANNING data to JDAFF fact table in the performance layer
--               with input ex JDAFF fnd_loc_item_om_wh_plan table from foundation layer.
--
--  Tables:      Input  - fnd_loc_item_om_wh_plan
--               Output - dwh_performance.rtl_loc_item_dc_wh_plan
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
--g_cases              rtl_loc_item_dc_wh_plan.dc_plan_store_cases%type;
g_rec_out            rtl_loc_item_dc_wh_plan%rowtype;
g_found              boolean;
g_date               date;
g_om_date            date;
--g_start_date         date;
--g_end_date           date;
g_today_day          number;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_731U_MERGE';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH DC PLAN FACT DATA FROM OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Bulk MERGE
--**************************************************************************************************
procedure local_bulk_merge as
begin
 
--merge /*+ PARALLEL(rtl_dcwhp,4) */ into rtl_loc_item_dc_wh_plan rtl_dcwhp USING
merge /*+ PARALLEL(rtl_dcwhp,4) */ into W6005682.RTL_LOC_ITEM_DC_WH_PLAQ rtl_dcwhp USING

(select     /*+ PARALLEL(jdaff,4) */
            dl.sk1_location_no, 
            di.sk1_item_no,
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
            week_3_day_7_cases

   from     dwh_foundation.fnd_loc_item_om_wh_plan jdaff,
            dim_location dl,
            dim_item di,
            fnd_jdaff_dept_rollout jda

   where jdaff.location_no       = dl.location_no
     and jdaff.item_no           = di.item_no
     and jda.department_no       = di.department_no
     and jda.department_live_ind = 'N'

) mer_dcwhp

on  (rtl_dcwhp.sk1_location_no  = mer_dcwhp.sk1_location_no
and rtl_dcwhp.sk1_item_no       = mer_dcwhp.sk1_item_no
and rtl_dcwhp.calendar_date     = g_om_date)

when matched then
update
set
       week_1_day_1_cases              = mer_dcwhp.week_1_day_1_cases,
       week_1_day_2_cases              = mer_dcwhp.week_1_day_2_cases,
       week_1_day_3_cases              = mer_dcwhp.week_1_day_3_cases,
       week_1_day_4_cases              = mer_dcwhp.week_1_day_4_cases,
       week_1_day_5_cases              = mer_dcwhp.week_1_day_5_cases,
       week_1_day_6_cases              = mer_dcwhp.week_1_day_6_cases,
       week_1_day_7_cases              = mer_dcwhp.week_1_day_7_cases,
       week_2_day_1_cases              = mer_dcwhp.week_2_day_1_cases,
       week_2_day_2_cases              = mer_dcwhp.week_2_day_2_cases,
       week_2_day_3_cases              = mer_dcwhp.week_2_day_3_cases,
       week_2_day_4_cases              = mer_dcwhp.week_2_day_4_cases,
       week_2_day_5_cases              = mer_dcwhp.week_2_day_5_cases,
       week_2_day_6_cases              = mer_dcwhp.week_2_day_6_cases,
       week_2_day_7_cases              = mer_dcwhp.week_2_day_7_cases,
       week_3_day_1_cases              = mer_dcwhp.week_3_day_1_cases,
       week_3_day_2_cases              = mer_dcwhp.week_3_day_2_cases,
       week_3_day_3_cases              = mer_dcwhp.week_3_day_3_cases,
       week_3_day_4_cases              = mer_dcwhp.week_3_day_4_cases,
       week_3_day_5_cases              = mer_dcwhp.week_3_day_5_cases,
       week_3_day_6_cases              = mer_dcwhp.week_3_day_6_cases,
       week_3_day_7_cases              = mer_dcwhp.week_3_day_7_cases,
       last_updated_date               = g_date
       
when not matched then
insert
(      sk1_location_no,
       sk1_item_no,
       calendar_date,
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
       last_updated_date
)
values
(      mer_dcwhp.sk1_location_no,
       mer_dcwhp.sk1_item_no,
       g_om_date,
       mer_dcwhp.week_1_day_1_cases,
       mer_dcwhp.week_1_day_2_cases,
       mer_dcwhp.week_1_day_3_cases,
       mer_dcwhp.week_1_day_4_cases,
       mer_dcwhp.week_1_day_5_cases,
       mer_dcwhp.week_1_day_6_cases,
       mer_dcwhp.week_1_day_7_cases,
       mer_dcwhp.week_2_day_1_cases,
       mer_dcwhp.week_2_day_2_cases,
       mer_dcwhp.week_2_day_3_cases,
       mer_dcwhp.week_2_day_4_cases,
       mer_dcwhp.week_2_day_5_cases,
       mer_dcwhp.week_2_day_6_cases,
       mer_dcwhp.week_2_day_7_cases,
       mer_dcwhp.week_3_day_1_cases,
       mer_dcwhp.week_3_day_2_cases,
       mer_dcwhp.week_3_day_3_cases,
       mer_dcwhp.week_3_day_4_cases,
       mer_dcwhp.week_3_day_5_cases,
       mer_dcwhp.week_3_day_6_cases,
       mer_dcwhp.week_3_day_7_cases,
       G_DATE
);

    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
    g_recs_updated  :=  g_recs_updated + SQL%ROWCOUNT;
    g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;

  commit;
  
  exception
      when dwh_errors.e_insert_error then
       l_message := 'MAIN MERGE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'MAIN MERGE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
end local_bulk_merge;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF rtl_loc_item_dc_wh_plan EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    g_date    := g_date + 1;
    g_om_date := g_date + 1;    --OM CALENDAR DATE MUST TIE UP WITH CALENDAR DATE THAT JDA WILL LOAD WHICH IS 1 DAY AHEAD

--    select this_week_start_date, fin_year_no, fin_week_no, fin_day_no
--    into   g_start_date,         g_year1,     g_week1,     g_today_day
--    from   dim_calendar
--    where  calendar_date = g_date;

    --g_end_date := g_start_date + 20;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    l_text := 'DATA PERIOD - '||g_start_date||' to '|| g_end_date;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    l_text := 'CURRENT CALENDAR DATE USED FOR RECORDS INSERTED / UPDATED - '||g_start_date;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'MERGE STARTING';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    local_bulk_merge;
    
    l_text := 'MERGE DONE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_corp_731U_merge;
