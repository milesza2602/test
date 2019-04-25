--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_762U_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_762U_MERGE" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2009
--  Author:      M Munnik
--  Purpose:     Update DC PLANNING data to OM fact table in the performance layer
--               with input ex OM fnd_loc_item_jdaff_wh_plan table from foundation layer.
--               On a Monday, week 1 on the input table belongs to previous week.
--               Therefore, on Mondays, only week 2 and week 3 are updated to the output table.
--  Tables:      Input  - fnd_loc_item_jdaff_wh_plan
--               Output - w6005682.rtl_loc_item_dy_wh_planq
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
g_recs_deleted       integer       :=  0;
g_recs               integer       :=  0;
g_cases              w6005682.rtl_loc_item_dy_wh_planq.dc_plan_store_cases%type;
g_rec_out            w6005682.rtl_loc_item_dy_wh_planq%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_today_day          number;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_762M';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PLAN FACT DATA FROM JDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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
    l_text := 'LOAD OF w6005682.rtl_loc_item_dy_wh_planq EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    
    select this_week_start_date, fin_year_no, fin_week_no, fin_day_no
    into   g_start_date,         g_year1,     g_week1,     g_today_day
    from   dim_calendar
    where  calendar_date = g_date;
    
    if g_today_day = 1 then
       g_end_date   := g_start_date + 13;
       g_year2      := g_year1;
       g_week2      := g_week1;
       select fin_year_no, fin_week_no
       into   g_year1,     g_week1
       from   dim_calendar
       where  calendar_date = g_start_date - 7;
    else
       g_end_date := g_start_date + 20;
       select fin_year_no, fin_week_no
       into   g_year2,     g_week2
       from   dim_calendar
       where  calendar_date = g_start_date + 7;
    end if;
    
    select fin_year_no, fin_week_no
    into   g_year3,     g_week3
    from   dim_calendar
    where  calendar_date = g_end_date;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DATA PERIOD - '||g_start_date||' to '|| g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


  MERGE /*+ parallel(rtl,8) */ into w6005682.rtl_loc_item_dy_wh_planq rtl using
  (
   SELECT /*+ parallel(jdaff,4) full(di) full(dl) full(dlh) full(dih) full(dc) */
            DL.SK1_LOCATION_NO, DI.SK1_ITEM_NO, DC.CALENDAR_DATE, DLH.SK2_LOCATION_NO, DIH.SK2_ITEM_NO,
            sum(nvl(owp.sysdata,0)) dc_plan_store_cases    , trunc(sysdate) last_updated_date
   from
   (select  /*+ parallel(jdaff,4)*/
            location_no,
            item_no,
            (case (to_number(substr(syscol,6,1))) when 1 then g_year1 when 2 then g_year2 else g_year3 end) yearno,
            (case (to_number(substr(syscol,6,1))) when 1 then g_week1 when 2 then g_week2 else g_week3 end) weekno,
            to_number(substr(syscol,12,1)) dayno,
            syscol,
            sysdata
   from     dwh_foundation.fnd_loc_item_jdaff_wh_plan jdaff
   unpivot  include nulls (sysdata for syscol in (week_1_day_1_cases,
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
                                                  week_3_day_7_cases))) owp
                                                  
   join     dim_calendar dc         on  owp.yearno      =  dc.fin_year_no
                                    and owp.weekno      =  dc.fin_week_no
                                    and owp.dayno       =  dc.fin_day_no
   join     dim_location dl         on  owp.location_no =  dl.location_no
   join     dim_location_hist dlh   on  owp.location_no =  dlh.location_no
                                    and dc.calendar_date   between dlh.sk2_active_from_date and dlh.sk2_active_to_date
   join     dim_item di             on  owp.item_no     =  di.item_no
   join     dim_item_hist dih       on  owp.item_no     =  dih.item_no
                                    and dc.calendar_date   between dih.sk2_active_from_date and dih.sk2_active_to_date
   WHERE    DC.CALENDAR_DATE                               BETWEEN G_START_DATE AND G_END_DATE
   group by  DL.SK1_LOCATION_NO, DI.SK1_ITEM_NO, DC.CALENDAR_DATE, DLH.SK2_LOCATION_NO, DIH.SK2_ITEM_NO
  
  ) mer_mart
  
  on (rtl.sk1_item_no     = mer_mart.sk1_item_no
  and rtl.calendar_date   = mer_mart.calendar_date
  and rtl.sk1_location_no = mer_mart.sk1_location_no
  
  and rtl.calendar_date between g_start_date and g_end_date )
  
  when matched then
    update set
      sk2_location_no     = mer_mart.sk2_location_no,
      sk2_item_no         = mer_mart.sk2_item_no,
      dc_plan_store_cases = mer_mart.dc_plan_store_cases,
      last_updated_date   = g_date
  
  when not matched then
    insert 
    ( SK1_LOCATION_NO,
      SK1_ITEM_NO,
      CALENDAR_DATE,
      SK2_LOCATION_NO,
      SK2_ITEM_NO,
      DC_PLAN_STORE_CASES,
      LAST_UPDATED_DATE
    )
    values
    ( mer_mart.SK1_LOCATION_NO,
      mer_mart.SK1_ITEM_NO,
      mer_mart.CALENDAR_DATE,
      mer_mart.SK2_LOCATION_NO,
      mer_mart.SK2_ITEM_NO,
      mer_mart.DC_PLAN_STORE_CASES,
      g_date
    )
    ;
    
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  commit;
      

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

end wh_prf_corp_762u_merge;
