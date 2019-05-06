--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_231U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_231U" (p_forall_limit in integer,p_success out boolean) as 

--**************************************************************************************************
--  Date:        June 2012
--  Author:      Quentin Smit
--  Purpose:     Roll ROS data from day to week
--  Tables:      Input  -   rtl_loc_item_dy_rate_of_sale
--               Output -   dwh_performance.rtl_loc_item_wk_rate_of_saleX
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  
--  September 2016 - QS - Rewritten to bulk merge to improve performance
--
--  Naming conventions:
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dwh_performance.rtl_loc_item_wk_rate_of_sale%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
 
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_231U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL RATE OF SALE FROM DAY TO WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_fin_year_no        number;
l_fin_week_no        number;
l_last_wk_start_date date;   -- := trunc(sysdate) - 18;   --41
l_last_wk_end_date   date;
l_from_date          date;  -- := trunc(sysdate) - 7;   --43
l_to_date            date;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin 

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'ROLL OF dwh_performance.rtl_loc_item_wk_rate_of_saleX STARTED AT '||
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
    l_to_date := g_date;
    
    --l_last_wk_end_date := g_date;
   
   select last_wk_start_date,
          last_wk_end_date
     into l_last_wk_start_date,
          l_last_wk_end_date
     from dim_control_report;
      
      l_from_date := l_last_wk_start_date - 14;
      l_to_date   := l_last_wk_end_date;

    
    l_text := 'Start date of period being processed :- '|| l_from_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'End date of period being processed :- '|| l_to_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  
merge /*+ parallel(ros,4) */ into dwh_performance.rtl_loc_item_wk_rate_of_sale ros using
 (
   select /*+ full(f) parallel(f,4) full(dc) */
          dc.fin_year_no, dc.fin_week_no, sk1_location_no, sk1_item_no, 
          sum(units_per_day) units_per_week,
          avg(units_per_day) avg_units_per_day
    from dwh_performance.rtl_loc_item_dy_rate_of_sale f,
         dim_calendar dc
   where f.calendar_date between l_from_date and l_to_date
     and f.calendar_date = dc.calendar_date
   group by dc.fin_year_no, dc.fin_week_no, f.sk1_location_no, f.sk1_item_no
  ) mer_rate
  
  on (mer_rate.sk1_location_no    = ros.sk1_location_no  
 and  mer_rate.sk1_item_no        = ros.sk1_item_no
 and  mer_rate.fin_year_no        = ros.fin_year_no
 and  mer_rate.fin_week_no        = ros.fin_week_no)

when matched then 
  update set 
    units_per_week    = mer_rate.units_per_week,
    avg_units_per_day = mer_rate.avg_units_per_day,
    last_updated_date = g_date
    
when not matched then
  insert
  ( SK1_LOCATION_NO,
    SK1_ITEM_NO,
    FIN_YEAR_NO,
    FIN_WEEK_NO,
    UNITS_PER_WEEK,
    AVG_UNITS_PER_DAY,
    LAST_UPDATED_DATE)
  
  values
  ( mer_rate.SK1_LOCATION_NO,
    mer_rate.SK1_ITEM_NO,
    mer_rate.FIN_YEAR_NO,
    mer_rate.FIN_WEEK_NO,
    mer_rate.UNITS_PER_WEEK,
    mer_rate.AVG_UNITS_PER_DAY,
    g_date
  );
   
  g_recs_read:=SQL%ROWCOUNT;
 
--************************************************************************************************** 
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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
end wh_prf_corp_231u;
