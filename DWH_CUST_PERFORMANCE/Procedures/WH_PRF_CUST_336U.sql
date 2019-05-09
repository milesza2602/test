--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_336U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_336U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        OCT 2017
--  Author:      Alastair de Wet
--  Purpose:     Create cust_mart_store_REV_BY_SUBURB fact table in the performance layer
--               with input ex cust_CSM_st_wk table from performance layer.
--               THIS JOB RUNS QUARTERLY AFTER THE START OF A NEW QUARTER
--  Tables:      Input  - cust_csm_st_wk
--               Output - cust_mart_store_rev_by_suburb
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            cust_mart_store_rev_by_suburb%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);

g_start_week         number         ;
g_end_week           number         ;
g_end_month          number         ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_stmt               varchar2(300);
g_yr_00              number;
g_qt_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_336U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE CSM STORE WK to STORE REV BY SUBURB MART';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

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

    l_text := 'ROLLUP OF cust_mart_store_rev_by_suburb EX cust_st_wk LEVEL STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
-- Determine if this is a day on which we process
--**************************************************************************************************    

   
   if to_char(sysdate,'DDMM') NOT IN  ('1107','1110','1101','1104') then
      l_text      := 'This job only runs QUARTERLY ON THE 11TH and today '||to_char(sysdate,'DDMM')||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;  
   l_text      := 'This job only runs on '||to_char(sysdate,'DDMM')||' and today '||to_char(sysdate,'DDMM')||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  


--**************************************************************************************************
-- Main loop
--**************************************************************************************************


    select fin_year_no,fin_quarter_no
    into   g_yr_00,g_qt_00
    from   dim_calendar
    where  calendar_date = g_date - 80;
    
    if g_qt_00 = 1 then
      g_start_week := 1;
      g_end_week   := 13;
      g_end_month  := 3;
    end if; 
    if g_qt_00 = 2 then
      g_start_week := 14;
      g_end_week   := 26;
      g_end_month  := 6;      
    end if; 
    if g_qt_00 = 3 then
      g_start_week := 27;
      g_end_week   := 39;
      g_end_month  := 9;      
    end if; 
    if g_qt_00 = 4 then
      g_start_week := 40;
       SELECT MAX(FIN_WEEK_NO) INTO g_end_week FROM DIM_CALENDAR_WK WHERE FIN_YEAR_NO = g_yr_00;
      g_end_month  := 12;      
    end if;     

    l_text := 'ROLLUP WEEK RANGE IS:- '||g_start_week||'  to '||g_end_week||' of '|| g_yr_00||' '|| g_end_month;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

    delete from cust_mart_store_rev_by_suburb where fin_year_no = g_yr_00 and fin_quarter_no =  g_qt_00;

    l_text := 'TABLE CLEARED FOR NEW QUARTER INSERTS '  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
commit;
insert /*+ APPEND  */ into cust_mart_store_rev_by_suburb csm

WITH      st_qtr as ( 
select /*+ FULL(stw) FULL(dc)  parallel (stw,8) parallel (dc,8) */
         stw.csm_customer_identifier,
         stw.location_no,
--         count(distinct stw.csm_customer_identifier) num_customers,
         max(dc.ea_code) ea_code,
         sum(stw.csm_num_st_dy_visit) num_visits,
         sum(stw.csm_num_item) num_items,
         sum(stw.csm_basket_value) basket_value
from     cust_csm_st_wk stw,
         dim_customer dc 
where    stw.fin_year_no       = g_yr_00 
and      stw.fin_week_no       between g_start_week and g_end_week
and      stw.csm_customer_identifier = dc.customer_no
and      dc.ea_code is not null
and      stw.csm_customer_identifier <> 998 
group by stw.csm_customer_identifier,
         stw.location_no
              )
select  /*+ FULL(a)  FULL(ea)  parallel (a,8)  parallel (ea,8) */
         g_yr_00,
         g_qt_00,
         a.location_no,
         nvl(ea.town_name,'NO TOWN'),
         nvl(ea.suburb_name,'NO SUBURB'),
         sum(a.basket_value) basket_value,
         sum(a.num_items) num_items,
         sum(a.num_visits) num_visits,
         count(distinct a.csm_customer_identifier) num_customers,
         g_date
from     st_qtr a
join
         fnd_lst_ea_codes ea
on       a.ea_code = ea.ea_code  

group by a.location_no,town_name,suburb_name 
order by a.location_no
         ;

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


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
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_cust_336u;
