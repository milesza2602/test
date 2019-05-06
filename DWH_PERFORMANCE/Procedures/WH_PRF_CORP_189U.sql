--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_189U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_189U" (p_forall_limit in integer, p_success out boolean)
                           --      p_from_loc_no in integer,  p_to_loc_no in integer)
as
--**************************************************************************************************
--  Date:        Mar 2017
--  Author:      Barry Kirschner 
--  Purpose:     Create extract summary table for ISO - Store Orders SSRS report (moegamat)
--  Tables:      Input  - RTL_JDAFF_ST_PLAN_ANALYSIS_DY
--               Output - RTL_LOC_ITEM_DY_PLAN_SNAPSHOT
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

g_forall_limit       integer       := dwh_constants.vc_forall_limit;
g_recs_read          integer       := 0;
g_recs_updated       integer       := 0;
g_recs_inserted      integer       := 0;
g_recs_hospital      integer       := 0;
g_count              number        := 0;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_max_week           number        := 0;
g_batch_date         date          := trunc(sysdate);
g_cfin_week_no       number        := 0;
g_cfin_year_no       number        := 0;
g_pfin_week_no       number        := 0;
g_pfin_year_no       number        := 0;
g_start_date         date;
g_end_date           date;

g_from_loc_no        integer       := 0;  
g_to_loc_no          integer       := 99999;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_189U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD PRF - ISO Store Orders summary extract table';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


begin
    ----------------------------------------
    -- A. Initialize
    ----------------------------------------
    if  p_forall_limit is not null 
    and p_forall_limit > dwh_constants.vc_forall_minimum then
        g_forall_limit := p_forall_limit;
    end if;
    
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD PRF - ISO Store Orders summary extract table STARTED AT '||  
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    -- Look up batch date from dim_control ...
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOCATION RANGE BEING PROCESSED - '||g_from_loc_no||' to '||g_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    execute immediate 'truncate table dwh_performance.RTL_LOC_ITEM_DY_PLAN_SNAPSHOT';
    l_text := 'DWH_PERFORMANCE.ISO_STORE_ORDERS_AGGR TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    ----------------------------------------------------------------------------------------------
    
    -- get current date/week parameters ...
    select  TODAY_FIN_YEAR_NO,
            TODAY_FIN_WEEK_NO,
            THIS_WK_START_DATE,
            THIS_WK_END_DATE,
            TODAY_DATE
    into    g_cfin_year_no, g_cfin_week_no, g_start_date, g_end_date, g_batch_date
    from    dim_control;
    
    l_text := 'BATCH DATE BEING PROCESSED - '||g_batch_date||' g_week_no - '||g_cfin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    -- adjust prev period for 01 week (prev week is 52 and year-1) ...
--    g_pfin_year_no := g_cfin_year_no;
--    if g_cfin_week_no > 01 then
--       g_pfin_week_no := g_cfin_week_no - 1;
--    else
--       g_pfin_year_no := g_cfin_year_no - 1;
--       g_pfin_week_no := 52; 
--    end if;
    
    -----------------------------------
    -- B. PRF Table build ...
    -----------------------------------
    insert into dwh_performance.RTL_LOC_ITEM_DY_PLAN_SNAPSHOT 
    with 
    locs    as (select sk1_LOCATION_NO, SK1_FD_ZONE_GROUP_ZONE_NO from dwh_performance.dim_location where AREA_NO = 8800 and CHAIN_NO = 20), 
    
    Zone    as (select SK1_ZONE_GROUP_ZONE_NO, ZONE_GROUP_NO, ZONE_NO from dim_zone),
    
    plnstmp as (
      select  /*+ parallel(pa,8) full(pa)   */ 
              SK1_ITEM_NO,
              pa.SK1_LOCATION_NO,
              TRADING_DATE,
              POST_DATE,
              
              REC_ARRIVAL_CASE,
              REC_ARRIVAL_UNIT,
              IN_TRANSIT_CASE,
              IN_TRANSIT_UNIT,
              REC_ARRIVAL_CASE+IN_TRANSIT_CASE                         tot_cases,
              LAST_UPDATED_DATE,
              l.SK1_FD_ZONE_GROUP_ZONE_NO,
              z.zone_no
      from    dwh_performance.RTL_JDAFF_ST_PLAN_ANALYSIS_DY pa
      join    locs                                          l   on (l.SK1_LOCATION_NO        = pa.SK1_LOCATION_NO)
      join    zone                                          z   on (z.SK1_ZONE_GROUP_ZONE_NO = l.SK1_FD_ZONE_GROUP_ZONE_NO)
      
      where   pa.sk1_LOCATION_NO  in (select sk1_LOCATION_NO from locs)
      and     post_DATE           >= g_start_date
      and     trading_DATE        >= g_start_date
    ),

    itm     as (
      select /*+ parallel(i,4) */ item_no, SK1_ITEM_NO from dim_item i where SK1_ITEM_NO in (select distinct SK1_ITEM_NO from plnstmp)
    ),
    
    plns    as (
      select  /*+ parallel(pa,8) full(pa)   */ 
              a.SK1_ITEM_NO,
              i.item_no,
              SK1_LOCATION_NO,
              TRADING_DATE,
              POST_DATE,
              
              REC_ARRIVAL_CASE,
              REC_ARRIVAL_UNIT,
              IN_TRANSIT_CASE,
              IN_TRANSIT_UNIT,
              tot_cases,
              SK1_FD_ZONE_GROUP_ZONE_NO,
              zone_no,
              LAST_UPDATED_DATE
      from    plnstmp a
      join    itm     i on (a.SK1_ITEM_NO = i.SK1_ITEM_NO)
    ),

    zone_itm as (
      select  /*+ parallel(fzi,8) full(fzi)   */
              ZONE_GROUP_NO,
              ZONE_NO,
              ITEM_NO,
              CASE_COST_PRICE, 
              CASE_SELLING_EXCL_VAT,
              COST_PRICE 
      from    dwh_foundation.fnd_zone_item fzi
      where   ITEM_NO          in (select distinct item_no          from itm)    
      and     ITEM_NO||ZONE_NO in (select distinct ITEM_NO||zone_no from plns)
    )

    select  a.SK1_ITEM_NO,
            a.SK1_LOCATION_NO,
            a.TRADING_DATE,
            a.POST_DATE,
            a.SK1_FD_ZONE_GROUP_ZONE_NO,
            a.ZONE_NO,
            to_number(to_char(a.POST_DATE,'WW')) || to_char(a.POST_DATE,'DAY','NLS_DATE_LANGUAGE=''numeric date language'''),
           
            g_start_date,        
            a.REC_ARRIVAL_CASE,
            a.REC_ARRIVAL_UNIT,
            a.IN_TRANSIT_CASE,
            a.IN_TRANSIT_UNIT,
            a.TOT_CASES,
            
            b.CASE_COST_PRICE, 
            b.CASE_SELLING_EXCL_VAT,
            b.COST_PRICE,
            
            a.LAST_UPDATED_DATE
    from    plns     A
    join    zone_itm B on (b.ITEM_NO = a.ITEM_NO and b.ZONE_NO = a.ZONE_NO);
    g_recs_updated := SQL%ROWCOUNT;
    commit;
    
    l_text := 'PRF TABLE RTL_LOC_ITEM_DY_PLAN_SNAPSHOT Update/Inserted';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_processed||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    
    -----------------------------------
    -- D. Wrap up
    -----------------------------------
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
       RAISE;
       
end WH_PRF_CORP_189U;
