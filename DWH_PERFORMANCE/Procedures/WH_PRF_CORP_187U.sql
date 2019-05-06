--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_187U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_187U" (p_forall_limit in integer, p_success out boolean)
                           --      p_from_loc_no in integer,  p_to_loc_no in integer)
as
--**************************************************************************************************
--  Date:        Mar 2017
--  Author:      Barry Kirschner 
--  Purpose:     Create extract summary table for ISO - Sales + Waste SSRS report (Sisanda)
--  Tables:      Input  - RTL_LOC_ITEM_WK_RMS_DENSE (sales)
--                      - RTL_LOC_ITEM_WK_RMS_SPARSE (waste)
--               Output - RTL_LOC_ITEM_WK_ISO_SNAPSHOT
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
g_cfin_strtdte       date;

g_from_loc_no        integer       := 0;  
g_to_loc_no          integer       := 99999;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_187U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD PRF - ISO Sales and Waste summary extract table';
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

    l_text := 'LOAD PRF - ISO Sales and Waste summary extract table STARTED AT '||  
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
    execute immediate 'truncate table dwh_performance.RTL_LOC_ITEM_WK_ISO_SNAPSHOT';
    l_text := 'DWH_PERFORMANCE.RTL_LOC_ITEM_WK_ISO_SNAPSHOT TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    ----------------------------------------------------------------------------------------------
    
    -- get current date/week parameters ...
    select  TODAY_FIN_YEAR_NO,
            LAST_WK_FIN_WEEK_NO,                                                -- use the last completed week for 'L'
            THIS_WK_START_DATE,
            TODAY_DATE
    into    g_cfin_year_no, g_cfin_week_no, g_cfin_strtdte, g_batch_date
    from    dim_control;
    
    l_text := 'BATCH DATE BEING PROCESSED - '||g_batch_date||' g_week_no - '||g_cfin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    -- adjust prev period for 01 week (prev week is 52 and year-1) ...
    g_pfin_year_no := g_cfin_year_no;
    if g_cfin_week_no > 01 then
       g_pfin_week_no := g_cfin_week_no - 1;
    else
       g_pfin_year_no := g_cfin_year_no - 1;
       g_pfin_week_no := 52; 
    end if;
    
    -----------------------------------
    -- B. PRF Table build ...
    -----------------------------------
    insert  /* + parallel (tgt,4)  */ into dwh_performance.RTL_LOC_ITEM_WK_ISO_SNAPSHOT tgt
    with Cal as (
      -- Curr YR/Curr WK ...
      select THIS_WEEK_END_DATE, 
             THIS_WEEK_START_DATE, 
             FIN_YEAR_NO,
             FIN_WEEK_NO,
            'L' typ 
      from   dim_calendar 
      where  FIN_YEAR_NO  = g_cfin_year_no
      and    FIN_week_NO  = g_cfin_week_no
      union
      -- Prev YR/Curr WK ...
      select THIS_WEEK_END_DATE, 
             THIS_WEEK_START_DATE, 
             FIN_YEAR_NO,
             FIN_WEEK_NO,
            'L' 
      from   dim_calendar 
      where  FIN_YEAR_NO  = g_cFIN_YEAR_NO - 1
      and    FIN_week_NO  = g_cfin_week_NO
      union
      -- Base week ...
      select THIS_WEEK_END_DATE,
             THIS_WEEK_START_DATE,
             FIN_YEAR_NO, 
             FIN_WEEK_NO,
            'B'
      from  (
              Select  THIS_WEEK_END_DATE,
                      THIS_WEEK_START_DATE,
                      CALENDAR_DATE,
                      FIN_YEAR_NO, FIN_WEEK_NO
              from    DIM_CALENDAR 
              where   rownum = 1 
              AND     SUBSTR(CALENDAR_DATE,1,2)          = SUBSTR(CURRENT_DATE + 7,1,2)
              AND   ((SUBSTR(CALENDAR_DATE + 6,1,2))     = SUBSTR(CURRENT_DATE + 13,1,2) 
                      or (SUBSTR(CALENDAR_DATE + 6,1,2)) = SUBSTR(CURRENT_DATE + 12,1,2) 
                      or (SUBSTR(CALENDAR_DATE + 6,1,2)) = SUBSTR(CURRENT_DATE + 14,1,2)
                    )
              AND     CALENDAR_DATE < CURRENT_DATE + 6
              AND     FIN_DAY_NO =  (1 + TRUNC (CURRENT_DATE + 7) - TRUNC (CURRENT_DATE + 7, 'IW'))
              ORDER BY 
                      CALENDAR_DATE DESC
            )
    ),
           
    locs as (select sk1_location_no from dwh_performance.dim_location where AREA_NO = 8800 and CHAIN_NO = 20),
     
    dns as (
      -- get Last Trading WK ...
      select  /* + parallel (wdns,8) full(wdns)   */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO,
              FIN_WEEK_NO,
             'L' Typ,
              
              nvl(sales, 0)  sales,
              nvl(SALES_QTY, 0) SALES_QTY
      from    RTL_LOC_ITEM_WK_RMS_DENSE wdns
      where   FIN_YEAR_NO     between (select min(FIN_YEAR_NO) from cal where typ = 'L') and (select max(FIN_YEAR_NO) from cal where typ = 'L') 
      and     FIN_WEEK_NO =   g_cfin_week_no
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)
      and     nvl(sales, 0) <> 0
      union
      -- get Base WK ...
      select  /* + parallel (wdns,8) full(wdns)   */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO,
              FIN_WEEK_NO,
             'B',
              
              nvl(sales, 0)  sales,
              nvl(SALES_QTY, 0) SALES_QTY
      from    RTL_LOC_ITEM_WK_RMS_DENSE wdns
      where   FIN_YEAR_NO     =  (select max(FIN_YEAR_NO) from cal where typ = 'B')
      and     FIN_WEEK_NO     =  (select max(FIN_WEEK_NO) from cal where typ = 'B')
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)
      and     nvl(sales, 0) <> 0
      union
      -- get Progessive data (all weeks in current year upto last completed week) ...
      select  /* + parallel (ddns,8) full(ddns)   */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO,
              99,        --FIN_WEEK_NO,
             'P',
              
              sum(nvl(sales, 0))      sales,
              sum(nvl(SALES_QTY, 0))  SALES_QTY
      from    RTL_LOC_ITEM_WK_RMS_DENSE ddns
      where   FIN_YEAR_NO       =  g_cfin_year_no 
      and     FIN_WEEK_NO       <= g_cfin_week_no
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)
      and     nvl(sales, 0)   <> 0
      group by
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO
      union
      -- get Progessive data (all weeks in prev year upto last completed week) ...
      select  /* + parallel (ddns,8) full(ddns)   */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO,
              99,        --FIN_WEEK_NO,
             'P',
              
              sum(nvl(sales, 0))      sales,
              sum(nvl(SALES_QTY, 0))  SALES_QTY
      from    RTL_LOC_ITEM_WK_RMS_DENSE ddns
      where   FIN_YEAR_NO       =  g_cfin_year_no-1 
      and     FIN_WEEK_NO       <= g_cfin_week_no
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)
      and     nvl(sales, 0)   <> 0
      group by
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO
    ),
    
    sprs as (
      -- get Last Trading WK ...
      select  /*+ parallel (wsprs,8) full(wsprs)   */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO,
              FIN_WEEK_NO,
             'L' Typ,
              
              nvl(WASTE_COST, 0)  WASTE_COST,
              nvl(WASTE_QTY, 0)   WASTE_QTY
      from    RTL_LOC_ITEM_WK_RMS_SPARSE wsprs
      where   FIN_YEAR_NO     between (select min(FIN_YEAR_NO) from cal where typ = 'L') and (select max(FIN_YEAR_NO) from cal where typ = 'L') 
      and     FIN_WEEK_NO =   g_cfin_week_no
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)
      and     nvl(WASTE_COST, 0) <> 0
      union
      -- get Base WK ...
      select  /*+ parallel (wsprs,8) full(wsprs)   */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO,
              FIN_WEEK_NO,
             'B',
              
              nvl(WASTE_COST, 0)  WASTE_COST,
              nvl(WASTE_QTY, 0)   WASTE_QTY
      from    RTL_LOC_ITEM_WK_RMS_SPARSE wsprs
      where   FIN_YEAR_NO     =  (select max(FIN_YEAR_NO) from cal where typ = 'B')
      and     FIN_WEEK_NO     =  (select max(FIN_WEEK_NO) from cal where typ = 'B')
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)
      and     nvl(WASTE_COST, 0) <> 0
      union
      -- get Progressive data (all weeks in current year upto last completed week) ...
      select  /*+ parallel (dsprs,8) full(dsprs)   */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO,
              99,  
             'P',
              
              sum(nvl(WASTE_COST, 0))  WASTE_COST,
              sum(nvl(WASTE_QTY, 0))   WASTE_QTY
      from    RTL_LOC_ITEM_WK_RMS_SPARSE dsprs
      where   FIN_YEAR_NO       =  g_cfin_year_no 
      and     FIN_WEEK_NO       <= g_cfin_week_no
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)
      and     nvl(WASTE_COST, 0) <> 0
      group by
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO
      union
      -- get Progressive data (all weeks in prev. year upto last completed week) ...
      select  /*+ parallel (dsprs,8) full(dsprs)   */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO,
              99,  
             'P',
              
              sum(nvl(WASTE_COST, 0))  WASTE_COST,
              sum(nvl(WASTE_QTY, 0))   WASTE_QTY
      from    RTL_LOC_ITEM_WK_RMS_SPARSE dsprs
      where   FIN_YEAR_NO       =  g_cfin_year_no-1 
      and     FIN_WEEK_NO       <= g_cfin_week_no
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)
      and     nvl(WASTE_COST, 0) <> 0
      group by
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              FIN_YEAR_NO
    )
    
    select  /*+ parallel(a,4) full(a) parallel(b,4) full(b) */      
            a.SK1_LOCATION_NO,
            a.SK1_ITEM_NO,
            a.FIN_YEAR_NO,
            a.FIN_WEEK_NO,
            a.typ,
            
            sum(a.sales),
            sum(a.SALES_QTY),
            sum(nvl(b.WASTE_COST, 0)),
            sum(nvl(b.WASTE_QTY, 0)) ,
            sysdate
    from    dns   a
    left join    
            sprs  b on (a.SK1_LOCATION_NO = b.SK1_LOCATION_NO and a.SK1_ITEM_NO = b.SK1_ITEM_NO and a.FIN_YEAR_NO = b.FIN_YEAR_NO and a.FIN_WEEK_NO = b.FIN_WEEK_NO and a.typ = b.typ)
    group by
            a.SK1_LOCATION_NO,
            a.SK1_ITEM_NO,
            a.FIN_YEAR_NO,
            a.FIN_WEEK_NO,
            a.typ;
    g_recs_updated := SQL%ROWCOUNT;
    commit;
    
    l_text := 'PRF TABLE RTL_LOC_ITEM_WK_ISO_SNAPSHOT Inserted';
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
       
end WH_PRF_CORP_187U;
