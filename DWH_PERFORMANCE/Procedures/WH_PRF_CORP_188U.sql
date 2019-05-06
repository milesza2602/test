--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_188U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_188U" (p_forall_limit in integer, p_success out boolean)
                           --      p_from_loc_no in integer,  p_to_loc_no in integer)
as
--**************************************************************************************************
--  Date:        Mar 2017
--  Author:      Barry Kirschner 
--  Purpose:     Create extract summary table for ISO - Orders + Delivery SSRS report (Karna).
--  Tables:      Input  - RTL_LOC_ITEM_DY_OM_ORD (orders)
--                      - RTL_LOC_ITEM_DY_RMS_DENSE (DNS)
--               Output - RTL_LOC_ITEM_DY_ISO_SNAPSHOT
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

g_from_loc_no        integer       := 0;  
g_to_loc_no          integer       := 99999;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_188U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD PRF - ISO - Orders and Delivery summary extract table';
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

    l_text := 'LOAD PRF - ISO - Orders and Delivery summary extract table STARTED AT '||  
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
    execute immediate 'truncate table dwh_performance.RTL_LOC_ITEM_DY_ISO_SNAPSHOT';
    l_text := 'DWH_PERFORMANCE.RTL_LOC_ITEM_DY_ISO_SNAPSHOT TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    ----------------------------------------------------------------------------------------------
    
    -- get current date/week parameters ...
    select  TODAY_FIN_YEAR_NO, 
            TODAY_FIN_WEEK_NO,  
            TODAY_DATE 
    into    g_cfin_year_no, g_cfin_week_no, g_batch_date
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
    insert /* + parallel (tgt,4)  */ into dwh_performance.RTL_LOC_ITEM_DY_ISO_SNAPSHOT tgt
    with prds as (
        -- get 2 week period ...
        select  distinct
                THIS_WEEK_END_DATE, THIS_WEEK_START_DATE, 'C' prd 
        from    dim_calendar 
        where   FIN_YEAR_NO  = g_cfin_year_no
        and     FIN_week_NO  = g_cfin_week_no
--        union 
--        select  distinct
--                THIS_WEEK_END_DATE, THIS_WEEK_START_DATE, 'P' 
--        from    dim_calendar
--        where   FIN_YEAR_NO  = g_cfin_year_no - 1
--        and     FIN_week_NO  = g_cfin_week_no
    ),
            
    locs as (
      select sk1_LOCATION_NO from dwh_performance.dim_location where AREA_NO = 8800 and CHAIN_NO = 20), 
    
    ord as (
      select  /*+ parallel (ord,8) full(ord)  */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              trunc(POST_DATE)    POST_DATE,
              
              nvl(IN_STORE_ORDER_CASES, 0) IN_STORE_ORDER_CASES
      from    RTL_LOC_ITEM_DY_OM_ORD ord
      where   post_DATE between (select THIS_WEEK_START_DATE from prds where prd = 'C') and (select THIS_WEEK_END_DATE from prds where prd = 'C')
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)  
      and     nvl(IN_STORE_ORDER_CASES, 0) <> 0
    ),
    
    dns as (
      select  /*+ parallel (dns,8) full(dns)   */ 
              SK1_LOCATION_NO,
              SK1_ITEM_NO,
              trunc(POST_DATE)    POST_DATE,
              
              nvl(SDN_IN_CASES, 0)  SDN_IN_CASES
      from    RTL_LOC_ITEM_DY_RMS_DENSE dns
      where   post_DATE between (select THIS_WEEK_START_DATE from prds where prd = 'C') and (select THIS_WEEK_END_DATE from prds where prd = 'C')
      and     sk1_LOCATION_NO in (select sk1_LOCATION_NO from locs)   
      and     nvl(SDN_IN_CASES, 0) <> 0
    ),
    
    -- the additional union/minus is to get all DNS records that do not have associated ORD records due to source JDAFF cut-over ...
    combo as (
    select ord.SK1_LOCATION_NO,
           ord.SK1_ITEM_NO,
           ord.POST_DATE,     
           ord.IN_STORE_ORDER_CASES,
           nvl(dns.SDN_IN_CASES,0) SDN_IN_CASES
    from   ord ord
    left join    
           dns dns on (ord.SK1_LOCATION_NO = dns.SK1_LOCATION_NO and ord.SK1_ITEM_NO = dns.SK1_ITEM_NO and ord.POST_DATE = dns.POST_DATE)
    union
    select SK1_LOCATION_NO,
           SK1_ITEM_NO,
           POST_DATE,     
           0,
           nvl(SDN_IN_CASES,0)
    from   dns
    minus
    select SK1_LOCATION_NO,
           SK1_ITEM_NO,
           POST_DATE,     
           0,
           IN_STORE_ORDER_CASES
    from   ord 
    )
    
    select  /*+ parallel(a,4) full(a) parallel(b,4) full(b)  */ 
            SK1_LOCATION_NO,
            SK1_ITEM_NO,
            POST_DATE,
            nvl(IN_STORE_ORDER_CASES, 0)  IN_STORE_ORDER_CASES,
            nvl(SDN_IN_CASES, 0)          SDN_IN_CASES,
            sysdate
    from    combo  a    ;
    g_recs_updated := SQL%ROWCOUNT;
    commit;
    
    l_text := 'PRF TABLE RTL_LOC_ITEM_DY_ISO_SNAPSHOT Inserted';
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
       
end WH_PRF_CORP_188U;
