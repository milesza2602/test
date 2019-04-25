--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_856U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_856U" (p_forall_limit in integer, p_success out boolean)
                           --      p_from_loc_no in integer,  p_to_loc_no in integer)
as
--**************************************************************************************************
--  Date:        Aug 2016
--  Author:      Barry Kirschner 
--  Purpose:     Create Supplier Plan Analysis data in the Performance layer
--               with input ex FND table from JDAFF.
--  Tables:      Input  - FND_SUP_PLAN_ANALYSIS_DY,
--               Output - RTL_SUP_PLAN_ANALYSIS_DY
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
g_calendar_date      date          := trunc(sysdate);
g_fin_week_no        number        := 0;

g_from_loc_no        integer       := 0;  
g_to_loc_no          integer       := 99999;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_856U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD PRF - Supplier Plan Analysis fact table';
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

    l_text := 'LOAD PRF - Supplier Plan Analysis fact table STARTED AT '||  
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    -- Look up batch date from dim_control ...
    dwh_lookup.dim_control(g_date);
    --G_DATE := G_DATE-1;
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOCATION RANGE BEING PROCESSED - '||g_from_loc_no||' to '||g_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    ----------------------------------------------------------------------------------------------
    
    -- get current date/week parameters ...
    select  fin_week_no, 
            calendar_date
    into    g_fin_week_no, g_calendar_date
    from    dim_calendar
    where   calendar_date = (select trunc(sysdate) from dual);
    
    l_text := 'CURRENT DATE BEING PROCESSED - '||g_calendar_date||' g_week_no - '||g_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    -----------------------------------
    -- C. PRF Table merge ...
    -----------------------------------
    merge /*+ parallel(tgt,4)  */ into DWH_PERFORMANCE.RTL_SUP_PLAN_ANALYSIS_DY tgt 
    USING  (    select b.sk1_ITEM_NO,
                       c.sk1_SUPPLIER_NO,
                       a.TRADING_DATE,

                       a.POST_DATE,
                       a.TOTAL_DEMAND_UNITS,
                       a.IGNORE_DEMAND_UNITS,
                       a.PLANNED_ORDERS_UNITS,
                       a.PLAN_SHIP_UNITS,
                       a.FIRM_PLAN_SHIP_UNITS,
                       a.REC_SHIP_UNITS
                from   dwh_foundation.FND_SUP_PLAN_ANALYSIS_DY a
                join   dim_item     b on (a.item_no = b.item_no)
                join   dim_supplier c on (a.SUPPLIER_NO = c.SUPPLIER_NO)
                where  a.LAST_UPDATED_DATE = g_date
          ) src
    on     (tgt.SK1_ITEM_NO     = src.SK1_ITEM_NO
    and     tgt.SK1_SUPPLIER_NO = src.SK1_SUPPLIER_NO
    and     tgt.TRADING_DATE    = src.TRADING_DATE
  --  and     tgt.POST_DATE       = src.POST_DATE
  )

    when matched then
    update set  POST_DATE             = src.POST_DATE,
                TOTAL_DEMAND_UNITS		= src.TOTAL_DEMAND_UNITS,	
                IGNORE_DEMAND_UNITS		= src.IGNORE_DEMAND_UNITS,	
                PLANNED_ORDERS_UNITS	= src.PLANNED_ORDERS_UNITS,	
                PLAN_SHIP_UNITS		    = src.PLAN_SHIP_UNITS,	
                FIRM_PLAN_SHIP_UNITS	= src.FIRM_PLAN_SHIP_UNITS,	
                REC_SHIP_UNITS		    = src.REC_SHIP_UNITS,
                LAST_UPDATED_DATE	    = g_date
    when not matched then
    insert values
              ( src.SK1_ITEM_NO,
                src.SK1_SUPPLIER_NO,
                src.TRADING_DATE,
                
                src.POST_DATE,
                src.TOTAL_DEMAND_UNITS,	
                src.IGNORE_DEMAND_UNITS,	
                src.PLANNED_ORDERS_UNITS,	
                src.PLAN_SHIP_UNITS,
                src.FIRM_PLAN_SHIP_UNITS,	
                src.REC_SHIP_UNITS,	
                g_date
              );
    g_recs_updated := SQL%ROWCOUNT;
    commit;
    
    l_text := 'PRF TABLE RTL_SUP_PLAN_ANALYSIS_DY Update/Inserted';
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
       
end WH_PRF_CORP_856U;
