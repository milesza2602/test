--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_131U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_131U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2015
--  Author:      Quentin Smit
--  Purpose:     Create a PRICE table to be used in WH_PRF_CORP_129U to overcome batch contention.
--               The 129u job uses data not processed into the PRICE table so it can use previously procssed data.
--  Tables:      Input  - rtl_loc_item_dy_rms_price
--               Output - rtl_lid_rms_price_ff  
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  06 Apr 2016 - Qc5070/Chg-43031 - Include CUSTORD products into the list for the price lookup
--  08 Sep 2016 - A Joshua Chg-202 -- Remove table fnd_jdaff_dept_rollout from selection criteria

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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_item_dy_pos_jv%rowtype;

g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_131U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS PRICE INFO FOR FF';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_from_date            date;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    
    execute immediate 'alter session enable parallel dml';

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_LID_RMS_PRICE_FF STARTED AT '||
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
    
    g_from_date := g_date - 7;
    
    l_text := 'TRUNCATING TABLE RTL_LID_RMS_PRICE_FF ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    
    
    execute immediate 'truncate table DWH_PERFORMANCE.RTL_LID_RMS_PRICE_FF';

    l_text := 'DISABLE PK CONSTRAINT ON RTL_LID_RMS_PRICE_FF ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
    
    execute immediate 'ALTER TABLE DWH_PERFORMANCE.RTL_LID_RMS_PRICE_FF DISABLE CONSTRAINT PK_P_RTL_LID_RMS_PRCF';
    
    l_text := 'PK CONSTRAINT DISABLED RTL_LID_RMS_PRICE_FF ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
    
    l_text := 'LOADING PRICE DATA FOR 129U FOR TOMORROWS BATCH INTO RTL_LID_RMS_PRICE_FF ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    
    
    l_text := 'Date range being loaded - ' || g_from_date ||' to ' || g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    
  

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

INSERT /*+ parallel(ff,4) */ into DWH_PERFORMANCE.rtl_lid_rms_price_ff ff
select  SK1_LOCATION_NO,
        SK1_ITEM_NO,
        CALENDAR_DATE,
        SK2_LOCATION_NO,
        SK2_ITEM_NO,
        max(REG_RSP)                        REG_RSP,
        max(REG_RSP_EXCL_VAT)               REG_RSP_EXCL_VAT,
        SELLING_UOM_CODE,
        max(PROM_RSP)                       PROM_RSP,
        max(CLEAR_RSP)                      CLEAR_RSP,
        max(WAC)                            WAC,
        max(VAT_RATE_PERC)                  VAT_RATE_PERC,
        max(RULING_RSP)                     RULING_RSP,
        max(RULING_RSP_EXCL_VAT)            RULING_RSP_EXCL_VAT,
        LAST_UPDATED_DATE,
        max(NUM_UNITS_PER_TRAY)             NUM_UNITS_PER_TRAY,
        max(CASE_COST)                      CASE_COST,
        max(CASE_SELLING)                   CASE_SELLING,
        max(CASE_SELLING_EXCL_VAT)          CASE_SELLING_EXCL_VAT
from    (
          select /*+ materialize cardinality (prc,300000) materialize cardinality (ord,300000) full (di), full (dl), full (d) */  prc.*
             from   rtl_loc_item_dy_rms_price prc
                    
                    join dim_item di             on prc.sk1_item_no                = di.sk1_item_no
                    join dim_location dl         on prc.sk1_location_no            = dl.sk1_location_no
--                    join fnd_jdaff_dept_rollout d on d.department_no = di.department_no
          
                    join fnd_rtl_loc_item_dy_ff_ord ord
                                                 on di.item_no             = ord.item_no
                                                and dl.location_no         = ord.location_no
                                                and (ord.post_date)        = prc.calendar_date
               where prc.calendar_date between g_from_date and g_date
--               and  d.department_live_ind = 'Y'  
                         
          union 
          
          select /*+ materialize cardinality (prc,300000) materialize cardinality (ord,300000) full (di), full (dl), full (d) */  prc.*
             from   rtl_loc_item_dy_rms_price prc
                    
                    join dim_item di             on prc.sk1_item_no                = di.sk1_item_no
                    join dim_location dl         on prc.sk1_location_no            = dl.sk1_location_no
--                    join fnd_jdaff_dept_rollout d on d.department_no = di.department_no
          
                    join FND_LOC_ITEM_DY_FF_CUST_ORD ord
                                                 on di.item_no             = ord.item_no
                                                and dl.location_no         = ord.location_no
                                                and (ord.post_date)        = prc.calendar_date
               where prc.calendar_date between g_from_date and g_date
--               and  d.department_live_ind = 'Y' 
                       )     
group by
        SK1_LOCATION_NO,
        SK1_ITEM_NO,
        CALENDAR_DATE,
        SK2_LOCATION_NO,
        SK2_ITEM_NO,
        SELLING_UOM_CODE,
        PROM_RSP,
        CLEAR_RSP,
        LAST_UPDATED_DATE;

    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    l_text := 'ENABLE PK CONSTRAINT ON RTL_LID_RMS_PRICE_FF ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
    
    execute immediate 'ALTER TABLE DWH_PERFORMANCE.RTL_LID_RMS_PRICE_FF ENABLE CONSTRAINT PK_P_RTL_LID_RMS_PRCF';
    
    l_text := 'PK CONSTRAINT DISABLED RTL_LID_RMS_PRICE_FF ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
    
    l_text := ' UPDATE STATS STARTED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','RTL_LID_RMS_PRICE_FF', DEGREE => 16);
    
    l_text := 'UPDATE STATS ENDED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
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
       
end wh_prf_corp_131u;
