--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_EXT_GRP_MTH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_EXT_GRP_MTH" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        JULY 2016
--  Author:      Alastair de Wet
--  Purpose:     Create OLD INFORMIX D&B DATA 2011 THRU 2013 EX EXTERNAL TABLE INPUT
--  Tables:      Input  - DWH_CUST_FOUNDATION.EXT_CUST_BU_MTH_2011
--               Output - OLD_CUST_BU_MTH_TMP
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
-- Note: This version Attempts to do a bulk insert / update / hospital. Downside is that hospital message is generic!!
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
--  Naming conventions
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
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;

 
g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_EXT_GRP_MTH';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD OLD INFORMIX D&B';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;






--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin

  insert /*+ APPEND parallel(oldcbm,6) */ into dwh_cust_foundation.old_cust_group_mth oldcbm
    (FIN_YEAR, 
        FIN_MONTH, 
        PRIMARY_ACCOUNT_NO, 
        GROUP_NO, 
        C2_CUSTOMER_NO, 
        NUM_ITEM, 
        NUM_ITEM_THREE_MTH, 
        NUM_ITEM_SIX_MTH, 
        NUM_ITEM_TWELVE_MTH, 
        "VALUE", 
        VALUE_THREE_MTH, 
        VALUE_SIX_MTH, 
        VALUE_TWELVE_MTH, 
        NUM_VISIT, 
        NUM_VISIT_THREE_MTH, 
        NUM_VISIT_SIX_MTH, 
        NUM_VISIT_TWELVE_MTH, 
        INVOLVE_SCORE_SIX_MTH, 
        INVOLVE_SCORE_TWELVE_MTH, 
        DATE_LAST_UPDATED)
  select /*+ parallel(ext,5) full(ext) */ 
        FIN_YEAR, 
        FIN_MONTH, 
        PRIMARY_ACCOUNT_NO, 
        GROUP_NO, 
        C2_CUSTOMER_NO, 
        NUM_ITEM, 
        NUM_ITEM_THREE_MTH, 
        NUM_ITEM_SIX_MTH, 
        NUM_ITEM_TWELVE_MTH, 
        "VALUE", 
        VALUE_THREE_MTH, 
        VALUE_SIX_MTH, 
        VALUE_TWELVE_MTH, 
        NUM_VISIT, 
        NUM_VISIT_THREE_MTH, 
        NUM_VISIT_SIX_MTH, 
        NUM_VISIT_TWELVE_MTH, 
        INVOLVE_SCORE_SIX_MTH, 
        INVOLVE_SCORE_TWELVE_MTH, 
        sysdate
    from dwh_cust_foundation.ext_cust_group_mth_2013 ext
;



      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';


    l_text := dwh_constants.vc_log_draw_line;
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
-- Call the bulk routines
--**************************************************************************************************
--    l_text := 'UPDATE STATS ON ALL TABLES'; 
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
--    DBMS_STATS.gather_table_stats ('DWH_CUST_FOUNDATION','OLD_CUST_GRP_MTH_TMP',estimate_percent=>1, DEGREE => 32);

    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;



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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
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
end WH_FND_CUST_EXT_GRP_MTH;
