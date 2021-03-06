--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_999UX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_999UX" 
(p_forall_limit in integer,
p_success out boolean)
as

--**************************************************************************************************
--  Date:        SEPTEMBER 2010
--  Author:      Wendy Lyttle
--  Purpose:     Create Bridgethorn extracts to flat files in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - vw_supp_item_wk_bridgethorn1
--                        vw_loc_item_wk_bridgethorn1
--                        vw_depot_item_wk_bridgethorn1
--                        vw_item_bridgethorn1
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_count              number        :=  0;
g_start_date               date    ;
g_end_date               date    ;
g_fin_day_no number :=0;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_BRTH_090U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT BRIDGETHORN DATA TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

    G_SQL VARCHAR2(1000);
--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
 G_SQL := '';
    G_SQL := 'select RECTYPE||''|''||RECTYPE_DATETIME||''|''||RECDESC 
           from dwh_cust_performance.cust_vmp_sales_ms_ENGN_hdr
    union all
    select  CUSTOMER_NO||''|''||
            TRAN_DATE||''|''||
            LOCATION_NO||''|''||
            TRAN_NO||''|''||
            TILL_NO||''|''||
            OPERATOR_ID||''|''||
            TRAN_SELLING||''|''||
            C2_CUSTOMER_NO 
           from dwh_cust_performance.cust_vmp_sales_myschl_engn
    union all
     select RECTYPE||''|''||
            RECCNT||''|''||
            TRAN_SELLING
     from dwh_cust_performance.cust_vmp_sales_ms_ENGN_FTR';
 
 delete from  DWH_PERFORMANCE.WLCHECK;
commit;
INSERT INTO DWH_PERFORMANCE.WLCHECK VALUES(G_SQL);
commit;
   

    g_count := 0;
    g_count := dwh_generic_file_extract(G_SQL,'|','DWH_FILES_OUT','20160719T160547-WWENGEN-38.txt');
   l_text :=  'Records extracted to 20160719T160547-WWENGEN-38 '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
    
-------------------------------------- CORPORATE   
    

     
g_count := 0;
    g_count := dwh_generic_file_extract('select RECTYPE||''|''||RECTYPE_DATETIME||''|''||RECDESC 
           from dwh_cust_performance.cust_vmp_sales_ms_CORP_hdr
    union all
    select  CUSTOMER_NO||''|''||
            TRAN_DATE||''|''||
            LOCATION_NO||''|''||
            TRAN_NO||''|''||
            TILL_NO||''|''||
            OPERATOR_ID||''|''||
            TRAN_SELLING||''|''||
            C2_CUSTOMER_NO 
           from dwh_cust_performance.cust_vmp_sales_myschl_CORP
    union all
     select RECTYPE||''|''||
            RECCNT||''|''||
            TRAN_SELLING
     from dwh_cust_performance.cust_vmp_sales_ms_CORP_FTR','|','DWH_FILES_OUT','cust_wod_vmp.txt');
  l_text :=  'Records extracted to cust_wod_vmp '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

INSERT INTO DWH_PERFORMANCE.WLCHECK VALUES(G_SQL);
commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


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
       raise;
end WH_PRF_CUST_999UX;
