--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_378E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_378E" 
                              (p_forall_limit in integer,p_success out boolean)
as
--**************************************************************************************************
--  Date:        February 2016
--  Author:      Barry Kirschner
--  Purpose:     Create Voucher product/item (coupon) links Extract to flat file in the performance layer
--               by reading data and calling generic function to output to flat file.
--               This is a simple snapshot extract refresh as data volume is low.
--  Tables:      Input  - FND_FV_VOUCHER_TYPE_ATTR
--                      - dim_business_unit
--                      - dim_department
--                      - dim_item 
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_count              number        :=  0;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_378E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT E20 CAMPAIGN-VOUCHER ITEMS LINK DATA TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'EXTRACT FOR E20 CAMPAIGN-VOUCHER ITEMS LINK STARTED AT '||
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
-- Write to external directory.
-- TO SETUP
-- 1. add directory path to database via CREATE DIRECTORY command
-- 2. ensure that permissions are correct
-- 3. format : 'A','|','B','C'
--       WHERE A = select statement
--             B = Database Directory Name as found on DBA_DIRECTORIES
--             C = output file name
--    eg.'select * from VS_EXT_VMP_SALES_WKLY','|','/dwh_files/files.out','nielsen.wk'
--**************************************************************************************************
    dbms_output.put_line('VOUCHER ITEMS-LINK');
    g_count := dwh_performance.dwh_generic_file_extract(
         q'[
            select  VOUCHER_TYPE_NO,
                    VOUCHER_RULE_DESC,
                    MERCH_HRCHY_TYPE                                          HRCHY_TYPE,
                    MERCH_HRCHY_TYPE_DESC                                     HRCHY_TYPE_DESC,
                    MERCH_HRCHY_VALUE                                         HRCHY_CODE,
                    coalesce(BUSINESS_UNIT_NAME, DEPARTMENT_NAME, item_desc)  DESCRIPTION
            from   (
                    select    VOUCHER_TYPE_NO,
                              VOUCHER_RULE_DESC,
                              MERCH_HRCHY_TYPE,
                              MERCH_HRCHY_TYPE_DESC,
                              MERCH_HRCHY_VALUE,
                              bu.BUSINESS_UNIT_NAME,
                              dpt.DEPARTMENT_NAME,
                              itm.item_desc
                    from      FND_FV_VOUCHER_TYPE_ATTR  vta
                    left join dim_business_unit         bu  on (MERCH_HRCHY_TYPE_DESC = 'BUSINESS UNIT' and vta.MERCH_HRCHY_VALUE = bu.BUSINESS_UNIT_NO)
                    left join dim_department            dpt on (MERCH_HRCHY_TYPE_DESC = 'DEPARTMENT' and vta.MERCH_HRCHY_VALUE = dpt.department_NO)
                    left join dim_item                  itm on (MERCH_HRCHY_TYPE_DESC in ('ITEM_NO', 'ITEM_LEVEL1_NO') and vta.MERCH_HRCHY_VALUE = itm.item_NO)
                   ) extr
           ]','|','DWH_FILES_OUT','e20_coupon_items.txt');
        l_text :=  'Records extracted to E20_COUPON_ITEMS '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

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
       RAISE;
       
end WH_PRF_CUST_378E;
