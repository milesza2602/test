-- ****** Object: Procedure W7131037.WH_PRF_CUST_364E Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_364E" (p_forall_limit in integer,p_success out boolean)
as

--**************************************************************************************************
--  Date:        Nov 2015
--  Author:      Kgomotso Lehabe
--  Purpose:     Create KEYSTONE extracts to a flat file in the performance layer
--               by reading views and calling generic function to output to flat file.
--  Tables:      Input  - DIM_ITEM
--               Output - dim_item.det
--  Tables:      Input  - CUST_SEGMENT_LIFESTYLE
--               Output - cust_segment_lifestyle.det
--  Tables:      Input  - CUST_CSM_VALUE_SEGMENT
--               Output - ckm_value_segment.det
--  Tables:      Input  - CUST_WOD_TIER_MTH_DETAIL
--               Output - wod_tier_mth_detail.det
--  Tables:      Input  - VS_EXT_KEYSTONE_CUSTOMER
--               Output - dim_c2_customer.det
--  Tables:      Input  - DIM_LOCATION
--               Output - dim_location.det
--  Tables:      Input  - CUST_BASKET_ITEM_SAMPLE
--               Output - ckm_basket_item_sample.det
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
g_forall_limit       INTEGER       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
G_COUNT              NUMBER        :=  0;
g_mth_start_date     date;
g_mth_end_date       DATE;

g_fin_year_no        number(4);
g_fin_month          number(4);

g_date               date          := trunc(sysdate);
G_YESTERDAY          date          := TRUNC(sysdate) - 1;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_364E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
L_SCRIPT_NAME        SYS_DWH_LOG.LOG_SCRIPT_NAME%type          := DWH_CONSTANTS.VC_LOG_SCRIPT_RTL_PRF_OTHER;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT KEYSTONE EXTRACTS TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'EXTRACT KEYSTONE EXTRACT DATA STARTED AT '||
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


      SELECT last_yr_fin_year_no,last_mn_fin_month_no
     into   g_fin_year_no, g_fin_month
    from dim_control;


      select this_mn_start_date, this_mn_end_date
      into g_mth_start_date, g_mth_end_date
      from dim_calendar
     where calendar_date = (select this_mn_start_date - 1
                            from dim_calendar
                           where calendar_date =g_date);

--**************************************************************************************************
-- Write to external directory.
-- TO SETUP
-- 1. add directory path to database via CREATE DIRECTORY command
-- 2. ensure that permissions are correct
-- 3. format : 'A','|','B','C'
--       WHERE A = select statement
--             B = Database Directory Name as found on DBA_DIRECTORIES
--             C = output file name
--    eg.'select * from VS_EXT_MYSTERY_SHOPPER','|','/dwh_files/files.out','nielsen.wk'
--**************************************************************************************************

----------------------------------------------------------------------
-- DIM ITEM EXTRACT
-----------------------------------------------------------------------
--  dbms_output.put_line('EXTRACT DIM_ITEM ');
--    l_text :=  'EXTRACT DIM_ITEM started at '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--     DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

--    G_COUNT := DWH_PERFORMANCE.DWH_GENERIC_FILE_EXTRACT
--       (' select * FROM W7131037.VS_EXT_KEYSTONE_DIM_ITEM ' ,
--        '|' ,
--        'DWH_FILES_OUT',
--        'dim_item.det');
--     --     'dim_item.csv');
--    l_text :=  'Records extracted to dim_item '||g_count;
--    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

--   l_text :=  'EXTRACT DIM_ITEM Ended at '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--     DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


----------------------------------------------------------------------
-- CUSTOMER SEGMENT LIFESTYLE EXTRACT
-----------------------------------------------------------------------
--  dbms_output.put_line('Extract CUSTOMER_SEGMENT_LIFESTYLE  ');
--  l_text :=  'EXTRACT CUSTOMER_SEGMENT_LIFESTYLE STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--  DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

--         G_COUNT := DWH_PERFORMANCE.DWH_GENERIC_FILE_EXTRACT
--       (' select * from W7131037.VS_EXT_CUST_SEGM_LIFESTYLE',
--         '|',
--         'DWH_FILES_OUT',
--     'cust_lss_lifestyle_segments.det');
--     --    'cust_lss_lifestyle_segments.csv');
--    L_TEXT :=  'Records extracted to cust_lss_lifestyle_segments '||G_COUNT;
--    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
--
--    l_text :=  'EXTRACT CUSTOMER_SEGMENT_LIFESTYLE ENDED  AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--  DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


 ----------------------------------------------------------------------
-- CKM VALUE SEGMENT
-----------------------------------------------------------------------
--     dbms_output.put_line('Extract CUST_CSM_VALUE_SEGMENT  ');

--    l_text :=  'EXTRACT CUST_CSM_VALUE_SEGMENT STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    G_COUNT := DWH_PERFORMANCE.DWH_GENERIC_FILE_EXTRACT
--      ('  select * from W7131037.VS_EXT_CUST_CSM_VALUE_SEGMENT ',
--          '|',
--          'DWH_FILES_OUT',
--        'cust_csm_value_segment.det');
--     --     'cust_csm_value_segment.csv');
--    l_text :=  'Records extracted to cust_csm_value_segment '||g_count;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   l_text :=  'EXTRACT CUST_CSM_VALUE_SEGMENT ENDED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--  DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   --  g_count := 0;
  ----------------------------------------------------------------------
-- WOD_TIER_MTH_DETAIL
-----------------------------------------------------------------------
--    dbms_output.put_line('WOD_TIER_MTH_DETAIL');
--    G_COUNT := DWH_PERFORMANCE.DWH_GENERIC_FILE_EXTRACT
--       ('  select * from W7131037.VS_EXT_CUST_WOD_TIER_MTH   ',
--       '|',
--       'DWH_FILES_OUT',
--       'wod_tier_mth_detail.det');
--    l_text :=  'Records extracted to wod_tier_mth_detail '||g_count;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    -- g_count := 0;

   ----------------------------------------------------------------------
-- DIM_CUSTOMER
-----------------------------------------------------------------------
--    dbms_output.put_line('DIM_CUSTOMER');

--   l_text :=  'EXTRACT DIM_CUSTOMER STARTED AT '|| TO_CHAR(SYSDATE,('dd mon yyyy hh24:mi:ss'));
--  DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

--    G_COUNT := DWH_PERFORMANCE.DWH_GENERIC_FILE_EXTRACT

--       ('  select  * from W7131037.VS_EXT_KEYSTONE_CUSTOMER  ',
--       '|',
--       'DWH_FILES_OUT',
--       'dim_customer.det');
--     --   'dim_customer.csv');
--    l_text :=  'Records extracted to DIM_CUSTOMER '||g_count;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--  l_text :=  'EXTRACT DIM_CUSTOMER ENDED AT '|| TO_CHAR(SYSDATE,('dd mon yyyy hh24:mi:ss'));
--  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--     g_count := 0;

----------------------------------------------------------------------
-- DIM_LOCATION
-----------------------------------------------------------------------
--   dbms_output.put_line('DIM_LOCATION');

--   l_text :=  'EXTRACT DIM_LOCATION STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--     DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


--    g_count := dwh_performance.dwh_generic_file_extract
--       (' select * from W7131037.VS_EXT_KEYSTONE_DIM_LOCATION  ',
--       '|',
--       'DWH_FILES_OUT',
--       'dim_location.det');
--    --   'dim_location.csv');
--    l_text :=  'Records extracted to dim_location '||g_count;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--     l_text :=  'EXTRACT DIM_LOCATION ENDED at '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--     DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

  --   g_count := 0;

----------------------------------------------------------------------
-- CKM_BASKET_ITEM_SAMPLE !!! NO TANLE !!
-----------------------------------------------------------------------
    dbms_output.put_line('CUST_BASKET_ITEM_SAMPLE');

    l_text :=  'EXTRACT CUST_BASKET_ITEM_SAMPLE STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    G_COUNT := DWH_PERFORMANCE.DWH_GENERIC_FILE_EXTRACT
       (' select * from VS_EXT_CUST_BASKET_ITEM_SAMPLE  ',
       '|',
       'DWH_FILES_OUT',
       'cust_basket_item_sample.det');
      --   'cust_basket_item_sample.csv');
    l_text :=  'Records extracted to CUST_BASKET_ITEM_SAMPLE '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       l_text :=  'EXTRACT CUST_BASKET_ITEM_SAMPLE ENDED AT '|| to_char(SYSDATE,('dd mon yyyy hh24:mi:ss'));
     DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);



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
end "WH_PRF_CUST_364E";
