-- ****** Object: Procedure W7131037.WH_PRF_CUST_134E Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_134E" 
(p_forall_limit in integer,p_success out boolean)
as

--**************************************************************************************************
--  Date:        MARCH 2014
--  Author:      Alastair de Wet
--  Purpose:     Create LIGHTSTONE CUSTOMER MASTER extract to flat file in the performance layer
--               by reading DIM_CUSTOMER and calling generic function to output to flat file.
--  Tables:      Input  - SQL
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_134E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT LIGHTSTONE CUST MASTER TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'EXTRACT LIGHTSTONE DATA STARTED AT '||
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
--    eg.'select * from VS_EXT_MYSTERY_SHOPPER','|','/dwh_files/files.out','nielsen.wk'
--**************************************************************************************************
    dbms_output.put_line('LIGHTSTONE CUSTOMER');
    G_COUNT := DWH_PERFORMANCE.DWH_GENERIC_FILE_EXTRACT
       ('select /*+ FULL(DC) FULL(DCS) PARALLEL(8) */
                DC.CUSTOMER_NO,DC.IDENTITY_DOCUMENT_CODE,DC.PHYSICAL_ADDRESS_LINE_1,DC.PHYSICAL_ADDRESS_LINE2,
                DC.PHYSICAL_SUBURB_NAME,DC.PHYSICAL_POSTAL_CODE,DC.PHYSICAL_CITY_NAME,
                DC.PHYSICAL_PROVINCE_NAME,DC.PHYSICAL_COUNTRY_CODE,DC.PHYSICAL_ADDRESS_OCCUPTN_DATE
         from   DIM_CUSTOMER DC, DIM_CUSTOMER_SS DCS
         where  DC.CUSTOMER_NO             =  DCS.CUSTOMER_NO(+) AND
                (
                DC.PHYSICAL_ADDRESS_LINE_1 <> NVL(DCS.PHYSICAL_ADDRESS_LINE_1,0) or
                DC.PHYSICAL_ADDRESS_LINE2  <> NVL(DCS.PHYSICAL_ADDRESS_LINE2,0)  or
                DC.PHYSICAL_SUBURB_NAME    <> NVL(DCS.PHYSICAL_SUBURB_NAME,0)    or
                DC.PHYSICAL_POSTAL_CODE    <> NVL(DCS.PHYSICAL_POSTAL_CODE,0)    or
                DC.PHYSICAL_CITY_NAME      <> NVL(DCS.PHYSICAL_CITY_NAME,0)      or
                DC.PHYSICAL_PROVINCE_NAME  <> NVL(DCS.PHYSICAL_PROVINCE_NAME,0)  or
                DC.PHYSICAL_COUNTRY_CODE   <> NVL(DCS.PHYSICAL_COUNTRY_CODE,0))',
                '|','DWH_FILES_OUT','lightstone_ww_customer.txt');
    l_text :=  'Records extracted to LIGHTSTONE '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    dbms_output.put_line('MYSTERY SHOPPER');
--    g_count := dwh_generic_file_extract('select * from VS_EXT_MYSTERY_SHOPPER','|','DWH_FILES_OUT','mystery_shopper_sc.txt');
--    l_text :=  'Records extracted to mystery_shopper '||g_count;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



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
end wh_prf_cust_134E;
