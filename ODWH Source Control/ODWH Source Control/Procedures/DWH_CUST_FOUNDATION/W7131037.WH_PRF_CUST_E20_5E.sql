-- ****** Object: Procedure W7131037.WH_PRF_CUST_E20_5E Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_E20_5E" 
(p_forall_limit in integer,p_success out boolean)
as

--**************************************************************************************************
--  Date:        February 2017
--  Author:      Theo Filander
--  Purpose:     Woolworths and MySchool are interested in understanding the current uplift that the
--               programme is generating for Woolworths. MySchool enrolment and Woolworths transaction
--               data over a 3 year period will be analysed.
--               Create Lifestyle Segmentation Extract to flat file by reading data and calling generic function
--               to output to flat file THIS IS AN AD-HOC EXTRACT.
--  Tables:      Input  - CUST_BASKET_TENDER
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_E20_5E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT E20 PERSONALISATION INTERFACE (CUST_BASKET_TENDER) TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_period             W7131037.keystone%ROWTYPE;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'EXTRACT FOR E20 PERSONALISATION INTERFACE-CUST_BASKET_TENDER STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up transaction date range from W7131037.keystone table
--**************************************************************************************************

    select fin_year_no,
           fin_month_no,
           this_mn_start_date,
           this_mn_end_date,
           NVL((select seq_num+1 from "W7131037"."KEYSTONE" where program_name = 'WH_PRF_CUST_E20_5E'),1) seq_num,
           'WH_PRF_CUST_E20_5E' program_name,
           this_mn_start_date curr_date
      into l_period
      from dim_calendar
     where calendar_date = NVL((select this_mn_start_date-1 START_DATE from "W7131037"."KEYSTONE" where program_name = 'WH_PRF_CUST_E20_5E'),trunc(sysdate)-20 );

     l_text := 'PROCESSING SEQUENCE NO '||l_period.seq_num||' FOR DATE: '||l_period.curr_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Reset the Keystone table
--**************************************************************************************************
    l_text :=  'UPDATE W7131037.KEYSTONE :'||l_period.fin_year_no||' - '||l_period.fin_month_no||' @ '|| to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   dbms_output.put_line('UPDATE W7131037"."KEYSTONE :'||l_period.fin_year_no||' - '||l_period.fin_month_no);
    update "W7131037"."KEYSTONE"
       set fin_year_no        = l_period.fin_year_no,
           fin_month_no       = l_period.fin_month_no,
           this_mn_start_date = l_period.this_mn_start_date,
           this_mn_end_date   = l_period.this_mn_end_date,
           seq_num            = l_period.seq_num,
           curr_date          = l_period.curr_date
     where program_name       = l_period.program_name;


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
    dbms_output.put_line('CUST_BASKET_TENDER');
    g_count := dwh_performance.dwh_generic_file_extract(
         q'[select /*+ Parallel(cbi,6) Full(cbi) */ distinct
                   *
              from W7131037.temp_e20_basket_tender cbi
             where tran_date between (select this_mn_start_date from W7131037.keystone where program_name = 'WH_PRF_CUST_E20_5E')
               and                   (select this_mn_end_date from W7131037.keystone where program_name = 'WH_PRF_CUST_E20_5E')
       ]','|','DWH_FILES_OUT','e20_basket_tender_'||l_period.fin_year_no||l_period.fin_month_no||'.txt');
        l_text :=  'Records extracted to E20_BASKET_TENDER '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--and cbt.tran_date between '01/JUL/13' and '29/JUN/14'
--and cbt.tran_date between '30/JUN/14' and '28/JUN/15'
--and cbt.tran_date between '29/JUN/15' and '26/JUN/16'

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
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
end "WH_PRF_CUST_E20_5E";

