--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_714TON1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_714TON1" 
(p_forall_limit in integer,
p_success out boolean) 
as

--**************************************************************************************************
--  Date:        April 2014
--  Author:      Alastair de Wet
--  Purpose:     Create Aztec extract to flat file in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - various incl dense
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  Notes:       RUN ON THE 10TH OF EACH MONTH TO PICK UP LAST FIN_MONTH DATA
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

g_this_mn_start_date   date ;  
g_this_mn_end_date     date;
G_LAST_MN_FIN_YEAR_NO  integer;
g_last_mn_fin_month_no integer;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_714E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT AZTEC DATA TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'EXTRACT Aztec DATA STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    
     select last_yr_fin_year_no, last_mn_fin_month_no
     into g_last_mn_fin_year_no, g_last_mn_fin_month_no
     from dim_control;
   
     select  distinct this_mn_start_date,this_mn_end_date
     into    g_this_mn_start_date,g_this_mn_end_date
     from    dim_calendar_wk
     where   fin_month_no = g_last_mn_fin_month_no 
     and     fin_year_no  = g_last_mn_fin_year_no;

--**************************************************************************************************
-- Write to external directory.
-- TO SETUP
-- 1. add directory path to database via CREATE DIRECTORY command
-- 2. ensure that permissions are correct
-- 3. format : 'A','|','B','C'
--       WHERE A = select statement
--             B = Database Directory Name as found on DBA_DIRECTORIES
--             C = output file name
--    eg.'select * from vw_extr_nielsens','|','/dwh_files/files.out','nielsen.wk'

--    AND FIN_YEAR_NO = 2014
--    AND FIN_WEEK_NO BETWEEN 1 AND 36
--**************************************************************************************************
--DBMS_OUTPUT.PUT_LINE('Department extract');
--    G_COUNT := DWH_GENERIC_FILE_EXTRACT(
--    'SELECT /*+ PARALLEL(DNS,8) FULL(DNS) */
--     LOCATION_NO, LOCATION_NAME, REGION_NO, REGION_NAME,DEPARTMENT_NO,DEPARTMENT_NAME,FIN_YEAR_NO, FIN_WEEK_NO,max(THIS_WEEK_START_DATE),
--     SUM(NVL(DNS.SALES_QTY,0)) AS SALES_QTY,SUM(NVL(DNS.SALES,0)) AS SALES
--     FROM RTL_LOC_ITEM_WK_RMS_DENSE DNS,DIM_ITEM DI,DIM_LOCATION DL
--     WHERE DNS.SK1_ITEM_NO = DI.SK1_ITEM_NO
--     and dns.sk1_location_no = dl.sk1_location_no
--    AND THIS_WEEK_START_DATE BETWEEN ''26 Mar 2012'' AND ''27 Apr 2014''
--     AND LOC_TYPE = ''S''
--     AND DEPARTMENT_NO = 75
--     GROUP BY FIN_YEAR_NO, FIN_WEEK_NO, LOCATION_NO, LOCATION_NAME, DEPARTMENT_NO, DEPARTMENT_NAME, REGION_NO, REGION_NAME',
--     '|','DWH_FILES_OUT','aztec_dept75_all.txt'); 
--     l_text :=  'Records extracted to aztec_dept.txt'||g_count;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
dbms_output.put_line('Sub Class extract');
     g_count := dwh_generic_file_extract(
     'SELECT /*+ PARALLEL(DNS,8) FULL(DNS)*/
     LOCATION_NO, LOCATION_NAME, REGION_NO, REGION_NAME,SUBCLASS_NO,SUBCLASS_NAME,FIN_YEAR_NO, FIN_WEEK_NO,max(THIS_WEEK_START_DATE),
     SUM(NVL(DNS.SALES_QTY,0)) AS SALES_QTY,SUM(NVL(DNS.SALES,0)) AS SALES      
     FROM RTL_LOC_ITEM_WK_RMS_DENSE DNS,DIM_ITEM DI,DIM_LOCATION DL
     WHERE DNS.SK1_ITEM_NO = DI.SK1_ITEM_NO
     AND DNS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
     AND THIS_WEEK_START_DATE BETWEEN ''26 MAR 2012'' AND ''29 Jun 2014''  
     AND LOC_TYPE = ''S''      
     AND DEPARTMENT_NO = 48 AND di.SUBCLASS_NO IN (2,3,4,5,6,7)
     GROUP BY FIN_YEAR_NO, FIN_WEEK_NO, LOCATION_NO, LOCATION_NAME, SUBCLASS_NO, SUBCLASS_NAME, REGION_NO, REGION_NAME',
     '|','DWH_FILES_OUT','aztec_subclass48_all.txt');
    l_text :=  'Records extracted to aztec_subclass.txt '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--DBMS_OUTPUT.PUT_LINE('Item extract 48');

--      G_COUNT := DWH_GENERIC_FILE_EXTRACT(
--      'select  /*+ PARALLEL(DENSE,8) FULL(DENSE) */
/*       LOC.LOCATION_NO, LOC.LOCATION_NAME, REGION_NO, REGION_NAME, ITM.ITEM_NO, ITM.ITEM_DESC, DENSE.FIN_YEAR_NO, DENSE.FIN_WEEK_NO,THIS_WEEK_START_DATE,
       DENSE.SALES_QTY, DENSE.SALES
       FROM DIM_ITEM ITM, DIM_LOCATION LOC, RTL_LOC_ITEM_WK_RMS_DENSE DENSE
       WHERE LOC.SK1_LOCATION_NO = DENSE.SK1_LOCATION_NO
       AND ITM.SK1_ITEM_NO = DENSE.SK1_ITEM_NO
       AND THIS_WEEK_START_DATE BETWEEN ''26 Mar 2012'' AND ''27 Apr 2014'' 
       AND ITM.ITEM_NO IN (3046920000925,
  3046920001748,3046920022538,3046920022545,3046920022552,3046920022569,3046920027267,3046920027298,3046920028004,3046920028363,3046920028370,3046920028585,3046920028752,
  3046920029452,3046920029582,3046920029674,3046920029759,3046920043854,3046920084000,4000539014000,4000539014307,4000539363108,6009900135519,7610400010481,7610400013222,
  7610400013239,7610400014632,7610400014649,7610400030632,7610400039796,7610400061049,7610400067362,7610400067690,7610400068505,7610400068512,7610400068529,7610400068536,
  7610400069397,7610400069472,7610400071628,7610400071925,7610400073981,7610400074155,7610400075770,7610400075787,7610400075794,7610400075848,7610400077392,8003340091136,
  8003340091280,8003340095509,8003340095516,8003340095523,8003340095530,8003340099002)',
      '|','DWH_FILES_OUT','aztec_item48.txt');
      l_text :=  'Records extracted to aztec_item48_all.txt '||g_count;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
  DBMS_OUTPUT.PUT_LINE('Item extract 75');
      G_COUNT := DWH_GENERIC_FILE_EXTRACT(
 */     
 --     'select  /*+ PARALLEL(DENSE,8) FULL(DENSE) */
 /*      LOC.LOCATION_NO, LOC.LOCATION_NAME, REGION_NO, REGION_NAME, ITM.ITEM_NO, ITM.ITEM_DESC, DENSE.FIN_YEAR_NO, DENSE.FIN_WEEK_NO,THIS_WEEK_START_DATE,
       DENSE.SALES_QTY, DENSE.SALES
       FROM DIM_ITEM ITM, DIM_LOCATION LOC, RTL_LOC_ITEM_WK_RMS_DENSE DENSE
       WHERE LOC.SK1_LOCATION_NO = DENSE.SK1_LOCATION_NO
       AND ITM.SK1_ITEM_NO = DENSE.SK1_ITEM_NO
       AND THIS_WEEK_START_DATE BETWEEN ''26 Mar 2012'' AND ''27 Apr 2014''
       AND ITM.ITEM_NO IN (93498739,96011683,96022078,9003600581321,37466064116,
  4000539361203,4000539387104,4000539387203,4000539606809,4000539609008,4000539618000,4000539631801,4000539631900,4000539632105,4000539661303,4000539669804,4000539670602,
  4000539670701,4000539670701,4000539671104,4000539671203,4000539671203,4000539671401,4000539674501,4000539675102,4000539688201,4000539727009,4000539727207,4000539727207,
  4000539740008,4000539740206,4000539740206,4000539770401,4000539770708,4000539771002,6009900135526,6009900135533,6009900135588,7610400075671,7610400075688,7610400075800,
  7610400075824,7610400076364,7610400076371,7610400081061,8003340062839,9003600582250)',
      '|','DWH_FILES_OUT','aztec_item75.txt');
      l_text :=  'Records extracted to aztec_item75_all.txt '||g_count;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    
*/
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
end wh_prf_corp_714ton1;
