--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_714E_AZTEC_SLAB_SS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_714E_AZTEC_SLAB_SS" (p_forall_limit in integer,p_success out boolean) 
as

--**************************************************************************************************
--  Date:        April 2014
--  Author:      Alastair de Wet
--  Purpose:     Create Aztec extract to flat file in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - various incl dense(p_forall_limit in integer,

--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  Notes:       RUN ON THE 11TH NIGHT OF EACH MONTH via Automic (Batch)
--
-- Changes : 
--
-- Aztec requires 5 new extracts with headers.                 Shuaib Salie 03/10/2018
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

g_previous_month_day_7    date;
g_current_month_day_6     date;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

g_date_yyyymm        varchar2(6)   := TO_CHAR(sysdate,'YYYYMM');

g_file_name1         varchar2(50);
g_file_name2         varchar2(50);
g_file_name3         varchar2(50);
g_file_name4         varchar2(50);
g_file_name5         varchar2(50);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_714E_AZTEC_SLAB_SS';
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
    --dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

    -- Set up post date month range variables

    SELECT ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1) + 6 into g_previous_month_day_7 FROM dual;

    SELECT ADD_MONTHS(TRUNC(SYSDATE, 'MM'),0) + 5 into g_current_month_day_6 FROM dual;  
    
    
 --   g_previous_month_day_7 := '07-SEPT-2018';
 --   g_current_month_day_6 := '06-OCT-2018';

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

    --execute immediate 'alter session enable parallel dml'; --Only for DEV and UAT
    /* File Name 3 : aztec_chocslab48_201809 */


--g_date_yyyymm          := '201901'; -- JAN 2019
--g_date_yyyymm          := '201812'; -- DEC 2018
--g_date_yyyymm          := '201811'; -- NOV 2018
g_date_yyyymm          := '201810'; -- OCT 2018


    g_file_name3 := 'aztec_chocslab48_'||g_date_yyyymm||'.txt';    
    DBMS_OUTPUT.PUT_LINE(g_file_name3||sysdate);          

    G_COUNT := DWH_GENERIC_FILE_EXTRACT(
    ' WITH sub1 AS
      (SELECT 
          SUPPLIER_NO,
          SUPPLIER_NAME,
          SK1_ITEM_NO,
          SUBCLASS_NO, 
          SUBCLASS_NAME,
          ITEM_NO,
          ITEM_DESC
       FROM dim_supplier a,
            dim_item b
       WHERE a.SK1_SUPPLIER_NO = b.SK1_SUPPLIER_NO
         AND department_no       = 48
         AND subclass_no        IN (51,52,53,54,71)
      )
        --HEADER
        SELECT
           
            ''LOCATION_NO'',
            ''LOCATION_NAME'',
            ''REGION_NO'',
            ''REGION_NAME'',
            ''SUBCLASS_NO'',
            ''SUBCLASS_NAME'',
            ''ITEM_NO'',
            ''ITEM_DESC'', 
        --    ''THIS_WEEK_START_DATE'',
            ''FIN_YEAR'',
            ''FIN_WEEK'',
            ''DATE'',             
            ''SALES_QTY'',
            ''SALES''
        from dual
       UNION ALL      
        SELECT
            /*+ PARALLEL(DNS,6) FULL(DNS) */
             
              to_char(LOCATION_NO),
              to_char(LOCATION_NAME),
              to_char(REGION_NO),
              to_char(REGION_NAME),
              to_char(SUBCLASS_NO),
              to_char(SUBCLASS_NAME),
              to_char(ITEM_NO),
              to_char(ITEM_DESC),               
              --replace(to_char(dc.this_week_start_date),'' 00:00'') ,
              to_char(dc.fin_year_no),
              to_char(dc.fin_week_no),
              --replace(to_char(POST_DATE),'' 00:00'') POST_DATE,             
              to_char(POST_DATE,''YYYY-MM-DD'')POST_DATE,                                                    
              to_char(SUM(NVL(DNS.SALES_QTY,0))) AS SALES_QTY,
              to_char(SUM(NVL(DNS.SALES,0)))    AS SALES
            FROM RTL_LOC_ITEM_DY_RMS_DENSE DNS,
              sub1 DI,
              DIM_LOCATION DL ,
              dim_calendar dc
            WHERE DNS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
            AND DNS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
            AND dns.post_date       = dc.calendar_date            
              --  AND POST_DATE  between '''||g_previous_month_day_7||''' AND '''||g_current_month_day_6||'''
         --      AND POST_DATE between ''07-DEC-2018'' and ''06-JAN-2019''    -- JAN 2019
        --     AND POST_DATE between ''07-NOV-2018'' and ''06-DEC-2018''    -- DEC 2018
   --          AND POST_DATE between ''07-OCT-2018'' and ''06-NOV-2018''    -- NOV 2018 
             AND POST_DATE between ''07-SEP-2018'' and ''06-OCT-2018''    -- OCT 2018
            AND LOC_TYPE = ''S''
            GROUP BY dc.this_week_start_date,
                     LOCATION_NO,
                     LOCATION_NAME,
                     SUBCLASS_NO,
                     SUBCLASS_NAME,
                     ITEM_NO,
                     ITEM_DESC,
                     dc.fin_year_no,
                     dc.fin_week_no,
                     REGION_NO,
                     REGION_NAME,
                     POST_DATE',
                         '|','DWH_FILES_OUT',g_file_name3
                 );

                  l_text :=  'Records extracted to '||g_file_name3||' :'||g_count;
                  DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

                  DBMS_OUTPUT.PUT_LINE('END : ' ||g_file_name3||' extract'||sysdate); 

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
end WH_PRF_CORP_714E_AZTEC_SLAB_SS;
