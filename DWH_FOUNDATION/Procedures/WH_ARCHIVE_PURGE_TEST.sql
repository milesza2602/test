--------------------------------------------------------
--  DDL for Procedure WH_ARCHIVE_PURGE_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_ARCHIVE_PURGE_TEST" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2017
--  Author:      Quentin Smit
--  Purpose:     load and process data pertaining to the RMS item / po / shipment reuse project
--               with input ex staging table from RMS.
--  Tables:      Input  - many
--               Output - ww_arch_metadataq
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_subgroup_hsp.sys_process_msg%type;
g_rec_out            fnd_subgroup%rowtype;
g_rec_in             stg_rms_subgroup_cpy%rowtype;
g_found              boolean;
g_valid              boolean;
g_restructure_ind    dim_control.restructure_ind%type;
g_group_no           fnd_subgroup.group_no%type;
--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

G_ERROR              BOOLEAN;

l_message            DWH_PERFORMANCE.sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_ARCHIVE_PURGE_TEST';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD AND PROCESS RMS ITEM/PO/SHIPMENT DATA TO BE ARCHIVED AND PURGED';
L_Process_Type       Sys_Dwh_Log_Summary.Log_Process_Type%Type := Dwh_Constants.Vc_Log_Process_Type_N;

   -- global definition
    l_directory              VARCHAR2 (256);
    g_error_message          VARCHAR2 (4000);
    
    -- local variables
    l_table_row_count        NUMBER;
    l_ext_table_row_count    NUMBER;
    l_external_table_name    VARCHAR2 (64);
    l_external_table_ddl     CLOB;   --VARCHAR2 (4000);
    L_EXT_TABLE_FILE_NAMES   VARCHAR2 (4000);
    l_error_stack            VARCHAR2 (1024);
    L_ERROR_BACKTRACE        VARCHAR2 (1024);
  
    L_WHERE_CLAUSE           varchar2(4000);
    L_string                 varchar2(4000);
    L_DATE_STRING            VARCHAR2(4000);
    l_sql_to_run             varchar2(1000);
    l_table_name             varchar2(30) := 'TAB1';
    l_table_owner            varchar2(30) := 'DWH_FOUNDATION';
    l_ext_table_ddl_sql      CLOB;   --varchar2(4000);
    l_ext_table_ddl_sql_1    varchar2(4000);
    l_ext_table_fname_01     VARCHAR2 (64) := 'TAB_FNAME1';
    l_ext_table_fname_02     VARCHAR2 (64) := 'TAB_FNAME2';
    l_ext_table_fname_03     VARCHAR2 (64) := 'TAB_FNAME3';
    l_ext_table_fname_04     VARCHAR2 (64) := 'TAB_FNAME4';
    l_sql_length             number;
    l_year_no                number := 1900;
    L_WEEK_NO                NUMBER;
    l_partition_name         VARCHAR2(30);
    L_PART_DATE              DATE;
    L_FROM_DATE              DATE;
    L_TO_DATE                DATE;
    L_CURSOR_STRING          VARCHAR2(4000);
    L_WHERE_FIELD            VARCHAR2(30);
     
    TYPE DATE_CUR            IS REF CURSOR;
    V_DATE_CURSOR            DATE_CUR;
     
    L_REC_CNT                NUMBER;
    L_EXT_TABLE_NAME         varchar2(30);
    L_SHORT_NAME             VARCHAR2(30);
  
    l_instring               varchar2(4000);
    l_comma_pos              number;
      
    L_CNT_STRING              VARCHAR2(4000);
    L_ROW_COUNT               NUMBER;
    --L_Ext_Table_File_Names	  Varchar2(4000 Byte);
    L_Num_Rows                Number;
    O_ERROR_MESSAGE           VARCHAR(1024);
    
    META_FAIL                 EXCEPTION;
    
    L_PART_COLUNN             VARCHAR2(4000);
    
    L_TEST_STRING             VARCHAR2(100);
    L_SEQ_NO                  NUMBER;
    L_CSTRING                 CLOB;
    L_TAB_LEN                 NUMBER;
    L_PAD_LEN                 NUMBER;
    L_STATE                   VARCHAR2(50 BYTE);
    L_AVG_ROW_LEN             NUMBER;
    L_TABLE_SIZE              NUMBER;
    
    L_CNT1                    NUMBER;
    
   

   -- NON-PARTITIONED FND TABLES
   --===========================
   CURSOR C2 IS
   SELECT *  --UNIQUE A.TABLE_OWNER, A.TABLE_NAME, FIN_YEAR_NO
     FROM W6005682.FND_NON_PART_ARCH_TABS1 
  --  WHERE TABLE_NAME IN ('FND_RTL_ALLOCATION')   --('FND_ITEM_VAT_RATE')
  --WHERE TABLE_NAME IN ('FND_ITEM_VAT_RATE', 'FND_RTL_ALLOCATION')
  ORDER BY TABLE_NAME
     ;
     

--======================================================================
-- LOAD NON-PARTITIONED FOUNDATION DATA
--======================================================================
procedure load_non_partitioned_fnd_data as 
BEGIN

  l_text := 'Starting load of non-partitioned day tables';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  FOR r IN C2
   LOOP
   
     L_TABLE_NAME   := r.TABLE_NAME;
     L_WHERE_FIELD  := r.WHERE_FIELD;
     --L_SHORT_NAME   := r.TABLE_SHORT_NAME;
     L_TAB_LEN      := LENGTH(L_TABLE_NAME);
     L_TABLE_OWNER  := r.TABLE_OWNER;
     
     SELECT AVG_ROW_LEN
       INTO L_AVG_ROW_LEN
       FROM dba_tables
       WHERE TABLE_NAME = L_TABLE_NAME;
       
    SELECT COUNT(*) INTO L_CNT1 FROM W6005682.WW_ARCH_METADATAQ;
    
    IF L_CNT1 > 0 THEN
       SELECT MAX(TABLE_SEQUENCE) INTO L_SEQ_NO FROM W6005682.WW_ARCH_METADATAQ;
     ELSE
       L_SEQ_NO :=1;
    END IF;
     
    -- dbms_output.put_line('--** TABLE BEING PROCESSED - ' || L_TABLE_NAME); 
     
    L_CURSOR_STRING := 'SELECT /*+ FULL(A) PARALLEL(A,4) */ B.FIN_YEAR_NO,  COUNT(*) FROM ' || L_TABLE_NAME || ' A, DIM_CALENDAR B WHERE A.' || L_WHERE_FIELD || ' = B.CALENDAR_DATE AND A.' || L_WHERE_FIELD || ' < ''29/JUN/09'' GROUP BY B.FIN_YEAR_NO ORDER BY B.FIN_YEAR_NO';
    
    L_STRING := 'LOADING META DATA FOR TABLE ' || L_TABLE_NAME;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_string);
    
    OPEN V_DATE_CURSOR FOR L_CURSOR_STRING;  -- USING '29/JUN/09';
        LOOP
         
            FETCH V_DATE_CURSOR INTO L_YEAR_NO, L_REC_CNT;
            EXIT WHEN V_DATE_CURSOR%NOTFOUND;
            
            L_SEQ_NO := L_SEQ_NO + 1;
            
            -- ADD A SEQUENCE NUMBER TO THE EXTERNAL TABLE NAME TO MAKE IT UNIQUE.
            --IF NAME OF THE DERVIED EXTERNAL TABLE IS TOO LONG, SUBSTRING IT SHORTEN IT
            ----------------------------------------------------------------------------------
            if L_TAB_LEN < 24 THEN
                L_EXT_TABLE_NAME := L_TABLE_NAME || '_' || L_SEQ_NO;
               --DBMS_OUTPUT.PUT_LINE('1 = ' || L_EXT_TABLE_NAME);
            ELSE
                L_EXT_TABLE_NAME := TO_CHAR((substr(L_TABLE_NAME, 1,23))) || '_' || L_SEQ_NO;
            END IF;
            
            --dbms_output.put_line('--TABLE - ' || L_TABLE_NAME || ' // YEAR - ' || L_YEAR_NO || ' RECORDS = ' || L_REC_CNT);  
            --dbms_output.put_line(' ');  
            
            L_TEXT := '--TABLE - ' || L_TABLE_NAME || ' // YEAR - ' || L_YEAR_NO || ' RECORDS = ' || L_REC_CNT;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            
            --L_DIRECTORY       := 'DWHPRD_ARCHIVE/' || L_YEAR_NO;
            L_DIRECTORY := 'QS_ARCH_' || L_YEAR_NO;
            
            l_ext_table_fname_01    := L_TABLE_NAME || '_' || L_SEQ_NO ||'_1a';
            l_ext_table_fname_02    := L_TABLE_NAME || '_' || L_SEQ_NO ||'_2a';
            l_ext_table_fname_03    := L_TABLE_NAME || '_' || L_SEQ_NO ||'_3a';
            l_ext_table_fname_04    := L_TABLE_NAME || '_' || L_SEQ_NO ||'_4a';
            
            L_EXT_TABLE_FILE_NAMES := l_ext_table_fname_01 || ',' || l_ext_table_fname_02 || ',' || l_ext_table_fname_03 || ',' || l_ext_table_fname_04;
          
            -- SET UP THE START OF THE EXTERNAL TABLE DDL SQL - NEW TABLE
            --------------------------------------------------
            --L_EXT_TABLE_DDL_SQL := 'CREATE TABLE ' || L_TABLE_NAME || '_' || l_year_no || '_EXT organization external (type oracle_datapump default directory ' || l_directory
            L_EXT_TABLE_DDL_SQL := 'CREATE TABLE ' || L_EXT_TABLE_NAME ||' organization external (type oracle_datapump default directory ' || l_directory
             || ' location ( '
             || ''''
             || l_ext_table_fname_01
             || ''''
             || ','
             || ''''
             || l_ext_table_fname_02
             || ''''
             || ','
             || ''''
             || l_ext_table_fname_03
             || ''''
             || ','
             || ''''
             || l_ext_table_fname_04
             || ''''
             || ') '
             || ') parallel (degree 4) as select /*+ parallel (t,4)  full(t) */ t.* from ' || L_TABLE_NAME || ' t, dim_calendar dc WHERE t.' || L_WHERE_FIELD || ' = dc.calendar_date and dc.fin_year_no = ' || L_YEAR_NO  ;
            
            L_DATE_STRING := 'SELECT MIN(CALENDAR_DATE), MAX(CALENDAR_DATE) FROM DIM_CALENDAR WHERE FIN_YEAR_NO = :L_YEAR';
            --dbms_output.put_line (L_DATE_STRING); 
            --dbms_output.put_line(' '); 
            
            EXECUTE IMMEDIATE L_DATE_STRING INTO L_FROM_DATE, L_TO_DATE USING L_YEAR_NO;
              
            L_WHERE_CLAUSE := 'WHERE ' || L_WHERE_FIELD || ' BETWEEN ''' || L_FROM_DATE || ''' AND ''' || L_TO_DATE || ''' ' ;
            --dbms_output.put_line (L_WHERE_CLAUSE); 
            
            --L_CNT_STRING := 'SELECT COUNT(*) FROM ' || L_TABLE_NAME || ' ' || L_WHERE_CLAUSE;
            
            L_STATE := 'START';
--            IF L_TABLE_NAME = 'FND_RTL_ALLOCATION'
--            OR L_TABLE_NAME = 'FND_RTL_SHIPMENT' THEN
--               L_STATE := 'MANUAL';
--            END IF;

            L_TABLE_SIZE := ROUND((((L_REC_CNT * L_AVG_ROW_LEN) / 1024) / 1024),2);  
            
            IF L_TABLE_SIZE > 400 THEN
               L_STATE := 'EXT_TAB_CREATED_MANUAL_DELETE_NEEDED';
               L_TEXT := ' TABLE ' || L_TABLE_NAME || ' FOR YEAR ' || L_YEAR_NO || ' - SIZE > 400MB - MANUAL COPY OUT NEEDED ';
            ELSE
               L_STATE := 'EXT_TAB_CREATED';
               L_TEXT := ' TABLE ' || L_TABLE_NAME || ' FOR YEAR ' || L_YEAR_NO || ' - SIZE < 400MB - DELETE WILL BE OK';
            END IF;
            DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
            
            INSERT INTO W6005682.WW_ARCH_METADATAQ (WHEN_LOADED, TABLE_SEQUENCE,  TABLE_OWNER, TABLE_NAME,  TABLE_ROW_COUNT, WHERE_CLAUSE, EXT_TABLE_NAME, EXT_TABLE_DDL, EXT_TABLE_FILE_NAMES, STATE, AVG_ROW_LEN, TABLE_SIZE)  --, INTERVAL_VAL) 
            VALUES (SYSDATE, L_SEQ_NO, L_TABLE_OWNER, L_TABLE_NAME, L_REC_CNT, L_WHERE_CLAUSE, L_EXT_TABLE_NAME, L_EXT_TABLE_DDL_SQL, L_EXT_TABLE_FILE_NAMES, L_STATE, L_AVG_ROW_LEN, L_TABLE_SIZE);   --, L_INTERVAL);
            COMMIT;
        
        END LOOP;
        
     CLOSE V_DATE_CURSOR;
     
          
  END LOOP;       --C2
  
  l_text := 'Completed load of non-partitioned day tables';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  EXCEPTION
   WHEN OTHERS
   THEN
      l_error_stack := SUBSTR (DBMS_UTILITY.format_error_stack, 1, 256);
      l_error_backtrace :=
         SUBSTR (DBMS_UTILITY.format_error_backtrace, 1, 256);
      o_error_message :=
         o_error_message || ' ' || l_error_stack || ' ' || l_error_backtrace;
      DBMS_OUTPUT.put_line (l_error_stack);
      DBMS_OUTPUT.put_line (l_error_backtrace);
      RAISE;

end load_non_partitioned_fnd_data;
--==========================================================================================================


  
--==========================================================================================================
-- MAIN PROCESS
--W6005682.WW_ARCH_METADATAQ
begin

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'DWH ARCHIVE AND PURGING STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    -- SET ALL PREVIOUS META DATA ENTRIES TO BE IGNORED AS NEW RECORDS WILL BE CREATED BY THIS PROCESS
    --UPDATE W6005682.WW_ARCH_METADATAQ SET STATE = 'IGNORE' WHERE STATE NOT LIKE 'EXT_TAB_CREATE%' AND STATE != 'IGNORE'; ---AND WHEN_LOADED < (SYSDATE - 2); 
    
    --commit;  
--    
--    UPDATE W6005682.WW_ARCH_SQL_TO_RUN SET STATE = 'IGNORE';
--    COMMIT;
                         

   select fin_year_no into l_year_no
     from dim_calendar 
    where calendar_date = trunc(sysdate)
    ;
  

  load_non_partitioned_fnd_data;
     
  Commit;
  P_SUCCESS := TRUE;
  
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



END WH_ARCHIVE_PURGE_TEST;
