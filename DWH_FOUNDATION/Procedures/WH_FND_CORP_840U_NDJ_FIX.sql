--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_840U_NDJ_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_840U_NDJ_FIX" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN
       )
AS
  --**************************************************************************************************
  --  Date:        May 2015
  --  Author:      W Lyttle
  --  Purpose:     Load NON-DJ(David Jones) Allocation data to allocation tracker table FOR THE LAST 120 DAYS
  --               from the 4 temp tables split into 4 periods;
  --               NB> the last 120-days sub-partitions are truncated via a procedure before this one runs.
  --               For CHBD only.
  --  Tables:      Input  - temp_alloc_tracker_alloc_NDJ1,temp_alloc_tracker_alloc_NDJ2,temp_alloc_tracker_alloc_NDJ3,temp_alloc_tracker_alloc_NDJ4
  --               Output - fnd_alloc_tracker_alloc
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_error_count   NUMBER  := 0;
  G_ERROR_INDEX   NUMBER  := 0;
  g_RECS   NUMBER  := 0;
  
  g_tot_ins NUMBER  := 0;
  g_count_weeks NUMBER  := 0;

  g_min_fin_year_no NUMBER  := 0;
  g_min_fin_month_no NUMBER  := 0;
  g_min_fin_week_no NUMBER  := 0;

  g_max_fin_year_no NUMBER  := 0;
  g_max_fin_month_no NUMBER  := 0;
  g_max_fin_week_no NUMBER  := 0;

  G_COUNT_DAYS  NUMBER;
  G_PART_DT  VARCHAR2(6);
  G_SUBPART_NAME  VARCHAR2(200);
  G_SQL_TRUNC_PARTITION VARCHAR2(200);
  G_SUB_DATE DATE;
    g_begin_subpart VARCHAR2(200);
  g_end_subpart VARCHAR2(200);

  G_NAME VARCHAR2(100);
  G_DATE DATE;
  g_LAST_2_YEARS DATE;
  g_min_date DATE;
  g_max_date DATE;
  g_start_date DATE;
  g_rec_out fnd_alloc_tracker_alloc%rowtype;
  G_CNT_DATE       INTEGER := 0;
  G_CNT_ITEM       INTEGER := 0;
  G_CNT_ALLOC      INTEGER := 0;
  G_FILLRATE_QTY   NUMBER;
  G_ORIG_ALLOC_QTY NUMBER;
  G_CNT_RECS       INTEGER := 0;
 -- p_from_loc_no integer := 0;
 -- p_to_loc_no integer := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_840U_NDJ';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOADS Alloc Data TO ALLOC TRACKER TABLE';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

  --**************************************************************************************************
  -- Remove constraints and indexes
  --**************************************************************************************************
procedure a_remove_indexes as
BEGIN
     g_name := null; 

  BEGIN
    SELECT index_NAME
    into G_NAME
    FROM all_indexes
    where INDEX_NAME = 'I10_TEMP_SHIPMNTS_MIN_NDJ'
    
    AND TABLE_NAME        = 'TEMP_SHIPMENTS_MIN_NDJ';
    
    l_text               := 'drop INDEX DWH_foundation.I10_TEMP_SHIPMNTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_foundation.I10_TEMP_SHIPMNTS_MIN_NDJ');
    COMMIT;
    
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    l_text := 'index I10_TEMP_SHIPMNTS_MIN_NDJ does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
  BEGIN
    SELECT index_NAME
    into G_NAME
    FROM all_indexes
    where INDEX_NAME = 'I20_TEMP_SHIPMNTS_MIN_NDJ'
    
    AND TABLE_NAME        = 'TEMP_SHIPMENTS_MIN_NDJ';
    
    l_text               := 'drop INDEX DWH_foundation.I20_TEMP_SHIPMNTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_foundation.I20_TEMP_SHIPMNTS_MIN_NDJ');
    COMMIT;
    
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    l_text := 'index I20_TEMP_SHIPMNTS_MIN_NDJ does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
  
    BEGIN
    SELECT index_NAME
    into G_NAME
    FROM all_indexes
    where INDEX_NAME = 'I30_TEMP_SHIPMNTS_MIN_NDJ'
    
    AND TABLE_NAME        = 'TEMP_SHIPMENTS_MIN_NDJ';
    
    l_text               := 'drop INDEX DWH_foundation.I30_TEMP_SHIPMNTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_foundation.I30_TEMP_SHIPMNTS_MIN_NDJ');
    COMMIT;
    
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    l_text := 'index I30_TEMP_SHIPMNTS_MIN_NDJ does not exist';
    DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);
  END;
  
    BEGIN
    SELECT index_NAME
    into G_NAME
    FROM all_indexes
    where INDEX_NAME = 'I40_TEMP_SHIPMNTS_MIN_NDJ'
    
    AND TABLE_NAME        = 'TEMP_SHIPMENTS_MIN_NDJ';
    
    l_text               := 'drop INDEX DWH_foundation.I40_TEMP_SHIPMNTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_foundation.I40_TEMP_SHIPMNTS_MIN_NDJ');
    COMMIT;
    
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    l_text := 'index I40_TEMP_SHIPMNTS_MIN_NDJ does not exist';
    DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);
  END;
  
    BEGIN
    SELECT index_NAME
    into G_NAME
    FROM all_indexes
    where INDEX_NAME = 'I50_TEMP_SHIPMNTS_MIN_NDJ'
    
    AND TABLE_NAME        = 'TEMP_SHIPMENTS_MIN_NDJ';
    
    l_text               := 'drop INDEX DWH_foundation.I50_TEMP_SHIPMNTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_foundation.I50_TEMP_SHIPMNTS_MIN_NDJ');
    COMMIT;
    
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    l_text := 'index I50_TEMP_SHIPMNTS_MIN_NDJ does not exist';
    DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);
  END;
  
    BEGIN
    SELECT index_NAME
    into G_NAME
    FROM all_indexes
    where INDEX_NAME = 'I60_TEMP_SHIPMNTS_MIN_NDJ'
    
    AND TABLE_NAME        = 'TEMP_SHIPMENTS_MIN_NDJ';
    
    l_text               := 'drop INDEX DWH_foundation.I60_TEMP_SHIPMNTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_foundation.I60_TEMP_SHIPMNTS_MIN_NDJ');
    COMMIT;
    
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    l_text := 'index I60_TEMP_SHIPMNTS_MIN_NDJ does not exist';
    DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);
  END;
  
    BEGIN
    SELECT index_NAME
    into G_NAME
    FROM all_indexes
    where INDEX_NAME = 'I70_TEMP_SHIPMNTS_MIN_NDJ'
    
    AND TABLE_NAME        = 'TEMP_SHIPMENTS_MIN_NDJ';
    
    l_text               := 'drop INDEX DWH_foundation.I70_TEMP_SHIPMNTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_foundation.I70_TEMP_SHIPMNTS_MIN_NDJ');
    COMMIT;
    
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    l_text := 'index I70_TEMP_SHIPMNTS_MIN_NDJ does not exist';
    DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);
  END;

    BEGIN
    SELECT index_NAME
    into G_NAME
    FROM ALL_INDEXES
    where INDEX_NAME = 'I80_TEMP_SHIPMNTS_MIN_NDJ'
    
    AND TABLE_NAME        = 'TEMP_SHIPMENTS_MIN_NDJ';
    
    l_text               := 'drop INDEX DWH_foundation.I80_TEMP_SHIPMNTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_foundation.I80_TEMP_SHIPMNTS_MIN_NDJ');
    COMMIT;
    
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    l_text := 'index I80_TEMP_SHIPMNTS_MIN_NDJ does not exist';
    DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);
  END;

    BEGIN
    SELECT index_NAME
    into G_NAME
    FROM ALL_INDEXES
    where INDEX_NAME = 'I90_TEMP_SHIPMNTS_MIN_NDJ'
    
    AND TABLE_NAME        = 'TEMP_SHIPMENTS_MIN_NDJ';
    
    l_text               := 'drop INDEX DWH_foundation.I90_TEMP_SHIPMNTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
    EXECUTE immediate('drop INDEX DWH_foundation.I90_TEMP_SHIPMNTS_MIN_NDJ');
    COMMIT;
    
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    l_text := 'index I90_TEMP_SHIPMNTS_MIN_NDJ does not exist';
    DWH_LOG.WRITE_LOG (L_NAME, L_SYSTEM_NAME, L_SCRIPT_NAME,L_PROCEDURE_NAME, L_TEXT);
  END;
      g_name := null;
 

   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_remove_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_remove_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end a_remove_indexes;
  
  --**************************************************************************************************
  -- Insert into RTL table
  --**************************************************************************************************
procedure b_insert as
BEGIN

           g_last_2_years := g_date - 550;                            --(365+185 - last 1.5 rolling years)
          
            L_TEXT := 'g_last_2_years='||g_last_2_years;
            DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
            
            l_text := 'Insert into TEMP_SHIPMENTS_MIN_NDJ ';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            
          insert /*+ append */ into
           DWH_FOUNDATION.TEMP_SHIPMENTS_MIN_NDJ
                        SELECT  /*+ full(s) parallel(s,4) */              
                                      ITEM_NO	,
                                      SUPPLIER_NO	,
                                      PO_NO	,
                                      RECEIVE_DATE,
                                      TO_LOC_NO	,
                                      FROM_LOC_NO	,
                                      DIST_NO	,
                                      TSF_ALLOC_NO	,
                                      DU_ID	,
                                      RECEIVED_QTY 	,
                                      ACTL_RCPT_DATE ,
                                      CANCELLED_QTY	,
                                      LAST_UPDATED_DATE,
                                      FINAL_LOC_NO	,
                                      CHAIN_CODE
                        FROM FND_RTL_SHIPMENT s
                        WHERE LAST_UPDATED_DATE >= G_LAST_2_YEARS
                        and (chain_code IS NULL OR CHAIN_CODE = 'WW');

        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        L_TEXT := 'TEMP_SHIPMENTS_MIN_NDJ : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
  WHEN no_data_found THEN
        l_text := 'no data found for insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end b_insert;

--**************************************************************************************************
-- create primary key and index
--**************************************************************************************************
procedure c_add_indexes as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON TEMP_SHIPMENTS_MIN_NDJ';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_foundation', 'TEMP_SHIPMENTS_MIN_NDJ', DEGREE => 8);

      l_text := 'create INDEX DWH_foundation.I10_TEMP_SHIPMNTS_MIN_NDJ';
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      EXECUTE immediate('CREATE INDEX DWH_foundation.I10_TEMP_SHIPMNTS_MIN_NDJ ON DWH_foundation.TEMP_SHIPMENTS_MIN_NDJ (LAST_UPDATED_DATE)     
      TABLESPACE FND_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_foundation.I10_TEMP_SHIPMNTS_MIN_NDJ LOGGING NOPARALLEL') ;
      
      l_text := 'create INDEX DWH_foundation.I20_TEMP_SHIPMNTS_MIN_NDJ';
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      EXECUTE immediate('CREATE INDEX DWH_foundation.I20_TEMP_SHIPMNTS_MIN_NDJ ON DWH_foundation.TEMP_SHIPMENTS_MIN_NDJ (to_loc_no)     
      TABLESPACE FND_MASTER NOLOGGING  PARALLEL');
      Execute Immediate('ALTER INDEX DWH_foundation.I20_TEMP_SHIPMNTS_MIN_NDJ LOGGING NOPARALLEL') ;
      
      
      l_text := 'create INDEX DWH_foundation.I30_TEMP_SHIPMNTS_MIN_NDJ';
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      EXECUTE immediate('CREATE INDEX DWH_foundation.I30_TEMP_SHIPMNTS_MIN_NDJ ON DWH_foundation.TEMP_SHIPMENTS_MIN_NDJ (FROM_LOC_NO)     
      TABLESPACE FND_MASTER NOLOGGING  PARALLEL');
      EXECUTE IMMEDIATE('ALTER INDEX DWH_foundation.I30_TEMP_SHIPMNTS_MIN_NDJ LOGGING NOPARALLEL') ;

      
      l_text := 'create INDEX DWH_foundation.I40_TEMP_SHIPMNTS_MIN_NDJ';
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      EXECUTE immediate('CREATE INDEX DWH_foundation.I40_TEMP_SHIPMNTS_MIN_NDJ ON DWH_foundation.TEMP_SHIPMENTS_MIN_NDJ (FINAL_LOC_NO)     
      TABLESPACE FND_MASTER NOLOGGING  PARALLEL');
      EXECUTE IMMEDIATE('ALTER INDEX DWH_foundation.I40_TEMP_SHIPMNTS_MIN_NDJ LOGGING NOPARALLEL') ;

      
      l_text := 'create INDEX DWH_foundation.I50_TEMP_SHIPMNTS_MIN_NDJ';
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      EXECUTE immediate('CREATE INDEX DWH_foundation.I50_TEMP_SHIPMNTS_MIN_NDJ ON DWH_foundation.TEMP_SHIPMENTS_MIN_NDJ (PO_NO)     
      TABLESPACE FND_MASTER NOLOGGING  PARALLEL');
      EXECUTE IMMEDIATE('ALTER INDEX DWH_foundation.I50_TEMP_SHIPMNTS_MIN_NDJ LOGGING NOPARALLEL') ;

      
      l_text := 'create INDEX DWH_foundation.I60_TEMP_SHIPMNTS_MIN_NDJ';
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      EXECUTE immediate('CREATE INDEX DWH_foundation.I60_TEMP_SHIPMNTS_MIN_NDJ ON DWH_foundation.TEMP_SHIPMENTS_MIN_NDJ (ACTL_RCPT_DATE)     
      TABLESPACE FND_MASTER NOLOGGING  PARALLEL');
      EXECUTE IMMEDIATE('ALTER INDEX DWH_foundation.I60_TEMP_SHIPMNTS_MIN_NDJ LOGGING NOPARALLEL') ;

      
      l_text := 'create INDEX DWH_foundation.I70_TEMP_SHIPMNTS_MIN_NDJ';
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      EXECUTE immediate('CREATE INDEX DWH_foundation.I70_TEMP_SHIPMNTS_MIN_NDJ ON DWH_foundation.TEMP_SHIPMENTS_MIN_NDJ (TSF_ALLOC_NO)     
      TABLESPACE FND_MASTER NOLOGGING  PARALLEL');
      EXECUTE IMMEDIATE('ALTER INDEX DWH_foundation.I70_TEMP_SHIPMNTS_MIN_NDJ LOGGING NOPARALLEL') ;

      
      l_text := 'create INDEX DWH_foundation.I80_TEMP_SHIPMNTS_MIN_NDJ';
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      EXECUTE immediate('CREATE INDEX DWH_foundation.I80_TEMP_SHIPMNTS_MIN_NDJ ON DWH_foundation.TEMP_SHIPMENTS_MIN_NDJ (DIST_NO)     
      TABLESPACE FND_MASTER NOLOGGING  PARALLEL');
      EXECUTE IMMEDIATE('ALTER INDEX DWH_foundation.I80_TEMP_SHIPMNTS_MIN_NDJ LOGGING NOPARALLEL') ;
 
       
      l_text := 'create INDEX DWH_foundation.I90_TEMP_SHIPMNTS_MIN_NDJ';
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      EXECUTE immediate('CREATE INDEX DWH_foundation.I90_TEMP_SHIPMNTS_MIN_NDJ ON DWH_foundation.TEMP_SHIPMENTS_MIN_NDJ (DU_ID)     
      TABLESPACE FND_MASTER NOLOGGING  PARALLEL');
      EXECUTE IMMEDIATE('ALTER INDEX DWH_foundation.I90_TEMP_SHIPMNTS_MIN_NDJ LOGGING NOPARALLEL') ;     
      

  

   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in c_add_indexes';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

END C_ADD_INDEXES;

  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************
BEGIN
       IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
        g_forall_limit  := p_forall_limit;
      END IF;
      
      p_success := false;
      
      l_text    := dwh_constants.vc_log_draw_line;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
    
      dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

      execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      EXECUTE immediate 'alter session enable parallel dml';

      
      --**************************************************************************************************
      -- Look up batch date from dim_control
      --**************************************************************************************************
      Dwh_Lookup.Dim_Control(G_Date);
      -- TESTING
      --G_DATE := '13 MAY 2015';
      -- TESTING
      l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--
--       **** determine period
--       going to do from monday or current_day-120 to sunday current_week
--
      select distinct this_week_start_date, fin_year_no, fin_month_no, fin_week_no into g_min_date, g_min_fin_year_no, g_min_fin_month_no, g_min_fin_week_no
      from dim_calendar 
      where calendar_date = g_date - 120;

      select distinct this_week_end_date, fin_year_no, fin_month_no, fin_week_no into g_max_date, g_max_fin_year_no, g_max_fin_month_no, g_max_fin_week_no
      from dim_calendar 
      where calendar_date = g_date ;  

      select count(distinct this_week_start_date), COUNT(CALENDAR_DATE) into g_count_weeks, G_COUNT_DAYS
      from dim_calendar 
      where calendar_date  between g_min_date and g_max_date;

       l_text := 'DATA LOADED FOR PERIOD '||g_min_date||' TO '||g_max_date||' **no_of_weeks='||g_count_weeks;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     
   
--**************************************************************************************************
-- build partition_truncate_sql
  -- TFND_ATAND_150515 
--**************************************************************************************************

      G_SUB_DATE := G_MAX_DATE + 2;
      g_count_days := g_count_days - 1;
      


     g_tot_ins := 0; 

--*********************************************************************************************   
-- full insert
--********************************************************************************************* 




--------------------------------------------------------------------------------------------------------------------------------------------------
--
-- CREATING SHIPMENTS SUBSET FOR DAVID JONES
--
--------------------------------------------------------------------------------------------------------------------------------------------------


  B_INSERT;

  C_ADD_INDEXES;

 l_text := 'Running GATHER_TABLE_STATS ON TEMP_SHIPMENTS_MIN_NDJ';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
     DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION',
                                   'TEMP_SHIPMENTS_MIN_NDJ', DEGREE => 8);
--------------------------------------------------------------------------------------------------------------------------------------------------
    g_recs_read := g_tot_ins;
    g_recs_inserted := g_tot_ins;

  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read||' '||g_start_date||' TO '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted||' '||g_start_date||' TO '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := true;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
END WH_FND_CORP_840U_NDJ_FIX;
