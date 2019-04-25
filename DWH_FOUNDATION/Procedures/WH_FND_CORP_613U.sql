--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_613U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_613U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
  --  Date:        may 2015
  --  Author:      W Lyttle
  --  Purpose:     Derive first_dc_no for supply_chain_code = 'WH'
  --               This procedure will replace wh_fnd_corp_841u
  --  Tables:      Input  - fnd_rtl_shipment
  --               Output - TEMP_ALLOC_SHIP1
  --                        TEMP_ALLOC_SHIP2
  --                        TEMP_ALLOC_SHIP3
  --                        TEMP_ALLOC_SHIP4
  --                        TEMP_ALLOC_FIRST_DC_NO
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
  g_recs_updated  INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_recs_hospital INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_date          DATE;
  g_start_date    DATE;
  g_rec_out fnd_alloc_tracker_alloc%rowtype;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_613U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_apps;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_apps;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'create temp table with correct first_dc_no';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
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
  
  EXECUTE immediate 'alter session set workarea_size_policy=manual';
  EXECUTE immediate 'alter session set sort_area_size=100000000';
  EXECUTE immediate 'alter session enable parallel dml';
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  Dwh_Lookup.Dim_Control(G_Date);
      --- testing may 2015
      --g_date := '19 may 2015';
      --- testing may 2015
      
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- truncate temp table
  --**************************************************************************************************
    --    l_text := 'Running GATHER_TABLE_STATS ON TEMP_ALLOC_FIRST_DC_NO';
  --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --   DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
  --                                'TEMP_ALLOC_FIRST_DC_NO', DEGREE => 8);
  --*********************************************************************************************
  -- Bulk fetch loop controlling main program execution
  --**************************************************************************************************
  l_text := 'truncate table DWH_FOUNDATION.TEMP_ALLOC_SHIP1' ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  EXECUTE immediate 'truncate table dwh_foundation.TEMP_ALLOC_SHIP1';
  
    INSERT /*+ append */
    INTO dwh_foundation.TEMP_ALLOC_SHIP1
                    WITH selitm AS
                                (SELECT
                                  /*+   parallel(di,2) */
                                  item_no
                                FROM dim_item di
                                WHERE business_unit_no NOT IN (50,70)
                                )
                    SELECT
                            /*+   parallel(a,2) parallel(si,2) */
                            DISTINCT tsf_alloc_no alloc_no ,
                            a.item_no
                    FROM fnd_rtl_shipment a,
                          selitm si
                    WHERE a.item_no         = si.item_no
                    AND a.last_updated_date = g_date
                    AND TSF_ALLOC_NO       IS NOT NULL ;
  g_recs_read            := 0;
  g_recs_inserted        := 0;
  g_recs_read            := g_recs_read     + SQL%ROWCOUNT;
  g_recs_inserted        := g_recs_inserted + SQL%ROWCOUNT;
  
  l_text                 := 'Insert complete -- TEMP_ALLOC_SHIP1='||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  COMMIT;
  --------------------------------------------------------------------------------------
  l_text := 'truncate table DWH_FOUNDATION.TEMP_ALLOC_SHIP2' ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  EXECUTE immediate 'truncate table dwh_foundation.TEMP_ALLOC_SHIP2';
  
  INSERT /*+ append */
  INTO dwh_foundation.TEMP_ALLOC_SHIP2
              WITH selitm AS
                        (SELECT
                              /*+  MATERIALIZE */
                              item_no
                        FROM dim_item di
                        WHERE business_unit_no NOT IN (50,70)
                        )
              SELECT
                        /*+   parallel(a,4) parallel(si,4) */
                        DISTINCT a.alloc_no,
                        a.item_no
                FROM fnd_rtl_allocation a,
                     selitm si
                WHERE a.po_no IS NULL
                AND a.item_no  = si.item_no
                AND a.last_updated_date = g_date
                 ;
    g_recs_read     := 0;
    g_recs_inserted := 0;
    g_recs_read     := g_recs_read     + SQL%ROWCOUNT;
    g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;
    
    l_text          := 'Insert complete -- extra ship into TEMP_ALLOC_SHIP2='||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    COMMIT;
--------------------------------------------------------------------------------------
    l_text := 'truncate table DWH_FOUNDATION.TEMP_ALLOC_SHIP3' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate 'truncate table dwh_foundation.TEMP_ALLOC_SHIP3';
    
    INSERT /*+ append */
    INTO dwh_foundation.TEMP_ALLOC_SHIP3
                    SELECT
                          /*+   parallel(a,4) parallel(si,4) */
                          DISTINCT a.alloc_no,
                          a.item_no
                    FROM fnd_rtl_allocation a,
                         dwh_foundation.TEMP_ALLOC_SHIP1 si
                    WHERE a.po_no    IS NULL -- IE. supply_chain_code = 'WH'
                    AND a.item_no     = si.item_no
                    AND release_date IS NOT NULL
                    AND a.alloc_no    = si.alloc_no
              UNION
                    ( SELECT a.* FROM dwh_foundation.TEMP_ALLOC_SHIP2 a
                    MINUS
                    SELECT b.* FROM dwh_foundation.TEMP_ALLOC_SHIP1 b
                    ) ;
    g_recs_read     := 0;
    g_recs_inserted := 0;
    g_recs_read     := g_recs_read     + SQL%ROWCOUNT;
    g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;
    
    l_text          := 'Insert complete -- extra ship into TEMP_ALLOC_SHIP3='||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    COMMIT;
--------------------------------------------------------------------------------------
    l_text := 'truncate table DWH_FOUNDATION.TEMP_ALLOC_SHIP4' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        EXECUTE immediate 'truncate table dwh_foundation.TEMP_ALLOC_SHIP4';
        
    INSERT /*+ append */ INTO DWH_FOUNDATION.TEMP_ALLOC_SHIP4
          WITH selalloc AS
                    (SELECT
                      /*+ materialize */
                      DISTINCT alloc_no
                    FROM dwh_foundation.TEMP_ALLOC_SHIP3
                    ORDER BY alloc_no
                    )
                  --,
                  --selship as
                  --        (
          SELECT
                /*+    parallel(a,4) parallel(s,4) */
                DISTINCT a.alloc_no ,
                s.from_loc_no ,
                s.to_loc_no
          FROM selalloc a
          JOIN fnd_rtl_shipment s
          ON A.alloc_no    = s.tsf_alloc_no ;
    
    g_recs_read     := 0;
    g_recs_inserted := 0;
    g_recs_read     := g_recs_read     + SQL%ROWCOUNT;
    g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;
    
    l_text          := 'Insert complete -- extra ship into TEMP_ALLOC_SHIP4='||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    COMMIT;
--------------------------------------------------------------------------------------
    l_text := 'truncate table DWH_FOUNDATION.TEMP_ALLOC_FIRST_DC_NO' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate 'truncate table dwh_foundation.TEMP_ALLOC_FIRST_DC_NO';
    
    INSERT /*+ append */ into dwh_foundation.temp_alloc_first_dc_no
 with    
                   selwh1 AS
                                      (SELECT 
                                      /*+  parallel(S,2) parallel(L,2) */ 
                                      TRUNC(sysdate) release_date,
                                              alloc_no,
                                              from_loc_no first_whse_supplier_no
                                    FROM DWH_FOUNDATION.TEMP_ALLOC_SHIP4 s
                                    JOIN dim_location l
                                        ON l.location_no = s.from_loc_no
                                    WHERE l.loc_type = 'W'
                                      GROUP BY alloc_no,
                                              from_loc_no),
                   selwh2 AS
                                      ( SELECT /*+  parallel(S,2) parallel(L,2) */   TRUNC(sysdate) release_date,
                                            alloc_no,
                                            to_loc_no first_whse_supplier_no
                                  FROM DWH_FOUNDATION.TEMP_ALLOC_SHIP4 s
                                  JOIN dim_location l
                                      ON l.location_no = s.to_loc_no
                                  WHERE l.loc_type = 'W'
                                    GROUP BY alloc_no,
                                              to_loc_no
                        ),
                   SELWH AS 
                      (      SELECT * FROM SELWH1 
                       MINUS SELECT * FROM SELWH2)
      SELECT
              /*+ parallel(a,4) parallel(B,4) */
                  a.alloc_no ,
                  a.first_whse_supplier_no ,
                  b.to_loc_no first_dc_no
            FROM selwh a,
                  fnd_rtl_shipment b
            WHERE a.alloc_no  = b.tsf_alloc_no
            AND a.first_whse_supplier_no = b.from_loc_no
            GROUP BY
                  a.alloc_no ,
                  a.first_whse_supplier_no ,
                  b.to_loc_no ;
      
    g_recs_read     := 0;
    g_recs_inserted := 0;
    g_recs_read     := g_recs_read     + SQL%ROWCOUNT;
    g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;
    
    l_text          := 'Insert complete -- extra ship into TEMP_ALLOC_SHIP12='||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    COMMIT;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_read||g_recs_read;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
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
END WH_FND_CORP_613U;
