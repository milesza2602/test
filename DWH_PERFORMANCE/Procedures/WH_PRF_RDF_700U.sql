--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_700U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_700U" (
        p_forall_limit IN INTEGER,
        p_success OUT BOOLEAN
    --    p_from_loc_no in integer,
    --    p_to_loc_no in integer
    )
    AS
-- *************************************************************************************************
-- * Notes from 12.2 upgrade performance tuning
-- *************************************************************************************************
-- Date:   2019-02-07
-- Author: Paul Wakefield
-- 1. Gather stats update to use WH_PRF_GENERIC_STATS and only gather stats on future partitions
-- 2. Remove drop and rebuild of PK.
-- 3. Tuned merge statements to remove FTS
-- **************************************************************************************************


      --**************************************************************************************************
      --  Date:        March 2008
      --  Author:      Alfonso Joshua
      --  Purpose:     Load weekly forecast LEVEL 1(LOCATION LEVEL) table in performance layer
      --               with input ex temporary RDF forecast level 1 table from foundation layer.
      --  Tables:      Input  - TEMP_LOC_ITM_WK_RDF_SYSFCST_L1
      --               Output - RTL_LOC_ITEM_RDF_WKFCST_L1
      --  Packages:    constants, dwh_log, dwh_valid
----------------------------PREV VERSION------------------------------------------------------------------------

--------------------------------NEW VERSION--------------------------------------------------------------------
      --  Maintenance:
      --  qc4340 - W LYTTLE: RDF Rollup of LEVEL 1(LOCATION LEVEL) data
      --                        - This procedure was copied from WH_PRF_RDF_005U in PRD
      --                        - was from temp_loc_item_wk_rdf_sysfcst	rtl_loc_item_wk_rdf_fcst
      --                          now from TEMP_RDF_LIW_L1	RTL_RDF_LOC_ITM_WK_L1
      --
      --  Naming conventions
      --  g_  -  Global variable
      --  l_  -  Log table variable
      --  a_  -  Array variable
      --  v_  -  Local variable as found in packages
      --  p_  -  Parameter
      --  c_  -  Prefix to cursor
      --**************************************************************************************************
      g_forall_limit      INTEGER := dwh_constants.vc_forall_limit;
      g_recs_read         INTEGER := 0;
      g_recs_updated      INTEGER := 0;
      g_recs_inserted     INTEGER := 0;
      g_recs_hospital     INTEGER := 0;
      g_error_count       NUMBER  := 0;
      g_error_index       NUMBER  := 0;
      g_count             NUMBER  := 0;
      g_fnd_count         NUMBER  := 0;
      g_today_fin_day_no  NUMBER  := 0;
      g_fd_num_catlg_days NUMBER  := 0;
      g_rec_out TEMP_LOC_ITM_WK_RDF_SYSFCST_L1%rowtype;
      g_found     BOOLEAN;
      g_date      DATE := TRUNC(sysdate);
      g_yesterday DATE := TRUNC(sysdate) - 1;
      l_message sys_dwh_errlog.log_text%type;
      l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_RDF_700U';
      l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rdf;
      l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_pln_prf;
      l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_rdf;
      l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
      l_text sys_dwh_log.log_text%type ;
      l_description sys_dwh_log_summary.log_description%type   := '(NEW)LOAD RDF WEEKLY FCST LEVEL 1 FACTS EX TEMP TABLES';
      l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
 
      l_range_text   varchar2(200 byte);
      g_min_year dim_calendar.fin_year_no%type;
      g_max_year dim_calendar.fin_year_no%type;
      g_min_year_from_wk dim_calendar.fin_week_no%type;
      g_min_year_to_wk dim_calendar.fin_week_no%type;
      g_max_year_from_wk dim_calendar.fin_week_no%type;
      g_max_year_to_wk dim_calendar.fin_week_no%type;
      
      --**************************************************************************************************
      -- Main process
      --**************************************************************************************************
    BEGIN
      IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
        g_forall_limit  := p_forall_limit;
      END IF;
      
      dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
      
      p_success := false;
      
      l_text    := dwh_constants.vc_log_draw_line;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      l_text := 'LOAD RTL_LOC_ITEM_RDF_WKFCST_L1 EX TEMP_LOC_ITM_WK_RDF_SYSFCST_L1 STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
      --**************************************************************************************************
      -- Look up batch date from dim_control
      --**************************************************************************************************
      dwh_lookup.dim_control(g_date);
      l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      execute immediate 'alter session set workarea_size_policy=manual';
      execute immediate 'alter session set sort_area_size=100000000';
      execute immediate 'alter session enable parallel dml';
      
    -- DETERMINE IF THE YEARS TO BE PROCESSED ARE ACROSS A SINGLE YEAR OR SPLIT OVER 2 YEARS:
    --  IF SPLIT OVER 2 YEARS THEN GET MIN AND MAX WEEK FOR EACH YEAR IN THE TEMP TABLE
    --  IF ONLY 1 YEAR THEN SIMPLY GET MIN AND MAX FOR THAT YEAR IN THE TEMP TABLE
    --=======================================================================================
      select min(fin_year_no), max(fin_year_no) 
        into g_min_year, g_max_year
        from TEMP_LOC_ITM_WK_RDF_SYSFCST_L1; 
        
      if g_min_year <> g_max_year then
         select min(fin_week_no), max(fin_week_no) 
           into g_min_year_from_wk, g_min_year_to_wk
           from TEMP_LOC_ITM_WK_RDF_SYSFCST_L1
          where fin_year_no = g_min_year;
          
          select min(fin_week_no), max(fin_week_no) 
           into g_max_year_from_wk, g_max_year_to_wk
           from TEMP_LOC_ITM_WK_RDF_SYSFCST_L1
          where fin_year_no = g_max_year;
          
          l_text := 'years and weeks in TEMP table : ('|| g_min_year || ' weeks  ' || g_min_year_from_wk|| ' to ' || g_min_year_to_wk||
                    ') and ('||g_max_year || ' weeks  ' || g_max_year_from_wk|| ' to ' || g_max_year_to_wk || ')';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
      else  
         
         select min(fin_week_no), max(fin_week_no) 
           into g_min_year_from_wk, g_min_year_to_wk
           from TEMP_LOC_ITM_WK_RDF_SYSFCST_L1
          where fin_year_no = g_min_year;
          
          l_text := 'year and weeks in TEMP table : ('|| g_min_year || ' weeks  ' || g_min_year_from_wk|| ' to ' || g_min_year_to_wk;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
      end if;
      
      --g_min_year := 'Moo';
      
      l_text := 'MERGE starting ' ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      --TRACING ADDED 17 MAY 2015 TO TRY ESTABLISH THE CAUSE OF DML/SELECT OVERHEADS THAT HAPPENS ON A SUNDAY
      --execute immediate 'alter session set events ''10046 trace name context forever, level 12'' ';
       
      --**************************************************************************************************
      -- Bulk fetch loop controlling main program execution
      --**************************************************************************************************
      
  if g_min_year <> g_max_year then
      l_text := 'SPLIT YEAR MERGE ' ;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --EXECUTE MERGE FOR WEEKS SPLIT OVER 2 YEARS
      --==========================================
      MERGE /*+ parallel(8) */ INTO dwh_performance.RTL_LOC_ITEM_RDF_WKFCST_L1 rdf_mart USING (
       
        SELECT /*+ PARALLEL(8) */
          DL.SK1_LOCATION_NO,
          DI.SK1_ITEM_NO ,
          TMP.FIN_YEAR_NO ,
          TMP.FIN_WEEK_NO ,
          DLH.SK2_LOCATION_NO, 
          DIH.SK2_ITEM_NO ,
          DC.FIN_WEEK_CODE ,
          SALES_WKLY_SYS_FCST_QTY,
          CASE
            WHEN DI.standard_uom_code = 'EA' AND DI.random_mass_ind    = 1 THEN
                  TMP.sales_wkly_sys_fcst_qty * ZI.reg_rsp * DI.static_mass
            ELSE
                        TMP.sales_wkly_sys_fcst_qty * ZI.reg_rsp
            END sales_wk_sys_fcst,
          SALES_WKLY_APP_FCST_QTY,
          CASE
            WHEN DI.standard_uom_code = 'EA' AND DI.random_mass_ind = 1 THEN
                  TMP.sales_wkly_APP_fcst_qty * ZI.reg_rsp * DI.static_mass
            ELSE
              TMP.sales_wkly_APP_fcst_qty * ZI.reg_rsp
            END sales_wk_APP_fcst,
          TMP.LAST_UPDATED_DATE,
          DC.THIS_WEEK_START_DATE,
          NULL ABS_APP_FCST_ERR_QTY_LL ,
          NULL ABS_RLTV_APP_FCST_ERR_QTY_LL, 
          NULL ABS_SYS_FCST_ERR_QTY_LL, 
          NULL ABS_RLTV_SYS_FCST_ERR_QTY_LL, 
          NULL ABS_DLY_APP_FCST_ERR_QTY_PERI, 
          NULL ABS_DLY_SYS_FCST_ERR_QTY_PERI ,
          NULL ABS_APP_FCST_ERR_QTY,
          NULL ABS_SYS_FCST_ERR_QTY, 
          NULL FCST_ERR_SLS_DLY_APP_FCST_QTY, 
          NULL FCST_ERR_SALES_DLY_APP_FCST ,
          NULL FCST_ERR_SLS_DLY_SYS_FCST_QTY, 
          NULL FCST_ERR_SALES_DLY_SYS_FCST, 
          NULL SALES_WK_APP_FCST_QTY_AV, 
          NULL SALES_WK_APP_FCST_QTY_FLT, 
          NULL SALES_WK_APP_FCST_QTY_FLT_AV
    FROM DWH_PERFORMANCE.TEMP_LOC_ITM_WK_RDF_SYSFCST_L1 TMP 
    JOIN dim_item di           ON TMP.item_no = di.item_no 
    JOIN dim_location dl       ON TMP.location_no = dl.location_no 
    JOIN dim_calendar dc       ON TMP.fin_year_no = dc.fin_year_no AND TMP.fin_week_no = dc.fin_week_no AND dc.fin_day_no = 4 
    JOIN dim_location_hist dlh ON TMP.location_no = dlh.location_no AND dc.calendar_date BETWEEN dlh.sk2_active_from_date AND dlh.sk2_active_to_date 
    JOIN dim_item_hist dih     ON TMP.item_no = dih.item_no AND dc.calendar_date BETWEEN dih.sk2_active_from_date AND dih.sk2_active_to_date 
    JOIN fnd_zone_item zi      on TMP.location_no = dl.location_no and dl.wh_fd_zone_group_no = zi.zone_group_no 
                                                                   and dl.wh_fd_zone_no    = zi.zone_no 
                                                                   and TMP.item_no     = zi.item_no
                                                                   
    WHERE  TMP.fin_year_no = g_min_year and TMP.fin_week_no between g_min_year_from_wk and g_min_year_to_wk
  
  UNION ALL
  
    SELECT /*+ PARALLEL(8) */
          DL.SK1_LOCATION_NO,
          DI.SK1_ITEM_NO ,
          TMP.FIN_YEAR_NO ,
          TMP.FIN_WEEK_NO ,
          DLH.SK2_LOCATION_NO, 
          DIH.SK2_ITEM_NO ,
          DC.FIN_WEEK_CODE ,
          SALES_WKLY_SYS_FCST_QTY,
          CASE
            WHEN DI.standard_uom_code = 'EA' AND DI.random_mass_ind    = 1 THEN
                  TMP.sales_wkly_sys_fcst_qty * ZI.reg_rsp * DI.static_mass
            ELSE
                        TMP.sales_wkly_sys_fcst_qty * ZI.reg_rsp
            END sales_wk_sys_fcst,
          SALES_WKLY_APP_FCST_QTY,
          CASE
            WHEN DI.standard_uom_code = 'EA' AND DI.random_mass_ind = 1 THEN
                  TMP.sales_wkly_APP_fcst_qty * ZI.reg_rsp * DI.static_mass
            ELSE
              TMP.sales_wkly_APP_fcst_qty * ZI.reg_rsp
            END sales_wk_APP_fcst,
          TMP.LAST_UPDATED_DATE,
          DC.THIS_WEEK_START_DATE,
          NULL ABS_APP_FCST_ERR_QTY_LL ,
          NULL ABS_RLTV_APP_FCST_ERR_QTY_LL, 
          NULL ABS_SYS_FCST_ERR_QTY_LL, 
          NULL ABS_RLTV_SYS_FCST_ERR_QTY_LL, 
          NULL ABS_DLY_APP_FCST_ERR_QTY_PERI, 
          NULL ABS_DLY_SYS_FCST_ERR_QTY_PERI ,
          NULL ABS_APP_FCST_ERR_QTY,
          NULL ABS_SYS_FCST_ERR_QTY, 
          NULL FCST_ERR_SLS_DLY_APP_FCST_QTY, 
          NULL FCST_ERR_SALES_DLY_APP_FCST ,
          NULL FCST_ERR_SLS_DLY_SYS_FCST_QTY, 
          NULL FCST_ERR_SALES_DLY_SYS_FCST, 
          NULL SALES_WK_APP_FCST_QTY_AV, 
          NULL SALES_WK_APP_FCST_QTY_FLT, 
          NULL SALES_WK_APP_FCST_QTY_FLT_AV
    FROM DWH_PERFORMANCE.TEMP_LOC_ITM_WK_RDF_SYSFCST_L1 TMP 
    JOIN dim_item di           ON TMP.item_no = di.item_no 
    JOIN dim_location dl       ON TMP.location_no = dl.location_no 
    JOIN dim_calendar dc       ON TMP.fin_year_no = dc.fin_year_no AND TMP.fin_week_no = dc.fin_week_no AND dc.fin_day_no = 4 
    JOIN dim_location_hist dlh ON TMP.location_no = dlh.location_no AND dc.calendar_date BETWEEN dlh.sk2_active_from_date AND dlh.sk2_active_to_date 
    JOIN dim_item_hist dih     ON TMP.item_no = dih.item_no AND dc.calendar_date BETWEEN dih.sk2_active_from_date AND dih.sk2_active_to_date 
    JOIN fnd_zone_item zi      on TMP.location_no = dl.location_no and dl.wh_fd_zone_group_no = zi.zone_group_no 
                                                                   and dl.wh_fd_zone_no    = zi.zone_no 
                                                                   and TMP.item_no     = zi.item_no
     
     WHERE TMP.fin_year_no = g_max_year and TMP.fin_week_no between g_max_year_from_wk and g_max_year_to_wk
    
  ) mer_mart
  
ON (mer_mart.sk1_item_no        = rdf_mart.sk1_item_no
and mer_mart.sk1_location_no    = rdf_mart.sk1_location_no
and mer_mart.fin_year_no        = rdf_mart.fin_year_no
and mer_mart.fin_week_no        = rdf_mart.fin_week_no
--and (
--     (rdf_mart.fin_year_no = g_min_year and rdf_mart.fin_week_no between g_max_year_from_wk and g_max_year_to_wk) 
--  or (rdf_mart.fin_year_no = g_max_year and rdf_mart.fin_week_no between g_min_year_from_wk and g_min_year_to_wk)
--    )

)
WHEN MATCHED THEN
UPDATE
SET       SK2_LOCATION_NO	                = mer_mart.SK2_LOCATION_NO,
          SK2_ITEM_NO	                    = mer_mart.SK2_ITEM_NO,
          FIN_WEEK_CODE	                  = mer_mart.FIN_WEEK_CODE,
          SALES_WK_SYS_FCST_QTY	          = mer_mart.SALES_WKLY_SYS_FCST_QTY,
          SALES_WK_SYS_FCST	              = mer_mart.SALES_WK_SYS_FCST,
          SALES_WK_APP_FCST_QTY	          = mer_mart.SALES_WKLY_APP_FCST_QTY,
          SALES_WK_APP_FCST	              = mer_mart.SALES_WK_APP_FCST,
          LAST_UPDATED_DATE	              = g_date,
          THIS_WEEK_START_DATE	          = mer_mart.THIS_WEEK_START_DATE,
          ABS_APP_FCST_ERR_QTY_LL	        = mer_mart.ABS_APP_FCST_ERR_QTY_LL,
          ABS_RLTV_APP_FCST_ERR_QTY_LL	  = mer_mart.ABS_RLTV_APP_FCST_ERR_QTY_LL,
          ABS_SYS_FCST_ERR_QTY_LL	        = mer_mart.ABS_SYS_FCST_ERR_QTY_LL,
          ABS_RLTV_SYS_FCST_ERR_QTY_LL	  = mer_mart.ABS_RLTV_SYS_FCST_ERR_QTY_LL,
          ABS_DLY_APP_FCST_ERR_QTY_PERI	  = mer_mart.ABS_DLY_APP_FCST_ERR_QTY_PERI,
          ABS_DLY_SYS_FCST_ERR_QTY_PERI	  = mer_mart.ABS_DLY_SYS_FCST_ERR_QTY_PERI,
          ABS_APP_FCST_ERR_QTY	          = mer_mart.ABS_APP_FCST_ERR_QTY,
          ABS_SYS_FCST_ERR_QTY	          = mer_mart.ABS_SYS_FCST_ERR_QTY,
          FCST_ERR_SLS_DLY_APP_FCST_QTY	  = mer_mart.FCST_ERR_SLS_DLY_APP_FCST_QTY,
          FCST_ERR_SALES_DLY_APP_FCST	    = mer_mart.FCST_ERR_SALES_DLY_APP_FCST,
          FCST_ERR_SLS_DLY_SYS_FCST_QTY	  = mer_mart.FCST_ERR_SLS_DLY_SYS_FCST_QTY,
          FCST_ERR_SALES_DLY_SYS_FCST	    = mer_mart.FCST_ERR_SALES_DLY_SYS_FCST,
          SALES_WK_APP_FCST_QTY_AV	      = mer_mart.SALES_WK_APP_FCST_QTY_AV,
          SALES_WK_APP_FCST_QTY_FLT	      = mer_mart.SALES_WK_APP_FCST_QTY_FLT,
          SALES_WK_APP_FCST_QTY_FLT_AV	  = mer_mart.SALES_WK_APP_FCST_QTY_FLT_AV
WHEN NOT MATCHED THEN
INSERT
(         SK1_LOCATION_NO,
          SK1_ITEM_NO,
          FIN_YEAR_NO,
          FIN_WEEK_NO,
          SK2_LOCATION_NO,
          SK2_ITEM_NO,
          FIN_WEEK_CODE,
          SALES_WK_SYS_FCST_QTY,
          SALES_WK_SYS_FCST,
          SALES_WK_APP_FCST_QTY,
          SALES_WK_APP_FCST,
          LAST_UPDATED_DATE,
          THIS_WEEK_START_DATE,
          ABS_APP_FCST_ERR_QTY_LL,
          ABS_RLTV_APP_FCST_ERR_QTY_LL,
          ABS_SYS_FCST_ERR_QTY_LL,
          ABS_RLTV_SYS_FCST_ERR_QTY_LL,
          ABS_DLY_APP_FCST_ERR_QTY_PERI,
          ABS_DLY_SYS_FCST_ERR_QTY_PERI,
          ABS_APP_FCST_ERR_QTY,
          ABS_SYS_FCST_ERR_QTY,
          FCST_ERR_SLS_DLY_APP_FCST_QTY,
          FCST_ERR_SALES_DLY_APP_FCST,
          FCST_ERR_SLS_DLY_SYS_FCST_QTY,
          FCST_ERR_SALES_DLY_SYS_FCST,
          SALES_WK_APP_FCST_QTY_AV,
          SALES_WK_APP_FCST_QTY_FLT,
          SALES_WK_APP_FCST_QTY_FLT_AV
          )
  values
(         mer_mart.SK1_LOCATION_NO,
          mer_mart.SK1_ITEM_NO,
          mer_mart.FIN_YEAR_NO,
          mer_mart.FIN_WEEK_NO,
          mer_mart.SK2_LOCATION_NO,
          mer_mart.SK2_ITEM_NO,
          mer_mart.FIN_WEEK_CODE,
          mer_mart.SALES_WKLY_SYS_FCST_QTY,
          mer_mart.SALES_WK_SYS_FCST,
          mer_mart.SALES_WKLY_APP_FCST_QTY,
          mer_mart.SALES_WK_APP_FCST,
          g_date,
          mer_mart.THIS_WEEK_START_DATE,
          mer_mart.ABS_APP_FCST_ERR_QTY_LL,
          mer_mart.ABS_RLTV_APP_FCST_ERR_QTY_LL,
          mer_mart.ABS_SYS_FCST_ERR_QTY_LL,
          mer_mart.ABS_RLTV_SYS_FCST_ERR_QTY_LL,
          mer_mart.ABS_DLY_APP_FCST_ERR_QTY_PERI,
          mer_mart.ABS_DLY_SYS_FCST_ERR_QTY_PERI,
          mer_mart.ABS_APP_FCST_ERR_QTY,
          mer_mart.ABS_SYS_FCST_ERR_QTY,
          mer_mart.FCST_ERR_SLS_DLY_APP_FCST_QTY,
          mer_mart.FCST_ERR_SALES_DLY_APP_FCST,
          mer_mart.FCST_ERR_SLS_DLY_SYS_FCST_QTY,
          mer_mart.FCST_ERR_SALES_DLY_SYS_FCST,
          mer_mart.SALES_WK_APP_FCST_QTY_AV,
          mer_mart.SALES_WK_APP_FCST_QTY_FLT,
          mer_mart.SALES_WK_APP_FCST_QTY_FLT_AV
          ) 
          ;
          
          g_recs_read := g_recs_read + SQL%ROWCOUNT;
          g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
          
          l_text := 'SPLIT YEAR MERGE DONE' ;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  ELSE
     --EXECUTE MERGE FOR WEEKS IN A SINGLE YEAR
     --========================================
     MERGE /*+ parallel(8) */ INTO dwh_performance.RTL_LOC_ITEM_RDF_WKFCST_L1 rdf_mart USING (
      
        SELECT /*+ PARALLEL(8) FULL(TMP) */
          DL.SK1_LOCATION_NO,
          DI.SK1_ITEM_NO ,
          TMP.FIN_YEAR_NO ,
          TMP.FIN_WEEK_NO ,
          DLH.SK2_LOCATION_NO, 
          DIH.SK2_ITEM_NO ,
          DC.FIN_WEEK_CODE ,
          SALES_WKLY_SYS_FCST_QTY,
          CASE
            WHEN DI.standard_uom_code = 'EA' AND DI.random_mass_ind    = 1 THEN
                  TMP.sales_wkly_sys_fcst_qty * ZI.reg_rsp * DI.static_mass
            ELSE
                        TMP.sales_wkly_sys_fcst_qty * ZI.reg_rsp
            END sales_wk_sys_fcst,
          SALES_WKLY_APP_FCST_QTY,
          CASE
            WHEN DI.standard_uom_code = 'EA' AND DI.random_mass_ind = 1 THEN
                  TMP.sales_wkly_APP_fcst_qty * ZI.reg_rsp * DI.static_mass
            ELSE
              TMP.sales_wkly_APP_fcst_qty * ZI.reg_rsp
            END sales_wk_APP_fcst,
          TMP.LAST_UPDATED_DATE,
          DC.THIS_WEEK_START_DATE,
          NULL ABS_APP_FCST_ERR_QTY_LL ,
          NULL ABS_RLTV_APP_FCST_ERR_QTY_LL, 
          NULL ABS_SYS_FCST_ERR_QTY_LL, 
          NULL ABS_RLTV_SYS_FCST_ERR_QTY_LL, 
          NULL ABS_DLY_APP_FCST_ERR_QTY_PERI, 
          NULL ABS_DLY_SYS_FCST_ERR_QTY_PERI ,
          NULL ABS_APP_FCST_ERR_QTY,
          NULL ABS_SYS_FCST_ERR_QTY, 
          NULL FCST_ERR_SLS_DLY_APP_FCST_QTY, 
          NULL FCST_ERR_SALES_DLY_APP_FCST ,
          NULL FCST_ERR_SLS_DLY_SYS_FCST_QTY, 
          NULL FCST_ERR_SALES_DLY_SYS_FCST, 
          NULL SALES_WK_APP_FCST_QTY_AV, 
          NULL SALES_WK_APP_FCST_QTY_FLT, 
          NULL SALES_WK_APP_FCST_QTY_FLT_AV
    FROM DWH_PERFORMANCE.TEMP_LOC_ITM_WK_RDF_SYSFCST_L1 TMP 
    JOIN dim_item di           ON TMP.item_no = di.item_no 
    JOIN dim_location dl       ON TMP.location_no = dl.location_no 
    JOIN dim_calendar dc       ON TMP.fin_year_no = dc.fin_year_no AND TMP.fin_week_no = dc.fin_week_no AND dc.fin_day_no = 4 
    JOIN dim_location_hist dlh ON TMP.location_no = dlh.location_no AND dc.calendar_date BETWEEN dlh.sk2_active_from_date AND dlh.sk2_active_to_date 
    JOIN dim_item_hist dih     ON TMP.item_no = dih.item_no AND dc.calendar_date BETWEEN dih.sk2_active_from_date AND dih.sk2_active_to_date 
    JOIN fnd_zone_item zi      on TMP.location_no = dl.location_no and dl.wh_fd_zone_group_no = zi.zone_group_no 
                                                                   and dl.wh_fd_zone_no    = zi.zone_no 
                                                                   and TMP.item_no     = zi.item_no
                                                                   
   WHERE TMP.fin_year_no = g_min_year
     AND TMP.fin_week_no between g_min_year_from_wk and g_min_year_to_wk
   
  ) mer_mart
  
ON (mer_mart.sk1_item_no        = rdf_mart.sk1_item_no
and mer_mart.sk1_location_no    = rdf_mart.sk1_location_no
and mer_mart.fin_year_no        = rdf_mart.fin_year_no
and mer_mart.fin_week_no        = rdf_mart.fin_week_no
--and rdf_mart.fin_year_no        = g_min_year 
--and rdf_mart.fin_week_no between g_min_year_from_wk and g_min_year_to_wk
)
WHEN MATCHED THEN
UPDATE
SET       SK2_LOCATION_NO	                = mer_mart.SK2_LOCATION_NO,
          SK2_ITEM_NO	                    = mer_mart.SK2_ITEM_NO,
          FIN_WEEK_CODE	                  = mer_mart.FIN_WEEK_CODE,
          SALES_WK_SYS_FCST_QTY	          = mer_mart.SALES_WKLY_SYS_FCST_QTY,
          SALES_WK_SYS_FCST	              = mer_mart.SALES_WK_SYS_FCST,
          SALES_WK_APP_FCST_QTY	          = mer_mart.SALES_WKLY_APP_FCST_QTY,
          SALES_WK_APP_FCST	              = mer_mart.SALES_WK_APP_FCST,
          LAST_UPDATED_DATE	              = g_date,
          THIS_WEEK_START_DATE	          = mer_mart.THIS_WEEK_START_DATE,
          ABS_APP_FCST_ERR_QTY_LL	        = mer_mart.ABS_APP_FCST_ERR_QTY_LL,
          ABS_RLTV_APP_FCST_ERR_QTY_LL	  = mer_mart.ABS_RLTV_APP_FCST_ERR_QTY_LL,
          ABS_SYS_FCST_ERR_QTY_LL	        = mer_mart.ABS_SYS_FCST_ERR_QTY_LL,
          ABS_RLTV_SYS_FCST_ERR_QTY_LL	  = mer_mart.ABS_RLTV_SYS_FCST_ERR_QTY_LL,
          ABS_DLY_APP_FCST_ERR_QTY_PERI	  = mer_mart.ABS_DLY_APP_FCST_ERR_QTY_PERI,
          ABS_DLY_SYS_FCST_ERR_QTY_PERI	  = mer_mart.ABS_DLY_SYS_FCST_ERR_QTY_PERI,
          ABS_APP_FCST_ERR_QTY	          = mer_mart.ABS_APP_FCST_ERR_QTY,
          ABS_SYS_FCST_ERR_QTY	          = mer_mart.ABS_SYS_FCST_ERR_QTY,
          FCST_ERR_SLS_DLY_APP_FCST_QTY	  = mer_mart.FCST_ERR_SLS_DLY_APP_FCST_QTY,
          FCST_ERR_SALES_DLY_APP_FCST	    = mer_mart.FCST_ERR_SALES_DLY_APP_FCST,
          FCST_ERR_SLS_DLY_SYS_FCST_QTY	  = mer_mart.FCST_ERR_SLS_DLY_SYS_FCST_QTY,
          FCST_ERR_SALES_DLY_SYS_FCST	    = mer_mart.FCST_ERR_SALES_DLY_SYS_FCST,
          SALES_WK_APP_FCST_QTY_AV	      = mer_mart.SALES_WK_APP_FCST_QTY_AV,
          SALES_WK_APP_FCST_QTY_FLT	      = mer_mart.SALES_WK_APP_FCST_QTY_FLT,
          SALES_WK_APP_FCST_QTY_FLT_AV	  = mer_mart.SALES_WK_APP_FCST_QTY_FLT_AV
WHEN NOT MATCHED THEN
INSERT
(         SK1_LOCATION_NO,
          SK1_ITEM_NO,
          FIN_YEAR_NO,
          FIN_WEEK_NO,
          SK2_LOCATION_NO,
          SK2_ITEM_NO,
          FIN_WEEK_CODE,
          SALES_WK_SYS_FCST_QTY,
          SALES_WK_SYS_FCST,
          SALES_WK_APP_FCST_QTY,
          SALES_WK_APP_FCST,
          LAST_UPDATED_DATE,
          THIS_WEEK_START_DATE,
          ABS_APP_FCST_ERR_QTY_LL,
          ABS_RLTV_APP_FCST_ERR_QTY_LL,
          ABS_SYS_FCST_ERR_QTY_LL,
          ABS_RLTV_SYS_FCST_ERR_QTY_LL,
          ABS_DLY_APP_FCST_ERR_QTY_PERI,
          ABS_DLY_SYS_FCST_ERR_QTY_PERI,
          ABS_APP_FCST_ERR_QTY,
          ABS_SYS_FCST_ERR_QTY,
          FCST_ERR_SLS_DLY_APP_FCST_QTY,
          FCST_ERR_SALES_DLY_APP_FCST,
          FCST_ERR_SLS_DLY_SYS_FCST_QTY,
          FCST_ERR_SALES_DLY_SYS_FCST,
          SALES_WK_APP_FCST_QTY_AV,
          SALES_WK_APP_FCST_QTY_FLT,
          SALES_WK_APP_FCST_QTY_FLT_AV
          )
  values
(         mer_mart.SK1_LOCATION_NO,
          mer_mart.SK1_ITEM_NO,
          mer_mart.FIN_YEAR_NO,
          mer_mart.FIN_WEEK_NO,
          mer_mart.SK2_LOCATION_NO,
          mer_mart.SK2_ITEM_NO,
          mer_mart.FIN_WEEK_CODE,
          mer_mart.SALES_WKLY_SYS_FCST_QTY,
          mer_mart.SALES_WK_SYS_FCST,
          mer_mart.SALES_WKLY_APP_FCST_QTY,
          mer_mart.SALES_WK_APP_FCST,
          g_date,
          mer_mart.THIS_WEEK_START_DATE,
          mer_mart.ABS_APP_FCST_ERR_QTY_LL,
          mer_mart.ABS_RLTV_APP_FCST_ERR_QTY_LL,
          mer_mart.ABS_SYS_FCST_ERR_QTY_LL,
          mer_mart.ABS_RLTV_SYS_FCST_ERR_QTY_LL,
          mer_mart.ABS_DLY_APP_FCST_ERR_QTY_PERI,
          mer_mart.ABS_DLY_SYS_FCST_ERR_QTY_PERI,
          mer_mart.ABS_APP_FCST_ERR_QTY,
          mer_mart.ABS_SYS_FCST_ERR_QTY,
          mer_mart.FCST_ERR_SLS_DLY_APP_FCST_QTY,
          mer_mart.FCST_ERR_SALES_DLY_APP_FCST,
          mer_mart.FCST_ERR_SLS_DLY_SYS_FCST_QTY,
          mer_mart.FCST_ERR_SALES_DLY_SYS_FCST,
          mer_mart.SALES_WK_APP_FCST_QTY_AV,
          mer_mart.SALES_WK_APP_FCST_QTY_FLT,
          mer_mart.SALES_WK_APP_FCST_QTY_FLT_AV
          )  ;
          
          g_recs_read := g_recs_read + SQL%ROWCOUNT;
          g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
  
  END IF;
  
    commit;
    
    l_text := 'MERGE ended ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
      
    l_text := 'RUNNING GATHER STATS ON RTL_LOC_ITEM_RDF_WKFCST_L1';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--        DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','RTL_LOC_ITEM_RDF_WKFCST_L1', DEGREE => 16);
-- PW Performance tuning
-- Only gather stats on new partitions; Assume FCST up to 52 weeks into the future, 
--    wh_prf_generic_stats ('DWH_PERFORMANCE','RTL_LOC_ITEM_RDF_WKFCST_L1', p_periods=>52, p_type=>'future');

-- below is the new code for stats collection in 12c - Ref: 23Jan2019
    DWH_FOUNDATION.GENERIC_GATHER_TABLE_STATS(l_procedure_name,'RTL_LOC_ITEM_RDF_WKFCST_L1');

    l_text := 'GATHER STATS COMPLETE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
   -- DISABLE TRACING
   -- execute immediate 'alter session set events ''10046 trace name context off'' ';
   -- l_text := 'TRACING TURNED OFF';
   -- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session disable parallel dml';
    l_text := 'PARALLEL DML DISABLED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
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

END WH_PRF_RDF_700U;
