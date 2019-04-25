--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_800U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_800U" (
        p_forall_limit IN INTEGER,
        p_success OUT BOOLEAN
   --     p_from_loc_no in integer,
   --     p_to_loc_no in integer
   )
    AS
      --**************************************************************************************************
      --  Date:        March 2008
      --  Author:      Alfonso Joshua
      --  Purpose:     Load weekly forecast LEVEL 2(DEPARTMENT LEVEL) table in performance layer
      --               with input ex temporary RDF forecast level 2 table from foundation layer.
      --  Tables:      Input  - RTL_LOC_ITEM_RDF_WKFCST_L2
      --               Output - RTL_LOC_ITEM_RDF_WKFCST_L2
      --  Packages:    constants, dwh_log, dwh_valid
----------------------------PREV VERSION------------------------------------------------------------------------
      --  Maintenance
      --  04 May 2009: TD-1143 - check for data duplication to prevent unique constraint as this program is insert only
      --  23 Aug 2011:QC4328 - add read to DIM_ITEM and DIM_LOCATION and hence extra fields
      --                        to temp_loc_item_dy_rdf_sysfcst
      --
--------------------------------NEW VERSION--------------------------------------------------------------------
      --  Maintenance:
      --  qc4340 - W LYTTLE     RDF Rollup of LEVEL 2(DEPARTMENT LEVEL) data
      --                        - This procedure was copied from WH_PRF_RDF_002A in PRD
      --                        - was from temp_loc_item_wk_rdf_sysfcstl1	  RTL_LOC_ITEM_RDF_WKFCST_L2
      --                          now from TEMP_LOC_ITM_WK_RDF_SYSFCST_L2   RTL_LOC_ITEM_RDF_WKFCST_L2
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
      g_rec_out RTL_LOC_ITEM_RDF_WKFCST_L2%rowtype;
      g_found     BOOLEAN;
      g_date      DATE := TRUNC(sysdate);
      g_yesterday DATE := TRUNC(sysdate) - 1;
      l_message sys_dwh_errlog.log_text%type;
      l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_RDF_800U';
      l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rdf;
      l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_pln_prf;
      l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_rdf;
      l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
      l_text sys_dwh_log.log_text%type ;
      l_description sys_dwh_log_summary.log_description%type   := '(NEW)LOAD RDF WEEKLY FCST LEVEL 2 FACTS EX TEMP TABLES';
      l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
      -- For output arrays into bulk load forall statements --
    type tbl_array_i
    IS
      TABLE OF RTL_LOC_ITEM_RDF_WKFCST_L2%rowtype INDEX BY binary_integer;
    type tbl_array_u
    IS
      TABLE OF RTL_LOC_ITEM_RDF_WKFCST_L2%rowtype INDEX BY binary_integer;
      a_tbl_insert tbl_array_i;
      a_tbl_update tbl_array_u;
      a_empty_set_i tbl_array_i;
      a_empty_set_u tbl_array_u;
      a_count   INTEGER := 0;
      a_count_i INTEGER := 0;
      a_count_u INTEGER := 0;
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
      
      l_text := 'LOAD RTL_LOC_ITEM_RDF_WKFCST_L2 EX FOUNDATION STARTED '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
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
         
      --**************************************************************************************************
      -- Bulk fetch loop controlling main program execution
      --**************************************************************************************************
      MERGE /*+ parallel (rdf_mart,4) */ INTO dwh_performance.RTL_LOC_ITEM_RDF_WKFCST_L2 rdf_mart USING (
      SELECT
        /*+ PARALLEL(TMP,4) FULL(TMP) FULL(di) FULL(dih) */
          DL.SK1_LOCATION_NO, 
          DI.SK1_ITEM_NO, 
          TMP.FIN_YEAR_NO,
          TMP.FIN_WEEK_NO, 
          DLH.SK2_LOCATION_NO, 
          DIH.SK2_ITEM_NO, 
          DC.FIN_WEEK_CODE, 
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
          NULL ABS_APP_FCST_ERR_QTY_LL, 
          NULL ABS_RLTV_APP_FCST_ERR_QTY_LL, 
          NULL ABS_SYS_FCST_ERR_QTY_LL, 
          NULL ABS_RLTV_SYS_FCST_ERR_QTY_LL, 
          NULL ABS_DLY_APP_FCST_ERR_QTY_PERI, 
          NULL ABS_DLY_SYS_FCST_ERR_QTY_PERI, 
          NULL ABS_APP_FCST_ERR_QTY,
          NULL ABS_SYS_FCST_ERR_QTY, 
          NULL FCST_ERR_SLS_DLY_APP_FCST_QTY, 
          NULL FCST_ERR_SALES_DLY_APP_FCST, 
          NULL FCST_ERR_SLS_DLY_SYS_FCST_QTY, 
          NULL FCST_ERR_SALES_DLY_SYS_FCST,
          NULL SALES_WK_APP_FCST_QTY_AV,
          NULL SALES_WK_APP_FCST_QTY_FLT,
          NULL SALES_WK_APP_FCST_QTY_FLT_AV
    FROM DWH_PERFORMANCE.TEMP_LOC_ITM_WK_RDF_SYSFCST_L2  TMP --TEMP_LOC_ITM_WK_RDF_SYSFCST_L2 TMP 
    JOIN dim_item di           ON TMP.item_no = di.item_no 
    JOIN dim_location dl       ON TMP.location_no = dl.location_no 
    JOIN dim_calendar dc       ON TMP.fin_year_no = dc.fin_year_no AND TMP.fin_week_no = dc.fin_week_no AND dc.fin_day_no = 4 
    JOIN dim_location_hist dlh ON TMP.location_no = dlh.location_no AND dc.calendar_date BETWEEN dlh.sk2_active_from_date AND dlh.sk2_active_to_date 
    JOIN dim_item_hist dih     ON TMP.item_no = dih.item_no AND dc.calendar_date BETWEEN dih.sk2_active_from_date AND dih.sk2_active_to_date 
    JOIN fnd_zone_item zi      on TMP.location_no = dl.location_no and dl.wh_fd_zone_group_no = zi.zone_group_no 
                                                                   and dl.wh_fd_zone_no    = zi.zone_no 
                                                                   and TMP.item_no     = zi.item_no
 -- where   TMP.location_no between p_from_loc_no and p_to_loc_no
  order by TMP.location_no, TMP.item_no, TMP.fin_year_no, TMP.fin_week_no
  
  ) mer_mart
  
ON (mer_mart.sk1_item_no        = rdf_mart.sk1_item_no
and mer_mart.sk1_location_no    = rdf_mart.sk1_location_no
and mer_mart.fin_year_no        = rdf_mart.fin_year_no
and mer_mart.fin_week_no        = rdf_mart.fin_week_no)
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


    commit;
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

END WH_PRF_RDF_800U;
