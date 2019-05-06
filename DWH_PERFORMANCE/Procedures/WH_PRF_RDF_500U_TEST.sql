--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_500U_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_500U_TEST" 
(p_forall_limit in integer,p_success out boolean--,p_from_loc_no in integer,p_to_loc_no in integer
) 
as
--**************************************************************************************************
--  Date:        October 2008
--  Author:      Alfonso Joshua
--  Purpose:     Load daily forecast table LEVEL 1(LOCATION LEVEL) in performance layer
--               with input ex RDF Sale table from foundation layer.
--  Tables:      Input  - TEMP_LOC_ITM_DY_RDF_SYSFCST_L1 and rtl_loc_item_dy_rdf_fcst_L1
--               Output - rtl_loc_item_dy_rdf_fcst_L1
--  Packages:    constants, dwh_log, dwh_valid
--
----------------------------PREV VERSION------------------------------------------------------------------------
--  Maintenance:
--  16 Feb 2009 - A. Joshua : TD-390  - Include ETL to fcst_err_sls_dly_app_fcst_qty,
--                                                    fcst_err_sales_dly_app_fcst,
--                                                    fcst_err_sls_dly_sys_fcst_qty,
--                                                    fcst_err_sales_dly_sys_fcst_qty
--  29 Apr 2009 - A. Joshua : TD-1490 - Remove lookup to table rtl_loc_item_dy_catalog
--                                    - The fcst_err* measures are now catered for in wh_prf_rdf_001c
--
--  30 june 2011 - W. Lyttle : TD-4328 - DATA FIX: De-vat Price values on RDF tables.
--                                       (see comments where code3 changed)
--  23 Aug 2011  - w.lyttle   :QC4328 - add read to DIM_ITEM and DIM_LOCATION and hence extra fields
--                                      to temp_loc_item_dy_rdf_sysfcst
--
--------------------------------NEW VERSION--------------------------------------------------------------------
--  Maintenance:
--  qc4340 - W LYTTLE: RDF Rollup of LEVEL 1(LOCATION LEVEL)
      --                        - This procedure was copied from WH_PRF_RDF_002U in PRD
      --                        - was from temp_loc_item_dy_rdf_sysfcstl1	  rtl_loc_item_dy_rdf_fcst_l1
      --                          now from TEMP_LOC_ITM_DY_RDF_SYSFCST_L1	  RTL_LOC_ITEM_DY_RDF_FCST_L1
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************


  g_forall_limit  integer := dwh_constants.vc_forall_limit;
  g_recs_read     integer := 0;
  g_recs_updated  integer := 0;
  g_recs_inserted integer := 0;
  g_recs_hospital integer := 0;
  g_recs_deleted  integer := 0;
  g_error_count   number  := 0;
  g_error_index   number  := 0;
  g_count         number  := 0;
  g_sub           integer := 0;
  g_part_name     varchar2(30);
  g_fin_month_code dim_calendar.fin_month_code%type;

  g_rec_out RTL_LOC_ITEM_WK_CATALOG%rowtype;
  g_found boolean;
  g_date date := trunc(sysdate);
  g_start_date date ;
  g_end_date date ;
  g_yesterday date := trunc(sysdate) - 1;
  g_fin_day_no dim_calendar.fin_day_no%type;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_RDF_500U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := '(NEW)LOAD RDF DAILY FCST LEVEL 1 FACTS EX TEMP TABLES';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  
  l_range_text   varchar2(200 byte);
  g_min_date dim_calendar.calendar_date%type;
  g_max_date dim_calendar.calendar_date%type;
  
  -- For Output Arrays Into Bulk Load Forall Statements --
  --**************************************************************************************************
  -- Main Process
  --**************************************************************************************************

begin
  if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
    g_forall_limit  := p_forall_limit;
  end if;
  dbms_output.put_line('BULK WRITE LIMIT '||p_forall_limit||' '||g_forall_limit);

  p_success := false;

  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text := 'Rollup of RTL_LOC_ITEM_WK_CATALOG ex day level started at '|| to_char(sysdate,('dd Mon Yyyy Hh24:Mi:Ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

  --**************************************************************************************************
  -- Look Up Batch Date From Dim_Control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  --g_date := '11/MAY/13';
  l_text := 'Batch date being processed is :- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  execute immediate 'alter session set workarea_size_policy=manual';
  execute immediate 'alter session set sort_area_size=100000000';
  execute immediate 'alter session enable parallel dml';
  
  --TRACING ADDED 17 MAY 2015 TO TRY ESTABLISH THE CAUSE OF OCCASIONAL SLOW-RUNNING
      --execute immediate 'alter session set events ''10046 trace name context forever, level 12'' ';
      --execute immediate 'alter session set events ''10046 trace name context off'' ';
      
      l_text := 'DISABLING PK CONSTRAINT - PK_P_RTL_LC_ITM_WK_FCST';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate 'alter table dwh_performance.RTL_LOC_ITEM_RDF_WKFCST_L1 disable constraint PK_P_RTL_LC_ITM_WK_FCST';
      l_text := 'PK CONSTRAINT DISABLED';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    -- DETERMINE IF THE YEARS TO BE PROCESSED ARE ACROSS A SINGLE YEAR OR SPLIT OVER 2 YEARS:
    --  IF SPLIT OVER 2 YEARS THEN GET MIN AND MAX WEEK FOR EACH YEAR IN THE TEMP TABLE
    --  IF ONLY 1 YEAR THEN SIMPLY GET MIN AND MAX FOR THAT YEAR IN THE TEMP TABLE
    --=======================================================================================
      select min(post_date), max(post_date) 
        into g_min_date, g_max_date
        from TEMP_LOC_ITM_DY_RDF_SYSFCST_L1; 
      
      l_text := 'min post_date : '|| g_min_date || ' max post_date  ' || g_max_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
     

  --**************************************************************************************************
  -- Insert Into RTL_LOC_ITEM_DY_RDF_FCST_L1
  --**************************************************************************************************
  g_recs_inserted := 0;
  
  MERGE /*+ parallel (rdf_mart,6) full(rdf_mart) */ into DWH_PERFORMANCE.RTL_LOC_ITEM_RDF_DYFCST_L1 rdf_mart USING      --rtl_loc_item_dy_rdf_fcst_L1
 ( 
  WITH SELRDF
                  AS(
                  SELECT /*+ PARALLEL(rtl,6) FULL(rtl) PARALLEL(sys_ilv,6) FULL(sys_ilv)) */
                    sys_ilv.location_no,
                    sys_ilv.item_no,
                    sys_ilv.post_date,
                    sys_ilv.sales_dly_sys_fcst_qty ,
                    sys_ilv.sales_dly_app_fcst_qty,
                    di.sk1_item_no,
                    dl.sk1_location_no,
                    di.vat_rate_perc,
                    zi.reg_rsp,
                    di.standard_uom_code,
                    di.random_mass_ind,
                    di.static_mass,
                    NVL(rtl.sales_dly_sys_fcst,0) sales_dly_sys_fcst,
                    NVL(rtl.sales_dly_app_fcst,0) sales_dly_app_fcst,
                    case when to_char(g_date,'DY') = 'SUN' then
                      NVL(sys_ilv.STATIC_WK_DLY_APP_FCST_QTY,0)
                    else
                      NVL(rtl.STATIC_WK_DLY_APP_FCST_QTY,0)
                    end STATIC_WK_DLY_APP_FCST_QTY
                  FROM dwh_performance.TEMP_LOC_ITM_DY_RDF_SYSFCST_L1  sys_ilv
                     JOIN DIM_ITEM DI ON di.item_no = sys_ilv.item_no
                     JOIN DIM_LOCATION DL on dl.location_no = sys_ilv.location_no
                     LEFT OUTER JOIN DWH_PERFORMANCE.RTL_LOC_ITEM_RDF_DYFCST_L1  rtl ON rtl.sk1_item_no     = di.sk1_item_no
                                                                                     AND rtl.sk1_location_no = dl.sk1_location_no
                                                                                     AND rtl.post_date       = sys_ilv.post_date
                     join fnd_zone_item zi on sys_ilv.location_no = dl.location_no 
                                          and dl.wh_fd_zone_group_no = zi.zone_group_no 
                                          and dl.wh_fd_zone_no    = zi.zone_no 
                                          and sys_ilv.item_no     = zi.item_no
          
          where sys_ilv.post_date between g_min_date and g_max_date                                
          --        WHERE sys_ilv.location_no BETWEEN P_FROM_LOC_NO AND P_TO_LOC_NO
          ----        order by sys_ilv.post_date
                  )
SELECT /*+ parallel (sys_ilv,2) parallel (rtl,2) */
SR.SK1_LOCATION_NO
,SR.SK1_ITEM_NO
,SR.POST_DATE
,DLH.SK2_LOCATION_NO
,DIH.SK2_ITEM_NO
,SR.SALES_DLY_SYS_FCST_QTY
,CASE WHEN SR.standard_uom_code  = 'EA' and
    SR.random_mass_ind  = 1 then
    SR.sales_dly_sys_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc))* SR.static_mass + 0.005
else
    SR.sales_dly_sys_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc)) +0.005
end sales_dly_sys_fcst
,SR.SALES_DLY_APP_FCST_QTY
,CASE WHEN SR.standard_uom_code  = 'EA' and
    SR.random_mass_ind  = 1 then
    SR.sales_dly_app_fcst_qty * (SR.reg_rsp * 100 / (100 +  SR.vat_rate_perc))* SR.static_mass + 0.005
else
    SR.sales_dly_app_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc)) + 0.005
end  sales_dly_app_fcst
,G_DATE LAST_UPDATED_DATE
,NULL FCST_ERR_SLS_DLY_APP_FCST_QTY
,NULL FCST_ERR_SLS_DLY_SYS_FCST_QTY
,NULL FCST_ERR_SALES_DLY_APP_FCST
,NULL FCST_ERR_SALES_DLY_SYS_FCST
,NULL ABS_DLY_APP_FCST_ERR_QTY_PERI
,NULL ABS_DLY_SYS_FCST_ERR_QTY_PERI
,NULL ABS_APP_FCST_ERR_QTY_LL
,NULL ABS_RLTV_APP_FCST_ERR_QTY_LL
,NULL ABS_SYS_FCST_ERR_QTY_LL
,NULL ABS_RLTV_SYS_FCST_ERR_QTY_LL
,NULL ABS_APP_FCST_ERR_QTY
,NULL ABS_SYS_FCST_ERR_QTY,
NULL SALES_DLY_APP_FCST_QTY_AV,
NULL SALES_DLY_APP_FCST_QTY_FLT,
NULL SALES_DLY_APP_FCST_QTY_FLT_AV,
SR.STATIC_WK_DLY_APP_FCST_QTY
 FROM SELRDF SR
JOIN dim_item_hist dih
ON SR.item_no = dih.item_no
AND SR.post_date BETWEEN dih.sk2_active_from_date AND dih.sk2_active_to_date
JOIN dim_location_hist dlh
ON SR.location_no = dlh.location_no
AND SR.post_date BETWEEN dlh.sk2_active_from_date AND dlh.sk2_active_to_date
WHERE (
  CAST((CASE
    WHEN SR.standard_uom_code = 'EA'
    AND SR.random_mass_ind    = 1
    THEN SR.sales_dly_sys_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc))* SR.static_mass + 0.005
    ELSE SR.sales_dly_sys_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc)) +0.005
  END) AS NUMBER(14,2)) <> CAST((NVL(SR.sales_dly_sys_fcst,0)) AS NUMBER(14,2))
OR
  CAST((CASE
    WHEN SR.standard_uom_code = 'EA'
    AND SR.random_mass_ind    = 1
    THEN SR.sales_dly_app_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc))* SR.static_mass + 0.005
    ELSE SR.sales_dly_app_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc)) +0.005
  END) AS NUMBER(14,2)) <> CAST((NVL(SR.sales_dly_app_fcst,0)) AS NUMBER(14,2)))
  
) mer_mart

ON (mer_mart.sk1_item_no        = rdf_mart.sk1_item_no
and mer_mart.sk1_location_no    = rdf_mart.sk1_location_no
and mer_mart.post_date          = rdf_mart.post_date)
WHEN MATCHED THEN
UPDATE
SET       SK2_LOCATION_NO               = mer_mart.SK2_LOCATION_NO,
          SK2_ITEM_NO                   = mer_mart.SK2_ITEM_NO,
          SALES_DLY_SYS_FCST_QTY        = mer_mart.SALES_DLY_SYS_FCST_QTY,
          SALES_DLY_SYS_FCST            = mer_mart.SALES_DLY_SYS_FCST,
          SALES_DLY_APP_FCST_QTY        = mer_mart.SALES_DLY_APP_FCST_QTY,
          SALES_DLY_APP_FCST            = mer_mart.SALES_DLY_APP_FCST,
          FCST_ERR_SLS_DLY_APP_FCST_QTY = mer_mart.FCST_ERR_SLS_DLY_APP_FCST_QTY,
          FCST_ERR_SLS_DLY_SYS_FCST_QTY = mer_mart.FCST_ERR_SLS_DLY_SYS_FCST_QTY,
          FCST_ERR_SALES_DLY_APP_FCST   = mer_mart.FCST_ERR_SALES_DLY_APP_FCST,
          FCST_ERR_SALES_DLY_SYS_FCST   = mer_mart.FCST_ERR_SALES_DLY_SYS_FCST,
          ABS_DLY_APP_FCST_ERR_QTY_PERI = mer_mart.ABS_DLY_APP_FCST_ERR_QTY_PERI,
          ABS_DLY_SYS_FCST_ERR_QTY_PERI = mer_mart.ABS_DLY_SYS_FCST_ERR_QTY_PERI,
          ABS_APP_FCST_ERR_QTY_LL       = mer_mart.ABS_APP_FCST_ERR_QTY_LL,
          ABS_RLTV_APP_FCST_ERR_QTY_LL  = mer_mart.ABS_RLTV_APP_FCST_ERR_QTY_LL,
          ABS_SYS_FCST_ERR_QTY_LL       = mer_mart.ABS_SYS_FCST_ERR_QTY_LL,
          ABS_RLTV_SYS_FCST_ERR_QTY_LL  = mer_mart.ABS_RLTV_SYS_FCST_ERR_QTY_LL,
          ABS_APP_FCST_ERR_QTY          = mer_mart.ABS_APP_FCST_ERR_QTY,
          ABS_SYS_FCST_ERR_QTY          = mer_mart.ABS_SYS_FCST_ERR_QTY,
          SALES_DLY_APP_FCST_QTY_AV     = mer_mart.SALES_DLY_APP_FCST_QTY_AV,
          SALES_DLY_APP_FCST_QTY_FLT    = mer_mart.SALES_DLY_APP_FCST_QTY_FLT,
          SALES_DLY_APP_FCST_QTY_FLT_AV = mer_mart.SALES_DLY_APP_FCST_QTY_FLT_AV,
          STATIC_WK_DLY_APP_FCST_QTY    = mer_mart.STATIC_WK_DLY_APP_FCST_QTY,
          last_updated_date             = g_date
WHEN NOT MATCHED THEN
INSERT
(         SK1_LOCATION_NO,
          SK1_ITEM_NO,
          POST_DATE,
          SK2_LOCATION_NO,
          SK2_ITEM_NO,
          SALES_DLY_SYS_FCST_QTY,
          SALES_DLY_SYS_FCST,
          SALES_DLY_APP_FCST_QTY,
          SALES_DLY_APP_FCST,
          last_updated_date,
          FCST_ERR_SLS_DLY_APP_FCST_QTY,
          FCST_ERR_SLS_DLY_SYS_FCST_QTY,
          FCST_ERR_SALES_DLY_APP_FCST,
          FCST_ERR_SALES_DLY_SYS_FCST,
          ABS_DLY_APP_FCST_ERR_QTY_PERI,
          ABS_DLY_SYS_FCST_ERR_QTY_PERI,
          ABS_APP_FCST_ERR_QTY_LL,
          ABS_RLTV_APP_FCST_ERR_QTY_LL,
          ABS_SYS_FCST_ERR_QTY_LL,
          ABS_RLTV_SYS_FCST_ERR_QTY_LL,
          ABS_APP_FCST_ERR_QTY,
          ABS_SYS_FCST_ERR_QTY,
          SALES_DLY_APP_FCST_QTY_AV,
          SALES_DLY_APP_FCST_QTY_FLT,
          SALES_DLY_APP_FCST_QTY_FLT_AV,
          STATIC_WK_DLY_APP_FCST_QTY
          
          )
  values
(         mer_mart.SK1_LOCATION_NO,
          mer_mart.SK1_ITEM_NO,
          mer_mart.POST_DATE,
          mer_mart.SK2_LOCATION_NO,
          mer_mart.SK2_ITEM_NO,
          mer_mart.SALES_DLY_SYS_FCST_QTY,
          mer_mart.SALES_DLY_SYS_FCST,
          mer_mart.SALES_DLY_APP_FCST_QTY,
          mer_mart.SALES_DLY_APP_FCST,
          g_date,
          mer_mart.FCST_ERR_SLS_DLY_APP_FCST_QTY,
          mer_mart.FCST_ERR_SLS_DLY_SYS_FCST_QTY,
          mer_mart.FCST_ERR_SALES_DLY_APP_FCST,
          mer_mart.FCST_ERR_SALES_DLY_SYS_FCST,
          mer_mart.ABS_DLY_APP_FCST_ERR_QTY_PERI,
          mer_mart.ABS_DLY_SYS_FCST_ERR_QTY_PERI,
          mer_mart.ABS_APP_FCST_ERR_QTY_LL,
          mer_mart.ABS_RLTV_APP_FCST_ERR_QTY_LL,
          mer_mart.ABS_SYS_FCST_ERR_QTY_LL,
          mer_mart.ABS_RLTV_SYS_FCST_ERR_QTY_LL,
          mer_mart.ABS_APP_FCST_ERR_QTY,
          mer_mart.ABS_SYS_FCST_ERR_QTY,
          mer_mart.SALES_DLY_APP_FCST_QTY_AV,
          mer_mart.SALES_DLY_APP_FCST_QTY_FLT,
          mer_mart.SALES_DLY_APP_FCST_QTY_FLT_AV,
          mer_mart.STATIC_WK_DLY_APP_FCST_QTY
          )

  ;

  g_recs_read := g_recs_read + sql%rowcount;
  g_recs_inserted := g_recs_inserted + sql%rowcount;

  l_text := 'Insert completed NEW:- RECS =  '||g_recs_inserted||' '||g_start_date||'  To '||g_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       commit;


  --**************************************************************************************************
  -- Write Final Log Data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd Mon Yyyy Hh24:Mi:Ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_deleted||g_recs_deleted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  commit;

  p_success := true;

exception
when dwh_errors.e_insert_error then
  l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;
when others then
  l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;


END WH_PRF_RDF_500U_TEST;
