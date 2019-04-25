--------------------------------------------------------
--  DDL for Procedure WH_PRF_WBL_TEST_EXAMPLE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_WBL_TEST_EXAMPLE" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
  ---
  --- RUN USING DWH_WH_PRF_ALASTAIR
  ---
AS
  g_sql VARCHAR2(8000);
  g_start DATE   := '01 Jul 2009';
  g_end DATE     := '30 Sep 2009';
  g_count               NUMBER := 0;
  g_recs_inserted       NUMBER := 0;
  g_cnt                 NUMBER;
  Gp_table_name         VARCHAR2(31) := 'stg_rms_rtl_allocation';
  Gp_log_script_name    VARCHAR2(31) :='';
  Gp_log_procedure_name VARCHAR2(31);
  Gp_description        VARCHAR2(31) := 'stg_rms_rtl_allocation';
  g_stmt                VARCHAR2(1500);
  g_table_name          VARCHAR2(31) := 'STG_RMS_RTL_ALLOCATION';
  --g_arc_table_name    varchar2(31);
  --g_hsp_table_name    varchar2(31);
  g_cpy_table_name VARCHAR2(31) := 'STG_RMS_RTL_ALLOCATION_CPY';
  g_index_name     VARCHAR2(31) := 'BS_RMS_RTL_ALLOCATION';
  g_cpy_index_name VARCHAR2(31) := 'BS_RMS_RTL_ALLOCATION_CPY';
  g_pk_name        VARCHAR2(31) := 'PK_S_STG_RMS_RTL_ALLCATN';
  g_cpy_pk_name    VARCHAR2(31) := 'PK_S_STG_RMS_RTL_ALLCATN_CPY';
  g_pk_stmt        VARCHAR2(1500);
  g_tablespace     VARCHAR2(31) := 'STG_STAGING';
  G_LAST_ANALYZED_DATE  date  := sysdate;
  G_start_DATE_time  date  := sysdate;
  G_date  date  := sysdate;
  g_xpart_name     VARCHAR2(32);
  g_wkpart_name     VARCHAR2(32);
  g_part_name     VARCHAR2(32);
  g_subpart_name     VARCHAR2(32);
   g_fin_year_no       NUMBER := 0; 
   g_fin_week_no       NUMBER := 0; 
   g_sub       NUMBER := 0; 
   g_sub1       NUMBER := 0; 
g_start_week                integer       :=  0;
g_start_year                integer       :=  0;
g_this_week_start_date      date          := trunc(sysdate);
g_this_week_end_date        date          := trunc(sysdate);
g_fin_week_code             varchar2(7);


    
  g_deal           NUMBER(14);
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_WBL_TEST';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'TEST';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  --
  --**************************************************************************************************
BEGIN
 --    p_success := false;

  --   l_text              := 'Insert-RTL_PROM_LOC_ITEM_DY_313801 to RTL_PROM_LOC_ITEM_DY started' ;
  --  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --  EXECUTE IMMEDIATE('ALTER TABLE DWH_PERFORMANCE.Temp_prod_status_1_code_4368 add area_no NUMBER(6)');
  -- commit;
  --   EXECUTE IMMEDIATE('truncate TABLE DWH_PERFORMANCE.Temp_prod_status_1_code_4368');
  -- commit;
  --   EXECUTE IMMEDIATE('ALTER TABLE DWH_PERFORMANCE.Temp_prod_status_1_code_4368 ADD CONSTRAINT PK_P_TMP_prd_sts_1_cde_4368
  --  PRIMARY KEY (FIN_YEAR_NO,FIN_WEEK_NO,SK1_ITEM_NO,PRODUCT_STATUS_1_CODE, area_no)
  --  USING INDEX TABLESPACE PRF_MASTER  ENABLE');
  --  EXECUTE IMMEDIATE('truncate TABLE dwh_performance.TEMP_RTL_AREA_ITEM_WK_CLG_4368');
  -- commit;
  --  EXECUTE IMMEDIATE('ALTER TABLE dwh_performance.TEMP_RTL_AREA_ITEM_WK_CLG_4368 drop CONSTRAINT PK_P_TMP_RTL_AR_ITM_WK_CTLG');
  -- commit;
  --   EXECUTE IMMEDIATE('ALTER TABLE dwh_performance.RTL_PROM_LOC_ITEM_DY_313801 ADD CONSTRAINT PK_P_prm_lc_itm_dy_313801
  --  PRIMARY KEY (POST_DATE,SK1_LOCATION_NO,SK1_ITEM_NO,SK1_PROM_NO,srce)
  --  USING INDEX TABLESPACE PRF_MASTER  ENABLE');
  --/*
  --L_Text := 'ALTER TABLE DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DY_FX ADD CONSTRAINT PK_N_RTL_PO_SPCHN_LC_ITM_DY_FX';
  --    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --    EXECUTE IMMEDIATE('alter TABLE dwh_performance.temp_auditor_ext_4727 modify Shrinkage_perc number(20,10)');
  --commit;
  --   L_Text := 'GATHER STATS on dwh_performance.temp_rtl_prom_loc_item_dy';
  --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --   Dbms_Stats.Gather_Table_Stats ('DWH_PERFORMANCE',
  --                                  'TEMP_RTL_PROM_LOC_ITEM_DY', Degree => 8);
  --   Commit;
  --   l_text := 'GATHER STATS  - Completed';
  --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --        g_sql := 'ALTER TABLE DWH_PERFORMANCE.RTL_LOC_ITEM_DY_AST_CATLG TRUNCATE SUBPARTITION RTL_LIDAC_060313'; 
  --   L_TEXT := 'ALTER TABLE DWH_PERFORMANCE.RTL_LOC_ITEM_DY_AST_CATLG TRUNCATE SUBPARTITION RTL_LIDAC_060313';
  --   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
  --   EXECUTE IMMEDIATE(G_SQL);
  --   COMMIT;
  --             G_SQL := 'ALTER TABLE DWH_PERFORMANCE.RTL_LOC_ITEM_DY_AST_CATLG TRUNCATE SUBPARTITION RTL_LIDAC_050313'; 
  --   L_TEXT := 'ALTER TABLE DWH_PERFORMANCE.RTL_LOC_ITEM_DY_AST_CATLG TRUNCATE SUBPARTITION RTL_LIDAC_050313';
  --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --   EXECUTE IMMEDIATE(G_SQL);
  --   commit;
 --    G_SQL := 'ALTER TABLE DWH_PERFORMANCE.RTL_LOC_ITEM_DY_AST_CATLG TRUNCATE SUBPARTITION FND_ALIDC_270113'; 
  --   L_TEXT := 'ALTER TABLE DWH_PERFORMANCE.RTL_LOC_ITEM_DY_AST_CATLG TRUNCATE SUBPARTITION FND_ALIDC_270113';
  --   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
  --   EXECUTE IMMEDIATE(G_SQL);
  --   commit;
  
--     l_text := 'GATHER STATS on DWH_PERFORMANCE.RTL_LOC_ITEM_DY_AST_CATLG';
--     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--     Dbms_Stats.Gather_Table_Stats ('DWH_PERFORMANCE',
--                                    'RTL_LOC_ITEM_DY_AST_CATLG', degree => 8);
--     commit;
  --   l_text := 'GATHER STATS  - Completed';
  --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  ---    l_text := 'GATHER STATS on dwh_PERFORMANCE.rtl_loc_item_dy_rdf_fcst';
  ---    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  ---    DBMS_STATS.gather_table_stats ('DWH_FOUNDATION',
  ---                                   'RTL_LOC_ITEM_DY_RDF_FCST', ' ', 'SUBPARTITION', DEGREE => 8);
  --    commit;
  --    l_text := 'GATHER STATS  - Completed';
  --    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --*/
 -- l_text := 'ALTER TABLE DWH_PERFORMANCE.temp_rtl_prom_loc_item_dy ADD CONSTRAINT PK_N_RTL_SPCHN_LC_ITM_DY';
  --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--  EXECUTE IMMEDIATE('truncate table dwh_performance.rtl_loc_item_dy_AST_catlg ');
 -- COMMIT;

--  l_text := 'CREATE INDEX DWH_PERFORMANCE.I12_N_RTL_SPCHN_LC_ITM_DY ON DWH_PERFORMANCE.RTL_SUPCHAIN_LOC_ITEM_DY  ' ;
--  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--  EXECUTE IMMEDIATE('create table stg_excel_ed_benefit_cntr tablespace STG_STAGING as select * from stg_excel_ed_benefit_cntr_cpy where 1=2 ');
--  COMMIT;
--  l_text := 'create table stg_excel_ed_benefit_cntr tablespace STG_STAGING as select * from stg_excel_ed_benefit_cntr_cpy where 1=2' ;
--  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--  EXECUTE IMMEDIATE('CREATE INDEX DWH_PERFORMANCE.I1_N_RTL_SPCHN_LC_ITM_DY ON dwh_performance.temp_prom_loc_item_dy  
--(    TRAN_DATE,    SK1_LOCATION_NO,    SK1_ITEM_NO,    SK1_SUPPLY_CHAIN_NO  )  COMPUTE STATISTICS  TABLESPACE PRF_MASTER');
--  COMMIT;
--  l_text := 'grant all on DWH_HR_PERFORMANCE.DIM_HR_BEE_ED_BENEFICIARY_HIST to public';
--  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--  EXECUTE IMMEDIATE('grant all on DWH_HR_PERFORMANCE.DIM_HR_BEE_ED_BENEFICIARY_HIST to public');
--  COMMIT;
--insert   /*+ APPEND */ INTO dwh_performance.wh_prf_corp_spacerace_4737
--WITH Seldns AS
--(SELECT
--/*+ parallel (a,4) */
--a.fin_year_no,a.this_week_start_date,Dl.Location_no,Di.Item_no,a.sales,
--a.Sales_qty,a.sales_cost ,a.sales_margin
--FROM Rtl_loc_item_wk_rms_dense a,Dim_location dl,DIM_ITEM_TEST Di
--WHERE ( (A.Fin_Year_No = 2012 AND a.fin_week_no BETWEEN 46 AND 52)
--or (A.Fin_Year_No = 2013 AND a.fin_week_no BETWEEN 1 AND 10))
--AND a.Sk1_location_no = Dl.Sk1_location_no AND a.Sk1_item_no = Di.Sk1_item_no
--AND Di.Business_Unit_No = 50 AND A.Sales_Qty IS NOT NULL
--and dl.chain_no = 10
---and a.sk1_location_no in (435,647,9382)
--),
--Selsprs AS
--(SELECT
--/*+ parallel (a,4) */
--a.fin_year_no,a.this_week_start_date,Dl.Location_no,Di.Item_no,a.waste_selling,
--a.waste_qty,a.waste_cost
--FROM Rtl_loc_item_wk_rms_sparse a,Dim_location dl,DIM_ITEM_TEST Di
--WHERE ( (A.Fin_Year_No = 2012 AND a.fin_week_no BETWEEN 46 AND 52)
--or (A.Fin_Year_No = 2013 AND a.fin_week_no BETWEEN 1 AND 10))
--AND a.Sk1_location_no = Dl.Sk1_location_no AND a.Sk1_item_no = Di.Sk1_item_no
--AND Di.Business_Unit_No = 50 AND A.Waste_Qty IS NOT NULL
--and dl.chain_no = 10
---and a.sk1_location_no in (435,647,9382)
--)
--SELECT NVL(sd.Fin_Year_No,ss.Fin_Year_No) Fin_Year_No,
--NVL(sd.this_week_start_date,ss.this_week_start_date) this_week_start_date ,
--NVL(sd.Location_No,ss.Location_No) Location_No ,
--NVL(sd.item_no,ss.item_no) item_no,
--sales,Sales_qty,Sales_Cost ,sales_margin ,waste_selling,Waste_Qty,waste_cost
--FROM Seldns Sd  FULL OUTER JOIN Selsprs Ss
--On Ss.Fin_Year_No = Sd.Fin_Year_No
--and Ss.this_week_start_date = Sd.this_week_start_date
--AND Ss.Location_No = Sd.Location_No
--AND ss.item_no = sd.item_no;-
--
--  COMMIT;
--  --    l_text              := 'Insert-RTL_PROM_LOC_ITEM_DY_313801 to RTL_PROM_LOC_ITEM_DY = '||g_recs_inserted  ;
  --    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  
-- execute immediate(' create table   dwh_performance.rtl_item_trading_max
-- as 
--SELECT
--    /*+ parallel(DNS,4) parallel(ritx,4)*/
--    DNS.sk1_item_no,
--    MAX(dns.this_week_start_date) max_this_week_start_date
--  FROM rtl_loc_item_wk_rms_dense DNS,
--   dwh_performance.rtl_item_trading ritx
--  WHERE   dns.sk1_item_no = ritx.sk1_item_no
--  AND DNS.sk1_item_no in (18796972,
--17872533, 18828139)
--and DNS.sales != 0
--  AND DNS.sales   IS NOT NULL
--  GROUP BY DNS.sk1_item_no');
--  commit;

 

/*G_SQL := 'CREATE INDEX DWH_FOUNDATION.FND_ITEM_test_INDEX1 ON DWH_FOUNDATION.FND_ITEM_test (LAST_UPDATED_DATE ASC) 
LOGGING 
TABLESPACE FND_MASTER ';
L_TEXT := 'CREATE INDEX DWH_FOUNDATION.FND_ITEM_test_INDEX1 ON DWH_FOUNDATION.FND_ITEM_test (LAST_UPDATED_DATE ASC) 
LOGGING 
TABLESPACE FND_MASTER';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
EXECUTE IMMEDIATE(G_SQL);
commit;

G_SQL := 'CREATE INDEX DWH_FOUNDATION.FND_ITEM_test_INDEX2 ON DWH_FOUNDATION.FND_ITEM_test (STYLE_COLOUR_NO ASC)  
LOGGING 
TABLESPACE FND_MASTER ';
L_TEXT := 'CREATE INDEX DWH_FOUNDATION.FND_ITEM_test_INDEX2 ON DWH_FOUNDATION.FND_ITEM_test (STYLE_COLOUR_NO ASC)  
LOGGING 
TABLESPACE FND_MASTER';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
execute immediate(g_sql);
commit;


G_SQL := 'CREATE INDEX DWH_FOUNDATION.FND_ITEM_test_INDEX4 ON DWH_FOUNDATION.FND_ITEM_test (SUBCLASS_NO ASC) 
LOGGING 
TABLESPACE FND_MASTER ';
L_TEXT := 'CREATE INDEX DWH_FOUNDATION.FND_ITEM_test_INDEX4 ON DWH_FOUNDATION.FND_ITEM_test (SUBCLASS_NO ASC) 
LOGGING 
TABLESPACE FND_MASTER';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
EXECUTE IMMEDIATE(G_SQL);
commit;


G_SQL := 'CREATE INDEX DWH_FOUNDATION.FND_ITEM_test_INDEX3 ON DWH_FOUNDATION.FND_ITEM_test (STYLE_NO ASC) 
LOGGING 
TABLESPACE FND_MASTER ';
L_TEXT := 'CREATE INDEX DWH_FOUNDATION.FND_ITEM_test_INDEX3 ON DWH_FOUNDATION.FND_ITEM_test (STYLE_NO ASC) 
LOGGING 
TABLESPACE FND_MASTER';
DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
EXECUTE IMMEDIATE(G_SQL);
commit;
*/


--g_sql := '
--CREATE INDEX DWH_PERFORMANCE.I35_P_DM_ITM_TST ON DWH_PERFORMANCE.DIM_ITEM_TEST (NEXT_COST_PRICE_EFFECTIVE_DATE ASC) 
--LOGGING 
--TABLESPACE PRF_MASTER 
--';
--L_TEXT := G_SQL;
--DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
--EXECUTE IMMEDIATE(G_SQL);
--commit;


 --   if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
 --      g_forall_limit := p_forall_limit;
 --   end if;
 --   dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);


  EXECUTE IMMEDIATE('
  CREATE TABLE DWH_PERFORMANCE.TEMP_RTL_AST_SC_DATES 
   (	SK1_STYLE_COLOUR_NO NUMBER(9,0) NOT NULL ENABLE, 
	CONTINUITY_IND NUMBER(1,0) NOT NULL ENABLE, 
	SEASON_FIRST_TRADE_DATE DATE NOT NULL ENABLE, 
	CONT_PREV_WEEK_YEAR_NO NUMBER(4,0), 
	CONT_PREV_WEEK_WEEK_NO NUMBER(2,0), 
	CONT_PREV_WEEK_DATE DATE NOT NULL ENABLE, 
	CONT_START_WEEK_DATE DATE, 
	CONT_END_WEEK_DATE DATE, 
	FASH_START_WEEK_DATE DATE, 
	FASH_END_WEEK_DATE DATE, 
	DERIVE_START_DATE DATE, 
	DERIVE_END_DATE DATE
   ) 
  TABLESPACE PRF_MASTER');
 


  --GRANT SELECT ON DWH_PERFORMANCE.TEMP_RTL_AST_SC_DATES TO READ_ONLY;
 


COMMIT;
  
  p_success := true;
    exception
--
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);

       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);

       rollback;
       p_success := false;
       raise;

END WH_PRF_WBL_TEST_EXAMPLE;
