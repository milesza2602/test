--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_280U_WL_PART2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_280U_WL_PART2" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
AS
  --**************************************************************************************************
-- test version for prd
  --**************************************************************************************************
--  --  Date:        October 2013
  --  Author:      W Lyttle
  --  Purpose:     Setup all items with no promotions during the last 12 weeks according to certain rules
  --               in preparation for loading Foods Sales data.
  --               The rules are :
  --                 Rule 1 - include records where on promotion for the first 6 consecutive weeks
  --                          but set average = zero
  --                 Rule 2 - include records where not on promotion for the first 6 consecutive weeks
  --                 Rule 3 - include records where not on promotion for the  6 weeks
  --                             EG. on promotion for weeks=1,2 of first 6 weeks and week=7, roll forward until have 6 weeks of no promotion
  --                                     ie. EXCLUDE weeks 1,2, 7 and INCLUDE weeks 3,4,5,6,8,9
  --
  --
  --
  --
  --  Tables:      Input  - DIM_PROM(dp),
  --                        DIM_PROM_ITEM_ALL(dpia),
  --                        DIM_LOCATION(dl),
  --                        FND_PROM_LOCATION(fpl) ,
  --                        DIM_ITEM (di),
  --                        DIM_ITEM_UDA(diu),
  --                        FND_UDA_VALUE(fuv),
  --                        FND_ITEM_UDA(fia),
  --                        DIM_CALENDAR_WL(dc)
  --               Output - rtl_no_promo_MART_FD_LOC_ITEM_WK_6WKAVG
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
  g_forall_limit    INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read       INTEGER := 0;
  g_recs_inserted   INTEGER := 0;
  g_chain_corporate INTEGER := 0;
  g_chain_franchise INTEGER := 0;
  g_fin_week_no DIM_CALENDAR_WL.fin_week_no%type;
  g_fin_year_no DIM_CALENDAR_WL.fin_year_no%type;
  g_ly_fin_week_no DIM_CALENDAR_WL.fin_week_no%type;
  g_ly_fin_year_no DIM_CALENDAR_WL.fin_year_no%type;
  g_lcw_fin_week_no DIM_CALENDAR_WL.fin_week_no%type;
  g_lcw_fin_year_no DIM_CALENDAR_WL.fin_year_no%type;
  g_date          DATE;
  g_start_date    DATE;
  g_end_date      DATE;
  g_ly_start_date DATE;
  g_ly_end_date   DATE;
  G_START_12WK    DATE;
  G_END_12WK      DATE;
  g_public_hol    NUMBER;
  g_sub           NUMBER;
  g_recs           NUMBER;
  g_seq number;
  g_item_no number;
  g_DEPARTMENT_no number;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_280U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_roll;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_roll;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%TYPE ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD foods 6wk records';
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
      l_text := 'LOAD OF TEMP_MART_FD_LOC_ITEM_WK_6WKAVG STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
      dwh_lookup.dim_control(g_date);

 ----- START -- this is for testing purposes
  --    G_DATE := '29 october 2013;
--
 --     update dim_control
  --    set today_date = g_date,
 --     last_wk_start_date = '21 OCTOBER 2013',
 --     last_wk_end_date = '27 OCTOBER 2013';


  --    g_department_no := 19;--	ORGANIC DAIRY
                            --81	LOW ALC BEER
 ----- END -- this is for testing purposes

      l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     EXECUTE immediate('alter session enable parallel dml');


  COMMIT;
  --**************************************************************************************************
  -- Derive dates taking public-holidays into account
  --**************************************************************************************************
      SELECT dc.last_wk_start_date-77,        dc.last_wk_end_date,              MAX(RSA_PUBLIC_HOLIDAY_IND)
                INTO G_START_12WK,        G_END_12WK,          g_public_hol
      FROM DIM_CALENDAR_WL dmc, dim_control dc
            WHERE dmc.CALENDAR_DATE = dc.last_wk_end_date
      GROUP BY dc.last_wk_start_date-77,        dc.last_wk_end_date;

      l_text := 'PERIOD='||G_START_12WK||' - '||G_END_12WK;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      IF g_public_hol IS NOT NULL THEN
            l_text        := 'Public Holiday - excluding this week ';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      END IF;

 
      --**************************************************************************************************
  -- Drop indexes if exist (ie. excluding PK)
  --**************************************************************************************************
     EXECUTE immediate('truncate table dwh_performance.temp_mart_6wkavg_PROM_VALUES');
     l_text := 'truncate table dwh_performance.temp_mart_6wkavg_PROM_VALUES';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TEMP_MART_6WKAVG_PROM_VALUES', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS on dwh_performance.temp_mart_6wkavg_PROM_VALUES';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
 
    BEGIN
        select 1
        into g_seq
        from dba_indexes
        where OWNER = 'DWH_PERFORMANCE'
        AND index_name = 'I10_P_TMP_MRT_6WKAVG_PROM_VLS';
              execute immediate('DROP index dwh_performance.I10_P_TMP_MRT_6WKAVG_PROM_VLS');
    exception
         when no_data_found then
          commit;
    END;

    BEGIN
        select 1
        into g_seq
        from dba_indexes
        where OWNER = 'DWH_PERFORMANCE'
        AND index_name = 'I20_P_TMP_MRT_6WKAVG_PROM_VLS';
              execute immediate('DROP index dwh_performance.I20_P_TMP_MRT_6WKAVG_PROM_VLS');
    exception
         when no_data_found then
          commit;
    END;
    
        BEGIN
        select 1
        into g_seq
        from dba_indexes
        where OWNER = 'DWH_PERFORMANCE'
        AND index_name = 'I30_P_TMP_MRT_6WKAVG_PROM_VLS';
              execute immediate('DROP index dwh_performance.I30_P_TMP_MRT_6WKAVG_PROM_VLS');
    exception
         when no_data_found then
          commit;
    END;
    
        BEGIN
        select 1
        into g_seq
        from dba_indexes
        where OWNER = 'DWH_PERFORMANCE'
        AND index_name = 'I40_P_TMP_MRT_6WKAVG_PROM_VLS';
              execute immediate('DROP index dwh_performance.I40_P_TMP_MRT_6WKAVG_PROM_VLS');
    exception
         when no_data_found then
          commit;
    END;

  
  --**************************************************************************************************
  -- Do insert into dwh_performance.temp_mart_6wkavg_PROM_VALUES
  --**************************************************************************************************
    G_RECS := 0;

      INSERT /*+ APPEND  */ INTO dwh_performance.temp_mart_6wkavg_PROM_VALUES aps

   with 
        seldns as (             select /*+ parallel(sc,2) parallel(sd,2)   */
                       SC.fin_year_no                         fin_year_no
                      ,SC.fin_week_no                       fin_week_no
                      ,SC.SK1_ITEM_NO                          SK1_ITEM_NO
                      ,SC.SK1_LOCATION_NO                   SK1_LOCATION_NO
                      ,SC.LOCATION_NO                          LOCATION_NO
                      ,SC.ITEM_NO                                 ITEM_NO
                      ,SC.THIS_WEEK_START_DATE         THIS_WEEK_START_DATE
                      ,SC.THIS_WEEK_END_DATE            THIS_WEEK_END_DATE
                      ,SC.sk1_prom_no                          sk1_prom_no
                      ,SC.WEEK_COUNT                            WEEK_COUNT
                      ,SUM(NVL(SALES_QTY,0))                                                    SALES_QTY 
                      ,SUM(NVL(SALES,0))                                                         SALES
                      ,SUM(NVL(SALES_MARGIN,0))                                                  SALES_MARGIN 
                from  dwh_performance.temp_mart_6wkavg_PROM_DATES SC
                join dwh_performance.RTL_LOC_ITEM_WK_RMS_DENSE sd
                  on sD.fin_year_no = sC.fin_year_no
                  and sD.fin_week_no = sC.fin_week_no
                  and sD.sk1_item_no = sC.sk1_item_no
                  and sD.sk1_location_no = sC.sk1_location_no
               where sd.this_week_start_date between  G_START_12WK and G_END_12WK
                  GROUP BY                        SC.fin_year_no                  
                      ,SC.fin_week_no                       
                      ,SC.SK1_ITEM_NO                          
                      ,SC.SK1_LOCATION_NO                   
                      ,SC.ITEM_NO                                 
                      ,SC.LOCATION_NO                         
                      ,SC.THIS_WEEK_START_DATE         
                      ,SC.THIS_WEEK_END_DATE           
                      ,SC.sk1_prom_no                        
                      ,SC.WEEK_COUNT    ),
        selsps as (
                      select /*+ parallel(sc,2) parallel(ss,2)    */
                       SC.fin_year_no                         fin_year_no
                      ,SC.fin_week_no                       fin_week_no
                      ,SC.SK1_ITEM_NO                          SK1_ITEM_NO
                      ,SC.SK1_LOCATION_NO                   SK1_LOCATION_NO
                      ,SC.LOCATION_NO                          LOCATION_NO
                      ,SC.ITEM_NO                                 ITEM_NO
                      ,SC.THIS_WEEK_START_DATE         THIS_WEEK_START_DATE
                      ,SC.THIS_WEEK_END_DATE            THIS_WEEK_END_DATE
                      ,SC.sk1_prom_no                          sk1_prom_no
                      ,SC.WEEK_COUNT                            WEEK_COUNT
                      ,SUM(NVL(WASTE_COST,0))                                                    WASTE_COST
                from  dwh_performance.temp_mart_6wkavg_PROM_DATES SC
                 join dwh_performance.RTL_LOC_ITEM_WK_RMS_SPARSE ss
                  on ss.fin_year_no = sC.fin_year_no
                  and ss.fin_week_no = sC.fin_week_no
                  and ss.sk1_item_no = sC.sk1_item_no
                  and ss.sk1_location_no = sC.sk1_location_no
                                 where ss.this_week_start_date between  G_START_12WK and G_END_12WK
GROUP BY                        SC.fin_year_no                  
                      ,SC.fin_week_no                       
                      ,SC.SK1_ITEM_NO                          
                      ,SC.SK1_LOCATION_NO                   
                      ,SC.ITEM_NO                                 
                      ,SC.LOCATION_NO                         
                      ,SC.THIS_WEEK_START_DATE         
                      ,SC.THIS_WEEK_END_DATE           
                      ,SC.sk1_prom_no                        
                      ,SC.WEEK_COUNT                           
)
                  select /*+ parallel(sc,2)  */
                       SC.fin_year_no                         fin_year_no
                      ,SC.fin_week_no                       fin_week_no
                      ,SC.SK1_ITEM_NO                          SK1_ITEM_NO
                      ,SC.SK1_LOCATION_NO                   SK1_LOCATION_NO
                      ,SC.LOCATION_NO                          LOCATION_NO
                      ,SC.ITEM_NO                                 ITEM_NO
                      ,SC.THIS_WEEK_START_DATE         THIS_WEEK_START_DATE
                      ,SC.THIS_WEEK_END_DATE            THIS_WEEK_END_DATE
                      ,SC.sk1_prom_no                          sk1_prom_no
                      ,SC.WEEK_COUNT                            WEEK_COUNT
                      ,SALES_QTY 
                      ,SALES
                      ,SALES_MARGIN 
                      ,WASTE_COST
                from  dwh_performance.temp_mart_6wkavg_PROM_DATES SC
                full outer join seldns sd
                  on sD.fin_year_no = sC.fin_year_no
                  and sD.fin_week_no = sC.fin_week_no
                  and sD.sk1_item_no = sC.sk1_item_no
                  and sD.sk1_location_no = sC.sk1_location_no  
                full outer join selsps ss
                  on ss.fin_year_no = sC.fin_year_no
                  and ss.fin_week_no = sC.fin_week_no
                  and ss.sk1_item_no = sC.sk1_item_no
                  and ss.sk1_location_no = sC.sk1_location_no
                        
                ;             
    g_recs := G_RECS + sql%rowcount;
    
    l_text := 'temp_mart_6wkavg_PROM_VALUES INSERT  ** g_recs='||g_recs;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    COMMIT;


--**************************************************************************************************
-- Rebuild indexes
--**************************************************************************************************

     execute immediate('create index dwh_performance.i10_p_tmp_mrt_6wkavg_PROM_VLS on dwh_performance.temp_mart_6wkavg_PROM_VALUES
                            (week_count, item_no) TABLESPACE PRF_MASTER') ;
    l_text := 'create index dwh_performance.i10_p_tmp_mrt_6wkavg_PROM_VLS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
     execute immediate('create index dwh_performance.i20_p_tmp_mrt_6wkavg_PROM_VLS on dwh_performance.temp_mart_6wkavg_PROM_VALUES
                            (week_count, sk1_prom_no) TABLESPACE PRF_MASTER') ;
    l_text := 'create index dwh_performance.i20_p_tmp_mrt_6wkavg_PROM_VLS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
     execute immediate('create index dwh_performance.i30_p_tmp_mrt_6wkavg_PROM_VLS on dwh_performance.temp_mart_6wkavg_PROM_VALUES
                            (item_no) TABLESPACE PRF_MASTER') ;
      l_text := 'create index dwh_performance.i30_p_tmp_mrt_6wkavg_PROM_VLS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     execute immediate('create index dwh_performance.i40_p_tmp_mrt_6wkavg_PROM_VLS on dwh_performance.temp_mart_6wkavg_PROM_VALUES
                             (sk1_prom_no) TABLESPACE PRF_MASTER') ;
    l_text := 'create index dwh_performance.i40_p_tmp_mrt_6wkavg_PROM_VLS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



     
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_read||g_recs_read;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
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


END WH_PRF_CORP_280U_WL_PART2;
