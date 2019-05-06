--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_281U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_281U" 
(p_forall_limit in integer,p_success out boolean)
as
--**************************************************************************************************
-- test version for prd
  --**************************************************************************************************
--  Date:        October 2013
--  Author:      W Lyttle
--  Purpose:     Load Foods Sales data where there have been no promotions
--               during last 12 weeks.
--               These values will be used to calculate 6wk average values.
--  Tables:      Input  - RTL_LOC_ITEM_WK_RMS_DENSE(dns) ,
--                        DIM_PROM(dp),
--                        DIM_PROM_ITEM_ALL(dpia),
--                        DIM_LOCATION(dl),
--                        FND_PROM_LOCATION(fpl) ,
--                        DIM_ITEM (di),
--                        DIM_ITEM_UDA(diu),
--                        FND_UDA_VALUE(fuv),
--                        FND_ITEM_UDA(fia),
--                        DIM_CALENDAR(dc)
--
--               Output - TEMP_MART_FD_LOC_ITEM_WK_6WKAVG
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
    g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
    g_recs_read          integer       :=  0;
    g_recs_inserted      integer       :=  0;
    g_chain_corporate    integer       :=  0;
    g_chain_franchise    integer       :=  0;
    g_fin_week_no        DIM_CALENDAR.fin_week_no%type;
    g_fin_year_no        DIM_CALENDAR.fin_year_no%type;
    g_ly_fin_week_no     DIM_CALENDAR.fin_week_no%type;
    g_ly_fin_year_no     DIM_CALENDAR.fin_year_no%type;
    g_lcw_fin_week_no    DIM_CALENDAR.fin_week_no%type;
    g_lcw_fin_year_no    DIM_CALENDAR.fin_year_no%type;
    g_date               date;
    g_start_date         date;
    g_end_date           date;
    g_ly_start_date      date;
    g_ly_end_date        date;
    G_START_12WK         DATE;
    G_END_12WK           DATE;
    G_item_no                    number := 0;
    G_sk1_item_no                number := 0;
    G_item_nod                number := 0;
    G_RECS                       number := 0;
    G_accum_SALES_6WK_QTY        number := 0;
    G_accum_SALES_6WK            number := 0;
    G_accum_SALES_6WK_MARGIN     number := 0;
    G_accum_WASTE_6WK_PROMO_COST number := 0;
    G_SALES_6WK_QTY        number := 0;
    G_SALES_6WK            number := 0;
    G_SALES_6WK_MARGIN     number := 0;
    G_WASTE_6WK_PROMO_COST number := 0;
    g_sub                        number := 0;
    g_wkcnt                      number := 0;



l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_281U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD foods 6wk records TSTWL';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
   if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
      g_forall_limit := p_forall_limit;
   end if;
   p_success := false;
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'LOAD OF TEMP_MART_FD_LOC_ITEM_WK_6WKAVG STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);

   ----- START -- this is for testing purposes
   --   G_DATE := '29 october 2013'-;

  --    update dim_control
 --     set today_date = g_date,
 --     last_wk_start_date = '21 OCTOBER 2013',
  --    last_wk_end_date = '27 OCTOBER 2013';
 ----- END -- this is for testing purposes



   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';
   l_text := 'alter session enable parallel dml';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   EXECUTE immediate('truncate table dwh_performance.MART_FD_ITEM_6WKAVG');
   l_text := 'truncate table dwh_performance.MART_FD_ITEM_6WKAVG';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  COMMIT;


 --    execute immediate('create index dwh_performance.i50_p_tmp_mrt_6wkavg_PRM_DTS on dwh_performance.temp_mart_6wkavg_prom_dates
 --                            (SK1_PROM_NO) TABLESPACE PRF_MASTER') ;
 --   l_text := 'create index dwh_performance.i50_p_tmp_mrt_6wkavg_PRM_DTS';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Determine period of extract
--**************************************************************************************************

   SELECT last_wk_start_date-77,
      last_wk_end_date
  INTO G_START_12WK,
    G_END_12WK
  FROM DIM_control;

  l_text := 'PERIOD='||G_START_12WK||' - '||G_END_12WK;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     G_RECS := 0;

--**************************************************************************************************
-- GATHER STATS
--**************************************************************************************************
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'MART_FD_ITEM_6WKAVG', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS on dwh_performance.MART_FD_ITEM_6WKAVG';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Main extract
--**************************************************************************************************

 insert /*+ APPEND parallel (aps,2) */ into dwh_performance.MART_FD_ITEM_6WKAVG
            WITH 
            selcal AS
                          (
                            SELECT distinct  fin_year_no,    fin_week_no
                            FROM dwh_performance.dim_calendar
                            where calendar_date between G_START_12WK and G_END_12WK
                            ORDER BY  fin_year_no,   fin_week_no
                           )
                           ,
            
            selmaxp AS
                          (
                            SELECT /*+ parallel(tmpm,2) */
                                        SK1_ITEM_NO,    item_no,    fin_year_no,    fin_week_no,    week_count,    MAX(sk1_prom_no) maxprom
                            FROM dwh_performance.temp_mart_6wkavg_prom_dates tmpm
                            GROUP BY    SK1_ITEM_NO,  item_no,  fin_year_no,  fin_week_no,  week_count
                            ORDER BY  SK1_ITEM_NO,   MAX(sk1_prom_no),item_no,  fin_year_no,   fin_week_no,  week_count
                           )
                           ,
             selzeroes AS
              -- rule 1--
                          (
                                SELECT
                                 SK1_ITEM_NO,    item_no,    COUNT(DISTINCT week_count) diswkcnt
                          FROM selmaxp sp
                          WHERE maxprom   IS NOT NULL
                            AND week_count   < 7
                          GROUP BY SK1_ITEM_NO,    item_no
                          HAVING COUNT(DISTINCT week_count) > 5
                          order by sk1_item_no
                          )
                          ,
              selext AS
              -- all recs where rule 1 does not apply
                         (
                            SELECT DISTINCT sk1_item_no,    item_no,    fin_year_no,    fin_week_no,    week_count
                            FROM selmaxp sp2
                            WHERE maxprom IS NULL
                              AND NOT EXISTS (SELECT sk1_item_no FROM selzeroes sz WHERE sp2.sk1_item_no = sz.sk1_item_no )
                            ORDER BY sk1_item_no,    fin_year_no,    fin_week_no
                          )
                          ,
              selrnk AS
                          (

                          SELECT srk.sk1_item_no,    srk.item_no,    srk.fin_year_no,    srk.fin_week_no,    srk. week_count , srk.rnk
                          FROM
                                (SELECT sk1_item_no,      item_no,      fin_year_no,      fin_week_no,      week_count ,
                                  dense_rank() over (partition BY sk1_item_no order by week_count) rnk
                                FROM selext se ) srk
                          WHERE rnk <= 6
                          ORDER BY    fin_year_no,    fin_week_no, sk1_item_no

                          )
                          ,
              selitm AS
              -- distinct NON-rule 1 items
                            (
                                SELECT sk1_item_no, COUNT(*) wkcnt
                            FROM selrnk
                            GROUP BY sk1_item_no
                            ORDER BY sk1_item_no
                            ),
              SELDNS as (select rdns.* from dwh_performance.RTL_LOC_ITEM_WK_RMS_DENSE rdns, selcal dc where rdns.fin_year_no = dc.fin_year_no and rdns.fin_week_no = dc.fin_week_no),
              SELsps as (select rsps.* from dwh_performance.RTL_LOC_ITEM_WK_RMS_sparse rsps, selcal dcs where rsps.fin_year_no = dcs.fin_year_no and rsps.fin_week_no = dcs.fin_week_no)
      SELECT SK1_ITEM_NO
                  , G_START_12WK
                  , G_END_12WK
                  ,
                  0 SALES_6WK_QTY ,
                  0 SALES_6WKAVG_EXCL_PROMO_QTY ,
                  0 SALES_6WK ,
                  0 SALES_6WKAVG_EXCL_PROMO ,
                  0 SALES_6WK_MARGIN ,
                  0 SALES_6WKAVG_MARGIN_PERC ,
                  0 WASTE_6WK_PROMO_COST ,
                  0 WASTE_6WKAVG_COST_PERC ,
                  ITEM_NO
      FROM selzeroes
      UNION ALL
      SELECT    SK1_ITEM_NO
                , g_START_12WK
                , g_END_12WK
                , SALES_6WK_QTY
                ,CASE    WHEN wkcnt = 6    THEN DECODE(wkcnt,0,0,SALES_6WK_QTY/wkcnt)    ELSE 0  END ,
                SALES_6WK ,
                CASE    WHEN wkcnt = 6    THEN DECODE(wkcnt,0,0,SALES_6WK/wkcnt)    ELSE 0  END ,
                SALES_6WK_MARGIN ,
                CASE    WHEN wkcnt = 6   THEN DECODE(SALES_6WK,0,0,SALES_6WK_MARGIN/SALES_6WK)    ELSE 0  END ,
                WASTE_6WK_PROMO_COST ,
                CASE    WHEN wkcnt = 6    THEN DECODE(wkcnt,0,0,WASTE_6WK_PROMO_COST/wkcnt)    ELSE 0  END ,
                ITEM_NO
      FROM
                (SELECT
                  /*+ parallel(tmp,2) parallel(dns,2) full(dns) parallel(sps,2) full(sps) */
                      TMP.SK1_ITEM_NO ,
                      SUM(NVL(SALES_QTY,0)) SALES_6WK_QTY ,
                      SUM(NVL(SALES,0)) SALES_6WK,
                      SUM(NVL(SALES_MARGIN,0)) SALES_6WK_MARGIN,
                      SUM(NVL(WASTE_COST,0)) WASTE_6WK_PROMO_COST,
                      TMP.ITEM_NO ITEM_NO,
                      si.wkcnt
                FROM SELitm Si
                JOIN sELrnk Sr
                   ON sr.SK1_ITEM_NO = si.SK1_ITEM_NO
                JOIN dwh_performance.temp_mart_6wkavg_prom_dates TMP
                    ON tmp.SK1_ITEM_NO = sr.SK1_ITEM_NO
                 --   AND tmp.week_count = sr.week_count
                    and tmp.fin_year_no = sr.fin_year_no
                    and tmp.fin_week_no = sr.fin_week_no
                LEFT OUTER JOIN seldns DNS
                    ON DNS.SK1_ITEM_NO      = Sr.SK1_ITEM_NO
                    AND DNS.SK1_LOCATION_NO = TMP.SK1_LOCATION_NO
                    AND DNS.FIN_YEAR_NO     = sr.FIN_YEAR_NO
                    AND DNS.FIN_WEEK_NO     = sr.FIN_WEEK_NO
                LEFT OUTER JOIN selsps SPS
                    ON SPS.SK1_ITEM_NO      = Sr.SK1_ITEM_NO
                    AND SPS.SK1_LOCATION_NO = TMP.SK1_LOCATION_NO
                    AND SPS.FIN_YEAR_NO     = sr.FIN_YEAR_NO
                    AND SPS.FIN_WEEK_NO     = sr.FIN_WEEK_NO
                GROUP BY TMP.SK1_ITEM_NO , TMP.ITEM_NO, si.wkcnt) ;

      g_recs_INSERTED := G_RECS_INSERTED + sql%rowcount;
          commit;
         l_text := 'RULE 2+3 ** g_recs='||g_recs;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'MART_FD_ITEM_6WKAVG', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS on dwh_performance.MART_FD_ITEM_6WKAVG';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    G_RECS_READ := G_RECS_INSERTED;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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


END WH_PRF_CORP_281U;
