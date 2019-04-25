--------------------------------------------------------
--  DDL for Procedure WH_PRF_EXT_009U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_EXT_009U" 

 (p_forall_limit in integer,p_success out boolean) 
 as
--**************************************************************************************************
--  Date:        JULY 2015
--  Author:      RAPHAEL RICKETTS
--  Purpose:     ALLOCATION OPTIMIZATION PROJECT  --  TY/LY performance report 2015
-- *********************************
--  Tables:      Input  - RTL_LOC_ITEM_WK_RMS_DENSE
--                  Output - RTL_LOC_DEPT_UDA_WK_ALOPT_TYLY
--  Packages:    dwh_constants, dwh_log, dwh_valid
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';

g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
g_start_date         date      ;
g_end_date           date      ;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_EXT_009U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'SALES SQL BASE FOR 6 WEEKS BACK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum  then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'SALES SQL BASE FOR 6 WEEKS BACK EX ODWH STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    SELECT  max(this_week_end_date)
   into g_end_date
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE <= g_date
    and fin_day_no = 7;

                   
   SELECT max(this_week_start_date)
   into g_start_date
                   FROM DIM_CALENDAR
                   WHERE calendar_date <=    ( SELECT  max(this_week_end_date) - 84
                                                FROM DIM_CALENDAR
                                                WHERE CALENDAR_DATE <= g_date
                                                and fin_day_no = 7);

    l_text := 'Period BEING PROCESSED = '||g_start_date|| ' to '|| g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  
/*  SELECT DISTINCT THIS_WEEK_START_DATE, THIS_WEEK_END_DATE
  INTO g_start_date, g_end_date
  from dim_calendar where calendar_date = g_date - 7;
*/

-----------------------------------------------------------------------
-- THIS YEAR CATALOG
-----------------------------------------------------------------------
    l_text := 'TRUNCATED table dwh_performance.TMP_LOC_DEPT_UDA_WK_ALOPT_TY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.TMP_LOC_DEPT_UDA_WK_ALOPT_TY');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_DEPT_UDA_WK_ALOPT_TY', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;
    
     INSERT /*+ APPEND */
      into DWH_PERFORMANCE.TMP_LOC_DEPT_UDA_WK_ALOPT_TY
              with selcal as
                            (select
                              /*+ full(dc) materialize */
                                    DISTINCT FIN_YEAR_NO,
                                    FIN_WEEK_NO,
                                     ('W'    || FIN_YEAR_NO    || '_'    || LPAD( FIN_WEEK_NO, 2 , '0' )) TY_SALES_WEEK,
                                   ('W'    || ly_Fin_Year_No    || '_'    || Lpad( ly_Fin_Week_No, 2 , '0' )) LY_Sales_Week
                                   from dim_calendar dc
                            where calendar_date between g_start_date and g_end_date
                            ORDER BY FIN_YEAR_NO, FIN_WEEK_NO
                            ),
                selitm as
                            (select
                              /*+ full(di) full(uda) materialize */
                                    DISTINCT
                                    SK1_STYLE_COLOUR_NO,
                                    DEPARTMENT_NO
                            from dim_item di
                            where di.business_unit_no   <> 50
                            order by SK1_STYLE_COLOUR_NO
                            ),
                selloc as
                            (select
                              /*+ materialize full(c) full(DL) */
                                    DISTINCT DL.LOCATION_NO,
                                    DL.SK1_LOCATION_NO
                            from dim_location dl
                            where dl.loc_type                  = 'S'
                                and chain_no                      in (10,30)
                            order by location_no
                            )
  SELECT
    /*+ materialize PARALLEL(R,8) no_index (R) */
    TY_SALES_WEEK,
    LY_SALES_WEEK,
    SL.LOCATION_NO,
    SI.DEPARTMENT_NO,
     R.SK1_AVAIL_UDA_VALUE_NO,
    SUM(r.Ch_Num_Avail_Days) Ch_Num_Avail_Days,
    SUM(r.Ch_Num_Catlg_Days) Ch_Num_Catlg_Days,
    SUM(r.Prom_Reg_Sales_Qty_Catlg) Prom_Reg_Sales_Qty_Catlg,
    SUM(r.Prom_Reg_Sales_Catlg) Prom_Reg_Sales_Catlg,
    SUM(r.Prom_Sales_Qty_Catlg) Prom_Sales_Qty_Catlg,
    SUM(r.Prom_Sales_Catlg) Prom_Sales_Catlg,
    SUM(r.Reg_Sales_Qty_Catlg) Reg_Sales_Qty_Catlg,
    SUM(r.Reg_Soh_Qty_Catlg) Reg_Soh_Qty_Catlg,
    SUM(r.Reg_Soh_Selling_Catlg) Reg_Soh_Selling_Catlg
  FROM SELCAL SC,
  SELITM SI,
  SELLOC SL,
  RTL_LOC_SC_WK_AST_CATLG R
  WHERE SC.FIN_YEAR_NO     = R.FIN_YEAR_NO
  AND SC.FIN_WEEK_NO       = R.FIN_WEEK_NO
  AND SI.SK1_STYLE_COLOUR_NO  = R.SK1_STYLE_COLOUR_NO
  AND sl.SK1_LOCATION_NO      = R.SK1_LOCATION_NO
  GROUP BY  TY_SALES_WEEK,
    LY_SALES_WEEK,  
    SL.LOCATION_NO,
    SI.DEPARTMENT_NO,
     R.SK1_AVAIL_UDA_VALUE_NO;
                      
   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.TMP_LOC_DEPT_UDA_WK_ALOPT_TY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_DEPT_UDA_WK_ALOPT_TY', DEGREE => 8);
    commit;
  
    l_text := 'recs written to TMP_LOC_DEPT_UDA_WK_ALOPT_TY = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;

-----------------------------------------------------------------------
-- LAST YEAR CATALOG
-----------------------------------------------------------------------
    l_text := 'TRUNCATED table dwh_performance.TMP_LOC_DEPT_UDA_WK_ALOPT_LY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.TMP_LOC_DEPT_UDA_WK_ALOPT_LY');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_DEPT_UDA_WK_ALOPT_LY', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;
    
     INSERT /*+ APPEND */
      into DWH_PERFORMANCE.TMP_LOC_DEPT_UDA_WK_ALOPT_LY
              with selcal as
                            (select
                              /*+ full(dc) materialize */
                                    DISTINCT LY_FIN_YEAR_NO,
                                    LY_FIN_WEEK_NO,
                                    ('W'    || FIN_YEAR_NO    || '_'    || LPAD( FIN_WEEK_NO, 2 , '0' )) TY_SALES_WEEK,
                                   ('W'    || ly_Fin_Year_No    || '_'    || Lpad( ly_Fin_Week_No, 2 , '0' )) LY_Sales_Week
                                   from dim_calendar dc
                            WHERE CALENDAR_DATE BETWEEN G_START_DATE AND G_END_DATE
                            ORDER BY ly_FIN_YEAR_NO, ly_FIN_WEEK_NO
                            ),
                selitm as
                            (select
                              /*+ full(di) full(uda) materialize */
                                    DISTINCT
                                    SK1_STYLE_COLOUR_NO,
                                    DEPARTMENT_NO
                            from dim_item di
                            where di.business_unit_no   <> 50
                            order by SK1_STYLE_COLOUR_NO
                            ),
                selloc as
                            (select
                              /*+ materialize full(c) full(DL) */
                                    DISTINCT DL.LOCATION_NO,
                                    DL.SK1_LOCATION_NO
                            from dim_location dl
                            where dl.loc_type                  = 'S'
                                and chain_no                      in (10,30)
                            order by location_no
                            )
  SELECT
    /*+ materialize PARALLEL(R,8) no_index (R) */
    TY_SALES_WEEK,
    LY_SALES_WEEK,
    SL.LOCATION_NO,
    SI.DEPARTMENT_NO,
     R.SK1_AVAIL_UDA_VALUE_NO,
     SUM(r.Ch_Num_Avail_Days) Ch_Num_Avail_Days,
    SUM(r.Ch_Num_Catlg_Days) Ch_Num_Catlg_Days,
    SUM(r.Prom_Reg_Sales_Qty_Catlg) Prom_Reg_Sales_Qty_Catlg,
    SUM(r.Prom_Reg_Sales_Catlg) Prom_Reg_Sales_Catlg,
    SUM(r.Prom_Sales_Qty_Catlg) Prom_Sales_Qty_Catlg,
    SUM(r.Prom_Sales_Catlg) Prom_Sales_Catlg,
    SUM(r.Reg_Sales_Qty_Catlg) Reg_Sales_Qty_Catlg,
    SUM(r.Reg_Soh_Qty_Catlg) Reg_Soh_Qty_Catlg,
    SUM(r.Reg_Soh_Selling_Catlg) Reg_Soh_Selling_Catlg
  FROM SELCAL SC,
  SELITM SI,
  SELLOC SL,
  RTL_LOC_SC_WK_AST_CATLG R
  WHERE SC.LY_FIN_YEAR_NO     = R.FIN_YEAR_NO
  AND SC.ly_FIN_WEEK_NO       = R.FIN_WEEK_NO
  AND si.SK1_STYLE_COLOUR_NO  = R.SK1_STYLE_COLOUR_NO
  AND sl.SK1_LOCATION_NO      = R.SK1_LOCATION_NO
  GROUP BY      TY_SALES_WEEK,
    LY_SALES_WEEK,
    SL.LOCATION_NO,
    SI.DEPARTMENT_NO,
     R.SK1_AVAIL_UDA_VALUE_NO;
                      
   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.TMP_LOC_DEPT_UDA_WK_ALOPT_LY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_DEPT_UDA_WK_ALOPT_LY', DEGREE => 8);
    commit;
  
    l_text := 'recs written to TMP_LOC_DEPT_UDA_WK_ALOPT_LY = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;


 -----------------------------------------------------------------------
--
-----------------------------------------------------------------------                       

    l_text := 'TRUNCATED table dwh_performance.RTL_LOC_DEPT_UDA_WK_ALOPT_TYLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.RTL_LOC_DEPT_UDA_WK_ALOPT_TYLY');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_LOC_DEPT_UDA_WK_ALOPT_TYLY', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;


      INSERT /*+ APPEND */
      into DWH_PERFORMANCE.RTL_LOC_DEPT_UDA_WK_ALOPT_TYLY 
            SELECT    /*+ parallel(ud) full(ud) */    distinct  sa.LOCATION_NO,
                                  SA.DEPARTMENT_NO,
                                  DL.LOCATION_NAME,
                                  dd.DEPARTMENT_NAME,
                                  UD.Uda_Value_Short_Desc,
                                  sa.TY_SALES_WEEK,
                                  SUM(NVL(r2.Prom_Reg_Sales_Qty_Catlg,0))
                                  over 
                                  (partition by sa.LOCATION_NO,
                                                SA.DEPARTMENT_NO,
                                                DL.LOCATION_NAME,
                                                dd.DEPARTMENT_NAME,
                                                UD.Uda_Value_Short_Desc,
                                                sa.TY_SALES_WEEK, order by sa.LOCATION_NO,
                                                SA.DEPARTMENT_NO,
                                                DL.LOCATION_NAME,
                                                dd.DEPARTMENT_NAME,
                                                UD.Uda_Value_Short_Desc,
                                                sa.TY_SALES_WEEK, 
                                  rows between unbounded preceding and current row) s, 
                                  Prom_Reg_Sales_Qty_Catlg 
          FROM DWH_PERFORMANCE.RTL_LOC_DEPT_UDA_WK_ALOPT_TYLY 
          ;


   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.RTL_LOC_DEPT_UDA_WK_ALOPT_TYLY';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_LOC_DEPT_UDA_WK_ALOPT_TYLY', DEGREE => 8);
    commit;
  
   l_text := 'recs written to RTL_LOC_DEPT_UDA_WK_ALOPT_TYLY = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--select dt, sum(xy) over (partition by trunc(dt) order by dt rows between 
--unbounded preceding and current row) s, xy from sum_example;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


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
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);


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

END WH_PRF_EXT_009U;
