--------------------------------------------------------
--  DDL for Procedure WH_PRF_EXT_006U_HIST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_EXT_006U_HIST" 

 (p_forall_limit in integer,p_success out boolean) 
 as
--**************************************************************************************************
--  Date:        august 2015
--  Author:      wendy lyttle
--  Purpose:     ALLOCATION OPTIMIZATION PROJECT  --  sales AND STOCK  extract history
--
--  Tables:      Input  - RTL_LOC_ITEM_WK_RMS_DENSE
--               Output - RTL_LOC_ITEM_WK_ALOPT_SASTKHST
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  12 aug 2015 wl Extract Fin Year 2015, subclass 5703 (request from Raph/leigh-ann)

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

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_EXT_006U_HIST';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'SALES SQL BASE FOR HISTORY';
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

    l_text := 'SALES SQL BASE FOR HISTORY  EX ODWH STARTED AT '||
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

    execute immediate 'alter session enable parallel dml';

/*
    SELECT  max(this_week_end_date)
   into g_end_date
    FROM DIM_CALENDAR
    WHERE CALENDAR_DATE <= g_date
    and fin_day_no = 7;

                   
   SELECT max(this_week_start_date)
   into g_start_date
                   FROM DIM_CALENDAR
                   WHERE calendar_date <=    ( SELECT  max(this_week_end_date) - 35
                                                FROM DIM_CALENDAR
                                                WHERE CALENDAR_DATE <= g_date
                                                and fin_day_no = 7);
*/
      SELECT  min(this_week_start_date), max(this_week_end_date)
   into g_start_date, g_end_date
    FROM DIM_CALENDAR
 --   WHERE calendar_date between '30 june 2014' and '28 june 2015'; --(last 52 weeks)
  WHERE calendar_date between '29 june 2015' and '1 may 2016'; --(last 52 weeks)

    l_text := 'Period BEING PROCESSED = '||g_start_date|| ' to '|| g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    


-----------------------------------------------------------------------
-- Create list
-----------------------------------------------------------------------
    l_text := 'TRUNCATED table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_SSA';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_SSA');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_SSA', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;
    
     INSERT /*+ APPEND */
      into DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_SSA
              with selcal as
                            (select
                              /*+ full(dc) materialize */
                                    distinct fin_year_no,
                                    fin_week_no,
                                    ly_fin_year_no,
                                    ly_fin_week_no
                            from dim_calendar dc
                            where calendar_date between g_start_date and g_end_date
                            ORDER BY FIN_YEAR_NO, FIN_WEEK_NO
                            ),
                selitm as
                            (select
                              /*+ full(di) full(uda) materialize */
                                    distinct di.sk1_item_no,
                                    di.item_no
                            from dim_item di
                            where di.business_unit_no   <> 50
                         --       and di.group_no           in(3)
                             --   and di.subclass_no = 5703
                            order by item_no
                            ),
                selloc as
                            (select
                              /*+ materialize full(c) full(DL) */
                                    distinct dl.location_no,
                                    dl.sk1_location_no
                            from dim_location dl
                            where dl.loc_type                  = 'S'
                                and chain_no                      in (10,30)
                            order by location_no
                            )
                SELECT
                                    /*+ PARALLEL(R,4) no_index (R) */
                                          SC.Fin_Year_No,
                                          SC.Fin_Week_No,
                                          SL.Location_No,
                                          SI.Item_No,
                                          NVL(clear_Sales_Qty,0) clear_Sales_Qty
                                  FROM SELLOC SL,
                                        SELITM SI,
                                        SELCAL SC,
                                        Rtl_Loc_Item_wk_Rms_Sparse R
                                  WHERE SC.Fin_Year_No   = R.Fin_Year_No
                                  AND SC.fin_week_no     = r.fin_week_no
                                  AND SL.Sk1_Location_No = R.Sk1_Location_No
                                  AND SI.Sk1_Item_No     = R.Sk1_Item_No
--                                  AND NVL(clear_Sales_Qty,0) > 0
;
                      
   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_SSA';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_SSA', DEGREE => 8);
    commit;
  
    l_text := 'recs written to TMP_LOC_ITEM_WK_ALOPT_SSA = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;


-----------------------------------------------------------------------
-- Create list
-----------------------------------------------------------------------
    l_text := 'TRUNCATED table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_SA';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_SA');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_SA', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;
    
     INSERT /*+ APPEND */
      into DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_SA
              with selcal as
                            (select
                              /*+ full(dc) materialize */
                                    distinct fin_year_no,
                                    fin_week_no,
                                    ly_fin_year_no,
                                    ly_fin_week_no
                            from dim_calendar dc
                            where calendar_date between g_start_date and g_end_date
                            ORDER BY FIN_YEAR_NO, FIN_WEEK_NO
                            ),
                selitm as
                            (select
                              /*+ full(di) full(uda) materialize */
                                    distinct di.sk1_item_no,
                                    di.item_no
                            from dim_item di
                            where di.business_unit_no   <> 50
                        --        and di.group_no           in(3)
                             --   and di.subclass_no = 5703
                            order by item_no
                            ),
                selloc as
                            (select
                              /*+ materialize full(c) full(DL) */
                                    distinct dl.location_no,
                                    dl.sk1_location_no
                            from dim_location dl
                            where dl.loc_type                  = 'S'
                                and chain_no                      in (10,30)
                            order by location_no
                            ),
                SELDNS AS (SELECT
                                    /*+ materialize PARALLEL(R,4) no_index (R) */
                                          SC.Fin_Year_No,
                                          SC.Fin_Week_No,
                                          SL.Location_No,
                                          SI.Item_No,
                                          NVL(Sales_Qty,0) Sales_Qty
                                  FROM SELLOC SL,
                                        SELITM SI,
                                        SELCAL SC,
                                        Rtl_Loc_Item_wk_Rms_Dense R
                                  WHERE SC.Fin_Year_No   = R.Fin_Year_No
                                  AND SC.fin_week_no     = r.fin_week_no
                                  AND SL.Sk1_Location_No = R.Sk1_Location_No
                                  AND SI.Sk1_Item_No     = R.Sk1_Item_No
--                                  AND NVL(Sales_Qty,0)  > 0
                                  )
               SELECT
                                    /*+  FULL(SA) FULL(SL) PARALLEL(SA,6)  */
                                          SL.Fin_Year_No,
                                          SL.Fin_Week_No,
                                          SL.Location_No,
                                          SL.Item_No,
                                          NVL(Sales_Qty,0) - NVL(SA.CLEAR_SALES_QTY,0) Sales_Qty
                                  FROM SELDNS SL
                                  LEFT OUTER JOIN   DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_SSA SA
                                  ON SL.Fin_Year_No = SA.Fin_Year_No
                                  AND SL.Fin_Week_No   = SA.Fin_Week_No
                                  AND SL.Location_No   = SA.Location_No
                                  And SL.Item_No       = SA.Item_No
                                  where
                                   NVL(Sales_Qty,0) - NVL(SA.CLEAR_SALES_QTY,0) > 0

;
                      
   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_SA';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_SA', DEGREE => 8);
    commit;
  
    l_text := 'recs written to TMP_LOC_ITEM_WK_ALOPT_SA = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;



-----------------------------------------------------------------------
-- Create list
-----------------------------------------------------------------------
    l_text := 'TRUNCATED table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_STK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_STK');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_STK', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;
    
     INSERT /*+ APPEND */
      into DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_STK
              with selcal as
                            (select
                              /*+ full(dc) materialize */
                                    distinct fin_year_no,
                                    fin_week_no,
                                    ly_fin_year_no,
                                    ly_fin_week_no
                            from dim_calendar dc
                            where calendar_date between g_start_date and g_end_date
                            ORDER BY FIN_YEAR_NO, FIN_WEEK_NO
                            ),
                selitm as
                            (select
                              /*+ full(di) full(uda) materialize */
                                    distinct di.sk1_item_no,
                                    di.item_no
                            from dim_item di
                            where di.business_unit_no   <> 50
                     --           and di.group_no           in(3)
                      --    and di.subclass_no = 5703
                            order by item_no
                            ),
                selloc as
                            (select
                              /*+ materialize full(c) full(DL) */
                                    distinct dl.location_no,
                                    dl.sk1_location_no
                            from dim_location dl
                            where dl.loc_type                  = 'S'
                                and chain_no                      in (10,30)
                            order by location_no
                            )
                SELECT
                                    /*+ PARALLEL(R,4) no_index (R) */
                                          SC.Fin_Year_No,
                                          SC.Fin_Week_No,
                                          SL.Location_No,
                                          SI.Item_No,
                                          NVL(SOH_Qty,0) - NVL(CLEAR_SOH_Qty,0) SOH_Qty
                                  FROM SELLOC SL,
                                        SELITM SI,
                                        SELCAL SC,
                                        Rtl_Loc_item_Wk_Rms_Stock R
                                  WHERE SC.Fin_Year_No   = R.Fin_Year_No
                                  AND SC.fin_week_no     = r.fin_week_no
                                  AND SL.Sk1_Location_No = R.Sk1_Location_No
                                  AND SI.Sk1_Item_No     = R.Sk1_Item_No
                                  AND (NVL(SOH_Qty,0) - NVL(CLEAR_SOH_Qty,0)) > 0
;
                      
   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_STK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_STK', DEGREE => 8);
    commit;
  
    l_text := 'recs written to TMP_LOC_ITEM_WK_ALOPT_STK = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;



-----------------------------------------------------------------------
--
-----------------------------------------------------------------------                       

/*    l_text := 'TRUNCATED table dwh_performance.RTL_LOC_ITEM_WK_ALOPT_SASTKHST';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.RTL_LOC_ITEM_WK_ALOPT_SASTKHST');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_LOC_ITEM_WK_ALOPT_SASTKHST', DEGREE => 8);
    commit;
*/
    g_recs_read     := 0;
    g_recs_inserted := 0;


      INSERT /*+ APPEND */
      into DWH_PERFORMANCE.RTL_LOC_ITEM_WK_ALOPT_SASTKHST
      WITH SELALL AS (      SELECT
                   /*+ MATERIALIZE FULL(LY) PARALLEL(LY,4)   FULL(TY) PARALLEL(TY,4)  */
                    NVL(SK.Location_No,SA.LOCATION_NO) LOCATION_NO,
                    NVL(SK.Item_No,SA.Item_No)         Item_No,
                    NVL(SK.Fin_Year_No,SA.Fin_Year_No) Fin_Year_No,
                    NVL(SK.Fin_week_No,SA.Fin_week_No) Fin_week_No,
                    nvl(SA.Sales_Qty,0)                SA_Qty,
                    nvl(SK.SOH_Qty,0)                  SOH_Qty
            FROM DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_STK SK
            FULL OUTER JOIN  DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_SA SA
            ON SK.Fin_Year_No = SA.Fin_Year_No
            AND SK.Fin_Week_No   = SA.Fin_Week_No
            AND SK.Location_No   = SA.Location_No
            And SK.Item_No       = SA.Item_No)
            SELECT Location_No,
                   Item_No,
                    'W'
                    || TO_CHAR(Fin_Year_No)
                    || '_'
                    || Lpad( TO_CHAR(Fin_Week_No), 2 , '0' ) Sales_Week,
                    Sa_Qty,
                    SOH_Qty
          FROM SELALL WHERE SA_Qty > 0 or SOH_Qty > 0;


   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.RTL_LOC_ITEM_WK_ALOPT_SASTKHST';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_LOC_ITEM_WK_ALOPT_SASTKHST', DEGREE => 8);
    commit;
  
   l_text := 'recs written to RTL_LOC_ITEM_WK_ALOPT_SASTKHST = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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

END WH_PRF_EXT_006U_HIST;
