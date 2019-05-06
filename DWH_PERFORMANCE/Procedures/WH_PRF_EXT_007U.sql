--------------------------------------------------------
--  DDL for Procedure WH_PRF_EXT_007U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_EXT_007U" 

 (p_forall_limit in integer,p_success out boolean) 
 as
--**************************************************************************************************
--  Date:        JULY 2015
--  Author:      RAPHAEL RICKETTS
--  Purpose:     ALLOCATION OPTIMIZATION PROJECT  --  sales extract 6 weeks - july 2015
-- *********************************
-- SALES SQL BASE FOR 6 WEEKS BACK *
-- VERSION 3: USING CC AS MASTER   *
-- AND USING WK_DENSE              *
-- AS AT 17 JULY 2015              *
-- WITH SALES_LY                   *
-- SHEETAL'S VERSION               *
-- *********************************
--  Tables:      Input  - RTL_LOC_ITEM_WK_RMS_DENSE
--               Output - RTL_LOC_ITEM_WK_ALOPT_TYLYSAL
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

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_EXT_007U';
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

    execute immediate 'alter session enable parallel dml';

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

    l_text := 'Period BEING PROCESSED = '||g_start_date|| ' to '|| g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    


-----------------------------------------------------------------------
-- Create list
-----------------------------------------------------------------------
    l_text := 'TRUNCATED table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_TYSAL';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_TYSAL');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_TYSAL', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;
    
     INSERT /*+ APPEND */
      into DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_TYSAL
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
                            from dim_item di, Dim_Ast_Lev1_Diff1 AST
                            where di.sk1_style_colour_no = ast.sk1_style_colour_no  --- IS THIS NEEDED???
                                and di.business_unit_no   <> 50
                          --      and di.group_no           in(3)
                                and di.style_colour_no    <> 0
                            order by item_no
                            ),
                selloc as
                            (select
                              /*+ materialize full(c) full(DL) */
                                    distinct dl.location_no,
                                    dl.sk1_location_no
                            from dim_location dl,
                                  dim_country c
                            where dl.loc_type                  = 'S'
                                and chain_no                      in (10,30)
                                and dl.sk1_country_code            = c.sk1_country_code  --- IS THIS NEEDED???
                                and dl.st_shape_of_the_chain_code != 'FSA' 
                            order by location_no
                            )
                SELECT
                                    /*+ PARALLEL(R,4) no_index (R) */
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
                                  AND NVL(Sales_Qty,0) > 0
;
                      
   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_TYSAL';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_TYSAL', DEGREE => 8);
    commit;
  
    l_text := 'recs written to TMP_LOC_ITEM_WK_ALOPT_TYSAL = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;

-----------------------------------------------------------------------
-- Create list
-----------------------------------------------------------------------
    l_text := 'TRUNCATED table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_LYSAL';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.TMP_LOC_ITEM_WK_ALOPT_LYSAL');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_LYSAL', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;
    
     INSERT /*+ APPEND */
      into DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_LYSAL
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
                            from dim_item di, Dim_Ast_Lev1_Diff1 AST
                            where di.sk1_style_colour_no = ast.sk1_style_colour_no  --- IS THIS NEEDED???
                                and di.business_unit_no   <> 50
                          --      and di.group_no           in(3)
                                and di.style_colour_no    <> 0
                            order by item_no
                            ),
                selloc as
                            (select
                              /*+ materialize full(c) full(DL) */
                                    distinct dl.location_no,
                                    dl.sk1_location_no
                            from dim_location dl,
                                  dim_country c
                            where dl.loc_type                  = 'S'
                                and chain_no                      in (10,30)
                                and dl.sk1_country_code            = c.sk1_country_code  --- IS THIS NEEDED???
                                and dl.st_shape_of_the_chain_code != 'FSA' 
                            order by location_no
                            )
                SELECT
                                    /*+ PARALLEL(R,4) no_index (R) */
                                          SC.Fin_Year_No,
                                          SC.Fin_Week_No,
                                          SL.Location_No,
                                          SI.Item_No,
                                          NVL(Sales_Qty,0) Sales_Qty
                                  FROM SELLOC SL,
                                        SELITM SI,
                                        SELCAL SC,
                                        Rtl_Loc_Item_wk_Rms_Dense R
                                  WHERE SC.LY_Fin_Year_No   = R.Fin_Year_No
                                  AND SC.LY_fin_week_no     = r.fin_week_no
                                  AND SL.Sk1_Location_No = R.Sk1_Location_No
                                  AND SI.Sk1_Item_No     = R.Sk1_Item_No
                                  AND NVL(Sales_Qty,0) > 0
;
                      
   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_LYSAL';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'TMP_LOC_ITEM_WK_ALOPT_LYSAL', DEGREE => 8);
    commit;
  
    l_text := 'recs written to TMP_LOC_ITEM_WK_ALOPT_LYSAL = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;



-----------------------------------------------------------------------
--
-----------------------------------------------------------------------                       

    l_text := 'TRUNCATED table dwh_performance.RTL_LOC_ITEM_WK_ALOPT_TYLYSAL';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.RTL_LOC_ITEM_WK_ALOPT_TYLYSAL');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_LOC_ITEM_WK_ALOPT_TYLYSAL', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;


      INSERT /*+ APPEND */
      into DWH_PERFORMANCE.RTL_LOC_ITEM_WK_ALOPT_TYLYSAL
      WITH SELALL AS (      SELECT
                   /*+ MATERIALIZE FULL(LY) PARALLEL(LY,4)   FULL(TY) PARALLEL(TY,4)  */
                    NVL(Ty.Location_No,LY.LOCATION_NO) LOCATION_NO,
                    NVL(Ty.Item_No,Ly.Item_No)         Item_No,
                    NVL(Ty.Fin_Year_No,Ly.Fin_Year_No) Fin_Year_No,
                    NVL(Ty.Fin_week_No,Ly.Fin_week_No) Fin_week_No,
                    nvl(Ty.Sales_Qty,0)                Sa_Qty,
                    nvl(Ly.Sales_Qty,0)                Sa_Qty_Ly
            FROM DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_LYSAL LY
            FULL OUTER JOIN  DWH_PERFORMANCE.TMP_LOC_ITEM_WK_ALOPT_TYSAL TY
            ON Ty.Fin_Year_No = Ly.Fin_Year_No
            AND Ty.Fin_Week_No   = Ly.Fin_Week_No
            AND Ty.Location_No   = Ly.Location_No
            And Ty.Item_No       = Ly.Item_No)
            SELECT Location_No,
                   Item_No,
                    'W'
                    || TO_CHAR(Fin_Year_No)
                    || '_'
                    || Lpad( TO_CHAR(Fin_Week_No), 2 , '0' ) Sales_Week,
                    Sa_Qty,
                    Sa_Qty_Ly
          FROM SELALL WHERE SA_Qty > 0 or Sa_Qty_Ly > 0;


   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.RTL_LOC_ITEM_WK_ALOPT_TYLYSAL';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_LOC_ITEM_WK_ALOPT_TYLYSAL', DEGREE => 8);
    commit;
  
   l_text := 'recs written to RTL_LOC_ITEM_WK_ALOPT_TYLYSAL = '|| g_recs_inserted;
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

END WH_PRF_EXT_007U;
