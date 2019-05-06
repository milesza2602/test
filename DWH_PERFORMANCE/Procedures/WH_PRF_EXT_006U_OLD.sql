--------------------------------------------------------
--  DDL for Procedure WH_PRF_EXT_006U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_EXT_006U_OLD" 
 (p_forall_limit in integer,p_success out boolean) 
 as
 --**************************************************************************************************
-- same as history load of sales and stock  - wh_prf_ext_001u
-- to be treated as a MART - ie. we drop and reload and DO NOT keep a history
 --**************************************************************************************************
--  Date:        JULY 2015
--  Author:      Wendy Lyttle
--  Purpose:     Extract no.1 for accenture - july 2015
--               Extract that contains :
--               item = non-food, all group_no=3
--               loc = all stores for chain_no in(10,30)
--               cal = the last completed 6 weeks
--               rule= all records where the sales_qty > 0 or soh_qty > 0
--                     note that some weeks there might not be a sales record but a stock and vice versa
--                     hence the full outer join between these
--               NB.
--                 there are some negative values in stock, but they do not want these
--                 this does not make sense as eventhough it's usually a differenc of 1 or 2, 
--                 these items do not have large values
--                 ie. if value = 200 and need to subtract 2, then 198 - not such a huge impact 
--                      vs
--                  if value = 3 and need to subtract 2, then 1 - a huge impact 

--  Tables:      Input  -  
--               Output - RTL_LOC_ITEM_WK_ALOPT_SASTK
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
g_hospital_text      stg_rms_merch_phase_hsp.sys_process_msg%type;
g_rec_out            fnd_merch_phase%rowtype;
g_rec_in             stg_rms_merch_phase_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
g_start_date         date      ;
g_end_date           date      ;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_EXT_006U)OLD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'last 6 weeks sales and stock';
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

    l_text := 'ACCENTURE EXTRACT - 1 EX ODWH STARTED AT '||
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
    
--**************************************************************************************************
-- LOAD SALES
--**************************************************************************************************

    l_text := 'TRUNCATED table dwh_performance.RTL_LOC_ITEM_WK_ALOPT_SASTK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate ('truncate table dwh_performance.RTL_LOC_ITEM_WK_ALOPT_SASTK');
    commit;

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_LOC_ITEM_WK_ALOPT_SASTK', DEGREE => 8);
    commit;

    g_recs_read     := 0;
    g_recs_inserted := 0;

      INSERT /*+ APPEND */
      INTO DWH_PERFORMANCE.RTL_LOC_ITEM_WK_ALOPT_SASTK
      WITH 
        SELCAL AS (
                   SELECT  /*+ full(d) materialize */ DISTINCT FIN_YEAR_NO, FIN_WEEK_NO
                   FROM DIM_CALENDAR d
                   WHERE CALENDAR_DATE BETWEEN G_START_date AND G_END_DATE
                   ),
        selitem AS
                  (SELECT
                    /*+ full(d) materialize */
                          DISTINCT D.Sk1_Item_No,
                          D.Item_No
                  FROM Dim_Item D,
                        Dim_Ast_Lev1_Diff1 ast
                  WHERE d.sk1_style_colour_no = ast.sk1_style_colour_no
                  AND D.Business_Unit_No     <> 50
   --           AND D.Group_No              = 3
 -- CHANGE TO INCLUDE MORE GROUPS                     AND D.Group_No              IN(1,3,5)
                  ORDER BY ITEM_NO
                  ),
        selloc AS
                  (SELECT
                    /*+ materialize full(c) full(Dl) */
                    DISTINCT Dl.location_no,
                    dl.sk1_location_no
                  FROM Dim_Location Dl,
                    Dim_Country C
                  WHERE Dl.Loc_Type       = 'S'
                  AND chain_no           IN (10,30)
                  AND Dl.Sk1_Country_Code = C.Sk1_Country_Code
                  ORDER BY LOCATION_NO
                  ) ,
        selsa AS
                  (SELECT
                    /*+ full(ss) full(sa) materialize */
                          sA.fin_year_no,
                          sA.fin_week_no,
                          sL.Location_No Store_Id,
                          sI.Item_No Sku_Id,
                          sa.Sales_Qty Sa_Qty
                  FROM Rtl_Loc_Item_Wk_Rms_Dense sa
                  JOIN selloc sl
                  ON sl.Sk1_Location_No = sa.Sk1_Location_No
                  JOIN selitem si
                  ON si.Sk1_item_No = sa.Sk1_item_No
                  JOIN selcal sc
                  ON sc.fin_year_no = sa.fin_year_no
                  and sc.fin_week_no = sa.fin_week_no
                  WHERE sa.Sales_Qty > 0
                  ORDER BY FIN_YEAR_NO ,     FIN_WEEK_NO,    LOCATION_NO,   ITEM_NO
                  ),
        selST AS
                (SELECT
                  /*+ full(ss) full(sa) materialize */
                        sA.fin_year_no,
                        sA.fin_week_no,
                        sL.Location_No Store_Id,
                        sI.Item_No Sku_Id,
                        sa.SOH_Qty SOH_Qty
                FROM Rtl_Loc_item_Wk_Rms_Stock sa
                JOIN selloc sl
                ON sl.Sk1_Location_No = sa.Sk1_Location_No
                JOIN selitem si
                ON si.Sk1_item_No = sa.Sk1_item_No
                JOIN selcal sc
                ON sc.fin_year_no = sa.fin_year_no
                and sc.fin_week_no = sa.fin_week_no
                WHERE sa.SOH_Qty > 0
                  ORDER BY FIN_YEAR_NO ,     FIN_WEEK_NO,    LOCATION_NO,   ITEM_NO
                ),
        selmrg AS
                (SELECT
                  /*+ materialize PARALLEL(4) */
                          NVL(sa.Store_Id,St.Store_Id) store_id,
                          NVL(sa.Sku_Id,St.Sku_Id) Sku_Id,
                          NVL(sa.fin_year_No,St.fin_year_No) fin_year_no,
                          NVL(sa.fin_week_No,St.fin_week_No) fin_week_no,
                          NVL(sa_qty,0) sa_qty,
                          NVL(soh_qty,0) soh_qty
                FROM selsa sa
                FULL OUTER JOIN selst st
                ON St.Fin_Year_No  = sa.Fin_Year_No
                AND St.Fin_Week_No = sa.Fin_Week_No
                AND St.Sku_Id      = sa.Sku_Id
                AND St.Store_Id    = sa.Store_Id
                )
      SELECT Store_Id,
              Sku_Id,
              'W'
              || TO_CHAR(Fin_Year_No)
              || '_'
              || Lpad( TO_CHAR(Fin_Week_No), 2 , '0' ) Sales_Week,
              Sa_Qty,
              Soh_Qty
      FROM selmrg
      WHERE sa_qty > 0
      OR soh_qty   > 0;


   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

    commit;
    l_text := 'GATHER STATS on DWH_PERFORMANCE.RTL_LOC_ITEM_WK_ALOPT_SASTK';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'RTL_LOC_ITEM_WK_ALOPT_SASTK', DEGREE => 8);
    commit;
  
   l_text := 'recs written to RTL_LOC_ITEM_WK_ALOPT_SASTK = '|| g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;


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


END WH_PRF_EXT_006U_OLD;
