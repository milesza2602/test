--------------------------------------------------------
--  DDL for Procedure WH_PRF_BRTH_030U_FOODX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_BRTH_030U_FOODX" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Sept 2010
--  Author:      Wendy Lyttle
--  Purpose:     BRIDGETHORN EXTRACT
--               Extract supplier_item data for :
--               last 4 weeks (this_week_start_date between today - 4 weeks and today)
--               sunday (fin_day_no = 7)
--               stores only (loc_type = 'S')
--               foods only (business_unit_no = 50)
--               any area except 'NON-CUST CORPORATE' (area_no <> 9978)

--  Tables:      Input  - rtl_supp_item_wk_bridgethorn
--                        dim_item
--                        dim_location
--                        dim_calendar
--               Output - rtl_supp_item_wk_bridgethorn
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 oct 2010 - added NO_MERGE statement and gather stats
--  9 dec 2010 - qc4190 - change to reflect all foods departments
--                        instead of certain foods departments (department_no in(12 ,15 ,16 ,
--                        22 ,23 ,32 ,34 ,37 ,40 ,41 ,42 ,43 ,44 ,45 ,53 ,59 ,66 ,73 ,87 ,
--                        88 ,93 ,95 ,97 ,99 )
-- 21 dec 2010 - qc4120 - only use order and delivered info from closed PO¿s
--                        use (amended (latest) order cases + cancelled cases)
--                        for ordered cases where the cancel code <> ¿B¿
  --
  -- 21 Jan 2011 - qc4120 - change to apply cancel code <> 'B' or
  --                        at record level
  --
--
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
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
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_recs_count         number        :=  0;
g_fin_day_no number :=0;
g_rec_out            rtl_supp_item_wk_bridgethorn%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_last_week_start_date date;
g_last_week_end_date date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_BRTH_030U_FOOD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'BRIDGETHORN EXTRACT OF SUPPLIER_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
g_sql            varchar2(8000);
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
    l_text := 'ROLLUP OF rtl_supp_item_wk_bridgethorn EX WEEK LEVEL STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     l_text := '---------------------------------------------------------------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    SELECT MIN(THIS_WEEK_START_DATE), MAX(THIS_WEEK_END_DATE) 
    INTO g_start_date,      g_end_date
    FROM DIM_CALENDAR 
    WHERE FIN_YEAR_NO = 2016;


   select min(calendar_date), max(calendar_date)
   into g_last_week_start_date ,g_last_week_end_date
   from dim_calendar
   where this_week_start_date = (select this_week_start_date from dim_calendar where calendar_date = g_end_date);

    l_text := 'LAST WEEK OF 4 WEEK PERIOD - '||g_last_week_start_date||' to '||g_last_week_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'START DATE OF ROLLUP - '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

  g_recs_count :=0;
  INSERT /*+ APPEND */ INTO dwh_DATAFIX.rtl14_supp_item_wk_bridgethorn    
WITH selsupp AS
                      (SELECT
                                /*+ no_merge full(rzia) parallel (rzia 4) */
                                rzia.sk1_item_no,
                                rzia.sk1_supplier_no,
                                MAX( (
                                CASE
                                  WHEN (THIS_WK_DAY_1_DAILY_PERC +THIS_WK_DAY_2_DAILY_PERC +THIS_WK_DAY_3_DAILY_PERC +THIS_WK_DAY_4_DAILY_PERC +THIS_WK_DAY_5_DAILY_PERC +THIS_WK_DAY_6_DAILY_PERC +THIS_WK_DAY_7_DAILY_PERC) > 0
                                  THEN 1
                                  ELSE 0
                                END ) ) current_supplier_ind
                                , b.this_week_start_date ,b.this_week_end_date
                      FROM dwh_performance.rtl_zone_item_supp_dy rzia,
                                        (SELECT
                                          /*+ full(rzi) parallel (rzi 4) */
                                          rzi.sk1_item_no ,
                                          rzi.sk1_supplier_no,
                                          MAX(rzi.calendar_date) maxdate,
                                          dc.this_week_start_date ,dc.this_week_end_date
                                        FROM dwh_performance.rtl_zone_item_supp_dy rzi,
                                          dwh_performance.dim_item di,
                                          dwh_performance.dim_calendar dc
                                        WHERE rzi.calendar_date BETWEEN dc.this_week_start_date AND dc.this_week_end_date
                                        AND di.sk1_item_no      = rzi.sk1_item_no
                                        AND di.business_unit_no = 50
--                                        AND dc.this_week_start_date BETWEEN G_START_DATE AND G_END_DATE       -- 2015                   2016
                                        --AND dc.this_week_start_date BETWEEN '30 JUNE 2014' AND '28 JUNE 2015'   --[30/JUN/14 	28/JUN/15] [29/JUN/15 26/JUN/16]
                                        AND dc.this_week_start_date BETWEEN '29 JUNE 2015' AND '26 JUNE 2016'
                                        GROUP BY rzi.sk1_item_no,
                                          rzi.sk1_supplier_no, dc.this_week_start_date ,dc.this_week_end_date
                                        ) b
                      WHERE rzia.sk1_item_no   = b.sk1_item_no
                      AND rzia.sk1_supplier_no = b.sk1_supplier_no
                      AND rzia.calendar_date   = b.maxdate
                      GROUP BY rzia.sk1_item_no,
                        rzia.sk1_supplier_no, b.this_week_start_date ,b.this_week_end_date
                      ) ,
  selsuppch AS
              (SELECT
                      /*+ no_merge full(rai) parallel (rai 4) */
                      dc.fin_year_no fin_year_no,
                      dc.fin_week_no fin_week_no,
                      dc.this_week_start_date ,
                      dc.this_week_end_date ,
                      di.item_no item_no,
                      ds.sk1_supplier_no sk1_supplier_no,
                      di.sk1_item_no sk1_item_no,
                      (DS.supplier_no
                      ||' '
                      ||DS.supplier_name ) supplier,
                      SUM( NVL(latest_po_cases,0)) latest_po_cases ,
                      SUM( NVL(po_grn_cases,0)) po_grn_cases ,
                      SUM(CAST ((
                      CASE
                        WHEN NVL(latest_po_cases,0) > 0
                        THEN NVL(po_grn_cases,0) / NVL(latest_po_cases,0)
                        ELSE NULL
                      END) AS NUMBER(30,15))) supplier_service_level ,
                      SUM( (
                      CASE
                        WHEN NVL(latest_po_cases,0) > 0
                        THEN 1
                        ELSE 0
                      END)) forcnt
              FROM dwh_performance.rtl_po_supchain_loc_item_dy rai
              LEFT OUTER JOIN dwh_performance.dim_item DI
                    ON di.sk1_item_no = rai.sk1_item_no
              LEFT OUTER JOIN dwh_performance.dim_supplier Ds
                   ON ds.sk1_supplier_no = rai.sk1_supplier_no
              LEFT OUTER JOIN dwh_performance.dim_calendar Dc
                   ON dc.calendar_date = rai.tran_date
              WHERE dc.this_week_start_date BETWEEN g_start_date AND g_end_date
                  AND di.business_unit_no = 50
                  AND RAI.PO_STATUS_CODE = 'C'
                  and (RAI.cancel_code <> 'B'or RAI.cancel_code is null)
              GROUP BY dc.fin_year_no ,
                        dc.fin_week_no ,
                        dc.this_week_start_date ,
                        dc.this_week_END_date ,
                        di.item_no ,
                        ds.sk1_supplier_no ,
                        di.sk1_item_no ,
                        (DS.supplier_no
                        ||' '
                        ||DS.supplier_name )
              )
SELECT DISTINCT
    fin_year_no fin_year_no,
  fin_week_no fin_week_no,
  SSC.this_week_start_date ,
  item_no item_no,
  supplier,
  NVL(latest_po_cases,0) latest_po_cases,
  NVL(po_grn_cases,0)  po_grn_cases,
  (CASE WHEN LATEST_PO_CASES > 0 THEN po_grn_cases / latest_po_cases ELSE 0 END) supplier_service_level,
  NVL(ss.current_supplier_ind,0) current_supplier_ind
FROM selsuppch ssc
LEFT OUTER JOIN selsupp ss
ON ss.sk1_item_no      = ssc.sk1_item_no
AND ss.sk1_supplier_no = ssc.sk1_supplier_no
and ss.this_week_start_date      = ssc.this_week_start_date
AND ss.this_week_end_date = ssc.this_week_end_date;

   g_recs_read     :=g_recs_count;
   g_recs_inserted :=g_recs_count;

commit;
    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
                                   'rtl14_supp_item_wk_bridgethorn', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS on dwh_DATAFIX.rtl14_supp_item_wk_bridgethorn';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text := '---------------------------------------------------------------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
 
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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


END WH_PRF_BRTH_030U_FOODX;
