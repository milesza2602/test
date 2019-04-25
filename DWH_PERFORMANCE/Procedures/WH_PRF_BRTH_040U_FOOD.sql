--------------------------------------------------------
--  DDL for Procedure WH_PRF_BRTH_040U_FOOD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_BRTH_040U_FOOD" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Special History Extract for foods - year 2013 and 2014
--  22 Dec 2014 - Wendy Lyttle
--**************************************************************************************************
--  Date:        Sept 2010
--  Author:      Wendy Lyttle
--  Purpose:     BRIDGETHORN EXTRACT
--               Extract depot_item data for :
--               last 4 weeks (this_week_start_date between today - 4 weeks and today)
--               sunday (fin_day_no = 7)
--               stores only (loc_type = 'S')
--               foods only (business_unit_no = 50)
--               any area except 'NON-CUST CORPORATE' (area_no <> 9978)

--  Tables:      Input  - rtl14_depot_item_dy
--                        dim_item
--                        dim_location
--                        dim_calendar
--               Output - rtl14_depotitemwk_bridgethorn
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 oct 2010 - added NO_MERGE statement and gather stats
--  9 dec 2010 - qc4190 - change to reflect all foods departments
--                        instead of certain foods departments (department_no in(12 ,15 ,16 ,
--                        22 ,23 ,32 ,34 ,37 ,40 ,41 ,42 ,43 ,44 ,45 ,53 ,59 ,66 ,73 ,87 ,
--                        88 ,93 ,95 ,97 ,99 )
--  10 dec 2010 - qc4190 - change to cater for Long-life departments only.
--
--
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';

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
g_recs_count         integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_fin_day_no number :=0;
--g_rec_out            rtl14_depotitemwk_bridgethorn%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_BRTH_040U_FOOD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'BRIDGETHORN EXTRACT OF DEPOT_ITEM';
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
    l_text := 'ROLLUP OF rtl14_depotitemwk_bridgethorn EX WEEK LEVEL STARTED '||
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



    l_text := 'START DATE OF ROLLUP - '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate ('truncate table dwh_datafix.rtl14_depotitemwk_bridgethorn');
    commit;
    l_text := 'TRUNCATED table dwh_datafix.rtl14_depotitemwk_bridgethorn';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
--                                   'rtl14_depotitemwk_bridgethorn', DEGREE => 8);
--    commit;
--    l_text := 'GATHER STATS on dwh_datafix.rtl14_depotitemwk_bridgethorn';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

  g_recs_count :=0;
  
  INSERT
    /*+ APPEND */   INTO dwh_datafix.rtl14_depotitemwk_bridgethorn
         WITH selcat AS
          (SELECT
                     /*+ no_merge full(rzi) parallel (rzi       4) */
                     dc.fin_year_no ,
            dc.fin_week_no ,
            di.item_no ,
            dL.wh_fd_zone_no,
            MAX(rzi.num_units_per_tray) num_units_per_tray
          FROM dwh_performance.dim_item DI,
            dwh_performance.dim_location DL,
            dwh_performance.dim_calendar Dc,
            dwh_performance.rtl_loc_item_wk_catalog rzi
          WHERE dc.this_week_start_date BETWEEN g_start_date and g_end_date
          AND dc.fin_day_no       = 7
          AND rzi.sk1_location_no = dl.sk1_location_no
          AND dc.fin_year_no      = rzi.fin_year_no
          AND dc.fin_week_no      = rzi.fin_week_no
          AND di.sk1_item_no      = rzi.sk1_item_no
          AND dl.area_no          = 9951
          AND di.business_unit_no = 50
--           AND di.department_no   IN(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99)
          --AND fd_discipline_type in ('SA', 'SF')
          and di.primary_supplier_no  in (9548)  --(24785, 25357)  --(59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110)  --(50885)  --, 14314)
         GROUP BY dc.fin_year_no, dc.fin_week_no, di.item_no, dL.wh_fd_zone_no
          )
                     SELECT XX.fin_year_no FIN_YEAR_NO,
          XX.fin_week_no FIN_WEEK_NO ,
          XX.this_week_start_date THIS_WEEK_START_DATE,
          XX.item_no  ITEM_NO ,
       SUM(XX.dc_stock_units) DC_STOCK_UNITS,
       SUM(XX.dc_stock_cases) DC_STOCK_CASES,
       SUM(XX.stock_selling) STOCK_SELLING
     FROM
        (   SELECT
        /*+ full(rai) parallel (rai      4) */
        dc.fin_year_no ,dc.fin_week_no ,
          dc.this_week_start_date ,
          di.item_no ,
          DL.wh_fd_zone_no,
       SUM((NVL(stock_cases,0) + NVL(outstore_cases,0)) * sc.num_units_per_tray) dc_stock_units ,
         SUM(NVL(stock_cases,0)  + NVL(outstore_cases,0)) dc_stock_cases ,
        SUM(NVL(stock_selling,0)) stock_selling
        FROM dwh_performance.rtl_depot_ITEM_DY rai,
          dwh_performance.dim_item DI,
          dwh_performance.dim_location DL,
          dwh_performance.dim_calendar Dc,
          selcat sc
        WHERE rai.post_date BETWEEN g_start_date and g_end_date
        AND rai.post_date       = dc.calendar_date
        AND dc.fin_day_no       = 7
        AND rai.sk1_item_no     = di.sk1_item_no
        AND rai.sk1_location_no = dl.sk1_location_no
        AND sc.item_no          = di.item_no
        AND sc.fin_year_no      = dc.fin_year_no
        AND sc.fin_week_no      = dc.fin_week_no
        AND SC.wh_fd_zone_no = DL.wh_fd_zone_no
        AND di.business_unit_no = 50
--        AND di.department_no   IN(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99)
        --AND fd_discipline_type in ('SA', 'SF')
        and di.primary_supplier_no  in (9548)  --(24785, 25357)  --(59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110)  --(50885)  --, 14314)
        GROUP BY dc.fin_year_no ,dc.fin_week_no ,
          dc.this_week_start_date ,
          di.item_no ,
          DL.wh_fd_zone_no) XX
        group by XX.fin_year_no, XX.fin_week_no, XX.this_week_start_date, XX.item_no
 ;

   g_recs_read     :=g_recs_count;
   g_recs_inserted :=g_recs_count;

commit;
    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
                                   'rtl14_depotitemwk_bridgethorn', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS on dwh_datafix.rtl14_depotitemwk_bridgethorn';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := '---------------------------------------------------------------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    SELECT MIN(THIS_WEEK_START_DATE), MAX(THIS_WEEK_END_DATE) 
    INTO g_start_date,      g_end_date
    FROM DIM_CALENDAR 
    WHERE FIN_YEAR_NO = 2017;


    l_text := 'START DATE OF ROLLUP - '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate ('truncate table dwh_datafix.rtl13_depotitemwk_bridgethorn');
    commit;
    l_text := 'TRUNCATED table dwh_datafix.rtl13_depotitemwk_bridgethorn';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
--                                   'rtl13_depotitemwk_bridgethorn', DEGREE => 8);
--    commit;
--    l_text := 'GATHER STATS on dwh_datafix.rtl13_depotitemwk_bridgethorn';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

  g_recs_count :=0;
  
  INSERT
    /*+ APPEND */   INTO dwh_datafix.rtl13_depotitemwk_bridgethorn
         WITH selcat AS
          (SELECT
                     /*+ no_merge full(rzi) parallel (rzi       4) */
                     dc.fin_year_no ,
            dc.fin_week_no ,
            di.item_no ,
            dL.wh_fd_zone_no,
            MAX(rzi.num_units_per_tray) num_units_per_tray
          FROM dwh_performance.dim_item DI,
            dwh_performance.dim_location DL,
            dwh_performance.dim_calendar Dc,
            dwh_performance.rtl_loc_item_wk_catalog rzi
          WHERE dc.this_week_start_date BETWEEN g_start_date and g_end_date
          AND dc.fin_day_no       = 7
          AND rzi.sk1_location_no = dl.sk1_location_no
          AND dc.fin_year_no      = rzi.fin_year_no
          AND dc.fin_week_no      = rzi.fin_week_no
          AND di.sk1_item_no      = rzi.sk1_item_no
          AND dl.area_no          = 9951
          AND di.business_unit_no = 50
--           AND di.department_no   IN(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99)
          --AND fd_discipline_type in ('SA', 'SF')
          and di.primary_supplier_no  in (9548)  --(24785, 25357)  --(59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110)  --(50885)  --, 14314)
         GROUP BY dc.fin_year_no, dc.fin_week_no, di.item_no, dL.wh_fd_zone_no
          )
                     SELECT XX.fin_year_no FIN_YEAR_NO,
          XX.fin_week_no FIN_WEEK_NO ,
          XX.this_week_start_date THIS_WEEK_START_DATE,
          XX.item_no  ITEM_NO ,
       SUM(XX.dc_stock_units) DC_STOCK_UNITS,
       SUM(XX.dc_stock_cases) DC_STOCK_CASES,
       SUM(XX.stock_selling) STOCK_SELLING
     FROM
        (   SELECT
        /*+ full(rai) parallel (rai      4) */
        dc.fin_year_no ,dc.fin_week_no ,
          dc.this_week_start_date ,
          di.item_no ,
          DL.wh_fd_zone_no,
       SUM((NVL(stock_cases,0) + NVL(outstore_cases,0)) * sc.num_units_per_tray) dc_stock_units ,
         SUM(NVL(stock_cases,0)  + NVL(outstore_cases,0)) dc_stock_cases ,
        SUM(NVL(stock_selling,0)) stock_selling
        FROM dwh_performance.rtl_depot_ITEM_DY rai,
          dwh_performance.dim_item DI,
          dwh_performance.dim_location DL,
          dwh_performance.dim_calendar Dc,
          selcat sc
        WHERE rai.post_date BETWEEN g_start_date and g_end_date
        AND rai.post_date       = dc.calendar_date
        AND dc.fin_day_no       = 7
        AND rai.sk1_item_no     = di.sk1_item_no
        AND rai.sk1_location_no = dl.sk1_location_no
        AND sc.item_no          = di.item_no
        AND sc.fin_year_no      = dc.fin_year_no
        AND sc.fin_week_no      = dc.fin_week_no
        AND SC.wh_fd_zone_no = DL.wh_fd_zone_no
        AND di.business_unit_no = 50
--        AND di.department_no   IN(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99)
        --AND fd_discipline_type in ('SA', 'SF')
        and di.primary_supplier_no  in (9548)  --(24785, 25357)  --(59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110)  --(50885)  --, 14314)
        GROUP BY dc.fin_year_no ,dc.fin_week_no ,
          dc.this_week_start_date ,
          di.item_no ,
          DL.wh_fd_zone_no) XX
        group by XX.fin_year_no, XX.fin_week_no, XX.this_week_start_date, XX.item_no
 ;

   g_recs_read     :=g_recs_count;
   g_recs_inserted :=g_recs_count;

commit;
    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
                                   'rtl13_depotitemwk_bridgethorn', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS on dwh_datafix.rtl13_depotitemwk_bridgethorn';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    l_text := '---------------------------------------------------------------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    SELECT MIN(THIS_WEEK_START_DATE), MAX(THIS_WEEK_END_DATE) 
    INTO g_start_date,      g_end_date
    FROM DIM_CALENDAR 
    WHERE FIN_YEAR_NO = 2018;


    l_text := 'START DATE OF ROLLUP - '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate ('truncate table dwh_datafix.rtl18_depotitemwk_bridgethorn');
    commit;
    l_text := 'TRUNCATED table dwh_datafix.rtl18_depotitemwk_bridgethorn';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
--    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
--                                   'rtl13_depotitemwk_bridgethorn', DEGREE => 8);
--    commit;
--    l_text := 'GATHER STATS on dwh_datafix.rtl13_depotitemwk_bridgethorn';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

  g_recs_count :=0;
  
  INSERT
    /*+ APPEND */   INTO dwh_datafix.rtl18_depotitemwk_bridgethorn
         WITH selcat AS
          (SELECT
                     /*+ no_merge full(rzi) parallel (rzi       4) */
                     dc.fin_year_no ,
            dc.fin_week_no ,
            di.item_no ,
            dL.wh_fd_zone_no,
            MAX(rzi.num_units_per_tray) num_units_per_tray
          FROM dwh_performance.dim_item DI,
            dwh_performance.dim_location DL,
            dwh_performance.dim_calendar Dc,
            dwh_performance.rtl_loc_item_wk_catalog rzi
          WHERE dc.this_week_start_date BETWEEN g_start_date and g_end_date
          AND dc.fin_day_no       = 7
          AND rzi.sk1_location_no = dl.sk1_location_no
          AND dc.fin_year_no      = rzi.fin_year_no
          AND dc.fin_week_no      = rzi.fin_week_no
          AND di.sk1_item_no      = rzi.sk1_item_no
          AND dl.area_no          = 9951
          AND di.business_unit_no = 50
--           AND di.department_no   IN(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99)
          --AND fd_discipline_type in ('SA', 'SF')
          --and di.primary_supplier_no = 50885-- in (59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110)  --(50885)  --, 14314)
          and di.primary_supplier_no in (9548)  --(50885, 59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110, 14314, 24785, 25357)
         GROUP BY dc.fin_year_no, dc.fin_week_no, di.item_no, dL.wh_fd_zone_no
          )
                     SELECT XX.fin_year_no FIN_YEAR_NO,
          XX.fin_week_no FIN_WEEK_NO ,
          XX.this_week_start_date THIS_WEEK_START_DATE,
          XX.item_no  ITEM_NO ,
       SUM(XX.dc_stock_units) DC_STOCK_UNITS,
       SUM(XX.dc_stock_cases) DC_STOCK_CASES,
       SUM(XX.stock_selling) STOCK_SELLING
     FROM
        (   SELECT
        /*+ full(rai) parallel (rai      4) */
        dc.fin_year_no ,dc.fin_week_no ,
          dc.this_week_start_date ,
          di.item_no ,
          DL.wh_fd_zone_no,
       SUM((NVL(stock_cases,0) + NVL(outstore_cases,0)) * sc.num_units_per_tray) dc_stock_units ,
         SUM(NVL(stock_cases,0)  + NVL(outstore_cases,0)) dc_stock_cases ,
        SUM(NVL(stock_selling,0)) stock_selling
        FROM dwh_performance.rtl_depot_ITEM_DY rai,
          dwh_performance.dim_item DI,
          dwh_performance.dim_location DL,
          dwh_performance.dim_calendar Dc,
          selcat sc
        WHERE rai.post_date BETWEEN g_start_date and g_end_date
        AND rai.post_date       = dc.calendar_date
        AND dc.fin_day_no       = 7
        AND rai.sk1_item_no     = di.sk1_item_no
        AND rai.sk1_location_no = dl.sk1_location_no
        AND sc.item_no          = di.item_no
        AND sc.fin_year_no      = dc.fin_year_no
        AND sc.fin_week_no      = dc.fin_week_no
        AND SC.wh_fd_zone_no = DL.wh_fd_zone_no
        AND di.business_unit_no = 50
--        AND di.department_no   IN(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99)
        --AND fd_discipline_type in ('SA', 'SF')
        --and di.primary_supplier_no = 50885-- in (59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110)  --(50885)  --, 14314)
        and di.primary_supplier_no in (9548)  --(50885, 59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110, 14314, 24785, 25357)
        GROUP BY dc.fin_year_no ,dc.fin_week_no ,
          dc.this_week_start_date ,
          di.item_no ,
          DL.wh_fd_zone_no) XX
        group by XX.fin_year_no, XX.fin_week_no, XX.this_week_start_date, XX.item_no
 ;

   g_recs_read     :=g_recs_count;
   g_recs_inserted :=g_recs_count;

commit;
    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
                                   'rtl18_depotitemwk_bridgethorn', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS on dwh_datafix.rtl18_depotitemwk_bridgethorn';
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

END WH_PRF_BRTH_040U_FOOD;
