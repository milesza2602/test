--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_181U_WL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_181U_WL" (
    p_forall_limit in integer,
    p_success out boolean)
as


--**************************************************************************************************
--  Date:        August 2013
--  Author:      Wendy Lyttle
--  Purpose:     load sale sand shrink data based upon the stock cycle from the MIC interface
--  Tables:      Input  - FND_4F_LOC_BU_STOCK_CYCLE
--               Output - RTL_LOC_BU_STOCK_CYCLE
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
  g_cycle_no        number  := 0;
  g_sub           integer := 0;
  g_part_name     varchar2(30);
  g_fin_month_code dim_calendar.fin_month_code%type;
  g_LOCS number;

  g_rec_out RTL_LOC_BU_STOCK_CYCLE%rowtype;
  g_found boolean;
  g_date date := trunc(sysdate);
  g_start_date date ;
  g_date_6wks date ;
  g_fix_date date;
  g_end_date date ;
  g_period_start date ;
  g_period_end date ;
  g_yesterday date := trunc(sysdate) - 1;
  g_fin_day_no dim_calendar.fin_day_no%type;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_181U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD BU STOCK CYCLE';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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

  l_text := 'LOAD BU STOCK CYCLE started at '|| to_char(sysdate,('dd Mon Yyyy Hh24:Mi:Ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

  --**************************************************************************************************
  -- Look Up Batch Date From Dim_Control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);

  l_text := 'Batch date being processed is :- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  select sysdate - 493
  into g_fix_date
  from dual;
  
    SELECT MIN(FN.CYCLE_FROM_DATE) , MAX(FN.CYCLE_TO_DATE), count(distinct fn.LOCATION_no)
    into g_START_DATE, g_END_DATE, g_locs
            FROM dwh_foundation.FND_4F_LOC_BU_STOCK_CYCLE fn, dim_location dl
            where area_no = 9951
            and fn.location_no = dl.location_no
        --    and cycle_no in(24,25)
     ;
    g_date_6wks := g_date - 72;

  --**************************************************************************************************
  -- Truncate TEMP_LOC_BU_STOCK_CYCLE
  --**************************************************************************************************
  l_text := 'Truncate table DWH_PERFORMANCE.TEMP_LOC_BU_STOCK_CYCLE';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  execute immediate 'TRUNCATE TABLE DWH_PERFORMANCE.TEMP_LOC_BU_STOCK_CYCLE';
  commit;

  --**************************************************************************************************
  --Gather Stats On TEMP_LOC_BU_STOCK_CYCLE
  --**************************************************************************************************
  l_text := 'Gather stats on DWH_PERFORMANCE.TEMP_LOC_BU_STOCK_CYCLE';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dbms_stats.gather_table_stats ('DWH_PERFORMANCE', 'TEMP_LOC_BU_STOCK_CYCLE', degree => 8);
  commit;


--**** setup rtl table with parttition on cycle_no.
  --**************************************************************************************************
  -- Insert into TEMP_LOC_BU_STOCK_CYCLE
  --**************************************************************************************************
        INSERT INTO dwh_performance.TEMP_LOC_BU_STOCK_CYCLE
      WITH SELLOC AS
                      ( SELECT DISTINCT LOCATION_NO FROM dwh_foundation.FND_4F_LOC_BU_STOCK_CYCLE
                 --       where cycle_no in(24,25)
                                            ) ,
        SELrnk AS
                      (SELECT LOCATION_NO,                        RNK
                      FROM
                            (SELECT LOCATION_NO ,
                              ROW_NUMBER() over (order by LOCATION_NO) rnk
                            FROM selloc))

      SELECT
                /*+ PARALLEL(FND,4) parallel(di,4) */
                sk1_business_unit_no,
                cycle_no,
                dl.SK1_LOCATION_NO,
                DL.LOCATION_NO,
                bI.BUSINESS_UNIT_NO,
                cycle_FROM_DATE,
                cycle_TO_DATE,
                fnd.calendar_date,
                nvl(dc.fin_year_no,0),
                SR.RNK
      FROM dwh_foundation.FND_4F_LOC_BU_STOCK_CYCLE fnd
      JOIN dwh_performance.dim_business_unit bi
           ON bi.business_unit_NO = fnd.business_unit_NO
      JOIN dwh_performance.dim_location dl
            ON DL.LOCATION_NO = FND.LOCATION_NO
      JOIN SELrnk SR
           ON SR.LOCATION_NO = DL.LOCATION_NO
                join dim_calendar dc
           on dc.calendar_date = fnd.cycle_TO_DATE
      WHERE area_no = 9951
  -- REMOVED WL 14 MAR 2014 - filter to limiting, must redo whole cycle if there is a change
       and fnd.last_updated_date between G_FIX_DATE and  g_date 
      GROUP BY sk1_business_unit_no,                cycle_no,                dl.SK1_LOCATION_NO,
                DL.LOCATION_NO,                bI.BUSINESS_UNIT_NO,                cycle_FROM_DATE,
                cycle_TO_DATE,                fnd.calendar_date,                nvl(dc.fin_year_no,0),
                SR.RNK
      ORDER BY
                SR.RNK ,                DL.LOCATION_NO,                BUSINESS_UNIT_NO,
                cycle_FROM_DATE,                cycle_TO_DATE,                fnd.calendar_date ;

  g_recs_inserted := g_recs_inserted + sql%rowcount;

  l_text := 'Insert into TEMP:- RECS =  '||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       commit;
  g_recs_inserted := 0;



  --**************************************************************************************************
  --Gather Stats On RTL_LOC_BU_STOCK_CYCLE
  --**************************************************************************************************
  l_text := 'Gather stats on DWH_PERFORMANCE.RTL_LOC_BU_STOCK_CYCLE';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dbms_stats.gather_table_stats ('DWH_PERFORMANCE', 'RTL_LOC_BU_STOCK_CYCLE', degree => 8);
  commit;
  --**************************************************************************************************
  --MAIN
  --**************************************************************************************************
  execute immediate 'alter session enable parallel dml';
--  g_locs := g_locs - 1;
--  g_locs := 2;
 FOR g_sub IN 0..g_locs
  LOOP
            g_recs_inserted := 0;




            l_text       := 'g_locs='||g_locs||' g_sub='||g_sub;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



merge
/*+ parallel(rtl,4) append */
INTO dwh_performance.RTL_LOC_BU_STOCK_CYCLE rtl USING
( WITH seldat AS
              (SELECT
                /*+ parallel(tmp,2) */
                SK1_BUSINESS_UNIT_NO ,    CYCLE_NO ,    SK1_LOCATION_NO ,    LOCATION_NO ,    BUSINESS_UNIT_NO ,
                CYCLE_FROM_DATE ,    CYCLE_TO_DATE ,    CALENDAR_DATE ,    FIN_YEAR_NO
              FROM DWH_PERFORMANCE.TEMP_LOC_BU_STOCK_CYCLE tmp
              WHERE loc_rank = g_locs
              ),
      seldns AS
              (SELECT
                /*+ parallel(dns,2) parallel(ddi,2) */
                dsd.sk1_business_unit_no,
                dsd.sk1_location_no,
                cycle_no,
                cycle_from_date,
                -- wl 22 june 2014 change from cycle_to_date to  max(cycle_to_date) to stop duplicates 
                -- issue to be resolved in write from staging to fnd
                max(cycle_to_date) cycle_to_date,
                SUM(NVL(sales_qty,0)) sales_qty,
                SUM(NVL(dns.sales,0)) sales,
                SUM(NVL(dns.sales_cost,0)) sales_cost,
                SUM(NVL(dns.shrinkage_qty,0)) shrinkage_qty,
                SUM(NVL(dns.shrinkage_selling,0)) shrinkage_selling,
                SUM(NVL(dns.shrinkage_cost,0)) shrinkage_cost,
                fin_year_no
              FROM seldat dsd
              JOIN dwh_performance.rtl_loc_bu_dy_shrinkage dns
              ON  dns.sk1_location_no      = dsd.sk1_location_no
              AND dns.sk1_business_unit_no = dsd.sk1_business_unit_no
              AND dns.post_date            = dsd.calendar_date
              GROUP BY dsd.sk1_business_unit_no,
                dsd.sk1_location_no,
                cycle_no,
                cycle_from_date,
               -- cycle_to_date,
                fin_year_no
              )

SELECT    ds.sk1_location_no,
          ds.sk1_business_unit_no,
          ds.cycle_no,
          ds.cycle_from_date,
          ds.cycle_to_date,
          sales_qty,
          sales,
          sales_cost,
          shrinkage_qty,
          shrinkage_selling,
          shrinkage_cost,
          g_date last_updated_date,
          ds.fin_year_no
FROM seldns ds
) f
ON (rtl.sk1_location_no = f.sk1_location_no
AND rtl.sk1_business_unit_no = f.sk1_business_unit_no
AND rtl.cycle_no = f.cycle_no)
WHEN matched THEN
  UPDATE
  SET sales_qty       = f.sales_qty ,
    sales             = f.sales ,
    sales_cost        = f.sales_cost ,
    shrinkage_qty     = f.shrinkage_qty ,
    shrinkage_selling = f.shrinkage_selling ,
    shrinkage_cost    = f.shrinkage_cost ,
    last_updated_date = f.last_updated_date
WHEN NOT matched THEN
  INSERT
    (
      rtl.SK1_LOCATION_NO ,      rtl.SK1_BUSINESS_UNIT_NO ,      rtl.CYCLE_NO ,      rtl.CYCLE_FROM_DATE ,
      rtl.CYCLE_TO_DATE ,      rtl.SALES_QTY ,      rtl.SALES ,      rtl.SALES_COST ,      rtl.shrinkage_QTY ,
      rtl.shrinkage_SELLING ,      rtl.shrinkage_COST ,      rtl.LAST_UPDATED_DATE ,      rtl.FIN_YEAR_NO
    )
    VALUES
    (
      f.SK1_LOCATION_NO ,      f.SK1_BUSINESS_UNIT_NO ,      f.CYCLE_NO ,      f.CYCLE_FROM_DATE ,
      f.CYCLE_TO_DATE ,      f.SALES_QTY ,      f.SALES ,      f.SALES_COST ,      f.shrinkage_QTY ,
      f.shrinkage_SELLING ,      f.shrinkage_COST ,      f.LAST_UPDATED_DATE ,      f.FIN_YEAR_NO
    );



            g_recs_read := g_recs_read + sql%rowcount;
            g_recs_inserted := g_recs_inserted + sql%rowcount;


            commit;
            g_locs := g_locs - 1;
    end loop;




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

END WH_PRF_CORP_181U_WL;
