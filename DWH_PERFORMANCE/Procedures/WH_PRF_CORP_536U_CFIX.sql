--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_536U_CFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_536U_CFIX" (
    p_forall_limit in integer,
    p_success out boolean)
as
  --**************************************************************************************************
  --  Date:        Apr 2010
  --  Author:      Wendy Lyttle
  --  Purpose:     Create Liwk Catalog Rollup Fact Table In The Performance Layer
  --               With Input Ex Lid Catalog Table From Performance Layer.
  --  Tables:      Input  - Rtl_Loc_Item_Dy_Catalog
  --               Output - RTL_LOC_ITEM_WK_CATALOG
  --  Packages:    Constants, Dwh_Log, Dwh_Valid
  --
  --
  --  Maintenance:
  --  20 Mar 2009 - Replaced insert/update with merge statement for better performance -TC
  --  06 OCT 2009 - Replaced Merge with Insert into select from with a generic partition truncate prior to run.
  --  04 DEC 2009 - Add 8 avg_boh_adj_ columns to RTL_LOC_ITEM_WK_CATALOG
  --  April 2010  - Change calculation of values and hence create of temp-table
  --  June 2011   - New measure added - min_shelf_life - QC2981
  --  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
  --
  --  Naming Conventions
  --  G_  -  Global Variable
  --  L_  -  Log Table Variable
  --  A_  -  Array Variable
  --  V_  -  Local Variable As Found In Packages
  --  P_  -  Parameter
  --  C_  -  Prefix To Cursor
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
  g_sub           integer := 0;
  g_part_name     varchar2(30);
  g_fin_month_code dim_calendar.fin_month_code%type;

  g_rec_out DWH_PERFORMANCE.RTL_LOC_ITEM_WK_CATALOG_N%rowtype;
  g_found boolean;
  g_date date := trunc(sysdate);
  g_start_date date ;
  g_end_date date ;
  g_yesterday date := trunc(sysdate) - 1;
  g_fin_day_no dim_calendar.fin_day_no%type;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_536U_CFIX';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_facts;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_facts;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'ROLL UP THE CATALOG PERFORMANCE TO WEEK';
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

  l_text := 'Rollup of AJ_RTL_LOC_ITEM_WK_CATALOG ex day level started at '|| to_char(sysdate,('dd Mon Yyyy Hh24:Mi:Ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

  --**************************************************************************************************
  -- Look Up Batch Date From Dim_Control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  g_date := '08/FEB/18';

  l_text := 'Batch date being processed is :- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  execute immediate 'alter session enable parallel dml';

 --**************************************************************************************************
  --Gather Stats On RTL_LOC_ITEM_WK_CATALOG
  --**************************************************************************************************
  SELECT FIN_MONTH_CODE
  INTO   G_FIN_MONTH_CODE
  FROM   DIM_CALENDAR
  where  calendar_date = g_date;
/*
  g_part_name := 'TP_RTLLIDYCAT_'||g_fin_month_code;
  l_text      := 'Gather stats on DWH_PERFORMANCE.RTL_LOC_ITEM_DY_CATALOG  '||g_part_name;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dbms_stats.gather_table_stats ('DWH_PERFORMANCE','RTL_LOC_ITEM_DY_CATALOG',g_part_name,granularity => 'PARTITION',DEGREE => 16);
  commit;
*/  
 --  execute immediate 'alter session enable parallel dml';

 FOR g_sub IN 0..1
  LOOP
    g_recs_inserted := 0;
    SELECT
      this_week_start_date,
      this_week_end_date
    INTO
      g_start_date,
      g_end_date
    FROM dim_calendar
    WHERE calendar_date = g_date - (g_sub * 7);

   l_text       := '-------------------------------------------------------------';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text       := 'Rollup range is:- '||g_start_date||'  To '||g_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Truncate TEMP_LOC_ITEM_WK_CATALOG
  --**************************************************************************************************
  l_text := 'Truncate table DWH_PERFORMANCE.TEMP_LOC_ITEM_WK_CATALOG';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  execute immediate 'TRUNCATE TABLE DWH_PERFORMANCE.TEMP_LOC_ITEM_WK_CATALOG';
  commit;

  --**************************************************************************************************
  --Gather Stats On TEMP_LOC_ITEM_WK_CATALOG
  --**************************************************************************************************
--  l_text := 'Gather stats on DWH_PERFORMANCE.TEMP_LOC_ITEM_WK_CATALOG';
--  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--  dbms_stats.gather_table_stats ('DWH_PERFORMANCE', 'TEMP_LOC_ITEM_WK_CATALOG', degree => 32);
--  commit;

  l_text := 'LOADING DWH_PERFORMANCE.TEMP_LOC_ITEM_WK_CATALOG';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Insert Into TEMP_LOC_ITEM_WK_CATALOG  --/*+ PARALLEL(AA,4) */
  --**************************************************************************************************
 
  insert /*+ PARALLEL(CAT,6) */ into dwh_performance.TEMP_LOC_ITEM_WK_CATALOG CAT
 
  WITH cal as (
    select /*+ PARALLEL(a,6) */ a.sk1_item_no, a.sk1_location_no, max(a.calendar_date) calendar_date
      from dwh_datafix.aj_rtl_loc_item_dy_catalog a
     where a.calendar_date between g_start_date and g_end_date
  GROUP BY a.sk1_item_no, a.sk1_location_no)
  
  select /*+ PARALLEL(aa,6) */
          aa.sk1_location_no sk1_location_no,
          aa.sk1_item_no sk1_item_no,
          c.fin_year_no fin_year_no,
          c.fin_week_no fin_week_no,
          aa.calendar_date maxdate,
          product_status_code,
          product_status_1_code,
          wk_delivery_pattern,
          num_shelf_life_days,
          next_wk_catalog_ind,
          num_units_per_tray,
          product_class,
          min_shelf_life
  from  dwh_datafix.aj_rtl_loc_item_dy_catalog aa,
        dim_calendar c,
        CAL 
  where aa.calendar_date = CAL.CALENDAR_DATE
    AND aa.SK1_ITEM_NO = CAL.SK1_ITEM_NO
    AND aa.sk1_location_no = CAL.SK1_LOCATION_NO
    and aa.calendar_date = c.calendar_date
    and aa.calendar_date between g_start_date and g_end_date
    and c.calendar_date between g_start_date and g_end_date
  
  ;

  g_recs_inserted := 0;
  g_recs_inserted := g_recs_inserted + sql%rowcount;

  l_text          := 'Insert completed TEMP :- RECS =  '||g_recs_inserted||' '||g_start_date||'  To '||g_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  commit;

  --**************************************************************************************************
  --Gather Stats On TEMP_LOC_ITEM_WK_CATALOG
  --**************************************************************************************************
  dbms_stats.gather_table_stats ('DWH_PERFORMANCE', 'TEMP_LOC_ITEM_WK_CATALOG', cascade => true, degree => 32);
  commit;
  l_text := 'Gather stats on  DWH_PERFORMANCE.TEMP_LOC_ITEM_WK_CATALOG COMPLETE - STARTING MERGE';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Insert Into RTL_LOC_ITEM_WK_CATALOG --  /*+ PARALLEL(lid,4) FULL(lid) PARALLEL(z,4) FULL(z) */ -----
  --**************************************************************************************************
  g_recs_inserted := 0;


  insert /*+ Append PARALLEL(CAT,6) */
  into DWH_PERFORMANCE.RTL_LOC_ITEM_WK_CATALOG_N CAT
  select  /*+ PARALLEL(lid,6)  PARALLEL(z,6)  */
          lid.sk1_location_no sk1_location_no,
          lid.sk1_item_no sk1_item_no,
          dc.fin_year_no fin_year_no,
          dc.fin_week_no fin_week_no,
          max(dc.fin_week_code) fin_week_code,
          max(dc.this_week_start_date) this_week_start_date,
          max(lid.sk2_location_no) sk2_location_no,
          max(lid.sk2_item_no) sk2_item_no,
          max(nvl(com_flag_adj_ind,0)) com_flag_adj_ind,
          sum(case dc.fin_day_no when 7 then nvl(boh_adj_qty,0) end) boh_adj_qty,
          sum( case dc.fin_day_no when 7 then nvl(boh_adj_selling,0) end) boh_adj_selling,
          sum( case dc.fin_day_no when 7 then nvl(boh_adj_cost,0)  end) boh_adj_cost,
          sum(case dc.fin_day_no when 7 then nvl(soh_adj_qty,0) end) soh_adj_qty,
          sum( case dc.fin_day_no when 7 then nvl(soh_adj_selling,0) end) soh_adj_selling,
          sum( case dc.fin_day_no when 7 then nvl(soh_adj_cost,0) end) soh_adj_cost,
          sum(nvl(fd_num_avail_days,0)) fd_num_avail_days,
          sum(nvl(fd_num_avail_days_adj,0)) fd_num_avail_days_adj,
          sum(nvl(fd_num_catlg_days,0)) fd_num_catlg_days,
          sum(nvl(fd_num_catlg_days_adj,0)) fd_num_catlg_days_adj,
          sum(nvl(fd_sod_num_avail_days,0)) fd_sod_num_avail_days,
          sum(nvl(fd_sod_num_avail_days_adj,0)) fd_sod_num_avail_days_adj,
          g_date last_updated_date,
          nvl(z.product_status_code,0) product_status_code,
          nvl(z.product_class,0) product_class,
          nvl(z.wk_delivery_pattern, '2222222') wk_delivery_pattern,
          sum(nvl(weighted_avail_sales,0) ) weighted_avail_sales,
          sum(nvl(weighted_adj_avail_sales,0) ) weighted_adj_avail_sales,
          sum(nvl(weighted_avail_sales_qty,0) ) weighted_avail_sales_qty,
          sum(nvl(weighted_adj_avail_sales_qty,0)) weighted_adj_avail_sales_qty,
          max(nvl(this_wk_catalog_ind,0) ) this_wk_catalog_ind,
          nvl(z.next_wk_catalog_ind,0) next_wk_catalog_ind,
          nvl(z.num_units_per_tray,0) num_units_per_tray,
          nvl(z.product_status_1_code,0) product_status_1_code,
          nvl(z.num_shelf_life_days,0) num_shelf_life_days,
          sum( case dc.fin_day_no when 7 then nvl(boh_adj_cases,0) end) boh_adj_cases,
          sum( case dc.fin_day_no when 7 then nvl(boh_adj_fr_cost,0) end) boh_adj_fr_cost,
          sum( case dc.fin_day_no when 7 then nvl(boh_adj_qty_dept,0) end) boh_adj_qty_dept,
          sum( case dc.fin_day_no when 7 then nvl(boh_adj_cases_dept,0) end) boh_adj_cases_dept,
          sum( case dc.fin_day_no when 7 then nvl(boh_adj_selling_dept,0) end) boh_adj_selling_dept,
          sum( case dc.fin_day_no when 7 then nvl(boh_adj_cost_dept,0) end) boh_adj_cost_dept,
          sum( case dc.fin_day_no when 7 then nvl(soh_adj_cases,0) end) soh_adj_cases,
          sum( case dc.fin_day_no when 7 then nvl(soh_adj_fr_cost,0) end) soh_adj_fr_cost,
          sum(nvl(fd_num_dc_catlg_days,0)) fd_num_dc_catlg_days,
          sum(nvl(fd_num_dc_catlg_adj_days,0)) fd_num_dc_catlg_adj_days,
          sum(nvl(fd_num_dc_avail_days,0)) fd_num_dc_avail_days,
          sum(nvl(fd_num_dc_avail_adj_days,0)) fd_num_dc_avail_adj_days,
          max(nvl(fd_num_catlg_days,0)) fd_num_catlg_wk,
          avg(nvl(boh_adj_qty_dept,0)) avg_boh_adj_qty_dept,
          avg(nvl(boh_adj_qty,0)) avg_boh_adj_qty,
          avg(nvl(boh_adj_selling_dept,0)) avg_boh_adj_selling_dept,
          avg(nvl(boh_adj_selling,0)) avg_boh_adj_selling,
          avg(nvl(boh_adj_cases_dept,0)) avg_boh_adj_cases_dept,
          avg(nvl(boh_adj_cases,0)) avg_boh_adj_cases,
          avg(nvl(boh_adj_cost_dept,0)) avg_boh_adj_cost_dept,
          avg(nvl(boh_adj_cost,0)) avg_boh_adj_cost,
          sum(case dc.fin_day_no when 7 then nvl(boh_adj_qty_flt,0) end) boh_adj_qty_flt,
          nvl(z.min_shelf_life,0) min_shelf_life,
          sum(nvl(fd_num_cust_avail_adj,0)) fd_num_cust_avail_adj, -- new code AJ
          sum(nvl(num_facings,0)) num_facings,      -- new code AJ
          sum(nvl(FD_NUM_CUST_CATLG_ADJ,0)) fd_num_cust_catlg_adj,         -- new code QS (for AJ)
          sum(nvl(fd_cust_avail,0)) fd_cust_avail   -- new code QS (for AJ)
 
  from  dwh_datafix.aj_rtl_loc_item_dy_catalog lid,
        dwh_performance.dim_calendar dc,
        dwh_performance.TEMP_LOC_ITEM_WK_CATALOG z
      where lid.calendar_date = dc.calendar_date
        and lid.sk1_item_no     = z.sk1_item_no
        and lid.sk1_location_no = z.sk1_location_no
        and dc.fin_year_no      = z.fin_year_no
        and dc.fin_week_no      = z.fin_week_no
        and lid.calendar_date between g_start_date and g_end_date

  group by lid.sk1_location_no, lid.sk1_item_no, dc.fin_year_no, dc.fin_week_no, g_date, nvl(z.product_status_code,0), nvl(z.product_class,0), nvl(z.wk_delivery_pattern, '2222222'), nvl(z.next_wk_catalog_ind,0), nvl(z.num_units_per_tray,0), nvl(z.product_status_1_code,0), nvl(z.num_shelf_life_days,0), nvl(z.min_shelf_life,0);

  g_recs_read := g_recs_read + sql%rowcount;
  g_recs_inserted := g_recs_inserted + sql%rowcount;

  l_text := 'Insert completed NEW:- RECS =  '||g_recs_inserted||' '||g_start_date||'  To '||g_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       commit;

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

end WH_PRF_CORP_536U_CFIX;
