--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_715U_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_715U_LOAD" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2015
--  Author:      Alastair de Wet
--  Purpose:     Create SPACERACE extract to flat file in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - sparse, dense, stock, catalog
--               Output - RTL_LOC_ITEM_WK_SPACERACE
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  
--  October 2016 - Q.Smit
--               - Change the program from an extract to write to a table that is read directly by AIT
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
g_count              number        :=  0;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_715U_LOAD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT SPACERACE DATA TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_last_week_no      dim_calendar.fin_week_no%type;
g_fin_year_no       dim_calendar.fin_year_no%type;
g_spacerace_file    varchar2(20 byte);

g_string            varchar2(5000 byte);


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOADING OF SPACERACE DATA STARTED AT '||
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
    
    select last_wk_fin_year_no, last_wk_fin_week_no
      into g_fin_year_no, g_last_week_no
      from dim_control;
    
    g_fin_year_no  := 2018;  
    g_last_week_no := 2;
    
    l_text := ' YEAR AND WEEK BEING EXTRACTED : ' || g_fin_year_no ||' - '|| g_last_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
    execute immediate 'truncate table DWH_PERFORMANCE.RTL_LOC_ITEM_WK_SPACERACE';
    l_text := ' DWH_PERFORMANCE.RTL_LOC_ITEM_WK_SPACERACE truncated ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
 insert /*+ parallel(dest,4) */ into DWH_PERFORMANCE.RTL_LOC_ITEM_WK_SPACERACE dest
  
    WITH Seldns AS  (SELECT    /*+ parallel (a,4) full(a)  */
    a.fin_year_no,
    a.fin_week_no,
    a.this_week_start_date,
    di.business_unit_no,
    Dl.Location_no,
    Di.Item_no,
    di.item_desc,
    a.sales,
    a.Sales_qty,
    a.sales_cost ,
    a.sales_margin
  FROM Rtl_loc_item_wk_rms_dense a,
    Dim_location dl,
    Dim_item Di
  WHERE a.fin_year_no = g_fin_year_no
  and a.fin_week_no = g_last_week_no
  AND a.sk1_location_no   = dl.sk1_location_no
  AND dl.area_no          = 9951
  AND di.business_unit_no = 50
  AND a.Sk1_item_no       = Di.Sk1_item_no
  AND A.Sales_Qty        IS NOT NULL
  ),
  
  Selsprs AS
  (SELECT
    /*+ parallel (a,4) full(a)  */
    a.fin_year_no,
    a.fin_week_no,
    a.this_week_start_date,
    di.business_unit_no,
    Dl.Location_no,
    Di.Item_no,
    di.item_desc,
    a.waste_selling,
    a.waste_qty,
    a.waste_cost,
    a.prom_sales,
    a.prom_sales_cost,
    a.prom_sales_qty
  FROM Rtl_loc_item_wk_rms_sparse a,
    Dim_location dl,
    Dim_item Di
   WHERE a.fin_year_no = g_fin_year_no 
  and a.fin_week_no = g_last_week_no
  AND a.sk1_location_no   = dl.sk1_location_no
  AND dl.area_no          = 9951
  AND di.business_unit_no = 50
  AND a.Sk1_item_no       = Di.Sk1_item_no
  --AND A.Waste_Qty        IS NOT NULL 
  ),
  
  wkcatalg AS
  (SELECT
    /*+ parallel (a,4) full(a)  */
   a.fin_year_no,
    a.fin_week_no,
    a.this_week_start_date,
    di.business_unit_no,
    Dl.Location_no,
    Di.Item_no,
    di.item_desc,
    a.boh_adj_qty,
    a.SOH_ADJ_QTY,
    a.FD_NUM_AVAIL_DAYS_ADJ,
    a.FD_NUM_CATLG_DAYS_ADJ
  FROM rtl_loc_item_wk_catalog a,
    Dim_location dl,
    Dim_item Di
    WHERE a.fin_year_no = g_fin_year_no 
  and a.fin_week_no = g_last_week_no 
  AND a.sk1_location_no   = dl.sk1_location_no
  AND dl.area_no          = 9951
  AND di.business_unit_no = 50
  AND a.Sk1_item_no       = Di.Sk1_item_no
  AND A.boh_adj_qty      IS NOT NULL
  ) ,
  
  rtl_stock AS
  (SELECT
    /*+ parallel (a,4) full(a)  */
    a.fin_year_no,
    a.fin_week_no,
    a.this_week_start_date,
    di.business_unit_no,
    Dl.Location_no,
    Di.Item_no,
    di.item_desc,
    a.SOH_QTY,
    a.SOH_COST,
    a.boh_qty,
    a.BOH_COST
  FROM rtl_loc_item_wk_rms_stock a,
    Dim_location dl,
    Dim_item Di
    WHERE a.fin_year_no = g_fin_year_no
  and a.fin_week_no = g_last_week_no
  AND a.sk1_location_no   = dl.sk1_location_no
  AND dl.area_no          = 9951
  AND di.business_unit_no = 50
  AND a.Sk1_item_no       = Di.Sk1_item_no
  --AND A.boh_qty          IS NOT NULL
  ) 
  
SELECT
  /*+ parallel (Sd,4) parallel (Ss,4) full(Sd) full(Ss) */
  NVL(NVL(NVL(sd.Fin_Year_No, ss.Fin_Year_No), wc.fin_year_no), rs.fin_year_no) Fin_Year_No,
  NVL(NVL(NVL(sd.Fin_week_No, ss.Fin_week_No), wc.Fin_week_No), rs.fin_week_no) Fin_week_No,
  NVL(NVL(NVL(sd.this_week_start_date, ss.this_week_start_date), wc.this_week_start_date), rs.this_week_start_date) this_week_start_date ,
  NVL(NVL(NVL(sd.business_unit_no, ss.business_unit_no), wc.business_unit_no), rs.business_unit_no) business_unit_no,
  NVL(NVL(NVL(sd.Location_No, ss.Location_No), wc.Location_No), rs.Location_No) Location_No ,
  NVL(NVL(NVL(sd.item_no, ss.item_no), wc.item_no), rs.item_no) item_no,
  NVL(NVL(NVL(sd.item_desc, ss.item_desc), wc.item_desc), rs.item_desc) item_desc,
  rs.SOH_QTY,
  wc.SOH_ADJ_QTY,
  rs.SOH_COST,
  boh_qty,
  boh_adj_qty,
  rs.BOH_COST,
  sales,
  Sales_qty,
  Sales_Cost ,
  sales_margin ,
  waste_selling,
  Waste_Qty,
  waste_cost,
  wc.FD_NUM_AVAIL_DAYS_ADJ,
  wc.FD_NUM_CATLG_DAYS_ADJ,
  prom_sales,                
  prom_sales_cost,
  prom_sales_qty
  
FROM Seldns Sd
FULL OUTER JOIN Selsprs Ss  ON Ss.Fin_week_No  = Sd.Fin_week_No
                            AND Ss.fin_year_no = Sd.fin_year_no
                            AND Ss.location_no = Sd.location_no
                            AND Ss.item_no     = Sd.item_no

FULL OUTER JOIN wkcatalg wc ON NVL(Ss.Fin_week_No, Sd.Fin_week_No)  = wc.Fin_week_No
                            AND NVL(Ss.Fin_Year_No, Sd.Fin_Year_No) = wc.fin_year_no
                            AND NVL(Ss.location_no, Sd.location_no) = wc.location_no
                            AND NVL(Ss.item_no, sd.item_no)         = wc.item_no

FULL OUTER JOIN rtl_stock rs  ON NVL(NVL(Ss.Fin_week_No, Sd.Fin_week_No),wc.Fin_week_No)   = rs.Fin_week_No
                              AND NVL(NVL(Ss.Fin_Year_No, Sd.Fin_Year_No), wc.fin_year_no) = rs.fin_year_no
                              AND NVL(NVL(Ss.location_no, Sd.location_no), wc.location_no) = rs.location_no
                              AND NVL(NVL(Ss.item_no, Sd.item_no), wc.item_no)             = rs.item_no
;  
    
    g_recs_inserted := + SQL%ROWCOUNT;
    commit;
--
    l_text := 'TABLE DWH_PERFORMANCE.RTL_LOC_ITEM_WK_SPACERACE POPULATED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    l_text := 'Running GATHER_TABLE_STATS ON RTL_LOC_ITEM_WK_SPACERACE';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--   
--   EXECUTE IMMEDIATE 'ALTER SESSION enable PARALLEL DML';
--
--   DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'RTL_LOC_ITEM_WK_SPACERACE', CASCADE => TRUE, DEGREE => 16);
--
--   l_text := 'GATHER_TABLE_STATS ON RTL_LOC_ITEM_WK_SPACERACE completed';
--   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
       
end wh_prf_corp_715u_load;
