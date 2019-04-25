--------------------------------------------------------
--  DDL for Procedure KQ_WK_FD_EXTRACT_DEPT_SALES
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."KQ_WK_FD_EXTRACT_DEPT_SALES" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        October 2016
--  Author:      Khanyisa Qamata
--  Purpose:     Create PO Foods weekly extracts to flat files in the performance layer
--               by executing scrips and calling generic function to output to flat file.
--  Tables:      Input  - scripts
--               Output - flat file extracts
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_count              number        :=  0;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'KQ_WK_FD_EXTRACT_DEPT_SALES';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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

    l_text := 'EXTRACT STARTED AT '||
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
    g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 29
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Fish_dept_29_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 66
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Take_Aways_dept_66_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 91
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Deli_Meats_dept_91_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 80
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Value_Added_Meat_dept_80_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 58
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Meat_dept_58_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 90
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Poultry_dept_90_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 50
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Value_Added_Poultry_dept_50_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 72
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Seafoods_dept_72_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 87
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Global_Meal_Solution_dept_87_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 59
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Deli_Snacks_dept_59_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 95
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Traditional_Meat_dept_95_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 13
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Plant_Bread_dept_13_Sales.txt');

g_count := dwh_generic_file_extract('WITH LOC_LIST AS
  (SELECT sk1_location_no,
    WH_FD_ZONE_NO,
    location_no,
    location_name
  FROM DIM_LOCATION
  WHERE area_no = 9951
  ),
  ITEM_LIST AS
  (SELECT itm.sk1_item_no,
    itm.item_desc,
    itm.item_no ,
    itm.subclass_no,
    itm.subclass_name,
    itm.department_no,
    itm.department_name ,
    sup.supplier_no,
    sup.supplier_name
  FROM dim_item itm,
    DIM_SUPPLIER SUP
  where ITM.SK1_SUPPLIER_NO = SUP.SK1_SUPPLIER_NO
  AND itm.Department_no     = 79
  AND BUSINESS_UNIT_NO      = 50
 ),
  sales_measures AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no,
    SUM (DNS.SALES) SALES_RANDS,
    SUM (DNS.SALES_QTY) SALES_UNITS,
    SUM(DNS.SALES_MARGIN) SALES_MARGIN,
    SUM(DNS.SDN_IN_SELLING)SDN_SELLING,
    SUM(SDN_IN_QTY)SDN_UNITS,
    SUM(SDN_IN_CASES)SDN_CASES,
    count(sdn_in_qty)count_sdn_units
  FROM Rtl_Loc_Item_dy_Rms_Dense dns,
    loc_list dl,
    item_list di,
    DIM_CALENDAR CAL,
    DIM_CONTROL_REPORT dcr
  WHERE dns.sk1_item_no   = di.sk1_item_no
  and dns.sk1_location_no = dl.sk1_location_no
  and DNS.POST_DATE = CAL.CALENDAR_DATE
  and dns.post_date >= dcr.LAST_WK_START_DATE and dns.post_date <=  dcr.LAST_WK_END_DATE
  GROUP BY di.item_no,
    di.item_desc,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    cal.fin_year_no,
    cal.fin_week_no
  ) ,
  corrected_list AS
  (SELECT
    /*+ parallel (dns,4) full(dns) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO,
    SUM(CR.CORR_SALES) CORRECTED_SALES_RANDS,
    SUM(cr.corr_sales_qty)CORRECTED_SALES_UNITS
  FROM rtl_loc_item_wk_rdf_sale cr ,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT dcr
  WHERE cr.sk1_item_no   = di.sk1_item_no
  and CR.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CR.FIN_WEEK_NO   =DCR.LAST_WK_FIN_WEEK_NO
  AND CR.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
   GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    cr.fin_year_no,
    CR.FIN_WEEK_NO
  ) ,
  FCST_list AS
  (SELECT
    /*+ parallel (FCST,4) full(FCST) */
    di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name ,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO,
    SUM(SALES_WK_APP_FCST)App_Forecast_Selling,
    SUM(FCST.SALES_wk_APP_FCST_QTY)FORECAST_UNITS
  FROM RTL_LOC_ITEM_wk_RDF_FCST FCST ,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE FCST.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and FCST.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and FCST.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND FCST.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
GROUP BY di.item_no,
    di.item_desc,
    di.sk1_item_no,
    di.subclass_no,
    di.subclass_name,
    dl.sk1_location_no,
    di.department_no,
    di.department_name,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    FCST.FIN_YEAR_NO,
    FCST.FIN_WEEK_NO
  ) ,
  CATALOG_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO,
    SUM (cat.fd_num_catlg_wk) No_of_weeks_catalogued,
    SUM(CAT.FD_NUM_AVAIL_DAYS_ADJ)FD_NUM_AVAIL_DAYS_ADJ,
    SUM(CAT.FD_NUM_CATLG_DAYS_ADJ)FD_NUM_CATLG_DAYS_ADJ,
    MAX(CAT.NUM_UNITS_PER_TRAY)UNITS_PER_TRAY,
    MAX(cat.NUM_SHELF_LIFE_DAYS)Shelf_Life
  FROM Rtl_Loc_Item_wk_Catalog cat,
    loc_list dl,
    ITEM_LIST DI,
    dim_control_report dcr
  WHERE cat.sk1_item_no   = di.sk1_item_no
  and CAT.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and CAT.FIN_WEEK_NO  =DCR.LAST_WK_FIN_WEEK_NO
  AND CAT.FIN_YEAR_NO  =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    di.sk1_item_no,
    cat.fin_year_no,
    CAT.FIN_WEEK_NO
  ),
  BOH_LIST AS
  (SELECT
    /*+ parallel (cat,4) full(cat) */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name ,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO,
   SUM(BOH_QTY)BOH_UNITS,
    SUM(BOH_SELLING)BOH_SELLING
  FROM RTL_LOC_ITEM_wk_RMS_STOCK RS,
    LOC_LIST DL,
    ITEM_LIST DI,
    dim_control_report DCR
  WHERE RS.SK1_ITEM_NO   = DI.SK1_ITEM_NO
  and RS.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and RS.FIN_WEEK_NO    =DCR.LAST_WK_FIN_WEEK_NO
  and RS.FIN_YEAR_NO    =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    DI.SK1_ITEM_NO,
    RS.FIN_YEAR_NO,
    RS.FIN_WEEK_NO
  ),
  WASTE_MEASURES AS
  (SELECT
    /*+ parallel (spa,4) full(spa)  */
    di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
   di.department_name ,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO,
    SUM(SPA.WASTE_QTY)WASTE_Units,
    SUM(SPA.WASTE_COST)WASTE_COST,
    SUM(spa.prom_sales)prom_sales
  FROM RTL_LOC_ITEM_WK_RMS_SPARSE SPA,
    LOC_LIST DL,
    ITEM_LIST DI,
    DIM_CONTROL_REPORT DCR
  WHERE spa.sk1_item_no   = di.sk1_item_no
  and SPA.SK1_LOCATION_NO = DL.SK1_LOCATION_NO
  and SPA.FIN_WEEK_NO   = DCR.LAST_WK_FIN_WEEK_NO
  and SPA.FIN_YEAR_NO     =DCR.LAST_WK_FIN_YEAR_NO
  GROUP BY di.item_no,
    di.subclass_no,
    di.subclass_name,
    di.department_no,
    di.department_name,
    dl.sk1_location_no,
    di.item_desc,
    di.supplier_no,
    di.supplier_name,
    dl.location_no,
    DL.LOCATION_NAME,
    DL.WH_FD_ZONE_NO,
    di.sk1_item_no,
    spa.fin_year_no,
    SPA.FIN_WEEK_NO
  )
SELECT NVL (NVL (NVL (NVL (NVL (F0.WH_FD_ZONE_NO, F1.WH_FD_ZONE_NO), F2.WH_FD_ZONE_NO), F3.WH_FD_ZONE_NO), F4.WH_FD_ZONE_NO),F5.WH_FD_ZONE_NO)DC_REGION,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NO, F1.LOCATION_NO), F2.LOCATION_NO), F3.LOCATION_NO),F4.LOCATION_NO),F5.LOCATION_NO)LOCATION_NO,
  NVL (NVL (NVL (NVL (NVL (F0.LOCATION_NAME, F1.LOCATION_NAME), F2.LOCATION_NAME), F3.LOCATION_NAME),F4.LOCATION_NAME),F5.LOCATION_NAME)LOCATION_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_NO, F1.ITEM_NO), F2.ITEM_NO), F3.ITEM_NO),F4.ITEM_NO),F5.ITEM_NO)ITEM_NO,
  NVL (NVL (NVL (NVL (NVL (F0.ITEM_DESC, F1.ITEM_DESC), F2.ITEM_DESC), F3.ITEM_DESC),F4.ITEM_DESC),F5.ITEM_DESC)ITEM_DESC,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NO, F1.SUPPLIER_NO), F2.SUPPLIER_NO), F3.SUPPLIER_NO),F4.SUPPLIER_NO),F5.SUPPLIER_NO)SUPPLIER_NO,
  NVL (NVL (NVL (NVL (NVL (F0.SUPPLIER_NAME, F1.SUPPLIER_NAME), F2.SUPPLIER_NAME), F3.SUPPLIER_NAME),F4.SUPPLIER_NAME),F5.SUPPLIER_NAME)SUPPLIER_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.SUBCLASS_NO, F1.SUBCLASS_NO), F2.SUBCLASS_NO), F3.SUBCLASS_NO),F4.SUBCLASS_NO),F5.SUBCLASS_NO)SUBCLASS_NO,
  NVL ( NVL (NVL (NVL (NVL (F0.SUBCLASS_NAME, F1.SUBCLASS_NAME), F2.SUBCLASS_NAME), F3.SUBCLASS_NAME),F4.SUBCLASS_NAME),F5.SUBCLASS_NAME)SUBCLASS_NAME,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NO, F1.DEPARTMENT_NO), F2.DEPARTMENT_NO), F3.DEPARTMENT_NO),F4.DEPARTMENT_NO),F5.DEPARTMENT_NO)DEPARTMENT_NO,
  NVL (NVL ( NVL (NVL (NVL (F0.DEPARTMENT_NAME, F1.DEPARTMENT_NAME), F2.DEPARTMENT_NAME), F3.DEPARTMENT_NAME),F4.DEPARTMENT_NAME),F5.DEPARTMENT_NAME)DEPARTMENT_NAME,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO), F3.FIN_WEEK_NO),F4.FIN_WEEK_NO),F5.FIN_WEEK_NO)FIN_WEEK_NO,
  NVL (NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO), F3.FIN_YEAR_NO),F4.FIN_YEAR_NO),F5.FIN_YEAR_NO)FIN_YEAR_NO,
  F1.SALES_RANDS,
  F1.SALES_UNITS,
  F1.SALES_MARGIN,
  F1.SDN_SELLING,
  F1.SDN_UNITS,
  F1.SDN_CASES,
  F1.COUNT_SDN_UNITS,
  F3.PROM_SALES,
  F2.CORRECTED_SALES_RANDS,
  F2.CORRECTED_SALES_UNITS,
  F3.WASTE_COST,
  F3.WASTE_UNITS,
  F0.NO_OF_WEEKS_CATALOGUED,
  F0.UNITS_PER_TRAY,
  F0.SHELF_LIFE,
  F4.APP_FORECAST_SELLING,
  f4.FORECAST_UNITS,
  F0.FD_NUM_AVAIL_DAYS_ADJ,
  F0.FD_NUM_CATLG_DAYS_ADJ,
  F5.BOH_SELLING,
  F5.BOH_UNITS
FROM CATALOG_LIST F0
FULL OUTER JOIN sales_measures f1
ON f0.sk1_location_no = f1.sk1_location_no
AND f0.sk1_item_no    = f1.sk1_item_no
AND F0.FIN_YEAR_NO    = F1.FIN_YEAR_NO
AND F0.FIN_week_NO    = F1.FIN_week_NO
FULL OUTER JOIN corrected_list f2
ON NVL (f0.sk1_location_no, f1.sk1_location_no) = f2.sk1_location_no
AND NVL (f0.sk1_item_no, f1.sk1_item_no)        = f2.sk1_item_no
AND NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO)        = F2.FIN_YEAR_NO
AND NVL (F0.FIN_week_NO, F1.FIN_week_NO)        = F2.FIN_week_NO
FULL OUTER JOIN WASTE_MEASURES f3
ON NVL (NVL (f0.sk1_location_no, f1.sk1_location_no), f2.sk1_location_no) = f3.sk1_location_no
AND NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO)            = F3.SK1_ITEM_NO
AND NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO)            = F3.FIN_YEAR_NO
AND NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO)            = F3.FIN_WEEK_NO
FULL OUTER JOIN FCST_LIST F4
ON NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO) = F4.SK1_LOCATION_NO
AND NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO)               = F4.SK1_ITEM_NO
AND NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO)                = F4.FIN_YEAR_NO
AND NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO)                = F4.FIN_WEEK_NO
FULL OUTER JOIN BOH_LIST F5
ON NVL (NVL (NVL (NVL (F0.SK1_LOCATION_NO, F1.SK1_LOCATION_NO), F2.SK1_LOCATION_NO),F3.SK1_LOCATION_NO),F4.SK1_LOCATION_NO) = F5.SK1_LOCATION_NO
AND NVL (NVL (NVL (NVL (F0.SK1_ITEM_NO, F1.SK1_ITEM_NO), F2.SK1_ITEM_NO) ,F3.SK1_ITEM_NO),F4.SK1_ITEM_NO)                   = F5.SK1_ITEM_NO
and NVL (NVL (NVL (NVL (F0.FIN_YEAR_NO, F1.FIN_YEAR_NO), F2.FIN_YEAR_NO),F3.FIN_YEAR_NO),F4.FIN_YEAR_NO)                    = F5.FIN_YEAR_NO
and NVL (NVL (NVL (NVL (F0.FIN_WEEK_NO, F1.FIN_WEEK_NO), F2.FIN_WEEK_NO),F3.FIN_WEEK_NO),F4.FIN_WEEK_NO)                    = F5.FIN_WEEK_NO','|','DWH_FILES_OUT','Gluten_Free_dept_79_Sales.txt');


    l_text :=  'Records extracted to extract file '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
end KQ_WK_FD_EXTRACT_DEPT_SALES;
