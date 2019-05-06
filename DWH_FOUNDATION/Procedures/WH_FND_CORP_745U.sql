--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_745U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_745U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2014
--  Author:      Quentin Smit
--  Purpose:     Populate FOODS LOC ITEM WK fact table in the foundation layer
--               with input ex staging table from JDA.
--  Tables:      Input  - RTL_JDAFF_WH_PLAN_WK_ANALYSIS
--                        DIM_ITEM
--                        RTL_ZONE_ITEM_OM
--               Output - fnd_rtl_loc_item_wk_fd
--  Packages:    constants, dwh_log, dwh_valid
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
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_rec_out            fnd_rtl_loc_item_wk_fd%rowtype;
g_found              boolean;
g_insert_rec         boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_745U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ROQ/CUST ORDERS FACTS EX RTL PERF TABLE';
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

    l_text := 'LOAD OF fnd_rtl_loc_item_wk_fd EX JDA STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date:= '22/JAN/15';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

MERGE  INTO dwh_foundation.fnd_rtl_loc_item_wk_fd fnd_liw USING
     (
       select b.item_no, c.location_no, d.fin_year_no, d.fin_week_no,
'' as supplier_no,
rzom.ship_hi as ship_hi,
rzom.ship_ti as ship_ti,
b.fd_discipline_type DCS_LOAD_TYPE,
nvl((a.rec_ship_unit / rzom.num_units_per_tray),0)  est_ll_cases,
nvl((a.rec_ship_unit * rzom.reg_rsp),0)  est_ll_selling,
a.rec_ship_unit  est_ll_qty,
a.DC_FORWARD_COVER_DAY NUM_PROJ_COVER_WKS,
a.CONSTRAINT_POH_UNIT  DEPOT_FWD_BOH_QTY,
nvl((a.REC_ARRIVAL_UNIT / rzom.NUM_UNITS_PER_TRAY),0) RECOM_ORDER_CASES,
a.IN_TRANSIT_UNIT CONFRM_ORDER_CASES,
0 CORR_SALES_CASES,
0 NUM_DEPOT_COVER_WEEKS,
0 NUM_MAX_DC_COVER_WEEKS,
rzom.min_order_qty,

--rzom.product_status_code,
Case rzom.product_status_code
        when  1 then  'A' 
        when  4  then 'D' 
        when  14 then 'N' 
        when  15 then 'O'
        when  21 then 'U' 
        when  24 then 'X'
        when  26 then 'Z'
        when  0  then ''
end as product_status_code,

' ' source_data_status_code,
--a.last_updated_date
g_date as last_updated_date

from RTL_JDAFF_WH_PLAN_WK_ANALYSIS a, 
     dim_item b, 
     dim_location c,
     dim_calendar d,
     rtl_zone_item_om rzom
where a.last_updated_date = g_date
and a.sk1_item_no = b.sk1_item_no
and a.SK1_LOCATION_NO = c.sk1_location_no
and a.trading_date = d.calendar_date
and b.sk1_item_no = rzom.sk1_item_no
and rzom.sk1_zone_group_zone_no = c.SK1_FD_ZONE_GROUP_ZONE_NO
and rzom.from_loc_no = c.location_no

         ) mer_lidso
ON  (mer_lidso.item_no    = fnd_liw.item_no
and mer_lidso.location_no = fnd_liw.location_no
and mer_lidso.fin_year_no = fnd_liw.fin_year_no
and mer_lidso.fin_week_no = fnd_liw.fin_week_no
)
WHEN MATCHED THEN
UPDATE
SET       SUPPLIER_NO               = mer_lidso.supplier_no,
          SHIP_TI                   = mer_lidso.SHIP_TI,
          SHIP_HI                   = mer_lidso.SHIP_HI,
          DCS_LOAD_TYPE             = mer_lidso.DCS_LOAD_TYPE,
          EST_LL_CASES              = mer_lidso.EST_LL_CASES,
          EST_LL_SELLING            = mer_lidso.EST_LL_SELLING,
          EST_LL_QTY                = mer_lidso.EST_LL_QTY,
          NUM_PROJ_COVER_WKS        = mer_lidso.NUM_PROJ_COVER_WKS,
          DEPOT_FWD_BOH_QTY         = mer_lidso.DEPOT_FWD_BOH_QTY,
          RECOM_ORDER_CASES         = mer_lidso.RECOM_ORDER_CASES,
          CONFRM_ORDER_CASES        = mer_lidso.CONFRM_ORDER_CASES,
          CORR_SALES_CASES          = mer_lidso.CORR_SALES_CASES,
          NUM_DEPOT_COVER_WEEKS     = mer_lidso.NUM_DEPOT_COVER_WEEKS,
          NUM_MAX_DC_COVER_WEEKS    = mer_lidso.NUM_MAX_DC_COVER_WEEKS,
          MIN_ORDER_QTY             = mer_lidso.MIN_ORDER_QTY,
          PRODUCT_STATUS_CODE       = mer_lidso.PRODUCT_STATUS_CODE,
          SOURCE_DATA_STATUS_CODE   = mer_lidso.SOURCE_DATA_STATUS_CODE,
          last_updated_date         = mer_lidso.last_updated_date
WHEN NOT MATCHED THEN
INSERT
(         LOCATION_NO,
          ITEM_NO,
          FIN_YEAR_NO,
          FIN_WEEK_NO,
          SUPPLIER_NO,
          SHIP_TI,
          SHIP_HI,
          DCS_LOAD_TYPE,
          EST_LL_CASES,
          EST_LL_SELLING,
          EST_LL_QTY,
          NUM_PROJ_COVER_WKS,
          DEPOT_FWD_BOH_QTY,
          RECOM_ORDER_CASES,
          CONFRM_ORDER_CASES,
          CORR_SALES_CASES,
          NUM_DEPOT_COVER_WEEKS,
          NUM_MAX_DC_COVER_WEEKS,
          MIN_ORDER_QTY,
          PRODUCT_STATUS_CODE,
          SOURCE_DATA_STATUS_CODE,
          LAST_UPDATED_DATE)
  values
(         mer_lidso.location_no,
          mer_lidso.item_no,
          mer_lidso.FIN_YEAR_NO,
          mer_lidso.FIN_WEEK_NO,
          mer_lidso.SUPPLIER_NO,
          mer_lidso.SHIP_TI,
          mer_lidso.SHIP_HI,
          mer_lidso.DCS_LOAD_TYPE,
          mer_lidso.EST_LL_CASES,
          mer_lidso.EST_LL_SELLING,
          mer_lidso.EST_LL_QTY,
          mer_lidso.NUM_PROJ_COVER_WKS,
          mer_lidso.DEPOT_FWD_BOH_QTY,
          mer_lidso.RECOM_ORDER_CASES,
          mer_lidso.CONFRM_ORDER_CASES,
          mer_lidso.CORR_SALES_CASES,
          mer_lidso.NUM_DEPOT_COVER_WEEKS,
          mer_lidso.NUM_MAX_DC_COVER_WEEKS,
          mer_lidso.MIN_ORDER_QTY,
          mer_lidso.PRODUCT_STATUS_CODE,
          mer_lidso.SOURCE_DATA_STATUS_CODE,     -- weigh ind
          mer_lidso.last_updated_date);

g_recs_read:=SQL%ROWCOUNT;
g_recs_inserted:=dwh_log.get_merge_insert_count;
g_recs_updated:=dwh_log.get_merge_update_count(SQL%ROWCOUNT);

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
end wh_fnd_corp_745u;
