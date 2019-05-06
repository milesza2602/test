--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_257U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_257U" (p_forall_limit in integer, p_success out boolean) as 

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Quentin Smit
--  Purpose:     Foods Renewal datacheck comparison extract 2
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   foods_renewal_extract_3_2
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            W6005682.foods_renewal_extract3_2%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_257U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FOODS RENEWAL DATACHECK COMPARISON EXTRACT 3 PART 2 - ALLOCATIONS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of W6005682.foods_renewal_extract3_2%rowtype index by binary_integer;
type tbl_array_u is table of W6005682.foods_renewal_extract3_2%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_today_date        date := trunc(sysdate) - 1;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF foods_renewal_extract_2_2 started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table W6005682.foods_renewal_extract3_2');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------
    
    execute immediate 'alter session enable parallel dml';
-- ######################################################################################### --
-- The outer joins are needed as there are cases when there are no sales in dense for items  --
-- which must be included in order to show a zero sales index as these records will be       --
-- created when the outer joins to either dense LY or the item price records are found       --
-- ######################################################################################### --

  l_text := 'Date being processed B4 lookup: ' || l_today_date ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 select today_date
   into l_today_date
   from dim_control;
     
   
 l_text := 'Date being processed AFTER lookup: ' || l_today_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
     
     
-- l_today_date := 'Moo';

INSERT /*+ APPEND PARALLEL (mart,4) */ INTO W6005682.foods_renewal_extract3_2 mart
WITH item_list AS
  (  select di.item_no, di.sk1_item_no, di.item_desc, di.sk1_supplier_no, dd.department_no, dd.department_name, di.subclass_no, di.subclass_name, di.fd_product_no, 
            di.standard_uom_code  --, di.fd_dis
            from dim_item di, dim_department dd
               where dd.department_no in (44, 88)  --(42,46,59,66,83,86,89)   --(41,58,73,81,88,96)   --,44,45,47,50,55,58,72,73,75,76,77,81,87,88,90,93,95,96,97,98,99)
                 and di.department_no = dd.department_no
  ) ,  --select * from item_list;
  
  loc_list AS
  (SELECT location_no,
    location_name    ,
    sk1_location_no  ,
    wh_fd_zone_no    ,
    SK1_FD_ZONE_GROUP_ZONE_NO,
    WH_FD_ZONE_GROUP_NO,
    chain_no,
    num_store_leadtime_days
     FROM dim_location
  ),  --   select * from loc_list;
  
 
SQL1 as 
    (SELECT /*+ PARALLEL(FND_RTL_ALLOCATION,2) PARALLEL(OM,2) PARALLEL(FND_RTL_SHIPMENT,2) FULL(FND_RTL_ALLOCATION) FULL(OM) FULL(FND_RTL_SHIPMENT) */
      dc.fin_year_no,
      dc.fin_week_no,
      dc.fin_day_no,
      loc_list.wh_fd_zone_no,
      FND_RTL_ALLOCATION.ALLOC_NO,
      DI.FD_PRODUCT_NO,
      DI.ITEM_NO,
      DI.ITEM_DESC,
      DI.STANDARD_UOM_CODE,
      DI.DEPARTMENT_NO,
      loc_list.LOCATION_NO,
      loc_list.LOCATION_NAME,
      WH.LOCATION_NO WH_NO,
      WH.LOCATION_NAME WH_NAME,
      FND_RTL_SHIPMENT.SDN_NO,
      FND_RTL_ALLOCATION.INTO_LOC_DATE,
      FND_RTL_SHIPMENT.ACTL_RCPT_DATE,
      FND_RTL_SHIPMENT.COST_PRICE,
      OM.NUM_UNITS_PER_TRAY,
      FND_RTL_SHIPMENT.RECEIVED_QTY
      
    FROM 
      FND_RTL_ALLOCATION,
      item_list DI, 
      loc_list , 
      FND_ZONE_ITEM_OM OM, 
      FND_RTL_SHIPMENT,
      DIM_CONTROL_REPORT, 
      dim_location wh,
      dim_calendar dc
       
    WHERE FND_RTL_ALLOCATION.WH_NO IN (6060,6010,6110)
      AND  FND_RTL_SHIPMENT.ACTL_RCPT_DATE  = l_today_date  --DIM_CONTROL_REPORT.EERGISTER_DATE
     	AND loc_list.CHAIN_NO = 10
      AND FND_RTL_ALLOCATION.ITEM_NO        = DI.ITEM_NO
      AND FND_RTL_ALLOCATION.WH_NO          = WH.LOCATION_NO
      AND OM.ZONE_GROUP_NO                  = loc_list.WH_FD_ZONE_GROUP_NO
      AND OM.ZONE_NO                        = loc_list.WH_FD_ZONE_NO
      AND OM.ITEM_NO                        = DI.ITEM_NO
      AND FND_RTL_ALLOCATION.TO_LOC_NO      = loc_list.LOCATION_NO   
      AND FND_RTL_SHIPMENT.ITEM_NO          = DI.ITEM_NO
      AND FND_RTL_SHIPMENT.TO_LOC_NO        = loc_list.LOCATION_NO
      AND FND_RTL_SHIPMENT.DIST_NO          = FND_RTL_ALLOCATION.ALLOC_NO
      AND FND_RTL_ALLOCATION.INTO_LOC_DATE  = l_today_date   --DIM_CONTROL_REPORT.EERGISTER_DATE
      and dim_control_report.eergister_date = dc.calendar_date
      
) 
select 
       SQL1.fin_year_no         as  FIN_YEAR_NO,
       SQL1.fin_week_no         as  FIN_WEEK_NO,
       SQL1.fin_day_no          as  FIN_DAY_NO,
       SQL1.wh_fd_zone_no       as  DC_REGION,
       SQL1.item_no             as  ITEM_NO,
       SQL1.ITEM_DESC           as  ITEM_DESC,
       SQL1.FD_PRODUCT_NO       as  FD_PRODUCT_NO,
       SQL1.LOCATION_NO         as  LOCATION_NO,
       SQL1.LOCATION_NAME       as  LOCATION_NAME,
       SQL1.ALLOC_NO            as  ALLOC_NO,
       SQL1.STANDARD_UOM_CODE   as  STANDARD_UOM_CODE,      
       SQL1.DEPARTMENT_NO       as  DEPARTMENT_NO,
       SQL1.WH_NO               as  WH_NO,
       SQL1.WH_NAME             as  WH_NAME,
       SQL1.SDN_NO              as  SDN_NO,
       SQL1.ACTL_RCPT_DATE      as  ACTL_RCPT_DATE,
       SQL1.INTO_LOC_DATE       as  INTO_LOC_DATE,
       SUM((SQL1.RECEIVED_QTY / SQL1.NUM_UNITS_PER_TRAY)) as qty_received_cases  
 from 
       SQL1
 GROUP BY SQL1.fin_year_no, SQL1.fin_week_no, SQL1.fin_day_no, SQL1.wh_fd_zone_no, SQL1.item_no, SQL1.ITEM_DESC, SQL1.FD_PRODUCT_NO, SQL1.LOCATION_NO, 
          SQL1.LOCATION_NAME, SQL1.ALLOC_NO, SQL1.STANDARD_UOM_CODE, SQL1.DEPARTMENT_NO, SQL1.WH_NO, SQL1.WH_NAME, SQL1.SDN_NO, SQL1.ACTL_RCPT_DATE, SQL1.INTO_LOC_DATE
 order by 
       LOCATION_NO asc,
       ALLOC_NO asc
 ;

g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;

commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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
       
end wh_prf_corp_257U;
