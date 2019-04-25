--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_265U_WLCURR
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_265U_WLCURR" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2014
--  Author:      Quentin Smit / Karna
--  Purpose:     Load Mart Foods Supplier Shipment mart
--               Contains only shipment information
--  Tables:      Input  - dim_item
--                        dim_department
--                        dim_location
--                        dim_item_uda
--                        dim_supplier
--                        dim_calendar  
--                        dim_purchase_order
--                        fnd_rtl_shipment
--               Output - MART_FOODS_SUPPLIER
--  Packages:    constants, dwh_log, dwh_valid
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
g_count              number        :=  0;
g_rec_out            rtl_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_265U_WLCURR';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE FOODS SUPPLIER SHIPMENT MART';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF MART_FOODS_SUPPLIER (SHIPMENTS ONLY) STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
 --**************************************************************************************************
-- Look up batch date from dim_control
--/*+ APPEND USE_HASH(rtl_mart ,mer_mart)*/
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --G_DATE := '22/OCT/14';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

    l_text := 'TRUNCATE TABLE MART_FOODS_SUPPLIER_WLCURR' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_DATAFIX.MART_FOODS_SUPPLIER_WLCURR');
COMMIT;

INSERT /*+ APPEND */ INTO DWH_DATAFIX.MART_FOODS_SUPPLIER_WLCURR rtl_mart 

with ITEM_LIST AS
  (
  SELECT DI.ITEM_NO,
    DI.SK1_ITEM_NO,
    DI.SK1_SUPPLIER_NO,
    DI.DEPARTMENT_NO,
    DI.SUBCLASS_NO,
    DI.SUBCLASS_NAME,
    DI.SUBCLASS_LONG_DESC,
    DI.DEPARTMENT_LONG_DESC,
    DI.ITEM_DESC,
    DI.ITEM_LONG_DESC,
    DI.FD_PRODUCT_NO,
    DD.JV_DEPT_IND,
    DD.PACKAGING_DEPT_IND,
    DD.GIFTING_DEPT_IND,
    DD.NON_MERCH_DEPT_IND,
    DD.NON_CORE_DEPT_IND,
    DD.BUCKET_DEPT_IND,
    DD.BOOK_MAGAZINE_DEPT_IND,
    DD.DEPARTMENT_NAME,
    DD.LAST_UPDATED_DATE,
    DD.SK1_DEPARTMENT_NO
  FROM DIM_ITEM DI,
       DIM_DEPARTMENT DD where DD.BUSINESS_UNIT_NO = 50
  and DI.SK1_DEPARTMENT_NO = DD.SK1_DEPARTMENT_NO
   ) 
  
,  
  LOC_LIST AS
  (SELECT DL.SK1_FD_ZONE_GROUP_ZONE_NO,
    DL.LOCATION_NO,
    DL.LOCATION_NAME,
    DL.SK1_LOCATION_NO,
    DL.LOCATION_LONG_DESC,
    DL.STOCK_HOLDING_IND,
    DL.LOC_TYPE,
    DL.DISTRICT_NAME,
    DZ.ZONE_NO, 
    DZ.ZONE_DESCRIPTION, 
    DZ.SK1_ZONE_GROUP_ZONE_NO
  FROM DIM_LOCATION DL, DIM_ZONE DZ
    where DL.SK1_FD_ZONE_GROUP_ZONE_NO = DZ.SK1_ZONE_GROUP_ZONE_NO
    and DZ.ZONE_GROUP_NO = 1 
    AND STOCK_HOLDING_IND = 1
    and DL.LOC_TYPE = 'W'
  ) 
  ,
    
  SUPP_LIST as
  (SELECT SUPPLIER_NO,SK1_SUPPLIER_NO,SUPPLIER_NAME,SUPPLIER_LONG_DESC FROM DIM_SUPPLIER),
  
  
  PURCH_LIST as 
  ( select DP.PO_NO, DP.SK1_PO_NO, DP.ORIG_APPROVAL_DATE, DP.PO_STATUS_CODE, DP.NOT_BEFORE_DATE, DP.NOT_AFTER_DATE, DP.INTO_STORE_DATE, DP.LAST_UPDATED_DATE 
     from DIM_PURCHASE_ORDER DP) 
  ,
  
 UDA_LIST as
  (
select SK1_ITEM_NO,
  COMMERCIAL_MANAGER_DESC_562,
  MERCH_CLASS_DESC_100,
  PRODUCT_CLASS_DESC_507
  from DIM_ITEM_UDA   -- where COMMERCIAL_MANAGER_DESC_562 not like '%C&H%' 
                    --  and  MERCH_CLASS_DESC_100 not  like 'C&H%' 
                   --   and PRODUCT_CLASS_DESC_507  not  like 'C&H%'
                     
   ) ,
SELALL AS (select /*+ parallel_index(g,4) */
       F.SK1_PO_NO,
       A.SK1_ITEM_NO,
       G.RECEIVE_DATE,
       G.SHIPMENT_NO  shipment_no  ,
       
       G.FROM_LOC_NO,
       G.TO_LOC_NO,
       G.FINAL_LOC_NO,
       
       A.SK1_DEPARTMENT_NO,
       E.SK1_SUPPLIER_NO         ,
       A.DEPARTMENT_NO,         
       A.DEPARTMENT_NAME    ,
       A.DEPARTMENT_LONG_DESC,
       A.SUBCLASS_NO  ,
       A.SUBCLASS_NAME ,         
       A.SUBCLASS_LONG_DESC ,
       D.CALENDAR_DATE,          
       D.FIN_DAY_NO,
       D.FIN_WEEK_NO,
       D.FIN_WEEK_SHORT_DESC,
       D.FIN_YEAR_NO ,
       E.SUPPLIER_NO  ,
       E.SUPPLIER_NAME            ,
       E.SUPPLIER_LONG_DESC,
       F.PO_NO            ,
       A.ITEM_NO            ,
       A.ITEM_DESC,      
       A.ITEM_LONG_DESC,
       A.FD_PRODUCT_NO          ,
       D.CAL_YEAR_MONTH_NO,
       D.FIN_MONTH_NO,
       F.ORIG_APPROVAL_DATE           ,
       F.NOT_BEFORE_DATE   ,
       F.NOT_AFTER_DATE      ,
       F.Into_Store_date ,       
       A.PACKAGING_DEPT_IND,              
       A.GIFTING_DEPT_IND,      
       A.NON_MERCH_DEPT_IND,   
       A.JV_DEPT_IND, 
       A.NON_CORE_DEPT_IND,               
       A.BUCKET_DEPT_IND,       
       A.BOOK_MAGAZINE_DEPT_IND,
       H.COMMERCIAL_MANAGER_DESC_562,
       H.MERCH_CLASS_DESC_100,
       H.PRODUCT_CLASS_DESC_507,
       G.RECEIVED_QTY
       
  
from ITEM_LIST a, 
     DIM_CALENDAR D, 
     SUPP_LIST E, 
     PURCH_LIST F ,
     FND_RTL_SHIPMENT G,
     UDA_LIST H
 
 where G.RECEIVE_DATE = D.CALENDAR_DATE 
  and G.RECEIVE_DATE = G_DATE
  and G.PO_NO = F.PO_NO 
  and G.ITEM_NO = a.ITEM_NO
  and G.SUPPLIER_NO = E.SUPPLIER_NO
  AND g.shipment_status_code = 'R'
  and G.RECEIVED_QTY > 0 
  and a.SK1_ITEM_NO = H.SK1_ITEM_NO
  and G.seq_no = 1

)
SELECT DISTINCT SK1_PO_NO,
          SK1_ITEM_NO,
          RECEIVE_DATE,
          SHIPMENT_NO,
          FROM_LOC_NO,	
          TO_LOC_NO,	
          FINAL_LOC_NO,	
          SK1_DEPARTMENT_NO,	
          SK1_SUPPLIER_NO	,
          DEPARTMENT_NO	,
          DEPARTMENT_NAME,	
          DEPARTMENT_LONG_DESC,	
          SUBCLASS_NO	,
          SUBCLASS_NAME,	
          SUBCLASS_LONG_DESC,	
          CALENDAR_DATE	,
          FIN_DAY_NO	,
          FIN_WEEK_NO	,
          FIN_WEEK_SHORT_DESC,	
          FIN_YEAR_NO	,
          SUPPLIER_NO	,
          SUPPLIER_NAME,	
          SUPPLIER_LONG_DESC,	
          PO_NO	,
          ITEM_NO,	
          ITEM_DESC,	
          ITEM_LONG_DESC,	
          FD_PRODUCT_NO	,
          CAL_YEAR_MONTH_NO,	
          FIN_MONTH_NO	,
          ORIG_APPROVAL_DATE,	
          NOT_BEFORE_DATE	,
          NOT_AFTER_DATE	,
          INTO_STORE_DATE	,
          PACKAGING_DEPT_IND,	
          GIFTING_DEPT_IND	,
          NON_MERCH_DEPT_IND,	
          JV_DEPT_IND	,
          NON_CORE_DEPT_IND,	
          BUCKET_DEPT_IND	,
          BOOK_MAGAZINE_DEPT_IND,	
          COMMERCIAL_MANAGER_DESC_562,	
          MERCH_CLASS_DESC_100	,
          PRODUCT_CLASS_DESC_507,	
          RECEIVED_QTY,
          G_DATE last_updated_date
FROM SELALL;

g_recs_read:=SQL%ROWCOUNT;
g_recs_inserted:=dwh_log.get_merge_insert_count;
g_recs_updated:=dwh_log.get_merge_update_count(SQL%ROWCOUNT);
COMMIT;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    l_text := 'TRUNCATE TABLE MART_FOODS_SUPPLIER_WLCURREX' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

EXECUTE IMMEDIATE('TRUNCATE TABLE DWH_DATAFIX.MART_FOODS_SUPPLIER_WLCURREX');
COMMIT;

INSERT /*+ APPEND */ INTO DWH_DATAFIX.MART_FOODS_SUPPLIER_WLCURREX 
WITH SELEXT AS (
                select * from  DWH_DATAFIX.MART_FOODS_SUPPLIER_WLamend
                MINUS
                select * from  DWH_DATAFIX.MART_FOODS_SUPPLIER_WLCURR)
SELECT DISTINCT A.*
FROM DWH_PERFORMANCE.MART_FOODS_SUPPLIER A, SELEXT B
WHERE A.SK1_ITEM_NO = B.SK1_ITEM_NO
AND A.RECEIVE_DATE = B.RECEIVE_DATE
AND A.SHIPMENT_NO = B.SHIPMENT_NO
AND A.SK1_PO_NO  = B.SK1_PO_NO;
g_recs_read:=SQL%ROWCOUNT;
g_recs_inserted:=dwh_log.get_merge_insert_count;
g_recs_updated:=dwh_log.get_merge_update_count(SQL%ROWCOUNT);
COMMIT;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||0;
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
end wh_prf_corp_265U_WLCURR;
