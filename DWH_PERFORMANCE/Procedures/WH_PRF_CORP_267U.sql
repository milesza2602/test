--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_267U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_267U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2014
--  Author:      Quentin Smit / Karna
--  Purpose:     Load Mart Foods Supplier mart
--
--  Tables:      Input  - rtl_zone_item_supp_hist
--                        rtl_loc_item_dy_st_ord
--                        rtl_location_item
--                        rtl_zone_item_om
--                        rtl_depot_item_wk
--               Output - MART_FOODS_SUPPLIER_PO
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_267U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE FOODS SUPPLIER MART';
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

    l_text := 'LOAD OF MART_FOODS_SUPPLIER_PO (PO ONLY) STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

 --**************************************************************************************************
-- Look up batch date from dim_control
--/*+ APPEND USE_HASH(rtl_mart ,mer_mart)*/
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

MERGE  INTO DWH_PERFORMANCE.MART_FOODS_SUPPLIER_PO rtl_mart USING
(
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
    dd.sk1_department_no
  FROM DIM_ITEM DI,
       DIM_DEPARTMENT DD where DD.BUSINESS_UNIT_NO = 50
  and di.sk1_department_no = dd.sk1_department_no
--  and di.department_no in (40,64,88)
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
    dz.sk1_zone_group_zone_no
  FROM DIM_LOCATION DL, DIM_ZONE DZ
    where DL.SK1_FD_ZONE_GROUP_ZONE_NO = DZ.SK1_ZONE_GROUP_ZONE_NO
    and DZ.ZONE_GROUP_NO = 1
    AND STOCK_HOLDING_IND = 1
    and DL.LOC_TYPE = 'W'
  )
  ,

  supp_list as
  (SELECT SUPPLIER_NO,SK1_SUPPLIER_NO,SUPPLIER_NAME,SUPPLIER_LONG_DESC FROM DIM_SUPPLIER),


  PURCH_LIST as
  ( select dp.po_no, dp.sk1_po_no, dp.orig_approval_date, dp.po_status_code, dp.not_before_date, dp.not_after_date, dp.into_store_date, dp.last_updated_date
     from DIM_PURCHASE_ORDER DP)
  ,

 UDA_LIST as
  (
select SK1_ITEM_NO,
  COMMERCIAL_MANAGER_DESC_562,
  MERCH_CLASS_DESC_100,
  product_class_desc_507
  from DIM_ITEM_UDA   -- where COMMERCIAL_MANAGER_DESC_562 not like '%C&H%'
                    --  and  MERCH_CLASS_DESC_100 not  like 'C&H%'
                   --   and PRODUCT_CLASS_DESC_507  not  like 'C&H%'

   )

--PO_SUPCHAIN as (
select /*+ parallel(c,4) full(c) */
          C.SK1_PO_NO ,
          C.SK1_LOCATION_NO ,
          C.SK1_ITEM_NO   ,
          C.TRAN_DATE,
          B.SK1_ZONE_GROUP_ZONE_NO    ,
          B.SK1_FD_ZONE_GROUP_ZONE_NO ,
          A.SK1_DEPARTMENT_NO,
          E.SK1_SUPPLIER_NO         ,
          C.SK1_SUPPLY_CHAIN_NO ,
          A.DEPARTMENT_NO,
          A.DEPARTMENT_NAME    ,
          A.DEPARTMENT_LONG_DESC,
          B.DISTRICT_NAME,
          A.SUBCLASS_NO  ,
          A.SUBCLASS_NAME ,
          A.SUBCLASS_LONG_DESC ,
          D.CALENDAR_DATE,
          D.FIN_DAY_NO,
          D.FIN_WEEK_NO,
          D.FIN_WEEK_SHORT_DESC,
          D.FIN_YEAR_NO ,
          B.LOCATION_NO   ,
          B.LOCATION_NAME,
          B.LOCATION_LONG_DESC ,
          B.ZONE_NO ,
          B.ZONE_DESCRIPTION         ,
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
          C.CANCEL_CODE ,
          F.ORIG_APPROVAL_DATE           ,
          F.NOT_BEFORE_DATE   ,
          F.NOT_AFTER_DATE      ,
          F.Into_Store_date ,
          C.SHORTS_CASES               ,
          C.REJECTED_CASES ,
          C.ORIGINAL_PO_CASES ,
          C.ORIGINAL_PO_SELLING              ,
          C.AMENDED_PO_CASES ,
          C.AMENDED_PO_SELLING ,
          C.PO_GRN_CASES ,
          C.PO_GRN_SELLING         ,
          C.SHORTS_SELLING           ,
          C.SHORTS_QTY,
          C.PO_GRN_QTY  ,
          C.ACTL_GRN_CASES  ,
          C.ACTL_GRN_QTY  ,
          C.FILLRATE_FD_PO_GRN_QTY ,
          C.FILLRATE_FD_LATEST_PO_QTY ,
          A.PACKAGING_DEPT_IND,
          A.GIFTING_DEPT_IND,
          A.NON_MERCH_DEPT_IND,
          A.JV_DEPT_IND,
          A.NON_CORE_DEPT_IND,
          A.BUCKET_DEPT_IND,
          A.BOOK_MAGAZINE_DEPT_IND,
          B.STOCK_HOLDING_IND,
          B.LOC_TYPE,
          H.COMMERCIAL_MANAGER_DESC_562,
          H.MERCH_CLASS_DESC_100,
          H.PRODUCT_CLASS_DESC_507,
          C.CANCEL_PO_CASES,
          C.CANCEL_PO_SELLING,
          C.ORIGINAL_PO_QTY,
          C.CANCEL_PO_QTY,
          C.AMENDED_PO_QTY,
          D.MONTH_SHORT_NAME

from ITEM_LIST a, LOC_LIST B, RTL_PO_SUPCHAIN_LOC_ITEM_DY C, DIM_CALENDAR D, SUPP_LIST E, PURCH_LIST F ,UDA_LIST H

   where c.po_ind = 1
      AND C.tran_date       >= G_DATE
      and C.TRAN_DATE       = D.CALENDAR_DATE
      and C.SK1_SUPPLIER_NO = E.SK1_SUPPLIER_NO
      and C.SK1_ITEM_NO     = a.SK1_ITEM_NO
      and C.SK1_LOCATION_NO = B.SK1_LOCATION_NO
      and C.SK1_PO_NO       = F.SK1_PO_NO
      and C.SK1_ITEM_NO     = H.SK1_ITEM_NO

/*   where C.PO_IND = 1             REMOVED 11 FEBRUARY 2015
    AND C.tran_date = G_DATE
    and D.CALENDAR_DATE = C.TRAN_DATE
    and a.SK1_SUPPLIER_NO = c.SK1_SUPPLIER_NO
    and a.sk1_supplier_no = e.sk1_supplier_no
    and a.SK1_ITEM_NO = C.SK1_ITEM_NO
    and B.SK1_LOCATION_NO = C.SK1_LOCATION_NO
    and C.SK1_PO_NO = F.SK1_PO_NO
    and a.SK1_ITEM_NO = H.SK1_ITEM_NO
*/

) mer_mart
ON  (mer_mart.SK1_PO_NO               = rtl_mart.SK1_PO_NO
and mer_mart.SK1_LOCATION_NO          = rtl_mart.SK1_LOCATION_NO
and mer_mart.SK1_ITEM_NO              = rtl_mart.SK1_ITEM_NO
and mer_mart.TRAN_DATE                = rtl_mart.TRAN_DATE)
WHEN MATCHED THEN
UPDATE
SET       SK1_ZONE_GROUP_ZONE_NO      = mer_mart.SK1_ZONE_GROUP_ZONE_NO,
          SK1_FD_ZONE_GROUP_ZONE_NO   = mer_mart.SK1_FD_ZONE_GROUP_ZONE_NO,
          SK1_DEPARTMENT_NO           = mer_mart.SK1_DEPARTMENT_NO,
          SK1_SUPPLIER_NO             = mer_mart.SK1_SUPPLIER_NO,
          SK1_SUPPLY_CHAIN_NO         = mer_mart.SK1_SUPPLY_CHAIN_NO,
          DEPARTMENT_NO               = mer_mart.DEPARTMENT_NO,
          DEPARTMENT_NAME             = mer_mart.DEPARTMENT_NAME,
          DEPARTMENT_LONG_DESC        = mer_mart.DEPARTMENT_LONG_DESC,
          DISTRICT_NAME               = mer_mart.DISTRICT_NAME,
          SUB_CLASS_NO                = mer_mart.SUBCLASS_NO,
          SUB_CLASS_NAME              = mer_mart.SUBCLASS_NAME,
          SUBCLASS_LONG_DESC          = mer_mart.SUBCLASS_LONG_DESC,
          CALENDAR_DATE               = mer_mart.CALENDAR_DATE,
          FIN_DAY_NO                  = mer_mart.FIN_DAY_NO,
          FIN_WEEK_NO                 = mer_mart.FIN_WEEK_NO,
          FIN_WEEK_SHORT_DESC         = mer_mart.FIN_WEEK_SHORT_DESC,
          FIN_YEAR_NO                 = mer_mart.FIN_YEAR_NO,
          LOCATION_NO                 = mer_mart.LOCATION_NO,
          LOCATION_NAME               = mer_mart.LOCATION_NAME,
          LOCATION_LONG_DESC          = mer_mart.LOCATION_LONG_DESC,
          ZONE_NO                     = mer_mart.ZONE_NO,
          ZONE_DESCRIPTION            = mer_mart.ZONE_DESCRIPTION,
          SUPPLIER_NO                 = mer_mart.SUPPLIER_NO,
          SUPPLIER_NAME               = mer_mart.SUPPLIER_NAME,
          SUPPLIER_LONG_DESC          = mer_mart.SUPPLIER_LONG_DESC,
          PO_NO                       = mer_mart.PO_NO,
          ITEM_NO                     = mer_mart.ITEM_NO,
          ITEM_DESC                   = mer_mart.ITEM_DESC,
          ITEM_LONG_DESC              = mer_mart.ITEM_LONG_DESC,
          FD_PRODUCT_NO               = mer_mart.FD_PRODUCT_NO,
          CAL_YEAR_MONTH_NO           = mer_mart.CAL_YEAR_MONTH_NO,
          FIN_MONTH_NO                = mer_mart.FIN_MONTH_NO,
          CANCEL_CODE                 = mer_mart.CANCEL_CODE,
          ORIG_APPROVAL_DATE          = mer_mart.ORIG_APPROVAL_DATE,
          NOT_BEFORE_DATE             = mer_mart.NOT_BEFORE_DATE,
          NOT_AFTER_DATE              = mer_mart.NOT_AFTER_DATE,
          INTO_STORE_DATE             = mer_mart.INTO_STORE_DATE,
          SHORTS_CASES                = mer_mart.SHORTS_CASES,
          REJECTED_CASES              = mer_mart.REJECTED_CASES,
          ORIGINAL_PO_CASES           = mer_mart.ORIGINAL_PO_CASES,
          ORIGINAL_PO_SELLING         = mer_mart.ORIGINAL_PO_SELLING,
          AMENDED_PO_CASES            = mer_mart.AMENDED_PO_CASES,
          AMENDED_PO_SELLING          = mer_mart.AMENDED_PO_SELLING,
          PO_GRN_CASES                = mer_mart.PO_GRN_CASES,
          PO_GRN_SELLING              = mer_mart.PO_GRN_SELLING,
          SHORTS_SELLING              = mer_mart.SHORTS_SELLING,
          SHORTS_QTY                  = mer_mart.SHORTS_QTY,
          PO_GRN_QTY                  = mer_mart.PO_GRN_QTY,
          ACTL_GRN_CASES              = mer_mart.ACTL_GRN_CASES,
          ACTL_GRN_QTY                = mer_mart.ACTL_GRN_QTY,
          FILLRATE_FD_PO_GRN_QTY      = mer_mart.FILLRATE_FD_PO_GRN_QTY,
          FILLRATE_FD_LATEST_PO_QTY   = mer_mart.FILLRATE_FD_LATEST_PO_QTY,
          PACKAGING_DEPT_IND          = mer_mart.PACKAGING_DEPT_IND,
          GIFTING_DEPT_IND            = mer_mart.GIFTING_DEPT_IND,
          NON_MERCH_DEPT_IND          = mer_mart.NON_MERCH_DEPT_IND,
          JV_DEPT_IND                 = mer_mart.JV_DEPT_IND,
          NON_CORE_DEPT_IND           = mer_mart.NON_CORE_DEPT_IND,
          BUCKET_DEPT_IND             = mer_mart.BUCKET_DEPT_IND,
          BOOK_MAGAZINE_DEPT_IND      = mer_mart.BOOK_MAGAZINE_DEPT_IND,
          STOCK_HOLDING_IND           = mer_mart.STOCK_HOLDING_IND,
          LOC_TYPE                    = mer_mart.LOC_TYPE,
          COMMERCIAL_MANAGER_DESC_562 = mer_mart.COMMERCIAL_MANAGER_DESC_562,
          MERCH_CLASS_DESC_100        = mer_mart.MERCH_CLASS_DESC_100,
          PRODUCT_CLASS_DESC_507      = mer_mart.PRODUCT_CLASS_DESC_507,
          CANCEL_PO_CASES             = mer_mart.CANCEL_PO_CASES,
          CANCEL_PO_SELLING           = mer_mart.CANCEL_PO_SELLING,
          last_updated_date           = g_date,
          ORIGINAL_PO_QTY             = mer_mart.ORIGINAL_PO_QTY,
          CANCEL_PO_QTY               = mer_mart.CANCEL_PO_QTY,
          AMENDED_PO_QTY              = mer_mart.AMENDED_PO_QTY,
          MONTH_SHORT_NAME            = mer_mart.MONTH_SHORT_NAME
WHEN NOT MATCHED THEN
INSERT
(         SK1_PO_NO,
          SK1_LOCATION_NO,
          SK1_ITEM_NO,
          TRAN_DATE,
          SK1_ZONE_GROUP_ZONE_NO,
          SK1_FD_ZONE_GROUP_ZONE_NO,
          SK1_DEPARTMENT_NO,
          SK1_SUPPLIER_NO,
          SK1_SUPPLY_CHAIN_NO,
          DEPARTMENT_NO,
          DEPARTMENT_NAME,
          DEPARTMENT_LONG_DESC,
          DISTRICT_NAME,
          SUB_CLASS_NO,
          SUB_CLASS_NAME,
          SUBCLASS_LONG_DESC,
          CALENDAR_DATE,
          FIN_DAY_NO,
          FIN_WEEK_NO,
          FIN_WEEK_SHORT_DESC,
          FIN_YEAR_NO,
          LOCATION_NO,
          LOCATION_NAME,
          LOCATION_LONG_DESC,
          ZONE_NO,
          ZONE_DESCRIPTION,
          SUPPLIER_NO,
          SUPPLIER_NAME,
          SUPPLIER_LONG_DESC,
          PO_NO,
          ITEM_NO,
          ITEM_DESC,
          ITEM_LONG_DESC,
          FD_PRODUCT_NO,
          CAL_YEAR_MONTH_NO,
          FIN_MONTH_NO,
          CANCEL_CODE,
          ORIG_APPROVAL_DATE,
          NOT_BEFORE_DATE,
          NOT_AFTER_DATE,
          INTO_STORE_DATE,
          SHORTS_CASES,
          REJECTED_CASES,
          ORIGINAL_PO_CASES,
          ORIGINAL_PO_SELLING,
          AMENDED_PO_CASES,
          AMENDED_PO_SELLING,
          PO_GRN_CASES,
          PO_GRN_SELLING,
          SHORTS_SELLING,
          SHORTS_QTY,
          PO_GRN_QTY,
          ACTL_GRN_CASES,
          ACTL_GRN_QTY,
          FILLRATE_FD_PO_GRN_QTY,
          FILLRATE_FD_LATEST_PO_QTY,
          PACKAGING_DEPT_IND,
          GIFTING_DEPT_IND,
          NON_MERCH_DEPT_IND,
          JV_DEPT_IND,
          NON_CORE_DEPT_IND,
          BUCKET_DEPT_IND,
          BOOK_MAGAZINE_DEPT_IND,
          STOCK_HOLDING_IND,
          LOC_TYPE,
          COMMERCIAL_MANAGER_DESC_562,
          MERCH_CLASS_DESC_100,
          PRODUCT_CLASS_DESC_507,
          CANCEL_PO_CASES,
          CANCEL_PO_SELLING,
          last_updated_date,
          ORIGINAL_PO_QTY,
          CANCEL_PO_QTY,
          AMENDED_PO_QTY,
          MONTH_SHORT_NAME
          )
  values
(         mer_mart.SK1_PO_NO,
          mer_mart.SK1_LOCATION_NO,
          mer_mart.SK1_ITEM_NO,
          mer_mart.TRAN_DATE,
          mer_mart.SK1_ZONE_GROUP_ZONE_NO,
          mer_mart.SK1_FD_ZONE_GROUP_ZONE_NO,
          mer_mart.SK1_DEPARTMENT_NO,
          mer_mart.SK1_SUPPLIER_NO,
          mer_mart.SK1_SUPPLY_CHAIN_NO,
          mer_mart.DEPARTMENT_NO,
          mer_mart.DEPARTMENT_NAME,
          mer_mart.DEPARTMENT_LONG_DESC,
          mer_mart.DISTRICT_NAME,
          mer_mart.SUBCLASS_NO,
          mer_mart.SUBCLASS_NAME,
          mer_mart.SUBCLASS_LONG_DESC,
          mer_mart.CALENDAR_DATE,
          mer_mart.FIN_DAY_NO,
          mer_mart.FIN_WEEK_NO,
          mer_mart.FIN_WEEK_SHORT_DESC,
          mer_mart.FIN_YEAR_NO,
          mer_mart.LOCATION_NO,
          mer_mart.LOCATION_NAME,
          mer_mart.LOCATION_LONG_DESC,
          mer_mart.ZONE_NO,
          mer_mart.ZONE_DESCRIPTION,
          mer_mart.SUPPLIER_NO,
          mer_mart.SUPPLIER_NAME,
          mer_mart.SUPPLIER_LONG_DESC,
          mer_mart.PO_NO,
          mer_mart.ITEM_NO,
          mer_mart.ITEM_DESC,
          mer_mart.ITEM_LONG_DESC,
          mer_mart.FD_PRODUCT_NO,
          mer_mart.CAL_YEAR_MONTH_NO,
          mer_mart.FIN_MONTH_NO,
          mer_mart.CANCEL_CODE,
          mer_mart.ORIG_APPROVAL_DATE,
          mer_mart.NOT_BEFORE_DATE,
          mer_mart.NOT_AFTER_DATE,
          mer_mart.INTO_STORE_DATE,
          mer_mart.SHORTS_CASES,
          mer_mart.REJECTED_CASES,
          mer_mart.ORIGINAL_PO_CASES,
          mer_mart.ORIGINAL_PO_SELLING,
          mer_mart.AMENDED_PO_CASES,
          mer_mart.AMENDED_PO_SELLING,
          mer_mart.PO_GRN_CASES,
          mer_mart.PO_GRN_SELLING,
          mer_mart.SHORTS_SELLING,
          mer_mart.SHORTS_QTY,
          mer_mart.PO_GRN_QTY,
          mer_mart.ACTL_GRN_CASES,
          mer_mart.ACTL_GRN_QTY,
          mer_mart.FILLRATE_FD_PO_GRN_QTY,
          mer_mart.FILLRATE_FD_LATEST_PO_QTY,
          mer_mart.PACKAGING_DEPT_IND,
          mer_mart.GIFTING_DEPT_IND,
          mer_mart.NON_MERCH_DEPT_IND,
          mer_mart.JV_DEPT_IND,
          mer_mart.NON_CORE_DEPT_IND,
          mer_mart.BUCKET_DEPT_IND,
          mer_mart.BOOK_MAGAZINE_DEPT_IND,
          mer_mart.STOCK_HOLDING_IND,
          mer_mart.LOC_TYPE,
          mer_mart.COMMERCIAL_MANAGER_DESC_562,
          mer_mart.MERCH_CLASS_DESC_100,
          mer_mart.PRODUCT_CLASS_DESC_507,
          mer_mart.CANCEL_PO_CASES,
          mer_mart.CANCEL_PO_SELLING,
          g_date,
          mer_mart.ORIGINAL_PO_QTY,
          mer_mart.CANCEL_PO_QTY,
          mer_mart.AMENDED_PO_QTY,
          mer_mart.MONTH_SHORT_NAME
          );

g_recs_read:=SQL%ROWCOUNT;
g_recs_inserted:=dwh_log.get_merge_insert_count;
g_recs_updated:=dwh_log.get_merge_update_count(SQL%ROWCOUNT);

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
end wh_prf_corp_267U;
