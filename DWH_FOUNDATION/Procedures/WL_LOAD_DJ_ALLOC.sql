--------------------------------------------------------
--  DDL for Procedure WL_LOAD_DJ_ALLOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WL_LOAD_DJ_ALLOC" 
(p_forall_limit in integer,p_success out boolean) as
-- **************************************************************************************************
--  Date:        may 2015
--  Author:      wendy lyttle
--  Purpose:     load data
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

g_found              boolean;
g_insert_rec         boolean;
g_invalid_plan_type_no boolean;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WL_LOAD_DJ_ALLOC';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_bam_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_pln_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD data';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'truncate table DWH_FOUNDATION.FND_RTL_ALLOCATION_WL';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate('truncate table DWH_FOUNDATION.FND_RTL_ALLOCATION_WL');
    
    l_text := 'update OF FND_RTL_ALLOCATION FIRST_DC_NO STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--DATA UPDATED FOR PERIOD 23-FEB-15 TO 24-MAY-15    
     INSERT /*+ APPEND */ INTO DWH_FOUNDATION.FND_RTL_ALLOCATION_WL
    with selitm as (select /*+ materialize */ item_no from dim_item where business_unit_no not in (50,70))
    SELECT /*+ PARALLEL(a,4) */ ALLOC_NO
          ,TO_LOC_NO
          ,RELEASE_DATE
          ,PO_NO
          ,WH_NO
          ,a.ITEM_NO
          ,ALLOC_STATUS_CODE
          ,TO_LOC_TYPE
          ,SDN_QTY
          ,ALLOC_QTY
          ,DIST_QTY
          ,APPORTION_QTY
          ,ALLOC_CANCEL_QTY
          ,RECEIVED_QTY
          ,PO_GRN_QTY
          ,EXT_REF_ID
          ,PLANNED_INTO_LOC_DATE
          ,INTO_LOC_DATE
          ,SCALE_PRIORITY_CODE
          ,TRUNK_IND
          ,OVERSTOCK_QTY
          ,PRIORITY1_QTY
          ,SAFETY_QTY
          ,SPECIAL_QTY
          ,ORIG_ALLOC_QTY
          ,ALLOC_LINE_STATUS_CODE
          ,SOURCE_DATA_STATUS_CODE
          ,A.LAST_UPDATED_DATE
          ,REG_RSP_EXCL_VAT
          ,WAC
          ,CHAIN_CODE
          ,dl.WH_PHYSICAL_WH_NO  FIRST_DC_NO
    FROM  fnd_rtl_allocation a, dim_location dl, selitm d
    where a.wh_no        = dl.location_no 
    and a.item_no = d.item_no
    AND release_date between '26/JAN/15' AND '22/FEB/15' ;
    g_recs_read    :=SQL%ROWCOUNT;
    g_recs_INSERTED :=SQL%ROWCOUNT;
    COMMIT;
    l_text := 'period loaded 26/jan/15 and 22/feb/15  - no of recs='||g_recs_INSERTED;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
 
 
    INSERT /*+ APPEND */ INTO DWH_FOUNDATION.FND_RTL_ALLOCATION_WL
    with selitm as (select /*+ materialize */ item_no from dim_item where business_unit_no not in (50,70))
    SELECT /*+ PARALLEL(a,4) */ ALLOC_NO
          ,TO_LOC_NO
          ,RELEASE_DATE
          ,PO_NO
          ,WH_NO
          ,a.ITEM_NO
          ,ALLOC_STATUS_CODE
          ,TO_LOC_TYPE
          ,SDN_QTY
          ,ALLOC_QTY
          ,DIST_QTY
          ,APPORTION_QTY
          ,ALLOC_CANCEL_QTY
          ,RECEIVED_QTY
          ,PO_GRN_QTY
          ,EXT_REF_ID
          ,PLANNED_INTO_LOC_DATE
          ,INTO_LOC_DATE
          ,SCALE_PRIORITY_CODE
          ,TRUNK_IND
          ,OVERSTOCK_QTY
          ,PRIORITY1_QTY
          ,SAFETY_QTY
          ,SPECIAL_QTY
          ,ORIG_ALLOC_QTY
          ,ALLOC_LINE_STATUS_CODE
          ,SOURCE_DATA_STATUS_CODE
          ,A.LAST_UPDATED_DATE
          ,REG_RSP_EXCL_VAT
          ,WAC
          ,CHAIN_CODE
          ,dl.WH_PHYSICAL_WH_NO  FIRST_DC_NO
    FROM  fnd_rtl_allocation a, dim_location dl, selitm d
    where a.wh_no        = dl.location_no 
    and a.item_no = d.item_no
    AND release_date between '23/feb/15' and '22/mar/15' ;
    g_recs_read    :=SQL%ROWCOUNT;
    g_recs_INSERTED :=SQL%ROWCOUNT;
    COMMIT;
    l_text := 'period loaded 23/feb/15 and 22/mar/15  - no of recs='||g_recs_INSERTED;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    INSERT /*+ APPEND */ INTO DWH_FOUNDATION.FND_RTL_ALLOCATION_WL
    with selitm as (select /*+ materialize */ item_no from dim_item where business_unit_no not in (50,70))
    SELECT /*+ PARALLEL(a,4) */ ALLOC_NO
          ,TO_LOC_NO
          ,RELEASE_DATE
          ,PO_NO
          ,WH_NO
          ,a.ITEM_NO
          ,ALLOC_STATUS_CODE
          ,TO_LOC_TYPE
          ,SDN_QTY
          ,ALLOC_QTY
          ,DIST_QTY
          ,APPORTION_QTY
          ,ALLOC_CANCEL_QTY
          ,RECEIVED_QTY
          ,PO_GRN_QTY
          ,EXT_REF_ID
          ,PLANNED_INTO_LOC_DATE
          ,INTO_LOC_DATE
          ,SCALE_PRIORITY_CODE
          ,TRUNK_IND
          ,OVERSTOCK_QTY
          ,PRIORITY1_QTY
          ,SAFETY_QTY
          ,SPECIAL_QTY
          ,ORIG_ALLOC_QTY
          ,ALLOC_LINE_STATUS_CODE
          ,SOURCE_DATA_STATUS_CODE
          ,A.LAST_UPDATED_DATE
          ,REG_RSP_EXCL_VAT
          ,WAC
          ,CHAIN_CODE
          ,dl.WH_PHYSICAL_WH_NO  FIRST_DC_NO
    FROM  fnd_rtl_allocation a, dim_location dl, selitm d
    where a.wh_no        = dl.location_no 
    and a.item_no = d.item_no
    AND release_date between '23/mar/15' and '19/apr/15' ;
    g_recs_read    :=SQL%ROWCOUNT;
    g_recs_INSERTED :=SQL%ROWCOUNT;
    COMMIT;
    l_text := 'period loaded 23/mar/15 and 19/apr/15  - no of recs='||g_recs_INSERTED;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    INSERT /*+ APPEND */ INTO DWH_FOUNDATION.FND_RTL_ALLOCATION_WL
    with selitm as (select /*+ materialize */ item_no from dim_item where business_unit_no not in (50,70))
    SELECT /*+ PARALLEL(a,4) */ ALLOC_NO
          ,TO_LOC_NO
          ,RELEASE_DATE
          ,PO_NO
          ,WH_NO
          ,a.ITEM_NO
          ,ALLOC_STATUS_CODE
          ,TO_LOC_TYPE
          ,SDN_QTY
          ,ALLOC_QTY
          ,DIST_QTY
          ,APPORTION_QTY
          ,ALLOC_CANCEL_QTY
          ,RECEIVED_QTY
          ,PO_GRN_QTY
          ,EXT_REF_ID
          ,PLANNED_INTO_LOC_DATE
          ,INTO_LOC_DATE
          ,SCALE_PRIORITY_CODE
          ,TRUNK_IND
          ,OVERSTOCK_QTY
          ,PRIORITY1_QTY
          ,SAFETY_QTY
          ,SPECIAL_QTY
          ,ORIG_ALLOC_QTY
          ,ALLOC_LINE_STATUS_CODE
          ,SOURCE_DATA_STATUS_CODE
          ,A.LAST_UPDATED_DATE
          ,REG_RSP_EXCL_VAT
          ,WAC
          ,CHAIN_CODE
          ,dl.WH_PHYSICAL_WH_NO  FIRST_DC_NO
    FROM  fnd_rtl_allocation a, dim_location dl, selitm d
    where a.wh_no        = dl.location_no 
    and a.item_no = d.item_no
    AND release_date between '20/apr/15' and '17/may/15 ' ;
    g_recs_read    :=SQL%ROWCOUNT;
    g_recs_INSERTED :=SQL%ROWCOUNT;
    COMMIT;
    l_text := 'period loaded 20/apr/15 and 17/may/15   - no of recs='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    



--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
--DATA LOADED FOR PERIOD 14/APR/15 TO 24/MAY/15 **no_of_weeks=0
--DATA LOADED FOR PERIOD 17/MAR/15 TO 19/APR/15 **no_of_weeks=0
--DATA LOADED FOR PERIOD 17/FEB/15 TO 22/MAR/15 **no_of_weeks=0
--DATA LOADED FOR PERIOD 19/JAN/15 TO 22/FEB/15 **no_of_weeks=1

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
end WL_LOAD_DJ_ALLOC ;
