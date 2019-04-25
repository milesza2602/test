--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_850U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_850U" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN
       )
AS
  --**************************************************************************************************
  --  Date:        August 2014
  --  Author:      W Lyttle
  --  Purpose:     Supply Chain Reports - part 1 of mart creation
  --  Tables:      Input  - fnd_rtl_allocation
  --               Output - fnd_alloc_tracker_alloc
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
  /*
  step 1
      WITH    SELCAL AS
            (
         SELECT MIN(THIS_WEEK_START_DATE)  START_DATE, MAX(CALENDAR_DATE)  END_DATE
            FROM DIM_CALENDAR
            WHERE CALENDAR_DATE <= (SELECT TODAY_DATE FROM DIM_CONTROL)
                 AND CALENDAR_DATE >= (SELECT TODAY_DATE - 15 FROM DIM_CONTROL)
             --    AND CALENDAR_DATE = '5 AUG 2014'
            )
       --  ,
      --      SElalloc AS 
           --       (   
        SELECT COUNT(*) FROM (
                   SELECT    
                   /*+ full(FRA)  parallel(FRA,4)  */
      /*          DISTINCT TO_LOC_NO, RELEASE_DATE,  ITEM_NO,START_DATE , END_DATE
                   FROM FND_RTL_allocation fra, SELCAL SC
                            where RELEASE_DATE BETWEEN START_DATE AND END_DATE
                            */
  --**************************************************************************************************
  g_forall_limit  INTEGER := dwh_constants.vc_forall_limit;
  g_recs_read     INTEGER := 0;
  g_recs_inserted INTEGER := 0;
  g_error_count   NUMBER  := 0;
  g_error_index   NUMBER  := 0;
  g_date DATE;
  g_start_date DATE;
  g_rec_out fnd_alloc_tracker_alloc%rowtype;
  G_CNT_DATE       INTEGER := 0;
  G_CNT_ITEM       INTEGER := 0;
  G_CNT_ALLOC      INTEGER := 0;
  G_FILLRATE_QTY   NUMBER;
  G_ORIG_ALLOC_QTY NUMBER;
  G_CNT_RECS       INTEGER := 0;
 -- p_from_loc_no integer := 0;
 -- p_to_loc_no integer := 0;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_FND_CORP_850U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOADS supply chain report - STEP 1';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
IS
  TABLE OF fnd_alloc_tracker_alloc%rowtype INDEX BY binary_integer;
type tbl_array_u
IS
  TABLE OF fnd_alloc_tracker_alloc%rowtype INDEX BY binary_integer;
  a_tbl_insert tbl_array_i;
  a_empty_set_i tbl_array_i;
  --**************************************************************************************************
  -- Main process
  --**************************************************************************************************
BEGIN
          IF p_forall_limit IS NOT NULL AND p_forall_limit > dwh_constants.vc_forall_minimum THEN
            g_forall_limit  := p_forall_limit;
          END IF;
          p_success := false;
          l_text    := dwh_constants.vc_log_draw_line;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          l_text := 'LOAD supply chain report - STEP 1 STARTED AT '|| TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
          dwh_lookup.dim_control(g_date);
          l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

           execute immediate('truncate table DWH_FOUNDATION.TEMP_MART_SHIPMENTS_SUPPCHN');
           commit;

          l_text := 'truncate table DWH_FOUNDATION.TEMP_MART_SHIPMENTS_SUPPCHN' ;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
-- RELEASE_DATE ON FND_RTL_ALLOCATION WHEN NEW THRU THEN ALLOCATION_DATE ON NEW FEED.
        execute immediate 'alter session set workarea_size_policy=manual';
        execute immediate 'alter session set sort_area_size=100000000';
        EXECUTE immediate 'alter session enable parallel dml';
      INSERT /*+ APPEND */
        INTO DWH_FOUNDATION.TEMP_MART_SHIPMENTS_SUPPCHN

        WITH    SELCAL AS
            (
         SELECT MIN(THIS_WEEK_START_DATE)  START_DATE, MAX(CALENDAR_DATE)  END_DATE
            FROM DIM_CALENDAR
            WHERE CALENDAR_DATE <= (SELECT TODAY_DATE FROM DIM_CONTROL)
                 AND CALENDAR_DATE >= (SELECT TODAY_DATE - 15 FROM DIM_CONTROL)
                 AND CALENDAR_DATE = '5 AUG 2014'
            )
         ,
            SElalloc AS 
                  (   
     --   SELECT COUNT(*) FROM (
                   SELECT    
                   /*+ full(FRA)  parallel(FRA,4)  */
                DISTINCT TO_LOC_NO, RELEASE_DATE,  ITEM_NO,START_DATE , END_DATE
                   FROM FND_RTL_allocation fra, SELCAL SC
                            where RELEASE_DATE BETWEEN START_DATE AND END_DATE
                                                                                                                                          --- insert filter - date period
                                                                                                                     --                  where ALLOC_NO = 113860406
                                                                                                                                            --    OR DU_ID =  '13893018765637001190'
                                                                                                                                  --     WHERE DU_ID in( '13893018765637001190', '13893018765651004440','96204867529010004050'            )
                                                                                                                            --           and item_no in (6009178922538            ,6009178922545            ,6009178922552            ,6009184379432            ,6009184379449            ,6009182738774, 9319885860595            )
                   -- 9544572 RECS, 2.08MINS
                   )
                  ,
               selship as 
                (   
         --SELECT COUNT(*) FROM (
                select 
                /*+ full(frts)  */ 
                distinct  frts.du_id, frts.final_loc_no, sa.release_date
                from fnd_rtl_shipment frts  
          --      inner join dim_location l 
           --              on l.location_no = frts.final_loc_no 
           --               and l.loc_type = 'S' 
                 inner join selalloc sa 
                           on sa.item_no =  frts.item_no 
                           and sa.to_loc_no = frts.to_loc_no 
                           and sa.release_date <= frts.ship_date
                 WHERE FRTS.SHIP_DATE >= START_DATE
                                             ),
                   
                seldu as 
                (   
                SELECT  /*+ full(frs)  */ 
                 frs.*, ss.release_date
                   FROM FND_RTL_SHIPMENT frs, selship ss
                   --- insert filter - date period
                   where frs.du_id = ss.du_id
                   --- NEED TO LIMIT TO MOST RECENT DU_ID
                   ),
                   selrnk as (
      SELECT  se.SHIPMENT_NO ,
                                se.SEQ_NO ,
                                se.ITEM_NO ,
                                se.SUPPLIER_NO ,
                                se.PO_NO ,
                                se.SDN_NO ,
                                se.ASN_ID ,
                                se.SHIP_DATE ,
                                se.RECEIVE_DATE ,
                                se.SHIPMENT_STATUS_CODE ,
                                se.INV_MATCH_STATUS_CODE ,
                                se.INV_MATCH_DATE ,
                                se.TO_LOC_NO ,
                                se.FROM_LOC_NO ,
                                se.COURIER_ID ,
                                se.EXT_REF_IN_ID ,
                                se.DIST_NO ,
                                se.DIST_TYPE ,
                                se.REF_ITEM_NO ,
                                se.TSF_ALLOC_NO ,
                                se.TSF_PO_LINK_NO ,
                                se.IBT_TYPE ,
                                se.SIMS_REF_ID ,
                               se.DU_ID,
                                se.SHIPMENT_LINE_STATUS_CODE ,
                                se.RECEIVED_QTY ,
                                se.COST_PRICE ,
                                se.REG_RSP ,
                                se.ASN_QTY ,
                                se.ACTUAL_MASS ,
                                se.CARTON_STATUS_CODE ,
                                se.CARTON_STATUS_DESC ,
                                se.SDN_QTY ,
                                se.PO_NOT_BEFORE_DATE ,
                                se.CONTRACT_NO ,
                                se.ACTL_RCPT_DATE ,
                                se.SUPP_PACK_SIZE ,
                                se.CANCELLED_QTY ,
                                se.AUTO_RECEIPT_CODE ,
                                se.SOURCE_DATA_STATUS_CODE ,
                                se.LAST_UPDATED_DATE ,
                                se.TO_LOC_DEBT_COMM_PERC ,
                                se.FINAL_LOC_NO ,
                                se.FROM_LOC_DEBT_COMM_PERC ,
                                se.release_date, 
                                                  DENSE_rank() over (partition BY DU_ID, ITEM_NO order by DU_ID, ITEM_NO, SHIPMENT_NO)  RNK
                                                FROM seldu se 
                ) ,
                selship2 as  (
                select SR.SHIPMENT_NO, SR.SEQ_NO, SR.ITEM_NO, SR.SUPPLIER_NO, SR.PO_NO, SR.SDN_NO, SR.ASN_ID
                , SR.SHIP_DATE, SR.RECEIVE_DATE, SR.SHIPMENT_STATUS_CODE, SR.INV_MATCH_STATUS_CODE, 
                SR.INV_MATCH_DATE, SR.TO_LOC_NO, SR.FROM_LOC_NO, SR.COURIER_ID, SR.EXT_REF_IN_ID, 
                SR.DIST_NO, SR.DIST_TYPE, SR.REF_ITEM_NO, SR.TSF_ALLOC_NO, SR.TSF_PO_LINK_NO, 
                SR.IBT_TYPE, SR.SIMS_REF_ID, SR.DU_ID, SR.SHIPMENT_LINE_STATUS_CODE, SR.RECEIVED_QTY, 
                SR.COST_PRICE, SR.REG_RSP, SR.ASN_QTY, SR.ACTUAL_MASS, SR.CARTON_STATUS_CODE, 
                SR.CARTON_STATUS_DESC, SR.SDN_QTY, SR.PO_NOT_BEFORE_DATE, SR.CONTRACT_NO, 
                SR.ACTL_RCPT_DATE, SR.SUPP_PACK_SIZE, SR.CANCELLED_QTY, SR.AUTO_RECEIPT_CODE, 
                SR.SOURCE_DATA_STATUS_CODE, SR.LAST_UPDATED_DATE, SR.TO_LOC_DEBT_COMM_PERC, 
                SR.FINAL_LOC_NO, SR.FROM_LOC_DEBT_COMM_PERC, SR.RELEASE_DATE, 
                 fra.trunk_ind, fra.alloc_no, case when fra.po_no is null then 'WH' ELSE 'XD' end supply_chain_code, 
                 to_number(SUBSTR(fra.wh_no,1,3))  first_dc_no, 
                 SR.RNK ship_rank,
                 dl.loc_type
                from selrnk sr
                left outer join   fnd_rtl_allocation fra
                on  ((fra.alloc_no = sr.dist_no 
                and fra.item_no = sr.item_no
                and fra.to_loc_no = sr.to_loc_no
                and fra.po_no is not null
                and fra.trunk_ind = 0)
                or (fra.alloc_no = sr.tsf_alloc_no 
                and fra.item_no = sr.item_no
                and fra.to_loc_no = sr.to_loc_no))
                join dim_location dl
                on dl.location_no = sr.final_loc_no
                )
                     SELECT tmp.SHIPMENT_NO ,tmp. SEQ_NO ,tmp. ITEM_NO ,tmp. SUPPLIER_NO ,tmp. PO_NO ,tmp. SDN_NO ,tmp. ASN_ID ,tmp. SHIP_DATE ,
tmp. RECEIVE_DATE ,tmp. SHIPMENT_STATUS_CODE ,tmp. INV_MATCH_STATUS_CODE ,tmp. INV_MATCH_DATE ,tmp. TO_LOC_NO ,tmp. FROM_LOC_NO ,
tmp. COURIER_ID ,tmp. EXT_REF_IN_ID ,tmp. DIST_NO ,tmp. DIST_TYPE ,tmp. REF_ITEM_NO ,tmp. TSF_ALLOC_NO ,tmp. TSF_PO_LINK_NO ,
tmp. IBT_TYPE ,tmp. SIMS_REF_ID ,tmp. DU_ID ,tmp. SHIPMENT_LINE_STATUS_CODE ,tmp. RECEIVED_QTY ,tmp. COST_PRICE ,tmp. REG_RSP ,
tmp. ASN_QTY ,tmp. ACTUAL_MASS ,tmp. CARTON_STATUS_CODE ,tmp. CARTON_STATUS_DESC ,tmp. SDN_QTY ,tmp. PO_NOT_BEFORE_DATE ,
tmp. CONTRACT_NO ,tmp. ACTL_RCPT_DATE ,tmp. SUPP_PACK_SIZE ,tmp. CANCELLED_QTY ,tmp. AUTO_RECEIPT_CODE ,tmp. SOURCE_DATA_STATUS_CODE ,
tmp. LAST_UPDATED_DATE ,tmp. TO_LOC_DEBT_COMM_PERC ,tmp. FINAL_LOC_NO ,tmp. FROM_LOC_DEBT_COMM_PERC ,tmp. RELEASE_DATE ,
tmp. TRUNK_IND ,tmp. ALLOC_NO ,tmp. SUPPLY_CHAIN_CODE ,tmp. FIRST_DC_NO ,tmp. SHIP_RANK ,tmp. LOC_TYPE ,
                max(case when ship_rank=1 then receive_date else null end)                                                                   date_received_by_1st_xdock, 
                max(case when ship_rank=2 then ship_date else null end)                                                                      date_despatched_by_1st_xdock, 
                max(case when ship_rank=2 and dl.loc_type='W' then receive_date else null end)                                   date_received_by_2nd_xdock, 
                max(case when ship_rank=3 then ship_date else null end)                                                                      date_despatched_by_2nd_xdock, 
                max(case when (ship_rank=2 and dl.loc_type='S') or ship_rank=3 then receive_date else null end)       into_store_date 
           FROM selship2 TMP, dim_location dl
           where dl.location_no = to_loc_no
          group by tmp.SHIPMENT_NO ,tmp. SEQ_NO ,tmp. ITEM_NO ,tmp. SUPPLIER_NO ,tmp. PO_NO ,tmp. SDN_NO ,tmp. ASN_ID ,tmp. SHIP_DATE ,
tmp. RECEIVE_DATE ,tmp. SHIPMENT_STATUS_CODE ,tmp. INV_MATCH_STATUS_CODE ,tmp. INV_MATCH_DATE ,tmp. TO_LOC_NO ,tmp. FROM_LOC_NO ,
tmp. COURIER_ID ,tmp. EXT_REF_IN_ID ,tmp. DIST_NO ,tmp. DIST_TYPE ,tmp. REF_ITEM_NO ,tmp. TSF_ALLOC_NO ,tmp. TSF_PO_LINK_NO ,
tmp. IBT_TYPE ,tmp. SIMS_REF_ID ,tmp. DU_ID ,tmp. SHIPMENT_LINE_STATUS_CODE ,tmp. RECEIVED_QTY ,tmp. COST_PRICE ,tmp. REG_RSP ,
tmp. ASN_QTY ,tmp. ACTUAL_MASS ,tmp. CARTON_STATUS_CODE ,tmp. CARTON_STATUS_DESC ,tmp. SDN_QTY ,tmp. PO_NOT_BEFORE_DATE ,
tmp. CONTRACT_NO ,tmp. ACTL_RCPT_DATE ,tmp. SUPP_PACK_SIZE ,tmp. CANCELLED_QTY ,tmp. AUTO_RECEIPT_CODE ,tmp. SOURCE_DATA_STATUS_CODE ,
tmp. LAST_UPDATED_DATE ,tmp. TO_LOC_DEBT_COMM_PERC ,tmp. FINAL_LOC_NO ,tmp. FROM_LOC_DEBT_COMM_PERC ,tmp. RELEASE_DATE ,
tmp. TRUNK_IND ,tmp. ALLOC_NO ,tmp. SUPPLY_CHAIN_CODE ,tmp. FIRST_DC_NO ,tmp. SHIP_RANK ,tmp. LOC_TYPE 
                  ;
               
           
        g_recs_read    :=SQL%ROWCOUNT;
        g_recs_inserted:=SQL%ROWCOUNT;
        COMMIT;

         l_text := 'records inserted='||g_recs_inserted;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
  l_text := dwh_constants.vc_log_time_completed ||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read||' '||g_start_date||' TO '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted||' '||g_start_date||' TO '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := true;
  
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;
WHEN OTHERS THEN
  l_message := dwh_constants.vc_err_mm_other||SQLCODE||' '||sqlerrm;
  dwh_log.record_error(l_module_name,SQLCODE,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := false;
  raise;

END wh_fnd_corp_850u;
