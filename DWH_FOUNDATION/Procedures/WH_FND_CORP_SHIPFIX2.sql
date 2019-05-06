--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_SHIPFIX2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_SHIPFIX2" 
(p_forall_limit in integer,p_success out boolean)
as
--**************************************************************************************************
--  Date:        AUG 2016
--  Author:      Wendy Lyttle
--  Purpose:     shipments datafix - cal_year 2016
--               1. Get Oracle DBA to copy snapshot from RMSPRD database to BI O2DWH database
--                   RMS.SHIPMENT, RMS.SHIPSKU
--                        to SUPP_SMOT.SHIPMENT, SUPP_SMOT.SHIPSKU
--
--               2. extract into DWH_FOUNDATION.RMS_SHIPMENT_SHIPSKU
--                  from SUPP_SMOT.SHIPMENT, SUPP_SMOT.SHIPSKU
--                  joined on shipment, seq_no
--                  and where ship_date >= '1 jan 2015'
--????????                  and status <> 'D' ?????
--
--               3. extract into DWH_FOUNDATION.RMS_O2DWH_STG_SHIPMENT
--                  from DWH_FOUNDATION.RMS_O2DWH_STG_SHIPMENT, DWH_FOUNDATION.FND_RTL_SHIPMENT
--                  left outer join on shipment, seq_no, item_no
--
--               4. extract into DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX
--                  from DWH_FOUNDATION.RMS_O2DWH_STG_SHIPMENT
--                  the records we want to fix


------------------------------------------------------------------------------
/*Mapping
SYS_SOURCE_BATCH_ID
SYS_SOURCE_SEQUENCE_NO
SYS_LOAD_DATE
SYS_PROCESS_CODE
SYS_LOAD_SYSTEM_NAME
SYS_MIDDLEWARE_BATCH_ID
SYS_PROCESS_MSG
SHIPMENT	SHIPMENT	SHIPMENT_NO	SHIPMENT_NO
SEQ_NO		SEQ_NO	SEQ_NO
ITEM		ITEM_NO	ITEM_NO
SUPPLIER_NO	SUPPLIER_NO
ORDER_NO	PO_NO	PO_NO
BOL_NO	SDN_NO	SDN_NO
ASN	ASN_ID	ASN_ID
SHIP_DATE	SHIP_DATE	SHIP_DATE
RECEIVE_DATE	RECEIVE_DATE	RECEIVE_DATE
STATUS_CODE	SHIPMENT_STATUS_CODE	SHIPMENT_STATUS_CODE
INVC_MATCH_STATUS	INV_MATCH_STATUS_CODE	INV_MATCH_STATUS_CODE
INVC_MATCH_DATE	INV_MATCH_DATE	INV_MATCH_DATE
TO_LOC	TO_LOC_NO	TO_LOC_NO
FROM_LOC	FROM_LOC_NO	FROM_LOC_NO
COURIER	COURIER_ID	COURIER_ID
EXT_REF_NO_IN	EXT_REF_IN_ID	EXT_REF_IN_ID
DISTRO_NO		DIST_NO	DIST_NO
WW_DISTRO_DOC_TYPE		DIST_TYPE	DIST_TYPE
REF_ITEM		REF_ITEM_NO	REF_ITEM_NO
WW_TSF_ALLOC_NO		TSF_ALLOC_NO	TSF_ALLOC_NO
TSF_PO_LINK_NO	TSF_PO_LINK_NO
WW_IBT_TYPE		IBT_TYPE	IBT_TYPE
WW_SIMS_REF_NO		SIMS_REF_ID	SIMS_REF_ID
CARTON		DU_ID	DU_ID
STATUS_CODE		SHIPMENT_LINE_STATUS_CODE	SHIPMENT_LINE_STATUS_CODE
QTY_RECEIVED		RECEIVED_QTY	RECEIVED_QTY
UNIT_COST		COST_PRICE	COST_PRICE
UNIT_RETAIL		REG_RSP	REG_RSP
QTY_EXPECTED		sdn_QTY	sdn_QTY
WW_ACTUAL_MASS		ACTUAL_MASS	ACTUAL_MASS
WW_CARTON_STATUS_IND		CARTON_STATUS_CODE	CARTON_STATUS_CODE
CARTON_STATUS_DESC	CARTON_STATUS_DESC
QTY_matched		asn_QTY	asn_QTY
PO_NOT_BEFORE_DATE	PO_NOT_BEFORE_DATE
CONTRACT_NO	CONTRACT_NO
WW_ACTUAL_RECEIVED_DATE		ACTL_RCPT_DATE	ACTL_RCPT_DATE
SUPP_PACK_SIZE	SUPP_PACK_SIZE
WW_QTY_CANCELLED		CANCELLED_QTY	CANCELLED_QTY
WW_AUTO_RECEIPT_IND		AUTO_RECEIPT_CODE	AUTO_RECEIPT_CODE
SOURCE_DATA_STATUS_CODE
WW_FINAL_LOC		FINAL_LOC_NO	FINAL_LOC_NO
*/
/*set serveroutput on;
exec sys.ww_set_datafix_details('This is for Defect ID : 3985');
select * from sys_dwh_LOG
where log_procedure_name = 'WH_FND_CORP_SHIPFIX2'
AND LOG_DATE_TIME >= TRUNC(SYSDATE) - 1
ORDER BY LOG_DATE_TIME DESC*/
--**************************************************************************************************

g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_date               date;
g_fin_week_no        dim_calendar.fin_week_no%type;
g_fin_year_no        dim_calendar.fin_year_no%type;
g_sub                integer       :=  0;
g_cnt                number        := 0;


g_start_date         date;
g_end_date           date;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_SHIPFIX2';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD ASSORT DAILY CATALOG DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_NAME VARCHAR2(40);
------------------------------------------------------------------------------
--                  2. Remove primary key and indexes from DWH_FOUNDATION.RMS_SHIPMENT_SHIPSKU
------------------------------------------------------------------------------
procedure A2_REMOVE_INDEXES as
BEGIN
     g_name := null; 
  BEGIN
    SELECT CONSTRAINT_NAME
    INTO G_name
    FROM ALL_CONSTRAINTS
    WHERE CONSTRAINT_NAME = 'PK_P_R2MS_SHPMNT_SHPSKU'
    AND TABLE_NAME        = 'R2MS_SHIPMENT_SHIPSKU';
    
    l_text               := 'drop constraint PK_P_R2MS_SHPMNT_SHPSKU';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_FOUNDATION.R2MS_SHIPMENT_SHIPSKU drop constraint PK_P_R2MS_SHPMNT_SHPSKU');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_P_R2MS_SHPMNT_SHPSKU does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
    
    g_name := null;
 
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in A2_REMOVE_INDEXES';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in A2_REMOVE_INDEXES';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end A2_REMOVE_INDEXES;

------------------------------------------------------------------------------
--               3. extract into DWH_FOUNDATION.R2MS_SHIPMENT_SHIPSKU
--                  from SUPP_SMOT.SHIPMENT, SUPP_SMOT.SHIPSKU
--                  joined on shipment, seq_no
--                  and where ship_date >= '1 jan 2015'
--????????                  and status <> 'D' ?????
------------------------------------------------------------------------------
procedure A3_R2MS_join_hdr_det as
begin

    insert /*+ append */ into DWH_FOUNDATION.R2MS_SHIPMENT_SHIPSKU
            WITH SELCAL AS (SELECT CALENDAR_DATE 
                            FROM DIM_CALENDAR 
                            WHERE CALENDAR_DATE between g_start_date and g_end_date),
                 SELHDR AS (
                            SELECT /*+ FULL(SH) PARALLEL(SH,4) */
                                    SHIPMENT
                                    ,ORDER_NO
                                    ,BOL_NO
                                    ,ASN
                                    ,SHIP_DATE
                                    ,RECEIVE_DATE
                                    ,EST_ARR_DATE
                                    ,SHIP_ORIGIN
                                    ,STATUS_CODE
                                    ,INVC_MATCH_STATUS
                                    ,INVC_MATCH_DATE
                                    ,TO_LOC
                                    ,TO_LOC_TYPE
                                    ,FROM_LOC
                                    ,FROM_LOC_TYPE
                                    ,COURIER
                                    ,NO_BOXES
                                    ,EXT_REF_NO_IN
                                    ,EXT_REF_NO_OUT
                                    ,COMMENTS
                                    ,WW_AUTO_RECEIPT_IND
                                    ,WW_CREATE_DATETIME
                            FROM  SUPP_SMOT.SHIPMENT SH, SELCAL SC
                            WHERE trunc(SHIP_DATE) = CALENDAR_DATE)
          SELECT /*+ full(sk) PARALLEL(SK,4) */ SH.*, SK.*
          FROM SELHDR SH,  SUPP_SMOT.SHIPSKU SK
          WHERE SH.SHIPMENT = SK.SHIPMENT
          ;

      g_recs_inserted := 0;
      g_recs_inserted := sql%rowcount;

      commit;
   l_text := 'insert R2MS_shipment_shipsku = '||g_recs_inserted;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end A3_R2MS_join_hdr_det;

------------------------------------------------------------------------------
--                  4. Add primary key and indexes from DWH_FOUNDATION.R2MS_SHIPMENT_SHIPSKU
------------------------------------------------------------------------------
procedure A4_ADD_INDEXES as
BEGIN
      l_text          := 'Running GATHER_TABLE_STATS ON R2MS_SHIPMENT_SHIPSKU';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'R2MS_SHIPMENT_SHIPSKU', DEGREE => 8);

      l_text := 'add constraint PK_P_R2MS_SHPMNT_SHPSKU';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_FOUNDATION.R2MS_SHIPMENT_SHIPSKU add CONSTRAINT PK_P_R2MS_SHPMNT_SHPSKU PRIMARY KEY (hSHIPMENT,dSEQ_NO,dITEM)                    
      USING INDEX tABLESPACE FND_MASTER  ENABLE');     
 
   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in A4_ADD_INDEXES';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in A4_ADD_INDEXES';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end A4_ADD_INDEXES;

------------------------------------------------------------------------------
--                  5. Remove primary key and indexes from DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT
------------------------------------------------------------------------------
procedure A5_REMOVE_INDEXES as
BEGIN
     g_name := null; 
  BEGIN
    SELECT CONSTRAINT_NAME
    INTO G_name
    FROM ALL_CONSTRAINTS
    WHERE CONSTRAINT_NAME = 'PK_P_R2MS_O2DWH_STG_SHPMNT'
    AND TABLE_NAME        = 'R2MS_O2DWH_STG_SHIPMENT';
    
    l_text               := 'alter table dwh_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT drop constraint PK_P_R2MS_O2DWH_STG_SHPMNT';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE immediate('alter table dwh_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT drop constraint PK_P_R2MS_O2DWH_STG_SHPMNT');
    COMMIT;
    
  EXCEPTION
  WHEN no_data_found THEN
    l_text := 'constraint PK_P_R2MS_O2DWH_STG_SHPMNT does not exist';
    dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
  END;
    
    g_name := null;
 
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in A5_REMOVE_INDEXES';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in A5_REMOVE_INDEXES';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end A5_REMOVE_INDEXES;

------------------------------------------------------------------------------
--               6. extract into DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT
--                  from DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT, DWH_FOUNDATION.FND_RTL_SHIPMENT
--                  left outer join on shipment, seq_no, item_no
------------------------------------------------------------------------------
procedure A6_R2MS_join_O2DWH as
begin

     insert /*+ append */ into DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT
            WITH SELCAL AS (SELECT CALENDAR_DATE 
                            FROM DIM_CALENDAR 
                            WHERE CALENDAR_DATE between g_start_date and g_end_date),
                 SELFND AS (
                            SELECT /*+  PARALLEL(FND,4) */ * FROM DWH_FOUNDATION.FND_RTL_SHIPMENT FND, SELCAL SC
                            WHERE SHIP_DATE = CALENDAR_DATE
            )
            SELECT /*+ full(R2MS) PARALLEL(R2MS,4) */ HSHIPMENT
                  ,HORDER_NO
                  ,HBOL_NO
                  ,HASN
                  ,HSHIP_DATE
                  ,HRECEIVE_DATE
                  ,HEST_ARR_DATE
                  ,HSHIP_ORIGIN
                  ,HSTATUS_CODE
                  ,HINVC_MATCH_STATUS
                  ,HINVC_MATCH_DATE
                  ,HTO_LOC
                  ,HTO_LOC_TYPE
                  ,HFROM_LOC
                  ,HFROM_LOC_TYPE
                  ,HCOURIER
                  ,HNO_BOXES
                  ,HEXT_REF_NO_IN
                  ,HEXT_REF_NO_OUT
                  ,HCOMMENTS
                  ,HWW_AUTO_RECEIPT_IND
                  ,HWW_CREATE_DATETIME
                  ,DSHIPMENT
                  ,DSEQ_NO
                  ,DITEM
                  ,DDISTRO_NO
                  ,DREF_ITEM
                  ,DCARTON
                  ,DINV_STATUS
                  ,DSTATUS_CODE
                  ,round(DQTY_RECEIVED,3) DQTY_RECEIVED
                  ,round(DUNIT_COST,2) DUNIT_COST
                  ,round(DUNIT_RETAIL,2) DUNIT_RETAIL
                  ,round(DQTY_EXPECTED,3) DQTY_EXPECTED
                  ,DMATCH_INVC_ID
                  ,DWW_ACTUAL_MASS
                  ,DWW_CARTON_STATUS_IND
                  ,DWW_TSF_ALLOC_NO
                  ,DWW_IBT_TYPE
                  ,DWW_DISTRO_DOC_TYPE
                  ,DWW_SIMS_REF_NO
                  ,round(DQTY_MATCHED,3) DQTY_MATCHED
                  ,DWW_ACTUAL_RECEIVED_DATE
                  ,round(DWW_QTY_CANCELLED,3) DWW_QTY_CANCELLED
                  ,DWW_CREATE_DATETIME
                  ,DWW_LAST_RECEIVED_DATETIME
                  ,nvl(DWW_FINAL_LOC, HTO_LOC) DWW_FINAL_LOC
                  ,NVL(SHIPMENT_NO,0) SHIPMENT_NO
                  ,SEQ_NO
                  ,ITEM_NO
                  ,SUPPLIER_NO
                  ,PO_NO
                  ,SDN_NO
                  ,ASN_ID
                  ,SHIP_DATE
                  ,RECEIVE_DATE
                  ,SHIPMENT_STATUS_CODE
                  ,INV_MATCH_STATUS_CODE
                  ,INV_MATCH_DATE
                  ,TO_LOC_NO
                  ,FROM_LOC_NO
                  ,COURIER_ID
                  ,EXT_REF_IN_ID
                  ,DIST_NO
                  ,DIST_TYPE
                  ,REF_ITEM_NO
                  ,TSF_ALLOC_NO
                  ,TSF_PO_LINK_NO
                  ,IBT_TYPE
                  ,SIMS_REF_ID
                  ,DU_ID
                  ,SHIPMENT_LINE_STATUS_CODE
                  ,round(RECEIVED_QTY,3) RECEIVED_QTY
                  ,round(COST_PRICE,2) COST_PRICE
                  ,round(REG_RSP,2) REG_RSP
                  ,round(ASN_QTY,3) ASN_QTY
                  ,ACTUAL_MASS
                  ,CARTON_STATUS_CODE
                  ,CARTON_STATUS_DESC
                  ,round(SDN_QTY,3) SDN_QTY
                  ,PO_NOT_BEFORE_DATE
                  ,CONTRACT_NO
                  ,ACTL_RCPT_DATE
                  ,SUPP_PACK_SIZE
                  ,round(CANCELLED_QTY,3) CANCELLED_QTY
                  ,AUTO_RECEIPT_CODE
                  ,SOURCE_DATA_STATUS_CODE
                  ,LAST_UPDATED_DATE
                  ,TO_LOC_DEBT_COMM_PERC
                  ,FINAL_LOC_NO
                  ,FROM_LOC_DEBT_COMM_PERC
                  ,CHAIN_CODE
            FROM DWH_FOUNDATION.R2MS_SHIPMENT_SHIPSKU R2MS, SELFND FND
            WHERE R2MS.hSHIPMENT = FND.SHIPMENT_NO(+)
            AND R2MS.dSEQ_NO = FND.SEQ_NO(+)
            AND TO_NUMBER(R2MS.dITEM) = TO_CHAR((nvl(FND.ITEM_NO,0)));

      g_recs_inserted := 0;
      g_recs_inserted := sql%rowcount;

      commit;
   l_text := 'insert R2MS_O2DWH_stg_shipment = '||g_recs_inserted;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end A6_R2MS_join_O2DWH;

------------------------------------------------------------------------------
--                  7. Add primary key and indexes from DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT
------------------------------------------------------------------------------
procedure A7_ADD_INDEXES as
BEGIN

      l_text          := 'Running GATHER_TABLE_STATS ON R2MS_O2DWH_STG_SHIPMENT';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'R2MS_O2DWH_STG_SHIPMENT', DEGREE => 8);

      l_text := 'add constraint PK_P_R2MS_O2DWH_STG_SHPMNT';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      EXECUTE immediate('alter table dwh_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT add CONSTRAINT PK_P_R2MS_O2DWH_STG_SHPMNT PRIMARY KEY (SHIPMENT_NO,hSHIPMENT,dSEQ_NO,dITEM )                    
      USING INDEX tABLESPACE FND_MASTER  ENABLE');
     

   EXCEPTION

      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in A7_ADD_INDEXES';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in A7_ADD_INDEXES';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end A7_ADD_INDEXES;
------------------------------------------------------------------------------
--               8. extract into DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX
--                  from DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT
--                  the records we want to fix
------------------------------------------------------------------------------
procedure A8_O2DWH_extract_fix as
begin

      insert /*+ append */ into DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX
            select /*+  PARALLEL(R2MS,4) */ 
                    HSHIPMENT
                  ,	DSHIPMENT
                  ,	SHIPMENT_NO
                  ,	DSEQ_NO
                  ,	SEQ_NO
                  ,	DITEM
                  ,	ITEM_NO
                  ,	HSHIP_DATE
                  ,	SHIP_DATE
                  ,	DWW_ACTUAL_RECEIVED_DATE
                  ,	ACTL_RCPT_DATE
                  ,	HINVC_MATCH_DATE
                  ,	INV_MATCH_DATE
                  ,	HINVC_MATCH_STATUS
                  ,	INV_MATCH_STATUS_CODE
                  ,	HRECEIVE_DATE
                  ,	RECEIVE_DATE
                  ,	HASN
                  ,	ASN_ID
                  ,	HWW_AUTO_RECEIPT_IND
                  ,	AUTO_RECEIPT_CODE
                  ,	DWW_CARTON_STATUS_IND
                  ,	CARTON_STATUS_CODE
                  ,	HCOURIER
                  ,	COURIER_ID
                  ,	DWW_DISTRO_DOC_TYPE
                  ,	DIST_TYPE
                  ,	DCARTON
                  ,	DU_ID
                  ,	HEXT_REF_NO_IN
                  ,	EXT_REF_IN_ID
                  ,	DWW_IBT_TYPE
                  ,	IBT_TYPE
                  ,	DREF_ITEM
                  ,	REF_ITEM_NO
                  ,	DSTATUS_CODE
                  ,	SHIPMENT_LINE_STATUS_CODE
                  ,	HSTATUS_CODE
                  ,	SHIPMENT_STATUS_CODE
                  ,	DWW_SIMS_REF_NO
                  ,	SIMS_REF_ID
                  ,	DWW_ACTUAL_MASS
                  ,	ACTUAL_MASS
                  ,	DQTY_EXPECTED
                  ,	ASN_QTY
                  ,	DWW_QTY_CANCELLED
                  ,	CANCELLED_QTY
                  ,	DUNIT_COST
                  ,	COST_PRICE
                  ,	DDISTRO_NO
                  ,	DIST_NO
                  ,	DWW_FINAL_LOC
                  ,	FINAL_LOC_NO
                  ,	HFROM_LOC
                  ,	FROM_LOC_NO
                  ,	PO_NO
                  ,	HORDER_NO
                  ,	DQTY_RECEIVED
                  ,	RECEIVED_QTY
                  ,	DUNIT_RETAIL
                  ,	REG_RSP
                  ,	HBOL_NO
                  ,	SDN_NO
                  ,	DQTY_MATCHED
                  ,	SDN_QTY
                  ,	HTO_LOC
                  ,	TO_LOC_NO
                  ,	DWW_TSF_ALLOC_NO
                  ,	TSF_ALLOC_NO
                  ,	CARTON_STATUS_DESC
                  ,	CHAIN_CODE
                  ,	CONTRACT_NO
                  ,	DINV_STATUS
                  ,	DMATCH_INVC_ID
                  ,	DWW_CREATE_DATETIME
                  ,	DWW_LAST_RECEIVED_DATETIME
                  ,	FROM_LOC_DEBT_COMM_PERC
                  ,	HCOMMENTS
                  ,	HEST_ARR_DATE
                  ,	HEXT_REF_NO_OUT
                  ,	HFROM_LOC_TYPE
                  ,	HNO_BOXES
                  ,	HSHIP_ORIGIN
                  ,	HTO_LOC_TYPE
                  ,	HWW_CREATE_DATETIME
                  ,	LAST_UPDATED_DATE
                  ,	PO_NOT_BEFORE_DATE
                  ,	SOURCE_DATA_STATUS_CODE
                  ,	SUPP_PACK_SIZE
                  ,	SUPPLIER_NO
                  ,	TO_LOC_DEBT_COMM_PERC
                  ,	TSF_PO_LINK_NO
      from DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT R2MS
            where (shipment_no is not null or shipment_no > 0)
                and (TRUNC(hship_date) <> ship_date
                or TRUNC(dWW_ACTUAL_RECEIVED_DATE) <> ACTL_RCPT_DATE
                or TRUNC(hINVC_MATCH_DATE)  <> INV_MATCH_DATE
                or TRUNC(hRECEIVE_DATE) <> RECEIVE_DATE
                or hINVC_MATCH_STATUS <>  INV_MATCH_STATUS_CODE
                or dITEM <> TO_CHAR((nvl(ITEM_NO,0)))
                or nvl(dSEQ_NO,0)+nvl(dSHIPMENT,0) <> nvl(SEQ_NO,0)+nvl(SHIPMENT_NO,0)
                or (nvl(dWW_ACTUAL_MASS,0)+nvl(dWW_QTY_CANCELLED,0)+nvl(dUNIT_COST,0)+nvl(dDISTRO_NO,0)+nvl(dWW_FINAL_LOC,0)+nvl(hFROM_LOC,0)  +nvl(hORDER_NO,0)+nvl(dQTY_RECEIVED,0)+nvl(dUNIT_RETAIL,0)+nvl(hBOL_NO,0)+nvl(hTO_LOC,0)+nvl(dWW_TSF_ALLOC_NO,0)) 
                    <>
                    (nvl(ACTUAL_MASS,0)   +nvl(CANCELLED_QTY,0)    +nvl(COST_PRICE,0)+nvl(DIST_NO,0)   +nvl(FINAL_LOC_NO,0) +nvl(FROM_LOC_NO,0)+nvl(PO_NO,0)    +nvl(RECEIVED_QTY,0) +nvl(REG_RSP,0)     +nvl(SDN_NO,0)+nvl(TO_LOC_NO,0)+nvl(TSF_ALLOC_NO,0))
                
                OR (cASE WHEN (HBol_No = '0' OR HBol_No IS NULL)  AND HAsn IS NOT NULL 
						                   THEN DQty_Expected
                         ELSE
                              NULL
                     END)  <> ASN_QTY
                OR   (CASE WHEN HBol_No IS NOT NULL AND HAsn IS NOT NULL 
						                     THEN DQty_Expected
						               WHEN HBol_No IS NULL 
                             AND (HAsn = '0' OR HAsn IS NOT NULL ) 
						                         THEN NULL
						               ELSE
						                   DQty_Expected
						           END) <> SDN_QTY
                or 	hASN	<>	ASN_ID
                or 	hWW_AUTO_RECEIPT_IND	<>	AUTO_RECEIPT_CODE
                or 	dWW_CARTON_STATUS_IND	<>	CARTON_STATUS_CODE
                or 	hCOURIER	<>	COURIER_ID
                or 	dWW_DISTRO_DOC_TYPE	<>	DIST_TYPE
                or 	dCARTON	<>	DU_ID
                or 	hEXT_REF_NO_IN	<>	EXT_REF_IN_ID
                or 	dWW_IBT_TYPE	<>	IBT_TYPE
                or 	dREF_ITEM	<>	TO_CHAR(REF_ITEM_NO)
                or 	dSTATUS_CODE	<>	SHIPMENT_LINE_STATUS_CODE
                or 	hSTATUS_CODE	<>	SHIPMENT_STATUS_CODE
              --  or 	dWW_SIMS_REF_NO	<>	SIMS_REF_ID
              )
union all
            select /*+  PARALLEL(R2MS,4) */ 
                      HSHIPMENT
                    ,	DSHIPMENT
                    ,	SHIPMENT_NO
                    ,	DSEQ_NO
                    ,	SEQ_NO
                    ,	DITEM
                    ,	ITEM_NO
                    ,	HSHIP_DATE
                    ,	SHIP_DATE
                    ,	DWW_ACTUAL_RECEIVED_DATE
                    ,	ACTL_RCPT_DATE
                    ,	HINVC_MATCH_DATE
                    ,	INV_MATCH_DATE
                    ,	HINVC_MATCH_STATUS
                    ,	INV_MATCH_STATUS_CODE
                    ,	HRECEIVE_DATE
                    ,	RECEIVE_DATE
                    ,	HASN
                    ,	ASN_ID
                    ,	HWW_AUTO_RECEIPT_IND
                    ,	AUTO_RECEIPT_CODE
                    ,	DWW_CARTON_STATUS_IND
                    ,	CARTON_STATUS_CODE
                    ,	HCOURIER
                    ,	COURIER_ID
                    ,	DWW_DISTRO_DOC_TYPE
                    ,	DIST_TYPE
                    ,	DCARTON
                    ,	DU_ID
                    ,	HEXT_REF_NO_IN
                    ,	EXT_REF_IN_ID
                    ,	DWW_IBT_TYPE
                    ,	IBT_TYPE
                    ,	DREF_ITEM
                    ,	REF_ITEM_NO
                    ,	DSTATUS_CODE
                    ,	SHIPMENT_LINE_STATUS_CODE
                    ,	HSTATUS_CODE
                    ,	SHIPMENT_STATUS_CODE
                    ,	DWW_SIMS_REF_NO
                    ,	SIMS_REF_ID
                    ,	DWW_ACTUAL_MASS
                    ,	ACTUAL_MASS
                    ,	DQTY_matched
                    ,	ASN_QTY
                    ,	DWW_QTY_CANCELLED
                    ,	CANCELLED_QTY
                    ,	DUNIT_COST
                    ,	COST_PRICE
                    ,	DDISTRO_NO
                    ,	DIST_NO
                    ,	DWW_FINAL_LOC
                    ,	FINAL_LOC_NO
                    ,	HFROM_LOC
                    ,	FROM_LOC_NO
                    ,	PO_NO
                    ,	HORDER_NO
                    ,	DQTY_RECEIVED
                    ,	RECEIVED_QTY
                    ,	DUNIT_RETAIL
                    ,	REG_RSP
                    ,	HBOL_NO
                    ,	SDN_NO
                    ,	DQTY_expected
                    ,	SDN_QTY
                    ,	HTO_LOC
                    ,	TO_LOC_NO
                    ,	DWW_TSF_ALLOC_NO
                    ,	TSF_ALLOC_NO
                    ,	CARTON_STATUS_DESC
                    ,	CHAIN_CODE
                    ,	CONTRACT_NO
                    ,	DINV_STATUS
                    ,	DMATCH_INVC_ID
                    ,	DWW_CREATE_DATETIME
                    ,	DWW_LAST_RECEIVED_DATETIME
                    ,	FROM_LOC_DEBT_COMM_PERC
                    ,	HCOMMENTS
                    ,	HEST_ARR_DATE
                    ,	HEXT_REF_NO_OUT
                    ,	HFROM_LOC_TYPE
                    ,	HNO_BOXES
                    ,	HSHIP_ORIGIN
                    ,	HTO_LOC_TYPE
                    ,	HWW_CREATE_DATETIME
                    ,	LAST_UPDATED_DATE
                    ,	PO_NOT_BEFORE_DATE
                    ,	SOURCE_DATA_STATUS_CODE
                    ,	SUPP_PACK_SIZE
                    ,	SUPPLIER_NO
                    ,	TO_LOC_DEBT_COMM_PERC
                    ,	TSF_PO_LINK_NO
            from DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT R2MS
            where shipment_no is null or shipment_no = 0
    ;

      g_recs_inserted := 0;
      g_recs_inserted := sql%rowcount;

      commit;
      l_text := 'insert O2DWH_stg_shipment_fix = '||g_recs_inserted;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
    exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end A8_O2DWH_extract_fix ;

------------------------------------------------------------------------------
--               9. extract into DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX_CPY
--                  from DWH_FOUNDATION.O2DWH_STG_SHIPMENT_fix
--                  the records we want to fix in the correct format for loading 
------------------------------------------------------------------------------
 procedure A9_O2DWH_format_fix_stg_cpy  as
   begin

        insert /*+ append */ into DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX_CPY
            SELECT /*+ full(ODW) PARALLEL(ODW,4) */
              9999999999 SYS_SOURCE_BATCH_ID
            , DWH_PERFORMANCE.TEMP_SEQ.nextval  SYS_SOURCE_SEQUENCE_NO
            , TRUNC(SYSDATE) SYS_LOAD_DATE
            , 'F' SYS_PROCESS_CODE
            , 'DWH' SYS_LOAD_SYSTEM_NAME
            , null SYS_MIDDLEWARE_BATCH_ID
            , 'FIX SHIP='|| TRUNC(SYSDATE) SYS_PROCESS_MSG
            , dSHIPMENT shipment_no
            , dSEQ_NO seq_no
            , dITEM item_no
            , null supplier_no	
            , hORDER_NO po_no
            , hBOL_NO sdn_no
            , hASN asn_id
            , TRUNC(hSHIP_DATE) ship_date
            , TRUNC(hRECEIVE_DATE) receive_date
            , hSTATUS_CODE shipment_status_code
            , hINVC_MATCH_STATUS INV_MATCH_STATUS_CODE
            , TRUNC(hINVC_MATCH_DATE) INV_MATCH_DATE
            , hTO_LOC to_loc_no
            , hFROM_LOC from_loc_no
            , hCOURIER courier_id
            , hEXT_REF_NO_IN EXT_REF_IN_ID
            , dDISTRO_NO DIST_NO
            , dWW_DISTRO_DOC_TYPE DIST_TYPE
            , dREF_ITEM REF_ITEM_NO
            , dWW_TSF_ALLOC_NO tsf_alloc_no
            , null TSF_PO_LINK_NO 
            , dWW_IBT_TYPE ibt_type
            , dWW_SIMS_REF_NO sim_ref_id
            , dCARTON du_id
            , dSTATUS_CODE  SHIPMENT_LINE_STATUS_CODE
            , dQTY_RECEIVED received_qty
            , dUNIT_COST cost_price
            , dUNIT_RETAIL reg_rsp
            , cASE WHEN (HBol_No = '0' OR HBol_No IS NULL)  AND HAsn IS NOT NULL 
						             THEN DQty_Expected
						       ELSE
						              NULL
						  END ASN_QTY
            , dWW_ACTUAL_MASS actual_mass
            , dWW_CARTON_STATUS_IND carton_status_code
            , null CARTON_STATUS_DESC
            ,   CASE WHEN HBol_No IS NOT NULL AND HAsn IS NOT NULL 
						               THEN DQty_Expected
						        WHEN HBol_No IS NULL 
                    AND (HAsn = '0' OR HAsn IS NOT NULL ) 
						            THEN   NULL
						    ELSE
						       DQty_Expected
						  END SDN_QTY
            , null PO_NOT_BEFORE_DATE
            , null CONTRACT_NO
            , TRUNC(dWW_ACTUAL_RECEIVED_DATE) actl_rcpt_date
            , null SUPP_PACK_SIZE
            , dWW_QTY_CANCELLED cancelled_qty
            , hWW_AUTO_RECEIPT_IND auto_receipt_code
            , null SOURCE_DATA_STATUS_CODE
            , dWW_FINAL_LOC final_loc_no
            FROM DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX ODW;
      g_recs_inserted := 0;
      g_recs_inserted := sql%rowcount;

      commit;
   l_text := 'insert O2DWH_stg_shipment_fix_cpy = '||g_recs_inserted;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end A9_O2DWH_format_fix_stg_cpy;

------------------------------------------------------------------------------
--               10. count of differences
------------------------------------------------------------------------------
 procedure A10_cnt_diff  as
   begin
   
        insert /*+ append */ into DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX_diff
with 
selcnt as (   select /*+  full(r2ms) PARALLEL(R2MS,4) */ trunc(hship_date) ship_date,
                      case when shipment_no is null then 0 when shipment_no = 0 then 0 else 1 end cnt_shipno_fnd
                      ,case when  TRUNC(hship_date) <> ship_date then 0 else 1 end cnt_shipdate
                      ,case when  TRUNC(dWW_ACTUAL_RECEIVED_DATE) <> ACTL_RCPT_DATE  then 0 else 1 end cnt_ACTL_RCPT_DATE
                      ,case when  TRUNC(hINVC_MATCH_DATE)  <> INV_MATCH_DATE  then 0 else 1 end cnt_INV_MATCH_DATE
                      ,case when  TRUNC(hRECEIVE_DATE) <> RECEIVE_DATE  then 0 else 1 end cnt_RECEIVE_DATE
                      ,case when  hINVC_MATCH_STATUS <>  INV_MATCH_STATUS_CODE  then 0 else 1 end cnt_INV_MATCH_STATUS_CODE
                      ,case when  dITEM <> TO_CHAR((nvl(ITEM_NO,0)))  then 0 else 1 end cnt_ITEM_NO
                      ,case when  nvl(dSEQ_NO,0)+nvl(dSHIPMENT,0) <> nvl(SEQ_NO,0)+nvl(SHIPMENT_NO,0)  then 0 else 1 end cnt_SHIPMENT_seq
                      ,case when  nvl(dWW_ACTUAL_MASS,0)  <> nvl(ACTUAL_MASS,0) then 0 else 1 end cnt_ACTUAL_MASS
                      ,case when  (nvl(dWW_QTY_CANCELLED,0)+nvl(dQTY_RECEIVED,0)  <> nvl(CANCELLED_QTY,0)  +nvl(RECEIVED_QTY,0)) then 0 else 1 end cnt_qty
                      ,case when  (nvl(dUNIT_COST,0)+nvl(dUNIT_RETAIL,0))  <> (nvl(COST_PRICE,0) +nvl(REG_RSP,0)   )  then 0 else 1 end cnt_price
                      ,case when  (nvl(dDISTRO_NO,0)  +nvl(hORDER_NO,0) +nvl(hBOL_NO,0)+nvl(dWW_TSF_ALLOC_NO,0))   <>  ( nvl(DIST_NO,0)   +nvl(PO_NO,0)     +nvl(SDN_NO,0)++nvl(TSF_ALLOC_NO,0))   then 0 else 1 end cnt_dist_alloc_po_sdn
                      ,case when  (nvl(dWW_FINAL_LOC,0)+nvl(hFROM_LOC,0)+nvl(hTO_LOC,0)) <> (nvl(FINAL_LOC_NO,0) +nvl(FROM_LOC_NO,0)+nvl(TO_LOC_NO,0)) then 0 else 1 end cnt_locs
                      ,case when (cASE WHEN (HBol_No = '0' OR HBol_No IS NULL)  AND HAsn IS NOT NULL 
                                     THEN DQty_Expected
                               ELSE
                                    NULL
                           END)  <> ASN_QTY  then 0 else 1 end cnt_ASN_QTY
                      ,case when    (CASE WHEN HBol_No IS NOT NULL AND HAsn IS NOT NULL 
                                       THEN DQty_Expected
                                 WHEN HBol_No IS NULL 
                                   AND (HAsn = '0' OR HAsn IS NOT NULL ) 
                                           THEN NULL
                                 ELSE
                                     DQty_Expected
                             END) <> SDN_QTY  then 0 else 1 end cnt_SDN_QTY
                      ,case when  	hASN	<>	ASN_ID  then 0 else 1 end cnt_ASN_ID
                      ,case when  	hWW_AUTO_RECEIPT_IND	<>	AUTO_RECEIPT_CODE  then 0 else 1 end cnt_sAUTO_RECEIPT_CODE
                      ,case when  	dWW_CARTON_STATUS_IND	<>	CARTON_STATUS_CODE  then 0 else 1 end cnt_CARTON_STATUS_CODE
                      ,case when  	hCOURIER	<>	COURIER_ID  then 0 else 1 end cnt_COURIER_ID
                      ,case when  	dWW_DISTRO_DOC_TYPE	<>	DIST_TYPE  then 0 else 1 end cnt_DIST_TYPE
                      ,case when  	dCARTON	<>	DU_ID  then 0 else 1 end cnt_DU_ID
                      ,case when  	hEXT_REF_NO_IN	<>	EXT_REF_IN_ID  then 0 else 1 end cnt_EXT_REF_IN_ID
                      ,case when  	dWW_IBT_TYPE	<>	IBT_TYPE  then 0 else 1 end cnt_IBT_TYPE
                      ,case when  	dREF_ITEM	<>	TO_CHAR(REF_ITEM_NO)  then 0 else 1 end cnt_REF_ITEM_NO
                      ,case when  	dSTATUS_CODE	<>	SHIPMENT_LINE_STATUS_CODE  then 0 else 1 end cnt_SHIPMENT_LINE_STATUS_CODE
                      ,case when  	hSTATUS_CODE	<>	SHIPMENT_STATUS_CODE  then 0 else 1 end cnt_SHIPMENT_STATUS_CODE
 --                     ,case when  	dWW_SIMS_REF_NO	<>	SIMS_REF_ID  then 0 else 1 end 
 , 0 cnt_SIMS_REF_ID        
                from DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT R2MS),
selsum as (select ship_date, count(*) cntrecs
                , sum(nvl(cnt_shipno_fnd,0)) cnt_shipno_fnd
                , sum(nvl(cnt_shipdate,0)) cnt_shipdate
                , sum(nvl(cnt_ACTL_RCPT_DATE,0)) cnt_ACTL_RCPT_DATE
                , sum(nvl(cnt_INV_MATCH_DATE,0)) cnt_INV_MATCH_DATE
                , sum(nvl(cnt_RECEIVE_DATE,0)) cnt_RECEIVE_DATE
                , sum(nvl(cnt_INV_MATCH_STATUS_CODE,0)) cnt_INV_MATCH_STATUS_CODE
                , sum(nvl(cnt_ITEM_NO,0)) cnt_ITEM_NO
                , sum(nvl(cnt_SHIPMENT_seq,0)) cnt_SHIPMENT_seq
                , sum(nvl(cnt_ACTUAL_MASS,0)) cnt_ACTUAL_MASS
                , sum(nvl(cnt_qty,0)) cnt_qty
                , sum(nvl(cnt_price,0)) cnt_price
                , sum(nvl(cnt_dist_alloc_po_sdn,0)) cnt_dist_alloc_po_sdn
                , sum(nvl(cnt_locs,0)) cnt_locs
                , sum(nvl(cnt_ASN_QTY,0)) cnt_ASN_QTY
                , sum(nvl(cnt_SDN_QTY,0)) cnt_SDN_QTY
                , sum(nvl(cnt_ASN_ID,0)) cnt_ASN_ID
                , sum(nvl(cnt_sAUTO_RECEIPT_CODE,0)) cnt_sAUTO_RECEIPT_CODE
                , sum(nvl(cnt_CARTON_STATUS_CODE,0)) cnt_CARTON_STATUS_CODE
                , sum(nvl(cnt_COURIER_ID,0)) cnt_COURIER_ID
                , sum(nvl(cnt_DIST_TYPE,0)) cnt_DIST_TYPE
                , sum(nvl(cnt_DU_ID,0)) cnt_DU_ID
                , sum(nvl(cnt_EXT_REF_IN_ID,0)) cnt_EXT_REF_IN_ID
                , sum(nvl(cnt_IBT_TYPE,0)) cnt_IBT_TYPE
                , sum(nvl(cnt_REF_ITEM_NO,0)) cnt_REF_ITEM_NO
                , sum(nvl(cnt_SHIPMENT_LINE_STATUS_CODE,0)) cnt_SHIPMENT_LINE_STATUS_CODE
                , sum(nvl(cnt_SHIPMENT_STATUS_CODE,0)) cnt_SHIPMENT_STATUS_CODE
                , sum(nvl(cnt_SIMS_REF_ID ,0)) cnt_SIMS_REF_ID
          from selcnt
          group by ship_date)
select ship_date
      , 'MATCH  ' recs_match
      , cntrecs
      ,cnt_shipno_fnd
      ,cnt_shipdate
      ,cnt_ACTL_RCPT_DATE
      ,cnt_INV_MATCH_DATE
      ,cnt_RECEIVE_DATE
      ,cnt_INV_MATCH_STATUS_CODE
      ,cnt_ITEM_NO
      ,cnt_SHIPMENT_seq
      ,cnt_ACTUAL_MASS
      ,cnt_qty
      ,cnt_price
      ,cnt_dist_alloc_po_sdn
      ,cnt_locs
      ,cnt_ASN_QTY
      ,cnt_SDN_QTY
      ,cnt_ASN_ID
      ,cnt_sAUTO_RECEIPT_CODE
      ,cnt_CARTON_STATUS_CODE
      ,cnt_COURIER_ID
      ,cnt_DIST_TYPE
      ,cnt_DU_ID
      ,cnt_EXT_REF_IN_ID
      ,cnt_IBT_TYPE
      ,cnt_REF_ITEM_NO
      ,cnt_SHIPMENT_LINE_STATUS_CODE
      ,cnt_SHIPMENT_STATUS_CODE
      ,cnt_SIMS_REF_ID 
from selsum A
UNION
select ship_date
      ,'NOMATCH' recs_match 
      , 0 cntrecs
      , cntrecs - cnt_shipno_fnd
      , cntrecs - cnt_shipdate
      , cntrecs - cnt_ACTL_RCPT_DATE
      , cntrecs - cnt_INV_MATCH_DATE
      , cntrecs - cnt_RECEIVE_DATE
      , cntrecs - cnt_INV_MATCH_STATUS_CODE
      , cntrecs - cnt_ITEM_NO
      , cntrecs - cnt_SHIPMENT_seq
      , cntrecs - cnt_ACTUAL_MASS
      , cntrecs - cnt_qty
      , cntrecs - cnt_price
      , cntrecs - cnt_dist_alloc_po_sdn
      , cntrecs - cnt_locs
      , cntrecs - cnt_ASN_QTY
      , cntrecs - cnt_SDN_QTY
      , cntrecs - cnt_ASN_ID
      , cntrecs - cnt_sAUTO_RECEIPT_CODE
      , cntrecs - cnt_CARTON_STATUS_CODE
      , cntrecs - cnt_COURIER_ID
      , cntrecs - cnt_DIST_TYPE
      , cntrecs - cnt_DU_ID
      , cntrecs - cnt_EXT_REF_IN_ID
      , cntrecs - cnt_IBT_TYPE
      , cntrecs - cnt_REF_ITEM_NO
      , cntrecs - cnt_SHIPMENT_LINE_STATUS_CODE
      , cntrecs - cnt_SHIPMENT_STATUS_CODE
      , 0 cnt_SIMS_REF_ID 
from selsum A
;
      g_recs_inserted := 0;
      g_recs_inserted := sql%rowcount;

      commit;
      l_text := 'insert O2DWH_stg_shipment_fix_diff = '||g_recs_inserted;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
    exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end A10_cnt_diff ;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
   if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
      g_forall_limit := p_forall_limit;
   end if;
   p_success := false;
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'LOAD OF FND_AST_LOC_ITEM_DY_CATLG STARTED AT '||
   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Process 1st week will include truncate of final change table
--**************************************************************************************************
    g_cnt := 0;
    g_cnt := g_cnt + 1;
--   g_start_date := '4 JAN 2016';
--   g_end_date := '10 jan 2016';
--   g_start_date := '1 feb 2016';
--   g_end_date := '7 feb 2016';
   g_start_date := '25 jul 2016';
   g_end_date := '31 jul 2016';
   
--   Period = 25-JUL-16 to 31-JUL-16
   
   
--   max_date = 5 oct 2016 - so finish to wk - 3 to 9 oct 2016
          
/*                          l_text := '**- cnt'||g_cnt||'- - - -*- - - - - - -*- - - - - -*- - - - -*- - -**' ;
                            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
          
                            l_text := 'Period = '||g_start_date||' to '||g_end_date;
                             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

                             l_text := '-------------';
                             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
   A2_REMOVE_INDEXES;
                             l_text := 'truncate R2MS_SHIPMENT_SHIPSKU' ;
                             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate 'truncate table DWH_FOUNDATION.R2MS_SHIPMENT_SHIPSKU';
   A3_R2MS_JOIN_HDR_DET;
   A4_ADD_INDEXES;

                             l_text := '-------------';
                             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  A5_REMOVE_INDEXES;
                             l_text := 'truncate R2MS_O2DWH_STG_SHIPMENT' ;
                             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  execute immediate 'truncate table DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT';
  A6_R2MS_JOIN_O2DWH;
  A7_ADD_INDEXES;

                             l_text := '-------------';
                             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
                             l_text := 'truncate O2DWH_STG_SHIPMENT_FIX' ;
                             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate 'truncate table DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX';
   A8_O2DWH_EXTRACT_FIX;

                             l_text := '-------------';
                             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
                             l_text := 'truncate O2DWH_STG_SHIPMENT_FIX_CPY' ;
                             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX_CPY';
    A9_O2DWH_FORMAT_FIX_STG_CPY;
                            l_text          := 'Running GATHER_TABLE_STATS ON O2DWH_STG_SHIPMENT_FIX_CPY';
                            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'O2DWH_STG_SHIPMENT_FIX_CPY', DEGREE => 8);
*/
--**************************************************************************************************
-- Process Loop per week
--**************************************************************************************************
  for g_sub in 0..9 loop
             g_cnt := g_cnt + 1;
                                       l_text := '**- cnt'||g_cnt||'- - - -*- - - - - - -*- - - - - -*- - - - -*- - -**' ;
                                       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
                   
             g_start_date := g_start_date + 7;
             g_end_date := g_end_date + 7;
                                       l_text := 'Period = '||g_start_date||' to '||g_end_date;
                                       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          
                                       l_text := '-------------';
                                       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
             A2_REMOVE_INDEXES;
                                       l_text := 'truncate R2MS_SHIPMENT_SHIPSKU' ;
                                       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
             execute immediate 'truncate table DWH_FOUNDATION.R2MS_SHIPMENT_SHIPSKU';
             A3_R2MS_JOIN_HDR_DET;
             A4_ADD_INDEXES;
          
                                       l_text := '-------------';
                                       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
             A5_REMOVE_INDEXES;
                                       l_text := 'truncate R2MS_O2DWH_STG_SHIPMENT' ;
                                       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
             execute immediate 'truncate table DWH_FOUNDATION.R2MS_O2DWH_STG_SHIPMENT';
             A6_R2MS_JOIN_O2DWH;
             A7_ADD_INDEXES;
          
                                         l_text := '-------------';
                                         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
                                         l_text := 'truncate O2DWH_STG_SHIPMENT_FIX' ;
                                         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
             execute immediate 'truncate table DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX';
                                          l_text          := 'Running GATHER_TABLE_STATS ON O2DWH_STG_SHIPMENT_FIX';
                                          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
             DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'O2DWH_STG_SHIPMENT_FIX', DEGREE => 8);
             A8_O2DWH_EXTRACT_FIX;
          
                                          l_text := '-------------';
                                          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
             A9_O2DWH_FORMAT_FIX_STG_CPY;
                                          l_text          := 'Running GATHER_TABLE_STATS ON O2DWH_STG_SHIPMENT_FIX_CPY';
                                          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
             DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'O2DWH_STG_SHIPMENT_FIX_CPY', DEGREE => 8);

   end loop;

--**************************************************************************************************
-- Write final checks to audit table per ship_date
--************************************************************************************************   
                                 l_text := '**- - - - -*- - - - - - -*- - - - - -*- - - - -*- - -**' ;
                                 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
                                 
                                 l_text := 'truncate O2DWH_STG_SHIPMENT_FIX_DIFF' ;
                                 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     execute immediate 'truncate table DWH_FOUNDATION.O2DWH_STG_SHIPMENT_FIX_DIFF';
     A10_CNT_DIFF;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');

   l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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

end WH_FND_CORP_SHIPFIX2;
