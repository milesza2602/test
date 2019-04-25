--------------------------------------------------------
--  DDL for Procedure WH_PRF_DJ_640U_VATFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_DJ_640U_VATFIX" (
    p_forall_limit in integer,
    p_success out boolean
    --,
  -- p_start_date in date,
 --   p_end_date in date
    )
as
  --**************************************************************************************************
  --  DATE:        OCT 2008
  --  AUTHOR:      WENDY LYTTLE
  --  PURPOSE:     BALANCE OF CONTRACTS : create a temporary list of contracts that have had
  --                                      contract and/or shipment and/or purchase_order
  --                                      information changes during the last 3-days.
  --                                    : the temporary list will drive the recalculation of measures
  --                                      for these contracts,
  --  TABLES:      INPUT  -    DWH_FOUNDATION.fnd_rtl_contract
  --                           DWH_PERFORMANCE.rtl_po_supchain_loc_item_dy_DJ
  --               OUTPUT -    DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj
  --                           (contains list of contracts
  --                                     that have contract/po_supchain info changed)
  --                           (informix equivalent - formally rtl_boc_contract_list
  --                                         with field added=contract_status_code)
  --  PACKAGES:    constants, dwh_log, dwh_valid
  --  COMMENTS:    single dml could be considered for this program.
  -- PRD
  --  $$$$$$$$$$$$
  -- $$$$$$$$$$$$$$$$$$$  W A R N I N G  $$$$$$$$$$
  -- $$$$$$$$$$$$
  --  NB> Hardcoding in Procedure - start and end date are hardcoded
  -- $$$$$$$$$$$$
  --
  --  MAINTENANCE:
  --  06 March 2009 - defect 1020 -Program WH_PRF_CORP_640U_VATFIX aborted
  --                             populating DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj
  --
  --  31 March 2009 - defect 1309 - Add start_date and end_date as parameters
  --                                to wh_prf_corp_640U_VATFIX
  --
  --  7 August 2009 - defect 2203 - PRC - Generation of the
  --                                RTL_CONTRACT_CHAIN_ITEM_WK_BOC table
  --                                (and the associated TEMP Table)
  --  17 August 2009 - defect 2268 - PRC - Deleted Contract Lines appearing
  --                                 rtl_contract_chain_item_wk_boc
  --  24 August 2009 - defect 2305 - BOC returning 3x value for some contracts
  --                                 as well as some deleted contracts
  --
  --  17 Sept 2009 - defect 2390 - WWP/ CHP - Fin Inventory Adjustments - GROSS_PROFIT_RC_EXCLUDE_IND
  --                                 as well as some deleted contracts
  --
  --  25 mAY 2010 - DEFECT 3805 - remove sk2_item_no processing
  --
  --
  --  NAMING CONVENTIONS:
  --  G_  -  GLOBAL VARIABLE
  --  L_  -  LOG TABLE VARIABLE
  --  A_  -  ARRAY VARIABLE
  --  V_  -  LOCAL VARIABLE AS FOUND IN PACKAGES
  --  P_  -  PARAMETER
  --  C_  -  PREFIX TO CURSOR
  --
  --  PROCESSING FLOW
  --   1. Clear DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj
  --   2. Select All contracts(at distinct contract_no level)
  --          in DWH_FOUNDATION.fnd_rtl_contract/DWH_PERFORMANCE.rtl_po_supchain_loc_item_dy_DJ
  --           where last_updated_date falls in period
  --      Select from DWH_FOUNDATION.fnd_rtl_contract contracts listed in Selection
  --                which have in DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj
  --**************************************************************************************************
g_recs_read          integer := 0;
g_recs_inserted      integer := 0;
g_recs_updated       integer := 0;
g_recs_count         integer := 0;
g_forall_limit       integer := dwh_constants.vc_forall_limit;
g_error_count        number  := 0;
g_error_index        number  := 0;
g_rec_out            DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj%rowtype;
g_found              boolean;
g_count              number       := 0;
g_date               date          := trunc(sysdate);
g_start_date         date          := trunc(sysdate);
g_end_date           date          := trunc(sysdate);
g_minus_days         number       := 3;
prev_con number := 0;
prev_item number := 0;
prev_stat varchar2(1);
prev_srce varchar2(1);
min_date date;

l_message sys_dwh_errlog.log_text%type;
l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_DJ_640U_VATFIX';
l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
l_text sys_dwh_log.log_text%type ;
l_description sys_dwh_log_summary.log_description%type   := 'BOC - CREATE LIST OF CHANGED CONTRACTS STARTED AT';
l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj%rowtype index by binary_integer;
type tbl_array_u is table of DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj%rowtype index by binary_integer;

a_tbl_insert tbl_array_i;
a_tbl_update tbl_array_u;
a_empty_set_i tbl_array_i;
a_empty_set_u tbl_array_u;
a_count   integer := 0;
a_count_i integer := 0;
a_count_u integer := 0;




--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'BOC - CREATE LIST OF CHANGED CONTRACTS STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dbms_output.put_line('BATCH DATE BEING PROCESSED - '||g_date); 
 --**************************************************************************************************
 -- hardcoded dates to do a full run
 --**************************************************************************************************
    g_start_date := '01 jan 01';
    g_end_date := '30 jun 30';
    l_text :='HARDCODED START AND END DATES :- '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- TRUNCATE DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj
  -- DWH_LOG.WRITE_LOG   : DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj is truncated before running
  --**************************************************************************************************
    l_text :='TRUNCATED DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj AT '||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate ('truncate table DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj');
    commit;

    execute immediate 'alter session enable parallel dml';
 
--**************************************************************************************************
-- CREATE LIST FROM CONTRACTS AND PURCHASE_ORDERS
--**************************************************************************************************
INSERT /*+ APPEND */ INTO DWH_PERFORMANCE.temp_fnd_boc_contract_list_dj
WITH 
     SELCON AS 
                (select  /*+ MATERIALIZE PARALLEL(RC,4) PARALLEL(DI,4) FULL(DI) */
                         DISTINCT
                         rc.contract_no contract_no                   ,
                         dcr.sk1_contract_no sk1_contract_no          ,
                         rc.item_no item_no                           ,
                         di.sk1_item_no sk1_item_no                   ,
                         rc.ready_date  tran_date                     ,
                         dc.fin_year_no fin_year_no                   ,
                         dc.fin_week_no fin_week_no                   ,
                         dc.fin_week_code fin_week_code               ,
                         (CASE WHEN (rc.contract_status_code IS NULL) THEN 'A' ELSE rc.CONTRACT_STATUS_CODE END) contract_status_code                   ,
                          g_date last_updated_date,
                          rc.source_data_status_code
                    from fnd_rtl_contract rc
                    inner join dim_dj_contract dcr
                          on(dcr.contract_no = rc.contract_no)
                    inner join dim_item di
                          on(di.item_no = rc.item_no)
                    inner join dim_calendar dc
                          on(dc.calendar_date = rc.ready_date)
                    where rc.last_updated_date >= g_start_date
                      and rc.last_updated_date <= g_end_date
                      and rc.contract_no > 0
                      and rc.item_no > 0
                      and rc.contract_status_code  in('A','C')
                      and (rc.source_data_status_code <> 'D'  or rc.source_data_status_code is null)
                      and rc.chain_code = 'DJ'
                     ),
             SELTPO AS 
                        (SELECT /*+ PARALLEL(A,6) */ DISTINCT a.sk1_contract_no,    a.sk1_item_no,    a.tran_date
                             FROM DWH_PERFORMANCE.rtl_po_supchain_loc_item_dy_DJ a,    
                                  SELCON b
                            WHERE po_ind          = 1
                              AND a.sk1_contract_no = b.sk1_contract_no
                              AND a.sk1_item_no         = b.sk1_item_no
                           ),
             selTcon AS
                          (SELECT DISTINCT sk1_contract_no,    sk1_item_no, tran_date
                             FROM SELCON
                             ),
             selTstat AS
                          (SELECT  sk1_contract_no,    sk1_item_no,
                                  item_no, contract_no, max(contract_status_code) contract_status_code
                           FROM SELCON
                           group by sk1_contract_no, sk1_item_no, item_no, contract_no ),
     SELPO AS 
                  ( SELECT  DISTINCT
                    a.sk1_contract_no sk1_contract_no ,
                    a.tran_date tran_date ,
                    a.sk1_item_no sk1_item_no ,
                    dcp.fin_year_no fin_year_no ,
                    dcp.fin_week_no fin_week_no ,
                    dcp.fin_week_code fin_week_code ,
                    c.item_no item_no,
                    c.contract_no contract_no,
                    c.contract_status_code contract_status_code,
                    b.sk1_contract_no con_no
        FROM selTpo a
        LEFT OUTER JOIN selTcon b
          ON (b.sk1_contract_no = a.sk1_contract_no
          AND b.sk1_item_no     = a.sk1_item_no
          AND b.tran_date      = a.tran_date)
        LEFT OUTER JOIN selTstat c
          ON (c.sk1_contract_no = a.sk1_contract_no
          AND c.sk1_item_no     = a.sk1_item_no)
        LEFT OUTER JOIN DWH_PERFORMANCE.DIM_calendar dcp
          ON (dcp.calendar_date = a.tran_date)
       HAVING b.sk1_contract_no IS NULL)
SELECT CONTRACT_NO
      ,SK1_CONTRACT_NO
      ,ITEM_NO
      ,SK1_ITEM_NO
      ,TRAN_DATE
      ,FIN_YEAR_NO
      ,FIN_WEEK_NO
      ,FIN_WEEK_CODE
      ,CONTRACT_STATUS_CODE
      ,G_DATE LAST_UPDATED_DATE
      ,SOURCE_DATA_STATUS_CODE
FROM SELCON SC
UNION
SELECT CONTRACT_NO
      ,SK1_CONTRACT_NO
      ,ITEM_NO
      ,SK1_ITEM_NO
      ,TRAN_DATE
      ,FIN_YEAR_NO
      ,FIN_WEEK_NO
      ,FIN_WEEK_CODE
      ,CONTRACT_STATUS_CODE
      ,G_DATE LAST_UPDATED_DATE
      ,NULL SOURCE_DATA_STATUS_CODE
FROM SELPO SP
 ;
  
    g_recs_inserted := 0;
    g_recs_inserted := SQL%ROWCOUNT;
    commit; 
    l_text := 'Recs inserted into temp_fnd_boc_contract_list_dj='||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);                 


--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
  l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
  l_text := dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed||sysdate;
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

end wh_prf_dj_640U_VATFIX;
