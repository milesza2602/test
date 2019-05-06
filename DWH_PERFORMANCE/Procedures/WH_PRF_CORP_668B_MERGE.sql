--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_668B_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_668B_MERGE" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2010
--  Author:      W Lyttle
--  Purpose:     Update Contract Chain Item WK BOC_ALLTIME values for the current week.
--               If no current week record exists, then one must be inserted with only.
--               the alltime values on it.
--               If there are future-dated boc records, then these also need
--               to have a current week inserted.
--               Contract tables will contain data for CHBD only.
--  Tables:      Input  - rtl_contract_chain_item_wk
--               Output - rtl_contract_chain_item_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  25 mAY 2010 - DEFECT 3805 - remove sk2_item_no processing
--  27 mAY 2010 - DEFECT  - ALLTIME BOC to be put into week= g_date - 1 day
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            DWH_PERFORMANCE.rtl_contract_chain_item_wk%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_this_week_start_date date;
g_fin_year_no        number        :=  0;
g_fin_week_no        number        :=  0;
g_fin_day_no         number        :=  0;
g_fin_week_code      varchar2(7)   ;
g7_date              date          := trunc(sysdate);
g7_this_week_start_date date;
g7_fin_year_no       number        :=  0;
g7_fin_week_no       number        :=  0;
g7_fin_week_code     varchar2(7)   ;

g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_668B_MERGE';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE CONTRACT_CHAIN_ITEM_WK BOC-ALLTIME VALUES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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
    
    l_text := 'UPDATE CONTRACT_CHAIN_ITEM_WK BOC-ALLTIME VALUES '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
 
          -- for testing - hardcoded date --begin
 --   g_date := '06-Dec-10';
 --   DBMS_OUTPUT.PUT_LINE('for testing - hardcoded date ='||g_date);
      --
    -- for testing - hardcoded date --end
  -- 
  --  The All Time BOC  on a Monday should show the BOC All Time for Sunday , 
  --  The ALL time BOC on a Tuesday should show the BOC for Monday  
  --   ( the previous day )  etc etc . 
  --   THus for example on Monday the 24th May  , 
  --            this will show the BOC All time  as at  previous day ,
  --          Sunday the 23rd .
  --        The current week for  Monday the 24th is week 48 . 
  --
  --   HENCE g_date - 1 day to determine the week to put ALLTIME_BOC into
  --
    select fin_year_no, fin_week_no, this_week_start_date, fin_week_code,fin_day_no
    into g_fin_year_no, g_fin_week_no, g_this_week_start_date, g_fin_week_code,
    g_fin_day_no
    from dim_calendar
    where calendar_date = g_date ;
    --
    --        
    l_text := 'g_date   processing dates - '||g_fin_year_no||' '||g_fin_week_no
    ||' '||g_this_week_start_date||' '||g_fin_week_code;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --
    if g_fin_day_no = 7
    then 
        select fin_year_no, fin_week_no, this_week_start_date, fin_week_code
          into g7_fin_year_no, g7_fin_week_no, g7_this_week_start_date, g7_fin_week_code
          from dim_calendar
          where calendar_date = g_date+1 ;
    else 
          g7_fin_year_no :=  g_fin_year_no;
          g7_fin_week_no := g_fin_week_no;
          g7_this_week_start_date := g_this_week_start_date;
          g7_fin_week_code := g_fin_week_code;
    end if;
    --
    --
    l_text := 'g_date+7   processing dates - '||g7_fin_year_no||' '||g7_fin_week_no
    ||' '||g7_this_week_start_date||' '||g7_fin_week_code;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

--************************************************************************************************** 
-- MAKE ALL ALLTIME VALUES ZERO ON RTL_CONTRACT_ITEM_CHAIN_WK before stamping current week
-- with alltime value
--**************************************************************************************************
    l_text := 'PRE-UPDATE GATHER STATS STARTING ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','rtl_contract_chain_item_wk_boc', DEGREE => 32);
    commit;

    l_text := 'DONE GATHER STATS ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     execute immediate 'alter session enable parallel dml';
     update /*+ parallel(chn_wk,8) */ DWH_PERFORMANCE.rtl_contract_chain_item_wkQ chn_wk
     set chn_wk.boc_qty_all_time     = 0,
         chn_wk.boc_selling_all_time = 0,
         chn_wk.boc_cost_all_time = 0; 
    
      g_recs_updated := sql%rowcount;
    commit;
    
    l_text := 'RECORDS alltime values SET TO ZERO - '||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
    l_text := 'STARTING GATHER STATS ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','rtl_contract_chain_item_wk', DEGREE => 32);
    commit;
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','rtl_contract_chain_item_wk_boc', DEGREE => 32);
    commit;

    l_text := 'DONE GATHER STATS ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'MERGE STARTING ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge /*+ parallel (boc_mart,6) */ into DWH_PERFORMANCE.rtl_contract_chain_item_wkq boc_mart 
    using (
    SELECT /*+ parallel(boc,6) full(boc) */
      boc.sk1_contract_no sk1_contract_no,
      boc.sk1_chain_no sk1_chain_no,
      boc.sk1_item_no sk1_item_no,
      g_fin_year_no FIN_YEAR_NO,
      g_fin_week_no FIN_WEEK_NO,
      g_fin_week_code FIN_WEEK_CODE,
      g_this_week_start_date THIS_WEEK_START_DATE,
      '' CONTRACT_STATUS_CODE,
      0 CONTRACT_QTY,
      0 CONTRACT_SELLING,
      0 CONTRACT_COST,
      0 ACTL_GRN_QTY,
      0 ACTL_GRN_SELLING,
      0 ACTL_GRN_COST,
      0 AMENDED_PO_QTY,
      0 AMENDED_PO_SELLING,
      0 AMENDED_PO_COST,
      0 BC_SHIPMENT_QTY,
      0 BC_SHIPMENT_SELLING,
      0 BC_SHIPMENT_COST,
      0 PO_GRN_QTY,
      0 PO_GRN_SELLING,
      0 PO_GRN_COST,
      0 LATEST_PO_QTY,
      0 LATEST_PO_SELLING,
      0 LATEST_PO_COST,
      0 BOC_QTY,
      0 BOC_SELLING,
      0 BOC_COST,
      sum(NVL(boc.boc_qty_all_time,0)) boc_qty_all_time,
      sum(NVL(boc.boc_selling_all_time,0)) boc_selling_all_time,
      sum(NVL(boc.boc_cost_all_time,0)) boc_cost_all_time,
      0 NUM_DU,
      0 NUM_WEIGHTED_DAYS_TO_DELIVER,
      g_date LAST_UPDATED_DATE
      FROM DWH_PERFORMANCE.rtl_contract_chain_item_wk_boc BOC
      group by boc.sk1_contract_no, boc.sk1_chain_no, boc.sk1_item_no, g_fin_year_no, g_fin_week_no, g_fin_week_code, g_this_week_start_date
      
      ) mer_mart
      
  on (mer_mart.SK1_CONTRACT_NO  = boc_mart.sk1_contract_no
  and mer_mart.SK1_CHAIN_NO     = boc_mart.sk1_chain_no
  and mer_mart.SK1_ITEM_NO      = boc_mart.sk1_item_no
  and mer_mart.FIN_YEAR_NO      = boc_mart.fin_year_no
  and mer_mart.FIN_WEEK_NO      = boc_mart.fin_week_no
     )
  when matched then
  update
     set
        boc_qty_all_time     = mer_mart.boc_qty_all_time,
        boc_selling_all_time = mer_mart.boc_selling_all_time,
        boc_cost_all_time    = mer_mart.boc_cost_all_time
        
  when not matched then
     insert 
      (
        SK1_CONTRACT_NO,
        SK1_CHAIN_NO,
        SK1_ITEM_NO,
        FIN_YEAR_NO,
        FIN_WEEK_NO,
        FIN_WEEK_CODE,
        THIS_WEEK_START_DATE,
        CONTRACT_STATUS_CODE,
        CONTRACT_QTY,
        CONTRACT_SELLING,
        CONTRACT_COST,
        ACTL_GRN_QTY,
        ACTL_GRN_SELLING,
        ACTL_GRN_COST,
        AMENDED_PO_QTY,
        AMENDED_PO_SELLING,
        AMENDED_PO_COST,
        BC_SHIPMENT_QTY,
        BC_SHIPMENT_SELLING,
        BC_SHIPMENT_COST,
        PO_GRN_QTY,
        PO_GRN_SELLING,
        PO_GRN_COST,
        LATEST_PO_QTY,
        LATEST_PO_SELLING,
        LATEST_PO_COST,
        BOC_QTY,
        BOC_SELLING,
        BOC_COST,
        BOC_QTY_ALL_TIME,
        BOC_SELLING_ALL_TIME,
        BOC_COST_ALL_TIME,
        NUM_DU,
        NUM_WEIGHTED_DAYS_TO_DELIVER,
        LAST_UPDATED_DATE
      )
   values 
      (
        mer_mart.SK1_CONTRACT_NO,
        mer_mart.SK1_CHAIN_NO,
        mer_mart.SK1_ITEM_NO,
        mer_mart.FIN_YEAR_NO,
        mer_mart.FIN_WEEK_NO,
        mer_mart.FIN_WEEK_CODE,
        mer_mart.THIS_WEEK_START_DATE,
        mer_mart.CONTRACT_STATUS_CODE,
        mer_mart.CONTRACT_QTY,
        mer_mart.CONTRACT_SELLING,
        mer_mart.CONTRACT_COST,
        mer_mart.ACTL_GRN_QTY,
        mer_mart.ACTL_GRN_SELLING,
        mer_mart.ACTL_GRN_COST,
        mer_mart.AMENDED_PO_QTY,
        mer_mart.AMENDED_PO_SELLING,
        mer_mart.AMENDED_PO_COST,
        mer_mart.BC_SHIPMENT_QTY,
        mer_mart.BC_SHIPMENT_SELLING,
        mer_mart.BC_SHIPMENT_COST,
        mer_mart.PO_GRN_QTY,
        mer_mart.PO_GRN_SELLING,
        mer_mart.PO_GRN_COST,
        mer_mart.LATEST_PO_QTY,
        mer_mart.LATEST_PO_SELLING,
        mer_mart.LATEST_PO_COST,
        mer_mart.BOC_QTY,
        mer_mart.BOC_SELLING,
        mer_mart.BOC_COST,
        mer_mart.BOC_QTY_ALL_TIME,
        mer_mart.BOC_SELLING_ALL_TIME,
        mer_mart.BOC_COST_ALL_TIME,
        mer_mart.NUM_DU,
        mer_mart.NUM_WEIGHTED_DAYS_TO_DELIVER,
        g_date
      )
      
      ;

    
   COMMIT;
 
--************************************************************************************************** 
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',''); 
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_corp_668b_merge;
