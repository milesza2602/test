--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_300U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_300U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        August 2017
--  Author:      A. Ugolini
--  Purpose:     Update DWH_PERFORMANCE.MART_CH_PROCUREMENT_PO table and 
--               data to DWH_PERFORMANCE.MART_CH_PROCUREMENT_BOC in the performance layer
--               with input mainly from DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DY table 
--               from foundation Performance layer.
--
--  Tables:      Input  - DWH_PERFORMANCE.RTL_CONTRACT_CHAIN_ITEM_WK
--                        DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DY
--                        DWH_PERFORMANCE.DIM_PURCHASE_ORDER
--                        DWH_PERFORMANCE.DIM_CONTRACT
--                        DWH_PERFORMANCE.DIM_ITEM
--                        DWH_PERFORMANCE.RTL_SC_SUPPLIER
--                        DWH_PERFORMANCE.DIM_CALENDAR_WK
--               Output - dwh_performance.MART_CH_PROCUREMENT_BOC
--                        dwh_performance.MART_CH_PROCUREMENT_PO
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
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
boc_recs_inserted    integer       :=  0;
po_recs_inserted     integer       :=  0;

g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_date               date;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_300U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PROCUREMENT FACT DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

proc_fin_yr number := 0;
--**************************************************************************************************
-- Inserts to output tables
--**************************************************************************************************
procedure rebuild_insert as
begin

--DWH_Procurement_qBoc.sql
---  item
EXECUTE IMMEDIATE 'truncate table dwh_performance.wrk_item_ch_subset drop storage';

insert /*+ parallel (ss,4) */ into dwh_performance.wrk_item_ch_subset ss
select /*+ parallel(dim,4) parallel(dimiu,4) */
       dim.SK1_ITEM_NO,
       dim.SK1_SUPPLIER_NO,
       dim.SK1_STYLE_COLOUR_NO,
       dim.RPL_IND, 
       dimiu.PRODUCT_SAMPLE_STATUS_DESC_304 PRODUCT_SAMPLE_STATUS,
       dim.DEPARTMENT_NO,
       dim.CLASS_NO,
       dim.SUBCLASS_NO
  from DWH_PERFORMANCE.DIM_ITEM             dim, 
       DWH_PERFORMANCE.DIM_ITEM_UDA         dimiu 
 where dim.SK1_ITEM_NO = dimiu.SK1_ITEM_NO 
   and dim.BUSINESS_UNIT_NO <> 50;
   
commit;
              
---  sku supplier
EXECUTE IMMEDIATE 'truncate table DWH_PERFORMANCE.wrk_supplier_primary_subset drop storage';

insert /*+ parallel (ss,4) */ into DWH_PERFORMANCE.wrk_supplier_primary_subset ss
select rtl.SK1_SUPPLIER_NO, 
       dims.SUPPLIER_NO, 
       dims.SUPPLIER_NAME, 
       rtl.SK1_STYLE_COLOUR_NO 
 from DWH_PERFORMANCE.RTL_SC_SUPPLIER              rtl, 
      DWH_PERFORMANCE.DIM_SUPPLIER                 dims 
where rtl.SK1_SUPPLIER_NO = dims.SK1_SUPPLIER_NO 
  and rtl.PRIMARY_SUPPLIER_IND = 1; 
               
commit;

--- Get current Fin_year_no

select (today_fin_year_no -3) into proc_fin_yr from dim_control;
               
--- balance of contract - Weekly
EXECUTE IMMEDIATE 'truncate table DWH_PERFORMANCE.MART_CH_PROCUREMENT_BOC drop storage';

insert /*+ parallel (boc,4) */ into DWH_PERFORMANCE.MART_CH_PROCUREMENT_BOC boc
select    SK1_ITEM_NO,
          sk1_supplier_no,
          this_week_start_date,
          CONTRACT_NO, 
          CONTRACT_QTY, 
          LATEST_PO_QTY, 
          BOC_QTY, 
          round(0, 2) AVG_CON_COST_PRICE, 
          round(0, 2) AVG_CON_RSP_EXCL_VAT, 
          round(0, 2) AVG_CON_MARGIN_PERC,
--    AVG_PO_COST_PRICE                
          case when sum(LATEST_PO_QTY_SUMM) > 0 then
          round(nvl(sum(LATEST_PO_COST)    / (sum(LATEST_PO_QTY_SUMM) ), 0), 2) 
              else 0 end AVG_PO_COST_PRICE,
--    AVG_PO_RSP_EXCL_VAT               
          case when sum(LATEST_PO_QTY_SUMM) > 0 then
          round(nvl(sum(LATEST_PO_SELLING)  / (sum(LATEST_PO_QTY_SUMM) ), 0), 2) 
              else 0 end AVG_PO_RSP_EXCL_VAT,
--    AVG_PO_MARGIN_PERC               
          case when sum(LATEST_PO_SELLING) > 0 then
          round((1 - nvl( (sum(LATEST_PO_COST)/ sum(LATEST_PO_SELLING)   ),0)) * 100, 2) 
              else 0 end AVG_PO_MARGIN_PERC,
--    AVG_BOC_COST_PRICE               
          case when sum(BOC_QTY_SUMM) > 0 then
          round(nvl(sum(BOC_COST)           / (sum(BOC_QTY_SUMM)       ), 0), 2)
              else 0 end AVG_BOC_COST_PRICE,
--    AVG_BOC_RSP_EXCL_VAT               
          case when sum(BOC_QTY_SUMM) > 0 then
          round(nvl(sum(BOC_SELLING)        / (sum(BOC_QTY_SUMM)       ), 0), 2)
              else 0 end AVG_BOC_RSP_EXCL_VAT, 
--    VG_BOC_MARGIN_PERC               
          case when sum(BOC_SELLING) > 0 then
          round((1 - nvl( (sum(BOC_COST)/ sum(BOC_SELLING)   ),0)) * 100, 2) 
              else 0 end AVG_BOC_MARGIN_PERC,
---- added columns
          nvl(sum(LATEST_PO_QTY_SUMM),0)            LATEST_PO_QTY_SUMM,
          round(nvl(sum(LATEST_PO_COST), 0), 2)     LATEST_PO_COST_SUM,
          round(nvl(sum(LATEST_PO_SELLING), 0), 2)  LATEST_PO_SELLING_SUM,
          round(nvl(sum(BOC_QTY_SUMM), 0), 2)       BOC_QTY_SUMM,
          round(nvl(sum(BOC_COST), 0), 2)           BOC_COST_SUMM,
          round(nvl(sum(BOC_SELLING), 0), 2)        BOC_SELLING_SUMM
--
  from (select /*+ full(rtl) parallel (rtl,4)*/ 
          rtl.sk1_item_no,
          supp.sk1_supplier_no,
          cal.this_week_start_date,
          dimc.CONTRACT_NO, 
          rtl.CONTRACT_QTY, 
          rtl.LATEST_PO_QTY, 
          rtl.BOC_QTY, 
          sum(rtl.LATEST_PO_COST) LATEST_PO_COST, 
          sum(rtl.LATEST_PO_QTY)  LATEST_PO_QTY_SUMM, 
          sum(rtl.LATEST_PO_SELLING) LATEST_PO_SELLING, 
          sum(rtl.BOC_COST) BOC_COST, 
          sum(rtl.BOC_QTY) BOC_QTY_SUMM, 
          sum(rtl.BOC_SELLING) BOC_SELLING, 
          sum(rtl.CONTRACT_QTY) CONTRACT_QTY_SUMM, 
          sum(rtl.CONTRACT_SELLING) CONTRACT_SELLING, 
          sum(rtl.CONTRACT_COST) CONTRACT_COST 
        from DWH_PERFORMANCE.RTL_CONTRACT_CHAIN_ITEM_WK          rtl, 
             DWH_PERFORMANCE.DIM_CONTRACT                        dimc,
             DWH_PERFORMANCE.wrk_item_ch_subset                  item,
             DWH_PERFORMANCE.wrk_supplier_primary_subset         supp,
             DWH_PERFORMANCE.DIM_CALENDAR_WK                     cal
        where
              rtl.SK1_CONTRACT_NO      = dimc.SK1_CONTRACT_NO 
          and rtl.SK1_ITEM_NO          = item.SK1_ITEM_NO
          and item.SK1_STYLE_COLOUR_NO = supp.SK1_STYLE_COLOUR_NO
          and item.SK1_SUPPLIER_NO     = supp.SK1_SUPPLIER_NO
          and rtl.THIS_WEEK_START_DATE = cal.this_week_start_date 
          and rtl.CONTRACT_STATUS_CODE in ('A','C')
          and cal.Fin_Year_no         >= proc_fin_yr
        group by  rtl.sk1_item_no,
                  supp.sk1_supplier_no,
                  cal.this_week_start_date,
                  dimc.CONTRACT_NO, 
                  rtl.CONTRACT_QTY, 
                  rtl.LATEST_PO_QTY, 
                  rtl.BOC_QTY
                   ) 
            group by  sk1_item_no,
                      sk1_supplier_no,
                      this_week_start_date,
                      CONTRACT_NO, 
                      CONTRACT_QTY, 
                      LATEST_PO_QTY, 
                      BOC_QTY; 
                      
      boc_recs_inserted  :=SQL%ROWCOUNT;
commit; 

--- DWH_Procurement_qPO.sql

---  Purchase order - Daily (summed up to Weekly)
EXECUTE IMMEDIATE 'truncate table DWH_PERFORMANCE.MART_CH_PROCUREMENT_PO drop storage';

insert /*+ parallel (po,4) */ into DWH_PERFORMANCE.MART_CH_PROCUREMENT_PO po
select    sk1_item_no,
          sk1_supplier_no,
          this_week_start_date,
          CONTRACT_NO,
          PO_NO,
          nvl(LATEST_PO_QTY,0) LATEST_PO_QTY,
--    AVG_PO_COST_PRICE                
          case when sum(LATEST_PO_QTY_SUMM) > 0 then
          round((nvl(sum(LATEST_PO_COST_SUMM)    / sum(LATEST_PO_QTY_SUMM),  0)), 2) 
                else 0 end AVG_PO_COST_PRICE,
--    AVG_PO_RSP_EXCL_VAT               
          case when sum(LATEST_PO_QTY_SUMM) > 0 then
          round((nvl(sum(LATEST_PO_SELLING_SUMM)  / sum(LATEST_PO_QTY_SUMM) , 0)), 2) 
              else 0 end AVG_PO_RSP_EXCL_VAT,
--    AVG_PO_MARGIN_PERC               
          case when sum(LATEST_PO_SELLING_SUMM) > 0 then
          round((1 - nvl( (sum(LATEST_PO_COST_SUMM)/ sum(LATEST_PO_SELLING_SUMM)   ),0)) * 100, 2) 
              else 0 end AVG_PO_MARGIN_PERC,
---- added columns
          nvl(sum(LATEST_PO_QTY_SUMM),0)                    LATEST_PO_QTY_SUMM,
          round(nvl(sum(LATEST_PO_COST_SUMM), 0), 2)        LATEST_PO_COST_SUMM,
          round(nvl(sum(LATEST_PO_SELLING_SUMM), 0), 2)     LATEST_PO_SELLING_SUMM
--
  from (select /*+ full (res) parallel (res,4) */  
              sk1_item_no,
              sk1_supplier_no,
              this_week_start_date,
              CONTRACT_NO,
              PO_NO,
              LATEST_PO_QTY,
              sum(LATEST_PO_QTY)     LATEST_PO_QTY_SUMM, 
              sum(LATEST_PO_COST)    LATEST_PO_COST_SUMM, 
              sum(LATEST_PO_SELLING) LATEST_PO_SELLING_SUMM 
            from (select /*+ full (rtl) parallel (rtl,4)*/ 
                      rtl.sk1_item_no,
                      supp.sk1_supplier_no,
                      cal.this_week_start_date, 
                      dimp.CONTRACT_NO,
                      case when dimsc.SUPPLY_CHAIN_CODE='WH' then dimp.PO_NO else NULL end PO_NO,
                      rtl.LATEST_PO_QTY,
                      rtl.LATEST_PO_COST, 
                      rtl.LATEST_PO_SELLING 
                    from DWH_PERFORMANCE.RTL_PO_SUPCHAIN_LOC_ITEM_DY       rtl, 
                         DWH_PERFORMANCE.DIM_SUPPLY_CHAIN_TYPE             dimsc,
                         DWH_PERFORMANCE.DIM_PURCHASE_ORDER                dimp,
                         DWH_PERFORMANCE.wrk_item_ch_subset                item,
                         DWH_PERFORMANCE.wrk_supplier_primary_subset       supp,
                         DWH_PERFORMANCE.DIM_CALENDAR                      cal
                   where dimsc.SK1_SUPPLY_CHAIN_NO = rtl.SK1_SUPPLY_CHAIN_NO 
                     and dimp.SK1_PO_NO             = rtl.SK1_PO_NO 
                     and rtl.SK1_ITEM_NO            = item.SK1_ITEM_NO 
                     and item.SK1_STYLE_COLOUR_NO   = supp.SK1_STYLE_COLOUR_NO
                     and  item.SK1_SUPPLIER_NO      = supp.SK1_SUPPLIER_NO
                     and rtl.TRAN_DATE              = cal.calendar_date
                     and rtl.PO_IND                 = 1 
  --                  and rtl.SK1_CONTRACT_NO = 0
                     and cal.Fin_Year_no            >= proc_fin_yr
                   ) 
                   group by sk1_item_no,
                            sk1_supplier_no,
                            this_week_start_date,
                            CONTRACT_NO,
                            PO_NO,
                            LATEST_PO_QTY) res
                   group by     sk1_item_no,
                                sk1_supplier_no,
                                this_week_start_date,
                                CONTRACT_NO,
                                PO_NO,
                                LATEST_PO_QTY;
  
  po_recs_inserted  :=SQL%ROWCOUNT;
                   
 commit;
 
 g_recs_read      := boc_recs_inserted + po_recs_inserted;
 g_recs_inserted  := boc_recs_inserted + po_recs_inserted;

end rebuild_insert;

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
    l_text := 'LOAD OF WH PROCUREMENT FACT DATA STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

EXECUTE IMMEDIATE 'alter session enable parallel dml';

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);


    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    rebuild_insert;

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

end wh_prf_corp_300u;
