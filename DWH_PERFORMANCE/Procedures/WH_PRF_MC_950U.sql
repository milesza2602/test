--------------------------------------------------------
--  DDL for Procedure WH_PRF_MC_950U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MC_950U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2018
--  Author:      Alastair de Wet
--  Purpose:     Create RMS LID SIMANTIC sales fact table in the performance layer
--               with input ex RMS DENSE table from performance layer.
--  Tables:      Input  - RTL_MC_LOC_ITEM_WK_RMS_STOCK
--               Output - RTL_MC_LOC_ITEM_WK_SIMANTIC
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
g_fnd_sale           number(14,2)  :=  0;
g_prf_sale           number(14,2)  :=  0;
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          number        :=  0;
g_recs_inserted      number        :=  0;
g_recs_updated       number        :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_rec_out            RTL_MC_LOC_ITEM_WK_SIMANTIC%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MC_950U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS MC STOCK SALES EX PERFORMANCE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF RTL_MC_LOC_ITEM_WK_SIMANTIC EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
    execute immediate 'alter session enable parallel dml';
    
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '14/AUG/17';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
    l_text := 'MERGE STARTING ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    MERGE /*+ parallel(rtl_sim,4) */ INTO RTL_MC_LOC_ITEM_WK_SIMANTIC rtl_sim
    USING
    (
--select /*+ parallel(di,4) full(dl) full(dlh) full(dih) parallel(fnd_li,4) full(vr) parallel(fnd_lid,4)  */  
--select /*+ parallel(fnd_lid,4) full(dl) full(dlh) full(dih) full(vr) index(fnd_li PK_P_RTL_LCTN_ITM) */
--select /*+ full(di) full(dl) full(dlh) full(dih) parallel(vr,4) full(fnd_li) parallel(fnd_li,4) */  -- as it is in corp 110u 
--/*+ parallel(di,4) parallel(dih,4)  full(dl) full(dlh)  parallel(vr,4) parallel(fi_vr,4)  parallel(fnd_li,4) parallel(fnd_lid,4)*/

    select /*+ parallel(STK,4) */ * 
              
       from   RTL_MC_LOC_ITEM_WK_RMS_STOCK STK
       where  STK.last_updated_date = g_date 
--  INSERT IF YOU WANT TO RESTRICT STOCK TO ONLY WHERE SALES/SPARSE EXITS -------       
--       AND     EXISTS (SELECT 1 FROM RTL_MC_LOC_ITEM_WK_SIMANTIC  SIMR
--                       WHERE SIMR.SK1_LOCATION_NO = STK.SK1_LOCATION_NO
--                       AND   SIMR.SK1_ITEM_NO     = STK.SK1_ITEM_NO
--                       AND   SIMR.POST_DATE       = STK.POST_DATE)
            
    ) mer_rec
    ON
       (mer_rec.SK1_LOCATION_NO   = rtl_sim.SK1_LOCATION_NO
    and mer_rec.SK1_ITEM_NO       = rtl_sim.SK1_ITEM_NO
    and mer_rec.FIN_YEAR_NO       = rtl_sim.FIN_YEAR_NO
    and mer_rec.FIN_WEEK_NO       = rtl_sim.FIN_WEEK_NO)
    WHEN MATCHED
    THEN
    UPDATE
    SET           
                  NUM_COM_FLAG_IND	=	MER_REC.	NUM_COM_FLAG_IND	,
                  SIT_QTY	=	MER_REC.	SIT_QTY	,
                  SIT_CASES	=	MER_REC.	SIT_CASES	,
                  SIT_SELLING	=	MER_REC.	SIT_SELLING	,
                  SIT_COST	=	MER_REC.	SIT_COST	,
                  SIT_FR_COST	=	MER_REC.	SIT_FR_COST	,
                  SIT_MARGIN	=	MER_REC.	SIT_MARGIN	,
                  NON_SELLABLE_QTY	=	MER_REC.	NON_SELLABLE_QTY	,
                  SOH_QTY	=	MER_REC.	SOH_QTY	,
                  SOH_CASES	=	MER_REC.	SOH_CASES	,
                  SOH_SELLING	=	MER_REC.	SOH_SELLING	,
                  SOH_COST	=	MER_REC.	SOH_COST	,
                  SOH_FR_COST	=	MER_REC.	SOH_FR_COST	,
                  SOH_MARGIN	=	MER_REC.	SOH_MARGIN	,
                  FRANCHISE_SOH_MARGIN	=	MER_REC.	FRANCHISE_SOH_MARGIN	,
                  INBOUND_EXCL_CUST_ORD_QTY	=	MER_REC.	INBOUND_EXCL_CUST_ORD_QTY	,
                  INBOUND_EXCL_CUST_ORD_SELLING	=	MER_REC.	INBOUND_EXCL_CUST_ORD_SELLING	,
                  INBOUND_EXCL_CUST_ORD_COST	=	MER_REC.	INBOUND_EXCL_CUST_ORD_COST	,
                  INBOUND_INCL_CUST_ORD_QTY	=	MER_REC.	INBOUND_INCL_CUST_ORD_QTY	,
                  INBOUND_INCL_CUST_ORD_SELLING	=	MER_REC.	INBOUND_INCL_CUST_ORD_SELLING	,
                  INBOUND_INCL_CUST_ORD_COST	=	MER_REC.	INBOUND_INCL_CUST_ORD_COST	,
                  BOH_QTY	=	MER_REC.	BOH_QTY	,
                  BOH_CASES	=	MER_REC.	BOH_CASES	,
                  BOH_SELLING	=	MER_REC.	BOH_SELLING	,
                  BOH_COST	=	MER_REC.	BOH_COST	,
                  BOH_FR_COST	=	MER_REC.	BOH_FR_COST	,
                  CLEAR_SOH_QTY	=	MER_REC.	CLEAR_SOH_QTY	,
                  CLEAR_SOH_SELLING	=	MER_REC.	CLEAR_SOH_SELLING	,
                  CLEAR_SOH_COST	=	MER_REC.	CLEAR_SOH_COST	,
                  CLEAR_SOH_FR_COST	=	MER_REC.	CLEAR_SOH_FR_COST	,
                  REG_SOH_QTY	=	MER_REC.	REG_SOH_QTY	,
                  REG_SOH_SELLING	=	MER_REC.	REG_SOH_SELLING	,
                  REG_SOH_COST	=	MER_REC.	REG_SOH_COST	,
                  REG_SOH_FR_COST	=	MER_REC.	REG_SOH_FR_COST	,
                  LAST_UPDATED_DATE	=	MER_REC.	LAST_UPDATED_DATE	,
--                  NUM_STOCK_COUNTS	=	MER_REC.	NUM_STOCK_COUNTS	,
                  CLEAR_SOH_MARGIN	=	MER_REC.	CLEAR_SOH_MARGIN	,
                  REG_SOH_MARGIN	=	MER_REC.	REG_SOH_MARGIN	,
                  INBND_EXCL_CUST_ORD_COST_LOCAL	=	MER_REC.	INBND_EXCL_CUST_ORD_COST_LOCAL	,
                  INBND_EXCL_CUST_ORD_SELL_LOCAL	=	MER_REC.	INBND_EXCL_CUST_ORD_SELL_LOCAL	,
                  INBND_INCL_CUST_ORD_COST_LOCAL	=	MER_REC.	INBND_INCL_CUST_ORD_COST_LOCAL	,
                  INBND_INCL_CUST_ORD_SELL_LOCAL	=	MER_REC.	INBND_INCL_CUST_ORD_SELL_LOCAL	,
                  SIT_COST_LOCAL	=	MER_REC.	SIT_COST_LOCAL	,
                  SIT_SELLING_LOCAL	=	MER_REC.	SIT_SELLING_LOCAL	,
                  SIT_FR_COST_LOCAL	=	MER_REC.	SIT_FR_COST_LOCAL	,
                  SIT_MARGIN_LOCAL	=	MER_REC.	SIT_MARGIN_LOCAL	,
                  SOH_COST_LOCAL	=	MER_REC.	SOH_COST_LOCAL	,
                  SOH_SELLING_LOCAL	=	MER_REC.	SOH_SELLING_LOCAL	,
                  SOH_FR_COST_LOCAL	=	MER_REC.	SOH_FR_COST_LOCAL	,
                  SOH_MARGIN_LOCAL	=	MER_REC.	SOH_MARGIN_LOCAL	,
                  FRANCHISE_SOH_MARGIN_LOCAL	=	MER_REC.	FRANCHISE_SOH_MARGIN_LOCAL	,
                  BOH_SELLING_LOCAL	=	MER_REC.	BOH_SELLING_LOCAL	,
                  BOH_COST_LOCAL	=	MER_REC.	BOH_COST_LOCAL	,
                  BOH_FR_COST_LOCAL	=	MER_REC.	BOH_FR_COST_LOCAL	,
                  CLEAR_SOH_COST_LOCAL	=	MER_REC.	CLEAR_SOH_COST_LOCAL	,
                  CLEAR_SOH_SELLING_LOCAL	=	MER_REC.	CLEAR_SOH_SELLING_LOCAL	,
                  CLEAR_SOH_MARGIN_LOCAL	=	MER_REC.	CLEAR_SOH_MARGIN_LOCAL	,
                  CLEAR_SOH_FR_COST_LOCAL	=	MER_REC.	CLEAR_SOH_FR_COST_LOCAL	,
                  REG_SOH_SELLING_LOCAL	=	MER_REC.	REG_SOH_SELLING_LOCAL	,
                  REG_SOH_COST_LOCAL	=	MER_REC.	REG_SOH_COST_LOCAL	,
                  REG_SOH_FR_COST_LOCAL	=	MER_REC.	REG_SOH_FR_COST_LOCAL	,
                  REG_SOH_MARGIN_LOCAL	=	MER_REC.	REG_SOH_MARGIN_LOCAL	 

                      
    WHEN NOT MATCHED
    THEN
    INSERT
    (
                  SK1_LOCATION_NO,
                  SK1_ITEM_NO,
                  FIN_YEAR_NO,
                  FIN_WEEK_NO,
                  FIN_WEEK_CODE,
                  THIS_WEEK_START_DATE,
                  SK2_LOCATION_NO,
                  SK2_ITEM_NO,
                  NUM_COM_FLAG_IND,
                  SIT_QTY,
                  SIT_CASES,
                  SIT_SELLING,
                  SIT_COST,
                  SIT_FR_COST,
                  SIT_MARGIN,
                  NON_SELLABLE_QTY,
                  SOH_QTY,
                  SOH_CASES,
                  SOH_SELLING,
                  SOH_COST,
                  SOH_FR_COST,
                  SOH_MARGIN,
                  FRANCHISE_SOH_MARGIN,
                  INBOUND_EXCL_CUST_ORD_QTY,
                  INBOUND_EXCL_CUST_ORD_SELLING,
                  INBOUND_EXCL_CUST_ORD_COST,
                  INBOUND_INCL_CUST_ORD_QTY,
                  INBOUND_INCL_CUST_ORD_SELLING,
                  INBOUND_INCL_CUST_ORD_COST,
                  BOH_QTY,
                  BOH_CASES,
                  BOH_SELLING,
                  BOH_COST,
                  BOH_FR_COST,
                  CLEAR_SOH_QTY,
                  CLEAR_SOH_SELLING,
                  CLEAR_SOH_COST,
                  CLEAR_SOH_FR_COST,
                  REG_SOH_QTY,
                  REG_SOH_SELLING,
                  REG_SOH_COST,
                  REG_SOH_FR_COST,
                  LAST_UPDATED_DATE,
--                  NUM_STOCK_COUNTS,
                  CLEAR_SOH_MARGIN,
                  REG_SOH_MARGIN,
                  INBND_EXCL_CUST_ORD_COST_LOCAL,
                  INBND_EXCL_CUST_ORD_SELL_LOCAL,
                  INBND_INCL_CUST_ORD_COST_LOCAL,
                  INBND_INCL_CUST_ORD_SELL_LOCAL,
                  SIT_COST_LOCAL,
                  SIT_SELLING_LOCAL,
                  SIT_FR_COST_LOCAL,
                  SIT_MARGIN_LOCAL,
                  SOH_COST_LOCAL,
                  SOH_SELLING_LOCAL,
                  SOH_FR_COST_LOCAL,
                  SOH_MARGIN_LOCAL,
                  FRANCHISE_SOH_MARGIN_LOCAL,
                  BOH_SELLING_LOCAL,
                  BOH_COST_LOCAL,
                  BOH_FR_COST_LOCAL,
                  CLEAR_SOH_COST_LOCAL,
                  CLEAR_SOH_SELLING_LOCAL,
                  CLEAR_SOH_MARGIN_LOCAL,
                  CLEAR_SOH_FR_COST_LOCAL,
                  REG_SOH_SELLING_LOCAL,
                  REG_SOH_COST_LOCAL,
                  REG_SOH_FR_COST_LOCAL,
                  REG_SOH_MARGIN_LOCAL

                  
                  
    )
    VALUES
    (
                  MER_REC.	SK1_LOCATION_NO,
                  MER_REC.	SK1_ITEM_NO,
                  mer_rec.  FIN_YEAR_NO,
                  mer_rec.  FIN_WEEK_NO,
                  mer_rec.  FIN_WEEK_CODE,
                  mer_rec.  THIS_WEEK_START_DATE,
                  MER_REC.	SK2_LOCATION_NO,
                  MER_REC.	SK2_ITEM_NO,
                  MER_REC.	NUM_COM_FLAG_IND	,
                  MER_REC.	SIT_QTY	,
                  MER_REC.	SIT_CASES	,
                  MER_REC.	SIT_SELLING	,
                  MER_REC.	SIT_COST	,
                  MER_REC.	SIT_FR_COST	,
                  MER_REC.	SIT_MARGIN	,
                  MER_REC.	NON_SELLABLE_QTY	,
                  MER_REC.	SOH_QTY	,
                  MER_REC.	SOH_CASES	,
                  MER_REC.	SOH_SELLING	,
                  MER_REC.	SOH_COST	,
                  MER_REC.	SOH_FR_COST	,
                  MER_REC.	SOH_MARGIN	,
                  MER_REC.	FRANCHISE_SOH_MARGIN	,
                  MER_REC.	INBOUND_EXCL_CUST_ORD_QTY	,
                  MER_REC.	INBOUND_EXCL_CUST_ORD_SELLING	,
                  MER_REC.	INBOUND_EXCL_CUST_ORD_COST	,
                  MER_REC.	INBOUND_INCL_CUST_ORD_QTY	,
                  MER_REC.	INBOUND_INCL_CUST_ORD_SELLING	,
                  MER_REC.	INBOUND_INCL_CUST_ORD_COST	,
                  MER_REC.	BOH_QTY	,
                  MER_REC.	BOH_CASES	,
                  MER_REC.	BOH_SELLING	,
                  MER_REC.	BOH_COST	,
                  MER_REC.	BOH_FR_COST	,
                  MER_REC.	CLEAR_SOH_QTY	,
                  MER_REC.	CLEAR_SOH_SELLING	,
                  MER_REC.	CLEAR_SOH_COST	,
                  MER_REC.	CLEAR_SOH_FR_COST	,
                  MER_REC.	REG_SOH_QTY	,
                  MER_REC.	REG_SOH_SELLING	,
                  MER_REC.	REG_SOH_COST	,
                  MER_REC.	REG_SOH_FR_COST	,
                  MER_REC.	LAST_UPDATED_DATE	,
--                  MER_REC.	NUM_STOCK_COUNTS	,
                  MER_REC.	CLEAR_SOH_MARGIN	,
                  MER_REC.	REG_SOH_MARGIN	,
                  MER_REC.	INBND_EXCL_CUST_ORD_COST_LOCAL	,
                  MER_REC.	INBND_EXCL_CUST_ORD_SELL_LOCAL	,
                  MER_REC.	INBND_INCL_CUST_ORD_COST_LOCAL	,
                  MER_REC.	INBND_INCL_CUST_ORD_SELL_LOCAL	,
                  MER_REC.	SIT_COST_LOCAL	,
                  MER_REC.	SIT_SELLING_LOCAL	,
                  MER_REC.	SIT_FR_COST_LOCAL	,
                  MER_REC.	SIT_MARGIN_LOCAL	,
                  MER_REC.	SOH_COST_LOCAL	,
                  MER_REC.	SOH_SELLING_LOCAL	,
                  MER_REC.	SOH_FR_COST_LOCAL	,
                  MER_REC.	SOH_MARGIN_LOCAL	,
                  MER_REC.	FRANCHISE_SOH_MARGIN_LOCAL	,
                  MER_REC.	BOH_SELLING_LOCAL	,
                  MER_REC.	BOH_COST_LOCAL	,
                  MER_REC.	BOH_FR_COST_LOCAL	,
                  MER_REC.	CLEAR_SOH_COST_LOCAL	,
                  MER_REC.	CLEAR_SOH_SELLING_LOCAL	,
                  MER_REC.	CLEAR_SOH_MARGIN_LOCAL	,
                  MER_REC.	CLEAR_SOH_FR_COST_LOCAL	,
                  MER_REC.	REG_SOH_SELLING_LOCAL	,
                  MER_REC.	REG_SOH_COST_LOCAL	,
                  MER_REC.	REG_SOH_FR_COST_LOCAL	,
                  MER_REC.	REG_SOH_MARGIN_LOCAL 


    );

   g_recs_inserted  := g_recs_inserted  + sql%rowcount;    --a_tbl_merge.count;
   g_recs_read      := g_recs_read      + sql%rowcount;
   g_recs_updated   := g_recs_updated   + sql%rowcount;

    commit;
    
        
    --**************************************** CHECK IF LOAD BALANCES ***********************************
/*    
    select sum(sales) 
    into g_prf_sale
    from RTL_MC_LOC_ITEM_WK_SIMANTIC
    where post_date = g_date;
    
    select  sum(sales)
    into g_fnd_sale
    from RTL_MC_LOC_ITEM_WK_RMS_STOCK  
    where post_date = g_date;
    
    l_text := ' Foundation sales = '||g_fnd_sale||'   Performance sales = '||g_prf_sale ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    if g_fnd_sale <> g_prf_sale then   
       g_prf_sale := g_prf_sale/0;
    end if;   
*/
    

--execute immediate 'alter session set events ''10046 trace name context off'' ';

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
l_text := '- abort-14---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
l_text := '- abort--15--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort--16--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
      when others then
l_text := '- abort-17---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
l_text := '- abort-18---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       rollback;
       p_success := false;
l_text := '- abort-19---';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       raise;
l_text := '- abort--20--';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end WH_PRF_MC_950U;
