--------------------------------------------------------
--  DDL for Procedure WH_PRF_MC_930U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MC_930U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2018
--  Author:      Alastair de Wet
--  Purpose:     Create RMS LID SIMANTIC sales fact table in the performance layer
--               with input ex RMS DENSE table from performance layer.
--  Tables:      Input  - RTL_MC_LOC_ITEM_WK_RMS_DENSE
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MC_930U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS MC DENSE SALES EX PERFORMANCE';
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

    select /*+ parallel(dns,4) */ *
       from   RTL_MC_LOC_ITEM_WK_RMS_DENSE dns
       where  dns.last_updated_date = g_date 
            
    ) mer_rec
    ON
       (mer_rec.SK1_LOCATION_NO   = rtl_sim.SK1_LOCATION_NO
    and mer_rec.SK1_ITEM_NO       = rtl_sim.SK1_ITEM_NO
    and mer_rec.FIN_YEAR_NO       = rtl_sim.FIN_YEAR_NO
    and mer_rec.FIN_WEEK_NO       = rtl_sim.FIN_WEEK_NO)
    WHEN MATCHED
    THEN
    UPDATE
    SET           sales_qty                       = mer_rec.sales_qty,
                  sales_cases                     = mer_rec.sales_cases,
                  sales                           = mer_rec.sales,
                  sales_incl_vat                  = mer_rec.sales_incl_vat,
                  sales_cost                      = mer_rec.sales_cost,
                  sales_fr_cost                   = mer_rec.sales_fr_cost,
                  sales_margin                    = mer_rec.sales_margin,
                  franchise_sales                 = mer_rec.franchise_sales,
                  franchise_sales_margin          = mer_rec.franchise_sales_margin,
                  reg_sales_qty                   = mer_rec.reg_sales_qty,
                  reg_sales                       = mer_rec.reg_sales,
                  reg_sales_cost                  = mer_rec.reg_sales_cost,
                  reg_sales_fr_cost               = mer_rec.reg_sales_fr_cost,
                  reg_sales_margin                = mer_rec.reg_sales_margin,
                  franchise_reg_sales_margin      = mer_rec.franchise_reg_sales_margin,
                  gross_sales_qty                 = mer_rec.gross_sales_qty,
                  gross_sales                     = mer_rec.gross_sales,
                  gross_sales_cost                = mer_rec.gross_sales_cost,
                  gross_sales_fr_cost             = mer_rec.gross_sales_fr_cost,
                  gross_reg_sales_qty             = mer_rec.gross_reg_sales_qty,
                  gross_reg_sales                 = mer_rec.gross_reg_sales,
                  gross_reg_sales_cost            = mer_rec.gross_reg_sales_cost,
                  gross_reg_sales_fr_cost         = mer_rec.gross_reg_sales_fr_cost,
                  sdn_in_qty                      = mer_rec.sdn_in_qty,
                  sdn_in_selling                  = mer_rec.sdn_in_selling,
                  sdn_in_cost                     = mer_rec.sdn_in_cost,
                  sdn_in_fr_cost                  = mer_rec.sdn_in_fr_cost,
                  sdn_in_cases                    = mer_rec.sdn_in_cases,
                  store_deliv_selling             = mer_rec.store_deliv_selling,
                  store_deliv_cost                = mer_rec.store_deliv_cost,
                  store_deliv_fr_cost             = mer_rec.store_deliv_fr_cost,
                  store_deliv_qty                 = mer_rec.store_deliv_qty,
                  store_deliv_cases               = mer_rec.store_deliv_cases,
                  store_intake_qty                = mer_rec.store_intake_qty,
                  store_intake_selling            = mer_rec.store_intake_selling,
                  store_intake_cost               = mer_rec.store_intake_cost,
                  store_intake_fr_cost            = mer_rec.store_intake_fr_cost,
                  store_intake_margin             = mer_rec.store_intake_margin,
                  sales_returns_qty               = mer_rec.sales_returns_qty,
                  sales_returns_selling           = mer_rec.sales_returns_selling,
                  sales_returns_cost              = mer_rec.sales_returns_cost,
                  sales_returns_fr_cost           = mer_rec.sales_returns_fr_cost,
                  reg_sales_returns_qty           = mer_rec.reg_sales_returns_qty,
                  reg_sales_returns_selling       = mer_rec.reg_sales_returns_selling,
                  reg_sales_returns_cost          = mer_rec.reg_sales_returns_cost,
                  reg_sales_returns_fr_cost       = mer_rec.reg_sales_returns_fr_cost,
                  clear_sales_returns_selling     = mer_rec.clear_sales_returns_selling,
                  clear_sales_returns_cost        = mer_rec.clear_sales_returns_cost,
                  clear_sales_returns_fr_cost     = mer_rec.clear_sales_returns_fr_cost,
                  clear_sales_returns_qty         = mer_rec.clear_sales_returns_qty,
--LOCAL                  
                  SALES_LOCAL	                    =	mer_rec.	SALES_LOCAL	,
                  SALES_INCL_VAT_LOCAL	          =	mer_rec.	SALES_INCL_VAT_LOCAL	,
                  SALES_COST_LOCAL	              =	mer_rec.	SALES_COST_LOCAL	,
                  SALES_FR_COST_LOCAL	            =	mer_rec.	SALES_FR_COST_LOCAL	,
                  SALES_MARGIN_LOCAL	            =	mer_rec.	SALES_MARGIN_LOCAL	,
                  FRANCHISE_SALES_LOCAL	          =	mer_rec.	FRANCHISE_SALES_LOCAL	,
                  FRANCHISE_SALES_MARGIN_LOCAL	  =	mer_rec.	FRANCHISE_SALES_MARGIN_LOCAL	,
                  REG_SALES_LOCAL	                =	mer_rec.	REG_SALES_LOCAL	,
                  REG_SALES_COST_LOCAL	          =	mer_rec.	REG_SALES_COST_LOCAL	,
                  REG_SALES_FR_COST_LOCAL	        =	mer_rec.	REG_SALES_FR_COST_LOCAL	,
                  REG_SALES_MARGIN_LOCAL	        =	mer_rec.	REG_SALES_MARGIN_LOCAL	,
                  FRANCHISE_REG_SALES_MRGN_LOCAL	=	mer_rec.	FRANCHISE_REG_SALES_MRGN_LOCAL	,
                  GROSS_SALES_LOCAL	              =	mer_rec.	GROSS_SALES_LOCAL	,
                  GROSS_SALES_COST_LOCAL	        =	mer_rec.	GROSS_SALES_COST_LOCAL	,
                  GROSS_SALES_FR_COST_LOCAL	      =	mer_rec.	GROSS_SALES_FR_COST_LOCAL	,
                  GROSS_REG_SALES_LOCAL	          =	mer_rec.	GROSS_REG_SALES_LOCAL	,
                  GROSS_REG_SALES_COST_LOCAL	    =	mer_rec.	GROSS_REG_SALES_COST_LOCAL	,
                  GROSS_REG_SALES_FR_COST_LOCAL	  =	mer_rec.	GROSS_REG_SALES_FR_COST_LOCAL	,
                  SDN_IN_SELLING_LOCAL	          =	mer_rec.	SDN_IN_SELLING_LOCAL	,
                  SDN_IN_COST_LOCAL	              =	mer_rec.	SDN_IN_COST_LOCAL	,
                  SDN_IN_FR_COST_LOCAL	          =	mer_rec.	SDN_IN_FR_COST_LOCAL	,
                  STORE_DELIV_SELLING_LOCAL	      =	mer_rec.	STORE_DELIV_SELLING_LOCAL	,
                  STORE_DELIV_COST_LOCAL	        =	mer_rec.	STORE_DELIV_COST_LOCAL	,
                  STORE_DELIV_FR_COST_LOCAL	      =	mer_rec.	STORE_DELIV_FR_COST_LOCAL	,
                  STORE_INTAKE_SELLING_LOCAL	    =	mer_rec.	STORE_INTAKE_SELLING_LOCAL	,
                  STORE_INTAKE_COST_LOCAL	        =	mer_rec.	STORE_INTAKE_COST_LOCAL	,
                  STORE_INTAKE_FR_COST_LOCAL	    =	mer_rec.	STORE_INTAKE_FR_COST_LOCAL	,
                  STORE_INTAKE_MARGIN_LOCAL	      =	mer_rec.	STORE_INTAKE_MARGIN_LOCAL	,
                  SALES_RETURNS_SELLING_LOCAL	    =	mer_rec.	SALES_RETURNS_SELLING_LOCAL	,
                  SALES_RETURNS_COST_LOCAL	      =	mer_rec.	SALES_RETURNS_COST_LOCAL	,
                  SALES_RETURNS_FR_COST_LOCAL	    =	mer_rec.	SALES_RETURNS_FR_COST_LOCAL	,
                  REG_SALES_RTNS_SELLING_LOCAL	  =	mer_rec.	REG_SALES_RTNS_SELLING_LOCAL	,
                  REG_SALES_RTNS_COST_LOCAL	      =	mer_rec.	REG_SALES_RTNS_COST_LOCAL	,
                  REG_SALES_RTNS_FR_COST_LOCAL	  =	mer_rec.	REG_SALES_RTNS_FR_COST_LOCAL	,
                  CLEAR_SALES_RTNS_SELLING_LOCAL	=	mer_rec.	CLEAR_SALES_RTNS_SELLING_LOCAL	,
                  CLEAR_SALES_RTNS_COST_LOCAL	    =	mer_rec.	CLEAR_SALES_RTNS_COST_LOCAL	,
                  CLEAR_SALES_RTNS_FR_COST_LOCAL	=	mer_rec.	CLEAR_SALES_RTNS_FR_COST_LOCAL	,
            
                  last_updated_date               = g_date
    
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
                  SALES_QTY,
                  SALES_CASES,
                  SALES,
                  SALES_INCL_VAT,
                  SALES_COST,
                  SALES_FR_COST,
                  SALES_MARGIN,
                  FRANCHISE_SALES,
                  FRANCHISE_SALES_MARGIN,
                  REG_SALES_QTY,
                  REG_SALES,
                  REG_SALES_COST,
                  REG_SALES_FR_COST,
                  REG_SALES_MARGIN,
                  FRANCHISE_REG_SALES_MARGIN,
                  GROSS_SALES_QTY,
                  GROSS_SALES,
                  GROSS_SALES_COST,
                  GROSS_SALES_FR_COST,
                  GROSS_REG_SALES_QTY,
                  GROSS_REG_SALES,
                  GROSS_REG_SALES_COST,
                  GROSS_REG_SALES_FR_COST,
                  SDN_IN_QTY,
                  SDN_IN_SELLING,
                  SDN_IN_COST,
                  SDN_IN_FR_COST,
                  SDN_IN_CASES,
                  ACTL_STORE_RCPT_QTY,
                  ACTL_STORE_RCPT_SELLING,
                  ACTL_STORE_RCPT_COST,
                  ACTL_STORE_RCPT_FR_COST,
                  STORE_DELIV_SELLING,
                  STORE_DELIV_COST,
                  STORE_DELIV_FR_COST,
                  STORE_INTAKE_QTY,
                  STORE_INTAKE_SELLING,
                  STORE_INTAKE_COST,
                  STORE_INTAKE_FR_COST,
                  STORE_INTAKE_MARGIN,
                  SALES_RETURNS_QTY,
                  SALES_RETURNS_SELLING,
                  SALES_RETURNS_COST,
                  SALES_RETURNS_FR_COST,
                  REG_SALES_RETURNS_QTY,
                  REG_SALES_RETURNS_SELLING,
                  REG_SALES_RETURNS_COST,
                  REG_SALES_RETURNS_FR_COST,
                  CLEAR_SALES_RETURNS_SELLING,
                  CLEAR_SALES_RETURNS_COST,
                  CLEAR_SALES_RETURNS_FR_COST,
                  CLEAR_SALES_RETURNS_QTY,
                  LAST_UPDATED_DATE,
                  STORE_DELIV_QTY,
                  STORE_DELIV_CASES,
--LOCAL                  
                  SALES_LOCAL	,
                  SALES_INCL_VAT_LOCAL	,
                  SALES_COST_LOCAL	,
                  SALES_FR_COST_LOCAL	,
                  SALES_MARGIN_LOCAL	,
                  FRANCHISE_SALES_LOCAL	,
                  FRANCHISE_SALES_MARGIN_LOCAL	,
                  REG_SALES_LOCAL	,
                  REG_SALES_COST_LOCAL	,
                  REG_SALES_FR_COST_LOCAL	,
                  REG_SALES_MARGIN_LOCAL	,
                  FRANCHISE_REG_SALES_MRGN_LOCAL	,
                  GROSS_SALES_LOCAL	,
                  GROSS_SALES_COST_LOCAL	,
                  GROSS_SALES_FR_COST_LOCAL	,
                  GROSS_REG_SALES_LOCAL	,
                  GROSS_REG_SALES_COST_LOCAL	,
                  GROSS_REG_SALES_FR_COST_LOCAL	,
                  SDN_IN_SELLING_LOCAL	,
                  SDN_IN_COST_LOCAL	,
                  SDN_IN_FR_COST_LOCAL	,
--                  ACTL_STORE_RCPT_SELLING_LOCAL	,
--                  ACTL_STORE_RCPT_COST_LOCAL	,
--                  ACTL_STORE_RCPT_FR_COST_LOCAL	,
                  STORE_DELIV_SELLING_LOCAL	,
                  STORE_DELIV_COST_LOCAL	,
                  STORE_DELIV_FR_COST_LOCAL	,
                  STORE_INTAKE_SELLING_LOCAL	,
                  STORE_INTAKE_COST_LOCAL	,
                  STORE_INTAKE_FR_COST_LOCAL	,
                  STORE_INTAKE_MARGIN_LOCAL	,
                  SALES_RETURNS_SELLING_LOCAL	,
                  SALES_RETURNS_COST_LOCAL	,
                  SALES_RETURNS_FR_COST_LOCAL	,
                  REG_SALES_RTNS_SELLING_LOCAL	,
                  REG_SALES_RTNS_COST_LOCAL	,
                  REG_SALES_RTNS_FR_COST_LOCAL	,
                  CLEAR_SALES_RTNS_SELLING_LOCAL	,
                  CLEAR_SALES_RTNS_COST_LOCAL	,
                  CLEAR_SALES_RTNS_FR_COST_LOCAL	 
--                  EOL_SALES_LOCAL	
--                  EOL_DISCOUNT_LOCAL	 

                  
                  
    )
    VALUES
    (
            mer_rec.SK1_LOCATION_NO,
            mer_rec.SK1_ITEM_NO,
            mer_rec.FIN_YEAR_NO,
            mer_rec.FIN_WEEK_NO,
            mer_rec.FIN_WEEK_CODE,
            mer_rec.THIS_WEEK_START_DATE,
            mer_rec.SK2_LOCATION_NO,
            mer_rec.SK2_ITEM_NO,
            mer_rec.SALES_QTY,
            mer_rec.SALES_CASES,
            mer_rec.SALES,
            mer_rec.SALES_INCL_VAT,
            mer_rec.SALES_COST,
            mer_rec.SALES_FR_COST,
            mer_rec.SALES_MARGIN,
            mer_rec.FRANCHISE_SALES,
            mer_rec.FRANCHISE_SALES_MARGIN,
            mer_rec.REG_SALES_QTY,
            mer_rec.REG_SALES,
            mer_rec.REG_SALES_COST,
            mer_rec.REG_SALES_FR_COST,
            mer_rec.REG_SALES_MARGIN,
            mer_rec.FRANCHISE_REG_SALES_MARGIN,
            mer_rec.GROSS_SALES_QTY,
            mer_rec.GROSS_SALES,
            mer_rec.GROSS_SALES_COST,
            mer_rec.GROSS_SALES_FR_COST,
            mer_rec.GROSS_REG_SALES_QTY,
            mer_rec.GROSS_REG_SALES,
            mer_rec.GROSS_REG_SALES_COST,
            mer_rec.GROSS_REG_SALES_FR_COST,
            mer_rec.SDN_IN_QTY,
            mer_rec.SDN_IN_SELLING,
            mer_rec.SDN_IN_COST,
            mer_rec.SDN_IN_FR_COST,
            mer_rec.SDN_IN_CASES,
            mer_rec.ACTL_STORE_RCPT_QTY,
            mer_rec.ACTL_STORE_RCPT_SELLING,
            mer_rec.ACTL_STORE_RCPT_COST,
            mer_rec.ACTL_STORE_RCPT_FR_COST,
            mer_rec.STORE_DELIV_SELLING,
            mer_rec.STORE_DELIV_COST,
            mer_rec.STORE_DELIV_FR_COST,
            mer_rec.STORE_INTAKE_QTY,
            mer_rec.STORE_INTAKE_SELLING,
            mer_rec.STORE_INTAKE_COST,
            mer_rec.STORE_INTAKE_FR_COST,
            mer_rec.STORE_INTAKE_MARGIN,
            mer_rec.SALES_RETURNS_QTY,
            mer_rec.SALES_RETURNS_SELLING,
            mer_rec.SALES_RETURNS_COST,
            mer_rec.SALES_RETURNS_FR_COST,
            mer_rec.REG_SALES_RETURNS_QTY,
            mer_rec.REG_SALES_RETURNS_SELLING,
            mer_rec.REG_SALES_RETURNS_COST,
            mer_rec.REG_SALES_RETURNS_FR_COST,
            mer_rec.CLEAR_SALES_RETURNS_SELLING,
            mer_rec.CLEAR_SALES_RETURNS_COST,
            mer_rec.CLEAR_SALES_RETURNS_FR_COST,
            mer_rec.CLEAR_SALES_RETURNS_QTY,
            g_date,
            mer_rec.STORE_DELIV_QTY,
            mer_rec.STORE_DELIV_CASES,
--LOCAL            
            mer_rec.	SALES_LOCAL	,
            mer_rec.	SALES_INCL_VAT_LOCAL	,
            mer_rec.	SALES_COST_LOCAL	,
            mer_rec.	SALES_FR_COST_LOCAL	,
            mer_rec.	SALES_MARGIN_LOCAL	,
            mer_rec.	FRANCHISE_SALES_LOCAL	,
            mer_rec.	FRANCHISE_SALES_MARGIN_LOCAL	,
            mer_rec.	REG_SALES_LOCAL	,
            mer_rec.	REG_SALES_COST_LOCAL	,
            mer_rec.	REG_SALES_FR_COST_LOCAL	,
            mer_rec.	REG_SALES_MARGIN_LOCAL	,
            mer_rec.	FRANCHISE_REG_SALES_MRGN_LOCAL	,
            mer_rec.	GROSS_SALES_LOCAL	,
            mer_rec.	GROSS_SALES_COST_LOCAL	,
            mer_rec.	GROSS_SALES_FR_COST_LOCAL	,
            mer_rec.	GROSS_REG_SALES_LOCAL	,
            mer_rec.	GROSS_REG_SALES_COST_LOCAL	,
            mer_rec.	GROSS_REG_SALES_FR_COST_LOCAL	,
            mer_rec.	SDN_IN_SELLING_LOCAL	,
            mer_rec.	SDN_IN_COST_LOCAL	,
            mer_rec.	SDN_IN_FR_COST_LOCAL	,
--            mer_rec.	ACTL_STORE_RCPT_SELLING_LOCAL	,
--            mer_rec.	ACTL_STORE_RCPT_COST_LOCAL	,
--            mer_rec.	ACTL_STORE_RCPT_FR_COST_LOCAL	,
            mer_rec.	STORE_DELIV_SELLING_LOCAL	,
            mer_rec.	STORE_DELIV_COST_LOCAL	,
            mer_rec.	STORE_DELIV_FR_COST_LOCAL	,
            mer_rec.	STORE_INTAKE_SELLING_LOCAL	,
            mer_rec.	STORE_INTAKE_COST_LOCAL	,
            mer_rec.	STORE_INTAKE_FR_COST_LOCAL	,
            mer_rec.	STORE_INTAKE_MARGIN_LOCAL	,
            mer_rec.	SALES_RETURNS_SELLING_LOCAL	,
            mer_rec.	SALES_RETURNS_COST_LOCAL	,
            mer_rec.	SALES_RETURNS_FR_COST_LOCAL	,
            mer_rec.	REG_SALES_RTNS_SELLING_LOCAL	,
            mer_rec.	REG_SALES_RTNS_COST_LOCAL	,
            mer_rec.	REG_SALES_RTNS_FR_COST_LOCAL	,
            mer_rec.	CLEAR_SALES_RTNS_SELLING_LOCAL	,
            mer_rec.	CLEAR_SALES_RTNS_COST_LOCAL	,
            mer_rec.	CLEAR_SALES_RTNS_FR_COST_LOCAL	 
--            mer_rec.	EOL_SALES_LOCAL	
--            mer_rec.	EOL_DISCOUNT_LOCAL	

    );

   g_recs_inserted  := g_recs_inserted  + sql%rowcount;    --a_tbl_merge.count;
   g_recs_read      := g_recs_read      + sql%rowcount;
   g_recs_updated   := g_recs_updated   + sql%rowcount;

    commit;
    
        
    --**************************************** CHECK IF LOAD BALANCES ***********************************
    
    select sum(sales) 
    into g_prf_sale
    from RTL_MC_LOC_ITEM_WK_SIMANTIC
    where LAST_UPDATED_DATE = g_date;
    
    select  sum(sales)
    into g_fnd_sale
    from RTL_MC_LOC_ITEM_WK_RMS_DENSE  
    where LAST_UPDATED_DATE = g_date;
    
    l_text := ' Foundation sales = '||g_fnd_sale||'   Performance sales = '||g_prf_sale ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    if g_fnd_sale <> g_prf_sale then   
       g_prf_sale := g_prf_sale/0;
    end if;   

    

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

end WH_PRF_MC_930U;
