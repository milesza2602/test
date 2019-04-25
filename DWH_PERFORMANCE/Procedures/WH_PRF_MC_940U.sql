--------------------------------------------------------
--  DDL for Procedure WH_PRF_MC_940U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MC_940U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2018
--  Author:      Alastair de Wet
--  Purpose:     Create RMS LID SIMANTIC sales fact table in the performance layer
--               with input ex RMS SPARSE table from performance layer.
--  Tables:      Input  - RTL_MC_LOC_ITEM_WK_RMS_SPARSE
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MC_940U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE RMS MC SPARSE SALES EX PERFORMANCE';
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

    select /*+ parallel(SPRS,4) */ *
       from   RTL_MC_LOC_ITEM_WK_RMS_SPARSE SPRS
       where  SPRS.last_updated_date = g_date 
            
    ) mer_rec
    ON
       (mer_rec.SK1_LOCATION_NO   = rtl_sim.SK1_LOCATION_NO
    and mer_rec.SK1_ITEM_NO       = rtl_sim.SK1_ITEM_NO
    and mer_rec.FIN_YEAR_NO       = rtl_sim.FIN_YEAR_NO
    and mer_rec.FIN_WEEK_NO       = rtl_sim.FIN_WEEK_NO)
    WHEN MATCHED
    THEN
    UPDATE
    SET           PROM_SALES_QTY	=	mer_rec.	PROM_SALES_QTY	,
                  PROM_SALES	=	mer_rec.	PROM_SALES	,
                  PROM_SALES_COST	=	mer_rec.	PROM_SALES_COST	,
                  PROM_SALES_FR_COST	=	mer_rec.	PROM_SALES_FR_COST	,
                  PROM_SALES_MARGIN	=	mer_rec.	PROM_SALES_MARGIN	,
                  FRANCHISE_PROM_SALES	=	mer_rec.	FRANCHISE_PROM_SALES	,
                  FRANCHISE_PROM_SALES_MARGIN	=	mer_rec.	FRANCHISE_PROM_SALES_MARGIN	,
                  PROM_DISCOUNT_NO	=	mer_rec.	PROM_DISCOUNT_NO	,
                  HO_PROM_DISCOUNT_AMT	=	mer_rec.	HO_PROM_DISCOUNT_AMT	,
                  HO_PROM_DISCOUNT_QTY	=	mer_rec.	HO_PROM_DISCOUNT_QTY	,
                  ST_PROM_DISCOUNT_AMT	=	mer_rec.	ST_PROM_DISCOUNT_AMT	,
                  ST_PROM_DISCOUNT_QTY	=	mer_rec.	ST_PROM_DISCOUNT_QTY	,
                  CLEAR_SALES_QTY	=	mer_rec.	CLEAR_SALES_QTY	,
                  CLEAR_SALES	=	mer_rec.	CLEAR_SALES	,
                  CLEAR_SALES_COST	=	mer_rec.	CLEAR_SALES_COST	,
                  CLEAR_SALES_FR_COST	=	mer_rec.	CLEAR_SALES_FR_COST	,
                  CLEAR_SALES_MARGIN	=	mer_rec.	CLEAR_SALES_MARGIN	,
                  FRANCHISE_CLEAR_SALES	=	mer_rec.	FRANCHISE_CLEAR_SALES	,
                  FRANCHISE_CLEAR_SALES_MARGIN	=	mer_rec.	FRANCHISE_CLEAR_SALES_MARGIN	,
                  WASTE_QTY	=	mer_rec.	WASTE_QTY	,
                  WASTE_SELLING	=	mer_rec.	WASTE_SELLING	,
                  WASTE_COST	=	mer_rec.	WASTE_COST	,
                  WASTE_FR_COST	=	mer_rec.	WASTE_FR_COST	,
                  SHRINK_QTY	=	mer_rec.	SHRINK_QTY	,
                  SHRINK_SELLING	=	mer_rec.	SHRINK_SELLING	,
                  SHRINK_COST	=	mer_rec.	SHRINK_COST	,
                  SHRINK_FR_COST	=	mer_rec.	SHRINK_FR_COST	,
                  GAIN_QTY	=	mer_rec.	GAIN_QTY	,
                  GAIN_SELLING	=	mer_rec.	GAIN_SELLING	,
                  GAIN_COST	=	mer_rec.	GAIN_COST	,
                  GAIN_FR_COST	=	mer_rec.	GAIN_FR_COST	,
                  GRN_QTY	=	mer_rec.	GRN_QTY	,
                  GRN_CASES	=	mer_rec.	GRN_CASES	,
                  GRN_SELLING	=	mer_rec.	GRN_SELLING	,
                  GRN_COST	=	mer_rec.	GRN_COST	,
                  GRN_FR_COST	=	mer_rec.	GRN_FR_COST	,
                  GRN_MARGIN	=	mer_rec.	GRN_MARGIN	,
                  SHRINKAGE_QTY	=	mer_rec.	SHRINKAGE_QTY	,
                  SHRINKAGE_SELLING	=	mer_rec.	SHRINKAGE_SELLING	,
                  SHRINKAGE_COST	=	mer_rec.	SHRINKAGE_COST	,
                  SHRINKAGE_FR_COST	=	mer_rec.	SHRINKAGE_FR_COST	,
                  ABS_SHRINKAGE_QTY	=	mer_rec.	ABS_SHRINKAGE_QTY	,
                  ABS_SHRINKAGE_SELLING	=	mer_rec.	ABS_SHRINKAGE_SELLING	,
                  ABS_SHRINKAGE_COST	=	mer_rec.	ABS_SHRINKAGE_COST	,
                  ABS_SHRINKAGE_FR_COST	=	mer_rec.	ABS_SHRINKAGE_FR_COST	,
                  CLAIM_QTY	=	mer_rec.	CLAIM_QTY	,
                  CLAIM_SELLING	=	mer_rec.	CLAIM_SELLING	,
                  CLAIM_COST	=	mer_rec.	CLAIM_COST	,
                  CLAIM_FR_COST	=	mer_rec.	CLAIM_FR_COST	,
                  SELF_SUPPLY_QTY	=	mer_rec.	SELF_SUPPLY_QTY	,
                  SELF_SUPPLY_SELLING	=	mer_rec.	SELF_SUPPLY_SELLING	,
                  SELF_SUPPLY_COST	=	mer_rec.	SELF_SUPPLY_COST	,
                  SELF_SUPPLY_FR_COST	=	mer_rec.	SELF_SUPPLY_FR_COST	,
                  WAC_ADJ_AMT	=	mer_rec.	WAC_ADJ_AMT	,
                  INVOICE_ADJ_QTY	=	mer_rec.	INVOICE_ADJ_QTY	,
                  INVOICE_ADJ_SELLING	=	mer_rec.	INVOICE_ADJ_SELLING	,
                  INVOICE_ADJ_COST	=	mer_rec.	INVOICE_ADJ_COST	,
                  RNDM_MASS_POS_VAR	=	mer_rec.	RNDM_MASS_POS_VAR	,
                  MKUP_SELLING	=	mer_rec.	MKUP_SELLING	,
                  MKUP_CANCEL_SELLING	=	mer_rec.	MKUP_CANCEL_SELLING	,
                  MKDN_SELLING	=	mer_rec.	MKDN_SELLING	,
                  MKDN_CANCEL_SELLING	=	mer_rec.	MKDN_CANCEL_SELLING	,
                  PROM_MKDN_QTY	=	mer_rec.	PROM_MKDN_QTY	,
                  PROM_MKDN_SELLING	=	mer_rec.	PROM_MKDN_SELLING	,
                  CLEAR_MKDN_SELLING	=	mer_rec.	CLEAR_MKDN_SELLING	,
                  MKDN_SALES_QTY	=	mer_rec.	MKDN_SALES_QTY	,
                  MKDN_SALES	=	mer_rec.	MKDN_SALES	,
                  MKDN_SALES_COST	=	mer_rec.	MKDN_SALES_COST	,
                  NET_MKDN	=	mer_rec.	NET_MKDN	,
                  RTV_QTY	=	mer_rec.	RTV_QTY	,
                  RTV_CASES	=	mer_rec.	RTV_CASES	,
                  RTV_SELLING	=	mer_rec.	RTV_SELLING	,
                  RTV_COST	=	mer_rec.	RTV_COST	,
                  RTV_FR_COST	=	mer_rec.	RTV_FR_COST	,
                  SDN_OUT_QTY	=	mer_rec.	SDN_OUT_QTY	,
                  SDN_OUT_SELLING	=	mer_rec.	SDN_OUT_SELLING	,
                  SDN_OUT_COST	=	mer_rec.	SDN_OUT_COST	,
                  SDN_OUT_FR_COST	=	mer_rec.	SDN_OUT_FR_COST	,
                  SDN_OUT_CASES	=	mer_rec.	SDN_OUT_CASES	,
                  IBT_IN_QTY	=	mer_rec.	IBT_IN_QTY	,
                  IBT_IN_SELLING	=	mer_rec.	IBT_IN_SELLING	,
                  IBT_IN_COST	=	mer_rec.	IBT_IN_COST	,
                  IBT_IN_FR_COST	=	mer_rec.	IBT_IN_FR_COST	,
                  IBT_OUT_QTY	=	mer_rec.	IBT_OUT_QTY	,
                  IBT_OUT_SELLING	=	mer_rec.	IBT_OUT_SELLING	,
                  IBT_OUT_COST	=	mer_rec.	IBT_OUT_COST	,
                  IBT_OUT_FR_COST	=	mer_rec.	IBT_OUT_FR_COST	,
                  NET_IBT_QTY	=	mer_rec.	NET_IBT_QTY	,
                  NET_IBT_SELLING	=	mer_rec.	NET_IBT_SELLING	,
                  SHRINK_EXCL_SOME_DEPT_COST	=	mer_rec.	SHRINK_EXCL_SOME_DEPT_COST	,
                  GAIN_EXCL_SOME_DEPT_COST	=	mer_rec.	GAIN_EXCL_SOME_DEPT_COST	,
                  NET_WASTE_QTY	=	mer_rec.	NET_WASTE_QTY	,
                  TRUNKED_QTY	=	mer_rec.	TRUNKED_QTY	,
                  TRUNKED_CASES	=	mer_rec.	TRUNKED_CASES	,
                  TRUNKED_SELLING	=	mer_rec.	TRUNKED_SELLING	,
                  TRUNKED_COST	=	mer_rec.	TRUNKED_COST	,
                  DC_DELIVERED_QTY	=	mer_rec.	DC_DELIVERED_QTY	,
                  DC_DELIVERED_CASES	=	mer_rec.	DC_DELIVERED_CASES	,
                  DC_DELIVERED_SELLING	=	mer_rec.	DC_DELIVERED_SELLING	,
                  DC_DELIVERED_COST	=	mer_rec.	DC_DELIVERED_COST	,
                  NET_INV_ADJ_QTY	=	mer_rec.	NET_INV_ADJ_QTY	,
                  NET_INV_ADJ_SELLING	=	mer_rec.	NET_INV_ADJ_SELLING	,
                  NET_INV_ADJ_COST	=	mer_rec.	NET_INV_ADJ_COST	,
                  NET_INV_ADJ_FR_COST	=	mer_rec.	NET_INV_ADJ_FR_COST	,
                  LAST_UPDATED_DATE	=	mer_rec.	LAST_UPDATED_DATE	,
                  CH_ALLOC_QTY	=	mer_rec.	CH_ALLOC_QTY	,
                  CH_ALLOC_SELLING	=	mer_rec.	CH_ALLOC_SELLING	,
--                  SHRINK_CASES	=	mer_rec.	SHRINK_CASES	,
--                  GAIN_CASES	=	mer_rec.	GAIN_CASES	,
--                  SHRINKAGE_CASES	=	mer_rec.	SHRINKAGE_CASES	,
                  ABS_SHRINKAGE_CASES	=	mer_rec.	ABS_SHRINKAGE_CASES	,
                  ABS_SHRINKAGE_SELLING_DEPT	=	mer_rec.	ABS_SHRINKAGE_SELLING_DEPT	,
                  ABS_SHRINKAGE_COST_DEPT	=	mer_rec.	ABS_SHRINKAGE_COST_DEPT	,
                  ABS_SHRINKAGE_QTY_DEPT	=	mer_rec.	ABS_SHRINKAGE_QTY_DEPT	,
                  ABS_SHRINKAGE_CASES_DEPT	=	mer_rec.	ABS_SHRINKAGE_CASES_DEPT	,
--                  WASTE_CASES	=	mer_rec.	WASTE_CASES	,
--                  CLAIM_CASES	=	mer_rec.	CLAIM_CASES	,
--                  SELF_SUPPLY_CASES	=	mer_rec.	SELF_SUPPLY_CASES	,
                  IBT_OUT_SELLING_LOCAL	=	mer_rec.	IBT_OUT_SELLING_LOCAL	,
                  IBT_OUT_COST_LOCAL	=	mer_rec.	IBT_OUT_COST_LOCAL	,
                  IBT_OUT_FR_COST_LOCAL	=	mer_rec.	IBT_OUT_FR_COST_LOCAL	,
                  NET_IBT_SELLING_LOCAL	=	mer_rec.	NET_IBT_SELLING_LOCAL	,
                  SHRINK_EXCL_SOME_DEPT_COST_LCL	=	mer_rec.	SHRINK_EXCL_SOME_DEPT_COST_LCL	,
                  GAIN_EXCL_SOME_DEPT_COST_LOCAL	=	mer_rec.	GAIN_EXCL_SOME_DEPT_COST_LOCAL	,
                  TRUNKED_SELLING_LOCAL	=	mer_rec.	TRUNKED_SELLING_LOCAL	,
                  TRUNKED_COST_LOCAL	=	mer_rec.	TRUNKED_COST_LOCAL	,
                  DC_DELIVERED_SELLING_LOCAL	=	mer_rec.	DC_DELIVERED_SELLING_LOCAL	,
                  DC_DELIVERED_COST_LOCAL	=	mer_rec.	DC_DELIVERED_COST_LOCAL	,
                  NET_INV_ADJ_SELLING_LOCAL	=	mer_rec.	NET_INV_ADJ_SELLING_LOCAL	,
                  NET_INV_ADJ_COST_LOCAL	=	mer_rec.	NET_INV_ADJ_COST_LOCAL	,
                  NET_INV_ADJ_FR_COST_LOCAL	=	mer_rec.	NET_INV_ADJ_FR_COST_LOCAL	,
                  CH_ALLOC_SELLING_LOCAL	=	mer_rec.	CH_ALLOC_SELLING_LOCAL	,
                  ABS_SHRINKAGE_SELLING_DEPT_LCL	=	mer_rec.	ABS_SHRINKAGE_SELLING_DEPT_LCL	,
                  ABS_SHRINKAGE_COST_DEPT_LOCAL	=	mer_rec.	ABS_SHRINKAGE_COST_DEPT_LOCAL	,
                  PROM_SALES_LOCAL	=	mer_rec.	PROM_SALES_LOCAL	,
                  PROM_SALES_COST_LOCAL	=	mer_rec.	PROM_SALES_COST_LOCAL	,
                  PROM_SALES_FR_COST_LOCAL	=	mer_rec.	PROM_SALES_FR_COST_LOCAL	,
                  PROM_SALES_MARGIN_LOCAL	=	mer_rec.	PROM_SALES_MARGIN_LOCAL	,
                  FRANCHISE_PROM_SALES_LOCAL	=	mer_rec.	FRANCHISE_PROM_SALES_LOCAL	,
                  FRNCH_PROM_SALES_MARGIN_LOCAL	=	mer_rec.	FRNCH_PROM_SALES_MARGIN_LOCAL	,
                  PROM_DISCOUNT_NO_LOCAL	=	mer_rec.	PROM_DISCOUNT_NO_LOCAL	,
                  HO_PROM_DISCOUNT_AMT_LOCAL	=	mer_rec.	HO_PROM_DISCOUNT_AMT_LOCAL	,
                  ST_PROM_DISCOUNT_AMT_LOCAL	=	mer_rec.	ST_PROM_DISCOUNT_AMT_LOCAL	,
                  CLEAR_SALES_LOCAL	=	mer_rec.	CLEAR_SALES_LOCAL	,
                  CLEAR_SALES_COST_LOCAL	=	mer_rec.	CLEAR_SALES_COST_LOCAL	,
                  CLEAR_SALES_FR_COST_LOCAL	=	mer_rec.	CLEAR_SALES_FR_COST_LOCAL	,
                  CLEAR_SALES_MARGIN_LOCAL	=	mer_rec.	CLEAR_SALES_MARGIN_LOCAL	,
                  FRANCHISE_CLEAR_SALES_LOCAL	=	mer_rec.	FRANCHISE_CLEAR_SALES_LOCAL	,
                  FRNCH_CLEAR_SALES_MARGIN_LOCAL	=	mer_rec.	FRNCH_CLEAR_SALES_MARGIN_LOCAL	,
                  WASTE_SELLING_LOCAL	=	mer_rec.	WASTE_SELLING_LOCAL	,
                  WASTE_COST_LOCAL	=	mer_rec.	WASTE_COST_LOCAL	,
                  WASTE_FR_COST_LOCAL	=	mer_rec.	WASTE_FR_COST_LOCAL	,
                  SHRINK_SELLING_LOCAL	=	mer_rec.	SHRINK_SELLING_LOCAL	,
                  SHRINK_COST_LOCAL	=	mer_rec.	SHRINK_COST_LOCAL	,
                  SHRINK_FR_COST_LOCAL	=	mer_rec.	SHRINK_FR_COST_LOCAL	,
                  GAIN_SELLING_LOCAL	=	mer_rec.	GAIN_SELLING_LOCAL	,
                  GAIN_COST_LOCAL	=	mer_rec.	GAIN_COST_LOCAL	,
                  GAIN_FR_COST_LOCAL	=	mer_rec.	GAIN_FR_COST_LOCAL	,
                  GRN_SELLING_LOCAL	=	mer_rec.	GRN_SELLING_LOCAL	,
                  GRN_COST_LOCAL	=	mer_rec.	GRN_COST_LOCAL	,
                  GRN_FR_COST_LOCAL	=	mer_rec.	GRN_FR_COST_LOCAL	,
                  GRN_MARGIN_LOCAL	=	mer_rec.	GRN_MARGIN_LOCAL	,
                  SHRINKAGE_SELLING_LOCAL	=	mer_rec.	SHRINKAGE_SELLING_LOCAL	,
                  SHRINKAGE_COST_LOCAL	=	mer_rec.	SHRINKAGE_COST_LOCAL	,
                  SHRINKAGE_FR_COST_LOCAL	=	mer_rec.	SHRINKAGE_FR_COST_LOCAL	,
                  ABS_SHRINKAGE_SELLING_LOCAL	=	mer_rec.	ABS_SHRINKAGE_SELLING_LOCAL	,
                  ABS_SHRINKAGE_COST_LOCAL	=	mer_rec.	ABS_SHRINKAGE_COST_LOCAL	,
                  ABS_SHRINKAGE_FR_COST_LOCAL	=	mer_rec.	ABS_SHRINKAGE_FR_COST_LOCAL	,
                  CLAIM_SELLING_LOCAL	=	mer_rec.	CLAIM_SELLING_LOCAL	,
                  CLAIM_COST_LOCAL	=	mer_rec.	CLAIM_COST_LOCAL	,
                  CLAIM_FR_COST_LOCAL	=	mer_rec.	CLAIM_FR_COST_LOCAL	,
                  SELF_SUPPLY_SELLING_LOCAL	=	mer_rec.	SELF_SUPPLY_SELLING_LOCAL	,
                  SELF_SUPPLY_COST_LOCAL	=	mer_rec.	SELF_SUPPLY_COST_LOCAL	,
                  SELF_SUPPLY_FR_COST_LOCAL	=	mer_rec.	SELF_SUPPLY_FR_COST_LOCAL	,
                  WAC_ADJ_AMT_LOCAL	=	mer_rec.	WAC_ADJ_AMT_LOCAL	,
                  INVOICE_ADJ_SELLING_LOCAL	=	mer_rec.	INVOICE_ADJ_SELLING_LOCAL	,
                  INVOICE_ADJ_COST_LOCAL	=	mer_rec.	INVOICE_ADJ_COST_LOCAL	,
                  MKUP_SELLING_LOCAL	=	mer_rec.	MKUP_SELLING_LOCAL	,
                  MKUP_CANCEL_SELLING_LOCAL	=	mer_rec.	MKUP_CANCEL_SELLING_LOCAL	,
                  MKDN_SELLING_LOCAL	=	mer_rec.	MKDN_SELLING_LOCAL	,
                  MKDN_CANCEL_SELLING_LOCAL	=	mer_rec.	MKDN_CANCEL_SELLING_LOCAL	,
                  PROM_MKDN_SELLING_LOCAL	=	mer_rec.	PROM_MKDN_SELLING_LOCAL	,
                  CLEAR_MKDN_SELLING_LOCAL	=	mer_rec.	CLEAR_MKDN_SELLING_LOCAL	,
                  MKDN_SALES_LOCAL	=	mer_rec.	MKDN_SALES_LOCAL	,
                  MKDN_SALES_COST_LOCAL	=	mer_rec.	MKDN_SALES_COST_LOCAL	,
                  NET_MKDN_LOCAL	=	mer_rec.	NET_MKDN_LOCAL	,
                  RTV_SELLING_LOCAL	=	mer_rec.	RTV_SELLING_LOCAL	,
                  RTV_COST_LOCAL	=	mer_rec.	RTV_COST_LOCAL	,
                  RTV_FR_COST_LOCAL	=	mer_rec.	RTV_FR_COST_LOCAL	,
                  SDN_OUT_SELLING_LOCAL	=	mer_rec.	SDN_OUT_SELLING_LOCAL	,
                  SDN_OUT_COST_LOCAL	=	mer_rec.	SDN_OUT_COST_LOCAL	,
                  SDN_OUT_FR_COST_LOCAL	=	mer_rec.	SDN_OUT_FR_COST_LOCAL	,
                  IBT_IN_SELLING_LOCAL	=	mer_rec.	IBT_IN_SELLING_LOCAL	,
                  IBT_IN_COST_LOCAL	=	mer_rec.	IBT_IN_COST_LOCAL	,
                  IBT_IN_FR_COST_LOCAL	=	mer_rec.	IBT_IN_FR_COST_LOCAL	

    
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
                  PROM_SALES_QTY,
                  PROM_SALES,
                  PROM_SALES_COST,
                  PROM_SALES_FR_COST,
                  PROM_SALES_MARGIN,
                  FRANCHISE_PROM_SALES,
                  FRANCHISE_PROM_SALES_MARGIN,
                  PROM_DISCOUNT_NO,
                  HO_PROM_DISCOUNT_AMT,
                  HO_PROM_DISCOUNT_QTY,
                  ST_PROM_DISCOUNT_AMT,
                  ST_PROM_DISCOUNT_QTY,
                  CLEAR_SALES_QTY,
                  CLEAR_SALES,
                  CLEAR_SALES_COST,
                  CLEAR_SALES_FR_COST,
                  CLEAR_SALES_MARGIN,
                  FRANCHISE_CLEAR_SALES,
                  FRANCHISE_CLEAR_SALES_MARGIN,
                  WASTE_QTY,
                  WASTE_SELLING,
                  WASTE_COST,
                  WASTE_FR_COST,
                  SHRINK_QTY,
                  SHRINK_SELLING,
                  SHRINK_COST,
                  SHRINK_FR_COST,
                  GAIN_QTY,
                  GAIN_SELLING,
                  GAIN_COST,
                  GAIN_FR_COST,
                  GRN_QTY,
                  GRN_CASES,
                  GRN_SELLING,
                  GRN_COST,
                  GRN_FR_COST,
                  GRN_MARGIN,
                  SHRINKAGE_QTY,
                  SHRINKAGE_SELLING,
                  SHRINKAGE_COST,
                  SHRINKAGE_FR_COST,
                  ABS_SHRINKAGE_QTY,
                  ABS_SHRINKAGE_SELLING,
                  ABS_SHRINKAGE_COST,
                  ABS_SHRINKAGE_FR_COST,
                  CLAIM_QTY,
                  CLAIM_SELLING,
                  CLAIM_COST,
                  CLAIM_FR_COST,
                  SELF_SUPPLY_QTY,
                  SELF_SUPPLY_SELLING,
                  SELF_SUPPLY_COST,
                  SELF_SUPPLY_FR_COST,
                  WAC_ADJ_AMT,
                  INVOICE_ADJ_QTY,
                  INVOICE_ADJ_SELLING,
                  INVOICE_ADJ_COST,
                  RNDM_MASS_POS_VAR,
                  MKUP_SELLING,
                  MKUP_CANCEL_SELLING,
                  MKDN_SELLING,
                  MKDN_CANCEL_SELLING,
                  PROM_MKDN_QTY,
                  PROM_MKDN_SELLING,
                  CLEAR_MKDN_SELLING,
                  MKDN_SALES_QTY,
                  MKDN_SALES,
                  MKDN_SALES_COST,
                  NET_MKDN,
                  RTV_QTY,
                  RTV_CASES,
                  RTV_SELLING,
                  RTV_COST,
                  RTV_FR_COST,
                  SDN_OUT_QTY,
                  SDN_OUT_SELLING,
                  SDN_OUT_COST,
                  SDN_OUT_FR_COST,
                  SDN_OUT_CASES,
                  IBT_IN_QTY,
                  IBT_IN_SELLING,
                  IBT_IN_COST,
                  IBT_IN_FR_COST,
                  IBT_OUT_QTY,
                  IBT_OUT_SELLING,
                  IBT_OUT_COST,
                  IBT_OUT_FR_COST,
                  NET_IBT_QTY,
                  NET_IBT_SELLING,
                  SHRINK_EXCL_SOME_DEPT_COST,
                  GAIN_EXCL_SOME_DEPT_COST,
                  NET_WASTE_QTY,
                  TRUNKED_QTY,
                  TRUNKED_CASES,
                  TRUNKED_SELLING,
                  TRUNKED_COST,
                  DC_DELIVERED_QTY,
                  DC_DELIVERED_CASES,
                  DC_DELIVERED_SELLING,
                  DC_DELIVERED_COST,
                  NET_INV_ADJ_QTY,
                  NET_INV_ADJ_SELLING,
                  NET_INV_ADJ_COST,
                  NET_INV_ADJ_FR_COST,
                  LAST_UPDATED_DATE,
                  CH_ALLOC_QTY,
                  CH_ALLOC_SELLING,
--                  SHRINK_CASES,
--                  GAIN_CASES,
--                  SHRINKAGE_CASES,
                  ABS_SHRINKAGE_CASES,
                  ABS_SHRINKAGE_SELLING_DEPT,
                  ABS_SHRINKAGE_COST_DEPT,
                  ABS_SHRINKAGE_QTY_DEPT,
                  ABS_SHRINKAGE_CASES_DEPT,
--                  WASTE_CASES,
--                  CLAIM_CASES,
--                  SELF_SUPPLY_CASES,
                  IBT_OUT_SELLING_LOCAL,
                  IBT_OUT_COST_LOCAL,
                  IBT_OUT_FR_COST_LOCAL,
                  NET_IBT_SELLING_LOCAL,
                  SHRINK_EXCL_SOME_DEPT_COST_LCL,
                  GAIN_EXCL_SOME_DEPT_COST_LOCAL,
                  TRUNKED_SELLING_LOCAL,
                  TRUNKED_COST_LOCAL,
                  DC_DELIVERED_SELLING_LOCAL,
                  DC_DELIVERED_COST_LOCAL,
                  NET_INV_ADJ_SELLING_LOCAL,
                  NET_INV_ADJ_COST_LOCAL,
                  NET_INV_ADJ_FR_COST_LOCAL,
                  CH_ALLOC_SELLING_LOCAL,
                  ABS_SHRINKAGE_SELLING_DEPT_LCL,
                  ABS_SHRINKAGE_COST_DEPT_LOCAL,
                  PROM_SALES_LOCAL,
                  PROM_SALES_COST_LOCAL,
                  PROM_SALES_FR_COST_LOCAL,
                  PROM_SALES_MARGIN_LOCAL,
                  FRANCHISE_PROM_SALES_LOCAL,
                  FRNCH_PROM_SALES_MARGIN_LOCAL,
                  PROM_DISCOUNT_NO_LOCAL,
                  HO_PROM_DISCOUNT_AMT_LOCAL,
                  ST_PROM_DISCOUNT_AMT_LOCAL,
                  CLEAR_SALES_LOCAL,
                  CLEAR_SALES_COST_LOCAL,
                  CLEAR_SALES_FR_COST_LOCAL,
                  CLEAR_SALES_MARGIN_LOCAL,
                  FRANCHISE_CLEAR_SALES_LOCAL,
                  FRNCH_CLEAR_SALES_MARGIN_LOCAL,
                  WASTE_SELLING_LOCAL,
                  WASTE_COST_LOCAL,
                  WASTE_FR_COST_LOCAL,
                  SHRINK_SELLING_LOCAL,
                  SHRINK_COST_LOCAL,
                  SHRINK_FR_COST_LOCAL,
                  GAIN_SELLING_LOCAL,
                  GAIN_COST_LOCAL,
                  GAIN_FR_COST_LOCAL,
                  GRN_SELLING_LOCAL,
                  GRN_COST_LOCAL,
                  GRN_FR_COST_LOCAL,
                  GRN_MARGIN_LOCAL,
                  SHRINKAGE_SELLING_LOCAL,
                  SHRINKAGE_COST_LOCAL,
                  SHRINKAGE_FR_COST_LOCAL,
                  ABS_SHRINKAGE_SELLING_LOCAL,
                  ABS_SHRINKAGE_COST_LOCAL,
                  ABS_SHRINKAGE_FR_COST_LOCAL,
                  CLAIM_SELLING_LOCAL,
                  CLAIM_COST_LOCAL,
                  CLAIM_FR_COST_LOCAL,
                  SELF_SUPPLY_SELLING_LOCAL,
                  SELF_SUPPLY_COST_LOCAL,
                  SELF_SUPPLY_FR_COST_LOCAL,
                  WAC_ADJ_AMT_LOCAL,
                  INVOICE_ADJ_SELLING_LOCAL,
                  INVOICE_ADJ_COST_LOCAL,
                  MKUP_SELLING_LOCAL,
                  MKUP_CANCEL_SELLING_LOCAL,
                  MKDN_SELLING_LOCAL,
                  MKDN_CANCEL_SELLING_LOCAL,
                  PROM_MKDN_SELLING_LOCAL,
                  CLEAR_MKDN_SELLING_LOCAL,
                  MKDN_SALES_LOCAL,
                  MKDN_SALES_COST_LOCAL,
                  NET_MKDN_LOCAL,
                  RTV_SELLING_LOCAL,
                  RTV_COST_LOCAL,
                  RTV_FR_COST_LOCAL,
                  SDN_OUT_SELLING_LOCAL,
                  SDN_OUT_COST_LOCAL,
                  SDN_OUT_FR_COST_LOCAL,
                  IBT_IN_SELLING_LOCAL,
                  IBT_IN_COST_LOCAL,
                  IBT_IN_FR_COST_LOCAL
                  

                  
                  
    )
    VALUES
    (
                  mer_rec.  SK1_LOCATION_NO,
                  mer_rec.  SK1_ITEM_NO,
                  mer_rec.  FIN_YEAR_NO,
                  mer_rec.  FIN_WEEK_NO,
                  mer_rec.  FIN_WEEK_CODE,
                  mer_rec.  THIS_WEEK_START_DATE,
                  mer_rec.  SK2_LOCATION_NO,
                  mer_rec.  SK2_ITEM_NO,
                  mer_rec.	PROM_SALES_QTY	,
                  mer_rec.	PROM_SALES	,
                  mer_rec.	PROM_SALES_COST	,
                  mer_rec.	PROM_SALES_FR_COST	,
                  mer_rec.	PROM_SALES_MARGIN	,
                  mer_rec.	FRANCHISE_PROM_SALES	,
                  mer_rec.	FRANCHISE_PROM_SALES_MARGIN	,
                  mer_rec.	PROM_DISCOUNT_NO	,
                  mer_rec.	HO_PROM_DISCOUNT_AMT	,
                  mer_rec.	HO_PROM_DISCOUNT_QTY	,
                  mer_rec.	ST_PROM_DISCOUNT_AMT	,
                  mer_rec.	ST_PROM_DISCOUNT_QTY	,
                  mer_rec.	CLEAR_SALES_QTY	,
                  mer_rec.	CLEAR_SALES	,
                  mer_rec.	CLEAR_SALES_COST	,
                  mer_rec.	CLEAR_SALES_FR_COST	,
                  mer_rec.	CLEAR_SALES_MARGIN	,
                  mer_rec.	FRANCHISE_CLEAR_SALES	,
                  mer_rec.	FRANCHISE_CLEAR_SALES_MARGIN	,
                  mer_rec.	WASTE_QTY	,
                  mer_rec.	WASTE_SELLING	,
                  mer_rec.	WASTE_COST	,
                  mer_rec.	WASTE_FR_COST	,
                  mer_rec.	SHRINK_QTY	,
                  mer_rec.	SHRINK_SELLING	,
                  mer_rec.	SHRINK_COST	,
                  mer_rec.	SHRINK_FR_COST	,
                  mer_rec.	GAIN_QTY	,
                  mer_rec.	GAIN_SELLING	,
                  mer_rec.	GAIN_COST	,
                  mer_rec.	GAIN_FR_COST	,
                  mer_rec.	GRN_QTY	,
                  mer_rec.	GRN_CASES	,
                  mer_rec.	GRN_SELLING	,
                  mer_rec.	GRN_COST	,
                  mer_rec.	GRN_FR_COST	,
                  mer_rec.	GRN_MARGIN	,
                  mer_rec.	SHRINKAGE_QTY	,
                  mer_rec.	SHRINKAGE_SELLING	,
                  mer_rec.	SHRINKAGE_COST	,
                  mer_rec.	SHRINKAGE_FR_COST	,
                  mer_rec.	ABS_SHRINKAGE_QTY	,
                  mer_rec.	ABS_SHRINKAGE_SELLING	,
                  mer_rec.	ABS_SHRINKAGE_COST	,
                  mer_rec.	ABS_SHRINKAGE_FR_COST	,
                  mer_rec.	CLAIM_QTY	,
                  mer_rec.	CLAIM_SELLING	,
                  mer_rec.	CLAIM_COST	,
                  mer_rec.	CLAIM_FR_COST	,
                  mer_rec.	SELF_SUPPLY_QTY	,
                  mer_rec.	SELF_SUPPLY_SELLING	,
                  mer_rec.	SELF_SUPPLY_COST	,
                  mer_rec.	SELF_SUPPLY_FR_COST	,
                  mer_rec.	WAC_ADJ_AMT	,
                  mer_rec.	INVOICE_ADJ_QTY	,
                  mer_rec.	INVOICE_ADJ_SELLING	,
                  mer_rec.	INVOICE_ADJ_COST	,
                  mer_rec.	RNDM_MASS_POS_VAR	,
                  mer_rec.	MKUP_SELLING	,
                  mer_rec.	MKUP_CANCEL_SELLING	,
                  mer_rec.	MKDN_SELLING	,
                  mer_rec.	MKDN_CANCEL_SELLING	,
                  mer_rec.	PROM_MKDN_QTY	,
                  mer_rec.	PROM_MKDN_SELLING	,
                  mer_rec.	CLEAR_MKDN_SELLING	,
                  mer_rec.	MKDN_SALES_QTY	,
                  mer_rec.	MKDN_SALES	,
                  mer_rec.	MKDN_SALES_COST	,
                  mer_rec.	NET_MKDN	,
                  mer_rec.	RTV_QTY	,
                  mer_rec.	RTV_CASES	,
                  mer_rec.	RTV_SELLING	,
                  mer_rec.	RTV_COST	,
                  mer_rec.	RTV_FR_COST	,
                  mer_rec.	SDN_OUT_QTY	,
                  mer_rec.	SDN_OUT_SELLING	,
                  mer_rec.	SDN_OUT_COST	,
                  mer_rec.	SDN_OUT_FR_COST	,
                  mer_rec.	SDN_OUT_CASES	,
                  mer_rec.	IBT_IN_QTY	,
                  mer_rec.	IBT_IN_SELLING	,
                  mer_rec.	IBT_IN_COST	,
                  mer_rec.	IBT_IN_FR_COST	,
                  mer_rec.	IBT_OUT_QTY	,
                  mer_rec.	IBT_OUT_SELLING	,
                  mer_rec.	IBT_OUT_COST	,
                  mer_rec.	IBT_OUT_FR_COST	,
                  mer_rec.	NET_IBT_QTY	,
                  mer_rec.	NET_IBT_SELLING	,
                  mer_rec.	SHRINK_EXCL_SOME_DEPT_COST	,
                  mer_rec.	GAIN_EXCL_SOME_DEPT_COST	,
                  mer_rec.	NET_WASTE_QTY	,
                  mer_rec.	TRUNKED_QTY	,
                  mer_rec.	TRUNKED_CASES	,
                  mer_rec.	TRUNKED_SELLING	,
                  mer_rec.	TRUNKED_COST	,
                  mer_rec.	DC_DELIVERED_QTY	,
                  mer_rec.	DC_DELIVERED_CASES	,
                  mer_rec.	DC_DELIVERED_SELLING	,
                  mer_rec.	DC_DELIVERED_COST	,
                  mer_rec.	NET_INV_ADJ_QTY	,
                  mer_rec.	NET_INV_ADJ_SELLING	,
                  mer_rec.	NET_INV_ADJ_COST	,
                  mer_rec.	NET_INV_ADJ_FR_COST	,
                  mer_rec.	LAST_UPDATED_DATE	,
                  mer_rec.	CH_ALLOC_QTY	,
                  mer_rec.	CH_ALLOC_SELLING	,
--                  mer_rec.	SHRINK_CASES	,
--                  mer_rec.	GAIN_CASES	,
--                  mer_rec.	SHRINKAGE_CASES	,
                  mer_rec.	ABS_SHRINKAGE_CASES	,
                  mer_rec.	ABS_SHRINKAGE_SELLING_DEPT	,
                  mer_rec.	ABS_SHRINKAGE_COST_DEPT	,
                  mer_rec.	ABS_SHRINKAGE_QTY_DEPT	,
                  mer_rec.	ABS_SHRINKAGE_CASES_DEPT	,
--                  mer_rec.	WASTE_CASES	,
--                  mer_rec.	CLAIM_CASES	,
--                  mer_rec.	SELF_SUPPLY_CASES,
                  mer_rec.	IBT_OUT_SELLING_LOCAL	,
                  mer_rec.	IBT_OUT_COST_LOCAL	,
                  mer_rec.	IBT_OUT_FR_COST_LOCAL	,
                  mer_rec.	NET_IBT_SELLING_LOCAL	,
                  mer_rec.	SHRINK_EXCL_SOME_DEPT_COST_LCL	,
                  mer_rec.	GAIN_EXCL_SOME_DEPT_COST_LOCAL	,
                  mer_rec.	TRUNKED_SELLING_LOCAL	,
                  mer_rec.	TRUNKED_COST_LOCAL	,
                  mer_rec.	DC_DELIVERED_SELLING_LOCAL	,
                  mer_rec.	DC_DELIVERED_COST_LOCAL	,
                  mer_rec.	NET_INV_ADJ_SELLING_LOCAL	,
                  mer_rec.	NET_INV_ADJ_COST_LOCAL	,
                  mer_rec.	NET_INV_ADJ_FR_COST_LOCAL	,
                  mer_rec.	CH_ALLOC_SELLING_LOCAL	,
                  mer_rec.	ABS_SHRINKAGE_SELLING_DEPT_LCL	,
                  mer_rec.	ABS_SHRINKAGE_COST_DEPT_LOCAL	,
                  mer_rec.	PROM_SALES_LOCAL	,
                  mer_rec.	PROM_SALES_COST_LOCAL	,
                  mer_rec.	PROM_SALES_FR_COST_LOCAL	,
                  mer_rec.	PROM_SALES_MARGIN_LOCAL	,
                  mer_rec.	FRANCHISE_PROM_SALES_LOCAL	,
                  mer_rec.	FRNCH_PROM_SALES_MARGIN_LOCAL	,
                  mer_rec.	PROM_DISCOUNT_NO_LOCAL	,
                  mer_rec.	HO_PROM_DISCOUNT_AMT_LOCAL	,
                  mer_rec.	ST_PROM_DISCOUNT_AMT_LOCAL	,
                  mer_rec.	CLEAR_SALES_LOCAL	,
                  mer_rec.	CLEAR_SALES_COST_LOCAL	,
                  mer_rec.	CLEAR_SALES_FR_COST_LOCAL	,
                  mer_rec.	CLEAR_SALES_MARGIN_LOCAL	,
                  mer_rec.	FRANCHISE_CLEAR_SALES_LOCAL	,
                  mer_rec.	FRNCH_CLEAR_SALES_MARGIN_LOCAL	,
                  mer_rec.	WASTE_SELLING_LOCAL	,
                  mer_rec.	WASTE_COST_LOCAL	,
                  mer_rec.	WASTE_FR_COST_LOCAL	,
                  mer_rec.	SHRINK_SELLING_LOCAL	,
                  mer_rec.	SHRINK_COST_LOCAL	,
                  mer_rec.	SHRINK_FR_COST_LOCAL	,
                  mer_rec.	GAIN_SELLING_LOCAL	,
                  mer_rec.	GAIN_COST_LOCAL	,
                  mer_rec.	GAIN_FR_COST_LOCAL	,
                  mer_rec.	GRN_SELLING_LOCAL	,
                  mer_rec.	GRN_COST_LOCAL	,
                  mer_rec.	GRN_FR_COST_LOCAL	,
                  mer_rec.	GRN_MARGIN_LOCAL	,
                  mer_rec.	SHRINKAGE_SELLING_LOCAL	,
                  mer_rec.	SHRINKAGE_COST_LOCAL	,
                  mer_rec.	SHRINKAGE_FR_COST_LOCAL	,
                  mer_rec.	ABS_SHRINKAGE_SELLING_LOCAL	,
                  mer_rec.	ABS_SHRINKAGE_COST_LOCAL	,
                  mer_rec.	ABS_SHRINKAGE_FR_COST_LOCAL	,
                  mer_rec.	CLAIM_SELLING_LOCAL	,
                  mer_rec.	CLAIM_COST_LOCAL	,
                  mer_rec.	CLAIM_FR_COST_LOCAL	,
                  mer_rec.	SELF_SUPPLY_SELLING_LOCAL	,
                  mer_rec.	SELF_SUPPLY_COST_LOCAL	,
                  mer_rec.	SELF_SUPPLY_FR_COST_LOCAL	,
                  mer_rec.	WAC_ADJ_AMT_LOCAL	,
                  mer_rec.	INVOICE_ADJ_SELLING_LOCAL	,
                  mer_rec.	INVOICE_ADJ_COST_LOCAL	,
                  mer_rec.	MKUP_SELLING_LOCAL	,
                  mer_rec.	MKUP_CANCEL_SELLING_LOCAL	,
                  mer_rec.	MKDN_SELLING_LOCAL	,
                  mer_rec.	MKDN_CANCEL_SELLING_LOCAL	,
                  mer_rec.	PROM_MKDN_SELLING_LOCAL	,
                  mer_rec.	CLEAR_MKDN_SELLING_LOCAL	,
                  mer_rec.	MKDN_SALES_LOCAL	,
                  mer_rec.	MKDN_SALES_COST_LOCAL	,
                  mer_rec.	NET_MKDN_LOCAL	,
                  mer_rec.	RTV_SELLING_LOCAL	,
                  mer_rec.	RTV_COST_LOCAL	,
                  mer_rec.	RTV_FR_COST_LOCAL	,
                  mer_rec.	SDN_OUT_SELLING_LOCAL	,
                  mer_rec.	SDN_OUT_COST_LOCAL	,
                  mer_rec.	SDN_OUT_FR_COST_LOCAL	,
                  mer_rec.	IBT_IN_SELLING_LOCAL	,
                  mer_rec.	IBT_IN_COST_LOCAL	,
                  mer_rec.	IBT_IN_FR_COST_LOCAL	


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
    from RTL_MC_LOC_ITEM_WK_RMS_SPARSE  
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

end WH_PRF_MC_940U;
