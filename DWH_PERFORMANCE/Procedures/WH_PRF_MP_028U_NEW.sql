--------------------------------------------------------
--  DDL for Procedure WH_PRF_MP_028U_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_MP_028U_NEW" 

                                                                                (p_forall_limit in integer,p_success out boolean) as

-- *************************************************************************************************
-- * Notes from 12.2 upgrade performance tuning
-- *************************************************************************************************
-- Date:   2019-01-25
-- Author: Paul Wakefield
-- 1. Removed full hints on merge in local_bulk_insert_stock, local_bulk_insert_sparse and local_bulk_insert_dense
-- 
-- **************************************************************************************************

--**************************************************************************************************
--  Date:        April 2015
--  Author:      K Lehabe
--  Purpose:     Load RMS actual measures into the location and space mart in the  performance level.
--  Tables:      Input  - rtl_loc_sc_wk_rms_sparse ,  rtl_loc_sc_wk_rms_dense, rtl_loc_sc_wk_rms_stock
--               Output - mart_rtl_loc_subc_pln_wk_rms
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  Date:         January 2016
--  Changed by:   K. Lehabe
--  Changes:      Add these columns to mart_rtl_loc_subc_pln_wk_rms
--                    - finance_loc_ia_cost, finance_loc_ia_qty, finance_loc_ia_selling from RTL_LOC_INV_ADJ_SUMMARY
--                    - wac_adj_amt from RTL_LOC_SC_WK_RMS_SPARSE
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit         integer       :=  dwh_constants.vc_forall_limit;
g_recs_read            integer       :=  0;
g_recs_inserted        integer       :=  0;
g_recs_updated         integer       :=  0;
g_date                 date;
g_this_week_start_date date;
g_this_week_end_date   date;
g_fin_week_no          integer;
g_fin_year_no          integer;
g_start_fin_week_no    integer;
g_start_fin_year_no    integer;
g_end_fin_week_no      integer;
g_end_fin_year_no      integer;
g_today_fin_day_no     integer;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_MP_028U_NEW';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'DATA TAKE ON MERGE!!! LOAD THE RMS ACTUALS FOR SPACE AND LOCATION PLANNING DATAMART';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;





 --**************************************************************************************************
--Update table with  Data from RMS Stock data
--**************************************************************************************************
procedure local_bulk_insert_stock as
begin

  L_TEXT := 'Updating RMS Stock data ....';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   merge /*+ parallel(mart,4) */   into  dwh_performance.mart_rtl_loc_subc_pln_wk_rms mart
    
   using (  select /*+ parallel(4) */
                  stck.sk1_location_no,
                  lev1.sk1_subclass_no,
           stck.fin_year_no,
          stck.fin_week_no,
          loc.customer_classification as customer_class_code,
          loc.store_size_cluster,
          sum(soh_selling) soh_selling,
          sum(soh_cost) soh_cost,
          sum(soh_qty) soh_qty,
          sum(soh_margin) soh_margin,
          sum(clear_soh_selling) clear_soh_selling,
          sum(clear_soh_cost) clear_soh_cost,
          sum(clear_soh_margin) clear_soh_margin,
          sum(clear_soh_qty) clear_soh_qty,
          sum(reg_soh_selling) reg_soh_selling,
          sum(reg_soh_cost) reg_soh_cost,
          sum(reg_soh_margin) reg_soh_margin,
          sum(reg_soh_qty) reg_soh_qty,
          sum(avail_reg_stock_selling) avail_reg_stock_selling,
          sum(avail_reg_stock_qty) avail_reg_stock_qty,
          sum(sit_selling) sit_selling,
          sum(sit_cost) sit_cost,
          sum(sit_margin) sit_margin,
          sum(sit_qty) sit_qty,
          sum(inbound_incl_cust_ord_selling) inbound_incl_cust_ord_selling,
          sum(inbound_incl_cust_ord_cost) inbound_incl_cust_ord_cost,
          sum(inbound_incl_cust_ord_qty) inbound_incl_cust_ord_qty,
         'W' || stck.fin_year_no || stck.fin_week_no as fin_week_code
   from  dwh_performance.rtl_loc_sc_wk_rms_stock stck,
         DIM_LEV1_DIFF1 LEV1,
         dim_location loc
   where lev1.sk1_style_colour_no = stck.sk1_style_colour_no
   and loc.sk1_location_no = stck.sk1_location_no
   and business_unit_no in (51, 52, 53, 54, 55)
   and stck.this_week_start_date between g_this_week_start_date and g_this_week_end_date
--     and fin_year_no = g_fin_year_no
--     and fin_week_no = g_fin_week_no
   group by  stck.sk1_location_no,  sk1_subclass_no,  stck.fin_year_no, stck.fin_week_no,loc.customer_classification,  loc.store_size_cluster, fin_week_code) stock
    ON (stock.sk1_location_no   = mart.sk1_location_no and
         stock.sk1_subclass_no   = mart.sk1_subclass_no and
         stock.fin_year_no       = mart.fin_year_no and
         stock.fin_week_no       = mart.fin_week_no)

   when matched then update
    set mart.soh_selling	          =	stock.soh_selling,
       mart.soh_cost	              = stock.soh_cost,
       mart.soh_qty	                = stock.soh_qty,
       mart.soh_margin	            = stock.soh_margin,
       mart.clear_soh_selling	      =	stock.clear_soh_selling,
       mart.clear_soh_cost	        =	stock.clear_soh_cost,
       mart.clear_soh_margin	      = stock.clear_soh_margin,
       mart.clear_soh_qty	          =	stock.clear_soh_qty,
       mart.reg_soh_selling 	      =	stock.reg_soh_selling,
       mart.reg_soh_cost	          = stock.reg_soh_cost,
       mart.reg_soh_margin	        =	stock.reg_soh_margin,
       mart.reg_soh_qty	            =	stock.reg_soh_qty,
       mart.avail_reg_stock_selling	= stock.avail_reg_stock_selling,
       mart.avail_reg_stock_qty	    =	stock.avail_reg_stock_qty,
       mart.sit_selling	            =	stock.sit_selling,
       mart.sit_cost	              =	stock.sit_cost,
       mart.sit_margin	            =	stock.sit_margin,
       mart.sit_qty       	        =	stock.sit_qty,
       mart.inbound_incl_cust_ord_selling	=	stock.inbound_incl_cust_ord_selling,
       mart.inbound_incl_cust_ord_cost	= stock.inbound_incl_cust_ord_cost,
       mart.inbound_incl_cust_ord_qty 	= stock.inbound_incl_cust_ord_qty,
       mart.last_updated_date	          = g_date,
       mart.fin_week_code         = stock.fin_week_code


   WHEN NOT MATCHED THEN INSERT
    (
       sk1_location_no,
       sk1_subclass_no,
       sk1_plan_type_no,
       fin_year_no,
       fin_week_no,
       customer_class_code,
       store_size_cluster,
       soh_selling,
       soh_cost,
       soh_qty,
       soh_margin,
       clear_soh_selling,
       clear_soh_cost,
       clear_soh_margin,
       clear_soh_qty,
       reg_soh_selling,
       reg_soh_cost,
       reg_soh_margin,
       reg_soh_qty,
       avail_reg_stock_selling,
       avail_reg_stock_qty,
       sit_selling,
       sit_cost,
       sit_margin,
       sit_qty,
       inbound_incl_cust_ord_selling,
       inbound_incl_cust_ord_cost,
       inbound_incl_cust_ord_qty	,
       last_updated_date	,
       fin_week_code
   )
   VALUES
   (
      stock.sk1_location_no,
      stock.sk1_subclass_no,
       1,
       stock.fin_year_no,
       stock.fin_week_no,
       stock.customer_class_code,
       stock.store_size_cluster,
       stock.soh_selling,
       stock.soh_cost,
       stock.soh_qty,
       stock.soh_margin,
       stock.clear_soh_selling,
       stock.clear_soh_cost,
       stock.clear_soh_margin,
       stock.clear_soh_qty,
       stock.reg_soh_selling,
       stock.reg_soh_cost,
       stock.reg_soh_margin,
       stock.reg_soh_qty,
       stock.avail_reg_stock_selling,
       stock.avail_reg_stock_qty,
       stock.sit_selling,
       stock.sit_cost,
       stock.sit_margin,
       stock.sit_qty,
       stock.inbound_incl_cust_ord_selling,
       stock.inbound_incl_cust_ord_cost,
       stock.inbound_incl_cust_ord_qty,
       g_date	,
       stock.fin_week_code
 );

      g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;


    l_text :=  'No of Rows Merged : '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      commit;
END local_bulk_insert_stock;
 --**************************************************************************************************
--Update table with  Data from RMS Dense data
--**************************************************************************************************
procedure local_bulk_insert_dense as
begin

  L_TEXT := 'Updating RMS Dense data ....';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 merge /*+ parallel(mart,4) */   into  dwh_performance.mart_rtl_loc_subc_pln_wk_rms mart
   using (  select /*+ parallel (dns,4) full lev1, full loc */
          dns.sk1_location_no,
          lev1.sk1_subclass_no,
          dns.fin_year_no,
          dns.fin_week_no,
          loc.customer_classification as customer_class_code,
          loc.store_size_cluster,
          sum(dns.sales) sales,
          sum(dns.sales_cost) sales_cost,
          sum(dns.sales_margin) sales_margin,
          sum(dns.sales_qty) sales_qty,
          sum(dns.reg_sales) reg_sales,
          sum(dns.reg_sales_cost) reg_sales_cost,
          sum(dns.reg_sales_margin) reg_sales_margin,
          sum(dns.reg_sales_qty) reg_sales_qty ,
          sum(dns.sales_returns_selling) sales_returns_selling,
          sum(dns.sales_returns_cost) sales_returns_cost,
          sum(dns.sales_returns_qty) sales_returns_qty,
          sum(dns.reg_sales_returns_selling) reg_sales_returns_selling,
          sum(dns.reg_sales_returns_cost) reg_sales_returns_cost,
          sum(dns.reg_sales_returns_qty) reg_sales_returns_qty,
          sum(dns.clear_sales_returns_selling) clear_sales_returns_selling,
          sum(dns.clear_sales_returns_cost) clear_sales_returns_cost,
          sum(dns.clear_sales_returns_qty) clear_sales_returns_qty,
          sum(dns.store_intake_selling) store_intake_selling,
          sum(dns.store_intake_cost) store_intake_cost,
          sum(dns.store_intake_margin) store_intake_margin,
          sum(dns.store_intake_qty) store_intake_qty,
          'W' || dns.fin_year_no || dns.fin_week_no as fin_week_code
      from dwh_performance.rtl_loc_sc_wk_rms_dense dns,
           dim_lev1_diff1 lev1,
           dim_location loc
    where lev1.sk1_style_colour_no = dns.sk1_style_colour_no
     and loc.sk1_location_no = dns.sk1_location_no
    and business_unit_no in (51, 52, 53, 54, 55)
    and dns.this_week_start_date between g_this_week_start_date and g_this_week_end_date
--     and fin_year_no = g_fin_year_no
--     and fin_week_no = g_fin_week_no
    group by  dns.sk1_location_no, lev1.sk1_subclass_no, dns.fin_year_no, dns.fin_week_no, loc.customer_classification,  loc.store_size_cluster, fin_week_code) dense

    ON (dense.sk1_location_no   = mart.sk1_location_no and
        dense.sk1_subclass_no   = mart.sk1_subclass_no and
         dense.fin_year_no       = mart.fin_year_no and
         dense.fin_week_no       = mart.fin_week_no)

   when matched then update
    set mart.sales                    		=	dense.sales,
        mart.sales_cost                   =	dense.sales_cost,
        mart.sales_margin               	=	dense.sales_margin,
        mart.sales_qty                		=	dense.sales_qty,
        mart.reg_sales                		=	dense.reg_sales,
        mart.reg_sales_cost           		=	dense.reg_sales_cost,
        mart.reg_sales_margin         		=	dense.reg_sales_margin,
        mart.reg_sales_qty            		=	dense.reg_sales_qty ,
        mart.sales_returns_selling    		=	dense.sales_returns_selling,
        mart. sales_returns_cost      		=	dense. sales_returns_cost,
        mart.sales_returns_qty        		=	dense.sales_returns_qty,
        mart.reg_sales_returns_selling  	=	dense.reg_sales_returns_selling,
        mart.reg_sales_returns_cost   		=	dense.reg_sales_returns_cost,
        mart.reg_sales_returns_qty    		=	dense.reg_sales_returns_qty,
        mart.clear_sales_returns_selling  =	dense.clear_sales_returns_selling,
        mart.clear_sales_returns_cost 		=	dense.clear_sales_returns_cost,
        mart.clear_sales_returns_qty  		=	dense.clear_sales_returns_qty,
        mart.store_intake_selling     		=	dense.store_intake_selling,
        mart.store_intake_cost        		=	dense.store_intake_cost,
        mart.store_intake_margin        	=	dense.store_intake_margin,
        mart.store_intake_qty		          =	dense.store_intake_qty,
        mart.last_updated_date	          = g_date,
        mart.fin_week_code                 = dense.fin_week_code


   WHEN NOT MATCHED THEN INSERT
    (
       sk1_location_no,
        sk1_subclass_no,
        sk1_plan_type_no,
        fin_year_no,
        fin_week_no,
        customer_class_code,
        store_size_cluster,
        sales,
        sales_cost,
        sales_margin,
        sales_qty,
        reg_sales,
        reg_sales_cost,
        reg_sales_margin,
        reg_sales_qty ,
        sales_returns_selling,
        sales_returns_cost,
        sales_returns_qty,
        reg_sales_returns_selling,
        reg_sales_returns_cost,
        reg_sales_returns_qty,
        clear_sales_returns_selling,
        clear_sales_returns_cost,
        clear_sales_returns_qty,
        store_intake_selling,
        store_intake_cost,
        store_intake_margin,
        store_intake_qty,
       last_updated_date,
       fin_week_code
   )
   VALUES
   (
        dense.sk1_location_no,
        dense.sk1_subclass_no,
        1,
        dense.fin_year_no,
        dense.fin_week_no,
        dense.customer_class_code,
        dense.store_size_cluster,
        dense.sales,
        dense.sales_cost,
        dense.sales_margin,
        dense.sales_qty,
        dense.reg_sales,
        dense.reg_sales_cost,
        dense.reg_sales_margin,
        dense.reg_sales_qty ,
        dense.sales_returns_selling,
        dense.sales_returns_cost,
        dense.sales_returns_qty,
        dense.reg_sales_returns_selling,
        dense.reg_sales_returns_cost,
        dense.reg_sales_returns_qty,
        dense.clear_sales_returns_selling,
        dense.clear_sales_returns_cost,
        dense.clear_sales_returns_qty,
        dense.store_intake_selling,
        dense.store_intake_cost,
        dense.store_intake_margin,
        dense.store_intake_qty,
        g_date,
        dense.fin_week_code
 );

    g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;


    l_text :=  'No of Rows Merged : '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      commit;
END local_bulk_insert_dense;

 --**************************************************************************************************
--Update table with  Data from RMS Sparse data
--**************************************************************************************************

procedure local_bulk_insert_sparse as
begin

    L_TEXT := 'Updating RMS Sparse data ....';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


 merge /*+ parallel(mart,4) */   into  dwh_performance.mart_rtl_loc_subc_pln_wk_rms mart
   using
   (select /*+ parallel (4) */
          sprs.sk1_location_no,
          lev1.sk1_subclass_no,
          sprs.fin_year_no,
          sprs.fin_week_no,
          loc.customer_classification as customer_class_code,
          loc.store_size_cluster,
          sum(sprs.clear_sales) clear_sales,
          sum(sprs.clear_sales_cost) clear_sales_cost,
          sum(sprs.clear_sales_margin) clear_sales_margin,
          sum(sprs.clear_sales_qty) clear_sales_qty ,
          sum(sprs.prom_sales) prom_sales,
          sum(sprs.prom_sales_cost) prom_sales_cost,
          sum(sprs.prom_sales_margin) prom_sales_margin,
          sum(sprs.prom_sales_qty) prom_sales_qty,
          sum(sprs.net_mkdn) net_mkdn,
          sum(sprs.mkdn_selling) mkdn_selling,
          sum(sprs.mkup_selling) mkup_selling,
          sum(sprs.clear_mkdn_selling) clear_mkdn_selling,
          sum(sprs.prom_mkdn_selling) prom_mkdn_selling,
          sum(sprs.prom_mkdn_qty) prom_mkdn_qty,
          sum(sprs.mkdn_cancel_selling) mkdn_cancel_selling,
          sum(sprs.mkup_cancel_selling) mkup_cancel_selling,
          sum(sprs.net_inv_adj_selling) net_inv_adj_selling,
          sum(sprs.net_inv_adj_cost) net_inv_adj_cost,
          sum(sprs.net_inv_adj_qty) net_inv_adj_qty,
          sum(sprs.waste_selling) waste_selling,
          sum(sprs.waste_cost) waste_cost,
          sum(sprs.waste_qty) waste_qty,
          sum(sprs.shrinkage_selling) shrinkage_selling,
          sum(sprs.shrinkage_cost) shrinkage_cost,
          sum(sprs.shrinkage_qty) shrinkage_qty,
          sum(sprs.self_supply_selling) self_supply_selling,
          sum(sprs.self_supply_cost) self_supply_cost,
          sum(sprs.self_supply_qty) self_supply_qty,
          sum(sprs.claim_selling) claim_selling,
          sum(sprs.claim_cost) claim_cost,
          sum(sprs.claim_qty) claim_qty,
          sum(sprs.rtv_selling) rtv_selling,
          sum(sprs.rtv_cost) rtv_cost,
          sum(sprs.rtv_qty) rtv_qty,
          sum(sprs.net_ibt_selling) net_ibt_selling,
          sum(sprs.net_ibt_qty) net_ibt_qty,
          sum(ch_alloc_selling) ch_alloc_selling,
          sum(ch_alloc_qty) ch_alloc_qty,
          'W' || sprs.fin_year_no || sprs.fin_week_no as fin_week_code,
          sum(wac_adj_amt) wac_adj_amt
     from dwh_performance.rtl_loc_sc_wk_rms_sparse sprs,
          dim_lev1_diff1 lev1,
         dim_location loc
     where lev1.sk1_style_colour_no = sprs.sk1_style_colour_no
      and loc.sk1_location_no = sprs.sk1_location_no
      and business_unit_no in (51, 52, 53, 54, 55)
      and sprs.this_week_start_date between g_this_week_start_date and g_this_week_end_date
--     and fin_year_no = g_fin_year_no
--     and fin_week_no = g_fin_week_no
     group by  sprs.sk1_location_no,  lev1.sk1_subclass_no, sprs.fin_year_no, sprs.fin_week_no, loc.customer_classification,  loc.store_size_cluster, fin_week_code ) spars

    ON (spars.sk1_location_no   = mart.sk1_location_no and
         spars.sk1_subclass_no = mart.sk1_subclass_no and
        spars.fin_year_no       = mart.fin_year_no and
         spars.fin_week_no       = mart.fin_week_no)

   when matched then update
    set mart.clear_sales        	=	spars.clear_sales,
        mart.clear_sales_cost	    =	spars.clear_sales_cost,
        mart.clear_sales_margin 	=	spars.clear_sales_margin,
        mart.clear_sales_qty    	=	spars.clear_sales_qty ,
        mart.prom_sales         	=	spars.prom_sales,
        mart.prom_sales_cost    	=	spars.prom_sales_cost,
        mart.prom_sales_margin  	=	spars.prom_sales_margin,
        mart.prom_sales_qty     	=	spars.prom_sales_qty,
        mart.net_mkdn           	=	spars.net_mkdn,
        mart.mkdn_selling       	=	spars.mkdn_selling,
        mart.mkup_selling         =	spars.mkup_selling,
        mart.clear_mkdn_selling 	=	spars.clear_mkdn_selling,
        mart.prom_mkdn_selling  	=	spars.prom_mkdn_selling,
        mart.prom_mkdn_qty        =	spars.prom_mkdn_qty,
        mart.mkdn_cancel_selling	=	spars.mkdn_cancel_selling,
        mart.mkup_cancel_selling	=	spars.mkup_cancel_selling,
        mart.net_inv_adj_selling	=	spars.net_inv_adj_selling,
        mart.net_inv_adj_cost 	  =	spars.net_inv_adj_cost,
        mart.net_inv_adj_qty	    =	spars.net_inv_adj_qty,
        mart.waste_selling      	=	spars.waste_selling,
        mart.waste_cost	          =	spars.waste_cost,
        mart.waste_qty        	  =	spars.waste_qty,
        mart.shrinkage_selling	  =	spars.shrinkage_selling,
        mart.shrinkage_cost   	  =	spars.shrinkage_cost,
        mart.shrinkage_qty    	  =	spars.shrinkage_qty,
        mart.self_supply_selling  =	spars.self_supply_selling,
        mart.self_supply_cost   	=	spars.self_supply_cost,
        mart.self_supply_qty    	=	spars.self_supply_qty,
        mart.claim_selling      	=	spars.claim_selling,
        mart.claim_cost         	=	spars.claim_cost,
        mart.claim_qty          	=	spars.claim_qty,
        mart.rtv_selling        	=	spars.rtv_selling,
        mart.rtv_cost           	=	spars.rtv_cost,
        mart. rtv_qty             =	spars. rtv_qty,
        mart.net_ibt_selling      =	spars.net_ibt_selling,
        mart.net_ibt_qty        	=	spars.net_ibt_qty,
        mart.ch_alloc_selling	    =	spars.ch_alloc_selling,
        mart.ch_alloc_qty	        =	spars.ch_alloc_qty,
        mart.last_updated_date    = g_date,
        mart.fin_week_code        = spars.fin_week_code,
        mart.wac_adj_amt          = spars.wac_adj_amt


   WHEN NOT MATCHED THEN INSERT
    (
       sk1_location_no,
       sk1_subclass_no,
        sk1_plan_type_no,
        fin_year_no,
        fin_week_no,
        customer_class_code,
        store_size_cluster,
        clear_sales,
        clear_sales_cost,
        clear_sales_margin,
        clear_sales_qty ,
        prom_sales,
        prom_sales_cost,
        prom_sales_margin,
        prom_sales_qty,
        net_mkdn,
        mkdn_selling,
        mkup_selling,
        clear_mkdn_selling,
        prom_mkdn_selling,
        prom_mkdn_qty,
        mkdn_cancel_selling,
        mkup_cancel_selling,
        net_inv_adj_selling,
        net_inv_adj_cost,
        net_inv_adj_qty,
        waste_selling,
        waste_cost,
        waste_qty,
        shrinkage_selling,
        shrinkage_cost,
        shrinkage_qty,
        self_supply_selling,
        self_supply_cost,
        self_supply_qty,
        claim_selling,
        claim_cost,
        claim_qty,
        rtv_selling,
        rtv_cost,
        rtv_qty,
        net_ibt_selling,
        net_ibt_qty,
        ch_alloc_selling,
        ch_alloc_qty,
        last_updated_date	,
        fin_week_code,
        wac_adj_amt
   )
   VALUES
   (
       spars.sk1_location_no,
       spars.sk1_subclass_no,
        1,
        spars.fin_year_no,
        spars.fin_week_no,
        spars.customer_class_code,
        spars.store_size_cluster,
        spars.clear_sales,
        spars.clear_sales_cost,
        spars.clear_sales_margin,
        spars.clear_sales_qty ,
        spars.prom_sales,
        spars.prom_sales_cost,
        spars.prom_sales_margin,
        spars.prom_sales_qty,
        spars.net_mkdn,
        spars.mkdn_selling,
        spars.mkup_selling,
        spars.clear_mkdn_selling,
        spars.prom_mkdn_selling,
        spars.prom_mkdn_qty,
        spars.mkdn_cancel_selling,
        spars.mkup_cancel_selling,
        spars.net_inv_adj_selling,
        spars.net_inv_adj_cost,
        spars.net_inv_adj_qty,
        spars.waste_selling,
        spars.waste_cost,
        spars.waste_qty,
        spars.shrinkage_selling,
        spars.shrinkage_cost,
        spars.shrinkage_qty,
        spars.self_supply_selling,
        spars.self_supply_cost,
        spars.self_supply_qty,
        spars.claim_selling,
        spars.claim_cost,
        spars.claim_qty,
        spars.rtv_selling,
        spars.rtv_cost,
        spars. rtv_qty,
        spars.net_ibt_selling,
        spars.net_ibt_qty,
        spars.ch_alloc_selling,
        spars.ch_alloc_qty,
        g_date,
        spars.fin_week_code,
        spars.wac_adj_amt
 );
    g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;


    l_text :=  'No of Rows Merged : '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      commit;
END local_bulk_insert_sparse;

 --**************************************************************************************************
--Update table with  IA summary Data from INVENTORY ADJ
--**************************************************************************************************
procedure local_bulk_insert_adj as
begin

    L_TEXT := 'Updating IA summary Data ....';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 merge /*+ parallel(mart,4) */   into  dwh_performance.mart_rtl_loc_subc_pln_wk_rms mart
   using
   (select /*+ parallel (adj,4) full itm, full loc */
      adj.SK1_LOCATION_NO,
      itm.sk1_subclass_no,
      dc.fin_year_no,
      dc.fin_week_no,
      max(dc.fin_week_code) fin_week_code,
      loc.customer_classification as customer_class_code,
      loc.store_size_cluster,
      sum(finance_loc_ia_cost) finance_loc_ia_cost,
      sum(finance_loc_ia_qty) finance_loc_ia_qty,
      sum(finance_loc_ia_selling) finance_loc_ia_selling

  from dwh_performance.RTL_LOC_INV_ADJ_SUMMARY adj,
      dim_item itm,
      dim_location loc,
       dwh_performance.dim_calendar dc
 where itm.SK1_ITEM_NO = adj.SK1_ITEM_NO
     and loc.sk1_location_no = adj.sk1_location_no
     and adj.post_date       = dc.calendar_date
     and business_unit_no in (51, 52, 53, 54, 55)
     and dc.calendar_date between g_this_week_start_date and g_this_week_end_date
--     and fin_year_no = g_fin_year_no
--     and fin_week_no = g_fin_week_no
  group by  adj.sk1_location_no,  sk1_subclass_no,  dc.fin_year_no, dc.fin_week_no,loc.customer_classification,  loc.store_size_cluster) ia

    ON (ia.sk1_location_no   = mart.sk1_location_no and
        ia.sk1_subclass_no   = mart.sk1_subclass_no and
         ia.fin_year_no      = mart.fin_year_no and
         ia.fin_week_no      = mart.fin_week_no)

   when matched then update
    set mart.finance_loc_ia_cost     		=	ia.finance_loc_ia_cost,
        mart.finance_loc_ia_qty         =	ia.finance_loc_ia_qty,
        mart.finance_loc_ia_selling    	=	ia.finance_loc_ia_selling,
        mart.last_updated_date	        = g_date,
        mart.fin_week_code              = ia.fin_week_code


   WHEN NOT MATCHED THEN INSERT
    (
       sk1_location_no,
        sk1_subclass_no,
        sk1_plan_type_no,
        fin_year_no,
        fin_week_no,
        customer_class_code,
        store_size_cluster,
        finance_loc_ia_cost,
        finance_loc_ia_qty,
        finance_loc_ia_selling,
        last_updated_date,
       fin_week_code
   )
   VALUES
   (
        ia.sk1_location_no,
        ia.sk1_subclass_no,
        1,
        ia.fin_year_no,
        ia.fin_week_no,
        ia.customer_class_code,
        ia.store_size_cluster,
        ia.finance_loc_ia_cost,
        ia.finance_loc_ia_qty,
        ia.finance_loc_ia_selling,
         g_date,
        ia.fin_week_code
 );

     g_recs_read     := g_recs_read     + sql%rowcount;
      g_recs_inserted := g_recs_inserted + sql%rowcount;


    l_text :=  'No of Rows Merged : '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      commit;
END local_bulk_insert_adj;
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
   l_text := 'LOAD OF MART_RTL_LOC_SUBC_PLN_WK_RMS MERGE!!!  STARTED AT '||
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

   select today_fin_day_no into g_today_fin_day_no from dim_control;
   
   if g_today_fin_day_no = 7 then
      begin
         select fin_week_no, fin_year_no, this_week_end_date
         into g_end_fin_week_no, g_end_fin_year_no, g_this_week_end_date
         from dim_calendar
         where calendar_date in (select this_wk_end_date from dim_control);
      end;
      
      begin      
        select fin_week_no, fin_year_no, this_week_start_date
        into g_start_fin_week_no, g_start_fin_year_no, g_this_week_start_date
        from dim_calendar
        where calendar_date in (select this_wk_start_date - 35 from dim_control);
      end;
   else
      begin
         select fin_week_no, fin_year_no, this_week_end_date
         into g_end_fin_week_no, g_end_fin_year_no, g_this_week_end_date
         from dim_calendar
         where calendar_date in (select last_wk_end_date from dim_control);
      end;
      
      begin      
        select fin_week_no, fin_year_no, this_week_start_date
        into g_start_fin_week_no, g_start_fin_year_no, g_this_week_start_date
        from dim_calendar
        where calendar_date in (select last_wk_start_date - 35 from dim_control);
      end;
   end if;

   L_TEXT := 'Start Data extract '|| g_start_fin_week_no || ' Week  '|| g_start_fin_year_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   L_TEXT := 'End Data extract '|| g_end_fin_week_no || ' Week  '|| g_end_fin_year_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

    local_bulk_insert_stock;
    local_bulk_insert_dense;
    local_bulk_insert_sparse;
    local_bulk_insert_adj;


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

end WH_PRF_MP_028U_NEW;
