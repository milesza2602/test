--------------------------------------------------------
--  DDL for Procedure WH_PRF_WBL_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_WBL_TEST" (
    p_forall_limit IN INTEGER,
    p_success OUT BOOLEAN)
  ---
  --- RUN USING DWH_WH_PRF_ALASTAIR
  ---
AS
  g_sql VARCHAR2(8000);
  g_start DATE   := '01 Jul 2009';
  g_end DATE     := '30 Sep 2009';
  g_count               NUMBER := 0;
  g_recs_inserted       NUMBER := 0;
  g_cnt                 NUMBER;
  Gp_table_name         VARCHAR2(31) := 'stg_rms_rtl_allocation';
  Gp_log_script_name    VARCHAR2(31) :='';
  Gp_log_procedure_name VARCHAR2(31);
  Gp_description        VARCHAR2(31) := 'stg_rms_rtl_allocation';
  g_stmt                VARCHAR2(1500);
  g_table_name          VARCHAR2(31) := 'STG_RMS_RTL_ALLOCATION';
  --g_arc_table_name    varchar2(31);
  --g_hsp_table_name    varchar2(31);
  g_cpy_table_name VARCHAR2(31) := 'STG_RMS_RTL_ALLOCATION_CPY';
  g_index_name     VARCHAR2(31) := 'BS_RMS_RTL_ALLOCATION';
  g_cpy_index_name VARCHAR2(31) := 'BS_RMS_RTL_ALLOCATION_CPY';
  g_pk_name        VARCHAR2(31) := 'PK_S_STG_RMS_RTL_ALLCATN';
  g_cpy_pk_name    VARCHAR2(31) := 'PK_S_STG_RMS_RTL_ALLCATN_CPY';
  g_pk_stmt        VARCHAR2(1500);
  g_tablespace     VARCHAR2(31) := 'STG_STAGING';
  G_LAST_ANALYZED_DATE  date  := sysdate;
  G_start_DATE_time  date  := sysdate;
  G_date  date  := sysdate;
  g_xpart_name     VARCHAR2(32);
  g_wkpart_name     VARCHAR2(32);
  g_xSUBpart_name     VARCHAR2(32);
  g_wkSUBpart_name     VARCHAR2(32);
  g_part_name     VARCHAR2(32);
  g_subpart_name     VARCHAR2(32);

   g_fin_year_no       NUMBER := 0; 
   g_fin_month_no       NUMBER := 0; 
   g_fin_week_no       NUMBER := 0; 
   g_sub       number := 0; 
   g_subp1       NUMBER := 0; 

   g_sub1       NUMBER := 0; 
g_start_week                integer       :=  0;
g_start_year                integer       :=  0;
g_this_week_start_date      date          := trunc(sysdate);
g_this_week_end_date        date          := trunc(sysdate);
g_fin_week_code             varchar2(7);
g_rec_cnt              number        :=  0;

    
  g_deal           NUMBER(14);
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_WBL_TEST';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'TEST';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  --
  --**************************************************************************************************
BEGIN

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
/*
    l_text := 'STARTING';
 --   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    G_START_DATE_TIME := sysdate;
    l_text := 'G_START_DATE_TIME= '||to_char(G_START_DATE_TIME,'dd-mm-yy hh24:mi');
 --   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

g_date := '21 AUGUST 2014';


    execute immediate 'alter session enable parallel dml';
        insert into dwh_performance.temp_rtl_lc_itm_dy_rms_sparse
    with 
        selext as (
         select /*+ parallel(fnd_lid,4) */
   /*               fnd_lid.*
           from   fnd_rtl_loc_item_dy_rms_sale fnd_lid
           where  fnd_lid.last_updated_date  = g_date and
                   ((
                  fnd_lid.prom_sales_qty       ||
                  fnd_lid.ho_prom_discount_qty ||
                  fnd_lid.st_prom_discount_qty ||
                  fnd_lid.clear_sales_qty      ||
                  fnd_lid.waste_qty            ||
                  fnd_lid.shrink_qty           ||
                  fnd_lid.gain_qty             ||
                  fnd_lid.grn_qty              ||
                  fnd_lid.claim_qty            ||
                  fnd_lid.self_supply_qty      ||
                  fnd_lid.wac_adj_amt          ||
                  fnd_lid.invoice_adj_qty      ||
                  fnd_lid.rndm_mass_pos_var    ||
                  fnd_lid.mkup_selling         ||
                  fnd_lid.mkup_cancel_selling  ||
                  fnd_lid.mkdn_selling         ||
                  fnd_lid.mkdn_cancel_selling  ||
                  fnd_lid.clear_mkdn_selling   ||
                  fnd_lid.rtv_qty              ||
                  fnd_lid.sdn_out_qty          ||
                  fnd_lid.ibt_in_qty           ||
                  fnd_lid.ibt_out_qty) is not null
                  )
                  ),
        selfnd as ( 
        select
                  fnd_lid.*,
                  di.standard_uom_code,di.business_unit_no,di.vat_rate_perc,di.sk1_department_no,di.sk1_item_no,
                  dl.chain_no,dl.sk1_location_no,
                  decode(nvl(fnd_li.num_units_per_tray,0),0,1,fnd_li.num_units_per_tray) num_units_per_tray,
                  nvl(fnd_li.clearance_ind,0) clearance_ind,
                  dih.sk2_item_no,
                  dlh.sk2_location_no,
                  dd.jv_dept_ind, dd.packaging_dept_ind, dd.gifting_dept_ind,
                  dd.non_core_dept_ind, dd.bucket_dept_ind, dd.book_magazine_dept_ind
           from   selext fnd_lid,
                  dim_item di,
                  dim_location dl,
                  fnd_location_item fnd_li,
                  dim_item_hist dih,
                  dim_location_hist dlh,
                  dim_department dd
           where  fnd_lid.item_no            = di.item_no and
                  fnd_lid.location_no        = dl.location_no and
                  fnd_lid.item_no            = dih.item_no and
                  fnd_lid.post_date          between dih.sk2_active_from_date and dih.sk2_active_to_date and
                  fnd_lid.location_no        = dlh.location_no and
                  fnd_lid.post_date          between dlh.sk2_active_from_date and dlh.sk2_active_to_date and
                  di.sk1_department_no       = dd.sk1_department_no and
                  fnd_lid.item_no            = fnd_li.item_no(+) and
                  fnd_lid.location_no        = fnd_li.location_no(+)
                  ),
         seldcp as (
         select sfd.sk1_location_no ,sfd.sk1_department_no, sfd.post_date, debtors_commission_perc
         from   rtl_loc_dept_dy ldd, selfnd sfd
         where  sfd.chain_no       = 20 and
                ldd.sk1_location_no       = sfd.sk1_location_no and
                ldd.sk1_department_no     = sfd.sk1_department_no and
                ldd.post_date             = sfd.post_date
                )
 select /*+ parallel(rtl,4) */ 
/* sf.*
 , sdcp.debtors_commission_perc
, rtl.post_date rec_exists
 from selfnd sf, seldcp sdcp,
          rtl_loc_item_dy_rms_sparse rtl
 where sf.sk1_item_no = rtl.sk1_item_no(+)
   and sf.sk1_location_no = rtl.sk1_location_no(+)
   and sf.post_date = rtl.post_date(+)
   and sf.sk1_item_no = sdcp.sk1_department_no(+)
   and sf.sk1_location_no = sdcp.sk1_location_no(+)
   and sf.post_date = sdcp.post_date(+);
  COMMIT;
   
   dbms_output.put_line('test of recs ='||g_rec_cnt);
    l_text := 'test of recs ='||g_rec_cnt;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 
 insert into dwh_performance.RTL_LC_ITM_DY_RMS_SPARSE_WL
 select /*+ parallel(rtl,4) */ 
/* sf.SK1_LOCATION_NO
, sf.SK1_ITEM_NO
, sf.POST_DATE
, sf.SK2_LOCATION_NO
, sf.SK2_ITEM_NO
, sf.PROM_SALES_QTY
, sf.PROM_SALES
, sf.PROM_SALES_COST
, case when chain_no = 20 then nvl(sf.prom_sales_cost,0) + round((nvl(sf.prom_sales_cost,0) * sdcp.debtors_commission_perc / 100),2)  else '' end  prom_sales_fr_cost           
, nvl(sf.prom_sales,0)  - nvl(sf.prom_sales_cost,0)      prom_sales_margin    
, case when chain_no = 20 then sf.prom_sales  else '' end        franchise_prom_sales         
, case when chain_no = 20 then nvl(sf.prom_sales,0) - nvl(( nvl(sf.prom_sales_cost,0) + round( (nvl(sf.prom_sales_cost,0)  * sdcp.debtors_commission_perc / 100),2)),0)  else '' end        franchise_prom_sales_margin 
,nvl(prom_discount_no,0)  prom_discount_no
, sf.HO_PROM_DISCOUNT_AMT
, sf.HO_PROM_DISCOUNT_QTY
, sf.ST_PROM_DISCOUNT_AMT
, sf.ST_PROM_DISCOUNT_QTY
, sf.CLEAR_SALES_QTY
, sf.CLEAR_SALES
, sf.CLEAR_SALES_COST
, case when chain_no = 20 then nvl(sf.clear_sales_cost,0) + round((nvl(sf.clear_sales_cost,0) * sdcp.debtors_commission_perc / 100),2)  else '' end        clear_sales_fr_cost          
, nvl(sf.clear_sales,0) - nvl(sf.clear_sales_cost,0)      clear_sales_margin   
, case when chain_no = 20 then  sf.clear_sales  else '' end        franchise_clear_sales   
, case when chain_no = 20 then nvl(sf.clear_sales,0) - nvl((nvl(sf.clear_sales_cost,0) + round((nvl(sf.clear_sales_cost,0) * sdcp.debtors_commission_perc / 100),2)),0)  else '' end        franchise_clear_sales_margin 
, sf.WASTE_QTY
, sf.WASTE_SELLING
, sf.WASTE_COST
, case when chain_no = 20 then nvl(sf.waste_cost,0)  + round((nvl(sf.waste_cost,0)  * sdcp.debtors_commission_perc / 100),2)  else '' end        waste_fr_cost                
, sf.SHRINK_QTY
, sf.SHRINK_SELLING
, sf.SHRINK_COST
, case when chain_no = 20 then nvl(sf.shrink_cost,0) + round((nvl(sf.shrink_cost,0) * sdcp.debtors_commission_perc / 100),2)  else '' end        shrink_fr_cost               
, sf.GAIN_QTY
, sf.GAIN_SELLING
, sf.GAIN_COST
, case when chain_no = 20 then nvl(sf.gain_cost,0)   + round((nvl(sf.gain_cost,0)   * sdcp.debtors_commission_perc / 100),2)  else '' end        gain_fr_cost                 
, sf.GRN_QTY
, case when sf.business_unit_no = 50 then round((nvl(sf.grn_qty,0)/sf.num_units_per_tray),0)  else '' end       grn_cases     
, sf.GRN_SELLING
, sf.GRN_COST
, case when chain_no = 20 then nvl(sf.grn_cost,0)    + round((nvl(sf.grn_cost,0)    * sdcp.debtors_commission_perc / 100),2)  else '' end        grn_fr_cost      
, nvl(sf.grn_selling,0) - nvl(sf.grn_cost,0)      grn_margin           
, nvl(sf.shrink_qty,0)  + nvl(sf.gain_qty,0)      shrinkage_qty        
, nvl(sf.shrink_selling,0)  + nvl(sf.gain_selling,0)      shrinkage_selling    
, nvl(sf.shrink_cost,0)     + nvl(sf.gain_cost,0)      shrinkage_cost       
, case when chain_no = 20 then (nvl(sf.shrink_cost,0)     + nvl(sf.gain_cost,0)) +  round((nvl(sf.shrink_cost,0)     + nvl(sf.gain_cost,0)) * sdcp.debtors_commission_perc / 100,2) else '' end shrinkage_fr_cost        
, nvl(abs(sf.shrink_qty),0) + nvl(abs(sf.gain_qty),0)      abs_shrinkage_qty    
, nvl(abs(sf.shrink_selling),0)  + nvl(abs(sf.gain_selling),0)      abs_shrinkage_selling
, nvl(abs(sf.shrink_cost),0)     + nvl(abs(sf.gain_cost),0)      abs_shrinkage_cost   
, case when chain_no = 20 then (nvl(abs(sf.shrink_cost),0)     + nvl(abs(sf.gain_cost),0)) + round((nvl(abs(sf.shrink_cost),0)     + nvl(abs(sf.gain_cost),0)) * sdcp.debtors_commission_perc / 100,2)   else '' end   abs_shrinkage_fr_cost   
, sf.CLAIM_QTY
, sf.CLAIM_SELLING
, sf.CLAIM_COST
, case when chain_no = 20 then nvl(sf.claim_cost,0)       + round((nvl(sf.claim_cost,0)       * sdcp.debtors_commission_perc / 100),2)  else '' end     claim_fr_cost                
, sf.SELF_SUPPLY_QTY
, sf.SELF_SUPPLY_SELLING
, sf.SELF_SUPPLY_COST
, case when chain_no = 20 then nvl(sf.self_supply_cost,0) + round((nvl(sf.self_supply_cost,0) * sdcp.debtors_commission_perc / 100),2)  else '' end        self_supply_fr_cost          
, sf.WAC_ADJ_AMT
, sf.INVOICE_ADJ_QTY
, sf.INVOICE_ADJ_SELLING
, sf.INVOICE_ADJ_COST
, sf.RNDM_MASS_POS_VAR
, sf.MKUP_SELLING
, sf.MKUP_CANCEL_SELLING
, sf.MKDN_SELLING
, sf.MKDN_CANCEL_SELLING
, nvl(sf.ho_prom_discount_qty,0) + nvl(sf.st_prom_discount_qty,0)      prom_mkdn_qty        
, nvl(sf.ho_prom_discount_amt,0) + nvl(sf.st_prom_discount_amt,0)      prom_mkdn_selling    
, sf.CLEAR_MKDN_SELLING
, nvl(sf.clear_sales_qty,0)  + nvl(sf.prom_sales_qty,0)      mkdn_sales_qty       
, nvl(sf.clear_sales,0)      + nvl(sf.prom_sales,0)      mkdn_sales           
, nvl(sf.clear_sales_cost,0) + nvl(sf.prom_sales_cost,0)      mkdn_sales_cost   
, nvl(sf.mkdn_selling,0) + nvl(sf.clear_mkdn_selling,0) - nvl(sf.mkdn_cancel_selling,0) + nvl(sf.mkup_cancel_selling,0) - nvl(sf.mkup_selling,0) + nvl((nvl(sf.ho_prom_discount_amt,0) + nvl(sf.st_prom_discount_amt,0)  ),0)  net_mkdn             
, sf.RTV_QTY
, case when sf.business_unit_no = 50 then  round((nvl(sf.rtv_qty,0)/sf.num_units_per_tray),0) else '' end       rtv_cases     
, sf.RTV_SELLING
, sf.RTV_COST
, case when chain_no = 20 then nvl(sf.rtv_cost,0)      + round((nvl(sf.rtv_cost,0)     * sdcp.debtors_commission_perc / 100),2)  else '' end        rtv_fr_cost                  
, sf.SDN_OUT_QTY
, sf.SDN_OUT_SELLING
, sf.SDN_OUT_COST
, case when chain_no = 20 then nvl(sf.sdn_out_cost,0)  + round((nvl(sf.sdn_out_cost,0) * sdcp.debtors_commission_perc / 100),2)  else '' end        sdn_out_fr_cost              
, case when sf.business_unit_no = 50 then round((nvl(sf.sdn_out_qty,0)/sf.num_units_per_tray),0) else '' end       sdn_out_cases 
, sf.IBT_IN_QTY
, sf.IBT_IN_SELLING
, sf.IBT_IN_COST
, case when chain_no = 20 then nvl(sf.ibt_in_cost,0)   + round((nvl(sf.ibt_in_cost,0)  * sdcp.debtors_commission_perc / 100),2)  else '' end        ibt_in_fr_cost               
, sf.IBT_OUT_QTY
, sf.IBT_OUT_SELLING
, sf.IBT_OUT_COST
, case when chain_no = 20 then nvl(sf.ibt_out_cost,0)  + round((nvl(sf.ibt_out_cost,0) * sdcp.debtors_commission_perc / 100),2)  else '' end        ibt_out_fr_cost              
, nvl(sf.ibt_in_qty,0)     - nvl(sf.ibt_out_qty,0)      net_ibt_qty          
, nvl(sf.ibt_in_selling,0) - nvl(sf.ibt_out_selling,0)      net_ibt_selling      
, case when sf.jv_dept_ind      = 0 and sf.packaging_dept_ind    = 0 and  sf.gifting_dept_ind  = 0 and sf.non_core_dept_ind      = 0 and
      sf.bucket_dept_ind   = 0 and sf.book_magazine_dept_ind = 0 then
     sf.shrink_cost else '' end   shrink_excl_some_dept_cost 
, case when sf.jv_dept_ind      = 0 and sf.packaging_dept_ind    = 0 and  sf.gifting_dept_ind  = 0 and sf.non_core_dept_ind      = 0 and
      sf.bucket_dept_ind   = 0 and sf.book_magazine_dept_ind = 0 then
     sf.gain_cost else '' end   gain_excl_some_dept_cost 
, ''      net_waste_qty                  
, 0     trunked_qty                    
, 0     trunked_cases                  
, 0     trunked_selling                
, 0     trunked_cost                   
, 0     dc_delivered_qty               
, 0     dc_delivered_cases             
, 0     dc_delivered_selling           
, 0     dc_delivered_cost              
, nvl(sf.waste_qty,0) + nvl(sf.shrink_qty,0) + nvl(sf.gain_qty,0) + nvl(sf.self_supply_qty,0) + nvl(sf.claim_qty,0)                     net_inv_adj_qty      
, nvl(sf.waste_selling,0)+ nvl(sf.shrink_selling,0) + nvl(sf.gain_selling,0) + nvl(sf.self_supply_selling,0) + nvl(sf.claim_selling,0)  net_inv_adj_selling  
, nvl(sf.waste_cost,0) + nvl(sf.shrink_cost,0) + nvl(sf.gain_cost,0) + nvl(sf.self_supply_cost,0) + nvl(sf.claim_cost,0)                net_inv_adj_cost     
,0 -- case when chain_no = 20 then nvl(sf.waste_fr_cost,0) + nvl(sf.shrink_fr_cost,0) + nvl(sf.gain_fr_cost,0) +  nvl(sf.self_supply_fr_cost,0) + nvl(sf.claim_fr_cost,0) else '' end net_inv_adj_fr_cost
, sf.LAST_UPDATED_DATE
, sf.CH_ALLOC_QTY
, sf.CH_ALLOC_SELLING
, sf.SHRINK_CASES
, sf.GAIN_CASES
, sf.SHRINKAGE_CASES
, sf.ABS_SHRINKAGE_SELLING_DEPT
, sf.ABS_SHRINKAGE_COST_DEPT
, sf.ABS_SHRINKAGE_QTY_DEPT
, sf.ABS_SHRINKAGE_CASES_DEPT
, null WASTE_CASES
, null CLAIM_CASES
, null SELF_SUPPLY_CASES
from dwh_performance.temp_rtl_lc_itm_dy_rms_sparse
where rec_exists is null
;
  COMMIT;
   
   dbms_output.put_line('test of recs ='||g_rec_cnt);
    l_text := 'test of recs ='||g_rec_cnt;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

/*


    l_text := 'test partition  RTL_LOC_ITEM_WK_ast_CATLG  STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.DIM_CONTROL(G_DATE);
--g_date := '1 mar 2013';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    FOR g_sub IN 0..5
      LOOP
        g_recs_inserted := 0;
        select fin_year_no, fin_week_no, this_week_start_date, this_week_end_date, fin_week_code
        into   g_start_year, g_start_week, g_this_week_start_date, g_this_week_end_date, g_fin_week_code
        from   dim_calendar
        WHERE calendar_date = g_date - (g_sub * 7);
    
            G_SUBp1 := G_SUB+1;
    
            l_text := '---- WEEK '||G_SUBp1||' ----';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    
        ----------------------------------------------------------    
        -- subpartition_name example = RTL_LIDAC_040313
        ----------------------------------------------------------
            l_text := '   ---- Daily SUBPARTITION and PARTITION ----';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
            for g_sub1 in 0..6
                loop
                g_subpart_name := 'RTL_LIDAC_'||to_char((g_this_week_start_date + 1 + g_sub1),'ddmmyy');
                l_text := '        subpartition='||g_subpart_name;
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                dbms_stats.gather_table_stats ('DWH_PERFORMANCE',
                                                'RTL_LOC_ITEM_DY_AST_CATLG',
                                                g_subpart_name,
                                                granularity => 'SUBPARTITION',
                                                degree => 4);
               commit;
            end loop;
    
        ----------------------------------------------------------
        -- week partition_name example = TP_RTL_LIDAC_040313
        ----------------------------------------------------------
            select fin_year_no,  fin_month_no
            into   g_fin_year_no, g_fin_month_no
            from   dim_calendar
            where calendar_date = g_this_week_start_date + 1
            group by fin_year_no,  fin_month_no;        
            
            g_Xpart_name := 'TP_RTL_LIDAC_M'||g_fin_year_no||g_fin_month_no;
            
            SELECT PARTITION_NAME, LAST_ANALYZED
            INTO G_PART_NAME, G_LAST_ANALYZED_DATE
            FROM DBA_TAB_PARTITIONS
            WHERE PARTITION_NAME = G_XPART_NAME;
            
                  -- check if analyzed during this run, if so then do not regenerate stats
                  G_START_DATE_TIME := sysdate;
                  
                  IF G_LAST_ANALYZED_DATE < G_START_DATE_TIME
                  THEN
                      l_text := '        partition='||g_part_name||' last_analyzed='||to_char(G_LAST_ANALYZED_DATE,'dd-mm-yy hh24:mi');
                      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                    dbms_stats.gather_table_stats ('DWH_PERFORMANCE',
                                                      'RTL_LOC_ITEM_DY_AST_CATLG',
                                                     g_part_name,
                                                      granularity => 'PARTITION',
                                                      degree => 4);
                  END IF;
          
                 commit;
    
        ----------------------------------------------------------
        -- partition_name example = RTL_LIWAC_M20124_14
        ----------------------------------------------------------
            l_text := '   ---- Weekly SUBPARTITION ----';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
            select fin_week_no
            into   g_fin_week_no
            from   dim_calendar
            where calendar_date = g_this_week_start_date + 1
            group by fin_week_no;       
    
            g_XSUBpart_name := 'RTL_LIWAC_M'||g_fin_year_no;
            g_XSUBpart_name := g_XSUBpart_name||g_fin_month_no;
            g_XSUBpart_name := g_XSUBpart_name||'_'||g_fin_week_no;
    
            SELECT SUBPARTITION_NAME, LAST_ANALYZED
            INTO G_WKSUBPART_NAME, G_LAST_ANALYZED_date
            FROM DBA_TAB_SUBPARTITIONS
            WHERE SUBPARTITION_NAME = G_XSUBPART_NAME;
                  -- check if analyzed during this run, if so then do not regenerate stats
                  G_START_DATE_TIME := sysdate;
                
                 IF G_LAST_ANALYZED_DATE < G_START_DATE_TIME
                  then
                      l_text := '        subpartition='||g_WKSUBpart_name||' last_analyzed='||to_char(G_LAST_ANALYZED_DATE,'dd-mm-yy hh24:mi');
                      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                    dbms_stats.gather_table_stats ('DWH_PERFORMANCE',
                                                      'RTL_LOC_ITEM_WK_AST_CATLG',
                                                      g_wkpart_name,
                                                      granularity => 'SUBPARTITION',
                                                      degree => 4);
                  END IF;
          
                 commit;



    end loop;

    l_text := 'ENDING';
 --   to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  */
 
 
 update fnd_rtl_shipment a set last_updated_date = '13 nov 2015'
where exists (select /*+ full(c) */ a.SHIPMENT_NO, SEQ_NO, ITEM_NO from stg_rms_rtl_shipment_cpy c
where a.SHIPMENT_NO = c.shipment_no and a.SEQ_NO = c.seq_no and a.item_no = c.item_no);
commit;
  
  p_success := true;
    exception
--
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);

       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);

       rollback;
       p_success := false;
       raise;
END WH_PRF_WBL_TEST;
