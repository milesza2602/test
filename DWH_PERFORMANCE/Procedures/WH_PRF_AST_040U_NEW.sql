--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_040U_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_040U_NEW" 
(p_forall_limit in integer,p_success out boolean
,p_from_loc_no in integer,p_to_loc_no in integer
) as


--**************************************************************************************************
--  Date:        Jan 2013
--  Author:      Wendy Lyttle
--  Purpose:      Create the daily CHBD item catalog table with RMS stock in the performance layer
--               with input ex RP table from foundation layer.
--
--               Cloned from WH_PRF_RP_001U
--
--  Runtime instructions :
--               Due to the fact that data is sent 1 day ahead of time and that we do not have the
--               stock and sales values at that point,
--               the PERFORMANCE layer is run first in batch before the FOUNDATION layer.
--               In this procedure WH_PRF_AST_040U, 
--                       we select the data based upon the POST_DATE= batch_DATE.
--               Eg. batch_date                = '5 March 2013'
--                   Data sent from srce       = '6 March 2013'
--                   Stock_data for this batch = '5 March 2013'
--                   Therefore, PRD will load with '5 March 2013'
--                         and FND will load with '6 March 2013';
--               In the next procedure WH_PRF_AST_041U, 
--                       we select the data based upon the LAST_UPDATED_DATE= batch_DATE. 
--                       This is due to the fact that sales data can be late
--
--
--  Tables:      Input  - FND_AST_loc_item_dy_catlg
--               Output - RTL_loc_item_dy_AST_catlg
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  W LYTTLE 15 JUNE 2016 -- TEMP FILTER FOR EXCLUDING CHAIN_NO = 40 ADDED
--                           procedure back = wh_prf_ast_040u_bck150616
--                           chg44990
--  W LYTTLE 28 JUNE 2016 -- TEMP FILTER FOR EXCLUDING CHAIN_NO = 40 removed
--                           chg??
---
--  W LYTTLE 20 OCTOBER 2016 -- ADD COLUMNS FOR PRODUCT-LINKING

---
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit           integer       :=  dwh_constants.vc_forall_limit;
g_recs_read              integer       :=  0;
g_recs_inserted          integer       :=  0;
g_recs_updated           integer       :=  0;
g_error_count            number        :=  0;
g_error_index            number        :=  0;
g_count                  number        :=  0;
g_found                  boolean;
g_rec_out                dwh_performance.RTL_loc_item_dy_AST_catlg%rowtype;
g_soh_qty_decatlg        number        :=  0;
g_soh_qty                number        :=  0;
g_soh_selling            number        :=  0;
g_fin_week_no            number        :=  0;
g_fin_year_no            number        :=  0;
g_item_no_decatlg        number(18,0)  :=  0;
g_sk1_item_no_decatlg    number(18,0)  :=  0;
G_FROM_LOC_NO NUMBER;
G_TO_LOC_NO NUMBER;
g_date                   date;
g_this_week_start_date   date;
G_THIS_WEEK_END_DATE     date;


l_message                sys_dwh_errlog.log_text%type;
l_module_name            sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_040U_'||p_from_loc_no;
l_name                   sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rpl;
l_system_name            sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name            sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rpl;
l_procedure_name         sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT                   SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description            sys_dwh_log_summary.log_description%type  := 'LOAD THE AST DAILY CHBD ITEM CATALOG FACTS EX FOUNDATION';
l_process_type           sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- Merge data - insert or update
--**************************************************************************************************
procedure merge_ins_upd as
begin
 
   merge  /*+ APPEND parallel (MERCAT,4) */ into DWH_performance.RTL_loc_item_dy_AST_catlg  MERCAT
   using (
        WITH 
           SELEXT AS 
                           ( 
                            select /*+ parallel(fnd,4)  */ 
                                      FND.*
                              from   DWH_FOUNDATION.FND_AST_loc_item_dy_catlg fnd 
                                      where  fnd.post_date   =  g_date
                                      AND fnd.location_no between g_from_loc_no and g_to_loc_no
                           )
                          ,
           SELFND as (
           select /*+  FULL(di) FULL( dih)  FULL(diu) FULL(DIG) */ 
                             dl.sk1_location_no, 
                             di.sk1_item_no, 
                             fnd_lid.post_date, 
                             dih.sk2_item_no, 
                             dlh.sk2_location_no, 
                                         CASE WHEN active_from_date  <= g_this_week_start_date
                                               AND active_to_date    >= g_this_week_end_date
                                                 then 1 ELSE 0 END  
                            ch_catalog_ind, 
                                         CASE WHEN active_from_date  <= g_this_week_start_date
                                         AND active_to_date >= g_this_week_end_date
                                            then 1 ELSE 0 END 
                            ch_num_catlg_days, 
                             fnd_lid.active_from_date, 
                             fnd_lid.active_to_date, 
                             fnd_lid.item_no,
                             fnd_lid.PROD_LINK_TYPE,
                             DIG.SK1_ITEM_NO SK1_GROUP_ITEM_NO,
                             prod_link_ind
                      from   SELEXT fnd_lid 
                      join   dim_item di 
                             on fnd_lid.item_no                       = di.item_no  
                      join   dim_location dl 
                             on fnd_lid.location_no                   = dl.location_no 
                      join   dim_item_hist dih 
                             On fnd_lid.item_no                        = dih.item_no 
                             and fnd_lid.post_date                     between dih.sk2_active_from_date and dih.sk2_active_to_date 
                      join   dim_location_hist dlh 
                             on fnd_lid.location_no                    = dlh.location_no 
                             and  fnd_lid.post_date                    between dlh.sk2_active_from_date and dlh.sk2_active_to_date 
                      join   dim_item diG 
                              on fnd_lid.GROUP_item_no                       = diG.item_no  
                    )  
          select /*+  PARALLEL(STK,6)  FULL(diu) full(dcap) */ 
                             sf.sk1_location_no, 
                             sf.sk1_item_no, 
                             sf.post_date, 
                             nvl(DCAP.SK1_AVAIL_UDA_VALUE_NO,0) sk1_avail_uda_value_no, 
                             sf.sk2_item_no, 
                             sf.sk2_location_no, 
                            sf.ch_catalog_ind, 
                             CASE WHEN nvl(stk.reg_soh_qty,0) > 0 then 1 ELSE 0 END   Ch_num_avail_days, 
                            sf.ch_num_catlg_days, 
                             0 reg_sales_qty_catlg, 
                             0 reg_sales_catlg, 
                             nvl(stk.reg_soh_qty,0) reg_soh_qty_catlg, 
                             nvl(stk.reg_soh_selling,0) reg_soh_selling_catlg, 
                             0 prom_sales_qty_catlg, 
                             0 prom_sales_catlg, 
                             0 prom_reg_sales_qty_catlg, 
                             0 prom_reg_sales_catlg, 
                             g_date last_updated_date, 
                             sf.active_from_date, 
                             sf.active_to_date, 
                             sf.item_no,
                             sf.PROD_LINK_TYPE,
                             sf.SK1_GROUP_ITEM_NO,
                              0 AVAIL_REG_SALES_QTY_CATLG,
                              0 AVAIL_REG_SALES_CATLG,
                              0 AVAIL_REG_SOH_QTY_CATLG,
                              0 AVAIL_REG_SOH_SELLING_CATLG,
                              0 AVAIL_PROM_SALES_QTY_CATLG,
                              0 AVAIL_PROM_SALES_CATLG,
                              0 AVAIL_PROM_REG_SALES_QTY_CATLG,
                              0 AVAIL_PROM_REG_SALES_CATLG,
                              0 AVAIL_CH_NUM_AVAIL_DAYS,
                              0 AVAIL_CH_NUM_CATLG_DAYS,
                              nvl(prod_link_ind,1) prod_link_ind
                      from   SELfnd sf
                left outer join   rtl_loc_item_dy_rms_stock  stk 
                       on stk.sk1_item_no                        = sf.sk1_item_no 
                          AND stk.sk1_location_no                = sf.sk1_location_no 
                          AND stk.post_date                      = sf.post_date 
                left outer join   dim_item_uda diu 
                        on diu.item_no                           = sf.item_no 
                left outer join    dim_ch_avail_period dcap 
                        on dcap.UDA_VALUE_SHORT_DESC             = diu.RANGE_STRUCTURE_CH_DESC_104    
 
  
        ) MEREXT
             on    ( MERCAT.sk1_location_no            = MEREXT.sk1_location_no  and
                     MERCAT.sk1_item_no                = MEREXT.sk1_item_no      and
                     MERCAT.post_date                  = MEREXT.post_date and
                     MERCAT.sk1_avail_uda_value_no     = MEREXT.sk1_avail_uda_value_no	
                    )
             when matched then 
             update set
                      ch_catalog_ind             = MEREXT.ch_catalog_ind,
                      ch_num_avail_days          = MEREXT.ch_num_avail_days,
                      ch_num_catlg_days          = MEREXT.ch_num_catlg_days,
                      reg_soh_selling_catlg      = MEREXT.reg_soh_selling_catlg,
                      reg_soh_qty_catlg          = MEREXT.reg_soh_qty_catlg,
                      sk2_location_no            = MEREXT.sk2_location_no,
                      sk2_item_no                = MEREXT.sk2_item_no,
                      LAST_UPDATED_DATE          = g_date,
                      PROD_LINK_TYPE             = MEREXT.PROD_LINK_TYPE,
                      SK1_GROUP_ITEM_NO          = MEREXT.SK1_GROUP_ITEM_NO,
                      AVAIL_REG_SALES_QTY_CATLG  = MEREXT.AVAIL_REG_SALES_QTY_CATLG,
                      AVAIL_REG_SALES_CATLG      = MEREXT.AVAIL_REG_SALES_CATLG,
                      AVAIL_REG_SOH_QTY_CATLG    = MEREXT.AVAIL_REG_SOH_QTY_CATLG,
                      AVAIL_REG_SOH_SELLING_CATLG = MEREXT.AVAIL_REG_SOH_SELLING_CATLG,
                      AVAIL_PROM_SALES_QTY_CATLG = MEREXT.AVAIL_PROM_SALES_QTY_CATLG,
                      AVAIL_PROM_SALES_CATLG     = MEREXT.AVAIL_PROM_SALES_CATLG,
                      AVAIL_PROM_REG_SALES_QTY_CATLG = MEREXT.AVAIL_PROM_REG_SALES_QTY_CATLG,
                      AVAIL_PROM_REG_SALES_CATLG = MEREXT.AVAIL_PROM_REG_SALES_CATLG,
                      AVAIL_CH_NUM_AVAIL_DAYS    = MEREXT.AVAIL_CH_NUM_AVAIL_DAYS, 
                      AVAIL_CH_NUM_CATLG_DAYS    = MEREXT.AVAIL_CH_NUM_CATLG_DAYS ,
                      PROD_LINK_IND              = MEREXT.PROD_LINK_IND
              WHEN NOT MATCHED THEN
              INSERT
              (  SK1_LOCATION_NO
                , SK1_ITEM_NO
                , POST_DATE
                , SK1_AVAIL_UDA_VALUE_NO
                , SK2_ITEM_NO
                , SK2_LOCATION_NO
                , CH_CATALOG_IND
                , CH_NUM_AVAIL_DAYS
                , CH_NUM_CATLG_DAYS
                , REG_SALES_QTY_CATLG
                , REG_SALES_CATLG
                , REG_SOH_QTY_CATLG
                , REG_SOH_SELLING_CATLG
                , PROM_SALES_QTY_CATLG
                , PROM_SALES_CATLG
                , PROM_REG_SALES_QTY_CATLG
                , PROM_REG_SALES_CATLG
                , LAST_UPDATED_DATE
                , PROD_LINK_TYPE
                , SK1_GROUP_ITEM_NO
                , AVAIL_REG_SALES_QTY_CATLG
                , AVAIL_REG_SALES_CATLG
                , AVAIL_REG_SOH_QTY_CATLG
                , AVAIL_REG_SOH_SELLING_CATLG
                , AVAIL_PROM_SALES_QTY_CATLG
                , AVAIL_PROM_SALES_CATLG
                , AVAIL_PROM_REG_SALES_QTY_CATLG
                , AVAIL_PROM_REG_SALES_CATLG
                , AVAIL_CH_NUM_AVAIL_DAYS
                , AVAIL_CH_NUM_CATLG_DAYS
                , PROD_LINK_IND
                 )
              values
               (  MEREXT.SK1_LOCATION_NO
                , MEREXT.SK1_ITEM_NO
                , MEREXT.POST_DATE
                , MEREXT.SK1_AVAIL_UDA_VALUE_NO
                , MEREXT.SK2_ITEM_NO
                , MEREXT.SK2_LOCATION_NO
                , MEREXT.CH_CATALOG_IND
                , MEREXT.CH_NUM_AVAIL_DAYS
                , MEREXT.CH_NUM_CATLG_DAYS
                , MEREXT.REG_SALES_QTY_CATLG
                , MEREXT.REG_SALES_CATLG
                , MEREXT.REG_SOH_QTY_CATLG
                , MEREXT.REG_SOH_SELLING_CATLG
                , MEREXT.PROM_SALES_QTY_CATLG
                , MEREXT.PROM_SALES_CATLG
                , MEREXT.PROM_REG_SALES_QTY_CATLG
                , MEREXT.PROM_REG_SALES_CATLG
                , g_date
                , MEREXT.PROD_LINK_TYPE
                , MEREXT.SK1_GROUP_ITEM_NO
                , MEREXT.AVAIL_REG_SALES_QTY_CATLG
                , MEREXT.AVAIL_REG_SALES_CATLG
                , MEREXT.AVAIL_REG_SOH_QTY_CATLG
                , MEREXT.AVAIL_REG_SOH_SELLING_CATLG
                , MEREXT.AVAIL_PROM_SALES_QTY_CATLG
                , MEREXT.AVAIL_PROM_SALES_CATLG
                , MEREXT.AVAIL_PROM_REG_SALES_QTY_CATLG
                , MEREXT.AVAIL_PROM_REG_SALES_CATLG
                , MEREXT.AVAIL_CH_NUM_AVAIL_DAYS
                , MEREXT.AVAIL_CH_NUM_CATLG_DAYS
                , MEREXT.PROD_LINK_IND)
                   ;

                g_recs_inserted :=  0;
                                 
                g_recs_inserted :=  sql%rowcount;       
          
                commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'merge_ins_upd '||'UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'merge_ins_upd '||'UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end merge_ins_upd;
  

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

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************

    dwh_lookup.dim_control(g_date);
    
    g_date := '14 nov 2016';
    
    l_text := 'runin BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    G_FROM_LOC_NO := p_from_loc_no;
    G_TO_LOC_NO := p_to_loc_no;

    execute immediate 'alter session enable parallel dml'; 
    

--**************************************************************************************************
-- Set date variables 
--**************************************************************************************************

    select this_week_start_date, this_week_end_date, fin_week_no, fin_year_no
    into g_this_week_start_date, g_this_week_end_date, g_fin_week_no, g_fin_year_no
    from dim_calendar
    where calendar_date = g_date;

     l_text := 'g_this_week_start_date='||g_this_week_start_date
            ||' g_this_week_end_date='||g_this_week_end_date
            ||' g_fin_week_no='||g_fin_week_no
            ||' g_fin_year_no='||g_fin_year_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     if g_from_loc_no = 0 or g_from_loc_no is null 
     then 
 
           l_text := 'Starting processing.........';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
 ----------------------     
            g_from_loc_no := 1;
            g_to_loc_no := 476;
      
            merge_ins_upd;
     
           l_text := 'Loc-range1 - '||g_from_loc_no||' to '||g_to_loc_no||' Merged RECS =  '||g_recs_inserted||' - '||g_date;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
           commit;
 ---------------------- 
            g_from_loc_no := 477;
            g_to_loc_no := 3047;
     
            merge_ins_upd;
      
           l_text := 'Loc-range2 - '||g_from_loc_no||' to '||g_to_loc_no||' Merged RECS =  '||g_recs_inserted||' - '||g_date;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
           commit;

            g_from_loc_no := 3048;
            g_to_loc_no := 99999;
      
            merge_ins_upd;
     
           l_text := 'Loc-range3 - '||g_from_loc_no||' to '||g_to_loc_no||' Merged RECS =  '||g_recs_inserted||' - '||g_date;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
           commit;
 ---------------------- 

 
--**************************************************************************************************
-- Update stats
--**************************************************************************************************  
     l_text := 'Running update stats on RTL_loc_item_dy_AST_catlg';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','RTL_LOC_ITEM_DY_AST_CATLG', DEGREE => 8);

--**************************************************************************************************  
    g_date := '15 nov 2016';
    
    l_text := 'runin BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    G_FROM_LOC_NO := NULL;
    G_TO_LOC_NO := NULL;

    G_FROM_LOC_NO := p_from_loc_no;
    G_TO_LOC_NO := p_to_loc_no;

    execute immediate 'alter session enable parallel dml'; 
    

--**************************************************************************************************
-- Set date variables 
--**************************************************************************************************

    select this_week_start_date, this_week_end_date, fin_week_no, fin_year_no
    into g_this_week_start_date, g_this_week_end_date, g_fin_week_no, g_fin_year_no
    from dim_calendar
    where calendar_date = g_date;

     l_text := 'g_this_week_start_date='||g_this_week_start_date
            ||' g_this_week_end_date='||g_this_week_end_date
            ||' g_fin_week_no='||g_fin_week_no
            ||' g_fin_year_no='||g_fin_year_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 
           l_text := 'Starting processing.........';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
 ----------------------     
            g_from_loc_no := 1;
            g_to_loc_no := 476;
      
            merge_ins_upd;
     
           l_text := 'Loc-range1 - '||g_from_loc_no||' to '||g_to_loc_no||' Merged RECS =  '||g_recs_inserted||' - '||g_date;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
           commit;
 ---------------------- 
            g_from_loc_no := 477;
            g_to_loc_no := 3047;
     
            merge_ins_upd;
      
           l_text := 'Loc-range2 - '||g_from_loc_no||' to '||g_to_loc_no||' Merged RECS =  '||g_recs_inserted||' - '||g_date;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
           commit;

            g_from_loc_no := 3048;
            g_to_loc_no := 99999;
      
            merge_ins_upd;
     
           l_text := 'Loc-range3 - '||g_from_loc_no||' to '||g_to_loc_no||' Merged RECS =  '||g_recs_inserted||' - '||g_date;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
           commit;
 ---------------------- 
    end if; 
 

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

--    commit;
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

end WH_PRF_AST_040U_NEW;
