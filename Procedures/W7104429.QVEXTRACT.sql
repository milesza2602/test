-- ****** Object: Procedure W7104429.QVEXTRACT Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "QVEXTRACT" as

  pdate     date := '27 jun 16';
  lrows  int;

begin
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
    EXECUTE IMMEDIATE 'set serveroutput on';
  
    EXECUTE IMMEDIATE 'truncate table W7104429.BIBrave_BaseA'; 
    commit;
    
    insert  /*+ parallel(tgt,4) */ into W7104429.BIBrave_BaseA tgt
    with
        FinPrds as
            (select Fin_Year_No, Fin_Week_No, FIN_MONTH_NO, THIS_WEEK_START_DATE, THIS_WEEK_END_DATE, trunc(sysdate, 'WW')+1 CURR_WEEK_START_DATE
             FROM   DWH_PERFORMANCE.DIM_CALENDAR_WK
             where 	Fin_Year_No = 2017    --(select TODAY_FIN_YEAR_NO from dim_control)
             and   	Fin_Week_No = 45    -- between (select TODAY_FIN_WEEK_NO-6 from dim_control) and (select TODAY_FIN_WEEK_NO-5 from dim_control)
    --      where Fin_Year_No = 2016 and FIN_MONTH_NO in (9)   --= 3
    --                  where Fin_Year_No = 2016 and FIN_WEEK_NO in (40,41,42)
            ),
            
        loc     as 
            (SELECT sk1_location_no, LOCATION_NO, SK1_REGION_NO, area_no, WH_FD_ZONE_NO FROM dwh_performance.dim_Location WHERE area_no IN (8700, 8800, 9951)
    and sk1_location_no = 805
            ),
    
        Itm    as 
             -- Foods only ...
            (select distinct sk1_item_no, SK1_BUSINESS_UNIT_NO, SK1_DEPARTMENT_NO from dwh_performance.dim_item where business_unit_no = 50
     and sk1_item_no in (21880738, 62030, 25918968)
             ),
    
        DNS    as (         
                      select    /*+ parallel(SRC,8) */ 
                                SRC.fin_year_no, 
                                SRC.fin_week_no,  
                                SRC.sk1_location_no, 
                                SRC.SK1_ITEM_NO,
                                si.SK1_DEPARTMENT_NO,
                                
                                nvl(SALES_QTY,0)                                    SALES_QTY,
                                nvl(SALES,0)                                        SALES,
                                nvl(SALES_COST,0)                                   SALES_COST,
                                nvl(SALES_MARGIN,0)                                 SALES_MARGIN,
                                nvl(REG_SALES_QTY,0)                                REG_SALES_QTY,
                                nvl(REG_SALES,0)                                    REG_SALES,
                                nvl(REG_SALES_COST,0)                               REG_SALES_COST,
                                nvl(REG_SALES_MARGIN,0)                             REG_SALES_MARGIN,
                                nvl(SDN_IN_QTY,0)                                   SDN_IN_QTY,
                                nvl(SDN_IN_SELLING,0)                               SDN_IN_SELLING,
                                nvl(SDN_IN_COST,0)                                  SDN_IN_COST,
                                nvl(ACTL_STORE_RCPT_QTY,0)                          ACTL_STORE_RCPT_QTY,
                                nvl(ACTL_STORE_RCPT_SELLING,0)                      ACTL_STORE_RCPT_SELLING,
                                nvl(ACTL_STORE_RCPT_COST,0)                         ACTL_STORE_RCPT_COST
                      from      dwh_performance.RTL_LOC_ITEM_WK_RMS_DENSE src, 
                                loc                       sl, 
                                itm                       si,
                                FinPrds                   sc
                      where     src.sk1_location_no = sl.sk1_location_no 
                      and       src.sk1_item_no     = si.sk1_item_no 
                      and       src.fin_year_no     = sc.fin_year_no 
                      and       src.fin_week_no     = sc.fin_week_no
                    ),
        
        SPRS   as
                    ( 
                      select    /*+ parallel(SRC,8) */ 
                                SRC.fin_year_no, 
                                SRC.fin_week_no, 
                                SRC.sk1_location_no,
                                SRC.SK1_ITEM_NO,
                              
                                nvl(PROM_SALES_QTY,0)	                              PROM_SALES_QTY,
                                nvl(PROM_SALES,0)	                                  PROM_SALES,
                                nvl(PROM_SALES_COST,0)	                            PROM_SALES_COST,
                                nvl(PROM_SALES_MARGIN,0)	                          PROM_SALES_MARGIN,
                                nvl(WASTE_QTY,0)	                                  WASTE_QTY,
                                nvl(WASTE_SELLING,0)	                              WASTE_SELLING,
                                nvl(WASTE_COST,0)	                                  WASTE_COST,
                                nvl(SHRINK_QTY,0)	                                  SHRINK_QTY,
                                nvl(SHRINK_SELLING,0)	                              SHRINK_SELLING,
                                nvl(SHRINK_COST,0)	                                SHRINK_COST,
                                nvl(GAIN_QTY,0)	                                    GAIN_QTY,
                                nvl(GAIN_SELLING,0)	                                GAIN_SELLING,
                                nvl(GAIN_COST,0)	                                  GAIN_COST,
                                nvl(GRN_QTY,0)	                                    GRN_QTY,
                                nvl(GRN_SELLING,0)	                                GRN_SELLING,
                                nvl(GRN_COST,0)	                                    GRN_COST,
                                nvl(GRN_MARGIN,0)	                                  GRN_MARGIN,
                                nvl(RTV_QTY,0)	                                    RTV_QTY,
                                nvl(RTV_SELLING,0)	                                RTV_SELLING,
                                nvl(RTV_COST,0)	                                    RTV_COST,
                                nvl(SDN_OUT_QTY,0)	                                SDN_OUT_QTY,
                                nvl(SDN_OUT_SELLING,0)	                            SDN_OUT_SELLING,
                                nvl(SDN_OUT_COST,0)	                                SDN_OUT_COST,
                                nvl(DC_DELIVERED_QTY,0)	                            DC_DELIVERED_QTY,
                                nvl(DC_DELIVERED_SELLING,0)	                        DC_DELIVERED_SELLING,
                                nvl(DC_DELIVERED_COST,0)	                          DC_DELIVERED_COST     
                             
                      from      dwh_performance.RTL_LOC_ITEM_WK_RMS_SPARSE  src, 
                                loc                         sl, 
                                itm                         si,
                                FinPrds                     sc
                      where     src.sk1_location_no = sl.sk1_location_no 
                      and       src.sk1_item_no     = si.sk1_item_no 
                      and       src.fin_year_no     = sc.fin_year_no 
                      and       src.fin_week_no     = sc.fin_week_no
                    ),
        
        POSJV   as
                    (
                      select    /*+ parallel(SRC,8) */ 
                                SRC.fin_year_no, 
                                SRC.fin_week_no, 
                                SRC.sk1_location_no,
                                SRC.SK1_ITEM_NO,
                                
                                nvl(WASTE_RECOV_QTY,0)	                            WASTE_RECOV_QTY,
                                nvl(WASTE_RECOV_REVENUE,0)	                        WASTE_RECOV_REVENUE,
                                nvl(SPEC_DEPT_QTY,0)	                              SPEC_DEPT_QTY,
                                nvl(SPEC_DEPT_REVENUE,0)	                          SPEC_DEPT_REVENUE,
                                nvl(SPEC_DEPT_WASTE_RECOV_QTY,0)	                  SPEC_DEPT_WASTE_RECOV_QTY,
                                nvl(SPEC_DEPT_WASTE_RECOV_REVENUE,0)	              SPEC_DEPT_WASTE_RECOV_REVENUE
                      from      dwh_performance.RTL_LOC_ITEM_wk_POS_JV  src, 
                                loc                     sl, 
                                itm                     si,
                                FinPrds                 sc
                      where     src.sk1_location_no = sl.sk1_location_no 
                      and       src.sk1_item_no     = si.sk1_item_no 
                      and       src.fin_year_no     = sc.fin_year_no 
                      and       src.fin_week_no     = sc.fin_week_no
                    ),
        
        Stock  as
                  (   
                      select    /*+ parallel(SRC,8) */ 
                                SRC.fin_year_no, 
                                SRC.fin_week_no,  
                                SRC.sk1_location_no,
                                SRC.SK1_ITEM_NO,
                                
                                nvl(SIT_QTY,0)	                                    SIT_QTY,
                                nvl(SIT_SELLING,0)	                                SIT_SELLING,
                                nvl(SIT_COST,0)	                                    SIT_COST,
                                nvl(SIT_MARGIN,0)	                                  SIT_MARGIN,
                                nvl(SOH_QTY,0)	                                    SOH_QTY,
                                nvl(SOH_SELLING,0)	                                SOH_SELLING,
                                nvl(SOH_COST,0)	                                    SOH_COST,
                                nvl(SOH_MARGIN,0)	                                  SOH_MARGIN,
                                nvl(BOH_QTY,0)	                                    BOH_QTY,
                                nvl(BOH_SELLING,0)	                                BOH_SELLING,
                                nvl(BOH_COST,0)	                                    BOH_COST
                      from      DWH_PERFORMANCE.RTL_LOC_ITEM_WK_RMS_STOCK src, 
                                loc                                       sl, 
                                itm                                       si,
                                FinPrds                                   sc
                      where     src.sk1_location_no = sl.sk1_location_no 
                      and       src.sk1_item_no     = si.sk1_item_no 
                      and       src.fin_year_no     = sc.fin_year_no 
                      and       src.fin_week_no     = sc.fin_week_no
                  ),
                  
        CTLG   as
                  (   
                      select    /*+ parallel(SRC,8) */ 
                                SRC.fin_year_no, 
                                SRC.fin_week_no,  
                                SRC.sk1_location_no,
                                SRC.SK1_ITEM_NO,
    
                                nvl(BOH_ADJ_QTY,0)	                                BOH_ADJ_QTY,
                                nvl(BOH_ADJ_SELLING,0)	                            BOH_ADJ_SELLING,
                                nvl(BOH_ADJ_COST,0)	                                BOH_ADJ_COST,
                                nvl(FD_NUM_AVAIL_DAYS,0)	                          FD_NUM_AVAIL_DAYS,
                                nvl(FD_NUM_AVAIL_DAYS_ADJ,0)	                      FD_NUM_AVAIL_DAYS_ADJ,
                                nvl(FD_NUM_CATLG_DAYS,0)	                          FD_NUM_CATLG_DAYS,
                                nvl(FD_NUM_CATLG_DAYS_ADJ,0)	                      FD_NUM_CATLG_DAYS_ADJ,
                                nvl(PRODUCT_STATUS_CODE,0)	                        PRODUCT_STATUS_CODE,
                                nvl(WK_DELIVERY_PATTERN,0)	                        WK_DELIVERY_PATTERN,
                                nvl(NUM_UNITS_PER_TRAY,0)	                          NUM_UNITS_PER_TRAY,
                                nvl(FD_NUM_DC_CATLG_DAYS,0)	                        FD_NUM_DC_CATLG_DAYS,
                                nvl(FD_NUM_DC_CATLG_ADJ_DAYS,0)	                    FD_NUM_DC_CATLG_ADJ_DAYS,
                                nvl(FD_NUM_DC_AVAIL_DAYS,0)	                        FD_NUM_DC_AVAIL_DAYS,
                                nvl(FD_NUM_DC_AVAIL_ADJ_DAYS,0)	                    FD_NUM_DC_AVAIL_ADJ_DAYS,
                                nvl(FD_NUM_CATLG_WK,0)	                            FD_NUM_CATLG_WK,
                                nvl(FD_NUM_CUST_AVAIL_ADJ,0)	                      FD_NUM_CUST_AVAIL_ADJ,
                                nvl(NUM_FACINGS,0)	                                NUM_FACINGS
                               
                      from      DWH_PERFORMANCE.RTL_LOC_ITEM_WK_CATALOG src, 
                                loc                                     sl, 
                                itm                                     si,
                                FinPrds                                 sc
                      where     src.sk1_location_no = sl.sk1_location_no 
                      and       src.sk1_item_no     = si.sk1_item_no 
                      and       src.fin_year_no     = sc.fin_year_no 
                      and       src.fin_week_no     = sc.fin_week_no
                      AND       fd_num_catlg_days   > 0
                  ),
        
        BaseA  as 
                (
                  select /*+ parallel(4) */
                          fin_year_no, 
                          fin_week_no,  
                          a.sk1_location_no,
                          a.SK1_ITEM_NO,
                          b.SK1_BUSINESS_UNIT_NO,
                          b.SK1_DEPARTMENT_NO,
                          
                          sum(SALES_QTY)	                                          SALES_QTY,
                          sum(SALES)	                                              SALES,
                          sum(SALES_COST)	                                          SALES_COST,
                          sum(SALES_MARGIN)	                                        SALES_MARGIN,
                          sum(REG_SALES_QTY)	                                      REG_SALES_QTY,
                          sum(REG_SALES)	                                          REG_SALES,
                          sum(REG_SALES_COST)	                                      REG_SALES_COST,
                          sum(REG_SALES_MARGIN)	                                    REG_SALES_MARGIN,
                          sum(SDN_IN_QTY)	                                          SDN_IN_QTY,
                          sum(SDN_IN_SELLING)	                                      SDN_IN_SELLING,
                          sum(SDN_IN_COST)	                                        SDN_IN_COST,
                          sum(ACTL_STORE_RCPT_QTY)	                                ACTL_STORE_RCPT_QTY,
                          sum(ACTL_STORE_RCPT_SELLING)	                            ACTL_STORE_RCPT_SELLING,
                          sum(ACTL_STORE_RCPT_COST)	                                ACTL_STORE_RCPT_COST,
                          sum(PROM_SALES_QTY)	                                      PROM_SALES_QTY,
                          sum(PROM_SALES)	                                          PROM_SALES,
                          sum(PROM_SALES_COST)	                                    PROM_SALES_COST,
                          sum(PROM_SALES_MARGIN)	                                  PROM_SALES_MARGIN,
                          sum(WASTE_QTY)	                                          WASTE_QTY,
                          sum(WASTE_SELLING)	                                      WASTE_SELLING,
                          sum(WASTE_COST)	                                          WASTE_COST,
                          sum(SHRINK_QTY)	                                          SHRINK_QTY,
                          sum(SHRINK_SELLING)	                                      SHRINK_SELLING,
                          sum(SHRINK_COST)	                                        SHRINK_COST,
                          sum(GAIN_QTY)	                                            GAIN_QTY,
                          sum(GAIN_SELLING)	                                        GAIN_SELLING,
                          sum(GAIN_COST)	                                          GAIN_COST,
                          sum(GRN_QTY)	                                            GRN_QTY,
                          sum(GRN_SELLING)	                                        GRN_SELLING,
                          sum(GRN_COST)	                                            GRN_COST,
                          sum(GRN_MARGIN)	                                          GRN_MARGIN,
                          sum(RTV_QTY)	                                            RTV_QTY,
                          sum(RTV_SELLING)	                                        RTV_SELLING,
                          sum(RTV_COST)	                                            RTV_COST,
                          sum(SDN_OUT_QTY)	                                        SDN_OUT_QTY,
                          sum(SDN_OUT_SELLING)	                                    SDN_OUT_SELLING,
                          sum(SDN_OUT_COST)	                                        SDN_OUT_COST,
                          sum(DC_DELIVERED_QTY)	                                    DC_DELIVERED_QTY,
                          sum(DC_DELIVERED_SELLING)	                                DC_DELIVERED_SELLING,
                          sum(DC_DELIVERED_COST)	                                  DC_DELIVERED_COST,
                          sum(WASTE_RECOV_QTY)	                                    WASTE_RECOV_QTY,
                          sum(WASTE_RECOV_REVENUE)	                                WASTE_RECOV_REVENUE,
                          sum(SPEC_DEPT_QTY)	                                      SPEC_DEPT_QTY,
                          sum(SPEC_DEPT_REVENUE)	                                  SPEC_DEPT_REVENUE,
                          sum(SPEC_DEPT_WASTE_RECOV_QTY)	                          SPEC_DEPT_WASTE_RECOV_QTY,
                          sum(SPEC_DEPT_WASTE_RECOV_REVENUE)	                      SPEC_DEPT_WASTE_RECOV_REVENUE,
                          sum(SIT_QTY)	                                            SIT_QTY,
                          sum(SIT_SELLING)	                                        SIT_SELLING,
                          sum(SIT_COST)	                                            SIT_COST,
                          sum(SIT_MARGIN)	                                          SIT_MARGIN,
                          sum(SOH_QTY)	                                            SOH_QTY,
                          sum(SOH_SELLING)	                                        SOH_SELLING,
                          sum(SOH_COST)	                                            SOH_COST,
                          sum(SOH_MARGIN)	                                          SOH_MARGIN,
                          sum(BOH_QTY)	                                            BOH_QTY,
                          sum(BOH_SELLING)	                                        BOH_SELLING,
                          sum(BOH_COST)	                                            BOH_COST,
                          sum(BOH_ADJ_QTY)	                                        BOH_ADJ_QTY,
                          sum(BOH_ADJ_SELLING)	                                    BOH_ADJ_SELLING,
                          sum(BOH_ADJ_COST)	                                        BOH_ADJ_COST,
                          sum(FD_NUM_AVAIL_DAYS)	                                  FD_NUM_AVAIL_DAYS,
                          sum(FD_NUM_AVAIL_DAYS_ADJ)	                              FD_NUM_AVAIL_DAYS_ADJ,
                          sum(FD_NUM_CATLG_DAYS)                                  	FD_NUM_CATLG_DAYS,
                          sum(FD_NUM_CATLG_DAYS_ADJ)	                              FD_NUM_CATLG_DAYS_ADJ,
                          sum(PRODUCT_STATUS_CODE)	                                PRODUCT_STATUS_CODE,
                          sum(WK_DELIVERY_PATTERN)                                	WK_DELIVERY_PATTERN,
                          sum(NUM_UNITS_PER_TRAY)	                                  NUM_UNITS_PER_TRAY,
                          sum(FD_NUM_DC_CATLG_DAYS)	                                FD_NUM_DC_CATLG_DAYS,
                          sum(FD_NUM_DC_CATLG_ADJ_DAYS)                           	FD_NUM_DC_CATLG_ADJ_DAYS,
                          sum(FD_NUM_DC_AVAIL_DAYS)	                                FD_NUM_DC_AVAIL_DAYS,
                          sum(FD_NUM_DC_AVAIL_ADJ_DAYS)	                            FD_NUM_DC_AVAIL_ADJ_DAYS,
                          sum(FD_NUM_CATLG_WK)	                                    FD_NUM_CATLG_WK,
                          sum(FD_NUM_CUST_AVAIL_ADJ)	                              FD_NUM_CUST_AVAIL_ADJ,
                          sum(NUM_FACINGS)	                                        NUM_FACINGS
    
                  from   ( 
                          select  /*+ parallel(4) */ 
                                  fin_year_no, 
                                  fin_week_no,  
                                  sk1_location_no,
                                  SK1_ITEM_NO,
                                  
                                  SALES_QTY,
                                  SALES,
                                  SALES_COST,
                                  SALES_MARGIN,
                                  REG_SALES_QTY,
                                  REG_SALES,
                                  REG_SALES_COST,
                                  REG_SALES_MARGIN,
                                  SDN_IN_QTY,
                                  SDN_IN_SELLING,
                                  SDN_IN_COST,
                                  ACTL_STORE_RCPT_QTY,
                                  ACTL_STORE_RCPT_SELLING,
                                  ACTL_STORE_RCPT_COST,
                                  
                                  0	                                                PROM_SALES_QTY,
                                  0	                                                PROM_SALES,
                                  0	                                                PROM_SALES_COST,
                                  0	                                                PROM_SALES_MARGIN,
                                  0	                                                WASTE_QTY,
                                  0	                                                WASTE_SELLING,
                                  0	                                                WASTE_COST,
                                  0	                                                SHRINK_QTY,
                                  0	                                                SHRINK_SELLING,
                                  0	                                                SHRINK_COST,
                                  0	                                                GAIN_QTY,
                                  0	                                                GAIN_SELLING,
                                  0	                                                GAIN_COST,
                                  0	                                                GRN_QTY,
                                  0	                                                GRN_SELLING,
                                  0	                                                GRN_COST,
                                  0	                                                GRN_MARGIN,
                                  0	                                                RTV_QTY,
                                  0	                                                RTV_SELLING,
                                  0	                                                RTV_COST,
                                  0	                                                SDN_OUT_QTY,
                                  0	                                                SDN_OUT_SELLING,
                                  0	                                                SDN_OUT_COST,
                                  0	                                                DC_DELIVERED_QTY,
                                  0	                                                DC_DELIVERED_SELLING,
                                  0	                                                DC_DELIVERED_COST,
                                  0	                                                WASTE_RECOV_QTY,
                                  0                                               	WASTE_RECOV_REVENUE,
                                  0	                                                SPEC_DEPT_QTY,
                                  0	                                                SPEC_DEPT_REVENUE,
                                  0	                                                SPEC_DEPT_WASTE_RECOV_QTY,
                                  0	                                                SPEC_DEPT_WASTE_RECOV_REVENUE,
                                  0	                                                SIT_QTY,
                                  0	                                                SIT_SELLING,
                                  0	                                                SIT_COST,
                                  0	                                                SIT_MARGIN,
                                  0	                                                SOH_QTY,
                                  0	                                                SOH_SELLING,
                                  0	                                                SOH_COST,
                                  0	                                                SOH_MARGIN,
                                  0	                                                BOH_QTY,
                                  0	                                                BOH_SELLING,
                                  0	                                                BOH_COST,
                                  0                                               	BOH_ADJ_QTY,
                                  0	                                                BOH_ADJ_SELLING,
                                  0	                                                BOH_ADJ_COST,
                                  0	                                                FD_NUM_AVAIL_DAYS,
                                  0	                                                FD_NUM_AVAIL_DAYS_ADJ,
                                  0	                                                FD_NUM_CATLG_DAYS,
                                  0	                                                FD_NUM_CATLG_DAYS_ADJ,
                                  0	                                                PRODUCT_STATUS_CODE,
                                  0	                                                WK_DELIVERY_PATTERN,
                                  0	                                                NUM_UNITS_PER_TRAY,
                                  0	                                                FD_NUM_DC_CATLG_DAYS,
                                  0                                               	FD_NUM_DC_CATLG_ADJ_DAYS,
                                  0                                               	FD_NUM_DC_AVAIL_DAYS,
                                  0	                                                FD_NUM_DC_AVAIL_ADJ_DAYS,
                                  0	                                                FD_NUM_CATLG_WK,
                                  0	                                                FD_NUM_CUST_AVAIL_ADJ,
                                  0                                                 NUM_FACINGS
                          from    DNS  
                          union all
                          select  /*+ parallel(4) */ 
                                  fin_year_no, 
                                  fin_week_no, 
                                  sk1_location_no,
                                  SK1_ITEM_NO,
                                  
                                  0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                                  PROM_SALES_QTY,
                                  PROM_SALES,
                                  PROM_SALES_COST,
                                  PROM_SALES_MARGIN,
                                  WASTE_QTY,
                                  WASTE_SELLING,
                                  WASTE_COST,
                                  SHRINK_QTY,
                                  SHRINK_SELLING,
                                  SHRINK_COST,
                                  GAIN_QTY,
                                  GAIN_SELLING,
                                  GAIN_COST,
                                  GRN_QTY,
                                  GRN_SELLING,
                                  GRN_COST,
                                  GRN_MARGIN,
                                  RTV_QTY,
                                  RTV_SELLING,
                                  RTV_COST,
                                  SDN_OUT_QTY,
                                  SDN_OUT_SELLING,
                                  SDN_OUT_COST,
                                  DC_DELIVERED_QTY,
                                  DC_DELIVERED_SELLING,
                                  DC_DELIVERED_COST,
                                  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
                          from    SPRS 
                          union all
                          select  /*+ parallel(4) */ 
                                  fin_year_no, 
                                  fin_week_no, 
                                  sk1_location_no,
                                  SK1_ITEM_NO,
                                  
                                  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                                  SIT_QTY,
                                  SIT_SELLING,
                                  SIT_COST,
                                  SIT_MARGIN,
                                  SOH_QTY,
                                  SOH_SELLING,
                                  SOH_COST,
                                  SOH_MARGIN,
                                  BOH_QTY,
                                  BOH_SELLING,
                                  BOH_COST,
                                  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0  
                          from    Stock
                          union all
                          select  /*+ parallel(4) */ 
                                  fin_year_no, 
                                  fin_week_no, 
                                  sk1_location_no,
                                  SK1_ITEM_NO,
                                  
                                  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                                  WASTE_RECOV_QTY,
                                  WASTE_RECOV_REVENUE,
                                  SPEC_DEPT_QTY,
                                  SPEC_DEPT_REVENUE,
                                  SPEC_DEPT_WASTE_RECOV_QTY,
                                  SPEC_DEPT_WASTE_RECOV_REVENUE,
                                  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
                          from    POSJV 
                          union all
                          select  /*+ parallel(4) */ 
                                  fin_year_no, 
                                  fin_week_no, 
                                  sk1_location_no,
                                  SK1_ITEM_NO,
                                  
                                  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                                  BOH_ADJ_QTY,
                                  BOH_ADJ_SELLING,
                                  BOH_ADJ_COST,
                                  FD_NUM_AVAIL_DAYS,
                                  FD_NUM_AVAIL_DAYS_ADJ,
                                  FD_NUM_CATLG_DAYS,
                                  FD_NUM_CATLG_DAYS_ADJ,
                                  PRODUCT_STATUS_CODE,
                                  WK_DELIVERY_PATTERN,
                                  NUM_UNITS_PER_TRAY,
                                  FD_NUM_DC_CATLG_DAYS,
                                  FD_NUM_DC_CATLG_ADJ_DAYS,
                                  FD_NUM_DC_AVAIL_DAYS,
                                  FD_NUM_DC_AVAIL_ADJ_DAYS,
                                  FD_NUM_CATLG_WK,
                                  FD_NUM_CUST_AVAIL_ADJ,
                                  NUM_FACINGS
                          from    CTLG
                         )   a 
                  join   Itm b on (a.SK1_ITEM_NO = b.SK1_ITEM_NO) 
                  group by
                          fin_year_no, 
                          fin_week_no,  
                          a.sk1_location_no,
                          a.SK1_ITEM_NO,
                          b.SK1_BUSINESS_UNIT_NO,
                          b.SK1_DEPARTMENT_NO
                )
        
    select  a.FIN_YEAR_NO,
            a.FIN_WEEK_NO,
    --        d.FIN_MONTH_NO,
            a.SK1_LOCATION_NO,
            a.SK1_ITEM_NO,
            a.SK1_BUSINESS_UNIT_NO,
            a.SK1_DEPARTMENT_NO,
            b.SK1_REGION_NO,
            b.WH_FD_ZONE_NO,
            
            SALES_QTY,
            SALES,
            SALES_COST,
            SALES_MARGIN,
            REG_SALES_QTY,
            REG_SALES,
            REG_SALES_COST,
            REG_SALES_MARGIN,
            SDN_IN_QTY,
            SDN_IN_SELLING,
            SDN_IN_COST,
            ACTL_STORE_RCPT_QTY,
            ACTL_STORE_RCPT_SELLING,
            ACTL_STORE_RCPT_COST,
            PROM_SALES_QTY,
            PROM_SALES,
            PROM_SALES_COST,
            PROM_SALES_MARGIN,
            WASTE_QTY,
            WASTE_SELLING,
            WASTE_COST,
            SHRINK_QTY,
            SHRINK_SELLING,
            SHRINK_COST,
            GAIN_QTY,
            GAIN_SELLING,
            GAIN_COST,
            GRN_QTY,
            GRN_SELLING,
            GRN_COST,
            GRN_MARGIN,
            RTV_QTY,
            RTV_SELLING,
            RTV_COST,
            SDN_OUT_QTY,
            SDN_OUT_SELLING,
            SDN_OUT_COST,
            DC_DELIVERED_QTY,
            DC_DELIVERED_SELLING,
            DC_DELIVERED_COST,
            WASTE_RECOV_QTY,
            WASTE_RECOV_REVENUE,
            SPEC_DEPT_QTY,
            SPEC_DEPT_REVENUE,
            SPEC_DEPT_WASTE_RECOV_QTY,
            SPEC_DEPT_WASTE_RECOV_REVENUE,
            SIT_QTY,
            SIT_SELLING,
            SIT_COST,
            SIT_MARGIN,
            SOH_QTY,
            SOH_SELLING,
            SOH_COST,
            SOH_MARGIN,
            BOH_QTY,
            BOH_SELLING,
            BOH_COST,
            BOH_ADJ_QTY,
            BOH_ADJ_SELLING,
            BOH_ADJ_COST,
            FD_NUM_AVAIL_DAYS,
            FD_NUM_AVAIL_DAYS_ADJ,
            FD_NUM_CATLG_DAYS,
            FD_NUM_CATLG_DAYS_ADJ,
            PRODUCT_STATUS_CODE,
            WK_DELIVERY_PATTERN,
            NUM_UNITS_PER_TRAY,
            FD_NUM_DC_CATLG_DAYS,
            FD_NUM_DC_CATLG_ADJ_DAYS,
            FD_NUM_DC_AVAIL_DAYS,
            FD_NUM_DC_AVAIL_ADJ_DAYS,
            FD_NUM_CATLG_WK,
            FD_NUM_CUST_AVAIL_ADJ,
            NUM_FACINGS,
            sysdate
    from    BaseA  a
    join    loc    b on (a.SK1_LOCATION_NO = b.SK1_LOCATION_NO);
    
    lrows := sql%rowcount;

    dbms_output.put_line('updates: ' || lrows);
    commit;
    
end "QVEXTRACT";
/