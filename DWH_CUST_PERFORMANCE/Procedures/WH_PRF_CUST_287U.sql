--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_287U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_287U" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        Dec 2015
--  Author:      Alastair de Wet
--  Purpose:    Generate THE BASE LIFESTYLE SEGMENT TABLE FOR NON_FOODS 
--  Tables:      Input  - cust_basket_item  
--               Output - temp_lss1_pci_pfc_ch
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--   
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_other         integer       :=  0;
g_forall_limit       integer       :=  10000;

g_total_customers    integer       :=  0;
g_product_family_code varchar(30)  ;

g_year_no            integer       :=  0;
g_month_no           integer       :=  0;

g_run_date           date  ;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

g_stmt                varchar(500); 

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_287U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE LIFE STYLE SEGMENT BASE TABLE FOR C&H ';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


              
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;  
    p_success := false;    
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF temp_lss1_pci_pfc_ch EX cust_basket_item STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
     dwh_lookup.dim_control(g_date);
     l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
     
     select last_yr_fin_year_no, last_mn_fin_month_no
     into g_year_no, g_month_no
     from dim_control;

--************************************************************************************************** 
-- DETERMINE WHEN JOB RUNS   
--**************************************************************************************************     
    select   max(fin_week_end_date)
    into     g_run_date
    from     dim_calendar
    where    fin_year_no  =  g_year_no  and
             fin_month_no =  g_month_no;
   
   g_run_date := g_run_date + 8;
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;  
   
   l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--************************************************************************************************** 
-- Main processing
--**************************************************************************************************
    l_text      := 'START OF PROCESS TO ELIMINATE CUSTOMERS THAT DO NOT NEED PROCESSING AND WRITE TO TEMP TABLE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
--***********************************************************************************************************   
    execute immediate 'alter session enable parallel dml'; 

    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_CH';
    execute immediate g_stmt;  
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_PFC_CH';
    execute immediate g_stmt;
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_FINAL_CH';
    execute immediate g_stmt;
    


    insert /*+ APPEND parallel (tmp,16) */ into temp_lss1_pci_ch tmp
    SELECT /*+ FULL(DI) FULL(BI) parallel (BI,16) */
           BI.PRIMARY_CUSTOMER_IDENTIFIER, 
           COUNT(DISTINCT LOCATION_NO||TRAN_NO||TILL_NO||TRAN_DATE) PCI_VISITS,
           SUM(ITEM_TRAN_SELLING - DISCOUNT_SELLING) PCI_VALUE
    FROM   CUST_BASKET_ITEM BI, 
           DIM_ITEM_CUST_LSS DI
    WHERE  TRAN_DATE BETWEEN g_date - 730 and g_date AND   --- Should allow for a full year in Foods
           BI.ITEM_NO = DI.ITEM_NO AND
           BI.PRIMARY_CUSTOMER_IDENTIFIER <> 998 AND
           BI.PRIMARY_CUSTOMER_IDENTIFIER <> 0 AND
           BI.TRAN_TYPE in ('S','V','R','Q') AND
           DI.BUSINESS_UNIT_NO IN (51,52,54,55)
    GROUP BY  BI.PRIMARY_CUSTOMER_IDENTIFIER
    HAVING COUNT(DISTINCT LOCATION_NO||TRAN_NO||TILL_NO||TRAN_DATE) > 2 and
           SUM(ITEM_TRAN_SELLING - DISCOUNT_SELLING) > 0;
        
    g_recs_read     :=  g_recs_read+SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted+SQL%ROWCOUNT;
    
    commit;
    
    l_text := 'UPDATE STATS ON TEMP TABLES'; 
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 

    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_LSS1_PCI_CH',estimate_percent=>1, DEGREE => 32);
 

    COMMIT;
--************************************************************************************************************************
   l_text      := 'PROCESS TEMP TABLE CREATED IN THE 1ST STEP TO CREATE FINAL VIEW FOR CALCS';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
--************************************************************************************************************************   

-- CREATE A TEMP TABLE WITH PCI/PFC'S THAT NEED TO BE CALCULATED IN NEXT PROCESS    
    insert  /*+ APPEND parallel(tmp,16) */ into dwh_cust_performance.temp_lss1_pci_pfc_ch tmp
    SELECT  /*+ FULL(DI) FULL(BI) FULL(SC) parallel (BI,16) */ 
            BI.PRIMARY_CUSTOMER_IDENTIFIER, 
            DI.PRODUCT_FAMILY_CODE, 
            SUM(ITEM_TRAN_SELLING - DISCOUNT_SELLING) PCI_VALUE
    FROM    CUST_BASKET_ITEM BI, 
            DIM_ITEM_CUST_LSS DI,
            TEMP_LSS1_PCI_CH SC 
    WHERE   TRAN_DATE BETWEEN G_DATE - 730 AND G_DATE AND     --- Should allow for a full year in Foods
            BI.ITEM_NO = DI.ITEM_NO AND
            BI.PRIMARY_CUSTOMER_IDENTIFIER = SC.PRIMARY_CUSTOMER_IDENTIFIER  AND
            BI.TRAN_TYPE in ('S','V','R','Q') AND
            DI.BUSINESS_UNIT_NO IN (51,52,54,55) AND
            DI.PRODUCT_FAMILY_CODE IS NOT NULL
    GROUP BY  BI.PRIMARY_CUSTOMER_IDENTIFIER, DI.PRODUCT_FAMILY_CODE
                 ;
                 
    g_recs_OTHER     :=  g_recs_OTHER+SQL%ROWCOUNT;
    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_LSS1_PCI_PFC_CH',estimate_percent=>1, DEGREE => 32);
 

    COMMIT;

--************************************************************************************************************************    
   l_text      := 'CALCULATIONS';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--************************************************************************************************************************ 
    INSERT  /*+ APPEND parallel(tmp,8) */ INTO DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_FINAL_CH TMP
    WITH PTHEME AS (
    SELECT  /*+ FULL(PT)FULL(SC) parallel (8) */ 
            SC.PRIMARY_CUSTOMER_IDENTIFIER, 
            SUM(PCI_VALUE  * PURCH_THEME_01) FTR_01,
            SUM(PCI_VALUE  * PURCH_THEME_02) FTR_02,
            SUM(PCI_VALUE  * PURCH_THEME_03) FTR_03,
            SUM(PCI_VALUE  * PURCH_THEME_04) FTR_04,
            SUM(PCI_VALUE  * PURCH_THEME_05) FTR_05,
            SUM(PCI_VALUE  * PURCH_THEME_06) FTR_06,
            SUM(PCI_VALUE  * PURCH_THEME_07) FTR_07,
            SUM(PCI_VALUE  * PURCH_THEME_08) FTR_08,
            SUM(PCI_VALUE  * PURCH_THEME_09) FTR_09,
            SUM(PCI_VALUE  * PURCH_THEME_10) FTR_10,
            SUM(PCI_VALUE  * PURCH_THEME_11) FTR_11,
            SUM(PCI_VALUE  * PURCH_THEME_12) FTR_12,
            SUM(PCI_VALUE  * PURCH_THEME_13) FTR_13,
            SUM(PCI_VALUE  * PURCH_THEME_14) FTR_14,
            SUM(PCI_VALUE  * PURCH_THEME_15) FTR_15,
            SUM(PCI_VALUE  * PURCH_THEME_16) FTR_16,
            SUM(PCI_VALUE  * PURCH_THEME_17) FTR_17,
            SUM(PCI_VALUE  * PURCH_THEME_18) FTR_18,
            SUM(PCI_VALUE  * PURCH_THEME_19) FTR_19 
    FROM    TEMP_LSS1_PCI_PFC_CH SC,
            DIM_LSS_PURCHASE_THEME PT
    WHERE   SC.PRODUCT_FAMILY_CODE =  PT.PRODUCT_FAMILY_CODE AND
            PT.SEGMENTATION_TYPE   =  'Non-Foods' AND
            NVL(PCI_VALUE,0)       <> 0
    GROUP BY  SC.PRIMARY_CUSTOMER_IDENTIFIER
                  ),
         CALC1 AS (
    SELECT  /*+ FULL(PT)  parallel (8) */ 
            PT.PRIMARY_CUSTOMER_IDENTIFIER, 
            FTR_01,
            FTR_02,
            FTR_03,
            FTR_04,
            FTR_05,
            FTR_06,
            FTR_07,
            FTR_08,
            FTR_09,
            FTR_10,
            FTR_11,
            FTR_12,
            FTR_13,
            FTR_14,
            FTR_15,
            FTR_16,
            FTR_17,
            FTR_18,
            FTR_19,
-- 15 and 17 removed as they are no longer active - refer Louis P            
            LEAST (FTR_01,FTR_02,FTR_03,FTR_04,FTR_05,FTR_06,FTR_07,FTR_08,FTR_09,FTR_10,
                   FTR_11,FTR_12,FTR_13,FTR_14,FTR_16,FTR_18,FTR_19) 
            AS MIN_FTR,
            FTR_01+FTR_02+FTR_03+FTR_04+FTR_05+FTR_06+FTR_07+FTR_08+FTR_09+FTR_10+
            FTR_11+FTR_12+FTR_13+FTR_14+FTR_16+FTR_18+FTR_19 
            AS TOT_FTR
    FROM    PTHEME PT                
                  ),
         CALC2 AS (
    SELECT  /*+ FULL(C1)  parallel (8) */ 
            PRIMARY_CUSTOMER_IDENTIFIER, 
            FTR_01 - MIN_FTR FTR_01,
            FTR_02 - MIN_FTR FTR_02,
            FTR_03 - MIN_FTR FTR_03,
            FTR_04 - MIN_FTR FTR_04,
            FTR_05 - MIN_FTR FTR_05,
            FTR_06 - MIN_FTR FTR_06,
            FTR_07 - MIN_FTR FTR_07,
            FTR_08 - MIN_FTR FTR_08,
            FTR_09 - MIN_FTR FTR_09,
            FTR_10 - MIN_FTR FTR_10,
            FTR_11 - MIN_FTR FTR_11,
            FTR_12 - MIN_FTR FTR_12,
            FTR_13 - MIN_FTR FTR_13,
            FTR_14 - MIN_FTR FTR_14,
            FTR_15 - MIN_FTR FTR_15,
            FTR_16 - MIN_FTR FTR_16,
            FTR_17 - MIN_FTR FTR_17,
            FTR_18 - MIN_FTR FTR_18,
            FTR_19 - MIN_FTR FTR_19,
-- 15 and 17 removed as they are no longer active - refer Louis P             
            (FTR_01+FTR_02+FTR_03+FTR_04+FTR_05+FTR_06+FTR_07+FTR_08+FTR_09+FTR_10+
            FTR_11+FTR_12+FTR_13+FTR_14+FTR_16+FTR_18+FTR_19) - (MIN_FTR * 17)
            TOT_FTR
    FROM    CALC1 C1               
                  ),
         CALC3 AS (
    SELECT  /*+ FULL(C2)  parallel (8) */ 
            PRIMARY_CUSTOMER_IDENTIFIER, 
            ((FTR_01 / TOT_FTR) - 0.0807291623) / 0.1023067785 AS FTR_01,
            ((FTR_02 / TOT_FTR) - 0.0805845873) / 0.0787326462 AS FTR_02,
            ((FTR_03 / TOT_FTR) - 0.0479713836) / 0.0458429007 AS FTR_03,
            ((FTR_04 / TOT_FTR) - 0.0401004228) / 0.0534601964 AS FTR_04,
            ((FTR_05 / TOT_FTR) - 0.0394382159) / 0.0415916549 AS FTR_05,
            ((FTR_06 / TOT_FTR) - 0.0610445614) / 0.0624445905 AS FTR_06,
            ((FTR_07 / TOT_FTR) - 0.0436527659) / 0.0543008139 AS FTR_07,
            ((FTR_08 / TOT_FTR) - 0.061235096)  / 0.0688141315 AS FTR_08,
            ((FTR_09 / TOT_FTR) - 0.0786350784) / 0.0617820016 AS FTR_09,
            ((FTR_10 / TOT_FTR) - 0.0626808021) / 0.0429097003 AS FTR_10,
            ((FTR_11 / TOT_FTR) - 0.0607182077) / 0.0504562011 AS FTR_11,
            ((FTR_12 / TOT_FTR) - 0.0378883837) / 0.0371548417 AS FTR_12,
            ((FTR_13 / TOT_FTR) - 0.0653333023) / 0.0786039554 AS FTR_13,
            ((FTR_14 / TOT_FTR) - 0.0385557706) / 0.0391903926 AS FTR_14,
            ((FTR_15 / TOT_FTR) - 0.0444777265) / 0.0254756685 AS FTR_15,
            ((FTR_16 / TOT_FTR) - 0.0420111064) / 0.0521702231 AS FTR_16,
            ((FTR_17 / TOT_FTR) - 0.0312771954) / 0.0176035247 AS FTR_17,
            ((FTR_18 / TOT_FTR) - 0.0585443192) / 0.0837490029 AS FTR_18,
            ((FTR_19 / TOT_FTR) - 0.0251125999) / 0.0273578839 AS FTR_19 
    FROM    CALC2 C2                
                  ) 
-- 15 and 17 removed as they are no longer active - refer Louis P                    
    SELECT  /*+ FULL(C3)  parallel (8) */
            PRIMARY_CUSTOMER_IDENTIFIER, 
            FTR_01,FTR_02,FTR_03,FTR_04,FTR_05,FTR_06,FTR_07,FTR_08,FTR_09,
            FTR_10,FTR_11,FTR_12,FTR_13,FTR_14,0,FTR_16,0,FTR_18,FTR_19 
    FROM CALC3  C3            ;
    COMMIT;
--************************************************************************************************************************    
   l_text      := 'CLUSTER SCORES';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--************************************************************************************************************************     
   
    INSERT /*+ APPEND parallel(LSS,8) */ INTO CUST_LSS_LIFESTYLE_SEGMENTS LSS
    WITH CLUSTER_SCORE AS    (
    SELECT /*+ FULL(FIN)FULL(CC) parallel (8) */ 
            FIN.PRIMARY_CUSTOMER_IDENTIFIER,
            CC.CLUSTER_NO,
            POWER((MEAN_01 - SCORE_01),2) + POWER((MEAN_02 - SCORE_02),2) + POWER((MEAN_03 - SCORE_03),2) + POWER((MEAN_04 - SCORE_04),2) + POWER((MEAN_05 - SCORE_05),2) + 
            POWER((MEAN_06 - SCORE_06),2) + POWER((MEAN_07 - SCORE_07),2) + POWER((MEAN_08 - SCORE_08),2) + POWER((MEAN_09 - SCORE_09),2) + POWER((MEAN_10 - SCORE_10),2) + 
            POWER((MEAN_11 - SCORE_11),2) + POWER((MEAN_12 - SCORE_12),2) + POWER((MEAN_13 - SCORE_13),2) + POWER((MEAN_14 - SCORE_14),2) + POWER((MEAN_16 - SCORE_16),2) + 
            POWER((MEAN_18 - SCORE_18),2) + POWER((MEAN_19 - SCORE_19),2) as  DISTANCE
    FROM    DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_FINAL_CH FIN,
            DWH_CUST_PERFORMANCE.DIM_LSS_CLUSTER_CENTRE CC
    WHERE   'Non-Foods' = CC.SEGMENT_TYPE  
                               ), 
           PIVOT_SCORE AS      (
      SELECT /*+ parallel (8) */ * FROM (     
      SELECT   /*+ parallel (8) */
              CS.PRIMARY_CUSTOMER_IDENTIFIER,CS.DISTANCE,CS.CLUSTER_NO AS COL
      FROM    CLUSTER_SCORE CS
                )
      PIVOT     (
                SUM(DISTANCE)  
                FOR   COL IN (1 AS CL1,2 AS CL2,3 AS CL3,4 AS CL4,5 AS CL5,6 AS CL6,7 AS CL7,8 AS CL8,9 AS CL9)
                )    
                     )
       SELECT  /*+ parallel (8) */ 
               PS.PRIMARY_CUSTOMER_IDENTIFIER,
               g_year_no,
               g_month_no,
               'Non-Foods',
               case
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9) = PS.CL1 then 01
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9) = PS.CL2 then 02
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9) = PS.CL3 then 03
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9) = PS.CL4 then 04
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9) = PS.CL5 then 05
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9) = PS.CL6 then 06
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9) = PS.CL7 then 07
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9) = PS.CL8 then 08
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9) = PS.CL9 then 09
               end SEGMENT_NO,  
               PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL5,PS.CL6,PS.CL7,PS.CL8,PS.CL9,
               G_DATE
       FROM    PIVOT_SCORE PS
       ;                         
    

       COMMIT;

--************************************************************************************************** 
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS UPDATED TO ? '||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'OTHER RECORDS WRITTEN '||g_recs_other;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true; 
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;       
       raise;
       
      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_CUST_287U;
