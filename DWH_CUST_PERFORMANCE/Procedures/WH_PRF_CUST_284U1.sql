--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_284U1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_284U1" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        Dec 2015
--  Author:      Alastair de Wet
--  Purpose:    Generate THE BASE LIFESTYLE SEGMENT TABLE FOR FOODS 
--  Tables:      Input  - cust_basket_item  
--               Output - temp_lss1_pci_pfc_fd
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

g_run_date           date ;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

g_stmt                varchar(500); 

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_284U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE LIFE STYLE SEGMENT BASE TABLE FOR FOODS ';
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
    
    l_text := 'LOAD OF temp_lss1_pci_pfc_fd EX cust_basket_item STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
     dwh_lookup.dim_control(g_date);
 
     g_date := '1 feb 2014';
--********************************** 
     l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
     
     select last_yr_fin_year_no, last_mn_fin_month_no
     into g_year_no, g_month_no
     from dim_control;
     
     g_year_no := 2014;
     g_month_no := 07;
--*************************************

--************************************************************************************************** 
-- DETERMINE WHEN JOB RUNS   
--**************************************************************************************************     
    select   max(fin_week_end_date)
    into     g_run_date
    from     dim_calendar
    where    fin_year_no  =  g_year_no  and
             fin_month_no =  g_month_no;
   
   g_run_date := g_run_date + 7;
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
--      return;
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

    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_FD';
    execute immediate g_stmt;  
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_PFC_FD';
    execute immediate g_stmt;
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_FINAL_FD';
    execute immediate g_stmt;
    


    insert /*+ APPEND parallel (tmp,16) */ into temp_lss1_pci_fd tmp
    SELECT /*+ FULL(DI) FULL(BI) parallel (BI,16) */
           BI.PRIMARY_CUSTOMER_IDENTIFIER, 
           COUNT(DISTINCT LOCATION_NO||TRAN_NO||TILL_NO||TRAN_DATE) PCI_VISITS,
           SUM(ITEM_TRAN_SELLING - DISCOUNT_SELLING) PCI_VALUE
    FROM   CUST_BASKET_ITEM BI, 
           DIM_ITEM_CUST_LSS DI
    WHERE  TRAN_DATE BETWEEN g_date - 365 and g_date AND   --- Should allow for a full year in Foods
           BI.ITEM_NO = DI.ITEM_NO AND
           BI.PRIMARY_CUSTOMER_IDENTIFIER <> 998 AND
           BI.PRIMARY_CUSTOMER_IDENTIFIER <> 0 AND
           BI.TRAN_TYPE in ('S','V','R','Q') AND
           DI.BUSINESS_UNIT_NO IN (50)
    GROUP BY  BI.PRIMARY_CUSTOMER_IDENTIFIER
    HAVING COUNT(DISTINCT LOCATION_NO||TRAN_NO||TILL_NO||TRAN_DATE) > 3 and
           SUM(ITEM_TRAN_SELLING - DISCOUNT_SELLING) > 0;
        
    g_recs_read     :=  g_recs_read+SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted+SQL%ROWCOUNT;
    
    commit;
    
    l_text := 'UPDATE STATS ON TEMP TABLES'; 
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 

    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_LSS1_PCI_FD',estimate_percent=>1, DEGREE => 32);
 

    COMMIT;
--************************************************************************************************************************
   l_text      := 'PROCESS TEMP TABLE CREATED IN THE 1ST STEP TO CREATE FINAL VIEW FOR CALCS';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
--************************************************************************************************************************   

-- CREATE A TEMP TABLE WITH PCI/PFC'S THAT NEED TO BE CALCULATED IN NEXT PROCESS    
    insert  /*+ APPEND parallel(tmp,16) */ into dwh_cust_performance.temp_lss1_pci_pfc_fd tmp
    SELECT  /*+ FULL(DI) FULL(BI) FULL(SC) parallel (BI,16) */ 
            BI.PRIMARY_CUSTOMER_IDENTIFIER, 
            DI.PRODUCT_FAMILY_CODE, 
            SUM(ITEM_TRAN_SELLING - DISCOUNT_SELLING) PCI_VALUE
    FROM    CUST_BASKET_ITEM BI, 
            DIM_ITEM_CUST_LSS DI,
            TEMP_LSS1_PCI_FD SC 
    WHERE   TRAN_DATE BETWEEN G_DATE - 365 AND G_DATE AND     --- Should allow for a full year in Foods
            BI.ITEM_NO = DI.ITEM_NO AND
            BI.PRIMARY_CUSTOMER_IDENTIFIER = SC.PRIMARY_CUSTOMER_IDENTIFIER  AND
            BI.TRAN_TYPE in ('S','V','R','Q') AND
            DI.BUSINESS_UNIT_NO IN (50) AND
            DI.PRODUCT_FAMILY_CODE IS NOT NULL
    GROUP BY  BI.PRIMARY_CUSTOMER_IDENTIFIER, DI.PRODUCT_FAMILY_CODE
                 ;
                 
    g_recs_OTHER     :=  g_recs_OTHER+SQL%ROWCOUNT;
    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_LSS1_PCI_PFC_FD',estimate_percent=>1, DEGREE => 32);
 

    COMMIT;

--************************************************************************************************************************    
   l_text      := 'CALCULATIONS';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--************************************************************************************************************************ 
    INSERT  /*+ APPEND parallel(tmp,8) */ INTO DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_FINAL_FD TMP
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
            SUM(PCI_VALUE  * PURCH_THEME_15) FTR_15
    FROM    TEMP_LSS1_PCI_PFC_FD SC,
            DIM_LSS_PURCHASE_THEME PT
    WHERE   SC.PRODUCT_FAMILY_CODE =  PT.PRODUCT_FAMILY_CODE AND
            PT.SEGMENTATION_TYPE   =  'Foods' AND
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

-- 15 and 17 removed as they are no longer active - refer Louis P            
            LEAST (FTR_01,FTR_02,FTR_03,FTR_04,FTR_05,FTR_06,FTR_07,FTR_08,FTR_09,FTR_10,
                   FTR_11,FTR_12,FTR_13,FTR_14,FTR_15) 
            AS MIN_FTR,
            FTR_01+FTR_02+FTR_03+FTR_04+FTR_05+FTR_06+FTR_07+FTR_08+FTR_09+FTR_10+
            FTR_11+FTR_12+FTR_13+FTR_14+FTR_15 
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
-- 15 and 17 removed as they are no longer active - refer Louis P             
            (FTR_01+FTR_02+FTR_03+FTR_04+FTR_05+FTR_06+FTR_07+FTR_08+FTR_09+FTR_10+
            FTR_11+FTR_12+FTR_13+FTR_14+FTR_15) - (MIN_FTR * 15)
            TOT_FTR
    FROM    CALC1 C1               
                  ),
         CALC3 AS (
    SELECT  /*+ FULL(C2)  parallel (8) */ 
            PRIMARY_CUSTOMER_IDENTIFIER, 
            ((FTR_01 / TOT_FTR) - 0.0808242762) / 0.0376120416 AS FTR_01,
            ((FTR_02 / TOT_FTR) - 0.087423765)  / 0.0411245872 AS FTR_02,
            ((FTR_03 / TOT_FTR) - 0.0876068179) / 0.041229201  AS FTR_03,
            ((FTR_04 / TOT_FTR) - 0.0450873353) / 0.0407123067 AS FTR_04,
            ((FTR_05 / TOT_FTR) - 0.0884163198) / 0.0415916549 AS FTR_05,
            ((FTR_06 / TOT_FTR) - 0.0553721493) / 0.0377201651 AS FTR_06,
            ((FTR_07 / TOT_FTR) - 0.0665206902) / 0.0401410148 AS FTR_07,
            ((FTR_08 / TOT_FTR) - 0.0387182831) / 0.0382918825 AS FTR_08,
            ((FTR_09 / TOT_FTR) - 0.0595667941) / 0.0318676181 AS FTR_09,
            ((FTR_10 / TOT_FTR) - 0.0757549042) / 0.0457344619 AS FTR_10,
            ((FTR_11 / TOT_FTR) - 0.0691835283) / 0.0406106494 AS FTR_11,
            ((FTR_12 / TOT_FTR) - 0.0502510666) / 0.0448271668 AS FTR_12,
            ((FTR_13 / TOT_FTR) - 0.0680884939) / 0.03912133   AS FTR_13,
            ((FTR_14 / TOT_FTR) - 0.0642490165) / 0.0295644414 AS FTR_14,
            ((FTR_15 / TOT_FTR) - 0.0629365597) / 0.0748680541 AS FTR_15 
    FROM    CALC2 C2                
                  ) 
-- 15 and 17 removed as they are no longer active - refer Louis P                    
    SELECT  /*+ FULL(C3)  parallel (8) */
            PRIMARY_CUSTOMER_IDENTIFIER, 
            FTR_01,FTR_02,FTR_03,FTR_04,FTR_05,FTR_06,FTR_07,FTR_08,FTR_09,
            FTR_10,FTR_11,FTR_12,FTR_13,FTR_14,FTR_15
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
            POWER((MEAN_11 - SCORE_11),2) + POWER((MEAN_12 - SCORE_12),2) + POWER((MEAN_13 - SCORE_13),2) + POWER((MEAN_14 - SCORE_14),2) + POWER((MEAN_15 - SCORE_15),2) as  DISTANCE
    FROM    DWH_CUST_PERFORMANCE.TEMP_LSS1_PCI_FINAL_FD FIN,
            DWH_CUST_PERFORMANCE.DIM_LSS_CLUSTER_CENTRE CC
    WHERE   'Foods' = CC.SEGMENT_TYPE  
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
               'Foods',
               case
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL6,PS.CL7,PS.CL8) = PS.CL1 then 01
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL6,PS.CL7,PS.CL8) = PS.CL2 then 02
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL6,PS.CL7,PS.CL8) = PS.CL3 then 03
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL6,PS.CL7,PS.CL8) = PS.CL4 then 04
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL6,PS.CL7,PS.CL8) = PS.CL6 then 06
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL6,PS.CL7,PS.CL8) = PS.CL7 then 07
                 when LEAST (PS.CL1,PS.CL2,PS.CL3,PS.CL4,PS.CL6,PS.CL7,PS.CL8) = PS.CL8 then 08
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

END WH_PRF_CUST_284U1;
