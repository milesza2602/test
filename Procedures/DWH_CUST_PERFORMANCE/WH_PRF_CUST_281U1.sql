--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_281U1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_281U1" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        Dec 2015
--  Author:      Alastair de Wet
--  Purpose:    Generate product family FOR FOODS for items that do not have one for Lifestyle Segmentation
--  Tables:      Input  - cust_basket_item  
--               Output - dim_item_cust_lss
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_281U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD PRODUCT FAMILY CODE FOR FOODS ';
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
    
    l_text := 'LOAD OF dim_item_cust_lss EX cust_basket_item STARTED AT '||
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
   
   g_run_date := g_run_date + 2;
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
    l_text      := 'START OF PROCESS TO ELIMINATE ITEMS THAT DO NOT NEED PROCESSING AND WRITE TO TEMP TABLE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
--***********************************************************************************************************   
    execute immediate 'alter session enable parallel dml'; 

    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS_PRODUCT_FAMILY_FD';
    execute immediate g_stmt;  
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS_FINAL_CALCS_FD';
    execute immediate g_stmt;
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS_PFC_EXCEPTIONS';
    execute immediate g_stmt;
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_LSS_PFC_NULL_FD';
    execute immediate g_stmt;
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.CUST_LSS_PFC_FEEDBACK';
    execute immediate g_stmt;
 
    insert /*+ APPEND parallel (tmp,12) */ into temp_lss_product_family_fd tmp
    with     filter_recs as ( 
    select /*+ FULL(cbi) FULL(di) parallel (cbi,12) */
             di.item_level1_no,
             count(distinct cbi.primary_customer_identifier) num_customers,
             min(cbi.tran_date)  min_date,
             max(cbi.tran_date)  max_date,
             max(cbi.tran_date) - min(cbi.tran_date) days_diff
    from     cust_basket_item cbi, 
             dim_item_cust_lss di
    where    cbi.tran_date between g_date - 365 and g_date   --** possibly make higher than 100 to ensure all products picked up **
    and      di.business_unit_no = 50
    and      di.product_family_code is null
    and      cbi.primary_customer_identifier <> 998 
    and      cbi.tran_type in ('S','V','R')
    and      cbi.primary_customer_identifier is not null  
    and      cbi.primary_customer_identifier <> 0   
    and      cbi.item_no   = di.item_no
    group by di.item_level1_no),
    
             ignore_recs as ( 
    select   /*+ FULL(fr)  parallel (fr,12) */ 
             item_level1_no 
    from     filter_recs fr
    where    days_diff     <= 56 or 
             num_customers <= 400)
             
    select   /*+ FULL(cbi) FULL(di) parallel (cbi,12) */
             cbi.primary_customer_identifier,
             di.item_level1_no,
             max(di.category_code) category_code,
             max(di.product_family_code) product_family_code,
             max( case  when di.product_family_code  is null    then di.item_level1_no else NULL  end) association_variable
    from     cust_basket_item cbi, 
             dim_item_cust_lss di 
    where    cbi.tran_date between g_date - 365  and g_date   --** change to 365 for live version **
    and      di.business_unit_no = 50
    and      cbi.primary_customer_identifier <> 998 
    and      cbi.tran_type in ('S','V','R')
    and      cbi.primary_customer_identifier is not null  
    and      cbi.primary_customer_identifier <> 0   
    and      cbi.item_no   = di.item_no
    and      di.item_level1_no not in 
            (select /*+ FULL(ir) parallel (12) */ item_level1_no from ignore_recs ir)
    group by cbi.primary_customer_identifier,
             di.item_level1_no ;
    
    g_recs_read     :=  g_recs_read+SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted+SQL%ROWCOUNT;
    
    commit;
    
    l_text := 'UPDATE STATS ON TEMP TABLES'; 
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 

    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_LSS_PRODUCT_FAMILY_FD',estimate_percent=>1, DEGREE => 32);
 

    COMMIT;
--************************************************************************************************************************
   l_text      := 'PROCESS TEMP TABLE CREATED IN THE 1ST STEP TO CREATE FINAL VIEW FOR CALCS - STEP 1 ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
--************************************************************************************************************************   
    select /*+ parallel(8) */ count(distinct primary_customer_identifier) 
    into   g_total_customers
    from   dwh_cust_performance.temp_lss_product_family_fd;

-- CREATE A TEMP TABLE WITH PFC'S THAT NEED TO BE CALCULATED AT CUST/ITEM_L1 LEVEL --   
    insert /*+ APPEND parallel(tmp,12) */ into dwh_cust_performance.temp_lss_pfc_null_fd tmp
    select /*+ FULL(pff2)   parallel(pff2,12) */
             pff2.primary_customer_identifier,
             pff2.item_level1_no 
    from     dwh_cust_performance.temp_lss_product_family_fd pff2 
    where    pff2.product_family_code is null;

    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_LSS_PFC_NULL_FD',estimate_percent=>1, DEGREE => 32);
 

    COMMIT;
--************************************************************************************************************************
   l_text      := 'PROCESS TEMP TABLE CREATED IN THE 2ND STEP TO CREATE FINAL VIEW FOR CALCS - STEP 2 ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
--************************************************************************************************************************       
    
    INSERT /*+ APPEND parallel(tmp,12) */ into dwh_cust_performance.temp_lss_final_calcs_fd tmp   
    with     assoc_var as ( 
    select /*+ FULL(pff)   parallel(pff,12) */
             pff.association_variable,
             count(distinct pff.primary_customer_identifier) num_customers 
    from     dwh_cust_performance.temp_lss_product_family_fd pff 
    where    pff.association_variable is not null
    group by pff.association_variable
                          ),
             prod_fam as  ( 
    select /*+ FULL(pff1)   parallel(pff1,12) */
             pff1.product_family_code,
             count(distinct pff1.primary_customer_identifier) num_customers 
    from     dwh_cust_performance.temp_lss_product_family_fd pff1 
    where    pff1.product_family_code is not null
    group by pff1.product_family_code
                          ) ,                   
             pfc as       ( 

--/*+  FULL(av) FULL(pf)  parallel(12) */  
-- BELOW SELECT IS THE BOTTLENECK AND TAKES THE MOST TIME --
-- MATCH UP AT CUSTOMER LEVEL THOSE IL1'S THAT HAVE A NULL PFC WITH ASSOCIATED PURCHASES WHERE THE PFC IS PRESENT --
    select   /*+  parallel(12) */  
             l1.item_level1_no,
             pff3.product_family_code,
             count(distinct pff3.primary_customer_identifier) together,
             MAX(pf.num_customers) product_family_total,
             g_total_customers customer_total,
             MAX(av.num_customers) item_level1_total
    from     temp_lss_pfc_null_fd l1,
--             dwh_cust_performance.temp_lss_product_family_fd l1, ** Alternative join method back to same table is slightly slower **
             dwh_cust_performance.temp_lss_product_family_fd pff3,
             assoc_var av,
             prod_fam pf 
    where    l1.primary_customer_identifier = pff3.primary_customer_identifier and
             pff3.product_family_code is not null and
--             l1.product_family_code is null and                   ** Used with alternative join method above **
             l1.item_level1_no = av.association_variable and
             pff3.product_family_code = pf.product_family_code
    group by l1.item_level1_no,
             pff3.product_family_code),

--YULE SCORE CALC --
             yule as         (              
    select   /*+ FULL(pf)   parallel(12) */   
             pf.item_level1_no,
             pf.product_family_code,
             pf.together,
             pf.product_family_total,
             pf.customer_total,
             pf.item_level1_total,
             case
             when 
              ((together * (customer_total - item_level1_total - product_family_total + together))  + 
              ((product_family_total - together) * (item_level1_total - together))) <> 0 
             then 
              ((together * (customer_total - item_level1_total - product_family_total + together))  - 
              ((product_family_total - together) * (item_level1_total - together)))              / 
              ((together * (customer_total - item_level1_total - product_family_total + together))  + 
              ((product_family_total - together) * (item_level1_total - together)))       
             else 
              -99 
             end yule_score,
             ''
    from     pfc pf),

-- KEEP ONLY MAXMUM YULE --
             maxyule as         ( 
    select   /*+ FULL(yl)   parallel(12) */
             item_level1_no,
             max(yule_score) max_yule_score
--             max(product_family_total) max_pft
    from     yule yl
    group by item_level1_no),
--added
              maxpft as (
    select   /*+ FULL(yle)  FULL(my) parallel(12) */
             my.item_level1_no,
             max(my.max_yule_score) as max_yule_score ,
             max(yle.product_family_total) as pftot
    from     maxyule my,
             yule yle
    where    yle.item_level1_no = my.item_level1_no
    and      yle.yule_score     = my.max_yule_score
    group by my.item_level1_no) 
--- end added
    select   /*+ FULL(yl1) FULL(my1)  parallel(12) */
             distinct yl1.*
    from     yule yl1,
             maxpft my1
    where    yl1.item_level1_no       = my1.item_level1_no
    and      yl1.yule_score           = my1.max_yule_score
    and      yl1.product_family_total = my1.pftot;
             
    commit;
--************************************************************************************************************************    
   l_text      := 'CALCULATE PREDOMINENT PFC';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--************************************************************************************************************************ 
    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_LSS_FINAL_CALCS_FD',estimate_percent=>1, DEGREE => 32);
    commit;

-- THE BELOW CODE CANNOT HANDLE WHEN TOTALLY NEW ITEMS ARE ADDED WHERE THE ENTIRE SUBCLASS HAS NULL PFC--
-- THAT WILL RESULT IN THE PFC REMAINING NULL --
--/*+ parallel(8) */
-- CALCULATE PREDOMINENT PFC AND PLACE ON TABLE --
       MERGE INTO  temp_lss_final_calcs_fd tmp USING
       (
       with iteml1 as (
       select  
              tmp.item_level1_no,
              tmp.product_family_code,
              di.department_no,
              di.class_no,
              di.subclass_no
      from    temp_lss_final_calcs_fd tmp,
              dim_item di
      where   tmp.item_level1_no = di.item_no  
                    ),       
          predom as (
       select   il1.item_level1_no,
                di.product_family_code,
                count(*) pfam_count 
       from     dim_item_cust_lss di,
                iteml1 il1
       where    di.department_no = il1.department_no 
       and      di.class_no      = il1.class_no 	
       and      di.subclass_no   = il1.subclass_no 
       and      di.product_family_code is not null
       group by il1.item_level1_no,
                di.product_family_code
                     ), 
          maxcnt as  (
       select   item_level1_no,
                max(pfam_count) max_count
       from     predom
       group by item_level1_no  
                      ) 
       select pd.item_level1_no,
              max(pd.product_family_code) product_family_code
       from   predom pd,
              maxcnt mc
       where  mc.max_count      = pd.pfam_count  
       and    mc.item_level1_no = pd.item_level1_no
       group by pd.item_level1_no
       ) mrg
       ON  (mrg.item_level1_no            = tmp.item_level1_no)
       WHEN MATCHED THEN
       update set   tmp.predominent_pfc   =  mrg.product_family_code ;      
 
       commit;
--************************************************************************************************************************    
   l_text      := 'WRITE EXCEPTIONS WHERE PFC NOT= PREDOMINENT PFC FOR THE SUBCLASS';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--************************************************************************************************************************  

       insert into temp_lss_pfc_exceptions 
       select tmp.item_level1_no,
              tmp.product_family_code,
              di.department_no,
              di.class_no,
              di.subclass_no,
              tmp.predominent_pfc
       from   temp_lss_final_calcs_fd tmp,
              dim_item di
       where  tmp.item_level1_no = di.item_no
       and    tmp.product_family_code <> NVL(tmp.predominent_pfc,0);
      
       g_recs_other :=  g_recs_other+SQL%ROWCOUNT;
      
       commit;

--************************************************************************************************************************    
   l_text      := 'WRITE PFC BACK TO ITEM MASTER';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--************************************************************************************************************************  

       MERGE INTO  DIM_ITEM_CUST_LSS di USING
       (
       select tmp.item_level1_no,
              tmp.product_family_code 
       from   temp_lss_final_calcs_fd tmp 
       where  tmp.product_family_code = tmp.predominent_pfc) mrg
       ON    (mrg.item_level1_no            = di.item_level1_no)
       WHEN MATCHED THEN
       update set   di.product_family_code   =  mrg.product_family_code ;      
       
       g_recs_updated :=  g_recs_updated+SQL%ROWCOUNT;
       
       commit;

--************************************************************************************************** 
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS UPDATED TO ITEM MASTER '||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'EXCEPTION RECORDS WRITTEN '||g_recs_other;
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

END WH_PRF_CUST_281U1;
