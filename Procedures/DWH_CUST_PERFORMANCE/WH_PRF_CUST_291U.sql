--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_291U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_291U" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        May 2016
--  Author:      Theo Filander
--  Purpose:     Create SubGroup Involvement Scores for the current month using Grouph and Breath Tables
--  Tables:      Input  - cust_db_subgroup_month
--                      - dim_calendar
--                      - temp_cust_sgrp_item_ranking
--                      - temp_cust_sgrp_sale_ranking
--               Output - cust_db_subgroup_month_involve
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  22 June 2016 - Mariska M - Optimised Code
--  JUNE 2017 FIX BUG  DONT DROP IF ZEROS 
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_deleted       integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  10000;


g_date                date         := trunc(sysdate);
g_year_no             number;
g_month_no            number;
g_prev_year_no        number;
g_prev_month_no       number;
g_trunc_year_no       number;
g_trunc_month_no      number;
g_run_day             number;
g_trunc_mn_date       date;
g_prev_mn_date        date;
g_run_date            date;
g_this_mn_end_date    date;
g_stmt                varchar(500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_291U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST_DB_SUBGROUP_MONTH_INVOLVE EX CUST_DB_SUBGROUP_MONTH';
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
    
    l_text := 'LOAD OF CUST_DB_SUBGROUP_MONTH_INVOLVE EX CUST_DB_SUBGROUP_MONTH STARTED AT '||
    to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  


--************************************************************************************************** 
-- Look up processing dates from dim_calendar   
--**************************************************************************************************    
    select fin_year_no,fin_month_no, this_mn_start_date, this_mn_end_date
      into g_year_no, g_month_no, g_prev_mn_date, g_this_mn_end_date
      from dim_calendar 
     where calendar_date in (select this_mn_start_date-1 
                               from dim_calendar 
                              where calendar_date = trunc(sysdate));
                            
    select fin_year_no,fin_month_no,this_mn_start_date
      into g_prev_year_no,g_prev_month_no,g_trunc_mn_date
      from dim_calendar 
     where calendar_date in (select this_mn_start_date-1 
                               from dim_calendar 
                              where calendar_date = trunc(g_prev_mn_date));  
       
    --determine the partition to be truncated after loading the new month                       
    select fin_year_no,fin_month_no
      into g_trunc_year_no,g_trunc_month_no
      from dim_calendar 
     where calendar_date in (select this_mn_start_date-1 
                               from dim_calendar 
                              where calendar_date = trunc(g_trunc_mn_date));  
    
--**************************************************************************************************
-- Determine if this is a day on which we process
--**************************************************************************************************  
   g_run_date := g_this_mn_end_date + 9;
   if trunc(sysdate) <> g_run_date then
      l_text := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := dwh_constants.vc_log_draw_line;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text :=  ' ';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;  
   
   l_text := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   l_text := 'CUST_DB_SUBGROUP_MONTH data being processed is Financial Year  : '|| g_year_no|| ' Month  ' || g_month_no; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
   
   l_text := 'CUST_DB_SUBGROUP_MONTH_INVOLVE data being rolled is Financial Year  '|| g_prev_year_no|| ' Month  ' || g_prev_month_no; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
            
    select count(*)
      into g_recs_read
      from cust_db_subgroup_month_involve
     where fin_year_no  = g_year_no
       and fin_month_no = g_month_no;

    execute immediate 'alter session enable parallel dml';
    
    l_text := 'TRUNCATE TEMP TABLES'; 
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    g_stmt := 'TRUNCATE table DWH_CUST_PERFORMANCE.TEMP_CUST_SGRP_SALE_RANKING';
    execute immediate g_stmt; 
    
    g_stmt := 'TRUNCATE table DWH_CUST_PERFORMANCE.TEMP_CUST_SGRP_ITEM_RANKING';
    execute immediate g_stmt;
    
--   Dont perform Update stats after truncate
--    l_text := 'UPDATE STATS ON ALL TEMP TABLES';   --Not required to perform update stats on an empty table.
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
--    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SGRP_SALE_RANKING',estimate_percent=>1, DEGREE => 32);
--    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SGRP_ITEM_RANKING',estimate_percent=>1, DEGREE => 32);
--    COMMIT;
    
    l_text := 'Populate Sale Ranking for CUST_DB_SUBGROUP_MONTH_INVOLVE. ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    INSERT INTO DWH_CUST_PERFORMANCE.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        a.fin_year_no,
                        a.fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn01 + num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11+num_item_yr1_mn12) item_sum, 
                        ROUND(sales_yr1_mn01 + sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06+
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12) sales_sum
                    FROM dwh_cust_performance.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn01 + num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                          num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11+num_item_yr1_mn12) > 0 AND 
                          (sales_yr1_mn01 + sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06+
                          sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12) > 0)
                 ),
         customer_cnt AS 
                 (SELECT /*+ PARALLEL(cust,8) FULL(cust) */
                         fin_year_no,
                         fin_month_no,
                         subgroup_no,
                         COUNT(primary_customer_identifier) customer_cnt
                    FROM cust
                   GROUP BY fin_year_no, fin_month_no, subgroup_no
                 ),
         customer_item_cnt AS 
                 (SELECT /*+ PARALLEL(c,8) PARALLEL(d,8) FULL(c) FULL(d) */
                         c.fin_year_no,
                         c.fin_month_no, 
                         c.subgroup_no,
                         c.sales_sum,
                         d.customer_cnt,
                         COUNT(c.primary_customer_identifier) customer_item_cnt
                    FROM cust c
                   INNER JOIN customer_cnt d
                      ON c.fin_year_no = d.fin_year_no AND 
                         c.fin_month_no = d.fin_month_no AND 
                         c.subgroup_no = d.subgroup_no
                   GROUP BY c.fin_year_no,c.fin_month_no,c.subgroup_no,c.sales_sum,d.customer_cnt
                 ),
         customer_item_ranking AS 
                 (SELECT /*+ PARALLEL(a,8) FULL(a) */
                         a.fin_year_no,
                         a.fin_month_no,
                         a.subgroup_no,
                         a.sales_sum,
                         a.customer_cnt,
                         a.customer_item_cnt,
                         SUM(a.customer_item_cnt) OVER (PARTITION BY a.fin_year_no, a.fin_month_no, a.subgroup_no ORDER BY a.fin_year_no, a.fin_month_no, a.subgroup_no, a.sales_sum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - a.customer_item_cnt AS diff_customer_item_cnt
                    FROM customer_item_cnt a 
                 )  
    SELECT /*+ PARALLEL(a,8) FULL(a) */
           fin_year_no,
           fin_month_no,
           subgroup_no,
           sales_sum,
           customer_item_cnt,
           customer_cnt,
           diff_customer_item_cnt,
           FLOOR((((diff_customer_item_cnt+(0.5*customer_item_cnt))/customer_cnt)*10)) AS ranking,
           SYSDATE last_updated_date
      FROM customer_item_ranking a;
  
    commit;
    
    l_text := 'Populate Item Ranking for CUST_DB_SUBGROUP_MONTH_INVOLVE. ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    INSERT INTO DWH_CUST_PERFORMANCE.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        a.fin_year_no,
                        a.fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn01 + num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11+num_item_yr1_mn12) item_sum, 
                        ROUND(sales_yr1_mn01 + sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06+
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12) sales_sum
                    FROM dwh_cust_performance.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn01 + num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                          num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11+num_item_yr1_mn12) > 0 AND 
                          (sales_yr1_mn01 + sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06+
                          sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12) > 0)
                 ),
         customer_cnt AS 
                 (SELECT /*+ PARALLEL(cust,8) FULL(cust) */
                         fin_year_no,
                         fin_month_no,
                         subgroup_no,
                         COUNT(primary_customer_identifier) customer_cnt
                    FROM cust
                   GROUP BY fin_year_no, fin_month_no, subgroup_no
                 ),
         customer_item_cnt AS 
                 (SELECT /*+ PARALLEL(c,8) PARALLEL(d,8) FULL(c) FULL(d) */
                         c.fin_year_no,
                         c.fin_month_no, 
                         c.subgroup_no,
                         c.item_sum,
                         d.customer_cnt,
                         COUNT(c.primary_customer_identifier) customer_item_cnt
                    FROM cust c
                   INNER JOIN customer_cnt d
                      ON c.fin_year_no = d.fin_year_no AND 
                         c.fin_month_no = d.fin_month_no AND 
                         c.subgroup_no = d.subgroup_no
                   GROUP BY c.fin_year_no,c.fin_month_no,c.subgroup_no,c.item_sum,d.customer_cnt
                 ),
         customer_item_ranking AS 
                 (SELECT /*+ PARALLEL(a,8) FULL(a) */
                         a.fin_year_no,
                         a.fin_month_no,
                         a.subgroup_no,
                         a.item_sum,
                         a.customer_cnt,
                         a.customer_item_cnt,
                         SUM(a.customer_item_cnt) OVER (PARTITION BY a.fin_year_no, a.fin_month_no, a.subgroup_no ORDER BY a.fin_year_no, a.fin_month_no, a.subgroup_no, a.item_sum ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - a.customer_item_cnt AS diff_customer_item_cnt
                    FROM customer_item_cnt a 
                 )   
    SELECT /*+ PARALLEL(a,8) FULL(a) */
           fin_year_no,
           fin_month_no,
           subgroup_no,
           item_sum,
           customer_item_cnt a_value,
           customer_cnt b_value,
           diff_customer_item_cnt c_value,
           FLOOR((((diff_customer_item_cnt+(0.5*customer_item_cnt))/customer_cnt)*10)) AS ranking,
           SYSDATE last_updated_date
      FROM customer_item_ranking a;
  
    commit;

    l_text := 'UPDATE STATS ON ALL TEMP TABLES';   --Required here as data has now been loaded into these tables.
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SGRP_SALE_RANKING',estimate_percent=>1, DEGREE => 32);
    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SGRP_ITEM_RANKING',estimate_percent=>1, DEGREE => 32);
    COMMIT;

    g_stmt := 'ALTER TABLE dwh_cust_performance.cust_db_subgroup_month_involve TRUNCATE SUBPARTITION FOR ('||g_year_no||','||g_month_no||')';
    l_text := g_stmt;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate g_stmt;
    
    l_text := 'Populate CUST_DB_SUBGROUP_MONTH_INVOLVE. ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--************************************************************************************************** 
-- Execute the insert
--**************************************************************************************************  
    insert /*+ APPEND Parallel(a,8) */ into dwh_cust_performance.cust_db_subgroup_month_involve a
    select /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               cma.fin_year_no,
               cma.fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               cma.num_item_yr1_mn01 + cma.num_item_yr1_mn02 + cma.num_item_yr1_mn03 + cma.num_item_yr1_mn04 + cma.num_item_yr1_mn05 + cma.num_item_yr1_mn06 + 
               cma.num_item_yr1_mn07 + cma.num_item_yr1_mn08 + cma.num_item_yr1_mn09 + cma.num_item_yr1_mn10 + cma.num_item_yr1_mn11 + cma.num_item_yr1_mn12 num_item_sum_yr1_mn01,
               cma.sales_yr1_mn01 + cma.sales_yr1_mn02 + cma.sales_yr1_mn03 + cma.sales_yr1_mn04 + cma.sales_yr1_mn05 + cma.sales_yr1_mn06 +
               cma.sales_yr1_mn07 + cma.sales_yr1_mn08 + cma.sales_yr1_mn09 + cma.sales_yr1_mn10 + cma.sales_yr1_mn11 + cma.sales_yr1_mn12 sales_sum_yr1_mn01,
               case
                    when cma.sales_yr1_mn01 + cma.sales_yr1_mn02 + cma.sales_yr1_mn03 + cma.sales_yr1_mn04 + cma.sales_yr1_mn05 + cma.sales_yr1_mn06 +
                         cma.sales_yr1_mn07 + cma.sales_yr1_mn08 + cma.sales_yr1_mn09 + cma.sales_yr1_mn10 + cma.sales_yr1_mn11 + cma.sales_yr1_mn12 <= 0 then 0
                    when cmb.ranking >= 9 and cmc.ranking >= 7 then 3
                    when cmb.ranking >= 8 and cmc.ranking >= 8 then 3
                    when cmb.ranking >= 7 and cmc.ranking >= 9 then 3
                    when cmb.ranking >= 9 and cmc.ranking >= 0 then 2
                    when cmb.ranking >= 8 and cmc.ranking >= 0 then 2
                    when cmb.ranking >= 7 and cmc.ranking >= 1 then 2
                    when cmb.ranking >= 6 and cmc.ranking >= 2 then 2
                    when cmb.ranking >= 5 and cmc.ranking >= 3 then 2
                    when cmb.ranking >= 4 and cmc.ranking >= 4 then 2
                    when cmb.ranking >= 3 and cmc.ranking >= 5 then 2
                    when cmb.ranking >= 2 and cmc.ranking >= 6 then 2
                    when cmb.ranking >= 1 and cmc.ranking >= 7 then 2
                    else 1
               end involvement_score_yr1_mn01,
               cmd.num_item_sum_yr1_mn01      num_item_sum_yr1_mn02,
               cmd.sales_sum_yr1_mn01         sales_sum_yr1_mn02,
               cmd.involvement_score_yr1_mn01 involvement_score_yr1_mn02,
               cmd.num_item_sum_yr1_mn02      num_item_sum_yr1_mn03,
               cmd.sales_sum_yr1_mn02         sales_sum_yr1_mn03,
               cmd.involvement_score_yr1_mn02 involvement_score_yr1_mn03,
               cmd.num_item_sum_yr1_mn03      num_item_sum_yr1_mn04,
               cmd.sales_sum_yr1_mn03         sales_sum_yr1_mn04,
               cmd.involvement_score_yr1_mn03 involvement_score_yr1_mn04,
               cmd.num_item_sum_yr1_mn04      num_item_sum_yr1_mn05,
               cmd.sales_sum_yr1_mn04         sales_sum_yr1_mn05,
               cmd.involvement_score_yr1_mn04 involvement_score_yr1_mn05,
               cmd.num_item_sum_yr1_mn05      num_item_sum_yr1_mn06,
               cmd.sales_sum_yr1_mn05         sales_sum_yr1_mn06,
               cmd.involvement_score_yr1_mn05 involvement_score_yr1_mn06,
               cmd.num_item_sum_yr1_mn06      num_item_sum_yr1_mn07,
               cmd.sales_sum_yr1_mn06         sales_sum_yr1_mn07,
               cmd.involvement_score_yr1_mn06 involvement_score_yr1_mn07,
               cmd.num_item_sum_yr1_mn07      num_item_sum_yr1_mn08,
               cmd.sales_sum_yr1_mn07         sales_sum_yr1_mn08,
               cmd.involvement_score_yr1_mn07 involvement_score_yr1_mn08,
               cmd.num_item_sum_yr1_mn08      num_item_sum_yr1_mn09,
               cmd.sales_sum_yr1_mn08         sales_sum_yr1_mn09,
               cmd.involvement_score_yr1_mn08 involvement_score_yr1_mn09,
               cmd.num_item_sum_yr1_mn09      num_item_sum_yr1_mn10,
               cmd.sales_sum_yr1_mn09         sales_sum_yr1_mn10,
               cmd.involvement_score_yr1_mn09 involvement_score_yr1_mn10,
               cmd.num_item_sum_yr1_mn10      num_item_sum_yr1_mn11,
               cmd.sales_sum_yr1_mn10         sales_sum_yr1_mn11,
               cmd.involvement_score_yr1_mn10 involvement_score_yr1_mn11,
               cmd.num_item_sum_yr1_mn11      num_item_sum_yr1_mn12,
               cmd.sales_sum_yr1_mn11         sales_sum_yr1_mn12,
               cmd.involvement_score_yr1_mn11 involvement_score_yr1_mn12,
               cmd.num_item_sum_yr1_mn12      num_item_sum_yr2_mn01,
               cmd.sales_sum_yr1_mn12         sales_sum_yr2_mn01,
               cmd.involvement_score_yr1_mn12 involvement_score_yr2_mn01,
               cmd.num_item_sum_yr2_mn01      num_item_sum_yr2_mn02,
               cmd.sales_sum_yr2_mn01         sales_sum_yr2_mn02,
               cmd.involvement_score_yr2_mn01 involvement_score_yr2_mn02,
               cmd.num_item_sum_yr2_mn02      num_item_sum_yr2_mn03,
               cmd.sales_sum_yr2_mn02         sales_sum_yr2_mn03,
               cmd.involvement_score_yr2_mn02 involvement_score_yr2_mn03,
               cmd.num_item_sum_yr2_mn03      num_item_sum_yr2_mn04,
               cmd.sales_sum_yr2_mn03         sales_sum_yr2_mn04,
               cmd.involvement_score_yr2_mn03 involvement_score_yr2_mn04,
               cmd.num_item_sum_yr2_mn04      num_item_sum_yr2_mn05,
               cmd.sales_sum_yr2_mn04         sales_sum_yr2_mn05,
               cmd.involvement_score_yr2_mn04 involvement_score_yr2_mn05,
               cmd.num_item_sum_yr2_mn05      num_item_sum_yr2_mn06,
               cmd.sales_sum_yr2_mn05         sales_sum_yr2_mn06,
               cmd.involvement_score_yr2_mn05 involvement_score_yr2_mn06,
               cmd.num_item_sum_yr2_mn06      num_item_sum_yr2_mn07,
               cmd.sales_sum_yr2_mn06         sales_sum_yr2_mn07,
               cmd.involvement_score_yr2_mn06 involvement_score_yr2_mn07,
               cmd.num_item_sum_yr2_mn07      num_item_sum_yr2_mn08,
               cmd.sales_sum_yr2_mn07         sales_sum_yr2_mn08,
               cmd.involvement_score_yr2_mn07 involvement_score_yr2_mn08,
               cmd.num_item_sum_yr2_mn08      num_item_sum_yr2_mn09,
               cmd.sales_sum_yr2_mn08         sales_sum_yr2_mn09,
               cmd.involvement_score_yr2_mn08 involvement_score_yr2_mn09,
               cmd.num_item_sum_yr2_mn09      num_item_sum_yr2_mn10,
               cmd.sales_sum_yr2_mn09         sales_sum_yr2_mn10,
               cmd.involvement_score_yr2_mn09 involvement_score_yr2_mn10,
               cmd.num_item_sum_yr2_mn10      num_item_sum_yr2_mn11,
               cmd.sales_sum_yr2_mn10         sales_sum_yr2_mn11,
               cmd.involvement_score_yr2_mn10 involvement_score_yr2_mn11,
               cmd.num_item_sum_yr2_mn11      num_item_sum_yr2_mn12,
               cmd.sales_sum_yr2_mn11         sales_sum_yr2_mn12,
               cmd.involvement_score_yr2_mn11 involvement_score_yr2_mn12,
               cmd.num_item_sum_yr2_mn12      num_item_sum_yr3_mn01,
               cmd.sales_sum_yr2_mn12         sales_sum_yr3_mn01,
               cmd.involvement_score_yr2_mn12 involvement_score_yr3_mn01,
               cmd.num_item_sum_yr3_mn01      num_item_sum_yr3_mn02,
               cmd.sales_sum_yr3_mn01         sales_sum_yr3_mn02,
               cmd.involvement_score_yr3_mn01 involvement_score_yr3_mn02,
               cmd.num_item_sum_yr3_mn02      num_item_sum_yr3_mn03,
               cmd.sales_sum_yr3_mn02         sales_sum_yr3_mn03,
               cmd.involvement_score_yr3_mn02 involvement_score_yr3_mn03,
               cmd.num_item_sum_yr3_mn03      num_item_sum_yr3_mn04,
               cmd.sales_sum_yr3_mn03         sales_sum_yr3_mn04,
               cmd.involvement_score_yr3_mn03 involvement_score_yr3_mn04,
               cmd.num_item_sum_yr3_mn04      num_item_sum_yr3_mn05,
               cmd.sales_sum_yr3_mn04         sales_sum_yr3_mn05,
               cmd.involvement_score_yr3_mn04 involvement_score_yr3_mn05,
               cmd.num_item_sum_yr3_mn05      num_item_sum_yr3_mn06,
               cmd.sales_sum_yr3_mn05         sales_sum_yr3_mn06,
               cmd.involvement_score_yr3_mn05 involvement_score_yr3_mn06,
               cmd.num_item_sum_yr3_mn06      num_item_sum_yr3_mn07,
               cmd.sales_sum_yr3_mn06         sales_sum_yr3_mn07,
               cmd.involvement_score_yr3_mn06 involvement_score_yr3_mn07,
               cmd.num_item_sum_yr3_mn07      num_item_sum_yr3_mn08,
               cmd.sales_sum_yr3_mn07         sales_sum_yr3_mn08,
               cmd.involvement_score_yr3_mn07 involvement_score_yr3_mn08,
               cmd.num_item_sum_yr3_mn08      num_item_sum_yr3_mn09,
               cmd.sales_sum_yr3_mn08         sales_sum_yr3_mn09,
               cmd.involvement_score_yr3_mn08 involvement_score_yr3_mn09,
               cmd.num_item_sum_yr3_mn09      num_item_sum_yr3_mn10,
               cmd.sales_sum_yr3_mn09         sales_sum_yr3_mn10,
               cmd.involvement_score_yr3_mn09 involvement_score_yr3_mn10,
               cmd.num_item_sum_yr3_mn10      num_item_sum_yr3_mn11,
               cmd.sales_sum_yr3_mn10         sales_sum_yr3_mn11,
               cmd.involvement_score_yr3_mn10 involvement_score_yr3_mn11,
               cmd.num_item_sum_yr3_mn11      num_item_sum_yr3_mn12,
               cmd.sales_sum_yr3_mn11         sales_sum_yr3_mn12,
               cmd.involvement_score_yr3_mn11 involvement_score_yr3_mn12,
               trunc(sysdate) last_updated_date
          from dwh_cust_performance.cust_db_subgroup_month cma 
               left join
               dwh_cust_performance.temp_cust_sgrp_item_ranking cmb on cma.num_item_yr1_mn01 + 
                                                                            cma.num_item_yr1_mn02 + 
                                                                            cma.num_item_yr1_mn03 + 
                                                                            cma.num_item_yr1_mn04 + 
                                                                            cma.num_item_yr1_mn05 + 
                                                                            cma.num_item_yr1_mn06 +
                                                                            cma.num_item_yr1_mn07 + 
                                                                            cma.num_item_yr1_mn08 + 
                                                                            cma.num_item_yr1_mn09 + 
                                                                            cma.num_item_yr1_mn10 + 
                                                                            cma.num_item_yr1_mn11 + 
                                                                            cma.num_item_yr1_mn12 = cmb.item_sum AND
                                                                        cma.fin_year_no = cmb.fin_year_no AND
                                                                        cma.fin_month_no = cmb.fin_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               dwh_cust_performance.temp_cust_sgrp_sale_ranking cmc on ROUND(cma.sales_yr1_mn01 + 
                                                                             cma.sales_yr1_mn02 +
                                                                             cma.sales_yr1_mn03 +
                                                                             cma.sales_yr1_mn04 + 
                                                                             cma.sales_yr1_mn05 + 
                                                                             cma.sales_yr1_mn06 +
                                                                             cma.sales_yr1_mn07 + 
                                                                             cma.sales_yr1_mn08 + 
                                                                             cma.sales_yr1_mn09 + 
                                                                             cma.sales_yr1_mn10 + 
                                                                             cma.sales_yr1_mn11 + 
                                                                             cma.sales_yr1_mn12) = cmc.sales_sum AND
                                                                        cma.fin_year_no = cmc.fin_year_no AND
                                                                        cma.fin_month_no = cmc.fin_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               dwh_cust_performance.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                          cma.subgroup_no = cmd.subgroup_no and 
                                                                          cmd.fin_year_no  = g_prev_year_no and 
                                                                          cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_year_no
           and cma.fin_month_no = g_month_no
           and cma.primary_customer_identifier <> 998
--           and ((cma.num_item_yr1_mn01 + cma.num_item_yr1_mn02 + cma.num_item_yr1_mn03 + cma.num_item_yr1_mn04 + cma.num_item_yr1_mn05 + cma.num_item_yr1_mn06 +
--                 cma.num_item_yr1_mn07 + cma.num_item_yr1_mn08 + cma.num_item_yr1_mn09 + cma.num_item_yr1_mn10 + cma.num_item_yr1_mn11 + cma.num_item_yr1_mn12) > 0 AND 
--                (cma.sales_yr1_mn01 + cma.sales_yr1_mn02 + cma.sales_yr1_mn03 + cma.sales_yr1_mn04 + cma.sales_yr1_mn05 + cma.sales_yr1_mn06+
--                 cma.sales_yr1_mn07 + cma.sales_yr1_mn08 + cma.sales_yr1_mn09 + cma.sales_yr1_mn10 + cma.sales_yr1_mn11 + cma.sales_yr1_mn12) > 0)
;

    g_recs_updated :=  SQL%ROWCOUNT;
    
    commit;
    
    g_stmt := 'ALTER TABLE dwh_cust_performance.cust_db_subgroup_month_involve TRUNCATE SUBPARTITION FOR ('||g_trunc_year_no||','||g_trunc_month_no||')';
    l_text := g_stmt;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate g_stmt;
    
--   l_text := 'UPDATE STATS ON CUST_DB_SUBGROUP_MONTH_INVOLVE TABLES'; 
--   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_DB_SUBGROUP_MONTH_INVOLVE',estimate_percent=>1, DEGREE => 32);

   commit;
           
           
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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
       rollback;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := false;
       raise;

end WH_PRF_CUST_291U;
