-- ****** Object: Procedure W7131037.WH_PRF_CUST_291U_HST1 Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_291U_HST1" (p_forall_limit in integer,p_success out boolean) AS
  g_year_no             NUMBER;
  g_month_no            NUMBER;
  g_prev_mn_date        DATE;
  g_prev_year_no        NUMBER;
  g_prev_month_no       NUMBER;
  g_stmt                VARCHAR(500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_291U_HST1';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST_DB_SUBGROUP_MONTH_INVOLVE EX CUST_DB_SUBGROUP_MONTH';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;
BEGIN

EXECUTE IMMEDIATE 'alter session enable parallel dml';

l_text := dwh_cust_constants.vc_log_draw_line;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'LOAD OF CUST_DB_SUBGROUP_MONTH_INVOLVE EX CUST_DB_SUBGROUP_MONTH STARTED AT '||
to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

g_stmt := 'TRUNCATE TABLE W7131037.TEMP_CUST_SGRP_SALE_RANKING';
EXECUTE IMMEDIATE g_stmt;

g_stmt := 'TRUNCATE TABLE W7131037.TEMP_CUST_SGRP_ITEM_RANKING';
EXECUTE IMMEDIATE g_stmt;

DBMS_STATS.gather_table_stats ('W7131037','TEMP_CUST_SGRP_SALE_RANKING',estimate_percent=>1, DEGREE => 32);
DBMS_STATS.gather_table_stats ('W7131037','TEMP_CUST_SGRP_ITEM_RANKING',estimate_percent=>1, DEGREE => 32);
COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(SYSDATE));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

-- 2017 01

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

-- 2016 12

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn03 + num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02) item_sum,
                        ROUND(sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn03 + num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02) > 0 AND
                          (sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn03 + num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02) item_sum,
                        ROUND(sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn03 + num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02) > 0 AND
                          (sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02) > 0)
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
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03) item_sum,
                        ROUND(sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03) > 0 AND
                          (sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03) item_sum,
                        ROUND(sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn04+num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03) > 0 AND
                          (sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03) > 0)
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
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04) item_sum,
                        ROUND(sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04) > 0 AND
                          (sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04) item_sum,
                        ROUND(sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn05 + num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04) > 0 AND
                          (sales_yr1_mn05 + sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04) > 0)
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
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05) item_sum,
                        ROUND(sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05) > 0 AND
                          (sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05) item_sum,
                        ROUND(sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn06 +
                         num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05) > 0 AND
                          (sales_yr1_mn06 +
                              sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05) > 0)
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
    COMMIT;

-- 5
SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06) item_sum,
                        ROUND(sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06) > 0 AND
                          (sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06) item_sum,
                        ROUND(sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06) > 0 AND
                          (sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06) > 0)
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
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07) item_sum,
                        ROUND(sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07) > 0 AND
                          (sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07) item_sum,
                        ROUND(sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn08 + num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07) > 0 AND
                          (sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07) > 0)
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
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08) item_sum,
                        ROUND(sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08) > 0 AND
                          (sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08) item_sum,
                        ROUND(sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn09+num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08) > 0 AND
                          (sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08) > 0)
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
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09) item_sum,
                        ROUND(sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09) > 0 AND
                          (sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09) item_sum,
                        ROUND(sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09) > 0 AND
                          (sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09) > 0)
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
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10) item_sum,
                        ROUND(sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10) > 0 AND
                          (sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10) item_sum,
                        ROUND(sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn11 + num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10) > 0 AND
                          (sales_yr1_mn11 + sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10) > 0)
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
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11) item_sum,
                        ROUND(sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11) > 0 AND
                          (sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11) item_sum,
                        ROUND(sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr1_mn12 +
                         num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11) > 0 AND
                          (sales_yr1_mn12 +
                              sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11) > 0)
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
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date
  INTO g_year_no,g_month_no,g_prev_mn_date
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date-1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(g_prev_mn_date));

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_SALE_RANKING (fin_year_no,fin_month_no,subgroup_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12) item_sum,
                        ROUND(sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12) > 0 AND
                          (sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12) > 0)
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
    COMMIT;

    insert /*+ APPEND Parallel(a,8) */ into W7131037.TEMP_CUST_SGRP_ITEM_RANKING (fin_year_no,fin_month_no,subgroup_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH cust AS (SELECT /*+ PARALLEL(a,8) FULL(a) */
                        g_year_no fin_year_no,
                        g_month_no fin_month_no,
                        subgroup_no,
                        primary_customer_identifier,
                        (num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12) item_sum,
                        ROUND(sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12) sales_sum
                    FROM W7131037.cust_db_subgroup_month a
                   WHERE primary_customer_identifier <> 998
                     AND ((num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                         num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12) > 0 AND
                          (sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                              sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12) > 0)
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
    COMMIT;

    p_success := true;

l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text := dwh_constants.vc_log_draw_line;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
l_text :=  ' ';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
END "WH_PRF_CUST_291U_HST1";
