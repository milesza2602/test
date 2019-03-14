-- ****** Object: Procedure W7131037.WH_PRF_CUST_291U_HST2 Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_291U_HST2" (p_forall_limit in integer,p_success out boolean) AS
  g_start_year_no       NUMBER;
  g_start_month_no      NUMBER;
  g_year_no             NUMBER;
  g_month_no            NUMBER;
  g_curr_mn_end_date    DATE;
  g_prev_mn_end_date    DATE;
  g_prev_year_no        NUMBER;
  g_prev_month_no       NUMBER;
  g_stmt                VARCHAR(500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_291U_HST2';
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

SELECT fin_year_no,fin_month_no
  INTO g_start_year_no,g_start_month_no
  FROM dim_calendar
 WHERE calendar_date IN (SELECT this_mn_start_date - 1
                           FROM dim_calendar
                          WHERE calendar_date = trunc(SYSDATE));

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (TRUNC(SYSDATE - 395));

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12) num_item_sum_yr1_mn01,
               (sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12) sales_sum_yr1_mn01,
               case
                    when (sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                          sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn10 + sales_yr2_mn12) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                                                                        num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                                                                             sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                 num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12) > 0 AND
                (sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                 sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11) num_item_sum_yr1_mn01,
               (sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                          sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                                                                        num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                                                                             sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11) > 0 AND
                (sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10) num_item_sum_yr1_mn01,
               (sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn11 + sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                          sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn11 + num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                                                                        num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn11 + sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                                                                             sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10) > 0 AND
                (sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09) num_item_sum_yr1_mn01,
               (sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                          sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                                                                        num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                                                                             sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09) > 0 AND
                (sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07 + num_item_yr2_mn08) num_item_sum_yr1_mn01,
               (sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07 + sales_yr2_mn08) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                          sales_yr2_mn07 + sales_yr2_mn08) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                                                                        num_item_yr2_mn07 + num_item_yr2_mn08) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                                                                             sales_yr2_mn07 + sales_yr2_mn08) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07 + num_item_yr2_mn08) > 0 AND
                (sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07 + sales_yr2_mn08) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07) num_item_sum_yr1_mn01,
               (sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                          sales_yr2_mn07) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                                                                        num_item_yr2_mn07) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                                                                             sales_yr2_mn07) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 +
                num_item_yr2_mn07) > 0 AND
                (sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 +
                sales_yr2_mn07) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06) num_item_sum_yr1_mn01,
               (sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06) > 0 AND
                (sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn06 +
                num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05) num_item_sum_yr1_mn01,
               (sales_yr1_mn06 +
                sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn06 +
                          sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn06 +
                                                                        num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn06 +
                                                                             sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn06 +
                num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05) > 0 AND
                (sales_yr1_mn06 +
                sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn05 + num_item_yr1_mn06 +
                num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04) num_item_sum_yr1_mn01,
               (sales_yr1_mn05 + sales_yr1_mn06 +
                sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn05 + sales_yr1_mn06 +
                          sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn05 + num_item_yr1_mn06 +
                                                                        num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn05 + sales_yr1_mn06 +
                                                                             sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn05 + num_item_yr1_mn06 +
                num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04) > 0 AND
                (sales_yr1_mn05 + sales_yr1_mn06 +
                sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 +
                num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03) num_item_sum_yr1_mn01,
               (sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                          sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 +
                                                                        num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                                                                             sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 +
                num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03) > 0 AND
                (sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

l_text := 'Year '||g_year_no||' Month'||g_month_no;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert /*+ APPEND Parallel(a,12) */ into W7131037.cust_db_subgroup_month_involve a
select  /*+ Parallel(cma,8) Parallel(cmb,8) Parallel(cmc,8) Parallel(cmd,8) Full(cma) Full(cmb) Full(cmc) Full(cmd) */
               distinct
               g_year_no fin_year_no,
               g_month_no fin_month_no,
               cma.primary_customer_identifier,
               cma.subgroup_no,
               cma.customer_no,
               (num_item_yr1_mn03 + num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 +
                num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02) num_item_sum_yr1_mn01,
               (sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02) sales_sum_yr1_mn01,
               case
                    when (sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                          sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                          sales_yr2_mn01 + sales_yr2_mn02) <= 0 then 0
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
          from W7131037.cust_db_subgroup_month cma
               left join
               W7131037.temp_cust_sgrp_item_ranking cmb on (num_item_yr1_mn03 + num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 +
                                                                        num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                                                                        num_item_yr2_mn01 + num_item_yr2_mn02) = cmb.item_sum AND
                                                                        cmb.fin_year_no = g_year_no AND
                                                                        cmb.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmb.subgroup_no
               left join
               W7131037.temp_cust_sgrp_sale_ranking cmc on ROUND(sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                                                                             sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                                                                             sales_yr2_mn01 + sales_yr2_mn02) = cmc.sales_sum AND
                                                                        cmc.fin_year_no = g_year_no AND
                                                                        cmc.fin_month_no = g_month_no AND
                                                                        cma.subgroup_no = cmc.subgroup_no
               left join
               W7131037.cust_db_subgroup_month_involve cmd on cma.primary_customer_identifier = cmd.primary_customer_identifier AND
                                                                      cma.subgroup_no = cmd.subgroup_no and
                                                                      cmd.fin_year_no  = g_prev_year_no and
                                                                      cmd.fin_month_no = g_prev_month_no
         where cma.fin_year_no  = g_start_year_no
           and cma.fin_month_no = g_start_month_no
           and cma.primary_customer_identifier <> 998
           and ((num_item_yr1_mn03 + num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 +
                num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 +
                num_item_yr2_mn01 + num_item_yr2_mn02) > 0 AND
                (sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 +
                sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 +
                sales_yr2_mn01 + sales_yr2_mn02) > 0);
    COMMIT;

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

-- 2016 12

SELECT fin_year_no,fin_month_no, this_mn_start_date - 1 prev_mn_end_dt, this_mn_end_date curr_mn_end_dt
  INTO g_year_no,g_month_no,g_prev_mn_end_date,g_curr_mn_end_date
  FROM dim_calendar
 WHERE calendar_date IN (g_curr_mn_end_date + 1);

SELECT fin_year_no,fin_month_no
  INTO g_prev_year_no,g_prev_month_no
  FROM dim_calendar
 WHERE calendar_date IN (g_prev_mn_end_date);

-- 2017 01

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
END "WH_PRF_CUST_291U_HST2";
