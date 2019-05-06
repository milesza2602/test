--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_294U_THEO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_294U_THEO" (p_forall_limit in integer,p_success out boolean) AS
--**************************************************************************************************
--  Date:        Aug 2017
--  Author:      Theo Filander
--  Purpose:     Create Company Involvement Scores History Update for the last 2 years.
--  Tables:      Input  - cust_db_business_unit_month;
--                      - dim_calendar
--                      - temp_cust_comp_item_ranking
--                      - temp_cust_comp_sale_ranking
--                      - temp_item_sales_comp_involve
--               Output - cust_db_company_month_involve
--  Packages:    constants, dwh_log, dwh_valid
--
--  Procedures: populate_item_rank, populate_sale_rank
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

  g_year_no             NUMBER;
  g_month_no            NUMBER;
  g_prev_year_no        NUMBER;
  g_prev_month_no       NUMBER;
  g_prev_mn_date        DATE;
  g_curr_year_no        NUMBER;
  g_curr_month_no       NUMBER;
  g_stmt                VARCHAR2(4000);
  g_stmt_main           CLOB;
  g_list_item           VARCHAR2(2000);
  g_list_sale           VARCHAR2(2000);
  g_loop                NUMBER DEFAULT 0;
  do_loop               NUMBER(3) DEFAULT 3;
  g_involve_column      VARCHAR2(30);
  g_item_column         VARCHAR2(30);
  g_sale_column         VARCHAR2(30);

  type array_t is table of varchar2(4000);
  array array_t := array_t(); -- Initialise it

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_294U_THEO';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST_DB_COMPANY_MONTH_INVOLVE EX CUST_DB_COMPANY_MONTH';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;



----------------------------------------------------------------------------------------------
--  Calculate the
--  
----------------------------------------------------------------------------------------------




PROCEDURE populate_item_rank (f_year_no         number,
                              f_month_no        number)
 AS

-- PRAGMA AUTONOMOUS_TRANSACTION;
    f_stmt  VARCHAR2(4000);
 BEGIN

--    dbms_output.put_line('PROCESSING populate_item_rank '||f_year_no||' - '||f_month_no);
    f_stmt := '
    insert /*+ Append Parallel(b,8) */ into dwh_cust_performance.temp_cust_comp_item_ranking b (fin_year_no,fin_month_no,company_no,item_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    with customer_cnt as 
                 (select /*+ Parallel(cust,8) Full(cust) */
                         fin_year_no,
                         fin_month_no,
                         company_no,
                         count(primary_customer_identifier) customer_cnt
                    from dwh_cust_performance.temp_item_sales_comp_involve
                   group by fin_year_no, fin_month_no, company_no
                 ),
         customer_item_cnt as 
                 (select /*+ Parallel(c,8) Parallel(d,8) Full(c) Full(d) */
                         c.fin_year_no,
                         c.fin_month_no, 
                         c.company_no,
                         c.item_sum,
                         d.customer_cnt,
                         count(c.primary_customer_identifier) customer_item_cnt
                    from dwh_cust_performance.temp_item_sales_comp_involve c
                   inner join customer_cnt d
                      on c.fin_year_no = d.fin_year_no and 
                         c.fin_month_no = d.fin_month_no and 
                         c.company_no = d.company_no
                   group by c.fin_year_no,c.fin_month_no,c.company_no,c.item_sum,d.customer_cnt
                 ),
         customer_item_ranking as 
                 (select /*+ Parallel(a,8) Full(a) */
                         a.fin_year_no,
                         a.fin_month_no,
                         a.company_no,
                         a.item_sum,
                         a.customer_cnt,
                         a.customer_item_cnt,
                         sum(a.customer_item_cnt) over (partition by a.fin_year_no, a.fin_month_no, a.company_no order by a.fin_year_no, a.fin_month_no, a.company_no, a.item_sum rows between unbounded preceding and current row) - a.customer_item_cnt as diff_customer_item_cnt
                    from customer_item_cnt a 
                 )   
    select /*+ Parallel(a,8) Full(a) */
           '||f_year_no||','||f_month_no||',
           company_no,
           item_sum,
           customer_item_cnt a_value,
           customer_cnt b_value,
           diff_customer_item_cnt c_value,
           floor((((diff_customer_item_cnt+(0.5*customer_item_cnt))/customer_cnt)*10)) as ranking,
           sysdate last_updated_date
      from customer_item_ranking a';

      execute immediate f_stmt;

    l_text := '   Item Ranking Records Inserted '|| SQL%ROWCOUNT;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    COMMIT;

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
END;

PROCEDURE populate_sale_rank (f_year_no         number,
                              f_month_no        number)
 AS

-- PRAGMA AUTONOMOUS_TRANSACTION;
 f_stmt     VARCHAR2(4000);

 BEGIN

--    dbms_output.put_line('PROCESSING populate_sale_rank '||f_year_no||' - '||f_month_no);
    f_stmt := '    
    insert /*+ Append Parallel(b,8) */ into dwh_cust_performance.temp_cust_comp_sale_ranking (fin_year_no,fin_month_no,company_no,sales_sum,customer_item_cnt,customer_cnt,diff_customer_item_cnt,ranking,last_updated_date)
    WITH customer_cnt AS 
                 (select /*+ Parallel(cust,8) Full(cust) */
                         fin_year_no,
                         fin_month_no,
                         company_no,
                         COUNT(primary_customer_identifier) customer_cnt
                    from dwh_cust_performance.temp_item_sales_comp_involve
                   group BY fin_year_no, fin_month_no,company_no
                 ),
         customer_item_cnt AS 
                 (select /*+ Parallel(c,8) Parallel(d,8) Full(c) Full(d) */
                         c.fin_year_no,
                         c.fin_month_no, 
                         c.company_no,
                         c.sales_sum,
                         d.customer_cnt,
                         count(c.primary_customer_identifier) customer_item_cnt
                    from dwh_cust_performance.temp_item_sales_comp_involve c
                   inner join customer_cnt d
                      on c.fin_year_no = d.fin_year_no and 
                         c.fin_month_no = d.fin_month_no and 
                         c.company_no = d.company_no
                   group by c.fin_year_no,c.fin_month_no,c.company_no,c.sales_sum,d.customer_cnt
                 ),
         customer_item_ranking as 
                 (select /*+ Parallel(a,8) Full(a) */
                         a.fin_year_no,
                         a.fin_month_no,
                         a.company_no,
                         a.sales_sum,
                         a.customer_cnt,
                         a.customer_item_cnt,
                         sum(a.customer_item_cnt) over (partition by a.fin_year_no, a.fin_month_no, a.company_no order by a.fin_year_no, a.fin_month_no, a.company_no, a.sales_sum rows between unbounded preceding and current row) - a.customer_item_cnt as diff_customer_item_cnt
                    from customer_item_cnt a 
                 )  
    select /*+ Parallel(a,8) Full(a) */
           '||f_year_no||','||f_month_no||',
           company_no,
           sales_sum,
           customer_item_cnt,
           customer_cnt,
           diff_customer_item_cnt,
           floor((((diff_customer_item_cnt+(0.5*customer_item_cnt))/customer_cnt)*10)) as ranking,
           sysdate last_updated_date
      from customer_item_ranking a';



    execute immediate f_stmt;

    l_text := '   Sales Ranking Records Inserted '|| SQL%ROWCOUNT;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    COMMIT;

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
    
END;

BEGIN

l_text := dwh_cust_constants.vc_log_draw_line;
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

l_text := 'LOAD OF CUST_DB_COMPANY_MONTH_INVOLVE EX CUST_DB_COMPANY_MONTH STARTED AT '||
to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

l_text := 'TRUNCATE TEMP TABLES'; 
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

g_stmt := 'truncate table dwh_cust_performance.temp_cust_comp_sale_ranking';
execute immediate g_stmt; 

g_stmt := 'truncate table dwh_cust_performance.temp_cust_comp_item_ranking';
execute immediate g_stmt;



COMMIT;

----------------------------------------------------------------------------------------------
--  Setup ranking for the respective months
--  Get the preceding Year and Months in decending order
----------------------------------------------------------------------------------------------
    for i in (     select fin_year_no,
                          fin_month_no
                        from (
                              select distinct fin_year_no,fin_month_no, this_mn_start_date,this_mn_end_date
                                from dim_calendar
                               where calendar_date < (select this_mn_start_date -1 from dim_calendar where calendar_date = to_date(trunc(sysdate)))

                               order by fin_year_no desc,fin_month_no desc
                             )
                           where rownum <= 25
                           order by fin_year_no desc,fin_month_no desc
                  )
     LOOP

        g_loop := g_loop + 1;

        l_text := g_loop||'. PROCESSING: Year '||i.fin_year_no||' Month '||i.fin_month_no;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        dbms_output.put_line('PROCESSING '||i.fin_year_no||' - '||i.fin_month_no||' - @ '||to_char(sysdate,('hh24:mi')));

----------------------------------------------------------------------------------------------
--  Specify column sequence against last completed Month (G_LOOP=1)
--  G_YEAR_NO AND G_MONTH_NO holds the last completed month.
----------------------------------------------------------------------------------------------     
        case when g_loop = 1 then
                  g_year_no  := i.fin_year_no;
                  g_month_no := i.fin_month_no;

                  g_stmt := 'alter table dwh_cust_performance.cust_db_company_month_involve truncate subpartition for ('||g_year_no||','||g_month_no||')';
                  l_text := g_stmt;
                  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                  execute immediate g_stmt;
                  commit;

                  g_list_item :='num_item_yr1_mn01 + num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 + '|| 
                                'num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 ';
                  g_list_sale :='sales_yr1_mn01 + sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 + '||
                                'sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 ';
                  g_stmt_main := 'select  /*+ Parallel(cma,8) Full(cma) */
                                          distinct
                                          cma.fin_year_no,
                                          cma.fin_month_no,
                                          cma.primary_customer_identifier,
                                          cma.company_no,
                                          cma.customer_no,('||
                                          g_list_item||') num_item_sum_yr1_mn01, ('||
                                          g_list_sale||') num_item_sum_yr1_mn01, 
                                          case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn01';
                  g_involve_column := 'involvement_score_yr1_mn01';
                  g_sale_column := 'sales_sum_yr1_mn01';
                  g_item_column := 'num_item_sum_yr1_mn01';

             when g_loop = 2 then
                  g_list_item :='num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 + num_item_yr1_mn07 + '|| 
                                'num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 + num_item_yr2_mn01';
                  g_list_sale :='sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 + sales_yr1_mn07 + '||
                                'sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 + sales_yr2_mn01';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn02, ('||
                                g_list_sale||') sale_sum_yr1_mn02, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn02';
                  g_involve_column := 'involvement_score_yr1_mn02';
                  g_sale_column := 'sales_sum_yr1_mn02';
                  g_item_column := 'num_item_sum_yr1_mn02';
             when g_loop = 3 then
                  g_list_item :='num_item_yr1_mn03 + num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 + '|| 
                                'num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 + num_item_yr2_mn01 + num_item_yr2_mn02';
                  g_list_sale :='sales_yr1_mn03 + sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 + '||
                                'sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 + sales_yr2_mn01 + sales_yr2_mn02';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn03, ('||
                                g_list_sale||') sale_sum_yr1_mn03, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn03';
                  g_involve_column := 'involvement_score_yr1_mn03';
                  g_sale_column := 'sales_sum_yr1_mn03';
                  g_item_column := 'num_item_sum_yr1_mn03';
             when g_loop = 4 then
                  g_list_item :='num_item_yr1_mn04 + num_item_yr1_mn05 + num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + '|| 
                                'num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 + num_item_yr2_mn01 + num_item_yr2_mn02+ num_item_yr2_mn03';
                  g_list_sale :='sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + '||
                                'sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 + sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn04, ('||
                                g_list_sale||') sale_sum_yr1_mn04, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn04';
                  g_involve_column := 'involvement_score_yr1_mn04';
                  g_sale_column := 'sales_sum_yr1_mn04';
                  g_item_column := 'num_item_sum_yr1_mn04';
             when g_loop = 5 then
                  g_list_item :='num_item_yr1_mn05 + num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + '|| 
                                'num_item_yr1_mn11 + num_item_yr1_mn12 + num_item_yr2_mn01 + num_item_yr2_mn02+ num_item_yr2_mn03 + num_item_yr2_mn04';
                  g_list_sale :='sales_yr1_mn05 + sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + '||
                                'sales_yr1_mn11 + sales_yr1_mn12 + sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn05, ('||
                                g_list_sale||') sale_sum_yr1_mn05, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn05';
                  g_involve_column := 'involvement_score_yr1_mn05';
                  g_sale_column := 'sales_sum_yr1_mn05';
                  g_item_column := 'num_item_sum_yr1_mn05';
             when g_loop = 6 then
                  g_list_item :='num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + '|| 
                                'num_item_yr1_mn12 + num_item_yr2_mn01 + num_item_yr2_mn02+ num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05';
                  g_list_sale :='sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + '||
                                'sales_yr1_mn12 + sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn06, ('||
                                g_list_sale||') sale_sum_yr1_mn06,case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn06';
                  g_involve_column := 'involvement_score_yr1_mn06';
                  g_sale_column := 'sales_sum_yr1_mn06';
                  g_item_column := 'num_item_sum_yr1_mn06';
             when g_loop = 7 then
                  g_list_item :='num_item_yr1_mn07 + num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 + '|| 
                                'num_item_yr2_mn01 + num_item_yr2_mn02+ num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06';
                  g_list_sale :='sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 + '||
                                'sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn07, ('||
                                g_list_sale||') sale_sum_yr1_mn07, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn07';
                  g_involve_column := 'involvement_score_yr1_mn07';
                  g_sale_column := 'sales_sum_yr1_mn07';
                  g_item_column := 'num_item_sum_yr1_mn07';
             when g_loop = 8 then
                  g_list_item :='num_item_yr1_mn08 + num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 + num_item_yr2_mn01 + '|| 
                                'num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 + num_item_yr2_mn07';
                  g_list_sale :='sales_yr1_mn08 + sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 + sales_yr2_mn01 + '||
                                'sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 + sales_yr2_mn07';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn08, ('||
                                g_list_sale||') sale_sum_yr1_mn08, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn08';
                  g_involve_column := 'involvement_score_yr1_mn08';
                  g_sale_column := 'sales_sum_yr1_mn08';
                  g_item_column := 'num_item_sum_yr1_mn08';
             when g_loop = 9 then
                  g_list_item :='num_item_yr1_mn09 + num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 + num_item_yr2_mn01 + num_item_yr2_mn02 + '|| 
                                'num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 + num_item_yr2_mn07 + num_item_yr2_mn08';
                  g_list_sale :='sales_yr1_mn09 + sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 + sales_yr2_mn01 + sales_yr2_mn02 + '||
                                'sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 + sales_yr2_mn07 + sales_yr2_mn08';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn09, ('||
                                g_list_sale||') sale_sum_yr1_mn09, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn09';
                  g_involve_column := 'involvement_score_yr1_mn09';
                  g_sale_column := 'sales_sum_yr1_mn09';
                  g_item_column := 'num_item_sum_yr1_mn09';
             when g_loop = 10 then
                  g_list_item :='num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12 + num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + '|| 
                                'num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 + num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09';
                  g_list_sale :='sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 + sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + '||
                                'sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 + sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn10, ('||
                                g_list_sale||') sale_sum_yr1_mn10, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn10';
                  g_involve_column := 'involvement_score_yr1_mn10';
                  g_sale_column := 'sales_sum_yr1_mn10';
                  g_item_column := 'num_item_sum_yr1_mn10';
             when g_loop = 11 then
                  g_list_item :='num_item_yr1_mn11 + num_item_yr1_mn12 + num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + '|| 
                                'num_item_yr2_mn05 + num_item_yr2_mn06 + num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10';
                  g_list_sale :='sales_yr1_mn11 + sales_yr1_mn12 + sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + '||
                                'sales_yr2_mn05 + sales_yr2_mn06 + sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10'; 
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn11, ('||
                                g_list_sale||') sale_sum_yr1_mn11, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn11';
                  g_involve_column := 'involvement_score_yr1_mn11';
                  g_sale_column := 'sales_sum_yr1_mn11';
                  g_item_column := 'num_item_sum_yr1_mn11';
             when g_loop = 12 then
                  g_list_item :='num_item_yr1_mn12 + num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + '|| 
                                'num_item_yr2_mn06 + num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11';
                  g_list_sale :='sales_yr1_mn12 + sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + '||
                                'sales_yr2_mn06 + sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11'; 
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr1_mn12, ('||
                                g_list_sale||') sale_sum_yr1_mn12, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr1_mn12';
                  g_involve_column := 'involvement_score_yr1_mn12';
                  g_sale_column := 'sales_sum_yr1_mn12';
                  g_item_column := 'num_item_sum_yr1_mn12';
             when g_loop = 13 then
                  g_list_item :='num_item_yr2_mn01 + num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 + '|| 
                                 'num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12 ';
                  g_list_sale :='sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 + '||
                                'sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12 ';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn01, ('||
                                g_list_sale||') sale_sum_yr2_mn01, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn01';
                  g_involve_column := 'involvement_score_yr2_mn01';
                  g_sale_column := 'sales_sum_yr2_mn01';
                  g_item_column := 'num_item_sum_yr2_mn01';
             when g_loop = 14 then
                  g_list_item :='num_item_yr2_mn02 + num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 + num_item_yr2_mn07 + '|| 
                                'num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12 + num_item_yr3_mn01';
                  g_list_sale :='sales_yr2_mn02 + sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 + sales_yr2_mn07 + '||
                                'sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12 + sales_yr3_mn01';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn02, ('||
                                g_list_sale||') sale_sum_yr2_mn02, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn02';
                  g_involve_column := 'involvement_score_yr2_mn02';
                  g_sale_column := 'sales_sum_yr2_mn02';
                  g_item_column := 'num_item_sum_yr2_mn02';
             when g_loop = 15 then
                  g_list_item :='num_item_yr2_mn03 + num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 + num_item_yr2_mn07 + num_item_yr2_mn08 + '|| 
                                'num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12 + num_item_yr3_mn01 + num_item_yr3_mn02';
                  g_list_sale :='sales_yr2_mn03 + sales_yr2_mn04 + sales_yr2_mn05 + sales_yr2_mn06 + sales_yr2_mn07 + sales_yr2_mn08 + '||
                                'sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12 + sales_yr3_mn01 + sales_yr3_mn02';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn03, ('||
                                g_list_sale||') sale_sum_yr2_mn03, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn03';
                  g_involve_column := 'involvement_score_yr2_mn03';
                  g_sale_column := 'sales_sum_yr2_mn03';
                  g_item_column := 'num_item_sum_yr2_mn03';
             when g_loop = 16 then
                  g_list_item :='num_item_yr2_mn04 + num_item_yr2_mn05 + num_item_yr2_mn06 + num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + '|| 
                                'num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12 + num_item_yr3_mn01 + num_item_yr3_mn02+ num_item_yr3_mn03';
                  g_list_sale :='sales_yr1_mn04 + sales_yr1_mn05 + sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 + sales_yr1_mn09 + '||
                                'sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12 + sales_yr2_mn01 + sales_yr2_mn02 + sales_yr2_mn03';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn04, ('||
                                g_list_sale||') sale_sum_yr2_mn04, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn04';
                  g_involve_column := 'involvement_score_yr2_mn04';
                  g_sale_column := 'sales_sum_yr2_mn04';
                  g_item_column := 'num_item_sum_yr2_mn04';
             when g_loop = 17 then
                  g_list_item :='num_item_yr2_mn05 + num_item_yr2_mn06 + num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + '|| 
                                'num_item_yr2_mn11 + num_item_yr2_mn12 + num_item_yr3_mn01 + num_item_yr3_mn02+ num_item_yr3_mn03 + num_item_yr3_mn04';
                  g_list_sale :='sales_yr2_mn05 + sales_yr2_mn06 + sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + '||
                                'sales_yr2_mn11 + sales_yr2_mn12 + sales_yr3_mn01 + sales_yr3_mn02 + sales_yr3_mn03 + sales_yr3_mn04';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn05, ('||
                                g_list_sale||') sale_sum_yr2_mn05, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn05';
                  g_involve_column := 'involvement_score_yr2_mn05';
                  g_sale_column := 'sales_sum_yr2_mn05';
                  g_item_column := 'num_item_sum_yr2_mn05';
             when g_loop = 18 then
                  g_list_item :='num_item_yr2_mn06 + num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + '|| 
                                'num_item_yr2_mn12 + num_item_yr3_mn01 + num_item_yr3_mn02+ num_item_yr3_mn03 + num_item_yr3_mn04 + num_item_yr3_mn05';
                  g_list_sale :='sales_yr2_mn06 + sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + '||
                                'sales_yr2_mn12 + sales_yr3_mn01 + sales_yr3_mn02 + sales_yr3_mn03 + sales_yr3_mn04 + sales_yr3_mn05';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn06, ('||
                                g_list_sale||') sale_sum_yr2_mn06, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn06';
                  g_involve_column := 'involvement_score_yr2_mn06';
                  g_sale_column := 'sales_sum_yr2_mn06';
                  g_item_column := 'num_item_sum_yr2_mn06';
             when g_loop = 19 then
                  g_list_item :='num_item_yr2_mn07 + num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12 + '|| 
                                'num_item_yr3_mn01 + num_item_yr3_mn02+ num_item_yr3_mn03 + num_item_yr3_mn04 + num_item_yr3_mn05 + num_item_yr3_mn06';
                  g_list_sale :='sales_yr2_mn07 + sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12 + '||
                                'sales_yr3_mn01 + sales_yr3_mn02 + sales_yr3_mn03 + sales_yr3_mn04 + sales_yr3_mn05 + sales_yr3_mn06';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn07, ('||
                                g_list_sale||') sale_sum_yr2_mn07, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn07';
                  g_involve_column := 'involvement_score_yr2_mn07';
                  g_sale_column := 'sales_sum_yr2_mn07';
                  g_item_column := 'num_item_sum_yr2_mn07';
             when g_loop = 20 then
                  g_list_item :='num_item_yr2_mn08 + num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12 + num_item_yr3_mn01 + '|| 
                                'num_item_yr3_mn02 + num_item_yr3_mn03 + num_item_yr3_mn04 + num_item_yr3_mn05 + num_item_yr3_mn06 + num_item_yr3_mn07';
                  g_list_sale :='sales_yr2_mn08 + sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12 + sales_yr3_mn01 + '||
                                'sales_yr3_mn02 + sales_yr3_mn03 + sales_yr3_mn04 + sales_yr3_mn05 + sales_yr3_mn06 + sales_yr3_mn07';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn08, ('||
                                g_list_sale||') sale_sum_yr2_mn08, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn08';
                  g_involve_column := 'involvement_score_yr2_mn08';
                  g_sale_column := 'sales_sum_yr2_mn08';
                  g_item_column := 'num_item_sum_yr2_mn08';
             when g_loop = 21 then
                  g_list_item :='num_item_yr2_mn09 + num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12 + num_item_yr3_mn01 + num_item_yr3_mn02 + '|| 
                                'num_item_yr3_mn03 + num_item_yr3_mn04 + num_item_yr3_mn05 + num_item_yr3_mn06 + num_item_yr3_mn07 + num_item_yr3_mn08';
                  g_list_sale :='sales_yr2_mn09 + sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12 + sales_yr3_mn01 + sales_yr3_mn02 + '||
                                'sales_yr3_mn03 + sales_yr3_mn04 + sales_yr3_mn05 + sales_yr3_mn06 + sales_yr3_mn07 + sales_yr3_mn08';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn09, ('||
                                g_list_sale||') sale_sum_yr2_mn09, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn09';
                  g_involve_column := 'involvement_score_yr2_mn09';
                  g_sale_column := 'sales_sum_yr2_mn09';
                  g_item_column := 'num_item_sum_yr2_mn09';
             when g_loop = 22 then
                  g_list_item :='num_item_yr2_mn10 + num_item_yr2_mn11 + num_item_yr2_mn12 + num_item_yr3_mn01 + num_item_yr3_mn02 + num_item_yr3_mn03 + '|| 
                                'num_item_yr3_mn04 + num_item_yr3_mn05 + num_item_yr3_mn06 + num_item_yr3_mn07 + num_item_yr3_mn08 + num_item_yr3_mn09';
                  g_list_sale :='sales_yr2_mn10 + sales_yr2_mn11 + sales_yr2_mn12 + sales_yr3_mn01 + sales_yr3_mn02 + sales_yr3_mn03 + '||
                                'sales_yr3_mn04 + sales_yr3_mn05 + sales_yr3_mn06 + sales_yr3_mn07 + sales_yr3_mn08 + sales_yr3_mn09';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn10, ('||
                                g_list_sale||') sale_sum_yr2_mn10, case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn10';
                  g_involve_column := 'involvement_score_yr2_mn10';
                  g_sale_column := 'sales_sum_yr2_mn10';
                  g_item_column := 'num_item_sum_yr2_mn10';
             when g_loop = 23 then
                  g_list_item :='num_item_yr2_mn11 + num_item_yr2_mn12 + num_item_yr3_mn01 + num_item_yr3_mn02 + num_item_yr3_mn03 + num_item_yr3_mn04 + '|| 
                                'num_item_yr3_mn05 + num_item_yr3_mn06 + num_item_yr3_mn07 + num_item_yr3_mn08 + num_item_yr3_mn09 + num_item_yr3_mn10';
                  g_list_sale :='sales_yr2_mn11 + sales_yr2_mn12 + sales_yr3_mn01 + sales_yr3_mn02 + sales_yr3_mn03 + sales_yr3_mn04 + '||
                                'sales_yr3_mn05 + sales_yr3_mn06 + sales_yr3_mn07 + sales_yr3_mn08 + sales_yr3_mn09 + sales_yr3_mn10';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn11, ('||
                                g_list_sale||') sale_sum_yr2_mn11,case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn11';
                  g_involve_column := 'involvement_score_yr2_mn11';
                  g_sale_column := 'sales_sum_yr2_mn11';
                  g_item_column := 'num_item_sum_yr2_mn11';
             when g_loop = 24 then
                  g_list_item :='num_item_yr2_mn12 + num_item_yr3_mn01 + num_item_yr3_mn02 + num_item_yr3_mn03 + num_item_yr3_mn04 + num_item_yr3_mn05 + '|| 
                                'num_item_yr3_mn06 + num_item_yr3_mn07 + num_item_yr3_mn08 + num_item_yr3_mn09 + num_item_yr3_mn10 + num_item_yr3_mn11';
                  g_list_sale :='sales_yr2_mn12 + sales_yr3_mn01 + sales_yr3_mn02 + sales_yr3_mn03 + sales_yr3_mn04 + sales_yr3_mn05 + '||
                                'sales_yr3_mn06 + sales_yr3_mn07 + sales_yr3_mn08 + sales_yr3_mn09 + sales_yr3_mn10 + sales_yr3_mn11'; 
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr2_mn12, ('||
                                g_list_sale||') sale_sum_yr2_mn12, 
                                case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr2_mn12';
                  g_involve_column := 'involvement_score_yr2_mn12';
                  g_sale_column := 'sales_sum_yr2_mn12';
                  g_item_column := 'num_item_sum_yr2_mn12';
             when g_loop = 25 then
                  g_list_item :='num_item_yr3_mn01 + num_item_yr3_mn02 + num_item_yr3_mn03 + num_item_yr3_mn04 + num_item_yr3_mn05 + num_item_yr3_mn06 + '|| 
                                'num_item_yr3_mn07 + num_item_yr3_mn08 + num_item_yr3_mn09 + num_item_yr3_mn10 + num_item_yr3_mn11 + num_item_yr3_mn12 ';
                  g_list_sale :='sales_yr3_mn01 + sales_yr3_mn02 + sales_yr3_mn03 + sales_yr3_mn04 + sales_yr3_mn05 + sales_yr3_mn06 + '||
                                'sales_yr3_mn07 + sales_yr3_mn08 + sales_yr3_mn09 + sales_yr3_mn10 + sales_yr3_mn11 + sales_yr3_mn12 ';
                  g_stmt_main :=g_stmt_main||', ('||g_list_item||') num_item_sum_yr3_mn01, ('||
                                g_list_sale||') sale_sum_yr3_mn01,case when ('||g_list_sale||') > 0 then 1 else 0 end involvement_score_yr3_mn01';
                  g_involve_column := 'involvement_score_yr3_mn01';             
                  g_sale_column := 'sales_sum_yr3_mn01';
                  g_item_column := 'num_item_sum_yr3_mn01';
        end case;

        dbms_output.put_line('Loop : '||g_loop);




        execute immediate ' create table dwh_cust_performance.temp_item_sales_comp_involve as 
                             (select /*+ Parallel(a,8) Full(a) */ '||
                                     i.fin_year_no||' fin_year_no,'||
                                     i.fin_month_no||' fin_month_no,'||
                                     'company_no,
                                     primary_customer_identifier,
                                     ('||g_list_item||') item_sum, 
                                     round('||g_list_sale||') sales_sum
                            from dwh_cust_performance.cust_db_company_month a
                           where primary_customer_identifier <> 998
                             and (('||g_list_item||') > 0 and 
                                  ('||g_list_sale||') > 0)
                             and fin_year_no = '||g_year_no||' 
                             and fin_month_no = '||g_month_no||'
                         )';


        l_text := '   TEMP_ITEM_SALES_COMP_INVOLVE Inserted '|| SQL%ROWCOUNT;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        COMMIT;

        ----------------------------------------------------------------------------------------------
        --  Formulate Involvement Score updates.
        ---------------------------------------------------------------------------------------------- 
        array.extend(); -- Extend it
        array(g_loop) := '
                          merge into dwh_cust_performance.cust_db_company_month_involve di
                          using (select ir.company_no,
                                        ir.item_sum,
                                        sr.sales_sum,
                                        ir.ranking item_rank ,
                                        sr.ranking sale_rank
                                   from dwh_cust_performance.temp_cust_comp_item_ranking ir
                                  inner join dwh_cust_performance.temp_cust_comp_sale_ranking sr
                                     on (ir.fin_year_no  = sr.fin_year_no and
                                         ir.fin_month_no = sr.fin_month_no and
                                         ir.company_no  = sr.company_no 
                                        )
                                  where ir.fin_year_no  = '||i.fin_year_no||' and 
                                        ir.fin_month_no = '||i.fin_month_no||' 
                                ) tt
                             on (
                                  di.company_no                = tt.company_no and
                                  ROUND(di.'||g_sale_column||') = tt.sales_sum and
                                  di.'||g_item_column||'        = tt.item_sum
                                )
                           when matched then update set '||g_involve_column||' = case when tt.item_rank >= 9 and tt.sale_rank >= 7 then 3
                                                                                          when tt.item_rank >= 8 and tt.sale_rank >= 8 then 3
                                                                                          when tt.item_rank >= 7 and tt.sale_rank >= 9 then 3
                                                                                          when tt.item_rank >= 9 and tt.sale_rank >= 0 then 2
                                                                                          when tt.item_rank >= 8 and tt.sale_rank >= 0 then 2
                                                                                          when tt.item_rank >= 7 and tt.sale_rank >= 1 then 2
                                                                                          when tt.item_rank >= 6 and tt.sale_rank >= 2 then 2
                                                                                          when tt.item_rank >= 5 and tt.sale_rank >= 3 then 2
                                                                                          when tt.item_rank >= 4 and tt.sale_rank >= 4 then 2
                                                                                          when tt.item_rank >= 3 and tt.sale_rank >= 5 then 2
                                                                                          when tt.item_rank >= 2 and tt.sale_rank >= 6 then 2
                                                                                          when tt.item_rank >= 1 and tt.sale_rank >= 7 then 2
                                                                                          else 1
                                                                                     end
                           where ROUND('||g_sale_column||') > 0  
                             and fin_year_no  = '||g_year_no||'
                             and fin_month_no = '||g_month_no;

        ----------------------------------------------------------------------------------------------
        --  Sales Ranking
        ----------------------------------------------------------------------------------------------  
            populate_sale_rank(i.fin_year_no,i.fin_month_no);

        --        dbms_output.put_line('SQL STATEMENT: '||LENGTH(g_stmt));


        ----------------------------------------------------------------------------------------------
        --  Item Ranking
        ----------------------------------------------------------------------------------------------        
            populate_item_rank(i.fin_year_no,i.fin_month_no);

        --        dbms_output.put_line('SQL STATEMENT: '||LENGTH(g_stmt));

            execute immediate 'DROP TABLE TEMP_ITEM_SALES_COMP_INVOLVE';
            COMMIT;

     end loop;


    l_text := ' UPDATE STATISTICS ON TEMP_CUST_COMP_ITEM_RANKING';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_COMP_ITEM_RANKING',estimate_percent=>1, DEGREE => 32);

    l_text := ' UPDATE STATISTICS ON TEMP_CUST_COMP_SALE_RANKING';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_COMP_SALE_RANKING',estimate_percent=>1, DEGREE => 32);
----------------------------------------------------------------------------------------------
--  Create the involve table.
----------------------------------------------------------------------------------------------  
    l_text := 'LOAD DWH_CUST_PERFORMANCE.CUST_DB_COMPANY_MONTH_INVOLVE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    g_stmt_main := 'insert /*+ Append Parallel(a,8) */ into dwh_cust_performance.cust_db_company_month_involve a 
                   '||g_stmt_main||',NULL num_item_sum_yr3_mn02,NULL sales_sum_yr3_mn02,NULL involvement_score_yr3_mn02,NULL num_item_sum_yr3_mn03,NULL sales_sum_yr3_mn03,NULL involvement_score_yr3_mn03,NULL num_item_sum_yr3_mn04,NULL sales_sum_yr3_mn04,NULL involvement_score_yr3_mn04,NULL num_item_sum_yr3_mn05,NULL sales_sum_yr3_mn05,NULL involvement_score_yr3_mn05,NULL num_item_sum_yr3_mn06,NULL sales_sum_yr3_mn06,NULL involvement_score_yr3_mn06,NULL num_item_sum_yr3_mn07,NULL sales_sum_yr3_mn07,NULL involvement_score_yr3_mn07,NULL num_item_sum_yr3_mn08,NULL sales_sum_yr3_mn08,NULL involvement_score_yr3_mn08,NULL num_item_sum_yr3_mn09,NULL sales_sum_yr3_mn09,NULL involvement_score_yr3_mn09,NULL num_item_sum_yr3_mn10,NULL sales_sum_yr3_mn10,NULL involvement_score_yr3_mn10,NULL num_item_sum_yr3_mn11,NULL sales_sum_yr3_mn11,NULL involvement_score_yr3_mn11,NULL num_item_sum_yr3_mn12,NULL sales_sum_yr3_mn12,NULL involvement_score_yr3_mn12,
                   to_date(trunc(sysdate)) last_updated_date from dwh_cust_performance.cust_db_company_month cma where cma.fin_year_no = '||g_year_no||' and cma.fin_month_no = '||g_month_no;

--    g_stmt_main := 'merge /*+ Append Parallel(di,8) */ into dwh_cust_performance.cust_db_company_month_involve di
--                    using ('||g_stmt_main||') ti 
--                       on (di.fin_year_no  = ti.fin_year_no and 
--                           di.fin_month_no = ti.fin_month_no and  
--                           di.company_no  = ti.company_no and 
--                           di.primary_customer_identifier =  ti.primary_customer_identifier
--                          ) 
--                    when matched then update set di.* = ti.*
--                    when not matched the insert (di.*) values (ti.*)';



--    dbms_output.put_line('MAIN SQL STATEMENT: '||LENGTH(g_stmt_main)||g_stmt_main);

    EXECUTE IMMEDIATE g_stmt_main;

    l_text := ' * COMPANY INVOLVE RECORDS INSERTED '|| SQL%ROWCOUNT;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);        
    COMMIT;

    l_text := ' UPDATE STATISTICS ON CUST_DB_COMPANY_MONTH_INVOLVE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_DB_COMPANY_MONTH_INVOLVE',estimate_percent=>1, DEGREE => 32);
    COMMIT;


    ----------------------------------------------------------------------------------------------
    --  Process all involvement score column updates.
    ----------------------------------------------------------------------------------------------     
    for i in 1..25 
    loop

      execute immediate array(i);

      l_text := '   INVOLVEMENT SCORE UPDATE - LOOP #'||i||' : '|| SQL%ROWCOUNT;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      COMMIT;
    end loop;

    l_text := ' UPDATE STATISTICS ON CUST_DB_COMPANY_MONTH_INVOLVE';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_DB_COMPANY_MONTH_INVOLVE',estimate_percent=>1, DEGREE => 32);
-------------------------------------------
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
END WH_PRF_CUST_294U_THEO;
