--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_272U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_272U" 

                                                                                                                                                                                                                                                            (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        Oct 2015
--  Author:      Alastair de Wet
--  Purpose:     TAKE ON OR RESTRUCTURE DEPTH AND BREADTH AT GROUP LEVEL
--  Tables:      Input  - cust_basket_item  
--               Output - cust_db_subgroup_month
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
g_forall_limit       integer       :=  10000;
g_sub                integer;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_yr_00               number;
g_mn_00               number;
g_last_yr             number;
g_last_mn             number;
g_yr_loop             number;
g_mn_loop             number;
g_this_mn_start_date  date;
g_this_mn_end_date    date;
g_run_date            date;
g_stmt                varchar(500); 

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_272U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TAKEON/RESTRUCTURE cust_db_subgroup_month EX cust_basket_item';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

TYPE df_array IS TABLE OF date
 INDEX BY BINARY_INTEGER;
 date_from df_array;
TYPE dt_array IS TABLE OF date
 INDEX BY BINARY_INTEGER;
 date_to   dt_array;

filter_lst  DBMS_STATS.OBJECTTAB := DBMS_STATS.OBJECTTAB(); 
 
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 
    execute immediate 'alter session enable parallel dml';
    
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;  
    p_success := false;    
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'TAKEON OF cust_db_subgroup_month EX cust_basket_item STARTED AT '||
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

    select last_yr_fin_year_no,last_mn_fin_month_no 
    into   g_yr_00,g_mn_00 
    from dim_control;
    
    select unique this_mn_start_date,this_mn_end_date
    into   g_this_mn_start_date, g_this_mn_end_date
    from   dim_calendar
    where  fin_year_no  = g_yr_00 and 
           fin_month_no = g_mn_00 and
           fin_day_no   = 1;
   
   g_last_mn := g_mn_00 - 1;
   g_last_yr := g_yr_00;
   if g_last_mn = 0 then
      g_last_mn := 12;
      g_last_yr := g_last_yr - 1;
   end if;   

   l_text := 'Month being processed:= '|| g_this_mn_start_date || g_this_mn_end_date ||g_yr_00||g_mn_00; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    

   g_run_date := g_this_mn_end_date + 5;
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;  
   
   l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

-- CLEAR TABLES DOWN ---
--   g_stmt      := 'Alter table  DWH_CUST_PERFORMANCE.CUST_DB_subgroup_MONTH truncate  subpartition for ('||g_yr_00||','||g_mn_00||') update global indexes';
--   l_text      := g_stmt;
    l_text      := 'TRUNCATE ALL TABLES';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.CUST_DB_SUBGROUP_MONTH';
    execute immediate g_stmt;  
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_SUBGROUP_MONTH_1';
    execute immediate g_stmt; 
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_SUBGROUP_MONTH_2';
    execute immediate g_stmt; 
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_SUBGROUP_MONTH_3';
    execute immediate g_stmt; 
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_SUBGROUP_MONTH_4';
    execute immediate g_stmt;     

   l_text := 'UPDATE STATS ON ALL TABLES'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_SUBGROUP_MONTH_1',estimate_percent=>1, DEGREE => 32);
   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_SUBGROUP_MONTH_2',estimate_percent=>1, DEGREE => 32);
   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_SUBGROUP_MONTH_3',estimate_percent=>1, DEGREE => 32);
   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_SUBGROUP_MONTH_4',estimate_percent=>1, DEGREE => 32);
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_DB_SUBGROUP_MONTH',estimate_percent=>1, DEGREE => 32);
   COMMIT;

-- SET UP DATE RANGES THAT HAVE TO BE PROCESSED -- 
   g_mn_loop := g_mn_00;
   g_yr_loop := g_yr_00;

   for g_sub in 1..36
   loop
 
    select unique this_mn_start_date,this_mn_end_date
    into   date_from(g_sub), date_to(g_sub)
    from   dim_calendar
    where  fin_year_no  = g_yr_loop and 
           fin_month_no = g_mn_loop and
           fin_day_no   = 1;

    g_mn_loop := g_mn_loop - 1;
    if g_mn_loop = 0 then
       g_mn_loop := 12;
       g_yr_loop := g_yr_loop - 1;
    end if;   

    l_text := 'Range of takeon being processed:= '||date_from(g_sub) || date_to(g_sub); 
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   end loop;

--MAIN INSERT STATEMENT USING UNION ALL ACCROSS ALL DATE RANGES --

   l_text := 'MAIN INSERT START INTO 4 TEMP TABLES'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   insert /*+ APPEND parallel (prf,12) */ into temp_db_subgroup_month_1 prf
   with     bskt1 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no,
            max(customer_no)   customer_no,
            sum(item_tran_qty) item_01,
            sum(item_tran_selling - discount_selling) sales_01,  
            count(unique tran_no||tran_date||till_no||location_no) visit_01,
            0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03, 0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07, 0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(1) and date_to(1) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt2 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,
            sum(item_tran_qty) item_02,
            sum(item_tran_selling - discount_selling) sales_02,  
            count(unique tran_no||tran_date||till_no||location_no) visit_02,
            0 item_03,0 sales_03,0 visit_03, 0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07, 0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(2) and date_to(2) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt3 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02, 
            sum(item_tran_qty) item_03,
            sum(item_tran_selling - discount_selling) sales_03,  
            count(unique tran_no||tran_date||till_no||location_no) visit_03,
            0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07, 0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(3) and date_to(3) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt4 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,
            sum(item_tran_qty) item_04,
            sum(item_tran_selling - discount_selling) sales_04,  
            count(unique tran_no||tran_date||till_no||location_no) visit_04,
            0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07, 0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(4) and date_to(4) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt5 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,
            sum(item_tran_qty) item_05,
            sum(item_tran_selling - discount_selling) sales_05,  
            count(unique tran_no||tran_date||till_no||location_no) visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07, 0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(5) and date_to(5) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt6 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            sum(item_tran_qty) item_06,
            sum(item_tran_selling - discount_selling) sales_06,  
            count(unique tran_no||tran_date||till_no||location_no) visit_06,
            0 item_07,0 sales_07,0 visit_07, 0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(6) and date_to(6) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt7 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06, 
            sum(item_tran_qty) item_07,
            sum(item_tran_selling - discount_selling) sales_07,  
            count(unique tran_no||tran_date||till_no||location_no) visit_07,
            0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(7) and date_to(7) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt8 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07, 
            sum(item_tran_qty) item_08,
            sum(item_tran_selling - discount_selling) sales_08,  
            count(unique tran_no||tran_date||till_no||location_no) visit_08,
            0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(8) and date_to(8) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt9 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08, 
            sum(item_tran_qty) item_09,
            sum(item_tran_selling - discount_selling) sales_09,  
            count(unique tran_no||tran_date||till_no||location_no) visit_09,
            0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(9) and date_to(9) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            cust_union_all as
   (
   select  /*+ FULL(b01)  parallel (b01,4)  */  *   from bskt1   b01
   union all
   select  /*+ FULL(b02)  parallel (b02,4)  */  *   from bskt2   b02
   union all
   select  /*+ FULL(b03)  parallel (b03,4)  */  *   from bskt3   b03
   union all
   select  /*+ FULL(b04)  parallel (b04,4)  */  *   from bskt4   b04
   union all
   select  /*+ FULL(b05)  parallel (b05,4)  */  *   from bskt5   b05
   union all
   select  /*+ FULL(b06)  parallel (b06,4)  */  *   from bskt6   b06
   union all
   select  /*+ FULL(b07)  parallel (b07,4)  */  *   from bskt7   b07
   union all
   select  /*+ FULL(b08)  parallel (b08,4)  */  *   from bskt8   b08
   union all
   select  /*+ FULL(b09)  parallel (b09,4)  */  *   from bskt9   b09
   )         
   select /*+ FULL(cua)  parallel (cua,8)  */
            g_yr_00,g_mn_00,
            primary_customer_identifier,
            subgroup_no,
            max(customer_no) customer_no,
            sum(item_01),sum(sales_01),sum(visit_01),
            sum(item_02),sum(sales_02),sum(visit_02),
            sum(item_03),sum(sales_03),sum(visit_03),
            sum(item_04),sum(sales_04),sum(visit_04),
            sum(item_05),sum(sales_05),sum(visit_05),
            sum(item_06),sum(sales_06),sum(visit_06),
            sum(item_07),sum(sales_07),sum(visit_07),
            sum(item_08),sum(sales_08),sum(visit_08),
            sum(item_09),sum(sales_09),sum(visit_09),
            sum(item_10),sum(sales_10),sum(visit_10),
            sum(item_11),sum(sales_11),sum(visit_11),
            sum(item_12),sum(sales_12),sum(visit_12),
            sum(item_13),sum(sales_13),sum(visit_13),
            sum(item_14),sum(sales_14),sum(visit_14),
            sum(item_15),sum(sales_15),sum(visit_15),
            sum(item_16),sum(sales_16),sum(visit_16),
            sum(item_17),sum(sales_17),sum(visit_17),
            sum(item_18),sum(sales_18),sum(visit_18),
            sum(item_19),sum(sales_19),sum(visit_19),
            sum(item_20),sum(sales_20),sum(visit_20),
            sum(item_21),sum(sales_21),sum(visit_21),
            sum(item_22),sum(sales_22),sum(visit_22),
            sum(item_23),sum(sales_23),sum(visit_23),
            sum(item_24),sum(sales_24),sum(visit_24),
            sum(item_25),sum(sales_25),sum(visit_25),
            sum(item_26),sum(sales_26),sum(visit_26),
            sum(item_27),sum(sales_27),sum(visit_27),
            sum(item_28),sum(sales_28),sum(visit_28),
            sum(item_29),sum(sales_29),sum(visit_29),
            sum(item_30),sum(sales_30),sum(visit_30),
            sum(item_31),sum(sales_31),sum(visit_31),
            sum(item_32),sum(sales_32),sum(visit_32),
            sum(item_33),sum(sales_33),sum(visit_33),
            sum(item_34),sum(sales_34),sum(visit_34),
            sum(item_35),sum(sales_35),sum(visit_35),
            sum(item_36),sum(sales_36),sum(visit_36),
            g_date
   from     cust_union_all cua
   group by primary_customer_identifier,subgroup_no
   ;

   commit;
   l_text := 'FIRST TEMP TABLE WRITTEN FOR 9 MONTHS DATA'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (prf,12) */ into temp_db_subgroup_month_2 prf
   with         bskt10 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,
            sum(item_tran_qty) item_10,
            sum(item_tran_selling - discount_selling) sales_10,  
            count(unique tran_no||tran_date||till_no||location_no) visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12, 0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17, 0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22, 0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27, 0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32, 0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(10) and date_to(10) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt11 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            sum(item_tran_qty) item_11,
            sum(item_tran_selling - discount_selling) sales_11,  
            count(unique tran_no||tran_date||till_no||location_no) visit_11,
            0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(11) and date_to(11) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt12 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11, 
            sum(item_tran_qty) item_12,
            sum(item_tran_selling - discount_selling) sales_12,  
            count(unique tran_no||tran_date||till_no||location_no) visit_12,
            0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(12) and date_to(12) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt13 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,
            sum(item_tran_qty) item_13,
            sum(item_tran_selling - discount_selling) sales_13,  
            count(unique tran_no||tran_date||till_no||location_no) visit_13,
            0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(13) and date_to(13) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt14 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,
            sum(item_tran_qty) item_14,
            sum(item_tran_selling - discount_selling) sales_14,  
            count(unique tran_no||tran_date||till_no||location_no) visit_14,
            0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(14) and date_to(14) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt15 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,
            sum(item_tran_qty) item_15,
            sum(item_tran_selling - discount_selling) sales_15,  
            count(unique tran_no||tran_date||till_no||location_no) visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(15) and date_to(15) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt16 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            sum(item_tran_qty) item_16,
            sum(item_tran_selling - discount_selling) sales_16,  
            count(unique tran_no||tran_date||till_no||location_no) visit_16,
            0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(16) and date_to(16) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt17 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16, 
            sum(item_tran_qty) item_17,
            sum(item_tran_selling - discount_selling) sales_17,  
            count(unique tran_no||tran_date||till_no||location_no) visit_17,
            0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(17) and date_to(17) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt18 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,
            sum(item_tran_qty) item_18,
            sum(item_tran_selling - discount_selling) sales_18,  
            count(unique tran_no||tran_date||till_no||location_no) visit_18,
            0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(18) and date_to(18) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            cust_union_all as
   (
   select  /*+ FULL(b10)  parallel (b10,4)  */  *   from bskt10  b10
   union all
   select  /*+ FULL(b11)  parallel (b11,4)  */  *   from bskt11  b11
   union all
   select  /*+ FULL(b12)  parallel (b12,4)  */  *   from bskt12  b12
   union all
   select  /*+ FULL(b13)  parallel (b13,4)  */  *   from bskt13  b13
   union all
   select  /*+ FULL(b14)  parallel (b14,4)  */  *   from bskt14  b14
   union all
   select  /*+ FULL(b15)  parallel (b15,4)  */  *   from bskt15  b15
   union all
   select  /*+ FULL(b16)  parallel (b16,4)  */  *   from bskt16  b16
   union all
   select  /*+ FULL(b17)  parallel (b17,4)  */  *   from bskt17  b17
   union all
   select  /*+ FULL(b18)  parallel (b18,4)  */  *   from bskt18  b18

   )         
   select /*+ FULL(cua)  parallel (cua,8)  */
            g_yr_00,g_mn_00,
            primary_customer_identifier,
            subgroup_no,
            max(customer_no) customer_no,
            sum(item_01),sum(sales_01),sum(visit_01),
            sum(item_02),sum(sales_02),sum(visit_02),
            sum(item_03),sum(sales_03),sum(visit_03),
            sum(item_04),sum(sales_04),sum(visit_04),
            sum(item_05),sum(sales_05),sum(visit_05),
            sum(item_06),sum(sales_06),sum(visit_06),
            sum(item_07),sum(sales_07),sum(visit_07),
            sum(item_08),sum(sales_08),sum(visit_08),
            sum(item_09),sum(sales_09),sum(visit_09),
            sum(item_10),sum(sales_10),sum(visit_10),
            sum(item_11),sum(sales_11),sum(visit_11),
            sum(item_12),sum(sales_12),sum(visit_12),
            sum(item_13),sum(sales_13),sum(visit_13),
            sum(item_14),sum(sales_14),sum(visit_14),
            sum(item_15),sum(sales_15),sum(visit_15),
            sum(item_16),sum(sales_16),sum(visit_16),
            sum(item_17),sum(sales_17),sum(visit_17),
            sum(item_18),sum(sales_18),sum(visit_18),
            sum(item_19),sum(sales_19),sum(visit_19),
            sum(item_20),sum(sales_20),sum(visit_20),
            sum(item_21),sum(sales_21),sum(visit_21),
            sum(item_22),sum(sales_22),sum(visit_22),
            sum(item_23),sum(sales_23),sum(visit_23),
            sum(item_24),sum(sales_24),sum(visit_24),
            sum(item_25),sum(sales_25),sum(visit_25),
            sum(item_26),sum(sales_26),sum(visit_26),
            sum(item_27),sum(sales_27),sum(visit_27),
            sum(item_28),sum(sales_28),sum(visit_28),
            sum(item_29),sum(sales_29),sum(visit_29),
            sum(item_30),sum(sales_30),sum(visit_30),
            sum(item_31),sum(sales_31),sum(visit_31),
            sum(item_32),sum(sales_32),sum(visit_32),
            sum(item_33),sum(sales_33),sum(visit_33),
            sum(item_34),sum(sales_34),sum(visit_34),
            sum(item_35),sum(sales_35),sum(visit_35),
            sum(item_36),sum(sales_36),sum(visit_36),
            g_date
   from     cust_union_all cua
   group by primary_customer_identifier,subgroup_no
   ;   
   commit;
   l_text := 'SECOND TEMP TABLE WRITTEN FOR 9 MONTHS DATA'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
   
   insert /*+ APPEND parallel (prf,12) */ into temp_db_subgroup_month_3 prf   
   with         bskt19 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,
            sum(item_tran_qty) item_19,
            sum(item_tran_selling - discount_selling) sales_19,  
            count(unique tran_no||tran_date||till_no||location_no) visit_19,
            0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(19) and date_to(19) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt20 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,
            sum(item_tran_qty) item_20,
            sum(item_tran_selling - discount_selling) sales_20,  
            count(unique tran_no||tran_date||till_no||location_no) visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(20) and date_to(20) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt21 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            sum(item_tran_qty) item_21,
            sum(item_tran_selling - discount_selling) sales_21,  
            count(unique tran_no||tran_date||till_no||location_no) visit_21,
            0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(21) and date_to(21) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt22 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,
            sum(item_tran_qty) item_22,
            sum(item_tran_selling - discount_selling) sales_22,  
            count(unique tran_no||tran_date||till_no||location_no) visit_22,
            0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(22) and date_to(22) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt23 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,
            sum(item_tran_qty) item_23,
            sum(item_tran_selling - discount_selling) sales_23,  
            count(unique tran_no||tran_date||till_no||location_no) visit_23,
            0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(23) and date_to(23) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),  
            bskt24 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,
            sum(item_tran_qty) item_24,
            sum(item_tran_selling - discount_selling) sales_24,  
            count(unique tran_no||tran_date||till_no||location_no) visit_24,
            0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(24) and date_to(24) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ), 
            bskt25 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,
            sum(item_tran_qty) item_25,
            sum(item_tran_selling - discount_selling) sales_25,  
            count(unique tran_no||tran_date||till_no||location_no) visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(25) and date_to(25) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ), 
            bskt26 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            sum(item_tran_qty) item_26,
            sum(item_tran_selling - discount_selling) sales_26,  
            count(unique tran_no||tran_date||till_no||location_no) visit_26,
            0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(26) and date_to(26) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt27 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,
            sum(item_tran_qty) item_27,
            sum(item_tran_selling - discount_selling) sales_27,  
            count(unique tran_no||tran_date||till_no||location_no) visit_27,
            0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(27) and date_to(27) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),  
             cust_union_all as
   (
   select  /*+ FULL(b19)  parallel (b19,4)  */  *   from bskt19  b19
   union all
   select  /*+ FULL(b20)  parallel (b20,4)  */  *   from bskt20  b20
   union all
   select  /*+ FULL(b21)  parallel (b21,4)  */  *   from bskt21  b21
   union all
   select  /*+ FULL(b22)  parallel (b22,4)  */  *   from bskt22  b22
   union all
   select  /*+ FULL(b23)  parallel (b23,4)  */  *   from bskt23  b23
   union all
   select  /*+ FULL(b24)  parallel (b24,4)  */  *   from bskt24  b24
   union all
   select  /*+ FULL(b25)  parallel (b25,4)  */  *   from bskt25  b25
   union all
   select  /*+ FULL(b26)  parallel (b26,4)  */  *   from bskt26  b26
    union all
   select  /*+ FULL(b27)  parallel (b27,4)  */  *   from bskt27  b27
   )         
   select /*+ FULL(cua)  parallel (cua,8)  */
            g_yr_00,g_mn_00,
            primary_customer_identifier,
            subgroup_no,
            max(customer_no) customer_no,
            sum(item_01),sum(sales_01),sum(visit_01),
            sum(item_02),sum(sales_02),sum(visit_02),
            sum(item_03),sum(sales_03),sum(visit_03),
            sum(item_04),sum(sales_04),sum(visit_04),
            sum(item_05),sum(sales_05),sum(visit_05),
            sum(item_06),sum(sales_06),sum(visit_06),
            sum(item_07),sum(sales_07),sum(visit_07),
            sum(item_08),sum(sales_08),sum(visit_08),
            sum(item_09),sum(sales_09),sum(visit_09),
            sum(item_10),sum(sales_10),sum(visit_10),
            sum(item_11),sum(sales_11),sum(visit_11),
            sum(item_12),sum(sales_12),sum(visit_12),
            sum(item_13),sum(sales_13),sum(visit_13),
            sum(item_14),sum(sales_14),sum(visit_14),
            sum(item_15),sum(sales_15),sum(visit_15),
            sum(item_16),sum(sales_16),sum(visit_16),
            sum(item_17),sum(sales_17),sum(visit_17),
            sum(item_18),sum(sales_18),sum(visit_18),
            sum(item_19),sum(sales_19),sum(visit_19),
            sum(item_20),sum(sales_20),sum(visit_20),
            sum(item_21),sum(sales_21),sum(visit_21),
            sum(item_22),sum(sales_22),sum(visit_22),
            sum(item_23),sum(sales_23),sum(visit_23),
            sum(item_24),sum(sales_24),sum(visit_24),
            sum(item_25),sum(sales_25),sum(visit_25),
            sum(item_26),sum(sales_26),sum(visit_26),
            sum(item_27),sum(sales_27),sum(visit_27),
            sum(item_28),sum(sales_28),sum(visit_28),
            sum(item_29),sum(sales_29),sum(visit_29),
            sum(item_30),sum(sales_30),sum(visit_30),
            sum(item_31),sum(sales_31),sum(visit_31),
            sum(item_32),sum(sales_32),sum(visit_32),
            sum(item_33),sum(sales_33),sum(visit_33),
            sum(item_34),sum(sales_34),sum(visit_34),
            sum(item_35),sum(sales_35),sum(visit_35),
            sum(item_36),sum(sales_36),sum(visit_36),
            g_date
   from     cust_union_all cua
   group by primary_customer_identifier,subgroup_no
   ;  
   
   commit;
   l_text := 'THIRD TEMP TABLE WRITTEN FOR 9 MONTHS DATA'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
   
   insert /*+ APPEND parallel (prf,12) */ into temp_db_subgroup_month_4 prf     
   with         bskt28 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,
            sum(item_tran_qty) item_28,
            sum(item_tran_selling - discount_selling) sales_28,  
            count(unique tran_no||tran_date||till_no||location_no) visit_28,
            0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(28) and date_to(28) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt29 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,
            sum(item_tran_qty) item_29,
            sum(item_tran_selling - discount_selling) sales_29,  
            count(unique tran_no||tran_date||till_no||location_no) visit_29,
            0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(29) and date_to(29) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt30 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,
            sum(item_tran_qty) item_30,
            sum(item_tran_selling - discount_selling) sales_30,  
            count(unique tran_no||tran_date||till_no||location_no) visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(30) and date_to(30) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt31 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            sum(item_tran_qty) item_31,
            sum(item_tran_selling - discount_selling) sales_31,  
            count(unique tran_no||tran_date||till_no||location_no) visit_31,
            0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(31) and date_to(31) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt32 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,
            sum(item_tran_qty) item_32,
            sum(item_tran_selling - discount_selling) sales_32,  
            count(unique tran_no||tran_date||till_no||location_no) visit_32,
            0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(32) and date_to(32) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),
            bskt33 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,
            sum(item_tran_qty) item_33,
            sum(item_tran_selling - discount_selling) sales_33,  
            count(unique tran_no||tran_date||till_no||location_no) visit_33,
            0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(33) and date_to(33) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),   
            bskt34 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,
            sum(item_tran_qty) item_34,
            sum(item_tran_selling - discount_selling) sales_34,  
            count(unique tran_no||tran_date||till_no||location_no) visit_34,
            0 item_35,0 sales_35,0 visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(34) and date_to(34) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),  
            bskt35 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,
            sum(item_tran_qty) item_35,
            sum(item_tran_selling - discount_selling) sales_35,  
            count(unique tran_no||tran_date||till_no||location_no) visit_35,
            0 item_36,0 sales_36,0 visit_36
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(35) and date_to(35) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),   
            bskt36 as 
   (
   select  /*+ FULL(cbi) parallel (cbi,16)  full(di) */ 
            primary_customer_identifier,
            di.subgroup_no   subgroup_no,
            max(customer_no)   customer_no,
            0 item_01,0 sales_01,0 visit_01,0 item_02,0 sales_02,0 visit_02,0 item_03,0 sales_03,0 visit_03,0 item_04,0 sales_04,0 visit_04,0 item_05,0 sales_05,0 visit_05,
            0 item_06,0 sales_06,0 visit_06,0 item_07,0 sales_07,0 visit_07,0 item_08,0 sales_08,0 visit_08,0 item_09,0 sales_09,0 visit_09,0 item_10,0 sales_10,0 visit_10,
            0 item_11,0 sales_11,0 visit_11,0 item_12,0 sales_12,0 visit_12,0 item_13,0 sales_13,0 visit_13,0 item_14,0 sales_14,0 visit_14,0 item_15,0 sales_15,0 visit_15,
            0 item_16,0 sales_16,0 visit_16,0 item_17,0 sales_17,0 visit_17,0 item_18,0 sales_18,0 visit_18,0 item_19,0 sales_19,0 visit_19,0 item_20,0 sales_20,0 visit_20,
            0 item_21,0 sales_21,0 visit_21,0 item_22,0 sales_22,0 visit_22,0 item_23,0 sales_23,0 visit_23,0 item_24,0 sales_24,0 visit_24,0 item_25,0 sales_25,0 visit_25,
            0 item_26,0 sales_26,0 visit_26,0 item_27,0 sales_27,0 visit_27,0 item_28,0 sales_28,0 visit_28,0 item_29,0 sales_29,0 visit_29,0 item_30,0 sales_30,0 visit_30,
            0 item_31,0 sales_31,0 visit_31,0 item_32,0 sales_32,0 visit_32,0 item_33,0 sales_33,0 visit_33,0 item_34,0 sales_34,0 visit_34,0 item_35,0 sales_35,0 visit_35,
            sum(item_tran_qty) item_36,
            sum(item_tran_selling - discount_selling) sales_36,  
            count(unique tran_no||tran_date||till_no||location_no) visit_36 
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between date_from(36) and date_to(36) and  
            cbi.item_no  = di.item_no and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999    
   group by primary_customer_identifier,di.subgroup_no
   ),   
   
            cust_union_all as
   (
   select  /*+ FULL(b28)  parallel (b28,4)  */  *   from bskt28  b28
   union all
   select  /*+ FULL(b29)  parallel (b29,4)  */  *   from bskt29  b29
   union all
   select  /*+ FULL(b30)  parallel (b30,4)  */  *   from bskt30  b30
   union all
   select  /*+ FULL(b31)  parallel (b31,4)  */  *   from bskt31  b31
   union all
   select  /*+ FULL(b32)  parallel (b32,4)  */  *   from bskt32  b32
   union all
   select  /*+ FULL(b33)  parallel (b33,4)  */  *   from bskt33  b33
   union all
   select  /*+ FULL(b34)  parallel (b34,4)  */  *   from bskt34  b34
   union all
   select  /*+ FULL(b35)  parallel (b35,4)  */  *   from bskt35  b35
   union all
   select  /*+ FULL(b36)  parallel (b36,4)  */  *   from bskt36  b36
   )         
   select /*+ FULL(cua)  parallel (cua,8)  */
            g_yr_00,g_mn_00,
            primary_customer_identifier,
            subgroup_no,
            max(customer_no) customer_no,
            sum(item_01),sum(sales_01),sum(visit_01),
            sum(item_02),sum(sales_02),sum(visit_02),
            sum(item_03),sum(sales_03),sum(visit_03),
            sum(item_04),sum(sales_04),sum(visit_04),
            sum(item_05),sum(sales_05),sum(visit_05),
            sum(item_06),sum(sales_06),sum(visit_06),
            sum(item_07),sum(sales_07),sum(visit_07),
            sum(item_08),sum(sales_08),sum(visit_08),
            sum(item_09),sum(sales_09),sum(visit_09),
            sum(item_10),sum(sales_10),sum(visit_10),
            sum(item_11),sum(sales_11),sum(visit_11),
            sum(item_12),sum(sales_12),sum(visit_12),
            sum(item_13),sum(sales_13),sum(visit_13),
            sum(item_14),sum(sales_14),sum(visit_14),
            sum(item_15),sum(sales_15),sum(visit_15),
            sum(item_16),sum(sales_16),sum(visit_16),
            sum(item_17),sum(sales_17),sum(visit_17),
            sum(item_18),sum(sales_18),sum(visit_18),
            sum(item_19),sum(sales_19),sum(visit_19),
            sum(item_20),sum(sales_20),sum(visit_20),
            sum(item_21),sum(sales_21),sum(visit_21),
            sum(item_22),sum(sales_22),sum(visit_22),
            sum(item_23),sum(sales_23),sum(visit_23),
            sum(item_24),sum(sales_24),sum(visit_24),
            sum(item_25),sum(sales_25),sum(visit_25),
            sum(item_26),sum(sales_26),sum(visit_26),
            sum(item_27),sum(sales_27),sum(visit_27),
            sum(item_28),sum(sales_28),sum(visit_28),
            sum(item_29),sum(sales_29),sum(visit_29),
            sum(item_30),sum(sales_30),sum(visit_30),
            sum(item_31),sum(sales_31),sum(visit_31),
            sum(item_32),sum(sales_32),sum(visit_32),
            sum(item_33),sum(sales_33),sum(visit_33),
            sum(item_34),sum(sales_34),sum(visit_34),
            sum(item_35),sum(sales_35),sum(visit_35),
            sum(item_36),sum(sales_36),sum(visit_36),
            g_date
   from     cust_union_all cua
   group by primary_customer_identifier,subgroup_no
   ;

   COMMIT;
   l_text := 'FORTH TEMP TABLE WRITTEN FOR 9 MONTHS DATA'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
   l_text := 'UPDATE STATS ON TEMP TABLES'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_SUBGROUP_MONTH_1',estimate_percent=>1, DEGREE => 32);
   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_SUBGROUP_MONTH_2',estimate_percent=>1, DEGREE => 32);
   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_SUBGROUP_MONTH_3',estimate_percent=>1, DEGREE => 32);
   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_DB_SUBGROUP_MONTH_4',estimate_percent=>1, DEGREE => 32);

   COMMIT;
   l_text := 'MAIN INSERT START'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
   
   insert /*+ APPEND parallel (prf,12) */ into cust_db_subgroup_month  prf
   with        cust_union_all as
   (
   select  /*+ FULL(t1)  parallel (t1,8)  */  *   from temp_db_subgroup_month_1  t1
   union all
   select  /*+ FULL(t2)  parallel (t2,8)  */  *   from temp_db_subgroup_month_2  t2
   union all
   select  /*+ FULL(t3)  parallel (t3,8)  */  *   from temp_db_subgroup_month_3  t3
   union all
   select  /*+ FULL(t4)  parallel (t4,8)  */  *   from temp_db_subgroup_month_4  t4
   )         
   select /*+  parallel (cua,12)  */
            g_yr_00,g_mn_00,
            primary_customer_identifier,
            subgroup_no,
            max(customer_no) customer_no,
            sum(	num_item_101	),
            sum(	sales_101	),
            sum(	num_visit_101	),
            sum(	num_item_102	),
            sum(	sales_102	),
            sum(	num_visit_102	),
            sum(	num_item_103	),
            sum(	sales_103	),
            sum(	num_visit_103	),
            sum(	num_item_104	),
            sum(	sales_104	),
            sum(	num_visit_104	),
            sum(	num_item_105	),
            sum(	sales_105	),
            sum(	num_visit_105	),
            sum(	num_item_106	),
            sum(	sales_106	),
            sum(	num_visit_106	),
            sum(	num_item_107	),
            sum(	sales_107	),
            sum(	num_visit_107	),
            sum(	num_item_108	),
            sum(	sales_108	),
            sum(	num_visit_108	),
            sum(	num_item_109	),
            sum(	sales_109	),
            sum(	num_visit_109	),
            sum(	num_item_110	),
            sum(	sales_110	),
            sum(	num_visit_110	),
            sum(	num_item_111	),
            sum(	sales_111	),
            sum(	num_visit_111	),
            sum(	num_item_112	),
            sum(	sales_112	),
            sum(	num_visit_112	),
            sum(	num_item_201	),
            sum(	sales_201	),
            sum(	num_visit_201	),
            sum(	num_item_202	),
            sum(	sales_202	),
            sum(	num_visit_202	),
            sum(	num_item_203	),
            sum(	sales_203	),
            sum(	num_visit_203	),
            sum(	num_item_204	),
            sum(	sales_204	),
            sum(	num_visit_204	),
            sum(	num_item_205	),
            sum(	sales_205	),
            sum(	num_visit_205	),
            sum(	num_item_206	),
            sum(	sales_206	),
            sum(	num_visit_206	),
            sum(	num_item_207	),
            sum(	sales_207	),
            sum(	num_visit_207	),
            sum(	num_item_208	),
            sum(	sales_208	),
            sum(	num_visit_208	),
            sum(	num_item_209	),
            sum(	sales_209	),
            sum(	num_visit_209	),
            sum(	num_item_210	),
            sum(	sales_210	),
            sum(	num_visit_210	),
            sum(	num_item_211	),
            sum(	sales_211	),
            sum(	num_visit_211	),
            sum(	num_item_212	),
            sum(	sales_212	),
            sum(	num_visit_212	),
            sum(	num_item_301	),
            sum(	sales_301	),
            sum(	num_visit_301	),
            sum(	num_item_302	),
            sum(	sales_302	),
            sum(	num_visit_302	),
            sum(	num_item_303	),
            sum(	sales_303	),
            sum(	num_visit_303	),
            sum(	num_item_304	),
            sum(	sales_304	),
            sum(	num_visit_304	),
            sum(	num_item_305	),
            sum(	sales_305	),
            sum(	num_visit_305	),
            sum(	num_item_306	),
            sum(	sales_306	),
            sum(	num_visit_306	),
            sum(	num_item_307	),
            sum(	sales_307	),
            sum(	num_visit_307	),
            sum(	num_item_308	),
            sum(	sales_308	),
            sum(	num_visit_308	),
            sum(	num_item_309	),
            sum(	sales_309	),
            sum(	num_visit_309	),
            sum(	num_item_310	),
            sum(	sales_310	),
            sum(	num_visit_310	),
            sum(	num_item_311	),
            sum(	sales_311	),
            sum(	num_visit_311	),
            sum(	num_item_312	),
            sum(	sales_312	),
            sum(	num_visit_312	),
            g_date
   from     cust_union_all cua
   group by primary_customer_identifier,subgroup_no
   ;
   
   
   g_recs_inserted         := g_recs_inserted + sql%rowcount;
   commit;

   l_text := 'UPDATE STATS ON FINAL TABLE AFTER INSERTS'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
   
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_DB_SUBGROUP_MONTH',estimate_percent=>1, DEGREE => 32);

    filter_lst.extend(1);
    filter_lst(1).ownname := 'DWH_CUST_PERFORMANCE';
    filter_lst(1).objname := 'CUST_DB_SUBGROUP_MONTH';
    DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>'DWH_CUST_PERFORMANCE',obj_filter_list=>filter_lst,options=>'gather auto');

   COMMIT;
   
    l_text      := 'TRUNCATE ALL TEMP TABLES';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_SUBGROUP_MONTH_1';
    execute immediate g_stmt; 
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_SUBGROUP_MONTH_2';
    execute immediate g_stmt; 
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_SUBGROUP_MONTH_3';
    execute immediate g_stmt; 
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.TEMP_DB_SUBGROUP_MONTH_4';
    execute immediate g_stmt;     

--************************************************************************************************** 
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
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
 
END WH_PRF_CUST_272U;
