--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_270U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_270U" (p_forall_limit in integer,p_success out boolean) AS 

-- *************************************************************************************************
-- * Notes from 12.2 upgrade performance tuning
-- *************************************************************************************************
-- Date:   2019-05-02 
-- Author: Paul Wakefield
-- Rewrote query to bypass temporary tables.
-- **************************************************************************************************

--**************************************************************************************************
--  Date:        Oct 2015
--  Author:      Alastair de Wet
--  Purpose:     TAKE ON OR RESTRUCTURE DEPTH AND BREADTH AT DEPARTMENT LEVEL
--  Tables:      Input  - cust_basket_item  
--               Output - cust_db_dept_month
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_270U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TAKEON/RESTRUCTURE cust_db_dept_month EX cust_basket_item';
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
    
    l_text := 'TAKEON OF cust_db_dept_month EX cust_basket_item STARTED AT '||
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

   g_run_date := g_this_mn_end_date + 6;
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;  
   
   l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

-- CLEAR TABLES DOWN ---
--   g_stmt      := 'Alter table  DWH_CUST_PERFORMANCE.CUST_DB_DEPT_MONTH truncate  subpartition for ('||g_yr_00||','||g_mn_00||') update global indexes';
   l_text      := 'TRUNCATE ALL TABLES'; 

   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    g_stmt      := 'TRUNCATE table  DWH_CUST_PERFORMANCE.CUST_DB_DEPT_MONTH';
    execute immediate g_stmt;  

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

-- cust_db_dept_month
-- department_no

   l_text := 'MAIN INSERT START'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
   
   insert /*+ APPEND parallel (prf,4) */ into cust_db_dept_month  prf
   with bskt_det as (
         select  /*+ full(cbi) parallel(8) no_gather_optimizer_statistics */
                  primary_customer_identifier,
                  di.department_no,
                  customer_no,
                  case when tran_date between date_from(1) and date_to(1) then 1
                       when tran_date between date_from(2) and date_to(2) then 2
                       when tran_date between date_from(3) and date_to(3) then 3
                       when tran_date between date_from(4) and date_to(4) then 4
                       when tran_date between date_from(5) and date_to(5) then 5
                       when tran_date between date_from(6) and date_to(6) then 6
                       when tran_date between date_from(7) and date_to(7) then 7
                       when tran_date between date_from(8) and date_to(8) then 8
                       when tran_date between date_from(9) and date_to(9) then 9
                       when tran_date between date_from(10) and date_to(10) then 10
                       when tran_date between date_from(11) and date_to(11) then 11
                       when tran_date between date_from(12) and date_to(12) then 12
                       when tran_date between date_from(13) and date_to(13) then 13
                       when tran_date between date_from(14) and date_to(14) then 14
                       when tran_date between date_from(15) and date_to(15) then 15
                       when tran_date between date_from(16) and date_to(16) then 16
                       when tran_date between date_from(17) and date_to(17) then 17
                       when tran_date between date_from(18) and date_to(18) then 18
                       when tran_date between date_from(19) and date_to(19) then 19
                       when tran_date between date_from(20) and date_to(20) then 20
                       when tran_date between date_from(21) and date_to(21) then 21
                       when tran_date between date_from(22) and date_to(22) then 22
                       when tran_date between date_from(23) and date_to(23) then 23
                       when tran_date between date_from(24) and date_to(24) then 24
                       when tran_date between date_from(25) and date_to(25) then 25
                       when tran_date between date_from(26) and date_to(26) then 26
                       when tran_date between date_from(27) and date_to(27) then 27
                       when tran_date between date_from(28) and date_to(28) then 28
                       when tran_date between date_from(29) and date_to(29) then 29
                       when tran_date between date_from(30) and date_to(30) then 30
                       when tran_date between date_from(31) and date_to(31) then 31
                       when tran_date between date_from(32) and date_to(32) then 32
                       when tran_date between date_from(33) and date_to(33) then 33
                       when tran_date between date_from(34) and date_to(34) then 34
                       when tran_date between date_from(35) and date_to(35) then 35
                       when tran_date between date_from(36) and date_to(36) then 36
                  end mnth,
                  item_tran_qty,
                  item_tran_selling,
                  discount_selling,
                  tran_no||tran_date||till_no||location_no visit
         from     cust_basket_item cbi,
                  dim_item di
         where    tran_date between date_from(36) and date_to(1) and  
                  cbi.item_no  = di.item_no and
                  tran_type in ('S','V','R') and
                  primary_customer_identifier is not null and
                  primary_customer_identifier <> 0  and
                  primary_customer_identifier not between 6007851400000000 and 6007851499999999),
      bskt_bd as (
         select   primary_customer_identifier,
                  department_no,
                  max(customer_no) customer_no,
                  mnth,
                  sum(item_tran_qty) item,
                  sum(item_tran_selling - discount_selling) sales,
                  count(distinct visit) visit
         from     bskt_det
         group by primary_customer_identifier, department_no, mnth),
      pvt as (
         select   primary_customer_identifier,
                  department_no,
                  customer_no,
                  NVL("1_ITEM",0) ITEM_01,NVL("1_SALES",0) SALES_01, NVL("1_VISIT",0) VISIT_01,
                  NVL("2_ITEM",0) ITEM_02,NVL("2_SALES",0) SALES_02, NVL("2_VISIT",0) VISIT_02,
                  NVL("3_ITEM",0) ITEM_03,NVL("3_SALES",0) SALES_03, NVL("3_VISIT",0) VISIT_03,
                  NVL("4_ITEM",0) ITEM_04,NVL("4_SALES",0) SALES_04, NVL("4_VISIT",0) VISIT_04,
                  NVL("5_ITEM",0) ITEM_05,NVL("5_SALES",0) SALES_05, NVL("5_VISIT",0) VISIT_05,
                  NVL("6_ITEM",0) ITEM_06,NVL("6_SALES",0) SALES_06, NVL("6_VISIT",0) VISIT_06,
                  NVL("7_ITEM",0) ITEM_07,NVL("7_SALES",0) SALES_07, NVL("7_VISIT",0) VISIT_07,
                  NVL("8_ITEM",0) ITEM_08,NVL("8_SALES",0) SALES_08, NVL("8_VISIT",0) VISIT_08,
                  NVL("9_ITEM",0) ITEM_09,NVL("9_SALES",0) SALES_09, NVL("9_VISIT",0) VISIT_09,
                  NVL("10_ITEM",0) ITEM_10,NVL("10_SALES",0) SALES_10, NVL("10_VISIT",0) VISIT_10,
                  NVL("11_ITEM",0) ITEM_11,NVL("11_SALES",0) SALES_11, NVL("11_VISIT",0) VISIT_11,
                  NVL("12_ITEM",0) ITEM_12,NVL("12_SALES",0) SALES_12, NVL("12_VISIT",0) VISIT_12,
                  NVL("13_ITEM",0) ITEM_13,NVL("13_SALES",0) SALES_13, NVL("13_VISIT",0) VISIT_13,
                  NVL("14_ITEM",0) ITEM_14,NVL("14_SALES",0) SALES_14, NVL("14_VISIT",0) VISIT_14,
                  NVL("15_ITEM",0) ITEM_15,NVL("15_SALES",0) SALES_15, NVL("15_VISIT",0) VISIT_15,
                  NVL("16_ITEM",0) ITEM_16,NVL("16_SALES",0) SALES_16, NVL("16_VISIT",0) VISIT_16,
                  NVL("17_ITEM",0) ITEM_17,NVL("17_SALES",0) SALES_17, NVL("17_VISIT",0) VISIT_17,
                  NVL("18_ITEM",0) ITEM_18,NVL("18_SALES",0) SALES_18, NVL("18_VISIT",0) VISIT_18,
                  NVL("19_ITEM",0) ITEM_19,NVL("19_SALES",0) SALES_19, NVL("19_VISIT",0) VISIT_19,
                  NVL("20_ITEM",0) ITEM_20,NVL("20_SALES",0) SALES_20, NVL("20_VISIT",0) VISIT_20,
                  NVL("21_ITEM",0) ITEM_21,NVL("21_SALES",0) SALES_21, NVL("21_VISIT",0) VISIT_21,
                  NVL("22_ITEM",0) ITEM_22,NVL("22_SALES",0) SALES_22, NVL("22_VISIT",0) VISIT_22,
                  NVL("23_ITEM",0) ITEM_23,NVL("23_SALES",0) SALES_23, NVL("23_VISIT",0) VISIT_23,
                  NVL("24_ITEM",0) ITEM_24,NVL("24_SALES",0) SALES_24, NVL("24_VISIT",0) VISIT_24,
                  NVL("25_ITEM",0) ITEM_25,NVL("25_SALES",0) SALES_25, NVL("25_VISIT",0) VISIT_25,
                  NVL("26_ITEM",0) ITEM_26,NVL("26_SALES",0) SALES_26, NVL("26_VISIT",0) VISIT_26,
                  NVL("27_ITEM",0) ITEM_27,NVL("27_SALES",0) SALES_27, NVL("27_VISIT",0) VISIT_27,
                  NVL("28_ITEM",0) ITEM_28,NVL("28_SALES",0) SALES_28, NVL("28_VISIT",0) VISIT_28,
                  NVL("29_ITEM",0) ITEM_29,NVL("29_SALES",0) SALES_29, NVL("29_VISIT",0) VISIT_29,
                  NVL("30_ITEM",0) ITEM_30,NVL("30_SALES",0) SALES_30, NVL("30_VISIT",0) VISIT_30,
                  NVL("31_ITEM",0) ITEM_31,NVL("31_SALES",0) SALES_31, NVL("31_VISIT",0) VISIT_31,
                  NVL("32_ITEM",0) ITEM_32,NVL("32_SALES",0) SALES_32, NVL("32_VISIT",0) VISIT_32,
                  NVL("33_ITEM",0) ITEM_33,NVL("33_SALES",0) SALES_33, NVL("33_VISIT",0) VISIT_33,
                  NVL("34_ITEM",0) ITEM_34,NVL("34_SALES",0) SALES_34, NVL("34_VISIT",0) VISIT_34,
                  NVL("35_ITEM",0) ITEM_35,NVL("35_SALES",0) SALES_35, NVL("35_VISIT",0) VISIT_35,
                  NVL("36_ITEM",0) ITEM_36,NVL("36_SALES",0) SALES_36, NVL("36_VISIT",0) VISIT_36
      from bskt_bd
      pivot (sum(item) as item, sum(sales) as sales, sum(visit) as visit
             for mnth in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36)))
   select g_yr_00,g_mn_00,
          primary_customer_identifier,
          department_no,
          max(customer_no) customer_no,
          sum(item_01) item_01, sum(sales_01) sales_01, sum(visit_01) visit_01,
          sum(item_02) item_02, sum(sales_02) sales_02, sum(visit_02) visit_02,
          sum(item_03) item_03, sum(sales_03) sales_03, sum(visit_03) visit_03,
          sum(item_04) item_04, sum(sales_04) sales_04, sum(visit_04) visit_04,
          sum(item_05) item_05, sum(sales_05) sales_05, sum(visit_05) visit_05,
          sum(item_06) item_06, sum(sales_06) sales_06, sum(visit_06) visit_06,
          sum(item_07) item_07, sum(sales_07) sales_07, sum(visit_07) visit_07,
          sum(item_08) item_08, sum(sales_08) sales_08, sum(visit_08) visit_08,
          sum(item_09) item_09, sum(sales_09) sales_09, sum(visit_09) visit_09,
          sum(item_10) item_10, sum(sales_10) sales_10, sum(visit_10) visit_10,
          sum(item_11) item_11, sum(sales_11) sales_11, sum(visit_11) visit_11,
          sum(item_12) item_12, sum(sales_12) sales_12, sum(visit_12) visit_12,
          sum(item_13) item_13, sum(sales_13) sales_13, sum(visit_13) visit_13,
          sum(item_14) item_14, sum(sales_14) sales_14, sum(visit_14) visit_14,
          sum(item_15) item_15, sum(sales_15) sales_15, sum(visit_15) visit_15,
          sum(item_16) item_16, sum(sales_16) sales_16, sum(visit_16) visit_16,
          sum(item_17) item_17, sum(sales_17) sales_17, sum(visit_17) visit_17,
          sum(item_18) item_18, sum(sales_18) sales_18, sum(visit_18) visit_18,
          sum(item_19) item_19, sum(sales_19) sales_19, sum(visit_19) visit_19,
          sum(item_20) item_20, sum(sales_20) sales_20, sum(visit_20) visit_20,
          sum(item_21) item_21, sum(sales_21) sales_21, sum(visit_21) visit_21,
          sum(item_22) item_22, sum(sales_22) sales_22, sum(visit_22) visit_22,
          sum(item_23) item_23, sum(sales_23) sales_23, sum(visit_23) visit_23,
          sum(item_24) item_24, sum(sales_24) sales_24, sum(visit_24) visit_24,
          sum(item_25) item_25, sum(sales_25) sales_25, sum(visit_25) visit_25,
          sum(item_26) item_26, sum(sales_26) sales_26, sum(visit_26) visit_26,
          sum(item_27) item_27, sum(sales_27) sales_27, sum(visit_27) visit_27,
          sum(item_28) item_28, sum(sales_28) sales_28, sum(visit_28) visit_28,
          sum(item_29) item_29, sum(sales_29) sales_29, sum(visit_29) visit_29,
          sum(item_30) item_30, sum(sales_30) sales_30, sum(visit_30) visit_30,
          sum(item_31) item_31, sum(sales_31) sales_31, sum(visit_31) visit_31,
          sum(item_32) item_32, sum(sales_32) sales_32, sum(visit_32) visit_32,
          sum(item_33) item_33, sum(sales_33) sales_33, sum(visit_33) visit_33,
          sum(item_34) item_34, sum(sales_34) sales_34, sum(visit_34) visit_34,
          sum(item_35) item_35, sum(sales_35) sales_35, sum(visit_35) visit_35,
          sum(item_36) item_36, sum(sales_36) sales_36, sum(visit_36) visit_36,
          g_date
     from pvt 
   group by primary_customer_identifier,
            department_no;


   g_recs_inserted         := g_recs_inserted + sql%rowcount;
   commit;

   l_text := 'UPDATE STATS ON FINAL TABLE AFTER INSERTS'; 
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
   
--   DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_DB_DEPT_MONTH',estimate_percent=>1, DEGREE => 32);

    filter_lst.extend(1);
    filter_lst(1).ownname := 'DWH_CUST_PERFORMANCE';
    filter_lst(1).objname := 'CUST_DB_DEPT_MONTH';
    DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>'DWH_CUST_PERFORMANCE',obj_filter_list=>filter_lst,options=>'gather auto');

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
 
END WH_PRF_CUST_270U;
