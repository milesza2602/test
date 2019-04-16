--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_223U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_223U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        April 2011
--  Author:      Alastair de Wet
--  Purpose:     Roll up for curent month values ex basket item
--  Tables:      Input  - cust_basket_item
--               Output - temp_cust_basket_item_c
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


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_yr_00               number;
g_mn_00               number;
g_this_mn_start_date  date;
g_this_mn_end_date    date;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_223U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE temp_cust_basket_item_c EX cust_basket_item';
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

    l_text := 'LOAD OF temp_cust_basket_item_c EX cust_basket_item STARTED AT '||
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
    where  fin_year_no = g_yr_00 and
           fin_month_no = g_mn_00 and
           fin_day_no   = 1;



   l_text := 'Month being processed:= '||
             g_this_mn_start_date || g_this_mn_end_date ;

   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'truncate table dwh_cust_performance.temp_cust_dept_mn_c';
   l_text := 'Truncate temp table  dwh_cust_performance.temp_cust_dept_mn_c' ;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert into temp_cust_dept_mn_c
   select   g_yr_00,g_mn_00,
            primary_customer_identifier,
            di.department_no,
            max(customer_no) customer_no,
            sum(item_tran_qty) num_item,
            sum(item_tran_qty) num_item_03_mth,
            sum(item_tran_qty) num_item_06_mth,
            sum(item_tran_qty) num_item_12_mth,
            sum(item_tran_selling - discount_selling) sales,
            sum(item_tran_selling - discount_selling) sales_03_mth,
            sum(item_tran_selling - discount_selling) sales_06_mth,
            sum(item_tran_selling - discount_selling) sales_12_mth,
            count(unique tran_no) num_visit  ,
            count(unique tran_no) num_visit_03_mth ,
            count(unique tran_no) num_visit_06_mth ,
            count(unique tran_no) num_visit_12_mth ,
            0,0,
            max(g_date) as last_updated_date
   from     cust_basket_item cbi,
            dim_item di
   where    tran_date between g_this_mn_start_date and g_this_mn_end_date  and
            cbi.item_no  = di.item_no and
            tran_type in ('S','V') and
            primary_customer_identifier <> 998 and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0  and
            primary_customer_identifier not between 6007851400000000 and 6007851499999999

   group by primary_customer_identifier,department_no;

   g_recs_inserted         := g_recs_inserted + sql%rowcount;

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

END WH_PRF_CUST_223U;
