-- ****** Object: Procedure W7131037.WH_PRF_CUST_204U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_204U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_basket_tender_mth fact table in the performance layer
--               with input ex basket tender transactions
--  Tables:      Input  - cust_basket_tender
--               Output - cust_basket_tendfer_mth
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
-- Note: This version Attempts to do a bulk insert / update
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_recs_read            integer       :=  0;
g_recs_updated         integer       :=  0;
g_recs_deleted         integer       :=  0;
g_recs_inserted        integer       :=  0;
g_truncate_count       integer       :=  0;
g_this_mn_start_date   date ;
g_this_mn_end_date     date;
g_last_mn_fin_year_no  integer;
g_last_mn_fin_month_no integer;
g_stmt                 varchar(300);
g_run_date             date;
g_date                 date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_204U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLUP BASKET TENDER MONTH EX BASKET TENDER DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin


insert /*+ APPEND parallel (prf,4) */ into cust_basket_tender_mth prf
 select    /*+ full(bt) parallel (bt,4) */
                   g_last_mn_fin_year_no,
                   g_last_mn_fin_month_no,
                   bt.primary_customer_identifier,
                   max(bt.customer_no) customer_no,
                   round(sum(case when tender_type_code = 1290 then tender_selling-change_selling end),2) as isc_tender_amt,
                   round(sum(case when tender_type_code = 1292 then tender_selling-change_selling end),2) as ww_visa_tender_amt,
                   round(sum(case when tender_type_code = 1294 then tender_selling-change_selling end),2) as alien_tender_amt,
                   round(sum(case when tender_type_code = 1215 then tender_selling-change_selling end),2) as cash_tender_amt,
                   round(sum(case when tender_type_code not in (1290,1292,1294,1215) then tender_selling-change_selling end),2) as other_tender_amt,
                   max(case when tender_type_code = 1290 then 1 end) as isc_tender_cust_count,
                   max(case when tender_type_code = 1292 then 1 end) as ww_visa_cust_count,
                   max(case when tender_type_code = 1294 then 1 end) as alien_tender_cust_count,
                   max(case when tender_type_code = 1215 then 1 end) as cash_tender_cust_count,
                   max(case when tender_type_code not in (1290,1292,1294,1215) then 1 end) as other_tender_cust_count,
                   G_DATE
         from      cust_basket_tender bt
         where     bt.tran_date                               between  g_this_mn_start_date and g_this_mn_end_date
         and       bt.primary_customer_identifier             is not null
         and       bt.primary_customer_identifier             <> 998
         and       primary_customer_identifier                <> 0
         and       substr(bt.primary_customer_identifier,1,8) <> 60078514
         AND       payment_account_no is null
         group by  bt.primary_customer_identifier;

      g_recs_inserted := g_recs_inserted + sql%rowcount;

      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert;



--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';


    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Call the bulk routines
--**************************************************************************************************
     select last_yr_fin_year_no, last_mn_fin_month_no
     into g_last_mn_fin_year_no, g_last_mn_fin_month_no
     from dim_control;

     select  distinct this_mn_start_date,this_mn_end_date
     into    g_this_mn_start_date,g_this_mn_end_date
     from    dim_calendar_wk
     where   fin_month_no = g_last_mn_fin_month_no
     and     fin_year_no  = g_last_mn_fin_year_no;

    l_text := 'Dates being rolled up '||g_this_mn_start_date||' thru '||g_this_mn_end_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Year/Month being procesed '||g_last_mn_fin_year_no||' '||g_last_mn_fin_month_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_run_date := g_this_mn_end_date + 8;
    if trunc(sysdate) <> g_run_date then
       l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       p_success := true;
       return;
    end if;

    l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_stmt   := 'Alter table  W7131037.CUST_BASKET_TENDER_MTH truncate  subpartition for ('||g_last_mn_fin_year_no||','||g_last_mn_fin_month_no||') update global indexes';
    l_text   := g_stmt;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate g_stmt;


--    delete from cust_basket_tender_mth
--    where  fin_month_no = g_last_mn_fin_month_no and
--           fin_year_no  = g_last_mn_fin_year_no;

--    g_recs_deleted :=  sql%rowcount;
    commit;


    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    g_recs_read := g_recs_updated + g_recs_inserted;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   if g_recs_read <> g_recs_inserted + g_recs_updated  then
--      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      p_success := false;
--      l_message := 'ERROR - Record counts do not balance see log file';
--      dwh_log.record_error(l_module_name,sqlcode,l_message);
--      raise_application_error (-20246,'Record count error - see log files');
--   end if;


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
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;
end wh_prf_cust_204u;
