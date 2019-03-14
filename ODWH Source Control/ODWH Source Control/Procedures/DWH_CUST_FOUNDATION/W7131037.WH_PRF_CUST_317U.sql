-- ****** Object: Procedure W7131037.WH_PRF_CUST_317U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_317U"      (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Nov 2017
--  Author:      Alastair de Wet
--  Purpose:     Create WEEKDAY SHOPPER MART table in the performance layer
--               with input ex basket ITEM transactions
--  Tables:      Input  - cust_basket_item
--               Output - cust_mart_weekday_shopper_type
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_317U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE WEEKDAY SHOPPER MART EX BASKET ITEM DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin

insert /*+ APPEND  */ into cust_mart_weekday_shopper_type prf
with
   fdshp as (
   select  /*+ FULL(cb) parallel (cb,8)  */
            primary_customer_identifier,
            tran_no,tran_date,till_no,location_no,
            max(case when  to_char(cb.tran_date,'DY')  in ('SAT','SUN')
                     then 1 else 0 end) weekend_shop_fd ,
            max(case when  to_char(cb.tran_date,'DY')  not in ('SAT','SUN')
                     then 1 else 0 end) weekday_shop_fd
--            count(unique tran_no||tran_date||till_no||location_no) visits
   from     cust_basket_item cb,
            dim_item di
   where    tran_date between g_this_mn_end_date - 56 and g_this_mn_end_date and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0 and
            primary_customer_identifier <> 998 and
            di.business_unit_no = 50 and
            cb.item_no = di.item_no
   group by primary_customer_identifier,tran_no,tran_date,till_no,location_no
            ),

   fdtot as (
   select   /*+ FULL(fd) parallel (fd,8)  */
            primary_customer_identifier,
            sum(weekend_shop_fd) weekend_shop_fd_tot,
            sum(weekday_shop_fd) weekday_shop_fd_tot
   from     fdshp fd
   group by primary_customer_identifier
            ),
   fdseg as (
   select   /*+ FULL(fd) parallel (fd,8)  */
            primary_customer_identifier,
            case when  weekday_shop_fd_tot = 0      then 'Weekend only'
                 when  weekend_shop_fd_tot = 0      then 'Weekday only'
                 when  weekend_shop_fd_tot <> 0 and weekday_shop_fd_tot <> 0  and
                       weekday_shop_fd_tot / weekend_shop_fd_tot > 1.59
                                                    then 'Predominantly weekday'
                 when  weekend_shop_fd_tot <> 0 and weekday_shop_fd_tot <> 0  and
                       weekend_shop_fd_tot / weekday_shop_fd_tot > 1.59
                                                    then 'Predominantly weekend'
                 else 'No preference'
            end  pref_fd,
            '' pref_ch
   from     fdtot fd
            ),
   chshp as (

   select  /*+ FULL(cb) parallel (cb,8)  */
            primary_customer_identifier,
            tran_no,tran_date,till_no,location_no,
            max(case when  to_char(cb.tran_date,'DY')  in ('SAT','SUN')
                     then 1 else 0 end) weekend_shop_ch ,
            max(case when  to_char(cb.tran_date,'DY')  not in ('SAT','SUN')
                     then 1 else 0 end) weekday_shop_ch
--            count(unique tran_no||tran_date||till_no||location_no) visits
   from     cust_basket_item cb,
            dim_item di
   where    tran_date between g_this_mn_end_date - 126 and g_this_mn_end_date and
            tran_type in ('S','V','R') and
            primary_customer_identifier is not null and
            primary_customer_identifier <> 0 and
            primary_customer_identifier <> 998 and
            di.business_unit_no  in (51, 52, 52, 53, 54, 55) and
            cb.item_no = di.item_no
   group by primary_customer_identifier,tran_no,tran_date,till_no,location_no ),

   chtot as (
   select   /*+ FULL(ch) parallel (ch,8)  */
            primary_customer_identifier,
            sum(weekend_shop_ch) weekend_shop_ch_tot,
            sum(weekday_shop_ch) weekday_shop_ch_tot
   from     chshp ch
   group by primary_customer_identifier
            ),
   chseg as (
   select   /*+ FULL(ch) parallel (ch,8)  */
            primary_customer_identifier,'' pref_fd,
            case when  weekday_shop_ch_tot = 0      then 'Weekend only'
                 when  weekend_shop_ch_tot = 0      then 'Weekday only'
                 when  weekend_shop_ch_tot <> 0 and weekday_shop_ch_tot <> 0  and
                       weekday_shop_ch_tot / weekend_shop_ch_tot > 1.59
                                                    then 'Predominantly weekday'
                 when  weekend_shop_ch_tot <> 0 and weekday_shop_ch_tot <> 0  and
                       weekend_shop_ch_tot / weekday_shop_ch_tot > 1.59
                                                    then 'Predominantly weekend'
                 else 'No preference'
            end  pref_ch
   from     chtot ch
            ),
   cust_union_all as
   (
   select  /*+ FULL(fdseg)  parallel (fdseg,4)  */  *   from fdseg
   union all
   select  /*+ FULL(chseg)  parallel (chseg,4)  */  *   from chseg
   )
   select primary_customer_identifier,
          g_last_mn_fin_year_no,
          g_last_mn_fin_month_no,
          max(pref_fd) pref_fd,
          max(pref_ch) pref_ch,
          g_date
   from   cust_union_all
   group by primary_customer_identifier

;



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

    g_run_date := g_this_mn_end_date + 4;
    if trunc(sysdate) <> g_run_date then
       l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       p_success := true;
       return;
    end if;

    l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DROP DATA FOR MONTH IF IT EXISTS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    delete from cust_mart_weekday_shopper_type
    where  fin_month_no = g_last_mn_fin_month_no and
           fin_year_no  = g_last_mn_fin_year_no;

    g_recs_deleted :=  sql%rowcount;
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
end wh_prf_cust_317u;
