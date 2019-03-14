-- ****** Object: Procedure W7131037.WH_PRF_CUST_178TO Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_178TO" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create  cust_wod_reward_swipe_cnt_mth fact table in the performance layer
--               with input ex basket transactions
--  Tables:      Input  - cust_basket
--               Output - cust_wod_reward_swipe_cnt_mth
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_178U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLUP SWIPE COUNT EX BASKET DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert as
begin

insert /*+ APPEND parallel (prf,8) */ into cust_wod_reward_swipe_cnt_mth prf
with baskt as (
select   /*+ Full(cb) Full(dl) Parallel(8) */  cb.*
from     cust_basket  cb,
         dim_location dl
where    cb.tran_date between g_this_mn_start_date and g_this_mn_end_date
and      cb.location_no = dl.location_no
and      dl.area_no in(9951,9952)
and      cb.tran_type = 'S'
and      cb.primary_customer_identifier <> 998
               ),
     bcount as (
select   /*+ Full(bk) Parallel(8) */
         bk.location_no,
         count(distinct bk.loyalty_ww_swipe_no) loyalty_swipe_count ,
         count(bk.loyalty_ext_swipe_no) myschool_swipe_count,
         count(*) total_tran_count
from     baskt bk
group by bk.location_no
               ),
     aux  as   (
select   /*+ Full(bk) full(ax) Parallel(8) */
         bk.location_no,
         sum(ax.wreward_sales_value) wreward_sales_value
from     baskt bk,
         cust_basket_aux ax
where    bk.location_no=ax.location_no
and      bk.till_no=ax.till_no
and      bk.tran_no=ax.tran_no
and      bk.tran_date=ax.tran_date
and      ax.tran_date  between g_this_mn_start_date and g_this_mn_end_date
group by bk.location_no
                )
select   /*+ Full(bc) full(au) Parallel(8) */
         g_last_mn_fin_year_no,
         g_last_mn_fin_month_no,
         bc.location_no,
         bc.loyalty_swipe_count,
         bc.myschool_swipe_count,
         bc.total_tran_count,
         au.wreward_sales_value,
         g_date
from     bcount bc,aux au
where    bc.location_no = au.location_no(+)
order by bc.location_no ;


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

---- TAKE ON
     g_last_mn_fin_year_no  := 2016;
     g_last_mn_fin_month_no := 08;

for x in 8..8 loop

     g_last_mn_fin_month_no := x;

     select  distinct this_mn_start_date,this_mn_end_date
     into    g_this_mn_start_date,g_this_mn_end_date
     from    dim_calendar_wk
     where   fin_month_no = g_last_mn_fin_month_no
     and     fin_year_no  = g_last_mn_fin_year_no;



    l_text := 'Dates being rolled up '||g_this_mn_start_date||' thru '||g_this_mn_end_date ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Year/Month being procesed '||g_last_mn_fin_year_no||' '||g_last_mn_fin_month_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_run_date := g_this_mn_end_date + 3;
--    if trunc(sysdate) <> g_run_date then
--       l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
--       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--       p_success := true;
--       return;
--    end if;

--    l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--    g_stmt   := 'Alter table  W7131037.cust_wod_reward_swipe_cnt_mth truncate  subpartition for ('||g_last_mn_fin_year_no||','||g_last_mn_fin_month_no||') update global indexes';
--    l_text   := g_stmt;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    execute immediate g_stmt;

--    delete from cust_wod_reward_swipe_cnt_mth
--    where  fin_month_no = g_last_mn_fin_month_no and
--           fin_year_no  = g_last_mn_fin_year_no;

    g_recs_deleted :=  sql%rowcount;
    commit;


    l_text := 'BULK INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert;

    g_recs_read := g_recs_updated + g_recs_inserted;

end loop;
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
end wh_prf_cust_178to;
