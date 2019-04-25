--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_752E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_752E" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2009
--  Author:      M Munnik
--  Purpose:     Creates Sales Forecast SMS message.
--  Tables:      Input  - rtl_loc_bus_unit_dy,
--                        rtl_loc_dept_wk
--               Output - rtl_sms_publish
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_date               date;
g_this_wk_strt_dte   date;
g_year               number(4,0);
g_week               number(2,0);
g_accum_fcst         number(13,1);
g_sms_string         varchar2(500);

g_rec_out            rtl_sms_publish%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_752E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES SALES FORECAST SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_sales is
   with sales as
   (select   nvl(budg.business_unit_no,fcst.business_unit_no) business_unit_no,
             nvl(fcst.sales_hl_ho_fcst,0) sales_hl_ho_fcst, nvl(budg.sales_budget,0) sales_budget
   from
   (select   b.business_unit_no, sum(r.sales_hl_ho_fcst) sales_hl_ho_fcst
   from      rtl_loc_bus_unit_dy r
   join      dim_location l       on r.sk1_location_no      = l.sk1_location_no
   join      dim_business_unit b  on r.sk1_business_unit_no = b.sk1_business_unit_no
   where     l.chain_no           = 10
   and       b.business_unit_no   in(50,51,52,53,54)
   and       r.tran_date          between g_this_wk_strt_dte and g_date
   group by  b.business_unit_no) fcst
   full outer join
   (select   d.business_unit_no, sum(r.sales_budget) sales_budget
   from      rtl_loc_dept_wk r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_department d     on r.sk1_department_no = d.sk1_department_no
   where     l.chain_no           =  10
   and       d.business_unit_no   in(50,51,52,53,54)
   and       r.fin_year_no        =  g_year
   and       r.fin_week_no        =  g_week
   group by  d.business_unit_no) budg
   on        fcst.business_unit_no =  budg.business_unit_no)

   select    'C' abbr, sum(sales_hl_ho_fcst) sales_hl_ho_fcst, sum(sales_budget) sales_budget
   from      sales
   where     business_unit_no     in(51,52,53,54)

   union all

   select    'F' abbr, sales_hl_ho_fcst, sales_budget
   from      sales
   where     business_unit_no     =  50
   order by  abbr;

g_rec_in             c_sales%rowtype;

--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************
procedure local_address_variables as
begin

   if g_rec_in.sales_hl_ho_fcst <> 0 and g_rec_in.sales_budget <> 0 then
      g_accum_fcst := (g_rec_in.sales_hl_ho_fcst - g_rec_in.sales_budget) / 1000000;
   else
      l_text := 'ABORTED - no FORECAST or BUDGET values !!';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      raise_application_error (-20500,'Application error - ABORTED - no FORECAST or BUDGET values !!');
   end if;

   g_sms_string     := g_sms_string||'#'||g_rec_in.abbr||g_accum_fcst;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Insert sms record
--**************************************************************************************************
procedure insert_sms_rec as
begin

   g_sms_string                     := g_sms_string||'#Excl_Waste_Recove';
   g_rec_out.calendar_datetime      := sysdate;
   g_rec_out.record_type            := 'FLSHFA';
   g_rec_out.processed_ind          := 0;
   g_rec_out.sms_text               := g_sms_string;
   g_rec_out.last_updated_date      := g_date;

   insert into rtl_sms_publish values g_rec_out;

   g_recs_inserted  := g_recs_inserted + sql%rowcount;

   g_rec_out.record_type            := 'FLSHFB';
   insert into rtl_sms_publish values g_rec_out;

   g_recs_inserted  := g_recs_inserted + sql%rowcount;

   commit;

   exception
      when dwh_errors.e_insert_error then
         l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
         dwh_log.record_error(l_module_name,sqlcode,l_message);
         raise;

      when others then
         l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
         dwh_log.record_error(l_module_name,sqlcode,l_message);
         raise;

end insert_sms_rec;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
   p_success := false;
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'CREATION OF SALES FORECAST SMS STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   select this_week_start_date, fin_year_no, fin_week_no
   into   g_this_wk_strt_dte,   g_year,      g_week
   from   dim_calendar
   where  calendar_date = g_date;

   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   update      rtl_sms_publish
   set         processed_ind = 1
   where       record_type in('FLSHFA','FLSHFB')
   and         processed_ind = 0;

   g_sms_string     := 'Fcst-Budget#'||trunc(sysdate);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
   open c_sales;
   loop
      fetch c_sales into g_rec_in;
      exit when c_sales%NOTFOUND;
      g_recs_read := g_recs_read + 1;
      local_address_variables;
   end loop;
   close c_sales;

   insert_sms_rec;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
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

end wh_prf_corp_752e;
