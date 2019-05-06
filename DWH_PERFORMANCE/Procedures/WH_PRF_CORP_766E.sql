--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_766E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_766E" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        June 2013
--  Author:      A de Wet
--  Purpose:     Creates Weekly Online sales SMS message.
--  Tables:      Input  - rtl_loc_item_dy_rms_dense
--               Output - rtl_sms_publish
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  02 June 2017 Theo Filander :Change Chg-6315   Exclude Now Now Stores
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
g_store_cnt          number        :=  0;

g_date               date;
g_fin_week_no        number(2);
g_fin_year_no        number(4);
g_ly_fin_week_no     number(2);
g_ly_fin_year_no     number(4);

g_sms_string         varchar2(500);

g_rec_out            rtl_sms_publish%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_766E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES WEEKLY ONLINE SALES SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_sales is
with first_pass as (
select cal.fin_year_no, cal.fin_week_no
, itm.business_unit_no, itm.business_unit_name
, sum(rms.sales) sales
, sum(rms.sales_qty) sales_units
, 0 as online_sales
, 0 as online_sales_units
, 0 as ly_ol_sales
, 0 as ly_ol_sales_units
from rtl_loc_item_dy_rms_dense rms
, dim_calendar cal
, dim_location loc
, dim_item itm
where rms.post_date = cal.calendar_date
and rms.sk1_location_no = loc.sk1_location_no
and rms.sk1_item_no = itm.sk1_item_no
and cal.fin_year_no = g_fin_year_no
and cal.fin_week_no = g_fin_week_no
and loc.area_no = 9951
and loc.st_store_type <> 'NN'
group by cal.fin_year_no, cal.fin_week_no
, itm.business_unit_no, itm.business_unit_name

union all

select cal.fin_year_no, cal.fin_week_no
, itm.business_unit_no, itm.business_unit_name
, 0 as sales
, 0 as sales_units
, sum(onl.online_sales) online_sales
, sum(onl.online_sales_qty) online_sales_units
, 0 as ly_ol_sales
, 0 as ly_ol_sales_units
from rtl_loc_item_dy_wwo_sale onl
, dim_calendar cal
, dim_location loc
, dim_item itm
where onl.post_date = cal.calendar_date
and onl.sk1_location_no = loc.sk1_location_no
and onl.sk1_item_no = itm.sk1_item_no
and cal.fin_year_no = g_fin_year_no 
and cal.fin_week_no = g_fin_week_no
and loc.area_no = 9951
and loc.st_store_type <> 'NN'
group by cal.fin_year_no, cal.fin_week_no
, itm.business_unit_no, itm.business_unit_name

union all

select cal.fin_year_no, cal.fin_week_no
, itm.business_unit_no, itm.business_unit_name
, 0 as sales
, 0 as sales_units
, 0 as online_sales
, 0 as online_sales_units
, sum(onl.online_sales) ly_ol_sales
, sum(onl.online_sales_qty) ly_ol_sales_units
from rtl_loc_item_dy_wwo_sale onl
, dim_calendar cal
, dim_location loc
, dim_item itm
where onl.post_date = cal.calendar_date
and onl.sk1_location_no = loc.sk1_location_no
and onl.sk1_item_no = itm.sk1_item_no
and cal.fin_year_no = g_ly_fin_year_no  
and cal.fin_week_no = g_ly_fin_week_no
and loc.area_no = 9951
and loc.st_store_type <> 'NN'
group by cal.fin_year_no, cal.fin_week_no
, itm.business_unit_no, itm.business_unit_name
),

second_pass as (
select   business_unit_no, business_unit_name,
         sum(sales) as sales, sum(sales_units) as sales_units,
         sum(online_sales) as online_sales, sum(online_sales_units) as online_sales_units,
         sum(ly_ol_sales) ly_ol_sales , sum(ly_ol_sales_units) ly_ol_sales_units
from     first_pass
group by business_unit_no, business_unit_name
order by business_unit_no, business_unit_name
),
third_pass as (
select ( case business_unit_no 
            when 50 then 3
            when 51 then 4
            when 52 then 5
            when 54 then 6
            when 55 then 7
            when 53 then 9
            when 70 then 99
            
       end) as seq,  
       substr( business_unit_name,1,1) as bu,
       sales,sales_units,online_sales,online_sales_units,ly_ol_sales,ly_ol_sales_units
from   second_pass
 

union all
 
select  1 as seq,' ' as bu,
        sum(sales) as sales, sum(sales_units) as sales_units,
        sum(online_sales) as online_sales, sum(online_sales_units) as online_sales_units,
        sum(ly_ol_sales) ly_ol_sales , sum(ly_ol_sales_units) ly_ol_sales_units
from    second_pass

union all

select  2 as seq,'T' as bu,
        sum(sales) as sales, sum(sales_units) as sales_units,
        sum(online_sales) as online_sales, sum(online_sales_units) as online_sales_units,
        sum(ly_ol_sales) ly_ol_sales , sum(ly_ol_sales_units) ly_ol_sales_units
from    second_pass
where   business_unit_no <> 70
)
select   tp.*,
         round(tp.online_sales/1000,1) as sms_online_sales,
         --         round((tp.online_sales-tp.ly_ol_sales) * 100 / tp.ly_ol_sales,1) as sms_perc_on_ly,
         (case TP.LY_OL_SALES
            when 0 then 0
            else ROUND((TP.ONLINE_SALES-TP.LY_OL_SALES) * 100 / TP.LY_OL_SALES,1)
          end) SMS_PERC_ON_LY,
--         round(tp.online_sales * 100 / tp.sales,2) as sms_perc_contrib
         (case TP.SALES
             when 0 then 0
             else ROUND(TP.ONLINE_SALES * 100 / TP.SALES,2) 
          end) sms_perc_contrib
from     third_pass tp
where    tp.seq < 9
order by seq 
;

g_rec_in             c_sales%rowtype;

--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************
procedure local_address_variables as
begin

  
   g_sms_string        := g_sms_string||'#'||g_rec_in.bu||trim(to_char(g_rec_in.sms_online_sales,'99990.0'))||','
                                                          ||trim(to_char(g_rec_in.sms_perc_on_ly,'99990.0'))||'%,'
                                                          ||trim(to_char(g_rec_in.sms_perc_contrib,'99990.00'))||'%';

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

   g_rec_out.calendar_datetime      := sysdate;
   g_rec_out.record_type            := 'ONLINES';
   g_rec_out.processed_ind          := 0;
   g_rec_out.sms_text               := g_sms_string;
   g_rec_out.last_updated_date      := g_date;

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
   l_text := 'CREATION OF WEEKLY ONLINE SALES SMS STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   select fin_week_no ,fin_year_no 
   into   g_fin_week_no,      g_fin_year_no
   from   dim_calendar 
   where  calendar_date = g_date - 6;
   
   select ly_fin_year_no,ly_fin_week_no
   into      g_ly_fin_year_no, g_ly_fin_week_no 
   from   dim_calendar_wk 
   where  fin_week_no = g_fin_week_no and
          fin_year_no = g_fin_year_no;

  

   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'WEEKS BEING EVEALUATED TY/LY - '||g_fin_week_no||g_fin_year_no||' / '||g_ly_fin_week_no||g_ly_fin_year_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   update      rtl_sms_publish
   set         processed_ind = 1
   where       record_type = 'ONLINES' 
   and         processed_ind = 0;

 --  g_sms_string := to_char(trunc(sysdate),'dd/mm/yyyy');
   g_sms_string := 'Week'||to_char(g_fin_week_no)||'-ONL';
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
   g_sms_string := g_sms_string||'#'||'T=EX70';
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

end wh_prf_corp_766e;
