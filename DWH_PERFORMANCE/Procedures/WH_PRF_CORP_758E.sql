--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_758E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_758E" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2009
--  Author:      M Munnik
--  Purpose:     Creates Weekly Flash Sales by Department Group SMS message.
--               Clothing only - Business Unit 51
--  Tables:      Input  - rtl_loc_dept_dy,
--                        rtl_loc_item_dy_rms_dense
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
g_store_cnt          number        :=  0;
g_hol_ind            number        :=  0;
g_count              number;
g_date               date;
g_year               number(4,0);
g_week               number(2,0);
g_this_wk_strt_dte   date;
g_ly_wk_strt_dte     date;
g_ly_to_dte          date;
g_accum_sales_ty     number(19,1);
g_sales_var_ty       number(19,1);
g_sales_ly_perc      number(19,1);
g_sms_string         varchar2(500);

g_rec_out            rtl_sms_publish%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_758E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES WEEKLY FLASH SALES BY DEPARTMENT GROUP SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- NOTE !!!
-- Business Unit 51 contains other groups than 1, 2, 3, 5, 6, 10. The other groups must be included in the totals,
-- but does not show individually in the SMS.

cursor c_sales is
   with sales as
   (select   tys.group_no, tys.this_year_sales, tyb.this_year_budget, lys.last_year_sales
   from
   (select   d.group_no, sum(r.cra_recon_2_sales) this_year_sales
   from      rtl_loc_dept_dy r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_department d     on r.sk1_department_no = d.sk1_department_no
   where     l.chain_no           =  10
   and       d.business_unit_no   =  51
   and       r.post_date          between g_this_wk_strt_dte and g_date
   group by  d.group_no) tys
   left join
   (select   d.group_no, sum(nvl(r.sales_budget,0) + nvl(r.online_sales_budget,0)) this_year_budget
   from      rtl_loc_dept_wk r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_department d     on r.sk1_department_no = d.sk1_department_no
   where     l.chain_no           =  10
   and       d.business_unit_no   =  51
   and       r.fin_year_no        =  g_year
   and       r.fin_week_no        =  g_week
   group by  d.group_no) tyb
   on        tys.group_no         =  tyb.group_no
   left join
   (select   i.group_no, sum(r.sales) last_year_sales
   from      rtl_loc_item_dy_rms_dense r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_item i           on r.sk1_item_no       = i.sk1_item_no
   where     l.chain_no           =  10
   and       i.business_unit_no   =  51
   and       i.department_no      not in(25,28,30,31,60,524,525,526)
   and       r.post_date          between g_ly_wk_strt_dte and g_ly_to_dte
   group by  i.group_no) lys
   on        tys.group_no         = lys.group_no)

   select    1 seq_no, 'C' grp_cde, sum(this_year_sales) this_year_sales, sum(this_year_budget) this_year_budget,
             sum(last_year_sales) last_year_sales
   from      sales

   union all

   select    (case group_no
                 when 1 then 2 when 2 then 3 when 3 then 4 when 5 then 5 when 6 then 6 else 7 end) seq_no,
             (case group_no
                 when 1 then 'G1' when 2 then 'G2' when 3 then 'G3' when 5 then 'G5' when 6 then 'G6' else 'G10' end) grp_cde,
             this_year_sales, this_year_budget, last_year_sales
   from      sales
   where     group_no in(1,2,3,5,6,10)
/*
   union all

   select    7 seq_no, 'TW' grp_cde,
             tys.this_year_sales, tyb.this_year_budget, 0 last_year_sales
   from
   (select   'TW' grp_cde, sum(r.cra_recon_2_sales) this_year_sales
   from      rtl_loc_dept_dy r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_department d     on r.sk1_department_no = d.sk1_department_no
   where     l.chain_no           =  10
   and       d.department_no      =  537
   and       r.post_date          between g_this_wk_strt_dte and g_date) tys
   left join
   (select   'TW' grp_cde, sum(nvl(r.sales_budget,0) + nvl(r.online_sales_budget,0)) this_year_budget
   from      rtl_loc_dept_wk r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_department d     on r.sk1_department_no = d.sk1_department_no
   where     l.chain_no           =  10
   and       d.department_no      =  537
   and       r.fin_year_no        =  g_year
   and       r.fin_week_no        =  g_week) tyb
   on        tys.grp_cde          = tyb.grp_cde
*/
   order by  seq_no;

g_rec_in             c_sales%rowtype;

--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************
procedure local_address_variables as
begin

--   if g_rec_in.grp_cde   = 'TW' then
--      g_sales_var_ty     := (nvl(g_rec_in.this_year_sales,0) - nvl(g_rec_in.this_year_budget,0)) / 1000000;
--      g_accum_sales_ty   := nvl(g_rec_in.this_year_sales,0) / 1000000;
--      g_sms_string       := g_sms_string||'#'||g_rec_in.grp_cde||':'||trim(to_char(g_accum_sales_ty,'99990.0'))||','
--                                                                    ||trim(to_char(g_sales_var_ty,'99990.0'));
--   else
      if nvl(g_rec_in.last_year_sales,0) = 0 then
         g_sales_ly_perc := 0;
      else
         g_sales_ly_perc := ((nvl(g_rec_in.this_year_sales,0) - g_rec_in.last_year_sales) / g_rec_in.last_year_sales) * 100;
      end if;
      g_sales_var_ty     := (nvl(g_rec_in.this_year_sales,0) - nvl(g_rec_in.this_year_budget,0)) / 1000000;
      g_accum_sales_ty   := nvl(g_rec_in.this_year_sales,0) / 1000000;

      g_sms_string       := g_sms_string||'#'||g_rec_in.grp_cde||':'||trim(to_char(g_accum_sales_ty,'99990.0'))||','
                                                                    ||trim(to_char(g_sales_ly_perc,'99990.0'))||'%,'
                                                                    ||trim(to_char(g_sales_var_ty,'99990.0'));
--   end if;

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
   g_rec_out.record_type            := 'FLSHCW';
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
   l_text := 'CREATION OF WEEKLY FLASH SALES BY DEPARTMENT GROUP SMS STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
 ---------  g_date := '9 Oct 2011';
   select ct.this_week_start_date, ct.fin_year_no, ct.fin_week_no, cs.ly_calendar_date, ct.ly_calendar_date, ct.rsa_public_holiday_ind
   into   g_this_wk_strt_dte,      g_year,         g_week,         g_ly_wk_strt_dte,    g_ly_to_dte,         g_hol_ind
   from   dim_calendar ct join dim_calendar cs on cs.calendar_date = ct.this_week_start_date
   where  ct.calendar_date = g_date;

   if g_hol_ind is null then
      g_hol_ind := 0;
   end if;

   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   update      rtl_sms_publish
   set         processed_ind = 1
   where       record_type = 'FLSHCW'
   and         processed_ind = 0;

   select      count(*)
   into        g_store_cnt
   from        dim_location l
   left join
   (select     sk1_location_no
   from        rtl_loc_dept_dy
   where       post_date              = g_date
   group by    sk1_location_no
   having      sum(nvl(cra_recon_2_sales,0)) <> 0) s
   on          l.sk1_location_no      = s.sk1_location_no
   where       l.chain_no             = 10
   and         l.active_store_ind     = 1
   and         (((trim(to_char(g_date,'day'))  = 'sunday') and l.sunday_store_trade_ind = 0)
   or             trim(to_char(g_date,'day')) <> 'sunday')
   and         l.district_no not in(9990,9999,9963)
   and         l.st_open_date         <= g_date
   and         l.st_close_date        >  g_date
   and         (s.sk1_location_no is null);

   g_sms_string     := 'Week'||g_week;

--**************************************************************************************************
-- Fetch loop controlling main program execution
--**************************************************************************************************
   if g_store_cnt < 30 or g_hol_ind = 1 then
      open c_sales;
      loop
         fetch c_sales into g_rec_in;
         exit when c_sales%NOTFOUND;
         g_recs_read := g_recs_read + 1;
         local_address_variables;
      end loop;
      close c_sales;
   else
      g_sms_string := 'Insufficient data available to publish weekly flash sales by department group sms - Communication to follow';
   end if;

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

end wh_prf_corp_758e;
