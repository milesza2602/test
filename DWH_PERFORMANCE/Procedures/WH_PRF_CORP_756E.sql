--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_756E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_756E" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2009
--  Author:      M Munnik
--  Purpose:     Creates Weekly Flash Sales by Region SMS message.
--  Tables:      Input  - rtl_loc_dept_dy,
--                        rtl_loc_dept_wk,
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

type region_rec   is record (region_no number(4,0), region_descr varchar2(3));
type regions_arr  is varray(4) of region_rec;
a_regions            regions_arr;

g_rec_out            rtl_sms_publish%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_756E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES WEEKLY FLASH SALES BY REGION SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_sales is
   with sales as
   (select   tys.region_no, tys.business_unit_no, tys.this_year_sales, tyb.this_year_budget, lys.last_year_sales
   from
   (select   l.region_no, d.business_unit_no, sum(r.cra_recon_2_sales) this_year_sales
   from      rtl_loc_dept_dy r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_department d     on r.sk1_department_no = d.sk1_department_no
   where     l.chain_no           = 10
   and       l.region_no          in(9941,9942,9943,9944)
   and       d.business_unit_no   in(50,51,52,53,54,55)
   and       r.post_date          between g_this_wk_strt_dte and g_date
   group by  l.region_no, d.business_unit_no) tys
   left join
   (select   l.region_no, d.business_unit_no, sum(nvl(r.sales_budget,0) + nvl(r.online_sales_budget,0)) this_year_budget
   from      rtl_loc_dept_wk r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_department d     on r.sk1_department_no = d.sk1_department_no
   where     l.chain_no           =  10
   and       l.region_no          in(9941,9942,9943,9944)
   and       d.business_unit_no   in(50,51,52,53,54,55)
   and       r.fin_year_no        =  g_year
   and       r.fin_week_no        =  g_week
   group by  l.region_no, d.business_unit_no) tyb
   on        tys.region_no        =  tyb.region_no
   and       tys.business_unit_no =  tyb.business_unit_no
   left join
   (select   l.region_no, i.business_unit_no, sum(r.sales) last_year_sales
   from      rtl_loc_item_dy_rms_dense r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_item i           on r.sk1_item_no       = i.sk1_item_no
   where     l.chain_no           = 10
   and       l.region_no          in(9941,9942,9943,9944)
   and       i.business_unit_no   in(50,51,52,53,54,55)
   and       i.department_no      not in(25,28,30,31,60,524,525,526)
   and       r.post_date          between g_ly_wk_strt_dte and g_ly_to_dte
   group by  l.region_no, i.business_unit_no) lys
   on        tys.region_no        = lys.region_no
   and       tys.business_unit_no = lys.business_unit_no)

   select    region_no, 1 seq_no, '' abbr, sum(this_year_sales) this_year_sales, sum(this_year_budget) this_year_budget,
             sum(last_year_sales) last_year_sales
   from      sales
   group by  region_no

   union all

   select    region_no,
             (case business_unit_no
                 when 50 then 6 when 51 then 2 when 52 then 3 when 53 then 5 when 54 then 4 else 7 end) seq_no,
             (case business_unit_no
                 when 50 then 'F' when 51 then 'C' when 52 then 'H' when 53 then 'D' when 54 then 'B' else 'P' end) abbr,
             this_year_sales, this_year_budget, last_year_sales
   from      sales
   order by  region_no, seq_no;

g_rec_in             c_sales%rowtype;

--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************
procedure local_address_variables as
begin

   if nvl(g_rec_in.last_year_sales,0) = 0 then
      g_sales_ly_perc := 0;
   else
      g_sales_ly_perc := ((nvl(g_rec_in.this_year_sales,0) - g_rec_in.last_year_sales) / g_rec_in.last_year_sales) * 100;
   end if;
   g_sales_var_ty     := (nvl(g_rec_in.this_year_sales,0) - nvl(g_rec_in.this_year_budget,0)) / 1000000;
   g_accum_sales_ty   := nvl(g_rec_in.this_year_sales,0) / 1000000;

   if g_sales_ly_perc not between -999.9 and 999.9 then
      g_sales_ly_perc := null;
   end if;

   g_sms_string       := g_sms_string||'#'||g_rec_in.abbr||trim(to_char(g_accum_sales_ty,'99990.0'))||','
                                                         ||trim(to_char(g_sales_ly_perc,'99990.0'))||'%,'
                                                         ||trim(to_char(g_sales_var_ty,'99990.0'));

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
   g_rec_out.record_type            := a_regions(g_count).region_descr||'W';
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
-- Count the stores that did not send EOD
--**************************************************************************************************
procedure count_stores_no_eod as
begin

   g_store_cnt := 0;

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
   and         l.region_no            = a_regions(g_count).region_no
   and         (((trim(to_char(g_date,'day'))  = 'sunday') and l.sunday_store_trade_ind = 0)
   or             trim(to_char(g_date,'day')) <> 'sunday')
   and         l.district_no not in(9990,9999,9963)
   and         l.st_open_date         <= g_date
   and         l.st_close_date        >  g_date
   and         (s.sk1_location_no is null);

end count_stores_no_eod;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
   p_success := false;
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'CREATION OF WEEKLY FLASH SALES BY REGION SMS STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
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
   where       record_type in('GCFW','GNEW','ECDW','WCAW')
   and         processed_ind = 0;

   a_regions                 := regions_arr();
   a_regions.extend(4);

   a_regions(1).region_no    := 9941;
   a_regions(2).region_no    := 9942;
   a_regions(3).region_no    := 9943;
   a_regions(4).region_no    := 9944;

   a_regions(1).region_descr := 'GCF';
   a_regions(2).region_descr := 'GNE';
   a_regions(3).region_descr := 'ECD';
   a_regions(4).region_descr := 'WCA';

--**************************************************************************************************
-- Fetch loop controlling main program execution
--**************************************************************************************************

   g_rec_in                := null;

   open c_sales;
   fetch c_sales into g_rec_in;

   for i in 1..4 loop

      g_sms_string     := 'Week'||g_week||a_regions(i).region_descr;

      g_count          := i;

      while g_rec_in.region_no = a_regions(g_count).region_no loop
         g_recs_read := g_recs_read + 1;
         local_address_variables;
         fetch c_sales into g_rec_in;
         exit when c_sales%NOTFOUND;
      end loop;

      count_stores_no_eod;

      if g_store_cnt > 0 then
         g_sms_string := g_sms_string||'#NoEODRec='||g_store_cnt;
      end if;

      if g_store_cnt < 30 or g_hol_ind = 1 then
         null;
      else
         g_sms_string := 'Insufficient data available to publish weekly flash sms for the region - Communication to follow';
      end if;

      insert_sms_rec;

   end loop;

   close c_sales;

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

end wh_prf_corp_756e;
