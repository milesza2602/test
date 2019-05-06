--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_245T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_245T" (p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        November 2012
--  Author:      Quentin Smit
--  Purpose:     Roll ROS data from location item day to item day
--  Tables:      Input  -   dwh_performance.rtl_item_day_rate_of_sale
--               Output -   dwh_performance.rtl_item_wk_rate_of_sale
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dwh_performance.rtl_item_wk_rate_of_sale%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_245T';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL RATE OF SALE FROM ITEM DAY TO ITEM WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.rtl_item_wk_rate_of_sale%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.rtl_item_wk_rate_of_sale%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

l_fin_year_no        number;
l_fin_week_no        number;
l_last_wk_start_date date;
l_last_wk_end_date   date;
l_last_week_no       number;
l_last_wk_fin_year_no number;

l_end_6wks            number;
l_last_fin_year       number;
l_6wks_wk1_yr         number;
l_6wks_wk2_yr         number;
l_6wks_wk3_yr         number;
l_6wks_wk4_yr         number;
l_6wks_wk5_yr         number;
l_6wks_wk6_yr         number;
l_6wks_wk1            number;
l_6wks_wk2            number;
l_6wks_wk3            number;
l_6wks_wk4            number;
l_6wks_wk5            number;
l_6wks_wk6            number;
l_end_6wks_date       date;
l_start_6wks_date     date;
l_6wk_avg1            number(18,7);
l_6wk_avg2            number(18,7);
l_max_6wk_last_year   number;
l_units_per_week_1    number(18,7);
l_units_per_week_2    number(18,7);
l_units_per_week_3    number(18,7);
l_units_per_week_4    number(18,7);
l_units_per_week_5    number(18,7);
l_units_per_week_6    number(18,7);

l_avg_units_per_day_1 number(18,7);
l_avg_units_per_day_2 number(18,7);
l_avg_units_per_day_3 number(18,7);
l_avg_units_per_day_4 number(18,7);
l_avg_units_per_day_5 number(18,7);
l_avg_units_per_day_6 number(18,7);

l_avg_wks_calc        number(1,0) :=6;
l_tot_units_per_wk    number(18,7);
l_tot_units_per_day   number(18,7);

cursor c_itm_wk_ros is
   select sk1_item_no,
          sum(sales_qty) sales_qty,
          sum(catalog_days) catalog_days,
          sum(units_per_day) units_per_week,
          avg(units_per_day) avg_units_per_day
from dwh_performance.rtl_item_day_rate_of_sale
where calendar_date between l_last_wk_start_date and l_last_wk_end_date
--and sk1_item_no = 12776  --17780902
group by sk1_item_no
order by sk1_item_no;

g_rec_in      c_itm_wk_ros%rowtype;

-- For input bulk collect --
type stg_array is table of c_itm_wk_ros%rowtype;
a_rate_of_sale_dy      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_item_no                    := g_rec_in.sk1_item_no;
   g_rec_out.fin_year_no                    := l_fin_year_no;
   g_rec_out.fin_week_no                    := l_fin_week_no;
   g_rec_out.sales_qty                      := g_rec_in.sales_qty;
   g_rec_out.catalog_days                   := g_rec_in.catalog_days;
   g_rec_out.units_per_week                 := g_rec_in.units_per_week;
   g_rec_out.avg_units_per_day              := g_rec_in.avg_units_per_day;
   g_rec_out.last_updated_date              := g_date;

--l_text := 'starting with item '|| g_rec_in.sk1_item_no;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   ------------------
   -- 6 week calcs --
   ------------------
/*
   select nvl(avg(units_per_week),0), nvl(avg(avg_units_per_day),0)
     into l_6wk_avg1, l_6wk_avg2
     from dwh_performance.rtl_item_wk_rate_of_sale f
    where ((f.fin_year_no = l_6wks_wk1_yr and f.fin_week_no = l_6wks_wk1)
        or (f.fin_year_no = l_6wks_wk2_yr and f.fin_week_no = l_6wks_wk2)
        or (f.fin_year_no = l_6wks_wk3_yr and f.fin_week_no = l_6wks_wk3)
        or (f.fin_year_no = l_6wks_wk4_yr and f.fin_week_no = l_6wks_wk4)
        or (f.fin_year_no = l_6wks_wk5_yr and f.fin_week_no = l_6wks_wk5)
        or (f.fin_year_no = l_6wks_wk6_yr and f.fin_week_no = l_6wks_wk6))
     and sk1_item_no = g_rec_in.sk1_item_no
    ;
*/
   select nvl(avg(units_per_week),9999999), nvl(sum(sales_qty)/sum(catalog_days),9999999)  --nvl(avg(avg_units_per_day),9999999)
     into l_units_per_week_1, l_avg_units_per_day_1
     from dwh_performance.rtl_item_wk_rate_of_sale f
    where f.fin_year_no = l_6wks_wk1_yr and f.fin_week_no = l_6wks_wk1
      and sk1_item_no = g_rec_in.sk1_item_no;

   select nvl(avg(units_per_week),9999999), nvl(sum(sales_qty)/sum(catalog_days),9999999)  --nvl(avg(avg_units_per_day),9999999)
    --select units_per_week, avg_units_per_day
     into l_units_per_week_2, l_avg_units_per_day_2
     from dwh_performance.rtl_item_wk_rate_of_sale f
    where f.fin_year_no = l_6wks_wk2_yr and f.fin_week_no = l_6wks_wk2
      and sk1_item_no = g_rec_in.sk1_item_no;
      
    select nvl(avg(units_per_week),9999999), nvl(sum(sales_qty)/sum(catalog_days),9999999)  --nvl(avg(avg_units_per_day),9999999)
     into l_units_per_week_3, l_avg_units_per_day_3
     from dwh_performance.rtl_item_wk_rate_of_sale f
    where f.fin_year_no = l_6wks_wk3_yr and f.fin_week_no = l_6wks_wk3
      and sk1_item_no = g_rec_in.sk1_item_no;
     
    select nvl(avg(units_per_week),9999999), nvl(sum(sales_qty)/sum(catalog_days),9999999)  --nvl(avg(avg_units_per_day),9999999)
     into l_units_per_week_4, l_avg_units_per_day_4
     from dwh_performance.rtl_item_wk_rate_of_sale f
    where f.fin_year_no = l_6wks_wk4_yr and f.fin_week_no = l_6wks_wk4
      and sk1_item_no = g_rec_in.sk1_item_no;
     
    select nvl(avg(units_per_week),9999999), nvl(sum(sales_qty)/sum(catalog_days),9999999)  --nvl(avg(avg_units_per_day),9999999)
     into l_units_per_week_5, l_avg_units_per_day_5
     from dwh_performance.rtl_item_wk_rate_of_sale f
    where f.fin_year_no = l_6wks_wk5_yr and f.fin_week_no = l_6wks_wk5
      and sk1_item_no = g_rec_in.sk1_item_no;
     
    select nvl(avg(units_per_week),9999999), nvl(sum(sales_qty)/sum(catalog_days),9999999)  --nvl(avg(avg_units_per_day),9999999)
     into l_units_per_week_6, l_avg_units_per_day_6
     from dwh_performance.rtl_item_wk_rate_of_sale f
    where f.fin_year_no = l_6wks_wk6_yr and f.fin_week_no = l_6wks_wk6
      and sk1_item_no = g_rec_in.sk1_item_no  ;    
 /*
  l_text := 'units per week1 = '||l_units_per_week_1;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per day1 = '||l_avg_units_per_day_1;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per week2 = '||l_units_per_week_2;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per day2 = '||l_avg_units_per_day_2;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per week3 = '||l_units_per_week_3;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per day3 = '||l_avg_units_per_day_3;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per week4 = '||l_units_per_week_4;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per day4 = '||l_avg_units_per_day_4;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per week5 = '||l_units_per_week_5;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per day5 = '||l_avg_units_per_day_5;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per week6 = '||l_units_per_week_6;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'units per day6 = '||l_avg_units_per_day_6;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 
 */ 
  if l_units_per_week_1 = 9999999 or l_avg_units_per_day_1 = 9999999 then
     --l_text := 'units per week 1 is null';
     --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_avg_wks_calc := l_avg_wks_calc - 1;
     l_units_per_week_1     :=0;
     l_avg_units_per_day_1  :=0;
     --l_text := 'here1 ';
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  end if;
    
  if l_units_per_week_2 = 9999999 or l_avg_units_per_day_2 = 9999999 then
     --l_text := 'units per week 2 is null';
     --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_avg_wks_calc := l_avg_wks_calc - 1;
     l_units_per_week_2     :=0;
     l_avg_units_per_day_2  :=0;

  end if;
    
  if l_units_per_week_3 = 9999999 or l_avg_units_per_day_3 = 9999999 then
     --l_text := 'units per week 3 is null';
     --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_avg_wks_calc := l_avg_wks_calc - 1;
     l_units_per_week_3     :=0;
     l_avg_units_per_day_3  :=0;
     --l_text := 'here3 ';
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  end if;
    
  if l_units_per_week_4 = 9999999 or l_avg_units_per_day_4 = 9999999 then
     --l_text := 'units per week 4 is null';
     --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_avg_wks_calc := l_avg_wks_calc - 1;
     l_units_per_week_4     :=0;
     l_avg_units_per_day_4  :=0;
     --l_text := 'here4 ';
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  end if;
    
  if l_units_per_week_5 = 9999999 or l_avg_units_per_day_5 = 9999999 then
     --l_text := 'units per week 5 is null';
     --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_avg_wks_calc := l_avg_wks_calc - 1;
     l_units_per_week_5     :=0;
     l_avg_units_per_day_5  :=0;
     --l_text := 'here5 ';
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  end if;
    
  if l_units_per_week_6 = 9999999 or l_avg_units_per_day_6 = 9999999 then
     --l_text := 'units per week 6 is null';
     --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_avg_wks_calc := l_avg_wks_calc - 1;
     l_units_per_week_6     :=0;
     l_avg_units_per_day_6  :=0;
     --l_text := 'here6 ';
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  end if;
  
 -- l_text := 'no of weeks to be used in calc = '|| l_avg_wks_calc;
 -- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
--  l_text := 'here1 ';
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
  
  if l_avg_wks_calc > 0 then
     l_tot_units_per_wk := ((l_units_per_week_1 + l_units_per_week_2 + l_units_per_week_3 + 
                            l_units_per_week_4 + l_units_per_week_5 + l_units_per_week_6)/l_avg_wks_calc);

     l_tot_units_per_day := ((l_avg_units_per_day_1 + l_avg_units_per_day_2 + l_avg_units_per_day_3 + 
                            l_avg_units_per_day_4 + l_avg_units_per_day_5 + l_avg_units_per_day_6)/l_avg_wks_calc);
  end if;
                         
  --l_text := 'avg units per week = '|| l_tot_units_per_wk;
  --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     
  --l_text := 'avg units per day = '|| l_tot_units_per_day;
  --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     

--l_text := 'here ';
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);                         
                         
   g_rec_out.avg_units_per_wk_6wk         := l_tot_units_per_wk;    --l_6wk_avg1;
   g_rec_out.avg_units_per_day_6wk        := l_tot_units_per_day;   --l_6wk_avg2;

    l_avg_wks_calc := 6;
--l_text := 'done with item '|| g_rec_in.sk1_item_no;
--dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variable;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into dwh_performance.rtl_item_wk_rate_of_sale values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update dwh_performance.rtl_item_wk_rate_of_sale
      set    sales_qty                      = a_tbl_update(i).sales_qty,
             catalog_days                   = a_tbl_update(i).catalog_days,
             units_per_week                 = a_tbl_update(i).units_per_week,
             avg_units_per_day              = a_tbl_update(i).avg_units_per_day,
             last_updated_date              = a_tbl_update(i).last_updated_date,
             avg_units_per_wk_6wk           = a_tbl_update(i).avg_units_per_wk_6wk,
             avg_units_per_day_6wk          = a_tbl_update(i).avg_units_per_day_6wk
      where  sk1_item_no                    = a_tbl_update(i).sk1_item_no
        and  fin_year_no                    = a_tbl_update(i).fin_year_no
        and  fin_week_no                    = a_tbl_update(i).fin_week_no;

      g_recs_updated := g_recs_updated + a_tbl_update.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_item_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin


   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
     into   g_count
     from   dwh_performance.rtl_item_wk_rate_of_sale
    where sk1_item_no       = g_rec_out.sk1_item_no
      and fin_year_no       = g_rec_out.fin_year_no
      and fin_week_no       = g_rec_out.fin_week_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;

   end if;

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;

      commit;
   end if;


   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'ROLL OF dwh_performance.rtl_item_day_rate_of_sale to dwh_performance.rtl_item_wk_rate_of_sale STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select today_fin_year_no,
           today_fin_week_no,
           last_wk_fin_week_no,            -- last_completed_week
           last_wk_fin_year_no,            -- last week's fin year
           last_wk_start_date,
           last_wk_end_date,
           last_wk_fin_week_no
      into l_fin_year_no,
           l_fin_week_no,
           l_last_week_no,
           l_last_wk_fin_year_no,
           l_last_wk_start_date,
           l_last_wk_end_date,
           l_end_6wks
      from dim_control_report;

/*
if l_end_6wks < 6 then
   select max(fin_week_no)
     into l_max_6wk_last_year
     from dim_calendar_wk
    where fin_year_no = l_last_fin_year;

   select calendar_date end_date
     into l_end_6wks_date
     from dim_calendar
    where fin_year_no = l_last_wk_fin_year_no
      and fin_week_no = l_end_6wks
      and fin_day_no = 7;

   --############################################################################################
   -- Below is a bit long but it breaks down the year and week grouping for the 6wk calculation.
   -- This needs to be done as the string can't be used in the insert / append statement as it
   -- uses a big 'with ..'  clause.
   -- There is probably a nicer and neater way of doing by using a loop but for now this will work..
   --############################################################################################
   if l_end_6wks = 5 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk4_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk5_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_end_6wks-2;
      l_6wks_wk4    := l_end_6wks-3;
      l_6wks_wk5    := l_end_6wks-4;
      l_6wks_wk6    := l_max_6wk_last_year;
   end if;
   if l_end_6wks = 4 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk4_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk5_yr := l_last_fin_year;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_end_6wks-2;
      l_6wks_wk4    := l_end_6wks-3;
      l_6wks_wk5    := l_max_6wk_last_year;
      l_6wks_wk6    := l_max_6wk_last_year-1;
   end if;
   if l_end_6wks = 3 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk4_yr := l_last_fin_year;
      l_6wks_wk5_yr := l_last_fin_year;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_end_6wks-2;
      l_6wks_wk4    := l_max_6wk_last_year;
      l_6wks_wk5    := l_max_6wk_last_year-1;
      l_6wks_wk6    := l_max_6wk_last_year-2;
   end if;
   if l_end_6wks = 2 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk3_yr := l_last_fin_year;
      l_6wks_wk4_yr := l_last_fin_year;
      l_6wks_wk5_yr := l_last_fin_year;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_end_6wks-1;
      l_6wks_wk3    := l_max_6wk_last_year;
      l_6wks_wk4    := l_max_6wk_last_year-1;
      l_6wks_wk5    := l_max_6wk_last_year-2;
      l_6wks_wk6    := l_max_6wk_last_year-3;
   end if;
   if l_end_6wks = 1 then
      l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
      l_6wks_wk2_yr := l_last_fin_year;
      l_6wks_wk3_yr := l_last_fin_year;
      l_6wks_wk4_yr := l_last_fin_year;
      l_6wks_wk5_yr := l_last_fin_year;
      l_6wks_wk6_yr := l_last_fin_year;
      l_6wks_wk1    := l_end_6wks;
      l_6wks_wk2    := l_max_6wk_last_year;
      l_6wks_wk3    := l_max_6wk_last_year-1;
      l_6wks_wk4    := l_max_6wk_last_year-2;
      l_6wks_wk5    := l_max_6wk_last_year-3;
      l_6wks_wk6    := l_max_6wk_last_year-4;
   end if;

      --Calendar dates for last 6 weeks
     select unique calendar_date start_date
       into l_start_6wks_date
       from dim_calendar
      where fin_year_no = l_last_fin_year
        and fin_week_no = l_6wks_wk6
        and fin_day_no = 1;

else
   select calendar_date end_date
     into l_end_6wks_date
     from dim_calendar
    where fin_year_no = l_last_wk_fin_year_no  --l_fin_year_no
      and fin_week_no = l_end_6wks
      and fin_day_no = 7;

     --l_6wks_string := '( fin_year_no = l_fin_year_no and fin_week between l_start_6wks and l_end_6wk)';
     l_6wks_wk1_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk2_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk3_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk4_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk5_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk6_yr := l_last_wk_fin_year_no;  --l_fin_year_no;
     l_6wks_wk1    := l_end_6wks;
     l_6wks_wk2    := l_end_6wks-1;
     l_6wks_wk3    := l_end_6wks-2;
     l_6wks_wk4    := l_end_6wks-3;
     l_6wks_wk5    := l_end_6wks-4;
     l_6wks_wk6    := l_end_6wks-5;

     --Calendar dates for last 6 weeks
     select unique calendar_date start_date
       into l_start_6wks_date
       from dim_calendar
      where fin_year_no = l_last_wk_fin_year_no  --l_fin_year_no
        and fin_week_no = l_6wks_wk6  --l_start_6wks
        and fin_day_no = 1;
end if
;
*/

    l_6wks_wk1_yr := 2013;
    l_6wks_wk2_yr := 2013;
    l_6wks_wk3_yr := 2013;
    l_6wks_wk4_yr := 2013;
    l_6wks_wk5_yr := 2013;
    l_6wks_wk6_yr := 2013;
    l_6wks_wk1    := 45;
    l_6wks_wk2    := l_6wks_wk1-1;
    l_6wks_wk3    := l_6wks_wk1-2;
    l_6wks_wk4    := l_6wks_wk1-3;
    l_6wks_wk5    := l_6wks_wk1-4;
    l_6wks_wk6    := l_6wks_wk1-5;

    select this_week_start_date, this_week_end_date 
      into l_last_wk_start_date, l_last_wk_end_date
      from dim_calendar_wk
      where fin_year_no = 2013
      and fin_week_no = 45;
      
    --l_last_wk_start_date := '27/AUG/12';
   -- l_last_wk_end_date   := '02/SEP/12';

    select fin_year_no, fin_week_no
      into l_fin_year_no, l_fin_week_no
      from dim_calendar
      where calendar_date = l_last_wk_start_date;

    l_text := 'Year and week being processed :- '|| l_fin_year_no || ' ' || l_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Start date of week being processed :- '|| l_last_wk_start_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'End date of week being processed :- '|| l_last_wk_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text:= '6wks week 1 year = ' || l_6wks_wk1_yr || ' week = ' || l_6wks_wk1;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text:= '6wks week 2 year = ' || l_6wks_wk2_yr || ' week = ' || l_6wks_wk2;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text:= '6wks week 3 year = ' || l_6wks_wk3_yr || ' week = ' || l_6wks_wk3;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text:= '6wks week 4 year = ' || l_6wks_wk4_yr || ' week = ' || l_6wks_wk4;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text:= '6wks week 5 year = ' || l_6wks_wk5_yr || ' week = ' || l_6wks_wk5;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text:= '6wks week 6 year = ' || l_6wks_wk6_yr || ' week = ' || l_6wks_wk6;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    --l_last_wk_start_date := 'Mooo';

--**************************************************************************************************
    open c_itm_wk_ros;
    fetch c_itm_wk_ros bulk collect into a_rate_of_sale_dy limit g_forall_limit;
    while a_rate_of_sale_dy.count > 0
    loop
      for i in 1 .. a_rate_of_sale_dy.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then          --200000
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_rate_of_sale_dy(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_itm_wk_ros bulk collect into a_rate_of_sale_dy limit g_forall_limit;
    end loop;
    close c_itm_wk_ros;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;


--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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

end wh_prf_corp_245t;
