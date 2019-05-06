--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_753EQ
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_753EQ" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2009
--  Author:      M Munnik
--  Purpose:     Creates Daily Sales Margin SMS message.
--  Tables:      Input  - rtl_loc_dept_wk,
--                        rtl_loc_item_dy_rms_dense
--               Output - w6005682.rtl_sms_publish_test
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  026 DEC 2015 - BACKUP OF CURRENT LIVE - CHANGE IMPLEMENTED IN CURRENT LIVE TO EXCLUDE DEPT=96 - CUT FLOWERS
--                   DUE TO COSTPRICE ISSUE -- CHANGE AS PER REQUEST FROM KARL AND JOHN
--  12 May 2016  - Add additional total line for the SMS in cursor (Bus. Units - 51, 52, 54).       Ref:  BK12May16
--  07 Jul 2016  - Remove 'Rec' from SMS string ('#NoCRARec=') to provide for space in SMS line     Ref:  BK07Jul16
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
g_date               date;
g_year               number(4,0);
g_week               number(2,0);
g_sales_margin       number(19,2);
g_sales_marg_perc    number(19,1);
g_budg_marg_perc     number(19,1);
g_sms_string         varchar2(500);

g_rec_out            w6005682.rtl_sms_publish_test%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_753EQ';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES DAILY SALES MARGIN SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_sales is
with tmp_sales as
   (select  lid.business_unit_no, 
            lid.sales,  
            lid.sales_cost, 
            ldw.sales_margin_budget, 
            ldw.sales_budget
    from
          (select  i.business_unit_no, 
                   sum(r.sales)  sales, 
                   sum(r.sales_cost) sales_cost
           from    rtl_loc_item_dy_rms_dense r
           join    dim_location l       on r.sk1_location_no   = l.sk1_location_no
           join    dim_item i           on r.sk1_item_no       = i.sk1_item_no
           where   l.chain_no           =  10
           and     i.business_unit_no   in(50,51,52,54,55)
           and     r.post_date          =  g_date
           AND     I.DEPARTMENT_NO <> 96 --change
           group by  i.business_unit_no) lid
    left join
         (select  d.business_unit_no, 
                  sum(r.sales_margin_budget) sales_margin_budget, 
                  sum(r.sales_budget) sales_budget
         from     rtl_loc_dept_wk r
         join     dim_location l       on r.sk1_location_no   = l.sk1_location_no
         join     dim_department d     on r.sk1_department_no = d.sk1_department_no
         where    l.chain_no           =  10
         and      d.business_unit_no   in(50,51,52,54,55)
         AND      D.DEPARTMENT_NO <> 96 --change
         and      r.fin_year_no        =  g_year
         and      r.fin_week_no        =  g_week
         group by d.business_unit_no) ldw      
         
    on lid.business_unit_no =  ldw.business_unit_no
  ),
  
  tmp_all as
  (select  lid.business_unit_no, 
            lid.sales,  
            lid.sales_cost, 
            ldw.sales_margin_budget, 
            ldw.sales_budget
    from
          (select  i.business_unit_no, 
                   sum(r.sales)  sales, 
                   sum(r.sales_cost) sales_cost
           from    rtl_loc_item_dy_rms_dense r
           join    dim_location l       on r.sk1_location_no   = l.sk1_location_no
           join    dim_item i           on r.sk1_item_no       = i.sk1_item_no
           where   l.chain_no           =  10
           and     i.business_unit_no   in(50,51,52,53,54,55)
           and     r.post_date          =  g_date
           AND     I.DEPARTMENT_NO <> 96 --change
           group by  i.business_unit_no) lid
    left join
         (select  d.business_unit_no, 
                  sum(r.sales_margin_budget) sales_margin_budget, 
                  sum(r.sales_budget) sales_budget
         from     rtl_loc_dept_wk r
         join     dim_location l       on r.sk1_location_no   = l.sk1_location_no
         join     dim_department d     on r.sk1_department_no = d.sk1_department_no
         where    l.chain_no           =  10
         and      d.business_unit_no   in(50,51,52,53,54,55)
         AND      D.DEPARTMENT_NO <> 96 --change
         and      r.fin_year_no        =  g_year
         and      r.fin_week_no        =  g_week
         group by d.business_unit_no) ldw      
         
    on lid.business_unit_no =  ldw.business_unit_no
  )
      
select    1 seq_no, 
          '' abbr, 
          sum(sales) sales, 
          sum(sales_cost) sales_cost,
          sum(sales_margin_budget) sales_margin_budget, sum(sales_budget) sales_budget
from      tmp_sales

union all

select  (case business_unit_no
             when 50 then 5 when 51 then 2 when 52 then 3 when 54 then 4 else 6 end) seq_no,
        (case business_unit_no
             when 50 then 'F' when 51 then 'C' when 52 then 'H' when 54 then 'B' else 'P' end) abbr,
--select  (case business_unit_no
--             when 50 then 5 when 51 then 2 when 52 then 3 when 54 then 4 end) seq_no,
--        (case business_unit_no
--             when 50 then 'F' when 51 then 'C' when 52 then 'H' when 54 then 'B' end) abbr,
         sales, 
         sales_cost, 
         sales_margin_budget, 
         sales_budget
from     tmp_sales

Union all                                                                                     -- BK12May16
   
   select    1 seq_no, 'CGM' abbr, sum(sales) sales, sum(sales_cost) sales_cost,             -- BK12May16
             Sum(Sales_Margin_Budget) Sales_Margin_Budget, Sum(Sales_Budget) Sales_Budget     -- BK12May16
   From      Tmp_all                                                                        -- BK12May16
   where     business_unit_no in (51, 52, 53, 54)                                                 -- BK12May16  

order by  seq_no;

g_rec_in             c_sales%rowtype;

--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************
procedure local_address_variables as
begin

   g_sales_margin           := (nvl(g_rec_in.sales,0) - nvl(g_rec_in.sales_cost,0));
   if nvl(g_rec_in.sales,0) = 0 then
      g_sales_marg_perc     := 0;
   else
      g_sales_marg_perc     := (g_sales_margin / g_rec_in.sales) * 100;
   end if;
   g_sales_margin           := g_sales_margin / 1000000;
   if nvl(g_rec_in.sales_budget,0) = 0 then
      g_budg_marg_perc      := 0;
   else
      g_budg_marg_perc      := (nvl(g_rec_in.sales_margin_budget,0) / g_rec_in.sales_budget) * 100;
   end if;

   if g_sales_marg_perc not between -999.9 and 999.9 then
      g_sales_marg_perc := null;
   end if;

   if g_budg_marg_perc not between -999.9 and 999.9 then
      g_budg_marg_perc := null;
   end if;

   g_sms_string             := g_sms_string||'#'||g_rec_in.abbr||trim(to_char(g_sales_margin,'99990.00'))||','
                                                               ||trim(to_char(g_sales_marg_perc,'99990.0'))||'%,'
                                                               ||trim(to_char(g_budg_marg_perc,'99990.0'))||'%';

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
   g_rec_out.record_type            := 'SLSMGDA';
   g_rec_out.processed_ind          := 0;
   g_rec_out.sms_text               := g_sms_string;
   g_rec_out.last_updated_date      := g_date;

   insert into w6005682.rtl_sms_publish_test values g_rec_out;

   g_recs_inserted  := g_recs_inserted + sql%rowcount;

   g_rec_out.record_type            := 'SLSMGDB';
   insert into w6005682.rtl_sms_publish_test values g_rec_out;

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
   l_text := 'CREATION OF DAILY SALES MARGIN SMS STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   select fin_year_no, fin_week_no, rsa_public_holiday_ind
   into   g_year,      g_week,      g_hol_ind
   from   dim_calendar
   where  calendar_date = g_date;

   if g_hol_ind is null then
      g_hol_ind := 0;
   end if;

   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   update      w6005682.rtl_sms_publish_test
   set         processed_ind = 1
   where       record_type in('SLSMGDA','SLSMGDB')
   and         processed_ind = 0;

   select      count(*)
   into        g_store_cnt
   from        dim_location l
   left join
   (select     sk1_location_no
   from        rtl_loc_item_dy_rms_dense a
   where       post_date              =  g_date
   group by    sk1_location_no
   having      sum(nvl(sales,0)) <> 0) s
   on          l.sk1_location_no      =  s.sk1_location_no
   where       l.chain_no             =  10
   and         l.active_store_ind     =  1
   and         (((trim(to_char(g_date,'day'))  = 'sunday') and l.sunday_store_trade_ind = 0)
   or             trim(to_char(g_date,'day')) <> 'sunday')
   and         l.district_no not in(9990, 9999)
   and         l.st_open_date         <= g_date
   and         l.st_close_date        >  g_date
   and         (s.sk1_location_no is null);

   g_sms_string := to_char(trunc(sysdate),'dd/mm/yy')||'-MGN';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
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

      if g_store_cnt > 0 then
         g_sms_string := g_sms_string||'#NoCRA='||g_store_cnt;                  -- Ref: BK07Jul16
      end if;

   else
      g_sms_string := 'Insufficient data available to publish sales margin sms - Communication to follow';
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

end wh_prf_corp_753eQ;
