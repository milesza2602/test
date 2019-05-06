--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_759E_BCK16MARCH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_759E_BCK16MARCH" 
                                                         (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2009
--  Author:      M Munnik
--  Purpose:     Creates Daily Foods KPI SMS message.
--  Tables:      Input  - fnd_rtl_loc_item_dy_rms_stk_fd,
--                        rtl_loc_item_dy_rms_dense,
--                        rtl_loc_item_dy_rms_sparse,
--                        rtl_loc_item_dy_catalog
--               Output - rtl_sms_publish
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  15 mar 2018 - New Customer Availability calculation (fd_cust_avail and fd_num_cust_catlg_adj): chg-13094
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
g_perc_too_low       number        :=  0;
g_date               date;
g_this_wk_strt_dte   date;
g_eod_list           varchar2(100);
g_sms_string         varchar2(500);

g_rec_out            rtl_sms_publish%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_759E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES DAILY FOODS KPI SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_waste is
   with sales as
   (select   1 rectype,  lidd.commercial_manager_desc_562 merch_cat, lidd.post_date,' ' prod_class, lidd.sales,
             lids.waste_selling, lids.waste_cost,
             0 fd_num_avail_days_adj, 0 fd_num_catlg_days_adj,
             0 fd_num_cust_avail_adj, -- new AJ
             0 fd_cust_avail,
             0 fd_num_cust_catlg_adj
   from
   (select   u.commercial_manager_desc_562,r.post_date, sum(r.sales) sales
   from      rtl_loc_item_dy_rms_dense r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_item i           on r.sk1_item_no       = i.sk1_item_no
   join      dim_item_uda u       on i.sk1_item_no       = u.sk1_item_no
   WHERE
--             l.loc_type           = 'S'
--   and       l.area_no            = 9951
             l.chain_no           = 10
   and       i.business_unit_no   = 50
   and       r.post_date          between g_this_wk_strt_dte and g_date
   group by  u.commercial_manager_desc_562,r.post_date) lidd
   left join
   (select   u.commercial_manager_desc_562,r.post_date,
             sum(r.waste_selling) waste_selling, sum(r.waste_cost) waste_cost
   from      rtl_loc_item_dy_rms_sparse r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_item i           on r.sk1_item_no       = i.sk1_item_no
   join      dim_item_uda u       on i.sk1_item_no       = u.sk1_item_no
   WHERE
--             l.loc_type           = 'S'
--   and       l.area_no            = 9951
             l.chain_no           = 10
   and       i.business_unit_no   = 50
   and       r.post_date          between g_this_wk_strt_dte and g_date
   group by  u.commercial_manager_desc_562,r.post_date) lids
   on        lidd.post_date                     = lids.post_date and
             lidd.commercial_manager_desc_562 = lids.commercial_manager_desc_562
   union all
   select    2 rectype, u.commercial_manager_desc_562 merch_cat, r.calendar_date,u.product_class_desc_507 prod_class,
             0 sales, 0 waste_selling, 0 waste_cost,
             sum(r.fd_num_avail_days_adj) fd_num_avail_days_adj, 
             sum(r.fd_num_catlg_days_adj) fd_num_catlg_days_adj,
             sum(r.fd_num_cust_avail_adj) fd_num_cust_avail_adj, -- new AJ
             sum(r.fd_cust_avail) fd_cust_avail,
             sum(r.fd_num_cust_catlg_adj) fd_num_cust_catlg_adj
   from      rtl_loc_item_dy_catalog r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_item i           on r.sk1_item_no       = i.sk1_item_no
   join      dim_item_uda u       on i.sk1_item_no       = u.sk1_item_no
   where     l.loc_type           = 'S'
   and       l.area_no            = 9951
   and       i.business_unit_no   = 50
   and       r.calendar_date      between g_this_wk_strt_dte and g_date
   group by  u.commercial_manager_desc_562,u.product_class_desc_507,r.calendar_date)
-- new AJ
   select    21 seq_no, 'T.CA%' descr,
             case when sum(nvl(fd_num_cust_catlg_adj,0)) = 0 then 0 
--                  else round(((sum(nvl(fd_num_cust_avail_adj,0)) / sum(fd_num_catlg_days_adj)) * 100),1) end perc
                  else round(((sum(nvl(fd_cust_avail,0)) / sum(fd_num_cust_catlg_adj)) * 100),1) end perc
   from      sales
   where     rectype   = 2
   and       post_date = g_date
   union all
   select    22 seq_no, 'T.CA%' descr,
             case when sum(nvl(fd_num_cust_catlg_adj,0)) = 0 then 0 
                   else round(((sum(nvl(fd_cust_avail,0)) / sum(fd_num_cust_catlg_adj)) * 100),1) end perc
   from      sales
   where     rectype   = 2
   union all
   select    31 seq_no, 'P.CA%' descr,
             case when sum(nvl(fd_num_cust_catlg_adj,0)) = 0 then 0
                  else round(((sum(nvl(fd_cust_avail,0)) / sum(fd_num_cust_catlg_adj)) * 100),1) end perc
   from      sales
   where     rectype   = 2
   and       merch_cat = 'TRADING GROUP 1'
   and       post_date = g_date
   union all
   select    32 seq_no, 'A' descr,
             case when nvl(fd_num_cust_catlg_adj,0) = 0 then 0
                  else round(((nvl(fd_cust_avail,0) / fd_num_cust_catlg_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 1'
   and       post_date  = g_date
   and       prod_class = 'A'
   union all
   select    33 seq_no, 'B' descr,
             case when nvl(fd_num_cust_catlg_adj,0) = 0 then 0
                  else round(((nvl(fd_cust_avail,0) / fd_num_cust_catlg_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 1'
   and       post_date  = g_date
   and       prod_class = 'B'
   union all
   select    34 seq_no, 'C' descr,
             case when nvl(fd_num_cust_catlg_adj,0) = 0 then 0
                  else round(((nvl(fd_cust_avail,0) / fd_num_cust_catlg_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 1'
   and       post_date  = g_date
   and       prod_class = 'C'
   union all
   select    41 seq_no, 'L.CA%' descr,
             case when sum(nvl(fd_num_cust_catlg_adj,0)) = 0 then 0
                  else round(((sum(nvl(fd_cust_avail,0)) / sum(fd_num_cust_catlg_adj)) * 100),1) end perc
   from      sales
   where     rectype   = 2
   and       merch_cat = 'TRADING GROUP 2'
   and       post_date = g_date
   union all
   select    42 seq_no, 'A' descr,
             case when nvl(fd_num_cust_catlg_adj,0) = 0 then 0
                  else round(((nvl(fd_cust_avail,0) / fd_num_cust_catlg_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 2'
   and       post_date  = g_date
   and       prod_class = 'A'
   union all
   select    43 seq_no, 'B' descr,
             case when nvl(fd_num_cust_catlg_adj,0) = 0 then 0
                  else round(((nvl(fd_cust_avail,0) / fd_num_cust_catlg_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 2'
   and       post_date  = g_date
   and       prod_class = 'B'
   union all
   select    44 seq_no, 'C' descr,
             case when nvl(fd_num_cust_catlg_adj,0) = 0 then 0
                  else round(((nvl(fd_cust_avail,0) / fd_num_cust_catlg_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 2'
   and       post_date  = g_date
   and       prod_class = 'C'
   union all
-- end new AJ   
   select    81 seq_no, 'T.WST%' descr,
             case when sum(nvl(sales,0)) = 0 then 0 else round(((sum(nvl(waste_cost,0)) / sum(sales)) * 100),1) end perc
   from      sales
   where     rectype   = 1
   and       post_date = g_date
   union all
   select    82 seq_no, 'T.WST%' descr,
             case when sum(nvl(sales,0)) = 0 then 0 else round(((sum(nvl(waste_cost,0)) / sum(sales)) * 100),1) end perc
   from      sales
   where     rectype   = 1
   union all
   select    91 seq_no, 'P.WST%' descr,
             case when nvl(sales,0) = 0 then 0 else round(((nvl(waste_cost,0) / sales) * 100),1) end perc
   from      sales
   where     rectype   = 1
   and       post_date = g_date
   and       merch_cat = 'TRADING GROUP 1'
   union all
   select    92 seq_no, 'P.WST%' descr,
             case when sum(nvl(sales,0)) = 0 then 0 else round(((sum(nvl(waste_cost,0)) / sum(sales)) * 100),1) end perc
   from      sales
   where     rectype   = 1
   and       merch_cat = 'TRADING GROUP 1'
   union all
   select    101 seq_no, 'L.WST%' descr,
             case when nvl(sales,0) = 0 then 0 else round(((nvl(waste_cost,0) / sales) * 100),1) end perc
   from      sales
   where     rectype   = 1
   and       post_date = g_date
   and       merch_cat = 'TRADING GROUP 2'
   union all
   select    102 seq_no, 'L.WST%' descr,
             case when sum(nvl(sales,0)) = 0 then 0 else round(((sum(nvl(waste_cost,0)) / sum(sales)) * 100),1) end perc
   from      sales
   where     rectype   = 1
   and       merch_cat = 'TRADING GROUP 2'
   union all
   select    51 seq_no, 'T.EOD%' descr,
             case when sum(nvl(fd_num_catlg_days_adj,0)) = 0 then 0
                  else round(((sum(nvl(fd_num_avail_days_adj,0)) / sum(fd_num_catlg_days_adj)) * 100),1) end perc
   from      sales
   where     rectype   = 2
   and       post_date = g_date
   union all
   select    52 seq_no, 'T.EOD%' descr,
             case when sum(nvl(fd_num_catlg_days_adj,0)) = 0 then 0
                  else round(((sum(nvl(fd_num_avail_days_adj,0)) / sum(fd_num_catlg_days_adj)) * 100),1) end perc
   from      sales
   where     rectype   = 2
   union all
   select    61 seq_no, 'P.EOD%' descr,
             case when sum(nvl(fd_num_catlg_days_adj,0)) = 0 then 0
                  else round(((sum(nvl(fd_num_avail_days_adj,0)) / sum(fd_num_catlg_days_adj)) * 100),1) end perc
   from      sales
   where     rectype   = 2
   and       merch_cat = 'TRADING GROUP 1'
   and       post_date = g_date
   union all
   select    62 seq_no, 'A' descr,
             case when nvl(fd_num_catlg_days_adj,0) = 0 then 0
                  else round(((nvl(fd_num_avail_days_adj,0) / fd_num_catlg_days_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 1'
   and       post_date  = g_date
   and       prod_class = 'A'
   union all
   select    63 seq_no, 'B' descr,
             case when nvl(fd_num_catlg_days_adj,0) = 0 then 0
                  else round(((nvl(fd_num_avail_days_adj,0) / fd_num_catlg_days_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 1'
   and       post_date  = g_date
   and       prod_class = 'B'
   union all
   select    64 seq_no, 'C' descr,
             case when nvl(fd_num_catlg_days_adj,0) = 0 then 0
                  else round(((nvl(fd_num_avail_days_adj,0) / fd_num_catlg_days_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 1'
   and       post_date  = g_date
   and       prod_class = 'C'
   union all
   select    71 seq_no, 'L.EOD%' descr,
             case when sum(nvl(fd_num_catlg_days_adj,0)) = 0 then 0
                  else round(((sum(nvl(fd_num_avail_days_adj,0)) / sum(fd_num_catlg_days_adj)) * 100),1) end perc
   from      sales
   where     rectype   = 2
   and       merch_cat = 'TRADING GROUP 2'
   and       post_date = g_date
   union all
   select    72 seq_no, 'A' descr,
             case when nvl(fd_num_catlg_days_adj,0) = 0 then 0
                  else round(((nvl(fd_num_avail_days_adj,0) / fd_num_catlg_days_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 2'
   and       post_date  = g_date
   and       prod_class = 'A'
   union all
   select    73 seq_no, 'B' descr,
             case when nvl(fd_num_catlg_days_adj,0) = 0 then 0
                  else round(((nvl(fd_num_avail_days_adj,0) / fd_num_catlg_days_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 2'
   and       post_date  = g_date
   and       prod_class = 'B'
   union all
   select    74 seq_no, 'C' descr,
             case when nvl(fd_num_catlg_days_adj,0) = 0 then 0
                  else round(((nvl(fd_num_avail_days_adj,0) / fd_num_catlg_days_adj) * 100),1) end perc
   from      sales
   where     rectype    = 2
   and       merch_cat  = 'TRADING GROUP 2'
   and       post_date  = g_date
   and       prod_class = 'C'

   order by  seq_no;

g_rec_in             c_waste%rowtype;

cursor c_stores is
   select      l.location_no
   from        dim_location l
   left join
   (select     location_no
   from        fnd_rtl_loc_item_dy_rms_stk_fd
   where       post_date                = g_date
   group by    location_no
   having      max(nvl(com_flag_ind,0)) = 1) s
   on          l.location_no            = s.location_no
   where       l.loc_type               = 'S'
   and         l.area_no                = 9951
   and         l.active_store_ind       = 1
   and         l.st_food_sell_store_ind = 1
   and         (((trim(to_char(g_date,'day'))  = 'sunday') and l.sunday_store_trade_ind = 0)
   or             trim(to_char(g_date,'day')) <> 'sunday')
   and         l.district_no not in(9990, 9999)
   and         l.region_no <> 9982
   and         l.st_open_date         <= g_date
   and         l.st_close_date        >  g_date
   and         (s.location_no is null)
   order by    l.location_no;

g_rec_in_stores      c_stores%rowtype;

--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************
procedure local_address_variables as
begin

--   g_sms_string := g_sms_string||'#'||g_rec_in.descr||trim(to_char(g_rec_in.perc,'99990.0'))||'%';

if g_rec_in.seq_no in (21,31,41,51,61,71,81,91,101) then
   g_sms_string := g_sms_string||'#'||g_rec_in.descr||' '||trim(to_char(g_rec_in.perc,'99990.0'));
end if;
if g_rec_in.seq_no in (22,52,82,92,102) then
   g_sms_string := g_sms_string||' '||trim(to_char(g_rec_in.perc,'99990.0'));
end if;
if g_rec_in.seq_no in (32,33,34,42,43,44,62,63,64,72,73,74) then
   g_sms_string := g_sms_string||' '||g_rec_in.descr||trim(to_char(g_rec_in.perc,'99990.0'));
end if;

   if g_rec_in.descr like '%EOD%' then
      if g_rec_in.perc < 75 then
         g_perc_too_low := 1;
      end if;
   end if;

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
   g_rec_out.record_type            := 'FDKPI';
   g_rec_out.processed_ind          := 0;
   g_rec_out.sms_text               := g_sms_string;
   g_rec_out.last_updated_date      := g_date;

   if g_store_cnt < 30 or g_hol_ind = 1 then
      if g_perc_too_low = 1 then
         g_rec_out.processed_ind    := 9;
         insert into rtl_sms_publish values g_rec_out;
         g_recs_inserted            := g_recs_inserted + sql%rowcount;
--         g_sms_string               := 'PILOT'||to_char(g_date,'dd/mm/yyyy')||
         g_sms_string               := to_char(g_date,'dd/mm/yyyy')||
            '#One or all of the EOD figures are below tolerance level of 75% and the FOODS KPI SMS has been withheld.';
         g_rec_out.processed_ind    := 0;
         g_rec_out.sms_text         := g_sms_string;
      end if;
   end if;

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
   l_text := 'CREATION OF DAILY FOODS KPI SMS STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);

   select this_week_start_date, rsa_public_holiday_ind
   into   g_this_wk_strt_dte,   g_hol_ind
   from   dim_calendar
   where  calendar_date = g_date;

   if g_hol_ind is null then
      g_hol_ind := 0;
   end if;

   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   update      rtl_sms_publish
   set         processed_ind = 1
   where       record_type = 'FDKPI'
   and         processed_ind = 0;

--   g_sms_string     := 'PILOT'||to_char(g_date,'dd/mm/yyyy');
   g_sms_string     := to_char(g_date,'dd/mm/yyyy');
--   g_eod_list       := 'NoEODRec:';

   open c_stores;
   loop
      fetch c_stores into g_rec_in_stores;
      exit when c_stores%NOTFOUND;
      g_store_cnt := c_stores%ROWCOUNT;
      
--      if g_store_cnt = 1 then
--         g_eod_list := g_eod_list||g_rec_in_stores.location_no;
--      else
--         if g_store_cnt < 8 then
--            g_eod_list := g_eod_list||','||g_rec_in_stores.location_no;
--         end if;
--      end if;
   end loop;
   close c_stores;

--   if g_store_cnt > 7 then
      g_eod_list := 'NoEODRcv:'||g_store_cnt||' str';
--   end if;

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
--   g_store_cnt := 29; -- REMOVE AJ
   dbms_output.put_line('Store Count '||' '||g_store_cnt);
   if g_store_cnt < 30 or g_hol_ind = 1 then
      open c_waste;
      loop
         fetch c_waste into g_rec_in;
         exit when c_waste%NOTFOUND;
         g_recs_read := g_recs_read + 1;
         local_address_variables;
      end loop;
      close c_waste;

      if g_store_cnt > 0 then
         g_sms_string := g_sms_string||'#'||g_eod_list;
      end if;

   else
      g_sms_string := g_sms_string||'#Insufficient data available to publish Daily Foods KPI SMS - Communication to follow.';
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

end WH_PRF_CORP_759E_BCK16MARCH;
