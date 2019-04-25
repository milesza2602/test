--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_770E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_770E" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Dec 2015
--  Author:      N Chauhan
--  Purpose:     Creates Daily Flash WFS Card Sales SMS message.
--  Tables:      Input  - rtl_loc_dept_dy,
--                        cust_basket_tender
--                        cust_basket_item
--
--               Output - rtl_sms_publish
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  2015-12-02 - N Chauhan - created
--  2015-12-08 - N Chauhan - store count check included again.
--  2015-12-14 - N Chauhan - source tables changed to match what wfs users use for manual reports
--
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
g_temp_val           number(15,4);

g_sms_string         varchar2(500);

g_rec_out            rtl_sms_publish%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_770E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES DAILY FLASH WFS SALES SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


cursor c_ww_sales is
   with sales as
   (select   tys.post_date, tys.fin_day_no, tys.business_unit_no, tys.sales
   from
   (select   r.post_date, c.fin_day_no, d.business_unit_no, sum(r.CRA_RECON_2_SALES_INCL_VAT) sales
   from      rtl_loc_dept_dy r
   join      dim_location l       on r.sk1_location_no   = l.sk1_location_no
   join      dim_department d     on r.sk1_department_no = d.sk1_department_no
   join      dim_calendar c       on c.calendar_date     = r.post_date
   where     l.chain_no           = 10
   and       d.business_unit_no   in(50,51,52,53,54,55,70)
   and l.area_no in (9951, 9978, 8800, 9952)
   and l.area_no not in (9979, 9965, 9953, 9954, 8700, 1000, 1003)
   and       r.post_date   =  g_date 
   group by  r.post_date, c.fin_day_no, d.business_unit_no) tys
)
   select
   post_date,
   sum(sales) ww_sales,
   sum(case when business_unit_no in (51,52,53,54,55,70) then sales else 0 end) CHB_sales,
   sum(case when business_unit_no in (50) then sales else 0 end) F_sales
   from sales
   group by post_date;
   
 
g_ww_rec_in             c_ww_sales%rowtype;


cursor c_crd_sales is

   select s.post_date,  -- i.business_unit_no,
      sum(case when i.business_unit_no in (50,51,52,53,54,55,70) then 
       (nvl(s.WWCARD_SALES_INCL_VAT,0)+nvl(s.NON_MERCH_WW_CARD_SALES,0)) else 0 end) fs_tot_sales,
      sum(case when i.business_unit_no in (51,52,53,54,55,70) then 
       (nvl(s.WWCARD_SALES_INCL_VAT,0)+nvl(s.NON_MERCH_WW_CARD_SALES,0)) else 0 end) fs_CHB_sales,
      sum(case when i.business_unit_no in (50) then 
       (nvl(s.WWCARD_SALES_INCL_VAT,0)+nvl(s.NON_MERCH_WW_CARD_SALES,0)) else 0 end) fs_F_sales
   from 
   VS_WWP_RTL_LC_ITM_DY_WK_POSJ_A s, /* source for WW performance business cube */
   DIM_ITEM i, DIM_LOCATION l
   where
   I.SK1_ITEM_NO = S.SK1_ITEM_NO
   and l.sk1_location_no = S.SK1_LOCATION_NO
   and i.business_unit_no in (50,51,52,53,54,55,70)
   and l.chain_no = 10
   and l.area_no in (9951, 9978, 8800, 9952)
   and l.area_no not in (9979, 9965, 9953, 9954, 8700, 1000, 1003)
   and post_date = g_date 
   group by s.post_date;

g_crd_rec_in     c_crd_sales%rowtype;


cursor c_rwd_sales is
   

   select tran_date, --i.business_unit_no,
   
      sum(case when i.business_unit_no in (50,51,52,53,54,55,70) then A.DIFF_REWARD_SALES_VAT else 0 end) fs_tot_sales,
      sum(case when i.business_unit_no in (51,52,53,54,55,70) then A.DIFF_REWARD_SALES_VAT else 0 end) fs_CHB_sales,
      sum(case when i.business_unit_no in (50) then A.DIFF_REWARD_SALES_VAT else 0 end) fs_F_sales,
      sum(case when i.business_unit_no in (50,51,52,53,54,55,70) then A.DIFF_REWARD_DISC_VALUE else 0 end) fs_tot_disc,
      sum(case when i.business_unit_no in (51,52,53,54,55,70) then A.DIFF_REWARD_DISC_VALUE else 0 end) fs_CHB_disc,
      sum(case when i.business_unit_no in (50) then A.DIFF_REWARD_DISC_VALUE else 0 end) fs_F_disc
   
   FROM DWH_CUST_PERFORMANCE.CUST_LOC_ITEM_PROM_DY A
   inner join DIM_ITEM i on i.item_no = A.item_no
   where a.tran_date = g_date 
   group by tran_date;

g_rwd_rec_in     c_rwd_sales%rowtype;


   cursor c_fs_tender is
      select 
      bt.tran_date, 
      sum(bt.tender_selling) fs_tot_tender
      from dwh_cust_performance.cust_basket_tender bt, dim_location l
      where
      l.location_no = bt.location_no
      and l.area_no in (9951, 9978, 8800, 9952)
      and l.chain_no = 10
      and tender_type_detail_code like 'WW%'
      and tender_seq_no = 0
      and bt.tran_date = g_date  
      group by bt.tran_date
      order by bt.tran_date;

g_fst_rec_in     c_fs_tender%rowtype;



--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************

procedure local_address_variables as
begin


-- 2nd row

   g_sms_string  := g_sms_string||'#';
   g_sms_string  := g_sms_string||'WW ';
   g_temp_val    := nvl(g_ww_rec_in.ww_sales,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   g_sms_string  := g_sms_string||',C ' ;
   g_temp_val    := nvl(g_ww_rec_in.CHB_sales,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   g_sms_string  := g_sms_string||',F ' ;
   g_temp_val    := nvl(g_ww_rec_in.F_sales,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
  
-- 3rd row

   g_sms_string  := g_sms_string||'#';
   g_sms_string  := g_sms_string||'FS ' ;
   g_temp_val    := nvl(g_crd_rec_in.fs_tot_sales,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   g_sms_string  := g_sms_string||',C ' ;
   g_temp_val    := nvl(g_crd_rec_in.fs_CHB_sales,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   g_sms_string  := g_sms_string||',F ' ;
   g_temp_val    := nvl(g_crd_rec_in.fs_F_sales,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
  
-- 4th row

   g_sms_string  := g_sms_string||'#';
   g_sms_string  := g_sms_string||'% ';
   if nvl(g_ww_rec_in.ww_sales,0) = 0 then
      g_sms_string  := g_sms_string||'**.*';
   else
      if nvl(g_crd_rec_in.fs_tot_sales,0) = 0 then
         g_temp_val    := 0;
      else
         g_temp_val    := nvl(g_crd_rec_in.fs_tot_sales,0)/nvl(g_ww_rec_in.ww_sales,0)*100;
      end if;
      g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   end if;
   g_sms_string  := g_sms_string||',C ' ;
   if nvl(g_ww_rec_in.ww_sales,0) = 0 then
      g_sms_string  := g_sms_string||'**.*';
   else
      if nvl(g_crd_rec_in.fs_CHB_sales,0) = 0 then
         g_temp_val    := 0;
      else
         g_temp_val    := nvl(g_crd_rec_in.fs_CHB_sales,0)/nvl(g_ww_rec_in.CHB_sales,0)*100;
      end if;
      g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   end if;
   g_sms_string  := g_sms_string||',F ' ;
   if nvl(g_ww_rec_in.ww_sales,0) = 0 then
      g_sms_string  := g_sms_string||'**.*';
   else
      if nvl(g_crd_rec_in.fs_F_sales,0) = 0 then
         g_temp_val    := 0;
      else
         g_temp_val    := nvl(g_crd_rec_in.fs_F_sales,0)/nvl(g_ww_rec_in.F_sales,0)*100;
      end if;
      g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   end if;
   
-- 5th row

   g_sms_string  := g_sms_string||'#';
   g_sms_string  := g_sms_string||'WRs ' ;
   g_temp_val    := nvl(g_rwd_rec_in.fs_tot_sales,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   g_sms_string  := g_sms_string||',C ' ;
   g_temp_val    := nvl(g_rwd_rec_in.fs_CHB_sales,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   g_sms_string  := g_sms_string||',F ' ;
   g_temp_val    := nvl(g_rwd_rec_in.fs_F_sales,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));

   
 -- 6th row
 
   g_sms_string  := g_sms_string||'#';
   g_sms_string  := g_sms_string||'%WRs ';
   if nvl(g_crd_rec_in.fs_tot_sales,0) = 0 then
      g_sms_string  := g_sms_string||'**.*';
   else
      if nvl(g_crd_rec_in.fs_tot_sales,0) = 0 then
         g_temp_val    := 0;
      else
         g_temp_val    := nvl(g_rwd_rec_in.fs_tot_sales,0)/nvl(g_crd_rec_in.fs_tot_sales,0)*100;
      end if;
      g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   end if;
   g_sms_string  := g_sms_string||',C ' ;
   if nvl(g_crd_rec_in.fs_tot_sales,0) = 0 then
      g_sms_string  := g_sms_string||'**.*';
   else
      if nvl(g_crd_rec_in.fs_CHB_sales,0) = 0 then
         g_temp_val    := 0;
      else
         g_temp_val    := nvl(g_rwd_rec_in.fs_CHB_sales,0)/nvl(g_crd_rec_in.fs_CHB_sales,0)*100;
      end if;
      g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   end if;
   g_sms_string  := g_sms_string||',F ' ;
   if nvl(g_crd_rec_in.fs_tot_sales,0) = 0 then
      g_sms_string  := g_sms_string||'**.*';
   else
      if nvl(g_crd_rec_in.fs_F_sales,0) = 0 then
         g_temp_val    := 0;
      else
         g_temp_val    := nvl(g_rwd_rec_in.fs_F_sales,0)/nvl(g_crd_rec_in.fs_F_sales,0)*100;
      end if;
      g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   end if;
   
  
 -- 7th row
 
   g_sms_string  := g_sms_string||'#';
   g_sms_string  := g_sms_string||'WRe ' ;
   g_temp_val    := nvl(g_rwd_rec_in.fs_tot_disc,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.00'));
   g_sms_string  := g_sms_string||',C ' ;
   g_temp_val    := nvl(g_rwd_rec_in.fs_CHB_disc,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.00'));
   g_sms_string  := g_sms_string||',F ' ;
   g_temp_val    := nvl(g_rwd_rec_in.fs_F_disc,0)/1000000;
   g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.00'));

  
 -- 8th row
 /*  exclude for initial release
 
   g_sms_string  := g_sms_string||'#';
   g_sms_string  := g_sms_string||'%T ';
   if nvl(g_crd_rec_in.fs_tot_sales,0) = 0 then
      g_sms_string  := g_sms_string||'**';
   else
      if nvl(g_fst_rec_in.fs_tot_tender,0) = 0 then
         g_temp_val    := 0;
      else
         g_temp_val    := nvl(g_fst_rec_in.fs_tot_tender,0)/nvl(g_crd_rec_in.fs_tot_sales,0)*100;
      end if;
      g_sms_string  := g_sms_string||trim(to_char(g_temp_val,'99990.0'));
   end if;
*/   


   l_text := ' 3 g_sms_string - '||g_sms_string;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   

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
   g_rec_out.record_type            := 'WFSSMS1'; 
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
   l_text := 'CREATION OF DAILY FLASH WFS SALES SMS STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');
   
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);


   select ct.rsa_public_holiday_ind
   into   g_hol_ind
   from   dim_calendar ct 
     join dim_calendar cs on cs.calendar_date = ct.this_week_start_date
   where  ct.calendar_date = g_date;

   if g_hol_ind is null then
      g_hol_ind := 0;
   end if;

   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   update      rtl_sms_publish
   set         processed_ind = 1
   where       record_type in('WFSSMS1')
   and         processed_ind = 0;



  select      count(*)
   into        g_store_cnt
   from        dim_location l
   left join
   (select     sk1_location_no
   from        rtl_loc_dept_dy
   where       post_date              =  g_date
   group by    sk1_location_no
   having      sum(nvl(cra_recon_2_sales,0)) <> 0) s
   on          l.sk1_location_no      =  s.sk1_location_no
   where       l.chain_no             =  10
   and         l.active_store_ind     =  1
   and         (((trim(to_char(g_date,'day'))  = 'sunday') and l.sunday_store_trade_ind = 0)
   or             trim(to_char(g_date,'day')) <> 'sunday')
   and         l.district_no not in(9990,9999,9963)
   and         l.st_open_date         <= g_date
   and         l.st_close_date        >  g_date
   and         (s.sk1_location_no is null);

 


   g_sms_string := to_char(trunc(sysdate),'dd/mm/yyyy');
   l_text := '1 g_sms_string - '||g_sms_string;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************

  l_text := 'g_store_cnt='||g_store_cnt||' g_hol_ind='||g_hol_ind;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := g_sms_string;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   if g_store_cnt < 30 or g_hol_ind = 1 then

      open c_ww_sales;
      fetch c_ww_sales into g_ww_rec_in;
      close c_ww_sales;
   
      open c_crd_sales;
      fetch c_crd_sales into g_crd_rec_in;
      close c_crd_sales;
   
      open c_rwd_sales;
      fetch c_rwd_sales into g_rwd_rec_in;
      close c_rwd_sales;
   
/*
      open c_fs_tender;
      fetch c_fs_tender into g_fst_rec_in;
      close c_fs_tender;
*/   
           
      local_address_variables;

/*
      if g_store_cnt > 0 then
         g_sms_string := g_sms_string||'#NoCRARec='||g_store_cnt;
      end if;
*/
      l_text := '2 g_store_cnt='||g_store_cnt||' g_hol_ind='||g_hol_ind;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   else

      l_text := '3 g_store_cnt='||g_store_cnt||' g_hol_ind='||g_hol_ind;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      g_sms_string := 'Insufficient data available to publish wfs daily flash sms - Communication to follow';

   end if;

   l_text := '2 g_sms_string - '||g_sms_string;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
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

end WH_PRF_CORP_770E;
