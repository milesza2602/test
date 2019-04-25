--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_767E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_767E" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Q Smit
--  Purpose:     Creates Daily Foods DC out of Stock count and supplier orderfill % SMS
--
--  Tables:      Input  - rtl_depot_item_dy,
--                        rtl_po_supchain_loc_item_dy
--
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
g_this_wk_strt_dte   date;
g_ly_wk_strt_dte     date;
g_ly_to_dte          date;
g_accum_sales_ty     number(19,1);
g_sales_ly_perc      number(19,1);
g_sales_wtd_perc     number(19,1);
g_sms_string         varchar2(500);

g_rec_out            rtl_sms_publish%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_767E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES DAILY FOODS DC OUT OF STOCK AND SUPPLIER ORDERFILL % SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_dc_date            date :=trunc(sysdate);


cursor c_stock is
   with stock as
(select tots.business_unit_no, tots.tot_out_of_stock,
        mont.mont_out_of_stock, monta.monta_out_of_stock, montb.montb_out_of_stock, montc.montc_out_of_stock,
        mid.mid_out_of_stock, mida.mida_out_of_stock, midb.midb_out_of_stock, midc.midc_out_of_stock,
        odf.orderfill, odftg1.orderfill_tg1, odftg1A.orderfill_tg1a, odftg1B.orderfill_tg1b, odftg1C.orderfill_tg1c,
                       odftg2.orderfill_tg2, odftg2A.orderfill_tg2a, odftg2B.orderfill_tg2b, odftg2C.orderfill_tg2c
from
  (select b.business_unit_no, sum(a.ll_dc_out_of_stock) tot_out_of_stock
     from rtl_depot_item_dy a
     join dim_item b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda c on b.sk1_item_no = c.sk1_item_no
     join dim_calendar e on a.post_date = e.calendar_date
    where a.post_date = l_dc_date --g_date
      and b.business_unit_no = 50
      and c.MERCHANDISE_CATEGORY_DESC_100 = 'L'
    group by b.business_unit_no ) tots


--MONTAGUE GARDENS
left join

  (select b.business_unit_no, sum(a.ll_dc_out_of_stock) mont_out_of_stock
     from  rtl_depot_item_dy a
     join dim_item b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda c on b.sk1_item_no = c.sk1_item_no
     join dim_location d on a.sk1_location_no = d.sk1_location_no
     join dim_calendar e on a.post_date = e.calendar_date
    where a.post_date = l_dc_date  --g_date
      and b.business_unit_no = 50
      and c.MERCHANDISE_CATEGORY_DESC_100 = 'L'
      and d.wh_fd_zone_no = 10
      group by b.business_unit_no) mont
on   tots.business_unit_no = mont.business_unit_no

left join

  (select  b.business_unit_no, sum(a.ll_dc_out_of_stock) monta_out_of_stock
     from  rtl_depot_item_dy a
     join dim_item b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda c on b.sk1_item_no = c.sk1_item_no
     join dim_location d on a.sk1_location_no = d.sk1_location_no
     join dim_calendar e on a.post_date = e.calendar_date
    where a.post_date = l_dc_date  --g_date
      and b.business_unit_no = 50
      and c.MERCHANDISE_CATEGORY_DESC_100 = 'L'
      and d.wh_fd_zone_no = 10
      and c.product_class_desc_507 = 'A'
      group by b.business_unit_no) monta
on  tots.business_unit_no = monta.business_unit_no

left join

  (select  b.business_unit_no, sum(a.ll_dc_out_of_stock) montb_out_of_stock
     from  rtl_depot_item_dy a
     join dim_item b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda c on b.sk1_item_no = c.sk1_item_no
     join dim_location d on a.sk1_location_no = d.sk1_location_no
     join dim_calendar e on a.post_date = e.calendar_date
    where a.post_date = l_dc_date  --g_date
      and b.business_unit_no = 50
      and c.MERCHANDISE_CATEGORY_DESC_100 = 'L'
      and d.wh_fd_zone_no = 10
      and c.product_class_desc_507 = 'B'
      group by b.business_unit_no) montb
on  tots.business_unit_no = montb.business_unit_no

left join

  (select  b.business_unit_no, sum(a.ll_dc_out_of_stock) montc_out_of_stock
     from  rtl_depot_item_dy a
     join dim_item b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda c on b.sk1_item_no = c.sk1_item_no
     join dim_location d on a.sk1_location_no = d.sk1_location_no
     join dim_calendar e on a.post_date = e.calendar_date
    where a.post_date = l_dc_date  --g_date
      and b.business_unit_no = 50
      and c.MERCHANDISE_CATEGORY_DESC_100 = 'L'
      and d.wh_fd_zone_no = 10
      and c.product_class_desc_507 = 'C'
      group by b.business_unit_no) montc
on  tots.business_unit_no = montc.business_unit_no

--MIDRAND
left join

  (select b.business_unit_no, sum(a.ll_dc_out_of_stock) mid_out_of_stock
     from  rtl_depot_item_dy a
     join dim_item b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda c on b.sk1_item_no = c.sk1_item_no
     join dim_location d on a.sk1_location_no = d.sk1_location_no
     join dim_calendar e on a.post_date = e.calendar_date
    where a.post_date = l_dc_date  --g_date
      and b.business_unit_no = 50
      and c.MERCHANDISE_CATEGORY_DESC_100 = 'L'
      and d.wh_fd_zone_no = 30
      group by b.business_unit_no) MID
on   tots.business_unit_no = mid.business_unit_no

left join

  (select  b.business_unit_no, sum(a.ll_dc_out_of_stock) mida_out_of_stock
     from  rtl_depot_item_dy a
     join dim_item b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda c on b.sk1_item_no = c.sk1_item_no
     join dim_location d on a.sk1_location_no = d.sk1_location_no
     join dim_calendar e on a.post_date = e.calendar_date
    where a.post_date = l_dc_date  --g_date
      and b.business_unit_no = 50
      and c.MERCHANDISE_CATEGORY_DESC_100 = 'L'
      and d.wh_fd_zone_no = 30
      and c.product_class_desc_507 = 'A'
      group by b.business_unit_no) mida
on  tots.business_unit_no = mida.business_unit_no

left join

  (select  b.business_unit_no, sum(a.ll_dc_out_of_stock) midb_out_of_stock
     from  rtl_depot_item_dy a
     join dim_item b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda c on b.sk1_item_no = c.sk1_item_no
     join dim_location d on a.sk1_location_no = d.sk1_location_no
     join dim_calendar e on a.post_date = e.calendar_date
    where a.post_date = l_dc_date  --g_date
      and b.business_unit_no = 50
      and c.MERCHANDISE_CATEGORY_DESC_100 = 'L'
      and d.wh_fd_zone_no = 30
      and c.product_class_desc_507 = 'B'
      group by b.business_unit_no) midb
on  tots.business_unit_no = midb.business_unit_no

left join

  (select  b.business_unit_no, sum(a.ll_dc_out_of_stock) midc_out_of_stock
     from  rtl_depot_item_dy a
     join dim_item b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda c on b.sk1_item_no = c.sk1_item_no
     join dim_location d on a.sk1_location_no = d.sk1_location_no
     join dim_calendar e on a.post_date = e.calendar_date
    where a.post_date = l_dc_date  --g_date
      and b.business_unit_no = 50
      and c.MERCHANDISE_CATEGORY_DESC_100 = 'L'
      and d.wh_fd_zone_no = 30
      and c.product_class_desc_507 = 'C'
      group by b.business_unit_no) midc
on  tots.business_unit_no = midc.business_unit_no

--SUPPLIER ORDERFILL % FOR TOTAL FOODS
left join

   (select b.business_unit_no,
      (case WHEN nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0 then  round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
         else 0
       end) orderfill
  from rtl_po_supchain_loc_item_dy a, dim_item b
where tran_date = g_date
and a.sk1_item_no = b.sk1_item_no
and b.business_unit_no = 50
group by b.business_unit_no) odf
on tots.business_unit_no = odf.business_unit_no


--SUPPLIER ORDERFILL% FOR TG1 =
left join

   (select b.business_unit_no,
      (case WHEN nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0 then  round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
         else 0
       end) orderfill_tg1
  from rtl_po_supchain_loc_item_dy a, dim_item b, dim_item_uda c
where tran_date = g_date
and a.sk1_item_no = b.sk1_item_no
and b.business_unit_no = 50
and b.sk1_item_no = c.sk1_item_no
and c.commercial_manager_desc_562 = 'TRADING GROUP 1'
group by b.business_unit_no) odftg1
on tots.business_unit_no = odftg1.business_unit_no

--SUPPLIER ORDERFILL% FOR TG1 FOR PROD CLASS A
left join

   (select b.business_unit_no,
      (case WHEN nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0 then  round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
         else 0
       end) orderfill_tg1a
  from rtl_po_supchain_loc_item_dy a, dim_item b, dim_item_uda c
where tran_date = g_date
and a.sk1_item_no = b.sk1_item_no
and b.business_unit_no = 50
and b.sk1_item_no = c.sk1_item_no
and c.commercial_manager_desc_562 = 'TRADING GROUP 1'
and c.product_class_desc_507 = 'A'
group by b.business_unit_no) odftg1A
on tots.business_unit_no = odftg1A.business_unit_no

--SUPPLIER ORDERFILL% FOR TG1 FOR PROD CLASS B
left join

   (select b.business_unit_no,
      (case WHEN nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0 then  round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
         else 0
       end) orderfill_tg1b
  from rtl_po_supchain_loc_item_dy a, dim_item b, dim_item_uda c
where tran_date = g_date
and a.sk1_item_no = b.sk1_item_no
and b.business_unit_no = 50
and b.sk1_item_no = c.sk1_item_no
and c.commercial_manager_desc_562 = 'TRADING GROUP 1'
and c.product_class_desc_507 = 'B'
group by b.business_unit_no) odftg1B
on tots.business_unit_no = odftg1B.business_unit_no

--SUPPLIER ORDERFILL% FOR TG1 FOR PROD CLASS C
left join

   (select b.business_unit_no,
      (case WHEN nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0 then  round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
         else 0
       end) orderfill_tg1c
  from rtl_po_supchain_loc_item_dy a, dim_item b, dim_item_uda c
where tran_date = g_date
and a.sk1_item_no = b.sk1_item_no
and b.business_unit_no = 50
and b.sk1_item_no = c.sk1_item_no
and c.commercial_manager_desc_562 = 'TRADING GROUP 1'
and c.product_class_desc_507 = 'C'
group by b.business_unit_no) odftg1C
on tots.business_unit_no = odftg1C.business_unit_no

--SUPPLIER ORDERFILL% FOR TG2 =
left join

   (select b.business_unit_no,
      (case WHEN nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0 then  round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
         else 0
       end) orderfill_TG2
  from rtl_po_supchain_loc_item_dy a, dim_item b, dim_item_uda c
where tran_date = g_date
and a.sk1_item_no = b.sk1_item_no
and b.business_unit_no = 50
and b.sk1_item_no = c.sk1_item_no
and c.commercial_manager_desc_562 = 'TRADING GROUP 2'
group by b.business_unit_no) odfTG2
on tots.business_unit_no = odfTG2.business_unit_no

--SUPPLIER ORDERFILL% FOR TG2 FOR PROD CLASS A
left join

   (select b.business_unit_no,
      (case WHEN nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0 then  round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
         else 0
       end) orderfill_TG2a
  from rtl_po_supchain_loc_item_dy a, dim_item b, dim_item_uda c
where tran_date = g_date
and a.sk1_item_no = b.sk1_item_no
and b.business_unit_no = 50
and b.sk1_item_no = c.sk1_item_no
and c.commercial_manager_desc_562 = 'TRADING GROUP 2'
and c.product_class_desc_507 = 'A'
group by b.business_unit_no) odfTG2A
on tots.business_unit_no = odfTG2A.business_unit_no

--SUPPLIER ORDERFILL% FOR TG2 FOR PROD CLASS B
left join

   (select b.business_unit_no,
      (case WHEN nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0 then  round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
         else 0
       end) orderfill_TG2b
  from rtl_po_supchain_loc_item_dy a, dim_item b, dim_item_uda c
where tran_date = g_date
and a.sk1_item_no = b.sk1_item_no
and b.business_unit_no = 50
and b.sk1_item_no = c.sk1_item_no
and c.commercial_manager_desc_562 = 'TRADING GROUP 2'
and c.product_class_desc_507 = 'B'
group by b.business_unit_no) odfTG2B
on tots.business_unit_no = odfTG2B.business_unit_no

--SUPPLIER ORDERFILL% FOR TG2 FOR PROD CLASS C
left join

   (select b.business_unit_no,
      (case WHEN nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0 then  round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
         else 0
       end) orderfill_TG2c
  from rtl_po_supchain_loc_item_dy a, dim_item b, dim_item_uda c
where tran_date = g_date
and a.sk1_item_no = b.sk1_item_no
and b.business_unit_no = 50
and b.sk1_item_no = c.sk1_item_no
and c.commercial_manager_desc_562 = 'TRADING GROUP 2'
and c.product_class_desc_507 = 'C'
group by b.business_unit_no) odfTG2C
on tots.business_unit_no = odfTG2C.business_unit_no
) select * from stock;

g_rec_in             c_stock%rowtype;

--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************
procedure local_address_variables as
begin

      l_text := 'LOCAL_ADDRESS_VARIABLES';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      g_sms_string        := g_sms_string||'#OOS'||  --#L.'||g_rec_in.tot_out_of_stock||
                             '#MG '||g_rec_in.mont_out_of_stock||' A'||g_rec_in.monta_out_of_stock||' B'||g_rec_in.montb_out_of_stock||' C'||g_rec_in.montc_out_of_stock||
                             '#MD '||g_rec_in.mid_out_of_stock||' A'||g_rec_in.mida_out_of_stock||' B'||g_rec_in.midb_out_of_stock||' C'||g_rec_in.midc_out_of_stock||
                             '#ORDER FILL%'||
                             '#T.OF% '||g_rec_in.orderfill||
                             '#P.OF% '||g_rec_in.orderfill_tg1 ||' A'||g_rec_in.orderfill_tg1a||' B'||g_rec_in.orderfill_tg1b||' C'||g_rec_in.orderfill_tg1c||
                             '#L.OF% '||g_rec_in.orderfill_tg2 ||' A'||g_rec_in.orderfill_tg2a||' B'||g_rec_in.orderfill_tg2b||' C'||g_rec_in.orderfill_tg2c ;


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
   g_rec_out.record_type            := 'FOOSMS';   --'FDOOS';
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
   l_text := 'CREATION OF DAILY FOODS DC OUT OF STOCK AND SUPPLIER ORDER FILL % SMS STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'system gdate = '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   g_date:=g_date;

   select ct.this_week_start_date, cs.ly_calendar_date, ct.ly_calendar_date, ct.rsa_public_holiday_ind
   into   g_this_wk_strt_dte,      g_ly_wk_strt_dte,    g_ly_to_dte,         g_hol_ind
   from   dim_calendar ct join dim_calendar cs on cs.calendar_date = ct.this_week_start_date
   where  ct.calendar_date = g_date;

   if g_hol_ind is null then
      g_hol_ind := 0;
   end if;

   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'DATE BEING PROCESSED FOR DC OUT OF STOCK - '||l_dc_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'DATE BEING PROCESSED FOR SUPPLIER ORDERFILL - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   update      rtl_sms_publish
   set         processed_ind = 1
   where       record_type = 'FOOSMS'   --'FDOOS'
   and         processed_ind = 0;


   g_sms_string := to_char(trunc(g_date),'dd/mm/yyyy');

   l_text := 'g_hod_ind = '||g_hol_ind;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Fetch loop controlling main program execution
--**************************************************************************************************
   open c_stock;
   loop
      fetch c_stock into g_rec_in;
      exit when c_stock%NOTFOUND;
      g_recs_read := g_recs_read + 1;
      local_address_variables;
   end loop;
   close c_stock;

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

end wh_prf_corp_767E;
