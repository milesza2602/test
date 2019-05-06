--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_768E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_768E" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2014
--  Author:      Q Smit
--  Purpose:     Creates Daily Foods End Of Day Business Availability SMS
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_768E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES DAILY FOODS EOD BUSINESS AVAILABILITY SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

g_eodtot_business_unit_no   number(2,0);
g_eodtot_avail              number(5,2);
g_eodtot_availA             number(5,2);
g_eodtot_availB             number(5,2);
g_eodtot_availC             number(5,2);
g_todavail                  number(5,2);
g_prgavail                  number(5,2);
g_eodtot_periavail          number(5,2);
g_eodtot_periavailA         number(5,2);
g_eodtot_periavailB         number(5,2);
g_eodtot_periavailC         number(5,2);


--cursor c_stock is
--   with stock as
--(select eodtot.business_unit_no, eodtot.avail, eodtota.availA, eodtotb.availB, eodtotc.availC,
--        todayavail.todavail, progavail.prgavail,
--        peritot.periavail, periA.periavailA, periB.periavailB, periC.periavailC


--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************
procedure local_address_variables as
begin

      l_text := 'LOCAL_ADDRESS_VARIABLES';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


with aa as (
select /*+ PARALLEL(a,4) */
        c.business_unit_no,
       sum(a.fd_num_avail_days_adj) sum1
from rtl_loc_item_dy_catalog a
--join rtl_loc_item_dy_st_ord b on a.calendar_date = b.post_date
--                             and a.sk1_item_no = b.sk1_item_no
--                             and a.sk1_location_no = b.sk1_location_no
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and a.num_shelf_life_days >= 3
  and a.boh_adj_qty >= 3
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  group by c.business_unit_no
),
bb as (
  select /*+ PARALLEL(a,4) */
        c.business_unit_no,
       sum(a.fd_num_catlg_days_adj) sum2
       from rtl_loc_item_dy_catalog a
--join rtl_loc_item_dy_st_ord b on a.calendar_date = b.post_date
--                             and a.sk1_item_no = b.sk1_item_no
--                             and a.sk1_location_no = b.sk1_location_no
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and a.num_shelf_life_days >= 3
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  group by c.business_unit_no
) select round(((aa.sum1 / bb.sum2) * 100),2)  --avail
  into g_eodtot_avail
 from aa, bb
 where aa.business_unit_no = bb.business_unit_no --eodtot.avail
;

--PRODUCT CLASS A
with aa as (
select /*+ PARALLEL(a,4)  */
        c.business_unit_no,
       sum(a.fd_num_avail_days_adj) sum1
from rtl_loc_item_dy_catalog a
--join rtl_loc_item_dy_st_ord b on a.calendar_date = b.post_date
---                             and a.sk1_item_no = b.sk1_item_no
--                            and a.sk1_location_no = b.sk1_location_no
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and a.num_shelf_life_days >= 3
  and a.boh_adj_qty >= 3
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  and d.product_class_desc_507 = 'A'
  group by c.business_unit_no
),
bb as (
  select /*+ PARALLEL(a,4) */
        c.business_unit_no,
       sum(a.fd_num_catlg_days_adj) sum2
       from rtl_loc_item_dy_catalog a
--join rtl_loc_item_dy_st_ord b on a.calendar_date = b.post_date
--                             and a.sk1_item_no = b.sk1_item_no
--                             and a.sk1_location_no = b.sk1_location_no
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and a.num_shelf_life_days >= 3
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  and d.product_class_desc_507 = 'A'
  group by c.business_unit_no
) select round(((aa.sum1 / bb.sum2) * 100),2)
  into g_eodtot_availA
 from aa, bb
 where aa.business_unit_no = bb.business_unit_no --eodtota.availA
;

--PRODUCT CLASS B
with aa as (
select /*+ PARALLEL(a,4) */
        c.business_unit_no,
       sum(a.fd_num_avail_days_adj) sum1
from rtl_loc_item_dy_catalog a
--join rtl_loc_item_dy_st_ord b on a.calendar_date = b.post_date
--                             and a.sk1_item_no = b.sk1_item_no
--                             and a.sk1_location_no = b.sk1_location_no
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and a.num_shelf_life_days >= 3
  and a.boh_adj_qty >= 3
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  and d.product_class_desc_507 = 'B'
  group by c.business_unit_no
),
bb as (
  select /*+ PARALLEL(a,4)  */
        c.business_unit_no,
       sum(a.fd_num_catlg_days_adj) sum2
       from rtl_loc_item_dy_catalog a
--join rtl_loc_item_dy_st_ord b on a.calendar_date = b.post_date
--                             and a.sk1_item_no = b.sk1_item_no
--                             and a.sk1_location_no = b.sk1_location_no
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and a.num_shelf_life_days >= 3
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  and d.product_class_desc_507 = 'B'
  group by c.business_unit_no
) select round(((aa.sum1 / bb.sum2) * 100),2)   --availB
into g_eodtot_availB
 from aa, bb
 where aa.business_unit_no = bb.business_unit_no --eodtota.availB
;

--PRODUCT CLASS C
with aa as (
select /*+ PARALLEL(a,4)  */
        c.business_unit_no,
       sum(a.fd_num_avail_days_adj) sum1
from rtl_loc_item_dy_catalog a
--join rtl_loc_item_dy_st_ord b on a.calendar_date = b.post_date
--                             and a.sk1_item_no = b.sk1_item_no
--                             and a.sk1_location_no = b.sk1_location_no
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and a.num_shelf_life_days >= 3
  and a.boh_adj_qty >= 3
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  and d.product_class_desc_507 = 'C'
  group by c.business_unit_no
),
bb as (
  select /*+ PARALLEL(a,4) */
        c.business_unit_no,
       sum(a.fd_num_catlg_days_adj) sum2
       from rtl_loc_item_dy_catalog a
--join rtl_loc_item_dy_st_ord b on a.calendar_date = b.post_date
--                             and a.sk1_item_no = b.sk1_item_no
--                             and a.sk1_location_no = b.sk1_location_no
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and a.num_shelf_life_days >= 3
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  and d.product_class_desc_507 = 'C'
  group by c.business_unit_no
) select round(((aa.sum1 / bb.sum2) * 100),2)   --availC
into g_eodtot_availC
 from aa, bb
 where aa.business_unit_no = bb.business_unit_no --eodtota.availC
;

--TOTAL AVAILABLE STOCK FOR FOODS FOR EOD
select /*+ PARALLEL(a,4) */
       -- c.business_unit_no,
       round(((sum(a.fd_num_avail_days_adj)/ sum(a.fd_num_catlg_days_adj)) * 100),1) todavail
into g_todavail
from rtl_loc_item_dy_catalog a
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
 -- group by c.business_unit_no --todayavail.todavail
;

--TOTAL AVAILABLE STOCK FOR FOODS FOR CURRENT WEEK PROGRESSIVE
select /*+ PARALLEL(a,4) */
       -- c.business_unit_no,
       round(((sum(a.fd_num_avail_days_adj)/ sum(a.fd_num_catlg_days_adj)) * 100),1) prgavail
into g_prgavail
from rtl_loc_item_dy_catalog a
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date between g_this_wk_strt_dte and g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
 -- group by c.business_unit_no
 ;   -- progavail

--PERI AVAILABILITY
 select /*+ PARALLEL(a,4)  */
       -- c.business_unit_no,
       round(((sum(a.fd_num_avail_days_adj)/ sum(a.fd_num_catlg_days_adj)) * 100),1) periavail
into g_eodtot_periavail
from rtl_loc_item_dy_catalog a
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
 -- group by c.business_unit_no
 ;   --peritot

--PERI AVAILABILITY PROD CLASS A
select /*+ PARALLEL(a,4)  */
      --  c.business_unit_no,
       round(((sum(a.fd_num_avail_days_adj)/ sum(a.fd_num_catlg_days_adj)) * 100),1) periavailA
into g_eodtot_periavailA
from rtl_loc_item_dy_catalog a
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  and d.product_class_desc_507 = 'A'
 -- group by c.business_unit_no
 ;  -- periA

--PERI AVAILABILITY PROD CLASS B
 select /*+ PARALLEL(a,4)  */
      --  c.business_unit_no,
       round(((sum(a.fd_num_avail_days_adj)/ sum(a.fd_num_catlg_days_adj)) * 100),1) periavailB
into g_eodtot_periavailB
from rtl_loc_item_dy_catalog a
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  and d.product_class_desc_507 = 'B'
  --group by c.business_unit_no
  ;   --periB


  --PERI AVAILABILITY PROD CLASS B
select /*+ PARALLEL(a,4)  */
      --  c.business_unit_no,
       round(((sum(a.fd_num_avail_days_adj)/ sum(a.fd_num_catlg_days_adj)) * 100),1) periavailC
into g_eodtot_periavailC
from rtl_loc_item_dy_catalog a
join dim_item c on a.sk1_item_no = c.sk1_item_no
join dim_item_uda d on c.sk1_item_no = d.sk1_item_no
join dim_location e on a.sk1_location_no = e.sk1_location_no
 where a.calendar_date = g_date
  and c.business_unit_no = 50
  and e.area_no = 9951
  and e.loc_type = 'S'
  and d.commercial_manager_desc_562 = 'TRADING GROUP 1'
  and d.product_class_desc_507 = 'C'
 -- group by c.business_unit_no
  ;   -- periC


      g_sms_string   := g_sms_string||'#SL >=3 BOH >=3'||
                     '#P.EOD% '||g_eodtot_avail ||' A'||g_eodtot_availA||' B'||g_eodtot_availB||' C'||g_eodtot_availC||
                     '#'||
                     --'#T.EOD%'||g_todavail||' '||g_prgavail||
                     '#P.EOD% '||g_eodtot_periavail ||' A'||g_eodtot_periavailA||' B'||g_eodtot_periavailB||' C'||g_eodtot_periavailC;


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
   g_rec_out.record_type            := 'FEODSMS';    --'FDEBA';
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
   l_text := 'CREATION OF DAILY FOODS END OF DAY BUSINESS AVAILABILITY SMS STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
   l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
   dwh_lookup.dim_control(g_date);
   l_text := 'system date = '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select ct.this_week_start_date, cs.ly_calendar_date, ct.ly_calendar_date, ct.rsa_public_holiday_ind
   into   g_this_wk_strt_dte,      g_ly_wk_strt_dte,    g_ly_to_dte,         g_hol_ind
   from   dim_calendar ct join dim_calendar cs on cs.calendar_date = ct.this_week_start_date
   where  ct.calendar_date = g_date;

   if g_hol_ind is null then
      g_hol_ind := 0;
   end if;

   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   update      rtl_sms_publish
   set         processed_ind = 1
   where       record_type = 'FEODSMS'   --'FDEBA'
   and         processed_ind = 0;


   g_sms_string := to_char(trunc(g_date),'dd/mm/yyyy');

   l_text := 'g_hol_ind = '||g_hol_ind;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Fetch loop controlling main program execution
--**************************************************************************************************

   local_address_variables;

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

end wh_prf_corp_768E;
