--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_769E_ADHOC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_769E_ADHOC" (p_forall_limit in integer, p_success out boolean) as
--**************************************************************************************************
--  Date:        July 2016
--  Author:      A Joshua
--  Purpose:     Creates Daily/Weekly Foods supplier orderfill% SMS
--
--  Tables:      Input  - rtl_depot_item_dy
--                        rtl_po_supchain_loc_item_dy
--
--               Output - rtl_sms_publish_supp
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
g_fin_year_no        number        :=  0;
g_fin_week_no        number        :=  0;
g_fin_day_no         number        :=  0;
g_date               date;
g_l_dc_date          date;
g_this_wk_start_dte  date;
g_this_wk_end_dte    date;
g_ly_to_dte          date;
g_total_shorts_selling number (14,2);
g_accum_sales_ty     number(19,1);
g_sales_ly_perc      number(19,1);
g_sales_wtd_perc     number(19,1);
g_sms_string         varchar2(500);

g_rec_out            dwh_datafix.aj_rtl_sms_publish_supp%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_769E_ADHOC';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATES DAILY FOODS SUPPLIER ORDERFILL % SMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_dc_date            date :=trunc(sysdate -1); -- NB remove -1 from code
p_cell_no            dwh_foundation.fnd_sp_sms_recipients.cell_no%type;
p_supplier_no        dwh_foundation.fnd_supplier.supplier_no%type;
--p_supplier_no        number := 11105;
g_control_no         dwh_datafix.aj_rtl_sms_publish_supp.control_no%type;


cursor c_supplier is
   select supplier_no, cell_no from dwh_foundation.fnd_sp_sms_recipients where active_ind = 'Y' order by supplier_no;

cursor c_stock_wkly (p_supplier_no number) is
   with stock as
  (select tots.primary_supplier_no, tots.business_unit_no, tots.tot_out_of_stock,
        mont.mont_out_of_stock, mid.mid_out_of_stock,
--        monta.monta_out_of_stock, montb.montb_out_of_stock, montc.montc_out_of_stock,
--        mida.mida_out_of_stock, midb.midb_out_of_stock, midc.midc_out_of_stock,
        mont_orderfill, md_orderfill,
        mont_shorts_selling, md_shorts_selling,
        mont_shorts_cases, md_shorts_cases,
        tots.supplier_long_desc
   from
  (select b.primary_supplier_no, b.business_unit_no, sum(a.ll_dc_out_of_stock) tot_out_of_stock,
          f.supplier_long_desc
     from rtl_depot_item_dy a
     join dim_item          b on a.sk1_item_no = b.sk1_item_no
     join dim_item_uda      c on b.sk1_item_no = c.sk1_item_no
     join dim_calendar      e on a.post_date   = e.calendar_date
     join dim_supplier      f on b.sk1_supplier_no = f.sk1_supplier_no -- AJ
    where a.post_date           = l_dc_date
      and b.business_unit_no    = 50
      and b.primary_supplier_no = p_supplier_no  --11105
      and c.merchandise_category_desc_100 = 'L'
    group by b.primary_supplier_no, b.business_unit_no, f.supplier_long_desc ) tots


-- OUT OF STOCK
  left join -- row 7

  (select b.primary_supplier_no, b.business_unit_no, sum(a.ll_dc_out_of_stock) mont_out_of_stock,
          f.supplier_long_desc
   from   rtl_depot_item_dy a
    join  dim_item          b on a.sk1_item_no     = b.sk1_item_no
    join  dim_item_uda      c on b.sk1_item_no     = c.sk1_item_no
    join  dim_location      d on a.sk1_location_no = d.sk1_location_no
    join  dim_calendar      e on a.post_date       = e.calendar_date
    join dim_supplier       f on b.sk1_supplier_no = f.sk1_supplier_no -- AJ
   where  a.post_date                     = l_dc_date
    and   b.business_unit_no              = 50
    and   b.primary_supplier_no           = p_supplier_no  --11105
    and   c.merchandise_category_desc_100 = 'L'
    and   d.wh_fd_zone_no                 = 10
   group by b.primary_supplier_no, b.business_unit_no, f.supplier_long_desc) mont
  on     (tots.business_unit_no    = mont.business_unit_no
    and   tots.primary_supplier_no = mont.primary_supplier_no)

--SUPPLIER ORDERFILL
  left join -- row 10

  (select c.supplier_no, b.business_unit_no,
      (case when nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0
            then     round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
            else 0
       end) mont_orderfill,
      round(nvl(sum(a.shorts_selling),0),0) mont_shorts_selling,
      round(nvl(sum(a.shorts_cases),0),0) mont_shorts_cases,
      c.supplier_long_desc -- AJ
   from rtl_po_supchain_loc_item_dy a,
        dim_item b,
        dim_supplier c,
        dim_item_uda d,
        dim_location e
   where tran_date                      between g_this_wk_start_dte and g_this_wk_end_dte
    and a.sk1_item_no                   = b.sk1_item_no
    and b.business_unit_no              = 50
    and a.sk1_supplier_no               = c.sk1_supplier_no
    and c.supplier_no                   = p_supplier_no  --11105
    and b.sk1_item_no                   = d.sk1_item_no
    and d.merchandise_category_desc_100 = 'L'
    and a.sk1_location_no               = e.sk1_location_no
    and e.wh_fd_zone_no                 = 10
   group by c.supplier_no, b.business_unit_no, 0, c.supplier_long_desc) mont_odf -- AJ
  on (tots.business_unit_no      = mont_odf.business_unit_no
    and tots.primary_supplier_no = mont_odf.supplier_no)

--MIDRAND
--SHORTS SELLING and CASES (Midrand)
-- OUT OF STOCK
  left join -- row 8 

  (select b.primary_supplier_no, b.business_unit_no, sum(a.ll_dc_out_of_stock) mid_out_of_stock,
          f.supplier_long_desc
   from   rtl_depot_item_dy a
    join  dim_item          b on a.sk1_item_no     = b.sk1_item_no
    join  dim_item_uda      c on b.sk1_item_no     = c.sk1_item_no
    join  dim_location      d on a.sk1_location_no = d.sk1_location_no
    join  dim_calendar      e on a.post_date       = e.calendar_date    
    join  dim_supplier      f on b.sk1_supplier_no = f.sk1_supplier_no -- AJ
   where  a.post_date                     = l_dc_date
    and   b.business_unit_no              = 50
    and   b.primary_supplier_no           = p_supplier_no  --11105
    and   c.merchandise_category_desc_100 = 'L'
    and   d.wh_fd_zone_no                 = 30
   group by b.primary_supplier_no, b.business_unit_no, f.supplier_long_desc) mid
  on      (tots.business_unit_no    = mid.business_unit_no
    and    tots.primary_supplier_no = mid.primary_supplier_no)

--SUPPLIER ORDERFILL
  left join -- row 11

  (select c.supplier_no, b.business_unit_no,
      (case when nvl(round(sum(a.fillrate_fd_latest_po_qty),1),0) > 0
            then     round(((sum(a.fillrate_fd_po_grn_qty)/sum(a.fillrate_fd_latest_po_qty)) * 100),1)
            else 0
       end) md_orderfill,
       round(nvl(sum(a.shorts_selling),0),0) md_shorts_selling,
       round(nvl(sum(a.shorts_cases),0),0) md_shorts_cases,
       c.supplier_long_desc
   from rtl_po_supchain_loc_item_dy a,
        dim_item b,
        dim_supplier c,
        dim_item_uda d,
        dim_location e
   where tran_date                      between g_this_wk_start_dte and g_this_wk_end_dte
    and a.sk1_item_no                   = b.sk1_item_no
    and b.business_unit_no              = 50
    and a.sk1_supplier_no               = c.sk1_supplier_no
    and c.supplier_no                   = p_supplier_no  --11105
    and b.sk1_item_no                   = d.sk1_item_no
    and d.merchandise_category_desc_100 = 'L'
    and a.sk1_location_no               = e.sk1_location_no
    and e.wh_fd_zone_no                 = 30
   group by c.supplier_no, b.business_unit_no, c.supplier_long_desc) md_odf
  on (tots.business_unit_no      = md_odf.business_unit_no
    and tots.primary_supplier_no = md_odf.supplier_no)

) select * from stock;

g_rec_in             c_stock_wkly%rowtype;

--**************************************************************************************************
-- Process data read from input
--**************************************************************************************************
procedure local_address_variables as
begin

--     if g_fin_day_no = 7 then
        g_sms_string        := --g_sms_string||
                             '#SUPPLIER WW'|| g_rec_in.supplier_long_desc||  -- AJ
                             '#'||g_fin_year_no|| ' WK ' ||g_fin_week_no||
                             '#SHORTS'||
                             '#MG '||'R'||nvl(trim(to_char(g_rec_in.mont_shorts_selling,'9,999,999')),0)||' CASES '||nvl(trim(to_char(g_rec_in.mont_shorts_cases,'9999999')),0)||
                             '#MD '||'R'||nvl(trim(to_char(g_rec_in.md_shorts_selling,'9,999,999')),0)||' CASES '||nvl(trim(to_char(g_rec_in.md_shorts_cases,'9999999')),0)||
                             '#OOS'||
                             '#MG '||g_rec_in.mont_out_of_stock||
--                                  ' A'||nvl(g_rec_in.monta_out_of_stock,0)||
--                                  ' B'||nvl(g_rec_in.montb_out_of_stock,0)||
--                                  ' C'||nvl(g_rec_in.montc_out_of_stock,0)||
                             '#MD '||nvl(g_rec_in.mid_out_of_stock,0)||
--                                  ' A'||nvl(g_rec_in.mida_out_of_stock,0)||
--                                  ' B'||nvl(g_rec_in.midb_out_of_stock,0)||
--                                  ' C'||nvl(g_rec_in.midc_out_of_stock,0)||
                             '#ORDER FILL%'||
                             '#MG '||nvl(g_rec_in.mont_orderfill,0)||'%'||
                             '#MD '||nvl(g_rec_in.md_orderfill,0)||'%';
--     else
--        g_sms_string        := --g_sms_string||
--                             '#SUPPLIER WW'|| g_rec_in.primary_supplier_no||
--                             '#'||to_char(trunc(l_dc_date),'dd/mm/yyyy')||
--                             '#SHORTS'||
--                             '#MG '||'R'||nvl(trim(to_char(g_rec_in.mont_shorts_selling,'9,999,999')),0)||
--                             '#MD '||'R'||nvl(trim(to_char(g_rec_in.md_shorts_selling,'9,999,999')),0)||
--                             '#OOS'||
--                             '#MG '||g_rec_in.mont_out_of_stock||
--                                  ' A'||nvl(g_rec_in.monta_out_of_stock,0)||
--                                  ' B'||nvl(g_rec_in.montb_out_of_stock,0)||
--                                  ' C'||nvl(g_rec_in.montc_out_of_stock,0)||
--                             '#MD '||nvl(g_rec_in.mid_out_of_stock,0)||
--                                  ' A'||nvl(g_rec_in.mida_out_of_stock,0)||
--                                  ' B'||nvl(g_rec_in.midb_out_of_stock,0)||
--                                  ' C'||nvl(g_rec_in.midc_out_of_stock,0);
--     end if;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      --l_text := 'LOCAL_ADDRESS_VARIABLES END';
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end local_address_variables;

--**************************************************************************************************
-- Insert sms record
--**************************************************************************************************
procedure insert_sms_rec as
begin

   g_control_no := g_control_no + 1;
   g_rec_out.calendar_datetime      := sysdate;
   g_rec_out.cell_no                := p_cell_no;
   g_rec_out.processed_ind          := 0;
   g_rec_out.sms_text               := g_sms_string;
   g_rec_out.last_updated_date      := g_date;
   g_rec_out.control_no             := g_control_no;
   g_rec_out.supplier_no            := p_supplier_no;

--   g_total_shorts_selling           := nvl(g_rec_in.mont_shorts_selling,0) + nvl(g_rec_in.md_shorts_selling,0);

--   if g_fin_day_no <> 7 then
--      if g_total_shorts_selling > 0 then
--         insert into rtl_sms_publish_supp values g_rec_out;
--         g_recs_inserted  := g_recs_inserted + sql%rowcount;
--      end if;
--   else
      insert into dwh_datafix.aj_rtl_sms_publish_supp values g_rec_out;
      g_recs_inserted  := g_recs_inserted + sql%rowcount;
--   end if;

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

   --l_text := 'INSERT_SMS_REC END';
   --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
   g_date := g_date - 1;  -- remove -1 from date batch date
   l_text := 'system gdate = '|| g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'dc date = '|| l_dc_date;
--   g_l_dc_date :=  to_char(trunc(l_dc_date),'dd/mm/yyyy');
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



   select ct.this_week_start_date, ct.this_week_end_date,
          ct.rsa_public_holiday_ind, ct.fin_year_no, ct.fin_week_no, ct.fin_day_no
   into   g_this_wk_start_dte, g_this_wk_end_dte, g_hol_ind, g_fin_year_no, g_fin_week_no, g_fin_day_no
   from   dim_calendar ct join dim_calendar cs on cs.calendar_date = ct.this_week_start_date
   where  ct.calendar_date = g_date;

   if g_hol_ind is null then
      g_hol_ind := 0;
   end if;

   l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'DATE BEING PROCESSED FOR SUPPLIER OUT OF STOCK - '||l_dc_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'DATE BEING PROCESSED FOR SUPPLIER ORDERFILL - '||l_dc_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   update      dwh_datafix.aj_rtl_sms_publish_supp
   set         processed_ind = 1
   --where       record_type = 'SOOSMS'   --'FDOOS'
   where         processed_ind = 0;

   commit;

   g_sms_string := to_char(trunc(g_date),'dd/mm/yyyy');

   l_text := 'g_hod_ind = '||g_hol_ind;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select nvl(max(control_no),0) into g_control_no from dwh_datafix.aj_rtl_sms_publish_supp;

   --if g_control_no is null then
   --   g_control_no := 0;
   --end if;

--**************************************************************************************************
-- Fetch loop controlling main program execution
--**************************************************************************************************
 open c_supplier;
 loop
    fetch c_supplier into p_supplier_no, p_cell_no;
    exit when c_supplier%NOTFOUND;

--    if g_fin_day_no <> 7 then
--      open c_stock_dly (p_supplier_no);
--        loop
--            fetch c_stock_dly into g_rec_in;
--            exit when c_stock_dly%NOTFOUND;
--            g_recs_read := g_recs_read + 1;
--            local_address_variables;
--            insert_sms_rec;
--            g_sms_string := to_char(trunc(g_date),'dd/mm/yyyy');
--        end loop;

--      close c_stock_dly;
--    else
      open c_stock_wkly (p_supplier_no);
        loop
            fetch c_stock_wkly into g_rec_in;
            exit when c_stock_wkly%NOTFOUND;
            g_recs_read := g_recs_read + 1;
            local_address_variables;
            insert_sms_rec;
            g_sms_string := to_char(trunc(g_date),'dd/mm/yyyy');
        end loop;

      close c_stock_wkly;
--    end if;
  end loop;

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

end wh_prf_corp_769e_adhoc;
