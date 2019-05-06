--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_258U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_258U" (p_forall_limit in integer, p_success out boolean) as

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Quentin Smit
--  Purpose:     Foods Renewal datacheck comparison extract 2
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   foods_renewal_extract2_2wks
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
g_rec_out            dwh_performance.foods_renewal_extract2_2wks%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_258U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FOODS RENEWAL DATACHECK COMPARISON EXTRACT 2 - 2WKS FORWARD ORDERS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_performance.foods_renewal_extract2_2wks%rowtype index by binary_integer;
type tbl_array_u is table of dwh_performance.foods_renewal_extract2_2wks%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_today_date        date := trunc(sysdate) - 1;


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

    l_text := 'LOAD OF foods_renewal_extract2_2wks started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table dwh_performance.foods_renewal_extract2_2wks');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';
-- ######################################################################################### --
-- The outer joins are needed as there are cases when there are no sales in dense for items  --
-- which must be included in order to show a zero sales index as these records will be       --
-- created when the outer joins to either dense LY or the item price records are found       --
-- ######################################################################################### --

  l_text := 'Date being processed B4 lookup: ' || l_today_date ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 select today_date
   into l_today_date
   from dim_control;


 l_text := 'Date being processed AFTER lookup: ' || l_today_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


-- l_today_date := 'Moo';

INSERT /*+ APPEND PARALLEL (mart,4) */ INTO dwh_performance.foods_renewal_extract2_2wks mart
with
 item_list as (select di.item_no, di.sk1_item_no, di.item_desc, di.sk1_supplier_no, dd.department_no, dd.department_name, di.subclass_no, di.subclass_name
                from dim_item di, dim_department dd
               --where dd.department_no in (44, 88, 41, 47, 68, 75, 85, 46, 51)  --(42,46,59,66,83,86,89)   --(41,58,73,81,88,96)   --,44,45,47,50,55,58,72,73,75,76,77,81,87,88,90,93,95,96,97,98,99)
               --where dd.department_no in   (97,88,83,59,73,91,13,53,95,93,66,89,90,80,58,16,34,42,52,62,85)   --(88, 96, 73, 58, 47,55, 13, 44, 68, 81, 85, 75, 41, 40)
               --where dd.department_no in (15,20,41,43,48,64,71,78,99)
               --where dd.department_no in (49,65,87,88)
--               where dd.department_no in (13,44,68,73,83,10,11,45,55,58,64,82,96)
--               where dd.department_no in (45,64,22,26,27,32,33,67,69,70,97,58,96)
--               where dd.department_no in (12,28,29,36,37,53,57,59,66,76,77,79,86,89,91,95,98)
               where dd.department_no in (14,17,18,19,21,23,24,30,31,35,39,50,56,60,63,72,80,84,90,93)
                 and di.department_no = dd.department_no),

 loc_list as (select location_no, location_name, sk1_location_no, wh_fd_zone_no from dim_location)

select dc.fin_year_no,
       dc.fin_week_no,
       dc.fin_day_no,
       di.item_no,
       di.item_desc,
       loc_list.location_no,
       loc_list.location_name,
       c.post_date,
       c.direct_delivery_ind,
       c.boh_1_qty,
       c.boh_2_qty,
       c.boh_3_qty,
       c.store_order1,
       c.store_order2,
       c.store_order3,
       c.day1_estimate,
       c.day2_estimate,
       c.day3_estimate,
       c.weekly_estimate1,
       c.weekly_estimate2,
       c.direct_mu_qty1,
       c.direct_mu_qty2,
       c.direct_mu_qty3,
       c.direct_mu_qty4,
       c.direct_mu_qty5,
       c.direct_mu_qty6,
       c.direct_mu_qty7,
       c.day4_estimate,
       c.day5_estimate,
       c.day6_estimate,
       c.day7_estimate,
       c.day1_est_unit2,
       c.day2_est_unit2,
       c.day3_est_unit2,
       c.day4_est_unit2,
       c.day5_est_unit2,
       c.day6_est_unit2,
       c.day7_est_unit2,
       c.day1_deliv_pat1,
       c.day2_deliv_pat1,
       c.day3_deliv_pat1,
       c.day4_deliv_pat1,
       c.day5_deliv_pat1,
       c.day6_deliv_pat1,
       c.day7_deliv_pat1

from item_list di,
     loc_list,
     rtl_loc_item_dy_st_ord c,
     dim_calendar dc
where c.post_date = l_today_date
  and c.sk1_item_no = di.sk1_item_no
  and c.sk1_location_no = loc_list.sk1_location_no
  and c.post_date = dc.calendar_date
  order by location_no, item_no
 ;

g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;

commit;

--**************************************************************************************************
-- Write final log data
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

end wh_prf_corp_258u;
