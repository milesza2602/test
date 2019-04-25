--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_743U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_743U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLAN PO data at zone item suppl level to JDAFF fact table in the performance layer
--               with input ex foundation layer.
--
--  Tables:      Input  - fnd_zone_item_supp_ff_po_plan
--               Output - rtl_zone_item_dy_supp_po_pln_r
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--               22/Feb/16 - B Kirschner  (ref: BK22/feb/16)
--               add logic for population of sk1_from_loc_no as provided in input table
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_recs_read          integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
--g_cases              rtl_zone_item_dy_supp_po_pln_r.dc_plan_store_cases%type;
g_rec_out            dwh_performance.rtl_zone_item_dy_supp_po_pln_r%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_today_day          number;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_743U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PLAN FACT DATA FROM OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_zone_item_dy_supp_po_pln_r%rowtype index by binary_integer;
type tbl_array_u is table of rtl_zone_item_dy_supp_po_pln_r%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

l_today_date        date          := trunc(sysdate);
l_day_no            number;

cursor c_jdaff_wh_plan is
   with item_list as (select item_no, sk1_item_no  from dim_item a),
      loc_listf as (select location_no, sk1_location_no from dim_location),     -- BK22/feb/16
      loc_listt as (select location_no, sk1_location_no from dim_location),     -- BK22/feb/16
      supp_list as (select supplier_no, sk1_supplier_no from dim_supplier),
      zone_list as (select zone_no, sk1_zone_group_zone_no from dim_zone),

 ff as (
   select zl.sk1_zone_group_zone_no sk1_zone_no,
          il.sk1_item_no,
          sl.sk1_supplier_no,
          llf.sk1_location_no sk1_from_loc_no,                                  -- BK22/feb/16
          llt.sk1_location_no sk1_to_loc_no,                                    -- BK22/feb/16
          a.day01_cases,
          a.day02_cases,
          a.day03_cases,
          a.day04_cases,
          a.day05_cases,
          a.day06_cases,
          a.day07_cases,
          a.day08_cases,
          a.day09_cases,
          a.day10_cases,
          a.day11_cases,
          a.day12_cases,
          a.day13_cases,
          a.day14_cases,
          a.day15_cases,
          a.day16_cases,
          a.day17_cases,
          a.day18_cases,
          a.day19_cases,
          a.day20_cases,
          a.day21_cases,
          a.calendar_date
      from FND_ZONE_ITEM_SUPP_FF_PO_PLAN a,
           item_list il,
--           loc_list ll,                                                       -- BK22/feb/16
           loc_listf llf,                                                       -- BK22/feb/16
           loc_listt llt,                                                       -- BK22/feb/16
           supp_list sl,
           zone_list zl
       where a.item_no      = il.item_no
         and a.supplier_no  = sl.supplier_no
--         and a.from_loc_no  = ll.location_no                                  -- BK22/feb/16
         and a.from_loc_no  = llf.location_no                                   -- BK22/feb/16
         and a.to_loc_no    = llt.location_no                                   -- BK22/feb/16
         and a.zone_no      = zl.zone_no),  -- select * from ff     ;

 all_together as (
    select atg.sk1_zone_no,
           atg.sk1_item_no,
           atg.sk1_supplier_no,
           atg.sk1_from_loc_no,                                                 -- BK22/feb/16
           atg.sk1_to_loc_no,                                                   -- BK22/feb/16
           atg.day01_cases,
           atg.day02_cases,
           atg.day03_cases,
           atg.day04_cases,
           atg.day05_cases,
           atg.day06_cases,
           atg.day07_cases,
           atg.day08_cases,
           atg.day09_cases,
           atg.day10_cases,
           atg.day11_cases,
           atg.day12_cases,
           atg.day13_cases,
           atg.day14_cases,
           atg.day15_cases,
           atg.day16_cases,
           atg.day17_cases,
           atg.day18_cases,
           atg.day19_cases,
           atg.day20_cases,
           atg.day21_cases,
           atg.calendar_date,
           null as dc_supp_inbound_cases,
           g_date as last_updated_date   --g_date
     from ff atg)

  select * from all_together;

-- For input bulk collect --
type stg_array is table of c_jdaff_wh_plan%rowtype;
a_stg_input          stg_array;
g_rec_in             c_jdaff_wh_plan%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_zone_no            := g_rec_in.sk1_zone_no;
   g_rec_out.sk1_item_no            := g_rec_in.sk1_item_no;
   g_rec_out.sk1_supplier_no        := g_rec_in.sk1_supplier_no;
   g_rec_out.sk1_from_loc_no        := g_rec_in.sk1_from_loc_no;                -- BK22/feb/16
   g_rec_out.sk1_to_loc_no          := g_rec_in.sk1_to_loc_no;
   g_rec_out.calendar_date          := g_rec_in.calendar_date;

   select fin_day_no
     into l_day_no
     from dim_calendar
    where calendar_date = g_rec_in.calendar_date;

   case l_day_no
     when 1 then
         g_rec_out.week_1_day_1_cases     := g_rec_in.day01_cases;
         g_rec_out.week_1_day_2_cases     := g_rec_in.day02_cases;
         g_rec_out.week_1_day_3_cases     := g_rec_in.day03_cases;
         g_rec_out.week_1_day_4_cases     := g_rec_in.day04_cases;
         g_rec_out.week_1_day_5_cases     := g_rec_in.day05_cases;
         g_rec_out.week_1_day_6_cases     := g_rec_in.day06_cases;
         g_rec_out.week_1_day_7_cases     := g_rec_in.day07_cases;
         g_rec_out.week_2_day_1_cases     := g_rec_in.day08_cases;
         g_rec_out.week_2_day_2_cases     := g_rec_in.day09_cases;
         g_rec_out.week_2_day_3_cases     := g_rec_in.day10_cases;
         g_rec_out.week_2_day_4_cases     := g_rec_in.day11_cases;
         g_rec_out.week_2_day_5_cases     := g_rec_in.day12_cases;
         g_rec_out.week_2_day_6_cases     := g_rec_in.day13_cases;
         g_rec_out.week_2_day_7_cases     := g_rec_in.day14_cases;
         g_rec_out.week_3_day_1_cases     := g_rec_in.day15_cases;
         g_rec_out.week_3_day_2_cases     := g_rec_in.day16_cases;
         g_rec_out.week_3_day_3_cases     := g_rec_in.day17_cases;
         g_rec_out.week_3_day_4_cases     := g_rec_in.day18_cases;
         g_rec_out.week_3_day_5_cases     := g_rec_in.day19_cases;
         g_rec_out.week_3_day_6_cases     := g_rec_in.day20_cases;
         g_rec_out.week_3_day_7_cases     := g_rec_in.day21_cases;

     when 2 then
         g_rec_out.week_1_day_1_cases     := 0;
         g_rec_out.week_1_day_2_cases     := g_rec_in.day01_cases;
         g_rec_out.week_1_day_3_cases     := g_rec_in.day02_cases;
         g_rec_out.week_1_day_4_cases     := g_rec_in.day03_cases;
         g_rec_out.week_1_day_5_cases     := g_rec_in.day04_cases;
         g_rec_out.week_1_day_6_cases     := g_rec_in.day05_cases;
         g_rec_out.week_1_day_7_cases     := g_rec_in.day06_cases;
         g_rec_out.week_2_day_1_cases     := g_rec_in.day07_cases;
         g_rec_out.week_2_day_2_cases     := g_rec_in.day08_cases;
         g_rec_out.week_2_day_3_cases     := g_rec_in.day09_cases;
         g_rec_out.week_2_day_4_cases     := g_rec_in.day10_cases;
         g_rec_out.week_2_day_5_cases     := g_rec_in.day11_cases;
         g_rec_out.week_2_day_6_cases     := g_rec_in.day12_cases;
         g_rec_out.week_2_day_7_cases     := g_rec_in.day13_cases;
         g_rec_out.week_3_day_1_cases     := g_rec_in.day14_cases;
         g_rec_out.week_3_day_2_cases     := g_rec_in.day15_cases;
         g_rec_out.week_3_day_3_cases     := g_rec_in.day16_cases;
         g_rec_out.week_3_day_4_cases     := g_rec_in.day17_cases;
         g_rec_out.week_3_day_5_cases     := g_rec_in.day18_cases;
         g_rec_out.week_3_day_6_cases     := g_rec_in.day19_cases;
         g_rec_out.week_3_day_7_cases     := g_rec_in.day20_cases;

     when 3 then
         g_rec_out.week_1_day_1_cases     := 0;
         g_rec_out.week_1_day_2_cases     := 0;
         g_rec_out.week_1_day_3_cases     := g_rec_in.day01_cases;
         g_rec_out.week_1_day_4_cases     := g_rec_in.day02_cases;
         g_rec_out.week_1_day_5_cases     := g_rec_in.day03_cases;
         g_rec_out.week_1_day_6_cases     := g_rec_in.day04_cases;
         g_rec_out.week_1_day_7_cases     := g_rec_in.day05_cases;
         g_rec_out.week_2_day_1_cases     := g_rec_in.day06_cases;
         g_rec_out.week_2_day_2_cases     := g_rec_in.day07_cases;
         g_rec_out.week_2_day_3_cases     := g_rec_in.day08_cases;
         g_rec_out.week_2_day_4_cases     := g_rec_in.day09_cases;
         g_rec_out.week_2_day_5_cases     := g_rec_in.day10_cases;
         g_rec_out.week_2_day_6_cases     := g_rec_in.day11_cases;
         g_rec_out.week_2_day_7_cases     := g_rec_in.day12_cases;
         g_rec_out.week_3_day_1_cases     := g_rec_in.day13_cases;
         g_rec_out.week_3_day_2_cases     := g_rec_in.day14_cases;
         g_rec_out.week_3_day_3_cases     := g_rec_in.day15_cases;
         g_rec_out.week_3_day_4_cases     := g_rec_in.day16_cases;
         g_rec_out.week_3_day_5_cases     := g_rec_in.day17_cases;
         g_rec_out.week_3_day_6_cases     := g_rec_in.day18_cases;
         g_rec_out.week_3_day_7_cases     := g_rec_in.day19_cases;

     when 4 then
         g_rec_out.week_1_day_1_cases     := 0;
         g_rec_out.week_1_day_2_cases     := 0;
         g_rec_out.week_1_day_3_cases     := 0;
         g_rec_out.week_1_day_4_cases     := g_rec_in.day01_cases;
         g_rec_out.week_1_day_5_cases     := g_rec_in.day02_cases;
         g_rec_out.week_1_day_6_cases     := g_rec_in.day03_cases;
         g_rec_out.week_1_day_7_cases     := g_rec_in.day04_cases;
         g_rec_out.week_2_day_1_cases     := g_rec_in.day05_cases;
         g_rec_out.week_2_day_2_cases     := g_rec_in.day06_cases;
         g_rec_out.week_2_day_3_cases     := g_rec_in.day07_cases;
         g_rec_out.week_2_day_4_cases     := g_rec_in.day08_cases;
         g_rec_out.week_2_day_5_cases     := g_rec_in.day09_cases;
         g_rec_out.week_2_day_6_cases     := g_rec_in.day10_cases;
         g_rec_out.week_2_day_7_cases     := g_rec_in.day11_cases;
         g_rec_out.week_3_day_1_cases     := g_rec_in.day12_cases;
         g_rec_out.week_2_day_2_cases     := g_rec_in.day13_cases;
         g_rec_out.week_3_day_3_cases     := g_rec_in.day14_cases;
         g_rec_out.week_3_day_4_cases     := g_rec_in.day15_cases;
         g_rec_out.week_3_day_5_cases     := g_rec_in.day16_cases;
         g_rec_out.week_3_day_6_cases     := g_rec_in.day17_cases;
         g_rec_out.week_3_day_7_cases     := g_rec_in.day18_cases;

     when 5 then
         g_rec_out.week_1_day_1_cases     := 0;
         g_rec_out.week_1_day_2_cases     := 0;
         g_rec_out.week_1_day_3_cases     := 0;
         g_rec_out.week_1_day_4_cases     := 0;
         g_rec_out.week_1_day_5_cases     := g_rec_in.day01_cases;
         g_rec_out.week_1_day_6_cases     := g_rec_in.day02_cases;
         g_rec_out.week_1_day_7_cases     := g_rec_in.day03_cases;
         g_rec_out.week_2_day_1_cases     := g_rec_in.day04_cases;
         g_rec_out.week_2_day_2_cases     := g_rec_in.day05_cases;
         g_rec_out.week_2_day_3_cases     := g_rec_in.day06_cases;
         g_rec_out.week_2_day_4_cases     := g_rec_in.day07_cases;
         g_rec_out.week_2_day_5_cases     := g_rec_in.day08_cases;
         g_rec_out.week_2_day_6_cases     := g_rec_in.day09_cases;
         g_rec_out.week_2_day_7_cases     := g_rec_in.day10_cases;
         g_rec_out.week_3_day_1_cases     := g_rec_in.day11_cases;
         g_rec_out.week_3_day_2_cases     := g_rec_in.day12_cases;
         g_rec_out.week_3_day_3_cases     := g_rec_in.day13_cases;
         g_rec_out.week_3_day_4_cases     := g_rec_in.day14_cases;
         g_rec_out.week_3_day_5_cases     := g_rec_in.day15_cases;
         g_rec_out.week_3_day_6_cases     := g_rec_in.day16_cases;
         g_rec_out.week_3_day_7_cases     := g_rec_in.day17_cases;

     when 6 then
         g_rec_out.week_1_day_1_cases     := 0;
         g_rec_out.week_1_day_2_cases     := 0;
         g_rec_out.week_1_day_3_cases     := 0;
         g_rec_out.week_1_day_4_cases     := 0;
         g_rec_out.week_1_day_5_cases     := 0;
         g_rec_out.week_1_day_6_cases     := g_rec_in.day01_cases;
         g_rec_out.week_1_day_7_cases     := g_rec_in.day02_cases;
         g_rec_out.week_2_day_1_cases     := g_rec_in.day03_cases;
         g_rec_out.week_2_day_2_cases     := g_rec_in.day04_cases;
         g_rec_out.week_2_day_3_cases     := g_rec_in.day05_cases;
         g_rec_out.week_2_day_4_cases     := g_rec_in.day06_cases;
         g_rec_out.week_2_day_5_cases     := g_rec_in.day07_cases;
         g_rec_out.week_2_day_6_cases     := g_rec_in.day08_cases;
         g_rec_out.week_2_day_7_cases     := g_rec_in.day09_cases;
         g_rec_out.week_3_day_1_cases     := g_rec_in.day10_cases;
         g_rec_out.week_3_day_2_cases     := g_rec_in.day11_cases;
         g_rec_out.week_3_day_3_cases     := g_rec_in.day12_cases;
         g_rec_out.week_3_day_4_cases     := g_rec_in.day13_cases;
         g_rec_out.week_3_day_5_cases     := g_rec_in.day14_cases;
         g_rec_out.week_3_day_6_cases     := g_rec_in.day15_cases;
         g_rec_out.week_3_day_7_cases     := g_rec_in.day16_cases;

     when 7 then
         g_rec_out.week_1_day_1_cases     := 0;
         g_rec_out.week_1_day_2_cases     := 0;
         g_rec_out.week_1_day_3_cases     := 0;
         g_rec_out.week_1_day_4_cases     := 0;
         g_rec_out.week_1_day_5_cases     := 0;
         g_rec_out.week_1_day_6_cases     := 0;
         g_rec_out.week_1_day_7_cases     := g_rec_in.day01_cases;
         g_rec_out.week_2_day_1_cases     := g_rec_in.day02_cases;
         g_rec_out.week_2_day_2_cases     := g_rec_in.day03_cases;
         g_rec_out.week_2_day_3_cases     := g_rec_in.day04_cases;
         g_rec_out.week_2_day_4_cases     := g_rec_in.day05_cases;
         g_rec_out.week_2_day_5_cases     := g_rec_in.day06_cases;
         g_rec_out.week_2_day_6_cases     := g_rec_in.day07_cases;
         g_rec_out.week_2_day_7_cases     := g_rec_in.day08_cases;
         g_rec_out.week_3_day_1_cases     := g_rec_in.day09_cases;
         g_rec_out.week_3_day_2_cases     := g_rec_in.day10_cases;
         g_rec_out.week_3_day_3_cases     := g_rec_in.day11_cases;
         g_rec_out.week_3_day_4_cases     := g_rec_in.day12_cases;
         g_rec_out.week_3_day_5_cases     := g_rec_in.day13_cases;
         g_rec_out.week_3_day_6_cases     := g_rec_in.day14_cases;
         g_rec_out.week_3_day_7_cases     := g_rec_in.day15_cases;

   end case;

   g_rec_out.dc_supp_inbound_cases  := g_rec_in.dc_supp_inbound_cases;
   g_rec_out.last_updated_date      := g_date;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into dwh_performance.rtl_zone_item_dy_supp_po_pln_r values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_to_loc_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).calendar_date;
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
       update rtl_zone_item_dy_supp_po_pln_r
       set    row                     = a_tbl_update(i)
       where  sk1_zone_no             = a_tbl_update(i).sk1_zone_no
       and    sk1_item_no             = a_tbl_update(i).sk1_item_no
       and    sk1_supplier_no         = a_tbl_update(i).sk1_supplier_no
       and    sk1_to_loc_no           = a_tbl_update(i).sk1_to_loc_no
       and    calendar_date           = a_tbl_update(i).calendar_date;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

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
                       ' '||a_tbl_update(g_error_index).sk1_zone_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no||
                       ' '||a_tbl_update(g_error_index).sk1_to_loc_no||
                       ' '||a_tbl_update(g_error_index).calendar_date;
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
   g_count :=0;

-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_zone_item_dy_supp_po_pln_r
   where  sk1_zone_no         = g_rec_out.sk1_zone_no
   and    sk1_item_no         = g_rec_out.sk1_item_no
   and    sk1_supplier_no     = g_rec_out.sk1_supplier_no
   and    sk1_to_loc_no       = g_rec_out.sk1_to_loc_no
   and    calendar_date       = g_rec_out.calendar_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
      a_count := a_count + 1;
  else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
      a_count := a_count + 1;
  end if;

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
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF rtl_zone_item_dy_supp_po_pln_r EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    l_text := 'TODAY DATE = ' || l_today_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
-- for testing run ....
--g_date := g_date - 3;

    select this_week_start_date, fin_year_no, fin_week_no, fin_day_no
    into   g_start_date,         g_year1,     g_week1,     g_today_day
    from   dim_calendar
    where  calendar_date = g_date;


    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DATA PERIOD - '||g_start_date||' to '|| g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    dbms_output.put_line(g_year1||g_week1||g_year2||g_week2||g_year3||g_week3||g_start_date||g_end_date);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_jdaff_wh_plan;
    fetch c_jdaff_wh_plan bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
       for i in 1 .. a_stg_input.count
       loop
          g_recs_read := g_recs_read + 1;
          if g_recs_read mod 1000000 = 0 then
             l_text := dwh_constants.vc_log_records_processed||
             to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          end if;

          g_rec_in                := a_stg_input(i);

          local_address_variables;
          local_write_output;

       end loop;
       fetch c_jdaff_wh_plan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_jdaff_wh_plan;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_corp_743u;
