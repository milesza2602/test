--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_760U_PO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_760U_PO" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        January 2013
--  Author:      Q Smit
--  Purpose:     Update DC PLAN PO data at zone item suppl level to JDA fact table in the performance layer
--               with input ex foundation layer.
--               Three weeks' data on each record on foundation layer
--               must be un-pivotted to result in every day of the three weeks, on a seperate record.
--               *** MUST RUN BEFORE WH_PRF_CORP_742U AS THIS PROGRAM DOES THE DELETES !! ***
--  Tables:      Input  - fnd_zone_item_supp_ff_po_plan
--               Output - rtl_zone_item_dy_supp_po_plan
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_recs_deleted       integer       :=  0;
g_recs               integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_zone_item_dy_supp_po_plan%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_760U_PO';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD ZONE/ITEM/SUPP PLAN FACT DATA FROM JDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_zone_item_dy_supp_po_plan%rowtype index by binary_integer;
type tbl_array_u is table of rtl_zone_item_dy_supp_po_plan%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

g_jda_start_date    date;  -- := sysdate+1;
g_stg_date          date;   

g_po_recs_updated   integer       := 0;

cursor c_zone_item_supp_po_plan is
   select   unique dz.sk1_zone_group_zone_no, di.sk1_item_no, ds.sk1_supplier_no, dc.calendar_date, dih.sk2_item_no,
            dlf.sk1_location_no sk1_from_loc_no, dlt.sk1_location_no sk1_to_loc_no,
            dlhf.sk2_location_no sk2_from_loc_no, dlht.sk2_location_no sk2_to_loc_no,
            sop.sysdata dc_supp_inbound_cases, trunc(sysdate) last_updated_date,
            (sop.sysdata * nvl(zi.case_selling_excl_vat,0)) dc_supp_inbound_selling
   from
   (select  zone_group_no, zone_no, item_no, supplier_no, from_loc_no, to_loc_no,   -- syscol in the unpivot equates to :
            (case (to_number(substr(syscol,4,2))) when 01 then g_year1              --  day01_cases, day02_cases, day03_cases etc
                                                  when 02 then g_year1              --  so the day number (01, 02, 03 etc) is
                                                  when 03 then g_year1              --  used to determine which year / week
                                                  when 04 then g_year1              --  is being referenced
                                                  when 05 then g_year1
                                                  when 06 then g_year1
                                                  when 07 then g_year1

                                                  when 08 then g_year2
                                                  when 09 then g_year2
                                                  when 10 then g_year2
                                                  when 11 then g_year2
                                                  when 12 then g_year2
                                                  when 13 then g_year2
                                                  when 14 then g_year2

                                                  when 15 then g_year3
                                                  when 16 then g_year3
                                                  when 17 then g_year3
                                                  when 18 then g_year3
                                                  when 19 then g_year3
                                                  when 20 then g_year3
                                                  else g_year3 end) yearno,

            (case (to_number(substr(syscol,4,2))) when 01 then g_week1
                                                  when 02 then g_week1
                                                  when 03 then g_week1
                                                  when 04 then g_week1
                                                  when 05 then g_week1
                                                  when 06 then g_week1
                                                  when 07 then g_week1

                                                  when 08 then g_week2
                                                  when 09 then g_week2
                                                  when 10 then g_week2
                                                  when 11 then g_week2
                                                  when 12 then g_week2
                                                  when 13 then g_week2
                                                  when 14 then g_week2

                                                  when 15 then g_week3
                                                  when 16 then g_week3
                                                  when 17 then g_week3
                                                  when 18 then g_week3
                                                  when 19 then g_week3
                                                  when 20 then g_week3
                                                  else g_week3 end) weekno,

           -- when 1 then g_week1 when 2 then g_week2 else g_week3 end) weekno,
           -- to_number(substr(syscol,12,1)) dayno,
           (case (to_number(substr(syscol,4,2))) when 01 then 01
                                                 when 02 then 02
                                                 when 03 then 03
                                                 when 04 then 04
                                                 when 05 then 05
                                                 when 06 then 06
                                                 when 07 then 07

                                                 when 08 then 01
                                                 when 09 then 02
                                                 when 10 then 03
                                                 when 11 then 04
                                                 when 12 then 05
                                                 when 13 then 06
                                                 when 14 then 07

                                                 when 15 then 01
                                                 when 16 then 02
                                                 when 17 then 03
                                                 when 18 then 04
                                                 when 19 then 05
                                                 when 20 then 06
                                                 when 21 then 07
                                                 end ) dayno,
            syscol,
            sysdata,
            last_updated_date
   from     fnd_zone_item_supp_ff_po_plan
   unpivot  include nulls (sysdata for syscol in (day01_cases,
                                                  day02_cases,
                                                  day03_cases,
                                                  day04_cases,
                                                  day05_cases,
                                                  day06_cases,
                                                  day07_cases,
                                                  day08_cases,
                                                  day09_cases,
                                                  day10_cases,
                                                  day11_cases,
                                                  day12_cases,
                                                  day13_cases,
                                                  day14_cases,
                                                  day15_cases,
                                                  day16_cases,
                                                  day17_cases,
                                                  day18_cases,
                                                  day19_cases,
                                                  day20_cases,
                                                  day21_cases))) sop
   join     dim_calendar dc         on  sop.yearno        = dc.fin_year_no
                                    and sop.weekno        = dc.fin_week_no
                                    and sop.dayno         = dc.fin_day_no
   left outer join
            fnd_zone_item zi     on  sop.zone_group_no = zi.zone_group_no
                                    and sop.zone_no       = zi.zone_no
                                    and sop.item_no       = zi.item_no
   join     dim_zone dz             on  sop.zone_group_no = dz.zone_group_no
                                    and sop.zone_no       = dz.zone_no
   join     dim_item di             on  sop.item_no       = di.item_no
   join     dim_item_hist dih       on  sop.item_no       = dih.item_no
                                    and dc.calendar_date  between dih.sk2_active_from_date and dih.sk2_active_to_date
   join     dim_supplier ds         on  sop.supplier_no   = ds.supplier_no
   join     dim_location dlf        on  sop.from_loc_no   = dlf.location_no
   join     dim_location_hist dlhf  on  sop.from_loc_no   = dlhf.location_no
                                    and dc.calendar_date  between dlhf.sk2_active_from_date and dlhf.sk2_active_to_date
   join     dim_location dlt        on  sop.to_loc_no     = dlt.location_no
   join     dim_location_hist dlht  on  sop.to_loc_no     = dlht.location_no
                                    and dc.calendar_date  between dlht.sk2_active_from_date and dlht.sk2_active_to_date
   where sop.last_updated_date = g_date
   --and di.item_no = 4002103218852
   --and di.item_no = 20008604 and ds.supplier_no = 57752 
   order by dz.sk1_zone_group_zone_no, di.sk1_item_no, ds.sk1_supplier_no, dc.calendar_date;

-- This procedure does not select only where last_updated_date = g_date, because fnd_zone_item_supp_ff_po_plan
-- gets fully refreshed every day.

-- For input bulk collect --
type stg_array is table of c_zone_item_supp_po_plan%rowtype;
a_stg_input          stg_array;
g_rec_in             c_zone_item_supp_po_plan%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                       := g_rec_in;
   g_rec_out.last_updated_date     := g_date;

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
       insert into W6005682.RTL_ZONE_ITEM_DY_SUPP_PO_PLANQ values a_tbl_insert(i);    --rtl_zone_item_dy_supp_po_plan


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
                       ' '||a_tbl_insert(g_error_index).sk1_zone_group_zone_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).sk1_supplier_no||
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
       update W6005682.RTL_ZONE_ITEM_DY_SUPP_PO_PLANQ   --rtl_zone_item_dy_supp_po_plan
       set    row                    = a_tbl_update(i)
       where  sk1_zone_group_zone_no = a_tbl_update(i).sk1_zone_group_zone_no
       and    sk1_item_no            = a_tbl_update(i).sk1_item_no
       and    sk1_supplier_no        = a_tbl_update(i).sk1_supplier_no
       and    calendar_date          = a_tbl_update(i).calendar_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_zone_group_zone_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no||
                       ' '||a_tbl_update(g_error_index).calendar_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- The input table is cleared each day before it has data added to it.
-- As a result of this, and are out of sync.
-- We have to delete all records from rtl_zone_item_dy_supp_po_plan for the same period from
-- ie. between g_start_date and g_end_date
-- Eventhough we could do a bulk delete, the consequences of running this job too late,
-- could result in resource conflicts and hence the delete taking along time and slowing-down the delete;
-- This has been in experience with other procedures.
--
-- VERY IMPORTANT !!
-- THIS PROCEDURE MUST RUN BEFORE THE EXISTING PROGRAM (WH_PRF_CORP_742U) IN ORDER FOR THE DELETES
-- TO BE DONE ONLY ONCE FOR THIS TABLE.
-- END OF VERY IMPORTANT MESSAGE !!
--**************************************************************************************************
procedure delete_rtl_om as
begin

  g_recs_deleted  := 0;

  delete from rtl_zone_item_dy_supp_po_plan r
   where exists (
     with aa as (
       select b.sk1_zone_group_zone_no, c.sk1_item_no, d.sk1_supplier_no  --, a.calendar_date--, a.last_updated_date
         from fnd_zone_item_supp_om_po_plan a, dim_zone b, dim_item c, dim_supplier d
        where a.last_updated_date = g_date-1
          and a.zone_group_no = b.zone_group_no
          and a.item_no = c.item_no
          and a.supplier_no = d.supplier_no
          and a.zone_no = b.zone_no)

           select aa.sk1_zone_group_zone_no,
                  aa.sk1_item_no,
                  aa.sk1_supplier_no
                  --aa.calendar_date
             from aa
            where aa.sk1_zone_group_zone_no = r.sk1_zone_group_zone_no
              and aa.sk1_item_no = r.sk1_item_no
              and aa.sk1_supplier_no = r.sk1_supplier_no)
              --and aa.calendar_date = r.calendar_date)

       and r.calendar_date between g_start_date and g_end_date  ;

     g_recs := sql%rowcount;
     g_recs_deleted  := g_recs_deleted  + g_recs;
  COMMIT;

  exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := 'delete rtl error'||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_zone_group_zone_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no||
                       ' '||a_tbl_update(g_error_index).calendar_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end delete_rtl_om;

--**************************************************************************************************
-- This code was changed to delete ALL forward JDA from the date from which data will be loaded in this program
-- which equates to sysdate + 1.
-- The JDA department rollout table is used to determine which items are JDA items and deletes all records for
-- those instead of using the data loaded into foundation in the current batch combined with the last updated date
-- This change was applied on 20 November 2015 as a result of an incident logged - WW-2395664
--***************************************************************************************************
procedure delete_rtl_jda as
begin

  g_recs_deleted  := 0;

  delete from W6005682.RTL_ZONE_ITEM_DY_SUPP_PO_PLANQ r       --rtl_zone_item_dy_supp_po_plan r
 --select count(*) into g_count from rtl_zone_item_dy_supp_po_plan r
   where exists (
     with aa as (
       select b.sk1_zone_group_zone_no, c.sk1_item_no, d.sk1_supplier_no, a.calendar_date--, a.last_updated_date
         from fnd_zone_item_supp_ff_po_plan a, dim_zone b, dim_item c, dim_supplier d, fnd_jdaff_dept_rollout dept_rollout
     --   where a.last_updated_date >= '01/NOV/15'
     where dept_rollout.department_no = c.department_no
          and dept_rollout.department_live_ind = 'Y'
          and a.zone_group_no = b.zone_group_no
          and a.item_no = c.item_no
          and a.supplier_no = d.supplier_no
          and a.zone_no = b.zone_no
          --and c.item_no = 20008604 and d.supplier_no = 57752
       )

           select aa.sk1_zone_group_zone_no,
                  aa.sk1_item_no,
                  aa.sk1_supplier_no 
                  --aa.calendar_date
             from aa
            where aa.sk1_zone_group_zone_no = r.sk1_zone_group_zone_no
              and aa.sk1_item_no = r.sk1_item_no
              and aa.sk1_supplier_no = r.sk1_supplier_no
              --and aa.calendar_date = r.calendar_date
              )
              
              and r.calendar_date >= g_jda_start_date   --between g_jda_start_date and g_end_date  ;  --g_start_date
   
    ; 

     g_recs := sql%rowcount;
     g_recs_deleted  := g_recs_deleted  + g_recs;
  COMMIT;
  
 exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := 'delete rtl error'||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_zone_group_zone_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no||
                       ' '||a_tbl_update(g_error_index).calendar_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
       
  

end delete_rtl_jda;


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
   from   W6005682.RTL_ZONE_ITEM_DY_SUPP_PO_PLANQ --rtl_zone_item_dy_supp_po_plan
   where  sk1_zone_group_zone_no = g_rec_out.sk1_zone_group_zone_no
   and    sk1_item_no            = g_rec_out.sk1_item_no
   and    sk1_supplier_no        = g_rec_out.sk1_supplier_no
   and    calendar_date          = g_rec_out.calendar_date;

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
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
procedure get_po_data as 
begin

   merge /*+ parallel(mer_lid,4) */ into W6005682.RTL_ZONE_ITEM_DY_SUPP_PO_PLANQ mer_lid      --rtl_zone_item_dy_supp_po_plan
     using (
       select  /*+ parallel(dy,4) parallel(rs,4) */ dy.tran_date, dy.sk1_item_no, dy.sk1_supplier_no,
              dl.sk1_fd_zone_group_zone_no, sum(dy.amended_po_cases) amended_po_cases 
        from rtl_supchain_loc_item_dy dy, 
              dim_item di, 
              dim_supplier ds,
              dim_location dl,
              W6005682.RTL_ZONE_ITEM_DY_SUPP_PO_PLANQ rs   --rtl_zone_item_dy_supp_po_plan rs  --> join to this table to get records that exist
       where rs.last_updated_date = g_date
         and di.sk1_item_no = dy.sk1_item_no
         and ds.sk1_supplier_no = dy.sk1_supplier_no
         and dy.sk1_location_no = dl.sk1_location_no
        
         and rs.sk1_item_no             = dy.sk1_item_no
         and rs.sk1_supplier_no         = dy.sk1_supplier_no
         and rs.sk1_zone_group_zone_no  = dl.sk1_fd_zone_group_zone_no
         and rs.calendar_date           = dy.tran_date
    group by dy.tran_date, dy.sk1_item_no, dy.sk1_supplier_no,dl.sk1_fd_zone_group_zone_no 
   
     ) mer_mart
     
     on (mer_lid.calendar_date          = mer_mart.tran_date
     and mer_lid.sk1_item_no            = mer_mart.sk1_item_no
     and mer_lid.sk1_supplier_no        = mer_mart.sk1_supplier_no
     and mer_lid.sk1_zone_group_zone_no = mer_mart.sk1_fd_zone_group_zone_no)
     
     when matched then update
        set mer_lid.dc_supp_inbound_cases = mer_mart.amended_po_cases
        ;
        
   g_po_recs_updated :=  SQL%ROWCOUNT;

end get_po_data;

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
    l_text := 'LOAD OF rtl_zone_item_dy_supp_po_plan EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    
    --g_date := g_date - 1;    --REMOVE!!
    
    select this_week_start_date, fin_year_no, fin_week_no
    into   g_start_date,         g_year1,     g_week1
    from   dim_calendar
    where  calendar_date = g_date + 1;

    g_end_date := g_start_date + 20;

    select fin_year_no, fin_week_no
    into   g_year2,     g_week2
    from   dim_calendar
    where  calendar_date = g_start_date + 7;

    select fin_year_no, fin_week_no
    into   g_year3,     g_week3
    from   dim_calendar
    where  calendar_date = g_end_date;

    --g_year1 := 2014;
    --g_year2 := 2014;
    --g_year3 := 2014;
    --g_week1 := 35;
    --g_week2 := 36;
    --g_week3 := 37;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Periods processed - '||g_year1||g_week1||'-'||g_year2||g_week2||'-'||g_year3||g_week3||'-'||g_start_date||'-'||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Delete from RTL all records for the period being refreshed.
--**************************************************************************************************
    l_text := 'Deleting OM data started for- '||g_start_date||'-'||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --delete_rtl_om;
    l_text := 'Deleting OM data ended  for- '||g_start_date||'-'||g_end_date||' recs='||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    g_jda_start_date := g_date + 1;
    l_text := 'Deleting JDA data started for- '||g_jda_start_date||'-'||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    delete_rtl_jda;
    l_text := 'Deleting JDA data ended  for- '||g_jda_start_date||'-'||g_end_date||' recs='||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    select min(calendar_date) into g_stg_date from stg_jdaff_po_plan_cpy;
    l_text := 'Calendar_date on Staging - '||g_stg_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'g_year1 = ' || g_year1 || ' : g_week1 = ' || g_week1;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'g_year2 = ' || g_year2 || ' : g_week2 = ' || g_week2;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'g_year3 = ' || g_year3 || ' : g_week3 = ' || g_week3;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_zone_item_supp_po_plan;
    fetch c_zone_item_supp_po_plan bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
       for i in 1 .. a_stg_input.count
       loop
          g_recs_read := g_recs_read + 1;
          if g_recs_read mod 10000 = 0 then
             l_text := dwh_constants.vc_log_records_processed||
             to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          end if;

          g_rec_in                := a_stg_input(i);

          local_address_variables;
          local_write_output;

       end loop;
       fetch c_zone_item_supp_po_plan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_zone_item_supp_po_plan;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
    commit;
    
    l_text := 'Done with inserts, committed, now checking for PO data substitution';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    get_po_data;
    
    l_text := 'Records substituted with PO data - '||g_po_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,g_recs_deleted,'');
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

end wh_prf_corp_760u_po;
