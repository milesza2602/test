--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_613U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_613U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2013
--  Author:      Quentin Smit
--  Purpose:     Create Store Order specific shipments table from existing shipments table
--
--  Tables:      Input  - fnd_rtl_loc_item_wk_rdf_dyfcst
--               Output - dwh_performance.rtl_loc_item_dy_so_fcst
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sep 2016 - A Joshua Chg-202 -- Remove table fnd_jdaff_dept_rollout from selection criteria
--
--  March 2018  - Q. Smit  Cater for the VAT change from 14% to 15% by joining to the item vat rate table
--                         using the 
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_weigh_ind          number        :=  0;
g_rec_out            dwh_performance.rtl_loc_item_dy_so_fcst%rowtype;

g_found              boolean;
g_valid              boolean;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_613U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD STORE-ORDER SPECIFIC SHIPMENT DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
l_today_date         date := trunc(sysdate) + 1;

l_min_date            date;   --QS
l_max_date            date;   --QS


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_so_fcst%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_so_fcst%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_rtl_loc_item_wk_rdf_dyfcst is
 with loc_item as (
  select /*+ PARALLEL(f,4) */
         dl.location_no,
         di.item_no,
         f.reg_rsp
  from fnd_location_item f, dim_location dl, dim_item di
 where f.location_no = dl.location_no
   and f.item_no = di.item_no
   and di.business_unit_no = 50
),

rdf_fcst as (
 select /*+ full(f) PARALLEL(f,4) */
        dl.sk1_location_no,
        di.sk1_item_no,
        l_today_date as post_date,
        sum(f.DY_01_SYS_FCST_QTY * ((fli.reg_rsp * 100 / (100 + nvl(fi_vr.vat_rate_perc, di.vat_rate_perc))))) as day1_estimate,
        sum(f.DY_02_SYS_FCST_QTY * ((fli.reg_rsp * 100 / (100 + nvl(fi_vr.vat_rate_perc, di.vat_rate_perc))))) as day2_estimate,
        sum(f.DY_03_SYS_FCST_QTY * ((fli.reg_rsp * 100 / (100 + nvl(fi_vr.vat_rate_perc, di.vat_rate_perc))))) as day3_estimate,
        sum(f.DY_01_APP_FCST_QTY * ((fli.reg_rsp * 100 / (100 + nvl(fi_vr.vat_rate_perc, di.vat_rate_perc))))) as weekly_estimate1, -- to DAY7
        sum(f.DY_08_APP_FCST_QTY * ((fli.reg_rsp * 100 / (100 + nvl(fi_vr.vat_rate_perc, di.vat_rate_perc))))) as weekly_estimate2, -- to DAY 14
        sum(f.DY_04_SYS_FCST_QTY * ((fli.reg_rsp * 100 / (100 + nvl(fi_vr.vat_rate_perc, di.vat_rate_perc))))) as day4_estimate,
        sum(f.DY_05_SYS_FCST_QTY * ((fli.reg_rsp * 100 / (100 + nvl(fi_vr.vat_rate_perc, di.vat_rate_perc))))) as day5_estimate,
        sum(f.DY_06_SYS_FCST_QTY * ((fli.reg_rsp * 100 / (100 + nvl(fi_vr.vat_rate_perc, di.vat_rate_perc))))) as day6_estimate,
        sum(f.DY_07_SYS_FCST_QTY * ((fli.reg_rsp * 100 / (100 + nvl(fi_vr.vat_rate_perc, di.vat_rate_perc))))) as day7_estimate,
        sum(round(f.DY_01_APP_FCST_QTY)) as day1_est_unit2,
        sum(round(f.DY_02_APP_FCST_QTY)) as day2_est_unit2,
        sum(round(f.DY_03_APP_FCST_QTY)) as day3_est_unit2,
        sum(round(f.DY_04_APP_FCST_QTY)) as day4_est_unit2,
        sum(round(f.DY_05_APP_FCST_QTY)) as day5_est_unit2,
        sum(round(f.DY_06_APP_FCST_QTY)) as day6_est_unit2,
        sum(round(f.DY_07_APP_FCST_QTY)) as day7_est_unit2
        
   from fnd_loc_item_rdf_dyfcst_l2 f
   join dim_item di     on f.item_no      = di.item_no
   join dim_location dl on f.location_no  = dl.location_no
   join loc_item fli    on f.item_no      = fli.item_no
                       and f.location_no  = fli.location_no
                       
   LEFT OUTER JOIN FND_ITEM_VAT_RATE  fi_vr  on (f.item_no      = fi_vr.item_no                                      -- VAT rate change
                                         and  dl.vat_region_no  = fi_vr.vat_region_no                                -- VAT rate change
                                         and  l_today_date between fi_vr.active_from_date and fi_vr.active_to_date)  -- VAT rate change
 
 --where f.item_no     = di.item_no
 --   and f.location_no = dl.location_no
     --and f.item_no     = fli.item_no
    --and f.location_no = fli.location_no
    
    where f.post_date between l_min_date and l_max_date   --QS

  group by dl.sk1_location_no, di.sk1_item_no, l_today_date
  order by dl.sk1_location_no, di.sk1_item_no, post_date
  )
select * from rdf_fcst;

g_rec_in             c_rtl_loc_item_wk_rdf_dyfcst%rowtype;

-- For input bulk collect --
type fnd_array is table of c_rtl_loc_item_wk_rdf_dyfcst%rowtype;
a_fnd_input      fnd_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
v_count              number        :=  0;
begin

   g_rec_out.sk1_location_no        := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no            := g_rec_in.sk1_item_no;
   g_rec_out.post_date              := g_rec_in.post_date;

   g_rec_out.day1_estimate          := g_rec_in.day1_estimate;
   g_rec_out.day2_estimate          := g_rec_in.day2_estimate;
   g_rec_out.day3_estimate          := g_rec_in.day3_estimate;
   g_rec_out.weekly_estimate1       := g_rec_in.weekly_estimate1;
   g_rec_out.weekly_estimate2       := g_rec_in.weekly_estimate2;
   g_rec_out.day4_estimate          := g_rec_in.day4_estimate;
   g_rec_out.day5_estimate          := g_rec_in.day5_estimate;
   g_rec_out.day6_estimate          := g_rec_in.day6_estimate;
   g_rec_out.day7_estimate          := g_rec_in.day7_estimate;
   g_rec_out.day1_est_unit2         := g_rec_in.day1_est_unit2;
   g_rec_out.day2_est_unit2         := g_rec_in.day2_est_unit2;
   g_rec_out.day3_est_unit2         := g_rec_in.day3_est_unit2;
   g_rec_out.day4_est_unit2         := g_rec_in.day4_est_unit2;
   g_rec_out.day5_est_unit2         := g_rec_in.day5_est_unit2;
   g_rec_out.day6_est_unit2         := g_rec_in.day6_est_unit2;
   g_rec_out.day7_est_unit2         := g_rec_in.day7_est_unit2;

   g_rec_out.last_updated_date       := g_date;

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
       insert into rtl_loc_item_dy_so_fcst values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).post_date;
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
       update rtl_loc_item_dy_so_fcst
       set    day1_estimate               = a_tbl_update(i).day1_estimate,
              day2_estimate               = a_tbl_update(i).day2_estimate,
              day3_estimate               = a_tbl_update(i).day3_estimate,
              weekly_estimate1            = a_tbl_update(i).weekly_estimate1,
              weekly_estimate2            = a_tbl_update(i).weekly_estimate2,
              day4_estimate               = a_tbl_update(i).day4_estimate,
              day5_estimate               = a_tbl_update(i).day5_estimate,
              day6_estimate               = a_tbl_update(i).day6_estimate,
              day7_estimate               = a_tbl_update(i).day7_estimate,
              day1_est_unit2              = a_tbl_update(i).day1_est_unit2,
              day2_est_unit2              = a_tbl_update(i).day2_est_unit2,
              day3_est_unit2              = a_tbl_update(i).day3_est_unit2,
              day4_est_unit2              = a_tbl_update(i).day4_est_unit2,
              day5_est_unit2              = a_tbl_update(i).day5_est_unit2,
              day6_est_unit2              = a_tbl_update(i).day6_est_unit2,
              day7_est_unit2              = a_tbl_update(i).day7_est_unit2,
              last_updated_date           = a_tbl_update(i).last_updated_date
       where  sk1_location_no             = a_tbl_update(i).sk1_location_no
         and  sk1_item_no                 = a_tbl_update(i).sk1_item_no
         and  post_date                   = a_tbl_update(i).post_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
 g_found := TRUE;

 g_found := FALSE;
  select count(1)
   into   g_count
   from   rtl_loc_item_dy_so_fcst
   where  sk1_item_no       = g_rec_out.sk1_item_no
     and  sk1_location_no   = g_rec_out.sk1_location_no
     and  post_date         = g_rec_out.post_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).sk1_location_no = g_rec_out.sk1_location_no and
            a_tbl_insert(i).sk1_item_no     = g_rec_out.sk1_item_no and
            a_tbl_insert(i).post_date       = g_rec_out.post_date then
            g_found := TRUE;
         end if;
      end loop;
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
--   if a_count > 1000 then
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

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF rtl_loc_item_dy_so_fcst EX fnd_rtl_loc_item_wk_rdf_dyfcst STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------

    execute immediate 'alter session enable parallel dml';

----------------------------------------------------------------------------------------------------

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '29/JAN/18';              --QST
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    SELECT /*+ PARALLEL(f,4) */ MIN(POST_DATE), MAX(POST_DATE)    --QS
      into l_min_date, l_max_date                                 --QS
      from fnd_loc_item_rdf_dyfcst_l2 f                           --QS
     where  f.last_updated_date = g_date;                         --QS
     
    --l_today_date := '30/JAN/18';

    l_text := 'DATE RANGE BEING PROCESSED IS:- '||l_min_date ||' to ' || l_max_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'DATE BEING USED AS POST_DATE IS:- '||l_today_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    --l_today_date := 'Moo';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_rtl_loc_item_wk_rdf_dyfcst;
    fetch c_rtl_loc_item_wk_rdf_dyfcst bulk collect into a_fnd_input limit g_forall_limit;
    while a_fnd_input.count > 0
    loop
      for i in 1 .. a_fnd_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_fnd_input(i);
         local_address_variables;
         local_write_output;

      end loop;
    fetch c_rtl_loc_item_wk_rdf_dyfcst bulk collect into a_fnd_input limit g_forall_limit;
    end loop;
    close c_rtl_loc_item_wk_rdf_dyfcst;

--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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
end wh_prf_corp_613U;
