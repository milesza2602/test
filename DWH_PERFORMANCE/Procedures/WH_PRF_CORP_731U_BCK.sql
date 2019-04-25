--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_731U_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_731U_BCK" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLANNING data to JDAFF fact table in the performance layer
--               with input ex JDAFF fnd_loc_item_om_wh_plan table from foundation layer.
--
--  Tables:      Input  - fnd_loc_item_om_wh_plan
--               Output - dwh_performance.rtl_loc_item_dc_wh_plan
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
--g_cases              rtl_loc_item_dc_wh_plan.dc_plan_store_cases%type;
g_rec_out            rtl_loc_item_dc_wh_plan%rowtype;
g_found              boolean;
g_date               date;
g_om_date            date;
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_731U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH DC PLAN FACT DATA FROM OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_m is table of rtl_loc_item_dc_wh_plan%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dc_wh_plan%rowtype index by binary_integer;
a_tbl_merge        tbl_array_m;
a_empty_set_m      tbl_array_m;



a_count             integer       := 0;
a_count_m           integer       := 0;

cursor c_jdaff_wh_plan is
   select   dl.sk1_location_no, di.sk1_item_no,
            week_1_day_1_cases,
            week_1_day_2_cases,
            week_1_day_3_cases,
            week_1_day_4_cases,
            week_1_day_5_cases,
            week_1_day_6_cases,
            week_1_day_7_cases,
            week_2_day_1_cases,
            week_2_day_2_cases,
            week_2_day_3_cases,
            week_2_day_4_cases,
            week_2_day_5_cases,
            week_2_day_6_cases,
            week_2_day_7_cases,
            week_3_day_1_cases,
            week_3_day_2_cases,
            week_3_day_3_cases,
            week_3_day_4_cases,
            week_3_day_5_cases,
            week_3_day_6_cases,
            week_3_day_7_cases

   from     dwh_foundation.fnd_loc_item_om_wh_plan jdaff,
            dim_location dl,
            dim_item di,
            fnd_jdaff_dept_rollout jda

   where jdaff.location_no       = dl.location_no
     and jdaff.item_no           = di.item_no
     and jda.department_no       = di.department_no
     and jda.department_live_ind = 'N'
  order by  dl.sk1_location_no, di.sk1_item_no;


-- For input bulk collect --
type stg_array is table of c_jdaff_wh_plan%rowtype;
a_stg_input          stg_array;
g_rec_in             c_jdaff_wh_plan%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_location_no        := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no            := g_rec_in.sk1_item_no;
   g_rec_out.calendar_date          := g_om_date;   --g_start_date;
   g_rec_out.week_1_day_1_cases     := g_rec_in.week_1_day_1_cases;
   g_rec_out.week_1_day_2_cases     := g_rec_in.week_1_day_2_cases;
   g_rec_out.week_1_day_3_cases     := g_rec_in.week_1_day_3_cases;
   g_rec_out.week_1_day_4_cases     := g_rec_in.week_1_day_4_cases;
   g_rec_out.week_1_day_5_cases     := g_rec_in.week_1_day_5_cases;
   g_rec_out.week_1_day_6_cases     := g_rec_in.week_1_day_6_cases;
   g_rec_out.week_1_day_7_cases     := g_rec_in.week_1_day_7_cases;
   g_rec_out.week_2_day_1_cases     := g_rec_in.week_2_day_1_cases;
   g_rec_out.week_2_day_2_cases     := g_rec_in.week_2_day_2_cases;
   g_rec_out.week_2_day_3_cases     := g_rec_in.week_2_day_3_cases;
   g_rec_out.week_2_day_4_cases     := g_rec_in.week_2_day_4_cases;
   g_rec_out.week_2_day_5_cases     := g_rec_in.week_2_day_5_cases;
   g_rec_out.week_2_day_6_cases     := g_rec_in.week_2_day_6_cases;
   g_rec_out.week_2_day_7_cases     := g_rec_in.week_2_day_7_cases;
   g_rec_out.week_3_day_1_cases     := g_rec_in.week_3_day_1_cases;
   g_rec_out.week_3_day_2_cases     := g_rec_in.week_3_day_2_cases;
   g_rec_out.week_3_day_3_cases     := g_rec_in.week_3_day_3_cases;
   g_rec_out.week_3_day_4_cases     := g_rec_in.week_3_day_4_cases;
   g_rec_out.week_3_day_5_cases     := g_rec_in.week_3_day_5_cases;
   g_rec_out.week_3_day_6_cases     := g_rec_in.week_3_day_6_cases;
   g_rec_out.week_3_day_7_cases     := g_rec_in.week_3_day_7_cases;
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
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_merge as
begin
    forall i in a_tbl_merge.first .. a_tbl_merge.last
       save exceptions

merge into rtl_loc_item_dc_wh_plan rtl_dcwhp USING
(select a_tbl_merge(i).sk1_location_no          as	sk1_location_no,
        a_tbl_merge(i).sk1_item_no              as	sk1_item_no,
        a_tbl_merge(i).calendar_date            as	calendar_date,
        a_tbl_merge(i).week_1_day_1_cases       as	week_1_day_1_cases,
        a_tbl_merge(i).week_1_day_2_cases       as	week_1_day_2_cases,
        a_tbl_merge(i).week_1_day_3_cases       as	week_1_day_3_cases,
        a_tbl_merge(i).week_1_day_4_cases       as	week_1_day_4_cases,
        a_tbl_merge(i).week_1_day_5_cases       as	week_1_day_5_cases,
        a_tbl_merge(i).week_1_day_6_cases       as	week_1_day_6_cases,
        a_tbl_merge(i).week_1_day_7_cases       as	week_1_day_7_cases,
        a_tbl_merge(i).week_2_day_1_cases       as	week_2_day_1_cases,
        a_tbl_merge(i).week_2_day_2_cases       as	week_2_day_2_cases,
        a_tbl_merge(i).week_2_day_3_cases       as	week_2_day_3_cases,
        a_tbl_merge(i).week_2_day_4_cases       as	week_2_day_4_cases,
        a_tbl_merge(i).week_2_day_5_cases       as	week_2_day_5_cases,
        a_tbl_merge(i).week_2_day_6_cases       as	week_2_day_6_cases,
        a_tbl_merge(i).week_2_day_7_cases       as	week_2_day_7_cases,
        a_tbl_merge(i).week_3_day_1_cases       as	week_3_day_1_cases,
        a_tbl_merge(i).week_3_day_2_cases       as	week_3_day_2_cases,
        a_tbl_merge(i).week_3_day_3_cases       as	week_3_day_3_cases,
        a_tbl_merge(i).week_3_day_4_cases       as	week_3_day_4_cases,
        a_tbl_merge(i).week_3_day_5_cases       as	week_3_day_5_cases,
        a_tbl_merge(i).week_3_day_6_cases       as	week_3_day_6_cases,
        a_tbl_merge(i).week_3_day_7_cases       as	week_3_day_7_cases,
        a_tbl_merge(i).last_updated_date        as	last_updated_date
from dual) mer_dcwhp
on  (rtl_dcwhp.sk1_location_no = mer_dcwhp.sk1_location_no
and rtl_dcwhp.sk1_item_no = mer_dcwhp.sk1_item_no
and rtl_dcwhp.calendar_date = mer_dcwhp.calendar_date)
when matched then
update
set
       week_1_day_1_cases              = mer_dcwhp.week_1_day_1_cases,
       week_1_day_2_cases              = mer_dcwhp.week_1_day_2_cases,
       week_1_day_3_cases              = mer_dcwhp.week_1_day_3_cases,
       week_1_day_4_cases              = mer_dcwhp.week_1_day_4_cases,
       week_1_day_5_cases              = mer_dcwhp.week_1_day_5_cases,
       week_1_day_6_cases              = mer_dcwhp.week_1_day_6_cases,
       week_1_day_7_cases              = mer_dcwhp.week_1_day_7_cases,
       week_2_day_1_cases              = mer_dcwhp.week_2_day_1_cases,
       week_2_day_2_cases              = mer_dcwhp.week_2_day_2_cases,
       week_2_day_3_cases              = mer_dcwhp.week_2_day_3_cases,
       week_2_day_4_cases              = mer_dcwhp.week_2_day_4_cases,
       week_2_day_5_cases              = mer_dcwhp.week_2_day_5_cases,
       week_2_day_6_cases              = mer_dcwhp.week_2_day_6_cases,
       week_2_day_7_cases              = mer_dcwhp.week_2_day_7_cases,
       week_3_day_1_cases              = mer_dcwhp.week_3_day_1_cases,
       week_3_day_2_cases              = mer_dcwhp.week_3_day_2_cases,
       week_3_day_3_cases              = mer_dcwhp.week_3_day_3_cases,
       week_3_day_4_cases              = mer_dcwhp.week_3_day_4_cases,
       week_3_day_5_cases              = mer_dcwhp.week_3_day_5_cases,
       week_3_day_6_cases              = mer_dcwhp.week_3_day_6_cases,
       week_3_day_7_cases              = mer_dcwhp.week_3_day_7_cases,
       last_updated_date               = mer_dcwhp.last_updated_date
when not matched then
insert
(      rtl_dcwhp.sk1_location_no,
       rtl_dcwhp.sk1_item_no,
       rtl_dcwhp.calendar_date,
       rtl_dcwhp.week_1_day_1_cases,
       rtl_dcwhp.week_1_day_2_cases,
       rtl_dcwhp.week_1_day_3_cases,
       rtl_dcwhp.week_1_day_4_cases,
       rtl_dcwhp.week_1_day_5_cases,
       rtl_dcwhp.week_1_day_6_cases,
       rtl_dcwhp.week_1_day_7_cases,
       rtl_dcwhp.week_2_day_1_cases,
       rtl_dcwhp.week_2_day_2_cases,
       rtl_dcwhp.week_2_day_3_cases,
       rtl_dcwhp.week_2_day_4_cases,
       rtl_dcwhp.week_2_day_5_cases,
       rtl_dcwhp.week_2_day_6_cases,
       rtl_dcwhp.week_2_day_7_cases,
       rtl_dcwhp.week_3_day_1_cases,
       rtl_dcwhp.week_3_day_2_cases,
       rtl_dcwhp.week_3_day_3_cases,
       rtl_dcwhp.week_3_day_4_cases,
       rtl_dcwhp.week_3_day_5_cases,
       rtl_dcwhp.week_3_day_6_cases,
       rtl_dcwhp.week_3_day_7_cases,
       rtl_dcwhp.last_updated_date
)
values
(      mer_dcwhp.sk1_location_no,
       mer_dcwhp.sk1_item_no,
       mer_dcwhp.calendar_date,
       mer_dcwhp.week_1_day_1_cases,
       mer_dcwhp.week_1_day_2_cases,
       mer_dcwhp.week_1_day_3_cases,
       mer_dcwhp.week_1_day_4_cases,
       mer_dcwhp.week_1_day_5_cases,
       mer_dcwhp.week_1_day_6_cases,
       mer_dcwhp.week_1_day_7_cases,
       mer_dcwhp.week_2_day_1_cases,
       mer_dcwhp.week_2_day_2_cases,
       mer_dcwhp.week_2_day_3_cases,
       mer_dcwhp.week_2_day_4_cases,
       mer_dcwhp.week_2_day_5_cases,
       mer_dcwhp.week_2_day_6_cases,
       mer_dcwhp.week_2_day_7_cases,
       mer_dcwhp.week_3_day_1_cases,
       mer_dcwhp.week_3_day_2_cases,
       mer_dcwhp.week_3_day_3_cases,
       mer_dcwhp.week_3_day_4_cases,
       mer_dcwhp.week_3_day_5_cases,
       mer_dcwhp.week_3_day_6_cases,
       mer_dcwhp.week_3_day_7_cases,
       mer_dcwhp.last_updated_date
);

    g_recs_inserted := g_recs_inserted + a_tbl_merge.count;

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
                       ' '||a_tbl_merge(g_error_index).sk1_location_no||
                       ' '||a_tbl_merge(g_error_index).sk1_item_no||
                       ' '||a_tbl_merge(g_error_index).calendar_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_merge;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin

   a_count_m               := a_count_m + 1;
   a_tbl_merge(a_count_m) := g_rec_out;
   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_merge;
      a_tbl_merge  := a_empty_set_m;
      a_count_m     := 0;
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
    l_text := 'LOAD OF rtl_loc_item_dc_wh_plan EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    g_date := g_date + 1;
  --  g_date := '07/APR/14';
    g_om_date := g_date + 1;    --OM CALENDAR DATE MUST TIE UP WITH CALENDAR DATE THAT JDA WILL LOAD WHICH IS 1 DAY AHEAD

    select this_week_start_date, fin_year_no, fin_week_no, fin_day_no
    into   g_start_date,         g_year1,     g_week1,     g_today_day
    from   dim_calendar
    where  calendar_date = g_date;

    g_end_date := g_start_date + 20;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'OM DATE BEING LOADED - '||g_om_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DATA PERIOD - '||g_start_date||' to '|| g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'CURRENT CALENDAR DATE USED FOR RECORDS INSERTED / UPDATED - '||g_start_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    --g_date := 'Moo';

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
    local_bulk_merge;


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

end wh_prf_corp_731U_bck;
