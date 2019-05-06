--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_741T
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_741T" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLANNING data to JDAFF fact table in the performance layer
--               with input ex JDAFF fnd_loc_item_jdaff_wh_plan table from foundation layer.
--
--  Tables:      Input  - fnd_loc_item_jdaff_wh_plan
--               Output - W6005682.RTL_LOC_ITEM_DC_WH_PLANQ
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
--g_cases              W6005682.RTL_LOC_ITEM_DC_WH_PLANQ.dc_plan_store_cases%type;
g_rec_out            W6005682.RTL_LOC_ITEM_DC_WH_PLANQ%rowtype;
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_741T';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PLAN FACT DATA FROM OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of W6005682.RTL_LOC_ITEM_DC_WH_PLANQ%rowtype index by binary_integer;
type tbl_array_u is table of W6005682.RTL_LOC_ITEM_DC_WH_PLANQ%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_cnt               integer;
--l_week_1_day_1_cases  RTL_LOC_ITEM_DC_WH_PLAN.week_1_day_1_cases%type;
--l_week_1_day_2_cases  RTL_LOC_ITEM_DC_WH_PLAN.week_1_day_1_cases%type;
--l_week_1_day_3_cases  RTL_LOC_ITEM_DC_WH_PLAN.week_1_day_1_cases%type;

l_max_fnd_date      date;
l_day_no            integer;

cursor c_jdaff_wh_plan_1 is
   select   dl.sk1_location_no, di.sk1_item_no, dc.calendar_date,
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

   from     dwh_foundation.fnd_loc_item_jdaff_wh_plan jdaff,
            dim_calendar dc,
            dim_location dl,
            dim_item di

   where jdaff.calendar_date  = dc.calendar_date
     and jdaff.location_no    = dl.location_no
     and jdaff.item_no        = di.item_no
     and dc.calendar_date     = g_start_date
;

 -- day 2
cursor c_jdaff_wh_plan_2 is
  select    dl.sk1_location_no, di.sk1_item_no, jdaff.calendar_date,
            nvl(rtl.week_1_day_1_cases,0)  week_1_day_1_cases,
            jdaff.week_1_day_2_cases  week_1_day_2_cases,
            jdaff.week_1_day_3_cases  week_1_day_3_cases,
            jdaff.week_1_day_4_cases  week_1_day_4_cases,
            jdaff.week_1_day_5_cases  week_1_day_5_cases,
            jdaff.week_1_day_6_cases  week_1_day_6_cases,
            jdaff.week_1_day_7_cases  week_1_day_7_cases,
            jdaff.week_2_day_1_cases  week_2_day_1_cases,
            jdaff.week_2_day_2_cases  week_2_day_2_cases,
            jdaff.week_2_day_3_cases  week_2_day_3_cases,
            jdaff.week_2_day_4_cases  week_2_day_4_cases,
            jdaff.week_2_day_5_cases  week_2_day_5_cases,
            jdaff.week_2_day_6_cases  week_2_day_6_cases,
            jdaff.week_2_day_7_cases  week_2_day_7_cases,
            jdaff.week_3_day_1_cases  week_3_day_1_cases,
            jdaff.week_3_day_2_cases  week_3_day_2_cases,
            jdaff.week_3_day_3_cases  week_3_day_3_cases,
            jdaff.week_3_day_4_cases  week_3_day_4_cases,
            jdaff.week_3_day_5_cases  week_3_day_5_cases,
            jdaff.week_3_day_6_cases  week_3_day_6_cases,
            jdaff.week_3_day_7_cases  week_3_day_7_cases

   from     dwh_foundation.fnd_loc_item_jdaff_wh_plan jdaff
   join     dim_location dl               on jdaff.location_no    = dl.location_no
   join     dim_item di                   on jdaff.item_no        = di.item_no
   
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl  on rtl.sk1_location_no  = dl.sk1_location_no
                                          and rtl.sk1_item_no     = di.sk1_item_no
                                          and rtl.calendar_date   = jdaff.calendar_date -1

   where jdaff.calendar_date  = g_start_date
     and jdaff.location_no    = dl.location_no
     and jdaff.item_no        = di.item_no
;

-- day 3
cursor c_jdaff_wh_plan_3 is
  select   /*+ parallel(jdaff,4) parallel(rtl_1,4) parallel(rtl_2,4) */
            dl.sk1_location_no, di.sk1_item_no, jdaff.calendar_date,
            
            nvl(rtl_2.week_1_day_1_cases,0) week_1_day_1_cases,  
            nvl(rtl_1.week_1_day_2_cases,0) week_1_day_2_cases,  
            jdaff.week_1_day_1_cases  week_1_day_3_cases, 
            jdaff.week_1_day_2_cases  week_1_day_4_cases,
            jdaff.week_1_day_3_cases  week_1_day_5_cases,
            jdaff.week_1_day_4_cases  week_1_day_6_cases,
            jdaff.week_1_day_5_cases  week_1_day_7_cases,
            
            jdaff.week_1_day_6_cases  week_2_day_1_cases,
            jdaff.week_1_day_7_cases  week_2_day_2_cases,
            jdaff.week_2_day_1_cases  week_2_day_3_cases,
            jdaff.week_2_day_2_cases  week_2_day_4_cases,
            jdaff.week_2_day_3_cases  week_2_day_5_cases,
            jdaff.week_2_day_4_cases  week_2_day_6_cases,
            jdaff.week_2_day_5_cases  week_2_day_7_cases,
            
            jdaff.week_2_day_6_cases  week_3_day_1_cases,
            jdaff.week_2_day_7_cases  week_3_day_2_cases,
            jdaff.week_3_day_1_cases  week_3_day_3_cases,
            jdaff.week_3_day_2_cases  week_3_day_4_cases,
            jdaff.week_3_day_3_cases  week_3_day_5_cases,
            jdaff.week_3_day_4_cases  week_3_day_6_cases,
            jdaff.week_3_day_5_cases  week_3_day_7_cases

   from     dwh_foundation.fnd_loc_item_jdaff_wh_plan jdaff
   join     dim_location dl               on jdaff.location_no    = dl.location_no
   join     dim_item di                   on jdaff.item_no        = di.item_no
   
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_1 on rtl_1.sk1_location_no  = dl.sk1_location_no
                                          and rtl_1.sk1_item_no     = di.sk1_item_no
                                          and rtl_1.calendar_date   = jdaff.calendar_date -1
                                          
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_2 on rtl_2.sk1_location_no  = dl.sk1_location_no
                                          and rtl_2.sk1_item_no     = di.sk1_item_no
                                          and rtl_2.calendar_date   = jdaff.calendar_date -2
 where jdaff.calendar_date = g_start_date
   and jdaff.location_no    = dl.location_no
   and jdaff.item_no        = di.item_no;

--day 4
cursor c_jdaff_wh_plan_4 is
  select   /*+ parallel(jdaff,4) parallel(rtl_1,4) parallel(rtl_2,4) */
            dl.sk1_location_no, di.sk1_item_no, jdaff.calendar_date,
            nvl(rtl_3.week_1_day_1_cases,0) week_1_day_1_cases,
            nvl(rtl_2.week_1_day_2_cases,0) week_1_day_2_cases,
            nvl(rtl_1.week_1_day_3_cases,0) week_1_day_3_cases,
            
            jdaff.week_1_day_1_cases week_1_day_4_cases,
            jdaff.week_1_day_2_cases week_1_day_5_cases,
            jdaff.week_1_day_3_cases week_1_day_6_cases,
            jdaff.week_1_day_4_cases week_1_day_7_cases,
            
            jdaff.week_1_day_5_cases week_2_day_1_cases,
            jdaff.week_1_day_6_cases week_2_day_2_cases,
            jdaff.week_1_day_7_cases week_2_day_3_cases,
            jdaff.week_2_day_1_cases week_2_day_4_cases,
            jdaff.week_2_day_2_cases week_2_day_5_cases,
            jdaff.week_2_day_3_cases week_2_day_6_cases,
            jdaff.week_2_day_4_cases week_2_day_7_cases,
            
            jdaff.week_2_day_4_cases week_3_day_1_cases,
            jdaff.week_2_day_5_cases week_3_day_2_cases,
            jdaff.week_2_day_7_cases week_3_day_3_cases,
            jdaff.week_3_day_1_cases week_3_day_4_cases,
            jdaff.week_3_day_2_cases week_3_day_5_cases,
            jdaff.week_3_day_3_cases week_3_day_6_cases,
            jdaff.week_3_day_4_cases week_3_day_7_cases

   from     dwh_foundation.fnd_loc_item_jdaff_wh_plan jdaff
   join     dim_location dl               on jdaff.location_no    = dl.location_no
   join     dim_item di                   on jdaff.item_no        = di.item_no
   
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_1  on rtl_1.sk1_location_no  = dl.sk1_location_no
                                           and rtl_1.sk1_item_no     = di.sk1_item_no
                                           and rtl_1.calendar_date   = jdaff.calendar_date -1
                                          
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_2 on rtl_2.sk1_location_no  = dl.sk1_location_no
                                          and rtl_2.sk1_item_no     = di.sk1_item_no
                                          and rtl_2.calendar_date   = jdaff.calendar_date -2
           
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_3 on rtl_3.sk1_location_no  = dl.sk1_location_no
                                          and rtl_3.sk1_item_no     = di.sk1_item_no
                                          and rtl_3.calendar_date   = jdaff.calendar_date -3

   where jdaff.calendar_date = g_start_date
   and jdaff.location_no    = dl.location_no
   and jdaff.item_no        = di.item_no
   ;

-- For input bulk collect --
type stg_array is table of c_jdaff_wh_plan_1%rowtype;
a_stg_input          stg_array;
g_rec_in             c_jdaff_wh_plan_1%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_location_no                  := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                      := g_rec_in.sk1_item_no;
   g_rec_out.calendar_date                    := g_rec_in.calendar_date;
     
   g_rec_out.week_1_day_1_cases               := g_rec_in.week_1_day_1_cases;
   g_rec_out.week_1_day_2_cases               := g_rec_in.week_1_day_2_cases;
   g_rec_out.week_1_day_3_cases               := g_rec_in.week_1_day_3_cases;
   g_rec_out.week_1_day_4_cases               := g_rec_in.week_1_day_4_cases;
   g_rec_out.week_1_day_5_cases               := g_rec_in.week_1_day_5_cases;
   g_rec_out.week_1_day_6_cases               := g_rec_in.week_1_day_6_cases;
   g_rec_out.week_1_day_7_cases               := g_rec_in.week_1_day_7_cases;
   g_rec_out.week_2_day_1_cases               := g_rec_in.week_2_day_1_cases;
   g_rec_out.week_2_day_2_cases               := g_rec_in.week_2_day_2_cases;
   g_rec_out.week_2_day_3_cases               := g_rec_in.week_2_day_3_cases;
   g_rec_out.week_2_day_4_cases               := g_rec_in.week_2_day_4_cases;
   g_rec_out.week_2_day_5_cases               := g_rec_in.week_2_day_5_cases;
   g_rec_out.week_2_day_6_cases               := g_rec_in.week_2_day_6_cases;
   g_rec_out.week_2_day_7_cases               := g_rec_in.week_2_day_7_cases;
   g_rec_out.week_3_day_7_cases               := g_rec_in.week_2_day_7_cases;
   g_rec_out.week_3_day_1_cases               := g_rec_in.week_3_day_1_cases;
   g_rec_out.week_3_day_2_cases               := g_rec_in.week_3_day_2_cases;
   g_rec_out.week_3_day_3_cases               := g_rec_in.week_3_day_3_cases;
   g_rec_out.week_3_day_4_cases               := g_rec_in.week_3_day_4_cases;
   g_rec_out.week_3_day_5_cases               := g_rec_in.week_3_day_5_cases;
   g_rec_out.week_3_day_6_cases               := g_rec_in.week_3_day_6_cases;
   g_rec_out.week_3_day_7_cases               := g_rec_in.week_3_day_7_cases;
   g_rec_out.last_updated_date                := g_date;

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
       insert into W6005682.RTL_LOC_ITEM_DC_WH_PLANQ values a_tbl_insert(i);

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
       update W6005682.RTL_LOC_ITEM_DC_WH_PLANQ
       set    row                    =  a_tbl_update(i)
       where  calendar_date          =  a_tbl_update(i).calendar_date
       and    sk1_location_no        =  a_tbl_update(i).sk1_location_no
       and    sk1_item_no            =  a_tbl_update(i).sk1_item_no;

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
   select count(1)  --, sum(dc_plan_store_cases)
   into   g_count  --,  g_cases
   from   W6005682.RTL_LOC_ITEM_DC_WH_PLANQ
   where  calendar_date    = g_rec_out.calendar_date
   and    sk1_location_no  = g_rec_out.sk1_location_no
   and    sk1_item_no      = g_rec_out.sk1_item_no;

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
    l_text := 'LOAD OF W6005682.RTL_LOC_ITEM_DC_WH_PLANQ EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    --g_date := '03/DEC/13';

    select this_week_start_date, fin_year_no, fin_week_no, fin_day_no
    into   g_start_date,         g_year1,     g_week1,     g_today_day
    from   dim_calendar
    where  calendar_date = g_date;

   select /*+ parallel(fnd,4) */ max(fnd.calendar_date) 
     into l_max_fnd_date
     from fnd_loc_item_jdaff_wh_plan fnd
    where fnd.last_updated_date = g_date;
    
    select fin_day_no 
     into l_day_no
     from dim_calendar
    where calendar_date = l_max_fnd_date;
    
    g_start_date := l_max_fnd_date;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DATA PERIOD - '||g_start_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'FIN DAY BENIG PROCESSED - '||l_day_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    dbms_output.put_line(g_year1||g_week1||g_year2||g_week2||g_year3||g_week3||g_start_date||g_end_date);

--l_max_fnd_date := 'Moo';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    if l_day_no = 1 then
      open c_jdaff_wh_plan_1;
      fetch c_jdaff_wh_plan_1 bulk collect into a_stg_input limit g_forall_limit;
      while a_stg_input.count > 0
      loop
         for i in 1 .. a_stg_input.count
         loop
            g_recs_read := g_recs_read + 1;
            if g_recs_read mod 100000 = 0 then
               l_text := dwh_constants.vc_log_records_processed||
               to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            end if;
  
            g_rec_in                := a_stg_input(i);
  
            local_address_variables;
            local_write_output;
  
         end loop;
         fetch c_jdaff_wh_plan_1 bulk collect into a_stg_input limit g_forall_limit;
      end loop;
      close c_jdaff_wh_plan_1;
  end if;
  
  
  if l_day_no = 2 then
    open c_jdaff_wh_plan_2;
    fetch c_jdaff_wh_plan_2 bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
       for i in 1 .. a_stg_input.count
       loop
          g_recs_read := g_recs_read + 1;
          if g_recs_read mod 100000 = 0 then
             l_text := dwh_constants.vc_log_records_processed||
             to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          end if;

          g_rec_in                := a_stg_input(i);

          local_address_variables;
          local_write_output;

       end loop;
       fetch c_jdaff_wh_plan_2 bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_jdaff_wh_plan_2;
  end if;
  
  
  if l_day_no = 3 then
    open c_jdaff_wh_plan_3;
    fetch c_jdaff_wh_plan_3 bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
       for i in 1 .. a_stg_input.count
       loop
          g_recs_read := g_recs_read + 1;
          if g_recs_read mod 100000 = 0 then
             l_text := dwh_constants.vc_log_records_processed||
             to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          end if;

          g_rec_in                := a_stg_input(i);

          local_address_variables;
          local_write_output;

       end loop;
       fetch c_jdaff_wh_plan_3 bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_jdaff_wh_plan_3;
  end if;
  
  if l_day_no = 4 then
    open c_jdaff_wh_plan_4;
    fetch c_jdaff_wh_plan_4 bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
       for i in 1 .. a_stg_input.count
       loop
          g_recs_read := g_recs_read + 1;
          if g_recs_read mod 100000 = 0 then
             l_text := dwh_constants.vc_log_records_processed||
             to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          end if;

          g_rec_in                := a_stg_input(i);

          local_address_variables;
          local_write_output;

       end loop;
       fetch c_jdaff_wh_plan_4 bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_jdaff_wh_plan_4;
  end if;
  
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

end wh_prf_corp_741t;
