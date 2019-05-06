--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_741U_MERGE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_741U_MERGE" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLANNING data to JDAFF fact table in the performance layer
--               with input ex JDAFF W6005682.RTL_LOC_ITEM_DC_WH_PLANQ table from foundation layer.
--
--  Tables:      Input  - DWH_FOUNDATION.FND_LOC_ITEM_JDAFF_WH_PLAN
--                        W6005682.RTL_LOC_ITEM_DC_WH_PLANQ
--               Output - W6005682.RTL_LOC_ITEM_DC_WH_PLANQ
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  16 May 2016 - Added substitution based on day of week relating to the calendar date.  
--                From day 2 onwards the previous day(s) of the week is substituted using
--                rules given by JDA.
--
--  August 2016 - Changed logic to use a merge but with outer joins to do the substituions 
--                that were added on 16 May 2016
--                A different version of the merge will be run based on what day of the week 
--                is being processed.
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_741M';
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
--l_week_1_day_1_cases  W6005682.RTL_LOC_ITEM_DC_WH_PLANQ.week_1_day_1_cases%type;
--l_week_1_day_2_cases  W6005682.RTL_LOC_ITEM_DC_WH_PLANQ.week_1_day_1_cases%type;
--l_week_1_day_3_cases  W6005682.RTL_LOC_ITEM_DC_WH_PLANQ.week_1_day_1_cases%type;

l_max_fnd_date      date;
l_day_no            integer;

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
    
    --g_date := g_date - 1;

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
    
    l_text := '** Merge starting ** ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session enable parallel dml';

--    dbms_output.put_line(g_year1||g_week1||g_year2||g_week2||g_year3||g_week3||g_start_date||g_end_date);

--l_max_fnd_date := 'Moo';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    if l_day_no = 1 then
      MERGE /*+ PARALLEL(di_mart,4) */ INTO W6005682.RTL_LOC_ITEM_DC_WH_PLANQ  di_mart  USING (   
       select /*+ parallel(jdaff,4) full(di) full(dl) */
            dl.sk1_location_no, di.sk1_item_no, dc.calendar_date,
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
     
    ) mer_mart
     
  on (mer_mart.sk1_location_no  = di_mart.sk1_location_no
  and mer_mart.sk1_item_no      = di_mart.sk1_item_no
  and mer_mart.calendar_date    = di_mart.calendar_date)
          
  when matched then 
    update set  WEEK_1_DAY_1_CASES  = mer_mart.week_1_day_1_cases,
                WEEK_1_DAY_2_CASES  = mer_mart.week_1_day_2_cases,
                WEEK_1_DAY_3_CASES  = mer_mart.week_1_day_3_cases,
                WEEK_1_DAY_4_CASES  = mer_mart.week_1_day_4_cases,
                WEEK_1_DAY_5_CASES  = mer_mart.week_1_day_5_cases,
                WEEK_1_DAY_6_CASES  = mer_mart.week_1_day_6_cases,
                WEEK_1_DAY_7_CASES  = mer_mart.week_1_day_7_cases,
                WEEK_2_DAY_1_CASES  = mer_mart.week_2_day_1_cases,
                WEEK_2_DAY_2_CASES  = mer_mart.week_2_day_2_cases,
                WEEK_2_DAY_3_CASES  = mer_mart.week_2_day_3_cases,
                WEEK_2_DAY_4_CASES  = mer_mart.week_2_day_4_cases,
                WEEK_2_DAY_5_CASES  = mer_mart.week_2_day_5_cases,
                WEEK_2_DAY_6_CASES  = mer_mart.week_2_day_6_cases,
                WEEK_2_DAY_7_CASES  = mer_mart.week_2_day_7_cases,
                WEEK_3_DAY_1_CASES  = mer_mart.week_3_day_1_cases,
                WEEK_3_DAY_2_CASES  = mer_mart.week_3_day_2_cases,
                WEEK_3_DAY_3_CASES  = mer_mart.week_3_day_3_cases,
                WEEK_3_DAY_4_CASES  = mer_mart.week_3_day_4_cases,
                WEEK_3_DAY_5_CASES  = mer_mart.week_3_day_5_cases,
                WEEK_3_DAY_6_CASES  = mer_mart.week_3_day_6_cases,
                WEEK_3_DAY_7_CASES  = mer_mart.week_3_day_7_cases,
                last_updated_date   = g_date
                
     when not matched then
       insert 
         (sk1_location_no, 
          sk1_item_no, 
          calendar_date,
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
          week_3_day_7_cases,
          last_updated_date
        )
        
      values
        ( mer_mart.sk1_location_no,
          mer_mart.sk1_item_no,
          mer_mart.calendar_date,
          mer_mart.week_1_day_1_cases,
          mer_mart.week_1_day_2_cases,
          mer_mart.week_1_day_3_cases,
          mer_mart.week_1_day_4_cases,
          mer_mart.week_1_day_5_cases,
          mer_mart.week_1_day_6_cases,
          mer_mart.week_1_day_7_cases,
          mer_mart.week_2_day_1_cases,
          mer_mart.week_2_day_2_cases,
          mer_mart.week_2_day_3_cases,
          mer_mart.week_2_day_4_cases,
          mer_mart.week_2_day_5_cases,
          mer_mart.week_2_day_6_cases,
          mer_mart.week_2_day_7_cases,
          mer_mart.week_3_day_1_cases,
          mer_mart.week_3_day_2_cases,
          mer_mart.week_3_day_3_cases,
          mer_mart.week_3_day_4_cases,
          mer_mart.week_3_day_5_cases,
          mer_mart.week_3_day_6_cases,
          mer_mart.week_3_day_7_cases,
          g_date
          ); 
     
  end if;
  
  
  if l_day_no = 2 then
    ---------------------------------------------------------------------------------------
    -- substitute day 1 value with yesterday's value to be used on the table with historical data
    -- this is done as the values are not passed by JDA but must be shown on the record
    ---------------------------------------------------------------------------------------
      MERGE /*+ PARALLEL(di_mart,4) */ INTO W6005682.RTL_LOC_ITEM_DC_WH_PLANQ  di_mart  USING (   
       select /*+ parallel(jdaff,4) parallel(rtl,4) full(di) full(dl) */
            dl.sk1_location_no, di.sk1_item_no, jdaff.calendar_date,
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
                                         and rtl.sk1_item_no      = di.sk1_item_no
                                         and rtl.calendar_date    = jdaff.calendar_date -1

   where jdaff.calendar_date  = g_start_date
     and jdaff.location_no    = dl.location_no
     and jdaff.item_no        = di.item_no
     
     ) mer_mart
     
  on (mer_mart.sk1_location_no  = di_mart.sk1_location_no
  and mer_mart.sk1_item_no      = di_mart.sk1_item_no
  and mer_mart.calendar_date    = di_mart.calendar_date)
          
  when matched then 
    update set  WEEK_1_DAY_1_CASES  = mer_mart.week_1_day_1_cases,
                WEEK_1_DAY_2_CASES  = mer_mart.week_1_day_2_cases,
                WEEK_1_DAY_3_CASES  = mer_mart.week_1_day_3_cases,
                WEEK_1_DAY_4_CASES  = mer_mart.week_1_day_4_cases,
                WEEK_1_DAY_5_CASES  = mer_mart.week_1_day_5_cases,
                WEEK_1_DAY_6_CASES  = mer_mart.week_1_day_6_cases,
                WEEK_1_DAY_7_CASES  = mer_mart.week_1_day_7_cases,
                WEEK_2_DAY_1_CASES  = mer_mart.week_2_day_1_cases,
                WEEK_2_DAY_2_CASES  = mer_mart.week_2_day_2_cases,
                WEEK_2_DAY_3_CASES  = mer_mart.week_2_day_3_cases,
                WEEK_2_DAY_4_CASES  = mer_mart.week_2_day_4_cases,
                WEEK_2_DAY_5_CASES  = mer_mart.week_2_day_5_cases,
                WEEK_2_DAY_6_CASES  = mer_mart.week_2_day_6_cases,
                WEEK_2_DAY_7_CASES  = mer_mart.week_2_day_7_cases,
                WEEK_3_DAY_1_CASES  = mer_mart.week_3_day_1_cases,
                WEEK_3_DAY_2_CASES  = mer_mart.week_3_day_2_cases,
                WEEK_3_DAY_3_CASES  = mer_mart.week_3_day_3_cases,
                WEEK_3_DAY_4_CASES  = mer_mart.week_3_day_4_cases,
                WEEK_3_DAY_5_CASES  = mer_mart.week_3_day_5_cases,
                WEEK_3_DAY_6_CASES  = mer_mart.week_3_day_6_cases,
                WEEK_3_DAY_7_CASES  = mer_mart.week_3_day_7_cases,
                last_updated_date   = g_date
                
     when not matched then
       insert 
         (sk1_location_no, 
          sk1_item_no, 
          calendar_date,
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
          week_3_day_7_cases,
          last_updated_date
        )
        
      values
        ( mer_mart.sk1_location_no,
          mer_mart.sk1_item_no,
          mer_mart.calendar_date,
          mer_mart.week_1_day_1_cases,
          mer_mart.week_1_day_2_cases,
          mer_mart.week_1_day_3_cases,
          mer_mart.week_1_day_4_cases,
          mer_mart.week_1_day_5_cases,
          mer_mart.week_1_day_6_cases,
          mer_mart.week_1_day_7_cases,
          mer_mart.week_2_day_1_cases,
          mer_mart.week_2_day_2_cases,
          mer_mart.week_2_day_3_cases,
          mer_mart.week_2_day_4_cases,
          mer_mart.week_2_day_5_cases,
          mer_mart.week_2_day_6_cases,
          mer_mart.week_2_day_7_cases,
          mer_mart.week_3_day_1_cases,
          mer_mart.week_3_day_2_cases,
          mer_mart.week_3_day_3_cases,
          mer_mart.week_3_day_4_cases,
          mer_mart.week_3_day_5_cases,
          mer_mart.week_3_day_6_cases,
          mer_mart.week_3_day_7_cases,
          g_date
          );
  end if;
  
  
  if l_day_no = 3 then
    ---------------------------------------------------------------------------------------
    -- sustitute the first 2 day cases values to be used on the table with historical data
    -- this is done as the values are not passed by JDA but must be shown on the record
    ---------------------------------------------------------------------------------------
    MERGE /*+ PARALLEL(di_mart,4) */ INTO W6005682.RTL_LOC_ITEM_DC_WH_PLANQ  di_mart  USING (   
       select /*+ parallel(jdaff,4) parallel(rtl_1,4) parallel(rtl_2,4) full(di) full(dl) */
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
   and jdaff.item_no        = di.item_no
       
       
  ) mer_mart
     
  on (mer_mart.sk1_location_no  = di_mart.sk1_location_no
  and mer_mart.sk1_item_no      = di_mart.sk1_item_no
  and mer_mart.calendar_date    = di_mart.calendar_date)
          
  when matched then 
    update set  WEEK_1_DAY_1_CASES  = mer_mart.week_1_day_1_cases,
                WEEK_1_DAY_2_CASES  = mer_mart.week_1_day_2_cases,
                WEEK_1_DAY_3_CASES  = mer_mart.week_1_day_3_cases,
                WEEK_1_DAY_4_CASES  = mer_mart.week_1_day_4_cases,
                WEEK_1_DAY_5_CASES  = mer_mart.week_1_day_5_cases,
                WEEK_1_DAY_6_CASES  = mer_mart.week_1_day_6_cases,
                WEEK_1_DAY_7_CASES  = mer_mart.week_1_day_7_cases,
                WEEK_2_DAY_1_CASES  = mer_mart.week_2_day_1_cases,
                WEEK_2_DAY_2_CASES  = mer_mart.week_2_day_2_cases,
                WEEK_2_DAY_3_CASES  = mer_mart.week_2_day_3_cases,
                WEEK_2_DAY_4_CASES  = mer_mart.week_2_day_4_cases,
                WEEK_2_DAY_5_CASES  = mer_mart.week_2_day_5_cases,
                WEEK_2_DAY_6_CASES  = mer_mart.week_2_day_6_cases,
                WEEK_2_DAY_7_CASES  = mer_mart.week_2_day_7_cases,
                WEEK_3_DAY_1_CASES  = mer_mart.week_3_day_1_cases,
                WEEK_3_DAY_2_CASES  = mer_mart.week_3_day_2_cases,
                WEEK_3_DAY_3_CASES  = mer_mart.week_3_day_3_cases,
                WEEK_3_DAY_4_CASES  = mer_mart.week_3_day_4_cases,
                WEEK_3_DAY_5_CASES  = mer_mart.week_3_day_5_cases,
                WEEK_3_DAY_6_CASES  = mer_mart.week_3_day_6_cases,
                WEEK_3_DAY_7_CASES  = mer_mart.week_3_day_7_cases,
                last_updated_date   = g_date
                
     when not matched then
       insert 
         (sk1_location_no, 
          sk1_item_no, 
          calendar_date,
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
          week_3_day_7_cases,
          last_updated_date
        )
        
      values
        ( mer_mart.sk1_location_no,
          mer_mart.sk1_item_no,
          mer_mart.calendar_date,
          mer_mart.week_1_day_1_cases,
          mer_mart.week_1_day_2_cases,
          mer_mart.week_1_day_3_cases,
          mer_mart.week_1_day_4_cases,
          mer_mart.week_1_day_5_cases,
          mer_mart.week_1_day_6_cases,
          mer_mart.week_1_day_7_cases,
          mer_mart.week_2_day_1_cases,
          mer_mart.week_2_day_2_cases,
          mer_mart.week_2_day_3_cases,
          mer_mart.week_2_day_4_cases,
          mer_mart.week_2_day_5_cases,
          mer_mart.week_2_day_6_cases,
          mer_mart.week_2_day_7_cases,
          mer_mart.week_3_day_1_cases,
          mer_mart.week_3_day_2_cases,
          mer_mart.week_3_day_3_cases,
          mer_mart.week_3_day_4_cases,
          mer_mart.week_3_day_5_cases,
          mer_mart.week_3_day_6_cases,
          mer_mart.week_3_day_7_cases,
          g_date
          ); 
       
  end if;
  
  if l_day_no = 4 then
    ---------------------------------------------------------------------------------------
    -- sustitute the first 3 days cases values to be used on the table with historical data
    -- this is done as the values are not passed by JDA but must be shown on the record
    ---------------------------------------------------------------------------------------
    MERGE /*+ PARALLEL(di_mart,4) */ INTO W6005682.RTL_LOC_ITEM_DC_WH_PLANQ  di_mart  USING (   
       select   /*+ parallel(jdaff,4) parallel(rtl_1,4) parallel(rtl_2,4) parallel(rtl_3,4) full(di) full(dl) */
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
            
            jdaff.week_2_day_5_cases week_3_day_1_cases,
            jdaff.week_2_day_6_cases week_3_day_2_cases,
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

   where jdaff.calendar_date  = g_start_date
     and jdaff.location_no    = dl.location_no
     and jdaff.item_no        = di.item_no
       
       
 ) mer_mart
     
  on (mer_mart.sk1_location_no  = di_mart.sk1_location_no
  and mer_mart.sk1_item_no      = di_mart.sk1_item_no
  and mer_mart.calendar_date    = di_mart.calendar_date)
          
  when matched then 
    update set  WEEK_1_DAY_1_CASES  = mer_mart.week_1_day_1_cases,
                WEEK_1_DAY_2_CASES  = mer_mart.week_1_day_2_cases,
                WEEK_1_DAY_3_CASES  = mer_mart.week_1_day_3_cases,
                WEEK_1_DAY_4_CASES  = mer_mart.week_1_day_4_cases,
                WEEK_1_DAY_5_CASES  = mer_mart.week_1_day_5_cases,
                WEEK_1_DAY_6_CASES  = mer_mart.week_1_day_6_cases,
                WEEK_1_DAY_7_CASES  = mer_mart.week_1_day_7_cases,
                WEEK_2_DAY_1_CASES  = mer_mart.week_2_day_1_cases,
                WEEK_2_DAY_2_CASES  = mer_mart.week_2_day_2_cases,
                WEEK_2_DAY_3_CASES  = mer_mart.week_2_day_3_cases,
                WEEK_2_DAY_4_CASES  = mer_mart.week_2_day_4_cases,
                WEEK_2_DAY_5_CASES  = mer_mart.week_2_day_5_cases,
                WEEK_2_DAY_6_CASES  = mer_mart.week_2_day_6_cases,
                WEEK_2_DAY_7_CASES  = mer_mart.week_2_day_7_cases,
                WEEK_3_DAY_1_CASES  = mer_mart.week_3_day_1_cases,
                WEEK_3_DAY_2_CASES  = mer_mart.week_3_day_2_cases,
                WEEK_3_DAY_3_CASES  = mer_mart.week_3_day_3_cases,
                WEEK_3_DAY_4_CASES  = mer_mart.week_3_day_4_cases,
                WEEK_3_DAY_5_CASES  = mer_mart.week_3_day_5_cases,
                WEEK_3_DAY_6_CASES  = mer_mart.week_3_day_6_cases,
                WEEK_3_DAY_7_CASES  = mer_mart.week_3_day_7_cases,
                last_updated_date   = g_date
                
     when not matched then
       insert 
         (sk1_location_no, 
          sk1_item_no, 
          calendar_date,
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
          week_3_day_7_cases,
          last_updated_date
        )
        
      values
        ( mer_mart.sk1_location_no,
          mer_mart.sk1_item_no,
          mer_mart.calendar_date,
          mer_mart.week_1_day_1_cases,
          mer_mart.week_1_day_2_cases,
          mer_mart.week_1_day_3_cases,
          mer_mart.week_1_day_4_cases,
          mer_mart.week_1_day_5_cases,
          mer_mart.week_1_day_6_cases,
          mer_mart.week_1_day_7_cases,
          mer_mart.week_2_day_1_cases,
          mer_mart.week_2_day_2_cases,
          mer_mart.week_2_day_3_cases,
          mer_mart.week_2_day_4_cases,
          mer_mart.week_2_day_5_cases,
          mer_mart.week_2_day_6_cases,
          mer_mart.week_2_day_7_cases,
          mer_mart.week_3_day_1_cases,
          mer_mart.week_3_day_2_cases,
          mer_mart.week_3_day_3_cases,
          mer_mart.week_3_day_4_cases,
          mer_mart.week_3_day_5_cases,
          mer_mart.week_3_day_6_cases,
          mer_mart.week_3_day_7_cases,
          g_date
          ); 
  end if;
  
  if l_day_no = 5 then
    ---------------------------------------------------------------------------------------
    -- sustitute the first 4 days cases values to be used on the table with historical data
    -- this is done as the values are not passed by JDA but must be shown on the record
    ---------------------------------------------------------------------------------------
    MERGE /*+ PARALLEL(di_mart,4) */ INTO W6005682.RTL_LOC_ITEM_DC_WH_PLANQ  di_mart  USING (   
     select   /*+ parallel(jdaff,4) parallel(rtl_1,4) parallel(rtl_2,4) parallel(rtl_3,4) parallel(rtl_4,4) full(di) full(dl) */
            dl.sk1_location_no, di.sk1_item_no, jdaff.calendar_date,
            nvl(rtl_3.week_1_day_1_cases,0) week_1_day_1_cases,
            nvl(rtl_2.week_1_day_2_cases,0) week_1_day_2_cases,
            nvl(rtl_1.week_1_day_3_cases,0) week_1_day_3_cases,
            nvl(rtl_4.week_1_day_4_cases,0) week_1_day_4_cases,
            
            jdaff.week_1_day_1_cases week_1_day_5_cases,
            jdaff.week_1_day_2_cases week_1_day_6_cases,
            jdaff.week_1_day_3_cases week_1_day_7_cases,
            
            jdaff.week_1_day_4_cases week_2_day_1_cases,
            jdaff.week_1_day_5_cases week_2_day_2_cases,
            jdaff.week_1_day_6_cases week_2_day_3_cases,
            jdaff.week_1_day_7_cases week_2_day_4_cases,
            jdaff.week_2_day_1_cases week_2_day_5_cases,
            jdaff.week_2_day_2_cases week_2_day_6_cases,
            jdaff.week_2_day_3_cases week_2_day_7_cases,
            
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
   
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_1 on rtl_1.sk1_location_no  = dl.sk1_location_no
                                                    and rtl_1.sk1_item_no     = di.sk1_item_no
                                                    and rtl_1.calendar_date   = jdaff.calendar_date -1
                                          
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_2 on rtl_2.sk1_location_no  = dl.sk1_location_no
                                                    and rtl_2.sk1_item_no     = di.sk1_item_no
                                                    and rtl_2.calendar_date   = jdaff.calendar_date -2
           
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_3 on rtl_3.sk1_location_no  = dl.sk1_location_no
                                                    and rtl_3.sk1_item_no     = di.sk1_item_no
                                                    and rtl_3.calendar_date   = jdaff.calendar_date -3
                                          
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_4 on rtl_4.sk1_location_no  = dl.sk1_location_no
                                                    and rtl_4.sk1_item_no     = di.sk1_item_no
                                                    and rtl_4.calendar_date   = jdaff.calendar_date -4

   where jdaff.calendar_date = g_start_date
   and jdaff.location_no    = dl.location_no
   and jdaff.item_no        = di.item_no
       
       
           ) mer_mart
     
  on (mer_mart.sk1_location_no  = di_mart.sk1_location_no
  and mer_mart.sk1_item_no      = di_mart.sk1_item_no
  and mer_mart.calendar_date    = di_mart.calendar_date)
          
  when matched then 
    update set  WEEK_1_DAY_1_CASES  = mer_mart.week_1_day_1_cases,
                WEEK_1_DAY_2_CASES  = mer_mart.week_1_day_2_cases,
                WEEK_1_DAY_3_CASES  = mer_mart.week_1_day_3_cases,
                WEEK_1_DAY_4_CASES  = mer_mart.week_1_day_4_cases,
                WEEK_1_DAY_5_CASES  = mer_mart.week_1_day_5_cases,
                WEEK_1_DAY_6_CASES  = mer_mart.week_1_day_6_cases,
                WEEK_1_DAY_7_CASES  = mer_mart.week_1_day_7_cases,
                WEEK_2_DAY_1_CASES  = mer_mart.week_2_day_1_cases,
                WEEK_2_DAY_2_CASES  = mer_mart.week_2_day_2_cases,
                WEEK_2_DAY_3_CASES  = mer_mart.week_2_day_3_cases,
                WEEK_2_DAY_4_CASES  = mer_mart.week_2_day_4_cases,
                WEEK_2_DAY_5_CASES  = mer_mart.week_2_day_5_cases,
                WEEK_2_DAY_6_CASES  = mer_mart.week_2_day_6_cases,
                WEEK_2_DAY_7_CASES  = mer_mart.week_2_day_7_cases,
                WEEK_3_DAY_1_CASES  = mer_mart.week_3_day_1_cases,
                WEEK_3_DAY_2_CASES  = mer_mart.week_3_day_2_cases,
                WEEK_3_DAY_3_CASES  = mer_mart.week_3_day_3_cases,
                WEEK_3_DAY_4_CASES  = mer_mart.week_3_day_4_cases,
                WEEK_3_DAY_5_CASES  = mer_mart.week_3_day_5_cases,
                WEEK_3_DAY_6_CASES  = mer_mart.week_3_day_6_cases,
                WEEK_3_DAY_7_CASES  = mer_mart.week_3_day_7_cases,
                last_updated_date   = g_date
                
     when not matched then
       insert 
         (sk1_location_no, 
          sk1_item_no, 
          calendar_date,
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
          week_3_day_7_cases,
          last_updated_date
        )
        
      values
        ( mer_mart.sk1_location_no,
          mer_mart.sk1_item_no,
          mer_mart.calendar_date,
          mer_mart.week_1_day_1_cases,
          mer_mart.week_1_day_2_cases,
          mer_mart.week_1_day_3_cases,
          mer_mart.week_1_day_4_cases,
          mer_mart.week_1_day_5_cases,
          mer_mart.week_1_day_6_cases,
          mer_mart.week_1_day_7_cases,
          mer_mart.week_2_day_1_cases,
          mer_mart.week_2_day_2_cases,
          mer_mart.week_2_day_3_cases,
          mer_mart.week_2_day_4_cases,
          mer_mart.week_2_day_5_cases,
          mer_mart.week_2_day_6_cases,
          mer_mart.week_2_day_7_cases,
          mer_mart.week_3_day_1_cases,
          mer_mart.week_3_day_2_cases,
          mer_mart.week_3_day_3_cases,
          mer_mart.week_3_day_4_cases,
          mer_mart.week_3_day_5_cases,
          mer_mart.week_3_day_6_cases,
          mer_mart.week_3_day_7_cases,
          g_date
          ); 
  end if;
  
  if l_day_no = 6 then
    ---------------------------------------------------------------------------------------
    -- sustitute the first 5 days cases values to be used on the table with historical data
    -- this is done as the values are not passed by JDA but must be shown on the record
    ---------------------------------------------------------------------------------------
    MERGE /*+ PARALLEL(di_mart,4) */ INTO W6005682.RTL_LOC_ITEM_DC_WH_PLANQ  di_mart  USING (   
      select /*+ parallel(jdaff,4) parallel(rtl_1,4) parallel(rtl_2,4) parallel(rtl_3,4) parallel(rtl_4,4) parallel(rtl_5,4) full(di) full(dl) */
            dl.sk1_location_no, di.sk1_item_no, jdaff.calendar_date,
            
            nvl(rtl_3.week_1_day_1_cases,0) week_1_day_1_cases,
            nvl(rtl_2.week_1_day_2_cases,0) week_1_day_2_cases,
            nvl(rtl_1.week_1_day_3_cases,0) week_1_day_3_cases,
            nvl(rtl_4.week_1_day_4_cases,0) week_1_day_4_cases,
            nvl(rtl_5.week_1_day_5_cases,0) week_1_day_5_cases,
            
            jdaff.week_1_day_1_cases week_1_day_6_cases,
            jdaff.week_1_day_2_cases week_1_day_7_cases,
            
            jdaff.week_1_day_3_cases week_2_day_1_cases,
            jdaff.week_1_day_4_cases week_2_day_2_cases,
            jdaff.week_1_day_5_cases week_2_day_3_cases,
            jdaff.week_2_day_6_cases week_2_day_4_cases,
            jdaff.week_2_day_7_cases week_2_day_5_cases,
            jdaff.week_3_day_1_cases week_2_day_6_cases,
            jdaff.week_3_day_2_cases week_2_day_7_cases,
            
            jdaff.week_2_day_3_cases week_3_day_1_cases,
            jdaff.week_2_day_4_cases week_3_day_2_cases,
            jdaff.week_2_day_5_cases week_3_day_3_cases,
            jdaff.week_2_day_6_cases week_3_day_4_cases,
            jdaff.week_2_day_7_cases week_3_day_5_cases,
            jdaff.week_3_day_1_cases week_3_day_6_cases,
            jdaff.week_3_day_2_cases week_3_day_7_cases

   from     dwh_foundation.fnd_loc_item_jdaff_wh_plan jdaff
   join     dim_location dl               on jdaff.location_no    = dl.location_no
   join     dim_item di                   on jdaff.item_no        = di.item_no
   
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_1 on rtl_1.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_1.sk1_item_no       = di.sk1_item_no
                                                    and rtl_1.calendar_date     = jdaff.calendar_date -1
                                          
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_2 on rtl_2.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_2.sk1_item_no       = di.sk1_item_no
                                                    and rtl_2.calendar_date     = jdaff.calendar_date -2
           
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_3 on rtl_3.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_3.sk1_item_no       = di.sk1_item_no
                                                    and rtl_3.calendar_date     = jdaff.calendar_date -3
                                          
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_4 on rtl_4.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_4.sk1_item_no       = di.sk1_item_no
                                                    and rtl_4.calendar_date     = jdaff.calendar_date -4
                                                    
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_5 on rtl_5.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_5.sk1_item_no       = di.sk1_item_no
                                                    and rtl_5.calendar_date     = jdaff.calendar_date -5
       
       
           ) mer_mart
     
  on (mer_mart.sk1_location_no  = di_mart.sk1_location_no
  and mer_mart.sk1_item_no      = di_mart.sk1_item_no
  and mer_mart.calendar_date    = di_mart.calendar_date)
          
  when matched then 
    update set  WEEK_1_DAY_1_CASES  = mer_mart.week_1_day_1_cases,
                WEEK_1_DAY_2_CASES  = mer_mart.week_1_day_2_cases,
                WEEK_1_DAY_3_CASES  = mer_mart.week_1_day_3_cases,
                WEEK_1_DAY_4_CASES  = mer_mart.week_1_day_4_cases,
                WEEK_1_DAY_5_CASES  = mer_mart.week_1_day_5_cases,
                WEEK_1_DAY_6_CASES  = mer_mart.week_1_day_6_cases,
                WEEK_1_DAY_7_CASES  = mer_mart.week_1_day_7_cases,
                WEEK_2_DAY_1_CASES  = mer_mart.week_2_day_1_cases,
                WEEK_2_DAY_2_CASES  = mer_mart.week_2_day_2_cases,
                WEEK_2_DAY_3_CASES  = mer_mart.week_2_day_3_cases,
                WEEK_2_DAY_4_CASES  = mer_mart.week_2_day_4_cases,
                WEEK_2_DAY_5_CASES  = mer_mart.week_2_day_5_cases,
                WEEK_2_DAY_6_CASES  = mer_mart.week_2_day_6_cases,
                WEEK_2_DAY_7_CASES  = mer_mart.week_2_day_7_cases,
                WEEK_3_DAY_1_CASES  = mer_mart.week_3_day_1_cases,
                WEEK_3_DAY_2_CASES  = mer_mart.week_3_day_2_cases,
                WEEK_3_DAY_3_CASES  = mer_mart.week_3_day_3_cases,
                WEEK_3_DAY_4_CASES  = mer_mart.week_3_day_4_cases,
                WEEK_3_DAY_5_CASES  = mer_mart.week_3_day_5_cases,
                WEEK_3_DAY_6_CASES  = mer_mart.week_3_day_6_cases,
                WEEK_3_DAY_7_CASES  = mer_mart.week_3_day_7_cases,
                last_updated_date   = g_date
                
     when not matched then
       insert 
         (sk1_location_no, 
          sk1_item_no, 
          calendar_date,
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
          week_3_day_7_cases,
          last_updated_date
        )
        
      values
        ( mer_mart.sk1_location_no,
          mer_mart.sk1_item_no,
          mer_mart.calendar_date,
          mer_mart.week_1_day_1_cases,
          mer_mart.week_1_day_2_cases,
          mer_mart.week_1_day_3_cases,
          mer_mart.week_1_day_4_cases,
          mer_mart.week_1_day_5_cases,
          mer_mart.week_1_day_6_cases,
          mer_mart.week_1_day_7_cases,
          mer_mart.week_2_day_1_cases,
          mer_mart.week_2_day_2_cases,
          mer_mart.week_2_day_3_cases,
          mer_mart.week_2_day_4_cases,
          mer_mart.week_2_day_5_cases,
          mer_mart.week_2_day_6_cases,
          mer_mart.week_2_day_7_cases,
          mer_mart.week_3_day_1_cases,
          mer_mart.week_3_day_2_cases,
          mer_mart.week_3_day_3_cases,
          mer_mart.week_3_day_4_cases,
          mer_mart.week_3_day_5_cases,
          mer_mart.week_3_day_6_cases,
          mer_mart.week_3_day_7_cases,
          g_date
          ); 
  end if;
  
  if l_day_no = 7 then
    ---------------------------------------------------------------------------------------
    -- sustitute the first 6 days cases values to be used on the table with historical data
    -- this is done as the values are not passed by JDA but must be shown on the record
    ---------------------------------------------------------------------------------------
    MERGE /*+ PARALLEL(di_mart,4) */ INTO W6005682.RTL_LOC_ITEM_DC_WH_PLANQ  di_mart  USING (   
       select /*+ parallel(jdaff,4) parallel(rtl_1,4) parallel(rtl_2,4) parallel(rtl_3,4) parallel(rtl_4,4) parallel(rtl_5,4) parallel(rtl_6,4) full(di) full(dl) */
            dl.sk1_location_no, di.sk1_item_no, jdaff.calendar_date,
            
            nvl(rtl_3.week_1_day_1_cases,0) week_1_day_1_cases,
            nvl(rtl_2.week_1_day_2_cases,0) week_1_day_2_cases,
            nvl(rtl_1.week_1_day_3_cases,0) week_1_day_3_cases,
            nvl(rtl_4.week_1_day_4_cases,0) week_1_day_4_cases,
            nvl(rtl_5.week_1_day_5_cases,0) week_1_day_5_cases,
            nvl(rtl_6.week_1_day_6_cases,0) week_1_day_6_cases,
            
            jdaff.week_1_day_1_cases week_1_day_7_cases,
            
            jdaff.week_1_day_2_cases week_2_day_1_cases,
            jdaff.week_1_day_3_cases week_2_day_2_cases,
            jdaff.week_1_day_4_cases week_2_day_3_cases,
            jdaff.week_1_day_5_cases week_2_day_4_cases,
            jdaff.week_1_day_6_cases week_2_day_5_cases,
            jdaff.week_1_day_7_cases week_2_day_6_cases,
            jdaff.week_2_day_1_cases week_2_day_7_cases,
            
            jdaff.week_2_day_2_cases week_3_day_1_cases,
            jdaff.week_2_day_3_cases week_3_day_2_cases,
            jdaff.week_2_day_4_cases week_3_day_3_cases,
            jdaff.week_2_day_5_cases week_3_day_4_cases,
            jdaff.week_2_day_6_cases week_3_day_5_cases,
            jdaff.week_2_day_7_cases week_3_day_6_cases,
            jdaff.week_3_day_1_cases week_3_day_7_cases

   from     dwh_foundation.fnd_loc_item_jdaff_wh_plan jdaff
   join     dim_location dl               on jdaff.location_no    = dl.location_no
   join     dim_item di                   on jdaff.item_no        = di.item_no
   
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_1 on rtl_1.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_1.sk1_item_no       = di.sk1_item_no
                                                    and rtl_1.calendar_date     = jdaff.calendar_date -1
                                          
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_2 on rtl_2.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_2.sk1_item_no       = di.sk1_item_no
                                                    and rtl_2.calendar_date     = jdaff.calendar_date -2
           
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_3 on rtl_3.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_3.sk1_item_no       = di.sk1_item_no
                                                    and rtl_3.calendar_date     = jdaff.calendar_date -3
                                          
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_4 on rtl_4.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_4.sk1_item_no       = di.sk1_item_no
                                                    and rtl_4.calendar_date     = jdaff.calendar_date -4
                                                    
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_5 on rtl_5.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_5.sk1_item_no       = di.sk1_item_no
                                                    and rtl_5.calendar_date     = jdaff.calendar_date -5
                                                    
   left join W6005682.RTL_LOC_ITEM_DC_WH_PLANQ rtl_6 on rtl_6.sk1_location_no   = dl.sk1_location_no
                                                    and rtl_6.sk1_item_no       = di.sk1_item_no
                                                    and rtl_6.calendar_date     = jdaff.calendar_date -6

           ) mer_mart
     
  on (mer_mart.sk1_location_no  = di_mart.sk1_location_no
  and mer_mart.sk1_item_no      = di_mart.sk1_item_no
  and mer_mart.calendar_date    = di_mart.calendar_date)
          
  when matched then 
    update set  WEEK_1_DAY_1_CASES  = mer_mart.week_1_day_1_cases,
                WEEK_1_DAY_2_CASES  = mer_mart.week_1_day_2_cases,
                WEEK_1_DAY_3_CASES  = mer_mart.week_1_day_3_cases,
                WEEK_1_DAY_4_CASES  = mer_mart.week_1_day_4_cases,
                WEEK_1_DAY_5_CASES  = mer_mart.week_1_day_5_cases,
                WEEK_1_DAY_6_CASES  = mer_mart.week_1_day_6_cases,
                WEEK_1_DAY_7_CASES  = mer_mart.week_1_day_7_cases,
                WEEK_2_DAY_1_CASES  = mer_mart.week_2_day_1_cases,
                WEEK_2_DAY_2_CASES  = mer_mart.week_2_day_2_cases,
                WEEK_2_DAY_3_CASES  = mer_mart.week_2_day_3_cases,
                WEEK_2_DAY_4_CASES  = mer_mart.week_2_day_4_cases,
                WEEK_2_DAY_5_CASES  = mer_mart.week_2_day_5_cases,
                WEEK_2_DAY_6_CASES  = mer_mart.week_2_day_6_cases,
                WEEK_2_DAY_7_CASES  = mer_mart.week_2_day_7_cases,
                WEEK_3_DAY_1_CASES  = mer_mart.week_3_day_1_cases,
                WEEK_3_DAY_2_CASES  = mer_mart.week_3_day_2_cases,
                WEEK_3_DAY_3_CASES  = mer_mart.week_3_day_3_cases,
                WEEK_3_DAY_4_CASES  = mer_mart.week_3_day_4_cases,
                WEEK_3_DAY_5_CASES  = mer_mart.week_3_day_5_cases,
                WEEK_3_DAY_6_CASES  = mer_mart.week_3_day_6_cases,
                WEEK_3_DAY_7_CASES  = mer_mart.week_3_day_7_cases,
                last_updated_date   = g_date
                
     when not matched then
       insert 
         (sk1_location_no, 
          sk1_item_no, 
          calendar_date,
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
          week_3_day_7_cases,
          last_updated_date
        )
        
      values
        ( mer_mart.sk1_location_no,
          mer_mart.sk1_item_no,
          mer_mart.calendar_date,
          mer_mart.week_1_day_1_cases,
          mer_mart.week_1_day_2_cases,
          mer_mart.week_1_day_3_cases,
          mer_mart.week_1_day_4_cases,
          mer_mart.week_1_day_5_cases,
          mer_mart.week_1_day_6_cases,
          mer_mart.week_1_day_7_cases,
          mer_mart.week_2_day_1_cases,
          mer_mart.week_2_day_2_cases,
          mer_mart.week_2_day_3_cases,
          mer_mart.week_2_day_4_cases,
          mer_mart.week_2_day_5_cases,
          mer_mart.week_2_day_6_cases,
          mer_mart.week_2_day_7_cases,
          mer_mart.week_3_day_1_cases,
          mer_mart.week_3_day_2_cases,
          mer_mart.week_3_day_3_cases,
          mer_mart.week_3_day_4_cases,
          mer_mart.week_3_day_5_cases,
          mer_mart.week_3_day_6_cases,
          mer_mart.week_3_day_7_cases,
          g_date
          ); 
  end if;
  
    g_recs_updated := g_recs_updated + SQL%ROWCOUNT;      
    
    COMMIT;  
    
    l_text := 'DONE W6005682.RTL_LOC_ITEM_DC_WH_PLANQ UPDATE ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);      
  
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

end wh_prf_corp_741u_merge;
