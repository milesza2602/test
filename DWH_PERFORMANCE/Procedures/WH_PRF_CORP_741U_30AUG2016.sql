--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_741U_30AUG2016
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_741U_30AUG2016" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2013
--  Author:      Q. Smit
--  Purpose:     Update DC PLANNING data to JDAFF fact table in the performance layer
--               with input ex JDAFF rtl_loc_item_dc_wh_plan table from foundation layer.
--
--  Tables:      Input  - rtl_loc_item_dc_wh_plan
--               Output - rtl_loc_item_dc_wh_plan
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  16 May 2016 - Added substitution based on day of week relating to the calendar date.  
--                From day 2 onwards the previous day(s) of the week is substituted using
--                rules given by JDA.
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_741U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD WH PLAN FACT DATA FROM OM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dc_wh_plan%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dc_wh_plan%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

l_day_no            integer;
l_week_1_day_1_cases       number(14,2);
l_week_1_day_2_cases       number(14,2);
l_week_1_day_3_cases       number(14,2);
l_week_1_day_4_cases       number(14,2);
l_week_1_day_5_cases       number(14,2);
l_week_1_day_6_cases       number(14,2);
l_cnt               integer;
l_date              date;
l_prev_date         date;

cursor c_jdaff_wh_plan is
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
     --and jdaff.calendar_date  = '17/MAY/16'  --g_end_date
     and jdaff.calendar_date  between g_start_date and g_end_date
     --and dc.calendar_date >= '02/MAY/16'
     --AND DI.ITEM_NO = 20093860 
     --AND DL.LOCATION_NO = 222
--order by  dl.sk1_location_no, di.sk1_item_no, dc.calendar_date
;


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
   g_rec_out.calendar_date          := g_rec_in.calendar_date;
   
   l_cnt := 0;
   
--   select fin_day_no 
--     into l_day_no
--     from dim_calendar
--    where calendar_date = g_rec_in.calendar_date;
    
    l_prev_date := g_rec_in.calendar_date-1;
     
    --l_text := 'calendar_date = '||g_rec_in.calendar_date;
    --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --l_text := 'l_day_no = '||l_day_no;
    --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --l_text := 'previous date = '||l_prev_date;
    --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
   
     
   case l_day_no
     when 1 then
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
         
         --l_text := 'g_rec_in.week_1_day_1_cases = '||g_rec_in.week_1_day_1_cases;
         -- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         -- l_text := 'g_rec_out.week_1_day_1_cases = '||g_rec_out.week_1_day_1_cases;
         -- dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         
   
     when 2 then
        -- substitute day 1 value with yesterday's value
        select count(*) into l_cnt 
          from rtl_loc_item_dc_wh_plan
         where sk1_item_no      = g_rec_out.sk1_item_no 
           and sk1_location_no  = g_rec_out.sk1_location_no
           and calendar_date    = g_rec_out.calendar_date-1;
            
        if l_cnt > 0 then
           --l_text := 'Record for previous day found ';
           --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
           select week_1_day_1_cases into l_week_1_day_1_cases
             from rtl_loc_item_dc_wh_plan
            where sk1_item_no     = g_rec_out.sk1_item_no 
              and sk1_location_no = g_rec_out.sk1_location_no
              and calendar_date   = g_rec_out.calendar_date-1;
              
              --l_text := 'l_week_1_day_1_cases from previous day = ' || l_week_1_day_1_cases;
              --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
             
             g_rec_out.week_1_day_1_cases     := l_week_1_day_1_cases;
        else
             --g_rec_out.week_1_day_1_cases     := 0;
             l_week_1_day_1_cases     := 0;
        end if;
        
        g_rec_out.week_1_day_1_cases     := l_week_1_day_1_cases;
        
        --l_text := 'g_rec_out.week_1_day_1_cases = ' || g_rec_out.week_1_day_1_cases;
        --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
        g_rec_out.week_1_day_2_cases     := g_rec_in.week_1_day_1_cases;
        g_rec_out.week_1_day_3_cases     := g_rec_in.week_1_day_2_cases;
        g_rec_out.week_1_day_4_cases     := g_rec_in.week_1_day_3_cases;
        g_rec_out.week_1_day_5_cases     := g_rec_in.week_1_day_4_cases;
        g_rec_out.week_1_day_6_cases     := g_rec_in.week_1_day_5_cases;
        g_rec_out.week_1_day_7_cases     := g_rec_in.week_1_day_6_cases;
        g_rec_out.week_2_day_1_cases     := g_rec_in.week_1_day_7_cases;
        g_rec_out.week_2_day_2_cases     := g_rec_in.week_2_day_1_cases;
        g_rec_out.week_2_day_3_cases     := g_rec_in.week_2_day_2_cases;
        g_rec_out.week_2_day_4_cases     := g_rec_in.week_2_day_3_cases;
        g_rec_out.week_2_day_5_cases     := g_rec_in.week_2_day_4_cases;
        g_rec_out.week_2_day_6_cases     := g_rec_in.week_2_day_5_cases;
        g_rec_out.week_2_day_7_cases     := g_rec_in.week_2_day_6_cases;
        g_rec_out.week_3_day_1_cases     := g_rec_in.week_2_day_7_cases;
        g_rec_out.week_3_day_2_cases     := g_rec_in.week_3_day_1_cases;
        g_rec_out.week_3_day_3_cases     := g_rec_in.week_3_day_2_cases;
        g_rec_out.week_3_day_4_cases     := g_rec_in.week_3_day_3_cases;
        g_rec_out.week_3_day_5_cases     := g_rec_in.week_3_day_4_cases;
        g_rec_out.week_3_day_6_cases     := g_rec_in.week_3_day_5_cases;
        g_rec_out.week_3_day_7_cases     := g_rec_in.week_3_day_6_cases; 
        
     when 3 then
          ---------------------------------------------------------------------------------------
          -- sustitute the first 2 day cases values to be used on the table with historical data
          -- this is done as the values are not passed by JDA but must be shown on the record
          ---------------------------------------------------------------------------------------
          select count(*) into l_cnt 
          from rtl_loc_item_dc_wh_plan
         where sk1_item_no        = g_rec_out.sk1_item_no 
           and sk1_location_no    = g_rec_out.sk1_location_no
           and calendar_date  = g_rec_out.calendar_date-1;

        if l_cnt > 0 then
           select week_1_day_2_cases into l_week_1_day_2_cases
             from rtl_loc_item_dc_wh_plan
            where sk1_item_no        = g_rec_out.sk1_item_no 
              and sk1_location_no    = g_rec_out.sk1_location_no
              and calendar_date  = g_rec_out.calendar_date-1;
             
             g_rec_out.week_1_day_2_cases     := l_week_1_day_2_cases;
        else
             g_rec_out.week_1_day_2_cases     := 0;
        end if;
        
         select count(*) into l_cnt 
          from rtl_loc_item_dc_wh_plan
         where sk1_item_no        = g_rec_out.sk1_item_no 
           and sk1_location_no    = g_rec_out.sk1_location_no
           and calendar_date  = g_rec_out.calendar_date-2;
           
        if l_cnt > 0 then
           select week_1_day_1_cases into l_week_1_day_1_cases
             from rtl_loc_item_dc_wh_plan
            where sk1_item_no        = g_rec_out.sk1_item_no 
              and sk1_location_no    = g_rec_out.sk1_location_no
              and calendar_date  = g_rec_out.calendar_date-2;
             
             g_rec_out.week_1_day_1_cases     := l_week_1_day_1_cases;
        else
             g_rec_out.week_1_day_1_cases     := 0;
        end if;
        
        g_rec_out.week_1_day_3_cases     := g_rec_in.week_1_day_1_cases;
        g_rec_out.week_1_day_4_cases     := g_rec_in.week_1_day_2_cases;
        g_rec_out.week_1_day_5_cases     := g_rec_in.week_1_day_3_cases;
        g_rec_out.week_1_day_6_cases     := g_rec_in.week_1_day_4_cases;
        g_rec_out.week_1_day_7_cases     := g_rec_in.week_1_day_5_cases;
        g_rec_out.week_2_day_1_cases     := g_rec_in.week_1_day_6_cases;
        g_rec_out.week_2_day_2_cases     := g_rec_in.week_1_day_7_cases;
        g_rec_out.week_2_day_3_cases    := g_rec_in.week_2_day_1_cases;
        g_rec_out.week_2_day_4_cases    := g_rec_in.week_2_day_2_cases;
        g_rec_out.week_2_day_5_cases    := g_rec_in.week_2_day_3_cases;
        g_rec_out.week_2_day_6_cases    := g_rec_in.week_2_day_4_cases;
        g_rec_out.week_2_day_7_cases    := g_rec_in.week_2_day_5_cases;
        g_rec_out.week_3_day_1_cases    := g_rec_in.week_2_day_6_cases;
        g_rec_out.week_3_day_2_cases    := g_rec_in.week_2_day_7_cases;
        g_rec_out.week_3_day_3_cases    := g_rec_in.week_3_day_1_cases;
        g_rec_out.week_3_day_4_cases    := g_rec_in.week_3_day_2_cases;
        g_rec_out.week_3_day_5_cases    := g_rec_in.week_3_day_3_cases;
        g_rec_out.week_3_day_6_cases    := g_rec_in.week_3_day_4_cases;
        g_rec_out.week_3_day_7_cases    := g_rec_in.week_3_day_5_cases;
           
   
      when 4 then
        ---------------------------------------------------------------------------------------
        -- sustitute the first 3 days cases values to be used on the table with historical data
        -- this is done as the values are not passed by JDA but must be shown on the record
        ---------------------------------------------------------------------------------------
        select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-1;

      if l_cnt > 0 then
         select week_1_day_3_cases into l_week_1_day_3_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-1;
           
           g_rec_out.week_1_day_3_cases     := l_week_1_day_3_cases;
      else
           g_rec_out.week_1_day_3_cases     := 0;
      end if;
      
       select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date      = g_rec_out.calendar_date-2;
         
      if l_cnt > 0 then
         select week_1_day_2_cases into l_week_1_day_2_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-2;
           
           g_rec_out.week_1_day_2_cases     := l_week_1_day_2_cases;
      else
           g_rec_out.week_1_day_2_cases     := 0;
      end if;
      
      select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-3;
         
      if l_cnt > 0 then
         select week_1_day_1_cases into l_week_1_day_1_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-3;
           
           g_rec_out.week_1_day_1_cases     := l_week_1_day_1_cases;
      else
           g_rec_out.week_1_day_1_cases     := 0;
      end if;
      
      g_rec_out.week_1_day_4_cases     := g_rec_in.week_1_day_1_cases;
      g_rec_out.week_1_day_5_cases     := g_rec_in.week_1_day_2_cases;
      g_rec_out.week_1_day_6_cases     := g_rec_in.week_1_day_3_cases;
      g_rec_out.week_1_day_7_cases     := g_rec_in.week_1_day_4_cases;
      g_rec_out.week_2_day_1_cases     := g_rec_in.week_1_day_5_cases;
      g_rec_out.week_2_day_2_cases     := g_rec_in.week_1_day_6_cases;
      g_rec_out.week_2_day_3_cases     := g_rec_in.week_1_day_7_cases;
      g_rec_out.week_2_day_4_cases    := g_rec_in.week_2_day_1_cases;
      g_rec_out.week_2_day_5_cases    := g_rec_in.week_2_day_2_cases;
      g_rec_out.week_2_day_6_cases    := g_rec_in.week_2_day_3_cases;
      g_rec_out.week_2_day_7_cases    := g_rec_in.week_2_day_4_cases;
      g_rec_out.week_3_day_1_cases    := g_rec_in.week_2_day_5_cases;
      g_rec_out.week_3_day_2_cases    := g_rec_in.week_2_day_6_cases;
      g_rec_out.week_3_day_3_cases    := g_rec_in.week_2_day_7_cases;
      g_rec_out.week_3_day_4_cases    := g_rec_in.week_3_day_1_cases;
     g_rec_out.week_3_day_5_cases    := g_rec_in.week_3_day_2_cases;
      g_rec_out.week_3_day_6_cases    := g_rec_in.week_3_day_3_cases;
      g_rec_out.week_3_day_7_cases    := g_rec_in.week_3_day_4_cases;
  
  
    when 5 then
        --l_text := 'HERE'; 
        --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-1;

      if l_cnt > 0 then
         select week_1_day_4_cases into l_week_1_day_4_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date      = g_rec_out.calendar_date-1;
           
           g_rec_out.week_1_day_4_cases     := l_week_1_day_4_cases;
      else
           g_rec_out.week_1_day_4_cases     := 0;
      end if;
      
       select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-2;
         
      if l_cnt > 0 then
         select week_1_day_3_cases into l_week_1_day_3_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-2;
           
           g_rec_out.week_1_day_3_cases     := l_week_1_day_3_cases;
      else
           g_rec_out.week_1_day_3_cases     := 0;
      end if;
      
      select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-3;
         
      if l_cnt > 0 then
         select week_1_day_2_cases into l_week_1_day_2_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-3;
           
           g_rec_out.week_1_day_2_cases     := l_week_1_day_2_cases;
      else
           g_rec_out.week_1_day_2_cases     := 0;
      end if;
      
      select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-4;
         
      if l_cnt > 0 then
         select week_1_day_1_cases into l_week_1_day_1_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-4;
           
           g_rec_out.week_1_day_1_cases     := l_week_1_day_1_cases;
      else
           g_rec_out.week_1_day_1_cases     := 0;
      end if;
      
      --l_text := 'week_1_day_3_cases (week_1_day_7_cases) = ' || g_rec_in.week_1_day_3_cases;
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      g_rec_out.week_1_day_5_cases     := g_rec_in.week_1_day_1_cases;
      g_rec_out.week_1_day_6_cases     := g_rec_in.week_1_day_2_cases;
      g_rec_out.week_1_day_7_cases     := g_rec_in.week_1_day_3_cases;
      g_rec_out.week_2_day_1_cases     := g_rec_in.week_1_day_4_cases;
      g_rec_out.week_2_day_2_cases     := g_rec_in.week_1_day_5_cases;
      g_rec_out.week_2_day_3_cases     := g_rec_in.week_1_day_6_cases;
      g_rec_out.week_2_day_4_cases     := g_rec_in.week_1_day_7_cases;
      g_rec_out.week_2_day_5_cases    := g_rec_in.week_2_day_1_cases;
      g_rec_out.week_2_day_6_cases    := g_rec_in.week_2_day_2_cases;
      g_rec_out.week_2_day_7_cases    := g_rec_in.week_2_day_3_cases;
      g_rec_out.week_3_day_1_cases    := g_rec_in.week_2_day_4_cases;
      g_rec_out.week_3_day_2_cases    := g_rec_in.week_2_day_5_cases;
      g_rec_out.week_3_day_3_cases    := g_rec_in.week_2_day_6_cases;
      g_rec_out.week_3_day_4_cases    := g_rec_in.week_2_day_7_cases;
      g_rec_out.week_3_day_5_cases    := g_rec_in.week_3_day_1_cases;
      g_rec_out.week_3_day_6_cases    := g_rec_in.week_3_day_2_cases;
      g_rec_out.week_3_day_7_cases    := g_rec_in.week_3_day_3_cases;
      

    when 6 then
        select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no      = g_rec_out.sk1_item_no 
         and sk1_location_no  = g_rec_out.sk1_location_no
         and calendar_date    = g_rec_out.calendar_date-1;

      if l_cnt > 0 then
         select week_1_day_5_cases into l_week_1_day_5_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no     = g_rec_out.sk1_item_no 
            and sk1_location_no = g_rec_out.sk1_location_no
            and calendar_date   = g_rec_out.calendar_date-1;
           
           g_rec_out.week_1_day_5_cases     := l_week_1_day_5_cases;
      else
           g_rec_out.week_1_day_5_cases     := 0;
      end if;
      
       select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-2;
         
      if l_cnt > 0 then
         select week_1_day_4_cases into l_week_1_day_4_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date      = g_rec_out.calendar_date-2;
           
           g_rec_out.week_1_day_4_cases     := l_week_1_day_4_cases;
      else
           g_rec_out.week_1_day_4_cases     := 0;
      end if;
      
      select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-3;
         
      if l_cnt > 0 then
         select week_1_day_3_cases into l_week_1_day_3_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-3;
           
           g_rec_out.week_1_day_3_cases     := l_week_1_day_3_cases;
      else
           g_rec_out.week_1_day_3_cases     := 0;
      end if;
      
      select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-4;
         
      if l_cnt > 0 then
         select week_1_day_2_cases into l_week_1_day_2_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date      = g_rec_out.calendar_date-4;
           
           g_rec_out.week_1_day_2_cases     := l_week_1_day_2_cases;
      else
           g_rec_out.week_1_day_2_cases     := 0;
      end if;
      
     select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date      = g_rec_out.calendar_date-5;
         
      if l_cnt > 0 then
         select week_1_day_1_cases into l_week_1_day_1_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-5;
           
           g_rec_out.week_1_day_1_cases     := l_week_1_day_1_cases;
      else
           g_rec_out.week_1_day_1_cases     := 0;
      end if;
           
      g_rec_out.week_1_day_6_cases    := g_rec_in.week_1_day_1_cases;
      g_rec_out.week_1_day_7_cases    := g_rec_in.week_1_day_2_cases;
      g_rec_out.week_2_day_1_cases    := g_rec_in.week_1_day_3_cases;
      g_rec_out.week_2_day_2_cases    := g_rec_in.week_1_day_4_cases;
      g_rec_out.week_2_day_3_cases    := g_rec_in.week_1_day_5_cases;
      g_rec_out.week_2_day_4_cases    := g_rec_in.week_1_day_6_cases;
      g_rec_out.week_2_day_5_cases    := g_rec_in.week_1_day_7_cases;
      g_rec_out.week_2_day_6_cases    := g_rec_in.week_2_day_1_cases;
      g_rec_out.week_2_day_7_cases    := g_rec_in.week_2_day_2_cases;
      g_rec_out.week_3_day_1_cases    := g_rec_in.week_2_day_3_cases;
      g_rec_out.week_3_day_2_cases    := g_rec_in.week_2_day_4_cases;
      g_rec_out.week_3_day_3_cases    := g_rec_in.week_2_day_5_cases;
      g_rec_out.week_3_day_4_cases    := g_rec_in.week_2_day_6_cases;
      g_rec_out.week_3_day_5_cases    := g_rec_in.week_2_day_7_cases;
      g_rec_out.week_3_day_6_cases    := g_rec_in.week_3_day_1_cases;
      g_rec_out.week_3_day_7_cases    := g_rec_in.week_3_day_2_cases;
      

    when 7 then
        select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-1;

      if l_cnt > 0 then
         select week_1_day_6_cases into l_week_1_day_6_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date      = g_rec_out.calendar_date-1;
           
           g_rec_out.week_1_day_6_cases     := l_week_1_day_6_cases;
      else
           g_rec_out.week_1_day_6_cases     := 0;
     end if;
      
       select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-2;
         
      if l_cnt > 0 then
         select week_1_day_5_cases into l_week_1_day_5_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-2;
           
           g_rec_out.week_1_day_5_cases     := l_week_1_day_5_cases;
      else
           g_rec_out.week_1_day_5_cases     := 0;
      end if;
      
      select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-3;
         
      if l_cnt > 0 then
         select week_1_day_4_cases into l_week_1_day_4_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-3;
           
           g_rec_out.week_1_day_4_cases     := l_week_1_day_4_cases;
      else
           g_rec_out.week_1_day_4_cases     := 0;
      end if;
      
      select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-4;
         
      if l_cnt > 0 then
         select week_1_day_3_cases into l_week_1_day_3_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-4;
           
           g_rec_out.week_1_day_3_cases     := l_week_1_day_3_cases;
      else
           g_rec_out.week_1_day_3_cases     := 0;
      end if;
      
     select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-5;
         
      if l_cnt > 0 then
         select week_1_day_2_cases into l_week_1_day_2_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-5;
           
           g_rec_out.week_1_day_2_cases     := l_week_1_day_2_cases;
      else
           g_rec_out.week_1_day_2_cases     := 0;
      end if;
      
      select count(*) into l_cnt 
        from rtl_loc_item_dc_wh_plan
       where sk1_item_no        = g_rec_out.sk1_item_no 
         and sk1_location_no    = g_rec_out.sk1_location_no
         and calendar_date  = g_rec_out.calendar_date-6;
         
      if l_cnt > 0 then
         select week_1_day_1_cases into l_week_1_day_1_cases
           from rtl_loc_item_dc_wh_plan
          where sk1_item_no        = g_rec_out.sk1_item_no 
            and sk1_location_no    = g_rec_out.sk1_location_no
            and calendar_date  = g_rec_out.calendar_date-6;
           
           g_rec_out.week_1_day_1_cases     := l_week_1_day_1_cases;
      else
           g_rec_out.week_1_day_1_cases     := 0;
      end if;
      
      g_rec_out.week_1_day_7_cases    := g_rec_in.week_1_day_1_cases;
      g_rec_out.week_2_day_1_cases    := g_rec_in.week_1_day_2_cases;
      g_rec_out.week_2_day_2_cases    := g_rec_in.week_1_day_3_cases;
      g_rec_out.week_2_day_3_cases    := g_rec_in.week_1_day_4_cases;
      g_rec_out.week_2_day_4_cases    := g_rec_in.week_1_day_5_cases;
      g_rec_out.week_2_day_5_cases    := g_rec_in.week_1_day_6_cases;
      g_rec_out.week_2_day_6_cases    := g_rec_in.week_1_day_7_cases;
      g_rec_out.week_2_day_7_cases    := g_rec_in.week_2_day_1_cases;
      g_rec_out.week_3_day_1_cases    := g_rec_in.week_2_day_2_cases;
      g_rec_out.week_3_day_2_cases    := g_rec_in.week_2_day_3_cases;
      g_rec_out.week_3_day_3_cases    := g_rec_in.week_2_day_4_cases;
      g_rec_out.week_3_day_4_cases    := g_rec_in.week_2_day_5_cases;
      g_rec_out.week_3_day_5_cases    := g_rec_in.week_2_day_6_cases;
      g_rec_out.week_3_day_6_cases    := g_rec_in.week_2_day_7_cases;
      g_rec_out.week_3_day_7_cases    := g_rec_in.week_3_day_1_cases;
      
     
   end case;

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
       insert into rtl_loc_item_dc_wh_plan values a_tbl_insert(i);

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
       update rtl_loc_item_dc_wh_plan
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
   from   rtl_loc_item_dc_wh_plan
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
    l_text := 'LOAD OF rtl_loc_item_dc_wh_plan EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);

    --g_date := '10/MAY/16';

    select this_week_start_date, fin_year_no, fin_week_no, fin_day_no
    into   g_start_date,         g_year1,     g_week1,     g_today_day
    from   dim_calendar
    where  calendar_date = g_date;

    --if g_today_day = 1 then
    --   g_end_date   := g_start_date + 13;
    --else
    --   g_end_date := g_start_date + 20;
    --end if;

 --   g_start_date  := '23/DEC/13';
 --   g_end_date    := '23/DEC/13';
 
    select min(calendar_date), max(calendar_date)
      into g_start_date, g_end_date
      from fnd_loc_item_jdaff_wh_plan
     where last_updated_date = g_date;

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'DATA PERIOD - '||g_start_date||' to '|| g_end_date;
    --l_text := 'DATA PERIOD - '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    dbms_output.put_line(g_year1||g_week1||g_year2||g_week2||g_year3||g_week3||g_start_date||g_end_date);

   select fin_day_no 
     into l_day_no
     from dim_calendar
    where calendar_date = g_start_date;
    
    l_text := 'FIN_DAY BEING PROCESSED - '||l_day_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--G_DATE := 'Moo';

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
          if g_recs_read mod 100000 = 0 then
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

end wh_prf_corp_741u_30AUG2016;
