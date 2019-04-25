--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_003U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_003U" (p_forall_limit in integer,p_success out boolean,P_end_date in date)
as
  --**************************************************************************************************
--  Date:        march 2009
--  Author:      Wendy Lyttle
--  Purpose:     Create a DIM_CALENDAR table loaded with
--               rolling 24 months of date data to be used for
--               various day and week sets of data
--               Covers periods with :
--               a.) 3wks rolling at day level and rest at week level for 1 year
--                          and then the equivalent for last-year
--               b.) 6wks rolling at day level and rest at week level for 1 year
--                          and then the equivalent for last-year
--               c.) 9wks rolling at day level and rest at week level for 1 year
--                          and then the equivalent for last-year
--               d.) 13wks rolling at day level and rest at week level for 1 year
--                          and then the equivalent for last-year
--               e.) 1wks rolling at WEEK level and rest at week level for a
--                          total of 30 months
--  Tables:      Input  - dim_calendar, dim_control
--               Output - dim_calendar
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  12 MAY 2009 - Defect 1541 - Add New Fields to Cater for Calendar Roll Time
--                               Periods to DIM_CALENDAR
--  26 May 2009 - defect 1657 - Change Cube_A FDP from 5 weeks to 6 weeks
--
--  04 July 2009 - defect 1981 - Dim_calendar to have 3wks-rolling instead
--                               of 2wks-rolling
--  10 July 2009 - defect 2031 - dim_calendar roll_2wk_ind should not be
--                                NULL for all future weeks
--  31 JULY 2009 - DEFECT 2164 -  WWPR:  New view VS_WWP_RTL_LC_ITM_DY_WK_STK_B
--                                to limit stock to 6wks @ day TY/LY and
--                                  remainder @ wk for total of 2yrs
--  13 june 2011 - defect 4354 - FIN YR-END 2011 - changes to generation of DIM_CALENDAR
--  13 July 2017               - update roll_2wk_ind for 2 years back aswell - needed due to 
--                               a business rule change
--
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
  g_forall_limit     integer := 10000;
  g_recs_read        integer := 0;
  g_recs_updated     integer := 0;
  g_recs_inserted    integer := 0;
  g_recs_hospital    integer := 0;
  g_recs_integ       integer := 0;
  g_recs_rejected    integer := 0;
  g_error_count      number  := 0;

  g_error_index      number  := 0;
  g_cnt              number  := 0;
  g_found            boolean;
  g_valid            boolean;
  g_date             date := trunc(sysdate);
  g_date_min_8days   date;
  g_date_plus_4wks   date;
  g_date_min_22days  date;
  g_date_min_50days  date;
  g_date_min_64days  date;
  g_date_min_92days  date;
  l_message sys_dwh_errlog.log_text%type;
--  l_module_name sys_dwh_errlog.log_procedure_name%type     := 'WH_PRF_DIM_CALENDAR_LOAD';
  l_module_name sys_dwh_errlog.log_procedure_name%type     := 'WH_PRF_CORP_003U';
  l_name sys_dwh_log.log_name%type                         := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type           := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type           := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type     := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'LOAD OF DIM_CALENDAR';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  
  L_FIN_YEAR_NO      number;
  l_prev_fin_year_no number;
  l_upd_count  number;

begin
      if p_forall_limit is not null and p_forall_limit > 100 then
        g_forall_limit  := p_forall_limit;
      end if;
      dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
      p_success := false;
      l_text    := dwh_constants.vc_log_draw_line;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'LOAD OF DIM_CALENDAR ex DIM_CALENDAR STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

  --**************************************************************************************************
  -- LOOK UP BATCH DATE FROM DIM_CONTROL
  --**************************************************************************************************
      if p_end_date is null then
           dwh_lookup.dim_control(g_date);
      else
           g_date := p_end_date;
      end if;

      dwh_lookup.dim_control(g_date);
      
      select today_fin_year_no, today_fin_year_no - 1
        into l_fin_year_no ,  l_prev_fin_year_no
        from dim_control;
        
      l_text := 'l_fin_year_no =  '||l_fin_year_no || ' / l_prev_fin_year_no = ' || l_prev_fin_year_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  


--g_date := '1 jun 2010';
--g_date := '10 jun 2010';
--g_date := '19 apr 2011';
--g_date := '26 jun 2011';
--g_date := '4 jul 2011';
--g_date := '19 jun 2011';
--g_date := '4 jul 2011';
--g_date := '15 aug 2011';

      l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      dbms_output.put_line(l_text);

  --**************************************************************************************************
  -- Clear DIM_CALENDAR 2-WK-ROLL-IND, 6-WK-ROLL-IND, 9-WK-ROLL-IND, 13-WK-ROLL-IND
  --**************************************************************************************************
       update DWH_PERFORMANCE.DIM_CALENDAR
                            set roll_2wk_ind = null,
                                roll_6wk_ind = null,
                                roll_9wk_ind = null,
                                roll_13wk_ind = null,
                                roll_1wk_30mth_ind = null;
      commit;
      l_text           := '** all rolling indicators updated to null on DIM_CALENDAR';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  -- Set-up BATCH DATE minus various days
  --**************************************************************************************************
      g_date_min_22days     := g_date-22;
      g_date_min_50days     := g_date-50;
      g_date_min_64days     := g_date-64;
      g_date_min_92days     := g_date-92;
      g_date_min_8days      := g_date-8;
      --
      -- The effect of the following code will be to set the indicators
      -- to 1 for future dates
      -- ie. current week and then 4 weeks at daily level
      --
      select this_week_end_date+28
      into g_date_plus_4wks
      from DIM_CALENDAR
      where calendar_date = g_date;
      -----
      l_text           := '** this_week_end_date+28='||g_date_plus_4wks;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


  --**************************************************************************************************
  -- Setup ROLL_2WK_IND, ROLL_6WK_IND, ROLL_9WK_IND, ROLL_13WK_IND, ROLL_1WK_30mth_IND for This Year and Last Year
  -- 1. Set indicator = 0 for full 12 months THIS YEAR
  -- 2. Set indicator = 0 for full 30 months  for roll_1wk_30mth_ind
  -- 3. Set indicator = 0 for equivalent 12 months LAST YEAR
  --**************************************************************************************************
      execute immediate('update DWH_PERFORMANCE.DIM_CALENDAR dc
                            set roll_6wk_ind = 0,
                                roll_9wk_ind = 0,
                                roll_13wk_ind = 0
                         where  dc.calendar_date <= '''||G_DATE||
                           '''and dc.calendar_date  >  trunc((ADD_MONTHS('''||G_DATE||''',-12))+ interval ''1'' day)');
      COMMIT;
      
      l_text := '1 - update DIM_CALENDAR dc set roll_6wk_ind = 0, roll_9wk_ind = 0,roll_13wk_ind = 0 where  dc.calendar_date <= '''||G_DATE||'''and dc.calendar_date  >  trunc((ADD_MONTHS('''||G_DATE||''',-12))+ interval ''1'' day)';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      execute immediate('update DWH_PERFORMANCE.DIM_CALENDAR dc
                            set roll_2wk_ind = 0,
                                roll_6wk_ind = 0
                         where  dc.calendar_date <= '''||g_date_plus_4wks||
                           '''and dc.calendar_date  >  trunc((ADD_MONTHS('''||G_DATE||''',-12))+ interval ''1'' day)');
      commit;
      
      l_text := '2 - update DIM_CALENDAR dc set roll_2wk_ind = 0, roll_6wk_ind = 0 where  dc.calendar_date <= '''||g_date_plus_4wks|| '''and dc.calendar_date  >  trunc((ADD_MONTHS('''||G_DATE||''',-12))+ interval ''1'' day)';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      execute immediate('update DWH_PERFORMANCE.DIM_CALENDAR dc
                            set roll_1wk_30mth_ind = 0
                         where  dc.calendar_date <= '''||G_DATE||
                           '''and dc.calendar_date  >  trunc((ADD_MONTHS('''||G_DATE||''',-30))+ interval ''1'' day)');
      commit;

      update DWH_PERFORMANCE.DIM_CALENDAR dc
            set dc.roll_2wk_ind = 0,
                 dc.roll_6wk_ind = 0,
                 dc.roll_9wk_ind = 0,
                 dc.roll_13wk_ind = 0
      where dc.calendar_date in (select dct.ly_calendar_date
                                 from DIM_CALENDAR dct
                                where dct.roll_2wk_ind = 0);
      commit;


      L_TEXT  := ' about to set 2wk ind for 2 years back using ly_calendar_date for ' ||l_prev_fin_year_no|| ' = 0';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      
      -- RUN THE UPDATE AGAIN NOW THAT THE PREVIOUS YEAR INDICATORS HAS BEEN SET IN ORDER TO SET INDICATORS FOR 2 YEARS BACK
      --====================================================================================================================
      update DWH_PERFORMANCE.DIM_CALENDAR dc
            set dc.roll_2wk_ind = 0,
                 dc.roll_6wk_ind = 0,
                 dc.roll_9wk_ind = 0,
                 dc.roll_13wk_ind = 0
      where dc.calendar_date in (select dct.ly_calendar_date
                                 from DIM_CALENDAR dct
                                where dct.roll_2wk_ind = 0
                                  and dct.fin_year_no = l_prev_fin_year_no   -- added 13 July 2017
                                );
      commit;


  --**************************************************************************************************
  -- Setup ROLL_2WK_IND
  -- 1. Set indicator = 1 for first 3 weeks at Daily Level THIS YEAR
  --    (might not be full 14 days depending on G_DATE)
  --    (ie. if run being done on a Wednesday, then indicator for 3 weeks will be set for
  --         Mon+Tues+Wed this week and then Mon-to-Sun of Last 2 weeks)
  -- 2. Set indicator = 1 for equivalent 3 weeks at Daily Level LAST YEAR
  --**************************************************************************************************
--      execute immediate('update DIM_CALENDAR dc
--                          set dc.roll_2wk_ind = 1
--                        where dc.calendar_date >=  (select min(calendar_date)
--                                                    from DIM_CALENDAR dct
--                                                    where dct.calendar_date > '''||g_date_min_22days||'''
--                                                      and dct.calendar_date <= '''||G_DATE||''' and dct.fin_day_no = 1)
--                          and dc.roll_2wk_ind = 0');
--      commit;
--
-- code prior to qc 2031
--
--      execute immediate('update DIM_CALENDAR dc
--                          set dc.roll_2wk_ind = 1
--                        where dc.calendar_date >=  (select min(dct.calendar_date)
--                                                    from DIM_CALENDAR dct
--                                                    where dct.calendar_date > '''||g_date_min_22days||'''
--                                                      and dct.calendar_date <= '''||G_DATE||''' and dct.fin_day_no = 1)
--                         and dc.calendar_date <= (select distinct dct2.this_week_end_date
--                                                      from DIM_CALENDAR dct2
--                                                    where dct2.calendar_date = '''||G_DATE||''')
--                                                    ' );
   L_TEXT           := '** g_date='||g_date;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   L_TEXT           := '** g_date_min_22days='||g_date_min_22days;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   L_TEXT           := '** g_date_plus_4wks='||g_date_plus_4wks;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      execute immediate('update DWH_PERFORMANCE.DIM_CALENDAR dc
                          set dc.roll_2wk_ind = 1
                        where dc.calendar_date >=  (select min(dct.calendar_date)
                                                    from DIM_CALENDAR dct
                                                    where dct.calendar_date > '''||g_date_min_22days||'''
                                                      and dct.calendar_date <= '''||g_date||''' and dct.fin_day_no = 1)
                          and dc.calendar_date <= (select distinct dct2.this_week_end_date
                                                      from DIM_CALENDAR dct2
                                                    where dct2.calendar_date = '''||g_date||''')
                                                    ' );
      commit;

      update DWH_PERFORMANCE.DIM_CALENDAR dc
        set dc.roll_2wk_ind = 1
      where  dc.calendar_date in (select dct.ly_calendar_date
                                  from DIM_CALENDAR dct
                                  where dct.roll_2wk_ind = 1 );
                              
      commit;
      
      L_TEXT := ' about to set 2wk ind for 2 years back using ly_calendar_date from FY ' ||l_prev_fin_year_no|| '  = 1';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_upd_count := 0;
      
      -- RUN THE UPDATE AGAIN NOW THAT THE PREVIOUS YEAR INDICATORS HAS BEEN SET IN ORDER TO SET INDICATORS FOR 2 YEARS BACK
      --====================================================================================================================
      update DWH_PERFORMANCE.DIM_CALENDAR dc
        set dc.roll_2wk_ind = 1
      where  dc.calendar_date in (select dct.ly_calendar_date
                                  from DIM_CALENDAR dct
                                  where dct.roll_2wk_ind = 1
                                    and fin_year_no = l_prev_fin_year_no    -- added 13 July 2017
                                 );


      l_upd_count := l_upd_count+SQL%ROWCOUNT;
      l_text := '(2) records updated = ' || l_upd_count;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
      
      commit;

    --**************************************************************************************************
  -- Setup ROLL_6WK_IND
  -- 1. Set indicator = 1 for first 6 weeks at Daily Level THIS YEAR
  --    (might not be full 49 days depending on G_DATE)
  --    (ie. if run being done on a Wednesday, then indicator for 6 weeks will be set for
  --         Mon+Tues+Wed this week and then Mon-to-Sun of Last week)
  -- 2. Set indicator = 1 for equivalent 6 weeks at Daily Level LAST YEAR
  --**************************************************************************************************
  --     execute immediate('update DIM_CALENDAR dc
  --                           set dc.roll_6wk_ind = 1
  --                         where dc.calendar_date >=  (select min(calendar_date)
   --                                                    from DIM_CALENDAR dct
   --                                                    where dct.calendar_date > '''||g_date_min_50days||'''
   --                                                      and dct.calendar_date <= '''||G_DATE||''' and dct.fin_day_no = 1)
   --                          and dc.roll_6wk_ind = 0');
   --    commit;
 --
   --  --   g_recs_inserted :=0;
    --   select count(*) into g_recs_inserted
    --   from DIM_CALENDAR
    --   where roll_6wk_ind = 1;
    --   dbms_output.put_line('No. of Recs for First Year Day level = '||g_recs_inserted);
 --
    --   update DIM_CALENDAR dc
    --     set dc.roll_6wk_ind = 1
    --   where  dc.calendar_date in (select dct.ly_calendar_date
    --                               from DIM_CALENDAR dct
    --                               where dct.roll_6wk_ind = 1);
    --   commit;
 --
   --    g_recs_inserted :=0;
   --    select count(*) into g_recs_inserted
   --    from DIM_CALENDAR
    --   where roll_6wk_ind = 1;
    --   dbms_output.put_line('No. of Recs for both Years Day level= '||g_recs_inserted);

           execute immediate('update DWH_PERFORMANCE.DIM_CALENDAR dc
                          set dc.roll_6wk_ind = 1
                        where dc.calendar_date >=  (select min(dct.calendar_date)
                                                    from DIM_CALENDAR dct
                                                    where dct.calendar_date > '''||g_date_min_50days||'''
                                                      and dct.calendar_date <= '''||g_date||''' and dct.fin_day_no = 1)
                          and dc.calendar_date <= (select distinct dct2.this_week_end_date
                                                      from DIM_CALENDAR dct2
                                                    where dct2.calendar_date = '''||g_date||''')
                                                    ' );
      commit;


      update DWH_PERFORMANCE.DIM_CALENDAR dc
        set dc.roll_6wk_ind = 1
      where  dc.calendar_date in (select dct.ly_calendar_date
                                  from DIM_CALENDAR dct
                                  where dct.roll_6wk_ind = 1);
      commit;


  --**************************************************************************************************
  -- Setup ROLL_9WK_IND
  -- 1. Set indicator = 1 for first 9 weeks at Daily Level THIS YEAR
  --    (might not be full 63 days depending on G_DATE)
  --    (ie. if run being done on a Wednesday, then indicator for 9 weeks will be set for
  --         Mon+Tues+Wed this week and then Mon-to-Sun of Last week)
  -- 2. Set indicator = 1 for equivalent 9 weeks at Daily Level LAST YEAR
  --**************************************************************************************************
      execute immediate('update DWH_PERFORMANCE.DIM_CALENDAR dc
                            set dc.roll_9wk_ind = 1
                          where dc.calendar_date >=  (select min(calendar_date)
                                                      from DIM_CALENDAR dct
                                                      where dct.calendar_date > '''||g_date_min_64days||'''
                                                        and dct.calendar_date <= '''||G_DATE||''' and dct.fin_day_no = 1)
                            and dc.roll_9wk_ind = 0');
      commit;

      update DWH_PERFORMANCE.DIM_CALENDAR dc
        set dc.roll_9wk_ind = 1
      where  dc.calendar_date in (select dct.ly_calendar_date
                                  from DIM_CALENDAR dct
                                  where dct.roll_9wk_ind = 1);
      commit;


    --**************************************************************************************************
  -- Setup ROLL_13WK_IND
  -- 1. Set indicator = 1 for first 13 weeks at Daily Level THIS YEAR
  --    (might not be full 91 days depending on G_DATE)
  --    (ie. if run being done on a Wednesday, then indicator for 13 weeks will be set for
  --         Mon+Tues+Wed this week and then Mon-to-Sun of Last week)
  -- 2. Set indicator = 1 for equivalent 13 weeks at Daily Level LAST YEAR
  --**************************************************************************************************
      execute immediate('update DWH_PERFORMANCE.DIM_CALENDAR dc
                            set dc.roll_13wk_ind = 1
                          where dc.calendar_date >=  (select min(calendar_date)
                                                      from DIM_CALENDAR dct
                                                      where dct.calendar_date > '''||g_date_min_92days||'''
                                                        and dct.calendar_date <= '''||G_DATE||''' and dct.fin_day_no = 1)
                            and dc.roll_13wk_ind = 0');
      commit;


    --**************************************************************************************************
  -- Setup ROLL_1WK_30mth_IND
  -- 1. Set indicator = 1 for first 1 weeks at Daily Level
  --    (might not be full 7 days depending on G_DATE)
  --    (ie. if run being done on a Wednesday, then indicator for 13 weeks will be set for
  --         Mon+Tues+Wed this week and then Mon-to-Sun of Last week)
  --**************************************************************************************************
      execute immediate('update DWH_PERFORMANCE.DIM_CALENDAR dc
                            set dc.roll_1wk_30mth_ind = 1
                          where dc.calendar_date >=  (select min(calendar_date)
                                                      from DIM_CALENDAR dct
                                                      where dct.calendar_date > '''||g_date_min_8days||'''
                                                        and dct.calendar_date <= '''||G_DATE||''' and dct.fin_day_no = 1)
                            and dc.roll_1wk_30mth_ind = 0');
      commit;

      update DWH_PERFORMANCE.DIM_CALENDAR dc
        set dc.roll_13wk_ind = 1
      where  dc.calendar_date in (select dct.ly_calendar_date
                                  from DIM_CALENDAR dct
                                  where dct.roll_13wk_ind = 1);
      commit;


      -- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
      -- check values on DIM_CALENDAR - written to log
      -- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
      --
      -- ROLL_2WK_IND
      --
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where roll_2wk_ind = 0;
--      IF g_cnt < 706
--      OR g_cnt > 719 then
      If g_cnt between 706 and 719 then
      L_TEXT           := '** ROLL_2WK_IND - indicator set to 0 = '||G_CNT;
      else
      l_text           := '** ERROR **  ROLL_2WK_IND - indicator set to 0 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where roll_2wk_ind = 1;
 --     IF g_cnt < 42
 --     OR g_cnt > 56 then
      If g_cnt between 42 and 56 then
      L_TEXT           := '** ROLL_2WK_IND - indicator set to 1 = '||G_CNT;
      ELSE
      l_text           := '** ERROR **  ROLL_2WK_IND - indicator set to 1 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --
      -- ROLL_6WK_IND
      --
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where roll_6wk_ind = 1;
--      IF g_cnt < 98
--      OR g_cnt > 112 then
      If g_cnt between 98 and 112 then
      L_TEXT           := '** ROLL_6WK_IND - indicator set to 1 = '||G_CNT;
      else
      l_text           := '** ERROR **  ROLL_6WK_IND - indicator set to 1 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where roll_6wk_ind = 0;
--      IF g_cnt < 650
--      OR g_cnt > 664 then
      If g_cnt between 650 and 664 then
      L_TEXT           := '** ROLL_6WK_IND - indicator set to 0 = '||G_CNT;
      else
      l_text           := '** ERROR **  ROLL_6WK_IND - indicator set to 0 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --
      -- ROLL_9WK_IND
      --
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where roll_9wk_ind = 1;
--      IF g_cnt < 595
--      OR g_cnt > 613 then
      If g_cnt between 115 and 133 then
      L_TEXT           := '** ROLL_9WK_IND - indicator set to 1 = '||G_CNT;
      else
      l_text           := '** ERROR **  ROLL_9WK_IND - indicator set to 1 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where roll_9wk_ind = 0;
--      IF g_cnt < 115
--      OR g_cnt > 133  then
      If g_cnt between 595 and 613 then
      L_TEXT           := '** ROLL_9WK_IND - indicator set to 0 = '||G_CNT;
      else
      l_text           := '** ERROR **  ROLL_9WK_IND - indicator set to 0 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --
      -- ROLL_13WK_IND
      --
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where roll_13wk_ind = 1;
--      IF g_cnt < 539
--      OR g_cnt > 549 then
      If g_cnt between 174 and 190 then
      L_TEXT           := '** ROLL_13WK_IND - indicator set to 1 = '||G_CNT;
      else
      l_text           := '** ERROR **  ROLL_13WK_IND - indicator set to 1 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where roll_13wk_ind = 0;
--      IF g_cnt < 179
--      OR g_cnt > 189 then
      If g_cnt between 545 and 555 then
      L_TEXT           := '** ROLL_13WK_IND - indicator set to 0 = '||G_CNT;
      else
      l_text           := '** ERROR **  ROLL_13WK_IND - indicator set to 0 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --
      -- ROLL_1WK_30MTH_IND
      --
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where ROLL_1WK_30MTH_IND = 1;
--      IF g_cnt < 897
--      OR g_cnt > 907 then
      If g_cnt between 3 and 13 then
      L_TEXT           := '** ROLL_1WK_30MTH_IND - indicator set to 1 = '||G_CNT;
      else
      l_text           := '** ERROR **  ROLL_1WK_30MTH_IND - indicator set to 1 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      select count(*) into g_cnt
      from DWH_PERFORMANCE.DIM_CALENDAR
      where ROLL_1WK_30MTH_IND = 0;
--      IF g_cnt < 3
--      OR g_cnt > 13 then
      If g_cnt between 896 and 912 then
      L_TEXT           := '** ROLL_1WK_30MTH_IND - indicator set to 0 = '||G_CNT;
      else
      l_text           := '** ERROR **  ROLL_1WK_30MTH_IND - indicator set to 0 = '||G_CNT;
      END IF;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
      g_recs_inserted :=0;

      SELECT COUNT(*) INTO g_recs_updated FROM DWH_PERFORMANCE.DIM_CALENDAR
      WHERE ROLL_1WK_30MTH_IND IS NOT NULL
      OR ROLL_13WK_IND IS NOT NULL
      OR ROLL_9WK_IND IS NOT NULL
      OR ROLL_6WK_IND IS NOT NULL
      OR ROLL_2WK_IND IS NOT NULL;

      dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
      l_text := dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := dwh_constants.vc_log_records_read||g_recs_read;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := dwh_constants.vc_log_run_completed ||sysdate;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := dwh_constants.vc_log_draw_line;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := ' ';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      commit;
      p_success := true;

exception
when dwh_errors.e_insert_error then
      l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
      rollback;
      p_success := false;
      raise;

when OTHERS then
      l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
      dwh_log.record_error(l_module_name,sqlcode,l_message);
      dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
      rollback;
      p_success := false;
      raise;

end WH_PRF_CORP_003U;
