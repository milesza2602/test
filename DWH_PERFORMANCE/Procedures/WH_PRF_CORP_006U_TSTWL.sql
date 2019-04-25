--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_006U_TSTWL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_006U_TSTWL" (
    p_forall_limit IN integer,
    p_success OUT boolean)
AS
  --**************************************************************************************************
  --  date:        april 2008
  --  author:      alastair de wet
  --  purpose:     create calendar dimention table in the performance layer
  --               with added value ex foundation layer calendar table.
  --  tables:      input  - fnd_calendar
  --               output - dim_caledar
  --  packages:    constants, dwh_log, dwh_valid
  --
  --  maintenance:
  --  10 feb 2009 - defect 782- dwh_performance.DIM_CALENDAR concatination spacings
  --                                          not as per etl, etl unclear.
  --                           field fin_half_code not mapping correctly.
  --                           fin_half_season_long_desc not mapping correctly.
  -- 18 march 2009 - defect 1153 - replace nulls with 'standard' values for some
  --                               performance layer dimension table attributes
  --                             - on column = day_name
  -- 19 march 2009 - defect 1171 - update etl comments for some dwh_performance.DIM_CALENDAR ...
  --                                _desc fields
  --                             - columns : fin_month_long_desc
  --                  ,fin_half_season_long_desc,month_name,cal_month_short_desc
  --                  ,fin_quarter_long_desc,fin_year_short_desc,fin_week_short_desc
  --                  ,fin_half_long_desc,season_short_desc,season_name
  --                  ,fin_month_short_desc,fin_half_short_desc,fin_day_short_desc
  --                  ,day_name,fin_week_long_desc,month_short_name
  --                  ,total_long_desc,fin_year_long_desc,season_long_desc
  --                  ,fin_half_season_short_desc,fin_week_day_long_desc
  --                   ,total_short_desc
  --                  ,day_short_name,fin_quarter_short_desc
  -- 25 march 2009 - defect 1238 - description need to be upper case on
  --                               dwh_performance.DIM_CALENDAR & dwh_performance.DIM_CALENDAR_wk
  -- 25 march 2009 - defect 1239 - cal_month_short_desc on dwh_performance.DIM_CALENDAR
  --                                & dwh_performance.DIM_CALENDAR_wk
  -- 17 july 2009 - defect 2040 - compl week ind logic needs to change for a
  --                                 monday (all cubes)
  -- 25 july 2009 - defect 2134 - compl week ind logic needs to change for
  --                               a monday (all cubes)
  -- 7 june 2011 - defect 4342 - add extra logging for dwh_performance.DIM_CALENDAR processing
  --                             add auto generate of new fin year
--  13 june 2011 - defect 4354 - FIN YR-END 2011 - changes to generation of dwh_performance.DIM_CALENDAR
--   Oct 2013 - defect 4932 - add RSA_PUBLIC_HOLIDAY_DESC to DIM_CALENDAR
----

  --  naming conventions:
  --  g_  -  global variable
  --  l_  -  log table variable
  --  a_  -  array variable
  --  v_  -  local variable as found in packages
  --  p_  -  parameter
  --  c_  -  prefix to cursor
  --**************************************************************************************************
  g_recs_read      integer := 0;
  g_recs_updated   integer := 0;
  g_recs_inserted  integer := 0;
  g_recs_hospital  integer := 0;

  g_forall_limit   integer := 10000;
  g_error_count    number  := 0;
  g_error_index    number  := 0;
  g_select_error   varchar2(40);

  g_rec_out dwh_performance.DIM_CALENDAR%ROWTYPE;
  g_rec_in fnd_calendar%ROWTYPE;

  g_found boolean;

  g_date date := trunc(sysdate);

  g_today_cal_month_no  integer := 0;
  g_today_fin_year_no   integer := 0;
  g_weeks_per_year      integer := 52;
  g_cal_year_end_date   date;
  g_calendar_date       date;
  g_current_fin_year    integer := 0;
  g_min_fin_year        integer := 0;
  g_max_fin_year        integer := 0;
  g_min_calendar_date   date;
  g_max_calendar_date   date;
  g_fin_year_start      date;
  g_fin_year_end        date;
  g_no_of_days          integer := 0;
  g_no_of_weeks         integer := 0;
  g_fin_year_no         integer := 0;
  g_fin_month_no        integer := 0;
  g_fin_week_no         integer := 0;
  g_fin_day_no          integer := 0;
  g_cal_year_no         integer := 0;
  g_last_updated_date   date;
  g_start_calendar_date date;
  g_end_calendar_date   date;
  g_load_new_fin_year   integer := 0;

  l_message sys_dwh_errlog.log_text%TYPE;
  l_module_name sys_dwh_errlog.log_procedure_name%TYPE := 'WH_PRF_CORP_006U_TSTWL';
  l_name sys_dwh_log.log_name%TYPE                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%TYPE       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%TYPE       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%TYPE := l_module_name;
  l_text sys_dwh_log.log_text%TYPE ;
  l_description sys_dwh_log_summary.log_description%TYPE   := 'CREATE dwh_performance.DIM_CALENDAR EX FND_CALENDAR';
  l_process_type sys_dwh_log_summary.log_process_type%TYPE := dwh_constants.vc_log_process_type_n;
  -- for input bulk collect --
TYPE stg_array
IS
  TABLE OF fnd_calendar%ROWTYPE;
  a_stg_input stg_array;

  -- for output arrays into bulk load forall statements --
TYPE tbl_array_i
IS
  TABLE OF dwh_performance.DIM_CALENDAR%ROWTYPE INDEX BY binary_integer;
TYPE tbl_array_u
IS
  TABLE OF dwh_performance.DIM_CALENDAR%ROWTYPE INDEX BY binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   integer := 0;
  a_count_i integer := 0;
  a_count_u integer := 0;

  CURSOR c_fnd_calendar
  IS
    SELECT * FROM fnd_calendar
    ;
  --**************************************************************************************************
  -- get the day name and short name
  --**************************************************************************************************
PROCEDURE get_day_name
AS
BEGIN
  IF g_rec_out.fin_day_no     = 1 THEN
    g_rec_out.day_name       := 'MONDAY';
    g_rec_out.day_short_name := 'MON';
  END IF;
  IF g_rec_out.fin_day_no     = 2 THEN
    g_rec_out.day_name       := 'TUESDAY';
    g_rec_out.day_short_name := 'TUE';
  END IF;
  IF g_rec_out.fin_day_no     = 3 THEN
    g_rec_out.day_name       := 'WEDNESDAY';
    g_rec_out.day_short_name := 'WED';
  END IF;
  IF g_rec_out.fin_day_no     = 4 THEN
    g_rec_out.day_name       := 'THURSDAY';
    g_rec_out.day_short_name := 'THU';
  END IF;
  IF g_rec_out.fin_day_no     = 5 THEN
    g_rec_out.day_name       := 'FRIDAY';
    g_rec_out.day_short_name := 'FRI';
  END IF;
  IF g_rec_out.fin_day_no     = 6 THEN
    g_rec_out.day_name       := 'SATURDAY';
    g_rec_out.day_short_name := 'SAT';
  END IF;
  IF g_rec_out.fin_day_no     = 7 THEN
    g_rec_out.day_name       := 'SUNDAY';
    g_rec_out.day_short_name := 'SUN';
  END IF;
  IF g_rec_out.day_name IS NULL THEN
    g_rec_out.day_name  := '-';
  END IF;
END get_day_name;
--**************************************************************************************************
-- get the day name and short name
--**************************************************************************************************
PROCEDURE get_month_name
AS
BEGIN
  IF g_rec_out.fin_month_no     = 1 THEN
    g_rec_out.month_name       := 'JULY';
    g_rec_out.month_short_name := 'JUL';
  END IF;
  IF g_rec_out.fin_month_no     = 2 THEN
    g_rec_out.month_name       := 'AUGUST';
    g_rec_out.month_short_name := 'AUG';
  END IF;
  IF g_rec_out.fin_month_no     = 3 THEN
    g_rec_out.month_name       := 'SEPTEMBER';
    g_rec_out.month_short_name := 'SEP';
  END IF;
  IF g_rec_out.fin_month_no     = 4 THEN
    g_rec_out.month_name       := 'OCTOBER';
    g_rec_out.month_short_name := 'OCT';
  END IF;
  IF g_rec_out.fin_month_no     = 5 THEN
    g_rec_out.month_name       := 'NOVEMBER';
    g_rec_out.month_short_name := 'NOV';
  END IF;
  IF g_rec_out.fin_month_no     = 6 THEN
    g_rec_out.month_name       := 'DECEMBER';
    g_rec_out.month_short_name := 'DEC';
  END IF;
  IF g_rec_out.fin_month_no     = 7 THEN
    g_rec_out.month_name       := 'JANUARY';
    g_rec_out.month_short_name := 'JAN';
  END IF;
  IF g_rec_out.fin_month_no     = 8 THEN
    g_rec_out.month_name       := 'FEBRUARY';
    g_rec_out.month_short_name := 'FEB';
  END IF;
  IF g_rec_out.fin_month_no     = 9 THEN
    g_rec_out.month_name       := 'MARCH';
    g_rec_out.month_short_name := 'MAR';
  END IF;
  IF g_rec_out.fin_month_no     = 10 THEN
    g_rec_out.month_name       := 'APRIL';
    g_rec_out.month_short_name := 'APR';
  END IF;
  IF g_rec_out.fin_month_no     = 11 THEN
    g_rec_out.month_name       := 'MAY';
    g_rec_out.month_short_name := 'MAY';
  END IF;
  IF g_rec_out.fin_month_no     = 12 THEN
    g_rec_out.month_name       := 'JUNE';
    g_rec_out.month_short_name := 'JUN';
  END IF;
END get_month_name;

--**************************************************************************************************
-- process, transform and validate the data read from the input interface
--**************************************************************************************************
PROCEDURE local_address_variable
AS
BEGIN
  g_rec_out.calendar_date     := g_rec_in.calendar_date;
  g_rec_out.fin_year_no       := g_rec_in.fin_year_no;
  g_rec_out.fin_month_no      := g_rec_in.fin_month_no;
  g_rec_out.fin_week_no       := g_rec_in.fin_week_no ;
  g_rec_out.fin_day_no        := g_rec_in.fin_day_no;
  g_rec_out.cal_year_no       := g_rec_in.cal_year_no;

  g_rec_out.last_updated_date := g_date;
  g_rec_out.cal_year_month_no := g_rec_in.cal_year_no|| to_char(g_rec_in.calendar_date,('mm'));
  g_rec_out.fin_quarter_no    := (g_rec_out.fin_month_no + 1) / 3;

  CASE g_rec_out.fin_quarter_no
  WHEN 1 THEN
    g_rec_out.fin_half_no := 1;
    g_rec_out.season_no   := 1;
    g_rec_out.season_name := 'SPRING';
  WHEN 2 THEN
    g_rec_out.fin_half_no := 1;
    g_rec_out.season_no   := 2;
    g_rec_out.season_name := 'SUMMER';
  WHEN 3 THEN
    g_rec_out.fin_half_no := 2;
    g_rec_out.season_no   := 3;
    g_rec_out.season_name := 'AUTUMN';
  WHEN 4 THEN
    g_rec_out.fin_half_no := 2;
    g_rec_out.season_no   := 4;
    g_rec_out.season_name := 'WINTER';
  END CASE;

  get_day_name;
  get_month_name;

  g_rec_out.ly_fin_year_no := g_rec_out.fin_year_no - 1;
  g_weeks_per_year         := 52;

  g_select_error           := 'Err 1 - fin_year_no = ly_fin_year_no';
  SELECT max(fin_week_no)
  INTO g_weeks_per_year
  FROM fnd_calendar
  WHERE fin_year_no           = g_rec_out.ly_fin_year_no;
  IF g_weeks_per_year         = 53 THEN
    g_rec_out.ly_fin_week_no := g_rec_out.fin_week_no + 1;
  ELSE
    g_rec_out.ly_fin_week_no   := g_rec_out.fin_week_no;
    IF g_rec_out.ly_fin_week_no = 53 THEN
      g_rec_out.ly_fin_week_no := 1;
      g_rec_out.ly_fin_year_no := g_rec_out.fin_year_no;
    END IF;
  END IF;
  g_rec_out.fin_quarter_no := (g_rec_out.fin_month_no + 1) / 3;

  g_select_error           := 'Err 2 - Current week day 1';
  SELECT calendar_date
  INTO g_rec_out.this_week_start_date
  FROM fnd_calendar
  WHERE fin_year_no = g_rec_out.fin_year_no
  AND fin_week_no   = g_rec_out.fin_week_no
  AND fin_day_no    = 1;

  g_select_error   := 'Err 3 - Current week day 7';
  SELECT calendar_date
  INTO g_rec_out.this_week_end_date
  FROM fnd_calendar
  WHERE fin_year_no = g_rec_out.fin_year_no
  AND fin_week_no   = g_rec_out.fin_week_no
  AND fin_day_no    = 7;

  g_select_error   := 'Err 4 - Month start';
  SELECT min(calendar_date)
  INTO g_rec_out.this_mn_start_date
  FROM fnd_calendar
  WHERE fin_year_no = g_rec_out.fin_year_no
  AND fin_month_no  = g_rec_out.fin_month_no;

  g_select_error   := 'Err 5 - Month end';
  SELECT max(calendar_date)
  INTO g_rec_out.this_mn_end_date
  FROM fnd_calendar
  WHERE fin_year_no = g_rec_out.fin_year_no
  AND fin_month_no  = g_rec_out.fin_month_no;

  g_select_error   := 'Err 55 - Other';
  ---------------------------------------------------------
  -- added for olap purposes
  ---------------------------------------------------------
  g_rec_out.fin_day_short_desc            := 'DAY'||g_rec_out.fin_day_no;
  g_rec_out.fin_week_day_long_desc        := 'WEEK '||g_rec_out.fin_week_no||' / DAY '||g_rec_out.fin_day_no;
  g_rec_out.num_fin_day_timespan_days     := 1;
  g_rec_out.fin_day_end_date              := g_rec_out.calendar_date;
  g_rec_out.fin_week_code                 := 'W'||g_rec_out.fin_year_no||g_rec_out.fin_week_no;
  g_rec_out.fin_week_short_desc           := 'WEEK '||g_rec_out.fin_week_no;
  g_rec_out.fin_week_long_desc            := 'WEEK '||g_rec_out.fin_week_no||' '||substr(to_char(g_rec_out.fin_year_no),3,2);
  g_rec_out.num_fin_week_timespan_days    := 7;
  g_rec_out.fin_week_end_date             := g_rec_out.this_week_end_date;
  g_rec_out.fin_month_code                := 'M'||g_rec_out.fin_year_no||g_rec_out.fin_month_no;
  g_rec_out.fin_month_short_desc          := upper(g_rec_out.month_short_name||' '||substr(to_char(g_rec_out.fin_year_no),3,2));
  g_rec_out.fin_month_long_desc           := upper(g_rec_out.month_name||' '||substr(to_char(g_rec_out.fin_year_no),3,2));
  g_rec_out.num_fin_month_timespan_days   := 28;

  IF g_rec_out.fin_month_no                = 2 OR g_rec_out.fin_month_no = 5 OR g_rec_out.fin_month_no = 8 OR g_rec_out.fin_month_no = 11 THEN
    g_rec_out.num_fin_month_timespan_days := 35;
  END IF;

  g_rec_out.fin_month_end_date            := g_rec_out.this_mn_end_date;
  g_rec_out.fin_quarter_code              := 'Q'||g_rec_out.fin_year_no||g_rec_out.fin_quarter_no;
  g_rec_out.fin_quarter_short_desc        := 'QTR '||g_rec_out.fin_quarter_no||' '||substr(to_char(g_rec_out.fin_year_no),3,2);
  g_rec_out.fin_quarter_long_desc         := 'QUARTER '||g_rec_out.fin_quarter_no||' '||substr(to_char(g_rec_out.fin_year_no),3,2);

  g_rec_out.num_fin_quarter_timespan_days := 91;

  CASE g_rec_out.fin_quarter_no
  WHEN 1 THEN
    g_select_error := 'Err 6 - Qtr end';
    SELECT max(calendar_date)
    INTO g_rec_out.fin_quarter_end_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 3;
    g_select_error   := 'Err 7 - Qtr start';
    SELECT min(calendar_date)
    INTO g_rec_out.season_start_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 1;
  WHEN 2 THEN
    g_select_error := 'Err 8 - Qtr end';
    SELECT max(calendar_date)
    INTO g_rec_out.fin_quarter_end_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 6;
    g_select_error   := 'Err 9 - Qtr start';
    SELECT min(calendar_date)
    INTO g_rec_out.season_start_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 4;
  WHEN 3 THEN
    g_select_error := 'Err 10 - Qtr end';
    SELECT max(calendar_date)
    INTO g_rec_out.fin_quarter_end_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 9;
    g_select_error   := 'Err 11 - Qtr start';
    SELECT min(calendar_date)
    INTO g_rec_out.season_start_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 7;
  WHEN 4 THEN
    g_select_error := 'Err 12 - Qtr end';
    SELECT max(calendar_date)
    INTO g_rec_out.fin_quarter_end_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 12;
    g_select_error   := 'Err 13 - Qtr start';
    SELECT min(calendar_date)
    INTO g_rec_out.season_start_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 10;
  END CASE;

  g_select_error                       := 'Err 133 - Other';
  g_rec_out.fin_half_code              := 'H'||g_rec_out.fin_year_no||g_rec_out.fin_half_no;
  g_rec_out.fin_half_short_desc        := 'HALF '||g_rec_out.fin_half_no||' '||substr(to_char(g_rec_out.fin_year_no),3,2);
  g_rec_out.fin_half_long_desc         := 'HALF '||g_rec_out.fin_half_no||' '||substr(to_char(g_rec_out.fin_year_no),3,2);
  g_rec_out.num_fin_half_timespan_days := 182;

  CASE g_rec_out.fin_half_no
  WHEN 1 THEN
    g_select_error := 'Err 14 - Half end';
    SELECT max(calendar_date)
    INTO g_rec_out.fin_half_end_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 6;
  WHEN 2 THEN
    g_select_error := 'Err 15 - Half end';
    SELECT max(calendar_date)
    INTO g_rec_out.fin_half_end_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.fin_year_no
    AND fin_month_no  = 12;
  END CASE;

  g_rec_out.fin_year_code              := 'Y'||g_rec_out.fin_year_no;
  g_rec_out.fin_year_short_desc        := g_rec_out.fin_year_no;
  g_rec_out.fin_year_long_desc         := 'YEAR '||substr(to_char(g_rec_out.fin_year_no),3,2);
  g_rec_out.num_fin_year_timespan_days := 364;

  g_select_error                       := 'Err 16 - Year end';
  SELECT max(calendar_date)
  INTO g_rec_out.fin_year_end_date
  FROM fnd_calendar
  WHERE fin_year_no            = g_rec_out.fin_year_no
  AND fin_month_no             = 12;

  g_select_error              := 'Err 166 - Other';
  g_rec_out.season_code       := 'S'||g_rec_out.fin_year_no||g_rec_out.season_no;
  g_rec_out.season_short_desc := upper(substr(g_rec_out.season_name,1,3)||' '||substr(to_char(g_rec_out.fin_year_no),3,2));
  g_rec_out.season_long_desc  := g_rec_out.season_name||' '||substr(to_char(g_rec_out.fin_year_no),3,2);
  --  g_rec_out.cal_month_short_desc := upper(to_char(g_rec_out.calendar_date, 'mon') || ' ' ||substr(to_char(g_rec_out.fin_year_no),3,2));
  g_rec_out.cal_month_short_desc := upper(g_rec_out.month_short_name|| ' ' ||substr(to_char(g_rec_out.fin_year_no),3,2));
  IF g_rec_out.fin_month_no       < 7 THEN
    g_rec_out.season_short_desc  := upper(substr(g_rec_out.season_name,1,3)||' '||substr(to_char(g_rec_out.fin_year_no - 1),3,2));
    g_rec_out.season_long_desc   := upper(g_rec_out.season_name||' '||substr(to_char(g_rec_out.fin_year_no             - 1),3,2));
    --     g_rec_out.cal_month_short_desc := upper(to_char(g_rec_out.calendar_date, 'mon') || ' ' ||substr(to_char(g_rec_out.fin_year_no - 1),3,2));
    g_rec_out.cal_month_short_desc := upper(g_rec_out.month_short_name || ' ' ||substr(to_char(g_rec_out.fin_year_no - 1),3,2));
  END IF;

  g_rec_out.num_season_timespan_days := 91;
  g_rec_out.season_end_date          := g_rec_out.fin_quarter_end_date;
  g_rec_out.total                    := 'TOTAL';
  g_rec_out.total_short_desc         := 'ALL TIME';
  g_rec_out.total_long_desc          := 'ALL TIME';

  g_select_error                     := 'Err 17 - Num Total timespan days';
  SELECT count(*)
  INTO g_rec_out.num_total_timespan_days
  FROM fnd_calendar
  WHERE fin_year_no > 2004;
  --   g_rec_out.total_end_date             := g_rec_out.fin_year_end_date;

  g_select_error := 'Err 17 - Max date';
  SELECT max(calendar_date) INTO g_rec_out.total_end_date FROM fnd_calendar;

  g_select_error             := 'Err 18 - LY calendar date';
  IF g_rec_out.ly_fin_year_no > 2000 THEN
    SELECT calendar_date
    INTO g_rec_out.ly_calendar_date
    FROM fnd_calendar
    WHERE fin_year_no = g_rec_out.ly_fin_year_no
    AND fin_week_no   = g_rec_out.ly_fin_week_no
    AND fin_day_no    = g_rec_out.fin_day_no;
  END IF;

  g_select_error             := 'Err 19 - Other';
  g_rec_out.ly_fin_week_code := 'W'||g_rec_out.ly_fin_year_no||g_rec_out.ly_fin_week_no;
  g_rec_out.order_by_seq_no  := 0;
  --
  -- fudge for 53 week year
  -- i know, i know this is unnecessary processing per calendar_day
  -- but i am doing it this way
  --
  SELECT count(DISTINCT fin_week_no)
  INTO g_no_of_weeks
  FROM FND_calendar
  WHERE fin_year_no                          = g_rec_out.fin_year_no;

  IF g_no_of_weeks                           = 53 AND g_rec_out.season_no = 4 THEN
    g_rec_out.num_season_timespan_days      := 98;
    g_rec_out.num_fin_quarter_timespan_days := 98;
  END IF;

  IF g_no_of_weeks                        = 53 AND g_rec_out.fin_half_no = 2 THEN
    g_rec_out.num_fin_half_timespan_days := 189;
  END IF ;

  IF g_no_of_weeks                        = 53 THEN
    g_rec_out.num_fin_year_timespan_days := 371;
  END IF ;

  IF g_rec_out.fin_half_no                = 1 THEN
    g_rec_out.fin_half_season_long_desc  := 'SPRINGSUMMER '||substr(to_char(g_rec_out.fin_year_no - 1),3,2);
    g_rec_out.fin_half_season_short_desc := 'SPRSUM '||substr(to_char(g_rec_out.fin_year_no       - 1),3,2);
  ELSE
    g_rec_out.fin_half_season_long_desc  := 'AUTUMNWINTER '||substr(to_char(g_rec_out.fin_year_no),3,2);
    g_rec_out.fin_half_season_short_desc := 'AUTWIN '||substr(to_char(g_rec_out.fin_year_no),3,2);
  END IF;
  --
  --  if g_rec_out.fin_year_no                  in (2008,2013) and g_rec_out.season_no = 4 then
  --    g_rec_out.num_season_timespan_days      := 98;
  --    g_rec_out.num_fin_quarter_timespan_days := 98;
  --  end if;
  --  if g_rec_out.fin_year_no               in (2008,2013) and g_rec_out.fin_half_no = 2 then
  --    g_rec_out.num_fin_half_timespan_days := 189;
  --  end if ;
  --  if g_rec_out.fin_year_no               in (2008,2013) then
  --    g_rec_out.num_fin_year_timespan_days := 371;
  --  end if ;
  --  if g_rec_out.fin_half_no                = 1 then
  --    g_rec_out.fin_half_season_long_desc  := 'springsummer '||substr(to_char(g_rec_out.fin_year_no - 1),3,2);
  --    g_rec_out.fin_half_season_short_desc := 'sprsum '||substr(to_char(g_rec_out.fin_year_no       - 1),3,2);
  --  else
  --    g_rec_out.fin_half_season_long_desc  := 'autumnwinter '||substr(to_char(g_rec_out.fin_year_no),3,2);
  --    g_rec_out.fin_half_season_short_desc := 'autwin '||substr(to_char(g_rec_out.fin_year_no),3,2);
  --  end if;

  g_rec_out.completed_fin_day_ind     := 0;
  g_rec_out.completed_fin_week_ind    := 0;
  g_rec_out.completed_fin_month_ind   := 0;
  g_rec_out.completed_fin_quarter_ind := 0;
  g_rec_out.completed_fin_half_ind    := 0;
  g_rec_out.completed_fin_year_ind    := 0;
  g_rec_out.completed_cal_year_ind    := 0;
  g_rec_out.completed_season_ind      := 0;

  --   if g_rec_out.calendar_date < g_date then
  --      g_rec_out.completed_fin_day_ind            := 1;
  --   end if;

  IF g_rec_out.calendar_date        <= g_date THEN
    g_rec_out.completed_fin_day_ind := 1;
  END IF;

  --  if g_rec_out.this_week_end_date < g_date then
  --     g_rec_out.completed_fin_week_ind           := 1;
  --  end if;
  --  if g_rec_out.this_mn_end_date < g_date then
  --      g_rec_out.completed_fin_month_ind          := 1;
  --   end if;
  --   if g_rec_out.fin_quarter_end_date < g_date then
  --      g_rec_out.completed_fin_quarter_ind        := 1;
  --   end if;
  --   if g_rec_out.fin_half_end_date < g_date then
  --      g_rec_out.completed_fin_half_ind           := 1;
  --   end if;
  --  if g_rec_out.fin_year_end_date < g_date then
  --     g_rec_out.completed_fin_year_ind           := 1;
  --  end if;
  --  if g_rec_out.season_end_date < g_date then
  --     g_rec_out.completed_season_ind             := 1;
  --   end if;

  g_cal_year_end_date := '31 dec '||to_char(g_rec_out.cal_year_no) ;

  --   if g_cal_year_end_date < g_date then
  --      g_rec_out.completed_cal_year_ind             := 1;
  --   end if;
  ---
  IF g_rec_out.this_week_end_date    <= g_date THEN
    g_rec_out.completed_fin_week_ind := 1;
  END IF;
  IF g_rec_out.this_mn_end_date       <= g_date THEN
    g_rec_out.completed_fin_month_ind := 1;
  END IF;
  IF g_rec_out.fin_quarter_end_date     <= g_date THEN
    g_rec_out.completed_fin_quarter_ind := 1;
  END IF;
  IF g_rec_out.fin_half_end_date     <= g_date THEN
    g_rec_out.completed_fin_half_ind := 1;
  END IF;
  IF g_rec_out.fin_year_end_date     <= g_date THEN
    g_rec_out.completed_fin_year_ind := 1;
  END IF;
  IF g_rec_out.season_end_date     <= g_date THEN
    g_rec_out.completed_season_ind := 1;
  END IF;
  IF g_cal_year_end_date             <= g_date THEN
    g_rec_out.completed_cal_year_ind := 1;
  END IF;

EXCEPTION
WHEN others THEN
  l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm||' '||g_select_error||' '|| g_rec_out.fin_year_no||g_rec_out.fin_month_no||g_rec_out.fin_day_no;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  RAISE;
END local_address_variable;
--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
PROCEDURE local_bulk_insert
AS
BEGIN

  FORALL i IN a_tbl_insert.first .. a_tbl_insert.last
  SAVE EXCEPTIONS
  INSERT INTO dwh_performance.DIM_CALENDAR VALUES a_tbl_insert    (i    );
  g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

EXCEPTION
WHEN others THEN
  g_error_count := SQL%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := SQL%bulk_exceptions    (      i    )    .error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-SQL%bulk_exceptions(i).error_code)|| ' '||a_tbl_insert(g_error_index).calendar_date;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
  END LOOP;
  RAISE;
END local_bulk_insert;
--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
PROCEDURE local_bulk_update
AS
BEGIN
  FORALL i IN a_tbl_update.first .. a_tbl_update.last
  SAVE EXCEPTIONS
  UPDATE dwh_performance.DIM_CALENDAR
  SET fin_year_no                 = a_tbl_update(i).fin_year_no,
    fin_month_no                  = a_tbl_update(i).fin_month_no,
    fin_week_no                   = a_tbl_update(i).fin_week_no,
    fin_day_no                    = a_tbl_update(i).fin_day_no,
    cal_year_no                   = a_tbl_update(i).cal_year_no,
    fin_half_no                   = a_tbl_update(i).fin_half_no,
    fin_quarter_no                = a_tbl_update(i).fin_quarter_no,
    ly_fin_year_no                = a_tbl_update(i).ly_fin_year_no,
    ly_fin_week_no                = a_tbl_update(i).ly_fin_week_no,
    cal_year_month_no             = a_tbl_update(i).cal_year_month_no,
    this_mn_start_date            = a_tbl_update(i).this_mn_start_date,
    this_mn_end_date              = a_tbl_update(i).this_mn_end_date,
    this_week_start_date          = a_tbl_update(i).this_week_start_date,
    this_week_end_date            = a_tbl_update(i).this_week_end_date,
    season_no                     = a_tbl_update(i).season_no,
    season_name                   = a_tbl_update(i).season_name,
    month_name                    = a_tbl_update(i).month_name,
    month_short_name              = a_tbl_update(i).month_short_name,
    day_name                      = a_tbl_update(i).day_name,
    day_short_name                = a_tbl_update(i).day_short_name,
    fin_day_short_desc            = a_tbl_update(i).fin_day_short_desc,
    fin_week_day_long_desc        = a_tbl_update(i).fin_week_day_long_desc,
    num_fin_day_timespan_days     = a_tbl_update(i).num_fin_day_timespan_days,
    fin_day_end_date              = a_tbl_update(i).fin_day_end_date,
    fin_week_code                 = a_tbl_update(i).fin_week_code,
    fin_week_short_desc           = a_tbl_update(i).fin_week_short_desc,
    fin_week_long_desc            = a_tbl_update(i).fin_week_long_desc,
    num_fin_week_timespan_days    = a_tbl_update(i).num_fin_week_timespan_days,
    fin_week_end_date             = a_tbl_update(i).fin_week_end_date,
    fin_month_code                = a_tbl_update(i).fin_month_code,
    fin_month_short_desc          = a_tbl_update(i).fin_month_short_desc,
    fin_month_long_desc           = a_tbl_update(i).fin_month_long_desc,
    num_fin_month_timespan_days   = a_tbl_update(i).num_fin_month_timespan_days,
    fin_month_end_date            = a_tbl_update(i).fin_month_end_date,
    fin_quarter_code              = a_tbl_update(i).fin_quarter_code,
    fin_quarter_short_desc        = a_tbl_update(i).fin_quarter_short_desc,
    fin_quarter_long_desc         = a_tbl_update(i).fin_quarter_long_desc,
    num_fin_quarter_timespan_days = a_tbl_update(i).num_fin_quarter_timespan_days,
    fin_quarter_end_date          = a_tbl_update(i).fin_quarter_end_date,
    fin_half_code                 = a_tbl_update(i).fin_half_code,
    fin_half_short_desc           = a_tbl_update(i).fin_half_short_desc,
    fin_half_long_desc            = a_tbl_update(i).fin_half_long_desc,
    num_fin_half_timespan_days    = a_tbl_update(i).num_fin_half_timespan_days,
    fin_half_end_date             = a_tbl_update(i).fin_half_end_date,
    fin_year_code                 = a_tbl_update(i).fin_year_code,
    fin_year_short_desc           = a_tbl_update(i).fin_year_short_desc,
    fin_year_long_desc            = a_tbl_update(i).fin_year_long_desc,
    num_fin_year_timespan_days    = a_tbl_update(i).num_fin_year_timespan_days,
    fin_year_end_date             = a_tbl_update(i).fin_year_end_date,
    season_code                   = a_tbl_update(i).season_code,
    season_short_desc             = a_tbl_update(i).season_short_desc,
    season_long_desc              = a_tbl_update(i).season_long_desc,
    num_season_timespan_days      = a_tbl_update(i).num_season_timespan_days,
    season_end_date               = a_tbl_update(i).season_end_date,
    total                         = a_tbl_update(i).total,
    total_short_desc              = a_tbl_update(i).total_short_desc,
    total_long_desc               = a_tbl_update(i).total_long_desc,
    num_total_timespan_days       = a_tbl_update(i).num_total_timespan_days,
    total_end_date                = a_tbl_update(i).total_end_date,
    ly_calendar_date              = a_tbl_update(i).ly_calendar_date,
    ly_fin_week_code              = a_tbl_update(i).ly_fin_week_code,
    order_by_seq_no               = a_tbl_update(i).order_by_seq_no,
    last_updated_date             = a_tbl_update(i).last_updated_date,
    season_start_date             = a_tbl_update(i).season_start_date,
    fin_half_season_long_desc     = a_tbl_update(i).fin_half_season_long_desc,
    completed_fin_day_ind         = a_tbl_update(i).completed_fin_day_ind,
    completed_fin_week_ind        = a_tbl_update(i).completed_fin_week_ind,
    completed_fin_month_ind       = a_tbl_update(i).completed_fin_month_ind,
    completed_fin_quarter_ind     = a_tbl_update(i).completed_fin_quarter_ind,
    completed_fin_half_ind        = a_tbl_update(i).completed_fin_half_ind,
    completed_fin_year_ind        = a_tbl_update(i).completed_fin_year_ind,
    completed_cal_year_ind        = a_tbl_update(i).completed_cal_year_ind,
    completed_season_ind          = a_tbl_update(i).completed_season_ind,
    cal_month_short_desc          = a_tbl_update(i).cal_month_short_desc,
    fin_half_season_short_desc    = a_tbl_update(i).fin_half_season_short_desc
  WHERE calendar_date             = a_tbl_update(i).calendar_date ;
  g_recs_updated                 := g_recs_updated + a_tbl_update.count;

EXCEPTION
WHEN others THEN
  g_error_count := SQL%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  FOR i IN 1 .. g_error_count
  LOOP
    g_error_index := SQL%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-SQL%bulk_exceptions(i).error_code)|| ' '||a_tbl_update(g_error_index).calendar_date;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
  END LOOP;
  RAISE;
END local_bulk_update;
--**************************************************************************************************
-- write valid data out to the item master table
--**************************************************************************************************
PROCEDURE local_write_output
AS
BEGIN
  g_found := dwh_valid.dim_calendar(g_rec_out.calendar_date);

  -- check if insert of item already in insert array and change to put duplicate in update array
  IF a_count_i > 0 AND NOT g_found THEN
    FOR i     IN a_tbl_insert.first .. a_tbl_insert.last
    LOOP
      IF a_tbl_insert(i).calendar_date = g_rec_out.calendar_date THEN
        g_found                       := TRUE;
      END IF;
    END LOOP;
  END IF;

  -- place record into array for later bulk writing
  IF NOT g_found THEN
    a_count_i               := a_count_i + 1;
    a_tbl_insert(a_count_i) := g_rec_out;
  ELSE
    a_count_u               := a_count_u + 1;
    a_tbl_update(a_count_u) := g_rec_out;
  END IF;

  a_count := a_count + 1;

  --**************************************************************************************************
  -- bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  IF a_count > g_forall_limit THEN
    local_bulk_insert;
    local_bulk_update;
    a_tbl_insert := a_empty_set_i;
    a_tbl_update := a_empty_set_u;
    a_count_i    := 0;
    a_count_u    := 0;
    a_count      := 0;
    COMMIT;
  END IF;

EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  RAISE;
WHEN others THEN
  l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  RAISE;
END local_write_output;

--**************************************************************************************************
-- if new fin_year loaded then updtae public_holidays
--**************************************************************************************************
PROCEDURE load_new_public_holidays
AS
BEGIN

    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_IND = 0
    WHERE RSA_PUBLIC_HOLIDAY_IND IS NULL;
    COMMIT;

    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_IND = 1
    where TO_CHAR(calendar_date, 'dd/mm') = '01/01'
    or TO_CHAR(calendar_date, 'dd/mm') = '21/03'
    or TO_CHAR(calendar_date, 'dd/mm') = '27/04'
    or TO_CHAR(calendar_date, 'dd/mm') = '01/05'
    or TO_CHAR(calendar_date, 'dd/mm') = '16/06'
    or TO_CHAR(calendar_date, 'dd/mm') = '09/08'
    or TO_CHAR(calendar_date, 'dd/mm') = '24/09'
    or TO_CHAR(calendar_date, 'dd/mm') = '16/12'
    or TO_CHAR(calendar_date, 'dd/mm') = '25/12'
    or TO_CHAR(calendar_date, 'dd/mm') = '26/12';
    ---
    --- Sunday public holdays
    ---
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_IND = 2
    where RSA_PUBLIC_HOLIDAY_IND = 1
    and UPPER(to_CHAR(calendar_date, 'dy')) = 'SUN'
    ;
    commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_IND = 1
    where calendar_date in(select calendar_date+1 from dwh_performance.DIM_CALENDAR where RSA_PUBLIC_HOLIDAY_IND = 2)
    ;
    commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_IND = 1
    where RSA_PUBLIC_HOLIDAY_IND = 2;
    commit;
    ---
    --- Hardcoded public holidays for Easter
    ---
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_IND = 1
    where calendar_date in (
    '22 apr 2011', '25 apr 2011',
    '6 apr 2012', '9 apr 2012',
    '29 mar 2013', '1 apr 2013',
    '18 apr 2014', '21 apr 2014',
    '3 apr 2015', '6 apr 2015',
    '25 mar 2016', '28 mar 2016',
    '14 apr 2017', '17 apr 2017',
    '30 mar 2018', '2 apr 2018',
    '19 apr 2019', '22 apr 2019',
    '10 apr 2020', '13 apr 2020',
    '1 apr 2021', '4 apr 2021',
    '15 apr 2022', '18 apr 2022',
    '7 apr 2023', '10 apr 2023',
    '29 apr 2024', '1 apr 2024',
    '18 apr 2025', '21 apr 2025'
    );
    commit;

    ---
    --- Add RAS_PUBLIC_HOLIDAY descriptions
    ---
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'NEW YEARS DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '01/01';
       commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'HUMAN RIGHTS DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '21/03';
       commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'FREEDOM DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '27/04';
       commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'WORKERS DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '01/05';
       commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'YOUTH DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '16/06';
       commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'NATIONAL WOMENS DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '09/08';
       commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'HERITAGE DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '24/09';
       commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'RECONCILIATION DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '16/12';
    commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'CHRISTMAS DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '25/12';
       commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'GOODWILL DAY'
    where TO_CHAR(calendar_date, 'dd/mm') = '26/12';
       commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'GOOD FRIDAY'
    where calendar_date in (
    '22 apr 2011',
    '6 apr 2012',
    '29 mar 2013',
    '18 apr 2014',
    '3 apr 2015',
    '25 mar 2016',
    '14 apr 2017',
    '30 mar 2018',
    '19 apr 2019',
    '10 apr 2020',
    '1 apr 2021',
    '15 apr 2022',
    '7 apr 2023',
    '29 apr 2024',
    '18 apr 2025'
    );
    commit;
    update dwh_performance.DIM_CALENDAR
    set RSA_PUBLIC_HOLIDAY_desc = 'EASTER FAMILY DAY'
    where calendar_date in (
     '25 apr 2011',
     '9 apr 2012',
     '1 apr 2013',
     '21 apr 2014',
     '6 apr 2015',
     '28 mar 2016',
     '17 apr 2017',
     '2 apr 2018',
     '22 apr 2019',
     '13 apr 2020',
     '4 apr 2021',
     '18 apr 2022',
     '10 apr 2023',
     '1 apr 2024',
     '21 apr 2025'
    );
    commit;


EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  RAISE;
WHEN others THEN
  l_message := dwh_constants.vc_err_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  RAISE;
END load_new_public_holidays;

--**************************************************************************************************
-- main process loop
--**************************************************************************************************
BEGIN

  p_success         := FALSE;

  IF p_forall_limit IS NOT NULL AND p_forall_limit > 1000 THEN
    g_forall_limit  := p_forall_limit;
  END IF;

  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF dwh_performance.DIM_CALENDAR EX FND_CALENDAR STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  --   g_date := '16-jul-09';
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- check current financial-year vs those on fnd_calendar
  --**************************************************************************************************
  SELECT fin_year_no
  INTO g_current_fin_year
  FROM fnd_calendar
  WHERE calendar_date = g_date;
  l_text             := '** Current Fin_year in FND_CALENDAR = '||g_current_fin_year;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- check maximum financial-year on fnd_calendar
  --**************************************************************************************************
  SELECT max(fin_year_no),
    max(calendar_date)
  INTO g_max_fin_year,
    g_max_calendar_date
  FROM fnd_calendar;
  l_text := '** Maximum Fin_year and date in FND_CALENDAR = '||g_max_fin_year||'  -  '||g_max_calendar_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  --**************************************************************************************************
  OPEN c_fnd_calendar;
  FETCH c_fnd_calendar BULK COLLECT INTO a_stg_input LIMIT g_forall_limit;
  WHILE a_stg_input.count > 0
  LOOP
    FOR i IN 1 .. a_stg_input.count
    LOOP

      g_recs_read            := g_recs_read + 1;

      IF g_recs_read mod 1000 = 0 THEN
        l_text               := dwh_constants.vc_log_records_processed|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      END IF;

      g_rec_in := a_stg_input(i);

      local_address_variable;

      local_write_output;

    END LOOP;

    FETCH c_fnd_calendar BULK COLLECT INTO a_stg_input LIMIT g_forall_limit;
  END LOOP;
  CLOSE c_fnd_calendar;
  --**************************************************************************************************
  -- at end write out what remains in the arrays
  --**************************************************************************************************
  local_bulk_insert;
  local_bulk_update;

  --**************************************************************************************************
  -- If new fin_year has been loaded then load the public_holodays
  --**************************************************************************************************
    g_today_cal_month_no   := to_number(to_char(g_date,'mm') );

  IF g_today_cal_month_no = 6 THEN
    g_today_fin_year_no  := to_number(to_char(g_date,'yyyy') );

    SELECT DISTINCT fin_year_no
    INTO g_load_new_fin_year
    FROM dwh_performance.DIM_CALENDAR
    WHERE fin_year_no      = g_today_fin_year_no + 3;
  IF g_load_new_fin_year > 0
  OR g_load_new_fin_year IS NOT NULL THEN
      load_new_public_holidays;
  end if;
  end if;

  --**************************************************************************************************
  -- check current financial-year vs those on dwh_performance.DIM_CALENDAR
  --**************************************************************************************************
  g_current_fin_year := 0;

  SELECT fin_year_no
  INTO g_current_fin_year
  FROM dwh_performance.DIM_CALENDAR
  WHERE calendar_date = g_date;

  l_text             := '** Current Fin_year in dwh_performance.DIM_CALENDAR = '||g_current_fin_year;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- check maximum financial-year on dwh_performance.DIM_CALENDAR
  --**************************************************************************************************
  g_min_fin_year      := 0;
  g_max_fin_year      := 0;
  g_max_calendar_date := NULL;

  SELECT max(fin_year_no),
    max(calendar_date),
    min(fin_year_no)
  INTO g_max_fin_year,
    g_max_calendar_date,
    g_min_fin_year
  FROM dwh_performance.DIM_CALENDAR;

  l_text := '** Maximum Fin_year and date in dwh_performance.DIM_CALENDAR = '||g_max_fin_year||'  -  '||g_max_calendar_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- count no. of weeks and days per fin_year on dwh_performance.DIM_CALENDAR
  --**************************************************************************************************
  FOR i IN g_min_fin_year..g_max_fin_year
  LOOP

    g_min_calendar_date := NULL;
    g_max_calendar_date := NULL;
    g_no_of_days        := 0;
    g_no_of_weeks       := 0;

    SELECT min(calendar_date) ,
      max(calendar_date),
      count(*),
      count(DISTINCT fin_week_no),
      max(fin_year_no)
    INTO g_min_calendar_date,
      g_max_calendar_date,
      g_no_of_days,
      g_no_of_weeks,
      g_fin_year_no
    FROM dwh_performance.DIM_CALENDAR
    WHERE fin_year_no = i;

    l_text           := '** Fin_year  = '||g_fin_year_no||' : from='||g_min_calendar_date||' to '||g_max_calendar_date||' : no.of.days='||g_no_of_days||' : no.of.wks='||g_no_of_weeks;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  END LOOP;
  --**************************************************************************************************
  -- at end write out log totals
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  COMMIT;
  p_success := TRUE;
EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := FALSE;
  RAISE;
WHEN others THEN
  l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  ROLLBACK;
  p_success := FALSE;
  RAISE;
END wh_prf_corp_006U_TSTWL;
