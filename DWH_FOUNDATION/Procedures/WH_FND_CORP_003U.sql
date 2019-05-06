--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_003U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_003U" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Create calendar dimention table in the foundation layer
--               with input ex staging table from Walker.
----------------------------------------------------------------------------
--  **** nb. this procedure must still run every day as it generates
--           the next financial year  to be added to dwh_foundation.fnd_calendar every June
--       nb. any fin_year with 53 weeks will have to be checked and manually added
--           to dwh_foundation.fnd_calendar
----------------------------------------------------------------------------
--  Tables:      Input  - stg_walker_calendar_cpy
--               Output - fnd_caledar
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  13 june 2011 - defect 4354 - FIN YR-END 2011 - changes to generation of dwh_performance.dim_calendar
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
g_hospital           char(1)       := 'N';
g_hospital_text      stg_walker_calendar_hsp.sys_process_msg%type;
g_rec_out            dwh_foundation.fnd_calendar%rowtype;
g_rec_in             stg_walker_calendar_cpy%rowtype;
g_found              boolean;
g_insert_rec         boolean;
g_invalid_fin_year   boolean;
g_invalid_fin_month  boolean;
g_invalid_fin_week   boolean;
g_invalid_fin_day    boolean;
g_invalid_cal_year   boolean;

  g_today_cal_month_no      integer := 0;
  g_today_fin_year_no      integer := 0;
  g_today_fin_year_no_3      integer := 0;
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

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_003U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CALENDAR MASTERDATA EX WALKER';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
-- For input bulk collect --
type stg_array is table of stg_walker_calendar_cpy%rowtype;
a_stg_input      stg_array;
-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_foundation.fnd_calendar%rowtype index by binary_integer;
type tbl_array_u is table of dwh_foundation.fnd_calendar%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_walker_calendar_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_walker_calendar_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;
a_count_stg         integer       := 0;
cursor c_stg_walker_calendar is
   select *
   from stg_walker_calendar_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;
-- order by only where sequencing is essential to the correct loading of data
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
   g_hospital                    := 'N';
   g_rec_out.calendar_date             := g_rec_in.calendar_date;
   g_rec_out.fin_year_no               := g_rec_in.fin_year_no;
   g_rec_out.fin_month_no              := g_rec_in.fin_month_no;
   g_rec_out.fin_week_no               := g_rec_in.fin_week_no ;
   g_rec_out.fin_day_no                := g_rec_in.fin_day_no;
   g_rec_out.cal_year_no               := g_rec_in.cal_year_no;
   g_rec_out.last_updated_date         := g_date;
   dwh_valid.fnd_calendar_fields(g_rec_out.calendar_date,
                                     g_rec_out.fin_year_no,
                                     g_rec_out.fin_month_no,
                                     g_rec_out.fin_week_no,
                                     g_rec_out.fin_day_no,
                                     g_rec_out.cal_year_no,
                                     g_invalid_fin_year,
                                     g_invalid_fin_month,
                                     g_invalid_fin_week,
                                     g_invalid_fin_day,
                                     g_invalid_cal_year);
   if g_invalid_cal_year or g_invalid_fin_year then
     g_hospital      := 'Y';
     g_hospital_text := ' INVALID YEAR';
     l_text          :=  g_rec_out.fin_year_no||' '||g_rec_out.cal_year_no||
                         g_hospital_text ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;
      if g_invalid_fin_month or g_invalid_fin_week or g_invalid_fin_day then
     g_hospital      := 'Y';
     g_hospital_text := ' INVALID FIN MONTH, WEEK OR DAY RANGE';
     l_text          :=  g_rec_out.fin_month_no||' '||g_rec_out.fin_week_no||' '||
                         g_rec_out.fin_day_no||g_hospital_text;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;
   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variables;
--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin
   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;
   insert into stg_walker_calendar_hsp values g_rec_in;
   g_recs_hospital := g_recs_hospital + sql%rowcount;
  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_write_hospital;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into dwh_foundation.fnd_calendar values a_tbl_insert(i);
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
       update dwh_foundation.fnd_calendar
       set    fin_year_no          = a_tbl_update(i).fin_year_no,
              fin_month_no         = a_tbl_update(i).fin_month_no,
              fin_week_no          = a_tbl_update(i).fin_week_no,
              fin_day_no           = a_tbl_update(i).fin_day_no,
              cal_year_no          = a_tbl_update(i).cal_year_no,
              last_updated_date    = a_tbl_update(i).last_updated_date
       where  calendar_date        = a_tbl_update(i).calendar_date  ;
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
                       ' '||a_tbl_update(g_error_index).calendar_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update stg_walker_calendar_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);
   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_staging_update;
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := dwh_valid.fnd_calendar(g_rec_out.calendar_date);
-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).calendar_date = g_rec_out.calendar_date then
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
      local_bulk_staging_update;
      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_staging1    := a_empty_set_s1;
      a_staging2    := a_empty_set_s2;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      a_count_stg   := 0;
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
-- if month = june then load the another fin_year
--**************************************************************************************************
PROCEDURE load_new_fin_year
AS
BEGIN

  l_text := '** Loading new Fin_year = '||g_current_fin_year;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  ----

  SELECT max(calendar_date) INTO g_max_calendar_date FROM dwh_foundation.fnd_calendar;
  dbms_output.put_line('--------------------------------------------------');
  dbms_output.put_line('g_max_calendar_date='||g_max_calendar_date);

  ----
  SELECT calendar_date ,
    fin_year_no ,
    fin_month_no ,
    fin_week_no ,
    fin_day_no ,
    cal_year_no
  INTO g_calendar_date ,
    g_fin_year_no ,
    g_fin_month_no ,
    g_fin_week_no ,
    g_fin_day_no ,
    g_cal_year_no
  FROM dwh_foundation.fnd_calendar
  WHERE calendar_date  = g_max_calendar_date;
  g_last_updated_date := trunc(g_date);

  dbms_output.put_line('--------------------------------------------------');
  dbms_output.put_line('g_calendar_date='||g_calendar_date);
  dbms_output.put_line('g_fin_year_no='||g_fin_year_no);
  dbms_output.put_line('g_fin_month_no='||g_fin_month_no);
  dbms_output.put_line('g_fin_week_no='||g_fin_week_no);
  dbms_output.put_line('g_fin_day_no='||g_fin_day_no);
  dbms_output.put_line('g_cal_year_no='||g_cal_year_no);
  dbms_output.put_line('g_last_updated_date='||g_last_updated_date);
  dbms_output.put_line('--------------------------------------------------');

  g_calendar_date := g_max_calendar_date;
  g_fin_year_no   := g_fin_year_no + 1;
  g_fin_month_no  := 0;
  g_fin_week_no   := 0;
  g_fin_day_no    := 0;

  dbms_output.put_line('g_calendar_date='||g_calendar_date);
  dbms_output.put_line('g_fin_year_no='||g_fin_year_no);
  dbms_output.put_line('g_fin_week_no='||g_fin_week_no);
  dbms_output.put_line('g_fin_day_no='||g_fin_day_no);
  dbms_output.put_line('--------------------------------------------------');
  ----

  FOR i IN 1..364
  --    for i in 1..372
  --  for i in 1..371
  LOOP
    g_calendar_date := g_calendar_date + 1;
    SELECT to_char(g_calendar_date,'yyyy') INTO g_cal_year_no FROM dual;
    g_fin_day_no   := g_fin_day_no + 1;
    IF g_fin_day_no = 8 THEN
      g_fin_day_no := 1;
    END IF;
    IF g_fin_day_no  = 1 THEN
      g_fin_week_no := g_fin_week_no + 1;
    END IF;
    INSERT
    INTO dwh_foundation.fnd_calendar VALUES
      (
        g_calendar_date ,
        g_fin_year_no ,
        g_fin_month_no ,
        g_fin_week_no ,
        g_fin_day_no ,
        g_cal_year_no ,
        g_last_updated_date
      );
    COMMIT;
  END LOOP;

  --- hardcoded update of fin_month_no as set by business and not derived
  --- add code
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 1
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 1 AND 4;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 2
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 5 AND 9;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 3
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 10 AND 13;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 4
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 14 AND 17;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 5
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 18 AND 22;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 6
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 23 AND 26;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 7
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 27 AND 30;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 8
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 31 AND 35;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 9
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 36 AND 39;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 10
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 40 AND 43;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 11
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 44 AND 48;
  COMMIT;
  UPDATE dwh_foundation.fnd_calendar
  SET fin_month_no  = 12
  WHERE fin_year_no = g_fin_year_no
  AND fin_week_no BETWEEN 49 AND 53;
  COMMIT;

EXCEPTION
WHEN dwh_errors.e_insert_error THEN
  l_message := dwh_constants.vc_err_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  RAISE;
WHEN others THEN
  l_message := dwh_constants.vc_err_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  RAISE;
END load_new_fin_year;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF dwh_foundation.fnd_calendar EX WALKER STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


      --**************************************************************************************************
  -- check if need to load another financial-year on to dwh_foundation.fnd_calendar
  --**************************************************************************************************
  g_today_cal_month_no   := to_number(to_char(g_date,'mm') );

  IF g_today_cal_month_no = 6 THEN
    g_today_fin_year_no  := to_number(to_char(g_date,'yyyy') );
    g_today_fin_year_no_3  := to_number(to_char(g_date,'yyyy') ) + 3;
  dbms_output.put_line('g_today_fin_year_no_3='||g_today_fin_year_no_3);
    BEGIN
    SELECT distinct(fin_year_no)
    INTO g_load_new_fin_year
    FROM dwh_performance.dim_calendar
    WHERE fin_year_no      = g_today_fin_year_no_3;

  dbms_output.put_line('g_today_fin_year_no_3 found - not loading......'||g_today_fin_year_no_3||' '||g_load_new_fin_year);
    EXCEPTION
     WHEN NO_DATA_FOUND THEN
  dbms_output.put_line('g_today_fin_year_no_3 not found - loading......'||g_today_fin_year_no_3||' '||g_load_new_fin_year);
       load_new_fin_year;
  SELECT max(fin_year_no),
        max(calendar_date)
      INTO g_max_fin_year,
        g_max_calendar_date
      FROM dwh_foundation.fnd_calendar;
      l_text := '** NEW Maximum Fin_year and date in dwh_foundation.fnd_calendar = '||g_max_fin_year||'  -  '||g_max_calendar_date;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    END;

  END IF;
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_walker_calendar;
    fetch c_stg_walker_calendar bulk collect into a_stg_input limit g_forall_limit;
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
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
         if g_hospital = 'Y' then
            local_write_hospital;
         else
            local_write_output;
         end if;
      end loop;
    fetch c_stg_walker_calendar bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_walker_calendar;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;

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
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
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
end wh_fnd_corp_003u;
