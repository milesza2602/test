--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_008U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_008U" 
                                                                                                                                                                                                                                                                (p_forall_limit in integer, p_success out boolean) as
  --**************************************************************************************************
  --  date:        sept 2008
  --  author:      alastair de wet
  --  purpose:     Loads merchandise season phase master data in the performance layer
  --               from merchandise season and phase tables ex foundation layer.
  --  tables:      input  - fnd_merch_phase,  fnd_merch_season
  --               output - dim_merch_season_phase
  --  packages:    constants, dwh_log, dwh_valid
  --
  --  maintenance:
  --  28 jan 2009 - defect 213- merch_season_period_type and merch_season_type
  --                            on fnd_merch_season
  --                defect 321- add merch_season_code and merch_season_type
  --                            to dim_merch_season_phase
  --  5 March 2009 - defect 321 - change field name from merch_season_code
  --                              to merch_season_code
  -- changes for 5 march 2009 defect 321 reversed
  --  2 July 2009  - defect 1929 - Update ETL Logic for
  --                               DIM_MERCH_SEASON_PHASE.REPORT_BURSTING_IND
  --  9 june 2010 - defect 3302 - add logic to set report_bursting_ind
  --                             where current_date > 90 days into current season
  --  6 July 2010 - defect 3911 - Change to report_bursting_ind for
  --                              P-previous merch_period_type on
  --                              DIM_MERCH_SEASON_PHASE
  --  02 Dec 2011 - defect 4551 - cater for future season (merch_season_period_type = 'F')
  --  03 Jul 2013 -              - change  from 
  --                                           where merch_season_period_type in ('N')
  --                                           and g_fin_half_no = 2 then  
  --                                       to
  --                                           where merch_season_period_type in ('N')
  --  naming conventions:
  --  g_  -  global variable
  --  l_  -  log table variable
  --  a_  -  array variable
  --  v_  -  local variable as found in packages
  --  p_  -  parameter
  --  c_  -  prefix to cursor
  --**************************************************************************************************
  g_recs_read     integer := 0;
  g_recs_inserted integer := 0;
  g_recs_updated  integer := 0;
  g_forall_limit  integer := dwh_constants.vc_forall_limit;
  g_error_count   number  := 0;
  g_error_index   number  := 0;
  G_CURRENT_SEASON_DATE DATE;
  g_rec_out               dim_merch_season_phase%rowtype;
  g_fin_half_no           dim_calendar.fin_half_no%type;
  g_fin_year_no           dim_calendar.fin_year_no%type;
  g_fin_half_start_date   dim_calendar.calendar_date%type;
  g_merch_season_desc     fnd_merch_season.merch_season_desc%type;
  g_found boolean;
  g_date date      := trunc(sysdate);
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_008U';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'CREATE DIM_MERCH_SEASON_PHASE EX FND_MERCH_PHASE & SEASON';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- for output arrays into bulk load forall statements --
type tbl_array_i
is
  table of dim_merch_season_phase%rowtype index by binary_integer;
type tbl_array_u
is
  table of dim_merch_season_phase%rowtype index by binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   integer := 0;
  a_count_i integer := 0;
  a_count_u integer := 0;
  -----------------------------------------------------------------------------------------------------------
  cursor c_fnd_merch_phase
  is
  select fp.*,
         fs.merch_season_desc,
         fs.merch_season_start_date,
         fs.merch_season_end_date,
         fs.merch_season_type,
         fs.merch_season_period_type,
         0 report_bursting_ind
  from   fnd_merch_phase fp, fnd_merch_season fs
  where  fp.merch_season_no = fs.merch_season_no;

  g_rec_in c_fnd_merch_phase%rowtype;
  -- for input bulk collect --
type stg_array
is
  table of c_fnd_merch_phase%rowtype;
  a_stg_input stg_array;
  --**************************************************************************************************
  -- process, transform and validate the data read from the input interface
  --**************************************************************************************************
procedure local_address_variable
as
begin
  g_rec_out.merch_season_no           := g_rec_in.merch_season_no;
  g_rec_out.merch_phase_no            := g_rec_in.merch_phase_no;
  g_rec_out.merch_phase_desc          := g_rec_in.merch_phase_desc;
  g_rec_out.merch_phase_start_date    := g_rec_in.merch_phase_start_date;
  g_rec_out.merch_phase_end_date      := g_rec_in.merch_phase_end_date;
  g_rec_out.merch_phase_published_ind := g_rec_in.merch_phase_published_ind;
  g_rec_out.merch_season_desc         := g_rec_in.merch_season_desc;
  g_rec_out.merch_season_start_date   := g_rec_in.merch_season_start_date;
  g_rec_out.merch_season_end_date     := g_rec_in.merch_season_end_date;
  g_rec_out.last_updated_date         := g_date;
  g_rec_out.merch_season_type         := g_rec_in.merch_season_type;
  g_rec_out.merch_season_period_type  := g_rec_in.merch_season_period_type;

  g_rec_out.report_bursting_ind       := g_rec_in.report_bursting_ind;
--  if g_rec_out.merch_season_type = 'A' then
--     g_rec_out.report_bursting_ind := 1;
--  end if;
--  if g_rec_in.merch_season_period_type in ('P','C') then
--     g_rec_out.report_bursting_ind := 1;
--  end if;

----- qc3302 change - old code start
--  if g_rec_out.merch_season_type = 'AY' then
--     g_rec_out.report_bursting_ind := 1;
--   else
--       if g_rec_in.merch_season_period_type in ('P','C') then
--         g_rec_out.report_bursting_ind := 1;
--       else
--          g_rec_out.report_bursting_ind := 0;
--       end if;
--  end if;
--  if g_rec_out.merch_season_type = 'AY'
--  and g_rec_in.merch_season_period_type = 'F' then
--     g_rec_out.report_bursting_ind := 0;
--  end if;
----- qc3302 change - old code end

--P	29/JUN/09
case
---
when g_rec_out.merch_season_type = 'AY'
 and g_rec_in.merch_season_period_type = 'F' then
     g_rec_out.report_bursting_ind := 0;
when g_rec_out.merch_season_type = 'AY' then
     g_rec_out.report_bursting_ind := 1;
---
when g_rec_in.merch_season_period_type in ('C') then
     g_rec_out.report_bursting_ind := 1;
---
when g_rec_in.merch_season_period_type in ('N')
and ((g_date < g_CURRENT_SEASON_DATE + 90)
and (g_date > g_CURRENT_SEASON_DATE + 11)) then
     g_rec_out.report_bursting_ind := 0;
when g_rec_in.merch_season_period_type in ('N') then
     g_rec_out.report_bursting_ind := 1;
---
-- commented out as part of change for qc 3911
--when g_rec_in.merch_season_period_type in ('P')
--and ((g_date < g_CURRENT_SEASON_DATE + 90)
--and (g_date > g_CURRENT_SEASON_DATE + 11)) then
--     g_rec_out.report_bursting_ind := 1;
when g_rec_in.merch_season_period_type in ('P')
and ((g_date < g_CURRENT_SEASON_DATE + 90)
and (g_date >= g_CURRENT_SEASON_DATE )) then
     g_rec_out.report_bursting_ind := 1;
when g_rec_in.merch_season_period_type in ('P') then
     g_rec_out.report_bursting_ind := 0;
---
else
     g_rec_out.report_bursting_ind := 0;
end case;

  case g_fin_half_no

     when 1 then
         select min(calendar_date)
         into   g_fin_half_start_date
         from   dim_calendar
         where  fin_year_no  = g_fin_year_no and
                fin_month_no < 7;
    when 2 then
         select min(calendar_date)
         into   g_fin_half_start_date
         from   dim_calendar
         where  fin_year_no  = g_fin_year_no and
                fin_month_no > 6;
  end case;

----
-- OLD code
  --if g_date < g_fin_half_start_date then
     --case
        --when g_rec_in.merch_season_period_type = 'O' then
          --case g_rec_out.merch_season_type
             --when 'O' then
                --g_rec_out.merch_season_code := 'Conversion Old';
             --when 'S' then
                --g_rec_out.merch_season_code := 'SOld';
             --when 'W' then
                --g_rec_out.merch_season_code := 'WOld';
             --else
                --g_rec_out.merch_season_code := 'AYOld';
           --end case;
         --when g_rec_in.merch_season_period_type in ('P','C','L','LP') then
                --g_rec_out.merch_season_code := g_rec_out.merch_season_desc;
         --when g_rec_in.merch_season_period_type = 'N' then
           --case g_rec_out.merch_season_type
             --when 'A' then
                --g_rec_out.merch_season_code := null;
             --else
                --g_rec_out.merch_season_code := g_rec_out.merch_season_desc;
           --end case;
         --else
            --g_rec_out.merch_season_code := null;
     --end case;
  --else
     --case
        --when g_rec_in.merch_season_period_type = 'O' then
          --case g_rec_out.merch_season_type
             --when 'O' then
                --g_rec_out.merch_season_code := 'Conversion Old';
             --when 'S' then
                --g_rec_out.merch_season_code := 'SOld';
             --when 'W' then
                --g_rec_out.merch_season_code := 'WOld';
             --else
                --g_rec_out.merch_season_code := 'AYOld';
           --end case;
         --when g_rec_in.merch_season_period_type in ('P','C','N','LP') then
                --g_rec_out.merch_season_code := g_rec_out.merch_season_desc;
         --when g_rec_in.merch_season_period_type = 'L' then
           --case g_rec_out.merch_season_type
             --when 'A' then
                --g_rec_out.merch_season_code := 'AYOld';
             --else
                --g_rec_out.merch_season_code := g_rec_out.merch_season_desc;
           --end case;
         --else
            --g_rec_out.merch_season_code := null;
     --end case;
  --end if;

---
---
--Set the start date for logic change to 11 days after start of half year
  g_fin_half_start_date := g_fin_half_start_date + 11;
  if g_date < g_fin_half_start_date then
        case
        when g_rec_in.merch_season_period_type = 'O' then
             case g_rec_out.merch_season_type
             when 'O' then
                g_rec_out.merch_season_code := 'Conversion Old';
             when 'S' then
                g_rec_out.merch_season_code := 'SOld';
             when 'W' then
                g_rec_out.merch_season_code := 'WOld';
             when 'AY' then
                g_rec_out.merch_season_code := 'AYOld';
             else
               g_merch_season_desc := '';
               select a.merch_season_desc
                    into g_merch_season_desc
               from fnd_merch_season a, (select x.merch_season_type,
                                           max(x.merch_season_start_date) maxdat
                                         from fnd_merch_season x
                                         group by x.merch_season_type) b
               where a.merch_season_type = g_rec_out.merch_season_type
                 and a.merch_season_start_date = b.maxdat;
               g_rec_out.merch_season_code := g_merch_season_desc;
             end case;
        when g_rec_in.merch_season_period_type in ('P','C','LP','F') then
                g_rec_out.merch_season_code := g_rec_out.merch_season_desc;
        when g_rec_in.merch_season_period_type in ('L') then
              Case
              when g_fin_half_no = 2
              and g_rec_out.merch_season_type = 'AY' then
                  g_rec_out.merch_season_code := 'AYOld';
              else
                  g_rec_out.merch_season_code := g_rec_out.merch_season_desc;
              end case;
        when g_rec_in.merch_season_period_type in ('N')
--          and g_rec_out.merch_season_type = 'AY'
--          and g_fin_half_no = 2 
            then
              g_rec_out.merch_season_code := g_rec_out.merch_season_desc;
        else
            g_rec_out.merch_season_code := null;
        end case;
  else
        case
        when g_rec_in.merch_season_period_type = 'O' then
             case g_rec_out.merch_season_type
             when 'O' then
                g_rec_out.merch_season_code := 'Conversion Old';
             when 'S' then
                g_rec_out.merch_season_code := 'SOld';
             when 'W' then
                g_rec_out.merch_season_code := 'WOld';
             else
                g_rec_out.merch_season_code := 'AYOld';
             end case;
        when g_rec_in.merch_season_period_type in ('P','C','LP','N','F') then
                g_rec_out.merch_season_code := g_rec_out.merch_season_desc;
        when g_rec_in.merch_season_period_type in ('L') then
             case
             when g_rec_out.merch_season_type = 'AY' then
                  g_rec_out.merch_season_code := 'AYOld';
             else
                  g_rec_out.merch_season_code := g_rec_out.merch_season_desc;
             end case;
        else
            g_rec_out.merch_season_code := null;
        end case;
  end if;

  ---------------------------------------------------------
  -- added for olap purposes
  ---------------------------------------------------------
  if g_rec_out.merch_season_start_date        < '01 jan 2004' then
    g_rec_out.merch_season_start_fin_year_no := 2001;
  else
     select distinct fin_year_no
       into g_rec_out.merch_season_start_fin_year_no
       from dim_calendar
      where calendar_date = g_rec_out.merch_season_start_date;
  end if;
  if g_rec_out.merch_phase_start_date        < '01 jan 2004' then
    g_rec_out.merch_phase_start_fin_year_no := 2001;
  else
     select fin_year_no
       into g_rec_out.merch_phase_start_fin_year_no
       from dim_calendar
      where calendar_date = g_rec_out.merch_phase_start_date;
  end if;

--
if g_rec_out.merch_season_type = 'AY' then
   g_rec_out.merch_parent_season_code := 'ALL YEAR';
else
   g_rec_out.merch_parent_season_code := g_rec_out.merch_season_code;
end if;

exception
when others then
  l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  raise;

end local_address_variable;

--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert
as
begin
  forall i in a_tbl_insert.first .. a_tbl_insert.last
  save exceptions
   insert into dim_merch_season_phase values a_tbl_insert
    (i
    );
  g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

exception
when others then
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
  dwh_log.record_error
  (
    l_module_name,sqlcode,l_message
  )
  ;
  for i in 1 .. g_error_count
  loop
    g_error_index := sql%bulk_exceptions
    (
      i
    )
    .error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm
    (
      -sql%bulk_exceptions(i).error_code
    )
    || ' '||a_tbl_insert
    (
      g_error_index
    )
    .merch_season_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .merch_phase_no;
    dwh_log.record_error
    (
      l_module_name,sqlcode,l_message
    )
    ;
  end loop;
  raise;

end local_bulk_insert;

--**************************************************************************************************
-- bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update
as
begin
  forall i in a_tbl_update.first .. a_tbl_update.last
  save exceptions
   update dim_merch_season_phase
   set    merch_season_desc              = a_tbl_update(i).merch_season_desc,
          merch_season_start_date        = a_tbl_update(i).merch_season_start_date,
          merch_season_end_date          = a_tbl_update(i).merch_season_end_date,
          merch_phase_desc               = a_tbl_update(i).merch_phase_desc,
          merch_phase_start_date         = a_tbl_update(i).merch_phase_start_date,
          merch_phase_end_date           = a_tbl_update(i).merch_phase_end_date,
          merch_phase_published_ind      = a_tbl_update(i).merch_phase_published_ind,
          merch_season_start_fin_year_no = a_tbl_update(i).merch_season_start_fin_year_no,
          merch_phase_start_fin_year_no  = a_tbl_update(i).merch_phase_start_fin_year_no,
          last_updated_date              = g_date,
          merch_season_type              = a_tbl_update(i).merch_season_type,
          merch_season_code              = a_tbl_update(i).merch_season_code,
          report_bursting_ind            = a_tbl_update(i).report_bursting_ind,
          merch_season_period_type       = a_tbl_update(i).merch_season_period_type,
          merch_parent_season_code       = a_tbl_update(i).merch_parent_season_code
    where merch_season_no                = a_tbl_update(i).merch_season_no
    and   merch_phase_no                 = a_tbl_update(i).merch_phase_no;

  g_recs_updated := g_recs_updated + a_tbl_update.count;

exception
when others then
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  for i in 1 .. g_error_count
  loop
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_update(g_error_index).merch_season_no|| ' '||a_tbl_update(g_error_index).merch_phase_no;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
  end loop;
  raise;

end local_bulk_update;

--**************************************************************************************************
-- write valid data out to the master table
--**************************************************************************************************
procedure local_write_output
as
begin
  g_found := dwh_valid.dim_merch_season_phase(g_rec_out.merch_season_no,g_rec_out.merch_phase_no);
  -- place record into array for later bulk writing
  if not g_found then
    g_rec_out.sk1_merch_season_phase_no := merch_hierachy_seq.nextval;
    a_count_i               := a_count_i + 1;
    a_tbl_insert(a_count_i) := g_rec_out;
  else
    a_count_u               := a_count_u + 1;
    a_tbl_update(a_count_u) := g_rec_out;
  end if;
  a_count := a_count + 1;
  --**************************************************************************************************
  -- bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  if a_count > g_forall_limit then
    local_bulk_insert;
    local_bulk_update;
    a_tbl_insert := a_empty_set_i;
    a_tbl_update := a_empty_set_u;
    a_count_i    := 0;
    a_count_u    := 0;
    a_count      := 0;
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
-- Write Dummy record
--**************************************************************************************************
procedure local_write_dummy as

begin
  g_rec_out.sk1_merch_season_phase_no := 0;
  g_rec_out.merch_season_no           := 0;
  g_rec_out.merch_phase_no            := 0;
  g_rec_out.merch_phase_desc          := 'Dummy phase';
  g_rec_out.merch_phase_published_ind := 0;
  g_rec_out.merch_season_desc         := 'Dummy season';
  g_rec_out.last_updated_date         := g_date;
  g_rec_out.merch_season_type         := '0';
  g_rec_out.merch_season_code         := '0';
  g_rec_out.report_bursting_ind       := 0;
  g_rec_out.merch_season_period_type  := '0';
  g_rec_out.merch_parent_season_code  := '0';

  g_found := dwh_valid.dim_merch_season_phase(0,0);

   if not g_found then
      insert into dim_merch_season_phase values g_rec_out;
      g_recs_inserted := g_recs_inserted + 1;
   else
      update dim_merch_season_phase
      set    merch_season_desc              = g_rec_out.merch_season_desc,
             merch_season_start_date        = g_rec_out.merch_season_start_date,
             merch_season_end_date          = g_rec_out.merch_season_end_date,
             merch_phase_desc               = g_rec_out.merch_phase_desc,
             merch_phase_start_date         = g_rec_out.merch_phase_start_date,
             merch_phase_end_date           = g_rec_out.merch_phase_end_date,
             merch_phase_published_ind      = g_rec_out.merch_phase_published_ind,
             merch_season_start_fin_year_no = g_rec_out.merch_season_start_fin_year_no,
             merch_phase_start_fin_year_no  = g_rec_out.merch_phase_start_fin_year_no,
             last_updated_date              = g_date,
             merch_season_type              = g_rec_out.merch_season_type,
             merch_season_code              = g_rec_out.merch_season_code,
             report_bursting_ind            = g_rec_out.report_bursting_ind,
             merch_season_period_type       = g_rec_out.merch_season_period_type,
             merch_parent_season_code       = g_rec_out.merch_parent_season_code
    where    merch_season_no                = g_rec_out.merch_season_no and
             merch_phase_no                 = g_rec_out.merch_phase_no;

      g_recs_updated := g_recs_updated + 1;

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

end local_write_dummy;

--**************************************************************************************************
-- main process loop
--**************************************************************************************************
begin
  if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
    g_forall_limit  := p_forall_limit;
  end if;
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF DIM_MERCH_SEASON_PHASE EX FND_MERCH_PHASE STARTED '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                             l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
--  g_date := '2 july 2013';
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  select merch_season_start_date
into g_CURRENT_season_date
from dwh_foundation.fnd_merch_season
where merch_season_period_type = 'C'
and merch_season_type in ('S','W');
  l_text := 'CURRENT SEASON START_DATE - '||g_CURRENT_season_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


  select fin_half_no,fin_year_no
  into   g_fin_half_no,g_fin_year_no
  from   dim_calendar
  where  calendar_date = g_date;

  --**************************************************************************************************
  open c_fnd_merch_phase;
  fetch c_fnd_merch_phase bulk collect into a_stg_input limit g_forall_limit;

  while a_stg_input.count > 0
  loop
    for i in 1 .. a_stg_input.count
    loop
      g_recs_read             := g_recs_read + 1;
      if g_recs_read mod 10000 = 0 then
        l_text                := dwh_constants.vc_log_records_processed|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||
                                                              '  '||g_recs_read ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
      g_rec_in := a_stg_input(i);
      local_address_variable;
      local_write_output;
    end loop;
    fetch c_fnd_merch_phase bulk collect into a_stg_input limit g_forall_limit;
  end loop;
  close c_fnd_merch_phase;
  --**************************************************************************************************
  -- at end write out what remains in the arrays
  --**************************************************************************************************
  local_bulk_insert;
  local_bulk_update;
  local_write_dummy;
  --**************************************************************************************************
  -- at end write out log totals
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,
                             dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
  l_text := dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed||sysdate;
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

end wh_prf_corp_008u;
