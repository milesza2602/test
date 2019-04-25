--------------------------------------------------------
--  DDL for Procedure WH_PRF_GENERIC_STATS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_GENERIC_STATS" (p_success out boolean, p_schema_name in varchar2, p_table_name in varchar2, 
                      p_periods in number default 0, p_type in varchar2 default 'history') as
--**************************************************************************************************
--  Date:        June 2009
--  Author:      M Munnik
--  Purpose:     Gather statistics on table - input parameters
--  Parameters:  p_periods   - number of periods (days or weeks) - if  0 (zero), then all partitions for the table
--                                                               - if -1, then only for last 12 fin months into the past, 
--                                                                        and all future fin months, for existing partitions
--                                                               - else, only for specified number of periods (days or weeks), for existing subpartitions
--               p_type      - 'history' - number of periods back, starting with yesterday for day tables and current week for week tables
--                             'future'  - number of periods into the future, starting with today for day tables and current week for week tables
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_count            number := 0;
g_part_cnt         number := 0;
g_date             date;
g_schema_name      varchar2(32);
g_table_name       varchar2(32);
g_periods          number;
g_type             varchar2(10);
g_start_date       date;
g_end_date         date;
v_start_date        varchar2(9);
v_end_date            varchar2(9);
g_part_name        varchar2(32);
g_part_pos         integer;
g_year_month       varchar2(6);
g_start_year       number;
g_end_year         number;
g_start_week       number;
g_end_week         number;
g_table_ind        varchar2(1);

l_message          sys_dwh_errlog.log_text%type;
l_module_name      sys_dwh_errlog.log_procedure_name%type      := 'WH_PRF_GENERIC_STATS';
l_name             sys_dwh_log.log_name%type                   := dwh_constants.vc_log_name_rtl_facts;
l_system_name      sys_dwh_log.log_system_name%type            := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name      sys_dwh_log.log_script_name%type            := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name   sys_dwh_log.log_procedure_name%type         := l_module_name;
l_text             sys_dwh_log.log_text%type;
l_description      sys_dwh_log_summary.log_description%type    := 'Gather Table Stats';
l_process_type     sys_dwh_log_summary.log_process_type%type   := dwh_constants.vc_log_process_type_n;

cursor part_cur is
   select   table_owner, table_name, partition_name 
   from     dba_tab_partitions
   where    table_owner        = g_schema_name 
   and      table_name         = g_table_name
   and      partition_position > g_part_pos
   order by partition_position;

cursor subpart_cur is
   select   p.table_owner, p.table_name, p.partition_name
   from     dba_tab_partitions p join dba_tab_subpartitions s
   on       p.table_owner        = s.table_owner
   and      p.table_name         = s.table_name
   and      p.partition_name     = s.partition_name
   where    p.table_owner        = g_schema_name
   and      p.table_name         = g_table_name
   and      p.partition_position > g_part_pos
   group by p.table_owner, p.table_name, p.partition_name, p.partition_position
   order by p.partition_position;

--
--uncommneted below code to test new dy_cur cursor below...05/05./2010 BB
--cursor dy_cur is
--   select   subpartition_name from
--  (select   s.subpartition_name, p.partition_position, s.subpartition_position, 
--            to_date((substr(s.subpartition_name, length(s.subpartition_name) - 5, 6)),'ddmmyy') pdate
--   from     dba_tab_partitions p join dba_tab_subpartitions s
--   on       p.table_owner        = s.table_owner
--   and      p.table_name         = s.table_name
--   and      p.partition_name     = s.partition_name
--   where    p.table_owner        = g_schema_name
--   and      p.table_name         = g_table_name)
--   where    pdate between g_start_date and g_end_date
--   where    to_char(pdate,'DD-MON-RR') between to_date(g_start_date,'DD-MON-RR') and to_date(g_end_date,'DD-MON-RR')
--   *** Use the above as commented out if it fails on date format error with current 'where' clause ***
--   order by partition_position, subpartition_position;
--

cursor dy_cur is
   select   subpartition_name from
  (select   s.subpartition_name, p.partition_position, s.subpartition_position, 
        to_char(
            (substr(s.subpartition_name, length(s.subpartition_name) - 5, 2))||
            case
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '01' then '-JAN-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '02' then '-FEB-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '03' then '-MAR-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '04' then '-APR-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '05' then '-MAY-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '06' then '-JUN-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '07' then '-JUL-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '08' then '-AUG-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '09' then '-SEP-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '10' then '-OCT-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '11' then '-NOV-'
                when
                (substr(s.subpartition_name, length(s.subpartition_name) - 3, 2)) = '12' then '-DEC-'
                END||
            (substr(s.subpartition_name, length(s.subpartition_name) - 1, 2))
               ) pdate
   from     dba_tab_partitions p join dba_tab_subpartitions s
   on       p.table_owner        = s.table_owner
   and      p.table_name         = s.table_name
   and      p.partition_name     = s.partition_name
   where    p.table_owner        = g_schema_name
   and      p.table_name         = g_table_name)
   where    pdate  between v_start_date and v_end_date
  order by partition_position, subpartition_position;

cursor wk_cur is
   select   sp.subpartition_name from
  (select   s.subpartition_name, p.partition_position, s.subpartition_position,
            substr(p.partition_name, (instr(p.partition_name,'_M') + 2), 4) year_no,
            case when length(substr(p.partition_name, (instr(p.partition_name,'_M') + 2))) = 5 
                 then substr(s.subpartition_name, (instr(s.subpartition_name,'_M') + 8))
                 else substr(s.subpartition_name, (instr(s.subpartition_name,'_M') + 9)) end week_no
   from     dba_tab_partitions p join dba_tab_subpartitions s
   on       p.table_owner        = s.table_owner
   and      p.table_name         = s.table_name
   and      p.partition_name     = s.partition_name
   where    p.table_owner        = g_schema_name
   and      p.table_name         = g_table_name) sp
   join     dim_calendar c
   on       sp.year_no    = c.fin_year_no
   and      sp.week_no    = c.fin_week_no
   and      c.fin_day_no = 1
   where    c.calendar_date between g_start_date and g_end_date
   order by sp.partition_position, sp.subpartition_position;

procedure stats_for_partitions as
begin
   for r in part_cur
      loop
         dbms_stats.gather_table_stats (r.table_owner,
                                        r.table_name,
                                        r.partition_name,
                                        granularity => 'PARTITION',
                                        degree => 4
         );
      end loop;

end stats_for_partitions;

procedure stats_for_subpartitions as
begin
   for r in subpart_cur
      loop
         dbms_stats.gather_table_stats (r.table_owner,
                                        r.table_name,
                                        r.partition_name,
                                        granularity => 'SUBPARTITION',
                                        degree => 4
         );
      end loop;

end stats_for_subpartitions;

procedure stats_for_dy_table as
begin
   for r in dy_cur
      loop
         dbms_stats.gather_table_stats (g_schema_name,
                                        g_table_name,
                                        r.subpartition_name,
                                        granularity => 'SUBPARTITION',
                                        degree => 4
         );
         l_text := 'STATS GATHERED FOR SUBPARTITION - '||r.subpartition_name||' at '||to_char(sysdate, ('hh24:mi:ss'));
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         g_part_cnt := g_part_cnt + 1;
      end loop;

end stats_for_dy_table;

procedure stats_for_wk_table as
begin
   for r in wk_cur
      loop
         dbms_stats.gather_table_stats (g_schema_name,
                                        g_table_name,
                                        r.subpartition_name,
                                        granularity => 'SUBPARTITION',
                                        degree => 4
         );
         l_text := 'STATS GATHERED FOR SUBPARTITION - '||r.subpartition_name||' at '||to_char(sysdate, ('hh24:mi:ss'));
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         g_part_cnt := g_part_cnt + 1;
      end loop;

end stats_for_wk_table;

procedure get_part_pos as
begin

   select fin_year_no||fin_month_no 
   into   g_year_month
   from   dim_calendar
   where  calendar_date = g_date - 366;

   begin
      select partition_position
      into   g_part_pos
      from   dba_tab_partitions
      where  table_owner = g_schema_name
      and    table_name  = g_table_name
      and    substr(partition_name, (instr(partition_name,'_M') + 2)) = trim(g_year_month);
      
      exception
         when no_data_found then
           g_part_pos := 0;
   end;

   if g_part_pos is null then
      g_part_pos := 0;
   end if;

end get_part_pos;

procedure get_start_and_end_dates as
begin

   select count(*)
   into   g_count
   from   dba_part_key_columns
   where  owner       = g_schema_name
   and    name        = g_table_name
   and    object_type = 'TABLE';
         
   if g_count = 1 then
      g_table_ind := 'D';
   else
      g_table_ind := 'W';
   end if;

   if g_type = 'HISTORY' then
      if g_table_ind = 'D' then
         g_end_date   := g_date + 1;
         g_start_date := g_end_date - (g_periods - 1);
        --added below 2 lines for date conversion issue...BB 05/05/2010
         v_end_date   :=  to_char((g_date + 1),'DD-MON-RR');
         v_start_date :=  to_char((g_end_date - (g_periods - 1)),'DD-MON-RR');
                  
      else
         select this_week_start_date, fin_year_no, fin_week_no
         into   g_end_date,           g_end_year,  g_end_week
         from   dim_calendar
         where  calendar_date = g_date;

         g_start_date   := g_end_date - ((g_periods - 1) * 7);

         select fin_year_no,  fin_week_no
         into   g_start_year, g_start_week
         from   dim_calendar
         where  calendar_date = g_start_date;
      end if;
   else
      if g_table_ind = 'D' then
         g_start_date := g_date + 2;
         g_end_date   := g_date + 1 + g_periods;
      else
         select this_week_start_date, fin_year_no,  fin_week_no
         into   g_start_date,         g_start_year, g_start_week
         from   dim_calendar
         where  calendar_date = g_date;
         
         g_end_date   := g_start_date + ((g_periods - 1) * 7);

         select fin_year_no, fin_week_no
         into   g_end_year,  g_end_week
         from   dim_calendar
         where  calendar_date = g_end_date;
      end if;
   end if;

end get_start_and_end_dates;

procedure do_stats as
begin

   select count(*)
   into   g_count
   from   dba_tab_partitions
   where  table_owner = g_schema_name 
   and    table_name  = g_table_name;

   if g_count = 0 then
      l_text := 'GATHER STATS FOR NON-PARTITIONED TABLE';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      dbms_stats.gather_table_stats(g_schema_name, g_table_name);
   else   
      if nvl(g_periods, 0) = 0 then
         g_part_pos := 0;
         l_text := 'GATHER ALL STATS FOR PARTITIONED TABLE';
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         stats_for_partitions;
         stats_for_subpartitions;
      else
         if g_periods = -1 then
            get_part_pos;
            l_text := 'GATHER STATS FOR PARTITIONED TABLE FOR PREVIOUS 12 MONTHS AND FUTURE';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            stats_for_partitions;
            stats_for_subpartitions;
         else
            get_start_and_end_dates;
            if g_table_ind = 'D' then
               l_text := 'GATHER STATS FOR PARTITIONED TABLE - SUBPARTITIONS BETWEEN '||to_char(g_start_date,'ddmmyy')||' AND '||to_char(g_end_date,'ddmmyy');
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
               stats_for_dy_table;
            else
               l_text := 'GATHER STATS FOR PARTITIONED TABLE - SUBPARTITIONS BETWEEN '||g_start_year||' '||g_start_week||' AND '||g_end_year||' '||g_end_week;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
               stats_for_wk_table;
            end if;
            if g_part_cnt = 0 then
               l_text := 'NO PARTITIONS FOUND !!!';
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            end if;
         end if;
      end if;   
   end if;

   dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,l_process_type,
                              dwh_constants.vc_log_ended,'','','','','');
                              
   l_text := 'GATHER STATS for '||trim(g_table_name)||' ENDED '||to_char(sysdate, ('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end do_stats;

--*************************************************************************************************************************
-- Main Routine
--*************************************************************************************************************************
begin
   p_success       := false;
   g_schema_name   := upper(p_schema_name);
   g_table_name    := upper(p_table_name);
   g_type          := upper(nvl(p_type,'history'));
   g_periods       := p_periods;
   
   dwh_lookup.dim_control(g_date);

   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'GATHER STATS for '||trim(g_table_name)||' STARTED '||to_char(sysdate, ('dd mon yyyy hh24:mi:ss'));
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,l_process_type,
                              dwh_constants.vc_log_started,'','','','','');

   select count(*) 
   into   g_count
   from   dba_tables
   where  owner      = g_schema_name
   and    table_name = g_table_name;

   if g_count = 0 then
      l_text := 'TABLE DOES NOT EXIST - '||trim(g_table_name);
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   else
      do_stats;
   end if;

   l_text := dwh_constants.vc_log_run_completed||sysdate;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := dwh_constants.vc_log_draw_line;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := ' ';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   commit;

   p_success       := true;

   exception
      when others then
         l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
         dwh_log.record_error(l_module_name,sqlcode,l_message);
         dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,l_process_type,
                                    dwh_constants.vc_log_aborted,'','','','','');
         rollback;
         p_success := false;
         raise;

end wh_prf_generic_stats;
