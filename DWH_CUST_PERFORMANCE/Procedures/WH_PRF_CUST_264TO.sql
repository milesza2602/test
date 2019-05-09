--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_264TO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_264TO" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        Feb 2014
--  Author:      Alastair de Wet
--  Purpose:     Create cust_csm_shopping_habits performance fact table in the performance layer
--               with added value ex  cust_csm_wk. Monthly around 10th of Month for last 3 months (13 weeks)
--  Tables:      Input  - cust_csm_wk
--               Output - cust_csm_shopping_habits
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_stmt               varchar2(300);


g_run_date           date;
g_start_week_no      integer;
g_start_date         date;
g_end_date           date;
g_end_year_no        integer;
g_end_week_no        integer;
g_end_month_no       integer;
g_last_wk_visit      integer;
g_num_st_dy_visit    integer;
g_num_wk_visit       integer;
g_visit_pattern      integer;
g_shop_habit_segment_no  integer;
g_customer_identifier    cust_csm_shopping_habits.csm_customer_identifier%type;
g_period_code            cust_csm_shopping_habits.csm_period_code%type;
g_avg_basket_vbn     integer;
g_average_basket     integer;
g_preferred_store    integer;
g_prev_date          date;
g_found              boolean;
g_count              integer       :=  0;
g_max_count          integer       :=  0;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_264U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE THE CSM SHOPPING HABITS PERIOD TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

--  array  used to store range of weeks being processed --
type weeks_array is table of dim_calendar.this_week_end_date%type
                                                       index by binary_integer;
a_wks                 weeks_array;
a_wks_count           integer       := 0;

type yr_array is table of integer index by binary_integer;
a_yr                 yr_array;
type wk_array is table of integer index by binary_integer;
a_wk                 wk_array;

type cs_array is table of integer index by binary_integer;
a_cs                 cs_array;


--parallel (cw,4) 



cursor c_prf_cust_csm_wk is

with consec as (   
select * from  (
      select   /*+ parallel (cs,4) */
              csm_customer_identifier,this_week_end_date,(this_week_end_date + 7 - g_start_date) / 7 as col
      from    cust_csm_wk cs,
              dim_calendar_wk dc
      where    (
           (cs.fin_year_no       = a_yr(1) and cs.fin_week_no       = a_wk(1))
   or      (cs.fin_year_no       = a_yr(2) and cs.fin_week_no       = a_wk(2))
   or      (cs.fin_year_no       = a_yr(3) and cs.fin_week_no       = a_wk(3))
   or      (cs.fin_year_no       = a_yr(4) and cs.fin_week_no       = a_wk(4))
   or      (cs.fin_year_no       = a_yr(5) and cs.fin_week_no       = a_wk(5))
   or      (cs.fin_year_no       = a_yr(6) and cs.fin_week_no       = a_wk(6))
   or      (cs.fin_year_no       = a_yr(7) and cs.fin_week_no       = a_wk(7))
   or      (cs.fin_year_no       = a_yr(8) and cs.fin_week_no       = a_wk(8))
   or      (cs.fin_year_no       = a_yr(9) and cs.fin_week_no       = a_wk(9))
   or      (cs.fin_year_no       = a_yr(10) and cs.fin_week_no      = a_wk(10))
   or      (cs.fin_year_no       = a_yr(11) and cs.fin_week_no      = a_wk(11))
   or      (cs.fin_year_no       = a_yr(12) and cs.fin_week_no      = a_wk(12))
   or      (cs.fin_year_no       = a_yr(13) and cs.fin_week_no      = a_wk(13))
             )
      and      cs.fin_year_no             = dc.fin_year_no
      and      cs.fin_week_no             = dc.fin_week_no
                )
pivot           (
                count(this_week_end_date) 
                for   col in (1 as wk1,2 as wk2,3 as wk3,4 as wk4,5 as wk5,6 as wk6,7 as wk7,
                              8 as wk8,9 as wk9,10 as wk10,11 as wk11,12 as wk12,13 as wk13)
                )
      order by  csm_customer_identifier  
             ),
     sop as (
     select  /*+ parallel (csm,4) */ sum(csm_basket_value) basket_value,location_no,csm_customer_identifier
     from   cust_csm_st_wk csm
     where   (
           (csm.fin_year_no       = a_yr(1) and csm.fin_week_no       = a_wk(1))
   or      (csm.fin_year_no       = a_yr(2) and csm.fin_week_no       = a_wk(2))
   or      (csm.fin_year_no       = a_yr(3) and csm.fin_week_no       = a_wk(3))
   or      (csm.fin_year_no       = a_yr(4) and csm.fin_week_no       = a_wk(4))
   or      (csm.fin_year_no       = a_yr(5) and csm.fin_week_no       = a_wk(5))
   or      (csm.fin_year_no       = a_yr(6) and csm.fin_week_no       = a_wk(6))
   or      (csm.fin_year_no       = a_yr(7) and csm.fin_week_no       = a_wk(7))
   or      (csm.fin_year_no       = a_yr(8) and csm.fin_week_no       = a_wk(8))
   or      (csm.fin_year_no       = a_yr(9) and csm.fin_week_no       = a_wk(9))
   or      (csm.fin_year_no       = a_yr(10) and csm.fin_week_no      = a_wk(10))
   or      (csm.fin_year_no       = a_yr(11) and csm.fin_week_no      = a_wk(11))
   or      (csm.fin_year_no       = a_yr(12) and csm.fin_week_no      = a_wk(12))
   or      (csm.fin_year_no       = a_yr(13) and csm.fin_week_no      = a_wk(13))
             )
     group by csm_customer_identifier,location_no),
     sop1 as (
     select   /*+ parallel (4) */ csm_customer_identifier,max(basket_value) max_basket_value
     from     sop
     group by csm_customer_identifier),
     sop2 as (
     select   /*+ parallel (4) */ sop.csm_customer_identifier,max(location_no) location_no
     from     sop, sop1
     where    sop.basket_value  = sop1.max_basket_value
     and      sop.csm_customer_identifier = sop1.csm_customer_identifier
     group by sop.csm_customer_identifier)

   select   /*+ parallel (4) */
            cw.csm_customer_identifier,
            cw.csm_period_code,
            sum(csm_basket_value) csm_basket_value,
            sum(csm_num_item) csm_num_item,
            sum(csm_num_st_dy_visit) csm_num_st_dy_visit,
            count(distinct cw.fin_week_no)  num_wk_visit,
            max(dc.fin_week_end_date) last_wk_date,
            max(sop2.location_no) location_no,
            max(consec.wk1) wk1,
            max(consec.wk2) wk2,
            max(consec.wk3) wk3,
            max(consec.wk4) wk4,
            max(consec.wk5) wk5,
            max(consec.wk6) wk6,
            max(consec.wk7) wk7,
            max(consec.wk8) wk8,
            max(consec.wk9) wk9,
            max(consec.wk10) wk10,
            max(consec.wk11) wk11,
            max(consec.wk12) wk12,
            max(consec.wk13) wk13
   from     cust_csm_wk cw,
            sop2,
            consec,
            dim_calendar_wk dc
   where     cw.fin_year_no             = dc.fin_year_no
   and       cw.fin_week_no             = dc.fin_week_no
   and       cw.csm_customer_identifier = sop2.csm_customer_identifier
   and       cw.csm_customer_identifier = consec.csm_customer_identifier
   and     (
           (cw.fin_year_no       = a_yr(1) and cw.fin_week_no       = a_wk(1))
   or      (cw.fin_year_no       = a_yr(2) and cw.fin_week_no       = a_wk(2))
   or      (cw.fin_year_no       = a_yr(3) and cw.fin_week_no       = a_wk(3))
   or      (cw.fin_year_no       = a_yr(4) and cw.fin_week_no       = a_wk(4))
   or      (cw.fin_year_no       = a_yr(5) and cw.fin_week_no       = a_wk(5))
   or      (cw.fin_year_no       = a_yr(6) and cw.fin_week_no       = a_wk(6))
   or      (cw.fin_year_no       = a_yr(7) and cw.fin_week_no       = a_wk(7))
   or      (cw.fin_year_no       = a_yr(8) and cw.fin_week_no       = a_wk(8))
   or      (cw.fin_year_no       = a_yr(9) and cw.fin_week_no       = a_wk(9))
   or      (cw.fin_year_no       = a_yr(10) and cw.fin_week_no      = a_wk(10))
   or      (cw.fin_year_no       = a_yr(11) and cw.fin_week_no      = a_wk(11))
   or      (cw.fin_year_no       = a_yr(12) and cw.fin_week_no      = a_wk(12))
   or      (cw.fin_year_no       = a_yr(13) and cw.fin_week_no      = a_wk(13))
           )
   group by cw.csm_customer_identifier,cw.csm_period_code;



--**************************************************************************************************
-- Look for max consecutive weeks IE. In the 13 weeks, the most number of visits that follow
-- directly after one another week wise. 0111001011110 would yield an answer of 4
--**************************************************************************************************
procedure lookup_consec_weeks as
begin
   g_max_count := 1;
   g_count     := 0;



   for i in 1..13 loop
        if a_cs(i)  = 1 then
          g_count := g_count + 1;
       else
          if g_count > g_max_count then
             g_max_count := g_count;
          end if;
          g_count := 0;
       end if;

     end loop;

   if g_count > g_max_count then
      g_max_count := g_count;
   end if;

   exception
      when others then
       l_message := 'LOOKUP CONSEC WEEKS - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end lookup_consec_weeks;



--**************************************************************************************************
-- Look up dimension measures based on data accumulated so far
--**************************************************************************************************
procedure lookup_dimension_data as
begin
   begin
   select avg_basket_value_band_no
   into   g_avg_basket_vbn
   from   fnd_csm_avg_basket_value_band
   where  min_avg_basket_val <= g_average_basket
   and    csm_period_code = g_period_code
   and    max_avg_basket_val >= g_average_basket;
   exception
            when no_data_found then
            g_avg_basket_vbn := null;
   end;
   begin
   select min(visit_pattern_no)
   into   g_visit_pattern
   from   fnd_csm_visit_pattern
   where  csm_num_wk_visit         = g_num_wk_visit
   and    csm_num_st_dy_visit_min <= g_num_st_dy_visit
   and    csm_num_st_dy_visit_max >= g_num_st_dy_visit
   and    csm_last_wk_visit_min   <= g_last_wk_visit
   and    csm_last_wk_visit_max   >= g_last_wk_visit
   and    csm_period_code          = g_period_code
   and    csm_max_consec_wk_visit_min <= g_max_count
   and    csm_max_consec_wk_visit_max >= g_max_count;
   exception
            when no_data_found then
            g_visit_pattern := null;
   end;
   begin
   select  shopping_habit_segment_no
   into    g_shop_habit_segment_no
   from    fnd_csm_shopping_habit_matrix
   where   visit_pattern_no         = g_visit_pattern
   and     csm_period_code          = g_period_code
   and     avg_basket_value_band_no = g_avg_basket_vbn;
   exception
            when no_data_found then
            g_shop_habit_segment_no := null;
   end;


   exception
      when others then
       l_message := 'LOOKUP DIMENSION DATA - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end lookup_dimension_data;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin


    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'CREATE CSM_CUST_PERIOD STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Look up range of weeks to be processed and store in variables
--**************************************************************************************************
    select   max(fin_week_end_date),max(fin_year_no),max(fin_week_no),max(fin_month_no)
    into     g_end_date,g_end_year_no,g_end_week_no,g_end_month_no
    from     dim_calendar
    where    fin_year_no  =  2013  and
             fin_month_no =  7;

    g_start_date := g_end_date - 84;


   g_run_date := g_end_date + 7;
/*   
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;  
   l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/  
       

  begin
    for i in 1..13 loop
       a_wks(i) := g_start_date + ((i-1)*7);
       
       select   fin_year_no,fin_week_no
       into     a_yr(i),a_wk(i)
       from     dim_calendar
       where    calendar_date = g_start_date + ((i-1)*7);       
       
       l_text   := 'Week end date:- '||i||' - '||a_wks(i)||' '||a_yr(i)||' '||a_wk(i);
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end loop;
  end;

    l_text := 'ROLLUP RANGE IS:- '||g_start_date||'  to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Delete previous records in the range so that we do only inserts
--**************************************************************************************************


    l_text := 'TRUNCATE PARTITION STARTED:- ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    

    g_stmt      := 'Alter table  DWH_CUST_PERFORMANCE.CUST_CSM_SHOPPING_HABITS truncate  subpartition for ('||g_end_year_no||','||g_end_month_no||') update global indexes';
    l_text      := g_stmt;

    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    execute immediate g_stmt;


    commit;

    l_text := 'TRUNCATE COMPLETED:- '||g_recs_deleted ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session enable parallel dml';

--**************************************************************************************************


for csm in c_prf_cust_csm_wk
    loop

-- Last wk visit takes the 13 weeks and determines the most recent week in which the customer shopped
-- Rating the answers from 1 thru 13.  0111001011110 would yield an answer of 12

       g_last_wk_visit := 0;
       for i in 1..13 loop
          if a_wks(i)  = csm.last_wk_date then
             g_last_wk_visit := i;
          end if;
       end loop;

       g_average_basket      := csm.csm_basket_value / csm.num_wk_visit;
       g_customer_identifier := csm.csm_customer_identifier;
       g_period_code         := csm.csm_period_code;
       g_num_wk_visit        := csm.num_wk_visit;
       g_num_st_dy_visit     := csm.csm_num_st_dy_visit;
       g_preferred_store     := csm.location_no;
       a_cs(1) := csm.wk1;
       a_cs(2) := csm.wk2;
       a_cs(3) := csm.wk3;
       a_cs(4) := csm.wk4;
       a_cs(5) := csm.wk5;
       a_cs(6) := csm.wk6;
       a_cs(7) := csm.wk7;
       a_cs(8) := csm.wk8;
       a_cs(9) := csm.wk9;
       a_cs(10) := csm.wk10;
       a_cs(11) := csm.wk11;
       a_cs(12) := csm.wk12;
       a_cs(13) := csm.wk13;

       g_max_count := 1;
       lookup_consec_weeks;       
       lookup_dimension_data;

--parallel (sh,4) /*+ APPEND */
   if g_shop_habit_segment_no is not null then
       insert into cust_csm_shopping_habits  values
       (
       csm.csm_customer_identifier,
       csm.csm_period_code,
       g_end_year_no,
       g_end_week_no,
       csm.csm_basket_value,
       csm.csm_num_item,
       csm.num_wk_visit,
       csm.csm_num_st_dy_visit,
       g_last_wk_visit,
       g_max_count,
       g_visit_pattern,
       g_avg_basket_vbn,
       g_shop_habit_segment_no,
       g_preferred_store,
       g_date,
       g_end_month_no
       );
   end if;
   
      g_recs_inserted := g_recs_inserted  + sql%rowcount;
      if g_recs_inserted  mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_inserted ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            commit;
      end if;
      

--      WILL NOT WORK BECAUSE CUST DENTIFIER IS NO LONGER THE WFS CUSTOMER NO BUT IS NOW THE C2 CUSTOMER NO
--      update fnd_wfs_customer
--      set    csm_basket_val_13wkbk =  csm.csm_basket_value,
--             csm_pref_store_13wkbk =  g_preferred_store,
--             csm_shop_habit_seg_no_13wkbk = g_shop_habit_segment_no
--      where  wfs_customer_no =  csm.csm_customer_identifier;

    end loop;

    commit;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_CUST_264TO;
