-- ****** Object: Procedure W7131037.WH_PRF_CUST_325U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_325U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        NOV 2015
--  Author:      Alastair de Wet
--  Purpose:     Create cust_mart_home_and_away_grp fact table in the performance layer
--               with input ex cust_basket_item table from performance layer.
--               THIS JOB RUNS QUARTERLY AFTER THE START OF A NEW QUARTER
--  Tables:      Input  - cust_basket_item
--               Output - cust_mart_home_and_away_grp
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            cust_mart_home_and_away_grp%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);

g_start_week         number         ;
g_end_week           number          ;
g_start_date         date         ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_stmt               varchar2(300);
g_yr_00              number;
g_qt_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_325U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE CUST_BASKET_ITEM to HOME48 GROUP MART';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'ROLLUP OF cust_mart_home_and_away_grp EX cust_basket item LEVEL STARTED AT '||
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
-- Determine if this is a day on which we process
--**************************************************************************************************


   if to_char(sysdate,'DDMM') NOT IN  ('1207','1210','1201','1204') then
      l_text      := 'This job only runs QUARTERLY ON THE 12TH and today '||to_char(sysdate,'DDMM')||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;
   l_text      := 'This job only runs on '||to_char(sysdate,'DDMM')||' and today '||to_char(sysdate,'DDMM')||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--**************************************************************************************************
-- Main loop
--**************************************************************************************************


    select fin_year_no,fin_quarter_no
    into   g_yr_00,g_qt_00
    from   dim_calendar
    where  calendar_date = g_date - 80;

    if g_qt_00 = 1 then
      g_start_week := 1;
      g_end_week   := 13;
    end if;
    if g_qt_00 = 2 then
      g_start_week := 14;
      g_end_week   := 26;
    end if;
    if g_qt_00 = 3 then
      g_start_week := 27;
      g_end_week   := 39;
    end if;
    if g_qt_00 = 4 then
      g_start_week := 40;
       SELECT MAX(FIN_WEEK_NO) INTO g_end_week FROM DIM_CALENDAR_WK WHERE FIN_YEAR_NO = g_yr_00;
    end if;

    select calendar_date,fin_quarter_end_date
    into   g_start_date,g_end_date
    from   dim_calendar
    where  fin_day_no  = 1
    and    fin_week_no = g_start_week
    and    fin_year_no = g_yr_00;

    l_text := 'ROLLUP DATE RANGE IS:- '||g_start_date||'  to '||g_end_date||' of '|| g_yr_00;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';



    l_text := '1ST PASS BEGINS DOING C48 BUSINESS UNITS:- ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


insert /*+ APPEND parallel(csm,4)*/ into cust_mart_home_and_away_grp csm

     with         home as  (
     select   /*+ FULL(bih)  parallel(bih,8) FULL(s2) parallel(s2,8) full(di) */
              max(s2.location_no) location_no,
              bih.primary_customer_identifier,
              di.group_no,
              sum ( case
                when bih.location_no     = s2.location_no then
                   item_tran_selling-discount_selling
                else
                   0
                end ) home_location_value    ,
               sum ( case
                when bih.location_no     = s2.location_no then
                   item_tran_qty
                else
                   0
                end ) home_location_qty
     from     cust_basket_item bih,
              dim_customer_store_of_pref s2,
              dim_item di
     where    bih.tran_date       between G_START_DATE and G_END_DATE
     and      bih.item_no         = di.item_no
     and      bih.primary_customer_identifier = s2.primary_customer_identifier
     and      s2.fd_ch            = 2
     and      bih.primary_customer_identifier not in (998,0)
     and      tran_type          not in ('P','N','L','R','Q')
     and      di.business_unit_no between 51 and 55
     group by bih.primary_customer_identifier, di.group_no),

              away as  (
     select   /*+ FULL(bi) parallel (bi,8)  full(di) */
              bi.primary_customer_identifier,
              di.group_no,
              sum(item_tran_selling-discount_selling) away_location_value,
              sum(item_tran_qty) away_location_qty
     from     cust_basket_item bi,
              dim_item di
     where    bi.tran_date       between G_START_DATE and G_END_DATE
     and      bi.item_no         = di.item_no
     and      primary_customer_identifier not in (998,0)
     and      tran_type          not in ('P','N','L','R','Q')
     and      di.business_unit_no between 51 and 55
     group by primary_customer_identifier,group_no)

select   /*+ FULL(home) FULL(away) parallel(8)  */
         g_yr_00,
         g_qt_00,
         home.location_no,
         home.group_no,
         sum(home_location_qty),
         sum(home_location_value),
         sum(away_location_qty   - home_location_qty),
         sum(away_location_value - home_location_value),
         g_date
from     home,
         away
where    home.primary_customer_identifier = away.primary_customer_identifier
and      home.group_no            = away.group_no
group by home.location_no,
         home.group_no
having   sum(away_location_qty   - home_location_qty) > 99 and
         sum(away_location_value - home_location_value) / (sum(away_location_value - home_location_value) + sum(home_location_value)) >= 0.1
order by home.location_no,
         home.group_no
        ;
    g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;

   l_text := 'UPDATE STATS ON MART TABLES';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   DBMS_STATS.gather_table_stats ('W7131037','cust_mart_home_and_away_grp',estimate_percent=>1, DEGREE => 32);

    commit;

    l_text := '2ND PASS BEGINS DOING FOODS BUSINESS UNIT:- ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


insert /*+ APPEND parallel(csm,4)*/ into cust_mart_home_and_away_grp csm
     with     home as  (
     select    /*+ FULL(bih)  parallel(bih,8) FULL(s2) parallel(s2,8) full(di) */
              max(s2.location_no) location_no,
              bih.primary_customer_identifier,
              di.group_no,
              sum ( case
                when bih.location_no     = s2.location_no then
                   item_tran_selling-discount_selling
                else
                   0
                end ) home_location_value    ,
               sum ( case
                when bih.location_no     = s2.location_no then
                   item_tran_qty
                else
                   0
                end ) home_location_qty
     from     cust_basket_item bih,
              dim_customer_store_of_pref s2,
              dim_item di
     where    bih.tran_date       between G_START_DATE and G_END_DATE
     and      bih.item_no         = di.item_no
     and      bih.primary_customer_identifier = s2.primary_customer_identifier
     and      s2.fd_ch            = 1
     and      bih.primary_customer_identifier not in (998,0)
     and      tran_type          not in ('P','N','L','R','Q')
     and      di.business_unit_no = 50
     group by bih.primary_customer_identifier, di.group_no),

              away as  (
     select   /*+ FULL(bi) parallel (bi,8)  full(di) */
              bi.primary_customer_identifier,
              di.group_no,
              sum(item_tran_selling-discount_selling) away_location_value,
              sum(item_tran_qty) away_location_qty
     from     cust_basket_item bi,
              dim_item di
     where    bi.tran_date       between G_START_DATE and G_END_DATE
     and      bi.item_no         = di.item_no
     and      primary_customer_identifier not in (998,0)
     and      tran_type          not in ('P','N','L','R','Q')
     and      di.business_unit_no = 50
     group by primary_customer_identifier,group_no)

select   /*+ FULL(home) FULL(away) parallel(8)  */
         g_yr_00,
         g_qt_00,
         home.location_no,
         home.group_no,
         sum(home_location_qty),
         sum(home_location_value),
         sum(away_location_qty   - home_location_qty),
         sum(away_location_value - home_location_value),
         g_date
from     home,
         away
where    home.primary_customer_identifier = away.primary_customer_identifier
and      home.group_no            = away.group_no
group by home.location_no,
         home.group_no
having   sum(away_location_qty   - home_location_qty) > 99 and
         sum(away_location_value - home_location_value) / (sum(away_location_value - home_location_value) + sum(home_location_value)) >= 0.1
order by home.location_no,
         home.group_no
        ;

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


    commit;

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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_cust_325u;
