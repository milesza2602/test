--------------------------------------------------------
--  DDL for Procedure BV_FOODEXTR_PRICE_NEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."BV_FOODEXTR_PRICE_NEW" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Aug 2016
--  Author:      BHAVESH VALODIA
--  Purpose:     EXTRACT PRICING DATA FOR FOODS - ONE OFF
--  Tables:      Input  - rtl_loc_item_dy_rms_price
--               Output - temp_food_extr_pricing
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
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            temp_food_extr_pricing_new%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_yr_00              number;
g_wk_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'BV_FOODEXTR_PRICING_NEW';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT 5 YEARS OF FOOD DATA PRICING';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'EXTRACT OF temp_food_extr_pricing EX food_pricing STARTED AT '||
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
-- Look up period
--**************************************************************************************************


    g_start_date := '22 AUG 2011';
    g_end_date   := '28 AUG 2011';
    
--**************************************************************************************************
-- Main loop
--**************************************************************************************************

for g_sub in 1..9 loop
--    select fin_day_no, this_week_start_date, this_week_end_date,fin_year_no,fin_week_no
--    into   g_fin_day_no, g_start_date, g_end_date,g_yr_00,g_wk_00
--    from   dim_calendar
--    where  calendar_date = g_date - (g_sub * 7);

    l_text := 'ROLLUP RANGE IS:- '||g_start_date||'  to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     execute immediate 'alter session enable parallel dml';

--    g_stmt      := 'Alter table  temp_food_extr_pricing truncate  subpartition for ('||g_yr_00||','||g_wk_00||') update global indexes';
--    l_text      := g_stmt;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    execute immediate g_stmt;


INSERT  /*+ APPEND parallel (tmp,4)*/ 
INTO TEMP_FOOD_EXTR_PRICING_NEW tmp
SELECT 
      l.sk1_fd_zone_group_zone_no, 
      to_char(sc.sk1_Item_no) as sk1_Item_no, 
      c.fin_week_code,
      to_char(sc.Calendar_date,'DD/MM/YYYY') as Calendar_date,
      STATS_MODE(CASE WHEN sc.prom_rsp IS NULL THEN sc.reg_rsp ELSE sc.prom_rsp END) as RSP 
FROM  rtl_loc_item_dy_rms_price sc
  INNER JOIN dim_calendar c on c.calendar_date = sc.calendar_date
  INNER JOIN dim_item i on i.sk1_item_no = sc.sk1_item_no AND i.item_no in (2022800000002,2022810000009,2022840000000,2022880000008,2023900000008,2025220000003,2025250000004,2028870000003,2062900000007,2062910000004,2064610000001,2064630000005,2064670000003,2065940000006,2067620000009,2070300000008,2070310000005,2070320000002,2070330000009,2070850000008,2071370000004,2071630000003,2071970000008,2072570000009,2073480000004,2076400000009,2076410000006,2076430000000,2076440000007,2076450000004,2076460000001,2076480000005,2076490000002,2076510000005,2078430000004,2082230000003,2082640000006,2083290000002,2084200000006,2084210000003,2084710000008,2085240000001,2086480000004,2088190000008,2088200000004,2088220000008,2088230000005,2088240000002,2088250000009,2088260000006,2088270000003,2088280000000,2088290000007,2088310000000,2089290000004,2089300000000,2089400000009,2091540000009,2094160000008,2094370000003,2094380000000,2094390000007,2094710000007,2098520000004,2098530000001,2098540000008,2098550000005,2099560000009,2560650000006,2561140000001,2907369000000,2907370000006)
  INNER JOIN dim_location l on l.sk1_location_no = sc.sk1_location_no
WHERE  sc.reg_rsp IS NOT NULL 
  AND sc.reg_rsp <> 1.14
  AND c.calendar_date between g_start_date and g_end_date --Full date range required: 27/JUN/11 - 31/JUL/16
  AND l.area_no = 9951
  AND l.sk1_fd_zone_group_zone_no IN (254,255,256)
  AND (sc.calendar_date <= l.st_close_date or  l.st_close_date ='31/DEC/99')
  AND sc.calendar_date >= (trunc(to_date(l.st_open_date), 'DAY')+7)
GROUP BY l.sk1_fd_zone_group_zone_no, to_char(sc.sk1_Item_no), c.fin_week_code, to_char(sc.Calendar_date,'DD/MM/YYYY');


/*
     select  /*+ FULL(sc) FULL(i)  parallel (sc,4) */
/*
	l.sk1_fd_zone_group_zone_no, 
	to_char(sc.sk1_Item_no) as sk1_Item_no, 
	c.fin_week_code,
	to_char(sc.Calendar_date,'DD/MM/YYYY') as Calendar_date,
	max(case when sc.prom_rsp is null then sc.reg_rsp else sc.prom_rsp end) as RSP
from 	rtl_loc_item_dy_rms_price sc
inner join dim_calendar c on c.calendar_date = sc.calendar_date
inner join dim_item i 	on i.sk1_item_no = sc.sk1_item_no 
			and i.item_no in 
			(2022800000002,2022810000009,2022840000000,2022880000008,2023900000008,
			2025220000003,2025250000004,2028870000003,2062900000007,2062910000004,2064610000001,
			2064630000005,2064670000003,2065940000006,2067620000009,2070300000008,2070310000005,
			2070320000002,2070330000009,2070850000008,2071370000004,2071630000003,2071970000008,
			2072570000009,2073480000004,2076400000009,2076410000006,2076430000000,2076440000007,
			2076450000004,2076460000001,2076480000005,2076490000002,2076510000005,2078430000004,
			2082230000003,2082640000006,2083290000002,2084200000006,2084210000003,2084710000008,
			2085240000001,2086480000004,2088190000008,2088200000004,2088220000008,2088230000005,
			2088240000002,2088250000009,2088260000006,2088270000003,2088280000000,2088290000007,
			2088310000000,2089290000004,2089300000000,2089400000009,2091540000009,2094160000008,
			2094370000003,2094380000000,2094390000007,2094710000007,2098520000004,2098530000001,
			2098540000008,2098550000005,2099560000009,2560650000006,2561140000001,2907369000000,
			2907370000006)
inner join dim_location l on l.sk1_location_no = sc.sk1_location_no

where  	sc.reg_rsp is not null and sc.reg_rsp <> 1.14
and 	c.calendar_date between g_start_date and g_end_date --Full date range required: 27/JUN/11 - 31/JUL/16
and 	l.area_no = 9951
and 	l.sk1_fd_zone_group_zone_no IN (254,255,256)
and 	(sc.calendar_date <= l.st_close_date or  l.st_close_date ='31/DEC/99')
and 	sc.calendar_date >= (trunc(to_date(l.st_open_date), 'DAY')+7)

group by l.sk1_fd_zone_group_zone_no, to_char(sc.sk1_Item_no), c.fin_week_code, to_char(sc.Calendar_date,'DD/MM/YYYY');
*/
    g_recs_read     :=  g_recs_read     + SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    l_text := 'INSERTED SO FAR:- '||g_recs_inserted ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    g_start_date    := g_start_date - 7;
    g_end_date      := g_end_date   - 7;

    commit;
end loop;
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

end BV_FOODEXTR_PRICE_NEW;
