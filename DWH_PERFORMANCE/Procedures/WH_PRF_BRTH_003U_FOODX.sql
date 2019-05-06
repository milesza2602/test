--------------------------------------------------------
--  DDL for Procedure WH_PRF_BRTH_003U_FOODX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_BRTH_003U_FOODX" 
                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Special History Extract for foods - year 2013 and 2014
--  22 Dec 2014 - Wendy Lyttle
--**************************************************************************************************
--  Date:        Sept 2010
--  Author:      Wendy Lyttle
--  Purpose:     BRIDGETHORN EXTRACT
--               Extract price data for :
--               last 4 weeks (this_week_start_date between today - 4 weeks and today)
--               sunday (fin_day_no = 7)
--               stores only (loc_type = 'S')
--               foods only (business_unit_no = 50)
--               any area except 'NON-CUST CORPORATE' (area_no <> 9978)

--  Tables:      Input  - rtl_loc_item_wk_rms_price
--                        dim_item
--                        dim_location
--               Output - temp_rtl_area_item_wk_price
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 oct 2010 - added full statement and gather stats
--  9 dec 2010 - qc4190 - change to reflect all foods departments
--                        instead of certain foods departments (department_no in(12 ,15 ,16 ,
--                        22 ,23 ,32 ,34 ,37 ,40 ,41 ,42 ,43 ,44 ,45 ,53 ,59 ,66 ,73 ,87 ,
--                        88 ,93 ,95 ,97 ,99 )
--
--  17 oct 2013 - wendy  - add in execute immediate 'alter session enable parallel dml';
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
g_fin_day_no number :=0;
g_rec_out            temp_rtl_area_item_wk_price%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_BRTH_003U_FOOD';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'BRIDGETHORN ROLLUP TO TEMP_price';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

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
    l_text := 'ROLLUP OF temp_rtl_loc_item_wk_price EX WEEK LEVEL STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    SELECT MIN(THIS_WEEK_START_DATE), MAX(THIS_WEEK_END_DATE) 
    INTO g_start_date,      g_end_date
    FROM DIM_CALENDAR 
    WHERE FIN_YEAR_NO = 2018;

     l_text := '---------------------------------------------------------------------------------------------------------';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

    l_text := 'START DATE OF ROLLUP - '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate ('truncate table dwh_DATAFIX.t18_rtl_area_item_wk_price');
--    commit;
    l_text := 'TRUNCATED table dwh_DATAFIX.t18_rtl_area_item_wk_price';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
                                   't18_rtl_area_item_wk_price', DEGREE => 8);
--    commit;
--    l_text := 'GATHER STATS on dwh_DATAFIX.t18_rtl_area_item_wk_price';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    execute immediate 'alter session enable parallel dml';
    
    INSERT /*+ APPEND */ INTO dwh_DATAFIX.t18_rtl_area_item_wk_price
       select   /*+ full(rai) parallel (rai	4) */
       dl.area_no
                ,di.item_no
                ,dc.fin_year_no
                ,dc.fin_week_no
                ,dl.st_store_type
                ,dc.this_week_start_date
                ,SUM(nvl(rai.reg_rsp_excl_vat ,0)) reg_rsp_excl_vat_avg
                ,COUNT(*)LOC_CNT
--                ,avg(nvl(rai.reg_rsp_excl_vat ,0)) reg_rsp_excl_vat_avg
     from     dwh_performance.rtl_loc_item_dy_rms_price rai,
              dwh_performance.dim_item DI,
              dwh_performance.dim_location DL,
              dwh_performance.dim_calendar Dc
     where    dc.this_week_start_date  between G_start_date and G_end_date
     and      rai.calendar_date = dc.calendar_date
     and      dc.fin_day_no = 7
     and      rai.sk1_item_no = di.sk1_item_no
     and      rai.sk1_location_no = dl.sk1_location_no
     and      dl.loc_type = 'S'
     and      dl.area_no <> 9978
     and      di.business_unit_no = 50
     AND      rai.reg_rsp_excl_vat > 0
--     and      di.department_no in(12,15,16,17,22,23,32,34,37,40,41,42,43,44,45,53,59,65,66,73,83,87,88,93,95,97,99)
--and di.primary_supplier_no = 50885  -- in (59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110) --(50885)  --, 14314)
and di.primary_supplier_no  in (50885, 59816,11821,10287,59815,  22111,22101,22106,22112,22102,22103,22104,22105,22107,22108,22109,22110, 14314, 24785, 25357)
     group by dl.area_no, di.item_no, dc.fin_year_no, dc.fin_week_no, dl.st_store_type, dc.this_week_start_date;

   g_recs_read     :=SQL%ROWCOUNT;
   g_recs_inserted :=SQL%ROWCOUNT;

commit;
    DBMS_STATS.gather_table_stats ('DWH_DATAFIX',
                                   't18_rtl_area_item_wk_price', DEGREE => 8);
    commit;
    l_text := 'GATHER STATS on dwh_DATAFIX.t18_rtl_area_item_wk_price';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     l_text := '---------------------------------------------------------------------------------------------------------';
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

END WH_PRF_BRTH_003U_FOODX;
