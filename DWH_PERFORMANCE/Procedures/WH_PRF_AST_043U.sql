--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_043U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_043U" 
(p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Jan 2013
--  Author:      Wendy Lyttle
--  Purpose:     Create the weekly style colour rollup CHBD catalog table in the performance layer
--               with input ex RP weekly item catalog table from performance layer.
--
--               Cloned from WH_PRF_RP_012U
--
-- Runtime Instructions :
--                We loop for 6 weeks due to the fact that Sales data can be up to 6 weeks late.
--
----------------------------------------------------------
-- partition_name example = RTL_LSWAC_M20154_14
--TP_RTL_LSWAC_M20156
--       g_WKpart_name := 'RTL_LSWAC_M'||g_start_year||g_start_month||'_'||g_start_week;
--        l_text := 'Running table partition stats on :'||g_WKpart_name;
--       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
-- partition_name example = -TP_RTL_LSWAC_M20156
 ----------------------------------------------------------
--
--  Tables:      Input  - rtl_loc_item_wk_ast_catlg
--               Output - RTL_loc_SC_WK_AST_catlg
--  Packages:    constants, dwh_log, dwh_valid
--**************************************************************************************************
--  Maintenance:
---
--  W LYTTLE 20 OCTOBER 2016 -- ADD COLUMNS FOR PRODUCT-LINKING
--

--**************************************************************************************************--  Naming conventions
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
g_start_week         integer       :=  0;
g_start_year         integer       :=  0;
g_start_month        integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_item_wk_ast_catlg%rowtype;
g_found              boolean;
g_date                 date        := trunc(sysdate);
g_this_week_start_date date        := trunc(sysdate);
g_this_week_end_date   date        := trunc(sysdate);
g_last_analyzed_date   date        := sysdate;
g_start_date_time      date        := sysdate;
g_fin_week_code      varchar2(7);

g_xpart_name         varchar2(32);
g_wkpart_name        varchar2(32);
g_xsubpart_name      varchar2(32);
g_wksubpart_name     varchar2(32);
g_part_name          varchar2(32);
g_subpart_name       varchar2(32);


g_MTHpart_name        varchar2(32);
g_xMTHpart_name      varchar2(32);
g_part_name       varchar2(32);


g_fin_year_no        number        := 0;
g_fin_month_no       number        := 0;
g_fin_week_no        number        := 0;
--
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_043U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rpl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rpl;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLUP THE AST WEEKLY SC CATALOG FACTS EX PERFORMANCE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
--

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

    l_text := 'ROLLUP OF RTL_loc_SC_WK_AST_catlg EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.DIM_CONTROL(G_DATE);
  --l_text := BATCH DATE BEING PROCESSED IS:- '||g_date;
  --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
 --   g_date := '30 oct 2016';
 --   l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date;
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    

 --   l_text := ' testing only TRUNCATE TABLE RTL_loc_SC_WK_AST_catlg';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
-- EXECUTE IMMEDIATE('TRUNCATE TABLE dwh_performance.RTL_loc_SC_WK_AST_catlg');



FOR g_sub IN 0..5
  LOOP
    g_recs_inserted := 0;
    select fin_year_no, fin_week_no, this_week_start_date, this_week_end_date, fin_week_code, fin_month_no
    into   g_start_year, g_start_week, g_this_week_start_date, g_this_week_end_date, g_fin_week_code, g_start_month
    from   dim_calendar
    WHERE calendar_date = g_date - (g_sub * 7);

l_text := 'dates='||g_start_year||'-'||g_start_week||'-'||g_this_week_start_date||'-'||g_this_week_end_date||'-'||g_fin_week_code||'-'||g_start_month;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


 


    INSERT /*+ APPEND */ 
INTO DWH_PERFORMANCE.RTL_loc_SC_WK_AST_catlg LIW
         SELECT   liw.sk1_location_no,
                  di.sk1_style_colour_no,
                  liw.fin_year_no,
                  liw.fin_week_no,
                  liw.fin_week_code,
                  liw.sk1_avail_uda_value_no,
                  max(liw.sk2_location_no) sk2_location_no,
                  liw.this_week_start_date,
            max(nvl(liw.ch_catalog_ind,0))       as ch_catalog_ind,
            sum(nvl(liw.ch_num_avail_days,0))        as ch_num_avail_days,
            sum(nvl(liw.ch_num_catlg_days,0))        as ch_num_catlg_days,
            sum(nvl(liw.reg_sales_qty_catlg,0))      as reg_sales_qty_catlg,
            sum(nvl(liw.reg_sales_catlg,0))          as reg_sales_catlg,
            sum(nvl(liw.reg_soh_qty_catlg,0))        as reg_soh_qty_catlg,
            sum(nvl(liw.reg_soh_selling_catlg,0))    as reg_soh_selling_catlg,
            sum(nvl(liw.prom_sales_qty_catlg,0))     as prom_sales_qty_catlg,
            sum(nvl(liw.prom_sales_catlg,0))         as prom_sales_catlg,
            sum(nvl(liw.prom_reg_sales_qty_catlg,0))     as prom_reg_sales_qty_catlg,
            sum(nvl(liw.prom_reg_sales_catlg,0))         as prom_reg_sales_catlg,
                  g_date last_updated_date,
                  sum(nvl(liw.Avail_REG_SALES_QTY_CATLG,0))     as Avail_REG_SALES_QTY_CATLG,
                  sum(nvl(liw.Avail_REG_SALES_CATLG,0))         as Avail_REG_SALES_CATLG,
                  sum(nvl(liw.Avail_REG_SOH_QTY_CATLG,0))       as Avail_REG_SOH_QTY_CATLG,
                  sum(nvl(liw.Avail_REG_SOH_SELLING_CATLG,0))   as Avail_REG_SOH_SELLING_CATLG,
                  sum(nvl(liw.Avail_PROM_SALES_QTY_CATLG,0))    as Avail_PROM_SALES_QTY_CATLG,
                  sum(nvl(liw.Avail_PROM_SALES_CATLG,0))        as Avail_PROM_SALES_CATLG,
                  sum(nvl(liw.Avail_PROM_REG_SALES_QTY_CATLG,0))as Avail_PROM_REG_SALES_QTY_CATLG,
                  sum(nvl(liw.Avail_PROM_REG_SALES_CATLG,0))    as Avail_PROM_REG_SALES_CATLG,
                  sum(nvl(liw.Avail_CH_NUM_AVAIL_DAYS,0))       as Avail_CH_NUM_AVAIL_DAYS,
                  sum(nvl(liw.AVAIL_CH_NUM_CATLG_DAYS,0))       as AVAIL_CH_NUM_CATLG_DAYS,
                  max(nvl(PROD_LINK_IND,0))
   from      RTL_loc_item_wk_ast_catlg liw, --DWH_DATAFIX.wTMP_loc_item_WK_AST_catlg LIW, 
            dim_item di                    
   where    liw.sk1_item_no      = di.sk1_item_no and
            liw.fin_year_no =  g_start_year   and
            liw.fin_week_no =  g_start_week
            AND (PROD_LINK_IND = 1 OR PROD_LINK_IND IS NULL)
  group by liw.sk1_location_no,
                  di.sk1_style_colour_no,
                  liw.fin_year_no,
                  liw.fin_week_no,
                  liw.fin_week_code,
                  liw.sk1_avail_uda_value_no,
                  liw.this_week_start_date;
 
   g_recs_read := g_recs_read + SQL%ROWCOUNT;
   g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


  l_text := ' ==================  ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  l_text := 'Insert NEW:- RECS =  '||g_recs_inserted||' '||g_this_week_start_date||'  To '||g_this_week_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       commit;


    end loop;


 

    select fin_year_no, fin_week_no, this_week_start_date, this_week_end_date, fin_week_code, fin_month_no
    into   g_start_year, g_start_week, g_this_week_start_date, g_this_week_end_date, g_fin_week_code, g_start_month
    from   dim_calendar
    WHERE calendar_date = g_date ;

       g_MTHpart_name := 'TP_RTL_LSWAC_M'||g_start_year||g_start_month;
        l_text := 'Running table partition stats on :'||g_MTHpart_name;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        dbms_stats.gather_table_stats('DWH_PERFORMANCE'
        ,'RTL_loc_SC_WK_AST_catlg'
        ,GRANULARITY=>'ALL'
        ,PARTNAME=>g_MTHpart_name
        ,CASCADE=>TRUE,DEGREE=>32
        ,ESTIMATE_PERCENT=>0.1); 
        
    

     commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
--
--
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text := ' ==================  ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
--
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

end wh_prf_ast_043u;
