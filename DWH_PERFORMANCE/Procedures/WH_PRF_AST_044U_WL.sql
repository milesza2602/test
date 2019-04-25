--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_044U_WL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_044U_WL" 
(p_forall_limit in integer,p_success out boolean) as
--
--**************************************************************************************************
---- FOR PRODLINK DATAFIX 2 AUG 2017 - POSTDATES = 3,10 JULY 2017
--**************************************************************************************************
--  Date:        aUGUST 2017
--  Author:      Wendy Lyttle
--**************************************************************************************************
--  Date:        oct 2016
--  Author:      Wendy Lyttle
--  Purpose:     Update daily CHBD ASSORT catalogue values for linked items in the performance layer
--               with input ex RP daily item catalog table from performance layer.
--               NB. uda excluded from key due to fact that uda for parent and child 
--                   can be different,SK1_AVAIL_UDA_VALUE_NO,
--               FYI - a parent  can be a catalogued-non-linked item (ie. normal unlinked item)
--                                          or  catalogued-linked item (ie. parent with child)
--
--
--**************************************************************************************************
--  Tables:      Input  - RTL_loc_item_dy_ast_catlg
--               Output - RTL_loc_item_dy_ast_catlg
--  Packages:    constants, dwh_log, dwh_valid
--
--**************************************************************************************************
--  Maintenance:
--  18 OCT 2016 - WENDY - add apex data and rollup new avail measures
--  W LYTTLE 20 OCTOBER 2016 -- ADD COLUMNS FOR PRODUCT-LINKING
--
--
--**************************************************************************************************
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit              integer       :=  dwh_constants.vc_forall_limit;
g_recs_read                 integer       :=  0;
g_recs_updated              integer       :=  0;
g_recs_inserted             integer       :=  0;
g_recs_hospital             integer       :=  0;
g_error_count               number        :=  0;
g_error_index               number        :=  0;
g_count                     number        :=  0;
g_rec_out                                 rtl_loc_item_wk_ast_catlg%rowtype;
g_found                     boolean;

g_date_minus_7wk         date          := trunc(sysdate);
g_date                      date          := trunc(sysdate);
g_last_analyzed_date        date          := sysdate;
g_start_date_time           date          := sysdate;
g_xpart_name                varchar2(32);
g_wkpart_name               varchar2(32);
g_xsubpart_name             varchar2(32);
g_wksubpart_name            varchar2(32);
g_part_name                 varchar2(32);
g_subpart_name              varchar2(32);

g_fin_year_no               number        := 0;
g_fin_month_no              number        := 0;
g_fin_week_no               number        := 0;
g_sub                       number        := 0;
g_subp1                     number        := 0;

g_sub1                      number        := 0;
g_start_week                integer       :=  0;
g_start_year                integer       :=  0;
g_this_week_start_date      date          := trunc(sysdate);
g_this_week_end_date        date          := trunc(sysdate);
g_fin_week_code             varchar2(7);
--
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_044U_WL';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rpl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rpl;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLUP THE AST WEEKLY ITEM CATALOG FACTS EX PERFORMANCE';
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

    l_text := 'UPDATE PARENT MEASURES STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
  DWH_LOOKUP.DIM_CONTROL(G_DATE);
  G_DATE := '10 JULY 2017';
  l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   g_date_minus_7wk := g_date - 49;

 --   l_text := 'Test BATCH DATE BEING PROCESSED IS:- '||g_date||' THRU 30 OCT 2016';
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     ----------------------------------------------------------
        -- sum values for SK1_GROUP_ITEM_NO
     ----------------------------------------------------------
    l_text := 'truncate table dwh_performance.tmp_LOC_ITEM_DY_AST_CATLG';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate('truncate table dwh_performance.tmp_LOC_ITEM_DY_AST_CATLG');

     insert /*+ append */ into dwh_performance.tmp_LOC_ITEM_DY_AST_CATLG ast
      with selsix as (     select /*+ materialize parallel (RTL_LOC_ITEM_DY_AST_CATLG,8)  full (RTL_LOC_ITEM_DY_AST_CATLG)*/
                             *
                   from dwh_performance.RTL_loc_item_dy_ast_catlg 
                   WHERE  POST_DATE between g_date_minus_7wk and g_date
                )
        select  post_date,  NVL(SK1_GROUP_ITEM_NO,SK1_ITEM_NO), sk1_location_no,
                 0 SK1_AVAIL_UDA_VALUE_NO, -- see comment above for why this is 0
                  sum(nvl(REG_SALES_QTY_CATLG,0))   as Avail_REG_SALES_QTY_CATLG,
                  sum(nvl(REG_SALES_CATLG,0))         as Avail_REG_SALES_CATLG,
                  sum(nvl(REG_SOH_QTY_CATLG,0))       as Avail_REG_SOH_QTY_CATLG,
                  sum(nvl(REG_SOH_SELLING_CATLG,0))   as Avail_REG_SOH_SELLING_CATLG,
                  sum(nvl(PROM_SALES_QTY_CATLG,0))    as Avail_PROM_SALES_QTY_CATLG,
                  sum(nvl(PROM_SALES_CATLG,0))        as Avail_PROM_SALES_CATLG,
                  sum(nvl(PROM_REG_SALES_QTY_CATLG,0))as Avail_PROM_REG_SALES_QTY_CATLG,
                  sum(nvl(PROM_REG_SALES_CATLG,0))    as Avail_PROM_REG_SALES_CATLG,
  --                sum(nvl(CH_NUM_AVAIL_DAYS,0))       as Avail_CH_NUM_AVAIL_DAYS,
  --                sum(nvl(CH_NUM_CATLG_DAYS,0))       as Avail_CH_NUM_CATLG_DAYS
                  MAX(nvl(CH_NUM_AVAIL_DAYS,0))       as Avail_CH_NUM_AVAIL_DAYS,
                  MAX(nvl(CH_NUM_CATLG_DAYS,0))       as Avail_CH_NUM_CATLG_DAYS
     from selsix ss
     WHERE  last_updated_date = g_date OR POST_DATE = G_DATE
     group by post_date, NVL(SK1_GROUP_ITEM_NO,SK1_ITEM_NO),sk1_location_no
     ;
     
     g_recs_read := g_recs_read + SQL%ROWCOUNT;
     g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


     l_text := 'Insert Temp:- RECS =  '||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     commit;
     ----------------------------------------------------------
        -- update data with avail for SK1_GROUP_ITEM_NO
        --      where matches sk1_item_no
        -- ie. will sum avail values children + parent into parent measures
     ----------------------------------------------------------     
     merge /*+ APPEND parallel (AST,8) */ into dwh_performance.RTL_LOC_ITEM_DY_AST_CATLG ast
     using (select /*+ materialize parallel (TMP_LOC_ITEM_DY_AST_CATLG,8)  full (TMP_LOC_ITEM_DY_AST_CATLG)*/ 
                     *
                   from  dwh_performance.tmp_LOC_ITEM_DY_AST_CATLG tmp
            ) g3     
             ON (trunc(AST.POST_DATE) = trunc(G3.POST_DATE) 
                 and AST.SK1_ITEM_NO = G3.SK1_GROUP_ITEM_NO 
                 and AST.SK1_LOCATION_NO = G3.SK1_LOCATION_NO
           --      and ast.SK1_AVAIL_UDA_VALUE_NO = g3.SK1_AVAIL_UDA_VALUE_NO
                 )
   when matched then
   update set last_updated_date = G_DATE, 
              Avail_REG_SALES_QTY_CATLG = g3.Avail_REG_SALES_QTY_CATLG,
              Avail_REG_SALES_CATLG = g3.Avail_REG_SALES_CATLG,
              Avail_REG_SOH_QTY_CATLG = g3.Avail_REG_SOH_QTY_CATLG,
              Avail_REG_SOH_SELLING_CATLG = g3.Avail_REG_SOH_SELLING_CATLG,
              Avail_PROM_SALES_QTY_CATLG = g3.Avail_PROM_SALES_QTY_CATLG,
              Avail_PROM_SALES_CATLG = g3.Avail_PROM_SALES_CATLG,
              Avail_PROM_REG_SALES_QTY_CATLG = g3.Avail_PROM_REG_SALES_QTY_CATLG,
              Avail_PROM_REG_SALES_CATLG = g3.Avail_PROM_REG_SALES_CATLG,
              Avail_CH_NUM_AVAIL_DAYS = g3.Avail_CH_NUM_AVAIL_DAYS,
              Avail_CH_NUM_CATLG_DAYS = g3.Avail_CH_NUM_CATLG_DAYS;
  
     g_recs_read := g_recs_read + SQL%ROWCOUNT;
     g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


     l_text := 'Updated Parents:- RECS =  '||g_recs_inserted;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
--
--
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


END WH_PRF_AST_044U_WL;
