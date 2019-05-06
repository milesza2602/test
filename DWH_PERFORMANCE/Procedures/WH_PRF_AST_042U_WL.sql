--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_042U_WL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_042U_WL" 
(p_forall_limit in integer,p_success out boolean) as
--
--**************************************************************************************************
--  Date:        Jan 2013
--  Author:      Wendy Lyttle
--  Purpose:     Create the weekly item rollup CHBD catalog table in the performance layer
--               with input ex RP daily item catalog table from performance layer.
--
--               Cloned from WH_PRF_RP_011U
--
-- Runtime Instructions :
--                We loop for 6 weeks due to the fact that Sales data can be up to 6 weeks late.
--
--
--
--  Tables:      Input  - RTL_loc_item_dy_ast_catlg
--               Output - rtl_loc_item_wk_ast_catlg
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  27 March 2013 - wendy   - Add in update stats due to fact that general update stats will reflect incorrect values.
--
-- ---
--  W LYTTLE 20 OCTOBER 2016 -- ADD COLUMNS FOR PRODUCT-LINKING
--  W LYTTLE 17 NOV 2016 - performance tuning
--                       - prev sql
--
                                --     WITH selprd as (    select /*+ parallel (A,8) */
                                /*                                sk1_location_no,
                                                                sk1_item_no,
                                                                post_date,
                                                                sk1_avail_uda_value_no,
                                                                (case when PROD_LINK_TYPE = 'CL' then 3
                                                                      when PROD_LINK_TYPE = 'UL' then 2
                                                                 else 1 end) as PROD_LINK_no
                                                       from     RTL_LOC_ITEM_DY_AST_CATLG A
                                                       WHERE POST_DATE BETWEEN G_THIS_WEEK_START_DATE AND G_THIS_WEEK_END_DATE
                                                       ),
                                      sellnk as (    select
                                                                sk1_location_no,
                                                                sk1_item_no,
                                                                fin_year_no, fin_week_no, 
                                                                sk1_avail_uda_value_no,
                                                                max(PROD_LINK_no) prod_link_no
                                                       from     selprd a, dim_calendar b
                                                       where    a.post_date = b.calendar_date
                                                                 group by sk1_location_no,
                                                                sk1_item_no,
                                                                fin_year_no, fin_week_no, 
                                                                sk1_avail_uda_value_no
                                                                    )
                                 */--      select /*+ parallel (LIW,8) */
                                /*                  liw.sk1_location_no,
                                                  liw.sk1_item_no,
                                                  cal.fin_year_no,
                                                  cal.fin_week_no,
                                                  cal.fin_week_code,
                                                  liw.sk1_avail_uda_value_no,
                                                  max(liw.sk2_item_no)                  as sk2_item_no,
                                                  max(liw.sk2_location_no)              as sk2_location_no,
                                                  cal.this_week_start_date,
                                                  max(nvl(liw.ch_catalog_ind,0))        as ch_catalog_ind,
                                                  sum(nvl(liw.ch_num_avail_days,0))     as ch_num_avail_days,
                                                  sum(nvl(liw.ch_num_catlg_days,0))     as ch_num_catlg_days,
                                                  sum(nvl(liw.reg_sales_qty_catlg,0))   as reg_sales_qty_catlg,
                                                  sum(nvl(liw.reg_sales_catlg,0))       as reg_sales_catlg,
                                                  sum(case
                                                     when cal.fin_day_no = 7 then nvl(liw.reg_soh_qty_catlg,0)
                                                     end )                              as reg_soh_qty_catlg,
                                                  sum(case
                                                     when cal.fin_day_no = 7 then nvl(liw.reg_soh_selling_catlg,0)
                                                     end )                              as reg_soh_selling_catlg,
                                
                                                  sum(nvl(liw.prom_sales_qty_catlg,0))   as prom_sales_qty_catlg,
                                                  sum(nvl(liw.prom_sales_catlg,0))       as prom_sales_catlg,
                                                  sum(nvl(liw.prom_reg_sales_qty_catlg,0))   as prom_reg_sales_qty_catlg,
                                                  sum(nvl(liw.prom_reg_sales_catlg,0))       as prom_reg_sales_catlg,
                                                  cal.this_week_end_date,
                                                  MAX((case when prod_link_no = 3 then 'CL'
                                                        when prod_link_no = 2 then 'UL'
                                                   else 'CU' end) ) PROD_LINK_TYPE,
                                                  max(LIW.SK1_GROUP_ITEM_NO),
                                                  sum(nvl(liw.Avail_REG_SALES_QTY_CATLG,0)),
                                                  sum(nvl(liw.Avail_REG_SALES_CATLG,0)),
                                                  sum(case
                                                     when cal.fin_day_no = 7 then nvl(liw.avail_reg_soh_qty_catlg,0)
                                                     end )                              as avail_reg_soh_qty_catlg,
                                                  sum(case
                                                     when cal.fin_day_no = 7 then nvl(liw.avail_reg_soh_selling_catlg,0)
                                                     end )                              as avail_reg_soh_selling_catlg,
                                                  sum(nvl(liw.Avail_PROM_SALES_QTY_CATLG,0)),
                                                  sum(nvl(liw.Avail_PROM_SALES_CATLG,0)),
                                                  sum(nvl(liw.Avail_PROM_REG_SALES_QTY_CATLG,0)),
                                                  sum(nvl(liw.Avail_PROM_REG_SALES_CATLG,0)),
                                                  sum(nvl(liw.Avail_CH_NUM_AVAIL_DAYS,0)),
                                                  sum(nvl(liw.Avail_CH_NUM_CATLG_DAYS,0)),
                                                  MAX(PROD_LINK_IND) PROD_LINK_IND -- NEED TO CHECK IF SHOULD RATHER BE PART OF KEY
                                         from     RTL_LOC_ITEM_DY_AST_CATLG liw,
                                                  dim_calendar cal,
                                                  sellnk sp
                                         where    liw.post_date = cal.calendar_date and
                                                  liw.post_date  between g_this_week_start_date and g_this_week_end_date
                                                  and liw.sk1_location_no = sp.sk1_location_no
                                                  and liw.sk1_item_no = sp.sk1_item_no
                                                  and cal.fin_year_no = sp.fin_year_no
                                                   and cal.fin_week_no = sp.fin_week_no
                                                  and liw.sk1_avail_uda_value_no = sp.sk1_avail_uda_value_no
                                         group by  liw.sk1_location_no,
                                                  liw.sk1_item_no,
                                                  cal.fin_year_no,
                                                  cal.fin_week_no,
                                                  cal.fin_week_code,
                                                 liw.sk1_avail_uda_value_no,
                                                  cal.this_week_start_date,
                                                  cal.this_week_end_date 
                                                  ;
                                */

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
g_cnt number := 0;
g_sub1                      number        := 0;
g_start_week                integer       :=  0;
g_start_year                integer       :=  0;
g_this_week_start_date      date          := trunc(sysdate);
g_this_week_end_date        date          := trunc(sysdate);
g_fin_week_code             varchar2(7);
--
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_042U_WL';
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

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.DIM_CONTROL(G_DATE);
    g_date := '6 AUG 2017';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
  FOR g_sub IN 0..5
     LOOP
        g_recs_inserted := 0;
        select fin_year_no, fin_week_no, this_week_start_date, this_week_end_date, fin_week_code
        into   g_start_year, g_start_week, g_this_week_start_date, g_this_week_end_date, g_fin_week_code
        from   dim_calendar
        WHERE calendar_date = g_date - (g_sub * 7);

            G_SUBp1 := G_SUB+1;

            l_text := '---- WEEK '||G_SUBp1||' ----';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

            l_text := '---- WEEK ='||g_start_year||'-'||g_start_week||'---PERIOD='||g_this_week_start_date||'-'||g_this_week_end_date;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


            select fin_year_no,  fin_month_no
            into   g_fin_year_no, g_fin_month_no
            from   dim_calendar
            where calendar_date = g_this_week_start_date + 1
            group by fin_year_no,  fin_month_no;

if g_this_week_start_date = '10 july 2017'
then
        ----------------------------------------------------------
        -- subpartition_name example = RTL_LIDAC_040313
        -- This will do the update stats for the FIRST DAY(subpartition level) OF THE WEEK(partition level)
        ----------------------------------------------------------
   --         l_text := '   ---- Daily SUBPARTITION and PARTITION ----';
   --         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    --            g_subpart_name := 'RTL_LIDAC_'||to_char((g_this_week_start_date + 1 ),'ddmmyy');
    --            l_text := '        subpartition='||g_subpart_name;
   --             dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --            dbms_stats.gather_table_stats ('DWH_PERFORMANCE',
    --                                            'RTL_LOC_ITEM_DY_AST_CATLG',
    --                                            g_subpart_name,
    --                                            granularity => 'SUBPARTITION',
    --                                            degree => 8);
     --          commit;
     --      l_text := '  UPDATE STATS COMPLETED ';
     --      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--NOTE: WHEN WE GO INTO A NEW MONTH THE JOB RUNS VERY LONG SO WE NEED TO RUN STATS ON THE MONTHLY PARTITION AS BELOW:
--THIS SHOULD BE AUTOMATED WITH THE PART NAME CHANGING EACH MONTH 'TP_RTL_LIDAC_M20179'
--------------------------------------------------------------------------------------------------------------------           
--begin
--DBMS_STATS.GATHER_TABLE_STATS (ownname => 'DWH_PERFORMANCE', tabname => 'RTL_LOC_ITEM_DY_AST_CATLG',
--partname => 'TP_RTL_LIDAC_M20179', estimate_percent => 10, degree => 16, granularity => 'PARTITION', cascade => TRUE);
--end;            
----------------------------------------------------------------------------------------------------------------------            

          INSERT /*+ APPEND */ INTO dwh_performance.RTL_LOC_ITEM_WK_AST_CATLG liw
                WITH sellnk
                     AS (  SELECT /*+ materialize full(b) parallel (b,8) */
                                 sk1_location_no,
                                  sk1_item_no,
                                  fin_year_no,
                                  fin_week_no,
                                  sk1_avail_uda_value_no,
                                  MAX (PROD_LINK_no) prod_link_no
                             FROM (SELECT /*+  full(b) materialize full(a) */
                                         sk1_location_no,
                                          sk1_item_no,
                                          post_date,
                                          sk1_avail_uda_value_no,
                                          (CASE
                                              WHEN PROD_LINK_TYPE = 'CL' THEN 3
                                              WHEN PROD_LINK_TYPE = 'UL' THEN 2
                                              ELSE 1
                                           END)
                                             AS PROD_LINK_no
                                     FROM RTL_LOC_ITEM_DY_AST_CATLG A
                                    WHERE POST_DATE BETWEEN g_this_week_start_date
                                                        AND g_this_week_end_date) a,
                                  dim_calendar b
                            WHERE a.post_date = b.calendar_date
                         GROUP BY sk1_location_no,
                                  sk1_item_no,
                                  fin_year_no,
                                  fin_week_no,
                                  sk1_avail_uda_value_no)
                  SELECT /*+ full(LIW) full(cal) parallel (liw,8) */
                        liw.sk1_location_no,
                         liw.sk1_item_no,
                         cal.fin_year_no,
                         cal.fin_week_no,
                         cal.fin_week_code,
                         liw.sk1_avail_uda_value_no,
                         MAX (liw.sk2_item_no) AS sk2_item_no,
                         MAX (liw.sk2_location_no) AS sk2_location_no,
                         cal.this_week_start_date,
                         MAX (NVL (liw.ch_catalog_ind, 0)) AS ch_catalog_ind,
                         SUM (NVL (liw.ch_num_avail_days, 0)) AS ch_num_avail_days,
                         SUM (NVL (liw.ch_num_catlg_days, 0)) AS ch_num_catlg_days,
                         SUM (NVL (liw.reg_sales_qty_catlg, 0)) AS reg_sales_qty_catlg,
                         SUM (NVL (liw.reg_sales_catlg, 0)) AS reg_sales_catlg,
                         SUM (
                            CASE
                               WHEN cal.fin_day_no = 7 THEN NVL (liw.reg_soh_qty_catlg, 0)
                            END)
                            AS reg_soh_qty_catlg,
                         SUM (
                            CASE
                               WHEN cal.fin_day_no = 7 THEN NVL (liw.reg_soh_selling_catlg, 0)
                            END)
                            AS reg_soh_selling_catlg,
                         SUM (NVL (liw.prom_sales_qty_catlg, 0)) AS prom_sales_qty_catlg,
                         SUM (NVL (liw.prom_sales_catlg, 0)) AS prom_sales_catlg,
                         SUM (NVL (liw.prom_reg_sales_qty_catlg, 0))
                            AS prom_reg_sales_qty_catlg,
                         SUM (NVL (liw.prom_reg_sales_catlg, 0)) AS prom_reg_sales_catlg,
                         cal.this_week_end_date,
                         MAX (
                            (CASE
                                WHEN prod_link_no = 3 THEN 'CL'
                                WHEN prod_link_no = 2 THEN 'UL'
                                ELSE 'CU'
                             END))
                            PROD_LINK_TYPE,
                         MAX (LIW.SK1_GROUP_ITEM_NO),
                         SUM (NVL (liw.Avail_REG_SALES_QTY_CATLG, 0)),
                         SUM (NVL (liw.Avail_REG_SALES_CATLG, 0)),
                         SUM (
                            CASE
                               WHEN cal.fin_day_no = 7
                               THEN
                                  NVL (liw.avail_reg_soh_qty_catlg, 0)
                            END)
                            AS avail_reg_soh_qty_catlg,
                         SUM (
                            CASE
                               WHEN cal.fin_day_no = 7
                               THEN
                                  NVL (liw.avail_reg_soh_selling_catlg, 0)
                            END)
                            AS avail_reg_soh_selling_catlg,
                         SUM (NVL (liw.Avail_PROM_SALES_QTY_CATLG, 0)),
                         SUM (NVL (liw.Avail_PROM_SALES_CATLG, 0)),
                         SUM (NVL (liw.Avail_PROM_REG_SALES_QTY_CATLG, 0)),
                         SUM (NVL (liw.Avail_PROM_REG_SALES_CATLG, 0)),
                         SUM (NVL (liw.Avail_CH_NUM_AVAIL_DAYS, 0)),
                         SUM (NVL (liw.Avail_CH_NUM_CATLG_DAYS, 0)),
                         MAX (PROD_LINK_IND) PROD_LINK_IND -- NEED TO CHECK IF SHOULD RATHER BE PART OF KEY
                    FROM (SELECT /*+ materialize parallel (RTL_LOC_ITEM_DY_AST_CATLG,8)  full (RTL_LOC_ITEM_DY_AST_CATLG)*/
                                *
                            FROM RTL_LOC_ITEM_DY_AST_CATLG
                           WHERE post_date BETWEEN g_this_week_start_date
                                                        AND g_this_week_end_date) liw,
                         dim_calendar cal,
                         sellnk sp
                   WHERE     liw.post_date = cal.calendar_date
                         AND liw.sk1_location_no = sp.sk1_location_no
                         AND liw.sk1_item_no = sp.sk1_item_no
                         AND cal.fin_year_no = sp.fin_year_no
                         AND cal.fin_week_no = sp.fin_week_no
                         AND liw.sk1_avail_uda_value_no = sp.sk1_avail_uda_value_no
                GROUP BY liw.sk1_location_no,
                         liw.sk1_item_no,
                         cal.fin_year_no,
                         cal.fin_week_no,
                         cal.fin_week_code,
                         liw.sk1_avail_uda_value_no,
                         cal.this_week_start_date,
                         cal.this_week_end_date
                ;

  

     g_recs_read := g_recs_read + SQL%ROWCOUNT;
     g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


     l_text := 'Insert NEW:- RECS =  '||g_recs_inserted||' '||g_this_week_start_date||'  To '||g_this_week_end_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     commit;
end if;

    end loop;

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


END WH_PRF_AST_042U_WL;
