--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_284U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_284U" 
  (p_forall_limit in integer,p_success out boolean)
  as
-- *************************************************************************************************
-- * Notes from 12.2 upgrade performance tuning
-- *************************************************************************************************
-- Date:   2019-01-25
-- Author: Paul Wakefield
-- 1. Tweaked hints on INSERT
-- **************************************************************************************************

  --**************************************************************************************************
  -- test version for prd
    --**************************************************************************************************
  --  Date:        December 2014
  --  Author:      Q Smit
  --  Purpose:     Load Foods Sales data where there have been no promotions
  --               during last 12 weeks - at zone / item / locationlevel.
  --               This is a re-write of the existing program (WH_PRF_CORP_282U) due to there being 
  --               more rules applied as to how the 6wk avg values are to be calculated based on 
  --               whether the item was on/ not on promotion and the number of weeks it was on/not on promtion.
  --               The rules are explained in a bit more detail in the program code.
  --               These values will be used to calculate 6wk average values.
  --  Tables:      Input  - RTL_LOC_ITEM_WK_RMS_DENSE(dns) ,
  --                        DIM_PROM(dp),
  --                        DIM_PROM_ITEM_ALL(dpia),
  --                        DIM_LOCATION(dl),
  --                        FND_PROM_LOCATION(fpl) ,
  --                        DIM_ITEM (di),
  --                        DIM_ITEM_UDA(diu),
  --                        FND_UDA_VALUE(fuv),
  --                        FND_ITEM_UDA(fia),
  --                        DIM_CALENDAR(dc)
  --
  --               Output - MART_FD_ZONE_LOC_ITEM_6WKAVG
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --
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
      g_chain_corporate    integer       :=  0;
      g_chain_franchise    integer       :=  0;
      g_fin_week_no        DIM_CALENDAR.fin_week_no%type;
      g_fin_year_no        DIM_CALENDAR.fin_year_no%type;
      g_ly_fin_week_no     DIM_CALENDAR.fin_week_no%type;
      g_ly_fin_year_no     DIM_CALENDAR.fin_year_no%type;
      g_lcw_fin_week_no    DIM_CALENDAR.fin_week_no%type;
      g_lcw_fin_year_no    DIM_CALENDAR.fin_year_no%type;
      g_date               date;
      g_start_date         date;
      g_end_date           date;
      g_ly_start_date      date;
      g_ly_end_date        date;
      g_start_12wk         date;
      g_end_12wk           date;
      g_item_no                    number := 0;
      g_sk1_item_no                number := 0;
      g_item_nod                   number := 0;
      g_recs                       number := 0;
      g_accum_sales_6wk_qty        number := 0;
      g_accum_sales_6wk            number := 0;
      g_accum_sales_6wk_margin     number := 0;
      g_accum_waste_6wk_promo_cost number := 0;
      g_sales_6wk_qty              number := 0;
      g_sales_6wk                  number := 0;
      g_sales_6wk_margin           number := 0;
      g_waste_6wk_promo_cost       number := 0;
      g_sub                        number := 0;
      g_wkcnt                      number := 0;
  
  
  
  l_message            sys_dwh_errlog.log_text%type;
  l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_284U';
  l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
  l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
  l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
  l_text               sys_dwh_log.log_text%type ;
  l_description        sys_dwh_log_summary.log_description%type  := 'LOAD foods 6wk records by zone/loc/itm';
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
     l_text := 'LOAD OF TEMP_MART_FD_LOC_ITEM_WK_6WKAVG_Q STARTED AT '||
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
  
     execute immediate 'alter session enable parallel dml';
     l_text := 'alter session enable parallel dml';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
     EXECUTE immediate('truncate table DWH_PERFORMANCE.mart_fd_zone_loc_item_6wkavg');
     l_text := 'truncate table DWH_PERFORMANCE.mart_fd_zone_loc_item_6wkavg';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
    COMMIT;
  
  
   --    execute immediate('create index dwh_performance.i50_p_tmp_mrt_6wkavg_PRM_DTS on dwh_performance.tmp_mart_6wkavg_prom_dates
   --                            (SK1_PROM_NO) TABLESPACE PRF_MASTER') ;
   --   l_text := 'create index dwh_performance.i50_p_tmp_mrt_6wkavg_PRM_DTS';
   --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  
  --**************************************************************************************************
  -- Determine period of extract
  --**************************************************************************************************
  
     select last_wk_start_date-77,
            last_wk_end_date
     into   g_start_12wk,
            G_END_12WK
     from   dim_control;
  
     --G_START_12WK := '25-AUG-14';
     --G_END_12WK   := '16-NOV-14';
  
     l_text := 'PERIOD='||g_start_12wk||' - '||g_end_12wk;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  
     g_recs := 0;
  
  --**************************************************************************************************
  -- GATHER STATS
  --**************************************************************************************************
  /*    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','mart_fd_zone_loc_item_6wkavg', DEGREE => 8);
      commit;
      l_text := 'GATHER STATS on dwh_performance.mart_fd_zone_loc_item_6wkavg';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  */
  --**************************************************************************************************
  -- Main extract
  --**************************************************************************************************
  
   insert /*+ APPEND parallel (aps,2) */ into DWH_PERFORMANCE.mart_fd_zone_loc_item_6wkavg aps
              
WITH 
            selcal AS
                          (
                            SELECT distinct  fin_year_no,    fin_week_no
                            FROM dwh_performance.dim_calendar
                            where calendar_date between G_START_12WK and G_END_12WK  --   '25-AUG-14' and '16-NOV-14'
                            ORDER BY  fin_year_no,   fin_week_no
                           )
                           ,
            
             -- SELECT THE ITEM AT LOCATION LEVEL WITH THE INDICATOR (MAXPROM) TO SHOW IF THE ITEM WAS ON PROMOTION PER WEEK
             --=============================================================================================================
             selloc AS
                            (
                              SELECT /*+ parallel(4) */
                                          SK1_ITEM_NO,    item_no,    fin_year_no,    fin_week_no,    week_count,    MAX(sk1_prom_no) maxprom,
                                          tmpm.sk1_location_no, tmpm.location_no, dl.sk1_fd_zone_group_zone_no sk1_zone_group_zone_no, dz.zone_no
                              FROM DWH_PERFORMANCE.tmp_mart_6wkavg_prom_dates tmpm,
                                   dim_location dl,
                                   dim_zone dz
                         --     where tmpm.sk1_item_no in (15846679)
                                    where tmpm.sk1_location_no = dl.sk1_location_no
                                    and dl.sk1_fd_zone_group_zone_no = dz.sk1_zone_group_zone_no
                               group by SK1_ITEM_NO, item_no, fin_year_no, fin_week_no, week_count, tmpm.sk1_location_no, tmpm.location_no, dl.sk1_fd_zone_group_zone_no, dz.zone_no
                         )  --select * from selloc where sk1_item_no = 12755 and sk1_location_no = 379 order by fin_week_no desc;
      ,
             
             -- GET SALES PER ITEM PER LOCATION PER WEEK   -- around 12 million records for the 12-week period
             --=========================================
             sales as (select /*+ PARALLEL(4) */ dense.sk1_item_no, dense.sk1_location_no, dense.fin_year_no, dense.fin_week_no, 
                              week_count, maxprom, location_no, item_no, sk1_zone_group_zone_no, zone_no , sales, sales_qty, sales_margin  , nvl(waste_cost,0) waste_cost
                         from rtl_loc_item_wk_rms_dense dense
                         join selloc on dense.sk1_item_no = selloc.sk1_item_no 
                                    and dense.sk1_location_no = selloc.sk1_location_no
                                    and dense.fin_year_no     = selloc.fin_year_no
                                    and dense.fin_week_no     = selloc.fin_week_no
                        
              left outer join rtl_loc_item_wk_rms_sparse sparsewk on sparsewk.sk1_location_no = dense.sk1_location_no
                          and sparsewk.sk1_item_no     = dense.sk1_item_no
                          and sparsewk.fin_year_no     = dense.fin_year_no
                          and sparsewk.fin_week_no     = dense.fin_week_no
                          
             -- where dense.sk1_item_no = 12755 and dense.sk1_location_no = 379 
                       ) --select * from sales order by fin_week_no desc;
     ,        
            
             -- rule 1 - NOT ON promotion for last (FIRST FROM BACK) 6 weeks of the 12 week period  (WEEKS 7 - 12)  
             -- THIS WOULD MEAN THE ITEM WAS NOT ON PROMOTION IN THE LAST 6 WEEKS.
             --===================================================================================================
             no_prom_first_6wks AS
                        (SELECT /*+ PARALLEL (sp, 4) */
                         SK1_ITEM_NO,    sk1_location_no,    COUNT(DISTINCT week_count) diswkcnt
                         FROM sales sp
                          WHERE maxprom IS NULL
                          AND week_count   >= 7
                          GROUP BY SK1_ITEM_NO,    sk1_location_no
                          HAVING COUNT(DISTINCT week_count) >= 6
                          order by sk1_item_no, sk1_location_no
                          ) --select * from no_prom_first_6wks;   -- where sk1_item_no = 12755 and sk1_location_no = 379 ; --where sk1_item_no = 19089216;  --188724 ;
                          ,
         
              -- WILL GIVE THE WEEKS THAT THE ITEM WAS NOT ON PROMOTION  
              --=======================================================
              selext_noprom AS
                         (
                       SELECT /*+ PARALLEL (sp2, 4) PARALLEL(np6wk,4) */ 
                                 sp2.sk1_item_no, sp2.sk1_location_no, sp2.item_no, sp2.location_no, fin_year_no, fin_week_no, week_count, 
                                 sk1_zone_group_zone_no, zone_no , sales, sales_qty, sales_margin, waste_cost
                            FROM sales sp2, no_prom_first_6wks np6wk
                            WHERE maxprom IS NULL
                              AND  sp2.sk1_item_no = np6wk.sk1_item_no 
                              and sp2.sk1_location_no = np6wk.sk1_location_no
               --               and sales > 0
                            ORDER BY sk1_item_no, sk1_location_no, fin_year_no,    fin_week_no
                          ) --select * from selext_noprom;   -- where sk1_item_no = 12755 and sk1_location_no = 383;
                          ,      
              -- GET ALL THE WEEKS BY RANK THAT HAD SALES (SALES > 0)   
              --=====================================================
              selrnk_rule1 AS
                          (SELECT srk.sk1_item_no, srk.sk1_location_no, srk.item_no, srk.location_no, srk.fin_year_no, srk.fin_week_no, srk.week_count, 
                                  srk.sk1_zone_group_zone_no, srk.zone_no ,srk.sales, srk.sales_qty, srk.sales_margin, srk.waste_cost, srk.rnk
                          FROM
                                (SELECT sk1_item_no, sk1_location_no, item_no, location_no,  fin_year_no, fin_week_no, week_count , sk1_zone_group_zone_no, zone_no ,
                                  dense_rank() over (partition BY sk1_item_no, sk1_location_no order by week_count) rnk, sales, sales_qty, sales_margin, waste_cost
                                FROM selext_noprom se 
                                where sales > 0 ) srk
                          --WHERE rnk >= 6
                         -- where sk1_item_no = 12755 and sk1_location_no = 383
                          ORDER BY    fin_year_no,    fin_week_no, sk1_item_no, sk1_location_no
                          )  --   select * from selrnk_rule1;
              , 
                -- GET MAX RANK THAT HAD SALES (SALES > 0)
                --========================================
                selmaxrnk as (select sk1_item_no, sk1_location_no, max(rnk) maxrnk from selrnk_rule1 group by sk1_item_no, sk1_location_no) --select * from selmaxrnk;
              ,
                selranks as (select sk1_item_no, sk1_location_no, maxrnk, 
                                  case 
                                   when maxrnk > 6 then (maxrnk - 5) else 1 
                                 end as startrnk,
                                  case 
                                   when maxrnk > 6 then 6 else maxrnk
                                 end as num_weeks
                               from selmaxrnk ) --select * from selranks;
             ,   
                --SUM THE VALUES FOR THE WEEKS, GET AVG ACROSS THE NUMBER OF WEEKS USED
              --=====================================================================
                sel_data_rule1 as 
                  (
                    select s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no ,
                           sum(sales)                                         sales_6wk, 
                           sum(sales_qty)                                     sales_6wk_qty, 
                           sum(sales_margin)                                  sales_6wk_margin, 
                           sum(waste_cost)                                    waste_6wk_promo_cost,
                           round(sum((sales) / sr.num_weeks),2)               sales_6wk_avg,
                           round(sum((sales_qty) / sr.num_weeks),2)           sales_qty_6wk_avg,
                           round( ( (sum(sales_margin) / sum(sales)) /6), 2)  sales_margin_6wk_avg,
                           round(sum((waste_cost) / sr.num_weeks),2)          waste_cost_6wk_avg,
                           '1'                                                AS RULE_MATCHED
                      from selrnk_rule1 s1, selranks sr
                     where s1.sk1_item_no = sr.sk1_item_no
                       and s1.sk1_location_no = sr.sk1_location_no
                       and s1.rnk between sr.startrnk and sr.maxrnk
                     group by s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no, '1'
                  ) --select * from sel_data_rule1;
                
   ,          
              -- rule 2 - ON promotion for 6 consequtive weeks from the last completed week backwards   ( WEEKS 7 - 12)
              --=======================================================================================================
              prom6wks AS 
                          (
                          SELECT /*+ PARALLEL (sp, 4) */
                                 SK1_ITEM_NO,    sk1_location_no,    COUNT(DISTINCT week_count) diswkcnt
                          FROM sales sp
                          WHERE maxprom IS NOT NULL
                          AND week_count   >= 7
                          and not exists (select sk1_item_no, sk1_location_no from no_prom_first_6wks p1 
                                             where sp.sk1_location_no = p1.sk1_location_no 
                                               and sp.sk1_item_no = p1.sk1_item_no)
                          GROUP BY SK1_ITEM_NO,    sk1_location_no
                          HAVING COUNT(DISTINCT week_count) >= 6
                          order by sk1_item_no, sk1_location_no
                          ) --select * from prom6wks;  -- where sk1_item_no = 12755 and sk1_location_no = 379 ; --where sk1_item_no = 19089216;  --188724 ;
                          ,
              -- WILL GIVE THE WEEKS THAT THE ITEM WAS ON PROMOTION
              --====================================================
              selext_prom AS
                         (
                            SELECT /*+ PARALLEL (sp2, 4) PARALLEL(np6wk,4) */ 
                                 sp2.sk1_item_no, sp2.sk1_location_no, sp2.item_no, sp2.location_no, fin_year_no, fin_week_no, week_count, 
                                 sk1_zone_group_zone_no, zone_no , sales, sales_qty, sales_margin, waste_cost
                            FROM sales sp2, prom6wks p6wk
                            WHERE maxprom IS NOT NULL
                              AND  sp2.sk1_item_no = p6wk.sk1_item_no 
                              and sp2.sk1_location_no = p6wk.sk1_location_no
                            ORDER BY sk1_item_no, sk1_location_no, fin_year_no,    fin_week_no
                          ) --select * from selext_prom;  -- where sk1_item_no = 12755 and sk1_location_no = 383;
                          ,      
              -- GET ALL THE WEEKS BY RANK THAT HAD SALES (SALES > 0)        
              -- ====================================================
              selrnk_rule2 AS
                          (SELECT srk.sk1_item_no, srk.sk1_location_no, srk.item_no, srk.location_no, srk.fin_year_no,  srk.fin_week_no, 
                                  srk.week_count, srk.sk1_zone_group_zone_no, srk.zone_no ,srk.sales, srk.sales_qty, srk.sales_margin, srk.waste_cost, srk.rnk
                          FROM
                                (SELECT sk1_item_no, sk1_location_no, item_no, location_no,  fin_year_no, fin_week_no, week_count , sk1_zone_group_zone_no, zone_no ,
                                  dense_rank() over (partition BY sk1_item_no, sk1_location_no order by week_count) rnk, sales, sales_qty, sales_margin, waste_cost
                                FROM selext_prom se 
                                where sales > 0 ) srk
                          --WHERE rnk >= 6
                         -- where sk1_item_no = 12755 and sk1_location_no = 383
                          ORDER BY    fin_year_no,    fin_week_no, sk1_item_no, sk1_location_no

                          )   -- SELECT * FROM SELRNK_RULE2; 
              , 
                -- GET MAX RANK THAT HAD SALES (SALES > 0)
                -- =======================================
                selmaxrnk2 as (select sk1_item_no, sk1_location_no, max(rnk) maxrnk from selrnk_rule2 group by sk1_item_no, sk1_location_no) --select * from selmaxrnk;
              ,
                -- GET THE START AND END RANKS - IF LESS THAN 6 WEEKS THEN SET START = 1 AND END = MAX RANK
                -- ========================================================================================
                selranks2 as (select sk1_item_no, sk1_location_no, maxrnk, 
                                  case 
                                   when maxrnk > 6 then (maxrnk - 5) else 1 
                                 end as startrnk,
                                 case 
                                   when maxrnk > 6 then 6 else maxrnk
                                 end as num_weeks
                               from selmaxrnk2 ) --select * from selranks2;
             ,   
              --SUM THE VALUES FOR THE WEEKS, GET AVG ACROSS THE NUMBER OF WEEKS USED
              --=====================================================================
                sel_data_rule2 as 
                  (
                    select s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no ,
                           sum(sales)                                         sales_6wk, 
                           sum(sales_qty)                                     sales_6wk_qty, 
                           sum(sales_margin)                                  sales_6wk_margin, 
                           sum(waste_cost)                                    waste_6wk_promo_cost,
                           round(sum((sales) / sr.num_weeks),2)               sales_6wk_avg,
                           round(sum((sales_qty) / sr.num_weeks),2)           sales_qty_6wk_avg,
                           round( ( (sum(sales_margin) / sum(sales)) /6), 2)  sales_margin_6wk_avg,
                           round(sum((waste_cost) / sr.num_weeks),2)          waste_cost_6wk_avg,
                           '2'                                                AS RULE_MATCHED
                      from selrnk_rule2 s1, selranks2 sr
                     where s1.sk1_item_no = sr.sk1_item_no
                       and s1.sk1_location_no = sr.sk1_location_no
                       and s1.rnk between sr.startrnk and sr.maxrnk
                     group by s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no, '2'
                  ) --select * from sel_data_rule2;
        ,
              -- rule 3 - ON promotion for any 6 consequtive weeks within the 12 week period      (WEEKS 1 - 12)
              --================================================================================================
               prom_any_consec_6wks AS 
                          (
                          SELECT /*+ PARALLEL (sp, 4) */ 
                          SK1_ITEM_NO,    sk1_location_no,    COUNT(DISTINCT week_count) diswkcnt
                        FROM sales sp
                          WHERE maxprom IS NOT NULL
                          AND week_count   <= 11
                         
                          and not exists (select sk1_item_no, sk1_location_no from selext_prom p1 
                                             where sp.sk1_location_no = p1.sk1_location_no 
                                               and sp.sk1_item_no = p1.sk1_item_no)
                          and not exists (select sk1_item_no, sk1_location_no from no_prom_first_6wks p2 
                                             where sp.sk1_location_no = p2.sk1_location_no 
                                               and sp.sk1_item_no = p2.sk1_item_no)
                         
                          GROUP BY SK1_ITEM_NO,    sk1_location_no
                          HAVING COUNT(DISTINCT week_count) = 6
                          order by sk1_item_no, sk1_location_no
                          ) --select * from prom_any_consec_6wks;  -- where sk1_item_no = 12755 and sk1_location_no = 379 ; --where sk1_item_no = 19089216;  --188724 ;
                          ,
             
              -- WILL GIVE THE WEEKS THAT THE ITEM WAS ON PROMOTION
              -- ==================================================
              selext_prom_any6 AS
                         (
                            SELECT /*+ PARALLEL (sp2, 4) PARALLEL(np6wk,4) */ 
                                 sp2.sk1_item_no, sp2.sk1_location_no, sp2.item_no, sp2.location_no, fin_year_no, fin_week_no, week_count, 
                                 sk1_zone_group_zone_no, zone_no , sales, sales_qty, sales_margin, waste_cost
                            FROM sales sp2, prom_any_consec_6wks p6wk
                            WHERE maxprom IS NOT NULL
                              AND  sp2.sk1_item_no = p6wk.sk1_item_no 
                              and sp2.sk1_location_no = p6wk.sk1_location_no
                       --       and sales > 0
                            ORDER BY sk1_item_no, sk1_location_no, fin_year_no,    fin_week_no
                          )  --select * from selext_prom_any6;  -- where sk1_item_no = 12755 and sk1_location_no = 383;
                          ,      
                          
              -- GET ALL THE WEEKS BY RANK THAT HAD SALES (SALES > 0)     
              -- ====================================================
              selrnk_rule3 AS
                          (SELECT srk.sk1_item_no, srk.sk1_location_no, srk.item_no, srk.location_no, srk.fin_year_no, srk.fin_week_no, srk.week_count, 
                                  srk.sk1_zone_group_zone_no, srk.zone_no ,srk.sales, srk.sales_qty, srk.sales_margin, srk.waste_cost, srk.rnk
                          FROM
                                (SELECT sk1_item_no, sk1_location_no, item_no, location_no,  fin_year_no, fin_week_no, week_count , sk1_zone_group_zone_no, zone_no ,
                                  dense_rank() over (partition BY sk1_item_no, sk1_location_no order by week_count) rnk, sales, sales_qty, sales_margin, waste_cost
                                FROM selext_prom_any6 se 
                                where sales > 0 ) srk
                          --WHERE rnk >= 6
                         -- where sk1_item_no = 12755 and sk1_location_no = 383
                          ORDER BY    fin_year_no,    fin_week_no, sk1_item_no, sk1_location_no

                          )    --SELECT * FROM SELRNK_RULE3; 
              , 
                -- GET MAX RANK THAT HAD SALES (SALES > 0)
                -- =======================================
                selmaxrnk3 as (select sk1_item_no, sk1_location_no, max(rnk) maxrnk from selrnk_rule3 group by sk1_item_no, sk1_location_no) --select * from selmaxrnk;
              ,
                -- GET THE START AND END RANKS - IF LESS THAN 6 WEEKS THEN SET START = 1 AND END = MAX RANK
                -- ========================================================================================
                selranks3 as (select sk1_item_no, sk1_location_no, maxrnk, 
                                  case 
                                   when maxrnk > 6 then (maxrnk - 5) else 1 
                                 end as startrnk,
                                 case 
                                   when maxrnk > 6 then 6 else maxrnk
                                 end as num_weeks
                               from selmaxrnk3 ) --select * from selranks3;
             ,   
              --SUM THE VALUES FOR THE WEEKS, GET AVG ACROSS THE NUMBER OF WEEKS USED
              --=====================================================================
                sel_data_rule3 as 
                  (
                    select s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no ,
                           sum(sales)                                         sales_6wk, 
                           sum(sales_qty)                                     sales_6wk_qty, 
                           sum(sales_margin)                                  sales_6wk_margin, 
                           sum(waste_cost)                                    waste_6wk_promo_cost,
                           round(sum((sales) / sr.num_weeks),2)               sales_6wk_avg,
                           round(sum((sales_qty) / sr.num_weeks),2)           sales_qty_6wk_avg,
                           round( ( (sum(sales_margin) / sum(sales)) /6), 2)  sales_margin_6wk_avg,
                           round(sum((waste_cost) / sr.num_weeks),2)          waste_cost_6wk_avg,
                           '3'                                               AS RULE_MATCHED
                      from selrnk_rule3 s1, selranks3 sr
                     where s1.sk1_item_no = sr.sk1_item_no
                       and s1.sk1_location_no = sr.sk1_location_no
                       and s1.rnk between sr.startrnk and sr.maxrnk
                     group by s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no, '3 '
                  ) --select * from sel_data_rule3;
        ,
              
              -- rule 4 - NOT ON promotion for any NON-CONSECUTIVE 6 weeks of the 12 week period rolling backwards (from wk12 back)         
              --=====================================================================================
              no_prom_any_6wks AS
                         (
                          SELECT SK1_ITEM_NO,    sk1_location_no,    COUNT(DISTINCT week_count) diswkcnt
                            FROM sales sp
                           WHERE maxprom IS NULL
                           AND week_count   >= 6
                           
                           and not exists (select sk1_item_no, sk1_location_no from selext_prom p1 
                                             where sp.sk1_location_no = p1.sk1_location_no 
                                               and sp.sk1_item_no = p1.sk1_item_no)
                           and not exists (select sk1_item_no, sk1_location_no from no_prom_first_6wks p2 
                                             where sp.sk1_location_no = p2.sk1_location_no 
                                               and sp.sk1_item_no = p2.sk1_item_no)
                           and not exists (select sk1_item_no, sk1_location_no from selext_prom_any6 p3
                                             where sp.sk1_location_no = p3.sk1_location_no 
                                               and sp.sk1_item_no = p3.sk1_item_no)
                                               
                        GROUP BY SK1_ITEM_NO,    sk1_location_no
                          HAVING COUNT(DISTINCT week_count) < 6
                          order by sk1_item_no, sk1_location_no
                          ) --select * from no_prom_any_6wks ;  --where sk1_item_no = 64800 and sk1_location_no = 500 ;
                          ,
              
            -- WILL GIVE THE WEEKS THAT THE ITEM WAS ON PROMOTION
            -- ==================================================
            selext_no_prom_any_6wks AS
                                     (
                            SELECT /*+ PARALLEL (sp2, 4) PARALLEL(np6wk,4) */ 
                                 sp2.sk1_item_no, sp2.sk1_location_no, sp2.item_no, sp2.location_no, fin_year_no, fin_week_no, week_count, 
                                 sk1_zone_group_zone_no, zone_no , sales, sales_qty, sales_margin, waste_cost
                           FROM sales sp2, no_prom_any_6wks pan6wk
                            WHERE maxprom IS NULL
                              AND  sp2.sk1_item_no = pan6wk.sk1_item_no 
                              and sp2.sk1_location_no = pan6wk.sk1_location_no
                       --       and sales > 0
                            ORDER BY sk1_item_no, sk1_location_no, fin_year_no,    fin_week_no
                          )-- select * from selext_no_prom_any_6wks where sk1_item_no = 64800 and sk1_location_no = 500  order by fin_Week_no desc;
                          ,      
              -- GET ALL THE WEEKS BY RANK THAT HAD SALES (SALES > 0)    
              -- ====================================================
              selrnk_rule4 AS
                          (SELECT srk.sk1_item_no, srk.sk1_location_no, srk.item_no, srk.location_no, srk.fin_year_no, srk.fin_week_no, srk.week_count, 
                                  srk.sk1_zone_group_zone_no, srk.zone_no ,srk.sales, srk.sales_qty, srk.sales_margin, srk.waste_cost, srk.rnk
                          FROM
                                (SELECT sk1_item_no, sk1_location_no, item_no, location_no,  fin_year_no, fin_week_no, week_count , sk1_zone_group_zone_no, zone_no ,
                                  dense_rank() over (partition BY sk1_item_no, sk1_location_no order by week_count) rnk, sales, sales_qty, sales_margin, waste_cost
                                FROM selext_no_prom_any_6wks se 
                                where sales > 0 ) srk
                          --WHERE rnk >= 6
                         -- where sk1_item_no = 12755 and sk1_location_no = 383
                          ORDER BY    fin_year_no,    fin_week_no, sk1_item_no, sk1_location_no

                          )   --SELECT * FROM selrnk_rule4; 
              , 
                -- GET MAX RANK THAT HAD SALES (SALES > 0)
                -- =======================================
                selmaxrnk4 as (select sk1_item_no, sk1_location_no, max(rnk) maxrnk from selrnk_rule4 group by sk1_item_no, sk1_location_no) --select * from selmaxrnk4;
              ,
                -- GET THE START AND END RANKS - IF LESS THAN 6 WEEKS THEN SET START = 1 AND END = MAX RANK
                -- ========================================================================================
                selranks4 as (select sk1_item_no, sk1_location_no, maxrnk, 
                                  case 
                                   when maxrnk > 6 then (maxrnk - 5) else 1 
                                 end as startrnk,
                                 case 
                                   when maxrnk > 6 then 6 else maxrnk
                                 end as num_weeks
                               from selmaxrnk4 ) --select * from selranks4;
             ,   
              --SUM THE VALUES FOR THE WEEKS, GET AVG ACROSS THE NUMBER OF WEEKS USED
              --=====================================================================
                sel_data_rule4 as 
                  (
                    select s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no ,
                           sum(sales)                                         sales_6wk, 
                           sum(sales_qty)                                     sales_6wk_qty, 
                           sum(sales_margin)                                  sales_6wk_margin, 
                           sum(waste_cost)                                    waste_6wk_promo_cost,
                           round(sum((sales) / sr.num_weeks),2)               sales_6wk_avg,
                           round(sum((sales_qty) / sr.num_weeks),2)           sales_qty_6wk_avg,
                           round( ( (sum(sales_margin) / sum(sales)) /6), 2)  sales_margin_6wk_avg,
                           round(sum((waste_cost) / sr.num_weeks),2)          waste_cost_6wk_avg,
                           '4'                                                AS RULE_MATCHED
                      from selrnk_rule4 s1, selranks4 sr
                     where s1.sk1_item_no = sr.sk1_item_no
                       and s1.sk1_location_no = sr.sk1_location_no
                       and s1.rnk between sr.startrnk and sr.maxrnk
                     group by s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no, '4'
                  ),
                  
             --=====================================================================================
              -- THE REST - ITEMS THAT HAD LESS THAN 6 WEEKS OF SALES  
              --=====================================================================================
              less_than_6wk_sales as 
                         (
                          SELECT /*+ FULL(sp) PARALLEL (sp, 4) */  SK1_ITEM_NO,    sk1_location_no,    COUNT(DISTINCT week_count) diswkcnt
                            FROM sales sp
                           where  not exists (select sk1_item_no, sk1_location_no from no_prom_first_6wks p1 
                                             where sp.sk1_location_no = p1.sk1_location_no 
                                               and sp.sk1_item_no = p1.sk1_item_no)
                           and not exists (select sk1_item_no, sk1_location_no from prom6wks p2 
                                             where sp.sk1_location_no = p2.sk1_location_no 
                                               and sp.sk1_item_no = p2.sk1_item_no)
                           and not exists (select sk1_item_no, sk1_location_no from prom_any_consec_6wks p3
                                             where sp.sk1_location_no = p3.sk1_location_no 
                                               and sp.sk1_item_no = p3.sk1_item_no)
                            and not exists (select sk1_item_no, sk1_location_no from no_prom_any_6wks p4
                                             where sp.sk1_location_no = p4.sk1_location_no 
                                               and sp.sk1_item_no = p4.sk1_item_no)
                                               
                        GROUP BY SK1_ITEM_NO,    sk1_location_no
                         
                          ) --select * from the_rest ;
  ,
               -- WILL GIVE THE WEEKS THAT THE ITEM WAS ON PROMOTION
            -- ==================================================
            selext_less_than_6wks AS
                                     (
                            SELECT /*+ PARALLEL (sp2, 4) PARALLEL(np6wk,4) */ 
                                 sp2.sk1_item_no, sp2.sk1_location_no, sp2.item_no, sp2.location_no, fin_year_no, fin_week_no, week_count, 
                                 sk1_zone_group_zone_no, zone_no , sales, sales_qty, sales_margin, waste_cost
                           FROM sales sp2, less_than_6wk_sales less6wk
                       --     WHERE maxprom IS NULL
                              where  sp2.sk1_item_no = less6wk.sk1_item_no 
                              and sp2.sk1_location_no = less6wk.sk1_location_no
                       --       and sales > 0
                            ORDER BY sk1_item_no, sk1_location_no, fin_year_no,    fin_week_no
                          )-- select * from selext_no_prom_any_6wks where sk1_item_no = 64800 and sk1_location_no = 500  order by fin_Week_no desc;
                          ,      
              -- GET ALL THE WEEKS BY RANK THAT HAD SALES (SALES > 0)    
              -- ====================================================
              selrnk_rule5 AS
                          (SELECT srk.sk1_item_no, srk.sk1_location_no, srk.item_no, srk.location_no, srk.fin_year_no, srk.fin_week_no, srk.week_count, 
                                  srk.sk1_zone_group_zone_no, srk.zone_no ,srk.sales, srk.sales_qty, srk.sales_margin, srk.waste_cost, srk.rnk
                          FROM
                                (SELECT sk1_item_no, sk1_location_no, item_no, location_no,  fin_year_no, fin_week_no, week_count , sk1_zone_group_zone_no, zone_no ,
                                  dense_rank() over (partition BY sk1_item_no, sk1_location_no order by week_count) rnk, sales, sales_qty, sales_margin, waste_cost
                                FROM selext_less_than_6wks se 
                                where sales > 0 ) srk
                          --WHERE rnk >= 6
                         -- where sk1_item_no = 12755 and sk1_location_no = 383
                          ORDER BY    fin_year_no,    fin_week_no, sk1_item_no, sk1_location_no

                          )   --SELECT * FROM selrnk_rule5; 
              , 
                -- GET MAX RANK THAT HAD SALES (SALES > 0)
                -- =======================================
                selmaxrnk5 as (select sk1_item_no, sk1_location_no, max(rnk) maxrnk from selrnk_rule5 group by sk1_item_no, sk1_location_no) --select * from selmaxrnk4;
              ,
                -- GET THE START AND END RANKS - IF LESS THAN 6 WEEKS THEN SET START = 1 AND END = MAX RANK
                -- ========================================================================================
                selranks5 as (select sk1_item_no, sk1_location_no, maxrnk, 
                                  case 
                                   when maxrnk > 6 then (maxrnk - 5) else 1 
                                 end as startrnk,
                                 case 
                                   when maxrnk > 6 then 6 else maxrnk
                                 end as num_weeks
                               from selmaxrnk5 ) --select * from selranks5;
             ,   
              --SUM THE VALUES FOR THE WEEKS, GET AVG ACROSS THE NUMBER OF WEEKS USED
              --=====================================================================
                sel_data_rule5 as 
                  (
                    select s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no ,
                           sum(sales)                                         sales_6wk, 
                           sum(sales_qty)                                     sales_6wk_qty, 
                           sum(sales_margin)                                  sales_6wk_margin, 
                           sum(waste_cost)                                    waste_6wk_promo_cost,
                           round(sum((sales) / sr.num_weeks),2)               sales_6wk_avg,
                           round(sum((sales_qty) / sr.num_weeks),2)           sales_qty_6wk_avg,
                           round( ( (sum(sales_margin) / sum(sales)) /6), 2)  sales_margin_6wk_avg,
                           round(sum((waste_cost) / sr.num_weeks),2)          waste_cost_6wk_avg,
                           '5'                                                 AS RULE_MATCHED
                      from selrnk_rule5 s1, selranks5 sr
                     where s1.sk1_item_no = sr.sk1_item_no
                       and s1.sk1_location_no = sr.sk1_location_no
                       and s1.rnk between sr.startrnk and sr.maxrnk
                     group by s1.sk1_item_no, s1.sk1_location_no, item_no, location_no, sk1_zone_group_zone_no, zone_no, '5'
                  ) 
                  
SELECT NVL(NVL(NVL(NVL(R1.ZONE_NO,      R2.ZONE_NO),     R3.ZONE_NO),     R4.ZONE_NO),     R5.ZONE_NO)                                                            AS ZONE_NO,
       NVL(NVL(NVL(NVL(R1.LOCATION_NO,  R2.LOCATION_NO), R3.LOCATION_NO), R4.LOCATION_NO), R5.LOCATION_NO)                                                        AS LOCATION_NO,
       NVL(NVL(NVL(NVL(R1.ITEM_NO,      R2.ITEM_NO),     R3.ITEM_NO),     R4.ITEM_NO),     R5.ITEM_NO)                                                            AS ITEM_NO,
       G_START_12WK                                                                                                                                               AS EXTRACT_START_DATE,
       G_END_12WK                                                                                                                                                 AS EXTRACT_END_DATE,
       NVL(NVL(NVL(NVL(R1.SALES_6WK_QTY,          R2.SALES_6WK_QTY),          R3.SALES_6WK_QTY),          R4.SALES_6WK_QTY),          R5.SALES_6WK_QTY)           AS SALES_6WK_QTY,
       NVL(NVL(NVL(NVL(R1.SALES_QTY_6WK_AVG,      R2.SALES_QTY_6WK_AVG),      R3.SALES_QTY_6WK_AVG),      R4.SALES_QTY_6WK_AVG),      R5.SALES_QTY_6WK_AVG)       AS SALES_6WKAVG_EXCL_PROMO_QTY,
       NVL(NVL(NVL(NVL(R1.SALES_6WK,              R2.SALES_6WK),              R3.SALES_6WK),              R4.SALES_6WK),              R5.SALES_6WK)               AS SALES_6WK,
       NVL(NVL(NVL(NVL(R1.SALES_6WK_AVG,          R2.SALES_6WK_AVG),          R3.SALES_6WK_AVG),          R4.SALES_6WK_AVG),          R5.SALES_6WK_AVG)           AS SALES_6WKAVG_EXCL_PROMO,
       NVL(NVL(NVL(NVL(R1.SALES_6WK_MARGIN,       R2.SALES_6WK_MARGIN),       R3.SALES_6WK_MARGIN),       R4.SALES_6WK_MARGIN),       R5.SALES_6WK_MARGIN)        AS SALES_6WK_MARGIN,
       NVL(NVL(NVL(NVL(R1.SALES_MARGIN_6WK_AVG,   R2.SALES_MARGIN_6WK_AVG),   R3.SALES_MARGIN_6WK_AVG),   R4.SALES_MARGIN_6WK_AVG),   R5.SALES_MARGIN_6WK_AVG)    AS SALES_6WKAVG_MARGIN_PERC,
       NVL(NVL(NVL(NVL(R1.WASTE_6WK_PROMO_COST,   R2.WASTE_6WK_PROMO_COST),   R3.WASTE_6WK_PROMO_COST),   R4.WASTE_6WK_PROMO_COST),   R5.WASTE_6WK_PROMO_COST)    AS WASTE_6WK_PROMO_COST,
       NVL(NVL(NVL(NVL(R1.WASTE_COST_6WK_AVG,     R2.WASTE_COST_6WK_AVG),     R3.WASTE_COST_6WK_AVG),     R4.WASTE_COST_6WK_AVG),     R5.WASTE_COST_6WK_AVG)      AS WASTE_6WKAVG_COST_PERC,
       NVL(NVL(NVL(NVL(R1.SK1_ZONE_GROUP_ZONE_NO, R2.SK1_ZONE_GROUP_ZONE_NO), R3.SK1_ZONE_GROUP_ZONE_NO), R4.SK1_ZONE_GROUP_ZONE_NO), R5.SK1_ZONE_GROUP_ZONE_NO)  AS SK1_ZONE_GROUP_ZONE_NO,
       NVL(NVL(NVL(NVL(R1.SK1_LOCATION_NO,        R2.SK1_LOCATION_NO),        R3.SK1_LOCATION_NO),        R4.SK1_LOCATION_NO),        R5.SK1_LOCATION_NO)         AS SK1_LOCATION_NO,
       NVL(NVL(NVL(NVL(R1.SK1_ITEM_NO,            R2.SK1_ITEM_NO),            R3.SK1_ITEM_NO),            R4.SK1_ITEM_NO),            R5.SK1_ITEM_NO)             AS SK1_ITEM_NO,
       NVL(NVL(NVL(NVL(R1.RULE_MATCHED,           R2.RULE_MATCHED),           R3.RULE_MATCHED),           R4.RULE_MATCHED),           R5.RULE_MATCHED)            AS RULE_MATCHED
    
    
  FROM sel_data_rule1 R1
  FULL OUTER JOIN sel_data_rule2 R2 ON R1.SK1_ITEM_NO = R2.SK1_ITEM_NO
                                   AND R1.SK1_LOCATION_NO = R2.SK1_LOCATION_NO
   
  FULL OUTER JOIN sel_data_rule3 R3 ON NVL(R1.SK1_ITEM_NO, R2.SK1_ITEM_NO) = R3.SK1_ITEM_NO
                                   AND NVL(R1.SK1_LOCATION_NO, R2.SK1_LOCATION_NO) = R3.SK1_LOCATION_NO                          
                                   
  FULL OUTER JOIN sel_data_rule4 R4 ON NVL(NVL(R1.SK1_ITEM_NO, R2.SK1_ITEM_NO), R3.SK1_ITEM_NO) = R4.SK1_ITEM_NO
                                   AND NVL(NVL(R1.SK1_LOCATION_NO, R2.SK1_LOCATION_NO), R3.SK1_LOCATION_NO) = R4.SK1_LOCATION_NO  
                                   
  FULL OUTER JOIN sel_data_rule5 R5 ON NVL(NVL(NVL(R1.SK1_ITEM_NO, R2.SK1_ITEM_NO), R3.SK1_ITEM_NO), R4.SK1_ITEM_NO)  = R5.SK1_ITEM_NO
                                   AND NVL(NVL(NVL(R1.SK1_LOCATION_NO, R2.SK1_LOCATION_NO), R3.SK1_LOCATION_NO), R4.SK1_LOCATION_NO) = R5.SK1_LOCATION_NO 
  ;
        
 
        g_recs_inserted := g_recs_inserted + sql%rowcount;
               commit;
           l_text := 'RULE 2+3 ** g_recs='||g_recs;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                     'mart_fd_zone_loc_item_6wkavg', DEGREE => 8);
      commit;
      l_text := 'GATHER STATS on DWH_PERFORMANCE.mart_fd_zone_loc_item_6wkavg';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
      G_RECS_READ := G_RECS_INSERTED;
  
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
     dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
     l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
  
     l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
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
  
  
  END WH_PRF_CORP_284U;
