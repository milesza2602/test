--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_282U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_282U" 
  (p_forall_limit in integer,p_success out boolean)
  as
  --**************************************************************************************************
  -- test version for prd
  -- ###################################################################
  -- THIS PROGRAM HAS BEEN REPLACED BY WH_PRF_CORP_284U - DECEMBER 2014
  -- ###################################################################
    --**************************************************************************************************
  --  Date:        June 2014
  --  Author:      Q Smit
  --  Purpose:     Load Foods Sales data where there have been no promotions
  --               during last 12 weeks - at zone level.
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
  l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_282U';
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
     l_text := 'LOAD OF TEMP_MART_FD_LOC_ITEM_WK_6WKAVG STARTED AT '||
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
  
     EXECUTE immediate('truncate table dwh_performance.mart_fd_zone_loc_item_6wkavg');
     l_text := 'truncate table dwh_performance.mart_fd_zone_loc_item_6wkavg';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
    COMMIT;
  
  
   --    execute immediate('create index dwh_performance.i50_p_tmp_mrt_6wkavg_PRM_DTS on dwh_performance.temp_mart_6wkavg_prom_dates
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
  
     --G_START_12WK := '03/MAR/14';
     --G_END_12WK   := '25/MAY/14';
  
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
  
   insert /*+ APPEND parallel (aps,2) */ into dwh_performance.mart_fd_zone_loc_item_6wkavg aps
              with
              selcal as
                            (
                              select distinct  fin_year_no,    fin_week_no
                              from dwh_performance.dim_calendar
                              where calendar_date between G_START_12WK and G_END_12WK
                        --      order by  fin_year_no,   fin_week_no
                             )-- select * from selcal;
                             ,
              selmaxp AS
                            (
                              SELECT /*+ parallel(tmpm,2) */
                                          SK1_ITEM_NO,    item_no,    fin_year_no,    fin_week_no,    week_count,    MAX(sk1_prom_no) maxprom
                              FROM dwh_performance.temp_mart_6wkavg_prom_dates tmpm
                            --  where tmpm.sk1_item_no in (15846679)
                              GROUP BY    SK1_ITEM_NO,  item_no,  fin_year_no,  fin_week_no,  week_count
                              ORDER BY  SK1_ITEM_NO,   MAX(sk1_prom_no),item_no,  fin_year_no,   fin_week_no,  week_count
                             )   ---select * from selmaxp where fin_week_no = 46;
                             ,
               selzeroes AS
                -- rule 1--
                            (
                                  SELECT
                                   SK1_ITEM_NO,    item_no,    COUNT(DISTINCT week_count) diswkcnt
                            FROM selmaxp sp
                            WHERE maxprom   IS NOT NULL
                              AND week_count   < 7
                            GROUP BY SK1_ITEM_NO,    item_no
                            HAVING COUNT(DISTINCT week_count) > 5
                       --     order by sk1_item_no
                            )  --select * from selzeroes;
                            ,
                selext AS
                -- all recs where rule 1 does not apply
                           (
                              SELECT DISTINCT sk1_item_no,    item_no,    fin_year_no,    fin_week_no,    week_count
                              FROM selmaxp sp2
                              WHERE maxprom IS NULL
                                AND NOT EXISTS (SELECT sk1_item_no FROM selzeroes sz WHERE sp2.sk1_item_no = sz.sk1_item_no )
                      --        ORDER BY sk1_item_no,    fin_year_no,    fin_week_no
                            )  --select * from selext where fin_week_no = 46;
                            ,
                selrnk AS
                            (
  
                            SELECT srk.sk1_item_no,    srk.item_no,    srk.fin_year_no,    srk.fin_week_no,    srk. week_count , srk.rnk
                            FROM
                                  (SELECT sk1_item_no,      item_no,      fin_year_no,      fin_week_no,      week_count ,
                                    dense_rank() over (partition BY sk1_item_no order by week_count) rnk
                                  FROM selext se ) srk
                            WHERE rnk <= 6
                         --   ORDER BY    fin_year_no,    fin_week_no, sk1_item_no
  
                            )  --selecT * from selrnk where fin_week_no = 46;
  
                 ,
   -- NEW
                selloc AS
                            (
                              SELECT /*+ parallel(tmpm,2) */
                                          SK1_ITEM_NO,    item_no,    fin_year_no,    fin_week_no,    week_count,    MAX(sk1_prom_no) maxprom,
                                          tmpm.sk1_location_no, tmpm.location_no, dl.sk1_fd_zone_group_zone_no sk1_zone_group_zone_no, dz.zone_no
                              FROM dwh_performance.temp_mart_6wkavg_prom_dates tmpm,
                                   dim_location dl,
                                   dim_zone dz
                         --     where tmpm.sk1_item_no in (15846679)
                                    where tmpm.sk1_location_no = dl.sk1_location_no
                                    and dl.sk1_fd_zone_group_zone_no = dz.sk1_zone_group_zone_no
                              group by SK1_ITEM_NO, item_no, fin_year_no, fin_week_no, week_count, tmpm.sk1_location_no, tmpm.location_no, dl.sk1_fd_zone_group_zone_no, dz.zone_no
                         )  --select * from selloc where fin_week_no = 46;
  
      ,
   -- END NEW
  
                selzeroes2 AS (
                  select sk1_item_no, sk1_location_no, item_no, location_no, zone_no, count(distinct week_count) diswkcnt
                    from selloc 
                  where sk1_item_no = 12816 and week_count < 7 and maxprom is not null
                 group by sk1_item_no, sk1_location_no, item_no, location_no, zone_no
                  HAVING COUNT(DISTINCT week_count) > 5
              ) --select * from selzeroes2;
,
  
                selitm AS
                -- distinct NON-rule 1 items
                              (
                                  SELECT sk1_item_no, COUNT(*) wkcnt
                              FROM selrnk
                              GROUP BY sk1_item_no
                              ORDER BY sk1_item_no
                              )
                              ,
                seldns as (select /*+ parallel(rdns,4) */ unique rdns.* from rtl_loc_item_wk_rms_dense rdns,  -- rdns,
                                  selcal dc,
                                  selmaxp mx,
                                  rtl_zone_item_om rzom,
                                  dim_location dl
                           where rdns.fin_year_no     = dc.fin_year_no
                           and   rdns.fin_week_no     = dc.fin_week_no
                           and   rdns.sk1_location_no = dl.sk1_location_no
                           and   rdns.sk1_item_no     = mx.sk1_item_no
                           and   rdns.sk1_item_no     = rzom.sk1_item_no
                       and   dl.sk1_fd_zone_group_zone_no = rzom.sk1_zone_group_zone_no
                   --    and rdns.sk1_item_no = 15846679  --
                   --       and rdns.fin_week_no in (47)
  
  
                           ) --select * from seldns ;  --where sk1_location_no = 12682 ;
                           ,
                selsps as (select /*+ parallel(rsps,4) */unique rsps.* from rtl_loc_item_wk_rms_sparse  rsps,   -- rsps,
                                  selcal dcs,
                                  selmaxp mx,
                                  rtl_zone_item_om rzom,
                                  dim_location dl
                           where rsps.fin_year_no     = dcs.fin_year_no
                           and   rsps.fin_week_no     = dcs.fin_week_no
                           and   rsps.sk1_location_no = dl.sk1_location_no
                           and   rsps.sk1_item_no     = mx.sk1_item_no
                           and   rsps.sk1_item_no     = rzom.sk1_item_no
                       --    and   rzom.sk1_zone_group_zone_no = dz.sk1_zone_group_zone_no (+)
                       --    and   mx.sk1_zone_group_zone_no = dz.sk1_zone_group_zone_no
                          and   dl.sk1_fd_zone_group_zone_no = rzom.sk1_zone_group_zone_no
                   --       and rsps.sk1_item_no = 15846679
                   --      and   rsps.fin_week_no in (47)
                           )   --select * from selsps where sk1_location_no = 683 ;
  
       -- xx as (
        select        zone_no,
                      location_no,
                      item_no,
                      G_START_12WK,
                      G_END_12WK,
                      0 sales_6wk_qty ,
                      0 sales_6wkavg_excl_promo_qty ,
                      0 sales_6wk ,
                      0 sales_6wkavg_excl_promo ,
                      0 sales_6wk_margin ,
                      0 sales_6wkavg_margin_perc ,
                      0 waste_6wk_promo_cost ,
                      0 waste_6wkavg_cost_perc ,
                      0 sk1_zone_group_zone_no,
                      0 sk1_location_no,
                      sk1_item_no
        from selzeroes2   --selzeroes
        union all
        select        zone_no,
                      location_no,
                      item_no,
                      G_START_12WK,
                      G_END_12WK,
                      sales_6wk_qty,
                      case    when wkcnt = 6  then decode(wkcnt,0,0,sales_6wk_qty/wkcnt)    else 0  end ,
                      sales_6wk ,
                      case    when wkcnt = 6  then decode(wkcnt,0,0,sales_6wk/wkcnt)    else 0  end ,
                      sales_6wk_margin ,
                      case    when wkcnt = 6  then decode(sales_6wk,0,0,sales_6wk_margin/sales_6wk)    else 0  end ,
                      waste_6wk_promo_cost ,
                      case    when wkcnt = 6  then decode(wkcnt,0,0,waste_6wk_promo_cost/wkcnt)    else 0  end ,
                      sk1_zone_group_zone_no,
                      sk1_location_no,
                      sk1_item_no
        from
                  (select
                    /*+ parallel(tmp,2) parallel(dns,2) full(dns) parallel(sps,2) full(sps) */
                      tmp.zone_no,
                      tmp.location_no,
                      tmp.item_no,
                      sum(nvl(sales_qty,0)) sales_6wk_qty ,
                      sum(nvl(sales,0)) sales_6wk,
                      sum(nvl(sales_margin,0)) sales_6wk_margin,
                      sum(nvl(waste_cost,0)) waste_6wk_promo_cost,
                      tmp.sk1_zone_group_zone_no,
                      tmp.sk1_location_no,
                      tmp.sk1_item_no,
                      si.wkcnt
                   from selitm si
                   join selrnk sr
                     on sr.sk1_item_no            = si.sk1_item_no
                 --   and sr.sk1_location_no        = si.sk1_location_no
                 --   and sr.sk1_zone_group_zone_no = si.sk1_zone_group_zone_no
  
  
                   join selloc tmp
                     on tmp.sk1_item_no            = sr.sk1_item_no
                --    and tmp.sk1_location_no        = sr.sk1_location_no
                --    and tmp.sk1_zone_group_zone_no = sr.sk1_zone_group_zone_no
                    and tmp.fin_year_no            = sr.fin_year_no
                    and tmp.fin_week_no            = sr.fin_week_no
  
                   left outer join seldns dns
                     on dns.sk1_item_no           = sr.sk1_item_no
                    and dns.sk1_location_no       = tmp.sk1_location_no
                    and dns.fin_year_no           = sr.fin_year_no
                    and dns.fin_week_no           = sr.fin_week_no
  
                   left outer join selsps sps
                     on sps.sk1_item_no           = sr.sk1_item_no
                    and sps.sk1_location_no       = tmp.sk1_location_no
                    and sps.fin_year_no           = sr.fin_year_no
                    and sps.fin_week_no           = sr.fin_week_no
                    --where wkcnt = 6
  
                  group by tmp.zone_no, tmp.location_no, tmp.item_no,
                           tmp.sk1_zone_group_zone_no, tmp.sk1_location_no, tmp.sk1_item_no, si.wkcnt
               --   order by tmp.location_no, tmp.zone_no
               )
                                  ;
  
        g_recs_inserted := g_recs_inserted + sql%rowcount;
               commit;
           l_text := 'RULE 2+3 ** g_recs='||g_recs;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
  
      DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                     'mart_fd_zone_loc_item_6wkavg', DEGREE => 8);
      commit;
      l_text := 'GATHER STATS on dwh_performance.mart_fd_zone_loc_item_6wkavg';
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
  
  
  END WH_PRF_CORP_282U;
