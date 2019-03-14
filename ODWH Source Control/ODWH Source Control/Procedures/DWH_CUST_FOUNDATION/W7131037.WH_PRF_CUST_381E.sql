-- ****** Object: Procedure W7131037.WH_PRF_CUST_381E Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_381E" (p_forall_limit in integer,p_success out boolean,p_run_date in date)
as

--**************************************************************************************************
--  Date:        APR 2016
--  Author:      Theo Filander
--  Purpose:     Create the Weekly interface for SVOC.
--               with input ex cust_basket_item table from performance layer.
--               THIS JOB RUNS WEEKLY
--  Tables:      Input  - dim_customer
--                      - dim_customer_card
--                      - dim_prom
--                      - dim_item
--                      - fnd_prom_item
--                      - fnd_prom_threshold_item
--                      - fnd_prom_mix_match_buy
--                      - fnd_prom_mix_match_get
--                      - cust_basket_item
--                      - cust_lss_lifestyle_segments
--               Output - OUT_DWH_SVOC_WEEKLY
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  18 August 2016    - Mariska Matthee - Add UCOUNT_CUST_TYPE and WFS_REG_IND, change the scheduling
--  11 October 2016   - Mariska Matthee - Populate TRADE_TOP_FOODS Promotion fields, default all wrewards columns to ''
--     - Mariska Matthee - Populate TRADE_TOP_NONFOODS Promotion fields + communication theme + opportunity category
--  10 July 2017:   Theo Filander   - Change output to OUT_DWH_SVOC_DAILY
--                  Remove 30 TRADE_?? Columns and YEAR and WEEK columns.
--                  No Business Rule changes
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
g_found              boolean       ;
g_date               date          := trunc(sysdate);
g_start_week         number        ;
g_end_week           number        ;
g_year_no            number(4,0)   ;
g_lss_year_no        number(4,0)   ;
g_month_no           number(2,0)   ;
g_lss_month_no       number(2,0)   ;
g_week_no            number(2,0)   ;
g_yesterday          date          := trunc(sysdate) - 1;
g_run_date           date          := trunc(sysdate);
g_prom_run_date      date          := trunc(sysdate);
g_this_wk_start_date date          := trunc(sysdate);
g_8wk_start_date     date          := trunc(sysdate);
g_8wk_end_date       date          := trunc(sysdate);
g_18wk_start_date     date          := trunc(sysdate);
g_18wk_end_date       date          := trunc(sysdate);
g_prom_end_date      date          := trunc(sysdate);
g_fin_day_no         dim_calendar.fin_day_no%type;
g_stmt               varchar2(300) ;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_381E';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE THE WEEKLY SVOC DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

procedure load_food_prom_field_data as
begin
  l_text := 'TRUNCATE PROMOTIONAL TEMP TABLES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_wk_trans';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_top_item';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_item';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_class';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_dept';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_lss_item';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_lss_notrn';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_lss_notrn2';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_notrnseg';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_ranked';

  l_text := 'BUILD TEMP TABLE STATS AFTER TRUNCATION';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_WK_TRANS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_TOP_ITEM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_ITEM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_CLASS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_DEPT',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_LSS_ITEM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_LSS_NOTRN',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_LSS_NOTRN2',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_NOTRNSEG',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'LOAD FOOD PROMOTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert into W7131037.temp_cust_svoc_prom tmp
      (prom_no,prom_desc,prom_week_start_no,prom_week_end_no,item_no,class_no,department_no,base_rsp)
    with cal as (select calendar_date, fin_week_no
                   from dim_calendar)
    select /*+ parallel(dimi,6) full(dimi) */
           prom.prom_no,
           prom.prom_desc,
           prom.prom_week_start_no,
           prom.prom_week_end_no,
           prom.item_no,
           dimi.class_no,
           dimi.department_no,
           dimi.base_rsp
      from (select dimp.prom_no,
                   dimp.prom_desc,
                   case when length(trim(cals.fin_week_no)) = 1 then 'wk0'||cals.fin_week_no else 'wk'||cals.fin_week_no end prom_week_start_no,
                   case when length(trim(cale.fin_week_no)) = 1 then 'wk0'||cale.fin_week_no else 'wk'||cale.fin_week_no end prom_week_end_no,
                   case when dimp.prom_type in ('SK') then fpi.item_no
                        when dimp.prom_type in ('TH','MU') then fpti.item_no
                        when dimp.prom_type in ('MM') then fpmm.item_no
                   end item_no
              from dwh_performance.dim_prom@dwhprd dimp
              left outer join dwh_foundation.fnd_prom_item@dwhprd fpi on dimp.prom_no = fpi.prom_no
              left outer join dwh_foundation.fnd_prom_threshold_item@dwhprd fpti on dimp.prom_no = fpti.prom_no
              left outer join (select prom_no, item_no
                                 from dwh_foundation.fnd_prom_mix_match_buy@dwhprd
                               union
                               select prom_no, item_no
                                 from dwh_foundation.fnd_prom_mix_match_get@dwhprd) fpmm on dimp.prom_no = fpmm.prom_no
              left outer join cal cals on dimp.prom_start_date = cals.calendar_date
              left outer join cal cale on dimp.prom_end_date = cale.calendar_date
             where dimp.prom_start_date <= g_this_wk_start_date
               and dimp.prom_end_date >= g_prom_end_date
               and upper(dimp.prom_name) not in ('NO PROMOTION NAME','REDUCED TO CLEAR','SAVE R0','SAVE 0%','0 EXTRA FREE')
               --and dimp.wreward_ind = 0 there will be no WREWARDS-only promotions in foods going forward
           ) prom
     inner join dwh_performance.dim_item dimi
        on prom.item_no = dimi.item_no and
           dimi.business_unit_no = 50; -- foods only (to be changed when loading nonfoods???)
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'LOAD CUSTOMER FOOD TRANSACTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_wk_trans tmp
    (primary_customer_identifier,segment_no,item_no,class_no,department_no,base_rsp,item_qty)
  with tbl as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from W7131037.cust_lss_lifestyle_segments@dwhprd seg
                where segment_type in ('Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no),
       prm as (select distinct department_no
                 from W7131037.temp_cust_svoc_prom tmp),
       trns as (select /*+ parallel(cbi,6) parallel(dimi,6) full(cbi) full(dimi) */
                       cbi.primary_customer_identifier,
                       cbi.item_no,
                       dimi.class_no,
                       dimi.department_no,
                       dimi.base_rsp,
                       sum(cbi.item_tran_qty) item_qty
                  from W7131037.cust_basket_item@dwhprd cbi
                 inner join dwh_performance.dim_item@dwhprd dimi
                    on cbi.item_no = dimi.item_no
                 inner join (select /*+ full(prm) */
                                    department_no
                               from prm) promo
                    on dimi.department_no = promo.department_no -- filter the transactions on departments sold on promotion
                 where cbi.tran_date between g_8wk_start_date and g_8wk_end_date -- get transactions for 8 weeks
                   and cbi.tran_type not in ('P','N','L','R','Q')
                   and cbi.primary_customer_identifier not in (998) -- cash
                   and cbi.customer_no > 0 -- extract for C2 customers only
                group by cbi.primary_customer_identifier,
                         cbi.item_no,
                         dimi.class_no,
                         dimi.department_no,
                         dimi.base_rsp
               )
  select /*+ full(trns) full(tbl) */
         trns.primary_customer_identifier,
         tbl.segment_no,
         trns.item_no,
         trns.class_no,
         trns.department_no,
         trns.base_rsp,
         trns.item_qty
    from trns
    left outer join tbl
      on trns.primary_customer_identifier = tbl.primary_customer_identifier;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_WK_TRANS',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RETURN PROMOTIONS WITH ONE ITEM LINKED';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert into W7131037.temp_cust_svoc_prom_top_item tmp
    (prom_no,item_no,base_rsp)
  select prom_no,
         item_no,
         base_rsp
    from W7131037.temp_cust_svoc_prom prm
   where prom_no in (select prom_no
                       from W7131037.temp_cust_svoc_prom prm
                      group by prom_no
                     having count(*) = 1);
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_TOP_ITEM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS AND ITEMS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_item tmp
    (primary_customer_identifier,prom_no,item_qty)
  with prm as (select distinct prom_no,item_no
                 from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(trn,6) full(trn)*/
         trn.primary_customer_identifier,
         tp.prom_no,
         sum(trn.item_qty) item_qty
    from (select /*+ parallel(tt,6) full(tt) */
                 primary_customer_identifier,
                 item_no,
                 sum(tt.item_qty) item_qty
            from W7131037.temp_cust_svoc_wk_trans tt
           group by primary_customer_identifier,item_no) trn
   inner join (select /*+ full(prm) */
                      prom_no,item_no
                 from prm) tp
      on trn.item_no = tp.item_no
   group by trn.primary_customer_identifier,tp.prom_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_ITEM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS AND ITEMS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_desc,
         rankprom.tag_no,
         rankprom.tag_desc,
         rankprom.base_rsp,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_desc,
                 distprom.tag_no,
                 distprom.tag_desc,
                 distprom.base_rsp,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc, distprom.tag_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_desc,
                         tagprom.tag_no,
                         tagprom.tag_desc,
                         tagprom.base_rsp,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.tag_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 pdet.prom_desc,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end tag_no,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end || '_' || pdet.prom_week_start_no || '_' || pdet.prom_week_end_no tag_desc,
                                 case when tidet.item_no is not null then tidet.base_rsp else 0 end base_rsp,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_prom_item tmp1
                                 ) allprom
                            left join W7131037.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS AND CLASSES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_class tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       prm as (select distinct prom_no,class_no
                 from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(trn,6) full(trn) */
         trn.primary_customer_identifier,
         tp.prom_no,
         sum(trn.item_qty) item_qty
    from (select /*+ parallel(tt,6) full(tt) */
                 primary_customer_identifier,
                 class_no,
                 sum(tt.item_qty) item_qty
            from W7131037.temp_cust_svoc_wk_trans tt
           where tt.primary_customer_identifier not in (select /*+ full(tbl) */
                                                                primary_customer_identifier
                                                           from tbl)
           group by primary_customer_identifier,class_no) trn
   inner join (select /*+ full(prm) */
                      prom_no,class_no
                 from prm) tp
      on trn.class_no = tp.class_no
   group by trn.primary_customer_identifier,tp.prom_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_CLASS',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS, ITEMS AND CLASSES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_desc,
         rankprom.tag_no,
         rankprom.tag_desc,
         rankprom.base_rsp,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_desc,
                 distprom.tag_no,
                 distprom.tag_desc,
                 distprom.base_rsp,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc, distprom.tag_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_desc,
                         tagprom.tag_no,
                         tagprom.tag_desc,
                         tagprom.base_rsp,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.tag_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 pdet.prom_desc,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end tag_no,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end || '_' || pdet.prom_week_start_no || '_' || pdet.prom_week_end_no tag_desc,
                                 case when tidet.item_no is not null then tidet.base_rsp else 0 end base_rsp,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_prom_class tmp2
                                 ) allprom
                            left join W7131037.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS AND DEPARTMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_dept tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       prm as (select distinct prom_no,department_no
                 from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(trn,6) full(trn) */
         trn.primary_customer_identifier,
         tp.prom_no,
         sum(trn.item_qty) item_qty
    from (select /*+ parallel(tt,6) full(tt) */
                 primary_customer_identifier,
                 department_no,
                 sum(tt.item_qty) item_qty
            from W7131037.temp_cust_svoc_wk_trans tt
           where tt.primary_customer_identifier not in (select /*+ full(tbl) */
                                                               primary_customer_identifier
                                                          from tbl)
           group by primary_customer_identifier,department_no) trn
   inner join (select /*+ full(prm) */
                      prom_no,department_no
                 from prm) tp
      on trn.department_no = tp.department_no
   group by trn.primary_customer_identifier,tp.prom_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_DEPT',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS, ITEMS, CLASSES AND DEPARTMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_desc,
         rankprom.tag_no,
         rankprom.tag_desc,
         rankprom.base_rsp,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_desc,
                 distprom.tag_no,
                 distprom.tag_desc,
                 distprom.base_rsp,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc, distprom.tag_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_desc,
                         tagprom.tag_no,
                         tagprom.tag_desc,
                         tagprom.base_rsp,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.tag_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 pdet.prom_desc,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end tag_no,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end || '_' || pdet.prom_week_start_no || '_' || pdet.prom_week_end_no tag_desc,
                                 case when tidet.item_no is not null then tidet.base_rsp else 0 end base_rsp,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_prom_class tmp2
                                  union all
                                  select /*+ parallel(tmp3,6) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from W7131037.temp_cust_svoc_prom_dept tmp3
                                 ) allprom
                            left join W7131037.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR SEGMENTS AND ITEMS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_lss_item tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       prm as (select distinct prom_no,item_no
                 from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(cust,6) parallel(seg,6) full(cust) full(seg)*/
         cust.primary_customer_identifier,
         seg.prom_no,
         seg.item_qty
    from (select /*+ parallel(trn,6) full(trn) */
                 distinct primary_customer_identifier,segment_no
            from W7131037.temp_cust_svoc_wk_trans trn
           where trn.primary_customer_identifier not in (select /*+ full(tbl) */
                                                               primary_customer_identifier
                                                          from tbl)
         ) cust -- foods only
   inner join (select /*+ parallel(trn,6) full(trn) */
                      trn.segment_no,
                      tp.prom_no,
                      sum(trn.item_qty) item_qty
                 from (select /*+ parallel(tt,6) full(tt) */
                              segment_no,
                              item_no,
                              sum(tt.item_qty) item_qty
                         from W7131037.temp_cust_svoc_wk_trans tt
                        where segment_no is not null
                        group by segment_no,item_no) trn
                inner join (select /*+ full(prm) */
                                   prom_no,item_no
                              from prm) tp
                   on trn.item_no = tp.item_no
                group by trn.segment_no,tp.prom_no) seg
      on cust.segment_no = seg.segment_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_LSS_ITEM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS, ITEMS, CLASSES, DEPARTMENTS AND LIFESTYLE SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_desc,
         rankprom.tag_no,
         rankprom.tag_desc,
         rankprom.base_rsp,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_desc,
                 distprom.tag_no,
                 distprom.tag_desc,
                 distprom.base_rsp,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc, distprom.tag_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_desc,
                         tagprom.tag_no,
                         tagprom.tag_desc,
                         tagprom.base_rsp,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.tag_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 pdet.prom_desc,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end tag_no,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end || '_' || pdet.prom_week_start_no || '_' || pdet.prom_week_end_no tag_desc,
                                 case when tidet.item_no is not null then tidet.base_rsp else 0 end base_rsp,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_prom_class tmp2
                                  union all
                                  select /*+ parallel(tmp3,6) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from W7131037.temp_cust_svoc_prom_dept tmp3
                                  union all
                                  select /*+ parallel(tmp4,6) full(tmp4) */
                                         primary_customer_identifier,prom_no,item_qty,4 precedence
                                    from W7131037.temp_cust_svoc_prom_lss_item tmp4
                                 ) allprom
                            left join W7131037.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS WITH NO TRANSACTIONS WITH LIFESTYLE SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_lss_notrn tmp
    (segment_no,prom_no,item_qty)
  with prm as (select distinct prom_no,item_no
                 from W7131037.temp_cust_svoc_prom prm)
  select segment_no,
         prom_no,
         item_qty
    from (select segment_no,
                 prom_no,
                 item_qty,
                 row_number() over (partition by segment_no order by item_qty desc, prom_no asc) rank_no
            from (select segment_no,
                         prom_no,
                         item_qty,
                         row_number() over (partition by segment_no, prom_no order by item_qty desc) rank_no
                    from (select /*+ parallel(trn,6) parallel(tp,6) full(trn) full(tp) */
                                 trn.segment_no,
                                 tp.prom_no,
                                 sum(trn.item_qty) item_qty
                            from (select /*+ parallel(tt,6) full(tt) */
                                         segment_no,
                                         item_no,
                                         sum(tt.item_qty) item_qty
                                    from W7131037.temp_cust_svoc_wk_trans tt
                                   where segment_no is not null
                                   group by segment_no,item_no) trn
                           inner join (select /*+ full(prm) */
                                              prom_no,item_no
                                         from prm) tp
                              on trn.item_no = tp.item_no
                           group by trn.segment_no,tp.prom_no
                         )
                 )
           where rank_no = 1)
   where rank_no <= 12;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_LSS_NOTRN',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS WITH RANKED LIFESTYLE SEGMENTS AND NO TRANSACTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_lss_notrn2 tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       lss as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from W7131037.cust_lss_lifestyle_segments@dwhprd seg
                where segment_type in ('Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no),
       lssprm as (select segment_no,
                         prom_no,
                         item_qty
                    from W7131037.temp_cust_svoc_prom_lss_notrn)
  select /*+ parallel(cust,6) parallel(promo,6) full(cust) full(promo)*/
         cust.primary_customer_identifier,
         promo.prom_no,
         promo.item_qty
    from (select /*+ parallel(dim,6) full(dim) */
                 dim.customer_no primary_customer_identifier,
                 seg.segment_no
            from W7131037.dim_customer@dwhprd dim
           inner join (select /*+ full(lss) */
                              primary_customer_identifier,
                              segment_no
                         from lss) seg
              on dim.customer_no = seg.primary_customer_identifier
           where dim.customer_no not in (select /*+ full(tbl) */
                                                primary_customer_identifier
                                           from tbl)
             and (dim.last_transaction_date is not null or
                  dim.wfs_app_reg_ind is not null or
                  dim.ucount_cust_type is not null)
         ) cust
   inner join (select /*+ full(lssprm) */
                      segment_no,
                      prom_no,
                      item_qty
                 from lssprm) promo
      on cust.segment_no = promo.segment_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_LSS_NOTRN2',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS, ITEMS, CLASSES, DEPARTMENTS AND LIFESTYLE SEGMENTS (W/WO TRANSACTIONS)';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_desc,
         rankprom.tag_no,
         rankprom.tag_desc,
         rankprom.base_rsp,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_desc,
                 distprom.tag_no,
                 distprom.tag_desc,
                 distprom.base_rsp,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc, distprom.tag_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_desc,
                         tagprom.tag_no,
                         tagprom.tag_desc,
                         tagprom.base_rsp,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.tag_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 pdet.prom_desc,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end tag_no,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end || '_' || pdet.prom_week_start_no || '_' || pdet.prom_week_end_no tag_desc,
                                 case when tidet.item_no is not null then tidet.base_rsp else 0 end base_rsp,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_prom_class tmp2
                                  union all
                                  select /*+ parallel(tmp3,6) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from W7131037.temp_cust_svoc_prom_dept tmp3
                                  union all
                                  select /*+ parallel(tmp4,6) full(tmp4) */
                                         primary_customer_identifier,prom_no,item_qty,4 precedence
                                    from W7131037.temp_cust_svoc_prom_lss_item tmp4
                                  union all
                                  select /*+ parallel(tmp5,6) full(tmp5) */
                                         primary_customer_identifier,prom_no,item_qty,5 precedence
                                    from W7131037.temp_cust_svoc_prom_lss_notrn2 tmp5
                                 ) allprom
                            left join W7131037.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS WITH NO TRANSACTIONS AND NO SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_notrnseg tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       prm as (select distinct prom_no,item_no
                 from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(cust,6) parallel(promo,6) full(cust) full(promo)*/
         cust.primary_customer_identifier,
         promo.prom_no,
         promo.item_qty
    from (select /*+ parallel(dim,6) full(dim) */
                 distinct customer_no primary_customer_identifier
            from W7131037.dim_customer dim
           where dim.customer_no not in (select /*+ full(tbl) */
                                                primary_customer_identifier
                                           from tbl)
             and (dim.last_transaction_date is not null or
                  dim.wfs_app_reg_ind is not null or
                  dim.ucount_cust_type is not null)
         ) cust
   cross join (select prom_no,
                      item_qty
                 from (select prom_no,
                              item_qty,
                              row_number() over (order by item_qty desc, prom_no asc) rank_no
                         from (select prom_no,
                                      item_qty,
                                      row_number() over (partition by prom_no order by item_qty desc) rank_no
                                 from (select /*+ parallel(trn,6) parallel(tp,6) full(trn) full(tp) */
                                              tp.prom_no,
                                              sum(trn.item_qty) item_qty
                                         from (select /*+ parallel(tt,6) full(tt) */
                                                      item_no,
                                                      sum(tt.item_qty) item_qty
                                                 from W7131037.temp_cust_svoc_wk_trans tt
                                                group by item_no) trn
                                        inner join (select /*+ full(prm) */
                                                           prom_no,item_no
                                                      from prm) tp
                                           on trn.item_no = tp.item_no
                                        group by tp.prom_no
                                      )
                              )
                        where rank_no = 1)
                where rank_no <= 12) promo;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_NOTRNSEG',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK ALL PROMOTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from W7131037.temp_cust_svoc_prom prm)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_desc,
         rankprom.tag_no,
         rankprom.tag_desc,
         rankprom.base_rsp,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_desc,
                 distprom.tag_no,
                 distprom.tag_desc,
                 distprom.base_rsp,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc, distprom.tag_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_desc,
                         tagprom.tag_no,
                         tagprom.tag_desc,
                         tagprom.base_rsp,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.tag_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 pdet.prom_desc,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end tag_no,
                                 case when tidet.item_no is not null then tidet.item_no else allprom.prom_no end || '_' || pdet.prom_week_start_no || '_' || pdet.prom_week_end_no tag_desc,
                                 case when tidet.item_no is not null then tidet.base_rsp else 0 end base_rsp,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_prom_class tmp2
                                  union all
                                  select /*+ parallel(tmp3,6) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from W7131037.temp_cust_svoc_prom_dept tmp3
                                  union all
                                  select /*+ parallel(tmp4,6) full(tmp4) */
                                         primary_customer_identifier,prom_no,item_qty,4 precedence
                                    from W7131037.temp_cust_svoc_prom_lss_item tmp4
                                  union all
                                  select /*+ parallel(tmp5,6) full(tmp5) */
                                         primary_customer_identifier,prom_no,item_qty,5 precedence
                                    from W7131037.temp_cust_svoc_prom_lss_notrn2 tmp5
                                  union all
                                  select /*+ parallel(tmp6,6) full(tmp6) */
                                         primary_customer_identifier,prom_no,item_qty,6 precedence
                                    from W7131037.temp_cust_svoc_prom_notrnseg tmp6
                                 ) allprom
                            left join W7131037.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  -- clean up disc space, truncate all temp tables, except the total ranking one
  l_text := 'TRUNCATE PROMOTIONAL TEMP TABLES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_wk_trans';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_top_item';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_item';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_class';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_dept';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_lss_item';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_lss_notrn';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_lss_notrn2';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_prom_notrnseg';

  l_text := 'BUILD TEMP TABLE STATS AFTER TRUNCATION';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_WK_TRANS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_TOP_ITEM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_ITEM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_CLASS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_DEPT',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_LSS_ITEM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_LSS_NOTRN',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_LSS_NOTRN2',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_PROM_NOTRNSEG',cascade=>true, DEGREE => 8);
  commit;
exception
  when others then
    l_message := 'LOAD PROMOTION FIELD DATA - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end load_food_prom_field_data;

procedure load_nonfood_prom_field_data as
begin
  l_text := 'TRUNCATE NON FOODS PROMOTIONAL TEMP TABLES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_wk_trans';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_top_tag';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_dept';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_sgrp';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_grp';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_lssitm';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_lssntrn';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_lssntn2';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_ntrnseg';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_ranked';

  l_text := 'BUILD TEMP TABLE STATS AFTER TRUNCATION';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_WK_TRANS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_TOP_TAG',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_DEPT',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_SGRP',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_GRP',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_LSSITM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_LSSNTRN',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_LSSNTN2',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_NTRNSEG',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'LOAD NON FOOD PROMOTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert into W7131037.temp_cust_svoc_nf_prom tmp
      (prom_no,prom_desc,department_no,department_name,subgroup_no,subgroup_name,group_no,group_name,business_unit_no,business_unit_name)
  select /*+ parallel(dimi,6) full(dimi) */
         distinct
         prom.prom_no,
         prom.prom_desc,
         dimi.department_no,
         dimi.department_name,
         dimi.subgroup_no,
         dimi.subgroup_name,
         dimi.group_no,
         dimi.group_name,
         dimi.business_unit_no,
         dimi.business_unit_name
    from (select dimp.prom_no,
                 dimp.prom_desc,
                 case when dimp.prom_type in ('SK') then fpi.item_no
                      when dimp.prom_type in ('TH','MU') then fpti.item_no
                      when dimp.prom_type in ('MM') then fpmm.item_no
                 end item_no
            from dwh_performance.dim_prom@dwhprd dimp
            left outer join dwh_foundation.fnd_prom_item@dwhprd fpi on dimp.prom_no = fpi.prom_no
            left outer join dwh_foundation.fnd_prom_threshold_item@dwhprd fpti on dimp.prom_no = fpti.prom_no
            left outer join (select prom_no, item_no
                                 from dwh_foundation.fnd_prom_mix_match_buy@dwhprd
                               union
                               select prom_no, item_no
                                 from dwh_foundation.fnd_prom_mix_match_get@dwhprd) fpmm on dimp.prom_no = fpmm.prom_no
           where dimp.prom_start_date <= g_this_wk_start_date
             and dimp.prom_end_date >= g_prom_end_date
             and upper(dimp.prom_name) not in ('NO PROMOTION NAME','REDUCED TO CLEAR','SAVE R0','SAVE 0%','0 EXTRA FREE')
         ) prom
   inner join dwh_performance.dim_item@dwhprd dimi
      on prom.item_no = dimi.item_no and
         dimi.business_unit_no in (51,52,54,55);
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'LOAD CUSTOMER NON FOOD TRANSACTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_wk_trans tmp
    (primary_customer_identifier,segment_no,department_no,subgroup_no,group_no,item_qty)
  with tbl as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from W7131037.cust_lss_lifestyle_segments@dwhprd seg
                where segment_type in ('Non-Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no),
       prm as (select distinct department_no
                 from W7131037.temp_cust_svoc_nf_prom tmp),
       trns as (select /*+ parallel(cbi,6) parallel(dimi,6) full(cbi) full(dimi) */
                       cbi.primary_customer_identifier,
                       dimi.department_no,
                       dimi.subgroup_no,
                       dimi.group_no,
                       sum(cbi.item_tran_qty) item_qty
                  from W7131037.cust_basket_item@dwhprd cbi
                 inner join dwh_performance.dim_item@dwhprd dimi
                    on cbi.item_no = dimi.item_no
                 inner join (select /*+ full(prm) */
                                    department_no
                               from prm) promo
                    on dimi.department_no = promo.department_no -- filter the transactions on departments sold on promotion
                 where cbi.tran_date between g_18wk_start_date and g_18wk_end_date -- get transactions for 18 weeks
                   and cbi.tran_type not in ('P','N','L','R','Q')
                   and cbi.primary_customer_identifier not in (998) -- cash
                   and cbi.customer_no > 0 -- extract for C2 customers only
                group by cbi.primary_customer_identifier,
                         dimi.department_no,
                         dimi.subgroup_no,
                         dimi.group_no
               )
  select /*+ full(trns) full(tbl) */
         trns.primary_customer_identifier,
         tbl.segment_no,
         trns.department_no,
         trns.subgroup_no,
         trns.group_no,
         trns.item_qty
    from trns
    left outer join tbl
      on trns.primary_customer_identifier = tbl.primary_customer_identifier;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_WK_TRANS',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RETURN PROMOTIONS WITH ONE DEPARTMENT, SUBGROUP, GROUP OR BUSINESS UNIT LINKED';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert into W7131037.temp_cust_svoc_nf_prom_top_tag tmp
    (prom_no,department_no,department_name,subgroup_no,subgroup_name,group_no,group_name,business_unit_no,business_unit_name)
  select prom_no,
         max(department_no) department_no,
         max(department_name) department_name,
         max(subgroup_no) subgroup_no,
         max(subgroup_name) subgroup_name,
         max(group_no) group_no,
         max(group_name) group_name,
         max(business_unit_no)  business_unit_no,
         max(business_unit_name)  business_unit_name
    from (select distinct prom_no,department_no,department_name,null subgroup_no,null subgroup_name,null group_no,null group_name,null business_unit_no,null business_unit_name
            from W7131037.temp_cust_svoc_nf_prom prm
           where prom_no in (select prom_no
                               from (select prom_no,department_no
                                       from W7131037.temp_cust_svoc_nf_prom
                                      group by prom_no,department_no)
                             group by prom_no
                            having count(*) = 1)
          union
          select distinct prom_no,null department_no,null department_name,subgroup_no,subgroup_name,null group_no,null group_name,null business_unit_no,null business_unit_name
            from W7131037.temp_cust_svoc_nf_prom prm
           where prom_no in (select prom_no
                               from (select prom_no,subgroup_no
                                       from W7131037.temp_cust_svoc_nf_prom
                                      group by prom_no,subgroup_no)
                             group by prom_no
                            having count(*) = 1)
          union
          select distinct prom_no,null department_no,null department_name,null subgroup_no,null subgroup_name,group_no,group_name,null business_unit_no,null business_unit_name
            from W7131037.temp_cust_svoc_nf_prom prm
           where prom_no in (select prom_no
                               from (select prom_no,group_no
                                       from W7131037.temp_cust_svoc_nf_prom
                                      group by prom_no,group_no)
                             group by prom_no
                            having count(*) = 1)
          union
          select distinct prom_no,null department_no,null department_name,null subgroup_no,null subgroup_name,null group_no,null group_name,business_unit_no,business_unit_name
                 business_unit_name
            from W7131037.temp_cust_svoc_nf_prom prm
           where prom_no in (select prom_no
                               from (select prom_no,business_unit_no
                                       from W7131037.temp_cust_svoc_nf_prom
                                      group by prom_no,business_unit_no)
                             group by prom_no
                            having count(*) = 1))
   group by prom_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_TOP_TAG',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE NON FOODS ITEM_QTY FOR CUSTOMERS AND DEPARTMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_dept tmp
    (primary_customer_identifier,prom_no,item_qty)
  with prm as (select distinct prom_no,department_no
                 from W7131037.temp_cust_svoc_nf_prom prm)
  select /*+ parallel(trn,6) full(trn)*/
         trn.primary_customer_identifier,
         tp.prom_no,
         sum(trn.item_qty) item_qty
    from (select /*+ parallel(tt,6) full(tt) */
                 primary_customer_identifier,
                 department_no,
                 sum(tt.item_qty) item_qty
            from W7131037.temp_cust_svoc_nf_wk_trans tt
           group by primary_customer_identifier,department_no) trn
   inner join (select /*+ full(prm) */
                      prom_no,department_no
                 from prm) tp
      on trn.department_no = tp.department_no
   group by trn.primary_customer_identifier,tp.prom_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_DEPT',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK NON FOODS PROMOTIONS FOR CUSTOMERS AND DEPARTMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_ranked tmp
    (primary_customer_identifier,prom_no,department_no,department_name,subgroup_no,subgroup_name,group_no,group_name,business_unit_no,business_unit_name,rank_no)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_no,
         rankprom.department_no,
         rankprom.department_name,
         rankprom.subgroup_no,
         rankprom.subgroup_name,
         rankprom.group_no,
         rankprom.group_name,
         rankprom.business_unit_no,
         rankprom.business_unit_name,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_no,
                 distprom.department_no,
                 distprom.department_name,
                 distprom.subgroup_no,
                 distprom.subgroup_name,
                 distprom.group_no,
                 distprom.group_name,
                 distprom.business_unit_no,
                 distprom.business_unit_name,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc,
                                                                                               distprom.department_no asc,distprom.subgroup_no asc,
                                                                                               distprom.group_no asc,distprom.business_unit_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_no,
                         tagprom.department_no,
                         tagprom.department_name,
                         tagprom.subgroup_no,
                         tagprom.subgroup_name,
                         tagprom.group_no,
                         tagprom.group_name,
                         tagprom.business_unit_no,
                         tagprom.business_unit_name,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.department_no,tagprom.subgroup_no,
                                                         tagprom.group_no,tagprom.business_unit_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 allprom.prom_no,
                                 nvl(tdet.department_no,999999) department_no,
                                 tdet.department_name,
                                 nvl(tdet.subgroup_no,999999) subgroup_no,
                                 tdet.subgroup_name,
                                 nvl(tdet.group_no,999999) group_no,
                                 tdet.group_name,
                                 nvl(tdet.business_unit_no,999999) business_unit_no,
                                 tdet.business_unit_name,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_dept tmp1
                                 ) allprom
                            left join W7131037.temp_cust_svoc_nf_prom_top_tag tdet
                              on allprom.prom_no = tdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 2;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE NON FOODS ITEM_QTY FOR CUSTOMERS AND SUBGROUPS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_sgrp tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_nf_prom_ranked rnkd
                where rank_no = 2),
       prm as (select distinct prom_no,subgroup_no
                 from W7131037.temp_cust_svoc_nf_prom prm)
  select /*+ parallel(trn,6) full(trn)*/
         trn.primary_customer_identifier,
         tp.prom_no,
         sum(trn.item_qty) item_qty
    from (select /*+ parallel(tt,6) full(tt) */
                 primary_customer_identifier,
                 subgroup_no,
                 sum(tt.item_qty) item_qty
            from W7131037.temp_cust_svoc_nf_wk_trans tt
           where tt.primary_customer_identifier not in (select /*+ full(tbl) */
                                                               primary_customer_identifier
                                                          from tbl)
           group by primary_customer_identifier,subgroup_no) trn
   inner join (select /*+ full(prm) */
                      prom_no,subgroup_no
                 from prm) tp
      on trn.subgroup_no = tp.subgroup_no
   group by trn.primary_customer_identifier,tp.prom_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_SGRP',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK NON FOODS PROMOTIONS FOR CUSTOMERS, DEPARTMENTS AND SUBGROUPS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_ranked tmp
    (primary_customer_identifier,prom_no,department_no,department_name,subgroup_no,subgroup_name,group_no,group_name,business_unit_no,business_unit_name,rank_no)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_no,
         rankprom.department_no,
         rankprom.department_name,
         rankprom.subgroup_no,
         rankprom.subgroup_name,
         rankprom.group_no,
         rankprom.group_name,
         rankprom.business_unit_no,
         rankprom.business_unit_name,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_no,
                 distprom.department_no,
                 distprom.department_name,
                 distprom.subgroup_no,
                 distprom.subgroup_name,
                 distprom.group_no,
                 distprom.group_name,
                 distprom.business_unit_no,
                 distprom.business_unit_name,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc,
                                                                                               distprom.department_no asc,distprom.subgroup_no asc,
                                                                                               distprom.group_no asc,distprom.business_unit_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_no,
                         tagprom.department_no,
                         tagprom.department_name,
                         tagprom.subgroup_no,
                         tagprom.subgroup_name,
                         tagprom.group_no,
                         tagprom.group_name,
                         tagprom.business_unit_no,
                         tagprom.business_unit_name,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.department_no,tagprom.subgroup_no,
                                                         tagprom.group_no,tagprom.business_unit_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 allprom.prom_no,
                                 nvl(tdet.department_no,999999) department_no,
                                 tdet.department_name,
                                 nvl(tdet.subgroup_no,999999) subgroup_no,
                                 tdet.subgroup_name,
                                 nvl(tdet.group_no,999999) group_no,
                                 tdet.group_name,
                                 nvl(tdet.business_unit_no,999999) business_unit_no,
                                 tdet.business_unit_name,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_dept tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_sgrp tmp2
                                 ) allprom
                            left join W7131037.temp_cust_svoc_nf_prom_top_tag tdet
                              on allprom.prom_no = tdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 2;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE NON FOODS ITEM_QTY FOR CUSTOMERS AND GROUPS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_grp tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_nf_prom_ranked rnkd
                where rank_no = 2),
       prm as (select distinct prom_no,group_no
                 from W7131037.temp_cust_svoc_nf_prom prm)
  select /*+ parallel(trn,6) full(trn)*/
         trn.primary_customer_identifier,
         tp.prom_no,
         sum(trn.item_qty) item_qty
    from (select /*+ parallel(tt,6) full(tt) */
                 primary_customer_identifier,
                 group_no,
                 sum(tt.item_qty) item_qty
            from W7131037.temp_cust_svoc_nf_wk_trans tt
           where tt.primary_customer_identifier not in (select /*+ full(tbl) */
                                                               primary_customer_identifier
                                                          from tbl)
           group by primary_customer_identifier,group_no) trn
   inner join (select /*+ full(prm) */
                      prom_no,group_no
                 from prm) tp
      on trn.group_no = tp.group_no
   group by trn.primary_customer_identifier,tp.prom_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_GRP',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK NON FOODS PROMOTIONS FOR CUSTOMERS, DEPARTMENTS, SUBGROUPS AND GROUPS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_ranked tmp
    (primary_customer_identifier,prom_no,department_no,department_name,subgroup_no,subgroup_name,group_no,group_name,business_unit_no,business_unit_name,rank_no)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_no,
         rankprom.department_no,
         rankprom.department_name,
         rankprom.subgroup_no,
         rankprom.subgroup_name,
         rankprom.group_no,
         rankprom.group_name,
         rankprom.business_unit_no,
         rankprom.business_unit_name,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_no,
                 distprom.department_no,
                 distprom.department_name,
                 distprom.subgroup_no,
                 distprom.subgroup_name,
                 distprom.group_no,
                 distprom.group_name,
                 distprom.business_unit_no,
                 distprom.business_unit_name,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc,
                                                                                               distprom.department_no asc,distprom.subgroup_no asc,
                                                                                               distprom.group_no asc,distprom.business_unit_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_no,
                         tagprom.department_no,
                         tagprom.department_name,
                         tagprom.subgroup_no,
                         tagprom.subgroup_name,
                         tagprom.group_no,
                         tagprom.group_name,
                         tagprom.business_unit_no,
                         tagprom.business_unit_name,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.department_no,tagprom.subgroup_no,
                                                         tagprom.group_no,tagprom.business_unit_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 allprom.prom_no,
                                 nvl(tdet.department_no,999999) department_no,
                                 tdet.department_name,
                                 nvl(tdet.subgroup_no,999999) subgroup_no,
                                 tdet.subgroup_name,
                                 nvl(tdet.group_no,999999) group_no,
                                 tdet.group_name,
                                 nvl(tdet.business_unit_no,999999) business_unit_no,
                                 tdet.business_unit_name,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_dept tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_sgrp tmp2
                                  union all
                                  select /*+ parallel(tmp3,6) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_grp tmp3
                                 ) allprom
                            left join W7131037.temp_cust_svoc_nf_prom_top_tag tdet
                              on allprom.prom_no = tdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 2;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE NON FOODS ITEM_QTY FOR SEGMENTS AND DEPARTMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_lssitm tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_nf_prom_ranked rnkd
                where rank_no = 2),
       prm as (select distinct prom_no,department_no
                 from W7131037.temp_cust_svoc_nf_prom prm)
  select /*+ parallel(cust,6) parallel(seg,6) full(cust) full(seg)*/
         cust.primary_customer_identifier,
         seg.prom_no,
         seg.item_qty
    from (select /*+ parallel(trn,6) full(trn) */
                 distinct primary_customer_identifier,segment_no
            from W7131037.temp_cust_svoc_nf_wk_trans trn
           where trn.primary_customer_identifier not in (select /*+ full(tbl) */
                                                               primary_customer_identifier
                                                          from tbl)
         ) cust -- non foods only
   inner join (select /*+ parallel(trn,6) full(trn) */
                      trn.segment_no,
                      tp.prom_no,
                      sum(trn.item_qty) item_qty
                 from (select /*+ parallel(tt,6) full(tt) */
                              segment_no,
                              department_no,
                              sum(tt.item_qty) item_qty
                         from W7131037.temp_cust_svoc_nf_wk_trans tt
                        where segment_no is not null
                        group by segment_no,department_no) trn
                inner join (select /*+ full(prm) */
                                   prom_no,department_no
                              from prm) tp
                   on trn.department_no = tp.department_no
                group by trn.segment_no,tp.prom_no) seg
      on cust.segment_no = seg.segment_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_LSSITM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS, DEPARTMENT, SUBGROUP, GROUP AND LIFESTYLE SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_ranked tmp
    (primary_customer_identifier,prom_no,department_no,department_name,subgroup_no,subgroup_name,group_no,group_name,business_unit_no,business_unit_name,rank_no)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_no,
         rankprom.department_no,
         rankprom.department_name,
         rankprom.subgroup_no,
         rankprom.subgroup_name,
         rankprom.group_no,
         rankprom.group_name,
         rankprom.business_unit_no,
         rankprom.business_unit_name,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_no,
                 distprom.department_no,
                 distprom.department_name,
                 distprom.subgroup_no,
                 distprom.subgroup_name,
                 distprom.group_no,
                 distprom.group_name,
                 distprom.business_unit_no,
                 distprom.business_unit_name,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc,
                                                                                               distprom.department_no asc,distprom.subgroup_no asc,
                                                                                               distprom.group_no asc,distprom.business_unit_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_no,
                         tagprom.department_no,
                         tagprom.department_name,
                         tagprom.subgroup_no,
                         tagprom.subgroup_name,
                         tagprom.group_no,
                         tagprom.group_name,
                         tagprom.business_unit_no,
                         tagprom.business_unit_name,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.department_no,tagprom.subgroup_no,
                                                         tagprom.group_no,tagprom.business_unit_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 allprom.prom_no,
                                 nvl(tdet.department_no,999999) department_no,
                                 tdet.department_name,
                                 nvl(tdet.subgroup_no,999999) subgroup_no,
                                 tdet.subgroup_name,
                                 nvl(tdet.group_no,999999) group_no,
                                 tdet.group_name,
                                 nvl(tdet.business_unit_no,999999) business_unit_no,
                                 tdet.business_unit_name,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_dept tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_sgrp tmp2
                                  union all
                                  select /*+ parallel(tmp3,6) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_grp tmp3
                                  union all
                                  select /*+ parallel(tmp4,6) full(tmp4) */
                                         primary_customer_identifier,prom_no,item_qty,4 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_lssitm tmp4
                                 ) allprom
                            left join W7131037.temp_cust_svoc_nf_prom_top_tag tdet
                              on allprom.prom_no = tdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 2;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE NON FOODS ITEM_QTY FOR CUSTOMERS WITH NO TRANSACTIONS WITH LIFESTYLE SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_lssntrn tmp
    (segment_no,prom_no,item_qty)
  with prm as (select distinct prom_no,department_no
                 from W7131037.temp_cust_svoc_nf_prom prm)
  select segment_no,
         prom_no,
         item_qty
    from (select segment_no,
                 prom_no,
                 item_qty,
                 row_number() over (partition by segment_no order by item_qty desc, prom_no asc) rank_no
            from (select segment_no,
                         prom_no,
                         item_qty,
                         row_number() over (partition by segment_no, prom_no order by item_qty desc) rank_no
                    from (select /*+ parallel(trn,6) parallel(tp,6) full(trn) full(tp) */
                                 trn.segment_no,
                                 tp.prom_no,
                                 sum(trn.item_qty) item_qty
                            from (select /*+ parallel(tt,6) full(tt) */
                                         segment_no,
                                         department_no,
                                         sum(tt.item_qty) item_qty
                                    from W7131037.temp_cust_svoc_nf_wk_trans tt
                                   where segment_no is not null
                                   group by segment_no,department_no) trn
                           inner join (select /*+ full(prm) */
                                              prom_no,department_no
                                         from prm) tp
                              on trn.department_no = tp.department_no
                           group by trn.segment_no,tp.prom_no
                         )
                 )
           where rank_no = 1)
   where rank_no <= 4;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_LSSNTRN',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE NON FOODS ITEM_QTY FOR CUSTOMERS WITH RANKED LIFESTYLE SEGMENTS AND NO TRANSACTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_lssntn2 tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_nf_prom_ranked rnkd
                where rank_no = 2),
       lss as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from W7131037.cust_lss_lifestyle_segments@dwhprd seg
                where segment_type in ('Non-Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no),
       lssprm as (select segment_no,
                         prom_no,
                         item_qty
                    from W7131037.temp_cust_svoc_nf_prom_lssntrn)
  select /*+ parallel(cust,6) parallel(promo,6) full(cust) full(promo)*/
         cust.primary_customer_identifier,
         promo.prom_no,
         promo.item_qty
    from (select /*+ parallel(dim,6) full(dim) */
                 dim.customer_no primary_customer_identifier,
                 seg.segment_no
            from W7131037.dim_customer@dwhprd dim
           inner join (select /*+ full(lss) */
                              primary_customer_identifier,
                              segment_no
                         from lss) seg
              on dim.customer_no = seg.primary_customer_identifier
           where dim.customer_no not in (select /*+ full(tbl) */
                                                primary_customer_identifier
                                           from tbl)
             and (dim.last_transaction_date is not null or
                  dim.wfs_app_reg_ind is not null or
                  dim.ucount_cust_type is not null)
         ) cust
   inner join (select /*+ full(lssprm) */
                      segment_no,
                      prom_no,
                      item_qty
                 from lssprm) promo
      on cust.segment_no = promo.segment_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_LSSNTN2',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK NON FOODS PROMOTIONS FOR CUSTOMERS, DEPARTMENT, SUBGROUP, GROUP AND LIFESTYLE SEGMENTS (W/WO TRANSACTIONS)';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_ranked tmp
    (primary_customer_identifier,prom_no,department_no,department_name,subgroup_no,subgroup_name,group_no,group_name,business_unit_no,business_unit_name,rank_no)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_no,
         rankprom.department_no,
         rankprom.department_name,
         rankprom.subgroup_no,
         rankprom.subgroup_name,
         rankprom.group_no,
         rankprom.group_name,
         rankprom.business_unit_no,
         rankprom.business_unit_name,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_no,
                 distprom.department_no,
                 distprom.department_name,
                 distprom.subgroup_no,
                 distprom.subgroup_name,
                 distprom.group_no,
                 distprom.group_name,
                 distprom.business_unit_no,
                 distprom.business_unit_name,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc,
                                                                                               distprom.department_no asc,distprom.subgroup_no asc,
                                                                                               distprom.group_no asc,distprom.business_unit_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_no,
                         tagprom.department_no,
                         tagprom.department_name,
                         tagprom.subgroup_no,
                         tagprom.subgroup_name,
                         tagprom.group_no,
                         tagprom.group_name,
                         tagprom.business_unit_no,
                         tagprom.business_unit_name,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.department_no,tagprom.subgroup_no,
                                                         tagprom.group_no,tagprom.business_unit_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 allprom.prom_no,
                                 nvl(tdet.department_no,999999) department_no,
                                 tdet.department_name,
                                 nvl(tdet.subgroup_no,999999) subgroup_no,
                                 tdet.subgroup_name,
                                 nvl(tdet.group_no,999999) group_no,
                                 tdet.group_name,
                                 nvl(tdet.business_unit_no,999999) business_unit_no,
                                 tdet.business_unit_name,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_dept tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_sgrp tmp2
                                  union all
                                  select /*+ parallel(tmp3,6) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_grp tmp3
                                  union all
                                  select /*+ parallel(tmp4,6) full(tmp4) */
                                         primary_customer_identifier,prom_no,item_qty,4 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_lssitm tmp4
                                  union all
                                  select /*+ parallel(tmp5,6) full(tmp5) */
                                         primary_customer_identifier,prom_no,item_qty,5 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_lssntn2 tmp5
                                 ) allprom
                            left join W7131037.temp_cust_svoc_nf_prom_top_tag tdet
                              on allprom.prom_no = tdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 2;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE NON FOODS ITEM_QTY FOR CUSTOMERS WITH NO TRANSACTIONS AND NO SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_ntrnseg tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from W7131037.temp_cust_svoc_nf_prom_ranked rnkd
                where rank_no = 2),
       prm as (select distinct prom_no,department_no
                 from W7131037.temp_cust_svoc_nf_prom prm)
  select /*+ parallel(cust,6) parallel(promo,6) full(cust) full(promo)*/
         cust.primary_customer_identifier,
         promo.prom_no,
         promo.item_qty
    from (select /*+ parallel(dim,6) full(dim) */
                 distinct customer_no primary_customer_identifier
            from W7131037.dim_customer@dwhprd dim
           where dim.customer_no not in (select /*+ full(tbl) */
                                                primary_customer_identifier
                                           from tbl)
             and (dim.last_transaction_date is not null or
                  dim.wfs_app_reg_ind is not null or
                  dim.ucount_cust_type is not null)
         ) cust
   cross join (select prom_no,
                      item_qty
                 from (select prom_no,
                              item_qty,
                              row_number() over (order by item_qty desc, prom_no asc) rank_no
                         from (select prom_no,
                                      item_qty,
                                      row_number() over (partition by prom_no order by item_qty desc) rank_no
                                 from (select /*+ parallel(trn,6) parallel(tp,6) full(trn) full(tp) */
                                              tp.prom_no,
                                              sum(trn.item_qty) item_qty
                                         from (select /*+ parallel(tt,6) full(tt) */
                                                      department_no,
                                                      sum(tt.item_qty) item_qty
                                                 from W7131037.temp_cust_svoc_nf_wk_trans tt
                                                group by department_no) trn
                                        inner join (select /*+ full(prm) */
                                                           prom_no,department_no
                                                      from prm) tp
                                           on trn.department_no = tp.department_no
                                        group by tp.prom_no
                                      )
                              )
                        where rank_no = 1)
                where rank_no <= 3) promo;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_NTRNSEG',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK ALL NON FOODS PROMOTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_ranked';
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_nf_prom_ranked tmp
    (primary_customer_identifier,prom_no,department_no,department_name,subgroup_no,subgroup_name,group_no,group_name,business_unit_no,business_unit_name,rank_no)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.prom_no,
         rankprom.department_no,
         rankprom.department_name,
         rankprom.subgroup_no,
         rankprom.subgroup_name,
         rankprom.group_no,
         rankprom.group_name,
         rankprom.business_unit_no,
         rankprom.business_unit_name,
         rankprom.rank_no
    from (select /*+ parallel(distprom,6) full(distprom) */
                 distprom.primary_customer_identifier,
                 distprom.prom_no,
                 distprom.department_no,
                 distprom.department_name,
                 distprom.subgroup_no,
                 distprom.subgroup_name,
                 distprom.group_no,
                 distprom.group_name,
                 distprom.business_unit_no,
                 distprom.business_unit_name,
                 row_number() over (partition by distprom.primary_customer_identifier order by distprom.precedence asc, distprom.item_qty desc,
                                                                                               distprom.department_no asc,distprom.subgroup_no asc,
                                                                                               distprom.group_no asc,distprom.business_unit_no asc) rank_no
            from (select /*+ parallel(tagprom,6) full(tagprom) */
                         tagprom.primary_customer_identifier,
                         tagprom.prom_no,
                         tagprom.department_no,
                         tagprom.department_name,
                         tagprom.subgroup_no,
                         tagprom.subgroup_name,
                         tagprom.group_no,
                         tagprom.group_name,
                         tagprom.business_unit_no,
                         tagprom.business_unit_name,
                         tagprom.item_qty,
                         tagprom.precedence,
                         row_number() over (partition by tagprom.primary_customer_identifier,tagprom.department_no,tagprom.subgroup_no,
                                                         tagprom.group_no,tagprom.business_unit_no order by tagprom.precedence asc, tagprom.item_qty desc) rank_no
                    from (select /*+ parallel(allprom,6) full(allprom) */
                                 allprom.primary_customer_identifier,
                                 allprom.prom_no,
                                 nvl(tdet.department_no,999999) department_no,
                                 tdet.department_name,
                                 nvl(tdet.subgroup_no,999999) subgroup_no,
                                 tdet.subgroup_name,
                                 nvl(tdet.group_no,999999) group_no,
                                 tdet.group_name,
                                 nvl(tdet.business_unit_no,999999) business_unit_no,
                                 tdet.business_unit_name,
                                 allprom.item_qty,
                                 allprom.precedence
                            from (select /*+ parallel(tmp1,6) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_dept tmp1
                                  union all
                                  select /*+ parallel(tmp2,6) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_sgrp tmp2
                                  union all
                                  select /*+ parallel(tmp3,6) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_grp tmp3
                                  union all
                                  select /*+ parallel(tmp4,6) full(tmp4) */
                                         primary_customer_identifier,prom_no,item_qty,4 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_lssitm tmp4
                                  union all
                                  select /*+ parallel(tmp5,6) full(tmp5) */
                                         primary_customer_identifier,prom_no,item_qty,5 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_lssntn2 tmp5
                                  union all
                                  select /*+ parallel(tmp6,6) full(tmp6) */
                                         primary_customer_identifier,prom_no,item_qty,6 precedence
                                    from W7131037.temp_cust_svoc_nf_prom_ntrnseg tmp6
                                 ) allprom
                            left join W7131037.temp_cust_svoc_nf_prom_top_tag tdet
                              on allprom.prom_no = tdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 2;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  -- clean up disc space, truncate all temp tables, except the total ranking one
  l_text := 'TRUNCATE NON FOODS PROMOTIONAL TEMP TABLES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_wk_trans';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_top_tag';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_dept';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_sgrp';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_grp';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_lssitm';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_lssntrn';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_lssntn2';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_nf_prom_ntrnseg';

  l_text := 'BUILD TEMP TABLE STATS AFTER TRUNCATION';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_WK_TRANS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_TOP_TAG',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_DEPT',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_SGRP',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_GRP',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_LSSITM',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_LSSNTRN',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_LSSNTN2',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_NF_PROM_NTRNSEG',cascade=>true, DEGREE => 8);
  commit;
exception
  when others then
    l_message := 'LOAD PROMOTION FIELD DATA - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end load_nonfood_prom_field_data;

procedure load_sugg_product_field_data as
begin
  l_text := 'TRUNCATE SUGGESTED PRODUCTS TEMP TABLES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spf_wk_trans';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spnf_wk_trans';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spf_lsssgrp';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spnf_lsssgrp';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spf_ranked';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spnf_ranked';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spfoc_ranked';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spnfoc_ranked';

  l_text := 'BUILD TEMP TABLE STATS AFTER TRUNCATION';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPF_WK_TRANS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNF_WK_TRANS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPF_LSSSGRP',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNF_LSSSGRP',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPF_RANKED',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNF_RANKED',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPFOC_RANKED',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNFOC_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'LOAD FOOD TRANSACTIONS FOR SUGGESTED PRODUCTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_spf_wk_trans tmp
      (primary_customer_identifier,segment_no,subgroup_no,subgroup_name,item_qty)
  with tbl as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from W7131037.cust_lss_lifestyle_segments@dwhprd seg
                where segment_type in ('Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no),
       trns as (select /*+ parallel(cbi,6) parallel(dimi,6) full(cbi) full(dimi) */
                       cbi.primary_customer_identifier,
                       dimi.subgroup_no,
                       dimi.subgroup_name,
                       sum(cbi.item_tran_qty) item_qty
                  from W7131037.cust_basket_item@dwhprd cbi
                 inner join dwh_performance.dim_item dimi
                    on cbi.item_no = dimi.item_no
                 where cbi.tran_date between g_8wk_start_date and g_8wk_end_date -- get transactions for 8 weeks
                   and cbi.tran_type not in ('P','N','L','R','Q')
                   and cbi.primary_customer_identifier not in (998) -- cash
                   and cbi.customer_no > 0 -- extract for C2 customers only
                   and dimi.business_unit_no in (50)
                group by cbi.primary_customer_identifier,
                         dimi.subgroup_no,
                         dimi.subgroup_name
               )
  select /*+ full(trns) full(tbl) */
         trns.primary_customer_identifier,
         tbl.segment_no,
         trns.subgroup_no,
         trns.subgroup_name,
         trns.item_qty
    from trns
    left outer join tbl
      on trns.primary_customer_identifier = tbl.primary_customer_identifier;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPF_WK_TRANS',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'LOAD NON FOOD TRANSACTIONS FOR SUGGESTED PRODUCTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_spnf_wk_trans tmp
      (primary_customer_identifier,segment_no,subgroup_no,subgroup_name,item_qty)
  with tbl as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from W7131037.cust_lss_lifestyle_segments@dwhprd seg
                where segment_type in ('Non-Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no),
       trns as (select /*+ parallel(cbi,6) parallel(dimi,6) full(cbi) full(dimi) */
                       cbi.primary_customer_identifier,
                       dimi.subgroup_no,
                       dimi.subgroup_name,
                       sum(cbi.item_tran_qty) item_qty
                  from W7131037.cust_basket_item@dwhprd cbi
                 inner join dwh_performance.dim_item@dwhprd dimi
                    on cbi.item_no = dimi.item_no
                 where cbi.tran_date between g_18wk_start_date and g_18wk_end_date -- get transactions for 18 weeks
                   and cbi.tran_type not in ('P','N','L','R','Q')
                   and cbi.primary_customer_identifier not in (998) -- cash
                   and cbi.customer_no > 0 -- extract for C2 customers only
                   and dimi.business_unit_no in (51,52,54,55)
                group by cbi.primary_customer_identifier,
                         dimi.subgroup_no,
                         dimi.subgroup_name
               )
  select /*+ full(trns) full(tbl) */
         trns.primary_customer_identifier,
         tbl.segment_no,
         trns.subgroup_no,
         trns.subgroup_name,
         trns.item_qty
    from trns
    left outer join tbl
      on trns.primary_customer_identifier = tbl.primary_customer_identifier;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNF_WK_TRANS',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE FOOD ITEM_QTY FOR SEGMENTS AND SUBGROUPS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_spf_lsssgrp tmp
    (primary_customer_identifier,subgroup_no,subgroup_name,item_qty)
  with lss as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from W7131037.cust_lss_lifestyle_segments@dwhprd seg
                where segment_type in ('Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no)
  select /*+ parallel(cust,6) parallel(seg,6) full(cust) full(seg)*/
         cust.primary_customer_identifier,
         seg.subgroup_no,
         seg.subgroup_name,
         seg.item_qty
    from (select /*+ parallel(dim,6) full(dim) */
                 dim.customer_no primary_customer_identifier,
                 seg.segment_no
            from W7131037.dim_customer@dwhprd dim
           inner join (select /*+ full(lss) */
                              primary_customer_identifier,
                              segment_no
                         from lss) seg
              on dim.customer_no = seg.primary_customer_identifier
           where (dim.last_transaction_date is not null or
                  dim.wfs_app_reg_ind is not null or
                  dim.ucount_cust_type is not null)
         ) cust
   inner join (select /*+ parallel(tt,6) full(tt) */
                      segment_no,
                      subgroup_no,
                      subgroup_name,
                      sum(tt.item_qty) item_qty
                 from W7131037.temp_cust_svoc_spf_wk_trans tt
                where segment_no is not null
                group by segment_no,subgroup_no,subgroup_name) seg
      on cust.segment_no = seg.segment_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPF_LSSSGRP',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE NON FOOD ITEM_QTY FOR SEGMENTS AND SUBGROUPS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_spnf_lsssgrp tmp
    (primary_customer_identifier,subgroup_no,subgroup_name,item_qty)
  with lss as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from W7131037.cust_lss_lifestyle_segments@dwhprd seg
                where segment_type in ('Non-Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no)
  select /*+ parallel(cust,6) parallel(seg,6) full(cust) full(seg)*/
         cust.primary_customer_identifier,
         seg.subgroup_no,
         seg.subgroup_name,
         seg.item_qty
    from (select /*+ parallel(dim,6) full(dim) */
                 dim.customer_no primary_customer_identifier,
                 seg.segment_no
            from W7131037.dim_customer@dwhprd dim
           inner join (select /*+ full(lss) */
                              primary_customer_identifier,
                              segment_no
                         from lss) seg
              on dim.customer_no = seg.primary_customer_identifier
           where (dim.last_transaction_date is not null or
                  dim.wfs_app_reg_ind is not null or
                  dim.ucount_cust_type is not null)
         ) cust
   inner join (select /*+ parallel(tt,6) full(tt) */
                      segment_no,
                      subgroup_no,
                      subgroup_name,
                      sum(tt.item_qty) item_qty
                 from W7131037.temp_cust_svoc_spnf_wk_trans tt
                where segment_no is not null
                group by segment_no,subgroup_no,subgroup_name) seg
      on cust.segment_no = seg.segment_no;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNF_LSSSGRP',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK FOOD TRANSACTIONS FOR CUSTOMERS, SUBGROUP AND LIFESTYLE SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_spf_ranked tmp
    (primary_customer_identifier,subgroup_name)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.subgroup_name
    from (select /*+ parallel(allprom,6) full(allprom) */
                 allprom.primary_customer_identifier,
                 allprom.subgroup_no,
                 allprom.subgroup_name,
                 row_number() over (partition by allprom.primary_customer_identifier order by allprom.precedence asc, allprom.item_qty desc, allprom.subgroup_no asc) rank_no
            from (select /*+ parallel(tmp1,6) full(tmp1) */
                         primary_customer_identifier,subgroup_no,subgroup_name,item_qty,1 precedence
                    from W7131037.temp_cust_svoc_spf_wk_trans tmp1
                  union all
                  select /*+ parallel(tmp2,6) full(tmp2) */
                         primary_customer_identifier,subgroup_no,subgroup_name,item_qty,2 precedence
                    from W7131037.temp_cust_svoc_spf_lsssgrp tmp2
                 ) allprom
         ) rankprom
   where rankprom.rank_no = 1;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPF_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK NON FOOD TRANSACTIONS FOR CUSTOMERS, SUBGROUP AND LIFESTYLE SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_spnf_ranked tmp
    (primary_customer_identifier,subgroup_name)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.subgroup_name
    from (select /*+ parallel(allprom,6) full(allprom) */
                 allprom.primary_customer_identifier,
                 allprom.subgroup_no,
                 allprom.subgroup_name,
                 row_number() over (partition by allprom.primary_customer_identifier order by allprom.precedence asc, allprom.item_qty desc, allprom.subgroup_no asc) rank_no
            from (select /*+ parallel(tmp1,6) full(tmp1) */
                         primary_customer_identifier,subgroup_no,subgroup_name,item_qty,1 precedence
                    from W7131037.temp_cust_svoc_spnf_wk_trans tmp1
                  union all
                  select /*+ parallel(tmp2,6) full(tmp2) */
                         primary_customer_identifier,subgroup_no,subgroup_name,item_qty,2 precedence
                    from W7131037.temp_cust_svoc_spnf_lsssgrp tmp2
                 ) allprom
         ) rankprom
   where rankprom.rank_no = 1;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNF_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK FOOD TRANSACTIONS FOR CUSTOMERS, SUBGROUP AND LIFESTYLE SEGMENTS FOR OPP CAT';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_spfoc_ranked tmp
    (primary_customer_identifier,subgroup_name)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.subgroup_name
    from (select /*+ parallel(allprom,6) full(allprom) */
                 allprom.primary_customer_identifier,
                 allprom.subgroup_no,
                 allprom.subgroup_name,
                 row_number() over (partition by allprom.primary_customer_identifier order by allprom.precedence, allprom.item_qty desc, allprom.subgroup_no asc) rank_no
            from ((select /*+ parallel(tmp1,6) full(tmp1) */
                          primary_customer_identifier,subgroup_no,subgroup_name,item_qty,1 precedence
                     from W7131037.temp_cust_svoc_spf_lsssgrp tmp1
                    where (primary_customer_identifier,subgroup_no) in (select /*+ parallel(lss,6) full(lss) */
                                                                               primary_customer_identifier,subgroup_no
                                                                          from W7131037.temp_cust_svoc_spf_lsssgrp lss
                                                                        minus
                                                                        select /*+ parallel(trn,6) full(trn) */
                                                                               primary_customer_identifier,subgroup_no
                                                                          from W7131037.temp_cust_svoc_spf_wk_trans trn2))
                  union all
                  select /*+ parallel(tmp3,6) full(tmp3) */
                         primary_customer_identifier,subgroup_no,subgroup_name,item_qty,2 precedence
                    from W7131037.temp_cust_svoc_spf_wk_trans tmp3
                   where (primary_customer_identifier,subgroup_no) in (select /*+ parallel(tt,6) full(tt) */
                                                                              primary_customer_identifier,max(subgroup_no) subgroup_no
                                                                         from W7131037.temp_cust_svoc_spf_wk_trans tt
                                                                        where (primary_customer_identifier,item_qty) in (select /*+ parallel(trn,6) full(trn) */
                                                                                                                                primary_customer_identifier,min(item_qty) item_qty
                                                                                                                           from W7131037.temp_cust_svoc_spf_wk_trans trn
                                                                                                                          where segment_no is not null
                                                                                                                          group by primary_customer_identifier)
                                                                        group by primary_customer_identifier)
                 ) allprom
         ) rankprom
   where rankprom.rank_no = 1;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPFOC_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK NON FOOD TRANSACTIONS FOR CUSTOMERS, SUBGROUP AND LIFESTYLE SEGMENTS FOR OPP CAT';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into W7131037.temp_cust_svoc_spnfoc_ranked tmp
    (primary_customer_identifier,subgroup_name)
  select /*+ parallel(rankprom,6) full(rankprom) */
         rankprom.primary_customer_identifier,
         rankprom.subgroup_name
    from (select /*+ parallel(allprom,6) full(allprom) */
                 allprom.primary_customer_identifier,
                 allprom.subgroup_no,
                 allprom.subgroup_name,
                 row_number() over (partition by allprom.primary_customer_identifier order by allprom.precedence, allprom.item_qty desc, allprom.subgroup_no asc) rank_no
            from ((select /*+ parallel(tmp1,6) full(tmp1) */
                          primary_customer_identifier,subgroup_no,subgroup_name,item_qty,1 precedence
                     from W7131037.temp_cust_svoc_spnf_lsssgrp tmp1
                    where (primary_customer_identifier,subgroup_no) in (select /*+ parallel(lss,6) full(lss) */
                                                                               primary_customer_identifier,subgroup_no
                                                                          from W7131037.temp_cust_svoc_spnf_lsssgrp lss
                                                                        minus
                                                                        select /*+ parallel(trn,6) full(trn) */
                                                                               primary_customer_identifier,subgroup_no
                                                                          from W7131037.temp_cust_svoc_spnf_wk_trans trn2))
                  union all
                  select /*+ parallel(tmp3,6) full(tmp3) */
                         primary_customer_identifier,subgroup_no,subgroup_name,item_qty,2 precedence
                    from W7131037.temp_cust_svoc_spnf_wk_trans tmp3
                   where (primary_customer_identifier,subgroup_no) in (select /*+ parallel(tt,6) full(tt) */
                                                                              primary_customer_identifier,max(subgroup_no) subgroup_no
                                                                         from W7131037.temp_cust_svoc_spnf_wk_trans tt
                                                                        where (primary_customer_identifier,item_qty) in (select /*+ parallel(trn,6) full(trn) */
                                                                                                                                primary_customer_identifier,min(item_qty) item_qty
                                                                                                                           from W7131037.temp_cust_svoc_spnf_wk_trans trn
                                                                                                                          where segment_no is not null
                                                                                                                          group by primary_customer_identifier)
                                                                        group by primary_customer_identifier)
                 ) allprom
         ) rankprom
   where rankprom.rank_no = 1;
  commit;

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNFOC_RANKED',cascade=>true, DEGREE => 8);
  commit;

  -- clean up disc space, truncate all temp tables, except the total ranking one
  l_text := 'TRUNCATE SUGGESTED PRODUCTS TEMP TABLES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spf_wk_trans';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spnf_wk_trans';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spf_lsssgrp';
  execute immediate 'TRUNCATE TABLE W7131037.temp_cust_svoc_spnf_lsssgrp';

  l_text := 'BUILD TEMP TABLE STATS AFTER TRUNCATION';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPF_WK_TRANS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNF_WK_TRANS',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPF_LSSSGRP',cascade=>true, DEGREE => 8);
  dbms_stats.gather_table_stats ('W7131037','TEMP_CUST_SVOC_SPNF_LSSSGRP',cascade=>true, DEGREE => 8);
  commit;
exception
  when others then
    l_message := 'LOAD SUGGESTED PRODUCT FIELD DATA - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end load_sugg_product_field_data;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    execute immediate 'alter session enable parallel dml';

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'BUILD OF CUSTOMER INSIGHTS TO OUT_DWH_SVOC_DAILY STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS: '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    if p_run_date is not null or p_run_date <> '' then
      select this_week_start_date,this_week_start_date
        into g_run_date,g_prom_run_date
        from dim_calendar
       where calendar_date = p_run_date;
    else
       select this_week_start_date,this_week_start_date+7
         into g_run_date,g_prom_run_date
         from dwh_performance.dim_calendar
        where calendar_date = (select this_week_start_date-1
                                 from dim_calendar
                                where calendar_date = trunc(sysdate)) ;
    end if;
--**************************************************************************************************
-- Main loop
--**************************************************************************************************
    select fin_year_no,fin_month_no,fin_week_no
      into g_year_no,g_month_no,g_week_no
      from dim_calendar
     where calendar_date = g_run_date;

    -- when this job runs on a Tuesday the promotions is from that Monday covering a period of 2 weeks into the future
    -- transactions are from the Sunday before going back 8 weeks
    -- transactions are from the Sunday before going back 18 weeks for nonfoods
    select this_week_start_date,
           this_week_start_date-(8*7),
           this_week_start_date-1,
           this_week_start_date-(18*7),
           this_week_start_date-1,
           this_week_start_date+13
      into g_this_wk_start_date,
           g_8wk_start_date,
           g_8wk_end_date,
           g_18wk_start_date,
           g_18wk_end_date,
           g_prom_end_date
      from dim_calendar
     where calendar_date = g_prom_run_date;

    -- when the weekly extract runs you need to get the latest month loaded in the lss table
    select /*+ parallel(seg,6) full(seg) */
           max(fin_year_no) fin_year_no
      into g_lss_year_no
      from W7131037.cust_lss_lifestyle_segments@dwhprd seg
     where fin_year_no <= g_year_no;

    select /*+ parallel(seg,6) full(seg) */
           max(fin_month_no) fin_month_no
      into g_lss_month_no
      from W7131037.cust_lss_lifestyle_segments@dwhprd seg
     where fin_year_no = g_lss_year_no
       and fin_month_no <= g_month_no;

    l_text := 'EXTRACT DATA FOR YEAR : '||g_year_no||'  WEEK : '||g_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'POPULATE FOODS PROMOTIONAL DATA' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    load_food_prom_field_data;

    l_text := 'POPULATE NONFOODS PROMOTIONAL DATA' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    load_nonfood_prom_field_data;

    l_text := 'POPULATE SUGGESTED PRODUCTS DATA' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --load_sugg_product_field_data;

    l_text := 'TRUNCATE TABLE OUT_DWH_SVOC_DAILY.' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'TRUNCATE TABLE "W7131037"."OUT_DWH_SVOC_DAILY"';
    dbms_stats.gather_table_stats ('W7131037','OUT_DWH_SVOC_DAILY',cascade=>true, DEGREE => 8);
    commit;

    l_text := 'POPULATE TABLE OUT_DWH_SVOC_DAILY.' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    insert /*+ parallel(OUT_DWH_SVOC_DAILY ,6) */ into W7131037.OUT_DWH_SVOC_DAILY
    with promo_details as (select /*+ Parallel(prm,6) full(prm) */
                                  primary_customer_identifier,
                                  prom_desc,
                                  tag_no,
                                  tag_desc,
                                  base_rsp,
                                  rank_no
                             from W7131037.temp_cust_svoc_prom_ranked prm),
         nf_promo_details as (select /*+ Parallel(prm,6) full(prm) */
                                     prm.primary_customer_identifier,
                                     prm.prom_no,
                                     prm.department_name,
                                     prm.subgroup_name,
                                     prm.group_name,
                                     prm.business_unit_name,
                                     prm.rank_no
                                from W7131037.temp_cust_svoc_nf_prom_ranked prm),
         spf_details as (select /*+ Parallel(sps,6) full(sps) */
                                primary_customer_identifier,
                                subgroup_name
                           from W7131037.temp_cust_svoc_spf_ranked sps),
         spnf_details as (select /*+ Parallel(sps,6) full(sps) */
                                 primary_customer_identifier,
                                 subgroup_name
                            from W7131037.temp_cust_svoc_spnf_ranked sps),
         spfoc_details as (select /*+ Parallel(sps,6) full(sps) */
                                  primary_customer_identifier,
                                  subgroup_name
                             from W7131037.temp_cust_svoc_spfoc_ranked sps),
         spnfoc_details as (select /*+ Parallel(sps,6) full(sps) */
                                   primary_customer_identifier,
                                   subgroup_name
                              from W7131037.temp_cust_svoc_spnfoc_ranked sps),
         voucher_details as (
                            select /*+ Parallel(fv,6)  */
                                   primary_account_no primary_customer_identifier,
                                   count (distinct case when voucher_status_description in ('Expired','Active','Redeemed') then 1 end) no_of_vouchers_issued_past3m,
                                   count( distinct case when voucher_status_description = 'Redeemed' then 1 end)                       no_of_vouchers_redeemed_past3m,
                                   count( distinct case when voucher_status_description = 'Active' then 1 end)                         number_of_active_vouchers
                             from  W7131037.cust_fv_voucher@dwhprd fv
                            where expiry_date > (
                                                 select min(start_date) from (
                                                                               select distinct this_mn_start_date start_date
                                                                                 from dim_calendar
                                                                                where this_mn_start_date <  trunc(sysdate)
                                                                                order by this_mn_start_date desc
                                                                               )
                                                  where rownum <= 4
                                               )
                              and voucher_status_description in ('Expired','Active','Redeemed')
                              and create_date > '01/JAN/2013'
                            group by primary_account_no
                            )
    select /*+ Parallel(cc,6) Parallel(dc,6) Full(cc) Full(dc) */
--           g_year_no fin_year_no,
--           g_week_no fin_week_no,
           case when dc.customer_no is not null then dc.customer_no
                when ww_card is not null then ww_card
                when ms_card is not null then ms_card
                when alien_card is not null then alien_card
           end primary_customer_identifier,
           dc.customer_no,
           cc.ww_card,
           cc.ms_card,
           cc.alien_card,
           '' retailsoft_customer_no,
           dc.ucount_cust_type,
           case when dc.wfs_app_reg_ind = 1 then 1 else 0 end wfs_app_reg_ind,
           title_code,
           first_name,
           first_middle_name_initial,
           last_name,
           home_cell_country_code,
           home_cell_area_code,
           home_cell_no,
           home_phone_country_code,
           home_phone_area_code,
           home_phone_no,
           home_phone_extension_no,
           work_cell_country_code,
           work_cell_area_code,
           work_cell_no,
           work_phone_country_code,
           work_phone_area_code,
           work_phone_no,
           work_phone_extension_no,
           home_email_address,
           work_email_address,
           estatement_email,
           '' wrewards_top_foods_sku01,
           '' wrewards_top_foods_sku02,
           '' wrewards_top_foods_sku03,
           '' wrewards_top_foods_sku04,
           '' wrewards_top_foods_sku05,
           '' wrewards_top_foods_sku06,
           '' wrewards_top_foods_tag01,
           '' wrewards_top_foods_tag02,
           '' wrewards_top_foods_tag03,
           '' wrewards_top_foods_tag04,
           '' wrewards_top_foods_tag05,
           '' wrewards_top_foods_tag06,
           '' wreards_top_foods_price01,
           '' wreards_top_foods_price02,
           '' wreards_top_foods_price03,
           '' wreards_top_foods_price04,
           '' wreards_top_foods_price05,
           '' wreards_top_foods_price06,
           '' wrewards_top_foods_saving01,
           '' wrewards_top_foods_saving02,
           '' wrewards_top_foods_saving03,
           '' wrewards_top_foods_saving04,
           '' wrewards_top_foods_saving05,
           '' wrewards_top_foods_saving06,
           '' wrewards_top_foods_prom_mech01,
           '' wrewards_top_foods_prom_mech02,
           '' wrewards_top_foods_prom_mech03,
           '' wrewards_top_foods_prom_mech04,
           '' wrewards_top_foods_prom_mech05,
           '' wrewards_top_foods_prom_mech06,
           '' wrewards_top_nonfoods_cat01,
           '' wrewards_top_nonfoods_cat02,
           '' wrewards_top_nonfoods_tag01,
           '' wrewards_top_nonfoods_tag02,
           '' wrewards_top_nf_prom_mech01,
           '' wrewards_top_nf_prom_mech02,
--           promo1.tag_no trade_top_foods_sku01,
--           promo2.tag_no trade_top_foods_sku02,
--           promo3.tag_no trade_top_foods_sku03,
--           promo4.tag_no trade_top_foods_sku04,
--           promo5.tag_no trade_top_foods_sku05,
--           promo6.tag_no trade_top_foods_sku06,
--           promo1.tag_desc trade_top_foods_tag01,
--           promo2.tag_desc trade_top_foods_tag02,
--           promo3.tag_desc trade_top_foods_tag03,
--           promo4.tag_desc trade_top_foods_tag04,
--           promo5.tag_desc trade_top_foods_tag05,
--           promo6.tag_desc trade_top_foods_tag06,
--           promo1.base_rsp trade_top_foods_price01,
--           promo2.base_rsp trade_top_foods_price02,
--           promo3.base_rsp trade_top_foods_price03,
--           promo4.base_rsp trade_top_foods_price04,
--           promo5.base_rsp trade_top_foods_price05,
--           promo6.base_rsp trade_top_foods_price06,
--           '' trade_top_foods_saving01,
--           '' trade_top_foods_saving02,
--           '' trade_top_foods_saving03,
--           '' trade_top_foods_saving04,
--           '' trade_top_foods_saving05,
--           '' trade_top_foods_saving06,
--           '' trade_top_foods_mech01,
--           '' trade_top_foods_mech02,
--           '' trade_top_foods_mech03,
--           '' trade_top_foods_mech04,
--           '' trade_top_foods_mech05,
--           '' trade_top_foods_mech06,
           nvl(case when nfpromo1.group_name is null then nfpromo1.business_unit_name else nfpromo1.group_name end,'') trade_nonfoods_cat01,
           nvl(case when nfpromo2.group_name is null then nfpromo2.business_unit_name else nfpromo2.group_name end,'') trade_nonfoods_cat02,
           nvl(case when nfpromo1.subgroup_name is null then
                 case when nfpromo1.group_name is null then nfpromo1.business_unit_name else nfpromo1.group_name end
               else nfpromo1.subgroup_name end,'') trade_nonfoods_tag01,
           nvl(case when nfpromo2.subgroup_name is null then
                 case when nfpromo2.group_name is null then nfpromo2.business_unit_name else nfpromo2.group_name end
               else nfpromo2.subgroup_name end,'') trade_nonfoods_tag02,
           nvl(case when nfpromo1.department_name is null then
                 case when nfpromo1.subgroup_name is null then
                   case when nfpromo1.group_name is null then
                     nfpromo1.business_unit_name
                   else nfpromo1.group_name end
                 else nfpromo1.subgroup_name end
               else nfpromo1.department_name end,'') trade_top_nonfoods_prom_mech01,
           nvl(case when nfpromo2.department_name is null then
                 case when nfpromo2.subgroup_name is null then
                   case when nfpromo2.group_name is null then
                     nfpromo2.business_unit_name
                   else nfpromo2.group_name end
                 else nfpromo2.subgroup_name end
               else nfpromo2.department_name end,'') trade_top_nonfoods_prom_mech02,
           nvl(spf.subgroup_name,'') best_foods_comm_theme, -- communication theme
           nvl(spnf.subgroup_name,'') best_nonfoods_comm_theme,
           nvl(spfoc.subgroup_name,'') foods_opp_cat, -- opportunity category
           nvl(spnfoc.subgroup_name,'') nonfoods_opp_cat,
           last_transaction_date,
           '' wom_outerw_tran_wfs_sc_12m,
           '' wom_outerw_tran_wfs_cc_12m,
           '' wom_outerw_tran_visa_12m,
           '' wom_outerw_tran_mstrcrd_12m,
           '' wom_lingerie_tran_wfs_sc_12m,
           '' wom_lingerie_tran_wfs_cc_12m,
           '' wom_lingerie_tran_visa_12m,
           '' wom_lingerie_tran_mstrcrd_12m,
           '' wom_foot_acc_tran_wfs_sc_12m,
           '' wom_foot_acc_tran_wfs_cc_12m,
           '' wom_foot_acc_tran_visa_12m,
           '' wom_foot_acc_tran_mstrcrd_12m,
           '' mens_formal_tran_wfs_sc_12m,
           '' mens_formal_tran_wfs_cc_12m,
           '' mens_formal_tran_visa_12m,
           '' mens_formal_tran_mstrcrd_12m,
           '' mens_smrt_cas_tran_wfs_sc_12m,
           '' mens_smrt_cas_tran_wfs_cc_12m,
           '' mens_smrt_cas_tran_visa_12m,
           '' mens_smrt_cas_tran_mstrcrd_12m,
           '' mens_cas_tran_wfs_sc_12m,
           '' mens_cas_tran_wfs_cc_12m,
           '' mens_cas_tran_visa_12m,
           '' mens_cas_tran_mstrcrd_12m,
           '' mens_essentls_tran_wfs_sc_12m,
           '' mens_essentls_tran_wfs_cc_12m,
           '' mens_essentls_tran_visa_12m,
           '' mens_essentls_tran_mstrcrd_12m,
           '' mens_foot_acc_tran_wfs_sc_12m,
           '' mens_foot_acc_tran_wfs_cc_12m,
           '' mens_foot_acc_tran_visa_12m,
           '' mens_foot_acc_tran_mstrcrd_12m,
           '' studio_w_tran_wfs_sc_12m,
           '' studio_w_tran_wfs_cc_12m,
           '' studio_w_tran_visa_12m,
           '' studio_w_tran_mstrcrd_12m,
           '' re_tran_wfs_sc_12m,
           '' re_tran_wfs_cc_12m,
           '' re_tran_visa_12m,
           '' re_tran_mstrcrd_12m,
           '' crg_grp_tran_wfs_sc_12m,
           '' crg_grp_tran_wfs_cc_12m,
           '' crg_grp_tran_visa_12m,
           '' crg_grp_tran_mstrcrd_12m,
           '' kids_girls_tran_wfs_sc_12m,
           '' kids_girls_tran_wfs_cc_12m,
           '' kids_girls_tran_visa_12m,
           '' kids_girls_tran_mstrcrd_12m,
           '' kids_boys_tran_wfs_sc_12m,
           '' kids_boys_tran_wfs_cc_12m,
           '' kids_boys_tran_visa_12m,
           '' kids_boys_tran_mstrcrd_12m,
           '' kids_baby_tran_wfs_sc_12m,
           '' kids_baby_tran_wfs_cc_12m,
           '' kids_baby_tran_visa_12m,
           '' kids_baby_tran_mstrcrd_12m,
           '' kids_school_tran_wfs_sc_12m,
           '' kids_school_tran_wfs_cc_12m,
           '' kids_school_tran_visa_12m,
           '' kids_school_tran_mstrcrd_12m,
           '' kids_foot_tran_wfs_sc_12m,
           '' kids_foot_tran_wfs_cc_12m,
           '' kids_foot_tran_visa_12m,
           '' kids_foot_tran_mstrcrd_12m,
           '' kids_acc_tran_wfs_sc_12m,
           '' kids_acc_tran_wfs_cc_12m,
           '' kids_acc_tran_visa_12m,
           '' kids_acc_tran_mstrcrd_12m,
           '' kids_essentls_tran_wfs_sc_12m,
           '' kids_essentls_tran_wfs_cc_12m,
           '' kids_essentls_tran_visa_12m,
           '' kids_essentls_tran_mstrcrd_12m,
           '' kids_re_tran_wfs_sc_12m,
           '' kids_re_tran_wfs_cc_12m,
           '' kids_re_tran_visa_12m,
           '' kids_re_tran_mstrcrd_12m,
           '' homeware_tran_wfs_sc_12m,
           '' homeware_tran_wfs_cc_12m,
           '' homeware_tran_visa_12m,
           '' homeware_tran_mstrcrd_12m,
           '' beauty_tran_wfs_sc_12m,
           '' beauty_tran_wfs_cc_12m,
           '' beauty_tran_visa_12m,
           '' beauty_tran_mstrcrd_12m,
           '' digital_tran_wfs_sc_12m,
           '' digital_tran_wfs_cc_12m,
           '' digital_tran_visa_12m,
           '' digital_tran_mstrcrd_12m,
           '' bakery_tran_wfs_sc_12m,
           '' bakery_tran_wfs_cc_12m,
           '' bakery_tran_visa_12m,
           '' bakery_tran_mstrcrd_12m,
           '' dairy_tran_wfs_sc_12m,
           '' dairy_tran_wfs_cc_12m,
           '' dairy_tran_visa_12m,
           '' dairy_tran_mstrcrd_12m,
           '' groceries_tran_wfs_sc_12m,
           '' groceries_tran_wfs_cc_12m,
           '' groceries_tran_visa_12m,
           '' groceries_tran_mstrcrd_12m,
           '' home_prsn_pet_tran_wfs_sc_12m,
           '' home_prsn_pet_tran_wfs_cc_12m,
           '' home_prsn_pet_tran_visa_12m,
           '' home_prsn_pet_tran_mstrcrd_12m,
           '' prepared_deli_tran_wfs_sc_12m,
           '' prepared_deli_tran_wfs_cc_12m,
           '' prepared_deli_tran_visa_12m,
           '' prepared_deli_tran_mstrcrd_12m,
           '' produce_horti_tran_wfs_sc_12m,
           '' produce_horti_tran_wfs_cc_12m,
           '' produce_horti_tran_visa_12m,
           '' produce_horti_tran_mstrcrd_12m,
           '' protein_tran_wfs_sc_12m,
           '' protein_tran_wfs_cc_12m,
           '' protein_tran_visa_12m,
           '' protein_tran_mstrcrd_12m,
           '' snack_gifting_tran_wfs_sc_12m,
           '' snack_gifting_tran_wfs_cc_12m,
           '' snack_gifting_tran_visa_12m,
           '' snack_gifting_tran_mstrcrd_12m,
           '' wine_bev_liq_tran_wfs_sc_12m,
           '' wine_bev_liq_tran_wfs_cc_12m,
           '' wine_bev_liq_tran_visa_12m,
           '' wine_bev_liq_tran_mstrcrd_12m,
           '' one_app_registration_indicator,
           '' one_app_registration_date,
           '' last_one_app_login_date,
           '' last_one_app_order_date,
           '' last_wifi_login_date,
           '' last_online_login_date,
           last_online_date last_online_order_date,
           '' nownow_app_registration_ind,
           '' nownow_app_registration_date,
           '' last_nownow_app_login_date,
           '' last_nownow_app_order_date,
           last_vitality_date last_vitality_purchase_date,
           no_of_vouchers_issued_past3m,
           no_of_vouchers_redeemed_past3m,
           number_of_active_vouchers,
           '' dm_customer_type,
           'ZA' customer_location,
           trunc(sysdate) create_date
      from W7131037.dim_customer@dwhprd dc
     inner join (select /*+ Parallel(dcc,6) Full(dcc)  */
                       customer_no,
                       max(ww_card) ww_card,
                       max(ms_card) ms_card,
                       max(alien_card) alien_card
                  from (
                        select  /*+ Parallel(a,6) Full(a)  */
                               customer_no,
                               case when a.card_no between 6007850000000000 and 6007859999999999 then a.card_no
                               end as ww_card,
                               case when a.card_no between 5900000000000000 and 5999999999999999 then a.card_no
                               end as ms_card,
                               case when a.card_no not between 5900000000000000 and 5999999999999999  and
                                         a.card_no not between 6007850000000000 and 6007859999999999 and
                                         a.card_no != a.customer_no then a.card_no
                               end as alien_card
                          from W7131037.dim_customer_card@dwhprd a
                          where customer_no > 0
                       ) dcc
                  group by customer_no
                ) cc on dc.customer_no = cc.customer_no
      left join promo_details promo1
        on dc.customer_no = promo1.primary_customer_identifier and
           promo1.rank_no = 1
      left join promo_details promo2
        on dc.customer_no = promo2.primary_customer_identifier and
           promo2.rank_no = 2
      left join promo_details promo3
        on dc.customer_no = promo3.primary_customer_identifier and
           promo3.rank_no = 3
      left join promo_details promo4
        on dc.customer_no = promo4.primary_customer_identifier and
           promo4.rank_no = 4
      left join promo_details promo5
        on dc.customer_no = promo5.primary_customer_identifier and
           promo5.rank_no = 5
      left join promo_details promo6
        on dc.customer_no = promo6.primary_customer_identifier and
           promo6.rank_no = 6
      left join nf_promo_details nfpromo1
        on dc.customer_no = nfpromo1.primary_customer_identifier and
           nfpromo1.rank_no = 1
      left join nf_promo_details nfpromo2
        on dc.customer_no = nfpromo2.primary_customer_identifier and
           nfpromo2.rank_no = 2
      left join spf_details spf
        on dc.customer_no = spf.primary_customer_identifier
      left join spnf_details spnf
        on dc.customer_no = spnf.primary_customer_identifier
      left join spfoc_details spfoc
        on dc.customer_no = spfoc.primary_customer_identifier
      left join spnfoc_details spnfoc
        on dc.customer_no = spnfoc.primary_customer_identifier
      left join voucher_details vouchers
        on dc.customer_no = vouchers.primary_customer_identifier
     where (last_transaction_date is not null or
            wfs_app_reg_ind is not null or
            ucount_cust_type is not null);

    g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;

    l_text := 'UPDATE STATS ON OUT_DWH_SVOC_DAILY TABLES';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('W7131037','OUT_DWH_SVOC_DAILY',cascade=>true, DEGREE => 8);
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
       rollback;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       p_success := false;
       raise;

end "WH_PRF_CUST_381E";
