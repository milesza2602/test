--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_382E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_382E" (p_forall_limit in integer,p_success out boolean,p_run_date in date)
as

--**************************************************************************************************
--  Date:        JUL 2017
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
--  20 July 17  Theo Filander Only retain promo and trade columns.
--  Known Issue Although performing update stats after truncating a table is unnecessary, we've found
--              that if this is not done the program aborts with ORA-8103 and ORA-20011 errors.
--              The sleeps gives the engine the required time to commit.
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_382E';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE THE WEEKLY CUSTOMER SVOC DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

procedure load_promotion_field_data as
begin
  l_text := 'TRUNCATE SVOC PROMOTIONAL TEMP TABLES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom';
  dbms_lock.sleep(0.25);        --
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_wk_trans';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_top_item';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_item';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_class';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_dept';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_lss_item';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_lss_notrn';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_lss_notrn2';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_notrnseg';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_ranked';

  l_text := 'BUILD TEMP TABLE STATS AFTER TRUNCATION';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_WK_TRANS',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_TOP_ITEM',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_ITEM',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_CLASS',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_DEPT',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_LSS_ITEM',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_LSS_NOTRN',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_LSS_NOTRN2',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_NOTRNSEG',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'LOAD FOOD PROMOTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert into dwh_cust_performance.temp_cust_svoc_prom tmp
      (prom_no,prom_desc,prom_week_start_no,prom_week_end_no,item_no,class_no,department_no,base_rsp)
    with cal as (select calendar_date, fin_week_no
                   from dim_calendar)
    select /*+ parallel(dimi,4) full(dimi) */
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
              from dwh_performance.dim_prom dimp
              left outer join dwh_foundation.fnd_prom_item fpi on dimp.prom_no = fpi.prom_no
              left outer join dwh_foundation.fnd_prom_threshold_item fpti on dimp.prom_no = fpti.prom_no
              left outer join (select prom_no, item_no
                                 from dwh_foundation.fnd_prom_mix_match_buy
                               union
                               select prom_no, item_no
                                 from dwh_foundation.fnd_prom_mix_match_get) fpmm on dimp.prom_no = fpmm.prom_no
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

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'LOAD CUSTOMER FOOD TRANSACTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_wk_trans tmp
    (primary_customer_identifier,segment_no,item_no,class_no,department_no,base_rsp,item_qty)
  with tbl as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from dwh_cust_performance.cust_lss_lifestyle_segments seg
                where segment_type in ('Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no),
       prm as (select distinct department_no
                 from dwh_cust_performance.temp_cust_svoc_prom tmp)
  select /*+ parallel(cbi,6) parallel(dimi,6) full(cbi) full(dimi) */
         cbi.primary_customer_identifier,
         clls.segment_no,
         cbi.item_no,
         dimi.class_no,
         dimi.department_no,
         dimi.base_rsp,
         sum(cbi.item_tran_qty) item_qty
    from dwh_cust_performance.cust_basket_item cbi
   inner join dwh_performance.dim_item dimi
      on cbi.item_no = dimi.item_no
   inner join (select /*+ full(prm) */
                      department_no
                 from prm) promo
      on dimi.department_no = promo.department_no -- filter the transactions on departments sold on promotion
    left outer join (select /*+ full(tbl) */
                            primary_customer_identifier,
                            segment_no
                       from tbl) clls
      on cbi.primary_customer_identifier = clls.primary_customer_identifier
   where cbi.tran_date between g_8wk_start_date and g_8wk_end_date -- get transactions for 8 weeks
     and cbi.tran_type not in ('P','N','L','R','Q')
     and cbi.primary_customer_identifier not in (998) -- cash
     and cbi.customer_no > 0 -- extract for C2 customers only
  group by cbi.primary_customer_identifier,
           clls.segment_no,
           cbi.item_no,
           dimi.class_no,
           dimi.department_no,
           dimi.base_rsp;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_WK_TRANS',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RETURN PROMOTIONS WITH ONE ITEM LINKED';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert into dwh_cust_performance.temp_cust_svoc_prom_top_item tmp
    (prom_no,item_no,base_rsp)
  select prom_no,
         item_no,
         base_rsp
    from dwh_cust_performance.temp_cust_svoc_prom prm
   where prom_no in (select prom_no
                       from dwh_cust_performance.temp_cust_svoc_prom prm
                      group by prom_no
                     having count(*) = 1);
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_TOP_ITEM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS AND ITEMS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_item tmp
    (primary_customer_identifier,prom_no,item_qty)
  with prm as (select distinct prom_no,item_no
                 from dwh_cust_performance.temp_cust_svoc_prom prm)
  select /*+ parallel(trn,6) full(trn)*/
         trn.primary_customer_identifier,
         tp.prom_no,
         sum(trn.item_qty) item_qty
    from (select /*+ parallel(tt,6) full(tt) */
                 primary_customer_identifier,
                 item_no,
                 sum(tt.item_qty) item_qty
            from dwh_cust_performance.temp_cust_svoc_wk_trans tt
           group by primary_customer_identifier,item_no) trn
   inner join (select /*+ full(prm) */
                      prom_no,item_no
                 from prm) tp
      on trn.item_no = tp.item_no
   group by trn.primary_customer_identifier,tp.prom_no;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_ITEM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS AND ITEMS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from dwh_cust_performance.temp_cust_svoc_prom prm)
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
                            from (select /*+ parallel(tmp1,4) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_item tmp1
                                 ) allprom
                            left join dwh_cust_performance.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS AND CLASSES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_class tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from dwh_cust_performance.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       prm as (select distinct prom_no,class_no
                 from dwh_cust_performance.temp_cust_svoc_prom prm)
  select /*+ parallel(trn,6) full(trn) */
         trn.primary_customer_identifier,
         tp.prom_no,
         sum(trn.item_qty) item_qty
    from (select /*+ parallel(tt,6) full(tt) */
                 primary_customer_identifier,
                 class_no,
                 sum(tt.item_qty) item_qty
            from dwh_cust_performance.temp_cust_svoc_wk_trans tt
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

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_CLASS',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS, ITEMS AND CLASSES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_ranked';
  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from dwh_cust_performance.temp_cust_svoc_prom prm)
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
                            from (select /*+ parallel(tmp1,4) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,4) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_class tmp2
                                 ) allprom
                            left join dwh_cust_performance.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS AND DEPARTMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_dept tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from dwh_cust_performance.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       prm as (select distinct prom_no,department_no
                 from dwh_cust_performance.temp_cust_svoc_prom prm)
  select /*+ parallel(trn,6) full(trn) */
         trn.primary_customer_identifier,
         tp.prom_no,
         sum(trn.item_qty) item_qty
    from (select /*+ parallel(tt,6) full(tt) */
                 primary_customer_identifier,
                 department_no,
                 sum(tt.item_qty) item_qty
            from dwh_cust_performance.temp_cust_svoc_wk_trans tt
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

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_DEPT',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS, ITEMS, CLASSES AND DEPARTMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_ranked';
  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from dwh_cust_performance.temp_cust_svoc_prom prm)
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
                            from (select /*+ parallel(tmp1,4) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,4) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_class tmp2
                                  union all
                                  select /*+ parallel(tmp3,4) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_dept tmp3
                                 ) allprom
                            left join dwh_cust_performance.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR SEGMENTS AND ITEMS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_lss_item tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from dwh_cust_performance.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       prm as (select distinct prom_no,item_no
                 from dwh_cust_performance.temp_cust_svoc_prom prm)
  select /*+ parallel(cust,6) parallel(seg,6) full(cust) full(seg)*/
         cust.primary_customer_identifier,
         seg.prom_no,
         seg.item_qty
    from (select /*+ parallel(trn,6) full(trn) */
                 distinct primary_customer_identifier,segment_no
            from dwh_cust_performance.temp_cust_svoc_wk_trans trn
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
                         from dwh_cust_performance.temp_cust_svoc_wk_trans tt
                        where segment_no is not null
                        group by segment_no,item_no) trn
                inner join (select /*+ full(prm) */
                                   prom_no,item_no
                              from prm) tp
                   on trn.item_no = tp.item_no
                group by trn.segment_no,tp.prom_no) seg
      on cust.segment_no = seg.segment_no;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_LSS_ITEM',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS, ITEMS, CLASSES, DEPARTMENTS AND LIFESTYLE SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_ranked';
  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from dwh_cust_performance.temp_cust_svoc_prom prm)
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
                            from (select /*+ parallel(tmp1,4) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,4) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_class tmp2
                                  union all
                                  select /*+ parallel(tmp3,4) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_dept tmp3
                                  union all
                                  select /*+ parallel(tmp4,4) full(tmp4) */
                                         primary_customer_identifier,prom_no,item_qty,4 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_lss_item tmp4
                                 ) allprom
                            left join dwh_cust_performance.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS WITH NO TRANSACTIONS WITH LIFESTYLE SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_lss_notrn tmp
    (segment_no,prom_no,item_qty)
  with prm as (select distinct prom_no,item_no
                 from dwh_cust_performance.temp_cust_svoc_prom prm)
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
                                    from dwh_cust_performance.temp_cust_svoc_wk_trans tt
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

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_LSS_NOTRN',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS WITH RANKED LIFESTYLE SEGMENTS AND NO TRANSACTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_lss_notrn2 tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from dwh_cust_performance.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       lss as (select /*+ parallel(seg,6) full(seg) */
                      primary_customer_identifier,
                      segment_no
                 from dwh_cust_performance.cust_lss_lifestyle_segments seg
                where segment_type in ('Foods')
                  and fin_year_no = g_lss_year_no
                  and fin_month_no = g_lss_month_no),
       lssprm as (select segment_no,
                         prom_no,
                         item_qty
                    from dwh_cust_performance.temp_cust_svoc_prom_lss_notrn)
  select /*+ parallel(cust,6) parallel(promo,6) full(cust) full(promo)*/
         cust.primary_customer_identifier,
         promo.prom_no,
         promo.item_qty
    from (select /*+ parallel(dim,6) full(dim) */
                 dim.customer_no primary_customer_identifier,
                 seg.segment_no
            from dwh_cust_performance.dim_customer dim
           inner join (select /*+ full(lss) */
                              primary_customer_identifier,
                              segment_no
                         from lss) seg
              on dim.customer_no = seg.primary_customer_identifier
           where dim.customer_no not in (select /*+ full(tbl) */
                                                primary_customer_identifier
                                           from tbl)
             and dim.last_transaction_date is not null
         ) cust
   inner join (select /*+ full(lssprm) */
                      segment_no,
                      prom_no,
                      item_qty
                 from lssprm) promo
      on cust.segment_no = promo.segment_no;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_LSS_NOTRN2',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK PROMOTIONS FOR CUSTOMERS, ITEMS, CLASSES, DEPARTMENTS AND LIFESTYLE SEGMENTS (W/WO TRANSACTIONS)';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_ranked';
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from dwh_cust_performance.temp_cust_svoc_prom prm)
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
                            from (select /*+ parallel(tmp1,4) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,4) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_class tmp2
                                  union all
                                  select /*+ parallel(tmp3,4) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_dept tmp3
                                  union all
                                  select /*+ parallel(tmp4,4) full(tmp4) */
                                         primary_customer_identifier,prom_no,item_qty,4 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_lss_item tmp4
                                  union all
                                  select /*+ parallel(tmp5,4) full(tmp5) */
                                         primary_customer_identifier,prom_no,item_qty,5 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_lss_notrn2 tmp5
                                 ) allprom
                            left join dwh_cust_performance.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'SUMMARISE ITEM_QTY FOR CUSTOMERS WITH NO TRANSACTIONS AND NO SEGMENTS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_notrnseg tmp
    (primary_customer_identifier,prom_no,item_qty)
  with tbl as (select /*+ parallel(rnkd,6) full(rnkd) */
                      primary_customer_identifier
                 from dwh_cust_performance.temp_cust_svoc_prom_ranked rnkd
                where rank_no = 6),
       prm as (select distinct prom_no,item_no
                 from dwh_cust_performance.temp_cust_svoc_prom prm)
  select /*+ parallel(cust,6) parallel(promo,6) full(cust) full(promo)*/
         cust.primary_customer_identifier,
         promo.prom_no,
         promo.item_qty
    from (select /*+ parallel(dim,6) full(dim) */
                 distinct customer_no primary_customer_identifier
            from dwh_cust_performance.dim_customer dim
           where dim.customer_no not in (select /*+ full(tbl) */
                                                primary_customer_identifier
                                           from tbl)
             and dim.last_transaction_date is not null
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
                                                 from dwh_cust_performance.temp_cust_svoc_wk_trans tt
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

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_NOTRNSEG',cascade=>true, DEGREE => 8);
  commit;

  l_text := 'RANK ALL PROMOTIONS';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_ranked';
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  insert /*+ APPEND parallel(tmp,6) */ into dwh_cust_performance.temp_cust_svoc_prom_ranked tmp
    (primary_customer_identifier,prom_desc,tag_no,tag_desc,base_rsp,rank_no)
  with prom_det as (select distinct prom_no,prom_desc,prom_week_start_no,prom_week_end_no
                      from dwh_cust_performance.temp_cust_svoc_prom prm)
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
                            from (select /*+ parallel(tmp1,4) full(tmp1) */
                                         primary_customer_identifier,prom_no,item_qty,1 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_item tmp1
                                  union all
                                  select /*+ parallel(tmp2,4) full(tmp2) */
                                         primary_customer_identifier,prom_no,item_qty,2 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_class tmp2
                                  union all
                                  select /*+ parallel(tmp3,4) full(tmp3) */
                                         primary_customer_identifier,prom_no,item_qty,3 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_dept tmp3
                                  union all
                                  select /*+ parallel(tmp4,4) full(tmp4) */
                                         primary_customer_identifier,prom_no,item_qty,4 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_lss_item tmp4
                                  union all
                                  select /*+ parallel(tmp5,4) full(tmp5) */
                                         primary_customer_identifier,prom_no,item_qty,5 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_lss_notrn2 tmp5
                                  union all
                                  select /*+ parallel(tmp6,4) full(tmp6) */
                                         primary_customer_identifier,prom_no,item_qty,6 precedence
                                    from dwh_cust_performance.temp_cust_svoc_prom_notrnseg tmp6
                                 ) allprom
                            left join dwh_cust_performance.temp_cust_svoc_prom_top_item tidet
                              on allprom.prom_no = tidet.prom_no
                           inner join prom_det pdet
                              on allprom.prom_no = pdet.prom_no
                         ) tagprom
                 ) distprom
           where distprom.rank_no = 1
         ) rankprom
   where rankprom.rank_no <= 6;
  commit;

  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_RANKED',cascade=>true, DEGREE => 8);
  commit;

  -- clean up disc space, truncate all temp tables, except the total ranking one
  l_text := 'TRUNCATE SVOC PROMOTIONAL TEMP TABLES';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_wk_trans';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_top_item';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_item';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_class';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_dept';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_lss_item';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_lss_notrn';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_lss_notrn2';
  dbms_lock.sleep(0.25);
  execute immediate 'TRUNCATE TABLE dwh_cust_performance.temp_cust_svoc_prom_notrnseg';

  l_text := 'BUILD TEMP TABLE STATS AFTER TRUNCATION';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_WK_TRANS',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_TOP_ITEM',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_ITEM',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_CLASS',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_DEPT',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_LSS_ITEM',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_LSS_NOTRN',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_LSS_NOTRN2',cascade=>true, DEGREE => 8);
--  dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_SVOC_PROM_NOTRNSEG',cascade=>true, DEGREE => 8);
  commit;
exception
  when others then
    l_message := 'LOAD PROMOTION FIELD DATA - OTHER ERROR '||sqlcode||' '||sqlerrm;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
    raise;
end load_promotion_field_data;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    
    if to_char(trunc(sysdate),'d') <> 3 then
        l_text := 'This job only runs on a Tuesday. Today is '||to_char(trunc(sysdate),'DAY')||'.';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        p_success := true;
        return;
    end if;
    
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    execute immediate 'alter session enable parallel dml';

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'BUILD OF CUSTOMER INSIGHTS TO OUT_DWH_SVOC_WEEKLY STARTED AT '||
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
    select this_week_start_date,
           this_week_start_date-(8*7),
           this_week_start_date-1,
           this_week_start_date+13
      into g_this_wk_start_date,
           g_8wk_start_date,
           g_8wk_end_date,
           g_prom_end_date
      from dim_calendar
     where calendar_date = g_prom_run_date;

    -- when the weekly extract runs you need to get the latest month loaded in the lss table
    select /*+ parallel(seg,6) full(seg) */
           max(fin_year_no) fin_year_no
      into g_lss_year_no
      from dwh_cust_performance.cust_lss_lifestyle_segments seg;

    select /*+ parallel(seg,6) full(seg) */
           max(fin_month_no) fin_month_no
      into g_lss_month_no
      from dwh_cust_performance.cust_lss_lifestyle_segments seg
     where fin_year_no = g_lss_year_no;

    l_text := 'EXTRACT DATA FOR YEAR : '||g_year_no||'  WEEK : '||g_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'POPULATE SVOC PROMOTIONAL DATA' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    load_promotion_field_data;

--    l_text := 'TRUNCATE TABLE OUT_DWH_SVOC_WEEKLY.' ;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    execute immediate 'TRUNCATE TABLE "DWH_CUST_PERFORMANCE"."OUT_DWH_SVOC_WEEKLY"';
--    dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','OUT_DWH_SVOC_WEEKLY',cascade=>true, DEGREE => 8);
--    commit;

    l_text := 'POPULATE TABLE OUT_DWH_SVOC_WEEKLY.' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    merge /*+ Parallel(od ,6) */ into DWH_CUST_PERFORMANCE.OUT_DWH_SVOC_WEEKLY od
    using (
            with promo_details as (select /*+ Parallel(prm,6) full(prm) */
                                          primary_customer_identifier,
                                          prom_desc,
                                          tag_no,
                                          tag_desc,
                                          base_rsp,
                                          rank_no
                                     from dwh_cust_performance.temp_cust_svoc_prom_ranked prm)
            select /*+ Parallel(cc,6) Parallel(dc,6) Full(cc) Full(dc) */
                   g_year_no fin_year_no,
                   g_week_no fin_week_no,
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
                   promo1.tag_no trade_top_foods_sku01,
                   promo2.tag_no trade_top_foods_sku02,
                   promo3.tag_no trade_top_foods_sku03,
                   promo4.tag_no trade_top_foods_sku04,
                   promo5.tag_no trade_top_foods_sku05,
                   promo6.tag_no trade_top_foods_sku06,
                   promo1.tag_desc trade_top_foods_tag01,
                   promo2.tag_desc trade_top_foods_tag02,
                   promo3.tag_desc trade_top_foods_tag03,
                   promo4.tag_desc trade_top_foods_tag04,
                   promo5.tag_desc trade_top_foods_tag05,
                   promo6.tag_desc trade_top_foods_tag06,
                   promo1.base_rsp trade_top_foods_price01,
                   promo2.base_rsp trade_top_foods_price02,
                   promo3.base_rsp trade_top_foods_price03,
                   promo4.base_rsp trade_top_foods_price04,
                   promo5.base_rsp trade_top_foods_price05,
                   promo6.base_rsp trade_top_foods_price06,
                   '' trade_top_foods_saving01,
                   '' trade_top_foods_saving02,
                   '' trade_top_foods_saving03,
                   '' trade_top_foods_saving04,
                   '' trade_top_foods_saving05,
                   '' trade_top_foods_saving06,
                   '' trade_top_foods_mech01,
                   '' trade_top_foods_mech02,
                   '' trade_top_foods_mech03,
                   '' trade_top_foods_mech04,
                   '' trade_top_foods_mech05,
                   '' trade_top_foods_mech06,
                   trunc(sysdate) create_date
              from dwh_cust_performance.dim_customer dc
             inner join (select /*+ Parallel(dcc,6) Full(dcc)  */ distinct
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
                                  from dwh_cust_performance.dim_customer_card a
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
             where last_transaction_date is not null
        ) td
        on (od.fin_year_no                 = td.fin_year_no and
            od.fin_week_no                 = td.fin_week_no and
            od.primary_customer_identifier = td.primary_customer_identifier)
       when matched then
            update
               set od.c2_customer_no            = td.customer_no,
                   od.ww_card_no                = td.ww_card,
                   od.ms_card_no                = td.ms_card,
                   od.alien_card_token          = td.alien_card,
                   od.retailsoft_customer_no    = td.retailsoft_customer_no,
                   od.trade_top_foods_sku01     = td.trade_top_foods_sku01,
                   od.trade_top_foods_sku02     = td.trade_top_foods_sku02,
                   od.trade_top_foods_sku03     = td.trade_top_foods_sku03,
                   od.trade_top_foods_sku04     = td.trade_top_foods_sku04,
                   od.trade_top_foods_sku05     = td.trade_top_foods_sku05,
                   od.trade_top_foods_sku06     = td.trade_top_foods_sku06,
                   od.trade_top_foods_tag01     = td.trade_top_foods_tag01,
                   od.trade_top_foods_tag02     = td.trade_top_foods_tag02,
                   od.trade_top_foods_tag03     = td.trade_top_foods_tag03,
                   od.trade_top_foods_tag04     = td.trade_top_foods_tag04,
                   od.trade_top_foods_tag05     = td.trade_top_foods_tag05,
                   od.trade_top_foods_tag06     = td.trade_top_foods_tag06,
                   od.trade_top_foods_price01   = td.trade_top_foods_price01,
                   od.trade_top_foods_price02   = td.trade_top_foods_price02,
                   od.trade_top_foods_price03   = td.trade_top_foods_price03,
                   od.trade_top_foods_price04   = td.trade_top_foods_price04,
                   od.trade_top_foods_price05   = td.trade_top_foods_price05,
                   od.trade_top_foods_price06   = td.trade_top_foods_price06,
                   od.trade_top_foods_saving01  = td.trade_top_foods_saving01,
                   od.trade_top_foods_saving02  = td.trade_top_foods_saving02,
                   od.trade_top_foods_saving03  = td.trade_top_foods_saving03,
                   od.trade_top_foods_saving04  = td.trade_top_foods_saving04,
                   od.trade_top_foods_saving05  = td.trade_top_foods_saving05,
                   od.trade_top_foods_saving06  = td.trade_top_foods_saving06,
                   od.trade_top_foods_mech01    = td.trade_top_foods_mech01,
                   od.trade_top_foods_mech02    = td.trade_top_foods_mech02,
                   od.trade_top_foods_mech03    = td.trade_top_foods_mech03,
                   od.trade_top_foods_mech04    = td.trade_top_foods_mech04,
                   od.trade_top_foods_mech05    = td.trade_top_foods_mech05,
                   od.trade_top_foods_mech06    = td.trade_top_foods_mech06,
                   od.create_date               = td.create_date
       when not matched then
            insert (fin_year_no,
                    fin_week_no,
                    primary_customer_identifier,
                    c2_customer_no,
                    ww_card_no,
                    ms_card_no,
                    alien_card_token,
                    retailsoft_customer_no,
                    trade_top_foods_sku01,
                    trade_top_foods_sku02,
                    trade_top_foods_sku03,
                    trade_top_foods_sku04,
                    trade_top_foods_sku05,
                    trade_top_foods_sku06,
                    trade_top_foods_tag01,
                    trade_top_foods_tag02,
                    trade_top_foods_tag03,
                    trade_top_foods_tag04,
                    trade_top_foods_tag05,
                    trade_top_foods_tag06,
                    trade_top_foods_price01,
                    trade_top_foods_price02,
                    trade_top_foods_price03,
                    trade_top_foods_price04,
                    trade_top_foods_price05,
                    trade_top_foods_price06,
                    trade_top_foods_saving01,
                    trade_top_foods_saving02,
                    trade_top_foods_saving03,
                    trade_top_foods_saving04,
                    trade_top_foods_saving05,
                    trade_top_foods_saving06,
                    trade_top_foods_mech01,
                    trade_top_foods_mech02,
                    trade_top_foods_mech03,
                    trade_top_foods_mech04,
                    trade_top_foods_mech05,
                    trade_top_foods_mech06,
                    create_date
                   )
            values (td.fin_year_no,
                    td.fin_week_no,
                    td.primary_customer_identifier,
                    td.customer_no,
                    td.ww_card,
                    td.ms_card,
                    td.alien_card,
                    td.retailsoft_customer_no,
                    td.trade_top_foods_sku01,
                    td.trade_top_foods_sku02,
                    td.trade_top_foods_sku03,
                    td.trade_top_foods_sku04,
                    td.trade_top_foods_sku05,
                    td.trade_top_foods_sku06,
                    td.trade_top_foods_tag01,
                    td.trade_top_foods_tag02,
                    td.trade_top_foods_tag03,
                    td.trade_top_foods_tag04,
                    td.trade_top_foods_tag05,
                    td.trade_top_foods_tag06,
                    td.trade_top_foods_price01,
                    td.trade_top_foods_price02,
                    td.trade_top_foods_price03,
                    td.trade_top_foods_price04,
                    td.trade_top_foods_price05,
                    td.trade_top_foods_price06,
                    td.trade_top_foods_saving01,
                    td.trade_top_foods_saving02,
                    td.trade_top_foods_saving03,
                    td.trade_top_foods_saving04,
                    td.trade_top_foods_saving05,
                    td.trade_top_foods_saving06,
                    td.trade_top_foods_mech01,
                    td.trade_top_foods_mech02,
                    td.trade_top_foods_mech03,
                    td.trade_top_foods_mech04,
                    td.trade_top_foods_mech05,
                    td.trade_top_foods_mech06,
                    td.create_date
                   );

    g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;

    l_text := 'UPDATE STATS ON OUT_DWH_SVOC_WEEKLY TABLES';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','OUT_DWH_SVOC_WEEKLY',cascade=>true, DEGREE => 8);
    commit;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
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

end WH_PRF_CUST_382E;
