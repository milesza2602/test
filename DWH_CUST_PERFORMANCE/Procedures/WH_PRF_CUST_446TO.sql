--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_446TO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_446TO" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Jun 2016
--  Author:      B Kirschner
--  Purpose:     Create Customer Basket and Lifestyle MART in the performance layer for STR application report (replaces Infomix/SAS query)
--               with input ex cust_basket_item table from performance layer.
--               THIS JOB RUNS MONTHLY AFTER THE START OF A NEW MONTH
--  Tables:      Input  - dwh_cust_performance.CUST_BASKET_ITEM
--                      - cust_lss_lifestyle_segments
--                      - dim_customer_race
--                      - dim_customer / dim_item
--               Output - CUST_MART_STR_PROFILE_CURR, CUST_MART_STR_PROFILE_PREV
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
g_recs_readp         integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_insertedp     integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);

g_SAT_date           date;
g_start_date         date;
g_end_date           date;
g_start_dateP        date;
g_end_dateP          date;
g_fin_yr             number         ;
g_fin_yrp            number         ;
g_fin_mth            number         ;
g_end_month          number         ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_stmt               varchar2(300);
g_yr_00              number;
g_qt_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_446U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'Create Customer Basket and Lifestyle MART for STR report';
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

    l_text := 'Create Customer Basket and Lifestyle MART for STR report - STARTED AT '||
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
   -- get the 2nd SAT of the month ...
   SELECT  NEXT_DAY (TRUNC(TODAY_DATE, 'MONTH') + 6, 'SATURDAY')
   into    g_SAT_date
   FROM    dim_control;
  
   -- Check if extract required ...

   l_text      := 'This job only runs on '||to_char(sysdate,'DDMMYY')||' and today '||to_char(sysdate,'DDMM')||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
--**************************************************************************************************
-- Main loop
--**************************************************************************************************
    -- extract is for the previous FIN month ...
--    select THIS_MN_START_DATE, THIS_MN_END_DATE, FIN_YEAR_NO, FIN_MONTH_NO 
--    into   g_start_date, g_end_date, g_fin_yr, g_fin_mth
--    from   dim_calendar
--    where  calendar_date = ADD_MONTHS(g_SAT_date,-1);
--    select distinct THIS_MN_START_DATE, THIS_MN_END_DATE, FIN_YEAR_NO
--    into   g_start_dateP, g_end_dateP, g_fin_yrP
--    from   dim_calendar
--    where  FIN_YEAR_NO  = g_fin_yr - 1
--    and    FIN_MONTH_NO = g_fin_mth;

    -- Build start/end parameters ...
    -- Curr YR (6 month span) ...
    select THIS_MN_END_DATE, FIN_YEAR_NO, FIN_MONTH_NO 
    into   g_end_date, g_fin_yr, g_fin_mth
    from   dim_calendar
    where  calendar_date = ADD_MONTHS(g_SAT_date,-1);
    select THIS_MN_start_DATE 
    into   g_start_date
    from   dim_calendar
    where  calendar_date = ADD_MONTHS(g_SAT_date,-6);
    
    -- Prev YR (6 month span) ...
    select distinct THIS_MN_END_DATE, FIN_YEAR_NO
    into   g_end_dateP, g_fin_yrP
    from   dim_calendar
    where  FIN_YEAR_NO  = g_fin_yr - 1
    and    FIN_MONTH_NO = g_fin_mth;
    select distinct THIS_MN_start_DATE
    into   g_start_dateP
    from   dim_calendar
    where  calendar_date = ADD_MONTHS(g_SAT_date,-18);
    
g_start_date := '25 Jan 2016';
g_end_date := '24 Jul 2016';
g_fin_yr := 2017;
g_fin_mth := 01;

g_start_datep := '26 Jan 2015';
g_end_datep := '26 Jul 2015';
g_fin_yrp := 2016;
   
    l_text := 'Extract Current RANGE:- '||g_start_date||' to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'Extract Previous RANGE:- '||g_start_dateP||' to '||g_end_dateP;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';
    execute immediate 'truncate table dwh_cust_performance.CUST_MART_STR_PROFILE_CURR';
    execute immediate 'truncate table dwh_cust_performance.CUST_MART_STR_PROFILE_PREV';
    commit;

    l_text := 'TABLES CLEARED FOR NEW Monthly extract ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Commencing - CURR YR load... ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
-------------------------------------
-- for testing ...

------------------------------------------

-- load the table - Curr period...
insert  /* + parallel(sys,6) */ into dwh_cust_performance.CUST_MART_STR_PROFILE_CURR sys
with 
    cust_basket as
    (
      select /*+ FULL(a) FULL(b) parallel(a,12) */      
              a.PRIMARY_CUSTOMER_IDENTIFIER,
              b.GROUP_NO,
              b.SUBGROUP_NO,
              b.DEPARTMENT_NO,
              b.CLASS_NO,
              b.SUBCLASS_NO,
            
              sum(a.item_tran_qty)                          num_items,
              sum(a.ITEM_TRAN_SELLING - a.DISCOUNT_SELLING) basket_value
              
      from    dwh_cust_performance.CUST_BASKET_ITEM a
      join    dim_item b on (a.ITEM_NO = b.ITEM_NO)
              
      where   a.TRAN_DATE between g_start_date and g_end_date
      and     primary_customer_identifier <> 998 
      and     a.tran_type in ('S','V','R')
      and     primary_customer_identifier <> 0   
      and     primary_customer_identifier is not null  
      and     b.BUSINESS_UNIT_NO not in (50, 70)

      group by
              a.PRIMARY_CUSTOMER_IDENTIFIER,
              b.GROUP_NO,
              b.SUBGROUP_NO,
              b.DEPARTMENT_NO,
              b.CLASS_NO,
              b.SUBCLASS_NO
    ),
    
    lifestyle as 
    (
      select  /*+  FULL(ls) parallel(ls,8) */
              ls.primary_customer_identifier,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 1   then 1 else 0  end) non_food_lifestyle_seg_1,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 2   then 1 else 0  end) non_food_lifestyle_seg_2,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 3   then 1 else 0  end) non_food_lifestyle_seg_3,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 4   then 1 else 0  end) non_food_lifestyle_seg_4,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 5   then 1 else 0  end) non_food_lifestyle_seg_5,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 6   then 1 else 0  end) non_food_lifestyle_seg_6,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 7   then 1 else 0  end) non_food_lifestyle_seg_7,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 8   then 1 else 0  end) non_food_lifestyle_seg_8,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 9   then 1 else 0  end) non_food_lifestyle_seg_9
  
      from    cust_lss_lifestyle_segments  ls
      where   fin_year_no       =  g_fin_yr
      and     fin_month_no      =  g_fin_mth
      and     segment_type      = 'Non-Foods'

      group by ls.primary_customer_identifier
    ),
     
    lsegtots as 
    (
      select   /*+  FULL(lst)  FULL(cb) full(dcr) full(dc) parallel(cb,8) parallel(lst,8) parallel(dcr,8)  */
              cb.primary_customer_identifier,
              cb.GROUP_NO,
              cb.SUBGROUP_NO,
              cb.DEPARTMENT_NO,
              cb.CLASS_NO,
              cb.SUBCLASS_NO,

              (case when lst.non_food_lifestyle_seg_1 = 1    then cb.basket_value else 0 end)   Tseg1,
              (case when lst.non_food_lifestyle_seg_2 = 1    then cb.basket_value else 0 end)   Tseg2,
              (case when lst.non_food_lifestyle_seg_3 = 1    then cb.basket_value else 0 end)   Tseg3,
              (case when lst.non_food_lifestyle_seg_4 = 1    then cb.basket_value else 0 end)   Tseg4,
              (case when lst.non_food_lifestyle_seg_5 = 1    then cb.basket_value else 0 end)   Tseg5,
              (case when lst.non_food_lifestyle_seg_6 = 1    then cb.basket_value else 0 end)   Tseg6,
              (case when lst.non_food_lifestyle_seg_7 = 1    then cb.basket_value else 0 end)   Tseg7,
              (case when lst.non_food_lifestyle_seg_8 = 1    then cb.basket_value else 0 end)   Tseg8,
              (case when lst.non_food_lifestyle_seg_9 = 1    then cb.basket_value else 0 end)   Tseg9,
              
              (case when dcr.DERIVED_RACE = 'Asian'          then cb.basket_value else 0 end)   Tasn,
              (case when dcr.DERIVED_RACE = 'Black'          then cb.basket_value else 0 end)   Tblk,
              (case when dcr.DERIVED_RACE = 'Coloured'       then cb.basket_value else 0 end)   Tcld,
              (case when dcr.DERIVED_RACE = 'White'          then cb.basket_value else 0 end)   Twht,
              
              (case when dc.age_acc_holder between 18 and 24 then cb.basket_value else 0  end)  age18_24,
              (case when dc.age_acc_holder between 25 and 34 then cb.basket_value else 0  end)  age25_34,
              (case when dc.age_acc_holder between 35 and 44 then cb.basket_value else 0  end)  age35_44,
              (case when dc.age_acc_holder between 45 and 54 then cb.basket_value else 0  end)  age45_54,
              (case when dc.age_acc_holder > 54              then cb.basket_value else 0  end)  age55_plus

      from    cust_basket       cb 
      left join    
              lifestyle         lst on (cb.primary_customer_identifier = lst.primary_customer_identifier)
      left join 
              dim_customer_race dcr on (cb.primary_customer_identifier = dcr.customer_no) 
      left join
              dim_customer      dc  on (cb.primary_customer_identifier = dc.customer_no) 
    ),
    
   alllsegtots as
    (
     select /*+  FULL(a)  parallel(a,8) */
            primary_customer_identifier,
            GROUP_NO,
            SUBGROUP_NO,
            DEPARTMENT_NO,
            CLASS_NO,
            SUBCLASS_NO,
           (Tseg1 + Tseg2 + Tseg3 + Tseg4 + Tseg5 + Tseg6 + Tseg7 + Tseg8 + Tseg9)     Totseg,
           (Tasn + tblk + tcld + twht)                                                 totrce,
           (age18_24 + age25_34 + age35_44 + age45_54 + age55_plus)                    totage
     from   lsegtots a
    )
   
     select  /*+ FULL(cb)  FULL(dcr)  FULL(dc)  FULL(ls) parallel(cb,8) parallel(ls,8) parallel(lt,8) parallel(ast,8) */
             to_char(g_fin_yr||g_fin_mth||1||cb.SUBCLASS_NO), 
             g_fin_yr,
             g_fin_mth,
             1,                                   -- lvl,
             cb.GROUP_NO,
             cb.SUBGROUP_NO,
             cb.DEPARTMENT_NO,
             cb.CLASS_NO,
             cb.SUBCLASS_NO,
             
             case when sum(ast.totseg) <> 0 then sum(lt.tseg1) / sum(ast.totseg) else 0 end modern_man,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg2) / sum(ast.totseg) else 0 end woolies_for_kids,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg3) / sum(ast.totseg) else 0 end classic_cross_shopper,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg4) / sum(ast.totseg) else 0 end modern_cross_shopper,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg5) / sum(ast.totseg) else 0 end bare_necessities,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg6) / sum(ast.totseg) else 0 end classic_man,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg7) / sum(ast.totseg) else 0 end accessorise_me,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg8) / sum(ast.totseg) else 0 end modern_basics,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg9) / sum(ast.totseg) else 0 end premium_tastes,
             
             case when sum(ast.totrce) <> 0 then sum(lt.tasn)  / sum(ast.totrce) else 0 end asian,
             case when sum(ast.totrce) <> 0 then sum(lt.tblk)  / sum(ast.totrce) else 0 end black,
             case when sum(ast.totrce) <> 0 then sum(lt.tcld)  / sum(ast.totrce) else 0 end coloured,
             case when sum(ast.totrce) <> 0 then sum(lt.twht)  / sum(ast.totrce) else 0 end white,
             
             case when sum(ast.totage) <> 0 then sum(lt.age18_24)   / sum(ast.totage) else 0 end ,
             case when sum(ast.totage) <> 0 then sum(lt.age25_34)   / sum(ast.totage) else 0 end ,
             case when sum(ast.totage) <> 0 then sum(lt.age35_44)   / sum(ast.totage) else 0 end ,
             case when sum(ast.totage) <> 0 then sum(lt.age45_54)   / sum(ast.totage) else 0 end ,
             case when sum(ast.totage) <> 0 then sum(lt.age55_plus) / sum(ast.totage) else 0 end ,
             
             0    lsm_6_7,
             0    lsm_8_10,
             g_date
    
    from    cust_basket       cb
    left join
            lifestyle         ls  on (cb.primary_customer_identifier = ls.primary_customer_identifier)
    left join
            lsegtots          lt  on (cb.primary_customer_identifier = lt.primary_customer_identifier and cb.GROUP_NO = lt.GROUP_NO and cb.SUBGROUP_NO = lt.SUBGROUP_NO and 
                                      cb.DEPARTMENT_NO = lt.DEPARTMENT_NO and cb.CLASS_NO = lt.class_no and cb.SUBCLASS_NO = lt.SUBCLASS_NO)
    left join
            alllsegtots       ast on (cb.primary_customer_identifier = ast.primary_customer_identifier and cb.GROUP_NO = ast.GROUP_NO and cb.SUBGROUP_NO = ast.SUBGROUP_NO and 
                                      cb.DEPARTMENT_NO = ast.DEPARTMENT_NO and cb.CLASS_NO = ast.class_no and cb.SUBCLASS_NO = ast.SUBCLASS_NO)            
            
    group by rollup (
             cb.GROUP_NO,
             cb.SUBGROUP_NO,
             cb.DEPARTMENT_NO,
             cb.CLASS_NO,
             cb.SUBCLASS_NO)
    order by cb.GROUP_NO,
             cb.SUBGROUP_NO,
             cb.DEPARTMENT_NO,
             cb.CLASS_NO,
             cb.SUBCLASS_NO;

g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
commit;

    l_text := 'Commencing - Set Level... ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

-- Set ID & Levels ...
update dwh_cust_performance.CUST_MART_STR_PROFILE_CURR
set     id_no = to_char(g_fin_yr||g_fin_mth||6||group_NO),
        lvl   = 6   
where (GROUP_NO is null and SUBGROUP_NO is null and DEPARTMENT_NO is null and CLASS_NO is null and SUBCLASS_NO is null);

update dwh_cust_performance.CUST_MART_STR_PROFILE_CURR
set     id_no = to_char(g_fin_yr||g_fin_mth||5||group_NO),
        lvl   = 5 
where (GROUP_NO is not null and SUBGROUP_NO is null and DEPARTMENT_NO is null and CLASS_NO is null and SUBCLASS_NO is null);

update dwh_cust_performance.CUST_MART_STR_PROFILE_CURR
set     id_no = to_char(g_fin_yr||g_fin_mth||4||subgroup_NO),
        lvl   = 4  
where (SUBGROUP_NO is not null and DEPARTMENT_NO is null and CLASS_NO is null and SUBCLASS_NO is null);

update dwh_cust_performance.CUST_MART_STR_PROFILE_CURR
set     id_no = to_char(g_fin_yr||g_fin_mth||3||DEPARTMENT_NO),
        lvl   = 3   
where (SUBGROUP_NO is not null and DEPARTMENT_NO is not null and CLASS_NO is null and SUBCLASS_NO is null);

update dwh_cust_performance.CUST_MART_STR_PROFILE_CURR
set     id_no = to_char(g_fin_yr||g_fin_mth||2||CLASS_NO),
        lvl   = 2  
where (SUBGROUP_NO is not null and DEPARTMENT_NO is not null and CLASS_NO is not null and SUBCLASS_NO is null);
commit;

-- B. Load table - Prev period (year - 1)...
-------------------------------------
-- for testing ...
g_start_datep := '26 Jan 2015';
g_end_datep := '26 Jul 2015';
g_fin_yrp := 2016;
------------------------------------------

l_text := 'Commencing - PREV YR load... starting at: '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

-- load the table ...
insert  /* + parallel(sys,6) */ into dwh_cust_performance.CUST_MART_STR_PROFILE_PREV
with 
    cust_basket as
    (
      select /*+ FULL(a) FULL(b) parallel(a,12) */      
              a.PRIMARY_CUSTOMER_IDENTIFIER,
              b.GROUP_NO,
              b.SUBGROUP_NO,
              b.DEPARTMENT_NO,
              b.CLASS_NO,
              b.SUBCLASS_NO,
            
              sum(a.item_tran_qty)                          num_items,
              sum(a.ITEM_TRAN_SELLING - a.DISCOUNT_SELLING) basket_value
              
      from    dwh_cust_performance.CUST_BASKET_ITEM a
      join    dim_item b on (a.ITEM_NO = b.ITEM_NO)
              
      where   a.TRAN_DATE between g_start_dateP and g_end_dateP
      and     primary_customer_identifier <> 998 
      and     a.tran_type in ('S','V','R')
      and     primary_customer_identifier <> 0   
      and     primary_customer_identifier is not null  
      and     b.BUSINESS_UNIT_NO not in (50, 70)
    
      group by
              a.PRIMARY_CUSTOMER_IDENTIFIER,
              b.GROUP_NO,
              b.SUBGROUP_NO,
              b.DEPARTMENT_NO,
              b.CLASS_NO,
              b.SUBCLASS_NO
    ),
    
    lifestyle as 
    (
      select  /*+  FULL(ls) parallel(ls,8) */
              ls.primary_customer_identifier,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 1   then 1 else 0  end) non_food_lifestyle_seg_1,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 2   then 1 else 0  end) non_food_lifestyle_seg_2,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 3   then 1 else 0  end) non_food_lifestyle_seg_3,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 4   then 1 else 0  end) non_food_lifestyle_seg_4,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 5   then 1 else 0  end) non_food_lifestyle_seg_5,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 6   then 1 else 0  end) non_food_lifestyle_seg_6,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 7   then 1 else 0  end) non_food_lifestyle_seg_7,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 8   then 1 else 0  end) non_food_lifestyle_seg_8,
              max ( case     when segment_type = 'Non-Foods' and segment_no = 9   then 1 else 0  end) non_food_lifestyle_seg_9
  
      from    cust_lss_lifestyle_segments  ls
      where   fin_year_no       =  g_fin_yrP
      and     fin_month_no      =  g_fin_mth
      and     segment_type      = 'Non-Foods'

      group by ls.primary_customer_identifier
    ),
     
    lsegtots as 
    (
      select   /*+  FULL(lst)  FULL(cb) full(dcr) full(dc) parallel(cb,8) parallel(lst,8) parallel(dcr,8) */
              cb.primary_customer_identifier,
              cb.GROUP_NO,
              cb.SUBGROUP_NO,
              cb.DEPARTMENT_NO,
              cb.CLASS_NO,
              cb.SUBCLASS_NO,

              (case when lst.non_food_lifestyle_seg_1 = 1    then cb.basket_value else 0 end)   Tseg1,
              (case when lst.non_food_lifestyle_seg_2 = 1    then cb.basket_value else 0 end)   Tseg2,
              (case when lst.non_food_lifestyle_seg_3 = 1    then cb.basket_value else 0 end)   Tseg3,
              (case when lst.non_food_lifestyle_seg_4 = 1    then cb.basket_value else 0 end)   Tseg4,
              (case when lst.non_food_lifestyle_seg_5 = 1    then cb.basket_value else 0 end)   Tseg5,
              (case when lst.non_food_lifestyle_seg_6 = 1    then cb.basket_value else 0 end)   Tseg6,
              (case when lst.non_food_lifestyle_seg_7 = 1    then cb.basket_value else 0 end)   Tseg7,
              (case when lst.non_food_lifestyle_seg_8 = 1    then cb.basket_value else 0 end)   Tseg8,
              (case when lst.non_food_lifestyle_seg_9 = 1    then cb.basket_value else 0 end)   Tseg9,
              
              (case when dcr.DERIVED_RACE = 'Asian'          then cb.basket_value else 0 end)   Tasn,
              (case when dcr.DERIVED_RACE = 'Black'          then cb.basket_value else 0 end)   Tblk,
              (case when dcr.DERIVED_RACE = 'Coloured'       then cb.basket_value else 0 end)   Tcld,
              (case when dcr.DERIVED_RACE = 'White'          then cb.basket_value else 0 end)   Twht,
              
              (case when dc.age_acc_holder between 18 and 24 then cb.basket_value else 0  end)  age18_24,
              (case when dc.age_acc_holder between 25 and 34 then cb.basket_value else 0  end)  age25_34,
              (case when dc.age_acc_holder between 35 and 44 then cb.basket_value else 0  end)  age35_44,
              (case when dc.age_acc_holder between 45 and 54 then cb.basket_value else 0  end)  age45_54,
              (case when dc.age_acc_holder > 54              then cb.basket_value else 0  end)  age55_plus

      from    cust_basket       cb 
      left join    
              lifestyle         lst on (cb.primary_customer_identifier = lst.primary_customer_identifier)
      left join 
              dim_customer_race dcr on (cb.primary_customer_identifier = dcr.customer_no) 
      left join
              dim_customer      dc  on (cb.primary_customer_identifier = dc.customer_no) 
    ),
    
    alllsegtots as
    (
     select /*+  FULL(a)  parallel(a,8) */
            primary_customer_identifier,
            GROUP_NO,
            SUBGROUP_NO,
            DEPARTMENT_NO,
            CLASS_NO,
            SUBCLASS_NO,
           (Tseg1 + Tseg2 + Tseg3 + Tseg4 + Tseg5 + Tseg6 + Tseg7 + Tseg8 + Tseg9)     Totseg,
           (Tasn + tblk + tcld + twht)                                                 totrce,
           (age18_24 + age25_34 + age35_44 + age45_54 + age55_plus)                    totage
     from   lsegtots a
    )
   
    select  /*+ FULL(cb)  FULL(dcr)  FULL(dc)  FULL(ls) parallel(cb,8) parallel(ls,8) parallel(lt,8) parallel(ast,8) */
             to_char(g_fin_yrp||g_fin_mth||1||cb.SUBCLASS_NO), 
             g_fin_yrp,
             g_fin_mth,
             1,                                   -- lvl,
             cb.GROUP_NO,
             cb.SUBGROUP_NO,
             cb.DEPARTMENT_NO,
             cb.CLASS_NO,
             cb.SUBCLASS_NO,
             
             case when sum(ast.totseg) <> 0 then sum(lt.tseg1) / sum(ast.totseg) else 0 end modern_man,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg2) / sum(ast.totseg) else 0 end woolies_for_kids,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg3) / sum(ast.totseg) else 0 end classic_cross_shopper,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg4) / sum(ast.totseg) else 0 end modern_cross_shopper,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg5) / sum(ast.totseg) else 0 end bare_necessities,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg6) / sum(ast.totseg) else 0 end classic_man,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg7) / sum(ast.totseg) else 0 end accessorise_me,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg8) / sum(ast.totseg) else 0 end modern_basics,
             case when sum(ast.totseg) <> 0 then sum(lt.tseg9) / sum(ast.totseg) else 0 end premium_tastes,
             
             case when sum(ast.totrce) <> 0 then sum(lt.tasn)  / sum(ast.totrce) else 0 end asian,
             case when sum(ast.totrce) <> 0 then sum(lt.tblk)  / sum(ast.totrce) else 0 end black,
             case when sum(ast.totrce) <> 0 then sum(lt.tcld)  / sum(ast.totrce) else 0 end coloured,
             case when sum(ast.totrce) <> 0 then sum(lt.twht)  / sum(ast.totrce) else 0 end white,
             
             case when sum(ast.totage) <> 0 then sum(lt.age18_24)   / sum(ast.totage) else 0 end ,
             case when sum(ast.totage) <> 0 then sum(lt.age25_34)   / sum(ast.totage) else 0 end ,
             case when sum(ast.totage) <> 0 then sum(lt.age35_44)   / sum(ast.totage) else 0 end ,
             case when sum(ast.totage) <> 0 then sum(lt.age45_54)   / sum(ast.totage) else 0 end ,
             case when sum(ast.totage) <> 0 then sum(lt.age55_plus) / sum(ast.totage) else 0 end ,
             
             0    lsm_6_7,
             0    lsm_8_10,
             g_date
    
    from    cust_basket       cb
    left join
            lifestyle         ls  on (cb.primary_customer_identifier = ls.primary_customer_identifier)
    left join
            lsegtots          lt  on (cb.primary_customer_identifier = lt.primary_customer_identifier and cb.GROUP_NO = lt.GROUP_NO and cb.SUBGROUP_NO = lt.SUBGROUP_NO and 
                                      cb.DEPARTMENT_NO = lt.DEPARTMENT_NO and cb.CLASS_NO = lt.class_no and cb.SUBCLASS_NO = lt.SUBCLASS_NO)
    left join
            alllsegtots       ast on (cb.primary_customer_identifier = ast.primary_customer_identifier and cb.GROUP_NO = ast.GROUP_NO and cb.SUBGROUP_NO = ast.SUBGROUP_NO and 
                                      cb.DEPARTMENT_NO = ast.DEPARTMENT_NO and cb.CLASS_NO = ast.class_no and cb.SUBCLASS_NO = ast.SUBCLASS_NO)      
    group by rollup (
             cb.GROUP_NO,
             cb.SUBGROUP_NO,
             cb.DEPARTMENT_NO,
             cb.CLASS_NO,
             cb.SUBCLASS_NO)
    order by cb.GROUP_NO,
             cb.SUBGROUP_NO,
             cb.DEPARTMENT_NO,
             cb.CLASS_NO,
             cb.SUBCLASS_NO;
   
g_recs_readp     :=  SQL%ROWCOUNT;
g_recs_insertedp :=  SQL%ROWCOUNT;
commit;

-- Set ID & Levels ...
update dwh_cust_performance.CUST_MART_STR_PROFILE_PREV
set     id_no = to_char(g_fin_yrp||g_fin_mth||6||group_NO),
        lvl   = 6   
where (GROUP_NO is null and SUBGROUP_NO is null and DEPARTMENT_NO is null and CLASS_NO is null and SUBCLASS_NO is null);

update dwh_cust_performance.CUST_MART_STR_PROFILE_PREV
set     id_no = to_char(g_fin_yrp||g_fin_mth||5||group_NO),
        lvl   = 5 
where (GROUP_NO is not null and SUBGROUP_NO is null and DEPARTMENT_NO is null and CLASS_NO is null and SUBCLASS_NO is null);

update dwh_cust_performance.CUST_MART_STR_PROFILE_PREV
set     id_no = to_char(g_fin_yrp||g_fin_mth||4||subgroup_NO),
        lvl   = 4  
where (SUBGROUP_NO is not null and DEPARTMENT_NO is null and CLASS_NO is null and SUBCLASS_NO is null);

update dwh_cust_performance.CUST_MART_STR_PROFILE_PREV
set     id_no = to_char(g_fin_yrp||g_fin_mth||3||DEPARTMENT_NO),
        lvl   = 3   
where (SUBGROUP_NO is not null and DEPARTMENT_NO is not null and CLASS_NO is null and SUBCLASS_NO is null);

update dwh_cust_performance.CUST_MART_STR_PROFILE_PREV
set     id_no = to_char(g_fin_yrp||g_fin_mth||2||CLASS_NO),
        lvl   = 2  
where (SUBGROUP_NO is not null and DEPARTMENT_NO is not null and CLASS_NO is not null and SUBCLASS_NO is null);
commit;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'Current period - read: '||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'Current period - insert: '||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text :=  'Previous period - read: '||g_recs_readp;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'Previous period - insert:'||g_recs_insertedp;
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

end WH_PRF_CUST_446to;
