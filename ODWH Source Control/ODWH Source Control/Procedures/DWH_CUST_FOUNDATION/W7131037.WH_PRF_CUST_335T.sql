-- ****** Object: Procedure W7131037.WH_PRF_CUST_335T Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_335T" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        NOV 2015
--  Author:      Alastair de Wet
--  Purpose:     Create cust_mart_company_profile fact table in the performance layer
--               with input ex cust_mart_st_wk table from performance layer.
--               THIS JOB RUNS QUARTERLY AFTER THE START OF A NEW QUARTER
--  Tables:      Input  - cust_csm_st_wk
--               Output - cust_mart_company_profile
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
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            cust_mart_company_profile%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);

g_start_week         number         ;
g_end_week           number         ;
g_end_month          number         ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_stmt               varchar2(300);
g_yr_00              number;
g_qt_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_335U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE CUST/STORE/WEEK to COMPANY PROFILE MART';
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

    l_text := 'ROLLUP OF cust_mart_company_profile EX cust_st_wk LEVEL STARTED AT '||
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


   if to_char(sysdate,'DDMM') NOT IN  ('1107','1110','1101','1104') then
      l_text      := 'This job only runs QUARTERLY ON THE 11TH and today '||to_char(sysdate,'DDMM')||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
--      return;
   end if;
   l_text      := 'This job only runs on '||to_char(sysdate,'DDMM')||' and today '||to_char(sysdate,'DDMM')||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--**************************************************************************************************
-- Main loop
--**************************************************************************************************


--    select fin_year_no,fin_quarter_no
--    into   g_yr_00,g_qt_00
--    from   dim_calendar
--    where  calendar_date = g_date - 80;

g_qt_00 := 5;
g_yr_00 := 2014;
for sub in 1..4
loop
    g_qt_00 := g_qt_00 - 1;

    if g_qt_00 = 1 then
      g_start_week := 1;
      g_end_week   := 13;
      g_end_month  := 3;
    end if;
    if g_qt_00 = 2 then
      g_start_week := 14;
      g_end_week   := 26;
      g_end_month  := 6;
    end if;
    if g_qt_00 = 3 then
      g_start_week := 27;
      g_end_week   := 39;
      g_end_month  := 9;
    end if;
    if g_qt_00 = 4 then
      g_start_week := 40;
       SELECT MAX(FIN_WEEK_NO) INTO g_end_week FROM DIM_CALENDAR_WK WHERE FIN_YEAR_NO = g_yr_00;
      g_end_month  := 12;
    end if;

    l_text := 'ROLLUP WEEK RANGE IS:- '||g_start_week||'  to '||g_end_week||' of '|| g_yr_00||' '|| g_end_month;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--execute immediate 'alter session set workarea_size_policy=manual';
--execute immediate 'alter session set sort_area_size=100000000';
    execute immediate 'alter session enable parallel dml';

    delete from cust_mart_company_profile where fin_year_no = g_yr_00 and fin_quarter_no =  g_qt_00;

    l_text := 'TABLE CLEARED FOR NEW QUARTER INSERTS '  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

insert  into cust_mart_company_profile csm
with  prof as (
select  /*+ parallel (8) */
           customer_no,
           max( case     when product_code_no in ( 1,2,3,4,5,6,7,21,11,16,32)  then 1 else 0  end) ww_store_card,
           max( case     when product_code_no in ( 28,30,80)  then 1 else 0  end) difference_card,
           max( case     when product_code_no in ( 20)  then 1 else 0  end) ww_visa_card,
           max( case     when product_code_no in ( 19)  then 1 else 0  end) myschool_card,
           max( case     when product_code_no in ( 99)  then 1 else 0  end) littleworld_member,
           max( case     when product_code_no in ( 92)  then 1 else 0  end) vitality_member
          from  dim_customer_portfolio
          where     portfolio_status_desc in ( 'ACTIVE','Open','Active')
          group by customer_no
              ),
      tier as (
           select  /*+ parallel (8) */
           customer_no,
           max( case     when month_tier = 1  then 1 else 0  end) wod_tier_1,
           max( case     when month_tier = 2  then 1 else 0  end) wod_tier_2,
           max( case     when month_tier = 3  then 1 else 0  end) wod_tier_3,
           max( case     when month_tier = 4  then 1 else 0  end) wod_tier_4,
           max( case     when month_tier = 1  then month_tier_value else 0  end) wod_tier_1_value,
           max( case     when month_tier = 2  then month_tier_value else 0  end) wod_tier_2_value,
           max( case     when month_tier = 3  then month_tier_value else 0  end) wod_tier_3_value,
           max( case     when month_tier = 4  then month_tier_value else 0  end) wod_tier_4_value
          from  cust_wod_tier_mth_detail
          where    fin_year_no       = g_yr_00
          and      fin_month_no      = g_end_month
          group by customer_no
                ),
      valseg as (
select   /*+ FULL(vs)  parallel (8) */
         primary_customer_identifier,
         max( case     when food_non_food = 'NFSHV' and   current_seg = 1  then 1 else 0  end) non_food_value_segment_1,
         max( case     when food_non_food = 'NFSHV' and   current_seg = 2  then 1 else 0  end) non_food_value_segment_2,
         max( case     when food_non_food = 'NFSHV' and   current_seg = 3  then 1 else 0  end) non_food_value_segment_3,
         max( case     when food_non_food = 'NFSHV' and   current_seg = 4  then 1 else 0  end) non_food_value_segment_4,
         max( case     when food_non_food = 'NFSHV' and   current_seg = 5  then 1 else 0  end) non_food_value_segment_5,
         max( case     when food_non_food = 'NFSHV' and   current_seg = 6  then 1 else 0  end) non_food_value_segment_6,
         max( case     when food_non_food = 'FSHV' and    current_seg = 1  then 1 else 0  end) food_value_segment_1,
         max( case     when food_non_food = 'FSHV' and    current_seg = 2  then 1 else 0  end) food_value_segment_2,
         max( case     when food_non_food = 'FSHV' and    current_seg = 3  then 1 else 0  end) food_value_segment_3,
         max( case     when food_non_food = 'FSHV' and    current_seg = 4  then 1 else 0  end) food_value_segment_4,
         max( case     when food_non_food = 'FSHV' and    current_seg = 5  then 1 else 0  end) food_value_segment_5,
         max( case     when food_non_food = 'FSHV' and    current_seg = 6  then 1 else 0  end) food_value_segment_6
from     cust_csm_value_segment vs
where    fin_year_no       = g_yr_00
and      fin_month_no      = g_end_month
group by primary_customer_identifier
              ),
      habits as (
select  /*+ FULL(sh)  parallel (8) */
        csm_customer_identifier,
        csm_shopping_habit_segment_no
from    cust_csm_shopping_habits sh
where   fin_year_no       = g_yr_00
and     fin_month_no      = g_end_month
              ),
      lifestyle as (
select  /*+  FULL(ls) parallel (ls,8) */
        ls.primary_customer_identifier,
        max ( case     when segment_type = 'Non-Foods' and segment_no = 1   then 1 else 0  end) non_food_lifestyle_seg_1,
        max ( case     when segment_type = 'Non-Foods' and segment_no = 2   then 1 else 0  end) non_food_lifestyle_seg_2,
        max ( case     when segment_type = 'Non-Foods' and segment_no = 3   then 1 else 0  end) non_food_lifestyle_seg_3,
        max ( case     when segment_type = 'Non-Foods' and segment_no = 4   then 1 else 0  end) non_food_lifestyle_seg_4,
        max ( case     when segment_type = 'Non-Foods' and segment_no = 5   then 1 else 0  end) non_food_lifestyle_seg_5,
        max ( case     when segment_type = 'Non-Foods' and segment_no = 6   then 1 else 0  end) non_food_lifestyle_seg_6,
        max ( case     when segment_type = 'Non-Foods' and segment_no = 7   then 1 else 0  end) non_food_lifestyle_seg_7,
        max ( case     when segment_type = 'Non-Foods' and segment_no = 8   then 1 else 0  end) non_food_lifestyle_seg_8,
        max ( case     when segment_type = 'Non-Foods' and segment_no = 9   then 1 else 0  end) non_food_lifestyle_seg_9,
        max ( case     when segment_type = 'Foods'     and segment_no = 1   then 1 else 0  end) food_lifestyle_seg_1,
        max ( case     when segment_type = 'Foods'     and segment_no = 2   then 1 else 0  end) food_lifestyle_seg_2,
        max ( case     when segment_type = 'Foods'     and segment_no = 3   then 1 else 0  end) food_lifestyle_seg_3,
        max ( case     when segment_type = 'Foods'     and segment_no = 4   then 1 else 0  end) food_lifestyle_seg_4,
        max ( case     when segment_type = 'Foods'     and segment_no = 6   then 1 else 0  end) food_lifestyle_seg_6,
        max ( case     when segment_type = 'Foods'     and segment_no = 7   then 1 else 0  end) food_lifestyle_seg_7,
        max ( case     when segment_type = 'Foods'     and segment_no = 8   then 1 else 0  end) food_lifestyle_seg_8
from    cust_lss_lifestyle_segments  ls
where   fin_year_no       = g_yr_00
--and     fin_month_no      = g_end_month
and     fin_month_no      = 7
group by ls.primary_customer_identifier
             ),
      st_qtr as (
select /*+ FULL(stw) FULL(dl) parallel (8) */
         csm_customer_identifier,
         company_no,
         count(distinct csm_customer_identifier) num_customers,
         sum(csm_num_st_dy_visit) num_visits,
         sum(csm_num_item) num_items,
         sum(csm_basket_value) basket_value
from     cust_csm_st_wk stw, dim_location dl
where    fin_year_no       = g_yr_00
and      fin_week_no       between g_start_week and g_end_week
and      csm_customer_identifier <> 998
and      stw.location_no   = dl.location_no
group by csm_customer_identifier,
         company_no
              )
select  /*+ FULL(a)  FULL(dc)  FULL(dcr)  FULL(prf) FULL(hbt) FULL(ttm)  parallel (8) */
         g_yr_00,
         g_qt_00,
         a.company_no,
         sum ( case     when derived_race = 'Black'               then 1 else 0  end) race_black,
         sum ( case     when derived_race = 'Coloured'            then 1 else 0  end) race_coloured,
         sum ( case     when derived_race = 'Asian'               then 1 else 0  end) race_asian,
         sum ( case     when derived_race = 'White'               then 1 else 0  end) race_white,
         sum ( case     when dc.age_acc_holder between 18 and 24  then 1 else 0  end) age18_24,
         sum ( case     when dc.age_acc_holder between 25 and 34  then 1 else 0  end) age25_34,
         sum ( case     when dc.age_acc_holder between 35 and 44  then 1 else 0  end) age35_44,
         sum ( case     when dc.age_acc_holder between 45 and 54  then 1 else 0  end) age45_54,
         sum ( case     when dc.age_acc_holder > 54               then 1 else 0  end) age55_up,
         sum ( case     when dc.gender_code = 'M'                 then 1 else 0  end) gender_m,
         sum ( case     when dc.gender_code = 'F'                 then 1 else 0  end) gender_f,
         sum ( case     when dc.preferred_language = 'Afrikaans'  then 1 else 0  end) afrikaans_preferred,
         sum ( case     when dc.preferred_language = 'English'    then 1 else 0  end) english_preferred,
         sum ( case     when ttm.email_address is not null        then 1 else 0  end) contactable_by_e_mail,
         sum ( case     when ttm.cell_no is not null              then 1 else 0  end) contactable_by_sms,
         sum ( case     when ttm.postal_address_line_1 is not null or
                             ttm.postal_address_line_2 is not null or
                             ttm.postal_address_line_3 is not null then 1 else 0  end) contactable_by_post,
         0,0,  -- Placeholder for LSM ex Knowledge Factory when available
         sum ( non_food_lifestyle_seg_1) non_food_lifestyle_seg_1,
         sum ( non_food_lifestyle_seg_2) non_food_lifestyle_seg_2,
         sum ( non_food_lifestyle_seg_3) non_food_lifestyle_seg_3,
         sum ( non_food_lifestyle_seg_4) non_food_lifestyle_seg_4,
         sum ( non_food_lifestyle_seg_5) non_food_lifestyle_seg_5,
         sum ( non_food_lifestyle_seg_6) non_food_lifestyle_seg_6,
         sum ( non_food_lifestyle_seg_7) non_food_lifestyle_seg_7,
         sum ( non_food_lifestyle_seg_8) non_food_lifestyle_seg_8,
         sum ( non_food_lifestyle_seg_9) non_food_lifestyle_seg_9,
         sum ( food_lifestyle_seg_1) food_lifestyle_seg_1 ,
         sum ( food_lifestyle_seg_2) food_lifestyle_seg_2 ,
         sum ( food_lifestyle_seg_3) food_lifestyle_seg_3 ,
         sum ( food_lifestyle_seg_4) food_lifestyle_seg_4 ,
         sum ( food_lifestyle_seg_6) food_lifestyle_seg_6 ,
         sum ( food_lifestyle_seg_7) food_lifestyle_seg_7 ,
         sum ( food_lifestyle_seg_8) food_lifestyle_seg_8 ,
         sum ( case     when csm_shopping_habit_segment_no  = 1    then 1 else 0  end) shopping_habits_1,
         sum ( case     when csm_shopping_habit_segment_no  = 2    then 1 else 0  end) shopping_habits_2,
         sum ( case     when csm_shopping_habit_segment_no  = 3    then 1 else 0  end) shopping_habits_3,
         sum ( case     when csm_shopping_habit_segment_no  = 4    then 1 else 0  end) shopping_habits_4,
         sum ( case     when csm_shopping_habit_segment_no  = 5    then 1 else 0  end) shopping_habits_5,
         sum ( case     when csm_shopping_habit_segment_no  = 6    then 1 else 0  end) shopping_habits_6,
         sum ( non_food_value_segment_1) non_food_value_segment_1,
         sum ( non_food_value_segment_2) non_food_value_segment_2,
         sum ( non_food_value_segment_3) non_food_value_segment_3,
         sum ( non_food_value_segment_4) non_food_value_segment_4,
         sum ( non_food_value_segment_5) non_food_value_segment_5,
         sum ( non_food_value_segment_6) non_food_value_segment_6,
         sum ( food_value_segment_1) food_value_segment_1,
         sum ( food_value_segment_2) food_value_segment_2,
         sum ( food_value_segment_3) food_value_segment_3,
         sum ( food_value_segment_4) food_value_segment_4,
         sum ( food_value_segment_5) food_value_segment_5,
         sum ( food_value_segment_6) food_value_segment_6,
         sum(prf.ww_store_card) ww_store_card,
         sum(prf.difference_card) difference_card,
         sum(prf.ww_visa_card) ww_visa_card,
         sum(prf.myschool_card) myschool_card,
         sum(prf.littleworld_member) littleworld_member,
         sum(prf.vitality_member) vitality_member,
         sum(tr.wod_tier_1) wod_tier_1,
         sum(tr.wod_tier_2) wod_tier_2,
         sum(tr.wod_tier_3) wod_tier_3,
         sum(tr.wod_tier_4) wod_tier_4,
         sum ( case     when derived_race = 'Black'               then a.basket_value else 0  end) race_black_value,
         sum ( case     when derived_race = 'Coloured'            then a.basket_value else 0  end) race_coloured_value,
         sum ( case     when derived_race = 'Asian'               then a.basket_value else 0  end) race_asian_value,
         sum ( case     when derived_race = 'White'               then a.basket_value else 0  end) race_white_value,
         sum ( case     when dc.age_acc_holder between 18 and 24  then a.basket_value else 0  end) age18_24_value,
         sum ( case     when dc.age_acc_holder between 25 and 34  then a.basket_value else 0  end) age25_34_value,
         sum ( case     when dc.age_acc_holder between 35 and 44  then a.basket_value else 0  end) age35_44_value,
         sum ( case     when dc.age_acc_holder between 45 and 54  then a.basket_value else 0  end) age45_54_value,
         sum ( case     when dc.age_acc_holder > 54               then a.basket_value else 0  end) age55_up_value,
         sum ( case     when dc.gender_code = 'M'                 then a.basket_value else 0  end) gender_m_value,
         sum ( case     when dc.gender_code = 'F'                 then a.basket_value else 0  end) gender_f_value,
         sum ( case     when dc.preferred_language = 'Afrikaans'  then a.basket_value else 0  end) afrikaans_preferred_value,
         sum ( case     when dc.preferred_language = 'English'    then a.basket_value else 0  end) english_preferred_value,
         sum ( case     when ttm.email_address is not null        then a.basket_value else 0  end) contactable_by_e_mail_value,
         sum ( case     when ttm.cell_no is not null              then a.basket_value else 0  end) contactable_by_sms_value,
         sum ( case     when ttm.postal_address_line_1 is not null or
                             ttm.postal_address_line_2 is not null or
                             ttm.postal_address_line_3 is not null then a.basket_value else 0  end) contactable_by_post_value,
         0,0,  -- Placeholder for LSM ex Knowledge Factory when available
         sum ( non_food_lifestyle_seg_1 * a.basket_value) non_food_lifestyle_seg_1_value,
         sum ( non_food_lifestyle_seg_2 * a.basket_value) non_food_lifestyle_seg_2_value,
         sum ( non_food_lifestyle_seg_3 * a.basket_value) non_food_lifestyle_seg_3_value,
         sum ( non_food_lifestyle_seg_4 * a.basket_value) non_food_lifestyle_seg_4_value,
         sum ( non_food_lifestyle_seg_5 * a.basket_value) non_food_lifestyle_seg_5_value,
         sum ( non_food_lifestyle_seg_6 * a.basket_value) non_food_lifestyle_seg_6_value,
         sum ( non_food_lifestyle_seg_7 * a.basket_value) non_food_lifestyle_seg_7_value,
         sum ( non_food_lifestyle_seg_8 * a.basket_value) non_food_lifestyle_seg_8_value,
         sum ( non_food_lifestyle_seg_9 * a.basket_value) non_food_lifestyle_seg_9_value,
         sum ( food_lifestyle_seg_1 * a.basket_value) food_lifestyle_seg_1_value ,
         sum ( food_lifestyle_seg_2 * a.basket_value) food_lifestyle_seg_2_value ,
         sum ( food_lifestyle_seg_3 * a.basket_value) food_lifestyle_seg_3_value ,
         sum ( food_lifestyle_seg_4 * a.basket_value) food_lifestyle_seg_4_value ,
         sum ( food_lifestyle_seg_6 * a.basket_value) food_lifestyle_seg_6_value ,
         sum ( food_lifestyle_seg_7 * a.basket_value) food_lifestyle_seg_7_value ,
         sum ( food_lifestyle_seg_8 * a.basket_value) food_lifestyle_seg_8_value ,
         sum ( case     when csm_shopping_habit_segment_no  = 1    then a.basket_value else 0  end) shopping_habits_1_value,
         sum ( case     when csm_shopping_habit_segment_no  = 2    then a.basket_value else 0  end) shopping_habits_2_value,
         sum ( case     when csm_shopping_habit_segment_no  = 3    then a.basket_value else 0  end) shopping_habits_3_value,
         sum ( case     when csm_shopping_habit_segment_no  = 4    then a.basket_value else 0  end) shopping_habits_4_value,
         sum ( case     when csm_shopping_habit_segment_no  = 5    then a.basket_value else 0  end) shopping_habits_5_value,
         sum ( case     when csm_shopping_habit_segment_no  = 6    then a.basket_value else 0  end) shopping_habits_6_value,
         sum ( non_food_value_segment_1 * a.basket_value) non_food_value_segment_1_value,
         sum ( non_food_value_segment_2 * a.basket_value) non_food_value_segment_2_value,
         sum ( non_food_value_segment_3 * a.basket_value) non_food_value_segment_3_value,
         sum ( non_food_value_segment_4 * a.basket_value) non_food_value_segment_4_value,
         sum ( non_food_value_segment_5 * a.basket_value) non_food_value_segment_5_value,
         sum ( non_food_value_segment_6 * a.basket_value) non_food_value_segment_6_value,
         sum ( food_value_segment_1 * a.basket_value) food_value_segment_1_value,
         sum ( food_value_segment_2 * a.basket_value) food_value_segment_2_value,
         sum ( food_value_segment_3 * a.basket_value) food_value_segment_3_value,
         sum ( food_value_segment_4 * a.basket_value) food_value_segment_4_value,
         sum ( food_value_segment_5 * a.basket_value) food_value_segment_5_value,
         sum ( food_value_segment_6 * a.basket_value) food_value_segment_6_value,
         sum(prf.ww_store_card * a.basket_value) ww_store_card_value,
         sum(prf.difference_card * a.basket_value) difference_card_value,
         sum(prf.ww_visa_card * a.basket_value) ww_visa_card_value,
         sum(prf.myschool_card * a.basket_value) myschool_card_value,
         sum(prf.littleworld_member * a.basket_value) littleworld_member_value,
         sum(prf.vitality_member * a.basket_value) vitality_member_value,
         sum(tr.wod_tier_1_value) wod_tier_1_value,
         sum(tr.wod_tier_2_value) wod_tier_2_value,
         sum(tr.wod_tier_3_value) wod_tier_3_value,
         sum(tr.wod_tier_4_value) wod_tier_4_value,
         count(distinct a.csm_customer_identifier) num_customers,
         sum(a.num_visits) num_visits,
         sum(a.num_items) num_items,
         sum(a.basket_value) basket_value,
         g_date
from     st_qtr a
left outer join
         dim_customer_race dcr
on       a.csm_customer_identifier = dcr.customer_no
left outer join
         dim_customer dc
on       a.csm_customer_identifier = dc.customer_no
left outer join
         prof prf
on       a.csm_customer_identifier = prf.customer_no
left outer join
         tier tr
on       a.csm_customer_identifier = tr.customer_no
left outer join
         habits hbt
on       a.csm_customer_identifier = hbt.csm_customer_identifier
left outer join
         lifestyle lsty
on       a.csm_customer_identifier = lsty.primary_customer_identifier
left outer join
         cust_talktome ttm
on       a.csm_customer_identifier = ttm.customer_no
left outer join
         valseg vseg
on       a.csm_customer_identifier = vseg.primary_customer_identifier
group by a.company_no
order by a.company_no
         ;

  g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


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

end wh_prf_cust_335t;
