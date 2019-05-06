--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_383E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_383E" (p_forall_limit in integer,p_success out boolean,p_run_date in date)
as
--**************************************************************************************************
--  Date:        APR 2016
--  Author:      Theo Filander
--  Purpose:     Create the Monthly Customer Experience interface for SVOC.
--               with input ex cust_basket_item table from performance layer.
--               THIS JOB RUNS MONTHLY AFTER THE START OF A NEW MONTH
--  Tables:      Input  - dim_customer
--                      - fnd_customer_card
--                      - dim_customer_store_of_pref
--                      - cust_lss_lifestyle_segments
--                      - cust_csm_value_segment
--                      - cust_csm_shopping_habits
--                      - cust_db_company_month
--                      - cust_db_dept_month
--                      - cust_db_group_month
--                      - cust_db_subgroup_month
--                      - cust_db_business_unit_month
--               Output - OUT_DWH_CUST_INSIGHTS
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  27 July 2016 - Mariska M - Add dim_customer_card and use cust_db_company_month only for customer base for cust_db
--  August 2016  - Mariska M - Only return data for C2 Customers
--  September 2016  - Mariska M - Remove data where all fields are 0, change the source for NUMBER_OF_ITEMS_BY_MONTH,
--                    MONTHLY_BASKET_VALUE and NUMBER_OF_VISITS_BY_MONTH to the business unit table

--  June 2017       - Theo Filander - Enhancements BCB-263
--  Rename Columns                    NUMBER_OF_ITEMS_BY_MONTH  to TOTAL_ITEMS_PAST12M
--                                    MONTHLY_BASKET_VALUE      to TOTAL_SPEND_PAST12M
--                                    NUMBER_OF_VISITS_BY_MONTH to TOTAL_VISITS_PAST12M
--                                    MODERN_ITEMS_PAST12M      to MOD_CONTEMP_ITEMS_PAST12M
--                                    MODERN_SPEND_PAST12M      to MOD_CONTEMP_SPEND_PAST12M
--                                    MODERN_VISITS_PAST12M     to MOD_CONTEMP_VISITS_PAST12M
-- Add 57 additional columns
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
g_found              boolean;
g_date               date          := trunc(sysdate);

g_start_week         number;
g_end_week           number;
g_year_no            number(4,0);
g_month_no           number(2,0);
g_yesterday          date          := trunc(sysdate) - 1;
g_run_date           date          := trunc(sysdate);
g_fin_day_no         dim_calendar.fin_day_no%type;
g_this_mn_start_date date;
g_this_mn_end_date   date;

g_stmt               varchar2(300);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_383E';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE THE MONTHLY SUBSCRIBER KEY CUSTOMER INSIGHTS DATA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

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

    l_text := 'BUILD OF CUSTOMER INSIGHTS TO OUT_DWH_CUST_INSIGHTS_SK STARTED AT '||
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

        execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Determine if this is a day on which we process
--**************************************************************************************************
    if p_run_date is not null or p_run_date <> '' then
      select this_mn_start_date
        into g_this_mn_start_date
        from dim_calendar
       where calendar_date = p_run_date;
    else
      select this_mn_start_date, this_mn_end_date
        into g_this_mn_start_date, g_this_mn_end_date
        from dwh_performance.dim_calendar
       where calendar_date = (select this_mn_start_date - 1
                                from dim_calendar
                               where calendar_date = trunc(sysdate));

      -- this job will run on the 12th day of the new month, to allow for the cust_db table to be populated with data
      g_run_date := g_this_mn_end_date + 11;
      if trunc(sysdate) <> g_run_date then
        l_text := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text := dwh_constants.vc_log_draw_line;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        p_success := true;
        return;
      end if;
    end if;
--**************************************************************************************************
-- Main loop
--**************************************************************************************************
    select fin_year_no, fin_month_no
      into g_year_no, g_month_no
      from dim_calendar
     where calendar_date = g_this_mn_start_date;

    l_text := 'EXTRACT DATA FOR YEAR :- '||g_year_no||'  MONTH :- '||g_month_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

---
    l_text := 'Truncate TEMP_CUST_INSIGHTS.' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table "DWH_CUST_PERFORMANCE"."TEMP_CUST_INSIGHTS"';
    commit;

--    l_text := 'UPDATE STATS ON TEMP_CUST_INSIGHTS TABLES';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CUST_INSIGHTS',estimate_percent=>1, DEGREE => 32);

    commit;

    l_text := 'Truncate OUT_DWH_CUST_INSIGHTS_SK.' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table "DWH_CUST_PERFORMANCE"."OUT_DWH_CUST_INSIGHTS_SK"';
    commit;

--    l_text := 'UPDATE STATS ON OUT_DWH_CUST_INSIGHTS_SK TABLES';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','OUT_DWH_CUST_INSIGHTS_SK',estimate_percent=>1, DEGREE => 32);

    commit;

    l_text := 'Populate TEMP_CUST_INSIGHTS. ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
---

--insert into dwh_cust_performance.out_dwh_cust_insights_sk sk
insert into dwh_cust_performance.temp_cust_insights
select /* Parallel(6)  */ 
       nvl(non.subscriber_key,fds.subscriber_key) subscriber_key,
       sa.segment_desc              non_food_life_seg_desc,
       sb.segment_desc              food_life_seg_desc,
       sc.segment_desc              nfshv_current_seg_desc,            
       sd.segment_desc              fshv_current_seg_desc,             
       se.segment_desc              shopping_habit_segment_desc,
       total_items_past12m,
       total_spend_past12m,
       total_visits_past12m,
       womens_studio_w_items_past12m,
       womens_studio_w_spend_past12m,
       womens_studio_w_visits_past12m,
       womens_re_items_past12m,
       womens_re_spend_past12m,
       womens_re_visits_past12m,
       mens_re_items_past12m,
       mens_re_spend_past12m,
       mens_re_visits_past12m,
       mens_studio_w_items_past12m,
       mens_studio_w_spend_past12m,
       mens_studio_w_visits_past12m,
       womens_outerw_items_past12m,
       womens_outerw_spend_past12m,
       womens_outerw_visits_past12m,
       womens_lingerie_items_past12m,
       womens_lingerie_spend_past12m,
       womens_lingerie_visits_past12m,
       kidswear_items_past12m,
       kidswear_spend_past12m,
       kidswear_visits_past12m,
       trenery_items_past12m,
       trenery_spend_past12m,
       trenery_visits_past12m,
       menswear_items_past12m,
       menswear_spend_past12m,
       menswear_visits_past12m,
       womens_foot_acc_items_past12m,
       womens_foot_acc_spend_past12m,
       womens_foot_acc_visits_past12m,
       country_road_items_past12m,
       country_road_spend_past12m,
       country_road_visits_past12m,
       witchery_items_past12m,
       witchery_spend_past12m,
       witchery_visits_past12m,
       mimco_items_past12m,
       mimco_spend_past12m,
       mimco_visits_past12m,
       groceries_items_past12m,
       groceries_spend_past12m,
       groceries_visits_past12m,
       wine_bev_liquor_items_past12m,
       wine_bev_liquor_spend_past12m,
       wine_bev_liquor_visits_past12m,
       snackin_gifting_items_past12m,
       snackin_gifting_spend_past12m,
       snackin_gifting_visits_past12m,
       prepared_deli_items_past12m,
       prepared_deli_spend_past12m,
       prepared_deli_visits_past12m,
       bakery_items_past12m,
       bakery_spend_past12m,
       bakery_visits_past12m,
       produce_horti_items_past12m,
       produce_horti_spend_past12m,
       produce_horti_visits_past12m,
       protein_items_past12m,
       protein_spend_past12m,
       protein_visits_past12m,
       dairy_items_past12m,
       dairy_spend_past12m,
       dairy_visits_past12m,
       home_person_pet_items_past12m,
       home_person_pet_spend_past12m,
       home_person_pet_visits_past12m,
       classic_items_past12m,
       classic_spend_past12m,
       classic_visits_past12m,
       mod_contemp_items_past12m,
       mod_contemp_spend_past12m,
       mod_contemp_visits_past12m,
       beauty_brand_items_past12m,
       beauty_brand_spend_past12m,
       beauty_brand_visits_past12m,
       beauty_ww_label_items_past12m,
       beauty_ww_label_spend_past12m,
       beauty_ww_label_visits_past12m,
       foods_items_past12m,
       foods_spend_past12m,
       foods_visits_past12m,
       clothing_items_past12m,
       clothing_spend_past12m,
       clothing_visits_past12m,
       homeware_items_past12m,
       homeware_spend_past12m,
       homeware_visits_past12m,
       digital_items_past12m,
       digital_spend_past12m,
       digital_visits_past12m,
       beauty_items_past12m,
       beauty_spend_past12m,
       beauty_visits_past12m,
       country_rd_grp_items_past12m,
       country_rd_grp_spend_past12m,
       country_rd_grp_visits_past12m,
       premium_brands_customer,
       classic_customer,
       timeless_customer,
       modern_customer,
       foods_preferred_store,
       non_foods_preferred_store,
       research_panel_ind, 
       sf.segment_desc              ly_non_foods_value_seg_desc, 
       sg.segment_desc              ly_food_value_seg_desc,       
       sh.segment_desc              ty_summer_non_fds_val_seg_desc,  
       si.segment_desc              ly_summer_non_fds_val_seg_desc, 
       sj.segment_desc              ty_winter_non_fds_val_seg_desc,  
       sk.segment_desc              ly_winter_non_fds_val_seg_desc,
       womens_edition_items_past12m,
       womens_edition_spend_past12m,
       womens_edition_visits_past12m,
       womens_dj_items_past12m,
       womens_dj_spend_past12m,
       womens_dj_visits_past12m,
       womens_saf_items_past12m,
       womens_saf_spend_past12m,
       womens_saf_visits_past12m,
       mens_edition_items_past12m,
       mens_edition_spend_past12m,
       mens_edition_visits_past12m,
       mens_dj_items_past12m,
       mens_dj_spend_past12m,
       mens_dj_visits_past12m,
       mens_saf_items_past12m,
       mens_saf_spend_past12m,
       mens_saf_visits_past12m,
       mens_supersport_items_past12m,
       mens_supersport_spend_past12m,
       mens_supersport_visits_past12m,
       kids_and_us_items_past12m,
       kids_and_us_spend_past12m,
       kids_and_us_visits_past12m,
       kids_younger_items_past12m,
       kids_younger_spend_past12m,
       kids_younger_visits_past12m,
       kids_babywear_items_past12m,
       kids_babywear_spend_past12m,
       kids_babywear_visits_past12m,
       foodservices_items_past12m,
       foodservices_spend_past12m,
       foodservices_visits_past12m,
       wine_liquor_items_past12m,
       wine_liquor_spend_past12m,
       wine_liquor_visits_past12m,
       beverage_items_past12m,
       beverage_spend_past12m,
       beverage_visits_past12m,
       foods_baby_items_past12m,
       foods_baby_spend_past12m,
       foods_baby_visits_past12m,
       foods_online_proportion,
       non_foods_online_proportion,
       months_since_last_foods_pur,
       months_since_last_cgm_purchase,
       months_since_last_crg_purchase,
       foods_weekday_shopper_type,
       non_foods_weekday_shopper_type,
       dm_customer_type,
       customer_location,
       create_date
  from (
                select subscriber_key,
                        non_food_life_seg_code,
                        nfshv_current_seg,            
                        fshv_current_seg,             
                        shopping_habit_segment_no,
                        total_items_past12m,
                        total_spend_past12m,
                        total_visits_past12m,
                        womens_studio_w_items_past12m,
                        womens_studio_w_spend_past12m,
                        womens_studio_w_visits_past12m,
                        womens_re_items_past12m,
                        womens_re_spend_past12m,
                        womens_re_visits_past12m,
                        mens_re_items_past12m,
                        mens_re_spend_past12m,
                        mens_re_visits_past12m,
                        mens_studio_w_items_past12m,
                        mens_studio_w_spend_past12m,
                        mens_studio_w_visits_past12m,
                        womens_outerw_items_past12m,
                        womens_outerw_spend_past12m,
                        womens_outerw_visits_past12m,
                        womens_lingerie_items_past12m,
                        womens_lingerie_spend_past12m,
                        womens_lingerie_visits_past12m,
                        kidswear_items_past12m,
                        kidswear_spend_past12m,
                        kidswear_visits_past12m,
                        trenery_items_past12m,
                        trenery_spend_past12m,
                        trenery_visits_past12m,
                        menswear_items_past12m,
                        menswear_spend_past12m,
                        menswear_visits_past12m,
                        womens_foot_acc_items_past12m,
                        womens_foot_acc_spend_past12m,
                        womens_foot_acc_visits_past12m,
                        country_road_items_past12m,
                        country_road_spend_past12m,
                        country_road_visits_past12m,
                        witchery_items_past12m,
                        witchery_spend_past12m,
                        witchery_visits_past12m,
                        mimco_items_past12m,
                        mimco_spend_past12m,
                        mimco_visits_past12m,
                        groceries_items_past12m,
                        groceries_spend_past12m,
                        groceries_visits_past12m,
                        wine_bev_liquor_items_past12m,
                        wine_bev_liquor_spend_past12m,
                        wine_bev_liquor_visits_past12m,
                        snackin_gifting_items_past12m,
                        snackin_gifting_spend_past12m,
                        snackin_gifting_visits_past12m,
                        prepared_deli_items_past12m,
                        prepared_deli_spend_past12m,
                        prepared_deli_visits_past12m,
                        bakery_items_past12m,
                        bakery_spend_past12m,
                        bakery_visits_past12m,
                        produce_horti_items_past12m,
                        produce_horti_spend_past12m,
                        produce_horti_visits_past12m,
                        protein_items_past12m,
                        protein_spend_past12m,
                        protein_visits_past12m,
                        dairy_items_past12m,
                        dairy_spend_past12m,
                        dairy_visits_past12m,
                        home_person_pet_items_past12m,
                        home_person_pet_spend_past12m,
                        home_person_pet_visits_past12m,
                        classic_items_past12m,
                        classic_spend_past12m,
                        classic_visits_past12m,
                        mod_contemp_items_past12m,
                        mod_contemp_spend_past12m,
                        mod_contemp_visits_past12m,
                        beauty_brand_items_past12m,
                        beauty_brand_spend_past12m,
                        beauty_brand_visits_past12m,
                        beauty_ww_label_items_past12m,
                        beauty_ww_label_spend_past12m,
                        beauty_ww_label_visits_past12m,
                        foods_items_past12m,
                        foods_spend_past12m,
                        foods_visits_past12m,
                        clothing_items_past12m,
                        clothing_spend_past12m,
                        clothing_visits_past12m,
                        homeware_items_past12m,
                        homeware_spend_past12m,
                        homeware_visits_past12m,
                        digital_items_past12m,
                        digital_spend_past12m,
                        digital_visits_past12m,
                        beauty_items_past12m,
                        beauty_spend_past12m,
                        beauty_visits_past12m,
                        country_rd_grp_items_past12m,
                        country_rd_grp_spend_past12m,
                        country_rd_grp_visits_past12m,
                        premium_brands_customer,
                        classic_customer,
                        timeless_customer,
                        modern_customer,
                        foods_preferred_store,
                        non_foods_preferred_store,
                        research_panel_ind,
                        ly_foods_value_seg,           
                        ly_non_foods_value_seg,       
                        ty_summer_non_foods_val_seg,  
                        ly_summer_non_foods_val_seg,  
                        ty_winter_non_foods_val_seg,  
                        ly_winter_non_foods_val_seg,
                        womens_edition_items_past12m,
                        womens_edition_spend_past12m,
                        womens_edition_visits_past12m,
                        womens_dj_items_past12m,
                        womens_dj_spend_past12m,
                        womens_dj_visits_past12m,
                        womens_saf_items_past12m,
                        womens_saf_spend_past12m,
                        womens_saf_visits_past12m,
                        mens_edition_items_past12m,
                        mens_edition_spend_past12m,
                        mens_edition_visits_past12m,
                        mens_dj_items_past12m,
                        mens_dj_spend_past12m,
                        mens_dj_visits_past12m,
                        mens_saf_items_past12m,
                        mens_saf_spend_past12m,
                        mens_saf_visits_past12m,
                        mens_supersport_items_past12m,
                        mens_supersport_spend_past12m,
                        mens_supersport_visits_past12m,
                        kids_and_us_items_past12m,
                        kids_and_us_spend_past12m,
                        kids_and_us_visits_past12m,
                        kids_younger_items_past12m,
                        kids_younger_spend_past12m,
                        kids_younger_visits_past12m,
                        kids_babywear_items_past12m,
                        kids_babywear_spend_past12m,
                        kids_babywear_visits_past12m,
                        foodservices_items_past12m,
                        foodservices_spend_past12m,
                        foodservices_visits_past12m,
                        wine_liquor_items_past12m,
                        wine_liquor_spend_past12m,
                        wine_liquor_visits_past12m,
                        beverage_items_past12m,
                        beverage_spend_past12m,
                        beverage_visits_past12m,
                        foods_baby_items_past12m,
                        foods_baby_spend_past12m,
                        foods_baby_visits_past12m,
                        foods_online_proportion,
                        non_foods_online_proportion,
                        months_since_last_foods_pur,
                        months_since_last_cgm_purchase,
                        months_since_last_crg_purchase,
                        foods_weekday_shopper_type,
                        non_foods_weekday_shopper_type,
                        dm_customer_type,
                        customer_location,
                        create_date,
                        row_number() over(partition by subscriber_key order by subscriber_key) seq_no
                      from (
                             with non_fd as ( select * from (
                                                ( select subscriber_key,
                                                         case when sum(nine) is not null then sum(nine)
                                                              when sum(four) is not null then sum(four)
                                                              when sum(three) is not null then sum(three)
                                                              when sum(two) is not null then sum(two)
                                                              when sum(eight) is not null then sum(eight)
                                                              when sum(one) is not null then sum(one)
                                                              when sum(five) is not null then sum(five)
                                                              when sum(six) is not null then sum(six)
                                                              when sum(seven) is not null then sum(seven)
                                                         end non_food_life_seg_code
                                                    from (
                                                                select /*+ Parallel(ci,6) Full(ci) Parallel(sm,6) */
                                                                       subscriber_key,
                                                                       non_food_life_seg_code
                                                                  from dwh_cust_performance.out_dwh_cust_insights ci
                                                                  inner join dwh_cust_foundation.fnd_svoc_mapping sm on ci.primary_customer_identifier = sm.source_key
                                                                  where sm.source = 'C2'
                                                                    and ci.fin_year_no = g_year_no
                                                                    and ci.fin_month_no = g_month_no
                                                                  )
                                                                  PIVOT
                                                                    (
                                                                     max(non_food_life_seg_code)
                                                                     for non_food_life_seg_code in ( 9 nine,4 four,3 three,2 two,8 eight,1 one,5 five,6 six ,7 seven )
                                                                    )
                                                  group by subscriber_key)
                                                 ))
                                                select /*+ Parallel(ci,6) Full(ci) Parallel(sm,6) */ distinct
                                                       sm.subscriber_key,
                                                       ps.non_food_life_seg_code,
                                                       min(nfshv_current_seg)                nfshv_current_seg,            
                                                       min(fshv_current_seg)                 fshv_current_seg,         
                                                       min(shopping_habit_segment_no)        shopping_habit_segment_no ,   
                                                       min(ly_foods_value_seg)               ly_foods_value_seg,           
                                                       min(ly_non_foods_value_seg)           ly_non_foods_value_seg,    
                                                       min(ty_summer_non_foods_val_seg)      ty_summer_non_foods_val_seg,  
                                                       min(ly_summer_non_foods_val_seg)      ly_summer_non_foods_val_seg,  
                                                       min(ty_winter_non_foods_val_seg)      ty_winter_non_foods_val_seg,  
                                                       min(ly_winter_non_foods_val_seg)      ly_winter_non_foods_val_seg,
                                                       sum(ci.total_items_past12m)            total_items_past12m,
                                                       sum(ci.total_spend_past12m)            total_spend_past12m,
                                                       sum(ci.total_visits_past12m)           total_visits_past12m,
                                                       sum(ci.womens_studio_w_items_past12m)  womens_studio_w_items_past12m,
                                                       sum(ci.womens_studio_w_spend_past12m)  womens_studio_w_spend_past12m,
                                                       sum(ci.womens_studio_w_visits_past12m) womens_studio_w_visits_past12m,
                                                       sum(ci.womens_re_items_past12m)        womens_re_items_past12m,
                                                       sum(ci.womens_re_spend_past12m)        womens_re_spend_past12m,
                                                       sum(ci.womens_re_visits_past12m)       womens_re_visits_past12m,
                                                       sum(ci.mens_re_items_past12m)          mens_re_items_past12m,
                                                       sum(ci.mens_re_spend_past12m)          mens_re_spend_past12m,
                                                       sum(ci.mens_re_visits_past12m)         mens_re_visits_past12m,
                                                       sum(ci.mens_studio_w_items_past12m)    mens_studio_w_items_past12m,
                                                       sum(ci.mens_studio_w_spend_past12m)    mens_studio_w_spend_past12m,
                                                       sum(ci.mens_studio_w_visits_past12m)   mens_studio_w_visits_past12m,
                                                       sum(ci.womens_outerw_items_past12m)    womens_outerw_items_past12m,
                                                       sum(ci.womens_outerw_spend_past12m)    womens_outerw_spend_past12m,
                                                       sum(ci.womens_outerw_visits_past12m)   womens_outerw_visits_past12m,
                                                       sum(ci.womens_lingerie_items_past12m)  womens_lingerie_items_past12m,
                                                       sum(ci.womens_lingerie_spend_past12m)  womens_lingerie_spend_past12m,
                                                       sum(ci.womens_lingerie_visits_past12m) womens_lingerie_visits_past12m,
                                                       sum(ci.kidswear_items_past12m)         kidswear_items_past12m,
                                                       sum(ci.kidswear_spend_past12m)         kidswear_spend_past12m,
                                                       sum(ci.kidswear_visits_past12m)        kidswear_visits_past12m,
                                                       sum(ci.trenery_items_past12m)          trenery_items_past12m,
                                                       sum(ci.trenery_spend_past12m)          trenery_spend_past12m,
                                                       sum(ci.trenery_visits_past12m)         trenery_visits_past12m,
                                                       sum(ci.menswear_items_past12m)         menswear_items_past12m,
                                                       sum(ci.menswear_spend_past12m)         menswear_spend_past12m,
                                                       sum(ci.menswear_visits_past12m)        menswear_visits_past12m,
                                                       sum(ci.womens_foot_acc_items_past12m)  womens_foot_acc_items_past12m,
                                                       sum(ci.womens_foot_acc_spend_past12m)  womens_foot_acc_spend_past12m,
                                                       sum(ci.womens_foot_acc_visits_past12m) womens_foot_acc_visits_past12m,
                                                       sum(ci.country_road_items_past12m)     country_road_items_past12m,
                                                       sum(ci.country_road_spend_past12m)     country_road_spend_past12m,
                                                       sum(ci.country_road_visits_past12m)    country_road_visits_past12m,
                                                       sum(ci.witchery_items_past12m)         witchery_items_past12m,
                                                       sum(ci.witchery_spend_past12m)         witchery_spend_past12m,
                                                       sum(ci.witchery_visits_past12m)        witchery_visits_past12m,
                                                       sum(ci.mimco_items_past12m)            mimco_items_past12m,
                                                       sum(ci.mimco_spend_past12m)            mimco_spend_past12m,
                                                       sum(ci.mimco_visits_past12m)           mimco_visits_past12m,
                                                       sum(ci.groceries_items_past12m)        groceries_items_past12m,
                                                       sum(ci.groceries_spend_past12m)        groceries_spend_past12m,
                                                       sum(ci.groceries_visits_past12m)       groceries_visits_past12m,
                                                       sum(ci.wine_bev_liquor_items_past12m)  wine_bev_liquor_items_past12m,
                                                       sum(ci.wine_bev_liquor_spend_past12m)  wine_bev_liquor_spend_past12m,
                                                       sum(ci.wine_bev_liquor_visits_past12m) wine_bev_liquor_visits_past12m,
                                                       sum(ci.snackin_gifting_items_past12m)  snackin_gifting_items_past12m,
                                                       sum(ci.snackin_gifting_spend_past12m)  snackin_gifting_spend_past12m,
                                                       sum(ci.snackin_gifting_visits_past12m) snackin_gifting_visits_past12m,
                                                       sum(ci.prepared_deli_items_past12m)    prepared_deli_items_past12m,
                                                       sum(ci.prepared_deli_spend_past12m)    prepared_deli_spend_past12m,
                                                       sum(ci.prepared_deli_visits_past12m)   prepared_deli_visits_past12m,
                                                       sum(ci.bakery_items_past12m)           bakery_items_past12m,
                                                       sum(ci.bakery_spend_past12m)           bakery_spend_past12m,
                                                       sum(ci.bakery_visits_past12m)          bakery_visits_past12m,
                                                       sum(ci.produce_horti_items_past12m)    produce_horti_items_past12m,
                                                       sum(ci.produce_horti_spend_past12m)    produce_horti_spend_past12m,
                                                       sum(ci.produce_horti_visits_past12m)   produce_horti_visits_past12m,
                                                       sum(ci.protein_items_past12m)          protein_items_past12m,
                                                       sum(ci.protein_spend_past12m)          protein_spend_past12m,
                                                       sum(ci.protein_visits_past12m)         protein_visits_past12m,
                                                       sum(ci.dairy_items_past12m)            dairy_items_past12m,
                                                       sum(ci.dairy_spend_past12m)            dairy_spend_past12m,
                                                       sum(ci.dairy_visits_past12m)           dairy_visits_past12m,
                                                       sum(ci.home_person_pet_items_past12m)  home_person_pet_items_past12m,
                                                       sum(ci.home_person_pet_spend_past12m)  home_person_pet_spend_past12m,
                                                       sum(ci.home_person_pet_visits_past12m) home_person_pet_visits_past12m,
                                                       sum(ci.classic_items_past12m)          classic_items_past12m,
                                                       sum(ci.classic_spend_past12m)          classic_spend_past12m,
                                                       sum(ci.classic_visits_past12m)         classic_visits_past12m,
                                                       sum(ci.mod_contemp_items_past12m)      mod_contemp_items_past12m,
                                                       sum(ci.mod_contemp_spend_past12m)      mod_contemp_spend_past12m,
                                                       sum(ci.mod_contemp_visits_past12m)     mod_contemp_visits_past12m,
                                                       sum(beauty_brand_items_past12m)        beauty_brand_items_past12m,
                                                       sum(beauty_brand_spend_past12m)        beauty_brand_spend_past12m,
                                                       sum(beauty_brand_visits_past12m)       beauty_brand_visits_past12m,
                                                       sum(beauty_ww_label_items_past12m)     beauty_ww_label_items_past12m,
                                                       sum(beauty_ww_label_spend_past12m)     beauty_ww_label_spend_past12m,
                                                       sum(beauty_ww_label_visits_past12m)    beauty_ww_label_visits_past12m,
                                                       sum(ci.foods_items_past12m)            foods_items_past12m,
                                                       sum(ci.foods_spend_past12m)            foods_spend_past12m,
                                                       sum(ci.foods_visits_past12m)           foods_visits_past12m,
                                                       sum(ci.clothing_items_past12m)         clothing_items_past12m,
                                                       sum(ci.clothing_spend_past12m)         clothing_spend_past12m,
                                                       sum(ci.clothing_visits_past12m)        clothing_visits_past12m,
                                                       sum(ci.homeware_items_past12m)         homeware_items_past12m,
                                                       sum(ci.homeware_spend_past12m)         homeware_spend_past12m,
                                                       sum(ci.homeware_visits_past12m)        homeware_visits_past12m,
                                                       sum(ci.digital_items_past12m)          digital_items_past12m,
                                                       sum(ci.digital_spend_past12m)          digital_spend_past12m,
                                                       sum(ci.digital_visits_past12m)         digital_visits_past12m,
                                                       sum(ci.beauty_items_past12m)           beauty_items_past12m,
                                                       sum(ci.beauty_spend_past12m)           beauty_spend_past12m,
                                                       sum(ci.beauty_visits_past12m)          beauty_visits_past12m,
                                                       sum(ci.country_rd_grp_items_past12m)   country_rd_grp_items_past12m,
                                                       sum(ci.country_rd_grp_spend_past12m)   country_rd_grp_spend_past12m,
                                                       sum(ci.country_rd_grp_visits_past12m)  country_rd_grp_visits_past12m,
                                                       min(ci.premium_brands_customer)        premium_brands_customer,
                                                       min(ci.classic_customer)               classic_customer,
                                                       min(ci.timeless_customer)              timeless_customer,
                                                       min(ci.modern_customer)                modern_customer,
                                                       case when count(ci.foods_preferred_store) = 1 then max(ci.foods_preferred_store) end     foods_preferred_store,
                                                       case when count(ci.non_foods_preferred_store) = 1 then max(ci.non_foods_preferred_store) end non_foods_preferred_store,
                                                       min(ci.research_panel_ind)             research_panel_ind,
                                                       sum(ci.womens_edition_items_past12m)   womens_edition_items_past12m,
                                                       sum(ci.womens_edition_spend_past12m)   womens_edition_spend_past12m,
                                                       sum(ci.womens_edition_visits_past12m)  womens_edition_visits_past12m,
                                                       sum(ci.womens_dj_items_past12m)        womens_dj_items_past12m,
                                                       sum(ci.womens_dj_spend_past12m)        womens_dj_spend_past12m,
                                                       sum(ci.womens_dj_visits_past12m)       womens_dj_visits_past12m,
                                                       sum(ci.womens_saf_items_past12m)       womens_saf_items_past12m,
                                                       sum(ci.womens_saf_spend_past12m)       womens_saf_spend_past12m,
                                                       sum(ci.womens_saf_visits_past12m)      womens_saf_visits_past12m,
                                                       sum(ci.mens_edition_items_past12m)     mens_edition_items_past12m,
                                                       sum(ci.mens_edition_spend_past12m)     mens_edition_spend_past12m,
                                                       sum(ci.mens_edition_visits_past12m)    mens_edition_visits_past12m,
                                                       sum(ci.mens_dj_items_past12m)          mens_dj_items_past12m,
                                                       sum(ci.mens_dj_spend_past12m)          mens_dj_spend_past12m,
                                                       sum(ci.mens_dj_visits_past12m)         mens_dj_visits_past12m,
                                                       sum(ci.mens_saf_items_past12m)         mens_saf_items_past12m,
                                                       sum(ci.mens_saf_spend_past12m)         mens_saf_spend_past12m,
                                                       sum(ci.mens_saf_visits_past12m)        mens_saf_visits_past12m,
                                                       sum(ci.mens_supersport_items_past12m)  mens_supersport_items_past12m,
                                                       sum(ci.mens_supersport_spend_past12m)  mens_supersport_spend_past12m,
                                                       sum(ci.mens_supersport_visits_past12m) mens_supersport_visits_past12m,
                                                       sum(ci.kids_and_us_items_past12m)      kids_and_us_items_past12m,
                                                       sum(ci.kids_and_us_spend_past12m)      kids_and_us_spend_past12m,
                                                       sum(ci.kids_and_us_visits_past12m)     kids_and_us_visits_past12m,
                                                       sum(ci.kids_younger_items_past12m)     kids_younger_items_past12m,
                                                       sum(ci.kids_younger_spend_past12m)     kids_younger_spend_past12m,
                                                       sum(ci.kids_younger_visits_past12m)    kids_younger_visits_past12m,
                                                       sum(ci.kids_babywear_items_past12m)    kids_babywear_items_past12m,
                                                       sum(ci.kids_babywear_spend_past12m)    kids_babywear_spend_past12m,
                                                       sum(ci.kids_babywear_visits_past12m)   kids_babywear_visits_past12m,
                                                       sum(ci.foodservices_items_past12m)     foodservices_items_past12m,
                                                       sum(ci.foodservices_spend_past12m)     foodservices_spend_past12m,
                                                       sum(ci.foodservices_visits_past12m)    foodservices_visits_past12m,
                                                       sum(ci.wine_liquor_items_past12m)      wine_liquor_items_past12m,
                                                       sum(ci.wine_liquor_spend_past12m)      wine_liquor_spend_past12m,
                                                       sum(ci.wine_liquor_visits_past12m)     wine_liquor_visits_past12m,
                                                       sum(ci.beverage_items_past12m)         beverage_items_past12m,
                                                       sum(ci.beverage_spend_past12m)         beverage_spend_past12m,
                                                       sum(ci.beverage_visits_past12m)        beverage_visits_past12m,
                                                       sum(ci.foods_baby_items_past12m)       foods_baby_items_past12m,
                                                       sum(ci.foods_baby_spend_past12m)       foods_baby_spend_past12m,
                                                       sum(ci.foods_baby_visits_past12m)      foods_baby_visits_past12m,
                                                       sum(ci.foods_online_proportion)        foods_online_proportion,
                                                       sum(ci.non_foods_online_proportion)    non_foods_online_proportion,
                                                       sum(ci.months_since_last_foods_pur)    months_since_last_foods_pur,
                                                       sum(ci.months_since_last_cgm_purchase) months_since_last_cgm_purchase,
                                                       sum(ci.months_since_last_crg_purchase) months_since_last_crg_purchase,
                                                       min(ci.foods_weekday_shopper_type)     foods_weekday_shopper_type,
                                                       min(ci.non_foods_weekday_shopper_type) non_foods_weekday_shopper_type,
                                                       min(ci.dm_customer_type)               dm_customer_type,
                                                       min(ci.customer_location)              customer_location,
                                                       min(ci.create_date)                    create_date     
                                                  from dwh_cust_performance.out_dwh_cust_insights ci 
                                                 inner join dwh_cust_foundation.fnd_svoc_mapping sm on ci.primary_customer_identifier = sm.source_key
                                                 inner join non_fd ps on sm.subscriber_key = ps.subscriber_key
                                                 where sm.source = 'C2'
                                                   and ci.fin_year_no = g_year_no
                                                   and ci.fin_month_no = g_month_no
                                                 group by sm.subscriber_key,
                                                          ps.non_food_life_seg_code,
                                                          ps.non_food_life_seg_code)
       ) non full join
       (
       select subscriber_key,
                       case when three is not null then three
                            when one is not null then one
                            when seven is not null then seven
                            when four is not null then four
                            when two is not null then two
                            when six is not null then six
                            when eight is not null then eight
                        end food_life_seg_code
                      from (
                            select /*+ Parallel(ci,6) Full(ci) Parallel(sm,6) */ distinct
                                   subscriber_key,
                                   food_life_seg_code
                              from dwh_cust_performance.out_dwh_cust_insights ci 
                             inner join dwh_cust_foundation.fnd_svoc_mapping sm on ci.primary_customer_identifier = sm.source_key
                             where sm.source = 'C2'
                               and ci.fin_year_no = g_year_no
                               and ci.fin_month_no = g_month_no
                            )
                            PIVOT
                            (
                             max(food_life_seg_code)
                             for food_life_seg_code in ( 3 three,1 one,7 seven,4 four,2 two,6 six,8 eight )
                            )
       ) fds on non.subscriber_key = fds.subscriber_key
       left join dwh_cust_foundation.fnd_segment_mapping sa     on non.non_food_life_seg_code       = sa.segment_code and sa.segment_name = 'NON_FOOD_LIFE_SEG_CODE'
       left join dwh_cust_foundation.fnd_segment_mapping sb     on fds.food_life_seg_code           = sb.segment_code and sb.segment_name = 'FOOD_LIFE_SEG_CODE'
       left join dwh_cust_foundation.fnd_segment_mapping sc     on non.nfshv_current_seg            = sc.segment_code and sc.segment_name = 'NFSHV_CURRENT_SEG'
       left join dwh_cust_foundation.fnd_segment_mapping sd     on non.fshv_current_seg             = sd.segment_code and sd.segment_name = 'FSHV_CURRENT_SEG'
       left join dwh_cust_foundation.fnd_segment_mapping se     on non.shopping_habit_segment_no    = se.segment_code and se.segment_name = 'SHOPPING_HABIT_SEGMENT_NO'
       left join dwh_cust_foundation.fnd_segment_mapping sf     on non.ly_foods_value_seg           = sf.segment_code and sf.segment_name = 'LY_FOODS_VALUE_SEGMENT'
       left join dwh_cust_foundation.fnd_segment_mapping sg     on non.ly_non_foods_value_seg       = sg.segment_code and sg.segment_name = 'LY_NON_FOODS_VALUE_SEGMENT'
       left join dwh_cust_foundation.fnd_segment_mapping sh     on non.ty_summer_non_foods_val_seg  = sh.segment_code and sh.segment_name = 'TY_SUMMER_NON_FOODS_VAL_SEG'
       left join dwh_cust_foundation.fnd_segment_mapping si     on non.ly_summer_non_foods_val_seg  = si.segment_code and si.segment_name = 'LY_SUMMER_NON_FOODS_VAL_SEG'
       left join dwh_cust_foundation.fnd_segment_mapping sj     on non.ty_winter_non_foods_val_seg  = sj.segment_code and sj.segment_name = 'TY_WINTER_NON_FOODS_VAL_SEG'
       left join dwh_cust_foundation.fnd_segment_mapping sk     on non.ly_winter_non_foods_val_seg  = sk.segment_code and sk.segment_name = 'LY_WINTER_NON_FOODS_VAL_SEG'
       where seq_no = 1;
---
    commit;

    l_text := 'Populate OUT_DWH_CUST_INSIGHTS_SK. ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 insert /*+ Parallel(sk,6) */ into DWH_CUST_PERFORMANCE.OUT_DWH_CUST_INSIGHTS_SK sk
  select * from temp_cust_insights;

    g_recs_read     :=  g_recs_read + SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

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

end WH_PRF_CUST_383E;
