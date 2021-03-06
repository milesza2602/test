--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_380E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_380E" (p_forall_limit in integer,p_success out boolean,p_run_date in date)
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_380E';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE THE MONTHLY CUSTOMER INSIGHTS DATA';
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

    l_text := 'BUILD OF CUSTOMER INSIGHTS TO OUT_DWH_CUST_INSIGHTS STARTED AT '||
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
                               where calendar_date = to_date(trunc(sysdate)));

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



    l_text := 'Truncate OUT_DWH_CUST_INSIGHTS.' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table "DWH_CUST_PERFORMANCE"."OUT_DWH_CUST_INSIGHTS"';
    commit;

--    l_text := 'UPDATE STATS ON OUT_DWH_CUST_INSIGHTS TABLES';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    DBMS_STATS.gather_table_stats ('DWH_CUST_PERFORMANCE','OUT_DWH_CUST_INSIGHTS',estimate_percent=>1, DEGREE => 32);

    commit;

    l_text := 'Populate OUT_DWH_CUST_INSIGHTS. ' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    insert /*+ parallel(OUT_DWH_CUST_INSIGHTS,6) */ into DWH_CUST_PERFORMANCE.OUT_DWH_CUST_INSIGHTS
    select *
      from (select /*+ Parallel(a,6) Parallel(b,6) Parallel(c,6) Parallel(d,6) Parallel(e,6) Parallel(f,6) Parallel(g,6) Parallel(h,6) Parallel(i,6) Parallel(j,6) Full(a) Full(b) Full(c) Full(d) Full(e) Full(f) Full(g) Full(h) Full(i) Full(j) */
                   distinct
                   c.fin_year_no,
                   c.fin_month_no,
                   c.primary_customer_identifier,
                   c.customer_no ,
                   c.ww_card,
                   c.ms_card,
                   c.alien_card,
                   c.gender_code,
                   c.age_acc_holder,
                   c.work_phone_country_code,
                   c.work_phone_area_code,
                   c.work_phone_no,
                   c.work_phone_extension_no,
                   c.work_cell_country_code,
                   c.work_cell_area_code,
                   c.work_cell_no,
                   c.home_phone_country_code,
                   c.home_phone_area_code,
                   c.home_phone_no,
                   c.home_phone_extension_no,
                   c.home_cell_country_code,
                   c.home_cell_area_code,
                   c.home_cell_no,
                   c.title_code,
                   c.first_name,
                   c.first_middle_name_initial,
                   c.last_name,
                   c.estatement_email,
                   c.home_email_address,
                   c.work_email_address,
                   a.non_food_life_seg_code         non_food_life_seg_code,
                   a.food_life_seg_code             food_life_seg_code,
                   b.nfshv_current_seg              nfshv_current_seg,
                   b.fshv_current_seg               fshv_current_seg,
                   d.csm_shopping_habit_segment_no  shopping_habit_segment_no,
                   nvl(i.total_items_past12m,0)            total_items_past12m,
                   nvl(i.total_spend_past12m,0)            total_spend_past12m,
                   nvl(i.total_visits_past12m,0)           total_visits_past12m,
                   nvl(f.womens_studio_w_items_past12m,0)  womens_studio_w_items_past12m,
                   nvl(f.womens_studio_w_spend_past12m,0)  womens_studio_w_spend_past12m,
                   nvl(f.womens_studio_w_visits_past12m,0) womens_studio_w_visits_past12m,
                   nvl(f.womens_re_items_past12m,0)        womens_re_items_past12m,
                   nvl(f.womens_re_spend_past12m,0)        womens_re_spend_past12m,
                   nvl(f.womens_re_visits_past12m,0)       womens_re_visits_past12m,
                   nvl(f.mens_re_items_past12m,0)          mens_re_items_past12m,
                   nvl(f.mens_re_spend_past12m,0)          mens_re_spend_past12m,
                   nvl(f.mens_re_visits_past12m,0)         mens_re_visits_past12m,
                   nvl(f.mens_studio_w_items_past12m,0)    mens_studio_w_items_past12m,
                   nvl(f.mens_studio_w_spend_past12m,0)    mens_studio_w_spend_past12m,
                   nvl(f.mens_studio_w_visits_past12m,0)   mens_studio_w_visits_past12m,
                   nvl(g.womens_outerw_items_past12m,0)    womens_outerw_items_past12m,
                   nvl(g.womens_outerw_spend_past12m,0)    womens_outerw_spend_past12m,
                   nvl(g.womens_outerw_visits_past12m,0)   womens_outerw_visits_past12m,
                   nvl(g.womens_lingerie_items_past12m,0)  womens_lingerie_items_past12m,
                   nvl(g.womens_lingerie_spend_past12m,0)  womens_lingerie_spend_past12m,
                   nvl(g.womens_lingerie_visits_past12m,0) womens_lingerie_visits_past12m,
                   nvl(g.kidswear_items_past12m,0)         kidswear_items_past12m,
                   nvl(g.kidswear_spend_past12m,0)         kidswear_spend_past12m,
                   nvl(g.kidswear_visits_past12m,0)        kidswear_visits_past12m,
                   nvl(g.trenery_items_past12m,0)          trenery_items_past12m,
                   nvl(g.trenery_spend_past12m,0)          trenery_spend_past12m,
                   nvl(g.trenery_visits_past12m,0)         trenery_visits_past12m,
                   nvl(g.menswear_items_past12m,0)         menswear_items_past12m,
                   nvl(g.menswear_spend_past12m,0)         menswear_spend_past12m,
                   nvl(g.menswear_visits_past12m,0)        menswear_visits_past12m,
                   nvl(g.womens_foot_acc_items_past12m,0)  womens_foot_acc_items_past12m,
                   nvl(g.womens_foot_acc_spend_past12m,0)  womens_foot_acc_spend_past12m,
                   nvl(g.womens_foot_acc_visits_past12m,0) womens_foot_acc_visits_past12m,
                   nvl(g.country_road_items_past12m,0)     country_road_items_past12m,
                   nvl(g.country_road_spend_past12m,0)     country_road_spend_past12m,
                   nvl(g.country_road_visits_past12m,0)    country_road_visits_past12m,
                   nvl(g.witchery_items_past12m,0)         witchery_items_past12m,
                   nvl(g.witchery_spend_past12m,0)         witchery_spend_past12m,
                   nvl(g.witchery_visits_past12m,0)        witchery_visits_past12m,
                   nvl(g.mimco_items_past12m,0)            mimco_items_past12m,
                   nvl(g.mimco_spend_past12m,0)            mimco_spend_past12m,
                   nvl(g.mimco_visits_past12m,0)           mimco_visits_past12m,
                   nvl(g.groceries_items_past12m,0)        groceries_items_past12m,
                   nvl(g.groceries_spend_past12m,0)        groceries_spend_past12m,
                   nvl(g.groceries_visits_past12m,0)       groceries_visits_past12m,
                   nvl(g.wine_bev_liquor_items_past12m,0)  wine_bev_liquor_items_past12m,
                   nvl(g.wine_bev_liquor_spend_past12m,0)  wine_bev_liquor_spend_past12m,
                   nvl(g.wine_bev_liquor_visits_past12m,0) wine_bev_liquor_visits_past12m,
                   nvl(g.snackin_gifting_items_past12m,0)  snackin_gifting_items_past12m,
                   nvl(g.snackin_gifting_spend_past12m,0)  snackin_gifting_spend_past12m,
                   nvl(g.snackin_gifting_visits_past12m,0) snackin_gifting_visits_past12m,
                   nvl(g.prepared_deli_items_past12m,0)    prepared_deli_items_past12m,
                   nvl(g.prepared_deli_spend_past12m,0)    prepared_deli_spend_past12m,
                   nvl(g.prepared_deli_visits_past12m,0)   prepared_deli_visits_past12m,
                   nvl(g.bakery_items_past12m,0)           bakery_items_past12m,
                   nvl(g.bakery_spend_past12m,0)           bakery_spend_past12m,
                   nvl(g.bakery_visits_past12m,0)          bakery_visits_past12m,
                   nvl(g.produce_horti_items_past12m,0)    produce_horti_items_past12m,
                   nvl(g.produce_horti_spend_past12m,0)    produce_horti_spend_past12m,
                   nvl(g.produce_horti_visits_past12m,0)   produce_horti_visits_past12m,
                   nvl(g.protein_items_past12m,0)          protein_items_past12m,
                   nvl(g.protein_spend_past12m,0)          protein_spend_past12m,
                   nvl(g.protein_visits_past12m,0)         protein_visits_past12m,
                   nvl(g.dairy_items_past12m,0)            dairy_items_past12m,
                   nvl(g.dairy_spend_past12m,0)            dairy_spend_past12m,
                   nvl(g.dairy_visits_past12m,0)           dairy_visits_past12m,
                   nvl(g.home_person_pet_items_past12m,0)  home_person_pet_items_past12m,
                   nvl(g.home_person_pet_spend_past12m,0)  home_person_pet_spend_past12m,
                   nvl(g.home_person_pet_visits_past12m,0) home_person_pet_visits_past12m,
                   nvl(h.classic_items_past12m,0)          classic_items_past12m,
                   nvl(h.classic_spend_past12m,0)          classic_spend_past12m,
                   nvl(h.classic_visits_past12m,0)         classic_visits_past12m,
                   nvl(h.MOD_CONTEMP_ITEMS_PAST12M,0)      MOD_CONTEMP_ITEMS_PAST12M,
                   nvl(h.MOD_CONTEMP_SPEND_PAST12M,0)      MOD_CONTEMP_SPEND_PAST12M,
                   nvl(h.MOD_CONTEMP_VISITS_PAST12M,0)     MOD_CONTEMP_VISITS_PAST12M,
                   nvl(h.beauty_brand_items_past12m,0)     beauty_brand_items_past12m,
                   nvl(h.beauty_brand_spend_past12m,0)     beauty_brand_spend_past12m,
                   nvl(h.beauty_brand_visits_past12m,0)    beauty_brand_visits_past12m,
                   nvl(h.beauty_ww_label_items_past12m,0)  beauty_ww_label_items_past12m,
                   nvl(h.beauty_ww_label_spend_past12m,0)  beauty_ww_label_spend_past12m,
                   nvl(h.beauty_ww_label_visits_past12m,0) beauty_ww_label_visits_past12m,
                   nvl(h.kids_re_outerw_items_past12m,0)   kids_re_outerw_items_past12m,
                   nvl(h.kids_re_outerw_spend_past12m,0)   kids_re_outerw_spend_past12m,
                   nvl(h.kids_re_outerw_visits_past12m,0)  kids_re_outerw_visits_past12m,
                   nvl(i.foods_items_past12m,0)            foods_items_past12m,
                   nvl(i.foods_spend_past12m,0)            foods_spend_past12m,
                   nvl(i.foods_visits_past12m,0)           foods_visits_past12m,
                   nvl(i.clothing_items_past12m,0)         clothing_items_past12m,
                   nvl(i.clothing_spend_past12m,0)         clothing_spend_past12m,
                   nvl(i.clothing_visits_past12m,0)        clothing_visits_past12m,
                   nvl(i.homeware_items_past12m,0)         homeware_items_past12m,
                   nvl(i.homeware_spend_past12m,0)         homeware_spend_past12m,
                   nvl(i.homeware_visits_past12m,0)        homeware_visits_past12m,
                   nvl(i.digital_items_past12m,0)          digital_items_past12m,
                   nvl(i.digital_spend_past12m,0)          digital_spend_past12m,
                   nvl(i.digital_visits_past12m,0)         digital_visits_past12m,
                   nvl(i.beauty_items_past12m,0)           beauty_items_past12m,
                   nvl(i.beauty_spend_past12m,0)           beauty_spend_past12m,
                   nvl(i.beauty_visits_past12m,0)          beauty_visits_past12m,
                   nvl(i.country_rd_grp_items_past12m,0)   country_rd_grp_items_past12m,
                   nvl(i.country_rd_grp_spend_past12m,0)   country_rd_grp_spend_past12m,
                   nvl(i.country_rd_grp_visits_past12m,0)  country_rd_grp_visits_past12m,
                   case when (trenery_visits_past12m > 1 or country_road_visits_past12m > 1 or witchery_visits_past12m > 0 or mimco_visits_past12m > 0) then 'Y'
                        else 'N'
                   end premium_brands_customer,
                   case when (trenery_visits_past12m > 1 or country_road_visits_past12m > 1 or witchery_visits_past12m > 0 or mimco_visits_past12m > 0) then 'N'
                        when ((classic_visits_past12M > 2 * MOD_CONTEMP_VISITS_PAST12M and classic_visits_past12M > 2) or (age_acc_holder >= 60 and gender_code='F' and classic_visits_past12M > 0)) and gender_code='F' then 'Y'
                        else 'N'
                   end classic_customer,
                   case when (trenery_visits_past12m > 1 or country_road_visits_past12m > 1 or witchery_visits_past12m > 0 or mimco_visits_past12m > 0) then 'N'
                        when ((classic_visits_past12M > 2 * MOD_CONTEMP_VISITS_PAST12M and classic_visits_past12M > 2) or (age_acc_holder >= 60 and gender_code='F' and classic_visits_past12M > 0)) and gender_code='F' then 'N'
                        when classic_visits_past12M > 0 and MOD_CONTEMP_VISITS_PAST12M > 0 and
                             (womens_outerw_visits_past12m > 1 or womens_lingerie_visits_past12M > 1 or womens_foot_acc_visits_past12m > 1 or womens_re_visits_past12M > 1 or womens_studio_w_visits_past12M > 1) and
                             age_acc_holder > 40 and gender_code='F' then 'Y'
                        else 'N'
                   end timeless_customer,
                   case when (trenery_visits_past12m > 1 or country_road_visits_past12m > 1 or witchery_visits_past12m > 0 or mimco_visits_past12m > 0) then 'N'
                        when ((classic_visits_past12M > 2 * MOD_CONTEMP_VISITS_PAST12M and classic_visits_past12M > 2) or (age_acc_holder >= 60 and gender_code='F' and classic_visits_past12M > 0)) and gender_code='F' then 'N'
                        when classic_visits_past12M > 0 and MOD_CONTEMP_VISITS_PAST12M > 0 and
                             (womens_outerw_visits_past12m > 1 or womens_lingerie_visits_past12M > 1 or womens_foot_acc_visits_past12m > 1 or womens_re_visits_past12M > 1 or womens_studio_w_visits_past12M > 1) and
                             age_acc_holder > 40 and gender_code='F'  then 'N'
                        when (womens_outerw_visits_past12m > 1 or womens_lingerie_visits_past12M > 1 or womens_foot_acc_visits_past12m > 1 or womens_re_visits_past12M > 1 or womens_studio_w_visits_past12M > 1) and
                             age_acc_holder<40 and gender_code='F' then 'Y'
                        else 'N'
                   end modern_customer,
                   c.africa_country_code,
                   0 africa_mkt_permissions_ind,
                   j.foods_preferred_store,
                   j.non_foods_preferred_store,
                   0 research_panel_ind,
                   0 tran_wfs_sc_past12m,
                   0 tran_wfs_cc_past12m,
                   0 tran_visa_past12m,
                   0 tran_mastercard_past12m,
                   trunc(sysdate) create_date,
                   b.nfshv_last_year_seg            nfshv_last_year_seg,
                   b.fshv_last_year_seg            fshv_last_year_seg,
                   NULL TY_SUMMER_NON_FOODS_VAL_SEG,
                   NULL LY_SUMMER_NON_FOODS_VAL_SEG,
                   NULL TY_WINTER_NON_FOODS_VAL_SEG,
                   NULL LY_WINTER_NON_FOODS_VAL_SEG,
                   NVL(WOMENS_EDITION_ITEMS_PAST12M,0)       WOMENS_EDITION_ITEMS_PAST12M,
                   NVL(WOMENS_EDITION_SPEND_PAST12M,0)       WOMENS_EDITION_SPEND_PAST12M,
                   NVL(WOMENS_EDITION_VISITS_PAST12M,0)      WOMENS_EDITION_VISITS_PAST12M,
                   NVL(WOMENS_DJ_ITEMS_PAST12M,0)            WOMENS_DJ_ITEMS_PAST12M,
                   NVL(WOMENS_DJ_SPEND_PAST12M,0)            WOMENS_DJ_SPEND_PAST12M,
                   NVL(WOMENS_DJ_VISITS_PAST12M,0)           WOMENS_DJ_VISITS_PAST12M,
                   NVL(WOMENS_SAF_ITEMS_PAST12M,0)           WOMENS_SAF_ITEMS_PAST12M,
                   NVL(WOMENS_SAF_SPEND_PAST12M,0)           WOMENS_SAF_SPEND_PAST12M,
                   NVL(WOMENS_SAF_VISITS_PAST12M,0)          WOMENS_SAF_VISITS_PAST12M,
                   NVL(MENS_EDITION_ITEMS_PAST12M,0)         MENS_EDITION_ITEMS_PAST12M,
                   NVL(MENS_EDITION_SPEND_PAST12M,0)         MENS_EDITION_SPEND_PAST12M,
                   NVL(MENS_EDITION_VISITS_PAST12M,0)        MENS_EDITION_VISITS_PAST12M,
                   NVL(MENS_DJ_ITEMS_PAST12M,0)              MENS_DJ_ITEMS_PAST12M,
                   NVL(MENS_DJ_SPEND_PAST12M,0)              MENS_DJ_SPEND_PAST12M,
                   NVL(MENS_DJ_VISITS_PAST12M,0)             MENS_DJ_VISITS_PAST12M,
                   NVL(MENS_SAF_ITEMS_PAST12M,0)             MENS_SAF_ITEMS_PAST12M,
                   NVL(MENS_SAF_SPEND_PAST12M,0)             MENS_SAF_SPEND_PAST12M,
                   NVL(MENS_SAF_VISITS_PAST12M,0)            MENS_SAF_VISITS_PAST12M,
                   NVL(MENS_SUPERSPORT_ITEMS_PAST12M,0)      MENS_SUPERSPORT_ITEMS_PAST12M,
                   NVL(MENS_SUPERSPORT_SPEND_PAST12M,0)      MENS_SUPERSPORT_SPEND_PAST12M,
                   NVL(MENS_SUPERSPORT_VISITS_PAST12M,0)     MENS_SUPERSPORT_VISITS_PAST12M,
                   NVL(KIDS_AND_US_ITEMS_PAST12M,0)          KIDS_AND_US_ITEMS_PAST12M,
                   NVL(KIDS_AND_US_SPEND_PAST12M,0)          KIDS_AND_US_SPEND_PAST12M,
                   NVL(KIDS_AND_US_VISITS_PAST12M,0)         KIDS_AND_US_VISITS_PAST12M,
                   NVL(KIDS_YOUNGER_ITEMS_PAST12M,0)         KIDS_YOUNGER_ITEMS_PAST12M,
                   NVL(KIDS_YOUNGER_SPEND_PAST12M,0)         KIDS_YOUNGER_SPEND_PAST12M,
                   NVL(KIDS_YOUNGER_VISITS_PAST12M,0)        KIDS_YOUNGER_VISITS_PAST12M,
                   NVL(KIDS_BABYWEAR_ITEMS_PAST12M,0)        KIDS_BABYWEAR_ITEMS_PAST12M,
                   NVL(KIDS_BABYWEAR_SPEND_PAST12M,0)        KIDS_BABYWEAR_SPEND_PAST12M,
                   NVL(KIDS_BABYWEAR_VISITS_PAST12M,0)       KIDS_BABYWEAR_VISITS_PAST12M,
                   NVL(FOODSERVICES_ITEMS_PAST12M,0)         FOODSERVICES_ITEMS_PAST12M,
                   NVL(FOODSERVICES_SPEND_PAST12M,0)         FOODSERVICES_SPEND_PAST12M,
                   NVL(FOODSERVICES_VISITS_PAST12M,0)        FOODSERVICES_VISITS_PAST12M,
                   NVL(WINE_LIQUOR_ITEMS_PAST12M,0)          WINE_LIQUOR_ITEMS_PAST12M,
                   NVL(WINE_LIQUOR_SPEND_PAST12M,0)          WINE_LIQUOR_SPEND_PAST12M,
                   NVL(WINE_LIQUOR_VISITS_PAST12M,0)         WINE_LIQUOR_VISITS_PAST12M,
                   NVL(BEVERAGE_ITEMS_PAST12M,0)             BEVERAGE_ITEMS_PAST12M,
                   NVL(BEVERAGE_SPEND_PAST12M,0)             BEVERAGE_SPEND_PAST12M,
                   NVL(BEVERAGE_VISITS_PAST12M,0)            BEVERAGE_VISITS_PAST12M,
                   0 FOODS_BABY_ITEMS_PAST12M                ,
                   0 FOODS_BABY_SPEND_PAST12M                ,
                   0 FOODS_BABY_VISITS_PAST12M               ,
                   0 FOODS_ONLINE_PROPORTION                 ,
                   0 NON_FOODS_ONLINE_PROPORTION             ,
                   0 MONTHS_SINCE_LAST_FOODS_PUR             ,
                   0 MONTHS_SINCE_LAST_CGM_PURCHASE          ,
                   0 MONTHS_SINCE_LAST_CRG_PURCHASE          ,
                   NULL FOODS_WEEKDAY_SHOPPER_TYPE,
                   NULL NON_FOODS_WEEKDAY_SHOPPER_TYPE,
                   NULL DM_CUSTOMER_TYPE,
                   TO_CHAR('ZA') CUSTOMER_LOCATION
             from  ((select /*+ Parallel(pci,6) Parallel(dc,6) Parallel(dcc,6) FULL(pci) FULL(dc) FULL(dcc) */
                            g_year_no fin_year_no,
                            g_month_no fin_month_no,
                            case when dc.customer_no is not null then dc.customer_no
                                 else pci.primary_customer_identifier
                            end as primary_customer_identifier,
                            dc.customer_no,
                            case when dc.customer_no is not null then dcc.ww_card
                                 when pci.primary_customer_identifier between 6007850000000000 and 6007859999999999 then pci.primary_customer_identifier
                            end as ww_card,
                            case when dc.customer_no is not null then dcc.ms_card
                                 when pci.primary_customer_identifier between 5900000000000000 and 5999999999999999 then pci.primary_customer_identifier
                            end as ms_card,
                            case when dc.customer_no is not null then dcc.alien_card
                                 when pci.primary_customer_identifier not between 5900000000000000 and 5999999999999999  and
                                      pci.primary_customer_identifier not between 6007850000000000 and 6007859999999999 and
                                      pci.primary_customer_identifier != nvl(dc.customer_no,0) then pci.primary_customer_identifier
                            end as alien_card,
                            dc.gender_code,
                            floor(months_between(g_run_date,dc.birth_date)/12) age_acc_holder, -- on dim_customer this field is not update regularly as on fnd_customer
                            dc.work_phone_country_code,
                            dc.work_phone_area_code,
                            dc.work_phone_no,
                            dc.work_phone_extension_no,
                            dc.work_cell_country_code,
                            dc.work_cell_area_code,
                            dc.work_cell_no,
                            dc.home_phone_country_code,
                            dc.home_phone_area_code,
                            dc.home_phone_no,
                            dc.home_phone_extension_no,
                            dc.home_cell_country_code,
                            dc.home_cell_area_code,
                            dc.home_cell_no,
                            dc.title_code,
                            dc.first_name,
                            dc.first_middle_name_initial,
                            dc.last_name,
                            dc.estatement_email,
                            dc.home_email_address,
                            dc.work_email_address,
                            NVL(UPPER(dc.physical_country_code),'ZA') africa_country_code
                      from (select /*+ Parallel(a,6) FULL(a) */
                                   primary_customer_identifier
                              from dwh_cust_performance.cust_lss_lifestyle_segments a
                             where fin_year_no = g_year_no
                               and fin_month_no = g_month_no
                               --and segment_type in ('Foods','Non-Foods') --these are the only 2 values in the table
                            union
                            select /*+ Parallel(a,6) FULL(a) */
                                   primary_customer_identifier
                              from dwh_cust_performance.cust_csm_value_segment a
                             where fin_year_no = g_year_no
                               and fin_month_no = g_month_no
                            union
                            select /*+ Parallel(a,6) FULL(a) */
                                   csm_customer_identifier primary_customer_identifier
                              from dwh_cust_performance.cust_csm_shopping_habits a
                             where fin_year_no = g_year_no
                               and fin_month_no = g_month_no
                            union
                            select /*+ Parallel(a,6) FULL(a) */
                                   primary_customer_identifier
                              from dwh_cust_performance.cust_db_company_month a
                             where fin_year_no = g_year_no
                               and fin_month_no = g_month_no
                           ) pci
                     inner join dwh_cust_performance.dim_customer dc
                        on pci.primary_customer_identifier = dc.customer_no
                      left join (select /*+ Parallel(b,4) Full(b)  */ --get the max card no because a customer can have more than one card per card type
                                        customer_no,
                                        max(ww_card) ww_card,
                                        max(ms_card) ms_card,
                                        max(alien_card) alien_card
                                   from (select /*+ Parallel(a,4) Full(a)  */
                                                customer_no,
                                                case when a.card_no between 6007850000000000 and 6007859999999999 then a.card_no end as ww_card,
                                                case when a.card_no between 5900000000000000 and 5999999999999999 then a.card_no end as ms_card,
                                                case when a.card_no not between 5900000000000000 and 5999999999999999 and
                                                          a.card_no not between 6007850000000000 and 6007859999999999 and
                                                          a.card_no != nvl(a.customer_no,0) then a.card_no
                                                end as alien_card
                                           from dwh_cust_performance.dim_customer_card a
                                          where customer_no > 0) b
                                  group by customer_no) dcc
                        on dc.customer_no = dcc.customer_no) c
              left join (select /*+ Parallel(clls,6) Full(clls)*/
                                fin_year_no,
                                fin_month_no,
                                primary_customer_identifier,
                                max(non_food_life_seg_code) non_food_life_seg_code,
                                max(food_life_seg_code) food_life_seg_code
                           from (select /*+ Parallel (cls,6) Full(cls) */
                                        fin_year_no,
                                        fin_month_no,
                                        primary_customer_identifier,
                                        case when segment_type = 'Non-Foods' then segment_no end non_food_life_seg_code,
                                        case when segment_type = 'Foods' then segment_no end food_life_seg_code
                                   from dwh_cust_performance.cust_lss_lifestyle_segments cls
                                  where fin_year_no = g_year_no
                                    and fin_month_no = g_month_no)clls
                                    --and segment_type in ('Foods','Non-Foods'))clls --these are the only 2 values in the table
                          group by fin_year_no, fin_month_no, primary_customer_identifier) a
                   on c.primary_customer_identifier = a.primary_customer_identifier
              left join (select /*+ Parallel (csvs,6) Full(csvs)*/
                                fin_year_no,
                                fin_month_no,
                                primary_customer_identifier,
                                max(nfshv_current_seg) nfshv_current_seg,
                                max(fshv_current_seg) fshv_current_seg,
                                max(nfshv_last_year_seg) nfshv_last_year_seg,
                                max(fshv_last_year_seg) fshv_last_year_seg
                           from (select /*+ Parallel (ccv,6) Full(ccv) Parallel (csv,6) Full(csv)*/
                                        ccv.fin_year_no,
                                        ccv.fin_month_no,
                                        ccv.primary_customer_identifier,
                                        case when ccv.food_non_food = 'NFSHV' then ccv.current_seg end nfshv_current_seg,
                                        case when ccv.food_non_food = 'FSHV' then ccv.current_seg end fshv_current_seg,
                                        case when csv.food_non_food = 'NFSHV' then csv.current_seg end nfshv_last_year_seg,
                                        case when csv.food_non_food = 'FSHV' then csv.current_seg end fshv_last_year_seg
                                   from dwh_cust_performance.cust_csm_value_segment ccv
                                   left join dwh_cust_performance.cust_csm_value_segment csv on ccv.fin_year_no-1               = csv.fin_year_no and
                                                                                                       ccv.fin_month_no                = csv.fin_month_no and
                                                                                                       ccv.primary_customer_identifier = csv.primary_customer_identifier
                                  where ccv.fin_year_no = g_year_no
                                    and ccv.fin_month_no = g_month_no) csvs
                                    --and food_non_food in ('NFSHV','FSHV')) csvs --these are the only 2 values in the table
                          group by fin_year_no, fin_month_no, primary_customer_identifier) b
                   on c.fin_year_no  = b.fin_year_no and
                      c.fin_month_no = b.fin_month_no and
                      c.primary_customer_identifier  = b.primary_customer_identifier
              left join (select /*+ Parallel (css,6) Full(css) */
                               fin_year_no,
                               fin_month_no,
                               csm_customer_identifier,
                               csm_shopping_habit_segment_no ,
                               csm_preferred_store
                          from dwh_cust_performance.cust_csm_shopping_habits css
                         where fin_year_no = g_year_no
                           and fin_month_no = g_month_no) d
                   on c.fin_year_no  = d.fin_year_no and
                      c.fin_month_no = d.fin_month_no and
                      c.primary_customer_identifier  = d.csm_customer_identifier
              left join (select /*+ Parallel (cddm,6) Full(cddm)*/
                                fin_year_no,
                                fin_month_no,
                                primary_customer_identifier,
                                sum(womens_studio_w_items_past12m)  womens_studio_w_items_past12m,
                                sum(womens_studio_w_spend_past12m)  womens_studio_w_spend_past12m,
                                sum(womens_studio_w_visits_past12m) womens_studio_w_visits_past12m,
                                sum(womens_re_items_past12m)        womens_re_items_past12m,
                                sum(womens_re_spend_past12m)        womens_re_spend_past12m,
                                sum(womens_re_visits_past12m)       womens_re_visits_past12m,
                                sum(mens_re_items_past12m)          mens_re_items_past12m,
                                sum(mens_re_spend_past12m)          mens_re_spend_past12m,
                                sum(mens_re_visits_past12m)         mens_re_visits_past12m,
                                sum(mens_studio_w_items_past12m)    mens_studio_w_items_past12m,
                                sum(mens_studio_w_spend_past12m)    mens_studio_w_spend_past12m,
                                sum(mens_studio_w_visits_past12m)   mens_studio_w_visits_past12m,
                                sum(WOMENS_EDITION_ITEMS_PAST12M)   WOMENS_EDITION_ITEMS_PAST12M,
                                sum(WOMENS_EDITION_SPEND_PAST12M)   WOMENS_EDITION_SPEND_PAST12M,
                                sum(WOMENS_EDITION_VISITS_PAST12M)  WOMENS_EDITION_VISITS_PAST12M,
                                sum(WOMENS_SAF_ITEMS_PAST12M)       WOMENS_SAF_ITEMS_PAST12M,
                                sum(WOMENS_SAF_SPEND_PAST12M)       WOMENS_SAF_SPEND_PAST12M,
                                sum(WOMENS_SAF_VISITS_PAST12M)      WOMENS_SAF_VISITS_PAST12M,
                                sum(MENS_EDITION_ITEMS_PAST12M)     MENS_EDITION_ITEMS_PAST12M,
                                sum(MENS_EDITION_SPEND_PAST12M)     MENS_EDITION_SPEND_PAST12M,
                                sum(MENS_EDITION_VISITS_PAST12M)    MENS_EDITION_VISITS_PAST12M,
                                sum(MENS_DJ_ITEMS_PAST12M)          MENS_DJ_ITEMS_PAST12M,
                                sum(MENS_DJ_SPEND_PAST12M)          MENS_DJ_SPEND_PAST12M,
                                sum(MENS_DJ_VISITS_PAST12M)         MENS_DJ_VISITS_PAST12M,
                                sum(MENS_SAF_ITEMS_PAST12M)         MENS_SAF_ITEMS_PAST12M,
                                sum(MENS_SAF_SPEND_PAST12M)         MENS_SAF_SPEND_PAST12M,
                                sum(MENS_SAF_VISITS_PAST12M)        MENS_SAF_VISITS_PAST12M,
                                sum(MENS_SUPERSPORT_ITEMS_PAST12M)  MENS_SUPERSPORT_ITEMS_PAST12M,
                                sum(MENS_SUPERSPORT_SPEND_PAST12M)  MENS_SUPERSPORT_SPEND_PAST12M,
                                sum(MENS_SUPERSPORT_VISITS_PAST12M) MENS_SUPERSPORT_VISITS_PAST12M,
                                sum(KIDS_AND_US_ITEMS_PAST12M)      KIDS_AND_US_ITEMS_PAST12M,
                                sum(KIDS_AND_US_SPEND_PAST12M)      KIDS_AND_US_SPEND_PAST12M,
                                sum(KIDS_AND_US_VISITS_PAST12M)     KIDS_AND_US_VISITS_PAST12M,
                                sum(KIDS_YOUNGER_ITEMS_PAST12M)     KIDS_YOUNGER_ITEMS_PAST12M,
                                sum(KIDS_YOUNGER_SPEND_PAST12M)     KIDS_YOUNGER_SPEND_PAST12M,
                                sum(KIDS_YOUNGER_VISITS_PAST12M)    KIDS_YOUNGER_VISITS_PAST12M,
                                sum(KIDS_BABYWEAR_ITEMS_PAST12M)    KIDS_BABYWEAR_ITEMS_PAST12M,
                                sum(KIDS_BABYWEAR_SPEND_PAST12M)    KIDS_BABYWEAR_SPEND_PAST12M,
                                sum(KIDS_BABYWEAR_VISITS_PAST12M)   KIDS_BABYWEAR_VISITS_PAST12M,
                                sum(BEVERAGE_ITEMS_PAST12M)         BEVERAGE_ITEMS_PAST12M,
                                sum(BEVERAGE_SPEND_PAST12M)         BEVERAGE_SPEND_PAST12M,
                                sum(BEVERAGE_VISITS_PAST12M)        BEVERAGE_VISITS_PAST12M

                           from (select /*+ Parallel (cdd,6) Full(cdd) */
                                        fin_year_no,
                                        fin_month_no,
                                        primary_customer_identifier,
                                        case when department_no = 107 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end womens_studio_w_items_past12m,
                                        case when department_no = 107 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end womens_studio_w_spend_past12m,
                                        case when department_no = 107 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                   num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end womens_studio_w_visits_past12m,
                                        case when department_no = 109 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end womens_re_items_past12m,
                                        case when department_no = 109 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end womens_re_spend_past12m,
                                        case when department_no = 109 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end womens_re_visits_past12m,
                                        case when department_no = 150 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end mens_re_items_past12m,
                                        case when department_no = 150 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end mens_re_spend_past12m,
                                        case when department_no = 150 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end mens_re_visits_past12m,
                                        case when department_no = 519 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end mens_studio_w_items_past12m,
                                        case when department_no = 519 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end mens_studio_w_spend_past12m,
                                        case when department_no = 519 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end mens_studio_w_visits_past12m,
                                        case when department_no in (105,110,665,678,682,712) then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end WOMENS_EDITION_ITEMS_PAST12M,
                                        case when department_no in (105,110,665,678,682,712) then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end WOMENS_EDITION_SPEND_PAST12M,
                                        case when department_no in (105,110,665,678,682,712) then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end WOMENS_EDITION_VISITS_PAST12M,
                                        case when department_no = 704 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end WOMENS_SAF_ITEMS_PAST12M,
                                        case when department_no = 704 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end WOMENS_SAF_SPEND_PAST12M,
                                        case when department_no = 704 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end WOMENS_SAF_VISITS_PAST12M,
                                        case when department_no in (147,692) then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end MENS_EDITION_ITEMS_PAST12M,
                                        case when department_no in (147,692) then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end MENS_EDITION_SPEND_PAST12M,
                                        case when department_no in (147,692) then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end MENS_EDITION_VISITS_PAST12M,
                                        case when department_no in (148,149,151,630,693,703) then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end MENS_DJ_ITEMS_PAST12M,
                                        case when department_no in (148,149,151,630,693,703) then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end MENS_DJ_SPEND_PAST12M,
                                        case when department_no in (148,149,151,630,693,703) then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end MENS_DJ_VISITS_PAST12M,
                                        case when department_no = 702 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end MENS_SAF_ITEMS_PAST12M,
                                        case when department_no = 702 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end MENS_SAF_SPEND_PAST12M,
                                        case when department_no = 702 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end MENS_SAF_VISITS_PAST12M,
                                        case when department_no = 140 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end MENS_SUPERSPORT_ITEMS_PAST12M,
                                        case when department_no = 140 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end MENS_SUPERSPORT_SPEND_PAST12M,
                                        case when department_no = 140 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end MENS_SUPERSPORT_VISITS_PAST12M,
                                      case when department_no in (158,163,687) then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end KIDS_AND_US_ITEMS_PAST12M,
                                        case when department_no in (158,163,687) then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end KIDS_AND_US_SPEND_PAST12M,
                                        case when department_no in (158,163,687) then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end KIDS_AND_US_VISITS_PAST12M,
                                        case when department_no in (157,162,523) then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end KIDS_YOUNGER_ITEMS_PAST12M,
                                        case when department_no in (157,162,523) then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end KIDS_YOUNGER_SPEND_PAST12M,
                                        case when department_no in (157,162,523) then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end KIDS_YOUNGER_VISITS_PAST12M, 
                                        case when department_no in (130,164,528,589,590,653,711) then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end KIDS_BABYWEAR_ITEMS_PAST12M,
                                        case when department_no in (130,164,528,589,590,653,711) then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end KIDS_BABYWEAR_SPEND_PAST12M,
                                        case when department_no in (130,164,528,589,590,653,711) then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end KIDS_BABYWEAR_VISITS_PAST12M,
                                        case when department_no in (37,45,56,64,81) then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end BEVERAGE_ITEMS_PAST12M,
                                        case when department_no in (37,45,56,64,81) then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end BEVERAGE_SPEND_PAST12M,
                                        case when department_no in (37,45,56,64,81) then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end BEVERAGE_VISITS_PAST12M

                                   from dwh_cust_performance.cust_db_dept_month cdd
                                  where fin_year_no = g_year_no
                                    and fin_month_no = g_month_no
                                    and department_no in (37,45,56,64,81,105,107,109,110,130,140,147,148,149,150,151,157,158,162,163,164,519,523,528,589,590,630,653,665,678,682,687,692,693,702,703,704,711,712)) cddm
                          group by fin_year_no, fin_month_no, primary_customer_identifier) f
                   on c.fin_year_no  = f.fin_year_no and
                      c.fin_month_no = f.fin_month_no and
                      c.primary_customer_identifier  = f.primary_customer_identifier
              left join (select /*+ Parallel (cdgm,6) Full(cdgm) */
                                fin_year_no,
                                fin_month_no,
                                primary_customer_identifier,
                                sum(womens_outerw_items_past12m)         womens_outerw_items_past12m,
                                sum(womens_outerw_spend_past12m)         womens_outerw_spend_past12m,
                                sum(womens_outerw_visits_past12m)        womens_outerw_visits_past12m,
                                sum(womens_lingerie_items_past12m)       womens_lingerie_items_past12m,
                                sum(womens_lingerie_spend_past12m)       womens_lingerie_spend_past12m,
                                sum(womens_lingerie_visits_past12m)      womens_lingerie_visits_past12m,
                                sum(kidswear_items_past12m)              kidswear_items_past12m,
                                sum(kidswear_spend_past12m)              kidswear_spend_past12m,
                                sum(kidswear_visits_past12m)             kidswear_visits_past12m,
                                sum(trenery_items_past12m)               trenery_items_past12m,
                                sum(trenery_spend_past12m)               trenery_spend_past12m,
                                sum(trenery_visits_past12m)              trenery_visits_past12m,
                                sum(menswear_items_past12m)              menswear_items_past12m,
                                sum(menswear_spend_past12m)              menswear_spend_past12m,
                                sum(menswear_visits_past12m)             menswear_visits_past12m,
                                sum(womens_foot_acc_items_past12m)       womens_foot_acc_items_past12m,
                                sum(womens_foot_acc_spend_past12m)       womens_foot_acc_spend_past12m,
                                sum(womens_foot_acc_visits_past12m)      womens_foot_acc_visits_past12m,
                                sum(country_road_items_past12m)          country_road_items_past12m,
                                sum(country_road_spend_past12m)          country_road_spend_past12m,
                                sum(country_road_visits_past12m)         country_road_visits_past12m,
                                sum(witchery_items_past12m)              witchery_items_past12m,
                                sum(witchery_spend_past12m)              witchery_spend_past12m,
                                sum(witchery_visits_past12m)             witchery_visits_past12m,
                                sum(mimco_items_past12m)                 mimco_items_past12m,
                                sum(mimco_spend_past12m)                 mimco_spend_past12m,
                                sum(mimco_visits_past12m)                mimco_visits_past12m,
                                sum(groceries_items_past12m)             groceries_items_past12m ,
                                sum(groceries_spend_past12m)             groceries_spend_past12m ,
                                sum(groceries_visits_past12m)            groceries_visits_past12m ,
                                sum(wine_bev_liquor_items_past12m)       wine_bev_liquor_items_past12m ,
                                sum(wine_bev_liquor_spend_past12m)       wine_bev_liquor_spend_past12m ,
                                sum(wine_bev_liquor_visits_past12m)      wine_bev_liquor_visits_past12m,
                                sum(snackin_gifting_items_past12m)       snackin_gifting_items_past12m,
                                sum(snackin_gifting_spend_past12m)       snackin_gifting_spend_past12m,
                                sum(snackin_gifting_visits_past12m)      snackin_gifting_visits_past12m,
                                sum(prepared_deli_items_past12m)         prepared_deli_items_past12m,
                                sum(prepared_deli_spend_past12m)         prepared_deli_spend_past12m,
                                sum(prepared_deli_visits_past12m)        prepared_deli_visits_past12m,
                                sum(bakery_items_past12m)                bakery_items_past12m,
                                sum(bakery_spend_past12m)                bakery_spend_past12m,
                                sum(bakery_visits_past12m)               bakery_visits_past12m,
                                sum(produce_horti_items_past12m)         produce_horti_items_past12m,
                                sum(produce_horti_spend_past12m)         produce_horti_spend_past12m,
                                sum(produce_horti_visits_past12m)        produce_horti_visits_past12m,
                                sum(protein_items_past12m)               protein_items_past12m,
                                sum(protein_spend_past12m)               protein_spend_past12m,
                                sum(protein_visits_past12m)              protein_visits_past12m,
                                sum(dairy_items_past12m)                 dairy_items_past12m,
                                sum(dairy_spend_past12m)                 dairy_spend_past12m,
                                sum(dairy_visits_past12m)                dairy_visits_past12m,
                                sum(home_person_pet_items_past12m)       home_person_pet_items_past12m,
                                sum(home_person_pet_spend_past12m)       home_person_pet_spend_past12m,
                                sum(home_person_pet_visits_past12m)      home_person_pet_visits_past12m,
                                sum(FOODSERVICES_ITEMS_PAST12M)          FOODSERVICES_ITEMS_PAST12M,
                                sum(FOODSERVICES_SPEND_PAST12M)          FOODSERVICES_SPEND_PAST12M,
                                sum(FOODSERVICES_VISITS_PAST12M)         FOODSERVICES_VISITS_PAST12M

                           from (select /*+ Parallel (cdg,6) Full(cdg) */
                                        fin_year_no,
                                        fin_month_no,
                                        primary_customer_identifier,
                                        case when group_no = 1 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end womens_outerw_items_past12m,
                                        case when group_no = 1 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end womens_outerw_spend_past12m,
                                        case when group_no = 1 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end womens_outerw_visits_past12m,
                                        case when group_no = 2 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end womens_lingerie_items_past12m,
                                        case when group_no = 2 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end womens_lingerie_spend_past12M,
                                        case when group_no = 2 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end womens_lingerie_visits_past12M,
                                        case when group_no = 3 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end kidswear_items_past12m,
                                        case when group_no = 3 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end kidswear_spend_past12m,
                                        case when group_no = 3 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end kidswear_visits_past12m,
                                        case when group_no = 4 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end trenery_items_past12m,
                                        case when group_no = 4 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end trenery_spend_past12m,
                                        case when group_no = 4 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end trenery_visits_past12m,
                                        case when group_no = 5 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end menswear_items_past12m,
                                        case when group_no = 5 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end menswear_spend_past12m,
                                        case when group_no = 5 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end menswear_visits_past12m,
                                        case when group_no = 6 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end womens_foot_acc_items_past12m,
                                        case when group_no = 6 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end womens_foot_acc_spend_past12m,
                                        case when group_no = 6 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end womens_foot_acc_visits_past12m,
                                        case when group_no = 12 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end country_road_items_past12m,
                                        case when group_no = 12 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end country_road_spend_past12m,
                                        case when group_no = 12 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end country_road_visits_past12m,
                                        case when group_no = 13 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end witchery_items_past12m,
                                        case when group_no = 13 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end witchery_spend_past12m,
                                        case when group_no = 13 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end witchery_visits_past12m,
                                        case when group_no = 14 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end mimco_items_past12m,
                                        case when group_no = 14 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end mimco_spend_past12m,
                                        case when group_no = 14 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end mimco_visits_past12m,
                                        case when group_no = 9000 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end groceries_items_past12m,
                                        case when group_no = 9000 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end groceries_spend_past12m,
                                        case when group_no = 9000 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end groceries_visits_past12m,
                                        case when group_no = 9001 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end wine_bev_liquor_items_past12m,
                                        case when group_no = 9001 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end wine_bev_liquor_spend_past12m,
                                        case when group_no = 9001 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end wine_bev_liquor_visits_past12m,
                                        case when group_no = 9002 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end snackin_gifting_items_past12m,
                                        case when group_no = 9002 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end snackin_gifting_spend_past12m,
                                        case when group_no = 9002 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end snackin_gifting_visits_past12m,
                                        case when group_no = 9004 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end prepared_deli_items_past12m,
                                        case when group_no = 9004 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end prepared_deli_spend_past12m,
                                        case when group_no = 9004 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end prepared_deli_visits_past12m,
                                        case when group_no = 9005 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end bakery_items_past12m,
                                        case when group_no = 9005 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end bakery_spend_past12m,
                                        case when group_no = 9005 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end bakery_visits_past12m,
                                        case when group_no = 9006 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end produce_horti_items_past12m,
                                        case when group_no = 9006 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end produce_horti_spend_past12m,
                                        case when group_no = 9006 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end produce_horti_visits_past12m,
                                        case when group_no = 9007 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end protein_items_past12m,
                                        case when group_no = 9007 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end protein_spend_past12m,
                                        case when group_no = 9007 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end protein_visits_past12m,
                                        case when group_no = 9008 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end dairy_items_past12m,
                                        case when group_no = 9008 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end dairy_spend_past12m,
                                        case when group_no = 9008 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end dairy_visits_past12m,
                                        case when group_no = 9010 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end home_person_pet_items_past12m,
                                        case when group_no = 9010 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end home_person_pet_spend_past12m,
                                        case when group_no = 9010 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end home_person_pet_visits_past12m,

                                        case when group_no = 9009 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end FOODSERVICES_ITEMS_PAST12M,
                                        case when group_no = 9009 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end FOODSERVICES_SPEND_PAST12M,
                                        case when group_no = 9009 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end FOODSERVICES_VISITS_PAST12M

                                   from dwh_cust_performance.cust_db_group_month cdg
                                  where fin_year_no = g_year_no
                                    and fin_month_no = g_month_no
                                    and group_no in (1,2,3,4,5,6,12,13,14,9000,9001,9002,9004,9005,9006,9007,9008,9009,9010)) cdgm
                          group by fin_year_no,fin_month_no,primary_customer_identifier) g
                   on c.fin_year_no  = g.fin_year_no and
                      c.fin_month_no = g.fin_month_no and
                      c.primary_customer_identifier  = g.primary_customer_identifier
              left join (select /*+ Parallel (cdsm,6) Full(cdsm) */
                                fin_year_no,
                                fin_month_no,
                                primary_customer_identifier,
                                sum(classic_items_past12m)          classic_items_past12m,
                                sum(classic_spend_past12m)          classic_spend_past12m,
                                sum(classic_visits_past12m)         classic_visits_past12m,
                                sum(MOD_CONTEMP_ITEMS_PAST12M)      MOD_CONTEMP_ITEMS_PAST12M,
                                sum(MOD_CONTEMP_SPEND_PAST12M)      MOD_CONTEMP_SPEND_PAST12M,
                                sum(MOD_CONTEMP_VISITS_PAST12M)     MOD_CONTEMP_VISITS_PAST12M,
                                sum(beauty_brand_items_past12m)     beauty_brand_items_past12m,
                                sum(beauty_brand_spend_past12m)     beauty_brand_spend_past12m,
                                sum(beauty_brand_visits_past12m)    beauty_brand_visits_past12m,
                                sum(beauty_ww_label_items_past12m)  beauty_ww_label_items_past12m,
                                sum(beauty_ww_label_spend_past12m)  beauty_ww_label_spend_past12m,
                                sum(beauty_ww_label_visits_past12m) beauty_ww_label_visits_past12m,
                                sum(kids_re_outerw_items_past12m)   kids_re_outerw_items_past12m,
                                sum(kids_re_outerw_spend_past12m)   kids_re_outerw_spend_past12m,
                                sum(kids_re_outerw_visits_past12m)  kids_re_outerw_visits_past12m,
                                sum(WINE_LIQUOR_ITEMS_PAST12M)      WINE_LIQUOR_ITEMS_PAST12M,
                                sum(WINE_LIQUOR_SPEND_PAST12M)      WINE_LIQUOR_SPEND_PAST12M,
                                sum(WINE_LIQUOR_VISITS_PAST12M)     WINE_LIQUOR_VISITS_PAST12M,
                                sum(WOMENS_DJ_ITEMS_PAST12M)       WOMENS_DJ_ITEMS_PAST12M,
                                sum(WOMENS_DJ_SPEND_PAST12M)       WOMENS_DJ_SPEND_PAST12M,
                                sum(WOMENS_DJ_VISITS_PAST12M)       WOMENS_DJ_VISITS_PAST12M

                          from (select /*+ Parallel (cds,6) Full(cds) */
                                       fin_year_no,
                                       fin_month_no,
                                       primary_customer_identifier,
                                       case when subgroup_no = 93 then
                                                 num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                 num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                 num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                       end classic_items_past12m,
                                       case when subgroup_no = 93 then
                                                 sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                 sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                 sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                       end classic_spend_past12m,
                                       case when subgroup_no = 93 then
                                                 num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                 num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                 num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                       end classic_visits_past12m,
                                       case when subgroup_no = 94 then
                                                 num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                 num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                 num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                       end MOD_CONTEMP_ITEMS_PAST12M,
                                       case when subgroup_no = 94 then
                                                 sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                 sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                 sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                       end MOD_CONTEMP_SPEND_PAST12M,
                                       case when subgroup_no = 94 then
                                                 num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                 num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                 num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                       end MOD_CONTEMP_VISITS_PAST12M,
                                       case when subgroup_no = 233 then
                                                 num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                 num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                 num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                       end beauty_brand_items_past12m,
                                       case when subgroup_no = 233 then
                                                 sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                 sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                 sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                       end beauty_brand_spend_past12m,
                                       case when subgroup_no = 233 then
                                                 num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                 num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                 num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                       end beauty_brand_visits_past12m,
                                       case when subgroup_no = 280 then
                                                 num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                 num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                 num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                       end beauty_ww_label_items_past12m,
                                       case when subgroup_no = 280 then
                                                 sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                 sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                 sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                       end beauty_ww_label_spend_past12m,
                                       case when subgroup_no = 280 then
                                                 num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                 num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                 num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                       end beauty_ww_label_visits_past12m,
                                       case when subgroup_no = 339 then
                                                 num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                 num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                 num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                       end kids_re_outerw_items_past12m,
                                       case when subgroup_no = 339 then
                                                 sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                 sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                 sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                       end kids_re_outerw_spend_past12m,
                                       case when subgroup_no = 339 then
                                                 num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                 num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                 num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                       end kids_re_outerw_visits_past12m,
                                       case when subgroup_no in (1006,1024) then
                                                 num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                 num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                 num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                       end WINE_LIQUOR_ITEMS_PAST12M,
                                       case when subgroup_no in (1006,1024) then
                                                 sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                 sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                 sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                       end WINE_LIQUOR_SPEND_PAST12M,
                                       case when subgroup_no in (1006,1024) then
                                                 num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                 num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                 num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                       end WINE_LIQUOR_VISITS_PAST12M,
                                       case when subgroup_no in (93) then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end WOMENS_DJ_ITEMS_PAST12M,
                                        case when subgroup_no in (93) then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end WOMENS_DJ_SPEND_PAST12M,
                                        case when subgroup_no in (93) then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end WOMENS_DJ_VISITS_PAST12M

                                  from dwh_cust_performance.cust_db_subgroup_month cds
                                 where fin_year_no = g_year_no
                                   and fin_month_no = g_month_no
                                   and subgroup_no in (93,94,233,280,339,1006,1024)) cdsm
                          group by fin_year_no, fin_month_no, primary_customer_identifier) h
                   on c.fin_year_no  = h.fin_year_no and
                      c.fin_month_no = h.fin_month_no and
                      c.primary_customer_identifier  = h.primary_customer_identifier
              left join (select /*+ Parallel (cdbm,6) Full(cdbm) */
                                fin_year_no,
                                fin_month_no,
                                primary_customer_identifier,
                                sum(TOTAL_ITEMS_PAST12M)           TOTAL_ITEMS_PAST12M,
                                sum(TOTAL_SPEND_PAST12M)           TOTAL_SPEND_PAST12M,
                                sum(TOTAL_VISITS_PAST12M)         TOTAL_VISITS_PAST12M,
                                sum(Foods_items_past12m)           Foods_items_past12m,
                                sum(Foods_spend_past12m)           Foods_spend_past12m,
                                sum(Foods_visits_past12m)          Foods_visits_past12m,
                                sum(Clothing_items_past12m)        Clothing_items_past12m,
                                sum(Clothing_spend_past12m)        Clothing_spend_past12m,
                                sum(Clothing_visits_past12m)       Clothing_visits_past12m,
                                sum(Homeware_items_past12m)        Homeware_items_past12m,
                                sum(Homeware_spend_past12m)        Homeware_spend_past12m,
                                sum(Homeware_visits_past12m)       Homeware_visits_past12m,
                                sum(Digital_items_past12m)         Digital_items_past12m,
                                sum(Digital_spend_past12m)         Digital_spend_past12m,
                                sum(Digital_visits_past12m)        Digital_visits_past12m,
                                sum(Beauty_items_past12m)          Beauty_items_past12m,
                                sum(Beauty_spend_past12m)          Beauty_spend_past12m,
                                sum(Beauty_visits_past12m)         Beauty_visits_past12m,
                                sum(Country_rd_grp_items_past12m)  Country_rd_grp_items_past12m,
                                sum(Country_rd_grp_spend_past12m)  Country_rd_grp_spend_past12m,
                                sum(Country_rd_grp_visits_past12m) Country_rd_grp_visits_past12m
                           from (select /*+ Parallel (cdb,6) Full(cdb) */
                                        fin_year_no,
                                        fin_month_no,
                                        primary_customer_identifier,
                                        case when business_unit_no in (50,51,52,53,54,55) then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end total_items_past12m,
                                        case when business_unit_no in (50,51,52,53,54,55) then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end total_spend_past12m,
                                        case when business_unit_no in (50,51,52,53,54,55) then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end total_visits_past12m,
                                        case when business_unit_no = 50 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end foods_items_past12m,
                                        case when business_unit_no = 50 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end foods_spend_past12m,
                                        case when business_unit_no = 50 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end foods_visits_past12m,
                                        case when business_unit_no = 51 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end clothing_items_past12m,
                                        case when business_unit_no = 51 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end clothing_spend_past12m,
                                        case when business_unit_no = 51 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end clothing_visits_past12m,
                                        case when business_unit_no = 52 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end homeware_items_past12m,
                                        case when business_unit_no = 52 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end homeware_spend_past12m,
                                        case when business_unit_no = 52 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end homeware_visits_past12m,
                                        case when business_unit_no = 53 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end digital_items_past12m,
                                        case when business_unit_no = 53 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end digital_spend_past12m,
                                        case when business_unit_no = 53 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end digital_visits_past12m,
                                        case when business_unit_no = 54 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end beauty_items_past12m,
                                        case when business_unit_no = 54 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end beauty_spend_past12m,
                                        case when business_unit_no = 54 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end beauty_visits_past12m,
                                        case when business_unit_no = 55 then
                                                  num_item_yr1_mn01 +  num_item_yr1_mn02 + num_item_yr1_mn03 + num_item_yr1_mn04 +
                                                  num_item_yr1_mn05 +  num_item_yr1_mn06 + num_item_yr1_mn07 + num_item_yr1_mn08 +
                                                  num_item_yr1_mn09 +  num_item_yr1_mn10 + num_item_yr1_mn11 + num_item_yr1_mn12
                                        end country_rd_grp_items_past12m,
                                        case when business_unit_no = 55 then
                                                  sales_yr1_mn01 +  sales_yr1_mn02 + sales_yr1_mn03 + sales_yr1_mn04 +
                                                  sales_yr1_mn05 +  sales_yr1_mn06 + sales_yr1_mn07 + sales_yr1_mn08 +
                                                  sales_yr1_mn09 +  sales_yr1_mn10 + sales_yr1_mn11 + sales_yr1_mn12
                                        end country_rd_grp_spend_past12m,
                                        case when business_unit_no = 55 then
                                                  num_visit_yr1_mn01 +  num_visit_yr1_mn02 + num_visit_yr1_mn03 + num_visit_yr1_mn04 +
                                                  num_visit_yr1_mn05 +  num_visit_yr1_mn06 + num_visit_yr1_mn07 + num_visit_yr1_mn08 +
                                                  num_visit_yr1_mn09 +  num_visit_yr1_mn10 + num_visit_yr1_mn11 + num_visit_yr1_mn12
                                        end country_rd_grp_visits_past12m
                                   from dwh_cust_performance.cust_db_business_unit_month cdb
                                  where fin_year_no = g_year_no
                                    and fin_month_no = g_month_no
                                    and business_unit_no in (50,51,52,53,54,55)) cdbm
                          group by fin_year_no,fin_month_no,primary_customer_identifier) i
                   on c.fin_year_no  = i.fin_year_no and
                      c.fin_month_no = i.fin_month_no and
                      c.primary_customer_identifier = i.primary_customer_identifier)
              left join (select /*+ Parallel (dsp,6) Full(dsp) */
                                primary_customer_identifier,
                                max(foods_preferred_store) foods_preferred_store,
                                max(non_foods_preferred_store) non_foods_preferred_store
                           from (select /*+ Parallel (dsop,6) Full(dsop) */
                                        primary_customer_identifier,
                                        case when fd_ch = 1 then location_no end foods_preferred_store,
                                        case when fd_ch = 2 then location_no end non_foods_preferred_store
                                   from dwh_cust_performance.dim_customer_store_of_pref dsop)dsp
                          group by primary_customer_identifier) j
                   on c.primary_customer_identifier = j.primary_customer_identifier
           )
    where (nvl(non_food_life_seg_code,0) +
            nvl(food_life_seg_code,0) +
            nvl(nfshv_current_seg,0) +
            nvl(fshv_current_seg,0) +
            nvl(shopping_habit_segment_no,0) +
            nvl(TOTAL_ITEMS_PAST12M,0) +
            nvl(TOTAL_SPEND_PAST12M,0) +
            nvl(TOTAL_VISITS_PAST12M,0) +
            nvl(womens_studio_w_items_past12m,0) +
            nvl(womens_studio_w_spend_past12m,0) +
            nvl(womens_studio_w_visits_past12m,0) +
            nvl(womens_re_items_past12m,0) +
            nvl(womens_re_spend_past12m,0) +
            nvl(womens_re_visits_past12m,0) +
            nvl(mens_re_items_past12m,0) +
            nvl(mens_re_spend_past12m,0) +
            nvl(mens_re_visits_past12m,0) +
            nvl(mens_studio_w_items_past12m,0) +
            nvl(mens_studio_w_spend_past12m,0) +
            nvl(mens_studio_w_visits_past12m,0) +
            nvl(womens_outerw_items_past12m,0) +
            nvl(womens_outerw_spend_past12m,0) +
            nvl(womens_outerw_visits_past12m,0) +
            nvl(womens_lingerie_items_past12m,0) +
            nvl(womens_lingerie_spend_past12m,0) +
            nvl(womens_lingerie_visits_past12m,0) +
            nvl(kidswear_items_past12m,0) +
            nvl(kidswear_spend_past12m,0) +
            nvl(kidswear_visits_past12m,0) +
            nvl(trenery_items_past12m,0) +
            nvl(trenery_spend_past12m,0) +
            nvl(trenery_visits_past12m,0) +
            nvl(menswear_items_past12m,0) +
            nvl(menswear_spend_past12m,0) +
            nvl(menswear_visits_past12m,0) +
            nvl(womens_foot_acc_items_past12m,0) +
            nvl(womens_foot_acc_spend_past12m,0) +
            nvl(womens_foot_acc_visits_past12m,0) +
            nvl(country_road_items_past12m,0) +
            nvl(country_road_spend_past12m,0) +
            nvl(country_road_visits_past12m,0) +
            nvl(witchery_items_past12m,0) +
            nvl(witchery_spend_past12m,0) +
            nvl(witchery_visits_past12m,0) +
            nvl(mimco_items_past12m,0) +
            nvl(mimco_spend_past12m,0) +
            nvl(mimco_visits_past12m,0) +
            nvl(groceries_items_past12m,0) +
            nvl(groceries_spend_past12m,0) +
            nvl(groceries_visits_past12m,0) +
            nvl(wine_bev_liquor_items_past12m,0) +
            nvl(wine_bev_liquor_spend_past12m,0) +
            nvl(wine_bev_liquor_visits_past12m,0) +
            nvl(snackin_gifting_items_past12m,0) +
            nvl(snackin_gifting_spend_past12m,0) +
            nvl(snackin_gifting_visits_past12m,0) +
            nvl(prepared_deli_items_past12m,0) +
            nvl(prepared_deli_spend_past12m,0) +
            nvl(prepared_deli_visits_past12m,0) +
            nvl(bakery_items_past12m,0) +
            nvl(bakery_spend_past12m,0) +
            nvl(bakery_visits_past12m,0) +
            nvl(produce_horti_items_past12m,0) +
            nvl(produce_horti_spend_past12m,0) +
            nvl(produce_horti_visits_past12m,0) +
            nvl(protein_items_past12m,0) +
            nvl(protein_spend_past12m,0) +
            nvl(protein_visits_past12m,0) +
            nvl(dairy_items_past12m,0) +
            nvl(dairy_spend_past12m,0) +
            nvl(dairy_visits_past12m,0) +
            nvl(home_person_pet_items_past12m,0) +
            nvl(home_person_pet_spend_past12m,0) +
            nvl(home_person_pet_visits_past12m,0) +
            nvl(classic_items_past12m,0) +
            nvl(classic_spend_past12m,0) +
            nvl(classic_visits_past12m,0) +
            nvl(MOD_CONTEMP_ITEMS_PAST12M,0) +
            nvl(MOD_CONTEMP_SPEND_PAST12M,0) +
            nvl(MOD_CONTEMP_VISITS_PAST12M,0) +
            nvl(beauty_brand_items_past12m,0) +
            nvl(beauty_brand_spend_past12m,0) +
            nvl(beauty_brand_visits_past12m,0) +
            nvl(beauty_ww_label_items_past12m,0) +
            nvl(beauty_ww_label_spend_past12m,0) +
            nvl(beauty_ww_label_visits_past12m,0) +
            nvl(kids_re_outerw_items_past12m,0) +
            nvl(kids_re_outerw_spend_past12m,0) +
            nvl(kids_re_outerw_visits_past12m,0) +
            nvl(foods_items_past12m,0) +
            nvl(foods_spend_past12m,0) +
            nvl(foods_visits_past12m,0) +
            nvl(clothing_items_past12m,0) +
            nvl(clothing_spend_past12m,0) +
            nvl(clothing_visits_past12m,0) +
            nvl(homeware_items_past12m,0) +
            nvl(homeware_spend_past12m,0) +
            nvl(homeware_visits_past12m,0) +
            nvl(digital_items_past12m,0) +
            nvl(digital_spend_past12m,0) +
            nvl(digital_visits_past12m,0) +
            nvl(beauty_items_past12m,0) +
            nvl(beauty_spend_past12m,0) +
            nvl(beauty_visits_past12m,0) +
            nvl(country_rd_grp_items_past12m,0) +
            nvl(country_rd_grp_spend_past12m,0) +
            nvl(country_rd_grp_visits_past12m,0) +
            nvl(foods_preferred_store,0) +
            nvl(non_foods_preferred_store,0)) <> 0;

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

end WH_PRF_CUST_380E;
