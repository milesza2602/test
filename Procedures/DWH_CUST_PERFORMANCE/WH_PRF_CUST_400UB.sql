--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_400UB
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_400UB" (p_forall_limit in integer,p_success out boolean) AS 

--**************************************************************************************************
--  Date:        May 2018
--  Author:      Theo Filander
--  Purpose:     Create Customer Satisfaction Survey table
--  Tables:      Input  - cust_basket_item
--                      - dim_customer_mapping
--                      - dim_customer_master
--                      - cust_csm_shopping_habits
--                      - cust_csm_value_segment
--                      - cust_wod_tier_mth_detail
--                      - dim_item
--                      - dim_location
--                      - apex_mkt_csats_summary
--                      - apex_mkt_csats_detail
--                      - apex_mkt_csats_opt_out
--                      - TEMP_ALL_PARAMS
--                      - TEMP_SURVEY_MASTER
--                      - TEMP_CSATS_SURVEY_DETAIL

--               Output - TEMP SURVEY_SQL (For SQL debugging purposes)
--
--               Output - cust_csats_survey_detail
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--               Theo Filander  08/Oct/2018
--               Added the functionality to cater for multiple records being returned
--               from dim_customer_mapping - Order by SOURCE_KEY and LAST_UPDATED_DATE desc.
--               Streamline the program to optimise for inclusion of new/amended business rules.
-- 
--               Theo Filander  22/NOV/2018
--               Added Code to extract Retail Call Centre Recipients.
--
--
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_recs_read          number         := 0;
g_recs_inserted      number         := 0;
g_recs_updated       number         := 0;
g_recs_deleted       number         := 0;
g_recs_hospital      number         := 0;
g_recs_in_temp       number         := 0;
g_recs_loaded        number         := 0;
g_forall_limit       number         := 10000;
g_fin_year           number;
g_fin_month          number;
g_day_of_week        number         := 0;

g_where_clause       varchar2(4000) := null;
g_and_clause         varchar2(4000) := null;
g_cols               varchar2(4000) := null;
g_csm_period         varchar2(40);
g_race_case          varchar2(4000);
g_age_case           varchar2(600);
g_runday             varchar2(9);
g_debug              boolean        :=false;
g_search_days        number         := 0;
g_online_search_days number         := 0;
g_min_age            number         := 0;
g_setup_sql          number         :=0;


g_date               date         := trunc(sysdate);
g_rundate            date         := trunc(sysdate);
g_sql_log_date       date         := sysdate;  
g_start_date         date         := trunc(sysdate);
g_end_date           date         := trunc(sysdate);
g_online_start_date  date         := trunc(sysdate);
g_online_end_date    date         := trunc(sysdate);



g_stmt                clob;

g_survey_code	                    number;
g_survey_name	                    varchar2(100);
g_survey_area	                    varchar2(100);
g_minimum_spend_criteria	        number;
g_minimum_units_criteria	        number;
g_priority_order	                number;
g_quota_minimum	                    number;
g_quota_maximum	                    number;
g_last_updated_date	                date;
g_updated_by	                    varchar2(20);
g_created_date	                    date;
g_created_by	                    varchar2(20);
g_modified_ind	                    char(1);
g_survey_status	                    varchar2(20);
g_same_csats_survey	                number;
g_diff_csats_survey         	    number;
g_different_survey              	number;
g_channel	                        varchar2(20);
g_survey_name_reformatted	        varchar2(100);
g_merchandise_classification	    varchar2(50);
g_survey_type	                    varchar2(50);
g_tran_start_date	                date;
g_tran_end_date	                    date;
g_survey_params	                    varchar2(4000);
g_all_survey_params	                varchar2(4000);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_400U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST_SURVEY EX CUST_BASKET_ITEM (TESTING)';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

procedure build_csats_survey_sql as
begin

      g_stmt :=' insert /*+ Append */ into dwh_cust_performance.temp_csats_basket_item 
                          (survey_code,
                           survey_name,
                           survey_area,
                           same_csats_survey_eligibility,
                           diff_csats_survey_eligibility,
                           different_survey_eligibility,
                           master_subscriber_key,
                           primary_customer_identifier,
                           customer_no,
                           identity_document_no,
                           populated_email_address,
                           work_email_address,
                           home_email_address,
                           statement_email_address,
                           ecommerce_email_address,
                           populated_cell_no,
                           home_cell_no,
                           work_cell_no,
                           title,
                           first_name,
                           last_name,
                           derived_race,
                           age_band,
                           channel,
                           location_no,
                           location_name,
                           district_no,
                           district_name,
                           region_no,
                           region_name,
                           st_store_type,
                           department_no,
                           department_name,
                           subgroup_no,
                           subgroup_name,
                           group_no,
                           group_name,
                           business_unit_no,
                           business_unit_name,
                           tran_date,
                           till_no,
                           item_qty,
                           selling_value,
                           month_tier,
                           csm_shopping_habit_segment_no,
                           foods_value_segment_no,
                           non_foods_value_segment_no,
                           survey_name_reformatted,
                           merchandise_classification,
                           survey_type,
                           tran_date_reformatted,
                           location_name_reformatted,
                           extracted_date,
                           last_updated_date,
                           survey_params,
                           all_survey_params)
                    select /*+ Parallel(8) Full */ 
                           survey_code,
                           survey_name,
                           survey_area,
                           same_csats_survey_eligibility,
                           diff_csats_survey_eligibility,
                           different_survey_eligibility,
                           master_subscriber_key,
                           primary_customer_identifier,
                           customer_no,
                           identity_document_no,
                           populated_email_address,
                           work_email_address,
                           home_email_address,
                           statement_email_address,
                           ecommerce_email_address,
                           populated_cell_no,
                           home_cell_no,
                           work_cell_no,
                           title,
                           first_name,
                           last_name,
                           derived_race,
                           age_band,
                           channel,
                           location_no,
                           case when survey_code in (29, 30, 31) then ''Website''
                                else location_name
                           end location_name,
                           district_no,
                           case when survey_code in (29, 30, 31) then ''Website''
                                else district_name
                           end district_name,
                           region_no,
                           case when survey_code in (29, 30, 31) then ''Website''
                                else region_name
                           end region_name,
                           st_store_type,
                           department_no,
                           department_name,
                           subgroup_no,
                           subgroup_name,
                           group_no,
                           group_name,
                           business_unit_no,
                           business_unit_name,
                           tran_date,
                           till_no,
                           item_qty,
                           selling_value,
                           month_tier,
                           csm_shopping_habit_segment_no,
                           foods_value_segment_no,
                           non_foods_value_segment_no,
                           case when survey_name_reformatted = ''Derived'' then
                                case when survey_code in (9,10,11,30)  and till_no <> 999 and business_unit_no = 51 then ''Fashion Department''
                                     when survey_code in (9,10,11,30)  and till_no <> 999 and business_unit_no = 52 then ''Homeware Department''
                                     when survey_code = 30  and till_no <> 999 and business_unit_no = 54 then ''Beauty Department''
                                     when survey_code = 31  and till_no <> 999 and business_unit_no <> 55 then
                                          case when subgroup_no <> 425 then ''Fashion Department''
                                               when subgroup_no =  425 then ''Homeware Department''
                                          end
                                     when survey_code = 32 and business_unit_no = 51 then ''Fashion Department''
                                     when survey_code = 32 and business_unit_no = 52 then ''Homeware Department''
                                     when survey_code = 32 and business_unit_no = 54 then ''Beauty Department''
                                     when survey_code = 32 and business_unit_no = 55 then
                                          case when subgroup_no <> 425 then ''Fashion Department''
                                               when subgroup_no =  425 then ''Homeware Department''
                                          end
                                end
                                else survey_name_reformatted
                            end survey_name_reformatted,
                           case when merchandise_classification = ''Derived'' then
                                case when survey_code in (9,10,11,30)  and till_no <> 999 and business_unit_no = 51 then ''Fashion''
                                     when survey_code in (9,10,11,30)  and till_no <> 999 and business_unit_no = 52 then ''Homeware''
                                     when survey_code = 30  and till_no <> 999 and business_unit_no = 54 then ''Beauty''
                                     when survey_code = 31  and till_no <> 999 and business_unit_no <> 55 then
                                          case when subgroup_no <> 425 then ''Fashion''
                                               when subgroup_no =  425 then ''Homeware''
                                          end
                                     when survey_code = 32 and business_unit_no = 51 then ''Fashion''
                                     when survey_code = 32 and business_unit_no = 52 then ''Homeware''
                                     when survey_code = 32 and business_unit_no = 54 then ''Beauty''
                                     when survey_code = 32 and business_unit_no = 55 then
                                          case when subgroup_no <> 425 then ''Fashion''
                                               when subgroup_no =  425 then ''Homeware''
                                          end

                                end
                                else merchandise_classification
                           end merchandise_classification,
                           survey_type,
                           tran_date_reformatted,
                           case when survey_code not in (29, 30, 31) then location_name_reformatted 
                                else ''Woolworths Website''
                           end location_name_reformatted,
                           extracted_date,
                           last_updated_date,
                           survey_params,
                           all_survey_params 
                    from (
                           with temp_mapping as (
                                                 select /*+ Parallel(dm,8) */
                                                        dm.*,
                                                        rank() over (partition by dm.source_key, dm.last_updated_date order by dm.last_updated_date desc) dm_seq
                                                   from dwh_cust_performance.dim_customer_mapping dm
                                                  where dm.source = ''C2''
                                                 )
                           select /*+Parallel(cbi,8) Parallel(di,8) Parallel(tm,8) Parallel(dcm,8) Parallel(dr,8) Parallel(csm,8) Parallel(cw,8) Parallel(cs,8) Parallel(sd,8) Parallel(ap,8) Parallel(dl,8) Parallel(cl,8)*/ 
                                  distinct cast('||g_survey_code||' as number) survey_code,
                                  '''||g_survey_name||''' survey_name,
                                  '''||g_survey_area||''' survey_area,
                                  '''||g_same_csats_survey||''' same_csats_survey_eligibility,
                                  '''||g_diff_csats_survey||''' diff_csats_survey_eligibility,
                                  '''||g_different_survey||''' different_survey_eligibility,
                                  dcm.master_subscriber_key,
                                  cbi.primary_customer_identifier,
                                  tm.source_key customer_no,
                                  dcm.identity_document_no,
                                  dcm.populated_email_address,
                                  dcm.work_email_address,
                                  dcm.home_email_address,
                                  dcm.statement_email_address,
                                  dcm.ecommerce_email_address,
                                  dcm.populated_cell_no,
                                  dcm.home_cell_no,
                                  dcm.work_cell_no,
                                  dcm.title,
                                  dcm.first_name,
                                  dcm.last_name,
                                  dcm.derived_race,
                                  dcm.age_band,
                                  '''||g_channel||''' channel,
                                  dl.location_no,
                                  dl.location_name,
                                  dl.district_no,
                                  dl.district_name,
                                  dl.region_no,
                                  dl.region_name,
                                  dl.st_s4s_shape_of_chain_desc st_store_type,
                                  di.department_no,
                                  di.department_name,
                                  di.subgroup_no,
                                  di.subgroup_name,
                                  di.business_unit_no,
                                  di.group_no,
                                  di.group_name,
                                  di.business_unit_name,
                                  trunc(cbi.tran_date) tran_date,
                                  cbi.till_no,
                                  sum(cbi.item_tran_qty) item_qty,
                                  sum(cbi.item_tran_selling-cbi.discount_selling) selling_value,
                                  cw.month_tier,
                                  csm.csm_shopping_habit_segment_no,
                                  cs.foods_value_segment_no,
                                  cs.non_foods_value_segment_no,
                                  '''||nvl(g_survey_name_reformatted,'Derived')||''' survey_name_reformatted,
                                  '''||nvl(g_merchandise_classification,'Derived')||''' merchandise_classification,
                                  '''||g_survey_type||''' survey_type,
                                  to_char(tran_date,''Day,DD Month YYYY'') tran_date_reformatted,
                                  cl.csats_location_name location_name_reformatted,
                                  trunc(sysdate) extracted_date,
                                  trunc(sysdate) last_updated_date,
                                  q''['||g_survey_params||']'' survey_params,
                                  q''['||g_all_survey_params||']'' all_survey_params 
                             from dwh_cust_performance.cust_basket_item cbi 
                            inner join dim_item di on (cbi.item_no=di.item_no) 
                            inner join temp_mapping tm on (cbi.primary_customer_identifier=tm.source_key) 
                            inner join (select /*+ Parallel(dcm,8) Full() */
                                               dcm.master_subscriber_key,
                                               dcm.identity_document_no,
                                               upper(dcm.populated_email_address) populated_email_address,
                                               upper(dcm.work_email_address)  work_email_address,
                                               upper(dcm.home_email_address)  home_email_address,
                                               upper(dcm.statement_email_address) statement_email_address,
                                               upper(dcm.ecommerce_email_address) ecommerce_email_address,
                                               dcm.populated_cell_no,
                                               dcm.home_cell_no,
                                               dcm.work_cell_no,
                                               dcm.title,
                                               dcm.first_name,
                                               dcm.last_name,
                                               '||g_race_case||' derived_race,
                                               '||g_age_case||' age_band,
                                               dcm.ww_man_email_opt_out_ind  man_email_opt_out_ind,
                                               dcm.ww_man_sms_opt_out_ind    man_sms_opt_out_ind,
                                               case when de.id_no is null then ''N'' else ''Y'' end is_staff
                                          from dwh_cust_performance.dim_customer_master dcm
                                          left join dwh_hr_performance.dim_employee de on (to_char(dcm.identity_document_no) = de.id_no)
                                          ) dcm on (tm.subscriber_key = dcm.master_subscriber_key)
                           inner join dwh_cust_performance.cust_csm_shopping_habits csm on (cbi.primary_customer_identifier=csm.csm_customer_identifier)
                           inner join cust_wod_tier_mth_detail cw on (cbi.primary_customer_identifier=cw.customer_no and 
                                                                             csm.fin_year_no=cw.fin_year_no and 
                                                                             csm.fin_month_no=cw.fin_month_no) 
                           inner join dim_location dl on (cbi.location_no = dl.location_no)
                           inner join apex_app_cust_01.apex_csats_location cl on (cbi.location_no = cl.location_no)
                           inner join (select /*+Parallel(piv,8) */
                                              primary_customer_identifier,
                                              fin_year_no,
                                              fin_month_no,
                                              max(foods_value_segment_no) foods_value_segment_no, 
                                              max(non_foods_value_segment_no) non_foods_value_segment_no 
                                         from (select /*+Parallel(8) */
                                                      primary_customer_identifier,
                                                      fin_year_no,
                                                      fin_month_no,
                                                      foods_value_segment_no,
                                                      non_foods_value_segment_no 
                                                 from dwh_cust_performance.cust_csm_value_segment 
                                                 pivot( max(current_seg)  
                                                       for food_non_food in (''FSHV'' as "FOODS_VALUE_SEGMENT_NO", ''NFSHV'' as "NON_FOODS_VALUE_SEGMENT_NO"  )) 
                                                      ) piv 
                                                group by primary_customer_identifier,fin_year_no,fin_month_no) cs on (cbi.primary_customer_identifier = cs.primary_customer_identifier and
                                                                                                                      csm.fin_year_no = cs.fin_year_no and
                                                                                                                      csm.fin_month_no = cs.fin_month_no)
                           where cbi.tran_date between '''||g_tran_start_date||''' and '''||g_tran_end_date||''' 
                            '||case when length(g_survey_params) > 0 then ' and '||g_survey_params else '' end||'
                             and cbi.primary_customer_identifier is not null 
                             and cbi.primary_customer_identifier<>0  
                             and cbi.primary_customer_identifier not between 6007851400000000 and 6007851499999999
                             and tm.dm_seq = 1
                             and dcm.is_staff = ''N''
                             and csm.fin_year_no='||g_fin_year ||' 
                             and csm.fin_month_no='||g_fin_month ||' 
                             and csm.csm_period_code='''||g_csm_period||''' 
                             and csm.last_updated_date='''||g_date||''' '||
                                 case when upper(g_channel) = 'E-MAIL' then ' and dcm.man_email_opt_out_ind != 1 and dcm.populated_email_address is not null' end||
                                 case when upper(g_channel) = 'MOBILE' then ' and dcm.man_sms_opt_out_ind != 1 and dcm.populated_cell_no is not null' end||'
                            group by cast('||g_survey_code||' as number),
                                  '''||g_survey_name||''' ,
                                  '''||g_survey_area||''' ,
                                  '''||g_same_csats_survey||''' ,
                                  '''||g_diff_csats_survey||''' ,
                                  '''||g_different_survey||''' ,
                                  dcm.master_subscriber_key,
                                  cbi.primary_customer_identifier,
                                  tm.source_key ,
                                  dcm.identity_document_no,
                                  dcm.populated_email_address,
                                  dcm.work_email_address,
                                  dcm.home_email_address,
                                  dcm.statement_email_address,
                                  dcm.ecommerce_email_address,
                                  dcm.populated_cell_no,
                                  dcm.home_cell_no,
                                  dcm.work_cell_no,
                                  dcm.title,
                                  dcm.first_name,
                                  dcm.last_name,
                                  dcm.derived_race,
                                  dcm.age_band,
                                  '''||g_channel||''' ,
                                  dl.location_no,
                                  dl.location_name,
                                  dl.district_no,
                                  dl.district_name,
                                  dl.region_no,
                                  dl.region_name,
                                  dl.st_s4s_shape_of_chain_desc ,
                                  di.department_no,
                                  di.department_name,
                                  di.subgroup_no,
                                  di.subgroup_name,
                                  di.business_unit_no,
                                  di.group_no,
                                  di.group_name,
                                  di.business_unit_name,
                                  cbi.tran_date,
                                  cbi.till_no,
                                  cw.month_tier,
                                  csm.csm_shopping_habit_segment_no,
                                  cs.foods_value_segment_no,
                                  cs.non_foods_value_segment_no,
                                  '''||nvl(g_survey_name_reformatted,'Derived')||''',
                                  '''||nvl(g_merchandise_classification,'Derived')||''',
                                  '''||g_survey_type||''',
                                  to_char(tran_date,''Day,DD Month YYYY'') ,
                                  cl.csats_location_name,
                                  trunc(sysdate) ,
                                  trunc(sysdate) ,
                                  q''['||g_survey_params||']'' ,
                                  q''['||g_all_survey_params||']''  
                           having sum(cbi.item_tran_selling-cbi.discount_selling) >= '||g_minimum_spend_criteria||' 
                              and sum(cbi.item_tran_qty) >= '||g_minimum_units_criteria||'
                           )';

   
  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
end build_csats_survey_sql;

--**************************************************************************************************
-- Retail and Online Call Centre has a different dataset as such it needs to be handled differently
--**************************************************************************************************
procedure build_callc_survey_sql as
begin

      g_stmt :=' insert /*+ Append */ into dwh_cust_performance.temp_csats_basket_item 
                        (survey_code,
                         survey_name,
                         survey_area,
                         same_csats_survey_eligibility,
                         diff_csats_survey_eligibility,
                         different_survey_eligibility,
                         master_subscriber_key,
                         primary_customer_identifier,
                         customer_no,
                         identity_document_no,
                         populated_email_address,
                         work_email_address,
                         home_email_address,
                         statement_email_address,
                         ecommerce_email_address,
                         populated_cell_no,
                         home_cell_no,
                         work_cell_no,
                         title,
                         first_name,
                         last_name,
                         derived_race,
                         age_band,
                         channel,
                         location_no, 
                         location_name,
                         district_no,
                         district_name,
                         region_no,
                         region_name,
                         st_store_type,
                         department_no,  
                         department_name,
                         subgroup_no,
                         subgroup_name,
                         group_no,
                         group_name,
                         business_unit_no,
                         business_unit_name,
                         tran_date,
                         tran_date_reformatted,
                         status_desc, 
                         category, 
                         tier1, 
                         tier2, 
                         tier3, 
                         detail, 
                         interaction_no, 
                         inquiry_no, 
                         inq_channel,
                         logged_by,  
                         logged_date, 
                         closed_date, 
                         owner_user,
                         call_center,
                         extracted_date,
                         last_updated_date,
                         survey_params,
                         all_survey_params)
                    with all_rows as (
                                        select /*+ Parallel(a,6) Parallel(b,6) Parallel(c,6) Parallel(d,6) Parallel(e,6) Parallel(f,6) Parallel(g,6) Parallel(h,6) Parallel(i,6) Parallel(j,6)*/
                                               distinct cast('||g_survey_code||' as number) survey_code,
                                               '''||g_survey_name||''' survey_name,
                                               '''||g_survey_area||''' survey_area,
                                               '''||g_same_csats_survey||''' same_csats_survey_eligibility,
                                               '''||g_diff_csats_survey||''' diff_csats_survey_eligibility,
                                               '''||g_different_survey||''' different_survey_eligibility,
                                               a.account_contact_id as primary_customer_identifier, 
                                               a.account_contact_id as customer_no,
                                               '''||g_channel||''' channel,
                                               i.location_no, 
                                               i.location_name,
                                               i.district_no,
                                               i.district_name,
                                               i.region_no,
                                               i.region_name,
                                               i.st_store_type,
                                               j.department_no,  
                                               j.department_name,
                                               j.subgroup_no,
                                               j.subgroup_name,
                                               j.group_no,
                                               j.group_name,
                                               j.business_unit_no,
                                               j.business_unit_name,
                                               f.closed_date tran_date,
                                               to_char(f.closed_date,''Day,DD Month YYYY'') tran_date_reformatted,
                                               a.status_desc, 
                                               b.cl_inq_category_desc as category, 
                                               c.cl_inq_feedback_desc as tier1, 
                                               d.cl_inq_type_cat_desc as tier2, 
                                               e.cl_inq_sub_cat_desc  as tier3, 
                                               f.inq_details as detail, 
                                               f.interaction_no, 
                                               f.inquiry_no, 
                                               g.channel_inbound_desc as inq_channel, 
                                               h.cl_user_name as logged_by, 
                                               f.logged_date, 
                                               f.closed_date, 
                                               h.cl_user_name as owner_user,
                                               f.inquiry_bus_area as call_center,
                                               trunc(sysdate) extracted_date,
                                               trunc(sysdate) last_updated_date,
                                               q''['||g_survey_params||']'' survey_params,
                                               q''['||g_all_survey_params||']'' all_survey_params 
                                           from cust_interaction a,
                                                dim_cust_cl_inq_cat b ,
                                                dim_cust_cl_feedback c,
                                                dim_cust_cl_type_cat d,
                                                dim_cust_cl_sub_cat e,
                                                cust_cl_inquiry f,
                                                dim_cust_cl_chanel_inbound g,
                                                dim_cust_cl_user h,
                                                dim_location i,
                                                dim_item j   
                                        where a.inquiry_id=f.inquiry_no
                                          and b.cl_inq_category_no=c.cl_inq_category_no
                                          and b.cl_inq_category_no in (1331,1332,1333,4317) 
                                          and c.cl_inq_feedback_no in (1337,1338,1339,1341,1342,1343,1345,1346,1347,32410,32368,32389,99991331,99991332,99991333)
                                          and c.sk1_cl_inq_feedback_no=d.sk1_cl_inq_feedback_no
                                          and d.sk1_cl_inq_type_cat_no=e.sk1_cl_inq_type_cat_no
                                          and e.sk1_cl_inq_sub_cat_no=f.sk1_cl_inq_sub_cat_no    
                                          and f.closed_date=trunc(sysdate)-2
                                          and g.sk1_channel_inbound_no=f.sk1_channel_inbound_no 
                                          and h.sk1_cl_user_no=f.sk1_logged_by_user_no    
                                          and h.sk1_cl_user_no=f.sk1_owner_user_no  
                                          and i.sk1_location_no=f.sk1_location_no
                                          and j.sk1_item_no=f.sk1_item_no),
                        temp_mapping as (
                                         select /*+ Parallel(dm,8) */
                                                dm.*,
                                                rank() over (partition by dm.source_key, dm.last_updated_date order by dm.last_updated_date desc) dm_seq
                                           from dwh_cust_performance.dim_customer_mapping dm
                                          where dm.source = ''C2''
                                         )
                    select /*+ Parallel(ar,8) Parallel(tm,8) Parallel(dcm,8) */
                           distinct ar.survey_code,
                           ar.survey_name,
                           ar.survey_area,
                           ar.same_csats_survey_eligibility,
                           ar.diff_csats_survey_eligibility,
                           ar.different_survey_eligibility,
                           dcm.master_subscriber_key,
                           ar.primary_customer_identifier,
                           ar.customer_no,
                           dcm.identity_document_no,
                           dcm.populated_email_address,
                           dcm.work_email_address,
                           dcm.home_email_address,
                           dcm.statement_email_address,
                           dcm.ecommerce_email_address,
                           dcm.populated_cell_no,
                           dcm.home_cell_no,
                           dcm.work_cell_no,
                           dcm.title,
                           dcm.first_name,
                           dcm.last_name,
                           dcm.derived_race,
                           dcm.age_band,
                           ar.channel,
                           ar.location_no, 
                           ar.location_name,
                           ar.district_no,
                           ar.district_name,
                           ar.region_no,
                           ar.region_name,
                           ar.st_store_type,
                           ar.department_no,  
                           ar.department_name,
                           ar.subgroup_no,
                           ar.subgroup_name,
                           ar.group_no,
                           ar.group_name,
                           ar.business_unit_no,
                           ar.business_unit_name,
                           ar.tran_date,
                           ar.tran_date_reformatted,
                           ar.status_desc, 
                           ar.category, 
                           ar.tier1, 
                           ar.tier2, 
                           ar.tier3, 
                           ar.detail, 
                           ar.interaction_no, 
                           ar.inquiry_no, 
                           ar.inq_channel, 
                           ar.logged_by, 
                           ar.logged_date, 
                           ar.closed_date, 
                           ar.owner_user,
                           ar.call_center,
                           ar.extracted_date,
                           ar.last_updated_date,
                           ar.survey_params,
                           ar.all_survey_params
                      from all_rows ar
                     inner join temp_mapping tm on (ar.customer_no=tm.source_key)
                     inner join (select /*+ Parallel(dcm,8) Full() */
                                         dcm.master_subscriber_key,
                                         dcm.identity_document_no,
                                         upper(dcm.populated_email_address) populated_email_address,
                                         upper(dcm.work_email_address)  work_email_address,
                                         upper(dcm.home_email_address)  home_email_address,
                                         upper(dcm.statement_email_address) statement_email_address,
                                         upper(dcm.ecommerce_email_address) ecommerce_email_address,
                                         dcm.populated_cell_no,
                                         dcm.home_cell_no,
                                         dcm.work_cell_no,
                                         dcm.title,
                                         dcm.first_name,
                                         dcm.last_name,
                                         '||g_race_case||' derived_race,
                                         '||g_age_case||' age_band,
                                         dcm.ww_man_email_opt_out_ind  man_email_opt_out_ind,
                                         dcm.ww_man_sms_opt_out_ind    man_sms_opt_out_ind,
                                         case when de.id_no is null then ''N'' else ''Y'' end is_staff
                                      from dwh_cust_performance.dim_customer_master dcm
                                      left join dwh_hr_performance.dim_employee de on (to_char(dcm.identity_document_no) = de.id_no)
                                      ) dcm on (tm.subscriber_key = dcm.master_subscriber_key)
                     where dcm.is_staff = ''N''   ' ;
   
  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
end build_callc_survey_sql;
--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 
    execute immediate 'alter session enable parallel dml';

--************************************************************************************************** 
-- All generated SQLs are written to the dwh_cust_performance.temp_survey_sql table
-- To display all DBMS OUTPUT to facilitate debugging, un-comment the next line
--**************************************************************************************************
--    g_debug := true;

--**************************************************************************************************
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;  
    p_success := false;    
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF CUST_SURVEY EX APEX_MKT_CSATS_SUMMARY STARTED AT '||
    to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  

--************************************************************************************************** 
-- Look up period   
--**************************************************************************************************
select today_,
       (end_date-start_date)+1 search_days,
       (online_end_date-online_start_date)+1 online_search_days,
       start_date,end_date,
       online_start_date,
       online_end_date
  into g_runday,g_search_days,g_online_search_days,g_start_date,g_end_date,g_online_start_date,g_online_end_date
  from (
        select to_char(calendar_date,'DAY') TODAY_,
               case
                    when to_char(calendar_date,'D') in (2) then             -- Monday (Friday’s transactions;)
                         calendar_date - 3
                    when to_char(calendar_date,'D') in (3) then             -- Tuesday (Saturday and Sunday’s transactions)
                         calendar_date - 3
                    when to_char(calendar_date,'D') in (4) then             -- Wednesday (Monday and Tuesday’s transactions;)
                         calendar_date - 2
                    when to_char(calendar_date,'D') in (5,6) then           -- Thursday/Friday (Wednesday’s/Thursday’s transactions )
                         calendar_date - 1
               end start_date,
               case
                    when to_char(calendar_date,'D') in (2) then             -- Monday (Friday’s transactions;)
                         calendar_date - 3
                    when to_char(calendar_date,'D') in (3) then             -- Tuesday (Saturday and Sunday’s transactions)
                         calendar_date - 2
                    when to_char(calendar_date,'D') in (4) then             -- Wednesday (Monday and Tuesday’s transactions;)
                         calendar_date - 1
                    when to_char(calendar_date,'D') in (5,6) then           -- Thursday/Friday (Wednesday’s/Thursday’s transactions )
                         calendar_date - 1
               end end_date,
               case
                    when to_char(calendar_date,'D') in (2) then             -- Monday (Friday’s transactions;)
                         calendar_date - 5
                    when to_char(calendar_date,'D') in (3) then             -- Tuesday (Saturday and Sunday’s transactions)
                         calendar_date - 4
                    when to_char(calendar_date,'D') in (4) then             -- Wednesday (Monday and Tuesday’s transactions;)
                         calendar_date - 3
                    when to_char(calendar_date,'D') in (5,6) then           -- Thursday/Friday (Wednesday’s/Thursday’s transactions )
                         calendar_date - 3
               end online_start_date,
               case
                    when to_char(calendar_date,'D') in (2) then             -- Monday (Friday’s transactions;)
                         calendar_date - 4
                    when to_char(calendar_date,'D') in (3) then             -- Tuesday (Saturday and Sunday’s transactions)
                         calendar_date - 3
                    when to_char(calendar_date,'D') in (4) then             -- Wednesday (Monday and Tuesday’s transactions;)
                         calendar_date - 3
                    when to_char(calendar_date,'D') in (5,6) then           -- Thursday/Friday (Wednesday’s/Thursday’s transactions )
                         calendar_date - 3
               end online_end_date
          from dim_calendar where calendar_date = g_rundate
       );

--************************************************************************************************** 
-- This job doesn't run on a Saturday or Sunday.   
--**************************************************************************************************       
     if to_char(g_rundate,'D') in (1,7) then
        l_text := 'This job only runs Monday Thru Friday. '||g_rundate||' is a '||to_char(g_rundate,'Day');
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        p_success := true;
        return;
     end if;

     l_text := 'Processing for '||to_char(g_rundate,'Day')||'. Extract Start Date : '||to_char(g_start_date,'dd/Mon/yyyy')||
               ' - End Date :'||to_char(g_end_date,'dd/Mon/yyyy')||'. Search Days : '||g_search_days;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     l_text := '                          Online Start Date : '||to_char(g_online_start_date,'dd/Mon/yyyy')||
               ' - End Date :'||to_char(g_online_end_date,'dd/Mon/yyyy')||'. Search Days : '||g_online_search_days;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--************************************************************************************************** 
-- Get CSM period and max year and week  
--**************************************************************************************************
    select /*+ Parallel(csh,16) */ max(fin_year_no),max(fin_month_no),csm_period_code,last_updated_date
      into g_fin_year,g_fin_month,g_csm_period,g_date
      from cust_csm_shopping_habits csh where last_updated_date = (
           select /*+ Parallel(sh,16) */ max(last_updated_date) 
             from  cust_csm_shopping_habits sh)
     group by csm_period_code,last_updated_date;    

--************************************************************************************************** 
-- Due to limitations on number of inner joins we've formulated the decode statement for race
-- This needs only be done once then it can be used in the main query.
-- Change to mitigate ORACLE version difference between prod and uat
--**************************************************************************************************    

       select 'decode(upper(dcm.derived_race), '||WM_CONCAT(race_name)||',0)'
         into g_race_case
         from (select race_no,''''||upper(race_name)||''','||race_no race_name from apex_app_cust_01.derived_race);

       select replace('case '||WM_CONCAT(case_stmt)||' end',',','')  
         into g_age_case
         from (select ' when dcm.age >= '||minimum_age||' and dcm.age <= '||maximum_age||' then '||age_band case_stmt 
                 from apex_app_cust_01.apex_age_band);

--************************************************************************************************** 
-- Create the survey master records.
-- Change to mitigate version difference between prod and uat
--**************************************************************************************************
    l_text := 'Clean all temporary tables incl. CUST_CSATS_SURVEY_DETAILS';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_cust_performance.temp_survey_params';

    execute immediate 'truncate table dwh_cust_performance.temp_survey_sql';
    
    execute immediate 'delete from dwh_cust_performance.cust_csats_survey_details where last_updated_date = '''||g_rundate||''' ';
    g_recs_deleted := sql%rowcount;
    commit;

--**************************************************************************************************
-- The table apex is the driving parameter tables for this program.
-- This table pivots and makes the summary and detail tables usable in a FOR Loop. 
--**************************************************************************************************

    insert /*+ Append */ into dwh_cust_performance.temp_survey_params
        select /*+ Parallel(aps,8) */
               aps.survey_code,
               aps.survey_name,
               aps.survey_area,
               aps.minimum_spend_criteria,
               aps.minimum_units_criteria,
               aps.priority_order,
               aps.quota_minimum,
               aps.quota_maximum,
               aps.last_updated_date,
               aps.updated_by,
               aps.created_date,
               aps.created_by,
               aps.modified_ind,
               aps.survey_status,
               aps.same_csats_survey_eligibility,
               aps.diff_csats_survey_eligibility,
               aps.different_survey_eligibility,
               aps.channel,
               aps.survey_name_reformatted,
               aps.merchandise_classification,
               aps.survey_type,
               case when upper(aps.survey_type) not like '%WEBSITE' then
                         g_start_date
                    else
                         g_online_start_date
               end tran_start_date,
               case when upper(aps.survey_type) not like '%WEBSITE' then
                         g_end_date
                    else
                         g_online_end_date
               end tran_end_date,
               app.survey_params,
               app.all_survey_params
          from apex_app_cust_01.apex_mkt_csats_summary aps
         left join (
                     select survey_code,
                               cast(substr(survey_params,5,length(survey_params)) as varchar(4000)) survey_params,
                               cast(substr(all_survey_params,5,length(all_survey_params)) as varchar(4000)) all_survey_params        
                         from (
                               select survey_code,replace(WM_CONCAT(concat( 'AND ',params0)),',AND','AND') survey_params,replace(WM_CONCAT(concat( 'AND ',params)),',AND','AND') all_survey_params from (
                               select survey_code,
                                      case when includ = 1 then
                                           case 
                                                when product_location_other_desc in ('ITEM_NO','ITEM_LEVEL1_NO','BUSINESS_UNIT_NO','GROUP_NO','SUBGROUP_NO','DEPARTMENT_NO')  then 'DI.'||product_location_other_desc
                                                when product_location_other_desc in ('AREA_NO','REGION_NO','DISTRICT_NO') then 'DL.'||product_location_other_desc
                                                when product_location_other_desc in ('TILL_NO','LOCATION_NO') then 'CBI.'||product_location_other_desc
                                                when product_location_other_desc in ('AGE_BAND','DERIVED_RACE')  then 'DCM.'||product_location_other_desc
                                                when product_location_other_desc = 'MONTH_TIER' then 'CW.'||product_location_other_desc
                                                when product_location_other_desc = 'TRAN_TYPE_NO' then 'CBI.TRAN_TYPE'
                                                when product_location_other_desc = 'SHOPPING_HABIT_SEGMENT_NO' then 'CSM.CSM_'||product_location_other_desc
                                                else product_location_other_desc 
                                           end ||include_exclude_ind||'('||case when product_location_other_desc = 'TRAN_TYPE_NO' then 
                                                                                replace(replace(replace(replace(concatdata,'1','''S'''),2,'''V'''),3,'''R'''),4,'''Q''') 
                                                                           else 
                                                                                concatdata 
                                                                           end||') ' 
                                      else null 
                                      end params0,
                                      case when product_location_other_desc = 'TRAN_TYPE_NO' then 
                                               'TRAN_TYPE' 
                                           else product_location_other_desc 
                                      end||include_exclude_ind||'('||case when product_location_other_desc = 'TRAN_TYPE_NO' then 
                                                                          replace(replace(replace(replace(concatdata,'1','''S'''),2,'''V'''),3,'''R'''),4,'''Q''') 
                                                                     else 
                                                                          concatdata 
                                                                     end||') ' params
                                  from (
                                        select /*+ Full(apd) */ 
                                               apd.survey_code,apd.product_location_other_desc,
                                               case include_exclude_ind when 'INCLUDE' then ' IN ' else ' NOT IN ' end include_exclude_ind,
                                               WM_CONCAT(product_location_other_no) concatdata,
                                               rank() over (partition by apd.survey_code,product_location_other_desc order by include_exclude_ind desc) includ
                                          from apex_app_cust_01.apex_mkt_csats_summary aps
                                        inner join apex_app_cust_01.apex_mkt_csats_detail apd on (aps.survey_code = apd.survey_code)
                                        where upper(aps.survey_status) like 'ACTIVE%' 
                                        group by apd.survey_code,apd.product_location_other_desc,include_exclude_ind
                                       ))group by survey_code
                                ) 

                    ) app on (aps.survey_code = app.survey_code)
                     where  upper(aps.survey_status) like 'ACTIVE%' ;
--                     where ( upper(aps.survey_status) like 'ACTIVE%' or aps.survey_code in (46,47)) ;   Testing Call Centre Surveys

    commit;

--************************************************************************************************** 
-- 1. Scroll through qualifying surveys
-- 2. Get the where clause.
-- 3. Build the Main SQL
-- * Apologies for the convoluted SQL. It's due to string limitations and the various permutations
--   The survey could present.
--**************************************************************************************************

    for c_survey in (select * from dwh_cust_performance.temp_survey_params order by priority_order)
    loop            -- Main LOOP
        execute immediate 'truncate table dwh_cust_performance.temp_csats_basket_item';
        execute immediate 'truncate table dwh_cust_performance.temp_csats_survey_details';    
--        dbms_output.put_line('Processing Survey '||c_survey.survey_code);
        commit;
        
        g_survey_code	                :=	c_survey.survey_code;
        g_survey_name	                :=	c_survey.survey_name;
        g_survey_area	                :=	c_survey.survey_area;
        g_minimum_spend_criteria	    :=	c_survey.minimum_spend_criteria;
        g_minimum_units_criteria	    :=	c_survey.minimum_units_criteria;
        g_priority_order	            :=	c_survey.priority_order;
        g_quota_minimum	                :=	c_survey.quota_minimum;
        g_quota_maximum	                :=	c_survey.quota_maximum;
        g_last_updated_date	            :=	c_survey.last_updated_date;
        g_updated_by	                :=	c_survey.updated_by;
        g_created_date	                :=	c_survey.created_date;
        g_created_by	                :=	c_survey.created_by;
        g_modified_ind	                :=	c_survey.modified_ind;
        g_survey_status	                :=	c_survey.survey_status;
        g_same_csats_survey	            :=	c_survey.same_csats_survey_eligibility;
        g_diff_csats_survey	            :=	c_survey.diff_csats_survey_eligibility;
        g_different_survey	            :=	c_survey.different_survey_eligibility;
        g_channel	                    :=	c_survey.channel;
        g_survey_name_reformatted	    :=	c_survey.survey_name_reformatted;
        g_merchandise_classification	:=	c_survey.merchandise_classification;
        g_survey_type	                :=	c_survey.survey_type;
        g_tran_start_date	            :=	c_survey.tran_start_date;
        g_tran_end_date	                :=	c_survey.tran_end_date;
        g_survey_params	                :=	c_survey.survey_params;
        g_all_survey_params	            :=	c_survey.all_survey_params;
        
        g_setup_sql :=1;
        case
             when upper(c_survey.survey_name) not like '%CALL CENTRE%' then
                  build_csats_survey_sql;
            else
                  build_callc_survey_sql;
        end case;
        
        insert into dwh_cust_performance.temp_survey_sql values (g_sql_log_date,c_survey.survey_code,g_setup_sql,g_stmt);
        commit;

        execute immediate g_stmt;
        g_recs_updated := sql%rowcount;
        commit;
--************************************************************************************************** 
-- Do we have the minimum number of records.
-- If not then fetch the next survey otherwise continue with data insert. 
-- 
--************************************************************************************************** 
        
        if g_recs_updated <= c_survey.quota_minimum then  
           l_text := '* Minimum count not met. Survey : '||c_survey.survey_code||
                     ' - Name : '||c_survey.survey_name||
                     ' - Area : '||c_survey.survey_area||
                     ' - Rows : '||g_recs_updated;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
        else   
        
           g_setup_sql :=2;
           g_stmt :=' insert /*+ Append Parallel(8) */ into dwh_cust_performance.temp_csats_survey_details 
                        select /*+ Parallel(bi,8)*/ distinct *
                          from (select /*+ Parallel(cb,8) Parallel(cc,8) Parallel(oo,8)*/ distinct 
                                       cb.survey_code,
                                       cb.survey_name,
                                       cb.survey_area,
                                       cb.master_subscriber_key,
                                       cb.primary_customer_identifier,
                                       cb.customer_no,
                                       cb.identity_document_no,
                                       cb.populated_email_address,
                                       cb.work_email_address,
                                       cb.home_email_address,
                                       cb.statement_email_address,
                                       cb.ecommerce_email_address,
                                       cb.populated_cell_no,
                                       cb.home_cell_no,
                                       cb.work_cell_no,
                                       cb.title,
                                       cb.first_name,
                                       cb.last_name,
                                       cb.derived_race,
                                       cb.age_band,
                                       cb.channel,
                                       cb.location_no,
                                       cb.location_name,
                                       cb.district_no,
                                       cb.district_name,
                                       cb.region_no,
                                       cb.region_name,
                                       cb.st_store_type,
                                       cb.department_no,
                                       cb.department_name,
                                       cb.subgroup_no,
                                       cb.subgroup_name,
                                       cb.group_no,
                                       cb.group_name,
                                       cb.business_unit_no,
                                       cb.business_unit_name,
                                       cb.tran_date,
                                       cb.till_no,
                                       cb.item_qty,
                                       cb.selling_value,
                                       cb.month_tier,
                                       cb.csm_shopping_habit_segment_no,
                                       cb.foods_value_segment_no,
                                       cb.non_foods_value_segment_no,
                                       cb.survey_name_reformatted,
                                       cb.merchandise_classification,
                                       cb.survey_type,
                                       cb.tran_date_reformatted,
                                       cb.location_name_reformatted,
                                       cb.status_desc,
                                       cb.category,
                                       cb.tier1,
                                       cb.tier2,
                                       cb.tier3,
                                       cb.detail,
                                       cb.interaction_no,
                                       cb.inquiry_no,
                                       cb.inq_channel,
                                       cb.logged_by,
                                       cb.logged_date,
                                       cb.closed_date,
                                       cb.owner_user,
                                       cb.call_center,
                                       cb.extracted_date,
                                       cb.last_updated_date,
                                       cb.survey_params,
                                       cb.all_survey_params
                                  from dwh_cust_performance.temp_csats_basket_item cb
                                  left join (select /*+ Parallel */ 
                                                    master_subscriber_key,
                                                    min(same_csats_survey_days) same_csats_survey_days,
                                                    min(diff_csats_survey_days) diff_csats_survey_days,
                                                    min(different_survey_days) different_survey_days
                                               from (
                                                     select /*+ Parallel(csd,8) Parallel(anc,8) */ 
                                                            csd.master_subscriber_key, 
                                                            trunc(sysdate) - nvl(max(case when csd.survey_code = '||g_survey_code||' then csd.last_updated_date end),trunc(sysdate-('||g_same_csats_survey||'+1))) same_csats_survey_days,
                                                            trunc(sysdate) - nvl(max(case when csd.survey_code <> '||g_survey_code||' then csd.last_updated_date end),trunc(sysdate-('||g_diff_csats_survey||'+1))) diff_csats_survey_days,
                                                            trunc(sysdate) - nvl(max(anc.last_updated_date),trunc(sysdate-('||g_different_survey||'+1))) different_survey_days
                                                       from dwh_cust_performance.cust_csats_survey_details csd
                                                       left join apex_app_cust_01.apex_non_csats_survey anc on (csd.master_subscriber_key = anc.subscriber_key)
                                                       group by csd.master_subscriber_key,survey_code
                                                    )
                                               group by master_subscriber_key) cc on (cb.master_subscriber_key = cc.master_subscriber_key) 
                         left join (select /*+ Parallel(ao,8)  */ 
                                           distinct ''Y'' opt_out_true,opt_out_subscriber_key,opt_out_identity_document_no,opt_out_email_address,opt_out_mobile_no 
                                      from apex_app_cust_01.apex_mkt_csats_opt_out ao
                                   ) oo on (cb.master_subscriber_key = oo.opt_out_subscriber_key or 
                                            cb.identity_document_no = oo.opt_out_identity_document_no or 
                                            cb.populated_email_address = upper(oo.opt_out_email_address) or 
                                            cb.work_email_address = upper(oo.opt_out_email_address) or 
                                            cb.home_email_address = upper(oo.opt_out_email_address) or 
                                            cb.statement_email_address = upper(oo.opt_out_email_address) or 
                                            cb.ecommerce_email_address = upper(oo.opt_out_email_address) or 
                                            cb.populated_cell_no = opt_out_mobile_no or 
                                            cb.home_cell_no = opt_out_mobile_no or 
                                            cb.work_cell_no = opt_out_mobile_no) 
                              where (cb.same_csats_survey_eligibility < cc.same_csats_survey_days
                                and  cb.diff_csats_survey_eligibility < cc.diff_csats_survey_days
                                and  cb.different_survey_eligibility  < cc.different_survey_days)         
                                and nvl(oo.opt_out_true,''N'') = ''N'' 
                              order by cb.selling_value desc,
                                    cb.item_qty desc) bi 
                        where master_subscriber_key not in (select master_subscriber_key 
                                                              from cust_csats_survey_details 
                                                             where last_updated_date = trunc(sysdate)) ';
        
           insert into dwh_cust_performance.temp_survey_sql values (g_sql_log_date,c_survey.survey_code,g_setup_sql,g_stmt);
           commit;
                execute immediate g_stmt;
           g_recs_read := g_recs_read + sql%rowcount;
           g_recs_in_temp := sql%rowcount;
           commit;
           
--**************************************************************************************************
-- Prep the data for the final csat_survey_detail insert.
-- 
--**************************************************************************************************
--           dbms_output.put_line('Start of Insert '||c_survey.survey_code);
           g_recs_loaded := 0;
           for c_insert in (
                    with all_rows as
                                  (
                                   select /*+ Parallel(td,8)*/
                                          td.*, 
                                          row_number() over  (PARTITION BY master_subscriber_key  order by master_subscriber_key ) rec_id
                                    from dwh_cust_performance.temp_csats_survey_details  td  
                                  ),
                      unique_rows as
                                  (
                                   select /*+ Parallel(ar,8) */ 
                                          survey_code,
                                          survey_name,
                                          survey_area,
                                          master_subscriber_key,
                                          primary_customer_identifier,
                                          customer_no,
                                          populated_email_address,
                                          populated_cell_no,
                                          title,
                                          first_name,
                                          last_name,
                                          derived_race,
                                          age_band,
                                          channel,
                                          location_no,
                                          location_name,
                                          district_no,
                                          district_name,
                                          region_no,
                                          region_name,
                                          st_store_type,
                                          subgroup_no,
                                          subgroup_name,
                                          group_no,
                                          group_name,
                                          business_unit_no,
                                          business_unit_name,
                                          tran_date,
                                          till_no,
                                          item_qty,
                                          selling_value,
                                          month_tier,
                                          csm_shopping_habit_segment_no shopping_habit_segment_no,
                                          foods_value_segment_no,
                                          non_foods_value_segment_no,
                                          survey_name_reformatted,
                                          merchandise_classification,
                                          survey_type,
                                          tran_date_reformatted,
                                          location_name_reformatted,
                                          status_desc,
                                          category,
                                          tier1,
                                          tier2,
                                          tier3,
                                          detail,
                                          interaction_no,
                                          inquiry_no,
                                          logged_by,
                                          logged_date,
                                          closed_date,
                                          owner_user,
                                          call_center,
                                          extracted_date,
                                          last_updated_date,
                                          survey_params,
                                          all_survey_params
                                     from all_rows ar
                                    where rec_id = 1
                                  ),
                         sum_rows as
                                  (
                                   select /*+ Parallel(ar,8) */
                                          master_subscriber_key,
                                          location_no,
                                          tran_date,
                                          till_no,
                                          sum(item_qty) item_qty,
                                          sum(selling_value) selling_value
                                     from all_rows ar
                                    group by master_subscriber_key,
                                          location_no,
                                          tran_date,
                                          till_no
                                    )
                    select 
                           ur.survey_code,
                           ur.survey_name,
                           ur.survey_area,
                           ur.master_subscriber_key,
                           ur.primary_customer_identifier,
                           ur.customer_no,
                           ur.populated_email_address,
                           ur.populated_cell_no,
                           ur.title,
                           ur.first_name,
                           ur.last_name,
                           ur.derived_race,
                           ur.age_band,
                           ur.channel,
                           ur.location_no,
                           ur.location_name,
                           ur.district_no,
                           ur.district_name,
                           ur.region_no,
                           ur.region_name,
                           ur.st_store_type,
                           ur.subgroup_no,
                           ur.subgroup_name,
                           ur.group_no,
                           ur.group_name,
                           ur.business_unit_no,
                           ur.business_unit_name,
                           ur.tran_date,
                           ur.till_no,
                           sr.item_qty,
                           sr.selling_value,
                           ur.month_tier,
                           ur.shopping_habit_segment_no,
                           ur.foods_value_segment_no,
                           ur.non_foods_value_segment_no,
                           ur.survey_name_reformatted,
                           ur.merchandise_classification,
                           ur.survey_type,
                           ur.tran_date_reformatted,
                           ur.location_name_reformatted,
                           ur.status_desc,
                           ur.category,
                           ur.tier1,
                           ur.tier2,
                           ur.tier3,
                           ur.detail,
                           ur.interaction_no,
                           ur.inquiry_no,
                           ur.logged_by,
                           ur.logged_date,
                           ur.closed_date,
                           ur.owner_user,
                           ur.call_center,
                           ur.extracted_date,
                           ur.last_updated_date,
                           ur.survey_params,
                           ur.all_survey_params
                      from unique_rows ur
                     inner join sum_rows sr on ( ur.master_subscriber_key = sr.master_subscriber_key and
                                                 ur.location_no = sr.location_no and
                                                 ur.tran_date = sr.tran_date and
                                                 ur.till_no = sr.till_no)
                     order by sr.selling_value desc, sr.item_qty desc) 
           loop
    
                insert /*+ Append */ into dwh_cust_performance.cust_csats_survey_details (survey_code,
                                                                                          survey_name,
                                                                                          survey_area,
                                                                                          master_subscriber_key,
                                                                                          primary_customer_identifier,
                                                                                          customer_no,
                                                                                          populated_email_address,
                                                                                          populated_cell_no,
                                                                                          title,
                                                                                          first_name,
                                                                                          last_name,
                                                                                          derived_race,
                                                                                          age_band,
                                                                                          channel,
                                                                                          location_no,
                                                                                          location_name,
                                                                                          district_no,
                                                                                          district_name,
                                                                                          region_no,
                                                                                          region_name,
                                                                                          st_store_type,
                                                                                          subgroup_no,
                                                                                          subgroup_name,
                                                                                          group_no,
                                                                                          group_name,
                                                                                          business_unit_no,
                                                                                          business_unit_name,
                                                                                          tran_date,
                                                                                          till_no,
                                                                                          item_qty,
                                                                                          selling_value,
                                                                                          month_tier,
                                                                                          shopping_habit_segment_no,
                                                                                          foods_value_segment_no,
                                                                                          non_foods_value_segment_no,
                                                                                          survey_name_reformatted,
                                                                                          merchandise_classification,
                                                                                          survey_type,
                                                                                          tran_date_reformatted,
                                                                                          location_name_reformatted,
                                                                                          status_desc,
                                                                                          category,
                                                                                          tier1,
                                                                                          tier2,
                                                                                          tier3,
                                                                                          detail,
                                                                                          interaction_no,
                                                                                          inquiry_no,
                                                                                          logged_by,
                                                                                          logged_date,
                                                                                          closed_date,
                                                                                          owner_user,
                                                                                          call_center,
                                                                                          extracted_date,
                                                                                          last_updated_date,
                                                                                          survey_params,
                                                                                          all_survey_params) 
                    values (c_insert.survey_code,
                            c_insert.survey_name,
                            c_insert.survey_area,
                            c_insert.master_subscriber_key,
                            c_insert.primary_customer_identifier,
                            c_insert.customer_no,
                            c_insert.populated_email_address,
                            c_insert.populated_cell_no,
                            c_insert.title,
                            c_insert.first_name,
                            c_insert.last_name,
                            c_insert.derived_race,
                            c_insert.age_band,
                            c_insert.channel,
                            c_insert.location_no,
                            c_insert.location_name,
                            c_insert.district_no,
                            c_insert.district_name,
                            c_insert.region_no,
                            c_insert.region_name,
                            c_insert.st_store_type,
                            c_insert.subgroup_no,
                            c_insert.subgroup_name,
                            c_insert.group_no,
                            c_insert.group_name,
                            c_insert.business_unit_no,
                            c_insert.business_unit_name,
                            c_insert.tran_date,
                            c_insert.till_no,
                            c_insert.item_qty,
                            c_insert.selling_value,
                            c_insert.month_tier,
                            c_insert.shopping_habit_segment_no,
                            c_insert.foods_value_segment_no,
                            c_insert.non_foods_value_segment_no,
                            c_insert.survey_name_reformatted,
                            c_insert.merchandise_classification,
                            c_insert.survey_type,
                            c_insert.tran_date_reformatted,
                            c_insert.location_name_reformatted,
                            c_insert.status_desc,
                            c_insert.category,
                            c_insert.tier1,
                            c_insert.tier2,
                            c_insert.tier3,
                            c_insert.detail,
                            c_insert.interaction_no,
                            c_insert.inquiry_no,
                            c_insert.logged_by,
                            c_insert.logged_date,
                            c_insert.closed_date,
                            c_insert.owner_user,
                            c_insert.call_center,
                            c_insert.extracted_date,
                            c_insert.last_updated_date,
                            c_insert.survey_params,
                            c_insert.all_survey_params);
                      
                         g_recs_inserted := g_recs_inserted + sql%rowcount;
                         g_recs_loaded := g_recs_loaded + 1;
                         commit;
                         exit when g_recs_loaded >= (c_survey.quota_maximum*(case when upper(c_survey.survey_type) not like '%WEBSITE' then 
                                                                                       g_search_days 
                                                                                  else g_online_search_days 
                                                                             end));
                     
           end loop;
    
               l_text := ' Survey : '||to_char(c_survey.survey_code,'99')||
                         ' - Name : '||rpad(c_survey.survey_name,34,' ')||
                         ' - Area : '||rpad(c_survey.survey_area,10,' ')||
                         ' - Rows : '||g_recs_loaded||'/'||g_recs_in_temp||
                         ' For '||c_survey.quota_maximum;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);          
               commit;
        end if;  
    end loop;       -- End Main Loop
    
    l_text := 'Updating Statistics...';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','CUST_CSATS_SURVEY_DETAIL',estimate_percent=>0.1, DEGREE => 16);

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

end WH_PRF_CUST_400UB;
