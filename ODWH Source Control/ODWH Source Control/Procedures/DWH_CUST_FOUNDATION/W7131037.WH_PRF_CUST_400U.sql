-- ****** Object: Procedure W7131037.WH_PRF_CUST_400U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_400U" (p_forall_limit in integer,p_success out boolean,p_date in date) AS

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
--                      - apex_mkt_csats_detail
--                      - TEMP_ALL_PARAMS
--                      - TEMP_CSATS_SURVEY_DETAIL
--                      - TEMP_SURVEY_MASTER
--                      -
--               Output - cust_csats_survey_detail
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_forall_limit       number         := 10000;
g_fin_year           number;
g_fin_month          number;
g_survey_code        number         := 0;
g_day_of_week        number         := 0;

g_where_clause       varchar2(4000) := null;
g_and_clause         varchar2(4000) := null;
g_cols               varchar2(4000) := null;
g_csm_period         varchar2(40);
g_race_case          varchar2(4000);


g_date               date         := NVL(p_date,trunc(sysdate));
g_start_date         date         := NVL(p_date,trunc(sysdate)-2);
g_end_date           date         := NVL(p_date,trunc(sysdate)-1);



g_stmt                varchar2(4000);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_400U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST_SURVEY EX CUST_BASKET_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';

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

    case
         when to_char(trunc(sysdate),'D') in (2) then      -- Monday (Friday’s transactions;)
              g_start_date  := g_start_date - 3;
              g_end_date    := g_end_date - 3;
         when to_char(trunc(sysdate),'D') in (3) then      -- Tuesday (Saturday and Sunday’s transactions)
              g_start_date  := g_start_date - 3;
              g_end_date    := g_end_date - 2;
         when to_char(trunc(sysdate),'D') in (4) then      -- Wednesday (Monday and Tuesday’s transactions;)
              g_start_date  := g_start_date - 2;
              g_end_date    := g_end_date - 1;
         when to_char(trunc(sysdate),'D') in (5,6) then    -- Thursday/Friday (Wednesday’s/Thursday’s transactions )
              g_start_date  := g_start_date - 1;
              g_end_date    := g_end_date - 1;
         else                                               --Saturday and Sunday
              l_text := 'This job only runs Monday Thru Friday. Today is a '||to_char(trunc(sysdate),'Day');
              dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         p_success := true;
         return;
     end case;

     l_text := 'Processing for '||to_char(trunc(sysdate),'Day')||'. Extract Start Date:'||to_char(g_start_date,'dd/Mon/yyyy')||
               ' - End Date :'||to_char(g_end_date,'dd/Mon/yyyy');
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Get CSM period and max year and week
--**************************************************************************************************
    select /*+ Parallel(csh,16) */ max(fin_year_no),max(fin_month_no),csm_period_code,last_updated_date
      into g_fin_year,g_fin_month,g_csm_period,g_date
      from cust_csm_shopping_habits@dwhprd csh where last_updated_date = (
           select /*+ Parallel(sh,16) */ max(last_updated_date)
             from  cust_csm_shopping_habits@dwhprd sh)
     group by csm_period_code,last_updated_date;
--**************************************************************************************************
-- Pivot and list as columns all known product_location_other_desc options
-- This is done in this fashion as there is no table in APEX than lists these.
--**************************************************************************************************
    select listagg('''' || colname || ''' as "' || colname || '"', ',') within group (order by colname)
      into g_cols
      from  (
             select 'AGE_BAND' colname from dual union
             select 'AREA_NO' colname from dual union
             select 'BUSINESS_UNIT_NO' colname from dual union
             select 'DEPARTMENT_NO' colname from dual union
             select 'DERIVED_RACE' colname from dual union
             select 'DISTRICT_NO' colname from dual union
             select 'FOODS_VALUE_SEGMENT_NO' colname from dual union
             select 'GROUP_NO' colname from dual union
             select 'INCLUDE' colname from dual union
             select 'LANGUAGE_PREFERENCE' colname from dual union
             select 'LOCATION_NO' colname from dual union
             select 'LOYALTY_TIER_NO' colname from dual union
             select 'MINIMUM_SPEND_CRITERIA' colname from dual union
             select 'NON_FOODS_VALUE_SEGMENT_NO' colname from dual union
             select 'REGION_NO' colname from dual union
             select 'ROA_IND' colname from dual union
             select 'SHOPPING_HABIT_SEGMENT_NO' colname from dual union
             select 'SUBGROUP_NO' colname from dual union
             select 'TILL_NO' from dual
             );
--**************************************************************************************************
-- Due to limitations on number of inner joins we've formulated the decode statement for race
-- This needs only be done once then it can be used in the main query.
--**************************************************************************************************
      select 'decode(upper(dcm.derived_race), '||listagg(race_name,',' ) within group (order by race_no)||' ,0)'
        into g_race_case
        from (select race_no,''''||upper(race_name)||''' , '||race_no race_name from derived_race);

--**************************************************************************************************
-- Create the survey master records.
--**************************************************************************************************
    execute immediate 'truncate table temp_all_params';
    execute immediate 'truncate table temp_survey_master';

    commit;

    g_stmt := ' insert into temp_all_params
                select survey_code,
                      listagg(params0, ''AND '') within group (order by survey_code) survey_params ,
                      listagg(params,'' AND '') within group (order by survey_code)  all_survey_params
                 from (select survey_code,
                              case when includ = 1 then
                              case
                                   when product_location_other_desc in (''BUSINESS_UNIT_NO'',''GROUP_NO'',''SUBGROUP_NO'',''DEPARTMENT_NO'')  then ''DI.''||product_location_other_desc
                                   when product_location_other_desc in (''AREA_NO'',''REGION_NO'',''DISTRICT_NO'') then ''DL.''||product_location_other_desc
                                   when product_location_other_desc in (''TILL_NO'',''LOCATION_NO'') then ''CBI.''||product_location_other_desc
                                   when product_location_other_desc in (''AGE_BAND'',''DERIVED_RACE'')  then ''CM.''||product_location_other_desc
                                   when product_location_other_desc = ''LOYALTY_TIER_NO'' then ''CW.''||product_location_other_desc
                                   when product_location_other_desc = ''SHOPPING_HABIT_SEGMENT_NO'' then ''CSM.''||product_location_other_desc
                                   else product_location_other_desc end ||include_exclude_ind||''(''||concatdata||'') '' else null end params0,
                              product_location_other_desc||include_exclude_ind||''(''||concatdata||'') '' params
                         from (select /*+ Full(apd) */ apd.survey_code,apd.product_location_other_desc,
                                      case include_exclude_ind when ''INCLUDE'' then '' IN '' else '' NOT IN '' end include_exclude_ind,
                                      listagg(product_location_other_no,'','') within group (order by product_location_other_no) concatdata,
                                      row_number() over (partition by product_location_other_desc order by include_exclude_ind desc) includ
                                 from apex_app_cust_01.apex_mkt_csats_summary aps
                                inner join apex_app_cust_01.apex_mkt_csats_detail apd on (aps.survey_code = apd.survey_code)
                                where upper(aps.survey_status) = ''ACTIVE''
                                group by apd.survey_code,apd.product_location_other_desc,include_exclude_ind)
                      ) group by survey_code';

--    dbms_output.put_line('All Params Statement Length - '||length(g_stmt));
--    dbms_output.put_line(g_stmt)  ;

    execute immediate g_stmt;
--    dbms_output.put_line('Inserted into ALL_PARAMS - '||sql%rowcount);
    commit;

    g_stmt := ' insert into temp_survey_master
                with all_surveys as (select distinct * from temp_all_params)
                select aps.survey_code,aps.survey_name,aps.survey_area,aps.minimum_spend_criteria,aps.minimum_units_criteria,aps.priority_order,
                       aps.quota_minimum,aps.quota_maximum,aps.last_updated_date,aps.updated_by,aps.created_date,aps.created_by,aps.modified_ind,
                       aps.survey_status,aps.same_csats_survey_eligibility,diff_csats_survey_eligibility,aps.different_survey_eligibility,apd.age_band,
                       apd.area_no,apd.business_unit_no,apd.department_no,apd.derived_race,apd.district_no,apd.foods_value_segment_no,
                       apd.group_no,apd.include,apd.language_preference,apd.location_no,apd.loyalty_tier_no,apd.non_foods_value_segment_no,apd.region_no,apd.roa_ind,
                       apd.shopping_habit_segment_no,apd.subgroup_no,apd.till_no,survey_params ,all_survey_params,aps.channel
                  from apex_app_cust_01.apex_mkt_csats_summary aps inner join
                       (select *
                          from (select survey_code, apd.product_location_other_desc, apd.product_location_other_no
                                  from apex_app_cust_01.apex_mkt_csats_detail apd
                                 order by product_location_other_desc asc
                               )
                          pivot
                               (
                               sum(product_location_other_no)
                               for product_location_other_desc in ('||g_cols||')
                               )
                        ) apd on (aps.survey_code = apd.survey_code)
                 inner join all_surveys ap on (aps.survey_code = ap.survey_code)
                 where upper(aps.survey_status) = ''ACTIVE'' ';

--        dbms_output.put_line('Survey Master Statement - '||length(g_stmt));
--        dbms_output.put_line(g_stmt)  ;

        execute immediate g_stmt;
--        dbms_output.put_line('Inserted into TEMP_SURVEY_MASTER - '||sql%rowcount);
    commit;

--**************************************************************************************************
-- 1. Scroll through qualifying surveys
-- 2. Get the where clause.
-- 3. Build the Main SQL
-- * Apologies for the convoluted SQL. It's due to string limitations and the various permutations
--   The survey could present.
--**************************************************************************************************
    for c_survey in (select * from temp_survey_master order by priority_order)
    loop
        execute immediate 'truncate table temp_basket_item';
        execute immediate 'truncate table temp_csats_survey_detail';
        dbms_output.put_line('Start Loop ');

        g_stmt :=' insert into temp_basket_item select /*+ Parallel(cbi,8) Parallel(di,8) Parallel(cm,8) Parallel(dcm,8) Parallel(dr,8) Parallel(csm,8) Parallel(cw,8) Parallel(cs,8) Parallel(sd,8) Parallel(ap,8) */ '||c_survey.survey_code||' survey_code,'||
                        'dcm.master_subscriber_key,cbi.primary_customer_identifier,cm.source_key customer_no,dcm.work_email_address,dcm.populated_cell_no,dcm.title,dcm.first_name,dcm.last_name,'||
                        'dl.location_name,dl.region_name,dl.st_s4s_shape_of_chain_desc st_store_type,'||g_race_case||' race_no,dcm.age,cbi.tran_date,sum(cbi.item_tran_qty) item_qty,sum(cbi.item_tran_selling - cbi.discount_selling) selling_value,'||
                        'cw.month_tier,csm.csm_shopping_habit_segment_no,cs.foods_value_segment_no,cs.nonfoods_value_segment_no '||
                   'from cust_basket_item@dwhprd cbi '||
                  'inner join dim_item@dwhprd di on (cbi.item_no  = di.item_no) '||
                  'inner join dim_customer_mapping@dwhprd cm on (cbi.primary_customer_identifier = cm.source_key) '||
                  'inner join dim_customer_master@dwhprd dcm on (cm.subscriber_key = dcm.master_subscriber_key) '||
                  'inner join cust_csm_shopping_habits@dwhprd csm on (cbi.primary_customer_identifier = csm.csm_customer_identifier) '||
                  'inner join cust_wod_tier_mth_detail@dwhprd cw on (cbi.primary_customer_identifier = cw.customer_no and '||
                                                                     'csm.fin_year_no = cw.fin_year_no and '||
                                                                     'csm.fin_month_no = cw.fin_month_no) '||
                  'inner join dim_location@dwhprd dl on (cbi.location_no = dl.location_no) '||
                  'inner join (select /*+ Parallel(piv,8) */primary_customer_identifier,fin_year_no,fin_month_no,max(foods_value_segment_no) foods_value_segment_no, max(nonfoods_value_segment_no) nonfoods_value_segment_no from (select /*+Parallel(8) */primary_customer_identifier,fin_year_no,fin_month_no,foods_value_segment_no,nonfoods_value_segment_no from W7131037.cust_csm_value_segment@dwhprd pivot( max(current_seg)  for food_non_food in (''FSHV'' as "FOODS_VALUE_SEGMENT_NO", ''NFSHV'' as "NONFOODS_VALUE_SEGMENT_NO"  )) ) piv group by primary_customer_identifier,fin_year_no,fin_month_no) cs '||
                  ' on (cbi.primary_customer_identifier = cs.primary_customer_identifier and '||
                  'csm.fin_year_no = cs.fin_year_no and '||
                  'csm.fin_month_no = cs.fin_month_no) '||
                  'where  cbi.tran_date between '''||g_start_date||''' and '''||g_end_date||''' '||
                  '  and '||case when length(c_survey.survey_params) > 0 then c_survey.survey_params||' and' else '' end||
                         ' cbi.tran_type in (''S'',''V'',''R'') and'||
                         ' cbi.primary_customer_identifier is not null and'||
                         ' cbi.primary_customer_identifier <> 0  and'||
                         ' cbi.primary_customer_identifier not between 6007851400000000 and 6007851499999999 and'||
                         ' cm.source = ''C2'' and'||
                         ' csm.fin_year_no = '||g_fin_year ||' and'||
                         ' csm.fin_month_no = '||g_fin_month ||' and'||
                         ' csm.csm_period_code = '''||g_csm_period||''' and'||
                         ' csm.last_updated_date = '''||g_date||''' '||
                         case when nvl(c_survey.age_band,0) > 0 then ' and cb.age between ab.minimum_age and ab.maximum_age ' else '' end||
                  ' group by '||c_survey.survey_code||',dcm.master_subscriber_key,cbi.primary_customer_identifier,cm.source_key ,dcm.work_email_address,'||
                         'dcm.populated_cell_no,dcm.title,dcm.first_name,dcm.last_name,dl.location_name,dl.region_name,dl.st_s4s_shape_of_chain_desc ,'||
                         g_race_case||
                         ',dcm.age,cbi.tran_date,cw.month_tier,csm.csm_shopping_habit_segment_no,cs.foods_value_segment_no,cs.nonfoods_value_segment_no '||
                         case when c_survey.minimum_spend_criteria > 0 and c_survey.minimum_units_criteria > 0 then
                                 ' having sum(cbi.item_tran_selling - cbi.discount_selling) >= '||c_survey.minimum_spend_criteria||' and sum(cbi.item_tran_qty) >= '||c_survey.minimum_units_criteria
                            when c_survey.minimum_spend_criteria > 0 then
                                 ' having sum(cbi.item_tran_selling - cbi.discount_selling) >= '||c_survey.minimum_spend_criteria
                            when c_survey.minimum_units_criteria > 0 then
                                 ' having sum(cbi.item_tran_qty) >= '||c_survey.minimum_units_criteria
                         end||
                         case when upper(c_survey.channel) = 'E-MAIL' then ' and dcm.work_email_address is not null ' else ' and dcm.populated_cell_no is not null' end;

--        dbms_output.put_line('MainBuild TEMP BASKET ITEM - '||length(g_stmt));
        dbms_output.put_line(g_stmt)   ;
        execute immediate g_stmt;
        g_recs_read := sql%rowcount;
        commit;


--**************************************************************************************************
-- Do we have the minimum number of records.
-- If so then insert the data.
-- The preceding statement ensures that the maximum is extracted for this survey.
--**************************************************************************************************
        case when g_recs_read >= c_survey.quota_minimum  then

            g_stmt :=' insert into temp_csats_survey_detail select /*+ Parallel(bi,8) */ *'||
                     ' from (select /*+ Parallel(sm,8) Parallel(cb,8) Parallel(cc,8)*/ '||
                                    'sm.survey_code,sm.survey_name,sm.survey_area,sm.minimum_spend_criteria,sm.minimum_units_criteria,'||
                                    'sm.priority_order,sm.quota_minimum,sm.quota_maximum,sm.last_updated_date survey_updated_date,'||
                                    'sm.updated_by,sm.created_date survey_created_date,sm.created_by,sm.modified_ind,'||
                                    'sm.survey_status,sm.same_csats_survey_eligibility,sm.different_survey_eligibility,sm.age_band,'||
                                    'sm.area_no survey_area_no,sm.business_unit_no survey_business_unit_no,sm.department_no survey_department_no,'||
                                    'sm.derived_race,sm.district_no survey_district_no,sm.group_no survey_group_no,sm.include,sm.language_preference,'||
                                    'sm.location_no survey_location_no,sm.loyalty_tier_no survey_loyalty_tier,sm.foods_value_segment_no survey_foods_value_segment_no,'||
                                    'sm.non_foods_value_segment_no survey_non_fd_value_segment_no,sm.region_no,'||
                                    'sm.roa_ind,sm.shopping_habit_segment_no,sm.subgroup_no,sm.till_no,sm.channel,sm.survey_params,sm.all_survey_params,'||
                                    'cb.master_subscriber_key,cb.primary_customer_identifier,cb.customer_no,cb.work_email_address,'||
                                    'cb.populated_cell_no,cb.title,cb.first_name,cb.last_name,cb.location_name,cb.region_name,'||
                                    'cb.st_store_type,cb.race_no,cb.age,cb.tran_date,cb.item_qty,cb.selling_value,'||
                                    'cb.month_tier,cb.csm_shopping_habit_segment_no,cb.foods_value_segment_no,cb.nonfoods_value_segment_no,'||
                                    'trunc(sysdate) last_updated_date '||
                     ' from W7131037.temp_survey_master sm '||
                     'inner join W7131037.temp_basket_item cb on (sm.survey_code = cb.survey_code) '||
                     'left join (select /*+ Parallel(ccs,8) Parallel(csd,8) */ ccs.master_subscriber_key,nvl(cast(trunc(sysdate)-max(ccs.last_updated_date) as number),9999) same_csats_survey_days, nvl(cast(trunc(sysdate)-max(csd.last_updated_date) as number),9999) diff_csats_survey_days, nvl(cast(trunc(sysdate)-max(ncs.last_updated_date) as number),9999) different_survey_days '||
                                  'from W7131037.cust_csats_survey_detail ccs '||
                                  'left join W7131037.cust_csats_survey_detail csd on (ccs.primary_customer_identifier = csd.primary_customer_identifier and ccs.survey_status = csd.survey_status) '||
                                  'left join apex_app_cust_01.non_csats_survey ncs on (ccs.master_subscriber_key = ncs.subscriber_key) '||
                                  'where ccs.survey_code = '||c_survey.survey_code||
                                  '  and ccs.survey_area = '''||c_survey.survey_area||''''||
                                  '  and csd.survey_area <> '''||c_survey.survey_area||''''||
                                  '  and upper(ccs.survey_status) = '''||'DEPLOYED'' ' ||
                                  ' group by ccs.master_subscriber_key) cc on (cb.master_subscriber_key = cc.master_subscriber_key) '||
                     'left join (select  /*+ Parallel(ao,8)  */ '||
                                        'distinct ''Y'' opt_out_true,opt_out_subscriber_key,opt_out_email_address,opt_out_mobile_no '||
                                  ' from apex_app_cust_01.apex_mkt_csats_opt_out ao      '||
                                  ')  oo on (cb.master_subscriber_key = oo.opt_out_subscriber_key or '||
                                            'upper(cb.work_email_address) = upper(oo.opt_out_email_address) or '||
                                            'cb.populated_cell_no = opt_out_mobile_no) '||
                                  case when c_survey.same_csats_survey_eligibility > 0 and
                                            c_survey.diff_csats_survey_eligibility > 0 then
                                            'where sm.same_csats_survey_eligibility < nvl(same_csats_survey_days,9999) and sm.diff_csats_survey_eligibility < nvl(diff_csats_survey_days,9999) '
                                       when c_survey.same_csats_survey_eligibility > 0 and c_survey.different_survey_eligibility = 0 then
                                            'where sm.same_csats_survey_eligibility < nvl(same_csats_survey_days,9999) '
                                       when c_survey.same_csats_survey_eligibility = 0 and c_survey.diff_csats_survey_eligibility > 0 then
                                            'where sm.diff_csats_survey_eligibility < nvl(diff_csats_survey_days,9999) '
                                  end ||
                                  case when c_survey.different_survey_eligibility > 0 and (c_survey.same_csats_survey_eligibility > 0 or c_survey.diff_csats_survey_eligibility > 0 )then
                                            'and sm.different_survey_eligibility < nvl(different_survey_days,9999) '
                                       else 'where sm.different_survey_eligibility < nvl(different_survey_days,9999) '
                                  end||
                                  ' and nvl(oo.opt_out_true,''N'') = ''N'' '||
                                  ' order by cb.item_qty desc,cb.selling_value desc) bi '||
                                  case when c_survey.quota_maximum > 0 then ' where rownum <= '||c_survey.quota_maximum else '' end;

--           dbms_output.put_line('Main Statement - '||length(g_stmt));
--           dbms_output.put_line(g_stmt)  ;
           execute immediate g_stmt;
--           dbms_output.put_line('Insert into TEMP_CSATS_SURVEY_DETAIL - '||sql%rowcount);
           commit;

--**************************************************************************************************
-- Had to be don like this as the above script with the merge exceded 400 Characters.
--**************************************************************************************************
           merge into W7131037.cust_csats_survey_detail c
           using ( select distinct * from W7131037.temp_csats_survey_detail) t on (c.master_subscriber_key = t.master_subscriber_key and
                                                                                               c.survey_code           = t.survey_code and
                                                                                               c.survey_area           = t.survey_area)
              when matched then
               update set c.survey_name                     = t.survey_name,
                          c.minimum_spend_criteria          = t.minimum_spend_criteria,
                          c.minimum_units_criteria          = t.minimum_units_criteria,
                          c.priority_order                  = t.priority_order,
                          c.quota_minimum                   = t.quota_minimum,
                          c.quota_maximum                   = t.quota_maximum,
                          c.survey_updated_date             = t.survey_updated_date,
                          c.updated_by                      = t.updated_by,
                          c.survey_created_date             = t.survey_created_date,
                          c.created_by                      = t.created_by,
                          c.modified_ind                    = t.modified_ind,
                          c.survey_status                   = t.survey_status,
                          c.same_csats_survey_eligibility   = t.same_csats_survey_eligibility,
                          c.different_survey_eligibility    = t.different_survey_eligibility,
                          c.age_band                        = t.age_band,
                          c.survey_area_no                  = t.survey_area_no,
                          c.survey_business_unit_no         = t.survey_business_unit_no,
                          c.survey_department_no            = t.survey_department_no,
                          c.derived_race                    = t.derived_race,
                          c.survey_district_no              = t.survey_district_no,
                          c.survey_group_no                 = t.survey_group_no,
                          c.include                         = t.include,
                          c.language_preference             = t.language_preference,
                          c.survey_location_no              = t.survey_location_no,
                          c.survey_loyalty_tier             = t.survey_loyalty_tier,
                          c.survey_foods_value_segment_no   = t.survey_foods_value_segment_no,
                          c.survey_non_fd_value_segment_no  = t.survey_non_fd_value_segment_no,
                          c.region_no                       = t.region_no,
                          c.roa_ind                         = t.roa_ind,
                          c.shopping_habit_segment_no       = t.shopping_habit_segment_no,
                          c.subgroup_no                     = t.subgroup_no,
                          c.till_no                         = t.till_no,
                          c.channel                         = t.channel,
                          c.survey_params                   = t.survey_params,
                          c.all_survey_params               = t.all_survey_params,
                          c.primary_customer_identifier     = t.primary_customer_identifier,
                          c.customer_no                     = t.customer_no,
                          c.work_email_address              = t.work_email_address,
                          c.populated_cell_no               = t.populated_cell_no,
                          c.title                           = t.title,
                          c.first_name                      = t.first_name,
                          c.last_name                       = t.last_name,
                          c.location_name                   = t.location_name,
                          c.region_name                     = t.region_name,
                          c.store_type                   = t.st_store_type,
                          c.race_no                         = t.race_no,
                          c.age                             = t.age,
                          c.tran_date                       = t.tran_date,
                          c.item_qty                        = t.item_qty,
                          c.selling_value                   = t.selling_value,
                          c.month_tier                      = t.month_tier,
                          c.csm_shopping_habit_segment_no   = t.csm_shopping_habit_segment_no,
                          c.foods_value_segment_no          = t.foods_value_segment_no,
                          c.nonfoods_value_segment_no       = t.nonfoods_value_segment_no,
                          c.last_updated_date               = t.last_updated_date
                    where c.survey_code                     = t.survey_code and
                          c.survey_area                     = t.survey_area and
                          c.master_subscriber_key           = t.master_subscriber_key and
                          c.survey_status                   = 'ACTIVE'
              when not matched then
                   insert (c.survey_code,c.survey_name,c.survey_area,c.minimum_spend_criteria,c.minimum_units_criteria,c.priority_order,c.quota_minimum,c.quota_maximum,c.survey_updated_date,c.updated_by,c.survey_created_date,c.created_by,c.modified_ind,c.survey_status,c.same_csats_survey_eligibility,c.different_survey_eligibility,c.age_band,c.survey_area_no,c.survey_business_unit_no,c.survey_department_no,c.derived_race,c.survey_district_no,c.survey_group_no,c.include,c.language_preference,c.survey_location_no,c.survey_loyalty_tier,c.survey_foods_value_segment_no,c.survey_non_fd_value_segment_no,c.region_no,c.roa_ind,c.shopping_habit_segment_no,c.subgroup_no,c.till_no,c.channel,c.survey_params,c.all_survey_params,c.master_subscriber_key,c.primary_customer_identifier,c.customer_no,c.work_email_address,c.populated_cell_no,c.title,c.first_name,c.last_name,c.location_name,c.region_name,c.store_type,c.race_no,c.age,c.tran_date,c.item_qty,c.selling_value,c.month_tier,c.csm_shopping_habit_segment_no,c.foods_value_segment_no,c.nonfoods_value_segment_no,c.last_updated_date)
                   values (t.survey_code,t.survey_name,t.survey_area,t.minimum_spend_criteria,t.minimum_units_criteria,t.priority_order,t.quota_minimum,t.quota_maximum,t.survey_updated_date,t.updated_by,t.survey_created_date,t.created_by,t.modified_ind,t.survey_status,t.same_csats_survey_eligibility,t.different_survey_eligibility,t.age_band,t.survey_area_no,t.survey_business_unit_no,t.survey_department_no,t.derived_race,t.survey_district_no,t.survey_group_no,t.include,t.language_preference,t.survey_location_no,t.survey_loyalty_tier,t.survey_foods_value_segment_no,t.survey_non_fd_value_segment_no,t.region_no,t.roa_ind,t.shopping_habit_segment_no,t.subgroup_no,t.till_no,t.channel,t.survey_params,t.all_survey_params,t.master_subscriber_key,t.primary_customer_identifier,t.customer_no,t.work_email_address,t.populated_cell_no,t.title,t.first_name,t.last_name,t.location_name,t.region_name,t.st_store_type,t.race_no,t.age,t.tran_date,t.item_qty,t.selling_value,t.month_tier,t.csm_shopping_habit_segment_no,t.foods_value_segment_no,t.nonfoods_value_segment_no,t.last_updated_date);

           g_recs_inserted := g_recs_inserted + sql%rowcount;
--           dbms_output.put_line('Insert into CUST_CSATS_SURVEY_DETAIL - '||sql%rowcount);

           l_text := 'Processed Survey : '||c_survey.survey_code||' - Survey Name : '||c_survey.survey_name||' - Survey Area : '||c_survey.survey_area||' - Rows : '||sql%rowcount||'  @ - '||to_char(sysdate,'dd mon yyyy hh24:mi:ss');
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
           commit;
           else
                dbms_output.put_line('Stepping over INSERT into CUST_CSATS_SURVEY_DETAIL. ');
        end case;

     commit;
    end loop;


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

end "WH_PRF_CUST_400U";
