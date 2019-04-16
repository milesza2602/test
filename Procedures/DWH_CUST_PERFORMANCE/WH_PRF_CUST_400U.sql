--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_400U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_400U" (p_forall_limit in integer,p_success out boolean) AS 

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
--               from dim_customer_mapping - Order by SOURCE_KEY and LAST_UPDATED_DATE desc  
--               Miles Mafu  08/Oct/2018
--               Ref: 20190219 MDM
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
g_survey_code        number         := 0;
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


g_date               date         := trunc(sysdate);
g_rundate            date         := trunc(sysdate);
g_sql_log_date       date         := sysdate;  
g_start_date         date         := trunc(sysdate);
g_end_date           date         := trunc(sysdate);
g_online_start_date  date         := trunc(sysdate);
g_online_end_date    date         := trunc(sysdate);



g_stmt                clob;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_400U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               SYS_DWH_LOG.LOG_TEXT%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST_SURVEY EX CUST_BASKET_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;

filter_lst   DBMS_STATS.OBJECTTAB := DBMS_STATS.OBJECTTAB ();

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 
    execute immediate 'alter session enable parallel dml';

--************************************************************************************************** 
-- All generated SQLs are written to the dwh_cust_performance.temp_survey_sql table
-- To display all DBMS OUTPUT to facilitate debugging, un-comment the next line
--**************************************************************************************************
    g_debug := true;

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
   
    filter_lst.extend(1);
    filter_lst(1).ownname := 'DWH_CUST_PERFORMANCE';
    filter_lst(1).objname := 'CUST_CSM_SHOPPING_HABITS';
    DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>'DWH_CUST_PERFORMANCE',obj_filter_list=>filter_lst,options=>'gather auto',granularity=> 'PARTITION');
    
    l_text := 'GATHER STATS - CUST_CSM_SHOPPING_HABITS' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
    
    COMMIT;
/*     
    filter_lst.extend(1);
    filter_lst(1).ownname := 'DWH_CUST_PERFORMANCE';
    filter_lst(1).objname := 'CUST_WOD_TIER_MTH_DETAIL';
    DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>'DWH_CUST_PERFORMANCE',obj_filter_list=>filter_lst,options=>'gather auto',granularity=> 'PARTITION');
    
    l_text := 'GATHER STATS - CUST_WOD_TIER_MTH_DETAIL' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
    
    COMMIT;
*/     
    filter_lst.extend(1);
    filter_lst(1).ownname := 'DWH_CUST_PERFORMANCE';
    filter_lst(1).objname := 'CUST_CSM_VALUE_SEGMENT';
    DBMS_STATS.GATHER_SCHEMA_STATS(ownname=>'DWH_CUST_PERFORMANCE',obj_filter_list=>filter_lst,options=>'gather auto',granularity=> 'PARTITION');
    
    l_text := 'GATHER STATS - CUST_CSM_VALUE_SEGMENT' ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  
   
    COMMIT;

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
                         calendar_date - 7
                    when to_char(calendar_date,'D') in (3) then             -- Tuesday (Saturday and Sunday’s transactions)
                         calendar_date - 7
                    when to_char(calendar_date,'D') in (4) then             -- Wednesday (Monday and Tuesday’s transactions;)
                         calendar_date - 3
                    when to_char(calendar_date,'D') in (5,6) then           -- Thursday/Friday (Wednesday’s/Thursday’s transactions )
                         calendar_date - 3
               end online_start_date,
               case
                    when to_char(calendar_date,'D') in (2) then             -- Monday (Friday’s transactions;)
                         calendar_date - 6
                    when to_char(calendar_date,'D') in (3) then             -- Tuesday (Saturday and Sunday’s transactions)
                         calendar_date - 6
                    when to_char(calendar_date,'D') in (4) then             -- Wednesday (Monday and Tuesday’s transactions;)
                         calendar_date - 3
                    when to_char(calendar_date,'D') in (5,6) then           -- Thursday/Friday (Wednesday’s/Thursday’s transactions )
                         calendar_date - 3
               end online_end_date
          from dim_calendar where calendar_date = trunc(sysdate)
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
-- Pivot and list as columns all known product_location_other_desc options
-- This is done in this fashion as there is no table in APEX than lists these.
-- LISTAGG  doesn't work with this version of oracle
-- Change to mitigate version difference between prod and uat
--**************************************************************************************************

--          select wm_concat(concat('''' || colname || ''' as "',colname || '"')) colnames
         select listagg(concat('''' || colname || ''' as "',colname || '"'), ',') within group (order by colname) colnames
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
                     select 'MONTH_TIER' colname from dual union
                     select 'MINIMUM_SPEND_CRITERIA' colname from dual union
                     select 'NON_FOODS_VALUE_SEGMENT_NO' colname from dual union
                     select 'REGION_NO' colname from dual union
                     select 'ROA_IND' colname from dual union
                     select 'SHOPPING_HABIT_SEGMENT_NO' colname from dual union
                     select 'SUBGROUP_NO' colname from dual union
                     select 'TILL_NO' from dual
                     )  ;

--************************************************************************************************** 
-- Due to limitations on number of inner joins we've formulated the decode statement for race
-- This needs only be done once then it can be used in the main query.
-- Change to mitigate version difference between prod and uat
--**************************************************************************************************    

--       select 'decode(upper(dcm.derived_race), '||WM_CONCAT(race_name)||',0)'
        select 'decode(upper(dcm.derived_race), ' || listagg(race_name, ',') within group (order by race_name) ||',0)'
         into g_race_case
         from (select race_no,''''||upper(race_name)||''','||race_no race_name from apex_app_cust_01.derived_race);

--       select replace('case '||WM_CONCAT(case_stmt)||' end',',','')  
         select replace('case '|| listagg(case_stmt, ',') within group (order by case_stmt) ||' end',',','') 
         into g_age_case
         from (select ' when dcm.age >= '||minimum_age||' and dcm.age <= '||maximum_age||' then '||age_band case_stmt 
                 from apex_app_cust_01.apex_age_band);

--************************************************************************************************** 
-- Create the survey master records.
-- Change to mitigate version difference between prod and uat
--**************************************************************************************************
    l_text := 'Clean all temporary tables incl. CUST_CSATS_SURVEY_DETAIL ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate 'truncate table temp_all_params';
    execute immediate 'truncate table temp_survey_master';
    execute immediate 'truncate table temp_survey_sql';
    execute immediate 'delete from cust_csats_survey_detail where last_updated_date = '''||g_rundate||''' ';
    g_recs_deleted := sql%rowcount;

    insert into temp_all_params
     select  survey_code,

        cast(substr(survey_params,5,length(survey_params)) as varchar(4000)) survey_params,

        cast(substr(all_survey_params,5,length(all_survey_params)) as varchar(4000)) all_survey_params       

from

    (



        select  survey_code,

                replace(listagg(survey_params,',')     within group (order by survey_params),',AND','AND') survey_params,

                replace(listagg(all_survey_params,',') within group (order by all_survey_params),',AND','AND') all_survey_params

        from (    

               select   survey_code,

                        concat( 'AND ',params0) survey_params,

                        concat( 'AND ',params)  all_survey_params

               from (

                       select survey_code,

                              case when includ = 1 then

                              case

                                   when product_location_other_desc in ('ITEM_NO','ITEM_LEVEL1_NO','BUSINESS_UNIT_NO','GROUP_NO','SUBGROUP_NO','DEPARTMENT_NO')  then 'DI.'||product_location_other_desc

                                   when product_location_other_desc in ('AREA_NO','REGION_NO','DISTRICT_NO') then 'DL.'||product_location_other_desc

                                   when product_location_other_desc in ('TRAN_TYPE','TILL_NO','LOCATION_NO') then 'CBI.'||product_location_other_desc

                                   when product_location_other_desc in ('AGE_BAND','DERIVED_RACE')  then 'DCM.'||product_location_other_desc

                                   when product_location_other_desc = 'MONTH_TIER' then 'CW.'||product_location_other_desc

                                    when product_location_other_desc = 'TRAN_TYPE_NO' then 'CBI.TRAN_TYPE'

                                   when product_location_other_desc = 'SHOPPING_HABIT_SEGMENT_NO' then 'CSM.CSM_'||product_location_other_desc

                                   else product_location_other_desc end ||include_exclude_ind||'('||case when product_location_other_desc = 'TRAN_TYPE_NO' then 
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

                                   select a.*,

                                          rank() over (partition by a.survey_code, a.product_location_other_desc order by a.include_exclude_ind desc) includ 

                                   from

                                        (

                                            select  /*+ Full(apd) */

                                                    apd.survey_code,

                                                    apd.product_location_other_desc,

                                                    case include_exclude_ind when 'INCLUDE' then ' IN ' else ' NOT IN ' end                             include_exclude_ind,

                                                    listagg(product_location_other_no,',') within group (order by product_location_other_no)            concatdata

                                            from    apex_app_cust_01.apex_mkt_csats_summary aps

                                            inner join

                                                    apex_app_cust_01.apex_mkt_csats_detail apd on (aps.survey_code = apd.survey_code)

                                            where   upper(aps.survey_status) like 'ACTIVE%'

                                            group by

                                                    apd.survey_code,

                                                    apd.product_location_other_desc,

                                                    case include_exclude_ind when 'INCLUDE' then ' IN ' else ' NOT IN ' end

                                        ) a

                            )

                    )

            )

        group by

                survey_code

    )           

;
--    dbms_output.put_line('All Params Statement Length - '||length(g_stmt));                    

--    dbms_output.put_line('Inserted into ALL_PARAMS - '||sql%rowcount);
    commit;

    g_stmt := ' insert /*+ Append */ into temp_survey_master
                with all_surveys as (select distinct * from temp_all_params)
                select aps.survey_code,aps.survey_name,aps.survey_area,aps.minimum_spend_criteria,aps.minimum_units_criteria,aps.priority_order,
                       aps.quota_minimum,aps.quota_maximum,aps.last_updated_date,aps.updated_by,aps.created_date,aps.created_by,aps.modified_ind,
                       aps.survey_status,aps.same_csats_survey_eligibility,aps.diff_csats_survey_eligibility,aps.different_survey_eligibility,
                       apd.age_band,nvl(ab.minimum_age,0) minimum_age,nvl(ab.maximum_age,99) maximum_age,apd.area_no,apd.business_unit_no,
                       apd.department_no,apd.derived_race,apd.district_no,apd.foods_value_segment_no,apd.group_no,apd.include,apd.language_preference,
                       apd.location_no,apd.month_tier,apd.non_foods_value_segment_no,apd.region_no,apd.roa_ind,apd.shopping_habit_segment_no,
                       apd.subgroup_no,apd.till_no,aps.survey_name_reformatted,aps.merchandise_classification,aps.survey_type,survey_params ,all_survey_params,aps.channel 
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
                 left join apex_app_cust_01.apex_age_band ab on (apd.age_band = ab.age_band)
                 where upper(aps.survey_status) like ''ACTIVE%'' ';

          if g_debug = true then
             dbms_output.put_line('Temp Survey Master Statement - '||length(g_stmt));
--             dbms_output.put_line(g_stmt)  ;
          end if;

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
--        dbms_output.put_line('Processing Survey '||c_survey.survey_code);
        commit;

        g_stmt :=' insert into temp_basket_item 
                   select /*+ materialize cardinality (cbi,300000) full(cbi) FULL(cm)  */ 
                          distinct cast('||c_survey.survey_code||' as number) survey_code,dcm.master_subscriber_key,cbi.primary_customer_identifier,cm.source_key customer_no,
                          dcm.identity_document_no, dcm.populated_email_address,dcm.work_email_address,dcm.home_email_address,dcm.statement_email_address,
                          dcm.ecommerce_email_address,dcm.populated_cell_no,dcm.home_cell_no,dcm.work_cell_no,dcm.title,dcm.first_name,dcm.last_name,
                          dl.location_name,dl.region_name,dl.st_s4s_shape_of_chain_desc st_store_type,derived_race,dcm.age,dcm.age_band,cbi.tran_date,
                          sum(cbi.item_tran_qty) item_qty,sum(cbi.item_tran_selling-cbi.discount_selling) selling_value,
                          cw.month_tier,csm.csm_shopping_habit_segment_no,cs.foods_value_segment_no,cs.non_foods_value_segment_no
                     from cust_basket_item cbi 
                    inner join dim_item di on (cbi.item_no=di.item_no) 
                    inner join (select /*+ materialize cardinality (dm,300000) */ 
                                       dm.*,
                                       rank() over (partition by dm.source_key, dm.last_updated_date order by dm.last_updated_date desc) dm_seq
                                  from dim_customer_mapping dm
                                 where dm.source = ''C2''
                               ) cm on (cbi.primary_customer_identifier=cm.source_key) 
                    inner join (select master_subscriber_key,identity_document_no,
                                       upper(dcm.populated_email_address) ,
                                       upper(populated_email_address) populated_email_address,
                                       upper(dcm.work_email_address) work_email_address,
                                       upper(dcm.home_email_address) home_email_address,
                                       upper(dcm.statement_email_address) ,
                                       upper(statement_email_address) statement_email_address,
                                       upper(dcm.ecommerce_email_address) ,
                                       upper(ecommerce_email_address) ecommerce_email_address,
                                       dcm.populated_cell_no,dcm.home_cell_no,dcm.work_cell_no,title,
                                       first_name,last_name,'||g_race_case||' derived_race,age,'||g_age_case||'
                                       age_band,ww_man_email_opt_out_ind man_email_opt_out_ind,ww_man_sms_opt_out_ind man_sms_opt_out_ind 
                                  from dim_customer_master dcm) dcm on (cm.subscriber_key = dcm.master_subscriber_key)
                   inner join cust_csm_shopping_habits csm on (cbi.primary_customer_identifier=csm.csm_customer_identifier)
                   inner join cust_wod_tier_mth_detail cw on (cbi.primary_customer_identifier=cw.customer_no and 
                                                                     csm.fin_year_no=cw.fin_year_no and 
                                                                     csm.fin_month_no=cw.fin_month_no) 
                   inner join dim_location dl on (cbi.location_no = dl.location_no) 
                   inner join (select /*+ materialize cardinality (piv,300000) */ 
                                      primary_customer_identifier,
                                      fin_year_no,
                                      fin_month_no,
                                      max(foods_value_segment_no) foods_value_segment_no, 
                                      max(non_foods_value_segment_no) non_foods_value_segment_no 
                                 from (select 
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
                   where cbi.tran_date between '''||case when upper(c_survey.survey_type) <> 'WEBSITE' then g_start_date||''' and '''||g_end_date else g_online_start_date||''' and '''|| g_online_end_date end||''' 
                     and '||case when length(c_survey.survey_params) > 0 then c_survey.survey_params else '' end||'
                     and cbi.primary_customer_identifier is not null 
                     and cbi.primary_customer_identifier<>0  
                     and cbi.primary_customer_identifier not between 6007851400000000 and 6007851499999999
                     and cm.dm_seq = 1
                     and csm.fin_year_no='||g_fin_year ||' 
                     and csm.fin_month_no='||g_fin_month ||' 
                     and csm.csm_period_code='''||g_csm_period||''' 
                     and csm.last_updated_date='''||g_date||''' '||
                         case when upper(c_survey.channel) = 'E-MAIL' then ' and dcm.man_email_opt_out_ind != 1 and dcm.populated_email_address is not null' end||
                         case when upper(c_survey.channel) = 'MOBILE' then ' and dcm.man_sms_opt_out_ind != 1 and dcm.populated_cell_no is not null' end||'
                    group by cast('||c_survey.survey_code||' as number),dcm.master_subscriber_key,cbi.primary_customer_identifier,cm.source_key,
                          dcm.identity_document_no, dcm.populated_email_address,dcm.work_email_address,dcm.home_email_address,dcm.statement_email_address,
                          dcm.ecommerce_email_address,dcm.populated_cell_no,dcm.home_cell_no,dcm.work_cell_no,dcm.title,dcm.first_name,dcm.last_name,
                          dl.location_name,dl.region_name,dl.st_s4s_shape_of_chain_desc ,derived_race,dcm.age,dcm.age_band,cbi.tran_date,
                          cw.month_tier,csm.csm_shopping_habit_segment_no,cs.foods_value_segment_no,cs.non_foods_value_segment_no
                   having sum(cbi.item_tran_selling-cbi.discount_selling) >= '||c_survey.minimum_spend_criteria||' 
                      and sum(cbi.item_tran_qty) >= '||c_survey.minimum_units_criteria;

        if g_debug = true then
           dbms_output.put_line('MainBuild TEMP BASKET ITEM - '||length(g_stmt));
--           dbms_output.put_line(g_stmt)   ; 
        end if;
       insert into dwh_cust_performance.temp_survey_sql values (g_sql_log_date,c_survey.survey_code,cast('1' as number),g_stmt);commit;
        execute immediate g_stmt;
        g_recs_updated := sql%rowcount;


--        insert into dwh_cust_performance.temp_survey_sql values (g_sql_log_date,c_survey.survey_code,cast('1' as number),g_stmt);
        commit;         


--************************************************************************************************** 
-- Do we have the minimum number of records.
-- If so then insert the data. 
-- The preceding statement ensures that the maximum+1000 is extracted for this survey (cater for below filter).
--************************************************************************************************** 
        case when g_recs_updated >= c_survey.quota_minimum  then

            g_stmt :=' insert  into temp_csats_survey_detail 
                       select /*+ materialize cardinality (bi,300000) */  distinct *
                         from (select /*+ materialize cardinality (sm,300000) */ distinct 
                                      sm.survey_code,sm.survey_name,sm.survey_area,sm.minimum_spend_criteria,sm.minimum_units_criteria,
                                      sm.priority_order,sm.quota_minimum,sm.quota_maximum,sm.last_updated_date survey_updated_date,
                                      sm.updated_by,sm.created_date survey_created_date,sm.created_by,sm.modified_ind,
                                      sm.survey_status,sm.same_csats_survey_eligibility,sm.diff_csats_survey_eligibility,
                                      sm.different_survey_eligibility,sm.age_band survey_age_band,
                                      sm.area_no survey_area_no,sm.business_unit_no survey_business_unit_no,sm.department_no survey_department_no,
                                      sm.derived_race,sm.district_no survey_district_no,sm.group_no survey_group_no,sm.include,sm.language_preference,
                                      sm.location_no survey_location_no,sm.month_tier survey_loyalty_tier,sm.foods_value_segment_no survey_foods_value_segment_no,
                                      sm.non_foods_value_segment_no survey_non_fd_value_segment_no,sm.region_no,
                                      sm.roa_ind,sm.shopping_habit_segment_no,sm.subgroup_no,sm.till_no,sm.channel,sm.survey_params,sm.all_survey_params,
                                      cb.master_subscriber_key,cb.primary_customer_identifier,cb.customer_no,
                                      cb.populated_email_address,cb.work_email_address,cb.home_email_address,cb.statement_email_address,cb.ecommerce_email_address,
                                      cb.populated_cell_no,cb.home_cell_no,cb.work_cell_no,cb.title,cb.first_name,cb.last_name,cb.location_name,cb.region_name,
                                      cb.st_store_type,cb.race_no,cb.age,cb.age_band,cb.tran_date,cb.item_qty,cb.selling_value,
                                      cb.month_tier,cb.csm_shopping_habit_segment_no,cb.foods_value_segment_no,cb.nonfoods_value_segment_no non_foods_value_segment_no,
                                      '''||g_rundate||''' extracted_date, trunc(sysdate) last_updated_date,
                                      sm.survey_use_name,sm.merchandise_class,sm.survey_type
                                 from dwh_cust_performance.temp_survey_master sm 
                                inner join dwh_cust_performance.temp_basket_item cb on (sm.survey_code = cb.survey_code) 
                                 left join (select 
                                                   master_subscriber_key,
                                                   min(same_csats_survey_days) same_csats_survey_days,
                                                   min(diff_csats_survey_days) diff_csats_survey_days,
                                                   min(different_survey_days) different_survey_days
                                              from (
                                                    select /*+ materialize cardinality (csd,300000) */ 
                                                           csd.master_subscriber_key, 
                                                           trunc(sysdate) - nvl(max(case when csd.survey_code = '||c_survey.survey_code||' then csd.last_updated_date end),trunc(sysdate-('||c_survey.same_csats_survey_eligibility||'+1))) same_csats_survey_days,
                                                           trunc(sysdate) - nvl(max(case when csd.survey_code <> '||c_survey.survey_code||' then csd.last_updated_date end),trunc(sysdate-('||c_survey.diff_csats_survey_eligibility||'+1))) diff_csats_survey_days,
                                                           trunc(sysdate) - nvl(max(anc.last_updated_date),trunc(sysdate-('||c_survey.different_survey_eligibility||'+1))) different_survey_days
                                                      from dwh_cust_performance.cust_csats_survey_detail csd
                                                      left join apex_app_cust_01.apex_non_csats_survey anc on (csd.master_subscriber_key = anc.subscriber_key)
                                                      group by csd.master_subscriber_key,survey_code
                                                   )
                                              group by master_subscriber_key) cc on (cb.master_subscriber_key = cc.master_subscriber_key) 
                        left join (select /*+ materialize cardinality (ao,300000) */ 
                                          distinct ''Y'' opt_out_true,opt_out_subscriber_key,opt_out_identity_document_no,opt_out_email_address,opt_out_mobile_no 
                                     from apex_app_cust_01.apex_mkt_csats_opt_out ao
                                  ) oo on (cb.master_subscriber_key = oo.opt_out_subscriber_key or 
--Ref: 20190219 MDM - Numeric test to remove non-numeric id numbers                                         
                                         case
                                           when regexp_like(cb.identity_document_no, ''^[^a-zA-Z]*$'')then cb.identity_document_no 
                                           else '''' end = oo.opt_out_identity_document_no 
                                           or 
                                           cb.populated_email_address = upper(oo.opt_out_email_address) or 
                                           cb.work_email_address = upper(oo.opt_out_email_address) or 
                                           cb.home_email_address = upper(oo.opt_out_email_address) or 
                                           cb.statement_email_address = upper(oo.opt_out_email_address) or 
                                           cb.ecommerce_email_address = upper(oo.opt_out_email_address) or 
                                           cb.populated_cell_no = opt_out_mobile_no or 
                                           cb.home_cell_no = opt_out_mobile_no or 
                                           cb.work_cell_no = opt_out_mobile_no) 
                             where  (sm.same_csats_survey_eligibility < same_csats_survey_days
                               and   sm.diff_csats_survey_eligibility < diff_csats_survey_days
                               and   sm.different_survey_eligibility  < different_survey_days)         
                               and nvl(oo.opt_out_true,''N'') = ''N'' 
                             order by cb.selling_value desc,cb.item_qty desc) bi 
                       where master_subscriber_key not in (select master_subscriber_key 
                                                             from cust_csats_survey_detail 
                                                            where last_updated_date = trunc(sysdate)) ';



            if g_debug = true then
               dbms_output.put_line('TEMP_CSATS_SURVEY_DETAIL Statement - '||length(g_stmt));
--               dbms_output.put_line(g_stmt)  ;
            end if;
            insert into dwh_cust_performance.temp_survey_sql values (g_sql_log_date,c_survey.survey_code,cast('2' as number),g_stmt);
            commit;

            execute immediate g_stmt;
            g_recs_read := g_recs_read + sql%rowcount;
            g_recs_in_temp := sql%rowcount;
     --      insert into dwh_cust_performance.temp_survey_sql values (g_sql_log_date,c_survey.survey_code,cast('2' as number),g_stmt);
           commit;
--           dbms_stats.gather_table_stats ('DWH_CUST_PERFORMANCE','TEMP_CSATS_SURVEY_DETAIL',estimate_percent=>0.1, DEGREE => 16);
--************************************************************************************************** 
-- Had to be don like this as the above script with the merge exceded 400 Characters.
--**************************************************************************************************
             g_recs_loaded := 0;
             for c_insert in (
                   with unique_rows as (
                   select /*+ Parallel(8) */ 
                          survey_code,
                          survey_name,
                          survey_area,
                          minimum_spend_criteria,
                          minimum_units_criteria,
                          priority_order,
                          quota_minimum,
                          quota_maximum,
                          survey_updated_date,
                          updated_by,
                          survey_created_date,
                          created_by,
                          modified_ind,
                          survey_status,
                          same_csats_survey_eligibility,
                          diff_csats_survey_eligibility,
                          different_survey_eligibility,
                          age_band,
                          survey_area_no,
                          survey_business_unit_no,
                          survey_department_no,
                          derived_race,
                          survey_district_no,
                          survey_group_no,
                          include,
                          language_preference,
                          survey_location_no,
                          survey_loyalty_tier,
                          survey_foods_value_segment_no,
                          survey_non_fd_value_segment_no,
                          region_no,
                          roa_ind,
                          shopping_habit_segment_no,
                          subgroup_no,
                          till_no,
                          channel,
                          survey_params,
                          all_survey_params,
                          master_subscriber_key,
                          primary_customer_identifier,
                          customer_no,
                          populated_email_address,
                          populated_cell_no,
                          title,
                          first_name,
                          last_name,
                          location_name,
                          region_name,
                          st_store_type store_type,
                          race_no,
                          age,
                          tran_date,
                          item_qty,
                          selling_value,
                          month_tier,
                          csm_shopping_habit_segment_no,
                          foods_value_segment_no,
                          nonfoods_value_segment_no,
                          extracted_date,
                          last_updated_date,
                          merchandise_class,
                          survey_type,
                          survey_use_name
                     from (
                           select td.*, 
                                  row_number() over  (PARTITION BY master_subscriber_key  order by master_subscriber_key ) rec_id
                             from dwh_cust_performance.temp_csats_survey_detail  td  
                           )
                     where rec_id = 1  )
                     select survey_code,
                          survey_name,
                          survey_area,
                          minimum_spend_criteria,
                          minimum_units_criteria,
                          priority_order,
                          quota_minimum,
                          quota_maximum,
                          survey_updated_date,
                          updated_by,
                          survey_created_date,
                          created_by,
                          modified_ind,
                          survey_status,
                          same_csats_survey_eligibility,
                          diff_csats_survey_eligibility,
                          different_survey_eligibility,
                          age_band,
                          survey_area_no,
                          survey_business_unit_no,
                          survey_department_no,
                          derived_race,
                          survey_district_no,
                          survey_group_no,
                          include,
                          language_preference,
                          survey_location_no,
                          survey_loyalty_tier,
                          survey_foods_value_segment_no,
                          survey_non_fd_value_segment_no,
                          region_no,
                          roa_ind,
                          shopping_habit_segment_no,
                          subgroup_no,
                          till_no,
                          channel,
                          survey_params,
                          all_survey_params,
                          master_subscriber_key,
                          primary_customer_identifier,
                          customer_no,
                          populated_email_address,
                          populated_cell_no,
                          title,
                          first_name,
                          last_name,
                          location_name,
                          region_name,
                          store_type,
                          race_no,
                          age,
                          tran_date,
                          item_qty,
                          selling_value,
                          month_tier,
                          csm_shopping_habit_segment_no,
                          foods_value_segment_no,
                          nonfoods_value_segment_no,
                          extracted_date,
                          last_updated_date,
                          merchandise_class,
                          survey_type,
                          survey_use_name
                     from unique_rows order by selling_value desc, item_qty desc )

                     loop

                      insert into dwh_cust_performance.cust_csats_survey_detail (survey_code,
                                                                                survey_name,
                                                                                survey_area,
                                                                                minimum_spend_criteria,
                                                                                minimum_units_criteria,
                                                                                priority_order,
                                                                                quota_minimum,
                                                                                quota_maximum,
                                                                                survey_updated_date,
                                                                                updated_by,
                                                                                survey_created_date,
                                                                                created_by,
                                                                                modified_ind,
                                                                                survey_status,
                                                                                same_csats_survey_eligibility,
                                                                                diff_csats_survey_eligibility,
                                                                                different_survey_eligibility,
                                                                                age_band,
                                                                                survey_area_no,
                                                                                survey_business_unit_no,
                                                                                survey_department_no,
                                                                                derived_race,
                                                                                survey_district_no,
                                                                                survey_group_no,
                                                                                include,
                                                                                language_preference,
                                                                                survey_location_no,
                                                                                survey_loyalty_tier,
                                                                                survey_foods_value_segment_no,
                                                                                survey_non_fd_value_segment_no,
                                                                                region_no,
                                                                                roa_ind,
                                                                                shopping_habit_segment_no,
                                                                                subgroup_no,
                                                                                till_no,
                                                                                channel,
                                                                                survey_params,
                                                                                all_survey_params,
                                                                                master_subscriber_key,
                                                                                primary_customer_identifier,
                                                                                customer_no,
                                                                                populated_email_address,
                                                                                populated_cell_no,
                                                                                title,
                                                                                first_name,
                                                                                last_name,
                                                                                location_name,
                                                                                region_name,
                                                                                store_type,
                                                                                race_no,
                                                                                age,
                                                                                tran_date,
                                                                                item_qty,
                                                                                selling_value,
                                                                                month_tier,
                                                                                csm_shopping_habit_segment_no,
                                                                                foods_value_segment_no,
                                                                                nonfoods_value_segment_no,
                                                                                extracted_date,
                                                                                last_updated_date,
                                                                                merchandise_class,
                                                                                survey_type,
                                                                                survey_use_name) values (c_insert.survey_code,
                                                                                                           c_insert.survey_name,
                                                                                                           c_insert.survey_area,
                                                                                                           c_insert.minimum_spend_criteria,
                                                                                                           c_insert.minimum_units_criteria,
                                                                                                           c_insert.priority_order,
                                                                                                           c_insert.quota_minimum,
                                                                                                           c_insert.quota_maximum,
                                                                                                           c_insert.survey_updated_date,
                                                                                                           c_insert.updated_by,
                                                                                                           c_insert.survey_created_date,
                                                                                                           c_insert.created_by,
                                                                                                           c_insert.modified_ind,
                                                                                                           c_insert.survey_status,
                                                                                                           c_insert.same_csats_survey_eligibility,
                                                                                                           c_insert.diff_csats_survey_eligibility,
                                                                                                           c_insert.different_survey_eligibility,
                                                                                                           c_insert.age_band,
                                                                                                           c_insert.survey_area_no,
                                                                                                           c_insert.survey_business_unit_no,
                                                                                                           c_insert.survey_department_no,
                                                                                                           c_insert.derived_race,
                                                                                                           c_insert.survey_district_no,
                                                                                                           c_insert.survey_group_no,
                                                                                                           c_insert.include,
                                                                                                           c_insert.language_preference,
                                                                                                           c_insert.survey_location_no,
                                                                                                           c_insert.survey_loyalty_tier,
                                                                                                           c_insert.survey_foods_value_segment_no,
                                                                                                           c_insert.survey_non_fd_value_segment_no,
                                                                                                           c_insert.region_no,
                                                                                                           c_insert.roa_ind,
                                                                                                           c_insert.shopping_habit_segment_no,
                                                                                                           c_insert.subgroup_no,
                                                                                                           c_insert.till_no,
                                                                                                           c_insert.channel,
                                                                                                           c_insert.survey_params,
                                                                                                           c_insert.all_survey_params,
                                                                                                           c_insert.master_subscriber_key,
                                                                                                           c_insert.primary_customer_identifier,
                                                                                                           c_insert.customer_no,
                                                                                                           c_insert.populated_email_address,
                                                                                                           c_insert.populated_cell_no,
                                                                                                           c_insert.title,
                                                                                                           c_insert.first_name,
                                                                                                           c_insert.last_name,
                                                                                                           c_insert.location_name,
                                                                                                           c_insert.region_name,
                                                                                                           c_insert.store_type,
                                                                                                           c_insert.race_no,
                                                                                                           c_insert.age,
                                                                                                           c_insert.tran_date,
                                                                                                           c_insert.item_qty,
                                                                                                           c_insert.selling_value,
                                                                                                           c_insert.month_tier,
                                                                                                           c_insert.csm_shopping_habit_segment_no,
                                                                                                           c_insert.foods_value_segment_no,
                                                                                                           c_insert.nonfoods_value_segment_no,
                                                                                                           c_insert.extracted_date,
                                                                                                           c_insert.last_updated_date,
                                                                                                           c_insert.merchandise_class,
                                                                                                           c_insert.survey_type,
                                                                                                           c_insert.survey_use_name);
                      g_recs_inserted := g_recs_inserted + sql%rowcount;
                      g_recs_loaded := g_recs_loaded + 1;
                      exit when g_recs_loaded >= (c_survey.quota_maximum*(case when upper(c_survey.survey_type) <> 'WEBSITE' then g_search_days else g_online_search_days end));
                     end loop;

        --           dbms_output.put_line('Insert into CUST_CSATS_SURVEY_DETAIL - '||sql%rowcount);

                   l_text := ' Survey : '||to_char(c_survey.survey_code,'99')||' - Name : '||rpad(c_survey.survey_name,34,' ')||' - Area : '||rpad(c_survey.survey_area,10,' ')||' - Rows : '||g_recs_loaded||'/'||g_recs_in_temp||' For '||c_survey.quota_maximum;
                   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);          
                   commit;
           else
               l_text := '* Minimum count not met. Survey : '||c_survey.survey_code||' - Name : '||c_survey.survey_name||' - Area : '||c_survey.survey_area||' - Rows : '||g_recs_read;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
--               dbms_output.put_line('Stepping over INSERT into CUST_CSATS_SURVEY_DETAIL. ');
        end case;    

     commit;
    end loop;
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

end WH_PRF_CUST_400U;
