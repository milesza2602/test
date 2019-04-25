--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_007U_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_007U_TEST" (p_forall_limit in integer, p_success out boolean)
                         --      p_from_loc_no in integer,  p_to_loc_no in integer)
as
--**************************************************************************************************
--  Date:        March 2016
--  Author:      Barry Kirschner 
--  Remarks:     Rewrite of SP 005A + 005U into one  
--  Purpose:     Create the weekly temp swing system forecast table in the performance layer
--               with input ex RDF Sale table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_wk_rdf_wkfcst
--               Output - temp_lc_item_wk_rdf_sysfcst52t (unpivot temp table)
--                      - RTL_LOC_ITEM_WK_RDF_FCST52
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

g_forall_limit       integer       := dwh_constants.vc_forall_limit;
g_recs_read          integer       := 0;
g_recs_updated       integer       := 0;
g_recs_inserted      integer       := 0;
g_recs_hospital      integer       := 0;
g_count              number        := 0;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_max_week           number        := 0;
g_calendar_date      date          := trunc(sysdate);
g_fin_week_no        number        := 0;

g_from_loc_no        integer       := 0;  
g_to_loc_no          integer       := 99999;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_007U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD RDF WEEKLY SYS FCST FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--l_temp_min_year      number;
--l_temp_max_year      number;
--l_temp_min_wk_min_year  number;
--l_temp_min_wk_max_year  number;
--l_update_count          number(14,0);
--l_insert_count          number(14,0);

l_temp_min_year      number;
l_temp_max_year      number;

l_temp_min_wk_min_year  number;
l_temp_max_wk_min_year  number;

l_temp_min_wk_max_year  number;
l_temp_max_wk_max_year  number;

l_update_count          number(14,0);
l_insert_count          number(14,0);

l_split_year            number(1,0);


begin
    ----------------------------------------
    -- A. Initialize
    ----------------------------------------
    if  p_forall_limit is not null 
    and p_forall_limit > dwh_constants.vc_forall_minimum then
        g_forall_limit := p_forall_limit;
    end if;
 --   if  p_from_loc_no is null then
--        g_from_loc_no := p_from_loc_no;
 --   end if;
 --   if  p_to_loc_no is not null then
 --       g_to_loc_no   := p_to_loc_no; 
 --   end if;
    
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_LOC_ITEM_WK_RDF_FCST EX FOUNDATION STARTED AT '||  
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    -- Look up batch date from dim_control ...
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOCATION RANGE BEING PROCESSED - '||g_from_loc_no||' to '||g_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
   
   --truncate temp table 
    execute immediate 'truncate table DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T';
    commit;
    l_text := 'TEMP TABLE temp_lc_item_wk_rdf_sysfcst52t TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    ----------------------------------------------------------------------------------------------
    
    -- get current date/week parameters ...
    select  fin_week_no, 
            calendar_date
    into    g_fin_week_no, g_calendar_date
    from    dim_calendar
    where   calendar_date = (select trunc(sysdate) from dual);
    
    l_text := 'CURRENT DATE BEING PROCESSED - '||g_calendar_date||' g_week_no - '||g_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    -- set up fin week calculators ...
    select max(fin_week_no) + 1
    into   g_max_week
    from   dim_calendar
    where  fin_year_no = (select fin_year_no
                          from   dim_calendar
                          where  calendar_date = g_date
                         );
    commit;

    l_text := 'No of weeks in the Fin_year + 1 for calculation in the code  - ' ||g_max_week;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
    l_text := 'Starting insert into temp table ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_split_year:=0;
    
      
    ----------------------------------------
    -- B. Un SRC pivot to TEMP (52 weeks)
    ----------------------------------------
    -- unpivot input FND table to temp table for later processing ...
    -- each column set in source is unpivoted into single rows with fin. week (FIN_WEEK_NO) set dependant on original week (FIN_WEEK_NO)
    -- original fin. week is always set from col. 1 (01)
    
   insert /*+ parallel(sys,8) */ into DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T sys
   select /*+ full(sys_ilv) parallel(sys_ilv,8) */
            sk1_location_no,
            sk1_item_no,
            case when (fin_week_no +  to_number(substr(syscol,4,2))) > g_max_week
                 then  fin_week_no + (to_number(substr(syscol,4,2))  - g_max_week)
                 else  fin_week_no + (to_number(substr(syscol,4,2))  - 1)
            end,                                                                -- fin_week_no
            fin_week_no,                                                        -- FIN_WEEK_NO_ORIG
            case when (fin_week_no + to_number(substr(syscol,4,2)))  > g_max_week
                 then  fin_year_no + 1
                 else  fin_year_no
            end,                                                                -- FIN_YEAR_NO
            sysfcst,                                                            -- SALES_WKLY_SYS_FCST_QTY
            g_date,
            appfcst,                                                             -- SALES_WKLY_APP_FCST_QTY
            STANDARD_UOM_CODE,
            RANDOM_MASS_IND,
            REG_RSP,
            STATIC_MASS,
            SK2_LOCATION_NO,
            SK2_ITEM_NO,
            THIS_WEEK_START_DATE
from (  
    SELECT /*+ full(di) full(dl) full(zi) full(dc) full(dlh) full(dih) full(sys_ilv) parallel(sys_ilv,8) */  sys_ilv.*,
    di.sk1_item_no,
    dl.sk1_location_no,
    dih.sk2_item_no ,
    dlh.sk2_location_no,
    di.standard_uom_code,
    di.random_mass_ind,
    zi.reg_rsp,
    di.static_mass,
    dc.this_week_start_date

 from   dwh_foundation.fnd_rtl_loc_item_wk_rdf_wkfcst   sys_ilv 
           join     dim_item                di  on (sys_ilv.item_no     = di.item_no)
           join     dim_location            dl  on (sys_ilv.location_no = dl.location_no)
           join     dim_calendar            dc  on (sys_ilv.fin_year_no = dc.fin_year_no  and sys_ilv.fin_week_no = dc.fin_week_no              and dc.fin_day_no   = 4)
           join     dim_location_hist       dlh on (sys_ilv.location_no = dlh.location_no and dc.calendar_date between dlh.sk2_active_from_date and dlh.sk2_active_to_date)
           join     dim_item_hist           dih on (sys_ilv.item_no     = dih.item_no     and dc.calendar_date between dih.sk2_active_from_date and dih.sk2_active_to_date)
           join     fnd_zone_item           zi  on (sys_ilv.location_no = dl.location_no  and dl.wh_fd_zone_group_no = zi.zone_group_no         and dl.wh_fd_zone_no = zi.zone_no 
                                                                                          and sys_ilv.item_no = zi.item_no)    
where sys_ilv.last_updated_date = g_date
) sys_ilv
unpivot include nulls (
                           (sysfcst,appfcst) for syscol in (
                                                            (wk_01_sys_fcst_qty,wk_01_app_fcst_qty),
                                                            (wk_02_sys_fcst_qty,wk_02_app_fcst_qty),
                                                            (wk_03_sys_fcst_qty,wk_03_app_fcst_qty),
                                                            (wk_04_sys_fcst_qty,wk_04_app_fcst_qty),
                                                            (wk_05_sys_fcst_qty,wk_05_app_fcst_qty),
                                                            (wk_06_sys_fcst_qty,wk_06_app_fcst_qty),
                                                            (wk_07_sys_fcst_qty,wk_07_app_fcst_qty),
                                                            (wk_08_sys_fcst_qty,wk_08_app_fcst_qty),
                                                            (wk_09_sys_fcst_qty,wk_09_app_fcst_qty),
                                                            (wk_10_sys_fcst_qty,wk_10_app_fcst_qty),
                                                            (wk_11_sys_fcst_qty,wk_11_app_fcst_qty),
                                                            (wk_12_sys_fcst_qty,wk_12_app_fcst_qty),
                                                            (wk_13_sys_fcst_qty,wk_13_app_fcst_qty),
                                                            (wk_14_sys_fcst_qty,wk_14_app_fcst_qty),
                                                            (wk_15_sys_fcst_qty,wk_15_app_fcst_qty),
                                                            (wk_16_sys_fcst_qty,wk_16_app_fcst_qty),
                                                            (wk_17_sys_fcst_qty,wk_17_app_fcst_qty),
                                                            (wk_18_sys_fcst_qty,wk_18_app_fcst_qty),
                                                            (wk_19_sys_fcst_qty,wk_19_app_fcst_qty),
                                                            (wk_20_sys_fcst_qty,wk_20_app_fcst_qty),
                                                            (wk_21_sys_fcst_qty,wk_21_app_fcst_qty),
                                                            (wk_22_sys_fcst_qty,wk_22_app_fcst_qty),
                                                            (wk_23_sys_fcst_qty,wk_23_app_fcst_qty),
                                                            (wk_24_sys_fcst_qty,wk_24_app_fcst_qty),
                                                            (wk_25_sys_fcst_qty,wk_25_app_fcst_qty),
                                                            (wk_26_sys_fcst_qty,wk_26_app_fcst_qty)))
;
    
    g_recs_inserted := + SQL%ROWCOUNT;
    commit;
--
--    l_text := 'TEMP TABLE temp_lc_item_wk_rdf_sysfcst52t POPULATED';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    commit;
--    
--    l_text := 'Running GATHER_TABLE_STATS ON temp_lc_item_wk_rdf_sysfcst52t';
--   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--   
--   EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
--
--   DBMS_STATS.gather_table_stats ('dwh_performance', 'temp_lc_item_wk_rdf_sysfcst52t', DEGREE => 16);
--
--   l_text := 'GATHER_TABLE_STATS ON temp_lc_item_wk_rdf_sysfcst52t completed';
--   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--
---- BELOW ADDED TO GIVE DAILY STATS ON UPDATES VERSUS INSERTS
--
    l_text := 'Check updates vs inserts between temp table and FCST52 table ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   select /*+ parallel (a,64) full(a) */  min(fin_year_no) 
     into l_temp_min_year
     from DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T a;
     
   select /*+ parallel (a,6) full(a) */  min(fin_week_no), max(fin_week_no) 
     into l_temp_min_wk_min_year, l_temp_max_wk_min_year
     from DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T a
     where fin_year_no = l_temp_min_year; 

   select /*+ parallel (a,6) full(a) */  max(fin_year_no) 
     into l_temp_max_year
     from DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T a;
     
   select /*+ parallel (a,6) full(a) */  min(fin_week_no), max(fin_week_no)
     into l_temp_min_wk_max_year, l_temp_max_wk_max_year
     from DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T a
     where fin_year_no = l_temp_max_year; 
     
    l_text := 'min year on temp table = '|| l_temp_min_year ||' - min week for min year = ' || l_temp_min_wk_min_year ||' / max week for min year = ' || l_temp_max_wk_min_year ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     
    l_text := 'max year on temp table = '|| l_temp_max_year ||' - min week for max year = ' || l_temp_min_wk_max_year ||' / max week for max year = ' || l_temp_max_wk_max_year ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    
    
    IF l_temp_min_year <> l_temp_max_year THEN
       l_split_year := 1;
       l_text := 'PERIOD SPANS 2 YEARS, RUNNING THE SPLIT-YEAR MERGE';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    else
       l_text := 'PERIOD ONLY ON 1 YEAR, RUNNING THE SINGLE-YEAR MERGE';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    end if;

  --DO THE UPDATE / INSERT CHECK 
--  with aa as (
--    select /*+ parallel (a,8) full(a) */ unique c.sk1_LOCATION_NO, b.sk1_ITEM_NO, FIN_YEAR_NO, FIN_WEEK_NO 
--      from DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T a, dim_item b, dim_location c
--     where a.item_no = b.item_no
--       and a.location_no = c.location_no
--             ), 
--       cc as (
--         select /*+ parallel(aa,8) full(aa) */ * from aa
--
--   INTERSECT
--
--    select /*+ parallel (bb,8) full(bb) */ sk1_LOCATION_NO, sk1_ITEM_NO, FIN_YEAR_NO, FIN_WEEK_NO 
--      from dwh_performance.RTL_LOC_ITEM_WK_RDF_FCST52 bb
--     where ((fin_year_no = l_temp_min_year and fin_week_no >= l_temp_min_wk_min_year) or (fin_year_no = l_temp_max_year and fin_week_no >= l_temp_min_wk_max_year))
--            ) 
--    select count(*) into l_update_count from cc;
--    
--    l_insert_count := g_recs_inserted - l_update_count;
--
--    l_text := 'number of records that will be inserted = ' || l_insert_count;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    
--    l_text := 'number of records that will be updated = ' || l_update_count;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--  NORMAL PROCESSING CONTINUES..
 
--    l_text := 'DROPPING PK CONSTRAINT - PK_P_RTL_LC_ITM_WK_FCST';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    execute immediate 'alter table dwh_performance.RTL_LOC_ITEM_WK_RDF_FCST52 drop constraint PK_P_NEW_LC_ITM_WK_FCST52';
--    l_text := 'PK CONSTRAINT DROPPED';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'Starting merge into RTL table ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    -----------------------------------
    -- C. PRF Table load ...
    -----------------------------------
    
    if l_split_year = 1 then
       --
       -- SPLIT YEAR MERGE
       --
        merge /*+ parallel(tgt,8) */ into dwh_performance.RTL_LOC_ITEM_WK_RDF_FCST52 tgt 
        USING
          (select  /*+ parallel(sys_ilv,8) full(sys_ilv) */
                    sk1_location_no                                                         SK1_LOCATION_NO, 
                    sk1_item_no                                                             SK1_ITEM_NO,
                    fin_year_no                                                             FIN_YEAR_NO,
                    fin_week_no                                                             FIN_WEEK_NO,
                    sk2_item_no                                                             SK2_ITEM_NO,
                    sk2_location_no                                                         SK2_LOCATION_NO,
                   ('W'||fin_year_no||fin_week_no)                                          FIN_WEEK_CODE,
                    sales_wkly_sys_fcst_qty                                                 SALES_WK_SYS_FCST_QTY,
                    case when (standard_uom_code = 'EA' and sys_ilv.random_mass_ind = 1)
                         then  sales_wkly_sys_fcst_qty * reg_rsp * static_mass
                         else  sales_wkly_sys_fcst_qty * reg_rsp
                    end                                                                     SALES_WK_SYS_FCST,
                    sales_wkly_app_fcst_qty                                                 SALES_WK_APP_FCST_QTY,
                    case when (standard_uom_code = 'EA' and random_mass_ind = 1)
                         then  sales_wkly_app_fcst_qty * reg_rsp * static_mass
                         else  sales_wkly_app_fcst_qty * reg_rsp
                    end                                                                    SALES_WK_APP_FCST,
                    g_date                                                                 LAST_UPDATED_DATE,
                    this_week_start_date                                                THIS_WEEK_START_DATE
          
           from     DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T   sys_ilv 

          ) src
           on      (tgt.sk1_location_no = src.sk1_location_no
           and      tgt.sk1_item_no     = src.sk1_item_no
           and      tgt.FIN_YEAR_NO     = src.FIN_YEAR_NO
           and      tgt.FIN_WEEK_NO     = src.FIN_WEEK_NO
           
           and (
                 (tgt.fin_year_no = l_temp_min_year and tgt.fin_week_no between l_temp_min_wk_min_year and l_temp_max_wk_min_year)
            or   (tgt.fin_year_no = l_temp_max_year and tgt.fin_week_no between l_temp_min_wk_max_year and l_temp_max_wk_max_year)
               )
                   )  
    
        when matched then
        update set  SK2_LOCATION_NO	      =	src.SK2_LOCATION_NO,
                    SK2_ITEM_NO	          =	src.SK2_ITEM_NO,
                    FIN_WEEK_CODE	        =	src.FIN_WEEK_CODE,
                    SALES_WK_SYS_FCST_QTY	=	src.SALES_WK_SYS_FCST_QTY,
                    SALES_WK_SYS_FCST	    =	src.SALES_WK_SYS_FCST,
                    SALES_WK_APP_FCST_QTY =	src.SALES_WK_APP_FCST_QTY,
                    SALES_WK_APP_FCST	    =	src.SALES_WK_APP_FCST,
                    LAST_UPDATED_DATE	    =	src.LAST_UPDATED_DATE,
                    THIS_WEEK_START_DATE  =	src.THIS_WEEK_START_DATE
                    
                     
        when not matched then
        insert values
                  ( src.SK1_LOCATION_NO,
                    src.SK1_ITEM_NO,
                    src.FIN_YEAR_NO,
                    src.FIN_WEEK_NO,
                    src.SK2_LOCATION_NO,
                    src.SK2_ITEM_NO,
                    src.FIN_WEEK_CODE,
                    src.SALES_WK_SYS_FCST_QTY,
                    src.SALES_WK_SYS_FCST,
                    src.SALES_WK_APP_FCST_QTY,
                    src.SALES_WK_APP_FCST,
                    src.LAST_UPDATED_DATE,
                    src.THIS_WEEK_START_DATE    
                  );
                  

     else
       --
       -- SINGLE YEAR MERGE
       --
        merge /*+ parallel(tgt,8) */ into dwh_performance.RTL_LOC_ITEM_WK_RDF_FCST52 tgt 
        USING
          (select  /*+ parallel(sys_ilv,8) full(sys_ilv) */
                    sk1_location_no                                               SK1_LOCATION_NO, 
                    sk1_item_no                                                   SK1_ITEM_NO,
                    fin_year_no                                                   FIN_YEAR_NO,
                    fin_week_no                                                   FIN_WEEK_NO,
                    sk2_item_no                                                   SK2_ITEM_NO,
                    sk2_location_no                                               SK2_LOCATION_NO,
                   ('W'||fin_year_no||fin_week_no)                                FIN_WEEK_CODE,
                    sales_wkly_sys_fcst_qty                                       SALES_WK_SYS_FCST_QTY,
                    case when (standard_uom_code = 'EA' and random_mass_ind = 1)
                         then  sales_wkly_sys_fcst_qty * reg_rsp * static_mass
                         else  sales_wkly_sys_fcst_qty * reg_rsp
                    end                                                           SALES_WK_SYS_FCST,
                    sales_wkly_app_fcst_qty                                       SALES_WK_APP_FCST_QTY,
                    case when (standard_uom_code = 'EA' and random_mass_ind = 1)
                         then  sales_wkly_app_fcst_qty * reg_rsp * static_mass
                         else  sales_wkly_app_fcst_qty * reg_rsp
                    end                                                           SALES_WK_APP_FCST,
                    g_date                                                        LAST_UPDATED_DATE,
                    this_week_start_date                                          THIS_WEEK_START_DATE
          
           from     DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T   sys_ilv 
 
     --where sys_ilv.fin_year_no = l_temp_min_year and sys_ilv.fin_week_no between l_temp_min_wk_min_year and l_temp_max_wk_min_year
         
               
          ) src
           on      (tgt.sk1_location_no = src.sk1_location_no
           and      tgt.sk1_item_no     = src.sk1_item_no
           and      tgt.FIN_YEAR_NO     = src.FIN_YEAR_NO
           and      tgt.FIN_WEEK_NO     = src.FIN_WEEK_NO
           
          and tgt.fin_year_no = l_temp_min_year 
          and tgt.fin_week_no between l_temp_min_wk_min_year and l_temp_max_wk_min_year  
        )
    
        when matched then
        update set  SK2_LOCATION_NO	      =	src.SK2_LOCATION_NO,
                    SK2_ITEM_NO	          =	src.SK2_ITEM_NO,
                    FIN_WEEK_CODE	        =	src.FIN_WEEK_CODE,
                    SALES_WK_SYS_FCST_QTY	=	src.SALES_WK_SYS_FCST_QTY,
                    SALES_WK_SYS_FCST	    =	src.SALES_WK_SYS_FCST,
                    SALES_WK_APP_FCST_QTY =	src.SALES_WK_APP_FCST_QTY,
                    SALES_WK_APP_FCST	    =	src.SALES_WK_APP_FCST,
                    LAST_UPDATED_DATE	    =	src.LAST_UPDATED_DATE,
                    THIS_WEEK_START_DATE  =	src.THIS_WEEK_START_DATE
                    
       when not matched then
        insert values
                  ( src.SK1_LOCATION_NO,
                    src.SK1_ITEM_NO,
                    src.FIN_YEAR_NO,
                    src.FIN_WEEK_NO,
                    src.SK2_LOCATION_NO,
                    src.SK2_ITEM_NO,
                    src.FIN_WEEK_CODE,
                    src.SALES_WK_SYS_FCST_QTY,
                    src.SALES_WK_SYS_FCST,
                    src.SALES_WK_APP_FCST_QTY,
                    src.SALES_WK_APP_FCST,
                    src.LAST_UPDATED_DATE,
                    src.THIS_WEEK_START_DATE    
                  );
                  
    end if;
    
    g_recs_updated := + SQL%ROWCOUNT;
    commit;
    
    l_text := 'PRF TABLE RTL_LOC_ITEM_WK_RDF_FCST52 Updated/Inserted';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_processed||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        
--    l_text := 'ADDING PK CONSTRAINT - PK_P_NEW_LC_ITM_WK_FCST52';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    execute immediate 'ALTER TABLE DWH_PERFORMANCE.RTL_LOC_ITEM_WK_RDF_FCST52 ADD CONSTRAINT "PK_P_NEW_LC_ITM_WK_FCST52" PRIMARY KEY ("SK1_LOCATION_NO", "SK1_ITEM_NO", "FIN_YEAR_NO", "FIN_WEEK_NO")';
--    l_text := 'PK CONSTRAINT ADDED';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      
--    l_text := 'RUNNING GATHER STATS ON RTL_LOC_ITEM_WK_RDF_FCST52';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','RTL_LOC_ITEM_WK_RDF_FCST52', DEGREE => 32);
--    l_text := 'GATHER STATS COMPLETE';
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    execute immediate 'alter session disable parallel dml';
    l_text := 'PARALLEL DML DISABLED';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    -----------------------------------
    -- D. Wrap up
    -----------------------------------
    --execute immediate 'truncate table DWH_PERFORMANCE.TEMP_LC_ITEM_WK_RDF_SYSFCST52T';
    --commit;
    --l_text := 'TEMP TABLE temp_lc_item_wk_rdf_sysfcst52t TRUNCATED.';
    --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
       RAISE;
       
end WH_PRF_RDF_007U_TEST;
