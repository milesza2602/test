--------------------------------------------------------
--  DDL for Procedure WH_FND_TRUNC_PARTN_MP_023U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_TRUNC_PARTN_MP_023U" (p_success out boolean) as
--**************************************************************************************************
--  Date:        Sep 2015
--  Author:      K. Lehabe
--  Purpose:     Truncate current month and future subpartitions on table prior to load.
--
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************

g_sub                 integer       :=  0;
g_no_of_months        number(4)    :=  0;
g_no_of_years         number(3)     :=  0;
g_partn_year          number(4)     :=  0;
g_fin_month_no        number(4) ;
g_fin_year_no         number(4) ;
g_subpartn_month      number(4) ;
g_date                date;
trunc_stmt            varchar2(1500);
trunc_sub_stmt        varchar2(1500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_TRUNC_PARTN_MP_023U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TRUNCATE CURRENT MONTH AND FUTURE PARTITIONS ON FND_RTL_LOC_DEPT_MTH_PLAN_MP';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




------------------------------------------------------------------------------------------------------------
--  TRUNCATE PARTITIONS IN THE FUTURE
---------------------------------------------------------------------------------------------------------

procedure local_trunc_future_partn as
 begin


    l_text := 'RUN DATE, NO OF YEARS FORWARD TO TRUNCATE AND END DATE:- '||g_date||' '||g_no_of_years;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


   for g_sub in 1 .. g_no_of_years
     loop

    g_partn_year := g_fin_year_no;
    g_partn_year := g_partn_year +  g_sub  ;

      trunc_stmt           :=  '  alter table dwh_foundation.FND_RTL_LOC_DEPT_MTH_PLAN_MP truncate partition  for (' || g_partn_year || ')';

    l_text := 'PARTITION TO BE TRUNCATED FOR YEAR:- '||g_partn_year ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'DDL Statement :- '||trunc_stmt ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate trunc_stmt;

    commit;
    DBMS_LOCK.sleep(0.25);      -- added 25 Oct 2016 to prevent shared pool issues
   end loop;

end local_trunc_future_partn;
------------------------------------------------------------------------------------------------------------
--  TRUNCATE PARTITIONS IN THE FUTURE
---------------------------------------------------------------------------------------------------------


procedure local_trunc_subpartn as
 begin

      l_text := 'NUMBER OF MONTHS TILL FIN YEAR END:- '||g_no_of_months || ' - CURRENT FIN MONTH is ' || g_fin_month_no;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      for g_sub in 0 .. g_no_of_months
     loop

    g_subpartn_month := g_fin_month_no;
     g_subpartn_month := g_subpartn_month +  g_sub  ;

 -- For current month it will be fin_month_no + 0, then the next month Fin_month_no + 1, etc

      trunc_sub_stmt           :=  '  alter table dwh_foundation.FND_RTL_LOC_DEPT_MTH_PLAN_MP truncate SUBPARTITION  for (' || g_fin_year_no || ',' || g_subpartn_month||')';


    l_text := 'SUBPARTITION TO BE TRUNCATED FOR YEAR AND MONTH:- '||g_fin_year_no || g_subpartn_month;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'DDL Statement :- '||trunc_sub_stmt ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate trunc_sub_stmt;

    commit;
    DBMS_LOCK.sleep(0.25);      -- added 25 Oct 2016 to prevent shared pool issues

   end loop;
 end local_trunc_subpartn;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    p_success := false;



    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'TRUNCATE MONTHLY PARTIONS ON  FND_RTL_LOC_DEPT_MTH_PLAN_MP'||' STARTED '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;

    select fin_year_no, fin_month_no
    into   g_fin_year_no, g_fin_month_no
    from   dim_calendar
    where  calendar_date = g_date;

    ---Check how many years in  the future

        select count(distinct fin_year_no)
        into g_no_of_years
        from  dwh_foundation.FND_RTL_LOC_DEPT_MTH_PLAN_MP
        where fin_year_no > g_fin_year_no;

          select count(distinct fin_month_no)
        into g_no_of_months
        from  dwh_foundation.FND_RTL_LOC_DEPT_MTH_PLAN_MP
        where fin_year_no = g_fin_year_no
        and fin_month_no > g_fin_month_no;



     local_trunc_future_partn;
    local_trunc_subpartn;

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,'','','','','');
    l_text := 'TRUNCATE MONTLHY PARTITIONS ON FND_RTL_LOC_DEPT_MTH_PLAN_MP ENDED' ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
      when others then
       l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

end WH_FND_TRUNC_PARTN_MP_023U;
