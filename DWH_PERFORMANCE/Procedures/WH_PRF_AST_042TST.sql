--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_042TST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_042TST" 
(p_forall_limit in integer,p_success out boolean) as
--
--**************************************************************************************************
--  Date:        Jan 2013
--  Author:      Wendy Lyttle
--  Purpose:     Create the weekly item rollup CHBD catalog table in the performance layer
--               with input ex RP daily item catalog table from performance layer.
--
--               Cloned from WH_PRF_RP_011U
--
-- Runtime Instructions :
--                We loop for 6 weeks due to the fact that Sales data can be up to 6 weeks late.
--
--
--
--  Tables:      Input  - RTL_loc_item_dy_ast_catlg
--               Output - rtl_loc_item_wk_ast_catlg
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit              integer       :=  dwh_constants.vc_forall_limit;
g_recs_read                 integer       :=  0;
g_recs_updated              integer       :=  0;
g_recs_inserted             integer       :=  0;
g_recs_hospital             integer       :=  0;
g_error_count               number        :=  0;
g_error_index               number        :=  0;
g_count                     number        :=  0;
g_rec_out                                 rtl_loc_item_wk_ast_catlg%rowtype;
g_found                     boolean;

g_date                      date          := trunc(sysdate);
g_last_analyzed_date        date          := sysdate;
g_start_date_time           date          := sysdate;
g_xpart_name                varchar2(32);
g_wkpart_name               varchar2(32);
g_xsubpart_name             varchar2(32);
g_wksubpart_name            varchar2(32);
g_part_name                 varchar2(32);
g_subpart_name              varchar2(32);

g_prev_month              number        := 0;
g_fin_year_no               number        := 0;
g_fin_month_no              number        := 0;
g_fin_week_no               number        := 0;
g_sub                       number        := 0;
g_subp1                     number        := 0;
g_cnt number := 0;
g_sub1                      number        := 0;
g_start_week                integer       :=  0;
g_start_year                integer       :=  0;
g_this_week_start_date      date          := trunc(sysdate);
g_this_week_end_date        date          := trunc(sysdate);
g_fin_week_code             varchar2(7);
--
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_042U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rpl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rpl;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLUP THE AST WEEKLY ITEM CATALOG FACTS EX PERFORMANCE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
--

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

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.DIM_CONTROL(G_DATE);
    
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      

    FOR g_sub IN 0..5
      LOOP
        g_recs_inserted := 0;
        select fin_year_no, fin_week_no, this_week_start_date, this_week_end_date, fin_week_code
        into   g_start_year, g_start_week, g_this_week_start_date, g_this_week_end_date, g_fin_week_code
        from   dim_calendar
        WHERE calendar_date = g_date - (g_sub * 7);

            G_SUBp1 := G_SUB+1;
            l_text := '   --------------------------------';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
            l_text := '---- WEEK '||G_SUBp1||' --WEEK ='||g_start_year||'-'||g_start_week||'---PERIOD='||g_this_week_start_date||'-'||g_this_week_end_date;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

            select fin_year_no,  fin_month_no
            into   g_fin_year_no, g_fin_month_no
            from   dim_calendar
            where calendar_date = g_this_week_start_date + 1
            group by fin_year_no,  fin_month_no;


        ----------------------------------------------------------
        -- subpartition_name example = RTL_LIDAC_040313
        -- This will do the update stats for the FIRST DAY(subpartition level) OF THE WEEK(partition level)
        ----------------------------------------------------------
            l_text := '   ---- SUBPARTITION ----';
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

                g_subpart_name := 'RTL_LIDAC_'||to_char((g_this_week_start_date + 1 ),'ddmmyy');
                l_text := '        subpartition='||g_subpart_name;
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                dbms_stats.gather_table_stats ('DWH_PERFORMANCE',
                                                'RTL_LOC_ITEM_DY_AST_CATLG',
                                                g_subpart_name,
                                                granularity => 'SUBPARTITION',
                                                degree => 8);
               commit;
           l_text := '  Subpartition UPDATE STATS COMPLETED '||g_start_year||g_start_week;
           dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--NOTE: WHEN WE GO INTO A NEW MONTH THE JOB RUNS VERY LONG SO WE NEED TO RUN STATS ON THE MONTHLY PARTITION AS BELOW:
--THIS SHOULD BE AUTOMATED WITH THE PART NAME CHANGING EACH MONTH 'TP_RTL_LIDAC_M20179'
--------------------------------------------------------------------------------------------------------------------           
--begin
--DBMS_STATS.GATHER_TABLE_STATS (ownname => 'DWH_PERFORMANCE', tabname => 'RTL_LOC_ITEM_DY_AST_CATLG',
--partname => 'TP_RTL_LIDAC_M20179', estimate_percent => 10, degree => 16, granularity => 'PARTITION', cascade => TRUE);
--end;            
---------------------------------------------------------------------------------------------------------------------- 

          if g_fin_month_no <> g_prev_month
             then
                l_text := '   ---- PARTITION ----';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

                g_part_name := 'TP_RTL_LIDAC_M'||g_fin_year_no||g_fin_month_no;
                l_text := '        partition='||g_part_name;
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                dbms_stats.gather_table_stats ('DWH_PERFORMANCE',
                                                'RTL_LOC_ITEM_DY_AST_CATLG',
                                                g_part_name,
                                                estimate_percent => 10, degree => 16, granularity => 'PARTITION', cascade => TRUE);
               commit;
               l_text := '  Partition UPDATE STATS COMPLETED '||g_fin_year_no||g_fin_month_no;
               dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          end if;
          g_prev_month := g_fin_month_no;
          


  

     g_recs_read := g_recs_read + SQL%ROWCOUNT;
     g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;


     l_text := 'Insert NEW:- RECS =  '||g_recs_inserted||' '||g_this_week_start_date||'  To '||g_this_week_end_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

     commit;


    end loop;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
--
--
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
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;
    p_success := true;
  exception
--
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


END WH_PRF_AST_042TST;
