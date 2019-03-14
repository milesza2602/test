-- ****** Object: Procedure W7131037.WH_PRF_CUST_295U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_295U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        Oct 2015
--  Author:      Kgomotso Lehabe
--  Purpose:    insert curent month values from foundation and roll down of monthly values to create new invlolvement scores
--  Tables:      Input  - temp_db_dept_month_involve
--               Output - cust_db_dept_month_involve
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  10000;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_yr_00               number;
g_mn_00               number;
g_last_yr             number;
g_last_mn             number;
g_this_mn_start_date  date;
g_this_mn_end_date    date;
g_run_date            date;
g_stmt                varchar(500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_295U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST_DB_DEPT_MONTH_INVOLVE EX TEMP_DB_DEPT_MONTH_INVOLVE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF CUST_DB_DEPT_MONTH_INVOLVE EX TEMP_DB_DEPT_MONTH_INVOLVE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select last_yr_fin_year_no,last_mn_fin_month_no
    into   g_yr_00,g_mn_00
    from dim_control;

    select unique this_mn_start_date,this_mn_end_date
    into   g_this_mn_start_date, g_this_mn_end_date
    from   dim_calendar
    where  fin_year_no  = g_yr_00 and
           fin_month_no = g_mn_00 and
           fin_day_no   = 1;

   g_last_mn := g_mn_00 - 1;
   g_last_yr := g_yr_00;
   if g_last_mn = 0 then
      g_last_mn := 12;
      g_last_yr := g_last_yr - 1;
   end if;

   l_text := 'Month being processed:= '||
             g_this_mn_start_date || g_this_mn_end_date ||g_yr_00||g_mn_00;

   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   g_stmt      := 'Alter table  W7131037.CUST_DB_DEPT_MONTH_INVOLVE truncate  subpartition for ('||g_yr_00||','||g_mn_00||') update global indexes';
   l_text      := g_stmt;

   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   execute immediate 'alter session force parallel dml';
--execute immediate 'alter session enable parallel dml';

--    execute immediate g_stmt;
  /* g_run_date := g_this_mn_end_date + 10;
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;

   l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); */

   l_text := 'DISABLING PK CONSTRAINT - PK_CST_DEPT_MONTH_INV';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate 'alter table W7131037.CUST_DB_DEPT_MONTH_INVOLVE disable constraint PK_CST_DEPT_MONTH_INV';
   l_text := 'PK CONSTRAINT DISABLED';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert /*+ APPEND parallel (prfi,16) */ into cust_db_dept_month_involve prfi
   select  /*+ FULL(prf)  parallel (prf,8)  */
            2014, 12,
            primary_customer_identifier,
            department_no,
            customer_no,
            0,0,0,
            prf.NUM_ITEM_SUM_YR1_MN01,   prf.SALES_SUM_YR1_MN01,    prf.INVOLVEMENT_SCORE_YR1_MN01,
            PRF.NUM_ITEM_SUM_YR1_MN02,   PRF.SALES_SUM_YR1_MN02,    PRF.INVOLVEMENT_SCORE_YR1_MN02,
            prf.NUM_ITEM_SUM_YR1_MN03,   prf.SALES_SUM_YR1_MN03,    prf.INVOLVEMENT_SCORE_YR1_MN03,
            prf.NUM_ITEM_SUM_YR1_MN04,   prf.SALES_SUM_YR1_MN04,    prf.INVOLVEMENT_SCORE_YR1_MN04,
            prf.NUM_ITEM_SUM_YR1_MN05,   prf.SALES_SUM_YR1_MN05,    prf.INVOLVEMENT_SCORE_YR1_MN05,
            prf.NUM_ITEM_SUM_YR1_MN06,   prf.SALES_SUM_YR1_MN06,    prf.INVOLVEMENT_SCORE_YR1_MN06,
            prf.NUM_ITEM_SUM_YR1_MN07,   prf.SALES_SUM_YR1_MN07,    prf.INVOLVEMENT_SCORE_YR1_MN07,
            prf.NUM_ITEM_SUM_YR1_MN08,   prf.SALES_SUM_YR1_MN08,    prf.INVOLVEMENT_SCORE_YR1_MN08,
            prf.NUM_ITEM_SUM_YR1_MN09,   prf.SALES_SUM_YR1_MN09,    prf.INVOLVEMENT_SCORE_YR1_MN09,
            prf.NUM_ITEM_SUM_YR1_MN10,   prf.SALES_SUM_YR1_MN10,    prf.INVOLVEMENT_SCORE_YR1_MN10,
            prf.NUM_ITEM_SUM_YR1_MN11,   prf.SALES_SUM_YR1_MN11,    prf.INVOLVEMENT_SCORE_YR1_MN11,
            prf.NUM_ITEM_SUM_YR1_MN12,   prf.SALES_SUM_YR1_MN12,    prf.INVOLVEMENT_SCORE_YR1_MN12,
            prf.NUM_ITEM_SUM_YR2_MN01,   prf.SALES_SUM_YR2_MN01,    prf.INVOLVEMENT_SCORE_YR2_MN01,
            prf.NUM_ITEM_SUM_YR2_MN02,   prf.SALES_SUM_YR2_MN02,    prf.INVOLVEMENT_SCORE_YR2_MN02,
            prf.NUM_ITEM_SUM_YR2_MN03,   prf.SALES_SUM_YR2_MN03,    prf.INVOLVEMENT_SCORE_YR2_MN03,
            prf.NUM_ITEM_SUM_YR2_MN04,   prf.SALES_SUM_YR2_MN04,    prf.INVOLVEMENT_SCORE_YR2_MN04,
            prf.NUM_ITEM_SUM_YR2_MN05,   prf.SALES_SUM_YR2_MN05,    prf.INVOLVEMENT_SCORE_YR2_MN05,
            prf.NUM_ITEM_SUM_YR2_MN06,   prf.SALES_SUM_YR2_MN06,    prf.INVOLVEMENT_SCORE_YR2_MN06,
            prf.NUM_ITEM_SUM_YR2_MN07,   prf.SALES_SUM_YR2_MN07,    prf.INVOLVEMENT_SCORE_YR2_MN07,
            prf.NUM_ITEM_SUM_YR2_MN08,   prf.SALES_SUM_YR2_MN08,    prf.INVOLVEMENT_SCORE_YR2_MN08,
            prf.NUM_ITEM_SUM_YR2_MN09,   prf.SALES_SUM_YR2_MN09,    prf.INVOLVEMENT_SCORE_YR2_MN09,
            prf.NUM_ITEM_SUM_YR2_MN10,   prf.SALES_SUM_YR2_MN10,    prf.INVOLVEMENT_SCORE_YR2_MN10,
            prf.NUM_ITEM_SUM_YR2_MN11,   prf.SALES_SUM_YR2_MN11,    prf.INVOLVEMENT_SCORE_YR2_MN11,
            prf.NUM_ITEM_SUM_YR2_MN12,   prf.SALES_SUM_YR2_MN12,    prf.INVOLVEMENT_SCORE_YR2_MN12,
            prf.NUM_ITEM_SUM_YR3_MN01,   prf.SALES_SUM_YR3_MN01,    prf.INVOLVEMENT_SCORE_YR3_MN01,
            prf.NUM_ITEM_SUM_YR3_MN02,   prf.SALES_SUM_YR3_MN02,    prf.INVOLVEMENT_SCORE_YR3_MN02,
            prf.NUM_ITEM_SUM_YR3_MN03,   prf.SALES_SUM_YR3_MN03,    prf.INVOLVEMENT_SCORE_YR3_MN03,
            prf.NUM_ITEM_SUM_YR3_MN04,   prf.SALES_SUM_YR3_MN04,    prf.INVOLVEMENT_SCORE_YR3_MN04,
            prf.NUM_ITEM_SUM_YR3_MN05,   prf.SALES_SUM_YR3_MN05,    prf.INVOLVEMENT_SCORE_YR3_MN05,
            prf.NUM_ITEM_SUM_YR3_MN06,   prf.SALES_SUM_YR3_MN06,    prf.INVOLVEMENT_SCORE_YR3_MN06,
            prf.NUM_ITEM_SUM_YR3_MN07,   prf.SALES_SUM_YR3_MN07,    prf.INVOLVEMENT_SCORE_YR3_MN07,
            prf.NUM_ITEM_SUM_YR3_MN08,   prf.SALES_SUM_YR3_MN08,    prf.INVOLVEMENT_SCORE_YR3_MN08,
            prf.NUM_ITEM_SUM_YR3_MN09,   prf.SALES_SUM_YR3_MN09,    prf.INVOLVEMENT_SCORE_YR3_MN09,
            prf.NUM_ITEM_SUM_YR3_MN10,   prf.SALES_SUM_YR3_MN10,    prf.INVOLVEMENT_SCORE_YR3_MN10,
            prf.NUM_ITEM_SUM_YR3_MN11,   prf.SALES_SUM_YR3_MN11,    prf.INVOLVEMENT_SCORE_YR3_MN11,
            g_date
   from     cust_db_dept_month_involve prf

   where   PRF.FIN_YEAR_NO                 = G_LAST_YR
   and      prf.fin_month_no                = g_last_mn ;

   g_recs_inserted         := g_recs_inserted + sql%rowcount;

   commit;

   l_text := 'ENABLING PK CONSTRAINT - PK_CST_DEPT_MONTH_INV';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate 'alter table W7131037.CUST_DB_DEPT_MONTH_INVOLVE enable constraint PK_CST_DEPT_MONTH_INV';
   l_text := 'PK CONSTRAINT ENABLED';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   commit;

   l_text      := 'Update stats on tables to be updated/merged';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   DBMS_STATS.gather_table_stats ('W7131037','temp_db_dept_month_involve',estimate_percent=>1, DEGREE => 32);
   l_text      := 'Update stats completed on fnd table';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   DBMS_STATS.gather_table_stats ('W7131037','CUST_DB_DEPT_MONTH_INVOLVE',estimate_percent=>1, DEGREE => 32);
   l_text      := 'Update stats completed on prf table';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   commit;

   l_text      := 'Start of Merge';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


execute immediate 'alter session force parallel dml';

MERGE  /*+ parallel (rli,12) */ INTO cust_db_dept_month_involve rli USING
(
select    /*+ FULL(fli)  parallel (fli,12) */
          FIN_YEAR_NO,
          FIN_MONTH_NO,
          PRIMARY_CUSTOMER_IDENTIFIER,
          DEPARTMENT_NO,
          CUSTOMER_NO,
          NUM_ITEM_SUM,
          SALES_SUM,
          INVOLVEMENT_SCORE
   from   temp_db_dept_month_involve fli
) a
ON  (a.FIN_YEAR_NO                 = rli.FIN_YEAR_NO
and  a.FIN_MONTH_NO                = rli.FIN_MONTH_NO
and  a.PRIMARY_CUSTOMER_IDENTIFIER = rli.PRIMARY_CUSTOMER_IDENTIFIER
and  a.DEPARTMENT_NO               = rli.DEPARTMENT_NO)
WHEN MATCHED THEN
  update set NUM_ITEM_SUM_YR1_MN01      =  a.NUM_ITEM_SUM,
             SALES_SUM_YR1_MN01         =  a.SALES_SUM,
             INVOLVEMENT_SCORE_YR1_MN01 =  a.INVOLVEMENT_SCORE
WHEN NOT MATCHED THEN
  insert (
          FIN_YEAR_NO,
          FIN_MONTH_NO,
          PRIMARY_CUSTOMER_IDENTIFIER,
          DEPARTMENT_NO,
          CUSTOMER_NO,
          NUM_ITEM_SUM_YR1_MN01,
          SALES_SUM_YR1_MN01,
          INVOLVEMENT_SCORE_YR1_MN01,
          NUM_ITEM_SUM_YR1_MN02,
          SALES_SUM_YR1_MN02,
          INVOLVEMENT_SCORE_YR1_MN02,
          NUM_ITEM_SUM_YR1_MN03,
          SALES_SUM_YR1_MN03,
          INVOLVEMENT_SCORE_YR1_MN03,
          NUM_ITEM_SUM_YR1_MN04,
          SALES_SUM_YR1_MN04,
          INVOLVEMENT_SCORE_YR1_MN04,
          NUM_ITEM_SUM_YR1_MN05,
          SALES_SUM_YR1_MN05,
          INVOLVEMENT_SCORE_YR1_MN05,
          NUM_ITEM_SUM_YR1_MN06,
          SALES_SUM_YR1_MN06,
          INVOLVEMENT_SCORE_YR1_MN06,
          NUM_ITEM_SUM_YR1_MN07,
          SALES_SUM_YR1_MN07,
          INVOLVEMENT_SCORE_YR1_MN07,
          NUM_ITEM_SUM_YR1_MN08,
          SALES_SUM_YR1_MN08,
          INVOLVEMENT_SCORE_YR1_MN08,
          NUM_ITEM_SUM_YR1_MN09,
          SALES_SUM_YR1_MN09,
          INVOLVEMENT_SCORE_YR1_MN09,
          NUM_ITEM_SUM_YR1_MN10,
          SALES_SUM_YR1_MN10,
          INVOLVEMENT_SCORE_YR1_MN10,
          NUM_ITEM_SUM_YR1_MN11,
          SALES_SUM_YR1_MN11,
          INVOLVEMENT_SCORE_YR1_MN11,
          NUM_ITEM_SUM_YR1_MN12,
          SALES_SUM_YR1_MN12,
          INVOLVEMENT_SCORE_YR1_MN12,
          NUM_ITEM_SUM_YR2_MN01,
          SALES_SUM_YR2_MN01,
          INVOLVEMENT_SCORE_YR2_MN01,
          NUM_ITEM_SUM_YR2_MN02,
          SALES_SUM_YR2_MN02,
          INVOLVEMENT_SCORE_YR2_MN02,
          NUM_ITEM_SUM_YR2_MN03,
          SALES_SUM_YR2_MN03,
          INVOLVEMENT_SCORE_YR2_MN03,
          NUM_ITEM_SUM_YR2_MN04,
          SALES_SUM_YR2_MN04,
          INVOLVEMENT_SCORE_YR2_MN04,
          NUM_ITEM_SUM_YR2_MN05,
          SALES_SUM_YR2_MN05,
          INVOLVEMENT_SCORE_YR2_MN05,
          NUM_ITEM_SUM_YR2_MN06,
          SALES_SUM_YR2_MN06,
          INVOLVEMENT_SCORE_YR2_MN06,
          NUM_ITEM_SUM_YR2_MN07,
          SALES_SUM_YR2_MN07,
          INVOLVEMENT_SCORE_YR2_MN07,
          NUM_ITEM_SUM_YR2_MN08,
          SALES_SUM_YR2_MN08,
          INVOLVEMENT_SCORE_YR2_MN08,
          NUM_ITEM_SUM_YR2_MN09,
          SALES_SUM_YR2_MN09,
          INVOLVEMENT_SCORE_YR2_MN09,
          NUM_ITEM_SUM_YR2_MN10,
          SALES_SUM_YR2_MN10,
          INVOLVEMENT_SCORE_YR2_MN10,
          NUM_ITEM_SUM_YR2_MN11,
          SALES_SUM_YR2_MN11,
          INVOLVEMENT_SCORE_YR2_MN11,
          NUM_ITEM_SUM_YR2_MN12,
          SALES_SUM_YR2_MN12,
          INVOLVEMENT_SCORE_YR2_MN12,
          NUM_ITEM_SUM_YR3_MN01,
          SALES_SUM_YR3_MN01,
          INVOLVEMENT_SCORE_YR3_MN01,
          NUM_ITEM_SUM_YR3_MN02,
          SALES_SUM_YR3_MN02,
          INVOLVEMENT_SCORE_YR3_MN02,
          NUM_ITEM_SUM_YR3_MN03,
          SALES_SUM_YR3_MN03,
          INVOLVEMENT_SCORE_YR3_MN03,
          NUM_ITEM_SUM_YR3_MN04,
          SALES_SUM_YR3_MN04,
          INVOLVEMENT_SCORE_YR3_MN04,
          NUM_ITEM_SUM_YR3_MN05,
          SALES_SUM_YR3_MN05,
          INVOLVEMENT_SCORE_YR3_MN05,
          NUM_ITEM_SUM_YR3_MN06,
          SALES_SUM_YR3_MN06,
          INVOLVEMENT_SCORE_YR3_MN06,
          NUM_ITEM_SUM_YR3_MN07,
          SALES_SUM_YR3_MN07,
          INVOLVEMENT_SCORE_YR3_MN07,
          NUM_ITEM_SUM_YR3_MN08,
          SALES_SUM_YR3_MN08,
          INVOLVEMENT_SCORE_YR3_MN08,
          NUM_ITEM_SUM_YR3_MN09,
          SALES_SUM_YR3_MN09,
          INVOLVEMENT_SCORE_YR3_MN09,
          NUM_ITEM_SUM_YR3_MN10,
          SALES_SUM_YR3_MN10,
          INVOLVEMENT_SCORE_YR3_MN10,
          NUM_ITEM_SUM_YR3_MN11,
          SALES_SUM_YR3_MN11,
          INVOLVEMENT_SCORE_YR3_MN11,
          NUM_ITEM_SUM_YR3_MN12,
          SALES_SUM_YR3_MN12,
          INVOLVEMENT_SCORE_YR3_MN12,
          LAST_UPDATED_DATE
         )
  values
         (
          a.FIN_YEAR_NO,
          a.FIN_MONTH_NO,
          a.PRIMARY_CUSTOMER_IDENTIFIER,
          a.DEPARTMENT_NO,
          a.CUSTOMER_NO,
          a.NUM_ITEM_SUM,
          a.SALES_SUM,
          a.INVOLVEMENT_SCORE,
          0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
          0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
          0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
          g_date
);

g_recs_read:=g_recs_read+SQL%ROWCOUNT;
--g_recs_inserted:=dwh_log.get_merge_insert_count;
g_recs_updated:=g_recs_updated+SQL%ROWCOUNT;






--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;


END "WH_PRF_CUST_295U";
