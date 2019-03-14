-- ****** Object: Procedure W7131037.WH_PRF_CUST_299U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_299U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        Oct 2015
--  Author:      Kgomotso Lehabe
--  Purpose:     Insert curent month values from foundation and roll down of monthly values to create new invlolvement scores
--  Tables:      Input  - temp_db_company_month_involve
--               Output - cust_db_company_month_involve
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


g_date                date          := trunc(sysdate);
g_yesterday           date          := trunc(sysdate) - 1;
g_yr_00               number;
g_mn_00               number;
g_last_yr             number;
g_last_mn             number;
g_this_mn_start_date  date;
g_this_mn_end_date    date;
g_run_date            date;
g_stmt                varchar(500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_299U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD CUST_DB_COMPANY_MONTH_INVOLVE EX TEMP_DB_COMPANY_MONTH_INVOLVE';
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

    l_text := 'LOAD OF CUST_DB_COMPANY_MONTH_INVOLVE EX TEMP_DB_COMPANY_MONTH_INVOLVE STARTED AT '||
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

   g_stmt      := 'Alter table  W7131037.CUST_DB_COMPANY_MONTH_INVOLVE truncate  subpartition for ('||g_yr_00||','||g_mn_00||') update global indexes';
   l_text      := g_stmt;

   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session force parallel dml';

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

   insert /*+  parallel (prfi,4) */ into cust_db_company_month_involve prfi

   SELECT /*+ FULL(prf)  parallel (prf,4) parallel (bskt,4) */
         --   g_yr_00,g_mn_00,
         2016,7,
            nvl(prf.primary_customer_identifier,bskt.primary_customer_identifier) primary_customer_identifier,
            nvl(prf.company_no,bskt.company_no) company_no,
            nvl(prf.customer_no,bskt.customer_no) customer_no,
            bskt.NUM_ITEM_SUM ,     bskt.SALES_SUM ,      bskt.INVOLVEMENT_SCORE,
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
   from     cust_db_company_month_involve prf
   full outer join
            temp_db_comp_month_involve bskt
   on       prf.primary_customer_identifier = bskt.primary_customer_identifier
   AND      PRF.COMPANY_NO                  = BSKT.COMPANY_NO
 --  and     prf.fin_year_no                 = g_last_yr
 --  and      prf.fin_month_no                = g_last_mn
      and      PRF.FIN_YEAR_NO                 = bskt.FIN_YEAR_NO
      and      prf.fin_month_no                = bskt.fin_month_no
   ;

   g_recs_inserted         := g_recs_inserted + sql%rowcount;

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


END "WH_PRF_CUST_299U";
