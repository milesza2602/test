-- ****** Object: Procedure W7131037.WH_PRF_CUST_242U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_242U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        April 2011
--  Author:      Alastair de Wet
--  Purpose:     Roll up for 3,6 & 12 months values
--  Tables:      Input  - cust_db_group_mn
--               Output - temp_cust_group_mn_3
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
g_yr_01               number;
g_yr_02               number;
g_yr_03               number;
g_mn_00               number;
g_mn_01               number;
g_mn_02               number;
g_mn_03               number;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_242U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE temp_cust_group_mn_3 EX cust_db_group_mn';
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

    l_text := 'LOAD OF temp_cust_group_mn_3 EX cust_db_group_mn STARTED AT '||
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

    g_mn_01 := g_mn_00 - 1;
    g_yr_01 := g_yr_00;
    if g_mn_01 = 0 then
       g_mn_01 := 12;
       g_yr_01 := g_yr_01 - 1;
    end if;
    g_mn_02 := g_mn_01 - 1;
    g_yr_02 := g_yr_01;
    if g_mn_02 = 0 then
       g_mn_02 := 12;
       g_yr_02 := g_yr_02 - 1;
    end if;
    g_mn_03 := g_mn_02 - 1;
    g_yr_03 := g_yr_02;
    if g_mn_03 = 0 then
       g_mn_03 := 12;
       g_yr_03 := g_yr_03 - 1;
    end if;


   l_text := 'Months being processed:= '||
          g_yr_01||g_mn_01||' '||g_yr_02||g_mn_02;

   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   execute immediate 'truncate table W7131037.temp_cust_group_mn_3';
   l_text := 'Truncate temp table  W7131037.temp_cust_group_mn_3' ;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   insert into temp_cust_group_mn_3
   select   g_yr_00,g_mn_00,
            primary_customer_identifier,
            group_no,
            max(customer_no) customer_no,
            0,
            sum(num_item)  num_item_03_mth,
            sum(num_item)  num_item_06_mth,
            sum(num_item)  num_item_12_mth,
            0,
            sum(sales) sales_03_mth,
            sum(sales) sales_06_mth,
            sum(sales) sales_12_mth,
            0,
            sum(num_visit) num_visit_03_mth,
            sum(num_visit) num_visit_06_mth,
            sum(num_visit) num_visit_12_mth,
            0,0,
            max(g_date) as last_updated_date
   from     cust_db_group_mn
   where    (fin_year_no = g_yr_02 and fin_month_no = g_mn_02) or
            (fin_year_no = g_yr_01 and fin_month_no = g_mn_01)
   group by primary_customer_identifier,group_no;
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

END "WH_PRF_CUST_242U";
