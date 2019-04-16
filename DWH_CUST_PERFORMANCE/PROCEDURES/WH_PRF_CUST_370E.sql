--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_370E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_370E" (p_forall_limit in integer,p_success out boolean,p_run_date in date)
as
--**************************************************************************************************
--  Date:        February 2016
--  Author:      Theo Filander
--  Purpose:     Create Transaction Extract to flat file in the performance layer
--               by reading data and calling generic function to output to flat file.
--  Tables:      Input  - CUST_BASKET, CUST_BASKET_ITEM, DIM_ITEM, FND_CUST_BASKET_AUX
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  July 2016 - Mariska Matthee - Remove join to fnd_cust_basket_aux, causes duplicate transaction lines
--              and sum per tran type, also remove cust_basket
--  May 2017  - Theo Filander Split the extract into 2 : 
--              Monday extract last Wednesday to Sunday
--              Thursday extract this weeks Monday to Tuesday data
--              BCB-258
--  Jun 2017  - Theo Filander Include business Units 51,52,54 and 55
--              BCB-248
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
g_count              number        :=  0;


g_date               date          := trunc(sysdate);
g_run_date           date          := NVL(p_run_date,trunc(sysdate)-1);
g_start_date         date          ;
g_end_date           date          ;
g_weekday            number        := to_char(g_run_date, 'd');
g_file_ext           VARCHAR2(40)   := to_char(g_run_date, 'YYYYMONddhh24mi');

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_370E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT E20 PERSONALISATION INTERFACE (TRANSACTIONS) TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Check for a run date parameter.
-- Remove this code. No longer valid
--**************************************************************************************************
--    if p_run_date is not null or p_run_date <> '' then
--       select this_week_start_date,(max(calendar_date) + (case when CAST(to_char(p_run_date,'d') AS NUMBER) <=3 then 2 else 3 end))
--         into g_run_date,g_end_date
--         from dim_calendar
--        where calendar_date = p_run_date;
--    else
--        select max(calendar_date),(max(calendar_date) + (case when CAST(to_char(trunc(sysdate),'d') AS NUMBER) <=3 then 2 else 3 end))
--          into g_run_date,g_end_date
--          from dwh_performance.dim_calendar
--         where fin_day_no = (case when CAST(to_char(trunc(sysdate),'d') AS NUMBER) <=3 then 1 else 4 end)
--           and calendar_date <= (select this_week_start_date-1
--                                   from dim_calendar
--                                  where calendar_date = trunc(sysdate)); 
                                  
       select calendar_date, case when to_char(g_run_date, 'd') = 2 then (calendar_date + 4)
                                  when to_char(g_run_date, 'd') = 4 then (calendar_date + 1)
                                  else calendar_date
                             end end_date 
         into g_start_date, g_end_date
         from dim_calendar
       where calendar_date = case when to_char(g_run_date, 'd') = 2 then trunc(g_run_date) - 5
                                  when to_char(g_run_date, 'd') = 4 then trunc(g_run_date) - 2
                                  else g_run_date
                             end;
--    end if;
    
--     dbms_output.put_line('StartDate = '||g_start_date||' - '||'EndDate = '||g_end_date);
    
    g_file_ext := to_char(g_run_date, 'YYYYMONddhh24mi');
    g_weekday := to_char(g_run_date, 'd');

--    dbms_output.put_line('Run date is '||g_run_date);
--    dbms_output.put_line('Weekday is '||g_weekday);
    
    if (g_weekday = 2 or 
        g_weekday = 4) then
       l_text := 'This job runs on a '||to_char(g_run_date, 'Day') ;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    else
       l_text := 'This job does not run on a '||to_char(g_run_date, 'Day') ;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       p_success := true;
       return;
    end if;

    execute immediate 'alter session enable parallel dml';

    l_text := 'EXTRACT FOR E20 PERSONALISATION INTERFACE-TRANSACTIONS STARTED AT '||
    to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

   g_count := 0;

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := ' PROCESSING EXTRACT FOR : '||g_run_date||' - RANGE = START DATE '||g_start_date||' TO END DATE '||g_end_date||' @ '||to_char(sysdate,('hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Write to external directory.
-- TO SETUP
-- 1. add directory path to database via CREATE DIRECTORY command
-- 2. ensure that permissions are correct
-- 3. format : 'A','|','B','C'
--       WHERE A = select statement
--             B = Database Directory Name as found on DBA_DIRECTORIES
--             C = output file name
--    eg.'select * from VS_EXT_VMP_SALES_WKLY','|','/dwh_files/files.out','nielsen.wk'
--**************************************************************************************************

    dbms_output.put_line('WRITING TRANSACTIONS TO DESTINATION FILE '||to_char(sysdate,('dd Mon yyyy hh24:mi:ss')) );
    g_count := dwh_performance.dwh_generic_file_extract(
          'select /*+ Full(bi) Parallel(bi,4) Full(di) Parallel(di,4) Full(dl) Parallel(dl,4) Index(fl PK_F_FND_LCTN_ITM) Parallel(fl,4)*/
                  bi.customer_no,
                  cast(cast(bi.location_no as varchar2(20))    ||''/''||
                  cast(bi.till_no as varchar2(20))             ||''/''||
                  cast(bi.tran_no as varchar2(20))             ||''/''||
                  to_char(bi.tran_date,''DDMMYYYY'')           ||''/''||
                  replace(to_char(bi.tran_time),'':'')         ||''/''||
                  cast(bi.item_seq_no as varchar2(20)) as varchar2(40)) AS invoice_code,
                  bi.item_no,
                  bi.item_tran_qty,
                  bi.item_tran_selling,
                  bi.discount_selling,
                  bi.tran_date,
                  bi.location_no,
                  '''' promotion_no,
                  '''' wreward_sales_value,
                  '''' promotion_discount_amount,
                  di.standard_uom_code,
                  di.standard_uom_desc,
                 di.standard_uom_class_code,
                  di.uom_conv_factor,
                  di.base_rsp,
                  fl.reg_rsp,
                  trunc(sysdate) last_updated_date
             from cust_basket_item bi
            inner join dim_item di on bi.item_no = di.item_no
            inner join dim_location dl on bi.location_no = dl.location_no
             left join fnd_location_item fl on bi.item_no     = fl.item_no and
                                     bi.location_no = fl.location_no
                    where bi.tran_date between '''||g_start_date||''' and '''||g_end_date||'''
                      and di.business_unit_no = 50
                      and dl.area_no <> 8800','|','DWH_FILES_OUT','e20_cust_tran.txt.'||g_file_ext );
    l_text :=  'Records extracted to E20_CUST_TRAN.TXT.'||g_file_ext||' '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd Mon yyyy hh24:mi:ss'));
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
end WH_PRF_CUST_370E;
