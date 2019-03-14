-- ****** Object: Procedure W7131037.WH_PRF_CUST_315U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_315U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        January 2014
--  Author:      Alastair de Wet
--  Purpose:     Create wfs_cust_vmp_ftp fact table in the performance layer
--               with input ex vmp from Engen and WW transactions
--  Tables:      Input  - cust_vmp_sales
--               Output - cust_vmp_ftp
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
-- Note: This version Attempts to do a bulk insert / update
--       This would be appropriate for large loads where most of the data is for Insert like with Sales transactions.
--       Updates however are also a lot faster that on the original template.
--  Naming conventions
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
g_truncate_count     integer       :=  0;
g_record_string      varchar(250) ;
g_record_type        varchar(8)    := 'VMPENGEN';
g_sequence           integer       :=  0;
g_date               date          := trunc(sysdate);
g_sysdate            date          := sysdate;

g_start_date         date ;
g_end_date           date ;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_315U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD MONTHLY VMP_FTP EX ENGEN AND WW VMP SALES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert_engen as
begin
     g_sequence      := 1;
     g_record_type   := 'VMPENGEN';
--     g_record_string := g_sysdate||'-WWENGEN-01.txt';
     g_record_string := TO_CHAR(g_sysdate,'yyyymmdd')||'T'||TO_CHAR(g_sysdate,'hh24miss')||'-WWENGEN-01.txt';
     insert into cust_vmp_ftp   values (g_record_type,g_sequence,g_record_string,g_date);

     g_sequence      := g_sequence + 1;
--     g_record_string := 'EngenTransactions,'||g_sysdate||',WW,1';
     g_record_string := 'EngenTransactions,'||TO_CHAR(g_sysdate,'yyyymmdd')||'T'||TO_CHAR(g_sysdate,'hh24miss')||',WW,1';
     insert into cust_vmp_ftp   values (g_record_type,g_sequence,g_record_string,g_date);

     g_sequence      := g_sequence + 1;

     insert  into cust_vmp_ftp prf
     select   g_record_type,g_sequence,
              to_char(tran_date,'DD MON YYYY')||' '||tran_time||','||LOCATION_NO||',,'||PRIMARY_CUSTOMER_IDENTIFIER||','||TILL_NO||'|'||TRAN_NO||'|'||CUSTOMER_NO||','||TRAN_SELLING,
              g_date
     from     cust_engen_vmp_sales fnd
     where    tran_date  between  g_start_date and g_end_date;



     g_recs_inserted := g_recs_inserted + sql%rowcount;

     g_sequence      := g_sequence + 1;

     select   'Total,'||
              COUNT(*)||','||
              sum(tran_selling)
     into     g_record_string
     from     cust_engen_vmp_sales fnd
     where    tran_date  between  g_start_date and g_end_date;

     insert into cust_vmp_ftp   values (g_record_type,g_sequence,g_record_string,g_date);

     commit;
----/*+ FULL(fnd)   */

  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG EGN INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG EGN INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert_engen;

--**************************************************************************************************
-- Insert all record flaged as 'I' in the staging table into foundation
--**************************************************************************************************
procedure flagged_records_insert_ww as
begin
     g_record_type   := 'VMPWW';
     g_sequence      := 5;
     g_record_string := TO_CHAR(g_sysdate,'yyyymmdd')||'T'||TO_CHAR(g_sysdate,'hh24miss')||'-WWORTHS-01.txt';
     insert into cust_vmp_ftp   values (g_record_type,g_sequence,g_record_string,g_date);

     g_sequence      := g_sequence + 1;
     g_record_string := 'WoolworthsTransactions,'||TO_CHAR(g_sysdate,'yyyymmdd')||'T'||TO_CHAR(g_sysdate,'hh24miss')||',WW,1';
     insert into cust_vmp_ftp   values (g_record_type,g_sequence,g_record_string,g_date);

     g_sequence      := g_sequence + 1;

     insert  into cust_vmp_ftp prf
     select   g_record_type,g_sequence,
              to_char(tran_date,'DD MON YYYY')||' '||tran_time||','||LOCATION_NO||',,'||PRIMARY_CUSTOMER_IDENTIFIER||','||TILL_NO||'|'||TRAN_NO||'|'||CUSTOMER_NO||','||TRAN_SELLING,
              g_date
     from     cust_vmp_sales fnd
     where    tran_date  between  g_start_date and g_end_date;



     g_recs_inserted := g_recs_inserted + sql%rowcount;

     g_sequence      := g_sequence + 1;

     select   'Total,'||
              COUNT(*)||','||
              sum(tran_selling)
     into     g_record_string
     from     cust_vmp_sales fnd
     where    tran_date  between  g_start_date and g_end_date;

     insert into cust_vmp_ftp   values (g_record_type,g_sequence,g_record_string,g_date);

     commit;
----/*+ FULL(fnd)   */
  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG WW INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG WW INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_records_insert_ww;
--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
--    execute immediate 'alter session enable parallel dml';


    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select  this_mn_start_date,
            this_mn_end_date
    into    g_start_date,
            g_end_date
    from    dim_calendar
    where   calendar_date = trunc(sysdate) - 28;

--**************************************************************************************************
-- Call the bulk routines
--**************************************************************************************************

    EXECUTE IMMEDIATE  'TRUNCATE TABLE W7131037.CUST_VMP_FTP';

    l_text := 'BULK ENGEN INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert_engen;

    l_text := 'BULK WW INSERT STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    flagged_records_insert_ww;

    g_recs_read := g_recs_updated + g_recs_inserted;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',0);



    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--   if g_recs_read <> g_recs_inserted + g_recs_updated  then
--      l_text :=  'RECORD COUNTS DO NOT BALANCE - CHECK YOUR CODE '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--      p_success := false;
--      l_message := 'ERROR - Record counts do not balance see log file';
--      dwh_log.record_error(l_module_name,sqlcode,l_message);
--      raise_application_error (-20246,'Record count error - see log files');
--   end if;


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
       raise;
end wh_prf_cust_315u;
