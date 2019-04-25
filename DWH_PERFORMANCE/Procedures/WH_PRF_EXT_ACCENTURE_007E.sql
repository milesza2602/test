--------------------------------------------------------
--  DDL for Procedure WH_PRF_EXT_ACCENTURE_007E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_EXT_ACCENTURE_007E" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        September 2017
--  Author:      A Ugolini
--  Purpose:     Create ACCENTURE extract to flat file in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - dwh_performance.dim_item
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  26 Sept 2017 - A Ugolini - ODWH Extract: Customer Basket info - ACCENTURE

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
g_seg_type           varchar(9)    := 'Non-Foods';
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;        

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_EXT_ACCENTURE_007E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT CUSTOMER BASKET DATA TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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

    l_text := 'EXTRACT CUSTOMER BASKET DATA STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
       g_count := dwh_generic_file_extract(
       'select /*+ parallel(cbi,8) full(cbi) full(itm) parallel(cls,8) */ cbi.*, cls.segment_no  
        from  dwh_cust_performance.cust_basket_item cbi 
        join  dwh_performance.dim_calendar   cal   
        on    cbi.tran_date                    = cal.calendar_date  
        join  dwh_performance.dim_item       itm   
        on    cbi.item_no                      = itm.item_no      
        left outer join dwh_cust_performance.cust_lss_lifestyle_segments  cls  
        on    cal.fin_year_no                  = cls.fin_year_no  
        and   cal.fin_month_no                 = cls.fin_month_no  
        and   cls.segment_type                 = ''Non-Foods''   
        and   cbi.primary_customer_identifier  = cls.primary_customer_identifier   
        where cbi.tran_date                     between ''30 Jun 2014'' and ''28 jun 2015''   
        and   itm.department_no                in (102,105,106,107,109,665,667,678,682)  
        and   itm.subclass_no                  in (2449,2450,2453,2460,2482,2767,4325,4327,2491,2496,2508,2720,4329,2523,2526,4011,5279,5284,5339,5428,5462,5467)',
        '|','DWH_FILES_OUT','Accenture_CustBasket.week2015');
                                              
                                              
--       g_count := dwh_generic_file_extract('select /*+ parallel(cbi,8) full(cbi) full(itm) parallel(cls,8) */ cbi.*, cls.segment_no ' ||
--                                            ' from  dwh_cust_performance.cust_basket_item             cbi ' ||
--                                                  ' left outer join dwh_cust_performance.cust_lss_lifestyle_segments  cls '||
--                                                  ' on  cal.fin_year_no                  = cls.fin_year_no '  ||
--                                                  ' and cal.fin_month_no                 = cls.fin_month_no ' ||
--                                                  ' and cls.segment_type                 = ''Non-Foods''  '   ||
--                                                  ' and cbi.primary_customer_identifier  = cls.primary_customer_identifier  ' ||
--                                                  ' join dwh_performance.dim_calendar                      cal  '  ||
--                                                  ' on cbi.tran_date                     = cal.calendar_date '     ||
--                                                  ' join dwh_performance.dim_item                          itm  '  ||
--                                                  ' on   cbi.item_no                       = itm.item_no     '     ||
--                                              ' where   cbi.tran_date                     between ''29 Jun 2015'' and ''30 Sep 2017''  ' ||
--                                              ' where   cbi.tran_date                     between ''26 Jun 2017'' and ''30 Sep 2017''  ' ||
--                                              ' and   itm.department_no                in (102,105,106,107,109,665,667,678,682) ' ||  
--                                              ' and   itm.subclass_no in (2449,2450,2453,2460,2482,2767,4325,4327,2491,2496,2508,2720,4329,2523,2526,4011,5279,5284,5339,5428,5462,5467) ', 
--                                              '|','DWH_FILES_OUT','ACCENTURE_CustBasket.week');                                              
    l_text :=  'Records extracted to ACCENTURE_Catlg.week '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************

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
       raise;
end WH_PRF_EXT_ACCENTURE_007E;
