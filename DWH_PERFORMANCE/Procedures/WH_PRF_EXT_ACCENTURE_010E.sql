--------------------------------------------------------
--  DDL for Procedure WH_PRF_EXT_ACCENTURE_010E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_EXT_ACCENTURE_010E" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        October 2017
--  Author:      A Ugolini
--  Purpose:     Create Accenture extract to flat file in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - dwh_performance.dim_item
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  17 Oct 2017 - A Ugolini - ODWH Extract: Weekly Sales info - Accenture

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
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_EXT_ACCENTURE_010E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT SALES DAILY DETAIL DATA TO FLAT FILE';
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

    l_text := 'EXTRACT SALES DAILY DETAIL DATA STARTED AT '||
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
    
       g_count := dwh_generic_file_extract('select /*+ parallel(sls,8) full(sls) full(itm) */ sls.location_no,sls.item_no, ' 	||
			'POST_DATE, SALES_QTY,SALES,SALES_COST, REG_SALES_QTY,REG_SALES,REG_SALES_COST, ' 	            ||
			'PROM_SALES_QTY,PROM_SALES, PROM_SALES_COST, CLEAR_SALES_QTY, CLEAR_SALES, CLEAR_SALES_COST, ' 	||                   
			'WASTE_QTY, WASTE_SELLING, WASTE_COST, SHRINK_QTY, SHRINK_SELLING,SHRINK_COST, ' 	              ||                       
			'SALES_RETURNS_QTY, SALES_RETURNS_COST, SALES_RETURNS, MKUP_SELLING, MKUP_CANCEL_SELLING, ' 	  ||                
			'MKDN_SELLING, MKDN_CANCEL_SELLING, CLEAR_MKDN_SELLING, HO_PROM_DISCOUNT_AMT, '		              ||
			'HO_PROM_DISCOUNT_QTY, ST_PROM_DISCOUNT_AMT, ST_PROM_DISCOUNT_QTY '	          ||
                        ' from DWH_FOUNDATION.FND_RTL_LOC_ITEM_DY_RMS_SALE  SLS, ' ||
                             ' dwh_performance.dim_item                     itm, ' ||
                             ' dwh_performance.dim_calendar                 cal '  ||                             
                        ' where sls.item_no       = itm.item_no '                  ||
                          ' and itm.department_no  in (102,105,106,107,109,665,667,678,682) ' ||  
                          ' and sls.post_date     =  cal.calendar_date '           ||
--                          ' and cal.fin_year_no   =  2015 ',
                            ' and cal.fin_year_no   =  2018 ' ||
                           ' and cal.fin_month_no >= 4 ' ||
                           ' and cal.fin_month_no <= 6',

                          '|','DWH_FILES_OUT','Daily_Sales_2018_456.txt');
    l_text :=  'Records extracted to Daily_Sales_2018.txt '||g_count;
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
end WH_PRF_EXT_ACCENTURE_010E;
