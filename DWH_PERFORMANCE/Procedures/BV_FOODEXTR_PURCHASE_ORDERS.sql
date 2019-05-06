--------------------------------------------------------
--  DDL for Procedure BV_FOODEXTR_PURCHASE_ORDERS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."BV_FOODEXTR_PURCHASE_ORDERS" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Aug 2016
--  Author:      BHAVESH VALODIA
--  Purpose:     EXTRACT PURCHASE ORDER DATA FOR FOODS - ONE OFF
--  Tables:      Input  -DIM_PURCHASE_ORDER
--               Output - temp_food_extr_purchase_orders
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--
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
g_recs_hospital      integer       :=  0;
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            temp_food_extr_purchase_orders%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

g_yr_00              number;
g_wk_00              number;

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'BV_FOODEXTR_PURCHASE_ORDERS';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT 5 YEARS OF FOOD DATA PURCHASE ORDERS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'EXTRACT OF temp_food_extr_pricing EX food_purchases STARTED AT '||
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

--**************************************************************************************************
-- Look up period
--**************************************************************************************************


    g_start_date := '12 SEP 2011';
    g_end_date   := '18 SEP 2011';
    
--**************************************************************************************************
-- Main loop
--**************************************************************************************************

for g_sub in 1..12 loop
--    select fin_day_no, this_week_start_date, this_week_end_date,fin_year_no,fin_week_no
--    into   g_fin_day_no, g_start_date, g_end_date,g_yr_00,g_wk_00
--    from   dim_calendar
--    where  calendar_date = g_date - (g_sub * 7);

    l_text := 'ROLLUP RANGE IS:- '||g_start_date||'  to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


     execute immediate 'alter session enable parallel dml';

--    g_stmt      := 'Alter table  temp_food_extr_purchase_orders truncate  subpartition for ('||g_yr_00||','||g_wk_00||') update global indexes';
--    l_text      := g_stmt;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    execute immediate g_stmt;





insert  /*+ APPEND parallel (tmp,4)*/ into temp_food_extr_purchase_orders tmp
     select  /*+ FULL(cbi) FULL(i)  parallel (cbi,4) */ 
to_char(s.supplier_no) as supplier_no,
s.supplier_name, 
to_char(po.PO_NO) as ORDER_PO_NO,
to_char(po.not_before_date,'DD/MM/YYYY') as not_before_date,
to_char(po.into_store_date,'DD/MM/YYYY') as into_store_date
from DIM_PURCHASE_ORDER po
inner join dim_calendar c on c.calendar_date = po.not_before_date
inner join dim_supplier s on s.supplier_no = po.supplier_no and s.supplier_no=53730
where c.calendar_date between g_start_date and g_end_date; --Full date range required: 27/JUN/11 - 26/JUN/16


    g_recs_read     :=  g_recs_read     + SQL%ROWCOUNT;
    g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    l_text := 'INSERTED SO FAR:- '||g_recs_inserted ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    g_start_date    := g_start_date - 7;
    g_end_date      := g_end_date   - 7;

    commit;
end loop;
--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end BV_FOODEXTR_PURCHASE_ORDERS;
