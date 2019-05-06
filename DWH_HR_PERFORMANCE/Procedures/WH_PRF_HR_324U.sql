--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_324U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_324U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2012
--  Author:      Alastair de Wet
--  Purpose:     Update YTD figures to invoice month table
--  Tables:      Input  - hr_invoice_supplier_mth
--               Output - hr_invoice_supplier_mth
--  Packages:    constants, dwh_log, dwh_valid
--
--
--  Maintenance:
--
--  Date :        Septemner 2014
--  Changed by:   Kgomotso Lehabe
--  Description:  Add invoice_amount_ex_vat_ytd
--                Grain of table has changed. Add capital_goods_ind to the joins
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_month              number      ;
g_year               number      ;
g_sub                integer       :=  0;
g_rec_out            hr_invoice_supplier_mth%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_324U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE HR_INVOICE_SUPPLIER_MTH';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
CURSOR C_LOOP IS
   select inv.sk1_supplier_id,
          inv.capital_goods_ind,
          sum(nvl(inv.invoice_amount,0)) invoice_amount_ytd,
          sum(nvl(inv.base_recognition,0)) base_recognition_ytd,
          sum(nvl(inv.recognition_all,0)) recognition_all_ytd,
          sum(nvl(inv.invoice_amount_ex_vat,0)) invoice_amount_ex_vat_ytd
   from   hr_invoice_supplier_mth inv
   where  inv.fin_year_no = g_year and
          inv.fin_month_no between 01 and g_month
   group by inv.sk1_supplier_id, capital_goods_ind;
g_rec_in     c_loop%rowtype;
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

    l_text := 'UPDATE OF hr_invoice_supplier_mth   STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    SELECT TODAY_FIN_MONTH_NO, TODAY_FIN_YEAR_NO
    INTO   G_MONTH, G_YEAR
    FROM   DIM_CONTROL;

for g_sub in 0..11 loop

    if g_month = 0 then
       g_month := 12;
       g_year  := g_year - 1;
    end if;
    l_text := 'ROLLUP RANGE IS:- '||'01 '||g_year||'  to '||g_month||' '||g_year;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    for loop_record in c_loop
    loop
      update hr_invoice_supplier_mth
      set    invoice_amount_ytd       = loop_record.invoice_amount_ytd,
             base_recognition_ytd     = loop_record.base_recognition_ytd,
             recognition_all_ytd      = loop_record.recognition_all_ytd,
             invoice_amount_ex_vat_ytd = loop_record.invoice_amount_ex_vat_ytd
      where  sk1_supplier_id           = loop_record.sk1_supplier_id and
             capital_goods_ind          =loop_record.capital_goods_ind and
             fin_year_no               = g_year and
             fin_month_no               = g_month;

      g_recs_read    :=  g_recs_read    + SQL%ROWCOUNT;
      g_recs_updated :=  g_recs_updated + SQL%ROWCOUNT;
    end loop;
    g_month := g_month - 1;

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

end wh_prf_hr_324u;
