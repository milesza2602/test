--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_353U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_353U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        November 2011
--  Author:      Alastair de Wet
--  Purpose:     Create hr_bee_ed_payment_mn fact table in the performance layer
--               with added value ex foundation layer hr_bee_ap_invoice_payment.
--  Tables:      Input  - hr_bee_ap_invoice_payment
--               Output - hr_bee_ed_payment_mn
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Date:        September 2014
--  Changed by:  Kgomotso Lehabe
--  Description: Lookup payment date from dim_hr_ap_invoice
--              : Remove the ED_Category rule from the evaluation
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            hr_bee_ed_payment_mn%rowtype;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;
g_fin_year_no        number        := 0;


l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_353U';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE hr_bee_ed_payment_mn EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of hr_bee_ed_payment_mn%rowtype index by binary_integer;
type tbl_array_u is table of hr_bee_ed_payment_mn%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_hr_ap_pay is
 with dates as
    (select distinct ai.sk1_supplier_id,
                sk1_benefit_factor_id2,
                sk2_ed_beneficiary_id,
                sk1_ed_beneficiary_id,
                 cal.this_mn_end_date,
                cal.fin_month_code,
                cal.fin_month_no,
                cal.fin_year_no
      from dwh_hr_performance.hr_bee_ap_invoice_payment ai,
           dim_calendar  cal
      where  cal.this_mn_end_date < g_date
      and  cal.fin_year_no = g_fin_year_no
      and   sk1_benefit_factor_id2 <> 0
     ),

     payments as
   (select   app.sk1_supplier_id,
          app.sk1_benefit_factor_id2,
          max(deb.ed_category) ed_category,
          cal.this_mn_end_date,
          deb.sk1_ed_beneficiary_id,
          max(debh.sk2_ed_beneficiary_id) sk2_ed_beneficiary_id,
          max(bf.ed_percentage) ed_percentage,
     --     sum(app.ed_payment_terms) ed_payment_terms,
          sum(app.ed_settlement_discount) ed_settlement_discount,
          cal.fin_year_no,
          cal.fin_month_no
   from   dwh_hr_performance.hr_bee_ap_invoice_payment  app,
         dwh_hr_performance.dim_hr_ap_invoice inv,
          dim_calendar cal,
          dwh_hr_performance.dim_hr_bee_supplier ds,
          dwh_hr_performance.dim_hr_bee_ed_beneficiary deb,
          dwh_hr_performance.dim_hr_bee_ed_beneficiary_hist debh,
          dwh_hr_performance.dim_hr_bee_benefit_factor bf
   where  app.sk1_supplier_id        = ds.sk1_supplier_id     and
          inv.sk1_invoice_id         = app.sk1_invoice_id and
          ds.supplier_no             = to_char(deb.vendor_no) and
          ds.supplier_no             = to_char(debh.vendor_no) and
          cal.this_mn_end_date    between debh.sk2_active_from_date and debh.sk2_active_to_date and
          inv.payment_date           = cal.calendar_date      and
          app.sk1_benefit_factor_id2 = bf.sk1_benefit_factor_id and
         app.sk1_benefit_factor_id2 <> 0

    group by app.sk1_supplier_id,
          app.sk1_benefit_factor_id2,
          cal.this_mn_end_date,
          deb.sk1_ed_beneficiary_id  ,
           cal.fin_year_no,
           cal.fin_month_no)

 select distinct dt.sk1_benefit_factor_id2,
          pay.ed_category,
          dt.this_mn_end_date,
          dt.sk1_ed_beneficiary_id,
          max(dt.sk2_ed_beneficiary_id) sk2_ed_beneficiary_id,
         max(ed_percentage) ed_percentage,
    --      pay.ed_payment_terms,
         max(nvl(pay.ed_settlement_discount,0)) ed_settlement_discount,
--         nvl(pay.ed_settlement_discount,0) ed_settlement_discount,
          dt.fin_year_no,
          dt.fin_month_no
    from dates dt
    left outer join payments pay
    on (pay.sk1_supplier_id = dt.sk1_supplier_id
    and pay.sk1_benefit_factor_id2 = dt.sk1_benefit_factor_id2
    and pay.this_mn_end_date = dt.this_mn_end_date)
--where  dt.sk1_ed_beneficiary_id != 716712
       group by  dt.sk1_benefit_factor_id2, pay.ed_category,  dt.this_mn_end_date,  dt.sk1_ed_beneficiary_id,dt.fin_year_no, dt.fin_month_no;


g_rec_in                   c_fnd_hr_ap_pay%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_hr_ap_pay%rowtype;
a_stg_input      stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_ed_beneficiary_id    := g_rec_in.sk1_ed_beneficiary_id;
   g_rec_out.sk2_ed_beneficiary_id    := g_rec_in.sk2_ed_beneficiary_id;
   g_rec_out.sk1_benefit_factor_id    := g_rec_in.sk1_benefit_factor_id2;
   g_rec_out.payment_mn_end_date      := g_rec_in.this_mn_end_date ;
--   g_rec_out.ed_payment_contribution  := g_rec_in.ed_payment_terms;
   g_rec_out.ed_payment_contribution  := g_rec_in.ed_settlement_discount;
   g_rec_out.last_updated_date        := g_date;
    g_rec_out.fin_year_no              := g_rec_in.fin_year_no;
     g_rec_out.fin_month_no            := g_rec_in.fin_month_no;

   g_rec_out.ed_payment_contribution_total  := 0;
      g_rec_out.ed_payment_contribution_total  :=  (g_rec_out.ed_payment_contribution *  g_rec_in.ed_percentage / 100);

   exception
      when others then
       l_message := dwh_hr_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variable;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin

   forall i in a_tbl_insert.first .. a_tbl_insert.last
      save exceptions
      insert into hr_bee_ed_payment_mn values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).sk1_ed_beneficiary_id;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update hr_bee_ed_payment_mn
      set     ed_payment_contribution       = a_tbl_update(i).ed_payment_contribution ,
              ed_payment_contribution_total = a_tbl_update(i).ed_payment_contribution_total ,
              last_updated_date             = a_tbl_update(i).last_updated_date,
              fin_year_no                   = a_tbl_update(i).fin_year_no,
              fin_month_no                   = a_tbl_update(i).fin_month_no
       where  sk1_ed_beneficiary_id         = a_tbl_update(i).sk1_ed_beneficiary_id  and
              sk1_benefit_factor_id         = a_tbl_update(i).sk1_benefit_factor_id  and
              payment_mn_end_date           = a_tbl_update(i).payment_mn_end_date;

      g_recs_updated := g_recs_updated + a_tbl_update.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_hr_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_hr_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_ed_beneficiary_id;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   hr_bee_ed_payment_mn
   where  sk1_ed_beneficiary_id      = g_rec_out.sk1_ed_beneficiary_id  and
          sk1_benefit_factor_id      = g_rec_out.sk1_benefit_factor_id  and
          payment_mn_end_date        = g_rec_out.payment_mn_end_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;


-- Place record into array for later bulk writing
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      commit;
   end if;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_hr_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF hr_bee_ed_payment_mn EX hr_bee_ap_invoice_payment STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      select fin_year_no
      INTO   g_fin_year_no
      FROM   dim_calendar
      WHERE  calendar_date = g_date -15;


--**************************************************************************************************
    open c_fnd_hr_ap_pay;
    fetch c_fnd_hr_ap_pay bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_hr_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_hr_ap_pay bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_hr_ap_pay;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_hr_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_hr_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_hr_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_hr_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_hr_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_hr_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_hr_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_hr_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_PRF_HR_353U;
