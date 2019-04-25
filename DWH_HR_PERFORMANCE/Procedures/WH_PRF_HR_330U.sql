--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_330U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_330U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        November 2011
--  Author:      Alastair de Wet
--  Purpose:     Create hr_bee_ap_invoice_payment fact table in the performance layer
--               with added value ex foundation layer fnd_hr_ap_invoice_payment.
--  Tables:      Input  - fnd_hr_ap_invoice_payment
--               Output - hr_bee_ap_invoice_payment
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Date:        September 2014
--  Changed by:  Kgomotso Lehabe
--  Description: Add columns Sk1_bee_business_unit_code and This_mn_end_date
--               Drop olums to be added to the new Invoice dim
--               invoice_no
--               invoice_id
--               payment_gl_period
--               paid_ind
--               post_date
--               invoice_date
--               payment_date
--               days_to_pay
--               shorter_payment_days
--               limited_days_to_pay
--               Change process to report on Suppliers even if there are no transactions for the month.
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
g_rec_out            dwh_hr_performance.hr_bee_ap_invoice_payment%rowtype;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        :=  0;
g_fin_year_no        number        := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_330U';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE hr_bee_ap_invoice_payment EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_hr_performance.hr_bee_ap_invoice_payment%rowtype index by binary_integer;
type tbl_array_u is table of dwh_hr_performance.hr_bee_ap_invoice_payment%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_hr_bee_edl is


with dates as
(select distinct supplier_id,
                org_id,
                invoice_id,
                payment_date,
                cal.this_mn_end_date,
                cal.fin_month_code,
                cal.fin_month_no,
                cal.fin_year_no,
                cal.fin_week_no
    from dwh_hr_foundation.fnd_hr_ap_invoice_payment ai,
          dim_calendar_wk cal
    where  this_mn_end_date < g_date
    and   cal.fin_year_no=  g_fin_year_no
    and ai.last_updated_date  > g_date - 400
  ),
  payments as
(   select  app.supplier_id,
           app.invoice_id,
           app.payment_date,
           api.invoice_date ,
           app.fin_year_no,
           app.fin_month_no,
           app.fin_week_no,
           amount_paid,
           amount_paid_ex_vat,
           discount_amount,
           discount_amount_ex_vat,
           invoice_amount_ex_vat


     from   dwh_hr_foundation.fnd_hr_ap_invoice_payment  app,
           dwh_hr_foundation.fnd_hr_ap_invoice  api

   where  app.invoice_id         = api.invoice_id and
          app.supplier_id        = api.supplier_id
   )


  select distinct dc.this_mn_end_date,
           ds.sk1_supplier_id,
           dsh.sk2_supplier_id,
           ds.bee_business_unit_code,
           nvl(app.invoice_id,0) invoice_id,
           nvl(app.payment_date, '07jul2014')  payment_date,
           app.invoice_date ,
           dc.fin_year_no,
           dc.fin_month_no,
           app.fin_week_no,
           dc.fin_month_code,
           nvl(ed_beneficiary_id, 'ED0000') ed_beneficiary_id,
          nvl(app.amount_paid,0) amount_paid,
          nvl(app.amount_paid_ex_vat,0) amount_paid_ex_vat,
          nvl(app.discount_amount,0) discount_amount,
          nvl(app.discount_amount_ex_vat,0) discount_amount_ex_vat,
          nvl(app.invoice_amount_ex_vat,0)  invoice_amount_ex_vat ,
          nvl( deb.settlement_discount_ind,0) settlement_discount_ind,
          nvl( deb.payment_terms_ind,0)    payment_terms_ind,
          nvl( deb.interest_fee_loan_ind,0) interest_fee_loan_ind,
          nvl( deb.std_loans_black_owned_ind,0)  std_loans_black_owned_ind,
          nvl( deb.standard_loan_other_ind,0) standard_loan_other_ind

     from     dates dc
   left outer join payments  app
        on ( dc.supplier_id = app.supplier_id and
             dc.fin_year_no  = app.fin_year_no  and
             dc.fin_month_no  = app.fin_month_no),

          dwh_hr_performance.dim_hr_bee_supplier ds ,
          dwh_hr_performance.dim_hr_bee_supplier_hist dsh ,
          dwh_hr_performance.dim_hr_bee_ed_beneficiary deb

    where  ds.supplier_no       = to_char(deb.vendor_no) and
           dc.supplier_id       = ds.supplier_id and
          dc.supplier_id       = dsh.supplier_id  and
          nvl(app.payment_date, g_date)    between dsh.sk2_active_from_date and dsh.sk2_active_to_date ;



g_rec_in                   c_fnd_hr_bee_edl%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_hr_bee_edl%rowtype;
a_stg_input      stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.this_mn_end_date                := g_rec_in.this_mn_end_date;
   g_rec_out.sk1_supplier_id                 := g_rec_in.sk1_supplier_id;
   g_rec_out.sk2_supplier_id                 := g_rec_in.sk2_supplier_id;
   g_rec_out.amount_paid                     := g_rec_in.amount_paid;
   g_rec_out.amount_paid_ex_vat              := g_rec_in.amount_paid_ex_vat;
   g_rec_out.discount_amount                 := g_rec_in.discount_amount;
   g_rec_out.discount_amount_ex_vat          := g_rec_in.discount_amount_ex_vat;
   g_rec_out.fin_year_no                     := g_fin_year_no ; --g_rec_in.fin_year_no;
   g_rec_out.fin_month_no                    := g_rec_in.fin_month_no;
   g_rec_out.fin_week_no                     := g_rec_in.fin_week_no;
   g_rec_out.fin_month_code                  := g_rec_in.fin_month_code;
   g_rec_out.days_to_pay                     := g_rec_in.payment_date - g_rec_in.invoice_date;
   g_rec_out.last_updated_date               := g_date;


   if g_rec_out.days_to_pay > 14 then
      g_rec_out.shorter_payment_days   := 0 ;
   else
      g_rec_out.shorter_payment_days   := 15 - g_rec_out.days_to_pay;
   end if;

   if g_rec_out.shorter_payment_days > 9 then
      g_rec_out.limited_days_to_pay    := 10 ;
   else
      g_rec_out.limited_days_to_pay    := g_rec_out.shorter_payment_days;
   end if;


   g_rec_out.ed_payment_terms_benefit        := (g_rec_out.shorter_payment_days *
                                                g_rec_in.invoice_amount_ex_vat) / 100;

   if g_rec_in.payment_terms_ind = 1 then
      g_rec_out.ed_payment_terms    :=  g_rec_out.ed_payment_terms_benefit ;
   else
      g_rec_out.ed_payment_terms    := 0 ;
   end if;
   if g_rec_in.settlement_discount_ind = 1 then
      g_rec_out.ed_settlement_discount    := g_rec_in.invoice_amount_ex_vat * .025 ;
   else
      g_rec_out.ed_settlement_discount    := 0 ;
   end if;
   if g_rec_out.limited_days_to_pay < 9 then
      g_rec_out.lost_opportunity_payment     := (8 - g_rec_out.limited_days_to_pay) *
                                                 g_rec_in.invoice_amount_ex_vat / 100;
   else
      g_rec_out.lost_opportunity_payment     := 0 ;
   end if;

    g_rec_out.sk1_benefit_factor_id1 := 0;
    g_rec_out.sk1_benefit_factor_id2 := 0;
    if g_rec_in.payment_terms_ind = 1 then
      select sk1_benefit_factor_id
      into   g_rec_out.sk1_benefit_factor_id1
      from   dwh_hr_performance.dim_hr_bee_benefit_factor
      where  benefit_factor_id = 6016;
   end if;
   if g_rec_in.settlement_discount_ind = 1 then
      select sk1_benefit_factor_id
      into   g_rec_out.sk1_benefit_factor_id2
      from   dwh_hr_performance.dim_hr_bee_benefit_factor
      where  benefit_factor_id = 6003;
   end if;


   begin
      select bu.sk1_bee_business_unit_code
     into   g_rec_out.sk1_bee_business_unit_code
     from    dwh_hr_performance.dim_hr_bee_bu bu
     where  bu.bee_business_unit_code =  g_rec_in.bee_business_unit_code  ;

         exception
            when no_data_found then
              g_rec_out.sk1_bee_business_unit_code := 176326;
    end;

     begin
      select inv.sk1_invoice_id
     into   g_rec_out.sk1_invoice_id
     from    dwh_hr_performance.dim_hr_ap_invoice inv
     where  inv.invoice_id =  g_rec_in.invoice_id
     and    inv.payment_date = g_rec_in.payment_date;

         exception
            when no_data_found then
              g_rec_out.sk1_invoice_id := 0;
      end;

 begin
      select ben.sk1_ed_beneficiary_id
     into   g_rec_out.sk1_ed_beneficiary_id
     from    dwh_hr_performance.dim_hr_bee_ed_beneficiary ben
     where  ben.ed_beneficiary_id =  g_rec_in.ed_beneficiary_id  ;

         exception
            when no_data_found then
              g_rec_out.sk1_ed_beneficiary_id := 716724;
    end;

       begin
      select benh.sk2_ed_beneficiary_id
     into   g_rec_out.sk2_ed_beneficiary_id
     from    dwh_hr_performance.dim_hr_bee_ed_beneficiary_hist benh
     where  benh.ed_beneficiary_id =  g_rec_in.ed_beneficiary_id
     and  g_date    between benh.sk2_active_from_date and benh.sk2_active_to_date;

         exception
            when no_data_found then
              g_rec_out.sk2_ed_beneficiary_id := 716726;
    end;



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
      insert into dwh_hr_performance.hr_bee_ap_invoice_payment values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).sk1_supplier_id ||
                        ' '||a_tbl_insert(g_error_index).sk1_invoice_id ||
                        ' '||a_tbl_update(g_error_index).this_mn_end_date;
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
      update dwh_hr_performance.hr_bee_ap_invoice_payment
      set      sk2_supplier_id                 = a_tbl_update(i).sk2_supplier_id,
              sk1_bee_business_unit_code      = a_tbl_update(i).sk1_bee_business_unit_code,
              amount_paid                     = a_tbl_update(i).amount_paid,
              amount_paid_ex_vat              = a_tbl_update(i).amount_paid_ex_vat,
              discount_amount                 = a_tbl_update(i).discount_amount,
              discount_amount_ex_vat          = a_tbl_update(i).discount_amount_ex_vat,
               fin_year_no                     = a_tbl_update(i).fin_year_no,
              fin_month_no                    = a_tbl_update(i).fin_month_no,
              fin_month_code                  = a_tbl_update(i).fin_month_code,
              fin_week_no                     = a_tbl_update(i).fin_week_no,
              ed_payment_terms_benefit        = a_tbl_update(i).ed_payment_terms_benefit,
              ed_payment_terms                = a_tbl_update(i).ed_payment_terms,
              ed_settlement_discount          = a_tbl_update(i).ed_settlement_discount,
              lost_opportunity_payment        = a_tbl_update(i).lost_opportunity_payment,
              sk1_benefit_factor_id1          = a_tbl_update(i).sk1_benefit_factor_id1 ,
              sk1_benefit_factor_id2          = a_tbl_update(i).sk1_benefit_factor_id2 ,
              days_to_pay                      = a_tbl_update(i).days_to_pay,
              shorter_payment_days            = a_tbl_update(i).shorter_payment_days,
              limited_days_to_pay              = a_tbl_update(i).limited_days_to_pay,
              last_updated_date               = a_tbl_update(i).last_updated_date,
             sk1_ed_beneficiary_id            = a_tbl_update(i).sk1_ed_beneficiary_id,
              sk2_ed_beneficiary_id           = a_tbl_update(i).sk2_ed_beneficiary_id
       where  sk1_supplier_id                 = a_tbl_update(i).sk1_supplier_id and
              sk1_invoice_id                   = a_tbl_update(i).sk1_invoice_id and
              this_mn_end_date                = a_tbl_update(i).this_mn_end_date;





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
                       ' '||a_tbl_update(g_error_index).sk1_supplier_id ||
                       ' '||a_tbl_update(g_error_index).sk1_invoice_id ||
                       ' '||a_tbl_update(g_error_index).this_mn_end_date;
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
   from   dwh_hr_performance.hr_bee_ap_invoice_payment
   where  sk1_supplier_id      = g_rec_out.sk1_supplier_id  and
          sk1_invoice_id        = g_rec_out.sk1_invoice_id  and
          this_mn_end_date      =g_rec_out.this_mn_end_date;

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

    l_text := 'LOAD OF hr_bee_ap_invoice_payment EX fnd_hr_ap_invoice_payment STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_started,'','','','','');

--    begin
--    execute immediate '  alter session set events ''10046 trace name context forever, level 12''   ';
--    end;

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
     DWH_LOOKUP.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

         select fin_year_no
      INTO   g_fin_year_no
      FROM   dim_calendar
      WHERE  calendar_date = g_date -15;

--**************************************************************************************************
    open c_fnd_hr_bee_edl;
    fetch c_fnd_hr_bee_edl bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_hr_bee_edl bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_hr_bee_edl;
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

END WH_PRF_HR_330U;
