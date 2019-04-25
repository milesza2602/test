--------------------------------------------------------
--  DDL for Procedure WH_PRF_HR_320U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_PERFORMANCE"."WH_PRF_HR_320U" 
(p_forall_limit in integer,p_success out boolean)
AS

--**************************************************************************************************
--  Date:        December 2011
--  Author:      Wendy Lyttle
--  Purpose:     Create hr_bee_invoice fact table in the performance layer
--               ex foundation layer fnd_hr_invoice_supplier.
--  Tables:      Input  - fnd_hr_invoice_supplier
--               Output - hr_invoice_supplier_mth
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  Date        :   	Jul 2014
--  Changed by  : Kgomotso Lehabe
--  Change      :	Add Capital_Goods_ind to hr_invoice_supplier_mth as part of the key
--              : Remove the filters
--                    Where country_code = 'ZA'
--                    and    nvl(ds.bee_status,0)     <> 'Exclusion'
--               : Change the way recognition_all gets calculated
--               : Change process to report on Suppliers even if there are no transactions for the month.
--               : Drop columns from AP Invoice Payment as the grain is now different. Capital goods indicator not required for payments
--               : They  will be in a new fact table HR_INVOICE_PAYMENT sum(ai.amount_paid) amount_paid,
--                   amount_paid,
--                   amount_paid_ex_vat,
--                   discount_amount,
--                  discount_amount_ex_vat
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
g_rec_out            dwh_hr_performance.hr_invoice_supplier_mth%rowtype;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        := 0;
g_procuremnt_contract number        := 0;
g_sd_beneficiary    number        := 0;
g_first_time_supplier number        := 0;
g_fin_year_no        number        := 0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_HR_320U';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE hr_invoice_supplier_mth EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dwh_hr_performance.hr_invoice_supplier_mth%rowtype index by binary_integer;
type tbl_array_u is table of dwh_hr_performance.hr_invoice_supplier_mth%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_hr_bee_sed is


 with dates as
    (select distinct supplier_id,
             capital_goods_ind,
             org_id,
           cal.this_mn_end_date,
           cal.fin_month_code,
           cal.fin_month_no,
           cal.fin_year_no,
           cal.fin_week_no
      from dwh_hr_foundation.fnd_hr_invoice_supplier ai,
        dim_calendar_wk cal
        where  this_mn_end_date < g_date
        and   cal.fin_year_no=  g_fin_year_no
        and    ai.org_id       = 81
        and  ai.supplier_id    <> 2598
        )

   select dc.this_mn_end_date,
          ds.sk1_supplier_id,
          dc.capital_goods_ind,
          max(dsh.sk2_supplier_id) sk2_supplier_id,
          max(ds.verification_status) verification_status ,
          max(ds.recognition_level_perc) recognition_level_perc,
          max(ds.value_adding_num) value_adding_num,
          max(ds.ed_beneficiary_num) ed_beneficiary_num ,
          max(ds.black_professional_company_num) black_professional_company_num,
          max(ds.empowering_num) empowering_num,
          max(ds.sd_beneficiary_num) sd_beneficiary_num,
          dc.fin_month_code,
          dc.fin_month_no,
          dc.fin_year_no,
          sum(coalesce(ai.invoice_amount,0)) invoice_amount,
          sum(coalesce(ai.invoice_amount_ex_vat,0)) invoice_amount_ex_vat,
          sum(coalesce(ai.vat_amount,0)) vat_amount ,
          sum(coalesce(ai.base_recognition,0))  base_recognition,
          sum(coalesce(ai.recognition_all,0)) recognition_all,
          sum(coalesce(ai.enhanced_recognition_sd_ben,0)) enhanced_recognition_sd_ben,
          sum(coalesce(ai.enhanced_recognition_1st_time,0)) enhanced_recognition_1st_time,
          sum(coalesce(ai.enhanced_recognition_procure,0))  enhanced_recognition_procure,
          max(nvl(enhanced_recog_sd_ben_perc,0))  enhanced_recog_sd_ben_perc,
          max(nvl(enhanced_recog_1st_time_perc,0)) enhanced_recog_1st_time_perc,
          max(nvl(enhanced_recog_procure_perc,0))  enhanced_recog_procure_perc


    from  dates dc
  left outer join   dwh_hr_foundation.fnd_hr_invoice_supplier ai
   on     (dc.supplier_id = ai.supplier_id and
           dc.capital_goods_ind = ai.capital_goods_ind and
          dc.fin_year_no  = ai.fin_year_no  and
          dc.fin_week_no  = ai.fin_week_no ),
          dwh_hr_performance.dim_hr_bee_supplier ds
         left join dim_calendar dcl
          on ds.supplier_take_on_date =dcl.calendar_date,
          dwh_hr_performance.dim_hr_bee_supplier_hist dsh

   where  dc.supplier_id  = ds.supplier_id  and
          dc.supplier_id  = dsh.supplier_id   and
          dc.this_mn_end_date   between dsh.sk2_active_from_date and dsh.sk2_active_to_date
   and    dc.org_id       = 81
   --and    ds.country_code = 'ZA'
   and    ds.pay_group not in ('EMPLOYEE_PAYMENT','JOINT_VENTURE_PAYMENT','UTILITY_ENGEN_PAYMENT',
                               'FRANCHISEE_PAYMENT','WFS_PAYMENT','WFS_POS','BIDVEST_PAYMENT')
   and    dc.supplier_id    <> 2598
  -- and    nvl(ds.bee_status,0)     <> 'Exclusion'
   group by dc.this_mn_end_date,ds.sk1_supplier_id, dc.capital_goods_ind, dc.fin_month_code,dc.fin_month_no,dc.fin_year_no
    ;


g_rec_in                   c_fnd_hr_bee_sed%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_hr_bee_sed%rowtype;
a_stg_input      stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

  g_rec_out.this_mn_end_date              := g_rec_in.this_mn_end_date;
  g_rec_out.sk1_supplier_id               := g_rec_in.sk1_supplier_id;
  g_rec_out.sk2_supplier_id               := g_rec_in.sk2_supplier_id;
  g_rec_out.fin_month_code                := g_rec_in.fin_month_code;
  g_rec_out.fin_month_no                  := g_rec_in.fin_month_no;
  g_rec_out.fin_year_no                   := g_rec_in.fin_year_no;
  g_rec_out.invoice_amount                := g_rec_in.invoice_amount;
  g_rec_out.invoice_amount_ex_vat         := g_rec_in.invoice_amount_ex_vat;
  g_rec_out.vat_amount                    := g_rec_in.vat_amount;
  g_rec_out.capital_goods_ind             := g_rec_in.capital_goods_ind;
  g_rec_out.base_recognition              := g_rec_in.base_recognition;
  g_rec_out.recognition_all               := g_rec_in.recognition_all;
  g_rec_out.enhanced_recognition_sd_ben   := g_rec_in.enhanced_recognition_sd_ben;
  g_rec_out.enhanced_recognition_1st_time := g_rec_in.enhanced_recognition_1st_time;
  g_rec_out.enhanced_recognition_procure  := g_rec_in.enhanced_recognition_procure;
  g_rec_out.enhanced_recog_sd_ben_perc    := g_rec_in.enhanced_recog_sd_ben_perc;
  g_rec_out.enhanced_recog_1st_time_perc  := g_rec_in.enhanced_recog_1st_time_perc;
  g_rec_out.enhanced_recog_procure_perc   := g_rec_in.enhanced_recog_procure_perc ;


  g_rec_out.last_updated_date             := g_date;


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
      insert into dwh_hr_performance.hr_invoice_supplier_mth values a_tbl_insert(i);
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
                       ' '||a_tbl_update(g_error_index).capital_goods_ind;
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
      update dwh_hr_performance.hr_invoice_supplier_mth
       set    invoice_amount                = a_tbl_update(i).invoice_amount,
              invoice_amount_ex_vat         = a_tbl_update(i).invoice_amount_ex_vat,
              vat_amount                    = a_tbl_update(i).vat_amount,
              base_recognition              = a_tbl_update(i).base_recognition,
              recognition_all               = a_tbl_update(i).recognition_all,
              fin_month_code                = a_tbl_update(i).fin_month_code,
              fin_month_no                  = a_tbl_update(i).fin_month_no,
              fin_year_no                   = a_tbl_update(i).fin_year_no,
              last_updated_date             = a_tbl_update(i).last_updated_date,
              enhanced_recognition_sd_ben   = a_tbl_update(i).enhanced_recognition_sd_ben,
              enhanced_recognition_1st_time = a_tbl_update(i).enhanced_recognition_1st_time,
              enhanced_recognition_procure  = a_tbl_update(i).enhanced_recognition_procure,
              enhanced_recog_sd_ben_perc    = a_tbl_update(i).enhanced_recog_sd_ben_perc,
              enhanced_recog_1st_time_perc  = a_tbl_update(i).enhanced_recog_1st_time_perc,
              enhanced_recog_procure_perc   = a_tbl_update(i).enhanced_recog_procure_perc
       where  this_mn_end_date            = a_tbl_update(i).this_mn_end_date
         and  sk1_supplier_id             = a_tbl_update(i).sk1_supplier_id
         and  capital_goods_ind           = a_tbl_update(i).capital_goods_ind ;

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
                       ' '||a_tbl_update(g_error_index).capital_goods_ind ;
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
   from   dwh_hr_performance.hr_invoice_supplier_mth
       where  sk1_supplier_id           = g_rec_out.sk1_supplier_id  and
              this_mn_end_date          = g_rec_out.this_mn_end_date and
              capital_goods_ind         = g_rec_out.capital_goods_ind ;

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

    l_text := 'LOAD OF hr_invoice_supplier_mth EX fnd_hr_invoice_supplier STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_hr_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    select fin_year_no
    INTO   g_fin_year_no
    FROM   dim_calendar
    WHERE  calendar_date = g_date -15;

    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_hr_bee_sed;
    fetch c_fnd_hr_bee_sed bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_hr_bee_sed bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_hr_bee_sed;
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

END WH_PRF_HR_320U;
