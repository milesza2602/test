--------------------------------------------------------
--  DDL for Procedure WH_FND_HR_321U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_HR_FOUNDATION"."WH_FND_HR_321U" 
(p_forall_limit in integer,p_success out boolean)
AS

--**************************************************************************************************
--  Date:        September 2014
--  Author:      Kgomotso Lehabe
--  Purpose:     Create fnd_hr_invoice_supplier in the foundation Layer
--               ex foundation layer fnd_hr_ap_invoice.
--  Tables:      Input  - fnd_hr_ap_invoice
--               Output - fnd_hr_invoice_supplier
--  Packages:    constants, dwh_hr_log, dwh_hr_valid
--
--  Maintenance:
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
g_rec_out            fnd_hr_invoice_supplier%rowtype;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_count              number        := 0;
g_month_end          date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_HR_321U';
l_name               sys_dwh_log.log_name%type                 := dwh_hr_constants.vc_log_name_hr_bee;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_hr_constants.vc_log_system_name_hr_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_hr_constants.vc_log_script_hr_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE fnd_hr_invoice_supplier EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_hr_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_hr_invoice_supplier%rowtype index by binary_integer;
type tbl_array_u is table of fnd_hr_invoice_supplier%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_hr_bee_sed is
  select ai.org_id,
          ai.supplier_id,
          ai.invoice_id,
          ai.capital_goods_ind,
          ai.invoice_no,
          ai.fin_month_no,
          ai.fin_year_no,
          ai.fin_week_no,
          ai.invoice_type_code,
          ai.invoice_gl_period,
          ai.invoice_date ,
          ai.invoice_amount ,
          ai.vat_amount ,
          ai.invoice_amount_ex_vat ,
          ai.posted_ind ,
          ai.post_date  ,
          ds.verification_status ,
          ds.recognition_level_perc,
          ds.value_adding_num,
          ds.ed_beneficiary_num,
          ds.empowering_num,
          ds.sd_beneficiary_num,
          dc.fin_month_code,
          ct.bee_procurement_contract_ind,
          ct.bee_sd_contract_ind,
          ct.bee_import_transition_ind,
          ds.bee_classification_code,
          ds.local_import,
          ds.country_code,
          ds.supplier_take_on_date,
          dcl.fin_year_no supplier_take_on_fin_yr,
          ds.black_ownership_perc,
          ds.sector_code_alignment_date,
          ed.ed_sd_indicator

   from   dwh_hr_foundation.fnd_hr_ap_invoice ai,
          dwh_hr_performance.dim_hr_bee_supplier ds
            left join dwh_hr_performance.br_hr_supplier_contract_type ct
            on ct.sk1_supplier_id =  ds.sk1_supplier_id
          left join dim_calendar dcl
          on ds.supplier_take_on_date =dcl.calendar_date
          left join  dwh_hr_performance.dim_hr_bee_ed_beneficiary ed
        --  on trim(to_char(ed.vendor_no)) =  trim(supplier_no)
         on ed.vendor_no =  to_number(supplier_no),
          dwh_hr_performance.dim_hr_bee_supplier_hist dsh,
          dim_calendar_WK dc

   where  ai.supplier_id  = ds.supplier_id  and
          ai.supplier_id  = dsh.supplier_id   and
          dc.this_mn_end_date   between dsh.sk2_active_from_date and dsh.sk2_active_to_date
   and    ai.FIN_YEAR_NO  = dc.FIN_YEAR_NO
   AND    AI.FIN_WEEK_NO  = DC.FIN_WEEK_NO
   and    ai.org_id       = 81
   --and    ds.country_code = 'ZA'
   and    ds.pay_group not in ('EMPLOYEE_PAYMENT','JOINT_VENTURE_PAYMENT','UTILITY_ENGEN_PAYMENT',
                               'FRANCHISEE_PAYMENT','WFS_PAYMENT','WFS_POS','BIDVEST_PAYMENT')
   and    ds.supplier_id    <> 2598
  -- and    nvl(ds.bee_status,0)     <> 'Exclusion'
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

  g_rec_out.org_id                          := g_rec_in.org_id;
  g_rec_out.supplier_id                     := g_rec_in.supplier_id;
  g_rec_out.invoice_no                      := g_rec_in.invoice_no;
  g_rec_out.invoice_id                      := g_rec_in.invoice_id ;
  g_rec_out.capital_goods_ind               := g_rec_in.capital_goods_ind;
  g_rec_out.fin_month_no                    := g_rec_in.fin_month_no;
  g_rec_out.fin_week_no                    := g_rec_in.fin_week_no;
  g_rec_out.fin_year_no                     := g_rec_in.fin_year_no;
  g_rec_out.invoice_type_code               := g_rec_in.invoice_type_code;
  g_rec_out.invoice_gl_period               := g_rec_in.invoice_gl_period;
  g_rec_out.invoice_date                    := g_rec_in.invoice_date;
  g_rec_out.invoice_amount                  := g_rec_in.invoice_amount;
  g_rec_out.vat_amount                      := g_rec_in.vat_amount;
  g_rec_out.invoice_amount_ex_vat           := g_rec_in.invoice_amount_ex_vat;
  g_rec_out.posted_ind                      := g_rec_in.posted_ind;
  g_rec_out.post_date                       := g_rec_in.post_date;
  g_rec_out.verification_status             := g_rec_in.verification_status;
  g_rec_out.recognition_level_perc          := g_rec_in.recognition_level_perc;
  g_rec_out.value_adding_num                := g_rec_in.value_adding_num;
  g_rec_out.ed_beneficiary_num              := g_rec_in.ed_beneficiary_num;
  g_rec_out.empowering_num                  := g_rec_in.empowering_num;
  g_rec_out.sd_beneficiary_num              := g_rec_in.sd_beneficiary_num;
  g_rec_out.bee_classification_code         := g_rec_in.bee_classification_code;
  g_rec_out.local_import                    := g_rec_in.local_import;
  g_rec_out.supplier_take_on_date           := g_rec_in.supplier_take_on_date;
  g_rec_out.black_ownership_perc            := g_rec_in.black_ownership_perc;

  g_rec_out.last_updated_date                 := g_date;


  g_rec_out.base_recognition            := 0;
  g_rec_out.recognition_all             := 0;

  g_rec_out.enhanced_recognition_procure    := 0;
  g_rec_out.enhanced_recognition_sd_ben     := 0;
  g_rec_out.enhanced_recognition_1st_time   := 0;
  g_rec_out.enhanced_recog_sd_ben_perc     := 0;
  g_rec_out.enhanced_recog_procure_perc      := 0;
  g_rec_out.enhanced_recog_1st_time_perc      := 0;

  select this_mn_end_date
   into  g_month_end
   from   dim_calendar
   where  calendar_date = g_date - 15; -- We want the date to a  week prior to the run. The previous month end. We set it back by 15 days in case the load is delayed.


    if upper(g_rec_in.verification_status) = 'VERIFIED'  and  upper(g_rec_in.country_code) <> 'ZA' and  g_rec_in.bee_import_transition_ind = 1 then
        g_rec_out.base_recognition   := g_rec_out.invoice_amount_ex_vat * g_rec_in.recognition_level_perc;
      end if;

     if upper(g_rec_in.verification_status) = 'VERIFIED' and g_rec_in.sector_code_alignment_date > g_month_end then
          g_rec_out.base_recognition   := g_rec_out.invoice_amount_ex_vat * g_rec_in.recognition_level_perc;

       elsif  upper(g_rec_in.verification_status) = 'VERIFIED' and g_rec_in.sector_code_alignment_date <= g_month_end then
          if  g_rec_in.empowering_num=1 then
             g_rec_out.base_recognition   := g_rec_out.invoice_amount_ex_vat * g_rec_in.recognition_level_perc;
          end if;
    end if;

  if upper(g_rec_in.verification_status) = 'VERIFIED'  and g_rec_in.empowering_num=1 and  upper(g_rec_in.country_code) ='ZA' then
     g_rec_out.base_recognition   := g_rec_out.invoice_amount_ex_vat * g_rec_in.recognition_level_perc;


      if  g_rec_in.bee_sd_contract_ind = 1  and g_rec_in.ed_sd_indicator = 'SD' then
         g_rec_out.enhanced_recog_sd_ben_perc   := 0.2;
         g_rec_out.enhanced_recognition_sd_ben  :=   g_rec_out.base_recognition  *  g_rec_out.enhanced_recog_sd_ben_perc;
     end if;

      if  g_rec_in.bee_procurement_contract_ind = 1 and upper(g_rec_in.bee_classification_code) in ('QSE (MEDIUM)', 'EME (SMALL)') and  g_rec_in.black_ownership_perc = '=> 51%' then
          g_rec_out.enhanced_recog_procure_perc := 0.2;
         g_rec_out.enhanced_recognition_procure  :=  g_rec_out.base_recognition *  g_rec_out.enhanced_recog_procure_perc;
       end if;

      if  g_rec_in.supplier_take_on_fin_yr  = g_rec_in.fin_year_no then
          g_rec_out.enhanced_recog_1st_time_perc   := 0.2;
          g_rec_out.enhanced_recognition_1st_time :=   g_rec_out.base_recognition  *   g_rec_out.enhanced_recog_1st_time_perc;
      end if;
  end if;


      g_rec_out.recognition_all    :=  g_rec_out.base_recognition +
                                       g_rec_out.enhanced_recognition_sd_ben +
                                       g_rec_out.enhanced_recognition_procure +
                                       g_rec_out.enhanced_recognition_1st_time  ;

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
      insert into fnd_hr_invoice_supplier values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).invoice_id ||
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
      update fnd_hr_invoice_supplier
       set  org_id                        = a_tbl_update(i).org_id,
            supplier_id                   = a_tbl_update(i).supplier_id,
            invoice_no                    = a_tbl_update(i).invoice_no,
            fin_month_no                  = a_tbl_update(i).fin_month_no,
            fin_week_no                   = a_tbl_update(i).fin_week_no,
            fin_year_no                   = a_tbl_update(i).fin_year_no,
            invoice_type_code             = a_tbl_update(i).invoice_type_code,
            invoice_gl_period             = a_tbl_update(i).invoice_gl_period,
            invoice_date                  = a_tbl_update(i).invoice_date,
            invoice_amount                = a_tbl_update(i).invoice_amount,
            vat_amount                    = a_tbl_update(i).vat_amount,
            invoice_amount_ex_vat         = a_tbl_update(i).invoice_amount_ex_vat,
            posted_ind                    = a_tbl_update(i).posted_ind,
            post_date                     = a_tbl_update(i).post_date,
            verification_status           = a_tbl_update(i).verification_status,
            recognition_level_perc        = a_tbl_update(i).recognition_level_perc,
            value_adding_num              = a_tbl_update(i).value_adding_num,
            ed_beneficiary_num            = a_tbl_update(i).ed_beneficiary_num,
            empowering_num                = a_tbl_update(i).empowering_num,
            sd_beneficiary_num            = a_tbl_update(i).sd_beneficiary_num,
            bee_classification_code       = a_tbl_update(i).bee_classification_code,
            local_import                  = a_tbl_update(i).local_import,
            supplier_take_on_date         = a_tbl_update(i).supplier_take_on_date,
            black_ownership_perc          = a_tbl_update(i).black_ownership_perc,
            last_updated_date             = a_tbl_update(i).last_updated_date,
            enhanced_recognition_sd_ben   = a_tbl_update(i).enhanced_recognition_sd_ben,
            enhanced_recognition_1st_time = a_tbl_update(i).enhanced_recognition_1st_time,
            enhanced_recognition_procure  = a_tbl_update(i).enhanced_recognition_procure,
            base_recognition              = a_tbl_update(i).base_recognition,
            recognition_all               = a_tbl_update(i).recognition_all,
            enhanced_recog_sd_ben_perc    = a_tbl_update(i).enhanced_recog_sd_ben_perc,
            enhanced_recog_1st_time_perc  = a_tbl_update(i).enhanced_recog_1st_time_perc ,
            enhanced_recog_procure_perc   = a_tbl_update(i).enhanced_recog_procure_perc


       where invoice_id             = a_tbl_update(i).invoice_id
         and  capital_goods_ind      = a_tbl_update(i).capital_goods_ind ;


     g_rec_out.last_updated_date := g_date;
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
                       ' '||a_tbl_update(g_error_index).Invoice_id ||
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
   from   DWH_HR_FOUNDATION.fnd_hr_invoice_supplier
       where   invoice_id       = g_rec_out.invoice_id
       and capital_goods_ind =g_rec_out.capital_goods_ind;

   if g_count = 1 then
      g_found := TRUE;
   end if;
-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).invoice_id  = g_rec_out.invoice_id
         and a_tbl_insert(i).capital_goods_ind =g_rec_out.capital_goods_ind then
            g_found := TRUE;
         end if;
      end loop;
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

    l_text := 'LOAD OF fnd_hr_invoice_supplier EX fnd_hr_ap_invoice STARTED AT '||
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

END WH_FND_HR_321U;
