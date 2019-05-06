--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_030E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_030E" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        July 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Merch hierachy dimention table in the performance layer
--               with added value ex foundation layer merch hierachy table.
--  Tables:      Input  - fnd_department, dim_subgroup
--               Output - dim_department
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  23 April 2009 - defect 1364 - Change total_descr from plural to singular
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
g_forall_limit       integer       :=  dwh_constants.vc_forall_limit;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_department%rowtype;
g_rec_in             fnd_department%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_030E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE DIM_DEPARTMENT EX FND_DEPARTMENT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of fnd_department%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_department%rowtype index by binary_integer;
type tbl_array_u is table of dim_department%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_department is
   select *
   from fnd_department;

-- No where clause used as we need to refresh all records so that the names and parents
-- can be aligned accross the entire hierachy. If a full refresh is not done accross all levels then you could
-- get name changes happening which do not filter down to lower levels where they are exploded too.

--   where last_updated_date >= g_yesterday;
--   order by group_no

-- order by only where sequencing is essential to the correct loading of data





--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.department_no                   := g_rec_in.department_no;
   g_rec_out.department_name                 := g_rec_in.department_name;
   g_rec_out.subgroup_no                     := g_rec_in.subgroup_no;
   g_rec_out.budget_intk_perc                := g_rec_in.budget_intk_perc;
   g_rec_out.budget_mkup_perc                := g_rec_in.budget_mkup_perc;
   g_rec_out.total_market_amt                := g_rec_in.total_market_amt;
   g_rec_out.markup_calc_type                := g_rec_in.markup_calc_type;
   g_rec_out.vat_ind                         := g_rec_in.vat_ind;
   g_rec_out.otb_calc_type                   := g_rec_in.otb_calc_type;
   g_rec_out.num_max_avg_counter_days        := g_rec_in.num_max_avg_counter_days;
   g_rec_out.avg_tolerance_perc              := g_rec_in.avg_tolerance_perc;
   g_rec_out.buyer_no                        := g_rec_in.buyer_no;
   g_rec_out.merch_no                        := g_rec_in.merch_no;
   g_rec_out.profit_calc_type                := g_rec_in.profit_calc_type;
   g_rec_out.purchase_type                   := g_rec_in.purchase_type;

   g_rec_out.jv_dept_ind                     := g_rec_in.jv_dept_ind;
   g_rec_out.packaging_dept_ind              := g_rec_in.packaging_dept_ind;
   g_rec_out.gifting_dept_ind                := g_rec_in.gifting_dept_ind;
   g_rec_out.non_merch_dept_ind              := g_rec_in.non_merch_dept_ind;
   g_rec_out.non_core_dept_ind               := g_rec_in.non_core_dept_ind;
   g_rec_out.bucket_dept_ind                 := g_rec_in.bucket_dept_ind;
   g_rec_out.book_magazine_dept_ind          := g_rec_in.book_magazine_dept_ind;
   g_rec_out.dept_placeholder_01_ind         := g_rec_in.dept_placeholder_01_ind;
   g_rec_out.dept_placeholder_02_ind         := g_rec_in.dept_placeholder_02_ind;
   g_rec_out.dept_placeholder_03_ind         := g_rec_in.dept_placeholder_03_ind;
   g_rec_out.dept_placeholder_04_ind         := g_rec_in.dept_placeholder_04_ind;
   g_rec_out.dept_placeholder_05_ind         := g_rec_in.dept_placeholder_05_ind;
   g_rec_out.dept_placeholder_06_ind         := g_rec_in.dept_placeholder_06_ind;
   g_rec_out.dept_placeholder_07_ind         := g_rec_in.dept_placeholder_07_ind;
   g_rec_out.dept_placeholder_08_ind         := g_rec_in.dept_placeholder_08_ind;
   g_rec_out.dept_placeholder_09_ind         := g_rec_in.dept_placeholder_09_ind;
   g_rec_out.dept_placeholder_10_ind         := g_rec_in.dept_placeholder_10_ind;
   g_rec_out.last_updated_date               := g_date;
--   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   dwh_lookup.dim_subgroup_hierachy(g_rec_out.subgroup_no,g_rec_out.subgroup_name,g_rec_out.sk1_subgroup_no,
                                         g_rec_out.group_no,g_rec_out.group_name,g_rec_out.sk1_group_no,
                                         g_rec_out.business_unit_no,g_rec_out.business_unit_name,g_rec_out.sk1_business_unit_no,
                                         g_rec_out.company_no,g_rec_out.company_name,g_rec_out.sk1_company_no);

---------------------------------------------------------
-- Added for OLAP purposes
---------------------------------------------------------
   g_rec_out.department_long_desc       := g_rec_out.department_no||' - '||g_rec_out.department_name;
   g_rec_out.subgroup_long_desc         := g_rec_out.subgroup_no||' - '||g_rec_out.subgroup_name;
   g_rec_out.group_long_desc            := g_rec_out.group_no||' - '||g_rec_out.group_name;
   g_rec_out.business_unit_long_desc    := g_rec_out.business_unit_no||' - '||g_rec_out.business_unit_name;
   g_rec_out.company_long_desc          := g_rec_out.company_no||' - '||g_rec_out.company_name;
   g_rec_out.total                      := 'TOTAL';
   g_rec_out.total_desc                 := 'ALL DEPARTMENT';

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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
      insert into dim_department values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).department_no;
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
      update dim_department
      set    department_name                 = a_tbl_update(i).department_name,
             sk1_subgroup_no                 = a_tbl_update(i).sk1_subgroup_no,
             subgroup_no                     = a_tbl_update(i).subgroup_no,
             subgroup_name                   = a_tbl_update(i).subgroup_name,
             sk1_group_no                    = a_tbl_update(i).sk1_group_no,
             group_no                        = a_tbl_update(i).group_no,
             group_name                      = a_tbl_update(i).group_name,
             sk1_business_unit_no            = a_tbl_update(i).sk1_business_unit_no,
             business_unit_no                = a_tbl_update(i).business_unit_no,
             business_unit_name              = a_tbl_update(i).business_unit_name,
             sk1_company_no                  = a_tbl_update(i).sk1_company_no,
             company_no                      = a_tbl_update(i).company_no,
             company_name                    = a_tbl_update(i).company_name,
             budget_intk_perc                = a_tbl_update(i).budget_intk_perc,
             budget_mkup_perc                = a_tbl_update(i).budget_mkup_perc,
             total_market_amt                = a_tbl_update(i).total_market_amt,
             markup_calc_type                = a_tbl_update(i).markup_calc_type,
             vat_ind                         = a_tbl_update(i).vat_ind,
             otb_calc_type                   = a_tbl_update(i).otb_calc_type,
             num_max_avg_counter_days        = a_tbl_update(i).num_max_avg_counter_days,
             avg_tolerance_perc              = a_tbl_update(i).avg_tolerance_perc,
             buyer_no                        = a_tbl_update(i).buyer_no,
             merch_no                        = a_tbl_update(i).merch_no,
             profit_calc_type                = a_tbl_update(i).profit_calc_type,
             purchase_type                   = a_tbl_update(i).purchase_type,
             department_long_desc            = a_tbl_update(i).department_long_desc,
             subgroup_long_desc              = a_tbl_update(i).subgroup_long_desc,
             group_long_desc                 = a_tbl_update(i).group_long_desc,
             business_unit_long_desc         = a_tbl_update(i).business_unit_long_desc,
             company_long_desc               = a_tbl_update(i).company_long_desc,
             jv_dept_ind                     = a_tbl_update(i).jv_dept_ind,
             packaging_dept_ind              = a_tbl_update(i).packaging_dept_ind,
             gifting_dept_ind                = a_tbl_update(i).gifting_dept_ind,
             non_merch_dept_ind              = a_tbl_update(i).non_merch_dept_ind,
             non_core_dept_ind               = a_tbl_update(i).non_core_dept_ind,
             bucket_dept_ind                 = a_tbl_update(i).bucket_dept_ind,
             book_magazine_dept_ind          = a_tbl_update(i).book_magazine_dept_ind,
             dept_placeholder_01_ind         = a_tbl_update(i).dept_placeholder_01_ind,
             dept_placeholder_02_ind         = a_tbl_update(i).dept_placeholder_02_ind,
             dept_placeholder_03_ind         = a_tbl_update(i).dept_placeholder_03_ind,
             dept_placeholder_04_ind         = a_tbl_update(i).dept_placeholder_04_ind,
             dept_placeholder_05_ind         = a_tbl_update(i).dept_placeholder_05_ind,
             dept_placeholder_06_ind         = a_tbl_update(i).dept_placeholder_06_ind,
             dept_placeholder_07_ind         = a_tbl_update(i).dept_placeholder_07_ind,
             dept_placeholder_08_ind         = a_tbl_update(i).dept_placeholder_08_ind,
             dept_placeholder_09_ind         = a_tbl_update(i).dept_placeholder_09_ind,
             dept_placeholder_10_ind         = a_tbl_update(i).dept_placeholder_10_ind,
             total                           = a_tbl_update(i).total,
             total_desc                      = a_tbl_update(i).total_desc,
             last_updated_date               = a_tbl_update(i).last_updated_date
      where  department_no                   = a_tbl_update(i).department_no  ;

      g_recs_updated := g_recs_updated + a_tbl_update.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).department_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := dwh_valid.dim_department(g_rec_out.department_no);

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_department_no  := merch_hierachy_seq.nextval;
--      g_rec_out.sk_from_date  := g_date;
--      g_rec_out.sk_to_date    := dwh_constants.sk_to_date;
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
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    dbms_output.put_line('Creating data for >= : '||g_yesterday);
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF DIM_DEPARTMENT EX FND_DEPARTMENT STARTED AT '||
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
    open c_fnd_department;
    fetch c_fnd_department bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_department bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_department;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_run_completed||sysdate;
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
end wh_prf_corp_030e;
