--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_052U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_052U" 
                                                                                                                                                                      (p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        August 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Department dimention table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - stg_rms_department_cpy
--               Output - fnd_department
--  Packages:    dwh_constants, dwh_log, dwh_valid
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
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_department_hsp.sys_process_msg%type;
g_rec_out            fnd_department%rowtype;
g_rec_in             stg_rms_department_cpy%rowtype;
g_found              boolean;
g_valid              boolean;
g_restructure_ind    dim_control.restructure_ind%type;
g_subgroup_no        fnd_department.subgroup_no%type;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_052U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE DEPARTMENT MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_rms_department_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_department%rowtype index by binary_integer;
type tbl_array_u is table of fnd_department%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_department_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_department_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_rms_department is
   select *
   from stg_rms_department_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                           := 'N';

   g_rec_out.last_updated_date               := g_date;


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
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
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

--   if not dwh_valid.source_status(g_rec_out.source_data_status_code) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_constants.vc_invalid_source_code;
--   end if;


   if not dwh_valid.indicator_field(g_rec_out.vat_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.jv_dept_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not dwh_valid.indicator_field(g_rec_out.packaging_dept_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_invalid_indicator;
   end if;

   if not  dwh_valid.fnd_subgroup(g_rec_out.subgroup_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_subgroup_not_found;
     l_text          := dwh_constants.vc_subgroup_not_found||g_rec_out.department_no||' '||g_rec_out.subgroup_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if g_restructure_ind = 0 then
      begin
        select subgroup_no
        into   g_subgroup_no
        from   fnd_department
        where  department_no = g_rec_out.department_no;

        exception
        when no_data_found then
          g_subgroup_no := g_rec_out.subgroup_no;
      end;

      if g_subgroup_no <> g_rec_out.subgroup_no then
         dwh_log.restructure_error(g_rec_in.sys_source_batch_id,g_rec_in.sys_source_sequence_no,g_date,l_procedure_name,
                                  'fnd_department',g_rec_out.department_no,g_subgroup_no,g_rec_out.subgroup_no);
         g_hospital      := 'Y';
         g_hospital_text := 'Trying to illegally restructure hierarchy ';
         l_text          := 'Trying to illegally restructure hierarchy '||g_rec_out.department_no||' '||g_rec_out.subgroup_no  ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
   end if;


   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin

   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into stg_rms_department_hsp values g_rec_in;
   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end local_write_hospital;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into fnd_department values a_tbl_insert(i);

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
       update fnd_department
       set    department_name                 = a_tbl_update(i).department_name,
              subgroup_no                     = a_tbl_update(i).subgroup_no,
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
              source_data_status_code         = a_tbl_update(i).source_data_status_code,
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
              last_updated_date                = a_tbl_update(i).last_updated_date
       where  department_no                   = a_tbl_update(i).department_no  ;




       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

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
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update stg_rms_department_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_staging_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := dwh_valid.fnd_department(g_rec_out.department_no);

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).department_no = g_rec_out.department_no then
            g_found := TRUE;
         end if;
      end loop;
   end if;

-- Place data into and array for later writing to table in bulk
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
--   if a_count > 1000 then
   if a_count > g_forall_limit then
      local_bulk_insert;
      local_bulk_update;
      local_bulk_staging_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_staging1    := a_empty_set_s1;
      a_staging2    := a_empty_set_s2;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      a_count_stg   := 0;

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
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_DEPARTMENT EX RMS STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

    select restructure_ind
    into   g_restructure_ind
    from   dim_control;

    l_text := 'RESTRUCTURE_IND IS:- '||g_restructure_ind;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);



--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_department;
    fetch c_stg_rms_department bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
         if g_hospital = 'Y' then
            local_write_hospital;
         else
            local_write_output;
         end if;
      end loop;
    fetch c_stg_rms_department bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_rms_department;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;


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
end wh_fnd_corp_052u;
