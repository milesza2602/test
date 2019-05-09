--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_030G
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_030G" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        July 2008
--  Author:      Alastair de Wet
--  Purpose:     Create Merch hierachy dimention table in the performance layer
--               with added value ex foundation layer merch hierachy table.
--  Tables:      Input  - fnd_subclass, dim_class
--               Output - dim_subclass
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  23 April 2009 - defect 1354 - Change total_descr from plural to singular
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
g_rec_out            dim_subclass%rowtype;
g_rec_in             fnd_subclass%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_030G';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE DIM_SUBCLASS EX FND_SUBCLASS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of fnd_subclass%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_subclass%rowtype index by binary_integer;
type tbl_array_u is table of dim_subclass%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_subclass is
   select *
   from fnd_subclass;

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
   g_rec_out.class_no                        := g_rec_in.class_no;
   g_rec_out.subclass_no                     := g_rec_in.subclass_no;
   g_rec_out.subclass_name                   := g_rec_in.subclass_name;
   g_rec_out.num_mkup_multi_price_days       := g_rec_in.num_mkup_multi_price_days;
   g_rec_out.num_sales_lead_time_weeks       := g_rec_in.num_sales_lead_time_weeks;
-- g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.last_updated_date               := g_date;

   dwh_lookup.dim_class_hierachy(g_rec_out.class_no,g_rec_out.department_no,g_rec_out.class_name,g_rec_out.sk1_class_no,
                                         g_rec_out.department_name,g_rec_out.sk1_department_no,
                                         g_rec_out.subgroup_no,g_rec_out.subgroup_name,g_rec_out.sk1_subgroup_no,
                                         g_rec_out.group_no,g_rec_out.group_name,g_rec_out.sk1_group_no,
                                         g_rec_out.business_unit_no,g_rec_out.business_unit_name,g_rec_out.sk1_business_unit_no,
                                         g_rec_out.company_no,g_rec_out.company_name,g_rec_out.sk1_company_no);

---------------------------------------------------------
-- Added for OLAP purposes
---------------------------------------------------------
   g_rec_out.subclass_long_desc         := g_rec_out.subclass_no||' - '||g_rec_out.subclass_name;
   g_rec_out.class_long_desc            := g_rec_out.class_no||' - '||g_rec_out.class_name;
   g_rec_out.department_long_desc       := g_rec_out.department_no||' - '||g_rec_out.department_name;
   g_rec_out.subgroup_long_desc         := g_rec_out.subgroup_no||' - '||g_rec_out.subgroup_name;
   g_rec_out.group_long_desc            := g_rec_out.group_no||' - '||g_rec_out.group_name;
   g_rec_out.business_unit_long_desc    := g_rec_out.business_unit_no||' - '||g_rec_out.business_unit_name;
   g_rec_out.company_long_desc          := g_rec_out.company_no||' - '||g_rec_out.company_name;
   g_rec_out.total                      := 'TOTAL';
   g_rec_out.total_desc                 := 'ALL SUBCLASS';

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
      insert into dim_subclass values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).subclass_no||
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
      update dim_subclass
      set    subclass_name                   = a_tbl_update(i).subclass_name,
             sk1_class_no                    = a_tbl_update(i).sk1_class_no,
             class_name                      = a_tbl_update(i).class_name,
             sk1_department_no               = a_tbl_update(i).sk1_department_no,
             department_name                 = a_tbl_update(i).department_name,
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
             num_mkup_multi_price_days       = a_tbl_update(i).num_mkup_multi_price_days,
             num_sales_lead_time_weeks       = a_tbl_update(i).num_sales_lead_time_weeks,
             subclass_long_desc              = a_tbl_update(i).subclass_long_desc,
             class_long_desc                 = a_tbl_update(i).class_long_desc,
             department_long_desc            = a_tbl_update(i).department_long_desc,
             subgroup_long_desc              = a_tbl_update(i).subgroup_long_desc,
             group_long_desc                 = a_tbl_update(i).group_long_desc,
             business_unit_long_desc         = a_tbl_update(i).business_unit_long_desc,
             company_long_desc               = a_tbl_update(i).company_long_desc,
             total                           = a_tbl_update(i).total,
             total_desc                      = a_tbl_update(i).total_desc,
             last_updated_date               = a_tbl_update(i).last_updated_date
      where  department_no                   = a_tbl_update(i).department_no and
             class_no                        = a_tbl_update(i).class_no and
             subclass_no                     = a_tbl_update(i).subclass_no;

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
                       ' '||a_tbl_update(g_error_index).department_no||
                       ' '||a_tbl_update(g_error_index).subclass_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := dwh_valid.dim_subclass(g_rec_out.subclass_no,g_rec_out.class_no,g_rec_out.department_no);

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found  then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).department_no = g_rec_out.department_no and
            a_tbl_insert(i).class_no = g_rec_out.class_no and
            a_tbl_insert(i).subclass_no = g_rec_out.subclass_no then
            g_found := TRUE;
         end if;
      end loop;
   end if;

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_subclass_no  := merch_hierachy_seq.nextval;
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

    l_text := 'LOAD OF DIM_SUBCLASS EX FND_SUBCLASS STARTED AT '||
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
    open c_fnd_subclass;
    fetch c_fnd_subclass bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_fnd_subclass bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_subclass;
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
end wh_prf_corp_030g;