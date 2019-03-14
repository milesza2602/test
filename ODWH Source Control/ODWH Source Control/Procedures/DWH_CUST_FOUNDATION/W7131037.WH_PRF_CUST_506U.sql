-- ****** Object: Procedure W7131037.WH_PRF_CUST_506U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_506U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        September 2010
--  Author:      Alastair de Wet
--  Purpose:     Create cl_sub_cat dimention table in the performance layer
--               with added value ex foundation layer cl_sub_cat table.
--  Tables:      Input  - fnd_cust_cl_sub_cat
--               Output - dim_cust_cl_sub_cat
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
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
g_rec_out            dim_cust_cl_sub_cat%rowtype;
g_rec_in             fnd_cust_cl_sub_cat%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_506U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE dim_cust_cl_sub_cat EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of fnd_cust_cl_sub_cat%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of dim_cust_cl_sub_cat%rowtype index by binary_integer;
type tbl_array_u is table of dim_cust_cl_sub_cat%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_cust_cl_sub_cat is
   select *
   from fnd_cust_cl_sub_cat;

-- No where clause used as we need to refresh all records for better continuity. Volumes are very small so no impact

--   where last_updated_date = g_date;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin


   g_rec_out.cl_inq_sub_cat_no        := g_rec_in.cl_inq_sub_cat_no;
   g_rec_out.cl_inq_sub_cat_level_ind := g_rec_in.cl_inq_sub_cat_level_ind;
   g_rec_out.cl_inq_sub_cat_desc      := g_rec_in.cl_inq_sub_cat_desc;
   g_rec_out.active_ind                := g_rec_in.active_ind;
   g_rec_out.inquiry_type_desc         := g_rec_in.inquiry_type_desc ;

   g_rec_out.cl_inq_type_cat_no        := g_rec_in.cl_inq_type_cat_no ;
   g_rec_out.last_updated_date         := g_date;

--   dwh_lookup.dim_business_unit_hierachy(g_rec_out.business_unit_no,g_rec_out.business_unit_name,g_rec_out.sk1_business_unit_no,
--                                         g_rec_out.company_no,g_rec_out.company_name,g_rec_out.sk1_company_no);

---------------------------------------------------------
-- Added for OLAP purposes
---------------------------------------------------------

   g_rec_out.cl_inq_sub_cat_long_desc  := g_rec_in.cl_inq_sub_cat_no||' - '||g_rec_out.cl_inq_sub_cat_desc;

   select sk1_cl_inq_type_cat_no,cl_inq_type_cat_desc,cl_inq_type_cat_long_desc,cl_inq_feedback_no
   into   g_rec_out.sk1_cl_inq_type_cat_no,g_rec_out.cl_inq_type_cat_desc,g_rec_out.cl_inq_type_cat_long_desc,g_rec_out.cl_inq_feedback_no
   from   dim_cust_cl_type_cat
   where  cl_inq_type_cat_no = g_rec_out.cl_inq_type_cat_no;

   select sk1_cl_inq_feedback_no,cl_inq_feedback_desc,cl_inq_feedback_long_desc,cl_inq_category_no
   into   g_rec_out.sk1_cl_inq_feedback_no,g_rec_out.cl_inq_feedback_desc,g_rec_out.cl_inq_feedback_long_desc,g_rec_out.cl_inq_category_no
   from   dim_cust_cl_feedback
   where  cl_inq_feedback_no = g_rec_out.cl_inq_feedback_no;

   select sk1_cl_inq_category_no,cl_inq_category_desc,cl_inq_category_long_desc
   into   g_rec_out.sk1_cl_inq_category_no,g_rec_out.cl_inq_category_desc,g_rec_out.cl_inq_category_long_desc
   from   dim_cust_cl_inq_cat
   where  cl_inq_category_no = g_rec_out.cl_inq_category_no;




   exception
      when others then
       l_message := dwh_cust_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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
      insert into dim_cust_cl_sub_cat values a_tbl_insert(i);
      g_recs_inserted := g_recs_inserted + a_tbl_insert.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).cl_inq_sub_cat_no;
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
      update dim_cust_cl_sub_cat
      set    cl_inq_sub_cat_level_ind = a_tbl_update(i).cl_inq_sub_cat_level_ind,
             cl_inq_sub_cat_desc      = a_tbl_update(i).cl_inq_sub_cat_desc,
             cl_inq_sub_cat_long_desc = a_tbl_update(i).cl_inq_sub_cat_long_desc,
             active_ind                = a_tbl_update(i).active_ind,
             inquiry_type_desc         = a_tbl_update(i).inquiry_type_desc,
             sk1_cl_inq_type_cat_no    = a_tbl_update(i).sk1_cl_inq_type_cat_no,
             cl_inq_type_cat_no        = a_tbl_update(i).cl_inq_type_cat_no,
             cl_inq_type_cat_desc      = a_tbl_update(i).cl_inq_type_cat_desc,
             cl_inq_type_cat_long_desc = a_tbl_update(i).cl_inq_type_cat_long_desc,
             sk1_cl_inq_feedback_no    = a_tbl_update(i).sk1_cl_inq_feedback_no,
             cl_inq_feedback_no        = a_tbl_update(i).cl_inq_feedback_no,
             cl_inq_feedback_desc      = a_tbl_update(i).cl_inq_feedback_desc,
             cl_inq_feedback_long_desc = a_tbl_update(i).cl_inq_feedback_long_desc,
             sk1_cl_inq_category_no    = a_tbl_update(i).sk1_cl_inq_category_no,
             cl_inq_category_no        = a_tbl_update(i).cl_inq_category_no,
             cl_inq_category_desc      = a_tbl_update(i).cl_inq_category_desc,
             cl_inq_category_long_desc = a_tbl_update(i).cl_inq_category_long_desc,
             last_updated_date         = a_tbl_update(i).last_updated_date
      where  cl_inq_sub_cat_no        = a_tbl_update(i).cl_inq_sub_cat_no  ;

      g_recs_updated := g_recs_updated + a_tbl_update.count;


   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).cl_inq_sub_cat_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := dwh_cust_valid.dim_cust_cl_sub_cat(g_rec_out.cl_inq_sub_cat_no);

-- Place record into array for later bulk writing
   if not g_found then
      g_rec_out.sk1_cl_inq_sub_cat_no  := cust_seq.nextval;
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
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
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
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF dim_cust_cl_sub_cat EX fnd_cust_cl_sub_cat STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
    open c_fnd_cust_cl_sub_cat;
    fetch c_fnd_cust_cl_sub_cat bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_cust_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_cust_cl_sub_cat bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_cust_cl_sub_cat;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END "WH_PRF_CUST_506U";
