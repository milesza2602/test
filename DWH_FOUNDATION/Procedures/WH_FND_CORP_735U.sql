--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_735U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_735U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        March 2014
--  Author:      Quentin Smit
--  Purpose:     Create triceps picking records with input from JDA Store orders foundation table
--
--  Tables:      Input  - fnd_rtl_loc_item_dy_ff_st_ord
--               Output - fnd_rtl_loc_item_dy_trcps_pick
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            fnd_rtl_loc_item_dy_trcps_pick%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_735U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE PICK ACCURACY FOODS FACTS EX JDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_m is table of fnd_rtl_loc_item_dy_trcps_pick%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_loc_item_dy_trcps_pick%rowtype index by binary_integer;
a_tbl_merge        tbl_array_m;
a_empty_set_m      tbl_array_m;

a_count             integer       := 0;
a_count_m           integer       := 0;


cursor c_st_ord_trcps is
   select location_no, item_no, post_date,
          special_cases, forecast_cases, safety_cases, over_cases
   from fnd_rtl_loc_item_dy_ff_st_ord
   where post_date = g_date;

--   where  last_updated_date >= g_yesterday;
-- order by only where sequencing is essential to the correct loading of data

g_rec_in             c_st_ord_trcps%rowtype;
-- For input bulk collect --
type stg_array is table of c_st_ord_trcps%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.item_no                     := g_rec_in.item_no;
   g_rec_out.location_no                 := g_rec_in.location_no;
   g_rec_out.into_store_date             := g_rec_in.post_date;
   g_rec_out.special_cases               := g_rec_in.special_cases;
   g_rec_out.forecast_cases              := g_rec_in.forecast_cases;
   g_rec_out.safety_cases                := g_rec_in.safety_cases;
   g_rec_out.over_cases                  := g_rec_in.over_cases;
   g_rec_out.last_updated_date           := g_date;


   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_merge as
begin
    forall i in a_tbl_merge.first .. a_tbl_merge.last
       save exceptions

merge into fnd_rtl_loc_item_dy_trcps_pick fnd_trcp_pick USING
(select a_tbl_merge(i).location_no           as	location_no,
        a_tbl_merge(i).item_no               as	item_no,
        a_tbl_merge(i).into_store_date       as	into_store_date,
        a_tbl_merge(i).special_cases         as	special_cases,
        a_tbl_merge(i).forecast_cases        as	forecast_cases,
        a_tbl_merge(i).safety_cases          as	safety_cases,
        a_tbl_merge(i).over_cases            as	over_cases,
        a_tbl_merge(i).last_updated_date     as	last_updated_date
from dual) mer_trcp_pick
on  (fnd_trcp_pick.location_no = mer_trcp_pick.location_no
and fnd_trcp_pick.item_no = mer_trcp_pick.item_no
and fnd_trcp_pick.into_store_date = mer_trcp_pick.into_store_date)
when matched then
update
set
       special_cases               = mer_trcp_pick.special_cases,
       forecast_cases              = mer_trcp_pick.forecast_cases,
       safety_cases                = mer_trcp_pick.safety_cases,
       over_cases                  = mer_trcp_pick.over_cases,
       last_updated_date           = mer_trcp_pick.last_updated_date
when not matched then
insert
(      fnd_trcp_pick.location_no,
       fnd_trcp_pick.item_no,
       fnd_trcp_pick.into_store_date,
       fnd_trcp_pick.safety_cases,
       fnd_trcp_pick.over_cases,
       fnd_trcp_pick.forecast_cases,
       fnd_trcp_pick.special_cases,
       fnd_trcp_pick.last_updated_date
)
values
(      mer_trcp_pick.location_no,
       mer_trcp_pick.item_no,
       mer_trcp_pick.into_store_date,
       mer_trcp_pick.over_cases,
       mer_trcp_pick.forecast_cases,
       mer_trcp_pick.special_cases,
       mer_trcp_pick.safety_cases,
       mer_trcp_pick.last_updated_date
);

    g_recs_inserted := g_recs_inserted + a_tbl_merge.count;

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
                       ' '||a_tbl_merge(g_error_index).location_no||
                       ' '||a_tbl_merge(g_error_index).item_no||
                       ' '||a_tbl_merge(g_error_index).into_store_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_merge;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
      a_count_m               := a_count_m + 1;
      a_tbl_merge(a_count_m) := g_rec_out;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_merge;

      a_tbl_merge  := a_empty_set_m;
      a_count_m     := 0;
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

    l_text := 'LOAD OF fnd_rtl_loc_item_dy_trcps_pick EX JDA STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    --g_date := '11/MAR/14';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_st_ord_trcps;
    fetch c_st_ord_trcps bulk collect into a_stg_input limit g_forall_limit;
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

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_st_ord_trcps bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_st_ord_trcps;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_merge;


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
end wh_fnd_corp_735u;
