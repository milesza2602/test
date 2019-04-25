--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_760U_X
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_760U_X" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        January 2013
--  Author:      Q Smit
--  Purpose:     Update DC PLAN PO data at zone item suppl level to JDA fact table in the performance layer
--               with input ex foundation layer.
--               Three weeks' data on each record on foundation layer
--               must be un-pivotted to result in every day of the three weeks, on a seperate record.
--               *** MUST RUN BEFORE WH_PRF_CORP_742U AS THIS PROGRAM DOES THE DELETES !! ***
--  Tables:      Input  - fnd_zone_item_supp_ff_po_plan
--               Output - rtl_zone_item_dy_supp_po_plan
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 Sep 2016 - A Joshua Chg-202 -- Remove table fnd_jdaff_dept_rollout from selection criteria
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_deleted       integer       :=  0;
g_recs               integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_zone_item_dy_supp_po_plan%rowtype;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_year1              number;
g_year2              number;
g_year3              number;
g_week1              number;
g_week2              number;
g_week3              number;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_760U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_depot;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_depot;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD ZONE/ITEM/SUPP PLAN FACT DATA FROM JDA';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_zone_item_dy_supp_po_plan%rowtype index by binary_integer;
type tbl_array_u is table of rtl_zone_item_dy_supp_po_plan%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

g_jda_start_date    date;  -- := sysdate+1;
g_stg_date          date;

g_po_recs_updated   integer       :=  values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_zone_group_zone_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).sk1_supplier_no||
                       ' '||a_tbl_insert(g_error_index).calendar_date;
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
       update rtl_zone_item_dy_supp_po_plan
       set    row                    = a_tbl_update(i)
       where  sk1_zone_group_zone_no = a_tbl_update(i).sk1_zone_group_zone_no
       and    sk1_item_no            = a_tbl_update(i).sk1_item_no
       and    sk1_supplier_no        = a_tbl_update(i).sk1_supplier_no
       and    calendar_date          = a_tbl_update(i).calendar_date;

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
                       ' '||a_tbl_update(g_error_index).sk1_zone_group_zone_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no||
                       ' '||a_tbl_update(g_error_index).calendar_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- The input table is cleared each day before it has data added to it.
-- As a result of this, and are out of sync.
-- We have to delete all records from rtl_zone_item_dy_supp_po_plan for the same period from
-- ie. between g_start_date and g_end_date
-- Eventhough we could do a bulk delete, the consequences of running this job too late,
-- could result in resource conflicts and hence the delete taking along time and slowing-down the delete;
-- This has been in experience with other procedures.
--
-- VERY IMPORTANT !!
-- THIS PROCEDURE MUST RUN BEFORE THE EXISTING PROGRAM (WH_PRF_CORP_742U) IN ORDER FOR THE DELETES
-- TO BE DONE ONLY ONCE FOR THIS TABLE.
-- END OF VERY IMPORTANT MESSAGE !!
--**************************************************************************************************
procedure delete_rtl_om as
begin

  g_recs_deleted  := 0;

/*  delete from rtl_zone_item_dy_supp_po_plan r
   where exists (
     with aa as (
       select b.sk1_zone_group_zone_no, c.sk1_item_no, d.sk1_supplier_no  --, a.calendar_date--, a.last_updated_date
         from fnd_zone_item_supp_om_po_plan a, dim_zone b, dim_item c, dim_supplier d
        where a.last_updated_date = g_date-1
          and a.zone_group_no = b.zone_group_no
          and a.item_no = c.item_no
          and a.supplier_no = d.supplier_no
          and a.zone_no = b.zone_no)

           select aa.sk1_zone_group_zone_no,
                  aa.sk1_item_no,
                  aa.sk1_supplier_no
                  --aa.calendar_date
             from aa
            where aa.sk1_zone_group_zone_no = r.sk1_zone_group_zone_no
              and aa.sk1_item_no = r.sk1_item_no
              and aa.sk1_supplier_no = r.sk1_supplier_no)
              --and aa.calendar_date = r.calendar_date)

       and r.calendar_date between g_start_date and g_end_date  ;

     g_recs := sql%rowcount;
     g_recs_deleted  := g_recs_deleted  + g_recs;
  COMMIT;

   exception
      when dwh_errors.e_insert_error then
       l_message := 'delete rtl_OM error '||dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'delete rtl_OM error '||dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
  /*
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := 'delete rtl_JDA error'||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
     for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_zone_group_zone_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_supplier_no||
                       ' '||a_tbl_update(g_error_index).calendar_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop*
-- Delete from RTL all records for the period being refreshed.
--**************************************************************************************************
    l_text := 'Deleting OM data started for- '||g_start_date||'-'||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    delete_rtl_om;
    l_text := 'Deleting OM data ended  for- '||g_start_date||'-'||g_end_date||' recs='||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    g_jda_start_date := g_date + 1;
    l_text := 'Deleting JDA data started for- '||g_jda_start_date||'-'||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    delete_rtl_jda;
    l_text := 'Deleting JDA data ended  for- '||g_jda_start_date||'-'||g_end_date||' recs='||g_recs_deleted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    select min(calendar_date) into g_stg_date from stg_jdaff_po_plan_cpy;
    l_text := 'Calendar_date on Staging - '||g_stg_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'g_year1 = ' || g_year1 || ' : g_week1 = ' || g_week1;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'g_year2 = ' || g_year2 || ' : g_week2 = ' || g_week2;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'g_year3 = ' || g_year3 || ' : g_week3 = ' || g_week3;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 --   execute immediate 'alter session enable parallel dml';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_zone_item_supp_po_plan;
    fetch c_zone_item_supp_po_plan bulk collect into a_stg_input limit g_forall_limit;
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

          g_rec_in                := a_stg_input(i);

          local_address_variables;
          local_write_output;

       end loop;
       fetch c_zone_item_supp_po_plan bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_zone_item_supp_po_plan;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
    commit;

    l_text := 'Done with inserts, committed, now checking for PO data substitution';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    get_po_data;

    l_text := 'Records substituted with PO data - '||g_po_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,g_recs_deleted,'');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    commit;
    p_success := true;


end wh_prf_corp_760u_x;
