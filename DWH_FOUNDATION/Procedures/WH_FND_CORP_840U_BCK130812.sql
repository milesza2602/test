--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_840U_BCK130812
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_840U_BCK130812" (p_forall_limit in integer,
p_success out boolean,
p_from_loc_no in integer,p_to_loc_no in integer) as
--**************************************************************************************************
-- backup of PRD taken before putting in version with optimization.
--**************************************************************************************************
--  Date:        February 2010
--  Author:      M Munnik
--  Purpose:     Load Allocation data to allocation tracker table FOR THE LAST 90 DAYS.
--               NB> the last 90-days sub-partitions are truncated via a procedure before this one runs.
--               For CHBD only.
--  Tables:      Input  - fnd_rtl_allocation
--               Output - fnd_alloc_tracker_alloc
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  7 may 2010 - wendy - add last insert
--  14 MAY 2010 - WENDY - add first_whse_supplier_no to output
--  28 may 2010 - wendy - ensure that extract query is same in live and full_live
--  28 may 2010 - wendy - add cancelled_qty and remove cancel_ind
--  23 July 2010 - wendy - add p_period_ind and hint to cursor

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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_date               date;
g_start_date         date;
g_rec_out            fnd_alloc_tracker_alloc%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_840U_'||p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOADS Alloc Data TO ALLOC TRACKER TABLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_alloc_tracker_alloc%rowtype index by binary_integer;
type tbl_array_u is table of fnd_alloc_tracker_alloc%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_empty_set_i       tbl_array_i;
cursor c_fnd_alloc is
SELECT
    /*+ full(a,4) */
  a.release_date,
  a.alloc_no,
  a.to_loc_no,
  a.item_no,
  i.primary_supplier_no supplier_no,
  (
  CASE
    WHEN a.po_no IS NULL
    THEN 'WH'
    ELSE 'XD'
  END) supply_chain_code,
  to_number(SUBSTR(a.wh_no,1,3)) first_dc_no,
  a.trunk_ind,
  NVL(a.alloc_qty,0) alloc_qty,
  NVL(a.orig_alloc_qty,0)   orig_alloc_qty,
  NVL(a.alloc_cancel_qty,0)  alloc_cancel_qty,
  (
  CASE
    WHEN NVL(a.orig_alloc_qty,0) <> NVL(a.alloc_cancel_qty,0)
    THEN a.orig_alloc_qty
  END) fillrate_qty,
  g_date last_updated_date,
--  (
--  CASE
--    WHEN NVL(a.orig_alloc_qty,0) = NVL(a.alloc_cancel_qty,0)
--    THEN 1
 --   ELSE 0
--  END) cancel_ind,
  to_number(SUBSTR(a.wh_no,1,3)) wh_no,
  to_number(SUBSTR(a.wh_no,1,3)) first_whse_supplier_no
FROM dwh_foundation.fnd_rtl_allocation a
JOIN dwh_performance.dim_item i
ON a.item_no = i.item_no
WHERE a.release_date BETWEEN g_start_date AND g_date
AND i.business_unit_no NOT IN(50,70);

g_rec_in             c_fnd_alloc%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_alloc%rowtype;
a_stg_input      stg_array;

a_count             integer       := 0;
a_count_i           integer       := 0;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                               := g_rec_in;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into fnd_alloc_tracker_alloc values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).release_date||
                       ' '||a_tbl_insert(g_error_index).alloc_no||
                       ' '||a_tbl_insert(g_error_index).to_loc_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
   a_count_i               := a_count_i + 1;
   a_tbl_insert(a_count_i) := g_rec_out;
   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_insert;

      a_tbl_insert  := a_empty_set_i;
      a_count_i     := 0;
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD OF fnd_alloc_tracker_alloc EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date||' '||p_from_loc_no||' '||p_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      IF p_from_loc_no = 0 THEN
        g_start_date := g_date - 30;
      ELSE
        IF p_to_loc_no = 99999 THEN
          g_start_date := g_date - 90;
          g_date       := g_date - 61;
        ELSE
          IF p_from_loc_no > 0 THEN
            g_start_date := g_date - 60;
            g_date       := g_date - 31;
          ELSE
            g_start_date := g_date - 90;
          END IF;
        END IF;
      END IF;


    l_text := 'DATA LOADED FOR PERIOD '||g_start_date||' TO '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_alloc;
    fetch c_fnd_alloc bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 500000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
   --       l_text := 'end loop -  '||
  --  to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
 --   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    fetch c_fnd_alloc bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_alloc;
       l_text := 'after close c_fnd_alloc -  '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
       l_text := 'after last local_bulk_insert -  '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,'','','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read||' '||g_start_date||' TO '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted||' '||g_start_date||' TO '||g_date;
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

END WH_FND_CORP_840U_BCK130812;
