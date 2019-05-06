--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_621U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_621U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Apr 2009
--  Author:      Alastair de Wet
--  Purpose:
--  Tables:      Input  -   fnd_rtl_loc_item_dy_rms_wac and ALLOC
--               Output -   fnd_rtl_allocation
--  Packages:    constants, dwh_log, dwh_valid
--  Comments:
--
--  Maintenance:

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
g_rec_out            fnd_rtl_allocation%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_start_date         date          := trunc(sysdate) - 60;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_621U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE ALLOC EX FND_RTL_LOC_ITEM_DY_RMS_WAC';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_rtl_allocation%rowtype index by binary_integer;
type tbl_array_u is table of fnd_rtl_allocation%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- To improve performance could filter for Foods only as C&H do not need WAC
-- 70% of file is however Foods so AVRG saving will be 30%
-- THE FACT THAT WE CHECK FOR LOCATION/ITEM WAC 'IS NOT NULL' FILTERS OUT C&H ANYWAY
-- C&H DO NOT HAVE WAC ON LOCATION/ITEM MASTER AS THE DO NOT NEED IT (QUOTE SERENA JUL 2009)


cursor c_fnd_rtl_loc_item_dy_rms_wac is

 SELECT  DL.LOCATION_NO,
         DI.ITEM_NO,
         ALLOC.RELEASE_DATE,
         MAX(LI.WAC) WAC
 FROM    FND_RTL_ALLOCATION  ALLOC,
         DIM_ITEM DI,
         DIM_LOCATION DL,
         RTL_LOCATION_ITEM LI
 WHERE       LI.SK1_ITEM_NO          = DI.SK1_ITEM_NO
         AND LI.SK1_LOCATION_NO      = DL.SK1_LOCATION_NO
         AND DL.LOCATION_NO          = ALLOC.TO_LOC_NO
         AND DI.ITEM_NO              = ALLOC.ITEM_NO
         AND ALLOC.LAST_UPDATED_DATE = G_DATE
         AND ALLOC.WAC IS NULL
         AND LI.WAC IS NOT NULL
         --            AND (PO_NO >= 3000000 OR PO_NO IS NULL) -- INTERIM MEASURE TO PREVENT OLD PO'S BEING OVERWRITTEN WITH RE-CYLCLED PO'S
 GROUP BY DL.LOCATION_NO,
          DI.ITEM_NO,
          ALLOC.RELEASE_DATE        ;
/*
 SELECT  WAC.LOCATION_NO,
         WAC.ITEM_NO,
         ALLOC.RELEASE_DATE,
         MAX(WAC.WAC) WAC
 FROM    FND_RTL_ALLOCATION  ALLOC,
         FND_RTL_LOC_ITEM_DY_RMS_WAC WAC
 WHERE   WAC.LOCATION_NO             = ALLOC.TO_LOC_NO
         AND WAC.ITEM_NO             = ALLOC.ITEM_NO
         AND WAC.TRAN_DATE           = ALLOC.RELEASE_DATE
         AND ALLOC.LAST_UPDATED_DATE = G_DATE
         AND ALLOC.WAC IS NULL
 GROUP BY WAC.LOCATION_NO,
          WAC.ITEM_NO,
          ALLOC.RELEASE_DATE        ;
*/

-- Input record declared as cursor%rowtype
g_rec_in             c_fnd_rtl_loc_item_dy_rms_wac%rowtype;

-- Input bulk collect table declared
type stg_array is table of c_fnd_rtl_loc_item_dy_rms_wac%rowtype;
a_stg_input      stg_array;


--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.to_loc_no                     := g_rec_in.location_no;
   g_rec_out.item_no                       := g_rec_in.item_no;
   g_rec_out.release_date                  := g_rec_in.release_date;
   g_rec_out.wac                           := g_rec_in.wac;
   g_rec_out.last_updated_date             := g_date;

   exception
      when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end local_address_variable;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

   forall i in a_tbl_update.first .. a_tbl_update.last
      save exceptions
      update fnd_rtl_allocation
      set wac                           = a_tbl_update(i).wac,
          last_updated_date             = a_tbl_update(i).last_updated_date
      where to_loc_no      = a_tbl_update(i).to_loc_no
      and   item_no        = a_tbl_update(i).item_no
      and   release_date   = a_tbl_update(i).release_date;

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
                       ' '||a_tbl_update(g_error_index).to_loc_no ||
                       ' '||a_tbl_update(g_error_index).item_no||
                       ' '||a_tbl_update(g_error_index).release_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

   g_found := TRUE;

-- Place record into array for later bulk writing

   a_count_u               := a_count_u + 1;
   a_tbl_update(a_count_u) := g_rec_out;

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
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

    l_text := 'LOAD OF fnd_rtl_allocation EX FND_RTL_LOC_ITEM_DY_RMS_WAC STARTED AT '||
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
    open c_fnd_rtl_loc_item_dy_rms_wac;
    fetch c_fnd_rtl_loc_item_dy_rms_wac bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_loc_item_dy_rms_wac bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_dy_rms_wac;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************


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


END WH_FND_CORP_621U;
