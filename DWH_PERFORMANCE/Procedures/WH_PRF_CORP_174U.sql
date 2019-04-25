--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_174U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_174U" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        Feb 2009
--  Author:      Alastair de Wet
--  Purpose:     Create location item day price fact table in the performance layer with
--               added value ex foundation layer location item day rms wac table.
--  Tables:      Input  -   fnd_rtl_loc_item_dy_rms_wac and price
--               Output -   rtl_loc_item_dy_rms_price
--  Packages:    constants, dwh_log, dwh_valid
--  Comments:    Single DML could be considered for this program.
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  16 Mar 2009 - Changed driving cursor to improve performance - Tien Cheng
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
g_rec_out            rtl_loc_item_dy_rms_price%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_start_date         date          := trunc(sysdate) - 60;
g_count              number        :=  0;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_174U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE RTL_LOC_ITEM_DY_RMS_PRICE EX FND_RTL_LOC_ITEM_DY_RMS_WAC';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_rms_price%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_rms_price%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_item_dy_rms_wac is
 SELECT  PRC.SK1_LOCATION_NO,
         PRC.SK1_ITEM_NO,
         PRC.CALENDAR_DATE,
         LI.WAC,
         NVL (PRC.NUM_UNITS_PER_TRAY, 1) AS NUM_UNITS_PER_TRAY
 FROM    RTL_LOC_ITEM_DY_RMS_PRICE PRC,
         RTL_LOCATION_ITEM  LI
 WHERE        LI.SK1_LOCATION_NO    = PRC.SK1_LOCATION_NO
         AND  LI.SK1_ITEM_NO        = PRC.SK1_ITEM_NO
         AND prc.last_updated_date  = g_date ;

/*
cursor c_fnd_rtl_loc_item_dy_rms_wac is
 SELECT  PRC.SK1_LOCATION_NO,
         PRC.SK1_ITEM_NO,
         PRC.CALENDAR_DATE,
         WAC.WAC,
         NVL (PRC.NUM_UNITS_PER_TRAY, 1) AS NUM_UNITS_PER_TRAY
 FROM    RTL_LOC_ITEM_DY_RMS_PRICE PRC,
         FND_RTL_LOC_ITEM_DY_RMS_WAC WAC,
         DIM_LOCATION DL,
         DIM_ITEM DI
 WHERE   PRC.SK1_LOCATION_NO        = DL.SK1_LOCATION_NO
         AND PRC.SK1_ITEM_NO        = DI.SK1_ITEM_NO
         AND WAC.LOCATION_NO        = DL.LOCATION_NO
         AND WAC.ITEM_NO            = DI.ITEM_NO
         AND WAC.TRAN_DATE          = PRC.CALENDAR_DATE
         AND prc.last_updated_date  = g_date ;
*/

/*
cursor c_fnd_rtl_loc_item_dy_rms_wac is
with Temp1 as
(SELECT  PRC.SK1_LOCATION_NO,
         PRC.SK1_ITEM_NO,
         WAC.LOCATION_NO,
         WAC.ITEM_NO,
         PRC.CALENDAR_DATE,
         NVL (PRC.NUM_UNITS_PER_TRAY, 1) AS NUM_UNITS_PER_TRAY,
         MAX (WAC.TRAN_DATE) as MAX_DATE
  FROM   RTL_LOC_ITEM_DY_RMS_PRICE PRC,
         FND_RTL_LOC_ITEM_DY_RMS_WAC WAC,
         DIM_LOCATION DL,
         DIM_ITEM DI
 WHERE       PRC.SK1_LOCATION_NO    = DL.SK1_LOCATION_NO
         AND PRC.SK1_ITEM_NO        = DI.SK1_ITEM_NO
         AND WAC.LOCATION_NO        = DL.LOCATION_NO
         AND WAC.ITEM_NO            = DI.ITEM_NO
--         AND WAC.TRAN_DATE between G_START_DATE AND PRC.CALENDAR_DATE
         AND WAC.TRAN_DATE <= PRC.CALENDAR_DATE
         AND prc.last_updated_date  = g_date

 GROUP BY PRC.SK1_LOCATION_NO, PRC.SK1_ITEM_NO,WAC.LOCATION_NO,WAC.ITEM_NO, PRC.CALENDAR_DATE, NUM_UNITS_PER_TRAY
 )
 SELECT T.SK1_LOCATION_NO, T.SK1_ITEM_NO, T.CALENDAR_DATE, WAC.WAC, T.NUM_UNITS_PER_TRAY
 FROM FND_RTL_LOC_ITEM_DY_RMS_WAC WAC,
      Temp1 T
 WHERE WAC.LOCATION_NO = T.LOCATION_NO
   AND WAC.ITEM_NO     = T.ITEM_NO
   AND WAC.TRAN_DATE   = T.MAX_DATE;

*/


--below more efficient (L.Hamel but cannot work in sub query as oredr by not allowed.
--Would need to be transfered to address variables which reduces efficiency

--        ( select wac_sub.tran_date
--         from   fnd_rtl_loc_item_dy_rms_wac wac_sub
--         where  wac_sub.location_no = dl.location_no and
--                wac_sub.item_no     = di.item_no and
--                wac_sub.tran_date   <= prc.calendar_date and
--                rownum               = 1
--                order by wac_sub.tran_date desc
--         );





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

   g_rec_out.sk1_location_no               := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no                   := g_rec_in.sk1_item_no;
   g_rec_out.calendar_date                 := g_rec_in.calendar_date;
   g_rec_out.wac                           := g_rec_in.wac;
   g_rec_out.last_updated_date             := g_date;
   g_rec_out.case_cost                     := g_rec_in.num_units_per_tray * g_rec_out.wac;

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
      insert into rtl_loc_item_dy_rms_price values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
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
      update rtl_loc_item_dy_rms_price
      set wac                           = a_tbl_update(i).wac,
          case_cost                     = a_tbl_update(i).case_cost,
          last_updated_date             = a_tbl_update(i).last_updated_date
      where sk1_location_no = a_tbl_update(i).sk1_location_no
      and   sk1_item_no     = a_tbl_update(i).sk1_item_no
      and   calendar_date   = a_tbl_update(i).calendar_date
     ;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).calendar_date;
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

    l_text := 'LOAD OF RTL_LOC_ITEM_DY_RMS_PRICE EX FND_RTL_LOC_ITEM_DY_RMS_WAC STARTED AT '||
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
end wh_prf_corp_174u;
