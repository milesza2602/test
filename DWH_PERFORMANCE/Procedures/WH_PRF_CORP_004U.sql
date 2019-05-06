--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_004U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_004U" (p_success out boolean) as
--**************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Generate the time dimention in performance layer by second
--               for every second in a 24 hour day.
--  Tables:      Input  - None
--               Output - dim_time
--  Packages:    constants, dwh_log,
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
--  c_  -  Prefix to cursor followed by table name
--**************************************************************************************************
g_recs_read         integer       :=  0;
g_recs_updated      integer       :=  0;
g_recs_inserted     integer       :=  0;
g_half_hour         integer       :=  0;
g_quarter_hour      integer       :=  0;
g_count             integer       :=  0;
g_hh                integer       :=  0;
g_mm                integer       :=  0;
g_ss                integer       :=  0;
g_calc              integer       :=  0;
g_hhmmss            varchar(20) ;
g_rec_out           dim_time%rowtype;
g_insert_rec        boolean;
g_date              date          :=  trunc(sysdate);
g_second            integer       :=  0;
l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_004U';
l_name              sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name    sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'GENERATE THE DIM_TIME TABLE BY SECONDS AFTER MIDNIGHT';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Process data
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.tran_time_second             := g_second;
   g_rec_out.hour_no                      := floor((g_second / 3600) + 1);
   g_rec_out.incremental_half_hour_no     := floor((g_second / 1800) + 1);
   g_rec_out.incremental_quarter_hour_no  := floor((g_second / 900)  + 1);

   g_half_hour := g_rec_out.incremental_half_hour_no -
           (floor(g_rec_out.incremental_half_hour_no / 2) * 2);
   if g_half_hour  = 0 then
      g_half_hour  := 2;
   end if;
   g_rec_out.half_hour_no                 := g_half_hour;

   g_quarter_hour := g_rec_out.incremental_quarter_hour_no -
           (floor(g_rec_out.incremental_quarter_hour_no / 2) * 2);
   if g_quarter_hour  = 0 then
      g_quarter_hour  := 2;
   end if;
   if g_half_hour  = 2 then
      g_quarter_hour  :=   g_quarter_hour + 2;
   end if;
   g_rec_out.quarter_hour_no              := g_quarter_hour;

   g_calc                                 := g_second;
   g_hh                                   := floor(g_calc / 3600);
   g_calc                                 := g_calc - (g_hh * 3600);
   g_mm                                   := floor(g_calc / 60);
   g_ss                                   := g_calc - (g_mm * 60);
   g_rec_out.tran_time_desc                  := lpad(to_char(g_hh),2,'00')||':'||
                                             lpad(to_char(g_mm),2,'00')||':'||
                                             lpad(to_char(g_ss),2,'00');

   g_rec_out.last_updated_date            := g_date;

   --------------------------------------
   -- What follows is for OLAP value add.
   --------------------------------------

   g_rec_out.hour_code                    := 'HR'||g_rec_out.hour_no;
   g_rec_out.hour_short_desc              := 'HR '||g_rec_out.hour_no;
   g_rec_out.hour_long_desc               := 'HOUR NO '||g_rec_out.hour_no;
   g_rec_out.half_hour_code               := g_rec_out.hour_code||'HH'||g_rec_out.half_hour_no;
   g_rec_out.half_hour_short_desc         := 'HLF HR '||g_rec_out.half_hour_no;
   g_rec_out.half_hour_long_desc          := 'HALF HOUR NO '||g_rec_out.half_hour_no;
   g_rec_out.quarter_hour_code            := g_rec_out.half_hour_code||'QH'||g_rec_out.quarter_hour_no;
   g_rec_out.quarter_hour_short_desc      := 'QTR HR '||g_rec_out.quarter_hour_no;
   g_rec_out.quarter_hour_long_desc       := 'QUARTER HOUR NO '||g_rec_out.quarter_hour_no;
   g_rec_out.order_by_seq_no              := 0;
   g_rec_out.total                        := 'TOTAL';
   g_rec_out.total_desc                   := 'ALL TIME OF DAY';

end local_address_variable;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_count := 0;
   select count(*)
   into   g_count
   from   dim_time
   where  tran_time_second = g_second;

   if g_count = 0 then
      g_insert_rec := TRUE;
   else
      g_insert_rec := FALSE;
   end if;

   if g_insert_rec then
      insert into dim_time values g_rec_out;
      g_recs_inserted         := g_recs_inserted + sql%rowcount;
   else
      update dim_time
      set    hour_no                     = g_rec_out.hour_no,
             half_hour_no                = g_rec_out.half_hour_no,
             quarter_hour_no             = g_rec_out.quarter_hour_no,
             incremental_half_hour_no    = g_rec_out.incremental_half_hour_no,
             incremental_quarter_hour_no = g_rec_out.incremental_quarter_hour_no,
             tran_time_desc              = g_rec_out.tran_time_desc,
             last_updated_date           = g_rec_out.last_updated_date
      where  tran_time_second            = g_rec_out.tran_time_second;
      g_recs_updated          := g_recs_updated + sql%rowcount;
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
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'GENERATE DIM_TIME TABLE STARTED AT '||
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
    for i in 0 .. 86399
    loop
       g_second    := i;
       g_recs_read := g_recs_read + 1;
       if g_recs_read mod 10000 = 0 then
          l_text   := dwh_constants.vc_log_records_processed||
          to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          commit;
       end if;

       local_address_variable;
       local_write_output;

    end loop;


--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');

    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

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
       p_success := false ;
       raise;


end wh_prf_corp_004u;
