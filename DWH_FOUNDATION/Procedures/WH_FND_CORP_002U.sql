--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_002U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_002U" (p_success out boolean) AS
--**************************************************************************************************
--  Date:        April 2008
--  Author:      Alastair de Wet
--  Purpose:     Generate the time dimention in the foundation layer by second
--               for every second in a 24 hour day.
--  Tables:      Input  - None
--               Output - fnd_time
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
g_rec_out           fnd_time%rowtype;
g_insert_rec        boolean;
--g_date              date          :=  to_char(sysdate,('dd mon yyyy'));
g_date              date          :=  trunc(sysdate);
g_second            integer       :=  0;

l_message           sys_dwh_errlog.log_text%type;
l_module_name       sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_002U';
l_name              sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name       sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name       sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name    sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text              sys_dwh_log.log_text%type ;
l_description       sys_dwh_log_summary.log_description%type  := 'GENERATE THE FND_TIME TABLE BY SECONDS AFTER MIDNIGHT';
l_process_type      sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



--**************************************************************************************************
-- Process data
--**************************************************************************************************
procedure local_address_variable as
begin
    --dbms_output.put_line('9');
   g_rec_out.tran_time                    := g_second;
   g_rec_out.hour_no                      := floor((g_second / 3600) + 1);
   g_rec_out.incremental_half_hour_no     := floor((g_second / 1800) + 1);
   g_rec_out.incremental_quarter_hour_no  := floor((g_second / 900)  + 1);

   g_half_hour := g_rec_out.incremental_half_hour_no -
           (floor(g_rec_out.incremental_half_hour_no / 2) * 2);
   if g_half_hour  = 0 then
      g_half_hour  := 2;
   end if;
   g_rec_out.half_hour_no                 := g_half_hour;
    --dbms_output.put_line('10');
   g_quarter_hour := g_rec_out.incremental_quarter_hour_no -
           (floor(g_rec_out.incremental_quarter_hour_no / 2) * 2);
   if g_quarter_hour  = 0 then
      g_quarter_hour  := 2;
   end if;
   if g_half_hour  = 2 then
      g_quarter_hour  :=   g_quarter_hour + 2;
   end if;
   g_rec_out.quarter_hour_no              := g_quarter_hour;
    --dbms_output.put_line('11');
   g_calc                                 := g_second;
   g_hh                                   := floor(g_calc / 3600);
   g_calc                                 := g_calc - (g_hh * 3600);
   g_mm                                   := floor(g_calc / 60);
   g_ss                                   := g_calc - (g_mm * 60);
   g_rec_out.tran_time_desc               := lpad(to_char(g_hh),2,'00')||':'||
                                             lpad(to_char(g_mm),2,'00')||':'||
                                             lpad(to_char(g_ss),2,'00');

   g_rec_out.last_updated_date            := g_date;

end local_address_variable;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_count := 0;
      --dbms_output.put_line('g_second='||g_second);
   select count(*)
   into   g_count
   from   fnd_time
   where  tran_time = g_second;
    --dbms_output.put_line('12');
   if g_count = 0 then
      g_insert_rec := TRUE;
   else
      g_insert_rec := FALSE;
   end if;
    --dbms_output.put_line('13');
   if g_insert_rec then
      insert into fnd_time values g_rec_out;
      g_recs_inserted         := g_recs_inserted + sql%rowcount;
   else
      update fnd_time
      set    hour_no                     = g_rec_out.hour_no,
             half_hour_no                = g_rec_out.half_hour_no,
             quarter_hour_no             = g_rec_out.quarter_hour_no,
             incremental_half_hour_no    = g_rec_out.incremental_half_hour_no,
             incremental_quarter_hour_no = g_rec_out.incremental_quarter_hour_no,
             tran_time_desc              = g_rec_out.tran_time_desc,
             last_updated_date           = g_rec_out.last_updated_date
      where  tran_time                   = g_rec_out.tran_time;
      g_recs_updated          := g_recs_updated + sql%rowcount;
   end if;
 --dbms_output.put_line('14');
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
 --dbms_output.put_line('1');
    l_text := 'GENERATE FND_TIME TABLE STARTED AT '||
     to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --dbms_output.put_line('2');
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
     --dbms_output.put_line('3');
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


--**************************************************************************************************
     --dbms_output.put_line('4');
    for i in 0 .. 86399
    loop
       g_second    := i;
       g_recs_read := g_recs_read + 1;
       if g_recs_read mod 10000 = 0 then
          l_text   :=  dwh_constants.vc_log_records_processed ||
          to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
          commit;
       end if;

       local_address_variable;
       local_write_output;

    end loop;
 --dbms_output.put_line('5');

--**************************************************************************************************

    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
   --dbms_output.put_line('6');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     --dbms_output.put_line('7');
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --dbms_output.put_line('8');
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
END WH_FND_CORP_002U;
