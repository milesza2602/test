--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_112U_MISSPO
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_112U_MISSPO" 
               (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- QC4595 - FIX MISSING PO'S
-- Wendy  - August 2012
--292301
--292401
--292402
--292403
--293203
--293204
--293205
--293307
--293308
--293309
--293906
--293916
--293919
--293920
--294001
--**************************************************************************************************
--  Date:        August 2008
--  Author:      Sean Le Roux
--  Purpose:     Update Promotion Location unit dimension table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      AIT load - stg_rms_prom_location
--               Input    - stg_rms_prom_location_cpy
--               Output   - DWH_FOUNDATION.TEMP_fnd_prom_location
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 August 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_hospital_text      stg_rms_prom_location_hsp.sys_process_msg%type;
g_rec_out            DWH_FOUNDATION.TEMP_fnd_prom_location%rowtype;

g_found              boolean;
g_valid              boolean;
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_112U_MISSPO';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE PROMOTION LOCATION MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_FOUNDATION.TEMP_fnd_prom_location%rowtype index by binary_integer;
type tbl_array_u is table of DWH_FOUNDATION.TEMP_fnd_prom_location%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_rms_prom_location_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_rms_prom_location_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;

cursor c_stg_rms_prom_location is
WITH SELEXT AS
(SELECT /*+ FULL(A) */
PROM_NO, LOCATION_NO, MAX(SYS_SOURCE_BATCH_ID) MAXBATCH
FROM dwh_foundation.TEMP_STG_RMS_PROM_LOCATION A
GROUP BY PROM_NO, LOCATION_NO)
SELECT /*+ FULL(Y) */ SYS_SOURCE_BATCH_ID
,SYS_SOURCE_SEQUENCE_NO
,SYS_LOAD_DATE
,SYS_PROCESS_CODE
,SYS_LOAD_SYSTEM_NAME
,SYS_MIDDLEWARE_BATCH_ID
,SYS_PROCESS_MSG
,Y.PROM_NO
,Y.LOCATION_NO
,PROM_STORE_START_DATE
,PROM_STORE_END_DATE
,PROM_EXTRACT_STATUS_CODE
,PROM_STORE_COMMENT
,Y.SOURCE_DATA_STATUS_CODE
,M.PROM_NO VALID_PROM_NO
,K.LOCATION_NO VALID_LOCATION_NO
--,PROM_STORE_START_DATE as LAST_UPDATED_DATE
--,PROM_STORE_START_DATE-20 LAST_UPDATED_DATE
FROM FND_PROM M, FND_LOCATION K, SELEXT X, dwh_foundation.TEMP_STG_RMS_PROM_LOCATION Y
WHERE X.PROM_NO = M.PROM_NO(+)
AND X.LOCATION_NO = K.LOCATION_NO(+)
AND X.PROM_NO = Y.PROM_NO(+)
AND X.LOCATION_NO = Y.LOCATION_NO(+)
AND X.MAXBATCH = Y.SYS_SOURCE_BATCH_ID(+)
   order by sys_source_batch_id,sys_source_sequence_no;


-- For input bulk collect --
type stg_array is table of c_stg_rms_prom_location%rowtype;
a_stg_input      stg_array;

g_rec_in             c_stg_rms_prom_location%rowtype;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
   g_hospital                            := 'N';
   g_rec_out.last_updated_date           := g_rec_in.prom_store_start_date - 20;
   g_rec_out.prom_no                     := g_rec_in.prom_no;
   g_rec_out.location_no                 := g_rec_in.location_no;
   g_rec_out.prom_store_start_date       := g_rec_in.prom_store_start_date;
   g_rec_out.prom_store_end_date         := g_rec_in.prom_store_end_date;
   g_rec_out.prom_extract_status_code    := g_rec_in.prom_extract_status_code;
   g_rec_out.prom_store_comment          := g_rec_in.prom_store_comment;
   g_rec_out.source_data_status_code     := g_rec_in.source_data_status_code;

   if g_rec_IN.VALID_prom_no IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_prom_not_found ;
     return;
   end if;

   if g_rec_IN.VALID_location_no IS NULL then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found ;
     return;
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

   insert into stg_rms_prom_location_hsp values (g_rec_in.SYS_SOURCE_BATCH_ID
,g_rec_in.SYS_SOURCE_SEQUENCE_NO
,g_rec_in.SYS_LOAD_DATE
,g_rec_in.SYS_PROCESS_CODE
,g_rec_in.SYS_LOAD_SYSTEM_NAME
,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
,g_rec_in.SYS_PROCESS_MSG
,g_rec_in.PROM_NO
,g_rec_in.LOCATION_NO
,g_rec_in.PROM_STORE_START_DATE
,g_rec_in.PROM_STORE_END_DATE
,g_rec_in.PROM_EXTRACT_STATUS_CODE
,g_rec_in.PROM_STORE_COMMENT
,g_rec_in.SOURCE_DATA_STATUS_CODE
);
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
       insert into DWH_FOUNDATION.TEMP_fnd_prom_location values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).prom_no||
                       ' '||a_tbl_insert(g_error_index).location_no;
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
       update DWH_FOUNDATION.TEMP_fnd_prom_location
       set   prom_store_start_date           = a_tbl_update(i).prom_store_start_date,
             prom_store_end_date             = a_tbl_update(i).prom_store_end_date,
             prom_extract_status_code        = a_tbl_update(i).prom_extract_status_code,
             prom_store_comment              = a_tbl_update(i).prom_store_comment,
             source_data_status_code         = a_tbl_update(i).source_data_status_code,
             last_updated_date	             = a_tbl_update(i).last_updated_date
       where  prom_no                        = a_tbl_update(i).prom_no
         and  location_no                    = a_tbl_update(i).location_no;

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
                       ' '||a_tbl_update(g_error_index).prom_no||
                       ' '||a_tbl_update(g_error_index).location_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
v_count integer := 0;
begin

   g_found := false;

   select count(1)
   into   v_count
   from   DWH_FOUNDATION.TEMP_fnd_prom_location
   where  prom_no = g_rec_out.prom_no
     and  location_no = g_rec_out.location_no;

   if v_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).prom_no = g_rec_out.prom_no
           and a_tbl_insert(i).location_no = g_rec_out.location_no then
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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF DWH_FOUNDATION.TEMP_fnd_prom_location EX RMS STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    execute immediate('truncate table DWH_FOUNDATION.TEMP_fnd_prom_location');
    l_text := 'truncate table DWH_FOUNDATION.TEMP_fnd_prom_location';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_rms_prom_location;
    fetch c_stg_rms_prom_location bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_stg_rms_prom_location bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_rms_prom_location;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;

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


END WH_FND_CORP_112U_MISSPO;
