--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_180U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_180U" 
(p_forall_limit in integer,p_success out boolean) as


--**************************************************************************************************
--  Date:        August 2013
--  Author:      Wendy Lyttle
--  Purpose:     Create new stock cycle interface from MIC table in the foundation layer
--               with input ex staging table from MIC
--  Tables:      Input  - STG_4F_LOC_BU_STOCK_CYCLE_cpy
--               Output - FND_4F_LOC_BU_STOCK_CYCLE
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
--  29 Apr 2016  BarryK
--               add code to remove existing/latest verion of current load cycle prior to interface ref: BK29Apr16

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
g_recs_removed       integer       :=  0;             -- BK29Apr16
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      STG_4F_LOC_BU_STOCK_CYCLE_hsp.sys_process_msg%type;
g_rec_out            FND_4F_LOC_BU_STOCK_CYCLE%rowtype;
g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_180U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD BU STOCK CYCLE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of FND_4F_LOC_BU_STOCK_CYCLE%rowtype index by binary_integer;
type tbl_array_u is table of FND_4F_LOC_BU_STOCK_CYCLE%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of STG_4F_LOC_BU_STOCK_CYCLE.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of STG_4F_LOC_BU_STOCK_CYCLE.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_STG_4F_LOC_BU_STOCK_CYCLE is
   select a.*, b.calendar_date
   from STG_4F_LOC_BU_STOCK_CYCLE_cpy a, dim_calendar b
   where b.calendar_date between a.from_date and a.to_date
--  and cycle_no in(24,25)
   order by sys_source_batch_id,sys_source_sequence_no;

g_rec_in             c_STG_4F_LOC_BU_STOCK_CYCLE%rowtype;
-- For input bulk collect --
type stg_array is table of c_STG_4F_LOC_BU_STOCK_CYCLE%rowtype;
a_stg_input      stg_array;
-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';

   g_rec_out.location_no                     := g_rec_in.location_no;
   g_rec_out.business_unit_no                := g_rec_in.business_unit_no;
   g_rec_out.cycle_from_date                 := g_rec_in.from_date;
   g_rec_out.cycle_no                        := g_rec_in.cycle_no;
   g_rec_out.cycle_to_date                   := g_rec_in.to_date;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.calendar_date                   := g_rec_in.calendar_date;



   if not dwh_valid.fnd_location(g_rec_out.location_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_location_not_found;
     l_text := dwh_constants.vc_location_not_found||' '||g_rec_out.location_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not  dwh_valid.fnd_business_unit(g_rec_out.business_unit_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_bu_not_found;
     l_text := dwh_constants.vc_bu_not_found||' '||g_rec_out.business_unit_no;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;


   if not dwh_valid.fnd_calendar(g_rec_out.cycle_from_date) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_date_not_found;
     l_text := dwh_constants.vc_date_not_found||' '||g_rec_out.cycle_from_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   end if;

   if not dwh_valid.fnd_calendar(g_rec_out.cycle_to_date) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_constants.vc_date_not_found;
     l_text := dwh_constants.vc_date_not_found||' '||g_rec_out.cycle_to_date;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
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

   insert into dwh_foundation.STG_4F_LOC_BU_STOCK_CYCLE_hsp values
   (g_rec_in.SYS_SOURCE_BATCH_ID
    ,g_rec_in.SYS_SOURCE_SEQUENCE_NO
    ,g_rec_in.SYS_LOAD_DATE
    ,g_rec_in.SYS_PROCESS_CODE
    ,g_rec_in.SYS_LOAD_SYSTEM_NAME
    ,g_rec_in.SYS_MIDDLEWARE_BATCH_ID
    ,g_rec_in.SYS_PROCESS_MSG
    ,g_rec_in.LOCATION_NO
    ,g_rec_in.BUSINESS_UNIT_NO
    ,g_rec_in.CYCLE_NO
    ,g_rec_in.FROM_DATE
    ,g_rec_in.TO_DATE);
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
       insert into FND_4F_LOC_BU_STOCK_CYCLE values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).location_no||
                       ' '||a_tbl_insert(g_error_index).business_unit_no||
                       ' '||a_tbl_insert(g_error_index).cycle_from_date||
                       ' '||a_tbl_insert(g_error_index).cycle_to_date;
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
       update FND_4F_LOC_BU_STOCK_CYCLE
       set    cycle_from_date          = a_tbl_update(i).cycle_from_date,
              cycle_to_date            = a_tbl_update(i).cycle_to_date,
              last_updated_date              = a_tbl_update(i).last_updated_date
       where  location_no                    = a_tbl_update(i).location_no and
              business_unit_no               = a_tbl_update(i).business_unit_no     and
              cycle_no                       = a_tbl_update(i).cycle_no and
                            calendar_date            = a_tbl_update(i).calendar_date;

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
                       ' '||a_tbl_update(g_error_index).location_no ||
                       ' '||a_tbl_update(g_error_index).business_unit_no  ||
                       ' '||a_tbl_update(g_error_index).cycle_from_date||
                       ' '||a_tbl_update(g_error_index).cycle_to_date ;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update STG_4F_LOC_BU_STOCK_CYCLE_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_staging_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin

   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   FND_4F_LOC_BU_STOCK_CYCLE
   where  location_no      = g_rec_out.location_no  and
          business_unit_no = g_rec_out.business_unit_no      and
          cycle_no         = g_rec_out.cycle_no  and
          calendar_date         = g_rec_out.calendar_date ;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).location_no = g_rec_out.location_no and
            a_tbl_insert(i).business_unit_no = g_rec_out.business_unit_no     and
             a_tbl_insert(i).calendar_date = g_rec_out.calendar_date     and
            a_tbl_insert(i).cycle_no   = g_rec_out.cycle_no then
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
      local_bulk_staging_update;

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
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum  then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_4F_LOC_BU_STOCK_CYCLE  STARTED AT '||
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
    
    -- remove current cycle version - only last loaded cycle will remain ...                  -- BK29Apr16
    delete /*+ PARALLEL(a,4) */ from dwh_foundation.FND_4F_LOC_BU_STOCK_CYCLE a               -- BK29Apr16
    where  CYCLE_NO = (select max(CYCLE_NO)                                                   -- BK29Apr16
                       from   STG_4F_LOC_BU_STOCK_CYCLE_cpy);                                 -- BK29Apr16
    g_recs_removed := sql%rowcount;                                                           -- BK29Apr16
    commit;                                                                                   -- BK29Apr16
    
    l_text := 'Existing Cycle rows removed:- '||g_date;                                       -- BK29Apr16          
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);            -- BK29Apr16

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_STG_4F_LOC_BU_STOCK_CYCLE;
    fetch c_STG_4F_LOC_BU_STOCK_CYCLE bulk collect into a_stg_input limit g_forall_limit;
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
    fetch c_STG_4F_LOC_BU_STOCK_CYCLE bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_STG_4F_LOC_BU_STOCK_CYCLE;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'Current Cycle records removed   '||g_recs_removed;                      -- BK29Apr16
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);      -- BK29Apr16
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

END WH_FND_CORP_180U;
