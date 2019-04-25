--------------------------------------------------------
--  DDL for Procedure WH_FND_WFS_022U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_WFS_FOUNDATION"."WH_FND_WFS_022U" (p_forall_limit in integer,p_success out boolean) AS 


--**************************************************************************************************
--  Date:        MARCH 2013
--  Author:      Alastair de Wet
--  Purpose:     Create Action dimention table in the foundation layer
--               with input ex staging table from Vision.
--  Tables:      Input  - stg_vsn_action_cpy
--               Output - fnd_wfs_action
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  08 Sept 2010 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   
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
g_hospital_text      stg_vsn_action_hsp.sys_process_msg%type;
g_rec_out            fnd_wfs_action%rowtype;
g_rec_in             stg_vsn_action_cpy%rowtype;
g_found              boolean;
g_count              number        :=  0;

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
L_MODULE_NAME        SYS_DWH_ERRLOG.LOG_PROCEDURE_NAME%TYPE    := 'WH_FND_WFS_022U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_wfs_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_wfs_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_wfs_fnd;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE ACTION DIM EX VISION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_vsn_action_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_wfs_action%rowtype index by binary_integer;
type tbl_array_u is table of fnd_wfs_action%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_vsn_action_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_vsn_action_cpy.sys_source_sequence_no%type 
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_vsn_action is
   select *
   from stg_vsn_action_cpy
   where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data
   
--************************************************************************************************** 
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';
   g_rec_out.action_code                     := g_rec_in.action_code;
   g_rec_out.action_descr                    := g_rec_in.action_descr;
   g_rec_out.action_group                    := g_rec_in.action_group;
   g_rec_out.action_group_desc               := g_rec_in.action_group_desc;
   g_rec_out.action_module                   := g_rec_in.action_module;
   g_rec_out.action_type                     := g_rec_in.action_type;
   g_rec_out.assoc_tran_code                 := g_rec_in.assoc_tran_code;
   g_rec_out.assoc_tran_page                 := g_rec_in.assoc_tran_page;
   g_rec_out.assoc_logicmodule               := g_rec_in.assoc_logicmodule;
   g_rec_out.dummy_ind                       := 0;
   
   g_rec_out.last_updated_date               := g_date;

if g_rec_out.action_code in ('PLAO','IDOL','BANK','PLBD','CDOA','CDDD','PLDO','PLFI') then g_rec_out.action_group_desc := 'LOAN DEBIT ORDERS'     ; end if;
if g_rec_out.action_code in ('WCAP','GAUT','ECAP','KWZN','CUST','STOR')               then g_rec_out.action_group_desc := 'LOAN INCOMING CALLS'   ; end if;
if g_rec_out.action_code in ('CAMP','UCAL','UCUS','PLUC','LACC','PLIL')               then g_rec_out.action_group_desc := 'LOAN CAMPAIGN'         ; end if;
if g_rec_out.action_code in ('IDDS','CDDS','DD12','DD24','DD36','RV12','RV24','RV36',
    'CR12','CR24','CR36','DR12','DR24','DR36','TT12','TT24', 'TT36','ADJC')           then g_rec_out.action_group_desc := 'LOAN DRAWDOWNS'        ; end if;
if g_rec_out.action_code  = 'PLTC'                                                    then g_rec_out.action_group_desc := 'LOAN OUTBOUND CALLS'   ; end if;
if g_rec_out.action_code in ('ILPR','APLR','CLPR','RLPR','MALP','MRLP','LPRC','LPCL',
                   'PLDC')                                                            then g_rec_out.action_group_desc  := 'LOAN LBP'             ; end if;
if g_rec_out.action_code  = 'PLRM'                                                    then g_rec_out.action_group_desc  := 'LOAN RETURN MAIL'     ; end if;
if g_rec_out.action_code  = 'PLLD'                                                    then g_rec_out.action_group_desc  := 'LOAN LIMIT DECREASE'  ; end if;
if g_rec_out.action_code  = 'PLLI'                                                    then g_rec_out.action_group_desc  := 'LOAN LIMIT INCREASE'  ; end if;
if g_rec_out.action_code in ('PLDR','PLFR')                                           then g_rec_out.action_group_desc  := 'LOAN FRAUD&DISPUTES'  ; end if;
if g_rec_out.action_code  = 'PLAC'                                                    then g_rec_out.action_group_desc  := 'LOAN CLOSED'          ; end if;
if g_rec_out.action_code  = 'PLMT'                                                    then g_rec_out.action_group_desc  := 'LOAN PAYMENT'         ; end if;
if g_rec_out.action_code in ('PLRA','PLUP')                                           then g_rec_out.action_group_desc  := 'LOAN REPORT'          ; end if;
if g_rec_out.action_code in ('LFAX','LAPP')                                           then g_rec_out.action_group_desc  := 'LOAN INCOMING MAIL'   ; end if;
if g_rec_out.action_code in ('PLNQ','PLRR')                                           then g_rec_out.action_group_desc  := 'LOAN OTHER'           ; end if;
if g_rec_out.action_code in ('IPLN','IWWN','IPLB','IPLL','IPLI','IPLO','IPLT','IPLS',
                   'ILST','ILRP','ILIN','ILSA','ILTC')                                then g_rec_out.action_group_desc  := 'LOAN ENQUIRY'         ; end if; 
if g_rec_out.action_code  = 'PLAO'                                                    then g_rec_out.action_group_desc  := 'LOAN ACCOUNTS OPENED' ; end if;
if g_rec_out.action_code in ('CCCL','CCLT','CDTR','COIN','CSSD','SCOM','NRST','COMP',
                   'ITCC')                                                            then g_rec_out.action_group_desc  := 'COMPLAINTS'           ; end if;
if g_rec_out.action_code in ('CLSE','CLEM','CLCA','CLFI','CLNR','CLHE','CLCO','CLPS',
           'CLCC','CLSY','CLDU','CLIN','CLFR','DIED','CLCL','CLTR', 'CSDS')           then g_rec_out.action_group_desc  := 'ACCOUNT CLOSURES'     ; end if;
if g_rec_out.action_code in ('STRU','CTRU')                                           then g_rec_out.action_group_desc  := 'TRUWORTHS SWOP'       ; end if;
if g_rec_out.action_code in ('PTEL','BURI','BDWO','LITC','CITC','PITC','COBB','CUST') then g_rec_out.action_group_desc  := 'LEGAL MONTHLY'        ; end if;
if g_rec_out.action_code in ('CIUP','RADV','CORR','INFO','ICRD','ASHI')               then g_rec_out.action_group_desc  := 'UPDATES'              ; end if;


   exception
      when others then
       l_message := dwh_cust_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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
   
   insert into stg_vsn_action_hsp values g_rec_in;
   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := dwh_cust_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
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
       insert into fnd_wfs_action values a_tbl_insert(i);
       
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
                       ' '||a_tbl_insert(g_error_index).action_code;
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
       update fnd_wfs_action
       set    action_descr                    = a_tbl_update(i).action_descr,
              action_group                    = a_tbl_update(i).action_group,
              action_group_desc               = a_tbl_update(i).action_group_desc,
              action_module                   = a_tbl_update(i).action_module,
              action_type                     = a_tbl_update(i).action_type,
              assoc_tran_code                 = a_tbl_update(i).assoc_tran_code,
              assoc_tran_page                 = a_tbl_update(i).assoc_tran_page,
              assoc_logicmodule               = a_tbl_update(i).assoc_logicmodule,
              dummy_ind                       = a_tbl_update(i).dummy_ind ,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  action_code                     = a_tbl_update(i).action_code;
       
       g_recs_updated  := g_recs_updated  + a_tbl_update.count;
               
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
                       ' '||a_tbl_update(g_error_index).action_code;
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
       update stg_vsn_action_cpy      
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);
             
   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
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
   
-- Check to see if present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   fnd_wfs_action
   where  action_code = g_rec_out.action_code;
      
   if g_count = 1 then
      g_found := TRUE;
   end if; 
-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).action_code  = g_rec_out.action_code  then
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
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := dwh_cust_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
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
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF fnd_wfs_action EX CUST CENTRAL STARTED AT '||
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
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_vsn_action;
    fetch c_stg_vsn_action bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_cust_constants.vc_log_records_processed||
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
    fetch c_stg_vsn_action bulk collect into a_stg_input limit g_forall_limit;     
    end loop;
    close c_stg_vsn_action;
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
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital); 
    
    l_text :=  dwh_cust_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_run_completed ||sysdate;
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

END WH_FND_WFS_022U;
