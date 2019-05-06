--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_456U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_456U" (p_forall_limit in integer,p_success out boolean) as 

--**************************************************************************************************
--  Date:        Feb 2018
--  Author:      Wendy Lyttle
--  Purpose:     Create trading_entity dimension table in the performance layer
--               with added value ex foundation layer merch hierachy table.
--  Tables:      Input  -   fnd_trading_entity
--               Output -   dim_trading_entity
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  23 April 2009 - defect 1352 - Change total_descr from plural to singular
--
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
g_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            dim_trading_entity%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_456U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE DIM_trading_entity EX FND_trading_entity';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DIM_TRADING_ENTITY%rowtype index by binary_integer;
type tbl_array_u is table of DIM_TRADING_ENTITY%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_fnd_trading_entity is
   select A.*, B.SK1_COUNTRY_CODE, C.SK1_CURRENCY_NO
   from fnd_trading_entity A, DIM_COUNTRY B, DIM_CURRENCY C
   WHERE A.COUNTRY_CODE = B.COUNTRY_CODE
   AND A. CURRENCY_CODE = C.CURRENCY_CODE;

g_rec_in                   c_FND_TRADING_ENTITY%rowtype;
-- For input bulk collect --
type stg_array is table of c_FND_TRADING_ENTITY%rowtype;
a_stg_input      stg_array;
--************************************************************************************************** 
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

        g_rec_out.TRADING_ENTITY_CODE     := g_rec_in.TRADING_ENTITY_CODE;
        g_rec_out.TRADING_ENTITY_NAME     := g_rec_in.TRADING_ENTITY_NAME;
        g_rec_out.LEGAL_ENTITY_IDENTIFIER := g_rec_in.LEGAL_ENTITY_IDENTIFIER;
        g_rec_out.COUNTRY_CODE            := g_rec_in.COUNTRY_CODE;
        g_rec_out.TRADING_REGISTRATION_NO := g_rec_in.TRADING_REGISTRATION_NO;
        g_rec_out.VAT_REGISTRATION_NO     := g_rec_in.VAT_REGISTRATION_NO;
        g_rec_out.PHYSICAL_ADDRESS_LINE_1 := g_rec_in.PHYSICAL_ADDRESS_LINE_1;
        g_rec_out.PHYSICAL_ADDRESS_LINE_2 := g_rec_in.PHYSICAL_ADDRESS_LINE_2;
        g_rec_out.PHYSICAL_CITY_NAME      := g_rec_in.PHYSICAL_CITY_NAME;
        g_rec_out.PHYSICAL_POSTAL_CODE    := g_rec_in.PHYSICAL_POSTAL_CODE;
        g_rec_out.POSTAL_ADDRESS_LINE_1   := g_rec_in.POSTAL_ADDRESS_LINE_1;
        g_rec_out.POSTAL_ADDRESS_LINE_2   := g_rec_in.POSTAL_ADDRESS_LINE_2;
        g_rec_out.POSTAL_CITY_NAME        := g_rec_in.POSTAL_CITY_NAME;
        g_rec_out.POSTAL_POSTAL_CODE      := g_rec_in.POSTAL_POSTAL_CODE;
        g_rec_out.EMAIL_ADDRESS           := g_rec_in.EMAIL_ADDRESS;
        g_rec_out.TELEPHONE_NO            := g_rec_in.TELEPHONE_NO;
        g_rec_out.FAX_NO                  := g_rec_in.FAX_NO;
        g_rec_out.CURRENCY_CODE           := g_rec_in.CURRENCY_CODE;
        g_rec_out.TAX_TYPE                := g_rec_in.TAX_TYPE;
        g_rec_out.last_updated_date       := g_date;
        g_rec_out.SK1_COUNTRY_CODE                := g_rec_in.SK1_COUNTRY_CODE;
        g_rec_out.SK1_CURRENCY_NO                := g_rec_in.SK1_CURRENCY_NO;
--------------------------------------------------------- 
-- Added for OLAP purposes                    
---------------------------------------------------------
 --  g_rec_out.trading_entity_long_name            := g_rec_in.trading_entity_code||' - '||g_rec_out.trading_entity_name;
--   g_rec_out.total                      := 'TOTAL';
 --  g_rec_out.total_desc                 := 'ALL trading_entity';

   
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
      insert into dim_trading_entity values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).trading_entity_code;
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
      UPDATE dim_trading_entity
      SET TRADING_ENTITY_CODE = a_tbl_update(i).TRADING_ENTITY_CODE,
         TRADING_ENTITY_NAME   = a_tbl_update(i).TRADING_ENTITY_NAME,
        LEGAL_ENTITY_IDENTIFIER = a_tbl_update(i).LEGAL_ENTITY_IDENTIFIER,
        COUNTRY_CODE           = a_tbl_update(i).COUNTRY_CODE,
        TRADING_REGISTRATION_NO = a_tbl_update(i).TRADING_REGISTRATION_NO,
        VAT_REGISTRATION_NO     = a_tbl_update(i).VAT_REGISTRATION_NO,
        PHYSICAL_ADDRESS_LINE_1 = a_tbl_update(i).PHYSICAL_ADDRESS_LINE_1,
        PHYSICAL_ADDRESS_LINE_2 = a_tbl_update(i).PHYSICAL_ADDRESS_LINE_2,
        PHYSICAL_CITY_NAME      = a_tbl_update(i).PHYSICAL_CITY_NAME,
        PHYSICAL_POSTAL_CODE    = a_tbl_update(i).PHYSICAL_POSTAL_CODE,
        POSTAL_ADDRESS_LINE_1   = a_tbl_update(i).POSTAL_ADDRESS_LINE_1,
        POSTAL_ADDRESS_LINE_2   = a_tbl_update(i).POSTAL_ADDRESS_LINE_2,
        POSTAL_CITY_NAME        = a_tbl_update(i).POSTAL_CITY_NAME,
        POSTAL_POSTAL_CODE      = a_tbl_update(i).POSTAL_POSTAL_CODE,
        EMAIL_ADDRESS           = a_tbl_update(i).EMAIL_ADDRESS,
        TELEPHONE_NO            = a_tbl_update(i).TELEPHONE_NO,
        FAX_NO                  = a_tbl_update(i).FAX_NO,
        CURRENCY_CODE           = a_tbl_update(i).CURRENCY_CODE,
        TAX_TYPE                = a_tbl_update(i).TAX_TYPE,
        LAST_UPDATED_DATE       = a_tbl_update(i).LAST_UPDATED_DATE,
        SK1_COUNTRY_CODE        = a_tbl_update(i).SK1_COUNTRY_CODE,
        SK1_CURRENCY_NO       = a_tbl_update(i).SK1_CURRENCY_NO
      WHERE SK1_TRADING_ENTITY_CODE = a_tbl_update(i).SK1_TRADING_ENTITY_CODE;

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
                       ' '||a_tbl_update(g_error_index).trading_entity_code;
          dwh_log.record_error(l_module_name,sqlcode,l_message);  
       end loop;   
       raise;        
end local_bulk_update;



--************************************************************************************************** 
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
 
begin

 g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly   
   select count(1)
   into   g_count
   from   DIM_trading_entity
   where  trading_entity_code    = g_rec_out.trading_entity_code ;
  
   if g_count = 1 then
      g_found := TRUE;
   end if;   

-- Place record into array for later bulk writing   
   if not g_found then
      g_rec_out.sk1_trading_entity_code  := location_hierachy_seq.nextval;
--      g_rec_out.sk_from_date  := g_date;
--      g_rec_out.sk_to_date    := dwh_constants.sk_to_date;
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
    
    l_text := 'LOAD OF DIM_trading_entity EX FND_trading_entity STARTED AT '||
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
    open c_fnd_trading_entity;
    fetch c_fnd_trading_entity bulk collect into a_stg_input limit g_forall_limit;
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
   
         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;
        
      end loop;
    fetch c_fnd_trading_entity bulk collect into a_stg_input limit g_forall_limit;     
    end loop;
    close c_fnd_trading_entity;
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
end wh_prf_corp_456U;
