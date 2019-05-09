--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_266U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_266U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        FEB 2014
--  Author:      Alastair de Wet
--  Purpose:     Create cust value segment table in the performance layer
--               with added value ex basket item.
--  Tables:      Input  - cust_basket_item
--               Output - cust_csm_value_segment
--  Packages:    constants, dwh_log, dwh_valid
--
--   !!!!!!!!!!!!!!!!!!!!!!!THIS IS THE NON FOODS PROGRAM !!!!!!!!!!!!!!!!!!!!!!!
--
--  Maintenance:
--  HardCode Value g_avg_spend needs to change regularly as inflation happens - address_variables
--
--  Naming conventions:
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor

--**************************************************************************************************
--g_avg_spend changes once a year to allow for inflation.
--MAKE SURE YOU COMMENT OUT THE OLD AND KEEP THE NEW SO THAT WE CAN SEE THE CHANGES OVER TIME
--See note next to variable below     
--**************************************************************************************************
g_avg_spend          integer       := 370;  -- Implemented this threshold in July 2017

g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_forall_limit       integer       :=  10000;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_rec_out            cust_csm_value_segment%rowtype;

g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
g_start_date         date;
g_end_date           date;
g_run_date           date;
g_end_year_no        integer;
g_end_month_no       integer;
g_count              integer;
g_table_count        integer;
g_stmt               varchar2(300);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_266U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'CREATE CUST_CSM_VALUE_SEGMENT EX BASKET_ITEM';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;




-- For output arrays into bulk load forall statements --
type tbl_array_i is table of cust_csm_value_segment%rowtype index by binary_integer;
type tbl_array_u is table of cust_csm_value_segment%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;


cursor c_cust_basket_item is
select /*+ FULL(a) parallel (a,8)  full(b) */
       a.primary_customer_identifier,
       sum(a.item_tran_selling - a.discount_selling) total_value,
       count(unique a.tran_no||a.location_no||a.till_no) num_visit ,
       sum(a.item_tran_qty) num_item
from   cust_basket_item a,
       dim_item b
where  a.tran_date between   g_start_date  and   g_end_date
and    a.item_no = b.item_no
and    b.business_unit_no not in(50,70)
and    a.primary_customer_identifier <> 998
and    a.primary_customer_identifier is not null
and    a.tran_type not in ('P','N','L')
and    substr(a.primary_customer_identifier,1,8) <> (60078514)
and    b.item_no not in (60,100,999911,999960,6008000027502)
group by a.primary_customer_identifier ;

g_rec_in             c_cust_basket_item%rowtype;

-- For input bulk collect --
type stg_array is table of c_cust_basket_item%rowtype;
a_stg_input      stg_array;
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin


   g_rec_out.primary_customer_identifier     := g_rec_in.primary_customer_identifier;
   g_rec_out.fin_year_no                     := g_end_year_no;
   g_rec_out.fin_month_no                    := g_end_month_no;
   g_rec_out.food_non_food                   := 'NFSHV';
   g_rec_out.start_date                      := g_start_date;
   g_rec_out.end_date                        := g_end_date;
   g_rec_out.num_visit                       := g_rec_in.num_visit;
   g_rec_out.total_value                     := g_rec_in.total_value;
   g_rec_out.num_item                        := g_rec_in.num_item;
   g_rec_out.last_updated_date               := g_date;

   g_rec_out.avg_value_per_visit             := g_rec_out.total_value /g_rec_out.num_visit ;

      if g_rec_out.num_visit <= 2 and g_rec_out.avg_value_per_visit < g_avg_spend then
        g_rec_out.current_seg := 6;
      end if;

      if (g_rec_out.num_visit = 3
      or  g_rec_out.num_visit = 4
      or  g_rec_out.num_visit = 5
      or  g_rec_out.num_visit = 6)
      and g_rec_out.avg_value_per_visit < g_avg_spend then
          g_rec_out.current_seg := 5;
      end if;

      if g_rec_out.num_visit <= 2 and g_rec_out.avg_value_per_visit >= g_avg_spend then
         g_rec_out.current_seg := 4;
      end if;

      if (g_rec_out.num_visit = 3
      or  g_rec_out.num_visit = 4
      or  g_rec_out.num_visit = 5
      or  g_rec_out.num_visit = 6)
      and g_rec_out.avg_value_per_visit >= g_avg_spend then
          g_rec_out.current_seg := 3;
      end if;

      if g_rec_out.num_visit > 6 and g_rec_out.avg_value_per_visit < g_avg_spend then
         g_rec_out.current_seg := 2;
      end if;

      if g_rec_out.num_visit > 6 and g_rec_out.avg_value_per_visit >= g_avg_spend then
         g_rec_out.current_seg := 1;
      end if;

   exception
      when others then
       l_message := dwh_cust_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
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
      insert into cust_csm_value_segment values a_tbl_insert(i);
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
                       ' '||SQLERRM(-SQL%BULK_EXCEPTIONS(I).ERROR_CODE)||
                       ' '||a_tbl_insert(g_error_index).primary_customer_identifier;
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
      update cust_csm_value_segment
      set    current_seg                     = a_tbl_update(i).current_seg,
             start_date                      = a_tbl_update(i).start_date,
             end_date                        = a_tbl_update(i).end_date,
             num_visit                       = a_tbl_update(i).num_visit,
             total_value                     = a_tbl_update(i).total_value,
             avg_value_per_visit             = a_tbl_update(i).avg_value_per_visit,
             num_item                        = a_tbl_update(i).num_item,
             last_updated_date               = a_tbl_update(i).last_updated_date
      where  primary_customer_identifier     = a_tbl_update(i).primary_customer_identifier and
             fin_year_no                     = a_tbl_update(i).fin_year_no and
             fin_month_no                    = a_tbl_update(i).fin_month_no and
             food_non_food                   = a_tbl_update(i).food_non_food and
             (
              nvl(current_seg,0)                    <> a_tbl_update(i).current_seg or
              nvl(num_visit,0)                      <> a_tbl_update(i).num_visit or
              nvl(total_value,0)                    <> a_tbl_update(i).total_value or
              nvl(avg_value_per_visit,0)            <> a_tbl_update(i).avg_value_per_visit or
              nvl(num_item,0)                       <> a_tbl_update(i).num_item
              );
      g_recs_updated := g_recs_updated + a_tbl_update.count;


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
                       ' '||a_tbl_update(g_error_index).primary_customer_identifier ;
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

-- Check to see if present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   cust_csm_value_segment
   where  primary_customer_identifier        = g_rec_out.primary_customer_identifier and
          fin_year_no                        = g_rec_out.fin_year_no and
          fin_month_no                       = g_rec_out.fin_month_no and
          food_non_food                      = g_rec_out.food_non_food ;

   if g_count = 1 then
      g_found := true;
   end if;
-- Place record into array for later bulk writing
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
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;




end local_write_output;

--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF cust_csm_value_segment EX cust_basket_item STARTED AT '||
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
-- Look up range of weeks to be processed and store in variables
--**************************************************************************************************
    select   max(fin_week_end_date),max(fin_year_no),max(fin_month_no)
    into     g_end_date,g_end_year_no,g_end_month_no
    from     dim_calendar
    where    fin_year_no  =  (select last_yr_fin_year_no from dim_control)  and
             fin_month_no =  (select last_mn_fin_month_no from dim_control);

    g_start_date := g_end_date - 125;

    l_text := 'ROLLUP RANGE IS:- '||g_start_date||'  to '||g_end_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
   g_run_date := g_end_date + 4;
   if trunc(sysdate) <> g_run_date then
      l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is not that day !';
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      p_success := true;
      return;
   end if;  
   
   l_text      := 'This job only runs on '||g_run_date||' and today '||trunc(sysdate)||' is that day !';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    select count(*) 
    into g_table_count
    from cust_csm_value_segment
    where fin_year_no = g_end_year_no
     and  fin_month_no = g_end_month_no;
     
    if g_table_count > 0 then
       g_stmt   := 'Alter table  DWH_CUST_PERFORMANCE.cust_csm_value_segment truncate  subpartition for ('||g_end_year_no||','||g_end_month_no||') update global indexes';
       l_text   := g_stmt;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       execute immediate g_stmt;  
    end if;
    


--**************************************************************************************************
    open c_cust_basket_item;
    fetch c_cust_basket_item bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 1000000 = 0 then
            l_text := dwh_cust_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in := a_stg_input(i);
         local_address_variable;
         local_write_output;

      end loop;
    fetch c_cust_basket_item bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_cust_basket_item;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;



--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
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

END WH_PRF_CUST_266U;
