--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_668B_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_668B_BCK" 
                                                                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        May 2010
--  Author:      W Lyttle
--  Purpose:     Update Contract Chain Item WK BOC_ALLTIME values for the current week.
--               If no current week record exists, then one must be inserted with only.
--               the alltime values on it.
--               If there are future-dated boc records, then these also need
--               to have a current week inserted.
--               Contract tables will contain data for CHBD only.
--  Tables:      Input  - rtl_contract_chain_item_wk
--               Output - rtl_contract_chain_item_wk
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--  25 mAY 2010 - DEFECT 3805 - remove sk2_item_no processing
--  27 mAY 2010 - DEFECT  - ALLTIME BOC to be put into week= g_date - 1 day
--
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            DWH_PERFORMANCE.rtl_contract_chain_item_wk%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_this_week_start_date date;
g_fin_year_no        number        :=  0;
g_fin_week_no        number        :=  0;
g_fin_day_no         number        :=  0;
g_fin_week_code      varchar2(7)   ;
g7_date              date          := trunc(sysdate);
g7_this_week_start_date date;
g7_fin_year_no       number        :=  0;
g7_fin_week_no       number        :=  0;
g7_fin_week_code     varchar2(7)   ;

g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_668B';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'UPDATE CONTRACT_CHAIN_ITEM_WK BOC-ALLTIME VALUES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_PERFORMANCE.rtl_contract_chain_item_wk%rowtype index by binary_integer;
type tbl_array_u is table of DWH_PERFORMANCE.rtl_contract_chain_item_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_contract_item_wk is
SELECT 
 boc.sk1_contract_no sk1_contract_no
,boc.sk1_chain_no sk1_chain_no
,boc.sk1_item_no sk1_item_no
,g_fin_year_no FIN_YEAR_NO
,g_fin_week_no FIN_WEEK_NO
,g_fin_week_code FIN_WEEK_CODE
,g_this_week_start_date THIS_WEEK_START_DATE
,'' CONTRACT_STATUS_CODE
,0 CONTRACT_QTY
,0 CONTRACT_SELLING
,0 CONTRACT_COST
,0 ACTL_GRN_QTY
,0 ACTL_GRN_SELLING
,0 ACTL_GRN_COST
,0 AMENDED_PO_QTY
,0 AMENDED_PO_SELLING
,0 AMENDED_PO_COST
,0 BC_SHIPMENT_QTY
,0 BC_SHIPMENT_SELLING
,0 BC_SHIPMENT_COST
,0 PO_GRN_QTY
,0 PO_GRN_SELLING
,0 PO_GRN_COST
,0 LATEST_PO_QTY
,0 LATEST_PO_SELLING
,0 LATEST_PO_COST
,0 BOC_QTY
,0 BOC_SELLING
,0 BOC_COST
,sum(NVL(boc.boc_qty_all_time,0)) boc_qty_all_time
,sum(NVL(boc.boc_selling_all_time,0)) boc_selling_all_time
,sum(NVL(boc.boc_cost_all_time,0)) boc_cost_all_time
,0 NUM_DU
,0 NUM_WEIGHTED_DAYS_TO_DELIVER
,g_date LAST_UPDATED_DATE
FROM DWH_PERFORMANCE.rtl_contract_chain_item_wk_boc BOC
group by boc.sk1_contract_no, boc.sk1_chain_no, boc.sk1_item_no, g_fin_year_no,
g_fin_week_no, g_fin_week_code, g_this_week_start_date,
 g_date;
   
-- For input bulk collect --
type stg_array is table of c_contract_item_wk%rowtype;
a_stg_input      stg_array;

g_rec_in             c_contract_item_wk%rowtype;

--************************************************************************************************** 
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out      := g_rec_in;
   g_rec_out.FIN_YEAR_NO := g7_fin_year_no ;
   g_rec_out.FIN_WEEK_NO := g7_fin_week_no ;
   g_rec_out.FIN_WEEK_CODE := g7_fin_week_code ;
   g_rec_out.THIS_WEEK_START_DATE := g7_this_week_start_date ;

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
      insert into DWH_PERFORMANCE.rtl_contract_chain_item_wk values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).sk1_contract_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).this_week_start_date||
                       ' '||a_tbl_insert(g_error_index).fin_week_no||
                       ' '||a_tbl_insert(g_error_index).sk1_chain_no;
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
       update DWH_PERFORMANCE.rtl_contract_chain_item_wk
       set    boc_qty_all_time     = a_tbl_update(i).boc_qty_all_time,
              boc_selling_all_time = a_tbl_update(i).boc_selling_all_time,
              boc_cost_all_time    = a_tbl_update(i).boc_cost_all_time
       where  sk1_item_no          = a_tbl_update(i).sk1_item_no  and
              sk1_chain_no         = a_tbl_update(i).sk1_chain_no  and
              sk1_contract_no      = a_tbl_update(i).sk1_contract_no and
              this_week_start_date  = a_tbl_update(i).this_week_start_date;
       
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
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).sk1_contract_no||
                       ' '||a_tbl_update(g_error_index).this_week_start_date||
                       ' '||a_tbl_update(g_error_index).fin_week_no||
                                    ' '||a_tbl_update(g_error_index).sk1_chain_no;
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
 ---

  select count(*) into g_count
    from DWH_PERFORMANCE.rtl_contract_chain_item_wk
   where sk1_contract_no  = g_rec_out.sk1_contract_no
     and sk1_item_no      = g_rec_out.sk1_item_no
     and sk1_chain_no     = g_rec_out.sk1_chain_no
     and fin_year_no      = g_rec_out.fin_year_no
     and fin_week_no      = g_rec_out.fin_week_no;
 
  --and this_week_start_date = g7_this_week_start_date;
 
  if g_count = 1 then
      g_found := TRUE;
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
   if a_count > g_forall_limit then  
      local_bulk_insert;
            local_bulk_update; 
     COMMIT;
    
      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
       
      commit;
   end if; 

   COMMIT;
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
    
    l_text := 'UPDATE CONTRACT_CHAIN_ITEM_WK BOC-ALLTIME VALUES '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');
    
--************************************************************************************************** 
-- Look up batch date from dim_control   
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
 
          -- for testing - hardcoded date --begin
 --   g_date := '06-Dec-10';
 --   DBMS_OUTPUT.PUT_LINE('for testing - hardcoded date ='||g_date);
      --
    -- for testing - hardcoded date --end
  -- 
  --  The All Time BOC  on a Monday should show the BOC All Time for Sunday , 
  --  The ALL time BOC on a Tuesday should show the BOC for Monday  
  --   ( the previous day )  etc etc . 
  --   THus for example on Monday the 24th May  , 
  --            this will show the BOC All time  as at  previous day ,
  --          Sunday the 23rd .
  --        The current week for  Monday the 24th is week 48 . 
  --
  --   HENCE g_date - 1 day to determine the week to put ALLTIME_BOC into
  --
    select fin_year_no, fin_week_no, this_week_start_date, fin_week_code,fin_day_no
    into g_fin_year_no, g_fin_week_no, g_this_week_start_date, g_fin_week_code,
    g_fin_day_no
    from dim_calendar
    where calendar_date = g_date ;
    --
    --        
    l_text := 'g_date   processing dates - '||g_fin_year_no||' '||g_fin_week_no
    ||' '||g_this_week_start_date||' '||g_fin_week_code;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    --
    if g_fin_day_no = 7
    then 
        select fin_year_no, fin_week_no, this_week_start_date, fin_week_code
          into g7_fin_year_no, g7_fin_week_no, g7_this_week_start_date, g7_fin_week_code
          from dim_calendar
          where calendar_date = g_date+1 ;
    else 
          g7_fin_year_no :=  g_fin_year_no;
          g7_fin_week_no := g_fin_week_no;
          g7_this_week_start_date := g_this_week_start_date;
          g7_fin_week_code := g_fin_week_code;
    end if;
    --
    --
    l_text := 'g_date+7   processing dates - '||g7_fin_year_no||' '||g7_fin_week_no
    ||' '||g7_this_week_start_date||' '||g7_fin_week_code;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--************************************************************************************************** 
-- MAKE ALL ALLTIME VALUES ZERO ON RTL_CONTRACT_ITEM_CHAIN_WK before stamping current week
-- with alltime value
--**************************************************************************************************
     execute immediate 'alter session enable parallel dml';
     update /*+ parallel (RTL_CONTRACT_CHAIN_ITEM_WK,8) */ DWH_PERFORMANCE.rtl_contract_chain_item_wk
     set boc_qty_all_time     = 0,
         boc_selling_all_time = 0,
         boc_cost_all_time = 0; 
    
      g_recs_updated := sql%rowcount;
    commit;
    
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','rtl_contract_chain_item_wk', DEGREE => 32);
    commit;
    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE','rtl_contract_chain_item_wk_boc', DEGREE => 32);
    commit;

    l_text := 'RECORDS alltime values SET TO ZEROES - '||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_recs_updated := 0;

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_contract_item_wk;
    fetch c_contract_item_wk bulk collect into a_stg_input limit g_forall_limit;
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

         local_address_variables;
         local_write_output;
      
      end loop;
    fetch c_contract_item_wk bulk collect into a_stg_input limit g_forall_limit;     
    end loop;
    close c_contract_item_wk;
--**************************************************************************************************  
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

   local_bulk_insert;
   local_bulk_update;   
   COMMIT;
 
--************************************************************************************************** 
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',''); 
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_corp_668b_bck;
