--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_643U_FIXWL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_643U_FIXWL" 
  (
    p_forall_limit in integer,
    p_success out boolean)
as
  --**************************************************************************************************
  --  Date:        Oct 2008
  --  Author:      Wendy Lyttle
  --  Purpose:     Balance Of Contracts : creates final Balance_of_Contracts Weekly data
  --  Tables:      Input  -    Temp_FND_BOC_CONTRACT_WK
  --               Output -    RTL_CONTRACT_CHAIN_ITEM_WK_BOC
  --  Packages:    Constants, Dwh_Log, Dwh_Valid
  --  Comments:    Single Dml Could Be Considered For This Program.
  --
  --  Maintenance:
  --  20 Jan 2008 - defect 351 - Change to Temp_Fnd_Boc_Contract_wk structure
  --                           - Rename RTL_CONTRACT_CHAIN_ITEM_WK_BOC
  --                              to RTL_CONTRACT_CHAIN_ITEM_WK_BOC
  --                           -  Change to RTL_CONTRACT_CHAIN_ITEM_WK_BOC
  --                               structure
  --                           - CHAIN_NO added with hardcoded value = 10
  --  12 Feb 2009 - defect 528 - Add dim_calendar.this_week_start_date
  --  2 March 2009 - DEFECT 387 - RTL_CONTRACT_CHAIN_ITEM_WK_BOC has new fields
  --                              Table-Field Numbers: CONTRACT_MARGIN (25822),
  --                                                   BOC_MARGIN (25823)
  --  7 August 2009 - defect 2203 - PRC - Generation of the
  --                                RTL_CONTRACT_CHAIN_ITEM_WK_BOC table
  --                                (and the associated TEMP Table)
  --  24 Feb 2010 - defect2592 - Incorrect BOC_QTY_ALL_TIME value on
  --                              RTL_CONTRACT_CHAIN_SC_WK_BOC
  --
  --
    --
  --  25 mAY 2010 - DEFECT 3805 - remove sk2_item_no processing
  
  --  Naming Conventions:
  --  G_  -  Global Variable
  --  L_  -  Log Table Variable
  --  A_  -  Array Variable
  --  V_  -  Local Variable As Found In Packages
  --  P_  -  Parameter
  --  C_  -  Prefix To Cursor
  --
  --
  -- PROCESSING FLOW
  --   1. RTL_CONTRACT_CHAIN_ITEM_WK_BOC is truncated
  --   2. All records in TEMP_FND_BOC_CONTRACT_WK
  --          are accumulated
  --              and written to RTL_CONTRACT_CHAIN_ITEM_WK_BOC
  --  **
  --  **  Program coding follows general program template and standards
  --  **
  --**************************************************************************************************
  ---------------------------
  -----
  -- GENERAL PROGRAM VARIABLES
  -----
  g_recs_read     integer := 0;
  g_recs_updated  integer := 0;
  g_recs_inserted integer := 0;
  g_recs_deleted  integer := 0;
  g_recs_hospital integer := 0;
  g_forall_limit  integer := dwh_constants.vc_forall_limit;
  g_error_count   number  := 0;
  g_error_index   number  := 0;
  g_rec_out dwh_performance.RTL_CONTRACT_CHAIN_ITEM_WK_BOC%rowtype;
  g_found boolean;
  g_date date      := trunc(sysdate);
  g_yesterday date := trunc(sysdate) - 1;
  g_count number   := 0;
  ---
  --- Message Variables
  ---
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_643U_FIXWL';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'BOC - WRITE DERIVED CALCS TO TARGET';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
is
  table of RTL_CONTRACT_CHAIN_ITEM_WK_BOC%rowtype index by binary_integer;
type tbl_array_u
is
  table of RTL_CONTRACT_CHAIN_ITEM_WK_BOC%rowtype index by binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   integer := 0;
 a_count_i integer := 0;
  a_count_u integer := 0;
 -----------------------------------------------------
 ---- Cursor : Select all temp_fnd_boc_contract_wk
 ---           but calculate the ALL_TIME values
 ---                   at contract/item level (ie. does NOT include dates)
 --------------------------------
  cursor c_boc
  is
--   with sel_alltime
--     as
 --    (
--     select
--      a.sk1_contract_no      ,
--      c.sk1_chain_no         ,
--      a.sk1_item_no          ,
-- ----     max(a.sk2_item_no) sk2_item_no,
--      sum(boc_qty) boc_qty_all_time                           ,
--      sum(boc_selling) boc_selling_all_time                   ,
--      sum(boc_cost) boc_cost_all_time
--       from dwh_performance.temp_fnd_boc_contract_wk a, 
--       dwh_performance.dim_chain c
--       where c.chain_no = 10
--   group by      a.sk1_contract_no    ,
--      c.sk1_chain_no          ,
--      a.sk1_item_no    )
   select
      a.sk1_contract_no      ,
      c.sk1_chain_no         ,
      a.sk1_item_no          ,
      a.fin_year_no          ,
      a.fin_week_no          ,
  --    max(a.sk2_item_no) sk2_item_no,
      d.fin_week_code,
  --    max(a.fin_week_code) fin_week_code,
      max(a.contract_status_code) contract_status_code ,
      max(a.last_updated_date) last_updated_date   ,
     d.this_week_start_date,
    --    max(d.this_week_start_date) this_week_start_date,
      sum(contract_qty) contract_qty                                   ,
      sum(contract_selling) contract_selling                           ,
      sum(contract_cost) contract_cost                                 ,
      sum(po_qty) amended_po_qty                               ,
      sum(po_selling) amended_po_selling                       ,
      sum(po_cost) amended_po_cost                             ,
      sum(bc_shipment_qty) bc_shipment_qty                             ,
      sum(bc_shipment_selling) bc_shipment_selling                     ,
      sum(bc_shipment_cost) bc_shipment_cost                           ,
      sum(po_grn_qty) grn_qty                                             ,
      sum(po_grn_selling) grn_selling                                     ,
      sum(po_grn_cost) grn_cost                                           ,
      sum(latest_po_qty) latest_po_qty                                 ,
      sum(latest_po_selling) latest_po_selling                         ,
      sum(latest_po_cost) latest_po_cost                               ,
      sum(boc_qty) boc_qty                                             ,
      sum(boc_selling) boc_selling                                     ,
      sum(boc_cost) boc_cost                                           ,
      0 boc_qty_all_time                           ,
      0 boc_selling_all_time                   ,
      0 boc_cost_all_time
       from dwh_performance.temp_fnd_boc_contract_wk a, 
       dwh_performance.dim_chain c, dwh_performance.dim_calendar d
       --,
       --sel_alltime e
       where c.chain_no = 10
       and a.fin_year_no = d.fin_year_no
       and a.fin_week_no = d.fin_week_no
       and d.fin_day_no  = 3 
       ----and e.sk1_contract_no(+) = a.sk1_contract_no
       ----and e.sk1_item_no(+) = a.sk1_item_no
   group by a.sk1_contract_no, c.sk1_chain_no, a.sk1_item_no, 
   a.fin_year_no, a.fin_week_no, d.fin_week_code, d.this_week_start_date;
  -- Input record declared as cursor%rowtype
  g_rec_in c_boc%rowtype;
  -- Input bulk collect table declared
type stg_array
is
  table of c_boc%rowtype;
  a_stg_input stg_array;
  -- order by only where sequencing is essential to the correct loading of data
  --**************************************************************************************************
  --
  -- PROCEDURE : local_address_variable : Move data from input to output
  --
  --**************************************************************************************************
procedure local_address_variable
as
begin

  g_rec_out.sk1_item_no                   := g_rec_in.sk1_item_no;
--  g_rec_out.sk2_item_no                   := g_rec_in.sk2_item_no;
  g_rec_out.sk1_contract_no               := g_rec_in.sk1_contract_no;
  g_rec_out.sk1_chain_no                  := g_rec_in.sk1_chain_no;
  g_rec_out.fin_year_no                   := g_rec_in.fin_year_no;
  g_rec_out.fin_week_no                   := g_rec_in.fin_week_no;
  g_rec_out.fin_week_code                 := g_rec_in.fin_week_code;
  g_rec_out.contract_status_code          := g_rec_in.contract_status_code;
  g_rec_out.last_updated_date             := g_rec_in.last_updated_date;
  g_rec_out.contract_qty                  := g_rec_in.contract_qty;
  g_rec_out.contract_selling              := g_rec_in.contract_selling;
  g_rec_out.contract_cost                 := g_rec_in.contract_cost;
  g_rec_out.amended_po_qty                := g_rec_in.amended_po_qty;
  g_rec_out.amended_po_selling            := g_rec_in.amended_po_selling;
  g_rec_out.amended_po_cost               := g_rec_in.amended_po_cost;
  g_rec_out.bc_shipment_qty               := g_rec_in.bc_shipment_qty;
  g_rec_out.bc_shipment_selling           := g_rec_in.bc_shipment_selling;
  g_rec_out.bc_shipment_cost              := g_rec_in.bc_shipment_cost;
  g_rec_out.grn_qty                       := g_rec_in.grn_qty;
  g_rec_out.grn_selling                   := g_rec_in.grn_selling;
  g_rec_out.grn_cost                      := g_rec_in.grn_cost;
  g_rec_out.boc_qty                       := g_rec_in.boc_qty;
  g_rec_out.boc_selling                   := g_rec_in.boc_selling;
  g_rec_out.boc_cost                      := g_rec_in.boc_cost;
  g_rec_out.boc_qty_all_time              := g_rec_in.boc_qty_all_time ;
  g_rec_out.boc_selling_all_time          := g_rec_in.boc_selling_all_time ;
  g_rec_out.boc_cost_all_time             := g_rec_in.boc_cost_all_time ;
  --
  g_rec_out.contract_margin               := g_rec_in.contract_selling - g_rec_in.contract_cost;
  g_rec_out.boc_margin                    := g_rec_in.boc_selling - g_rec_in.boc_cost;
  g_rec_out.latest_po_qty                 := g_rec_in.latest_po_qty;
  g_rec_out.latest_po_selling             := g_rec_in.latest_po_selling;
  g_rec_out.latest_po_cost                := g_rec_in.latest_po_cost;
  g_rec_out.this_week_start_date          := g_rec_in.this_week_start_date;
exception
when others then
  l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  raise;
end local_address_variable;
--**************************************************************************************************
--
-- PROCEDURE : local_bulk_insert : Insert into Output Table
--
--**************************************************************************************************
procedure local_bulk_insert
as
begin
  forall i in a_tbl_insert.first .. a_tbl_insert.last
  save exceptions
   insert into dwh_performance.RTL_CONTRACT_CHAIN_ITEM_WK_BOC values a_tbl_insert
    (i
    );

  g_recs_inserted := g_recs_inserted + a_tbl_insert.count;
exception
when others then
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
  dwh_log.record_error
  (
    l_module_name,sqlcode,l_message
  )
  ;
  for i in 1 .. g_error_count
  loop
    g_error_index := sql%bulk_exceptions
    (
      i
    )
    .error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm
    (
      -sql%bulk_exceptions(i).error_code
    )
    || ' '||a_tbl_insert
    (
      g_error_index
    )
    .sk1_contract_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .sk1_item_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .fin_year_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .fin_week_no|| a_count;
    dwh_log.record_error
    (
      l_module_name,sqlcode,l_message
    )
    ;
  end loop;
  raise;
end local_bulk_insert;
--**************************************************************************************************
--
-- PROCEDURE : local_write_output : Process Procedures to Insert/Update to OUtput Table
--
--**************************************************************************************************
procedure local_write_output
as
begin
  a_count_i := a_count_i + 1;
  a_tbl_insert
  (
    a_count_i
  )
          := g_rec_out;
  a_count := a_count + 1;
  --**************************************************************************************************
  -- Bulk 'write from array' loop controlling bulk inserts and updates to output table
  --**************************************************************************************************
  if a_count > g_forall_limit then
    local_bulk_insert;
    a_tbl_insert := a_empty_set_i;
    a_count_i    := 0;
    a_count      := 0;
    commit;
  end if;
exception
when dwh_errors.e_insert_error then
  l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error
  (
    l_module_name,sqlcode,l_message
  )
  ;
  raise;
when others then
  l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error
  (
    l_module_name,sqlcode,l_message
  )
  ;
  raise;
end local_write_output;
--**************************************************************************************************
--
-- PROCEDURE : local_bulk_insert : Insert into Output Table
--
--**************************************************************************************************
procedure full_update
as
begin
for v_cur in (
--with selext as
(select sk1_contract_no, sk1_item_no, max(this_week_start_date) max_week,
sum(boc_qty) boc_qty_all_time                           ,
sum(boc_selling) boc_selling_all_time                   ,
sum(boc_cost) boc_cost_all_time
from dwh_performance.temp_fnd_boc_contract_wk a, dwh_performance.dim_calendar b
where a.fin_year_no = b.fin_year_no
and a.fin_week_no = b.fin_week_no
and b.fin_day_no = 3
group by sk1_contract_no, sk1_item_no))
--select sk1_contract_no, sk1_item_no,  max_week,
-- boc_qty_all_time                           ,
-- boc_selling_all_time                   ,
-- boc_cost_all_time
--from selext b)
loop 
update dwh_performance.RTL_CONTRACT_CHAIN_ITEM_WK_BOC d
set d.boc_qty_all_time = v_cur.boc_qty_all_time,
d.boc_selling_all_time = v_cur.boc_selling_all_time,
d.boc_cost_all_time = v_cur.boc_cost_all_time
where d.sk1_contract_no = v_cur.sk1_contract_no
and d.sk1_item_no = v_cur.sk1_item_no
and d.this_week_start_date = v_cur.max_week;
commit;
g_recs_updated := g_recs_updated + 1;
end loop;
   
exception
when others then
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
  dwh_log.record_error
  (
    l_module_name,sqlcode,l_message
  )
  ;
  for i in 1 .. g_error_count
  loop
    g_error_index := sql%bulk_exceptions
    (
      i
    )
    .error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm
    (
      -sql%bulk_exceptions(i).error_code
    )
    || ' '||a_tbl_insert
    (
      g_error_index
    )
    .sk1_contract_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .sk1_item_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .fin_year_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .fin_week_no|| a_count;
    dwh_log.record_error
    (
      l_module_name,sqlcode,l_message
    )
    ;
  end loop;
  raise;
end full_update;

--**************************************************************************************************
--
--
-- M A I N   P R O C E S S
--
--
-- DWH_LOG.WRITE_LOG          : BOC - LOAD OF RTL_CONTRACT_CHAIN_ITEM_WK_BOC STARTED AT
-- DWH_LOG.INSERT_LOG_SUMMARY : BOC - LOAD OF RTL_CONTRACT_CHAIN_ITEM_WK_BOC STARTED AT
--**************************************************************************************************
begin
  p_success      := false;
  l_text         := dwh_constants.vc_log_draw_line;
  dwh_log.write_log ( l_name,l_system_name,l_script_name,l_procedure_name,l_text ) ;
  l_text := 'BOC - WRITE TO TARGET STARTED AT '|| to_char ( sysdate,('dd mon yyyy hh24:mi:ss') ) ;
  dwh_log.write_log ( l_name,l_system_name,l_script_name,l_procedure_name,l_text ) ;
  dwh_log.insert_log_summary ( l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','' ) ;
  --**************************************************************************************************
  -- LOOK UP BATCH DATE FROM DIM_CONTROL
  -- DWH_LOG.WRITE_LOG   : g_calc_sysdate
  --**************************************************************************************************
  dwh_lookup.dim_control ( g_date ) ;
  
  G_DATE := G_DATE + 100;
  
  l_text := 'Batch date selected is :- '||g_date;
  dwh_log.write_log ( l_name,l_system_name,l_script_name,l_procedure_name,l_text ) ;
        if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;


 --**************************************************************************************************
 -- hardcoded dates to do a full run
 --**************************************************************************************************
    l_text :='HARDCODED START AND END DATES :- 01 jan 01 to 30 jun 30';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   execute immediate
   ('truncate table dwh_performance.RTL_CONTRACT_CHAIN_ITEM_WK_BOC');
   commit;
  l_text :='TRUNCATED RTL_CONTRACT_CHAIN_ITEM_WK_BOC AT '||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   --**************************************************************************************************
  -- Delete from  RTL_CONTRACT_CHAIN_ITEM_WK_BOC where contracts in Temp_FND_BOC_CONTRACT_WK
  --
  -- DWH_LOG.WRITE_LOG   : RTL_CONTRACT_CHAIN_ITEM_WK_BOC pre-load records deleted
  --**************************************************************************************************
 --  Delete from  RTL_CONTRACT_CHAIN_ITEM_WK_BOC a
-- where a.sk1_contract_no in(select distinct c.sk1_contract_no from Temp_FND_BOC_CONTRACT_WK b,
-- dim_contract c
 --where b.contract_no = c.contract_no)
  -- ;
 --  g_recs_deleted := to_number
 --  (
 --    to_char(sql%rowcount)
--  )
 --  ;
 --  l_text :='RTL_CONTRACT_CHAIN_ITEM_WK_BOC pre-load records deleted = '||g_recs_deleted;
--   dwh_log.write_log
--   (
 --   l_name,l_system_name,l_script_name,l_procedure_name,l_text
--   )
 --  ;
 --  commit;
  --**************************************************************************************************
  -- Insert intoe RTL_CONTRACT_CHAIN_ITEM_WK_BOC
  --**************************************************************************************************
   open c_boc;
   fetch c_boc bulk collect into a_stg_input limit g_forall_limit;

   while a_stg_input.count > 0
   loop
     for i in 1 .. a_stg_input.count
     loop
       g_recs_read             := g_recs_read + 1;
       if g_recs_read mod 10000 = 0 then
         l_text                := dwh_constants.vc_log_records_processed|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
         dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       end if;
      g_rec_in := a_stg_input(i);
       local_address_variable;
       local_write_output;
     end loop;
     fetch c_boc bulk collect into a_stg_input limit g_forall_limit;
   end loop;
   close c_boc;
  --**************************************************************************************************
  -- At end write out what remains in the arrays
  --**************************************************************************************************
  local_bulk_insert;
  --full_update;
  --**************************************************************************************************
  -- At end write out log totals
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);
  l_text := dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read||'  AT '||to_char ( sysdate,('dd mon yyyy hh24:mi:ss') ) ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated||'  AT '||to_char ( sysdate,('dd mon yyyy hh24:mi:ss') ) ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted||'  AT '||to_char ( sysdate,('dd mon yyyy hh24:mi:ss') ) ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_hospital||g_recs_hospital||'  AT '||to_char ( sysdate,('dd mon yyyy hh24:mi:ss') ) ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed||'  AT '||to_char ( sysdate,('dd mon yyyy hh24:mi:ss') ) ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := ' ';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  commit;
  p_success := true;
exception
when dwh_errors.e_insert_error then
  l_message := dwh_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;
when others then
  l_message := dwh_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_aborted,'','','','','');
  rollback;
  p_success := false;
  raise;
end wh_prf_corp_643u_fixWL;
