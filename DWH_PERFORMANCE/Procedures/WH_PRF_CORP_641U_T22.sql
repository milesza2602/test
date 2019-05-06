--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_641U_T22
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_641U_T22" 
  (
    p_forall_limit in integer,
    p_success out boolean)
as
  --**************************************************************************************************
  --  Date:        Oct 2008
  --  Author:      Wendy Lyttle
  --  Purpose:     Balance Of Contracts : Create A Temporary Table of recalculated values for Contracts
  --                                      That Have Had Contract And/Or Shipment And/Or Purchase_Order
  --                                      Information Changes During The Last 3-Days.
  --  Tables:      Input  -    fnd_rtl_contract
  --                           Fnd_rtl_Shipment
  --                           Fnd_rtl_Purchase_Order
  --                           dim_item
  --                           Dim_calendar
  --                           temp_fnd_boc_contract_list
  --               Output -    temp_fnd_boc_contract_wk
  --  Packages:    Constants, Dwh_Log, Dwh_Valid
  --  Comments:    Single Dml Could Be Considered For This Program.
  --
  --  Maintenance:
  --  20 Jan 2008   - defect 351  - Change to temp_fnd_boc_contract_wk structure
  --  27 June 2009  - defect 1848 - Change requests and defects for BOC set
  --                                of Procedures
  --  03 Aug 2009   - defect 2174 - BOC Overstated on RTL_CONTRACT_CHAIN_ITEM_WK_BOC
  --  03 Aug 2009   - defect 2175 - Closed Contracts - BOC Not being zeroised (0)
  --  7 August 2009 - defect 2203 - PRC - Generation of the
  --                                RTL_CONTRACT_CHAIN_ITEM_WK_BOC table
  --                                (and the associated TEMP Table)
  --  17 August 2009 - defect 2268 - PRC - Deleted Contract Lines appearing
  --                                 rtl_contract_chain_item_wk_boc
  --  24 August 2009 - defect 2305 - BOC returning 3x value for some contracts
  --                                 as well as some deleted contracts
  --
  --  17 Sept 2009 - defect 2390 - WWP/ CHP - Fin Inventory Adjustments - GROSS_PROFIT_RC_EXCLUDE_IND
  --                                 as well as some deleted contracts
  --
  --
  --  25 mAY 2010 - DEFECT 3805 - remove sk2_item_no processing
  --
  --  Naming Conventions:
  --  G_  -  Global Variable
  --  L_  -  Log Table Variable
  --  A_  -  Array Variable
  --  V_  -  Local Variable As Found In Packages
  --  P_  -  Parameter
  --  C_  -  Prefix To Cursor
  --
  --  PROCESSING FLOW
  --   1. Clear temp_fnd_boc_contract_wk
  --   2. Select contracts from temp_fnd_boc_contract_wk
  --  With
  --      ext_con as - select contract data, calculate contract_measures where contracts in list
  --      ext_ship as - select shipments data , calculate asn_measures where contracts in list
  --      ext_po as - select po data , calculate amended_po_measures where contracts in list
  --      ext_grn as - select grn data , calculate grn_measures where contracts in list
  --      ext_stat as - select contracts in list but transforming to derive surrogate_keys
  --                                               and rolling-up to weekly level.
  --  Select by matching (ext_con, ext_ship, ext_po, ext_grn) OUTERJOIN to ext_stat
  --            on contract_no,
  --               sk1_item_no, sk2_item_no,
  --               fin_year_no, fin_week_no,
  --               contract_status_code
  --  Calculate latest_po_measures and boc_measures
  --        latest_po_measures = amended_po + grn + asn
  --        With conditions
  --        contract_status_code = 'C'
  --                     then boc_measures = 0
  --        contract_status_code = 'A' and po_status_code = 'C'
  --                     then boc_measures = contract - grn
  --        contract_status_code = 'A' and po_status_code <> 'C' and asn_measures = 0
  --                     then boc_measures = contract - po
  --        contract_status_code = 'A' and po_status_code <> 'C' and asn_measures <> 0
  --                     then boc_measures = contract - asn
  -- Insert into temp_fnd_boc_contract_list
  --        key = contract_no. sk1_item_no, sk2_item_no
  --              fin_year_no, fin_week_no, fin_week_code
  --              contract_status_code, po_status_code
  --        measures = contract, asn, amended_po, grn, latest_po, boc
  ------------
  --**************************************************************************************************
  --**************************************************************************************************
  -----
  -- NEW PROGRAM VARIABLES
  -----
  g_update_count                 number(13);
  g_force_error                  integer := 0;
  g_recs_merged_contract         integer := 0;
  g_recs_merged_purchase_order   integer := 0;
  g_recs_updated_shipment        integer := 0;
  g_recs_updated_contract        integer := 0;
  g_recs_updated_purchase_order  integer := 0;
  g_recs_inserted_shipment       integer := 0;
  g_recs_inserted_contract       integer := 0;
  g_recs_inserted_purchase_order integer := 0;
  g_recs_contract                integer := 0;
  g_recs_purch_order             integer := 0;
  g_recs_shipment                integer := 0;
  ------
  -- Variables used for DATE_LAST_UPDATED
  -- eg. DATE_LAST_UPDATED > SYSDATE - 3
  ------
  g_calc_sysdate date;
  g_calc_sysdate_days integer := 100;
  ------
  -- Variables used for Calculations
  ------
  v_boc_qty                      number(14,3);
  v_boc_selling                  number(14,2);
  v_boc_cost                     number(14,2);
  v_latest_po_qty                number(14,3) :=0;
  v_latest_po_selling            number(14,2) :=0;
  v_latest_po_cost               number(14,2) :=0;
  g_fin_week_code                varchar2(7);
  g_fin_week_year                number(4) :=0;
  ---------------------------
  -----
  -- GENERAL PROGRAM VARIABLES
  -----
  g_forall_limit  integer := dwh_constants.vc_forall_limit;
  g_recs_read     integer := 0;
  g_recs_updated  integer := 0;
  g_recs_inserted integer := 0;
  g_error_count   number  := 0;
  g_error_index   number  := 0;
  g_found         boolean;
  g_valid         boolean;
  g_count         integer := 0;
  g_date date             := trunc(sysdate);
  ---
  --- Message Variables
  ---
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_641U_T22';
  ----
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_fnd;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_fnd_md;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  ----
  l_description sys_dwh_log_summary.log_description%type   := 'BOC - CREATE CALCULATIONS';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

cursor c_boc
is
with
  ext_con as
--------------
-- select all contracts contract measures
--     where contract_no exists in the list of extracted contracts
--       and contract_status_code on list in('A','C')
--------------
          (select tf.sk1_contract_no      ,
                  tf.sk1_item_no          ,
   --               tf.sk2_item_no          ,
                  tf.fin_year_no          ,
                  tf.fin_week_no          ,
                  tf.fin_week_code        ,
                  tf.contract_status_code ,
                  sum(contract_qty) contract_qty                           ,
                  sum(contract_qty * (nvl(frc.reg_rsp_excl_vat,di.base_rsp_excl_vat))) contract_selling,
 --                 sum(contract_qty * di.base_rsp_excl_vat) contract_selling,
                  sum(contract_qty * frc.cost_price) contract_cost
             from dwh_foundation.fnd_rtl_contract frc,
                  dwh_performance.temp_fnd_boc_contract_list tf,
                  dwh_performance.dim_item di
            where frc.contract_no          = tf.contract_no
              and frc.ready_date           = tf.tran_date
              and frc.item_no              = tf.item_no
              and frc.contract_status_code = tf.contract_status_code
              and nvl(frc.source_data_status_code,0) = nvl(tf.source_data_status_code,0)
              and frc.item_no = di.item_no
              and (frc.chain_code <> 'DJ' or frc.chain_code is null)
--              and frc.contract_status_code  in('A','C')
--              and (frc.source_data_status_code <> 'D'
 --             OR frc.source_data_status_code is null)
--              and (tf.source_data_status_code <> 'D'
--               OR tf.source_data_status_code is null)

         group by tf.sk1_contract_no, tf.sk1_item_no, tf.fin_year_no, tf.fin_week_no, tf.fin_week_code, tf.contract_status_code),
--------------
  ext_posup as
--------------
-- select all po_supchain amended_po, grn, bc_shipment measures
--     where contract_no exists in the list of extracted contracts
--       and contract_status_code on list in('A','C')
--------------
          (select tf.sk1_contract_no      ,
                  tf.sk1_item_no          ,
        --          tf.sk2_item_no          ,
                  tf.fin_year_no          ,
                  tf.fin_week_no          ,
                  tf.fin_week_code        ,
                  tf.contract_status_code ,
                  sum(bc_shipment_qty) bc_shipment_qty        ,
                  sum(bc_shipment_selling) bc_shipment_selling,
                  sum(bc_shipment_cost) bc_shipment_cost      ,
                  sum(amended_po_qty) amended_po_qty          ,
                  sum(amended_po_selling) amended_po_selling  ,
                  sum(amended_po_cost) amended_po_cost        ,
                  sum(po_grn_qty) po_grn_qty                  ,
                  sum(po_grn_selling) po_grn_selling          ,
                  sum(po_grn_cost) po_grn_cost                ,
                  sum(latest_po_qty) latest_po_qty                  ,
                  sum(latest_po_selling) latest_po_selling          ,
                  sum(latest_po_cost) latest_po_cost
             from  dwh_performance.rtl_po_supchain_loc_item_dy posp,
                   dwh_performance.temp_fnd_boc_contract_list tf
            where posp.sk1_contract_no          = tf.sk1_contract_no
              and posp.tran_date                = tf.tran_date
              and posp.sk1_item_no              = tf.sk1_item_no
 --             and posp.contract_status_code = tf.contract_status_code
 --             and tf.contract_status_code  in('A','C')
 --             and (tf.source_data_status_code <> 'D'
 --              OR tf.source_data_status_code is null)
         group by tf.sk1_contract_no      ,
                  tf.sk1_item_no          ,
       --           tf.sk2_item_no          ,
                  tf.fin_year_no          ,
                  tf.fin_week_no          ,
                  tf.fin_week_code        ,
                  tf.contract_status_code ),
--------------
    ext_stat as
--------------
-- select all contract_no, item_no, fin_year_no, fin_week_no
--     where contract_status_code on list in('A','C')
--------------
  (select tf.sk1_contract_no               ,
    tf.contract_no               ,
    tf.item_no                         ,
    tf.sk1_item_no                     ,
--    tf.sk2_item_no                     ,
    tf.fin_year_no                     ,
    tf.fin_week_no                     ,
    tf.fin_week_code                   ,
    tf.contract_status_code
     from  dwh_performance.temp_fnd_boc_contract_list tf
 --   where tf.contract_status_code in('A','C')
 --     and (tf.source_data_status_code <> 'D'
 --      OR tf.source_data_status_code is null)
 group by tf.sk1_contract_no               ,
    tf.contract_no               ,
    tf.item_no                         ,
    tf.sk1_item_no                     ,
 --   tf.sk2_item_no ,
    tf.fin_year_no                     ,
    tf.fin_week_no                     ,
    tf.fin_week_code                   ,
    tf.contract_status_code)
 select bocl.sk1_contract_no sk1_contract_no          ,
        bocl.contract_no contract_no                  ,
        bocl.sk1_item_no sk1_item_no                  ,
        bocl.item_no item_no                  ,
  --      bocl.sk2_item_no sk2_item_no                  ,
        bocl.fin_year_no fin_year_no                  ,
        bocl.fin_week_no fin_week_no                  ,
        bocl.fin_week_code fin_week_code              ,
        bocl.contract_status_code contract_status_code,
        g_date last_updated_date                      ,
        --
        nvl(extc.contract_qty,0) contract_qty         ,
        nvl(extc.contract_selling,0) contract_selling ,
        nvl(extc.contract_cost,0) contract_cost       ,
        --
        nvl(extp.amended_po_qty,0) po_qty             ,
        nvl(extp.amended_po_selling,0) po_selling     ,
        nvl(extp.amended_po_cost,0) po_cost           ,
        --
        nvl(extp.bc_shipment_qty,0) bc_shipment_qty        ,
        nvl(extp.bc_shipment_selling,0) bc_shipment_selling,
        nvl(extp.bc_shipment_cost,0) bc_shipment_cost      ,
        --
        nvl(extp.po_grn_qty,0) po_grn_qty                   ,
        nvl(extp.po_grn_selling,0) po_grn_selling           ,
        nvl(extp.po_grn_cost,0) po_grn_cost      ,
        --
        nvl(extp.latest_po_qty,0) latest_po_qty                   ,
        nvl(extp.latest_po_selling,0) latest_po_selling           ,
        nvl(extp.latest_po_cost,0) latest_po_cost
 from
    ext_con extc       ,
    ext_posup extp     ,
    ext_stat bocl
  where bocl.sk1_contract_no      = extc.sk1_contract_no(+)
    and bocl.sk1_item_no          = extc.sk1_item_no(+)
 --   and bocl.sk2_item_no          = extc.sk2_item_no(+)
    and bocl.contract_status_code = extc.contract_status_code(+)
    and bocl.fin_year_no          = extc.fin_year_no(+)
    and bocl.fin_week_no          = extc.fin_week_no(+)
    and bocl.sk1_contract_no      = extp.sk1_contract_no(+)
    and bocl.sk1_item_no          = extp.sk1_item_no(+)
 --   and bocl.sk2_item_no          = extp.sk2_item_no(+)
    and bocl.fin_year_no          = extp.fin_year_no(+)
    and bocl.fin_week_no          = extp.fin_week_no(+)
    and bocl.contract_status_code = extp.contract_status_code(+)
 ;

-- Input record declared as cursor%rowtype
g_rec_in                   c_boc%rowtype;
-- Input bulk collect table declared
type stg_array is table of c_boc%rowtype;
a_stg_input                stg_array;

-- Output record declared as cursor%rowtype
g_rec_out    dwh_performance.temp_fnd_boc_contract_wk%rowtype;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of temp_fnd_boc_contract_wk%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_empty_set_i       tbl_array_i;
a_count             integer       := 0;
a_count_i           integer       := 0;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

   g_rec_out.sk1_contract_no                := g_rec_in.sk1_contract_no;
   g_rec_out.contract_no                    := g_rec_in.contract_no;
   g_rec_out.sk1_item_no                    := g_rec_in.sk1_item_no;
   g_rec_out.item_no                        := g_rec_in.item_no;
 --  g_rec_out.sk2_item_no                    := g_rec_in.sk2_item_no;
   g_rec_out.fin_year_no                    := g_rec_in.fin_year_no;
   g_rec_out.fin_week_no                    := g_rec_in.fin_week_no;
   g_rec_out.fin_week_code                  := g_rec_in.fin_week_code;
   g_rec_out.contract_status_code           := g_rec_in.contract_status_code;
   g_rec_out.last_updated_date              := g_rec_in.last_updated_date;

   g_rec_out.contract_qty                   := g_rec_in.contract_qty;
   g_rec_out.contract_selling               := g_rec_in.contract_selling;
   g_rec_out.contract_cost                  := g_rec_in.contract_cost;

   g_rec_out.po_qty                         := g_rec_in.po_qty;
   g_rec_out.po_selling                     := g_rec_in.po_selling;
   g_rec_out.po_cost                        := g_rec_in.po_cost;

   g_rec_out.po_grn_qty                     := g_rec_in.po_grn_qty;
   g_rec_out.po_grn_selling                 := g_rec_in.po_grn_selling;
   g_rec_out.po_grn_cost                    := g_rec_in.po_grn_cost;

   g_rec_out.latest_po_qty                  := g_rec_in.latest_po_qty;
   g_rec_out.latest_po_selling              := g_rec_in.latest_po_selling;
   g_rec_out.latest_po_cost                 := g_rec_in.latest_po_cost;

   g_rec_out.bc_shipment_qty                := g_rec_in.bc_shipment_qty;
   g_rec_out.bc_shipment_selling            := g_rec_in.bc_shipment_selling;
   g_rec_out.bc_shipment_cost               := g_rec_in.bc_shipment_cost;

   case g_rec_in.contract_status_code
    when 'C' then
      g_rec_out.boc_qty     :=0;
      g_rec_out.boc_selling :=0;
      g_rec_out.boc_cost    :=0;
    else
        g_rec_out.boc_qty     :=g_rec_in.contract_qty     - g_rec_in.latest_po_qty;
        g_rec_out.boc_selling :=g_rec_in.contract_selling - g_rec_in.latest_po_selling;
        g_rec_out.boc_cost    :=g_rec_in.contract_cost    - g_rec_in.latest_po_cost;
    end case;

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
      insert into  dwh_performance.temp_fnd_boc_contract_wk values a_tbl_insert(i);
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
                       ' '||a_tbl_insert(g_error_index).fin_year_no||
                       ' '||a_tbl_insert(g_error_index).fin_week_no;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_insert;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin

   g_found := TRUE;
   a_count_i               := a_count_i + 1;
   a_tbl_insert(a_count_i) := g_rec_out;

   a_count := a_count + 1;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
   if a_count > g_forall_limit then
      local_bulk_insert;
      a_tbl_insert  := a_empty_set_i;
      a_count_i     := 0;
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

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
  g_calc_sysdate := trunc(sysdate - g_calc_sysdate_days);
  p_success      := false;
  l_text         := dwh_constants.vc_log_draw_line;
  dwh_log.write_log ( l_name,l_system_name,l_script_name,l_procedure_name,l_text ) ;
  l_text := 'LOAD OF temp_fnd_boc_contract_wk EX fnd_rtl_contract AND rtl_po_supchain_LOC_ITEM_DY  STARTED '|| to_char ( sysdate,('dd mon yyyy hh24:mi:ss') ) ;
  dwh_log.write_log ( l_name,l_system_name,l_script_name,l_procedure_name,l_text ) ;
  dwh_log.insert_log_summary ( l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','' ) ;
  --**************************************************************************************************
  -- LOOK UP BATCH DATE FROM DIM_CONTROL
  -- DWH_LOG.WRITE_LOG   : g_calc_sysdate
  --**************************************************************************************************
  dwh_lookup.dim_control ( g_date ) ;
  l_text := 'Batch date selected is :- '||g_date;
  dwh_log.write_log ( l_name,l_system_name,l_script_name,l_procedure_name,l_text ) ;
  g_calc_sysdate := trunc(g_date - g_calc_sysdate_days);
  l_text         := 'Batch date being processed is gt :- '||g_calc_sysdate;
  dwh_log.write_log ( l_name,l_system_name,l_script_name,l_procedure_name,l_text ) ;
  --**************************************************************************************************
  -- TRUNCATE TEMP_FND_BOC_CONTRACT_LIST
  -- DWH_LOG.WRITE_LOG   : temp_fnd_boc_contract_list truncated
  --**************************************************************************************************
  execute immediate('truncate table  temp_fnd_boc_contract_wk');
  commit;
  l_text :='TRUNCATED temp_fnd_boc_contract_wk AT '||to_char ( sysdate,('dd mon yyyy hh24:mi:ss') ) ;
  dwh_log.write_log ( l_name,l_system_name,l_script_name,l_procedure_name,l_text ) ;

--**************************************************************************************************
     open c_boc;
     fetch c_boc bulk collect into a_stg_input limit g_forall_limit;
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
     fetch c_boc bulk collect into a_stg_input limit g_forall_limit;
     end loop;
    close c_boc;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
       local_bulk_insert;
--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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
end wh_prf_corp_641u_T22;
