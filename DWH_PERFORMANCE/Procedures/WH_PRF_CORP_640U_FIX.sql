--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_640U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_640U_FIX" 
  (
    p_forall_limit in integer,
    p_success out boolean,
    p_start_date in date,
    p_end_date in date
    )
as
  --**************************************************************************************************
  --  DATE:        OCT 2008
  --  AUTHOR:      WENDY LYTTLE
  --  PURPOSE:     BALANCE OF CONTRACTS : create a temporary list of contracts that have had
  --                                      contract and/or shipment and/or purchase_order
  --                                      information changes during the last 3-days.
  --                                    : the temporary list will drive the recalculation of measures
  --                                      for these contracts,
  --  TABLES:      INPUT  -    DWH_FOUNDATION.fnd_rtl_contract
  --                           DWH_PERFORMANCE.rtl_po_supchain_loc_item_dy
  --               OUTPUT -    DWH_PERFORMANCE.temp_fnd_boc_contract_list
  --                           (contains list of contracts
  --                                     that have contract/po_supchain info changed)
  --                           (informix equivalent - formally rtl_boc_contract_list
  --                                         with field added=contract_status_code)
  --  PACKAGES:    constants, dwh_log, dwh_valid
  --  COMMENTS:    single dml could be considered for this program.
  -- PRD
  --  $$$$$$$$$$$$
  -- $$$$$$$$$$$$$$$$$$$  W A R N I N G  $$$$$$$$$$
  -- $$$$$$$$$$$$
  --  NB> Hardcoding in Procedure - start and end date are hardcoded
  -- $$$$$$$$$$$$
  --
  --  MAINTENANCE:
  --  06 March 2009 - defect 1020 -Program WH_PRF_CORP_640U aborted
  --                             populating DWH_PERFORMANCE.temp_fnd_boc_contract_list
  --
  --  31 March 2009 - defect 1309 - Add start_date and end_date as parameters
  --                                to wh_prf_corp_640u
  --
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
  --  25 mAY 2010 - DEFECT 3805 - remove sk2_item_no processing
  --
  --
  --  NAMING CONVENTIONS:
  --  G_  -  GLOBAL VARIABLE
  --  L_  -  LOG TABLE VARIABLE
  --  A_  -  ARRAY VARIABLE
  --  V_  -  LOCAL VARIABLE AS FOUND IN PACKAGES
  --  P_  -  PARAMETER
  --  C_  -  PREFIX TO CURSOR
  --
  --  PROCESSING FLOW
  --   1. Clear DWH_PERFORMANCE.temp_fnd_boc_contract_list
  --   2. Select All contracts(at distinct contract_no level)
  --          in DWH_FOUNDATION.fnd_rtl_contract/DWH_PERFORMANCE.rtl_po_supchain_loc_item_dy
  --           where last_updated_date falls in period
  --      Select from DWH_FOUNDATION.fnd_rtl_contract contracts listed in Selection
  --                which have in DWH_PERFORMANCE.temp_fnd_boc_contract_list
  --**************************************************************************************************
g_recs_read          integer := 0;
g_recs_inserted      integer := 0;
g_recs_updated       integer := 0;
g_recs_count         integer := 0;
g_forall_limit       integer := dwh_constants.vc_forall_limit;
g_error_count        number  := 0;
g_error_index        number  := 0;
g_rec_out            DWH_PERFORMANCE.temp_fnd_boc_contract_list%rowtype;
g_found              boolean;
g_count              number       := 0;
g_date               date          := trunc(sysdate);
g_start_date         date          := trunc(sysdate);
g_end_date           date          := trunc(sysdate);
g_minus_days         number       := 3;
prev_con number := 0;
prev_item number := 0;
prev_stat varchar2(1);
prev_srce varchar2(1);
min_date date;

l_message sys_dwh_errlog.log_text%type;
l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_640U_FIX';
l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_md;
l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
l_text sys_dwh_log.log_text%type ;
l_description sys_dwh_log_summary.log_description%type   := 'BOC - CREATE LIST OF CHANGED CONTRACTS STARTED AT';
l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_PERFORMANCE.temp_fnd_boc_contract_list%rowtype index by binary_integer;
type tbl_array_u is table of DWH_PERFORMANCE.temp_fnd_boc_contract_list%rowtype index by binary_integer;

a_tbl_insert tbl_array_i;
a_tbl_update tbl_array_u;
a_empty_set_i tbl_array_i;
a_empty_set_u tbl_array_u;
a_count   integer := 0;
a_count_i integer := 0;
a_count_u integer := 0;
  --**************************************************************************************************
  --
  -- PROCEDURE : LOCAL_INSERT_CON : inserts all changed contracts  to output table
  --
  --------------
-- select a list of all contracts at contract_no, item_no, ready_date, contract_status_code level
--     where contract_no is in the list of extracted contracts
--               (ie. all contracts, PO_supchain
--               (where the contract_no is not null
--               (and last updated_date > parameter_date (default is sysdate - 2 days))
--     and contract_status_code is 'A'pproved or 'C'ompleted
--      but also only those where contract_no and item_no are > 0
---
--- select all contracts, POs and shipments where the contract_no is not null
---           and last updated_date > parameter_date (default is sysdate - 2 days)
---
---------------
 --**************************************************************************************************
cursor c_boc is
 select
         rc.contract_no contract_no                   ,
         dcr.sk1_contract_no sk1_contract_no          ,
         rc.item_no item_no                           ,
         di.sk1_item_no sk1_item_no                   ,
         rc.ready_date  tran_date                     ,
         dc.fin_year_no fin_year_no                   ,
         dc.fin_week_no fin_week_no                   ,
         dc.fin_week_code fin_week_code               ,
         (CASE WHEN (rc.contract_status_code IS NULL)
                    THEN 'A'
          ELSE rc.CONTRACT_STATUS_CODE
          END) contract_status_code                   ,
       g_date last_updated_date,
          rc.source_data_status_code
    from fnd_rtl_contract rc
    inner join dim_contract dcr
          on(dcr.contract_no = rc.contract_no)
    inner join dim_item di
          on(di.item_no = rc.item_no)
  inner join dim_calendar dc
          on(dc.calendar_date = rc.ready_date)
--    where rc.last_updated_date >= g_start_date
    where rc.last_updated_date >= '29 jun 15' 
 --   where rc.last_updated_date between '29 jun 15' and '09 aug 15'
--  and rc.last_updated_date <= g_end_date
      and rc.contract_no > 0
      and rc.item_no > 0
      and rc.contract_status_code  in('A','C')
      and (rc.source_data_status_code <> 'D'
       or rc.source_data_status_code is null)
      and (rc.chain_code <> 'DJ' or rc.chain_code is null)
 --      and rc.contract_no in(252722	,252744	,253034	,253028	,242026,228740,209022)
 --      and dcr.sk1_contract_no in(5762793,5766989,5823185,3468720,5392165)
    group by
         rc.contract_no                  ,
         dcr.sk1_contract_no           ,
         rc.item_no                          ,
         di.sk1_item_no                   ,
          rc.ready_date                   ,
         dc.fin_year_no                 ,
         dc.fin_week_no                   ,
         dc.fin_week_code                ,
         (CASE WHEN (rc.contract_status_code IS NULL)
                    THEN 'A'
          ELSE rc.CONTRACT_STATUS_CODE
          END)                  ,
         g_date ,
          rc.source_data_status_code;
          -- Input record declared as cursor%rowtype
g_rec_in c_boc%rowtype;
-- Input bulk collect table declared
type stg_array is table of c_boc%rowtype;
a_stg_input stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variable as
begin

  g_rec_out.contract_no             := g_rec_in.contract_no;
  g_rec_out.item_no                 := g_rec_in.item_no;
  g_rec_out.tran_date               := g_rec_in.tran_date;
  g_rec_out.contract_status_code    := g_rec_in.contract_status_code;
  g_rec_out.source_data_status_code := g_rec_in.source_data_status_code;
  g_rec_out.last_updated_date       := g_date;
  g_rec_out.sk1_item_no             := g_rec_in.sk1_item_no;
--  g_rec_out.sk2_item_no             := g_rec_in.sk2_item_no;
  g_rec_out.sk1_contract_no         := g_rec_in.sk1_contract_no;
  g_rec_out.fin_year_no             := g_rec_in.fin_year_no;
  g_rec_out.fin_week_no             := g_rec_in.fin_week_no;
  g_rec_out.fin_week_code           := g_rec_in.fin_week_code;

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
   insert into DWH_PERFORMANCE.temp_fnd_boc_contract_list values a_tbl_insert(i);

  g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

  exception
    when others then
      g_error_count := sql%bulk_exceptions.count;
      l_message     := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
      dwh_log.record_error(l_module_name,sqlcode,l_message);

  for i in 1 .. g_error_count
  loop
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm
                 (-sql%bulk_exceptions(i).error_code)|| ' '||
                 a_tbl_insert(g_error_index).sk1_contract_no|| ' '||
                 a_tbl_insert(g_error_index).sk1_item_no|| ' '||
                 a_tbl_insert(g_error_index).tran_date;

    dwh_log.record_error(l_module_name,sqlcode,l_message);
  end loop;
  raise;

end local_bulk_insert;

--**************************************************************************************************
-- Write any purchase_orders to temp table
--          where matching on contract_no and item_no
--   the tran_date(not_before_date) does not exist.
--
--**************************************************************************************************
procedure local_insert_from_po as
begin
  g_recs_count :=0;
  for v_cur    in
      ( WITH selpo AS
                  (SELECT a.sk1_contract_no,    a.sk1_item_no,    a.tran_date
                     FROM DWH_PERFORMANCE.rtl_po_supchain_loc_item_dy a,    DWH_PERFORMANCE.temp_fnd_boc_contract_list b
                    WHERE po_ind          = 1
                      AND a.sk1_contract_no = b.sk1_contract_no
                      AND a.sk1_item_no         = b.sk1_item_no
          --            AND B.contract_no in(252722	,252744	,253034	,253028	,242026,228740,209022	)
                   GROUP BY a.sk1_contract_no,    a.sk1_item_no,    a.tran_date  ),
              selcon AS
                  (SELECT sk1_contract_no,    sk1_item_no, tran_date
                     FROM DWH_PERFORMANCE.temp_fnd_boc_contract_list
                 --    WHERE contract_no in(252722,	252744	,253034	,253028	,242026,228740,209022	)
                     ),
              selstat AS
                  (SELECT sk1_contract_no,    sk1_item_no,
                  --sk2_item_no,
                  item_no, contract_no, MAX(contract_status_code) contract_status_code
                     FROM DWH_PERFORMANCE.temp_fnd_boc_contract_list
                 --    WHERE contract_no in(252722	,252744	,253034	,253028	,242026,228740,209022	)
                   group by sk1_contract_no, sk1_item_no, item_no, contract_no )
        SELECT  a.sk1_contract_no sk1_contract_no ,
              a.tran_date tran_date ,
              a.sk1_item_no sk1_item_no ,
     --        c.sk2_item_no sk2_item_no ,
              dcp.fin_year_no fin_year_no ,
              dcp.fin_week_no fin_week_no ,
              dcp.fin_week_code fin_week_code ,
              c.item_no item_no,
              c.contract_no contract_no,
              c.contract_status_code contract_status_code,
              b.sk1_contract_no con_no
        FROM selpo a
        LEFT OUTER JOIN selcon b
          ON (b.sk1_contract_no = a.sk1_contract_no
          AND b.sk1_item_no     = a.sk1_item_no
          AND b.tran_date      = a.tran_date)
        LEFT OUTER JOIN selstat c
          ON (c.sk1_contract_no = a.sk1_contract_no
          AND c.sk1_item_no     = a.sk1_item_no)
        LEFT OUTER JOIN DWH_PERFORMANCE.DIM_calendar dcp
          ON (dcp.calendar_date = a.tran_date)
        GROUP BY a.sk1_contract_no  ,
                a.tran_date  ,
                a.sk1_item_no  ,
    --            c.sk2_item_no  ,
                dcp.fin_year_no  ,
                dcp.fin_week_no  ,
                dcp.fin_week_code  ,
                c.item_no  ,
                c.contract_no  ,
                c.contract_status_code,
                b.sk1_contract_no
      HAVING b.sk1_contract_no IS NULL
  )
  loop
     insert
       into DWH_PERFORMANCE.temp_fnd_boc_contract_list values
     ( v_cur.contract_no          ,
        v_cur.sk1_contract_no          ,
        v_cur.item_no              ,
        v_cur.sk1_item_no              ,
  --      v_cur.sk2_item_no              ,
        v_cur.tran_date           ,
        v_cur.fin_year_no           ,
        v_cur.fin_week_no           ,
        v_cur.fin_week_code           ,
        v_cur.contract_status_code ,
        g_date,
        null
        );
    g_recs_count := g_recs_count + to_number(to_char(sql%rowcount));
    commit;
  end loop;
  commit;
  l_text := 'INSERTED FROM fnd_rtl_purchase_order :- '||g_recs_count||'       AT :-'||to_char
  (sysdate,('dd mon yyyy hh24:mi:ss') );
  dwh_log.write_log
  ( l_name,l_system_name,l_script_name,l_procedure_name,l_text );
  commit;

exception
when dwh_errors.e_insert_error then
  l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm||' local_insert_po' ;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  raise;
when others then
  l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm||' local_insert_po' ;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  raise;
end local_insert_from_po;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as
begin
  g_found := true;
  a_count_i               := a_count_i + 1;
  a_tbl_insert(a_count_i) := g_rec_out;
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
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'BOC - CREATE LIST OF CHANGED CONTRACTS STARTED AT '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- LOOK UP BATCH DATE FROM DIM_CONTROL
  -- DWH_LOG.WRITE_LOG   : g_start_date
  --**************************************************************************************************
    if p_start_date is null or p_end_date is null then
       dwh_lookup.dim_control(g_end_date);
       g_start_date := trunc(g_end_date - g_minus_days);
   else
       g_end_date := p_end_date;
       g_start_date := p_start_date;
   end if;
   l_text := 'Batch period selected is :- '||g_start_date||' to '||g_end_date;
   dwh_log.write_log  (l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--   if p_minus_days is not null and p_minus_days > dwh_constants.vc_boc_minus_days then
--    g_minus_days  := p_minus_days;
--  end if;
 --**************************************************************************************************
 -- hardcoded dates to do a full run
 --**************************************************************************************************
    g_start_date := '01 jan 01';
    g_end_date := '30 jun 30';
    l_text :='HARDCODED START AND END DATES :- '||g_start_date||' to '||g_end_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- TRUNCATE DWH_PERFORMANCE.temp_fnd_boc_contract_list
  -- DWH_LOG.WRITE_LOG   : DWH_PERFORMANCE.temp_fnd_boc_contract_list is truncated before running
  --**************************************************************************************************
   execute immediate
   ('truncate table DWH_PERFORMANCE.temp_fnd_boc_contract_list');
  commit;
  l_text :='TRUNCATED DWH_PERFORMANCE.temp_fnd_boc_contract_list AT '||sysdate;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
dbms_output.put_line('TRUNCATED DWH_PERFORMANCE.temp_fnd_boc_contract_list AT '||sysdate);
   if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
     g_forall_limit  := p_forall_limit;
   end if;
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'LOAD OF DWH_PERFORMANCE.temp_fnd_boc_contract_list EX fnd_contract and DWH_PERFORMANCE.rtl_po_supchain_loc_item_dy STARTED '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
dbms_output.put_line('BATCH DATE BEING PROCESSED - '||g_date);
--**************************************************************************************************
  open c_boc;
   fetch c_boc bulk collect
      into a_stg_input limit g_forall_limit;
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
     fetch c_boc bulk collect
        into a_stg_input limit g_forall_limit;
   end loop;
   close c_boc;
--**************************************************************************************************
-- At end write out what remains in the arrays
--**************************************************************************************************
   local_bulk_insert;
    local_insert_from_po;

--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
  l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
  l_text := dwh_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed||sysdate;
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

end wh_prf_corp_640u_fix;
