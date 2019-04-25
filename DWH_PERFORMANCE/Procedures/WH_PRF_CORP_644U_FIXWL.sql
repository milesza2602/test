--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_644U_FIXWL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_644U_FIXWL" (p_forall_limit in integer,p_success out boolean) as
  --**************************************************************************************************
  --  Date:        Jan 2009
  --  Author:      Wendy Lyttle
  --  Purpose:     Create CScWk BOC rollup fact table in the performance layer
  --               with input ex CIWk BOC table from performance layer.
  --  Tables:      Input  - rtl_contract_chain_item_wk_boc
  --               Output - rtl_contract_chain_sc_wk_boc
  --  Packages:    constants, dwh_log, dwh_valid
  --
  --  Maintenance:
  --  1 April 2009 - defect 1231 - no filter on driving cursor
  -- 1 April 2009 - defect 1281 - Two fields BOC_MARGIN  and  CONTRACT_MARGIN
  --                               are not populated in table
  --                               RTL_CONTRACT_CHAIN_SC_WK_BOC
    -- 4 May 2009 - defect 1532 - this_week_start_date is not being updated
  --                           on rtl_contract_chain_sc_wk
  --
  --   27 July 2009 - defect 2147 - FND-PRF Procedures that over-write SUMS
  --                                with new values, rather than re-SUMMING for
  --                                the primary key
  --  24 Feb 2010 - defect2592 - Incorrect BOC_QTY_ALL_TIME value on
  --                              RTL_CONTRACT_CHAIN_SC_WK_BOC
  --
  --  Naming conventions
  --  g_  -  Global variable
  --  l_  -  Log table variable
  --  a_  -  Array variable
  --  v_  -  Local variable as found in packages
  --  p_  -  Parameter
  --  c_  -  Prefix to cursor
  --**************************************************************************************************
  g_forall_limit  integer := dwh_constants.vc_forall_limit;
  g_recs_read     integer := 0;
  g_recs_inserted integer := 0;
  g_recs_updated  integer := 0;
  g_error_count   number  := 0;
  g_error_index   number  := 0;
  g_count         number  := 0;
  g_rec_out rtl_contract_chain_sc_wk_boc%rowtype;
  g_found boolean;
  g_date date := trunc(sysdate);
  g_start_date date ;
  g_yesterday date := trunc(sysdate) - 1;
  l_message sys_dwh_errlog.log_text%type;
  l_module_name sys_dwh_errlog.log_procedure_name%type := 'WH_PRF_CORP_644U_FIXWL';
  l_name sys_dwh_log.log_name%type                     := dwh_constants.vc_log_name_rtl_apps;
  l_system_name sys_dwh_log.log_system_name%type       := dwh_constants.vc_log_system_name_rtl_prf;
  l_script_name sys_dwh_log.log_script_name%type       := dwh_constants.vc_log_script_rtl_prf_apps;
  l_procedure_name sys_dwh_log.log_procedure_name%type := l_module_name;
  l_text sys_dwh_log.log_text%type ;
  l_description sys_dwh_log_summary.log_description%type   := 'ROLL UP THE BOC ITEMS to STYLE-COLOUR';
  l_process_type sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
  -- For output arrays into bulk load forall statements --
type tbl_array_i
is
  table of rtl_contract_chain_sc_wk_boc%rowtype index by binary_integer;
type tbl_array_u
is
  table of rtl_contract_chain_sc_wk_boc%rowtype index by binary_integer;
  a_tbl_insert tbl_array_i;
  a_tbl_update tbl_array_u;
  a_empty_set_i tbl_array_i;
  a_empty_set_u tbl_array_u;
  a_count   integer := 0;
  a_count_i integer := 0;
  a_count_u integer := 0;
CURSOR c_rtl_contr_chain_item_wk_boc
IS
WITH chgrecs AS
  (SELECT rcc.sk1_contract_no ,
    rcc.sk1_chain_no ,
    dit.sk1_style_colour_no ,
    fin_year_no ,
    fin_week_no,
    sel_boca.boc_qty_all_time,
    sel_boca.boc_selling_all_time,
    sel_boca.boc_cost_all_time
  FROM rtl_contract_chain_item_wk_boc rcc,
    dim_item dit,
    (SELECT sk1_contract_no ,
      sk1_chain_no ,
      sk1_style_colour_no ,
      SUM(boc_qty_all_time) boc_qty_all_time,
      SUM(boc_selling_all_time) boc_selling_all_time,
      SUM(boc_cost_all_time) boc_cost_all_time
    FROM
      (SELECT sk1_contract_no ,
        sk1_chain_no ,
        sk1_style_colour_no ,
        ALTM.SK1_ITEM_NO ,
        boc_qty_all_time ,
        boc_selling_all_time,
        boc_cost_all_time
      FROM rtl_contract_chain_item_wk_boc altm,
        dim_item dit
      WHERE dit.sk1_item_no = altm.sk1_item_no
      GROUP BY sk1_contract_no ,
        sk1_chain_no ,
        sk1_style_colour_no ,
        ALTM.SK1_ITEM_NO ,
        boc_qty_all_time ,
        boc_selling_all_time,
        boc_cost_all_time
      )
  GROUP BY sk1_contract_no,
    sk1_chain_no,
    sk1_style_colour_no
    ) sel_boca
  WHERE
  rcc.last_updated_date      = g_date
  AND
  dit.sk1_item_no              = rcc.sk1_item_no
  AND sel_boca.sk1_contract_no     = rcc.sk1_contract_no
  AND sel_boca.sk1_style_colour_no = dit.sk1_style_colour_no
  GROUP BY rcc.sk1_contract_no ,
    rcc.sk1_chain_no ,
    dit.sk1_style_colour_no ,
    fin_year_no ,
    fin_week_no,
    sel_boca.boc_qty_all_time,
    sel_boca.boc_selling_all_time,
    sel_boca.boc_cost_all_time
  )
SELECT ciw.sk1_contract_no ,
  ciw.sk1_chain_no ,
  di.sk1_style_colour_no ,
  ciw.fin_year_no ,
  ciw.fin_week_no ,
  ciw.fin_week_code ,
  ciw.this_week_start_date ,
  MAX(ciw.contract_status_code)       AS contract_status_code,
  SUM(NVL(ciw.contract_qty,0))        AS contract_qty ,
  SUM(NVL(ciw.contract_selling,0))    AS contract_selling ,
  SUM(NVL(ciw.contract_cost,0))       AS contract_cost ,
  SUM(NVL(ciw.amended_po_qty,0))      AS amended_po_qty ,
  SUM(NVL(ciw.amended_po_selling,0))  AS amended_po_selling ,
  SUM(NVL(ciw.amended_po_cost,0))     AS amended_po_cost ,
  SUM(NVL(ciw.bc_shipment_qty,0))     AS bc_shipment_qty ,
  SUM(NVL(ciw.bc_shipment_selling,0)) AS bc_shipment_selling ,
  SUM(NVL(ciw.bc_shipment_cost,0))    AS bc_shipment_cost ,
  SUM(NVL(ciw.grn_qty,0))             AS grn_qty ,
  SUM(NVL(ciw.grn_selling,0))         AS grn_selling ,
  SUM(NVL(ciw.grn_cost,0))            AS grn_cost ,
  SUM(NVL(ciw.latest_po_qty,0))       AS latest_po_qty ,
  SUM(NVL(ciw.latest_po_selling,0))   AS latest_po_selling ,
  SUM(NVL(ciw.latest_po_cost,0))      AS latest_po_cost ,
  SUM(NVL(ciw.boc_qty,0))             AS boc_qty ,
  SUM(NVL(ciw.boc_selling,0))         AS boc_selling ,
  SUM(NVL(ciw.boc_cost,0))            AS boc_cost ,
  NVL(cr.boc_qty_all_time,0)          AS boc_qty_all_time ,
  NVL(cr.boc_selling_all_time,0)      AS boc_selling_all_time,
  NVL(cr.boc_cost_all_time,0)         AS boc_cost_all_time,
  SUM(NVL(ciw.boc_margin,0))          AS boc_margin,
  SUM(NVL(ciw.contract_margin,0))     AS contract_margin
FROM chgrecs cr,
  rtl_contract_chain_item_wk_boc ciw,
  dim_item di
WHERE ciw.sk1_item_no      = di.sk1_item_no
AND cr.sk1_contract_no     = ciw.sk1_contract_no
AND cr.sk1_chain_no        = ciw.sk1_chain_no
AND cr.sk1_style_colour_no = di.sk1_style_colour_no
AND cr.fin_year_no         = ciw.fin_year_no
AND cr.fin_week_no         = ciw.fin_week_no
GROUP BY ciw.sk1_contract_no,
  ciw.sk1_chain_no,
  di.sk1_style_colour_no,
  ciw.fin_year_no,
  ciw.fin_week_no,
  ciw.fin_week_code,
  ciw.this_week_start_date,
  NVL(cr.boc_qty_all_time,0),
  NVL(cr.boc_selling_all_time,0),
  NVL(cr.boc_cost_all_time,0) ;
--   where  last_updated_date >= g_yesterday;

  g_rec_in c_rtl_contr_chain_item_wk_boc%rowtype;
  -- For input bulk collect --
type stg_array
is
  table of c_rtl_contr_chain_item_wk_boc%rowtype;
  a_stg_input stg_array;
  --**************************************************************************************************
  -- Process, transform and validate the data read from the input interface
  --**************************************************************************************************
procedure local_address_variables
as
begin
  g_rec_out.sk1_contract_no      := g_rec_in.sk1_contract_no;
  g_rec_out.sk1_chain_no         := g_rec_in.sk1_chain_no;
  g_rec_out.sk1_style_colour_no  := g_rec_in.sk1_style_colour_no;
  g_rec_out.fin_year_no          := g_rec_in.fin_year_no;
  g_rec_out.fin_week_no          := g_rec_in.fin_week_no;
  g_rec_out.fin_week_code        := g_rec_in.fin_week_code;
  g_rec_out.this_week_start_date := g_rec_in.this_week_start_date;
  g_rec_out.contract_status_code := g_rec_in.contract_status_code;
  g_rec_out.contract_qty         := g_rec_in.contract_qty;
  g_rec_out.contract_selling     := g_rec_in.contract_selling;
  g_rec_out.contract_cost        := g_rec_in.contract_cost;
  g_rec_out.amended_po_qty       := g_rec_in.amended_po_qty;
  g_rec_out.amended_po_selling   := g_rec_in.amended_po_selling;
  g_rec_out.amended_po_cost      := g_rec_in.amended_po_cost;
  g_rec_out.bc_shipment_qty      := g_rec_in.bc_shipment_qty;
  g_rec_out.bc_shipment_selling  := g_rec_in.bc_shipment_selling;
  g_rec_out.bc_shipment_cost     := g_rec_in.bc_shipment_cost;
  g_rec_out.grn_qty              := g_rec_in.grn_qty;
  g_rec_out.grn_selling          := g_rec_in.grn_selling;
  g_rec_out.grn_cost             := g_rec_in.grn_cost;
  g_rec_out.latest_po_qty        := g_rec_in.latest_po_qty;
  g_rec_out.latest_po_selling    := g_rec_in.latest_po_selling;
  g_rec_out.latest_po_cost       := g_rec_in.latest_po_cost;
  g_rec_out.boc_qty              := g_rec_in.boc_qty;
  g_rec_out.boc_selling          := g_rec_in.boc_selling;
  g_rec_out.boc_cost             := g_rec_in.boc_cost;
  g_rec_out.boc_qty_all_time     := g_rec_in.boc_qty_all_time;
  g_rec_out.boc_selling_all_time := g_rec_in.boc_selling_all_time;
  g_rec_out.boc_cost_all_time    := g_rec_in.boc_cost_all_time;
  g_rec_out.boc_margin           := g_rec_in.boc_margin;
  g_rec_out.contract_margin      := g_rec_in.contract_margin;
  g_rec_out.last_updated_date    := g_date;
exception
when others then
  l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  raise;
end local_address_variables;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert
as
begin
  forall i in a_tbl_insert.first .. a_tbl_insert.last
  save exceptions
   insert into rtl_contract_chain_sc_wk_boc values a_tbl_insert
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
    .sk1_chain_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .sk1_style_colour_no|| ' '||a_tbl_insert
    (
      g_error_index
    )
    .fin_week_no;
    dwh_log.record_error
    (
      l_module_name,sqlcode,l_message
    )
    ;
  end loop;
  raise;
end local_bulk_insert;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update
as
begin
  forall i in a_tbl_update.first .. a_tbl_update.last
  save exceptions
   update rtl_contract_chain_sc_wk_boc
  set 
/*    fin_week_code        = a_tbl_update(i).fin_week_code,
    contract_status_code   = a_tbl_update(i).contract_status_code,
    contract_qty           = a_tbl_update(i).contract_qty        , */
    contract_selling       = a_tbl_update(i).contract_selling    ,
--    contract_cost          = a_tbl_update(i).contract_cost       ,
--    amended_po_qty         = a_tbl_update(i).amended_po_qty      ,
    amended_po_selling     = a_tbl_update(i).amended_po_selling  ,
--    amended_po_cost        = a_tbl_update(i).amended_po_cost     ,
--    bc_shipment_qty        = a_tbl_update(i).bc_shipment_qty     ,
    bc_shipment_selling    = a_tbl_update(i).bc_shipment_selling ,
--    bc_shipment_cost       = a_tbl_update(i).bc_shipment_cost    ,
--    grn_qty                = a_tbl_update(i).grn_qty             ,
    grn_selling            = a_tbl_update(i).grn_selling         ,
--    grn_cost               = a_tbl_update(i).grn_cost            ,
--    latest_po_qty          = a_tbl_update(i).latest_po_qty       ,
    latest_po_selling      = a_tbl_update(i).latest_po_selling   ,
--    latest_po_cost         = a_tbl_update(i).latest_po_cost      ,
--    boc_qty                = a_tbl_update(i).boc_qty             ,
    boc_selling            = a_tbl_update(i).boc_selling         ,
--    boc_cost               = a_tbl_update(i).boc_cost            ,
--    boc_qty_all_time       = a_tbl_update(i).boc_qty_all_time    ,
--    boc_selling_all_time   = a_tbl_update(i).boc_selling_all_time,
--    boc_cost_all_time      = a_tbl_update(i).boc_cost_all_time   ,
    boc_margin             = a_tbl_update(i).boc_margin   ,
    contract_margin        = a_tbl_update(i).contract_margin   
--    last_updated_date      = a_tbl_update(i).last_updated_date
    where sk1_contract_no  = a_tbl_update(i).sk1_contract_no
  and sk1_chain_no         = a_tbl_update(i).sk1_chain_no
  and sk1_style_colour_no  = a_tbl_update(i).sk1_style_colour_no
  and fin_year_no          = a_tbl_update(i).fin_year_no
  and fin_week_no          = a_tbl_update(i).fin_week_no;

  g_recs_updated := g_recs_updated + a_tbl_update.count;
exception
when others then
  g_error_count := sql%bulk_exceptions.count;
  l_message     := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
  dwh_log.record_error(l_module_name,sqlcode,l_message);
  for i in 1 .. g_error_count
  loop
    g_error_index := sql%bulk_exceptions(i).error_index;
    l_message     := dwh_constants.vc_err_lb_loop||i|| ' '||g_error_index|| ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)|| ' '||a_tbl_update(g_error_index).sk1_chain_no|| ' '||a_tbl_update(g_error_index).sk1_style_colour_no|| ' '||a_tbl_update(g_error_index).fin_week_no;
    dwh_log.record_error(l_module_name,sqlcode,l_message);
  end loop;
  raise;
end local_bulk_update;
--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output
as
begin
  g_found := false;
  -- Check to see if item is present on table and update/insert accordingly
   select count(1)
     into g_count
     from rtl_contract_chain_sc_wk_boc
    where sk1_contract_no  = g_rec_out.sk1_contract_no
  and sk1_chain_no         = g_rec_out.sk1_chain_no
  and sk1_style_colour_no  = g_rec_out.sk1_style_colour_no
  and fin_year_no          = g_rec_out.fin_year_no
  and fin_week_no          = g_rec_out.fin_week_no;
  if g_count               = 1 then
    g_found               := true;
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
--    local_bulk_insert;
    local_bulk_update;
    a_tbl_insert := a_empty_set_i;
    a_tbl_update := a_empty_set_u;
    a_count_i    := 0;
    a_count_u    := 0;
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
-- Main process
--**************************************************************************************************
begin
  if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
    g_forall_limit  := p_forall_limit;
  end if;
  dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
  p_success := false;
  l_text    := dwh_constants.vc_log_draw_line;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := 'ROLLUP OF rtl_contract_chain_sc_wk_boc EX DAY LEVEL STARTED '|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description, l_process_type,dwh_constants.vc_log_started,'','','','','');
  --**************************************************************************************************
  -- Look up batch date from dim_control
  --**************************************************************************************************
  dwh_lookup.dim_control(g_date);
  l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
  
  G_DATE := G_DATE+100;
  
  
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   select this_week_start_date-35
     into g_start_date
     from dim_calendar
    where calendar_date = g_date;

  l_text := 'START DATE OF ROLLUP - '||g_start_date;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  --**************************************************************************************************
  -- Bulk fetch loop controlling main program execution
  --**************************************************************************************************
  open c_rtl_contr_chain_item_wk_boc;
  fetch c_rtl_contr_chain_item_wk_boc bulk collect
     into a_stg_input limit g_forall_limit;

  while a_stg_input.count > 0
  loop
    for i in 1 .. a_stg_input.count
    loop
      g_recs_read            := g_recs_read + 1;
      if g_recs_read mod 100000 = 0 then
        l_text               := dwh_constants.vc_log_records_processed|| to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      end if;
      g_rec_in := a_stg_input(i);
      local_address_variables;
      local_write_output;
    end loop;
    fetch c_rtl_contr_chain_item_wk_boc bulk collect
       into a_stg_input limit g_forall_limit;
  end loop;
  close c_rtl_contr_chain_item_wk_boc;
  --**************************************************************************************************
  -- At end write out what remains in the arrays at end of program
  --**************************************************************************************************
--  local_bulk_insert;
  local_bulk_update;
  commit;
  --**************************************************************************************************
  -- Write final log data
  --**************************************************************************************************
  dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,l_process_type,
                             dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
  l_text := dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_read||g_recs_read;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_inserted||g_recs_inserted;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_records_updated||g_recs_updated;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  l_text := dwh_constants.vc_log_run_completed ||sysdate;
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
end wh_prf_corp_644u_fixWL;
