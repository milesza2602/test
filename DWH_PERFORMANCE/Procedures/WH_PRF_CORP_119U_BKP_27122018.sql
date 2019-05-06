--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_119U_BKP_27122018
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_119U_BKP_27122018" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        FEB 2009
--  Author:      Alastair de Wet
--  Purpose:     Load Allocation fact table in performance layer
--               with input ex RMS Allocation table from foundation layer (CH Only).
--  Tables:      Input  - fnd_rtl_allocation
--               Output - rtl_loc_item_dy_rms_sparse
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  19 oct 2012 - qc4779 - Procedure Change: Align CH_ALLOC measure between reports and cubes
--                         remove filter 'nvl(aloc.alloc_qty,0)      <> 0 AND'
--
--  29 april 2015 wendy lyttle  DAVID JONES - do not load where  chain_code = 'DJ'
--  03 Mar 2018   VAT 2018 (increase) amendements as required                                       REF03Mar2018
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
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_ch_alloc_selling   rtl_loc_item_dy_rms_sparse.ch_alloc_selling%type;
g_ch_alloc_qty       rtl_loc_item_dy_rms_sparse.ch_alloc_qty%type;
g_rec_out            rtl_loc_item_dy_rms_sparse%rowtype;
--g_wac                rtl_loc_item_dy_rms_price.wac%type   := 0;
--g_reg_rsp_excl_vat   rtl_loc_item_dy_rms_price.reg_rsp_excl_vat%type   := 0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          := trunc(sysdate) - 92;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_119U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP RMS ALLOC DATA EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_loc_item_dy_rms_sparse%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_rms_sparse%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_allocation is
 with SELitm AS (
  select /*+ MATERIALIZE  FULL(DI)  */ 
          di.sk1_item_no,
          di.vat_rate_perc,
          item_no
   from  dim_item di
          where  di.business_unit_no        <> 50 
-- and sk1_item_no in (27481398,27481396)
          order by item_no
)

select /*+ MATERIALIZE  PARALLEL(ALOC,8) PARALLEL(LI,8) FULL(LI)  FULL(DIH) FULL(DL) FULL(DLH) */
          sum(nvl(aloc.alloc_qty,0)) alloc_qty,
          sum(nvl(aloc.apportion_qty,0)) apportion_qty,
          release_date,
          di.sk1_item_no,
          dl.sk1_location_no,

--          max(nvl(di.vat_rate_perc,0)) vat_rate_perc,
          max(case when li.tax_perc is null then
                (case when dl.vat_region_no = 1000 then
                    di.VAT_RATE_PERC
                 else
                    dl.default_tax_region_no_perc
                 end)
              else 
                li.tax_perc                                  
              end) as vat_rate_perc,

--          max(round(nvl(li.reg_rsp,0) * 100 / (100 + nvl(di.vat_rate_perc,0)),2)) reg_rsp_excl_vat,
--          max(case when li.tax_perc is null then                              -- REF03Mar2018
          max(case when vr.VAT_RATE_PERC is null then                           -- REF03Mar2018
                 case when dl.vat_region_no = 1000 then
                    round(NVL(LI.reg_rsp,0) * 100 / (100 + di.VAT_RATE_PERC),2) 
                 else
                    round(NVL(LI.reg_rsp,0) * 100 / (100 + dl.default_tax_region_no_perc),2)
                 end
              else 
--                round(NVL(LI.reg_rsp,0) * 100 / (100 + LI.tax_perc),2)        -- REF03Mar2018
                round(NVL(LI.reg_rsp,0) * 100 / (100 + vr.VAT_RATE_PERC),2)     -- REF03Mar2018
              end)  as reg_rsp_excl_vat,

          max(dlh.sk2_location_no) sk2_location_no,
          max(dih.sk2_item_no) sk2_item_no

   from   fnd_rtl_allocation aloc
          join SELitm di on aloc.item_no                              = di.item_no 
          join dim_item_hist dih on aloc.item_no               = dih.item_no  
          join dim_location dl on aloc.to_loc_no      = dl.location_no 
          join dim_location_hist dlh on aloc.to_loc_no         = dlh.location_no 
          left outer join rtl_location_item li on di.sk1_item_no      = li.sk1_item_no
                                              and dl.sk1_location_no  = li.sk1_location_no
          --left outer join fnd_vr vr on di.item_no   = vr.item_no
          --                                    and dl.vat_region_no    = vr.vat_region_no 
          left join fnd_item_vat_rate vr                                                                               -- REF03Mar2018
                                        on (vr.item_no = di.item_no and vr.vat_region_no = dl.VAT_REGION_NO and        -- REF03Mar2018
                                            aloc.RELEASE_DATE between active_from_date and active_to_date)             -- REF03Mar2018

   where aloc.release_date between dih.sk2_active_from_date and dih.sk2_active_to_date 

     and aloc.release_date between g_start_date  and g_date 
     and aloc.release_date IS NOT NULL 
  --   and (aloc.CHAIN_CODE <> 'DJ' or aloc.chain_code is null)
     --and vr.active_from_date       < g_date
--and dl.SK1_LOCATION_NO = 8982
--and aloc.release_date = '28 oct 2017'
group by release_date, di.sk1_item_no, dl.sk1_location_no;

g_rec_in             c_fnd_rtl_allocation%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_rtl_allocation%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.post_date                       := g_rec_in.release_date;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
--   g_rec_out.ch_apportion_qty                := g_rec_in.apportion_qty;
   g_rec_out.ch_alloc_qty                    := g_rec_in.alloc_qty;
   g_rec_out.ch_alloc_selling                := g_rec_in.alloc_qty * g_rec_in.reg_rsp_excl_vat;


--    l_text := 'sk1item='||g_rec_out.sk1_item_no||'sk1loc='||g_rec_out.sk1_location_no||'post_date='||g_rec_out.post_date||
--    'ch_alloc_selling='||g_rec_out.ch_alloc_selling||'alloc_qty='||g_rec_in.alloc_qty||'regxvt='||g_rec_in.reg_rsp_excl_vat;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   g_rec_out.last_updated_date               := g_date;


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
       insert into rtl_loc_item_dy_rms_sparse values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).post_date;
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
       update rtl_loc_item_dy_rms_sparse
       set    ch_alloc_selling                = a_tbl_update(i).ch_alloc_selling,
              ch_alloc_qty                    = a_tbl_update(i).ch_alloc_qty,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  sk1_location_no                 = a_tbl_update(i).sk1_location_no      and
              sk1_item_no                     = a_tbl_update(i).sk1_item_no    and
              post_date                       = a_tbl_update(i).post_date ;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).post_date;
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
   select count(1),max(ch_alloc_selling),max(ch_alloc_qty)
   into   g_count,g_ch_alloc_selling,g_ch_alloc_qty
   from   rtl_loc_item_dy_rms_sparse
   where  sk1_location_no     = g_rec_out.sk1_location_no      and
          sk1_item_no         = g_rec_out.sk1_item_no    and
          post_date           = g_rec_out.post_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

-- Place data into and array for later writing to table in bulk
-- NB **************** this will ensure updates are only done when input values are different **********************
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
      a_count := a_count + 1;
   else
      if NVL(g_ch_alloc_selling,0) <> g_rec_out.ch_alloc_selling or
         NVL(g_ch_alloc_qty,0)     <> g_rec_out.ch_alloc_qty then
           a_count_u               := a_count_u + 1;
           a_tbl_update(a_count_u) := g_rec_out;
           a_count := a_count + 1;
      end if;
   end if;

--   a_count := a_count + 1;
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
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD rtl_loc_item_dy_rms_sparse EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    Dwh_Lookup.Dim_Control(G_Date);
    -- begin test
--G_Date := '12 Feb 2018';
    -- end test
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'RANGE BEING PROCESSED - '||g_start_date||' through '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_allocation;
    fetch c_fnd_rtl_allocation bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 500000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_allocation bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_allocation;
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

end WH_PRF_CORP_119U_BKP_27122018;
