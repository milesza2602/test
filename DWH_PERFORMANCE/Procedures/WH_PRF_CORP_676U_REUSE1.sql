--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_676U_REUSE1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_676U_REUSE1" 
                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2009
--  Author:      M Munnik
--  Purpose:     Rollup Sales Sparse to Promotions fact table for promotions that have been approved.
--               CHBD only.
--  Tables:      Input  - rtl_loc_item_dy_rms_sparse
--               Output - rtl_prom_loc_sc_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
-- 
--  wendy lyttle 5 july 2012 removed to allow thru -and      pl.prom_no <>  313801xx
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
g_recs_inserted      integer       :=  0;
g_recs_updated       integer       :=  0;
G_SUB                INTEGER       :=  0;
G_UPD                INTEGER       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_date               date;
g_start_date         date;
g_end_date           date;
g_sk1_prom_period_1  rtl_prom_loc_sc_dy.sk1_prom_period_no%type;
g_sk1_prom_period_2  rtl_prom_loc_sc_dy.sk1_prom_period_no%type;
g_rec_out            rtl_prom_loc_sc_dy%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_676U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLS SALES SPARSE TO PROM/LOC/SC/DY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_prom_loc_sc_dy%rowtype index by binary_integer;
type tbl_array_u is table of rtl_prom_loc_sc_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
--full(ia) parallel(sp,8)
cursor c_prom_loc_sc_dy is
   select   /*+ no_index(sp) */
            ia.sk1_prom_no, sp.sk1_location_no, di.sk1_style_colour_no, sp.post_date,
            max(case when sp.post_date between dp.prom_start_date and dp.prom_end_date then g_sk1_prom_period_2
                     else g_sk1_prom_period_1 end) sk1_prom_period_no,
            max(sp.sk2_location_no) sk2_location_no,
            sum(sp.prom_sales_qty) prom_sales_qty, sum(sp.prom_sales) prom_sales, sum(sp.prom_sales_cost) prom_sales_cost,
            sum(sp.ho_prom_discount_qty) ho_prom_discount_qty, sum(sp.ho_prom_discount_amt) ho_prom_discount_amt,
            sum(sp.st_prom_discount_qty) st_prom_discount_qty, sum(sp.st_prom_discount_amt) st_prom_discount_amt
   from     rtl_loc_item_dy_rms_sparse sp
   join     dim_item di                   on  sp.sk1_item_no         = di.sk1_item_no
   join     dim_location dl               on  sp.sk1_location_no     = dl.sk1_location_no
   join     rtl_prom_item_all ia          on  di.sk1_item_no         = ia.sk1_item_no
   join     dim_prom dp                   on  ia.sk1_prom_no         = dp.sk1_prom_no
   join     fnd_prom_location pl          on  dp.prom_no             = pl.prom_no
                                          and dl.location_no         = pl.location_no
   where    sp.post_date                  between g_start_date and G_END_DATE
   and      sp.post_date                  between dp.approval_date and dp.prom_end_date
   And      Di.Business_Unit_No           <>  50
   group by sp.post_date, ia.sk1_prom_no, sp.sk1_location_no, di.sk1_style_colour_no;

g_rec_in             c_prom_loc_sc_dy%rowtype;
-- For input bulk collect --
type stg_array is table of c_prom_loc_sc_dy%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out          			                     := null;
   g_rec_out.sk1_prom_no                         := g_rec_in.sk1_prom_no;
   g_rec_out.sk1_location_no                     := g_rec_in.sk1_location_no;
   g_rec_out.sk1_style_colour_no                 := g_rec_in.sk1_style_colour_no;
   g_rec_out.post_date                           := g_rec_in.post_date;
   g_rec_out.sk1_prom_period_no                  := g_rec_in.sk1_prom_period_no;
   g_rec_out.sk2_location_no                     := g_rec_in.sk2_location_no;
   g_rec_out.prom_sales_qty                      := g_rec_in.prom_sales_qty;
   g_rec_out.prom_sales                          := g_rec_in.prom_sales;
   g_rec_out.prom_sales_cost                     := g_rec_in.prom_sales_cost;
   g_rec_out.ho_prom_discount_qty                := g_rec_in.ho_prom_discount_qty;
   g_rec_out.ho_prom_discount_amt                := g_rec_in.ho_prom_discount_amt;
   g_rec_out.st_prom_discount_qty                := g_rec_in.st_prom_discount_qty;
   g_rec_out.st_prom_discount_amt                := g_rec_in.st_prom_discount_amt;
   g_rec_out.last_updated_date                   := g_date;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_prom_loc_sc_dy values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_prom_no||
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_insert(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update rtl_prom_loc_sc_dy
       set    sk1_prom_period_no              = a_tbl_update(i).sk1_prom_period_no,
              sk2_location_no                 = a_tbl_update(i).sk2_location_no,
              prom_sales_qty                  = a_tbl_update(i).prom_sales_qty,
              prom_sales                      = a_tbl_update(i).prom_sales,
              prom_sales_cost                 = a_tbl_update(i).prom_sales_cost,
              ho_prom_discount_qty            = a_tbl_update(i).ho_prom_discount_qty,
              ho_prom_discount_amt            = a_tbl_update(i).ho_prom_discount_amt,
              st_prom_discount_qty            = a_tbl_update(i).st_prom_discount_qty,
              st_prom_discount_amt            = a_tbl_update(i).st_prom_discount_amt,
              last_updated_date               = a_tbl_update(i).last_updated_date
       where  post_date                       = a_tbl_update(i).post_date
       and    sk1_prom_no                     = a_tbl_update(i).sk1_prom_no
       and    sk1_location_no                 = a_tbl_update(i).sk1_location_no
       and    sk1_style_colour_no             = a_tbl_update(i).sk1_style_colour_no
              and
            (
            nvl(prom_sales_qty	        ,0) <>	a_tbl_update(i).prom_sales_qty or
            nvl(prom_sales              ,0) <>	a_tbl_update(i).prom_sales  or
            nvl(ho_prom_discount_qty    ,0) <>	a_tbl_update(i).ho_prom_discount_qty	or
            nvl(ho_prom_discount_amt	  ,0) <>	a_tbl_update(i).ho_prom_discount_amt	or
            nvl(st_prom_discount_qty    ,0) <>	a_tbl_update(i).st_prom_discount_qty 	or
            nvl(st_prom_discount_amt    ,0) <>	a_tbl_update(i).st_prom_discount_amt	or
            nvl(sk1_prom_period_no      ,0) <>	a_tbl_update(i).sk1_prom_period_no	 
            );

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
                       ' '||a_tbl_update(g_error_index).sk1_prom_no||
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_style_colour_no||
                       ' '||a_tbl_update(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates to output table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_prom_loc_sc_dy
   where  post_date             = g_rec_out.post_date
   and    sk1_prom_no           = g_rec_out.sk1_prom_no
   and    sk1_location_no       = g_rec_out.sk1_location_no
   and    sk1_style_colour_no   = g_rec_out.sk1_style_colour_no;

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

   a_count                    := a_count + 1;

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
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD RTL_PROM_LOC_SC_DY from SALES SPARSE STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    g_start_date := g_date - 41;

--    l_text := 'DATA PROCESSED FROM '||g_start_date||' TO '||g_date;
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Once-off retrieval of data to improve performance
--**************************************************************************************************
    select sk1_prom_period_no
    into   g_sk1_prom_period_1
    from   dim_prom_period
    where  prom_period_no = '1';

    select sk1_prom_period_no
    into   g_sk1_prom_period_2
    from   dim_prom_period
    where  prom_period_no = '2';

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
G_START_DATE := '12 Aug 2016';
G_END_DATE   := '13 Aug 2016';

FOR G_SUB IN 1..30
  LOOP
   L_TEXT := 'Outer loop through program = '||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||G_SUB;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
     
      G_START_DATE := G_START_DATE + 2;
      G_END_DATE   := G_END_DATE + 2;
  
    L_TEXT := 'DATA PROCESSED FROM '||G_START_DATE||' TO '||G_END_DATE;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

/*   
   select   count(*)
   into     g_upd
   from     rtl_loc_item_dy_rms_sparse spr
   join     dim_item di                   on  spr.sk1_item_no        = di.sk1_item_no
   join     dim_location dl               on  spr.sk1_location_no    = dl.sk1_location_no
   join     rtl_prom_item_all ia          on  di.sk1_item_no         = ia.sk1_item_no
   join     dim_prom dp                   on  ia.sk1_prom_no         = dp.sk1_prom_no
   join     fnd_prom_location pl          on  dp.prom_no             = pl.prom_no
                                          and dl.location_no         = pl.location_no
   where    spr.post_date                 between g_start_date and G_END_DATE
   and      spr.post_date                 between dp.approval_date and dp.prom_end_date
   And      Di.Business_Unit_No           <>  50
      -- removed to allow thru
-- 5 july 201
 --     and      pl.prom_no <> 313801 
   and      spr.last_updated_date         = g_date ;

   if g_upd = 0 then
      L_TEXT := 'No data to process in this range'||G_START_DATE||' to '||G_END_DATE;
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      continue;
   end if;

*/
 
    open c_prom_loc_sc_dy;
    fetch c_prom_loc_sc_dy bulk collect into a_stg_input limit g_forall_limit;
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

         g_rec_in                := null;
         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_prom_loc_sc_dy bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_prom_loc_sc_dy;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

      local_bulk_insert;
      local_bulk_update;
      A_TBL_INSERT  := A_EMPTY_SET_I;
      A_TBL_UPDATE  := A_EMPTY_SET_U;
      A_COUNT_I     := 0;
      A_COUNT_U     := 0;
      A_COUNT       := 0;
      COMMIT;
END LOOP;
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

end wh_prf_corp_676u_reuse1;
