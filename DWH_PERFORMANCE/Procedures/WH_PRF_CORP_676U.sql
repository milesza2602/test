--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_676U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_676U" 
                                                                                                                             (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2009
--  Author:      M Munnik
--  Purpose:     Rollup Sales Sparse to Promotions fact table for promotions that have been approved.
--               CHBD only.
--  Tables:      Input  - rtl_loc_item_dy_rms_sparse
--               Output - RTL_PROM_LOC_SC_DY
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
g_sk1_prom_period_1  RTL_PROM_LOC_SC_DY.sk1_prom_period_no%type;
g_sk1_prom_period_2  RTL_PROM_LOC_SC_DY.sk1_prom_period_no%type;
g_rec_out            RTL_PROM_LOC_SC_DY%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_676U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLLS SALES SPARSE TO PROM/LOC/SC/DY';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

---- For output arrays into bulk load forall statements --
--type tbl_array_i is table of RTL_PROM_LOC_SC_DY%rowtype index by binary_integer;
--type tbl_array_u is table of RTL_PROM_LOC_SC_DY%rowtype index by binary_integer;
--a_tbl_insert        tbl_array_i;
--a_tbl_update        tbl_array_u;
--a_empty_set_i       tbl_array_i;
--a_empty_set_u       tbl_array_u;
--
--a_count             integer       := 0;
--a_count_i           integer       := 0;
--a_count_u           integer       := 0;


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
    --g_date := g_date -1;
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


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
FOR G_SUB IN 0..3
  LOOP
   L_TEXT := 'Outer loop through program = '||TO_CHAR(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||G_SUB;
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      
   IF G_SUB = 0 THEN
      G_START_DATE := G_DATE - 41;
      G_END_DATE   := G_DATE - 31;
   END IF;   
   
   IF G_SUB = 1 THEN
      G_START_DATE := G_DATE - 30;
      G_END_DATE   := G_DATE - 21;
   END IF;
   
   IF G_SUB = 2 THEN
      G_START_DATE := G_DATE - 20;
      G_END_DATE   := G_DATE - 11;
   END IF;
   
   IF G_SUB = 3 THEN
      G_START_DATE := G_DATE - 10;
      G_END_DATE   := G_DATE ;
   END IF;
   
    L_TEXT := 'DATA PROCESSED FROM '||G_START_DATE||' TO '||G_END_DATE;
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);

   g_upd    := 1;
   select   /*+ parallel(spr,6) */ count(*)
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
   and      spr.last_updated_date         = g_date ;

   if g_upd = 0 then
      L_TEXT := 'No data to process in this range'||G_START_DATE||' to '||G_END_DATE;
      DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
      continue;
   end if;

   --  MERGE --
   L_TEXT := 'STARTING THE MERGE';
   DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
   
   merge /*+ parallel(prom,6) */ into RTL_PROM_LOC_SC_DY prom using 
   (
     select   /*+ parallel(sp,6) */
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
  
     group by sp.post_date, ia.sk1_prom_no, sp.sk1_location_no, di.sk1_style_colour_no
   
   ) mer_mart
   
   on (prom.post_date             = mer_mart.post_date
   and prom.sk1_prom_no           = mer_mart.sk1_prom_no
   and prom.sk1_location_no       = mer_mart.sk1_location_no
   and prom.sk1_style_colour_no   = mer_mart.sk1_style_colour_no)
   
  when matched then 
    update 
      set sk1_prom_period_no              = mer_mart.sk1_prom_period_no,
          sk2_location_no                 = mer_mart.sk2_location_no,
          prom_sales_qty                  = mer_mart.prom_sales_qty,
          prom_sales                      = mer_mart.prom_sales,
          prom_sales_cost                 = mer_mart.prom_sales_cost,
          ho_prom_discount_qty            = mer_mart.ho_prom_discount_qty,
          ho_prom_discount_amt            = mer_mart.ho_prom_discount_amt,
          st_prom_discount_qty            = mer_mart.st_prom_discount_qty,
          st_prom_discount_amt            = mer_mart.st_prom_discount_amt,
          last_updated_date               = g_date
          
  when not matched then
    insert
     ( prom.sk1_prom_no,
       prom.sk1_location_no,
       prom.sk1_style_colour_no,
       prom.post_date,
       prom.sk1_prom_period_no,
       prom.sk2_location_no,
       prom.prom_sales_qty,
       prom.prom_sales,
       prom.prom_sales_cost ,
       prom.ho_prom_discount_qty,
       prom.ho_prom_discount_amt,
       prom.st_prom_discount_qty,
       prom.st_prom_discount_amt,
       prom.last_updated_date
      )
    values 
      (mer_mart.sk1_prom_no,
       mer_mart.sk1_location_no,
       mer_mart.sk1_style_colour_no,
       mer_mart.post_date,
       mer_mart.sk1_prom_period_no,
       mer_mart.sk2_location_no,
       mer_mart.prom_sales_qty,
       mer_mart.prom_sales,
       mer_mart.prom_sales_cost ,
       mer_mart.ho_prom_discount_qty,
       mer_mart.ho_prom_discount_amt,
       mer_mart.st_prom_discount_qty,
       mer_mart.st_prom_discount_amt,
       g_date
       );
 
      g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;
      g_recs_updated :=  g_recs_updated + SQL%ROWCOUNT;
      g_recs_read :=  g_recs_read + SQL%ROWCOUNT;

 
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

end WH_PRF_CORP_676U;
