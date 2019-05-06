--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_253A
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_253A" (p_forall_limit in integer, p_success out boolean) as 

--**************************************************************************************************
--  Date:        August 2013
--  Author:      Quentin Smit
--  Purpose:     Foods Renewal datacheck comparison extract 1.3 (file 6 BRS document)
--               FOR ALL DEPARTMENTS FOR CHRISTMAS PERIOD TRADE COMPARISONS
--  Tables:      Input  -   rtl_loc_item_dy_catalog, rtl_loc_item_dy_rms_dense
--               Output -   FOODS_RENEWAL_EXTRACT1_3 FOR CURRENT WEEK TO DATE
--  Packages:    constants, dwh_log, dwh_valid
--  
--  Maintenance:
--  
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
g_error_index        number        :=  0;
g_rec_out            W6005682.FOODS_RENEWAL_EXTRACT1_3_ALL%rowtype;
g_count              number        :=  0;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_253A';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'FOODS RENEWAL DATACHECK COMPARISON EXTRACT 1.3';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of W6005682.FOODS_RENEWAL_EXTRACT1_3_ALL%rowtype index by binary_integer;
type tbl_array_u is table of W6005682.FOODS_RENEWAL_EXTRACT1_3_ALL%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;
a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;
l_today_date        date := trunc(sysdate) - 1;
l_today_no          integer;
l_less_days         integer;
l_end_date          date;
l_start_date        date;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin 

    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    l_text := 'LOAD OF FOODS_RENEWAL_EXTRACT1_3 started AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

----------------------------------------------------------------------------------------------------
    l_text := 'Truncate table begin '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'))  ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    EXECUTE IMMEDIATE('truncate table W6005682.FOODS_RENEWAL_EXTRACT1_3_ALL');
    l_text := 'Truncate Mart table completed '||to_char(sysdate,('dd mon yyyy hh24:mi:ss')) ;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

----------------------------------------------------------------------------------------------------
    
    execute immediate 'alter session enable parallel dml';
-- ######################################################################################### --
-- The outer joins are needed as there are cases when there are no sales in dense for items  --
-- which must be included in order to show a zero sales index as these records will be       --
-- created when the outer joins to either dense LY or the item price records are found       --
-- ######################################################################################### --

  l_text := 'Today Date being processed B4 lookup: ' || l_today_date ;
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 

 select today_date, today_fin_day_no
   into l_today_date, l_today_no
   from dim_control;
     
   
 l_text := 'Today Date being processed AFTER lookup: ' || l_today_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
 l_text := 'Today day no : ' || l_today_no ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);     
 
 if l_today_no = 1 then l_less_days := 42; end if;
 if l_today_no = 2 then l_less_days := 43; end if;
 if l_today_no = 3 then l_less_days := 44; end if;
 if l_today_no = 4 then l_less_days := 45; end if;
 if l_today_no = 5 then l_less_days := 46; end if;
 if l_today_no = 6 then l_less_days := 48; end if;
 if l_today_no = 7 then l_less_days := 49; end if;
     
 select calendar_date
   into l_start_date
   from dim_calendar
  where calendar_date = trunc(sysdate) - l_less_days;

 l_text := 'Start date : ' || l_start_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);       
 l_text := 'End date : ' || l_today_date ;
 dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);       
  
-- l_today_date := 'Moo';
 
INSERT /*+ APPEND PARALLEL (mart,4) */ INTO W6005682.FOODS_RENEWAL_EXTRACT1_3_ALL mart
WITH item_list AS
  (  select di.item_no, di.sk1_item_no, di.item_desc, di.sk1_supplier_no, dd.department_no, dd.department_name, di.subclass_no, di.subclass_name 
            From Dim_item Di, Dim_department Dd
               where di.department_no in (13,41,44,47,55,58,68,73,75,81,85,88,96)   --(41,58,73,81,88,96)   --,44,45,47,50,55,58,72,73,75,76,77,81,87,88,90,93,95,96,97,98,99)
                 and di.department_no = dd.department_no
  ) ,  --select * from item_list;
  
  loc_list AS
  (SELECT location_no,
    location_name    ,
    sk1_location_no  ,
    wh_fd_zone_no    ,
    SK1_FD_ZONE_GROUP_ZONE_NO,
    region_no
     FROM dim_location
     where chain_no = 10
  ) ,  --   select * from loc_list;
  
  supp_list AS
  (SELECT sk1_supplier_no, supplier_no, supplier_name FROM dim_supplier
  ),  -- select * from supp_list;
  
  
  ee AS
  (SELECT /*+ PARALLEL(c,4) FULL(c) */
    d.fin_year_no                                              ,
    d.fin_week_no                                                    ,
    d.fin_day_no                                                     ,
    b.wh_fd_zone_no                                                  ,
    c.tran_date                                                ,
    a.item_no,        
    b.location_no                                                    ,
    e.po_no,
    nvl(c.fillrate_fd_po_grn_qty,0)        AS fill_po_grn_units         ,
    nvl(c.fillrate_fd_latest_po_qty,0)      AS fill_latest_po_units     ,
    case when fillrate_fd_latest_po_qty > 0 then 
        (nvl(c.fillrate_fd_po_grn_qty,0)/nvl(c.fillrate_fd_latest_po_qty,0) ) * 100
    else
        0
    end as supplier_orderfil_perc
  FROM item_list a       ,
    loc_list b              ,
    rtl_po_supchain_loc_item_dy c,
    dim_calendar d,          --,
    dim_purchase_order e
 WHERE c.tran_date between  l_start_date and l_today_date  --  = '29/JUL/13' --and '21/JUL/13'
   AND c.tran_date     = d.calendar_date
   AND a.sk1_item_no             = c.sk1_item_no
   AND b.sk1_location_no         = c.sk1_location_no
   and c.sk1_po_no               = e.sk1_po_no
  ),  
  
  xx AS (
   SELECT 
    e.fin_year_no fin_year_no,
    e.fin_week_no fin_week_no,
    e.fin_day_no fin_day_no,
    e.wh_fd_zone_no wh_fd_zone_no,
    e.item_no item_no,
    e.location_no location_no,    
    e.po_no,
    NVL(e.fill_po_grn_units,0)              AS fill_po_grn_units ,
    nvl(e.fill_latest_po_units,0)           AS fill_latest_po_units,
    nvl(e.supplier_orderfil_perc,0)         AS supplier_orderfil_perc
    
  FROM ee e

  )
  SELECT xx.fin_year_no                  ,        
        xx.fin_week_no                  ,
        xx.fin_day_no                   ,
        loc_list.region_no,
        xx.wh_fd_zone_no as dc_region   ,
        xx.item_no                      ,
        item_list.item_desc             ,
        xx.location_no                  ,
        loc_list.location_name,
        item_list.department_no         ,
        item_list.department_name       ,
        item_list.subclass_no           ,
        item_list.subclass_name         ,
        xx.po_no,
        xx.fill_po_grn_units,
        xx.fill_latest_po_units,
        xx.supplier_orderfil_perc
        
   FROM xx ,
        item_list,
        loc_list
  WHERE xx.item_no            = item_list.item_no
    and xx.location_no        = loc_list.location_no
    
    ORDER BY xx.item_no
;
  g_recs_read     := g_recs_read + SQL%ROWCOUNT;
  g_recs_inserted := g_recs_inserted + SQL%ROWCOUNT;

commit;

--**************************************************************************************************
-- Write final log data
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
       
end wh_prf_corp_253A;
