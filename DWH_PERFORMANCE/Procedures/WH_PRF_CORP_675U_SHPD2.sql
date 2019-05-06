--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_675U_SHPD2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_675U_SHPD2" 
                                
(p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  ROLLUP FOR shpd DATAFIX - wENDY - 13 SEP 2016
--**************************************************************************************************
--  Date:        March 2009
--  Author:      M Munnik
--  Purpose:     Rollup Sales Dense to Promotions fact table for promotions that have been approved.
--               CHBD only.
--  Tables:      Input  - rtl_loc_item_dy_rms_dense
--               Output - rtl_prom_loc_sc_dy
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  20 dec 2011 Wendy - Change loop from 3 to 9 times thereby inccreasing no. of reads but reducing
--                      amount of records inserted/updated.
-- 
--  wendy lyttle 5 july 2012 removed to allow thru -and      pl.prom_no <>  313801
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
g_recs_deleted       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_sub                integer       :=  0;
g_rec_out            rtl_loc_item_wk_rms_dense%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_start_date         date          ;
g_end_date           date          ;
g_yesterday          date          := trunc(sysdate) - 1;
g_fin_day_no         dim_calendar.fin_day_no%type;
g_partition_name       varchar2(2000) ;
g_fin_year_no        number        :=  0;
g_fin_month_no        number        :=  0;
g_fin_week_no        number        :=  0;   
g_sql_trunc_partition  varchar2(2000) ;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_675U_SHPD2';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS DENSE PERFORMANCE to WEEK';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --

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

    l_text := 'ROLLUP OF rtl_loc_item_wk_rms_dense EX DAY LEVEL STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
 --   mindate=10/DEC/13 - to 12 sep 2016 = 145, - 6 = 139 hence 138
    G_DATE := g_date - 35;
--    G_DATE := g_date - 800;
    l_text := 'Derived ----->>>>BATCH DATE BEING PROCESSED  - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'alter session enable parallel dml';

          select min(this_week_start_date) , max(this_week_end_date)
          into   g_start_date, g_end_date
          from   dim_calendar
          where  calendar_date between  g_date - 7 and g_date;

          l_text := 'g_start='||g_start_date||' - '||g_end_date;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--for g_sub in 0..139 loop
for g_sub in 0..69 loop
--for g_sub in 0..2 loop
          g_start_date := g_start_date - 14;
          g_end_date := g_end_date - 14;

         MERGE  INTO rtl_prom_loc_sc_dy MER_RTL
        USING
        (
                select   /*+ parallel(dn,4) full(pl) full(ia) */
                             ia.sk1_prom_no, dn.sk1_location_no, di.sk1_style_colour_no, dn.post_date,
            max( (case when dn.post_date between dp.prom_start_date and dp.prom_end_date then 9749661
                                  else 9749660 end)) sk1_prom_period_no,
            max(dn.sk2_location_no) sk2_location_no,
            sum(dn.sales_qty) sales_qty, sum(dn.sales) sales, sum(dn.sales_cost) sales_cost,
            sum(dn.reg_sales_qty) reg_sales_qty, sum(dn.reg_sales) reg_sales, sum(dn.reg_sales_cost) reg_sales_cost,
            sum(dn.actl_store_rcpt_qty) actl_store_rcpt_qty, sum(dn.actl_store_rcpt_selling) actl_store_rcpt_selling,
            sum(dn.actl_store_rcpt_cost) actl_store_rcpt_cost
   from     rtl_loc_item_dy_rms_dense dn
   join     dim_item di                   on  dn.sk1_item_no         = di.sk1_item_no
   join     dim_location dl               on  dn.sk1_location_no     = dl.sk1_location_no
   join     rtl_prom_item_all ia          on  di.sk1_item_no         = ia.sk1_item_no
   join     dim_prom dp                   on  ia.sk1_prom_no         = dp.sk1_prom_no
   join     fnd_prom_location pl          on  dp.prom_no             = pl.prom_no
                                          and dl.location_no         = pl.location_no
   where    dn.post_date                  between g_start_date and G_END_DATE
   and      dn.post_date                  between dp.approval_date and dp.prom_end_date
   And      Di.Business_Unit_No           <>  50
   -- removed to allow thru
-- 5 july 2012
--      and      pl.prom_no <> 313801
   group by dn.post_date, ia.sk1_prom_no, dn.sk1_location_no, di.sk1_style_colour_no
        ) SEL_RTL
        ON
        (             SEL_RTL.post_date                       = MER_RTL.post_date
               and    SEL_RTL.sk1_location_no                 = MER_RTL.sk1_location_no
               and    SEL_RTL.sk1_style_colour_no             = MER_RTL.sk1_style_colour_no
               and    SEL_RTL.sk1_prom_no                     = MER_RTL.sk1_prom_no
        )
        WHEN MATCHED
        THEN
        UPDATE
        SET        
                      sk2_location_no                 = SEL_RTL.sk2_location_no,
                      sk1_prom_period_no              = SEL_RTL.sk1_prom_period_no,
                      sales_qty                       = SEL_RTL.sales_qty,
                      sales                           = SEL_RTL.sales,
                      sales_cost                      = SEL_RTL.sales_cost,
                      reg_sales_qty                   = SEL_RTL.reg_sales_qty,
                      reg_sales                       = SEL_RTL.reg_sales,
                      reg_sales_cost                  = SEL_RTL.reg_sales_cost,
                      actl_store_rcpt_qty             = SEL_RTL.actl_store_rcpt_qty,
                      actl_store_rcpt_selling         = SEL_RTL.actl_store_rcpt_selling,
                      actl_store_rcpt_cost            = SEL_RTL.actl_store_rcpt_cost,
                      last_updated_date               = G_DATE
        
        WHEN NOT MATCHED
        THEN
        INSERT
        (SK1_PROM_NO
          ,SK1_LOCATION_NO
          ,SK1_STYLE_COLOUR_NO
          ,POST_DATE
          ,SK1_PROM_PERIOD_NO
          ,SK2_LOCATION_NO
          ,SOH_QTY
          ,SOH_SELLING
          ,SOH_COST
          ,REG_SOH_QTY
          ,REG_SOH
          ,REG_SOH_COST
          ,REG_SOH_MARGIN
          ,CLEAR_SOH_QTY
          ,CLEAR_SOH_SELLING
          ,CLEAR_SOH_COST
          ,CLEAR_SOH_MARGIN
          ,SALES_QTY
          ,SALES
          ,SALES_COST
          ,REG_SALES_QTY
          ,REG_SALES
          ,REG_SALES_COST
          ,PROM_SALES_QTY
          ,PROM_SALES
          ,PROM_SALES_COST
          ,INBOUND_INCL_CUST_ORD_QTY
          ,INBOUND_INCL_CUST_ORD_SELLING
          ,INBOUND_INCL_CUST_ORD_COST
          ,ACTL_STORE_RCPT_QTY
          ,ACTL_STORE_RCPT_SELLING
          ,ACTL_STORE_RCPT_COST
          ,HO_PROM_DISCOUNT_QTY
          ,HO_PROM_DISCOUNT_AMT
          ,ST_PROM_DISCOUNT_QTY
          ,ST_PROM_DISCOUNT_AMT
          ,LAST_UPDATED_DATE
        )
        VALUES
        (
                 SEL_RTL.SK1_PROM_NO
                , SEL_RTL.SK1_LOCATION_NO
                , SEL_RTL.SK1_STYLE_COLOUR_NO
                , SEL_RTL.POST_DATE
                , SEL_RTL.SK1_PROM_PERIOD_NO
                , SEL_RTL.SK2_LOCATION_NO
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , SEL_RTL.SALES_QTY
                , SEL_RTL.SALES
                , SEL_RTL.SALES_COST
                , SEL_RTL.REG_SALES_QTY
                , SEL_RTL.REG_SALES
                , SEL_RTL.REG_SALES_COST
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , SEL_RTL.ACTL_STORE_RCPT_QTY
                , SEL_RTL.ACTL_STORE_RCPT_SELLING
                , SEL_RTL.ACTL_STORE_RCPT_COST
                , NULL 
                , NULL 
                , NULL 
                , NULL 
                , G_DATE 
        );           
        g_recs_read := 0;
        g_recs_inserted :=  0;    
        g_recs_read := g_recs_read + SQL%ROWCOUNT;
        g_recs_inserted :=  g_recs_inserted + SQL%ROWCOUNT;

    commit;

          l_text := 'Period='||g_start_date||' - '||g_end_date||' Recs MERGED = '||g_recs_inserted;
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
 end loop;   
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
    l_text :=  dwh_constants.vc_log_records_deleted||g_recs_deleted;
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

end wh_prf_corp_675U_SHPD2;
