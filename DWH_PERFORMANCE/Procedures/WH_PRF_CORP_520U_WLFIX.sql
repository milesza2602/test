--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_520U_WLFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_520U_WLFIX" (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        Jan 2009
--  Author:      Alastair de Wet
--  Purpose:     Create LscWk stock rollup fact table in the performance layer
--               with input ex liw stock table from performance layer.
--  Tables:      Input  - rtl_loc_item_wk_rms_stock
--               Output - rtl_loc_sc_wk_rms_stock
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  08 May 2008 - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_recs_DELETED      integer       :=  0;
g_recs_updated       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_sc_wk_rms_stock%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_fin_year_no         integer;
g_fin_week_no    integer;
g_start_week         integer;
g_start_year         integer;
g_start_date date;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_520U_WLFIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_roll;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_roll;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'ROLL UP THE RMS STOCK PERFORMANCE to SC/Week';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure delete_tab as
begin

    g_recs_read := 0;
    g_recs_DELETED := 0;
    
      DELETE FROM dwh_performance.rtl_loc_sc_wk_rms_stock A
      WHERE EXISTS (select DISTINCT B.sk1_style_colour_no
                              from dwh_datafix.wl_itemfix_style_colour B
                               where B.sk1_style_colour_no = A.sk1_style_colour_no)
      AND A.FIN_YEAR_NO = g_fin_year_no AND A.FIN_WEEK_NO = g_fin_week_no;

    g_recs_read := SQL%ROWCOUNT;
    g_recs_DELETED := SQL%ROWCOUNT;
    commit;


    l_text := 'DELETED RECS = '||G_RECS_DELETED;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
exception
         when no_data_found then

    l_text := 'DELETED RECS = 0';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end delete_tab;    
--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure insert_tab as
begin
    g_recs_read := 0;
    g_recs_DELETED := 0;
    g_recs_INSERTED := 0;

    
INSERT INTO  dwh_performance.rtl_loc_sc_wk_rms_stock rtl_lswr
with 
        selitem as (select DISTINCT di.SK1_ITEM_NO, di.sk1_style_colour_no, di.style_colour_no, di.sk1_style_no, di.style_no
                        from dwh_datafix.wl_itemfix_style_colour B, dim_item di
                        where b.sk1_item_no = di.sk1_item_no
                        
) 
select   liw.sk1_location_no as sk1_location_no,   
            di.sk1_style_colour_no,
            liw.fin_year_no as fin_year_no,
            liw.fin_week_no as fin_week_no,
            max(liw.fin_week_code) as fin_week_code,           
            max(liw.this_week_start_date) as this_week_start_date,
            max(liw.sk2_location_no) as sk2_location_no,
            sum(nvl(liw.NUM_COM_FLAG_IND,0)) as	NUM_COM_FLAG_IND,
            sum(nvl(liw.SIT_QTY,0)) as	SIT_QTY,
            sum(nvl(liw.SIT_CASES,0)) as	SIT_CASES,
            sum(nvl(liw.SIT_SELLING,0)) as	SIT_SELLING,
            sum(nvl(liw.SIT_COST,0)) as	SIT_COST,
            sum(nvl(liw.SIT_FR_COST,0)) as	SIT_FR_COST,
            sum(nvl(liw.SIT_MARGIN,0)) as	SIT_MARGIN,
            sum(nvl(liw.NON_SELLABLE_QTY,0)) as	NON_SELLABLE_QTY,
            sum(nvl(liw.SOH_QTY,0)) as	SOH_QTY,
            sum(nvl(liw.SOH_CASES,0)) as	SOH_CASES,
            sum(nvl(liw.SOH_SELLING,0)) as	SOH_SELLING,
            sum(nvl(liw.SOH_COST,0)) as	SOH_COST,
            sum(nvl(liw.SOH_FR_COST,0)) as	SOH_FR_COST,
            sum(nvl(liw.SOH_MARGIN,0)) as	SOH_MARGIN,
            sum(nvl(liw.FRANCHISE_SOH_MARGIN,0)) as	FRANCHISE_SOH_MARGIN,
            sum(nvl(liw.INBOUND_EXCL_CUST_ORD_QTY,0)) as	INBOUND_EXCL_CUST_ORD_QTY,
            sum(nvl(liw.INBOUND_EXCL_CUST_ORD_SELLING,0)) as	INBOUND_EXCL_CUST_ORD_SELLING,
            sum(nvl(liw.INBOUND_EXCL_CUST_ORD_COST,0)) as	INBOUND_EXCL_CUST_ORD_COST,
            sum(nvl(liw.INBOUND_INCL_CUST_ORD_QTY,0)) as	INBOUND_INCL_CUST_ORD_QTY,
            sum(nvl(liw.INBOUND_INCL_CUST_ORD_SELLING,0)) as	INBOUND_INCL_CUST_ORD_SELLING,
            sum(nvl(liw.INBOUND_INCL_CUST_ORD_COST,0)) as	INBOUND_INCL_CUST_ORD_COST,
            sum(nvl(liw.BOH_QTY,0)) as	BOH_QTY,
            sum(nvl(liw.BOH_CASES,0)) as	BOH_CASES,
            sum(nvl(liw.BOH_SELLING,0)) as	BOH_SELLING,
            sum(nvl(liw.BOH_COST,0)) as	BOH_COST,
            sum(nvl(liw.BOH_FR_COST,0)) as	BOH_FR_COST,
            sum(nvl(liw.CLEAR_SOH_QTY,0)) as	CLEAR_SOH_QTY,
            sum(nvl(liw.CLEAR_SOH_SELLING,0)) as	CLEAR_SOH_SELLING,
            sum(nvl(liw.CLEAR_SOH_COST,0)) as	CLEAR_SOH_COST,
            sum(nvl(liw.CLEAR_SOH_FR_COST,0)) as	CLEAR_SOH_FR_COST,
            sum(nvl(liw.REG_SOH_QTY,0)) as	REG_SOH_QTY,
            sum(nvl(liw.REG_SOH_SELLING,0)) as	REG_SOH_SELLING,
            sum(nvl(liw.REG_SOH_COST,0)) as	REG_SOH_COST,
            sum(nvl(liw.REG_SOH_FR_COST,0)) as	REG_SOH_FR_COST,
            max(g_date) as last_updated_date,
            sum(nvl(liw.CLEAR_SOH_MARGIN,0)) as	CLEAR_SOH_MARGIN,
            sum(nvl(liw.REG_SOH_MARGIN,0)) as	REG_SOH_MARGIN,
            null CH_OPENING_REG_STOCK_QTY, 
            null CH_OPENING_REG_STOCK_SELLING, 
            null AVAIL_REG_STOCK_QTY, 
            null AVAIL_REG_STOCK_SELLING, 
            null NUM_CH_OPTIONS_AVAIL_REG_STOCK
   from     rtl_loc_item_wk_rms_stock liw,
            SELITEM di
   where    liw.sk1_item_no          =  di.sk1_item_no
   and      liw.fin_year_no          =  g_fin_year_no
   and      liw.fin_week_no          =  g_fin_week_no
   group by liw.fin_year_no,
            liw.fin_week_no,
            di.sk1_style_colour_no,
            liw.sk1_location_no;


    g_recs_read :=  SQL%ROWCOUNT;
    g_recs_inserted := SQL%ROWCOUNT;
    commit; 
    l_text := 'INSERTED RECS = '||G_RECS_INSERTED;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
exception
         when no_data_found then
           l_text := 'INSERTED RECS = 0';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

end insert_tab;
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
    l_text := 'ROLLUP OF rtl_loc_sc_wk_rms_stock EX WEEK LEVEL STARTED '||
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

g_start_date := '5 sep 2016';

for g_sub in 0..10 loop
    select distinct fin_year_no, fin_week_no
    into   g_fin_year_no, g_fin_week_no
    from   dim_calendar
    where  calendar_date = g_start_date - (g_sub * 7);

    l_text := 'ROLLUP RANGE IS:- '||g_fin_year_no||'-'||g_fin_week_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   
     delete_tab;
          commit;

     l_text := 'Delete completed, Insert starting';
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
     insert_tab;
          commit;
     
end loop;
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

end wh_prf_corp_520u_WLFIX       ;
