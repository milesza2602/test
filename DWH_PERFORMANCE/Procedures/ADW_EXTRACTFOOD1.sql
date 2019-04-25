--------------------------------------------------------
--  DDL for Procedure ADW_EXTRACTFOOD1
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."ADW_EXTRACTFOOD1" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        November 2008
--  Author:      Alastair de Wet
--  Purpose:     Create GFK extract to flat file in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - vw_extr_gfk
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  28 July 2009 - qc1865 - ODWH Extract: WH_PRF_CORP_718E - vw_extr_gfk
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
g_count              number        :=  0;


g_date               date          := trunc(sysdate);
G_YESTERDAY          DATE          := TRUNC(SYSDATE) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'ADW_EXTRACTFOOD1';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


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

    l_text := 'EXTRACT  STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');


--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
    g_count := dwh_generic_file_extract(
'select lid.fin_year_no,lid.fin_week_no,dc.this_week_start_date,location_no,location_name,item_no,boh_qty
from rtl_loc_item_wk_rms_stock lid,
     dim_location dl,
     dim_item di,
     dim_calendar_wk dc
where lid.sk1_location_no = dl.sk1_location_no and
      lid.sk1_item_no     = di.sk1_item_no and
      lid.fin_year_no     = dc.fin_year_no and
      lid.fin_week_no     = dc.fin_week_no and
      dl.chain_no         = 10 and
      lid.boh_qty         > 0  and lid.boh_qty is not null and        
      di.business_unit_no  = 50 and
      lid.fin_year_no = 2014 and lid.fin_week_no between 10 and 13 ','|','DWH_FILES_OUT','extr_food_boh_2014_1013');    
    
    
 /*   
    g_count := dwh_generic_file_extract(
'SELECT LOCATION_NO,LOCATION_NAME,stk.FIN_WEEK_CODE,DLD.STYLE_COLOUR_NO,
      SOH_QTY,SOH_SELLING,SOH_COST 
from  RTL_LOC_SC_WK_RMS_STOCK STK, 
      DIM_LEV1_DIFF1 dld, 
      DIM_LOCATION DL 
WHERE STK.SK1_STYLE_COLOUR_NO = DLD.SK1_STYLE_COLOUR_NO AND
      stk.sk1_location_no     = dl.sk1_location_no and
      DLD.BUSINESS_UNIT_NO <> 50 AND
      STK.FIN_YEAR_NO = 2013 AND
      stk.FIN_WEEK_NO  between 1 and 35','|','DWH_FILES_OUT','stock_extract_2013');
*/      
--   g_count := dwh_generic_file_extract('Select /*+ parallel (A,4) */
--             Dl.Location_no, Dl.Location_name , Di.Item_no , Di.Item_short_desc, A.Sales_qty, A.Post_date
--             From Rtl_loc_item_dy_rms_dense A, Dim_location dl, Dim_item Di, rtl_item_supplier z
--             Where  A.Post_date Between ''24 Oct 11'' And ''01 Nov 11''
--             and A. Sk1_location_no = Dl.Sk1_location_no 
--             and A.Sk1_item_no = Di.Sk1_item_no 
--             And Di.Sk1_item_no = Z.Sk1_item_no
--             and z.Sk1_supplier_no In (3590, 3710)
--             And A.Sales_qty Is not Null 
--             ORDER BY Dl.Location_no, Dl.Location_name, Di.Item_no, Di.Item_short_desc, A.Post_date',
--             '|','DWH_FILES_OUT','alr_pos18');
  
    --g_count := dwh_generic_file_extract('select s.post_date, b.department_no, b.item_no, s.boh_qty                                                                                           
--from rtl_loc_item_dy_rms_stock  partition (tp_rlidrmsstkc_m20114) s, dim_item b, dim_location c
--where s.post_date between ''01 oct 10'' and ''24 oct 10'' 
--and s.sk1_location_no = c.sk1_location_no
--and c.sk1_location_no = 451
--and s.sk1_item_no = b.sk1_item_no','|','DWH_FILES_OUT','aj_stock');

    --g_count := dwh_generic_file_extract('select * from dim_item ','|','DWH_FILES_OUT','dim_item_extr1');   
   /* g_count := dwh_generic_file_extract('select location_no,subclass_no,item_no,''W''||fin_year_no||fin_week_no,soh_qty
from rtl_loc_item_wk_rms_stock lid,dim_location dl,dim_item di
where lid.sk1_location_no = dl.sk1_location_no and
      lid.sk1_item_no = di.sk1_item_no and
      dl.loc_type = ''S'' and
      lid.soh_qty <> 0 and
      di.business_unit_no <> 50 and
      fin_year_no = 2011 and fin_week_no between 37 and 39','|','DWH_FILES_OUT','extr_boh_3739');        
/*    g_count := dwh_generic_file_extract('select dns.post_date,di.fd_product_no,di.item_no,di.item_desc,
dl.location_no,dl.location_name,sales,sales_qty,waste_cost,waste_qty,
fd_num_catlg_days,fd_num_avail_days,fd_num_catlg_days_adj,fd_num_avail_days_adj
from   rtl_loc_item_dy_rms_dense dns,rtl_loc_item_dy_rms_sparse spr,rtl_loc_item_dy_catalog cat,dim_item di,dim_location dl
where di.sk1_item_no in (229063,229071,228885,227155,229058,229064,229066,229069,229070,227154,228887,6300914) and
dl.location_no in (255,128,121,127,131,267,126,117,242,105,112,1001,48,122,244,118,273,119,262,274) and
dns.post_date > ''27 June 2010'' and
dns.sk1_item_no     = di.sk1_item_no and
      dns.sk1_location_no = dl.sk1_location_no and
      dns.post_date       = spr.post_date(+) and
      dns.sk1_item_no     = spr.sk1_item_no(+) and
      dns.sk1_location_no = spr.sk1_location_no(+) and      
      dns.post_date       = cat.calendar_date  and
      dns.sk1_item_no     = cat.sk1_item_no  and
      dns.sk1_location_no = cat.sk1_location_no 
order by dns.post_date','|','DWH_FILES_OUT','food_avail'); 
/*    g_count := dwh_generic_file_extract('select fin_year_no,fin_week_no,di.item_no,dl.location_no,sales_qty
from   rtl_loc_item_wk_rms_dense rms,dim_item di, dim_location dl
where rms.sk1_item_no = di.sk1_item_no and rms.sk1_location_no = dl.sk1_location_no and
fin_year_no = 2010 and fin_week_no in (50,51,52,53) and di.business_unit_no = 50','|','DWH_FILES_OUT','food_wk50to53');    

    g_count := dwh_generic_file_extract('select fin_year_no,fin_week_no,di.item_no,dl.location_no,sales_qty
from   rtl_loc_item_wk_rms_dense rms,dim_item di, dim_location dl
where rms.sk1_item_no = di.sk1_item_no and rms.sk1_location_no = dl.sk1_location_no and
dl.location_no > 999 and
((fin_year_no = 2010 and fin_week_no >= 46 ) or fin_year_no = 2011)','|','DWH_FILES_OUT','food_999');
*/
    l_text :=  'Records extracted to extract file '||g_count;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
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
end adw_extractfood1;
