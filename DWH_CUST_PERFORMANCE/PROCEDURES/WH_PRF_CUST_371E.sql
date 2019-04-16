--------------------------------------------------------
--  DDL for Procedure WH_PRF_CUST_371E
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_PERFORMANCE"."WH_PRF_CUST_371E" 
(p_forall_limit in integer,p_success out boolean)
as

--**************************************************************************************************
--  Date:        February 2016
--  Author:      Theo Filander
--  Purpose:     Create Item to flat file in the performance layer
--               by reading a view and calling generic function to output to flat file.
--  Tables:      Input  - DIM_ITEM, RTL_LOC_ITEM_WK_RMS_DENSE, DIM_LOCATION
--               Output - flat file extracts
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  Jun 2017  - Theo Filander Include business Units 51,52,54 and 55
--              BCB-248
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
g_sdate              date          := trunc(sysdate);  
g_edate              date          := trunc(sysdate);  
g_yesterday          date          := trunc(sysdate) - 1;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_371E';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_other;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_other;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT               SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'EXTRACT E20 PERSONALISATION INTERFACE (ITEMS) TO FLAT FILE';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;
         

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin

    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


    l_text := 'EXTRACT FOR E20 PERSONALISATION INTERFACE-ITEMS STARTED AT '||
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

--**************************************************************************************************
-- Write to external directory.
-- TO SETUP
-- 1. add directory path to database via CREATE DIRECTORY command
-- 2. ensure that permissions are correct
-- 3. format : 'A','|','B','C'
--       WHERE A = select statement
--             B = Database Directory Name as found on DBA_DIRECTORIES
--             C = output file name
--    eg.'select * from VS_EXT_VMP_SALES_WKLY','|','/dwh_files/files.out','nielsen.wk'
--**************************************************************************************************


    dbms_output.put_line('ITEMS');
    g_count := dwh_performance.dwh_generic_file_extract(
         q'[select sk1_item_no,
                   item_no,
                   item_desc,
                   item_short_desc,
                   item_upper_desc,
                   item_scndry_desc,
                   sk1_subclass_no,
                   subclass_no,
                   subclass_name,
                   sk1_class_no,
                   class_no,
                   class_name,
                   sk1_department_no,
                   department_no,
                   department_name,
                   sk1_subgroup_no,
                   subgroup_no,
                   subgroup_name,
                   sk1_group_no,
                   group_no,
                   group_name,
                   sk1_business_unit_no,
                   business_unit_no,
                   business_unit_name,
                   sk1_company_no,
                   company_no,
                   company_name,
                   item_status_code,
                   item_level_no,
                   tran_level_no,
                   tran_ind,
                   primary_ref_item_ind,
                   fd_product_no,
                   item_parent_no,
                   item_grandparent_no,
                   item_level1_no,
                   item_level2_no,
                   rpl_ind,
                   item_no_type,
                   format_id,
                   upc_prefix_no,
                   diff_1_code,
                   diff_2_code,
                   diff_3_code,
                   diff_4_code,
                   diff_1_code_desc,
                   diff_2_code_desc,
                   diff_3_code_desc,
                   diff_4_code_desc,
                   diff_1_diff_type,
                   diff_2_diff_type,
                   diff_3_diff_type,
                   diff_4_diff_type,
                   diff_1_type_desc,
                   diff_2_type_desc,
                   diff_3_type_desc,
                   diff_4_type_desc,
                   diff_type_colour_diff_code,
                   diff_type_prim_size_diff_code,
                   diff_type_scnd_size_diff_code,
                   diff_type_fragrance_diff_code,
                   diff_1_diff_group_code,
                   diff_2_diff_group_code,
                   diff_3_diff_group_code,
                   diff_4_diff_group_code,
                   diff_1_diff_group_desc,
                   diff_2_diff_group_desc,
                   diff_3_diff_group_desc,
                   diff_4_diff_group_desc,
                   diff_1_display_seq,
                   diff_2_display_seq,
                   diff_3_display_seq,
                   diff_4_display_seq,
                   item_aggr_ind,
                   diff_1_aggr_ind,
                   diff_2_aggr_ind,
                   diff_3_aggr_ind,
                   diff_4_aggr_ind,
                   retail_zone_group_no,
                   cost_zone_group_no,
                   standard_uom_code,
                   standard_uom_desc,
                   standard_uom_class_code,
                   uom_conv_factor,
                   package_size,
                   package_uom_code,
                   package_uom_desc,
                   package_uom_class_code,
                   merchandise_item_ind,
                   store_ord_mult_unit_type_code,
                   ext_sys_forecast_ind,
                   primary_currency_original_rsp,
                   mfg_recommended_rsp,
                   retail_label_type,
                   retail_label_value,
                   handling_temp_code,
                   handling_sensitivity_code,
                   random_mass_ind,
                   first_received_date,
                   last_received_date,
                   most_recent_received_qty,
                   waste_type,
                   avg_waste_perc,
                   default_waste_perc,
                   constant_dimension_ind,
                   pack_item_ind,
                   pack_item_simple_ind,
                   pack_item_inner_pack_ind,
                   pack_item_sellable_unit_ind,
                   pack_item_orderable_ind,
                   pack_item_type,
                   pack_item_receivable_type,
                   item_comment,
                   item_service_level_type,
                   gift_wrap_ind,
                   ship_alone_ind,
                   origin_item_ext_src_sys_name,
                   banded_item_ind,
                   static_mass,
                   ext_ref_id,
                   create_date,
                   size_id,
                   color_id,
                   style_colour_no,
                   style_no,
                   buying_ind,
                   selling_ind,
                   product_class,
                   fd_discipline_type,
                   handling_method_code,
                   handling_method_name,
                   display_method_code,
                   display_method_name,
                   tray_size_code,
                   segregation_ind,
                   outer_case_barcode,
                   rpl_merch_season_no,
                   prod_catg_code,
                   supp_comment,
                   rdf_forecst_ind,
                   item_launch_date,
                   product_profile_code,
                   vat_rate_perc,
                   sk1_supplier_no,
                   primary_supplier_no,
                   sk1_merch_season_phase_no,
                   item_long_desc
              from dim_item 
             where business_unit_no = 50]','|','DWH_FILES_OUT','e20_cust_item.txt');
    l_text :=  'Records extracted to E20_CUST_ITEM '||g_count;
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
       RAISE;
end WH_PRF_CUST_371E;
