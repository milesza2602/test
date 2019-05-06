--------------------------------------------------------
--  DDL for Procedure WH_FND_FPI_012U_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_FPI_012U_BCK" -- STORE PROC CHANGE
                                    (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        February 2018
--  Author:      Francisca De Vaal
--               DESCRIPTION OF WHAT THIS STORE PROC IS DOING
--
--  Tables:      Input  - STG_FPI_SPEC_COMPONENT_CPY                                                            -- STG TABLE NAME CHANGE 
--               Output - FND_ITEM_SUP_SPEC_COMP                                                                -- FND TABLE NAME CHANGE 
--  Packages:    constants, dwh_log, dwh_valid
--  Maintenance:
--  dd mon yyyy - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx       
--
--  Naming conventions
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
g_recs_duplicate     integer       :=  0;
g_recs_dummy         integer       :=  0;
g_truncate_count     integer       :=  0;
g_physical_updated   integer       :=  0;

g_date               date          := trunc(sysdate);

L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_FPI_012U';                              -- STORE PROC CHANGE
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STG_FPI_SPEC_COMPONENT_CPY EX FPI';    -- STG TABLE NAME CHANGE
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin
--/*+ first_rows parallel(fnd) parallel(upd_rec) */
 
   merge  into fnd_item_sup_spec_comp fnd     		                                                           -- FND TABLE NAME CHANGE
   using (

    select /*+ PARALLEL(CPY1,8) */ cpy1.*
    from   stg_fpi_spec_component_cpy cpy1,
           fnd_supplier sup,
           fnd_item itm,
           fnd_supplier_site ss
    where  cpy1.supplier_no   = sup.supplier_no and
           cpy1.item_no       = itm.item_no and
           cpy1.supplier_code = ss.supplier_code and
           cpy1.site_code     = ss.site_code and
           cpy1.supplier_no   = ss.supplier_no and
          (sys_source_batch_id, sys_source_sequence_no,
	         cpy1.item_no, cpy1.supplier_code, cpy1.site_code, cpy1.supplier_no, component_name, spec_version            
	  ) in
         (select /*+ PARALLEL(8) */ sys_source_batch_id, sys_source_sequence_no,
	      	  item_no,supplier_code,site_code,supplier_no,component_name, spec_version    
          from (select /*+ PARALLEL(tmp,8) */ tmp.*,
                rank ()
                    over (partition by 
			          item_no,supplier_code,site_code,supplier_no,component_name, spec_version    
		      order by sys_source_batch_id desc, sys_source_sequence_no desc)
                    as rank
                  from stg_fpi_spec_component_cpy tmp                                                               -- STG TABLE NAME CHANGE 
           )
          where rank = 1)
    order by sys_source_batch_id desc, sys_source_sequence_no

         ) mer_rec
   on    (  
            fnd.item_no	        =	mer_rec.item_no 	    and
            fnd.supplier_code	=	mer_rec.supplier_code 	and
            fnd.site_code	    =	mer_rec.site_code 	    and
            fnd.supplier_no	    =	mer_rec.supplier_no 	and
            fnd.component_name	=	mer_rec.component_name  and
            fnd.spec_version    =   mer_rec.spec_version
	 )
   when matched then 
   update set                                                                                                      -- COLUNM NAME CHANGE 
            fnd.parent_ind	                    =	mer_rec.parent_ind,
            fnd.component_id	                =	mer_rec.component_id,
            fnd.number_in_parent	            =	mer_rec.number_in_parent,
            fnd.weight_of_component	            =	mer_rec.weight_of_component,
            fnd.pack_copy_required	            =	mer_rec.pack_copy_required,
            fnd.multipack_comments	            =	mer_rec.multipack_comments,
            fnd.bus_lang_ingre_list	            =	mer_rec.bus_lang_ingre_list,
            fnd.pack_copy_ingre_list	        =	mer_rec.pack_copy_ingre_list,
            fnd.ingre_req_on_pack	            =	mer_rec.ingre_req_on_pack,
            fnd.on_pack_ingre_list	            =	mer_rec.on_pack_ingre_list,
            fnd.character_ingredient	        =	mer_rec.character_ingredient,
            fnd.character_ingredient_country	=	mer_rec.character_ingredient_country,
            fnd.nutrition_panel_title	        =	mer_rec.nutrition_panel_title,
            fnd.foot_note	                    =	mer_rec.foot_note,
            fnd.no_of_servings	                =	mer_rec.no_of_servings,
            fnd.supporting_statements	        =	mer_rec.supporting_statements,
            fnd.front_of_pack_declaration_html	=	mer_rec.front_of_pack_declaration_html,
            fnd.back_of_pack_declaration_html 	=	mer_rec.back_of_pack_declaration_html,
            fnd.foot_note_table_required	    =	mer_rec.foot_note_table_required,
            fnd.front_of_pack_nut_tbl_fnote	    =	mer_rec.front_of_pack_nut_tbl_fnote,
            fnd.front_of_pack_per_100_text	    =	mer_rec.front_of_pack_per_100_text,
            fnd.include_gda_table	            =	mer_rec.include_gda_table,            
            fnd.per_100_text	                =	mer_rec.per_100_text,
            fnd.per_serv_text	                =	mer_rec.per_serv_text,
            fnd.percentage_nvr	                =	mer_rec.percentage_nvr,
            fnd.serving_unit_of_measure	        =	mer_rec.serving_unit_of_measure,
            fnd.sel_1	                        =	mer_rec.sel_1,
            fnd.sel_2	                        =	mer_rec.sel_2,
            fnd.sel_3	                        =	mer_rec.sel_3,
            fnd.unit_of_measure	                =	mer_rec.unit_of_measure,
            fnd.units_printing	                =	mer_rec.units_printing,
            fnd.print_height	                =	mer_rec.print_height,
            fnd.country_of_origin_state_1   	=	mer_rec.country_of_origin_state_1,
            fnd.country_of_origin_state_2   	=	mer_rec.country_of_origin_state_2,
            fnd.declared_qty	                =	mer_rec.declared_qty,
            fnd.declared_qty_type	            =	mer_rec.declared_qty_type,
            fnd.declared_qty_loc	            =	mer_rec.declared_qty_loc,
            fnd.airfreight_product	            =	mer_rec.airfreight_product,
            fnd.any_other_info_back_of_pack 	=	mer_rec.any_other_info_back_of_pack,
            fnd.food_for_thought	            =	mer_rec.food_for_thought,
            fnd.serving_suggestions	            =	mer_rec.serving_suggestions,
            fnd.serving_suggestion_required 	=	mer_rec.serving_suggestion_required,
            fnd.drained_weight	                =	mer_rec.drained_weight,
            fnd.drained_weight_location     	=	mer_rec.drained_weight_location,
            fnd.serves	                        =	mer_rec.serves,
            fnd.copyright_year	                =	mer_rec.copyright_year,
            fnd.price_box	                    =	mer_rec.price_box,
            fnd.unit_pricing_uom	            =	mer_rec.unit_pricing_uom,
            fnd.standard_price	                =	mer_rec.standard_price,
            fnd.launch_promotional_price    	=	mer_rec.launch_promotional_price,
            fnd.average_net_quantity	        =	mer_rec.average_net_quantity,
            fnd.declared_net_quantity	        =	mer_rec.declared_net_quantity,
            fnd.max_net_quantity	            =	mer_rec.max_net_quantity,
            fnd.min_net_quantity	            =	mer_rec.min_net_quantity,
            fnd.product_gross_weight	        =	mer_rec.product_gross_weight,
            fnd.product_width	                =	mer_rec.product_width,
            fnd.product_height	                =	mer_rec.product_height,
            fnd.product_depth	                =	mer_rec.product_depth,
            fnd.unit_of_measure_packaging	    =	mer_rec.unit_of_measure_packaging,
            fnd.last_updated_date	            =	g_date

   when not matched then
   insert                                                                                                          -- COLUNM NAME CHANGE 
          (
            item_no,
            supplier_code,
            site_code,
            supplier_no,
            component_name,
            spec_version,
            parent_ind,
            component_id,
            number_in_parent,
            weight_of_component,
            pack_copy_required,
            multipack_comments,
            bus_lang_ingre_list,
            pack_copy_ingre_list,
            ingre_req_on_pack,
            on_pack_ingre_list,
            character_ingredient,
            character_ingredient_country,
            nutrition_panel_title,
            foot_note,
            no_of_servings,
            supporting_statements,
            front_of_pack_declaration_html,
            back_of_pack_declaration_html,
            foot_note_table_required,
            front_of_pack_nut_tbl_fnote,
            front_of_pack_per_100_text,
            include_gda_table,
            per_100_text,
            per_serv_text,
            percentage_nvr,
            serving_unit_of_measure,
            sel_1,
            sel_2,
            sel_3,
            unit_of_measure,
            units_printing,
            print_height,
            country_of_origin_state_1,
            country_of_origin_state_2,
            declared_qty,
            declared_qty_type,
            declared_qty_loc,
            airfreight_product,
            any_other_info_back_of_pack,
            food_for_thought,
            serving_suggestions,
            serving_suggestion_required,
            drained_weight,
            drained_weight_location,
            serves,
            copyright_year,
            price_box,
            unit_pricing_uom,
            standard_price,
            launch_promotional_price,
            average_net_quantity,
            declared_net_quantity,
            max_net_quantity,
            min_net_quantity,
            product_gross_weight,
            product_width,
            product_height,
            product_depth,
            unit_of_measure_packaging,
            last_updated_date
          )
  values                                                                                                           -- COLUNM NAME CHANGE 
          (         
           mer_rec.item_no,
           mer_rec.supplier_code,
           mer_rec.site_code,
           mer_rec.supplier_no,
           mer_rec.component_name,
           mer_rec.spec_version,
           mer_rec.parent_ind,
           mer_rec.component_id,
           mer_rec.number_in_parent,
           mer_rec.weight_of_component,
           mer_rec.pack_copy_required,
           mer_rec.multipack_comments,
           mer_rec.bus_lang_ingre_list,
           mer_rec.pack_copy_ingre_list,
           mer_rec.ingre_req_on_pack,
           mer_rec.on_pack_ingre_list,
           mer_rec.character_ingredient,
           mer_rec.character_ingredient_country,
           mer_rec.nutrition_panel_title,
           mer_rec.foot_note,
           mer_rec.no_of_servings,
           mer_rec.supporting_statements,
           mer_rec.front_of_pack_declaration_html,
           mer_rec.back_of_pack_declaration_html,
           mer_rec.foot_note_table_required,
           mer_rec.front_of_pack_nut_tbl_fnote,
           mer_rec.front_of_pack_per_100_text,
           mer_rec.include_gda_table,
           mer_rec.per_100_text,
           mer_rec.per_serv_text,
           mer_rec.percentage_nvr,
           mer_rec.serving_unit_of_measure,
           mer_rec.sel_1,
           mer_rec.sel_2,
           mer_rec.sel_3,
           mer_rec.unit_of_measure,
           mer_rec.units_printing,
           mer_rec.print_height,
           mer_rec.country_of_origin_state_1,
           mer_rec.country_of_origin_state_2,
           mer_rec.declared_qty,
           mer_rec.declared_qty_type,
           mer_rec.declared_qty_loc,
           mer_rec.airfreight_product,
           mer_rec.any_other_info_back_of_pack,
           mer_rec.food_for_thought,
           mer_rec.serving_suggestions,
           mer_rec.serving_suggestion_required,
           mer_rec.drained_weight,
           mer_rec.drained_weight_location,
           mer_rec.serves,
           mer_rec.copyright_year,
           mer_rec.price_box,
           mer_rec.unit_pricing_uom,
           mer_rec.standard_price,
           mer_rec.launch_promotional_price,
           mer_rec.average_net_quantity,
           mer_rec.declared_net_quantity,
           mer_rec.max_net_quantity,
           mer_rec.min_net_quantity,
           mer_rec.product_gross_weight,
           mer_rec.product_width,
           mer_rec.product_height,
           mer_rec.product_depth,
           mer_rec.product_depth,
          g_date
          )           
          ;  
             
      g_recs_updated := g_recs_updated +  sql%rowcount;       

     commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
       
      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
 
end flagged_records_update;

--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    execute immediate 'alter session enable parallel dml';
 
    l_text := dwh_constants.vc_log_draw_line;
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
-- Call the bulk routines 
--**************************************************************************************************
--    l_text := 'REMOVAL OF STAGING DUPLICATES STARTED AT '||
--    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
--    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
--    remove_duplicates;
    
    
    select count(*)
    into   g_recs_read
    from   STG_FPI_SPEC_COMPONENT_CPY                                                                           -- STG TABLE NAME CHANGE 
    where  sys_process_code = 'N';

    l_text := 'BULK UPDATE STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   
    flagged_records_update;

    l_text := 'BULK HOSPITALIZATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    
--    flagged_records_hospital;
    

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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'PHYSICAL UPDATES ACTUALLY DONE '||g_physical_updated;            --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
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
end wh_fnd_fpi_012u_bck                                                                                             -- STORE PROC CHANGE 
;
