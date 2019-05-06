--------------------------------------------------------
--  DDL for Procedure WH_FND_FPI_004U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_FPI_004U" 
                             (p_forall_limit in integer,p_success out boolean) as
                                                                
--**************************************************************************************************
--  Date:        February 2018
--  Author:      Francisca De Vaal
--  Tables:      Input  - STG_FPI_PRODUCT_SPEC_CPY                                                              
--               Output - FND_ITEM_SUP_PROD_SPEC                                                                
--  Packages:    constants, dwh_log, dwh_valid
--  
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
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_FPI_004U';				-- STORE PROC CHANGE 
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE STG_FPI_PRODUCT_SPEC_CPY EX FPI';	-- STG TABLE NAME CHANGE
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

--************************************************************************************************** 
-- UPDATE all record flaged as 'U' in the staging table into foundation
--**************************************************************************************************

procedure flagged_records_update as
begin
--/*+ first_rows parallel(fnd) parallel(upd_rec) */
 
    merge  into fnd_item_sup_prod_spec fnd     		                                                        -- FND TABLE NAME CHANGE
      using (
   
    select /*+ PARALLEL(CPY1,8) */ cpy1.*
    from  stg_fpi_product_spec_cpy cpy1, 
          fnd_supplier sup,
          fnd_item itm,
          fnd_supplier_site ss
    where  cpy1.supplier_no = sup.supplier_no and
           cpy1.item_no     = itm.item_no and
           cpy1.supplier_code = ss.supplier_code and
           cpy1.site_code     = ss.site_code and
           cpy1.supplier_no   = ss.supplier_no and
    (     sys_source_batch_id, sys_source_sequence_no,
          cpy1.item_no, cpy1.supplier_code, cpy1.site_code, cpy1.supplier_no, spec_version
	  ) in
         (select /*+ PARALLEL(8) */ sys_source_batch_id, sys_source_sequence_no,
	      	      item_no, supplier_code, site_code, supplier_no, spec_version
          from 
          (select /*+ PARALLEL(tmp,8) */ tmp.*,
                rank ()
                    over (partition by 
			          item_no, supplier_code, site_code, supplier_no, spec_version
		       order by sys_source_batch_id desc, sys_source_sequence_no desc)
                    as rank
           from stg_fpi_product_spec_cpy tmp                                                               -- STG TABLE NAME CHANGE 
          )
          where rank = 1
         )
    order by sys_source_batch_id desc, sys_source_sequence_no

         ) mer_rec
   on    
   (fnd.item_no		    =	mer_rec.item_no         and 
    fnd.supplier_code	=	mer_rec.supplier_code   and
	fnd.site_code	    =	mer_rec.site_code       and
	fnd.supplier_no	    =	mer_rec.supplier_no     and
    fnd.spec_version	=	mer_rec.spec_version
	 )
   when matched then 
   update set                                                                                                      -- COLUNM NAME CHANGE 
            fnd.product_name	           =	mer_rec.product_name,
            fnd.product_long_description   =	mer_rec.product_long_description,
            fnd.product_short_description  =	mer_rec.product_short_description,
            fnd.product_group	           =	mer_rec.product_group,
            fnd.event	                   =	mer_rec.event,
            fnd.spec_type	               =	mer_rec.spec_type,
            fnd.spec_number	               =	mer_rec.spec_number,
            fnd.spec_status	               =	mer_rec.spec_status,
            fnd.multipack_ind	           =	mer_rec.multipack_ind,
            fnd.brand	                   =	upper(mer_rec.brand),
            fnd.sub_brand	               =	mer_rec.sub_brand,
            fnd.brand_type	               =	mer_rec.brand_type,
            fnd.seasonal_product	       =	mer_rec.seasonal_product,
            fnd.other_brand_details	       =	mer_rec.other_brand_details,
            fnd.product_record_code	       =	mer_rec.product_record_code,
            fnd.product_record_id	       =	mer_rec.product_record_id,
            fnd.product_record_title	   =	mer_rec.product_record_title,
            fnd.product_record_size	       =	mer_rec.product_record_size,
            fnd.pack_copy_language	       =	mer_rec.pack_copy_language,
            fnd.first_production_dte	   =	mer_rec.first_production_dte,
            fnd.out_of_store_dte	       =	mer_rec.out_of_store_dte,
            fnd.target_launch_dte	       =	mer_rec.target_launch_dte,
            fnd.actual_launch_dte	       =	mer_rec.actual_launch_dte,
            fnd.review_dte	               =	mer_rec.review_dte,
            fnd.retail_approv_dte	       =	mer_rec.retail_approv_dte,
            fnd.supplier_approv_dte	       =	mer_rec.supplier_approv_dte,
            fnd.produce_effective_from_dte =	mer_rec.produce_effective_from_dte,
            fnd.produce_effective_to_dte   =	mer_rec.produce_effective_to_dte,   
            fnd.last_updated_date	       =	g_date


   when not matched then
   insert                                                                                                      -- COLUNM NAME CHANGE 
          (
            item_no,
            supplier_code,
            site_code,
            supplier_no,
            spec_version,
            product_name,
            product_long_description,
            product_short_description,
            product_group,
            event,
            spec_type,
            spec_number,
            spec_status,
            multipack_ind,
            brand,
            sub_brand,
            brand_type,
            seasonal_product,
            other_brand_details,
            product_record_code,
            product_record_id,
            product_record_title,
            product_record_size,
            pack_copy_language,
            first_production_dte,
            out_of_store_dte,
            target_launch_dte,
            actual_launch_dte,
            review_dte,
            retail_approv_dte,
            supplier_approv_dte,
            produce_effective_from_dte,
            produce_effective_to_dte,
            spec_active_from_dte,
            spec_active_to_dte,
            last_updated_date

          )
  values                                                                                                      -- COLUNM NAME CHANGE 
          (         
            mer_rec.item_no,
            mer_rec.supplier_code,
            mer_rec.site_code,
            mer_rec.supplier_no,
            mer_rec.spec_version,
            mer_rec.product_name,
            mer_rec.product_long_description,
            mer_rec.product_short_description,
            mer_rec.product_group,
            mer_rec.event,
            mer_rec.spec_type,
            mer_rec.spec_number,
            mer_rec.spec_status,
            mer_rec.multipack_ind,
            upper(mer_rec.brand),
            mer_rec.sub_brand,
            mer_rec.brand_type,
            mer_rec.seasonal_product,
            mer_rec.other_brand_details,
            mer_rec.product_record_code,
            mer_rec.product_record_id,
            mer_rec.product_record_title,
            mer_rec.product_record_size,
            mer_rec.pack_copy_language,
            mer_rec.first_production_dte,
            mer_rec.out_of_store_dte,
            mer_rec.target_launch_dte,
            mer_rec.actual_launch_dte,
            mer_rec.review_dte,
            mer_rec.retail_approv_dte,
            mer_rec.supplier_approv_dte,
            mer_rec.produce_effective_from_dte,
            mer_rec.produce_effective_to_dte,
            g_date,
            '31/DEC/3999',
            g_date
          )           
          ;  
             
      g_recs_updated := g_recs_updated +  sql%rowcount;       

     commit;

--************************************************************************************************** 
-- UPDATE history records in the foundation table
--**************************************************************************************************
   merge into fnd_item_sup_prod_spec a                                                        
          using ( 
            with 
                minspec as (
                   SELECT distinct
                        item_no,
                        supplier_code,
                        site_code,
                        supplier_no,
                        min(spec_version) as Seq 
                        ,Min(spec_active_from_dte) as spec_active_from_dte
                    FROM
                        dwh_foundation.fnd_item_sup_prod_spec 
                    Where spec_active_to_dte = '31/DEC/3999'
                    Group by item_no,supplier_code,site_code,supplier_no
                    ),
                maxspec as (
                  SELECT distinct
                        item_no,
                        supplier_code,
                        site_code,
                        supplier_no,
                        max(spec_version) as Seq 
                        ,max(spec_active_from_dte) as spec_active_from_dte
                    FROM
                        dwh_foundation.fnd_item_sup_prod_spec 
                    Where spec_active_to_dte = '31/DEC/3999'
                    Group by item_no,supplier_code,site_code,supplier_no
                    )
                    select mins.item_no,
                           mins.supplier_code,
                           mins.site_code,
                           mins.supplier_no,
                           mins.Seq,
                           mins.spec_active_from_dte,
                           maxs.spec_active_from_dte  as maxspec_date
                    from maxspec maxs,
                         minspec mins
                    Where maxs.item_no = mins.item_no
                      and maxs.supplier_code = mins.supplier_code
                      and maxs.site_code = mins.site_code
                      and maxs.supplier_no = mins.supplier_no
                      and maxs.Seq <> mins.Seq
                )b
            on    
               (a.item_no		=	b.item_no         and 
                a.supplier_code	=	b.supplier_code   and
                a.site_code	    =	b.site_code       and
                a.supplier_no	=	b.supplier_no     and
                a.spec_version	=	b.Seq
                 )            
            when matched then 
               update set 
                   a.spec_active_to_dte = b.maxspec_date-1, 
                   a.spec_status        = 'INACTIVE',
                   a.last_updated_date  = g_date; 
                   
        commit;           
        
 --**************************************************************************************************           
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
--    g_date := g_date;
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
    from   STG_FPI_PRODUCT_SPEC_CPY									-- STG TABLE NAME CHANGE
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
    l_text :=  'DUPLICATE REMOVED '||g_recs_duplicate;                              --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'DUMMY RECS CREATED '||g_recs_dummy;                                 --Bulk load--
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);  --Bulk Load--
    l_text :=  'PHYSICAL UPDATES ACTUALLY DONE '||g_physical_updated;               --Bulk load--
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
end wh_fnd_fpi_004u                                                             				-- STORE PROC CHANGE 
;
