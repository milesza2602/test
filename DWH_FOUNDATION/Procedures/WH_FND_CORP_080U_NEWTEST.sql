--------------------------------------------------------
--  DDL for Procedure WH_FND_CORP_080U_NEWTEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_FOUNDATION"."WH_FND_CORP_080U_NEWTEST" 
(p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
-- new version of wh_fnd_Corp_080u
--**************************************************************************************************
--  Date:        JUNE 2015
--  Author:      Wendy lyttle
--  Purpose:     Create location_item dimention table in the foundation layer
--               with input ex staging table from RMS.
--  Tables:      Input  - w6005682.stg_rms_loc_item_cpy
--               Output - fnd_location_item
--  Packages:    dwh_constants, dwh_log, dwh_valid
--
--  Maintenance:
--

--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit       integer       :=  10000;
g_recs_read          integer       :=  0;
g_recs_updated       integer       :=  0;
g_recs_inserted      integer       :=  0;
g_recs_hospital      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_hospital           char(1)       := 'N';
g_hospital_text      stg_rms_location_item_hsp.sys_process_msg%type;
g_rec_out            fnd_location_item%rowtype;
g_rec_in             w6005682.stg_rms_loc_item_cpy%rowtype;
g_found              boolean;
g_valid              boolean;

--g_date              date          := to_char(sysdate,('dd mon yyyy'));
g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CORP_080U_NEWTEST';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_fnd_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION_ITEM MASTERDATA EX RMS';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of w6005682.stg_rms_loc_item_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of fnd_location_item%rowtype index by binary_integer;
type tbl_array_u is table of fnd_location_item%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of w6005682.stg_rms_loc_item_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of w6005682.stg_rms_loc_item_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************

procedure cursor_temp as
begin

insert /*+ append */ into w6005682.temp_location_item
           with     selbat as 
                               (select /*+ full(stg) */  location_no, item_no, max(sys_source_batch_id) maxbat
                                    from w6005682.stg_rms_loc_item_cpy stg
                                     --  where sys_source_batch_id  = 5294 --5246 -- 5294
                                group by location_no, item_no),
                    selseq as 
                                (select /*+ full(stg) full(sb) */ stg.location_no, stg.item_no, maxbat, max(stg.sys_source_sequence_no) maxseq
                                    from w6005682.stg_rms_loc_item_cpy stg, selbat sb
                                where stg.location_no = sb.location_no
                                    and stg.item_no = sb.item_no
                                    and stg.sys_source_batch_id = sb.maxbat
                                group by stg.location_no, stg.item_no, maxbat ),
                    selstg as (select  /*+ full(stg) full(ss) */ distinct stg.*
                               from w6005682.stg_rms_loc_item_cpy stg,
                                     selseq ss
                               where  stg.item_no                 = ss.item_no
                                   and stg.location_no            = ss.location_no
                                   and stg.sys_source_batch_id    = ss.maxbat
                                   and stg.sys_source_sequence_no = ss.maxseq),
                    selfnd as (select  /*+ full(stg) full(fli) */ distinct stg.*,
                                       fli.item_no fnd_exists
                               from selstg stg,
                                     fnd_location_item fli
                               where  stg.item_no                 = fli.item_no(+)
                                   and stg.location_no            = fli.location_no(+))
         select * from 
         (  select  /*+ full(stg)  full(fi) */  distinct stg.*,
                   fl.location_no fl_location_no,
                   fi.item_no fi_item_no,
                   fs.supplier_no fs_supplier_no,
                             case when clearance_ind is null then 'V'
                                   when clearance_ind = 0 then 'V'
                                   when clearance_ind = 1 then 'V'
                                   else    null
                             end  
                   chk_clearance_ind,
                               case when taxable_ind is null then 'V'
                                     when taxable_ind = 0 then 'V'
                                     when taxable_ind = 1 then 'V'
                                     else    null
                               end  
                   chk_taxable_ind,
                               case when wh_supply_chain_type_ind is null then 'V'
                                     when wh_supply_chain_type_ind = 0 then 'V'
                                     when wh_supply_chain_type_ind = 1 then 'V'
                                     else    null
                               end  
                   chk_wh_supply_chain_type_ind
           from selfnd stg,
                 fnd_location fl,
                 fnd_item fi,
                 fnd_supplier fs
           where   stg.primary_supplier_no   = fs.supplier_no(+)
               and stg.item_no                = fi.item_no(+)
               and stg.location_no            = fl.location_no(+)
               and stg.fnd_exists is null
   union 
           select  /*+ full(stg)  full(fi) */
                  distinct stg.*,
                   fl.location_no fl_location_no,
                   fi.item_no fi_item_no,
                   fs.supplier_no fs_supplier_no,
                   'V' chk_clearance_ind,
                   'V' chk_taxable_ind,
                   'V' chk_wh_supply_chain_type_ind
           from selfnd stg,
                 fnd_location fl,
                 fnd_item fi,
                 fnd_supplier fs
           where    stg.primary_supplier_no   = fs.supplier_no(+)
               and stg.item_no                = fi.item_no(+)
               and stg.location_no            = fl.location_no(+)
               and stg.fnd_exists is not null )
        order by location_no, item_no;
               
  g_recs_inserted :=  sql%rowcount;
      commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'TEMP INSERT - INSERT ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'TEMP INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end cursor_temp;  

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************

procedure local_write_hospital as
begin

        insert /*+ append */  into dwh_foundation.stg_rms_location_item_hsp 
         select /*+ full(hsp_rec) */ 
                     hsp_rec.SYS_SOURCE_BATCH_ID
                    ,hsp_rec.SYS_SOURCE_SEQUENCE_NO
                    ,trunc(sysdate) --sys_load_date
                    ,hsp_rec.SYS_PROCESS_CODE
                    ,'DWH' --load_system_name
                    ,hsp_rec.SYS_MIDDLEWARE_BATCH_ID
                    ,g_hospital_text --SYS_PROCESS_MSG
                    ,hsp_rec.ITEM_NO
                    ,hsp_rec.LOCATION_NO
                    ,hsp_rec.SUPPLY_CHAIN_TYPE
                    ,hsp_rec.REG_RSP
                    ,hsp_rec.SELLING_RSP
                    ,hsp_rec.SELLING_UOM_CODE
                    ,hsp_rec.PROM_RSP
                    ,hsp_rec.PROM_SELLING_RSP
                    ,hsp_rec.PROM_SELLING_UOM_CODE
                    ,hsp_rec.CLEARANCE_IND
                    ,hsp_rec.TAXABLE_IND
                    ,hsp_rec.POS_ITEM_DESC
                    ,hsp_rec.POS_SHORT_DESC
                    ,hsp_rec.NUM_TI_PALLET_TIER_CASES
                    ,hsp_rec.NUM_HI_PALLET_TIER_CASES
                    ,hsp_rec.STORE_ORD_MULT_UNIT_TYPE_CODE
                    ,hsp_rec.LOC_ITEM_STATUS_CODE
                    ,hsp_rec.LOC_ITEM_STAT_CODE_UPDATE_DATE
                    ,hsp_rec.AVG_NATURAL_DAILY_WASTE_PERC
                    ,hsp_rec.MEAS_OF_EACH
                    ,hsp_rec.MEAS_OF_PRICE
                    ,hsp_rec.RSP_UOM_CODE
                    ,hsp_rec.PRIMARY_VARIANT_ITEM_NO
                    ,hsp_rec.PRIMARY_COST_PACK_ITEM_NO
                    ,hsp_rec.PRIMARY_SUPPLIER_NO
                    ,hsp_rec.PRIMARY_COUNTRY_CODE
                    ,hsp_rec.RECEIVE_AS_PACK_TYPE
                    ,hsp_rec.SOURCE_METHOD_LOC_TYPE
                    ,hsp_rec.SOURCE_LOCATION_NO
                    ,hsp_rec.WH_SUPPLY_CHAIN_TYPE_IND
                    ,hsp_rec.SOURCE_DATA_STATUS_CODE
          from w6005682.temp_location_item hsp_rec
          where hsp_rec.fl_location_no is null
             or hsp_rec.fi_item_no is null
             or hsp_rec.fs_supplier_no is null
             or hsp_rec.chk_clearance_ind is null
             or hsp_rec.chk_taxable_ind is null
             or hsp_rec.chk_wh_supply_chain_type_ind is null;
             
      g_recs_hospital := g_recs_hospital + sql%rowcount;
      
      commit;
      

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end local_write_hospital;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure flagged_insert as
begin

        insert /*+ append */  into w6005682.fnd_loc_item_qs
      --  values
                (item_no
                ,LOCATION_NO
                ,SUPPLY_CHAIN_TYPE
                ,REG_RSP
                ,SELLING_RSP
                ,SELLING_UOM_CODE
                ,PROM_RSP
                ,PROM_SELLING_RSP
                ,PROM_SELLING_UOM_CODE
                ,CLEARANCE_IND
                ,TAXABLE_IND
                ,POS_ITEM_DESC
                ,POS_SHORT_DESC
                ,NUM_HI_PALLET_TIER_CASES
                ,STORE_ORD_MULT_UNIT_TYPE_CODE
                ,LOC_ITEM_STATUS_CODE
                ,LOC_ITEM_STAT_CODE_UPDATE_DATE
                ,AVG_NATURAL_DAILY_WASTE_PERC
                ,MEAS_OF_EACH
                ,MEAS_OF_PRICE
                ,RSP_UOM_CODE
                ,PRIMARY_VARIANT_ITEM_NO
                ,PRIMARY_COST_PACK_ITEM_NO
                ,PRIMARY_SUPPLIER_NO
                ,PRIMARY_COUNTRY_CODE
                ,RECEIVE_AS_PACK_TYPE
                ,SOURCE_METHOD_LOC_TYPE
                ,SOURCE_LOCATION_NO
                ,WH_SUPPLY_CHAIN_TYPE_IND
                ,SOURCE_DATA_STATUS_CODE
                ,last_updated_date
                )
         (select 
         /*+ full(ins_rec) */ 
                 item_no
                ,LOCATION_NO
                ,SUPPLY_CHAIN_TYPE
                ,REG_RSP
                ,SELLING_RSP
                ,SELLING_UOM_CODE
                ,PROM_RSP
                ,PROM_SELLING_RSP
                ,PROM_SELLING_UOM_CODE
                ,CLEARANCE_IND
                ,TAXABLE_IND
                ,POS_ITEM_DESC
                ,POS_SHORT_DESC
                ,NUM_HI_PALLET_TIER_CASES
                ,STORE_ORD_MULT_UNIT_TYPE_CODE
                ,LOC_ITEM_STATUS_CODE
                ,LOC_ITEM_STAT_CODE_UPDATE_DATE
                ,AVG_NATURAL_DAILY_WASTE_PERC
                ,MEAS_OF_EACH
                ,MEAS_OF_PRICE
                ,RSP_UOM_CODE
                ,PRIMARY_VARIANT_ITEM_NO
                ,PRIMARY_COST_PACK_ITEM_NO
                ,PRIMARY_SUPPLIER_NO
                ,PRIMARY_COUNTRY_CODE
                ,RECEIVE_AS_PACK_TYPE
                ,SOURCE_METHOD_LOC_TYPE
                ,SOURCE_LOCATION_NO
                ,WH_SUPPLY_CHAIN_TYPE_IND
                ,SOURCE_DATA_STATUS_CODE
                , g_date last_updated_date
          from w6005682.temp_location_item ins_rec
          where ins_rec.fl_location_no is not null
             and ins_rec.fi_item_no is not null
             and ins_rec.fs_supplier_no is not null
             and ins_rec.chk_clearance_ind is not null
             and ins_rec.chk_taxable_ind is not null
             and ins_rec.chk_wh_supply_chain_type_ind is not null
             and ins_rec.fnd_exists is null);
      g_recs_inserted :=  sql%rowcount;
      commit;


  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG INSERT - insert ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG INSERT - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_insert;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure flagged_update as
begin

MERGE /*+ first_rows parallel(fnd) parallel(upd_rec) */ INTO w6005682.fnd_loc_item_qs FND
USING (select /*+ FULL(TMP) */ 
                *
          from w6005682.temp_location_item TMP
          where fl_location_no is not null
             and fi_item_no is not null
             and fs_supplier_no is not null
             and chk_clearance_ind is not null
             and chk_taxable_ind is not null
             and chk_wh_supply_chain_type_ind is not null
             and fnd_exists is not null) UPD_REC
ON (  fnd.item_no = upd_rec.item_no
       and fnd.location_no = upd_rec.location_no)
WHEN MATCHED THEN 
   UPDATE SET
              fnd.supply_chain_type               = upd_rec.supply_chain_type,
              fnd.reg_rsp                         = upd_rec.reg_rsp,
              fnd.selling_rsp                     = upd_rec.selling_rsp,
              fnd.selling_uom_code                = upd_rec.selling_uom_code,
              fnd.prom_rsp                        = upd_rec.prom_rsp,
              fnd.prom_selling_rsp                = upd_rec.prom_selling_rsp,
              fnd.prom_selling_uom_code           = upd_rec.prom_selling_uom_code,
              fnd.clearance_ind                   = upd_rec.clearance_ind,
              fnd.taxable_ind                     = upd_rec.taxable_ind,
              fnd.pos_item_desc                   = upd_rec.pos_item_desc,
              fnd.pos_short_desc                  = upd_rec.pos_short_desc,
              fnd.num_ti_pallet_tier_cases        = upd_rec.num_ti_pallet_tier_cases,
              fnd.num_hi_pallet_tier_cases        = upd_rec.num_hi_pallet_tier_cases,
              fnd.store_ord_mult_unit_type_code   = upd_rec.store_ord_mult_unit_type_code,
              fnd.loc_item_status_code            = upd_rec.loc_item_status_code,
              fnd.loc_item_stat_code_update_date  = upd_rec.loc_item_stat_code_update_date,
              fnd.avg_natural_daily_waste_perc    = upd_rec.avg_natural_daily_waste_perc,
              fnd.meas_of_each                    = upd_rec.meas_of_each,
              fnd.meas_of_price                   = upd_rec.meas_of_price,
              fnd.rsp_uom_code                    = upd_rec.rsp_uom_code,
              fnd.primary_variant_item_no         = upd_rec.primary_variant_item_no,
              fnd.primary_cost_pack_item_no       = upd_rec.primary_cost_pack_item_no,
              fnd.primary_supplier_no             = upd_rec.primary_supplier_no,
              fnd.primary_country_code            = upd_rec.primary_country_code,
              fnd.receive_as_pack_type            = upd_rec.receive_as_pack_type,
              fnd.source_method_loc_type          = upd_rec.source_method_loc_type,
              fnd.source_location_no              = upd_rec.source_location_no,
              fnd.wh_supply_chain_type_ind        = upd_rec.wh_supply_chain_type_ind,
              fnd.source_data_status_code         = upd_rec.source_data_status_code,
              fnd.last_updated_date               = g_date
       ;
       
      g_recs_updated :=  sql%rowcount;
      commit;

  exception
      when dwh_errors.e_insert_error then
       l_message := 'FLAG UPDATE - update ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := 'FLAG UPDATE - OTHER ERROR '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;
end flagged_update;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF FND_LOCATION_ITEM EX RMS STARTED AT '||
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


       execute immediate ('ALTER SESSION ENABLE PARALLEL DML');

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************

---------------------
-- Create temp table
---------------------
       g_recs_inserted  := 0;
      
       l_text := 'truncate table w6005682.temp_location_item';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       execute immediate ('truncate table w6005682.temp_location_item');
      
       l_text := 'INSERT into temp_location_item starting ';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       CURSOR_TEMP;
       l_text := 'INSERT into temp_location_item completed - '||g_recs_inserted ||' records inserted';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

       l_text := 'Running GATHER_TABLE_STATS ON TEMP_LOCATION_ITEM';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       DBMS_STATS.gather_table_stats ('W6005682', 'TEMP_LOCATION_ITEM', DEGREE => 32);       

---------------------
-- Insert into hospital
---------------------
       g_recs_hospital  := 0;
      
    --   LOCAL_WRITE_HOSPITAL;

       l_text := 'Recs INSERTED into hospital = '||g_recs_hospital;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

                                   
---------------------
-- Update of fnd_location_item
---------------------
       g_recs_updated  := 0;
    
       FLAGGED_UPDATE;
       
       l_text := 'Recs UPDATED in fnd_location_item = '||g_recs_updated;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       
---------------------
-- Insert into fnd_location_item
---------------------
       g_recs_updated  := 0;
      
       FLAGGED_INSERT;
       
       l_text := 'Recs INSERTED into fnd_location_item = '||g_recs_inserted;
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


---------------------------------

       l_text := 'Running GATHER_TABLE_STATS ON FND_LOCATION_ITEM';
       dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       DBMS_STATS.gather_table_stats ('W6005682','FND_LOC_ITEM_QS', DEGREE => 8); 
                                     
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


END WH_FND_CORP_080U_NEWTEST;
