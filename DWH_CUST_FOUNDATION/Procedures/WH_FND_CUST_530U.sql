--------------------------------------------------------
--  DDL for Procedure WH_FND_CUST_530U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_CUST_FOUNDATION"."WH_FND_CUST_530U" (p_forall_limit in integer,p_success out boolean) AS


--**************************************************************************************************
--  Date:        Sept 2010
--  Author:      Alastair de Wet
--  Purpose:     Create CL_INQUIRY fact table in the foundation layer
--               with input ex staging table from Customer Liaison.
--  Tables:      Input  - stg_cust_cl_inquiry_cpy
--               Output - fnd_cust_cl_inquiry
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  16 OCT 2015 PBLM – We cannot load data at the moment due to the fact that since Sunday night, we have been receiving fact data with missing DIMS.
--                     As the lookup value on the DIM is derived in the performance layer, it does not go to hospital but results in the proc aborting.
--                     At the moment, we are bypassing this load as we cannot insert a null for the SK (or at least have no business rule about what to do)
--                     I have added the same derivation to a test fnd  version(wh_fnd_cust_530u_wl) and have run it through.
--                     Seems fine, but I will have to then do a datafix to WL_fnd_cust_cl_inquiry to complete the change.
--                     This is a workaround until either source sorts out it’s story or we are given a new business rule.
--                     I don’t know exactly who uses this data – Customer Liason??
--                     And hence what the impact is.
-- xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
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
g_hospital_text      stg_cust_cl_inquiry_hsp.sys_process_msg%type;
g_rec_out            fnd_cust_cl_inquiry%rowtype;
g_rec_in             stg_cust_cl_inquiry_cpy%rowtype;
g_found              boolean;
G_COUNT NUMBER := 0 ;
g_CL_INQ_SUB_CAT_NO	NUMBER(10,0);
g_INQ_TYPE_NO	NUMBER(10,0);
g_orig_INQ_TYPE_NO	NUMBER(10,0);

g_date               date          := trunc(sysdate);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_FND_CUST_530U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_fnd;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_fnd;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CL_INQUIRY DATA EX CUST LIAISON';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


-- For input bulk collect --
type stg_array is table of stg_cust_cl_inquiry_cpy%rowtype;
a_stg_input      stg_array;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_CUST_FOUNDATION.fnd_cust_cl_inquiry%rowtype index by binary_integer;
type tbl_array_u is table of DWH_CUST_FOUNDATION.fnd_cust_cl_inquiry%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- For arrays used to update the staging table process_code --
type staging_array1 is table of stg_cust_cl_inquiry_cpy.sys_source_batch_id%type
                                                       index by binary_integer;
type staging_array2 is table of stg_cust_cl_inquiry_cpy.sys_source_sequence_no%type
                                                       index by binary_integer;
a_staging1          staging_array1;
a_staging2          staging_array2;
a_empty_set_s1      staging_array1;
a_empty_set_s2      staging_array2;

a_count_stg         integer       := 0;


cursor c_stg_cust_cl_inquiry is
--  WITH SELALL AS (            SELECT /*+ MATERIALIZEE  FULL(A) PARALLEL(A,6) */ * FROM stg_cust_cl_inquiry_arc A
--                  UNION ALL SELECT /*+ MATERIALIZE  FULL(C) PARALLEL(C,6) */ * FROM stg_cust_cl_inquiry_CPY C
-- ),
--SELBAT AS (SELECT /*+ MATERIALIZE */ INQUIRY_NO, MAX(SYS_SOURCE_BATCH_ID) MAXBAT FROM SELALL
--GROUP BY INQUIRY_NO),
--SELSEQ AS (SELECT /*+ MATERIALIZE */ SA.INQUIRY_NO,  MAXBAT, MAX(SYS_SOURCE_SEQUENCE_NO) MAXSEQ FROM SELALL SA, SELBAT SB
--WHERE SA.INQUIRY_NO = SB.INQUIRY_NO
--AND SA.SYS_SOURCE_BATCH_ID = SB.MAXBAT
--GROUP BY SA.INQUIRY_NO, MAXBAT)
--SELECT SA.* FROM SELALL SA, SELSEQ SQ
--WHERE SA.INQUIRY_NO = SQ.INQUIRY_NO
--AND SA.SYS_SOURCE_BATCH_ID = SQ.MAXBAT
--AND SA.SYS_SOURCE_SEQUENCE_NO = SQ.MAXSEQ
--order by sys_source_batch_id,sys_source_sequence_no;

select *
   from stg_cust_cl_inquiry_cpy
 --  where sys_process_code = 'N'
   order by sys_source_batch_id,sys_source_sequence_no;

-- order by only where sequencing is essential to the correct loading of data

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_hospital                                := 'N';
   g_rec_out.inquiry_no                      := g_rec_in.inquiry_no ;
   g_rec_out.inq_type_no                     := g_rec_in.inq_type_no;
   g_rec_out.inq_type_level_ind              := g_rec_in.inq_type_level_ind;
   g_rec_out.owner_user_no                   := g_rec_in.owner_user_no;
   g_rec_out.logged_by_user_no               := g_rec_in.logged_by_user_no;
   g_rec_out.channel_inbound_no              := g_rec_in.channel_inbound_no;
   g_rec_out.cl_status_no                    := g_rec_in.cl_status_no;
   g_rec_out.interaction_no                  := g_rec_in.interaction_no;
   g_rec_out.bus_area_no                     := g_rec_in.bus_area_no;
   g_rec_out.priority                        := g_rec_in.priority;
   g_rec_out.status_central                  := g_rec_in.status_central;
   g_rec_out.inquiry_bus_area                := g_rec_in.inquiry_bus_area;
   g_rec_out.owner_grp                       := g_rec_in.owner_grp;
   g_rec_out.transfer_flag                   := g_rec_in.transfer_flag;
   g_rec_out.transfer_user                   := g_rec_in.transfer_user;
   g_rec_out.transfer_grp                    := g_rec_in.transfer_grp;
   g_rec_out.escal_ind                       := g_rec_in.escal_ind;
   g_rec_out.escal_level                     := g_rec_in.escal_level;
   g_rec_out.receipt_ackn_required           := g_rec_in.receipt_ackn_required;
   g_rec_out.cust_feedback_required          := g_rec_in.cust_feedback_required;
   g_rec_out.inq_details                     := g_rec_in.inq_details;
   g_rec_out.logged_date                     := g_rec_in.logged_date;
   g_rec_out.classified_date                 := g_rec_in.classified_date;
   g_rec_out.receipt_ackn_date               := g_rec_in.receipt_ackn_date;
   g_rec_out.last_prg_upd_date               := g_rec_in.last_prg_upd_date;
   g_rec_out.cust_resolved_date              := g_rec_in.cust_resolved_date;
   g_rec_out.transfer_date                   := g_rec_in.transfer_date;
   g_rec_out.transfer_acc_date               := g_rec_in.transfer_acc_date;
   g_rec_out.special_resolved_date           := g_rec_in.special_resolved_date;
   g_rec_out.special_ext_period              := g_rec_in.special_ext_period;
   g_rec_out.closed_date                     := g_rec_in.closed_date;
   g_rec_out.qa_rqd_ind                      := g_rec_in.qa_rqd_ind;
   g_rec_out.qa_compl_date                   := g_rec_in.qa_compl_date;
   g_rec_out.item_no                         := nvl(g_rec_in.item_no,0);
   g_rec_out.txt_style_no                    := g_rec_in.txt_style_no;
   g_rec_out.product_desc                    := g_rec_in.product_desc;
   g_rec_out.txt_size                        := g_rec_in.txt_size;
   g_rec_out.txt_col_fragr                   := g_rec_in.txt_col_fragr;
   g_rec_out.supplier_no                     := nvl(g_rec_in.supplier_no,0);
   g_rec_out.department_no                   := nvl(g_rec_in.department_no,0);
   g_rec_out.purchase_date                   := g_rec_in.purchase_date;
   g_rec_out.location_no                     := nvl(g_rec_in.location_no,0);
   g_rec_out.item_qty                        := g_rec_in.item_qty;
   g_rec_out.selling_price                   := g_rec_in.selling_price;
   g_rec_out.store_refund_given              := g_rec_in.store_refund_given;
   g_rec_out.refund_value                    := g_rec_in.refund_value;
   g_rec_out.complementary_item_given        := g_rec_in.complementary_item_given;
   g_rec_out.complementary_item_value        := g_rec_in.complementary_item_value;
   g_rec_out.txt_debit_memo                  := g_rec_in.txt_debit_memo;
   g_rec_out.foods_sell_by_date              := g_rec_in.foods_sell_by_date;
   g_rec_out.foods_prod_batch                := g_rec_in.foods_prod_batch;
   g_rec_out.foods_mass_size                 := g_rec_in.foods_mass_size;
   g_rec_out.fo_avail                        := g_rec_in.fo_avail ;
   g_rec_out.fo_present                      := g_rec_in.fo_present;
   g_rec_out.fo_received_date_store          := g_rec_in.fo_received_date_store;
   g_rec_out.fo_received_date_cl             := g_rec_in.fo_received_date_cl;
   g_rec_out.fo_received_date_tech           := g_rec_in.fo_received_date_tech;
   g_rec_out.svc_staff_member                := g_rec_in.svc_staff_member;
   g_rec_out.svc_cia_store_no                := g_rec_in.svc_cia_store_no;
   g_rec_out.cia_incident_date               := g_rec_in.cia_incident_date;
   g_rec_out.cia_cust_asstd                  := g_rec_in.cia_cust_asstd;
   g_rec_out.cia_claims_impl                 := g_rec_in.cia_claims_impl;
   g_rec_out.cia_asst_given                  := g_rec_in.cia_asst_given;
   g_rec_out.cia_store_resolved              := g_rec_in.cia_store_resolved;
   g_rec_out.cia_asst_mgmt                   := g_rec_in.cia_asst_mgmt;
   g_rec_out.resol_type_lev1                 := g_rec_in.resol_type_lev1;
   g_rec_out.resol_type_lev2                 := g_rec_in.resol_type_lev2;
   g_rec_out.specl_resol_user                := g_rec_in.specl_resol_user;
   g_rec_out.cust_resol_user                 := g_rec_in.cust_resol_user;
   g_rec_out.justified_status                := g_rec_in.justified_status;
   g_rec_out.penalty_area                    := g_rec_in.penalty_area;
   g_rec_out.penalty_value                   := g_rec_in.penalty_value;
   g_rec_out.no_corresp_sent                 := g_rec_in.no_corresp_sent;
   g_rec_out.spec_resolved_ind               := g_rec_in.spec_resolved_ind;
   g_rec_out.spec_resolution                 := g_rec_in.spec_resolution;
   g_rec_out.cust_resolved_ind               := g_rec_in.cust_resolved_ind;
   g_rec_out.cust_resolution                 := g_rec_in.cust_resolution;
   g_rec_out.break_cold_chain                := g_rec_in.break_cold_chain;
   g_rec_out.quality_complaint_reason        := g_rec_in.quality_complaint_reason;
   g_rec_out.source_data_status_code         := g_rec_in.source_data_status_code;
   g_rec_out.taste_wild_ind                  := nvl(g_rec_in.taste_wild_ind,0)  ;
   g_rec_out.taste_off_ind                   := nvl(g_rec_in.taste_off_ind,0);
   g_rec_out.taste_rancid_ind                := nvl(g_rec_in.taste_rancid_ind,0);
   g_rec_out.taste_salty_ind                 := nvl(g_rec_in.taste_salty_ind,0);
   g_rec_out.taste_sour_ind                  := nvl(g_rec_in.taste_sour_ind,0);
   g_rec_out.taste_tasteless_ind             := nvl(g_rec_in.taste_tasteless_ind,0);
   g_rec_out.taste_chemical_ind              := nvl(g_rec_in.taste_chemical_ind,0);
   g_rec_out.taste_tough_ind                 := nvl(g_rec_in.taste_tough_ind,0);
   g_rec_out.taste_dry_ind                   := nvl(g_rec_in.taste_dry_ind,0);
   g_rec_out.smell_chemical_ind              := nvl(g_rec_in.smell_chemical_ind,0);
   g_rec_out.smell_off_ind                   := nvl(g_rec_in.smell_off_ind,0);
   g_rec_out.smell_bad_rotten_ind            := nvl(g_rec_in.smell_bad_rotten_ind,0);
   g_rec_out.feel_hard_ind                   := nvl(g_rec_in.feel_hard_ind,0);
   g_rec_out.feel_soft_ind                   := nvl(g_rec_in.feel_soft_ind,0);
   g_rec_out.feel_dry_ind                    := nvl(g_rec_in.feel_dry_ind,0);
   g_rec_out.feel_mushy_ind                  := nvl(g_rec_in.feel_mushy_ind,0);
   g_rec_out.look_fatty_ind                  := nvl(g_rec_in.look_fatty_ind,0);
   g_rec_out.look_discoloured_ind            := nvl(g_rec_in.look_discoloured_ind,0);
   g_rec_out.look_separated_ind              := nvl(g_rec_in.look_separated_ind,0);
   g_rec_out.look_burnt_ind                  := nvl(g_rec_in.look_burnt_ind,0);
   g_rec_out.look_pale_ind                   := nvl(g_rec_in.look_pale_ind,0);
   g_rec_out.look_underbaked_raw_ind         := nvl(g_rec_in.look_underbaked_raw_ind,0);
   g_rec_out.look_dry_ind                    := nvl(g_rec_in.look_dry_ind,0);
   g_rec_out.look_over_ripe_ind              := nvl(g_rec_in.look_over_ripe_ind,0);
   g_rec_out.look_under_ripe_ind             := nvl(g_rec_in.look_under_ripe_ind,0);
   g_rec_out.packaging_not_sealed_ind        := nvl(g_rec_in.packaging_not_sealed_ind,0);
   g_rec_out.packaging_leaks_ind             := nvl(g_rec_in.packaging_leaks_ind,0);
   g_rec_out.packaging_misleading_ind        := nvl(g_rec_in.packaging_misleading_ind,0);
   g_rec_out.packaging_blewup_microwave_ind  := nvl(g_rec_in.packaging_blewup_microwave_ind,0);
   g_rec_out.packaging_lack_of_info_ind      := nvl(g_rec_in.packaging_lack_of_info_ind,0);
   g_rec_out.packaging_incorrect_info_ind    := nvl(g_rec_in.packaging_incorrect_info_ind,0);
   g_rec_out.packaging_wrong_product_ind     := nvl(g_rec_in.packaging_wrong_product_ind,0);
   g_rec_out.value_poor_value_ind            := nvl(g_rec_in.value_poor_value_ind,0);
   g_rec_out.value_incorrect_price_ind       := nvl(g_rec_in.value_incorrect_price_ind,0);
   g_rec_out.value_incorrect_promotion_ind   := nvl(g_rec_in.value_incorrect_promotion_ind,0);
   g_rec_out.value_too_expensive_ind         := nvl(g_rec_in.value_too_expensive_ind,0);
   g_rec_out.last_updated_date               := g_date;



   if not dwh_valid.indicator_field(g_rec_out.qa_rqd_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_invalid_indicator;
     return;
   end if;
   if not dwh_valid.indicator_field(g_rec_out.escal_ind) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_invalid_indicator;
     return;
   end if;
   if not dwh_valid.indicator_field(g_rec_out.receipt_ackn_required) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_invalid_indicator;
     return;
   end if;
   if not dwh_valid.indicator_field(g_rec_out.cust_feedback_required) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_invalid_indicator;
     return;
   end if;

   if not dwh_cust_valid.fnd_cust_cl_user(g_rec_out.owner_user_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_cl_owner_not_found;
     l_text          := dwh_cust_constants.vc_cl_owner_not_found||g_rec_out.owner_user_no||' '||g_rec_out.inquiry_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;
   if not dwh_cust_valid.fnd_cust_cl_user(g_rec_out.logged_by_user_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_cl_logged_not_found;
     l_text          := dwh_cust_constants.vc_cl_logged_not_found||g_rec_out.logged_by_user_no||' '||g_rec_out.inquiry_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;
   if g_rec_out.inq_type_level_ind NOT IN (0,1,2,3) then
      g_rec_out.inq_type_level_ind := 3;
      g_rec_out.inq_type_no        := 99991332;
   end if;
   if not dwh_cust_valid.fnd_cust_cl_chanel_inbound(g_rec_out.channel_inbound_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_cl_inbound_not_found;
     l_text          := dwh_cust_constants.vc_cl_inbound_not_found||g_rec_out.channel_inbound_no||' '||g_rec_out.inquiry_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;
   if not dwh_cust_valid.fnd_cust_cl_status(g_rec_out.cl_status_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_cl_status_not_found;
     l_text          := dwh_cust_constants.vc_cl_status_not_found||g_rec_out.cl_status_no||' '||g_rec_out.inquiry_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;
   if not dwh_cust_valid.fnd_cust_cl_bus_area(g_rec_out.bus_area_no) then
     g_rec_out.bus_area_no := 0; 
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_cust_constants.vc_cl_bus_area_not_found;
--     l_text          := dwh_cust_constants.vc_cl_bus_area_not_found||g_rec_out.bus_area_no||' '||g_rec_out.inquiry_no  ;
--     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--     return;
   end if;

 case g_rec_out.inq_type_level_ind
   when 0 then
   if not dwh_cust_valid.fnd_cust_cl_inq_cat(g_rec_out.inq_type_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_cl_inq_cat_not_found;
     l_text          := dwh_cust_constants.vc_cl_inq_cat_not_found||g_rec_out.inq_type_no||' '||g_rec_out.inquiry_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;
   when 1 then
   if not dwh_cust_valid.fnd_cust_cl_feedback(g_rec_out.inq_type_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_cl_inq_feedback_not_found;
     l_text          := dwh_cust_constants.vc_cl_inq_feedback_not_found||g_rec_out.inq_type_no||' '||g_rec_out.inquiry_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;
   when 2 then
   if not dwh_cust_valid.fnd_cust_cl_type_cat(g_rec_out.inq_type_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_cl_type_cat_not_found;
     l_text          := dwh_cust_constants.vc_cl_type_cat_not_found||g_rec_out.inq_type_no||' '||g_rec_out.inquiry_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;
   when 3 then
   if not dwh_cust_valid.fnd_cust_cl_sub_cat(g_rec_out.inq_type_no) then
     g_hospital      := 'Y';
     g_hospital_text := dwh_cust_constants.vc_cl_sub_cat_not_found;
     l_text          := dwh_cust_constants.vc_cl_sub_cat_not_found||g_rec_out.inq_type_no||' '||g_rec_out.inquiry_no  ;
     dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
     return;
   end if;
 end case;



--   if not dwh_cust_valid.source_status(g_rec_out.source_data_status_code) then
--     g_hospital      := 'Y';
--     g_hospital_text := dwh_cust_constants.vc_invalid_source_code;
--   end if;

---------------------------------------------------------
-- default to a dummy level 3 number if input not at level 3
---------------------------------------------------------
    g_orig_inq_type_no :=  g_rec_in.inq_type_no;

   if g_rec_in.inq_type_level_ind = 2 then
    
    begin
         select cl_inq_feedback_no
                 into   g_rec_in.inq_type_no
                 from   fnd_cust_cl_type_cat
               where  g_rec_in.inq_type_no = cl_inq_type_cat_no;
      
     exception
               when no_data_found then
                  g_rec_in.inq_type_no          := null;
    end;  
    
    begin      
     
         select cl_inq_category_no
               into   g_rec_in.inq_type_no
               from   fnd_cust_cl_feedback
             where  g_rec_in.inq_type_no = cl_inq_feedback_no;
        --      g_rec_in.inq_type_no := g_rec_in.inq_type_no + 99990000;
          
     exception
               when no_data_found then
                  g_rec_in.inq_type_no          := null;
                  
      g_rec_in.inq_type_no := nvl(g_rec_in.inq_type_no,0) + 99990000;
     end;
   end if;

   if g_rec_in.inq_type_level_ind = 1 then
      
      begin
          select CL_INQ_CATEGORY_NO
                into   g_rec_in.inq_type_no
                from   fnd_cust_cl_feedback
                where  g_rec_in.inq_type_no = cl_inq_feedback_no;
          --      g_rec_in.inq_type_no := g_rec_in.inq_type_no + 99990000;
      
      exception
               when no_data_found then
                  g_rec_in.inq_type_no          := null;
                  g_rec_in.inq_type_no := nvl(g_rec_in.inq_type_no,0) + 99990000;
       end;
    end if;
       
   if g_rec_in.inq_type_level_ind = 0 then
      g_rec_in.inq_type_no := nvl(g_rec_in.inq_type_no,0) + 99990000;
   end if;

    g_cl_inq_sub_cat_no := null ; 
       begin 
              select cl_inq_sub_cat_no
             into   g_cl_inq_sub_cat_no
             from   fnd_cust_cl_sub_cat
             where  cl_inq_sub_cat_no = g_rec_in.inq_type_no;
        exception
               when no_data_found then
                    g_cl_inq_sub_cat_no := null ; 
          end;
      if g_cl_inq_sub_cat_no is null  then
               g_hospital      := 'Y';
               g_hospital_text := 'DERIVED cl_inq_sub_cat_no not found = '||g_orig_inq_type_no||' '||g_rec_in.inq_type_no||' '||g_rec_out.inquiry_no;
               return;
      end if;

     g_rec_OUT.inq_type_no         := g_orig_inq_type_no;
     g_rec_OUT.DERIVED_INQ_TYPE_NO := g_cl_inq_sub_cat_no;
--DERIVED_INQ_TYPE_NO

   exception
      when others then
       l_message := dwh_cust_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Write invalid data out to the hostpital table
--**************************************************************************************************
procedure local_write_hospital as
begin

   g_rec_in.sys_load_date         := sysdate;
   g_rec_in.sys_load_system_name  := 'DWH';
   g_rec_in.sys_process_code      := 'Y';
   g_rec_in.sys_process_msg       := g_hospital_text;

   insert into stg_cust_cl_inquiry_hsp values 
   (g_rec_in.SYS_SOURCE_BATCH_ID
, g_rec_in.SYS_SOURCE_SEQUENCE_NO
, g_rec_in.SYS_LOAD_DATE
, g_rec_in.SYS_PROCESS_CODE
, g_rec_in.SYS_LOAD_SYSTEM_NAME
, g_rec_in.SYS_MIDDLEWARE_BATCH_ID
, g_rec_in.SYS_PROCESS_MSG
, g_rec_in.INQUIRY_NO
,  g_orig_inq_type_no  --  must load original and not derived INQ_TYPE_NO to hospital for reprocessing purposes
, g_rec_in.INQ_TYPE_LEVEL_IND
, g_rec_in.OWNER_USER_NO
, g_rec_in.LOGGED_BY_USER_NO
, g_rec_in.CHANNEL_INBOUND_NO
, g_rec_in.CL_STATUS_NO
, g_rec_in.INTERACTION_NO
, g_rec_in.INQUIRY_BUS_AREA
, g_rec_in.PRIORITY
, g_rec_in.STATUS_CENTRAL
, g_rec_in.BUS_AREA_NO
, g_rec_in.OWNER_GRP
, g_rec_in.TRANSFER_FLAG
, g_rec_in.TRANSFER_USER
, g_rec_in.TRANSFER_GRP
, g_rec_in.ESCAL_IND
, g_rec_in.ESCAL_LEVEL
, g_rec_in.RECEIPT_ACKN_REQUIRED
, g_rec_in.CUST_FEEDBACK_REQUIRED
, g_rec_in.INQ_DETAILS
, g_rec_in.LOGGED_DATE
, g_rec_in.CLASSIFIED_DATE
, g_rec_in.RECEIPT_ACKN_DATE
, g_rec_in.LAST_PRG_UPD_DATE
, g_rec_in.CUST_RESOLVED_DATE
, g_rec_in.TRANSFER_DATE
, g_rec_in.TRANSFER_ACC_DATE
, g_rec_in.SPECIAL_RESOLVED_DATE
, g_rec_in.SPECIAL_EXT_PERIOD
, g_rec_in.CLOSED_DATE
, g_rec_in.QA_RQD_IND
, g_rec_in.QA_COMPL_DATE
, g_rec_in.ITEM_NO
, g_rec_in.TXT_STYLE_NO
, g_rec_in.PRODUCT_DESC
, g_rec_in.TXT_SIZE
, g_rec_in.TXT_COL_FRAGR
, g_rec_in.SUPPLIER_NO
, g_rec_in.DEPARTMENT_NO
, g_rec_in.PURCHASE_DATE
, g_rec_in.LOCATION_NO
, g_rec_in.ITEM_QTY
, g_rec_in.SELLING_PRICE
, g_rec_in.STORE_REFUND_GIVEN
, g_rec_in.REFUND_VALUE
, g_rec_in.COMPLEMENTARY_ITEM_GIVEN
, g_rec_in.COMPLEMENTARY_ITEM_VALUE
, g_rec_in.TXT_DEBIT_MEMO
, g_rec_in.FOODS_SELL_BY_DATE
, g_rec_in.FOODS_PROD_BATCH
, g_rec_in.FOODS_MASS_SIZE
, g_rec_in.FO_AVAIL
, g_rec_in.FO_PRESENT
, g_rec_in.FO_RECEIVED_DATE_STORE
, g_rec_in.FO_RECEIVED_DATE_CL
, g_rec_in.FO_RECEIVED_DATE_TECH
, g_rec_in.SVC_STAFF_MEMBER
, g_rec_in.SVC_CIA_STORE_NO
, g_rec_in.CIA_INCIDENT_DATE
, g_rec_in.CIA_CUST_ASSTD
, g_rec_in.CIA_CLAIMS_IMPL
, g_rec_in.CIA_ASST_GIVEN
, g_rec_in.CIA_STORE_RESOLVED
, g_rec_in.CIA_ASST_MGMT
, g_rec_in.RESOL_TYPE_LEV1
, g_rec_in.RESOL_TYPE_LEV2
, g_rec_in.SPECL_RESOL_USER
, g_rec_in.CUST_RESOL_USER
, g_rec_in.JUSTIFIED_STATUS
, g_rec_in.PENALTY_AREA
, g_rec_in.PENALTY_VALUE
, g_rec_in.NO_CORRESP_SENT
, g_rec_in.SPEC_RESOLVED_IND
, g_rec_in.SPEC_RESOLUTION
, g_rec_in.CUST_RESOLVED_IND
, g_rec_in.CUST_RESOLUTION
, g_rec_in.BREAK_COLD_CHAIN
, g_rec_in.QUALITY_COMPLAINT_REASON
, g_rec_in.SOURCE_DATA_STATUS_CODE
, g_rec_in.TASTE_WILD_IND
, g_rec_in.TASTE_OFF_IND
, g_rec_in.TASTE_RANCID_IND
, g_rec_in.TASTE_SALTY_IND
, g_rec_in.TASTE_SOUR_IND
, g_rec_in.TASTE_TASTELESS_IND
, g_rec_in.TASTE_CHEMICAL_IND
, g_rec_in.TASTE_TOUGH_IND
, g_rec_in.TASTE_DRY_IND
, g_rec_in.SMELL_CHEMICAL_IND
, g_rec_in.SMELL_OFF_IND
, g_rec_in.SMELL_BAD_ROTTEN_IND
, g_rec_in.FEEL_HARD_IND
, g_rec_in.FEEL_SOFT_IND
, g_rec_in.FEEL_DRY_IND
, g_rec_in.FEEL_MUSHY_IND
, g_rec_in.LOOK_FATTY_IND
, g_rec_in.LOOK_DISCOLOURED_IND
, g_rec_in.LOOK_SEPARATED_IND
, g_rec_in.LOOK_BURNT_IND
, g_rec_in.LOOK_PALE_IND
, g_rec_in.LOOK_UNDERBAKED_RAW_IND
, g_rec_in.LOOK_DRY_IND
, g_rec_in.LOOK_OVER_RIPE_IND
, g_rec_in.LOOK_UNDER_RIPE_IND
, g_rec_in.PACKAGING_NOT_SEALED_IND
, g_rec_in.PACKAGING_LEAKS_IND
, g_rec_in.PACKAGING_MISLEADING_IND
, g_rec_in.PACKAGING_BLEWUP_MICROWAVE_IND
, g_rec_in.PACKAGING_LACK_OF_INFO_IND
, g_rec_in.PACKAGING_INCORRECT_INFO_IND
, g_rec_in.PACKAGING_WRONG_PRODUCT_IND
, g_rec_in.VALUE_POOR_VALUE_IND
, g_rec_in.VALUE_INCORRECT_PRICE_IND
, g_rec_in.VALUE_INCORRECT_PROMOTION_IND
, g_rec_in.VALUE_TOO_EXPENSIVE_IND
 )  
   ;
   g_recs_hospital := g_recs_hospital + sql%rowcount;

  exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lh_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_lh_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;


end local_write_hospital;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into DWH_CUST_FOUNDATION.fnd_cust_cl_inquiry values a_tbl_insert(i);

    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).inquiry_no ;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_insert;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_update as
begin

    forall i in a_tbl_update.first .. a_tbl_update.last
       save exceptions
       update DWH_CUST_FOUNDATION.fnd_cust_cl_inquiry
       set  inq_type_no                     = a_tbl_update(i).inq_type_no,
            inq_type_level_ind              = a_tbl_update(i).inq_type_level_ind,
            owner_user_no                   = a_tbl_update(i).owner_user_no,
            logged_by_user_no               = a_tbl_update(i).logged_by_user_no,
            channel_inbound_no              = a_tbl_update(i).channel_inbound_no,
            cl_status_no                    = a_tbl_update(i).cl_status_no,
            interaction_no                  = a_tbl_update(i).interaction_no,
            bus_area_no                     = a_tbl_update(i).bus_area_no,
            priority                        = a_tbl_update(i).priority,
            status_central                  = a_tbl_update(i).status_central,
            inquiry_bus_area                 = a_tbl_update(i).inquiry_bus_area,
            owner_grp                       = a_tbl_update(i).owner_grp,
            transfer_flag                   = a_tbl_update(i).transfer_flag,
            transfer_user                   = a_tbl_update(i).transfer_user,
            transfer_grp                    = a_tbl_update(i).transfer_grp,
            escal_ind                       = a_tbl_update(i).escal_ind,
            escal_level                     = a_tbl_update(i).escal_level,
            receipt_ackn_required           = a_tbl_update(i).receipt_ackn_required,
            cust_feedback_required          = a_tbl_update(i).cust_feedback_required,
            inq_details                     = a_tbl_update(i).inq_details,
            logged_date                     = a_tbl_update(i).logged_date,
            classified_date                 = a_tbl_update(i).classified_date,
            receipt_ackn_date               = a_tbl_update(i).receipt_ackn_date,
            last_prg_upd_date               = a_tbl_update(i).last_prg_upd_date,
            cust_resolved_date              = a_tbl_update(i).cust_resolved_date,
            transfer_date                   = a_tbl_update(i).transfer_date,
            transfer_acc_date               = a_tbl_update(i).transfer_acc_date,
            special_resolved_date           = a_tbl_update(i).special_resolved_date,
            special_ext_period              = a_tbl_update(i).special_ext_period,
            closed_date                     = a_tbl_update(i).closed_date,
            qa_rqd_ind                      = a_tbl_update(i).qa_rqd_ind,
            qa_compl_date                   = a_tbl_update(i).qa_compl_date,
            item_no                         = a_tbl_update(i).item_no,
            txt_style_no                    = a_tbl_update(i).txt_style_no,
            product_desc                    = a_tbl_update(i).product_desc,
            txt_size                        = a_tbl_update(i).txt_size,
            txt_col_fragr                   = a_tbl_update(i).txt_col_fragr,
            supplier_no                     = a_tbl_update(i).supplier_no,
            department_no                   = a_tbl_update(i).department_no,
            purchase_date                   = a_tbl_update(i).purchase_date,
            location_no                     = a_tbl_update(i).location_no,
            item_qty                        = a_tbl_update(i).item_qty,
            selling_price                   = a_tbl_update(i).selling_price,
            store_refund_given              = a_tbl_update(i).store_refund_given,
            refund_value                    = a_tbl_update(i).refund_value,
            complementary_item_given        = a_tbl_update(i).complementary_item_given,
            complementary_item_value        = a_tbl_update(i).complementary_item_value,
            txt_debit_memo                  = a_tbl_update(i).txt_debit_memo,
            foods_sell_by_date              = a_tbl_update(i).foods_sell_by_date,
            foods_prod_batch                = a_tbl_update(i).foods_prod_batch,
            foods_mass_size                 = a_tbl_update(i).foods_mass_size,
            fo_avail                        = a_tbl_update(i).fo_avail,
            fo_present                      = a_tbl_update(i).fo_present,
            fo_received_date_store          = a_tbl_update(i).fo_received_date_store,
            fo_received_date_cl             = a_tbl_update(i).fo_received_date_cl,
            fo_received_date_tech           = a_tbl_update(i).fo_received_date_tech,
            svc_staff_member                = a_tbl_update(i).svc_staff_member,
            svc_cia_store_no                = a_tbl_update(i).svc_cia_store_no,
            cia_incident_date               = a_tbl_update(i).cia_incident_date,
            cia_cust_asstd                  = a_tbl_update(i).cia_cust_asstd,
            cia_claims_impl                 = a_tbl_update(i).cia_claims_impl,
            cia_asst_given                  = a_tbl_update(i).cia_asst_given,
            cia_store_resolved              = a_tbl_update(i).cia_store_resolved,
            cia_asst_mgmt                   = a_tbl_update(i).cia_asst_mgmt,
            resol_type_lev1                 = a_tbl_update(i).resol_type_lev1,
            resol_type_lev2                 = a_tbl_update(i).resol_type_lev2,
            specl_resol_user                = a_tbl_update(i).specl_resol_user,
            cust_resol_user                 = a_tbl_update(i).cust_resol_user,
            justified_status                = a_tbl_update(i).justified_status,
            penalty_area                    = a_tbl_update(i).penalty_area,
            penalty_value                   = a_tbl_update(i).penalty_value,
            no_corresp_sent                 = a_tbl_update(i).no_corresp_sent,
            spec_resolved_ind               = a_tbl_update(i).spec_resolved_ind,
            spec_resolution                 = a_tbl_update(i).spec_resolution,
            cust_resolved_ind               = a_tbl_update(i).cust_resolved_ind,
            cust_resolution                 = a_tbl_update(i).cust_resolution,
            break_cold_chain                = a_tbl_update(i).break_cold_chain,
            quality_complaint_reason        = a_tbl_update(i).quality_complaint_reason,
            source_data_status_code         = a_tbl_update(i).source_data_status_code,
            taste_wild_ind                  = a_tbl_update(i).taste_wild_ind,
            taste_off_ind                   = a_tbl_update(i).taste_off_ind,
            taste_rancid_ind                = a_tbl_update(i).taste_rancid_ind,
            taste_salty_ind                 = a_tbl_update(i).taste_salty_ind,
            taste_sour_ind                  = a_tbl_update(i).taste_sour_ind,
            taste_tasteless_ind             = a_tbl_update(i).taste_tasteless_ind,
            taste_chemical_ind              = a_tbl_update(i).taste_chemical_ind,
            taste_tough_ind                 = a_tbl_update(i).taste_tough_ind,
            taste_dry_ind                   = a_tbl_update(i).taste_dry_ind,
            smell_chemical_ind              = a_tbl_update(i).smell_chemical_ind,
            smell_off_ind                   = a_tbl_update(i).smell_off_ind,
            smell_bad_rotten_ind            = a_tbl_update(i).smell_bad_rotten_ind,
            feel_hard_ind                   = a_tbl_update(i).feel_hard_ind,
            feel_soft_ind                   = a_tbl_update(i).feel_soft_ind,
            feel_dry_ind                    = a_tbl_update(i).feel_dry_ind,
            feel_mushy_ind                  = a_tbl_update(i).feel_mushy_ind,
            look_fatty_ind                  = a_tbl_update(i).look_fatty_ind,
            look_discoloured_ind            = a_tbl_update(i).look_discoloured_ind,
            look_separated_ind              = a_tbl_update(i).look_separated_ind,
            look_burnt_ind                  = a_tbl_update(i).look_burnt_ind,
            look_pale_ind                   = a_tbl_update(i).look_pale_ind,
            look_underbaked_raw_ind         = a_tbl_update(i).look_underbaked_raw_ind,
            look_dry_ind                    = a_tbl_update(i).look_dry_ind,
            look_over_ripe_ind              = a_tbl_update(i).look_over_ripe_ind,
            look_under_ripe_ind             = a_tbl_update(i).look_under_ripe_ind,
            packaging_not_sealed_ind        = a_tbl_update(i).packaging_not_sealed_ind,
            packaging_leaks_ind             = a_tbl_update(i).packaging_leaks_ind,
            packaging_misleading_ind        = a_tbl_update(i).packaging_misleading_ind,
            packaging_blewup_microwave_ind  = a_tbl_update(i).packaging_blewup_microwave_ind,
            packaging_lack_of_info_ind      = a_tbl_update(i).packaging_lack_of_info_ind,
            packaging_incorrect_info_ind    = a_tbl_update(i).packaging_incorrect_info_ind,
            packaging_wrong_product_ind     = a_tbl_update(i).packaging_wrong_product_ind,
            value_poor_value_ind            = a_tbl_update(i).value_poor_value_ind,
            value_incorrect_price_ind       = a_tbl_update(i).value_incorrect_price_ind,
            value_incorrect_promotion_ind   = a_tbl_update(i).value_incorrect_promotion_ind,
            value_too_expensive_ind         = a_tbl_update(i).value_too_expensive_ind,
            DERIVED_INQ_TYPE_NO          = a_tbl_update(i).DERIVED_INQ_TYPE_NO,
            last_updated_date               = a_tbl_update(i).last_updated_date


       where             inquiry_no         = a_tbl_update(i).inquiry_no  ;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).inquiry_no ;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_update;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk updates  to output table
--**************************************************************************************************
procedure local_bulk_staging_update as
begin
    forall i in a_staging1.first .. a_staging1.last
       save exceptions
       update stg_cust_cl_inquiry_cpy
       set    sys_process_code       = 'Y'
       where  sys_source_batch_id    = a_staging1(i) and
              sys_source_sequence_no = a_staging2(i);

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_cust_constants.vc_err_lb_staging||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_cust_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_staging1(g_error_index)||' '||a_staging2(g_error_index);

          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_staging_update;


--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
  g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   DWH_CUST_FOUNDATION.fnd_cust_cl_inquiry
   where  inquiry_no             = g_rec_out.inquiry_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;
-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).inquiry_no  = g_rec_out.inquiry_no  then
            g_found := TRUE;
         end if;
      end loop;
   end if;

-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
   else
      a_count_u               := a_count_u + 1;
      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************
--   if a_count > 1000 then
   if a_count > g_forall_limit then
      local_bulk_insert;
     local_bulk_update;
      local_bulk_staging_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_staging1    := a_empty_set_s1;
      a_staging2    := a_empty_set_s2;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;
      a_count_stg   := 0;

      commit;
   end if;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;


--**************************************************************************************************
-- Main process
--**************************************************************************************************
begin
    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF fnd_cust_cl_inquiry EX CUST LIAISON STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

 execute immediate 'alter session enable parallel dml';
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_stg_cust_cl_inquiry;
    fetch c_stg_cust_cl_inquiry bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_cust_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);
         a_count_stg             := a_count_stg + 1;
         a_staging1(a_count_stg) := g_rec_in.sys_source_batch_id;
         a_staging2(a_count_stg) := g_rec_in.sys_source_sequence_no;
         local_address_variables;
         if g_hospital = 'Y' then
            local_write_hospital;
         else
            local_write_output;
         end if;
      end loop;
    fetch c_stg_cust_cl_inquiry bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_stg_cust_cl_inquiry;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_insert;
    local_bulk_update;
    local_bulk_staging_update;


--**************************************************************************************************
-- Write final log data
--**************************************************************************************************


    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_hospital||g_recs_hospital;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    commit;
    p_success := true;
  exception

      when dwh_errors.e_insert_error then
       l_message := dwh_cust_constants.vc_err_mm_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

      when others then
       l_message := dwh_cust_constants.vc_err_mm_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
                                  l_process_type,dwh_cust_constants.vc_log_aborted,'','','','','');
       rollback;
       p_success := false;
       raise;

END WH_FND_CUST_530U;
