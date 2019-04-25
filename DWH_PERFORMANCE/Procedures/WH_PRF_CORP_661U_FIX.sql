--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_661U_FIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_661U_FIX" 
                                                                                                                                                                (p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:         February 2009
--  Author:       M Munnik
--  Purpose:      Load PO combination fact table in performance layer
--                with input ex Purchase Order RMS table from foundation layer.
--                The combination fact table combines measures from PO's and Shipments.
--                This program loads ONLY the PO info.
--                This program loads the PO info to a record keyed with the not_before_date/not_after_date of the PO.
--                (not_after_date for Long Life Food items and not_before_date for all other)
--                The po_ind is set to 1 for the records loaded from the PO table.
--                The program wh_prf_corp_662u, will load the Shipment info to a record
--                keyed with the actl_rcpt_date of the Shipment.
--                If the not_before_date/not_after_date and actl_rcpt_date are equal,
--                then the PO and Shipment measures will be contained on the same record.
--                If there are no shipments received on the not_before_date/not_after_date,
--                the record will only contain the PO measures.
--                The Bulk Confirm measures are also loaded by this program,
--                because Bulk Confirm is not linked to a receipt date.
--                The table temp_po_list contains the POs updated during batch or between selected from and to not_before_dates.
--                The not_before_date/not_after_date for a PO may change,
--                but this can only happen if there are no shipments yet.
--                This program will delete the records which have not been updated today, but exist in temp_po_list.
--                These will be the records where the not_before_date/not_after_date is the tran_date for the record,
--                but the not_before_date/not_after_date has changed after the record was inserted to rtl_po_supchain_loc_item_dy.
--  Tables:       Input  - fnd_rtl_purchase_order, fnd_rtl_shipment
--                Output - rtl_po_supchain_loc_item_dy
--  Packages:     constants, dwh_log, dwh_valid
--
--  Maintenance:
--  09 Jul 2010 - M Munnik
--                The join between Purchase Orders and Shipments is changed to include location in the join criteria.
--                This will prevent the join to have duplicate records in the resultset.
--                The test to exclude PO's with more than one location, (causing duplicates), has been removed from wh_prf_corp_660u.
--                There are records on the PO table with a null supply_chain_type.
--                A list is created in wh_prf_corp_660u, to get the distinct not null supply_chain_type per PO.
--                However, there are PO's where all supply_chain_type's for the PO are null. In this case, a null is selected.
--                The distinct supply_chain_type is then used for all the records of the PO, regardless of the item and location (in wh_prf_corp_661UD and _662u).
--  2 nov 2011 - qc 4419 - wendy lyttle
--               Procedure Change:WH_PRF_CORP_661U - Use DIM_ITEM.PACK_ITEM_IND to update
--  9 Feb 2012 - qc4603 - wendy lyttle
--               Supplier order fill % : DATA FIX
--               Note : that change is only being made to the cursor and not the actual main body of code
--              -- the default is that it IS A SINGLE_ITEM
--  9 March 2012 - qc4604 - wendy lyttle
--              Composites Correction: Supplier Fillrate for COMPONENTS of a Composite
--              Logic as per qc
--For the incoming item:
--1. Determine, at least, if the item is part of a pack.
--Select pack_item_no from fnd_pack_item_detail where item_no = incoming_item_no;
--2. Use the pack item number: because we want to test of the incoming record
--item number is part of a composite pack
--(N.B - this is done so that we can evaluate and do something for
--composite pack component items only.
--3. Use a new function to detemine if the queried item is a
--composite via this logic (Case pack_item_no from dim_item.item_no
--When (Pack_Item_Ind = 0 And Pack_Item_Simple_Ind = 0) Then '1 - single item'
--When (Pack_Item_Ind = 1 And Pack_Item_Simple_Ind = 1) Then '2 - simple pack'
--When (Pack_Item_Ind = 1 And Pack_Item_Simple_Ind = 0) Then '3 - composite pack'
--Else '0 - not listed' End) New_item_type_Ind,
--4. If New_Item_Type_Ind = '3 - composite pack'
--Then Incoming_Item_No Is_Part_Of_Complex_Pack; And Therefore Set Fill_Rate.

--              --
                --Becks	                    SKU1	      SKU2	      SKU3
                --Pack size	                1	          6	          24
                --Pack_Item_Ind	            0	          1	          1
                --fnd_pack_item_detail	    not null	not null	    null
                --Fill rate call required	  1	          0	          0


                --Yogurt	                  SKU4	    SKU5
                --Pack size	                1	          4
                --Pack_Item_Ind	            0	          1
                --fnd_pack_item_detail	    not null	null
                --Fill rate call required	  1	          0


                --Chips	SKU6
                --Pack size	                1
                --Pack_Item_Ind	            0
                --fnd_pack_item_detail	    null
                --Fill rate call required	  0


                --Composite	SKU7
                --Pack size	                12
                --Pack_Item_Ind	            1
                --fnd_pack_item_detail	    null
                --Fill rate call required	  0

--
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
g_recs_deleted       integer       :=  0;
g_recs_dlet_cnt      integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_found              boolean;
g_param_ind          number        :=  0;
g_date               date          :=  trunc(sysdate);
g_rec_out            rtl_po_supchain_loc_item_dy%rowtype;

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_661U_FIX';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_apps;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_apps;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOADS PO INFO TO PO COMBINATION FACT';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of rtl_po_supchain_loc_item_dy%rowtype index by binary_integer;
type tbl_array_u is table of rtl_po_supchain_loc_item_dy%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

-- Before June 2007, it was allowed to completely cancel a Purchase Order
-- and then re-use the PO Number with a different location, not_before_date, etc. (therefore more than 1 location per PO).
-- The procedures joining Purchase Orders with Shipments, used to only join on po_no and item_no.
-- When doing this join on records where there are more than 1 location per PO, the resultset will contain duplicate records.
-- To prevent this, all PO's with more than one location (duplicate PO numbers), were excluded from all the wh_prf_corp_66* programs.
-- The join between Purchase Orders and Shipments were changed to include location in the join criteria.
-- First join to dim_location with the location_no on the PO.
-- If the location on the PO is a store (loc_type 'S') then use location_no to join to Shipment else use wh_physical_wh_no to join to Shipment.
-- A decision was made and aggreed with Business, to only load PO's from Fin Year 2008 (starting at 25 June 2007) and onwards.

cursor c_fnd_po_ship is
   with
   shipmbulk as
   (select    s.po_no, s.to_loc_no, s.item_no, sum(s.asn_qty) bc_shipment_qty,
              sum(case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                    then (round((s.asn_qty * (s.reg_rsp * 100 / (100 + di.vat_rate_perc)) * di.static_mass),2))
                    else (round((s.asn_qty * (s.reg_rsp * 100 / (100 + di.vat_rate_perc))),2)) end) bc_shipment_selling,
              sum(case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                    then (round((s.asn_qty * s.cost_price * di.static_mass),2))
                    else (round((s.asn_qty * s.cost_price),2)) end) bc_shipment_cost
    from      fnd_rtl_shipment s
    join      temp_po_list pl    on  s.po_no = pl.po_no and s.item_no = pl.item_no
    join      dim_item di        on  pl.item_no = di.item_no
    where     s.shipment_status_code = 'I'
    and       s.asn_id like '%BULK%'
    group by  s.po_no, s.to_loc_no, s.item_no),

   purchord as
   (select    p.po_no, pl.supply_chain_type, p.location_no, p.item_no, di.business_unit_no, pl.param_ind,
              pl.sk1_po_no, nvl(dsc.sk1_supply_chain_no,0) sk1_supply_chain_no, dl.sk1_location_no, di.sk1_item_no,
              nvl(dc.sk1_contract_no, 0) sk1_contract_no, nvl(ds.sk1_supplier_no, 0) sk1_supplier_no,
              dlh.sk2_location_no, dih.sk2_item_no, di.fd_discipline_type, di.standard_uom_code, di.random_mass_ind,
              dd.dept_placeholder_02_ind, dd.dept_placeholder_03_ind,
              p.not_before_date, p.not_after_date, p.po_status_code, p.cancel_code, p.reg_rsp, zif.reg_rsp fd_from_loc_reg_rsp,
              p.cost_price, di.vat_rate_perc, di.static_mass,
              nvl(ldd.debtors_commission_perc,0) debtors_commission_perc, dl.chain_no, di.pack_item_simple_ind,
              decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray) num_units_per_tray,
              (case when iu501.item_no is null then 0 else 1 end) uda_item501,
              (case when iu1601.item_no is null then 0 else to_number(iu1601.uda_value_no_or_text_or_date) end) uda_item1601,
              dd.jv_dept_ind, dd.packaging_dept_ind, dd.gifting_dept_ind, dd.non_core_dept_ind, dd.bucket_dept_ind,
              dd.book_magazine_dept_ind,
     -- Code for determining a single-item is being replaced by qc4603
     -- qc4419        (case when pi.item_no is null then 0 else 1 end) single_item_ind,
     -- qc4603         (case when di.pack_item_ind = 1 then 0 else 1 end) single_item_ind,
               (case when pi.item_no is null -- eg. yoghurt packsize=4  or composite
                     and di.pack_item_ind = 1
                           then 0
                    when pi.item_no is null -- eg. chips packsize=1
                     and di.pack_item_ind = 0
                           then 0
                    when pi.item_no is not null -- eg.becks packsize=6 if part of packsize=24
                     and di.pack_item_ind = 1
                           then 0
                    when pi.item_no is not null -- eg. yoghurt packsize=1 or becks packsize=1
                     and di.pack_item_ind = 0
                           then 1
                     else
                        1   --- default is single_item
                  end) single_item_ind,
     -- qc4603 code end
              (case di.business_unit_no when 50 then nvl(zif.case_selling_excl_vat,0)
                                        else nvl(zic.case_selling_excl_vat,0) end) case_selling_excl_vat,
              (case di.business_unit_no when 50 then nvl(zif.case_cost,0)
                                        else nvl(zic.case_cost,0) end) case_cost,
              p.supp_pack_size, p.original_po_qty,
              (case when di.business_unit_no = 50 then
                    case when di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                         then round((p.original_po_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc)) * di.static_mass),2)
                         else round((p.original_po_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end
               else round((p.original_po_qty * (p.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end) original_po_selling,
              (case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                    then round((p.original_po_qty * p.cost_price * di.static_mass),2)
                    else round((p.original_po_qty * p.cost_price),2) end) original_po_cost,
              (case when di.business_unit_no = 50 then
                    round((p.original_po_qty/(decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray))),0)
               end) original_po_cases,
              p.amended_po_qty,
              (case when di.business_unit_no = 50 then
                    case when di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                         then round((p.amended_po_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc)) * di.static_mass),2)
                         else round((p.amended_po_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end
               else round((p.amended_po_qty * (p.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end) amended_po_selling,
              (case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                    then round((p.amended_po_qty * p.cost_price * di.static_mass),2)
                    else round((p.amended_po_qty * p.cost_price),2) end) amended_po_cost,
              (case when di.business_unit_no = 50 then
                    round((p.amended_po_qty/(decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray))),0)
               end) amended_po_cases,
              p.buyer_edited_po_qty, p.buyer_edited_cancel_po_qty, p.buyer_edited_cancel_code,
              (case when di.business_unit_no = 50 then
                    case when di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                         then round((p.buyer_edited_po_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc)) * di.static_mass),2)
                         else round((p.buyer_edited_po_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end
               else round((p.buyer_edited_po_qty * (p.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end) buyer_edited_po_selling,
              p.cancel_po_qty,
              (case when di.business_unit_no = 50 then
                    case when di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                         then round((p.cancel_po_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc)) * di.static_mass),2)
                         else round((p.cancel_po_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end
               else round((p.cancel_po_qty * (p.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end) cancel_po_selling,
              (case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                    then round((p.cancel_po_qty * p.cost_price * di.static_mass),2)
                    else round((p.cancel_po_qty * p.cost_price),2) end) cancel_po_cost,
              (case when di.business_unit_no = 50 then
                    round((p.cancel_po_qty/(decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray))),0)
               end) cancel_po_cases,
              (case when di.business_unit_no = 50 then
                    round((p.rejected_cases*(decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray))),3)
               end) rejection_qty,
              p.rejected_cases, p.grn_qty po_grn_qty,
              (case when di.business_unit_no = 50 then
                    case when di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                         then round((p.grn_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc)) * di.static_mass),2)
                         else round((p.grn_qty * (zif.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end
               else round((p.grn_qty * (p.reg_rsp * 100 / (100 + di.vat_rate_perc))),2) end) po_grn_selling,
              (case when di.business_unit_no = 50 and di.standard_uom_code = 'EA' and di.random_mass_ind = 1
                    then round((p.grn_qty * p.cost_price * di.static_mass),2)
                    else round((p.grn_qty * p.cost_price),2) end) po_grn_cost,
              (case when di.business_unit_no = 50 then
                    round((p.grn_qty/(decode(nvl(li.num_units_per_tray,0),0,1,li.num_units_per_tray))),0)
               end) po_grn_cases
    from      fnd_rtl_purchase_order p
    join      temp_po_list pl            on  p.po_no              = pl.po_no
                                         and p.item_no            = pl.item_no
    join      dim_item di                on  pl.item_no           = di.item_no
    join      dim_item_hist dih          on  pl.item_no           = dih.item_no
                                         and p.not_before_date    between dih.sk2_active_from_date and dih.sk2_active_to_date
    join      dim_location dl            on  p.location_no        = dl.location_no
    join      dim_location_hist dlh      on  p.location_no        = dlh.location_no
                                         and p.not_before_date    between dlh.sk2_active_from_date and dlh.sk2_active_to_date
    join      dim_department dd          on  dd.department_no     = di.department_no
    left join dim_supply_chain_type dsc  on  pl.supply_chain_type = dsc.supply_chain_code
    left join dim_contract dc            on  p.contract_no        = dc.contract_no
    left join dim_supplier ds            on  p.supplier_no        = ds.supplier_no
    left join dim_location dl2           on  p.distribute_from_location_no = dl2.location_no
    left join rtl_zone_item_om zif       on  dl2.sk1_fd_zone_group_zone_no = zif.sk1_zone_group_zone_no
                                         and di.sk1_item_no       = zif.sk1_item_no
    left join rtl_zone_item_om zic       on  dl.sk1_ch_zone_group_zone_no = zic.sk1_zone_group_zone_no
                                         and di.sk1_item_no       = zic.sk1_item_no
--    left join rtl_loc_item_dy_catalog li on  dl.sk1_location_no   = li.sk1_location_no
--                                         and di.sk1_item_no       = li.sk1_item_no
--                                         and (case di.fd_discipline_type when 'SA' then nvl(p.not_after_date,p.not_before_date)
--                                                                         when 'SF' then nvl(p.not_after_date,p.not_before_date)
--                                                                         else p.not_before_date end)
--                                                                  = li.calendar_date
    left join rtl_location_item li       on  di.sk1_item_no       = li.sk1_item_no
                                         and dl.sk1_location_no   = li.sk1_location_no
    left join rtl_loc_dept_dy ldd        on  dl.sk1_location_no   = ldd.sk1_location_no
                                         and di.sk1_department_no = ldd.sk1_department_no
                                         and (case di.fd_discipline_type when 'SA' then nvl(p.not_after_date,p.not_before_date)
                                                                         when 'SF' then nvl(p.not_after_date,p.not_before_date)
                                                                         else p.not_before_date end)
                                                                  = ldd.post_date
    left join (select /*+ push_subq */
               item_no from fnd_item_uda where uda_no = 501 and uda_value_no_or_text_or_date = '2') iu501
                                         on  p.item_no            = iu501.item_no
    left join (select /*+ push_subq */
                      item_no, uda_value_no_or_text_or_date
               from   fnd_item_uda
               where  uda_no = 1601 and uda_value_no_or_text_or_date in('1','2')) iu1601
                                         on  p.item_no            = iu1601.item_no
     -- Code for determining a single-item is being replaced by qc4603
--qc4419    left join fnd_pack_item_detail pi    on  p.item_no            = pi.item_no
--    where     p.not_before_date > '24 Jun 2007'
     left join (select distinct(item_no) from
                fnd_pack_item_detail ) pi
                on  p.item_no            = pi.item_no
-- qc4603 code end
     where  (p.chain_code <> 'DJ' or p.chain_code is null)
    )

    select     p.PO_NO,p.SUPPLY_CHAIN_TYPE,p.LOCATION_NO,p.ITEM_NO,p.BUSINESS_UNIT_NO,
p.PARAM_IND,p.SK1_PO_NO,p.SK1_SUPPLY_CHAIN_NO,p.SK1_LOCATION_NO,p.SK1_ITEM_NO,
p.SK1_CONTRACT_NO,p.SK1_SUPPLIER_NO,p.SK2_LOCATION_NO,p.SK2_ITEM_NO,p.FD_DISCIPLINE_TYPE,
p.STANDARD_UOM_CODE,p.RANDOM_MASS_IND,p.DEPT_PLACEHOLDER_02_IND,p.DEPT_PLACEHOLDER_03_IND,
p.NOT_BEFORE_DATE,p.NOT_AFTER_DATE,p.PO_STATUS_CODE,p.CANCEL_CODE,p.REG_RSP,
p.FD_FROM_LOC_REG_RSP,p.COST_PRICE,p.VAT_RATE_PERC,p.STATIC_MASS,p.DEBTORS_COMMISSION_PERC,
p.CHAIN_NO,p.PACK_ITEM_SIMPLE_IND,p.NUM_UNITS_PER_TRAY,p.UDA_ITEM501,p.UDA_ITEM1601,
p.JV_DEPT_IND,p.PACKAGING_DEPT_IND,p.GIFTING_DEPT_IND,p.NON_CORE_DEPT_IND,
p.BUCKET_DEPT_IND,p.BOOK_MAGAZINE_DEPT_IND,p.SINGLE_ITEM_IND,p.CASE_SELLING_EXCL_VAT,
p.CASE_COST,p.SUPP_PACK_SIZE,p.ORIGINAL_PO_QTY,p.ORIGINAL_PO_SELLING,
p.ORIGINAL_PO_COST,p.ORIGINAL_PO_CASES,p.AMENDED_PO_QTY,p.AMENDED_PO_SELLING,
p.AMENDED_PO_COST,p.AMENDED_PO_CASES,p.BUYER_EDITED_PO_QTY,p.BUYER_EDITED_CANCEL_PO_QTY,
p.BUYER_EDITED_CANCEL_CODE,p.BUYER_EDITED_PO_SELLING,p.CANCEL_PO_QTY,
p.CANCEL_PO_SELLING,p.CANCEL_PO_COST,p.CANCEL_PO_CASES,p.REJECTION_QTY,
P.Rejected_Cases,P.Po_Grn_Qty,P.Po_Grn_Selling,P.Po_Grn_Cost,P.Po_Grn_Cases,
Bc_Shipment_Qty,Bc_Shipment_Selling,Bc_Shipment_Cost,
 -- QC4604 START
(Case
When (Dit.Pack_Item_Ind = 0 And Dit.Pack_Item_Simple_Ind = 0) Then '1'-- - single item
When (Dit.Pack_Item_Ind = 1 And Dit.Pack_Item_Simple_Ind = 1) Then '2'-- - simple pack
When (Dit.Pack_Item_Ind = 1 And Dit.Pack_Item_Simple_Ind = 0) Then '3'-- - composite pack
Else '0'-- - not listed
End) New_Item_Type_Ind
 -- QC4604 ENDED
    from      purchord p
    join      dim_location l on p.location_no = l.location_no
    Left Join Shipmbulk Sb
    on        p.po_no      = sb.po_no
    and       p.item_no    = sb.item_no
    And       Sb.To_Loc_No = (Case L.Loc_Type When 'S' Then L.Location_No Else L.Wh_Physical_Wh_No End)
 -- QC4604 START
Left Join  Fnd_Pack_Item_Detail Pdit
ON      pDit.Item_No    = P.Item_No
Left Join Dim_Item Dit
On      Dit.Item_No    = Pdit.Pack_Item_No
 -- QC4604 ENDED
      -- qc4419 start - group by added
group by  p.PO_NO,p.SUPPLY_CHAIN_TYPE,p.LOCATION_NO,p.ITEM_NO,p.BUSINESS_UNIT_NO,
p.PARAM_IND,p.SK1_PO_NO,p.SK1_SUPPLY_CHAIN_NO,p.SK1_LOCATION_NO,p.SK1_ITEM_NO,
p.SK1_CONTRACT_NO,p.SK1_SUPPLIER_NO,p.SK2_LOCATION_NO,p.SK2_ITEM_NO,p.FD_DISCIPLINE_TYPE,
p.STANDARD_UOM_CODE,p.RANDOM_MASS_IND,p.DEPT_PLACEHOLDER_02_IND,p.DEPT_PLACEHOLDER_03_IND,
p.NOT_BEFORE_DATE,p.NOT_AFTER_DATE,p.PO_STATUS_CODE,p.CANCEL_CODE,p.REG_RSP,
p.FD_FROM_LOC_REG_RSP,p.COST_PRICE,p.VAT_RATE_PERC,p.STATIC_MASS,p.DEBTORS_COMMISSION_PERC,
p.CHAIN_NO,p.PACK_ITEM_SIMPLE_IND,p.NUM_UNITS_PER_TRAY,p.UDA_ITEM501,p.UDA_ITEM1601,
p.JV_DEPT_IND,p.PACKAGING_DEPT_IND,p.GIFTING_DEPT_IND,p.NON_CORE_DEPT_IND,
p.BUCKET_DEPT_IND,p.BOOK_MAGAZINE_DEPT_IND,p.SINGLE_ITEM_IND,p.CASE_SELLING_EXCL_VAT,
p.CASE_COST,p.SUPP_PACK_SIZE,p.ORIGINAL_PO_QTY,p.ORIGINAL_PO_SELLING,
p.ORIGINAL_PO_COST,p.ORIGINAL_PO_CASES,p.AMENDED_PO_QTY,p.AMENDED_PO_SELLING,
p.AMENDED_PO_COST,p.AMENDED_PO_CASES,p.BUYER_EDITED_PO_QTY,p.BUYER_EDITED_CANCEL_PO_QTY,
p.BUYER_EDITED_CANCEL_CODE,p.BUYER_EDITED_PO_SELLING,p.CANCEL_PO_QTY,
p.CANCEL_PO_SELLING,p.CANCEL_PO_COST,p.CANCEL_PO_CASES,p.REJECTION_QTY,
p.REJECTED_CASES,p.PO_GRN_QTY,p.PO_GRN_SELLING,p.PO_GRN_COST,p.PO_GRN_CASES,
BC_SHIPMENT_QTY,BC_SHIPMENT_SELLING,BC_SHIPMENT_COST,
(Case
When (Dit.Pack_Item_Ind = 0 And Dit.Pack_Item_Simple_Ind = 0) Then '1'-- - single item
When (Dit.Pack_Item_Ind = 1 And Dit.Pack_Item_Simple_Ind = 1) Then '2'-- - simple pack
When (Dit.Pack_Item_Ind = 1 And Dit.Pack_Item_Simple_Ind = 0) Then '3'-- - composite pack
Else '0'-- - not listed
End);
-- qc4419 end
-- Case quantities can not contain fractions, the case quantity has to be an integer value (ie. 976.0).

g_rec_in             c_fnd_po_ship%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_po_ship%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out                                       := null;

   g_param_ind                                     := g_rec_in.param_ind;
   g_rec_out.sk1_po_no			                       := g_rec_in.sk1_po_no;
   g_rec_out.sk1_supply_chain_no	  	             := g_rec_in.sk1_supply_chain_no;
   g_rec_out.sk1_location_no			                 := g_rec_in.sk1_location_no;
   g_rec_out.sk1_item_no			                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_contract_no		                   := g_rec_in.sk1_contract_no;
   g_rec_out.sk1_supplier_no			                 := g_rec_in.sk1_supplier_no;
   g_rec_out.sk2_location_no			                 := g_rec_in.sk2_location_no;
   g_rec_out.sk2_item_no			                     := g_rec_in.sk2_item_no;
   g_rec_out.po_ind      			                     := 1;
   g_rec_out.not_before_date  			               := g_rec_in.not_before_date;
   g_rec_out.not_after_date		  	                 := g_rec_in.not_after_date;
   g_rec_out.po_status_code			                   := g_rec_in.po_status_code;
   g_rec_out.cancel_code			                     := g_rec_in.cancel_code;
   g_rec_out.reg_rsp                               := g_rec_in.reg_rsp;
   g_rec_out.cost_price                            := g_rec_in.cost_price;
   g_rec_out.case_selling_excl_vat                 := g_rec_in.case_selling_excl_vat;
   g_rec_out.case_cost                             := g_rec_in.case_cost;
   g_rec_out.supp_pack_size                        := g_rec_in.supp_pack_size;
   g_rec_out.original_po_qty			                 := g_rec_in.original_po_qty;
   g_rec_out.original_po_selling		               := g_rec_in.original_po_selling;
   g_rec_out.original_po_cost  			               := g_rec_in.original_po_cost;
   g_rec_out.original_po_cases  			             := g_rec_in.original_po_cases;
   g_rec_out.amended_po_qty		                     := g_rec_in.amended_po_qty;
   g_rec_out.amended_po_selling		                 := g_rec_in.amended_po_selling;
   g_rec_out.amended_po_cost		                   := g_rec_in.amended_po_cost;
   g_rec_out.amended_po_cases		                   := g_rec_in.amended_po_cases;
   g_rec_out.buyer_edited_po_qty		               := g_rec_in.buyer_edited_po_qty;
   g_rec_out.cancel_po_qty			                   := g_rec_in.cancel_po_qty;
   g_rec_out.cancel_po_selling	                   := g_rec_in.cancel_po_selling;
   g_rec_out.cancel_po_cost			                   := g_rec_in.cancel_po_cost;
   g_rec_out.cancel_po_cases  	                   := g_rec_in.cancel_po_cases;
   g_rec_out.rejection_qty		  	                 := g_rec_in.rejection_qty;
   g_rec_out.rejected_cases  	                     := g_rec_in.rejected_cases;
   g_rec_out.po_grn_qty			                       := g_rec_in.po_grn_qty;
   g_rec_out.po_grn_selling			                   := g_rec_in.po_grn_selling;
   g_rec_out.po_grn_cost			                     := g_rec_in.po_grn_cost;
   g_rec_out.po_grn_cases			                     := g_rec_in.po_grn_cases;
   g_rec_out.bc_shipment_qty		                   := g_rec_in.bc_shipment_qty;
   g_rec_out.bc_shipment_selling		               := g_rec_in.bc_shipment_selling;
   g_rec_out.bc_shipment_cost		                   := g_rec_in.bc_shipment_cost;
   g_rec_out.fd_from_loc_reg_rsp                   := g_rec_in.fd_from_loc_reg_rsp;

   g_rec_out.amended_po_margin                     := g_rec_in.amended_po_selling - g_rec_in.amended_po_cost;

   if g_rec_in.fd_discipline_type in('SA','SF') and (g_rec_in.not_after_date is not null) then
      g_rec_out.tran_date			                     := g_rec_in.not_after_date;
   else
      g_rec_out.tran_date			                     := g_rec_in.not_before_date;
   end if;

   if g_rec_in.business_unit_no = 50 then
      if g_rec_in.standard_uom_code = 'EA' and g_rec_in.random_mass_ind = 1 then
         g_rec_out.rejection_selling               := round((g_rec_in.rejected_cases * g_rec_in.case_selling_excl_vat *
                                                           g_rec_in.static_mass),2);
         g_rec_out.rejection_cost                  := round((g_rec_in.rejected_cases * g_rec_in.case_cost *
                                                           g_rec_in.static_mass),2);
      else
         g_rec_out.rejection_selling               := round((g_rec_in.rejected_cases * g_rec_in.case_selling_excl_vat),2);
         g_rec_out.rejection_cost                  := round((g_rec_in.rejected_cases * g_rec_in.case_cost),2);
      end if;
   end if;

   if g_rec_in.chain_no = 20 then
      g_rec_out.po_grn_fr_cost                     := round(g_rec_in.po_grn_cost +
                                                      (g_rec_in.po_grn_cost * (g_rec_in.debtors_commission_perc/100)),2);
   end if;

   if g_rec_in.business_unit_no = 50 then
      if (g_rec_out.amended_po_qty is null) then
         g_rec_out.latest_po_qty		               := g_rec_out.original_po_qty;
         g_rec_out.latest_po_selling	             := g_rec_out.original_po_selling;
         g_rec_out.latest_po_cost		               := g_rec_out.original_po_cost;
      else
         g_rec_out.latest_po_qty		               := g_rec_out.amended_po_qty + nvl(g_rec_out.cancel_po_qty,0);
         g_rec_out.latest_po_selling	             := g_rec_out.amended_po_selling + nvl(g_rec_in.cancel_po_selling,0);
         g_rec_out.latest_po_cost		               := g_rec_out.amended_po_cost + nvl(g_rec_in.cancel_po_cost,0);
      end if;
      g_rec_out.latest_po_cases                    := round((g_rec_out.latest_po_qty
                                                        /(case when nvl(g_rec_in.num_units_per_tray,0) = 0 then 1 else g_rec_in.num_units_per_tray end)),0);
   else
      if g_rec_out.po_status_code = 'C' then
         g_rec_out.latest_po_qty		               := g_rec_out.po_grn_qty;
         g_rec_out.latest_po_selling	             := g_rec_out.po_grn_selling;
         g_rec_out.latest_po_cost		               := g_rec_out.po_grn_cost;
      else
         if g_rec_out.po_status_code = 'A' and (g_rec_out.bc_shipment_qty is not null) then
            g_rec_out.latest_po_qty		             := g_rec_out.bc_shipment_qty;
            g_rec_out.latest_po_selling	           := g_rec_out.bc_shipment_selling;
            g_rec_out.latest_po_cost		           := g_rec_out.bc_shipment_cost;
         else
            g_rec_out.latest_po_qty		             := g_rec_out.amended_po_qty;
            g_rec_out.latest_po_selling	           := g_rec_out.amended_po_selling;
            g_rec_out.latest_po_cost		           := g_rec_out.amended_po_cost;
         end if;
      end if;
   end if;

   if g_rec_in.business_unit_no not in(50, 70) then
      if g_rec_out.po_status_code = 'C' and nvl(g_rec_in.supply_chain_type,'NSC') not in('HS','VMI','NSC')  then
         if (g_rec_in.original_po_qty is null) then
            g_rec_out.fillrate_order_qty           := null;
            g_rec_out.fillrate_order_selling       := null;
         else
            if g_rec_in.buyer_edited_cancel_code = 'B' then
               if g_rec_in.original_po_qty = g_rec_in.buyer_edited_cancel_po_qty then
                  g_rec_out.fillrate_order_qty     := null;
                  g_rec_out.fillrate_order_selling := null;
               else
                  g_rec_out.fillrate_order_qty     := g_rec_in.buyer_edited_po_qty;
                  g_rec_out.fillrate_order_selling := g_rec_in.buyer_edited_po_selling;
               end if;
            else
               g_rec_out.fillrate_order_qty        := g_rec_out.original_po_qty;
               g_rec_out.fillrate_order_selling    := g_rec_out.original_po_selling;
            end if;
         end if;
      end if;
      if (g_rec_in.supply_chain_type is null) or g_rec_in.supply_chain_type <> 'WH' then
         g_rec_out.fillrate_order_excl_wh_qty		   := g_rec_out.fillrate_order_qty;
         g_rec_out.fillrate_order_excl_wh_selling  := g_rec_out.fillrate_order_selling;
      end if;
   end if;

   if g_rec_in.business_unit_no = 50 then
      if ((g_rec_in.cancel_code is null) or g_rec_in.cancel_code <> 'B') and nvl(g_rec_out.po_grn_qty,0) < g_rec_out.latest_po_qty then
         g_rec_out.shorts_qty                      := g_rec_out.latest_po_qty - nvl(g_rec_out.po_grn_qty,0);
         g_rec_out.shorts_selling                  := g_rec_out.latest_po_selling - nvl(g_rec_out.po_grn_selling,0);
         g_rec_out.shorts_cost                     := g_rec_out.latest_po_cost - nvl(g_rec_out.po_grn_cost,0);
         g_rec_out.shorts_cases                    := round((g_rec_out.shorts_qty
                                                        /(case when nvl(g_rec_in.num_units_per_tray,0) = 0 then 1 else g_rec_in.num_units_per_tray end)),0);
      else
         g_rec_out.shorts_qty                      := 0;
         g_rec_out.shorts_selling                  := 0;
         g_rec_out.shorts_cost                     := 0;
         g_rec_out.shorts_cases                    := 0;
      end if;
   end if;

   If G_Rec_In.Business_Unit_No = 50 Then
      if (((g_rec_in.cancel_code is null) or g_rec_in.cancel_code <> 'B')
         and g_rec_in.jv_dept_ind = 0              and g_rec_in.packaging_dept_ind = 0
         And G_Rec_In.Gifting_Dept_Ind = 0         And G_Rec_In.Non_Core_Dept_Ind = 0  And G_Rec_In.Bucket_Dept_Ind = 0
         And G_Rec_In.Book_Magazine_Dept_Ind = 0   And G_Rec_In.Uda_Item501 = 1        And G_Rec_In.Single_Item_Ind = 0 )
 -- QC4604 START
         Or
         G_Rec_In.New_Item_Type_Ind = '3'
 -- QC4604 ENDED
         then
         g_rec_out.fillrate_fd_po_grn_qty          := g_rec_out.po_grn_qty;
         g_rec_out.fillrate_fd_latest_po_qty       := g_rec_out.latest_po_qty;
      end if;
      if g_rec_in.uda_item1601 = 1 then
         g_rec_out.fillrate_fd_po_grn_qty_import   := g_rec_out.fillrate_fd_po_grn_qty;
         g_rec_out.fillrte_fd_latest_po_qty_imprt  := g_rec_out.fillrate_fd_latest_po_qty;
      end if;
      if g_rec_in.uda_item1601 = 2 then
         g_rec_out.fillrate_fd_po_grn_qty_local    := g_rec_out.fillrate_fd_po_grn_qty;
         g_rec_out.fillrte_fd_latest_po_qty_local  := g_rec_out.fillrate_fd_latest_po_qty;
      end if;
   end if;

   g_rec_out.last_updated_date                     := g_date;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;

--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_insert as
begin
    forall i in a_tbl_insert.first .. a_tbl_insert.last
       save exceptions
       insert into rtl_po_supchain_loc_item_dy values a_tbl_insert(i);

    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_insert||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_insert(g_error_index).sk1_po_no||
                       ' '||a_tbl_insert(g_error_index).sk1_supply_chain_no||
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).tran_date;
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
       update rtl_po_supchain_loc_item_dy
       set    
--              sk1_contract_no		              = a_tbl_update(i).sk1_contract_no,
--              sk1_supplier_no			            = a_tbl_update(i).sk1_supplier_no,
--              sk2_location_no			            = a_tbl_update(i).sk2_location_no,
--              sk2_item_no			                = a_tbl_update(i).sk2_item_no,
--              po_ind    			                = a_tbl_update(i).po_ind,
--              not_before_date			            = a_tbl_update(i).not_before_date,
--              not_after_date			            = a_tbl_update(i).not_after_date,
--              po_status_code                  = a_tbl_update(i).po_status_code,
--              cancel_code			                = a_tbl_update(i).cancel_code,
--              reg_rsp                         = a_tbl_update(i).reg_rsp,
--              cost_price                      = a_tbl_update(i).cost_price,
--              case_selling_excl_vat           = a_tbl_update(i).case_selling_excl_vat,
--              case_cost                       = a_tbl_update(i).case_cost,
--              supp_pack_size                  = a_tbl_update(i).supp_pack_size,
--              original_po_qty			            = a_tbl_update(i).original_po_qty,
              original_po_selling		          = a_tbl_update(i).original_po_selling,
--              original_po_cost			          = a_tbl_update(i).original_po_cost,
--              original_po_cases               = a_tbl_update(i).original_po_cases,
--              amended_po_qty		              = a_tbl_update(i).amended_po_qty,
              amended_po_selling		          = a_tbl_update(i).amended_po_selling,
--              amended_po_cost		              = a_tbl_update(i).amended_po_cost,
--              amended_po_cases                = a_tbl_update(i).amended_po_cases,
              amended_po_margin               = a_tbl_update(i).amended_po_margin,
--              buyer_edited_po_qty		          = a_tbl_update(i).buyer_edited_po_qty,
--              cancel_po_qty			              = a_tbl_update(i).cancel_po_qty,
              cancel_po_selling               = a_tbl_update(i).cancel_po_selling,
--              cancel_po_cost                  = a_tbl_update(i).cancel_po_cost,
--              cancel_po_cases                 = a_tbl_update(i).cancel_po_cases,
--              rejection_qty                   = a_tbl_update(i).rejection_qty,
              rejection_selling               = a_tbl_update(i).rejection_selling,
--              rejection_cost                  = a_tbl_update(i).rejection_cost,
--              rejected_cases                  = a_tbl_update(i).rejected_cases,
--              po_grn_qty			                = a_tbl_update(i).po_grn_qty,
              po_grn_selling			            = a_tbl_update(i).po_grn_selling,
--              po_grn_cost			                = a_tbl_update(i).po_grn_cost,
--              po_grn_cases                    = a_tbl_update(i).po_grn_cases,
--              po_grn_fr_cost                  = a_tbl_update(i).po_grn_fr_cost,
--              shorts_qty                      = a_tbl_update(i).shorts_qty,
--              shorts_selling                  = a_tbl_update(i).shorts_selling,
--              shorts_cost                     = a_tbl_update(i).shorts_cost,
--              shorts_cases                    = a_tbl_update(i).shorts_cases,
--              bc_shipment_qty		              = a_tbl_update(i).bc_shipment_qty,
              bc_shipment_selling		          = a_tbl_update(i).bc_shipment_selling,
--              bc_shipment_cost		            = a_tbl_update(i).bc_shipment_cost,
--              latest_po_qty		                = a_tbl_update(i).latest_po_qty,
              latest_po_selling	              = a_tbl_update(i).latest_po_selling,
--              latest_po_cost		              = a_tbl_update(i).latest_po_cost,
--              latest_po_cases                 = a_tbl_update(i).latest_po_cases,
--              latest_po_qty_all_time          = a_tbl_update(i).latest_po_qty_all_time,
--              latest_po_selling_all_time      = a_tbl_update(i).latest_po_selling_all_time,
--              latest_po_cost_all_time         = a_tbl_update(i).latest_po_cost_all_time,
--              avg_po_rsp_excl_vat_all_time    = a_tbl_update(i).avg_po_rsp_excl_vat_all_time,
--              avg_po_cost_price_all_time      = a_tbl_update(i).avg_po_cost_price_all_time,
--              avg_po_margin_perc_all_time     = a_tbl_update(i).avg_po_margin_perc_all_time,
--              fillrate_order_qty              = a_tbl_update(i).fillrate_order_qty,
              fillrate_order_selling          = a_tbl_update(i).fillrate_order_selling,
--              fillrate_order_excl_wh_qty      = a_tbl_update(i).fillrate_order_excl_wh_qty,
              fillrate_order_excl_wh_selling  = a_tbl_update(i).fillrate_order_excl_wh_selling
--              last_updated_date               = a_tbl_update(i).last_updated_date,
--              fillrate_fd_po_grn_qty          = a_tbl_update(i).fillrate_fd_po_grn_qty,
--              fillrate_fd_po_grn_qty_import   = a_tbl_update(i).fillrate_fd_po_grn_qty_import,
--              fillrate_fd_po_grn_qty_local    = a_tbl_update(i).fillrate_fd_po_grn_qty_local,
--              fillrate_fd_latest_po_qty       = a_tbl_update(i).fillrate_fd_latest_po_qty,
--              fillrte_fd_latest_po_qty_imprt  = a_tbl_update(i).fillrte_fd_latest_po_qty_imprt,
--              fillrte_fd_latest_po_qty_local  = a_tbl_update(i).fillrte_fd_latest_po_qty_local,
--              fd_from_loc_reg_rsp             = a_tbl_update(i).fd_from_loc_reg_rsp
       where  sk1_po_no                       = a_tbl_update(i).sk1_po_no
       and    sk1_supply_chain_no             = a_tbl_update(i).sk1_supply_chain_no
       and    sk1_location_no                 = a_tbl_update(i).sk1_location_no
       and    sk1_item_no                     = a_tbl_update(i).sk1_item_no
       and    tran_date                       = a_tbl_update(i).tran_date;

       g_recs_updated  := g_recs_updated  + a_tbl_update.count;

   exception
      when others then
       g_error_count := sql%bulk_exceptions.count;
       l_message := dwh_constants.vc_err_lb_update||g_error_count|| ' '||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       for i in 1 .. g_error_count loop
          g_error_index := sql%bulk_exceptions(i).error_index;
          l_message := dwh_constants.vc_err_lb_loop||i||
                       ' '||g_error_index||
                       ' '||sqlerrm(-sql%bulk_exceptions(i).error_code)||
                       ' '||a_tbl_update(g_error_index).sk1_po_no||
                       ' '||a_tbl_update(g_error_index).sk1_supply_chain_no||
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).tran_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to output table
--**************************************************************************************************
procedure local_write_output as
begin
   g_found := FALSE;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   rtl_po_supchain_loc_item_dy
   where  sk1_po_no             = g_rec_out.sk1_po_no
   and    sk1_supply_chain_no   = g_rec_out.sk1_supply_chain_no
   and    sk1_location_no       = g_rec_out.sk1_location_no
   and    sk1_item_no           = g_rec_out.sk1_item_no
   and    tran_date             = g_rec_out.tran_date;

   if g_count = 1 then
      g_found := TRUE;
   end if;

--Emergency Change to stop unique constraint error
-- Check if insert of item already in insert array and change to put duplicate in update array
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if a_tbl_insert(i).sk1_po_no             = g_rec_out.sk1_po_no and
            a_tbl_insert(i).sk1_supply_chain_no   = g_rec_out.sk1_supply_chain_no and
            a_tbl_insert(i).sk1_location_no       = g_rec_out.sk1_location_no and
            a_tbl_insert(i).sk1_item_no           = g_rec_out.sk1_item_no and
            a_tbl_insert(i).tran_date             = g_rec_out.tran_date then
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

   if a_count > g_forall_limit then
--      local_bulk_insert;
      local_bulk_update;

      a_tbl_insert  := a_empty_set_i;
      a_tbl_update  := a_empty_set_u;
      a_count_i     := 0;
      a_count_u     := 0;
      a_count       := 0;

      commit;
   end if;
   exception
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_write_output;

--**************************************************************************************************
-- Delete records where record has not been updated
--**************************************************************************************************
procedure delete_invalid_recs as
begin

--    insert into temp_po_deletes
--    select /*+ no_index (r) full(r) parallel (r,8) */ r.sk1_po_no, r.sk1_supply_chain_no, r.sk1_location_no, r.sk1_item_no, r.tran_date,
--	         r.sk1_supplier_no, di.sk1_style_colour_no, dc.fin_year_no, dc.fin_week_no
--    from   rtl_po_supchain_loc_item_dy r
--    join   temp_po_list t     on  r.sk1_po_no        = t.sk1_po_no
--                              and r.sk1_item_no      = t.sk1_item_no
--    join   dim_item di        on  t.sk1_item_no      = di.sk1_item_no
--    join   dim_calendar dc    on  r.tran_date        = dc.calendar_date
--    where  r.po_ind = 1
--    and    r.last_updated_date <> g_date;

    g_recs_dlet_cnt := sql%rowcount;
    commit;
    l_text := 'RECORDS TO BE DELETED - '||g_recs_dlet_cnt;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
--    delete from DWH_PERFORMANCE.rtl_po_supchain_loc_item_dy r
--    where  exists(select t.sk1_po_no
--                  from   temp_po_deletes t
--                  where  t.sk1_po_no   = r.sk1_po_no
--                  and    t.sk1_item_no = r.sk1_item_no
--                  and    t.SK1_SUPPLY_CHAIN_NO = r.SK1_SUPPLY_CHAIN_NO
--                  and    t.SK1_LOCATION_NO = r.SK1_LOCATION_NO
--                  and    t.tran_date = r.TRAN_DATE);
--    delete from rtl_po_supchain_loc_item_dy r
--    where  exists(select t.po_no
--                  from   temp_po_list t
--                  join   dim_purchase_order dp on dp.po_no   = t.po_no
--                  join   dim_item di           on di.item_no = t.item_no
--                  where  dp.sk1_po_no   = r.sk1_po_no
--                  and    di.sk1_item_no = r.sk1_item_no)
--    and    po_ind = 1
--    and    last_updated_date <> g_date;

    g_recs_deleted  := g_recs_deleted  + sql%rowcount;
    commit;

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end delete_invalid_recs;

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
    l_text := 'LOAD OF rtl_po_supchain_loc_item_dy EX FOUNDATION STARTED '||
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
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_po_ship;
    fetch c_fnd_po_ship bulk collect into a_stg_input limit g_forall_limit;

    if a_stg_input.count > 0 then
       g_rec_in    := a_stg_input(1);
       g_param_ind := g_rec_in.param_ind;

       if g_param_ind = 1 then
--          execute immediate 'truncate table dwh_performance.temp_po_deletes';
          l_text := 'TABLE temp_po_deletes TRUNCATED.';
          dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
       end if;

    end if;

    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 10000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := null;
         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_po_ship bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_po_ship;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
--    local_bulk_insert;
    local_bulk_update;
    commit;
    if g_param_ind = 1 then
       delete_invalid_recs;
    end if;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,g_recs_deleted,'');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
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

end wh_prf_corp_661U_FIX;
