--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_129U_BCK
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_129U_BCK" (p_forall_limit in integer,p_success out boolean) as

--**************************************************************************************************
--  Date:        May 2014
--  Author:      Quentin Smit
--  Purpose:     Create Customer orders and ROQ fact table in the performance layer
--               with input ex JDA table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_dy_ff_ord
--               Output - rtl_loc_item_dy_om_ord
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:

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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_rec_out            rtl_loc_item_dy_om_ord%rowtype;
g_found              boolean;

g_date               date          := trunc(sysdate);
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_129U';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_facts;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_facts;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE CUST ORDERS & ROQ FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;


-- For output arrays into bulk load forall statements --
type tbl_array_m is table of rtl_loc_item_dy_om_ord%rowtype index by binary_integer;
type tbl_array_u is table of rtl_loc_item_dy_om_ord%rowtype index by binary_integer;
a_tbl_merge        tbl_array_m;
a_empty_set_m       tbl_array_m;

a_count             integer       := 0;
a_count_m           integer       := 0;


cursor c_fnd_rtl_loc_item_dy_ff_orq is
   select /*+ parallel(ord,3) parallel(ff_ord,3) parallel(sdn,3) index(prc,PK_P_RTL_LID_RMS_PRCF) index(dir,PK_P_RTL_LC_ITM_DY_STD_ORD)  */
          ord.*,
          di.sk1_item_no,
          di.standard_uom_code,
          di.static_mass,
          di.random_mass_ind,
          di.business_unit_no,
          dl.sk1_location_no,
          dl.sk1_fd_zone_group_zone_no,
          dih.sk2_item_no,
          dlh.sk2_location_no,
          nvl(dir.boh_1_qty,0) + nvl(sdn.sdn_qty,0)  as sod_boh_qty,  -- SDN_1_1   ==> RTL_LOC_ITEM_SO_SDN
                                                                        -- BOH_1_QTY ==> RTL_LOC_ITEM_DY_ST_DIR_ORD
          nvl(prc.case_selling_excl_vat,0) as case_selling_excl_vat,
          nvl(ffd_ord.num_units_per_tray,1) as num_units_per_tray,
          nvl(ffd_ord.num_units_per_tray2,1) as num_units_per_tray2,
          nvl(prc.reg_rsp_excl_vat,0) as reg_rsp_excl_vat,
          nvl(prc.case_cost,0) as case_cost,
          nvl(prc.wac,0) as wac,

          nvl(dir.boh_1_qty,0) boh_1_qty,
          nvl(sdn.sdn_qty,0) sdn_qty

   from   fnd_rtl_loc_item_dy_ff_ord ord
          join dim_item di             on ord.item_no                   = di.item_no
          join dim_location dl         on ord.location_no               = dl.location_no
          join fnd_jdaff_dept_rollout d on d.department_no              = di.department_no

          left outer join rtl_loc_item_so_sdn sdn on dl.sk1_location_no = sdn.sk1_location_no   --PK_RTL_LI_SO_SDN
                                      and di.sk1_item_no                = sdn.sk1_item_no
                                      and ord.post_date                 = sdn.receive_date

          left outer join rtl_loc_item_dy_st_dir_ord dir
                                       on dl.sk1_location_no            = dir.sk1_location_no
                                      and di.sk1_item_no                = dir.sk1_item_no
                                      and ord.post_date                 = dir.post_date

          join dim_item_hist dih       on ord.item_no                = dih.item_no
                                      and ord.post_date   between dih.sk2_active_from_date and dih.sk2_active_to_date
          join dim_location_hist dlh   on ord.location_no            = dlh.location_no
                                      and ord.post_date   between dlh.sk2_active_from_date and dlh.sk2_active_to_date
   
          left outer join fnd_loc_item_dy_ff_ord ffd_ord
                                       on di.item_no                    = ffd_ord.item_no
                                      and dl.location_no                = ffd_ord.location_no
                                      and (ord.post_date - 1)           = ffd_ord.post_date

        --  left outer join rtl_loc_item_dy_rms_price prc
          left outer join RTL_LID_RMS_PRICE_FF prc
                                       on di.sk1_item_no                = prc.sk1_item_no
                                      and dl.sk1_location_no            = prc.sk1_location_no
                                      and (ord.post_date - 1)           = prc.calendar_date
                                      
   where  ord.last_updated_date      = g_date
     and  d.department_live_ind = 'Y'
     --and ord.location_no = 497
     --and di.item_no = 6001065680637
   ;


g_rec_in             c_fnd_rtl_loc_item_dy_ff_orq%rowtype;
-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_dy_ff_orq%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   /*
   l_text := 'ITEM_NO =  '||g_rec_in.item_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'LOCATION_NO =  '||g_rec_in.location_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
   l_text := 'POST_DATE =  '||g_rec_in.post_date;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  */
  
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.roq_qty                         := g_rec_in.roq_qty;
   g_rec_out.cust_order_cases                := g_rec_in.cust_order_cases;
   g_rec_out.last_updated_date               := g_date;
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.sod_boh_qty                     := g_rec_in.sod_boh_qty;
   --g_rec_out.weigh_ind                       := g_rec_in.weigh_ind ;

-- Value add and calculated fields added to performance layer

   g_rec_out.roq_cases                       := '';
   g_rec_out.roq_selling                     := '';
   g_rec_out.roq_cost                        := '';
   g_rec_out.cust_order_qty                  := '';
   g_rec_out.cust_order_selling              := '';
   g_rec_out.cust_order_cost                 := '';


   if g_rec_in.num_units_per_tray = 0  then
      g_rec_in.num_units_per_tray := 1;
   end if;
     

   g_rec_out.num_units_per_tray := g_rec_in.num_units_per_tray;
   g_rec_out.num_units_per_tray2 := g_rec_in.num_units_per_tray2;
   
        
      g_rec_out.roq_cases          := round(nvl(g_rec_out.roq_qty,0) / g_rec_in.num_units_per_tray,0);
      g_rec_out.roq_selling        := g_rec_out.roq_cases * g_rec_in.case_selling_excl_vat;
      g_rec_out.roq_cost           := g_rec_out.roq_cases * g_rec_in.case_cost;
      g_rec_out.cust_order_qty     := g_rec_out.cust_order_cases * g_rec_in.num_units_per_tray;
      g_rec_out.cust_order_selling := g_rec_out.cust_order_cases * g_rec_in.case_selling_excl_vat;
      g_rec_out.cust_order_cost    := g_rec_out.cust_order_cases * g_rec_in.case_cost;

/*
      l_text := 'BOH_1_QTY =  '||g_rec_in.boh_1_qty;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'SDN_QTY =  '||g_rec_in.sdn_qty;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

      l_text := 'RANDOM_MASS_IND =  '||g_rec_in.random_mass_ind;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'STANDARD_UOM_CODE =  '||g_rec_in.standard_uom_code;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'RANDOM_MASS_IND =  '||g_rec_in.random_mass_ind;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'SOD_BOH_QTY =  '||g_rec_in.sod_boh_qty;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'REG_RSP_EXCL_VAT =  '||g_rec_in.reg_rsp_excl_vat;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'STATIC_MASS =  '||g_rec_in.static_mass;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      l_text := 'WAC =  '||g_rec_in.wac;
      dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/
      if g_rec_in.standard_uom_code = 'EA' and g_rec_in.random_mass_ind = 1 then
         g_rec_out.sod_boh_selling    := g_rec_in.sod_boh_qty * g_rec_in.reg_rsp_excl_vat * g_rec_in.static_mass;
      else
         --l_text := 'SOC_BOH_SELLING = '|| ' SOD_BOH_QTY (' ||g_rec_in.sod_boh_qty ||') * REG_RSP_EXCL_VAL ('||g_rec_in.reg_rsp_excl_vat||')';
         --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         g_rec_out.sod_boh_selling    := g_rec_in.sod_boh_qty * g_rec_in.reg_rsp_excl_vat;
      end if;
      g_rec_out.sod_boh_cost       := g_rec_in.sod_boh_qty * g_rec_in.wac;

      --l_text := 'SOD_BOH_SELLING =  '||g_rec_out.sod_boh_selling;
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
      --l_text := 'SOD_BOH_COST =  '||g_rec_out.sod_boh_cost;
      --dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
     when others then
       l_message := dwh_constants.vc_err_av_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       raise;

end local_address_variables;


--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts  to output table
--**************************************************************************************************
procedure local_bulk_merge as
begin
    forall i in a_tbl_merge.first .. a_tbl_merge.last
       save exceptions

merge into rtl_loc_item_dy_om_ord rtl_lidoo USING
(select a_tbl_merge(i).sk1_location_no           as	sk1_location_no,
        a_tbl_merge(i).sk1_item_no               as	sk1_item_no,
        a_tbl_merge(i).post_date                 as	post_date,
        a_tbl_merge(i).sk2_location_no           as	sk2_location_no,
        a_tbl_merge(i).sk2_item_no               as	sk2_item_no,
        a_tbl_merge(i).roq_qty                   as	roq_qty,
        a_tbl_merge(i).roq_cases                 as	roq_cases,
        a_tbl_merge(i).roq_selling               as	roq_selling,
        a_tbl_merge(i).roq_cost                  as	roq_cost,
        a_tbl_merge(i).cust_order_qty            as	cust_order_qty,
        a_tbl_merge(i).cust_order_cases          as	cust_order_cases,
        a_tbl_merge(i).cust_order_selling        as	cust_order_selling,
        a_tbl_merge(i).cust_order_cost           as	cust_order_cost,
        a_tbl_merge(i).sod_boh_qty               as	sod_boh_qty,
        a_tbl_merge(i).last_updated_date         as	last_updated_date,
        a_tbl_merge(i).sod_boh_selling           as	sod_boh_selling,
        a_tbl_merge(i).sod_boh_cost              as	sod_boh_cost,
        a_tbl_merge(i).num_units_per_tray        as	num_units_per_tray,
        a_tbl_merge(i).num_units_per_tray2       as	num_units_per_tray2
from dual) mer_lidoo
on  (rtl_lidoo.sk1_location_no  = mer_lidoo.sk1_location_no
and rtl_lidoo.sk1_item_no       = mer_lidoo.sk1_item_no
and rtl_lidoo.post_date         = mer_lidoo.post_date)
when matched then
update
set
       roq_qty                         = mer_lidoo.roq_qty,
       roq_cases                       = mer_lidoo.roq_cases,
       roq_selling                     = mer_lidoo.roq_selling,
       roq_cost                        = mer_lidoo.roq_cost,
       cust_order_qty                  = mer_lidoo.cust_order_qty,
       cust_order_cases                = mer_lidoo.cust_order_cases,
       cust_order_selling              = mer_lidoo.cust_order_selling,
       cust_order_cost                 = mer_lidoo.cust_order_cost,
       sod_boh_qty                     = mer_lidoo.sod_boh_qty,
       sod_boh_selling                 = mer_lidoo.sod_boh_selling,
       sod_boh_cost                    = mer_lidoo.sod_boh_cost,
       num_units_per_tray              = mer_lidoo.num_units_per_tray,
       num_units_per_tray2             = mer_lidoo.num_units_per_tray2,
       last_updated_date               = mer_lidoo.last_updated_date
when not matched then
insert
(      rtl_lidoo.sk1_location_no,
       rtl_lidoo.sk1_item_no,
       rtl_lidoo.post_date,
       rtl_lidoo.sk2_location_no,
       rtl_lidoo.sk2_item_no,
       rtl_lidoo.roq_qty,
       rtl_lidoo.roq_cases,
       rtl_lidoo.roq_selling,
       rtl_lidoo.roq_cost,
       rtl_lidoo.cust_order_qty,
       rtl_lidoo.cust_order_cases,
       rtl_lidoo.cust_order_selling,
       rtl_lidoo.cust_order_cost,
       rtl_lidoo.sod_boh_qty,
       rtl_lidoo.last_updated_date,
       rtl_lidoo.sod_boh_selling,
       rtl_lidoo.sod_boh_cost,
       rtl_lidoo.num_units_per_tray,
       rtl_lidoo.num_units_per_tray2
)
values
(      mer_lidoo.sk1_location_no,
       mer_lidoo.sk1_item_no,
       mer_lidoo.post_date,
       mer_lidoo.sk2_location_no,
       mer_lidoo.sk2_item_no,
       mer_lidoo.roq_qty,
       mer_lidoo.roq_cases,
       mer_lidoo.roq_selling,
       mer_lidoo.roq_cost,
       mer_lidoo.cust_order_qty,
       mer_lidoo.cust_order_cases,
       mer_lidoo.cust_order_selling,
       mer_lidoo.cust_order_cost,
       mer_lidoo.sod_boh_qty,
       mer_lidoo.last_updated_date,
       mer_lidoo.sod_boh_selling,
       mer_lidoo.sod_boh_cost,
       mer_lidoo.num_units_per_tray,
       mer_lidoo.num_units_per_tray2
);

    g_recs_inserted := g_recs_inserted + a_tbl_merge.count;

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
                       ' '||a_tbl_merge(g_error_index).sk1_location_no||
                       ' '||a_tbl_merge(g_error_index).sk1_item_no||
                       ' '||a_tbl_merge(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;
end local_bulk_merge;



--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
      a_count_m               := a_count_m + 1;
      a_tbl_merge(a_count_m) := g_rec_out;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_merge;



      a_tbl_merge  := a_empty_set_m;
      a_count_m     := 0;
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

    l_text := 'LOAD OF rtl_loc_item_dy_om_ord EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    Dwh_Lookup.Dim_Control(G_Date);
    --g_date:= '21/MAR/15';
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_loc_item_dy_ff_orq;
    fetch c_fnd_rtl_loc_item_dy_ff_orq bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 100000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_loc_item_dy_ff_orq bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_dy_ff_orq;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************

    local_bulk_merge;


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
end wh_prf_corp_129u_bck;
