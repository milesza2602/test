--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_001U
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_001U" 
(p_forall_limit in integer,p_success out boolean
,p_from_loc_no in integer,p_to_loc_no in integer
) as
--**************************************************************************************************
--  Date:        October 2008
--  Author:      Alfonso Joshua
--  Purpose:     Load daily forecast table in performance layer
--               with input ex RDF Sale table from foundation layer.
--  Tables:      Input  - temp_loc_item_dy_rdf_sysfcst and temp_loc_item_dy_rdf_appfcst
--               Output - rtl_loc_item_dy_rdf_fcst
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  16 Feb 2009 - A. Joshua : TD-390  - Include ETL to fcst_err_sls_dly_app_fcst_qty,
--                                                    fcst_err_sales_dly_app_fcst,
--                                                    fcst_err_sls_dly_sys_fcst_qty,
--                                                    fcst_err_sales_dly_sys_fcst_qty
--  29 Apr 2009 - A. Joshua : TD-1490 - Remove lookup to table rtl_loc_item_dy_catalog
--                                    - The fcst_err* measures are now catered for in wh_prf_rdf_001c
--
--  30 june 2011 - W. Lyttle : TD-4328 - DATA FIX: De-vat Price values on RDF tables.
--                                       (see comments where code3 changed)
--  23 Aug 2011  - w.lyttle   :QC4328 - add read to DIM_ITEM and DIM_LOCATION and hence extra fields
--                                      to temp_loc_item_dy_rdf_sysfcst
--
--
--  12 Sept 2013     qc4918 STATIC_WK_DLY_APP_FCST_QTY - NEW FIELD
--                          Static field only updated on Sunday evening batch at the last update of this record.
--                          This field must be preserved for the rest of the week, so if the post_date is a Wednesday,
--                          the entry for this field that was made on the Monday evening must be preserved
--                          (possibly need an audit date for create date for this field)
--                          ie.Due to the fact that there is a requirement to have the 'first-loaded' values of the week
--                             available for calculations in reporting, we have to 'keep' the values loaded at the
--                             beginning of each week for the first week
--                             eg.
--                                                    batch_date=8-sept-2013      batch_date=9-sept-2013
--                             item_no  postdate	    APP	STATIC		              APP	STATIC
--                             100     09-Sep-2013	  10	10	    	              12	10
--                             100     10-Sep-2013	  20	20	    	              22	20
--                             100     11-Sep-2013	  30	30	    	              32	30
--                             100     12-Sep-2013	  40	40	    	              42	40
--                             100     13-Sep-2013	  50	50	    	              52	50
--                             100     14-Sep-2013	  60	60	    	              62	60
--                             100     15-Sep-2013	  70	70	    	              72	70
--  23 oct 2013   On recommendation of Sean P......
--                change hint to include index(rtl_loc_item_dy_rdf_fcst PK_P_RTL_LC_ITM_DY_FCST)
--                remove hint use_index (rtl) 
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
g_recs_ignored       integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_today_fin_day_no   number        :=  0;
g_fd_num_catlg_days  number        :=  0;
g_sales_dly_app_fcst number(14,2)  :=  0;
g_sales_dly_sys_fcst number(14,2)  :=  0;
g_sales_dly_app_fcst_qty number(14,3) :=  0;
g_sales_dly_sys_fcst_qty number(14,3) :=  0;
g_rec_out            DWH_PERFORMANCE.rtl_loc_item_dy_rdf_fcst%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
--l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_001U_'|| p_from_loc_no;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_001U_'|| p_from_loc_no;
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD RDF DAILY FCST FACTS EX TEMP TABLES';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of DWH_PERFORMANCE.rtl_loc_item_dy_rdf_fcst%rowtype index by binary_integer;
type tbl_array_u is table of DWH_PERFORMANCE.rtl_loc_item_dy_rdf_fcst%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_item_wk_dyfcst is
WITH SELRDF
AS(
SELECT /*+ full (sys_ilv) parallel (sys_ilv,6) index(rtl_loc_item_dy_rdf_fcst PK_P_RTL_LC_ITM_DY_FCST) */
--use_index (rtl) */
  sys_ilv.location_no,
  sys_ilv.item_no,
  sys_ilv.post_date,
  sys_ilv.sales_dly_sys_fcst_qty ,
  sys_ilv.sales_dly_app_fcst_qty,
  sys_ilv.sk1_item_no,
  sys_ilv.sk1_location_no,
  sys_ilv.vat_rate_perc,
  sys_ilv.reg_rsp,
  sys_ilv.standard_uom_code,
  sys_ilv.random_mass_ind,
  sys_ilv.static_mass,
  NVL(rtl.sales_dly_sys_fcst,0) sales_dly_sys_fcst,
  NVL(rtl.sales_dly_app_fcst,0) sales_dly_app_fcst,
  case when to_char(g_date,'DY') = 'SUN'
  then
  NVL(sys_ilv.STATIC_WK_DLY_APP_FCST_QTY,0)
  else
  NVL(rtl.STATIC_WK_DLY_APP_FCST_QTY,0)
  end STATIC_WK_DLY_APP_FCST_QTY
  FROM dwh_performance.temp_loc_item_dy_rdf_sysfcst sys_ilv
   LEFT OUTER JOIN DWH_PERFORMANCE.rtl_loc_item_dy_rdf_fcst rtl
ON rtl.sk1_item_no      = sys_ilv.sk1_item_no
AND rtl.sk1_location_no = sys_ilv.sk1_location_no
AND rtl.post_date       = sys_ilv.post_date
WHERE sys_ilv.location_no BETWEEN P_FROM_LOC_NO AND P_TO_LOC_NO
order by sys_ilv.post_date
)
--SELECT /*+ parallel (sys_ilv,2) parallel (rtl,2) */
SELECT
  SR.location_no,
  SR.item_no,
  SR.post_date,
  SR.sales_dly_sys_fcst_qty ,
  SR.sales_dly_app_fcst_qty,
  SR.sk1_item_no,
  SR.sk1_location_no,
  dih.sk2_item_no,
  dlh.sk2_location_no,
  SR.vat_rate_perc,
  SR.reg_rsp,
  SR.standard_uom_code,
  SR.random_mass_ind,
  SR.static_mass,
  SR.sales_dly_sys_fcst,
  SR.sales_dly_app_fcst,
  SR.STATIC_WK_DLY_APP_FCST_QTY
FROM SELRDF SR
JOIN dim_item_hist dih
ON SR.item_no = dih.item_no
AND SR.post_date BETWEEN dih.sk2_active_from_date AND dih.sk2_active_to_date
JOIN dim_location_hist dlh
ON SR.location_no = dlh.location_no
AND SR.post_date BETWEEN dlh.sk2_active_from_date AND dlh.sk2_active_to_date
WHERE 
(
  CAST((CASE
    WHEN SR.standard_uom_code = 'EA'
    AND SR.random_mass_ind    = 1
    THEN SR.sales_dly_sys_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc))* SR.static_mass + 0.005
    ELSE SR.sales_dly_sys_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc)) +0.005
  END) AS NUMBER(14,2)) <> CAST((NVL(SR.sales_dly_sys_fcst,0)) AS NUMBER(14,2))
OR
  CAST((CASE
    WHEN SR.standard_uom_code = 'EA'
    AND SR.random_mass_ind    = 1
    THEN SR.sales_dly_app_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc))* SR.static_mass + 0.005
    ELSE SR.sales_dly_app_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc)) +0.005
  END) AS NUMBER(14,2)) <> CAST((NVL(SR.sales_dly_app_fcst,0)) AS NUMBER(14,2))
or
((to_char(g_date,'DY') = 'SUN' and (SR.STATIC_WK_DLY_APP_FCST_QTY <> 0 or SR.STATIC_WK_DLY_APP_FCST_QTY is not null)))
)
  ;
/*WHERE (
  CAST((CASE
    WHEN SR.standard_uom_code = 'EA'
    AND SR.random_mass_ind    = 1
    THEN SR.sales_dly_sys_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc))* SR.static_mass + 0.005
    ELSE SR.sales_dly_sys_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc)) +0.005
  END) AS NUMBER(14,2)) <> CAST((NVL(SR.sales_dly_sys_fcst,0)) AS NUMBER(14,2))
OR
  CAST((CASE
    WHEN SR.standard_uom_code = 'EA'
    AND SR.random_mass_ind    = 1
    THEN SR.sales_dly_app_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc))* SR.static_mass + 0.005
    ELSE SR.sales_dly_app_fcst_qty * (SR.reg_rsp * 100 / (100 + SR.vat_rate_perc)) +0.005
  END) AS NUMBER(14,2)) <> CAST((NVL(SR.sales_dly_app_fcst,0)) AS NUMBER(14,2)))
  ;
  */
g_rec_in                   c_fnd_rtl_loc_item_wk_dyfcst%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_wk_dyfcst%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sales_dly_sys_fcst_qty          := g_rec_in.sales_dly_sys_fcst_qty;
   g_rec_out.sales_dly_app_fcst_qty          := g_rec_in.sales_dly_app_fcst_qty;
   g_rec_out.STATIC_WK_DLY_APP_FCST_QTY      := g_rec_in.STATIC_WK_DLY_APP_FCST_QTY;

/*
   g_rec_out.sales_dly_sys_fcst              := (((g_rec_in.sales_dly_sys_fcst_qty * g_rec_in.base_rsp_excl_vat)
                                                       * 100 / (100 + g_rec_in.vat_rate_perc)) + 0.005);

   g_rec_out.sales_dly_app_fcst              := (((g_rec_in.sales_dly_app_fcst_qty * g_rec_in.base_rsp_excl_vat)
                                                       * 100 / (100 + g_rec_in.vat_rate_perc)) + 0.005);
*/
-- QC 4328 The following code has been commented out
-- A great deal of confusion arose regarding the vatting and de-vatting required.
-- As the code contained NO de-vatting, the business decided that it should be de-vatted.
--
--if g_rec_in.standard_uom_code = 'EA' and
--      g_rec_in.random_mass_ind   = 1 then
--      g_rec_out.sales_dly_sys_fcst           := g_rec_in.sales_dly_sys_fcst_qty * g_rec_in.reg_rsp * g_rec_in.static_mass;
--      g_rec_out.sales_dly_app_fcst           := g_rec_in.sales_dly_app_fcst_qty * g_rec_in.reg_rsp * g_rec_in.static_mass;
--   else
--      g_rec_out.sales_dly_sys_fcst           := g_rec_in.sales_dly_sys_fcst_qty * g_rec_in.reg_rsp;
--      g_rec_out.sales_dly_app_fcst           := g_rec_in.sales_dly_app_fcst_qty * g_rec_in.reg_rsp;
--   end if;
---------------------------------------------------------------
-- QC 4328 The following code has been commented added : START
If g_rec_in.standard_uom_code  = 'EA' and
    g_rec_in.random_mass_ind  = 1 then
    g_rec_out.sales_dly_sys_fcst  := g_rec_in.sales_dly_sys_fcst_qty * (g_rec_in.reg_rsp * 100 / (100 + g_rec_in.vat_rate_perc))* g_rec_in.static_mass + 0.005;
    g_rec_out.sales_dly_app_fcst  := g_rec_in.sales_dly_app_fcst_qty * (g_rec_in.reg_rsp * 100 / (100 +  g_rec_in.vat_rate_perc))* g_rec_in.static_mass + 0.005;
else
    g_rec_out.sales_dly_sys_fcst  := g_rec_in.sales_dly_sys_fcst_qty * (g_rec_in.reg_rsp * 100 / (100 + g_rec_in.vat_rate_perc)) +0.005;
    g_rec_out.sales_dly_app_fcst  := g_rec_in.sales_dly_app_fcst_qty * (g_rec_in.reg_rsp * 100 / (100 + g_rec_in.vat_rate_perc)) + 0.005;
end if;
-- QC 4328  : END

   g_rec_out.last_updated_date               := g_date;

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
       insert into DWH_PERFORMANCE.rtl_loc_item_dy_rdf_fcst values a_tbl_insert(i);

--    g_recs_inserted := g_recs_inserted + a_tbl_insert.count;
    g_recs_inserted := g_recs_inserted + sql%rowcount;

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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).post_date;
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
       update DWH_PERFORMANCE.rtl_loc_item_dy_rdf_fcst
       set    sales_dly_sys_fcst_qty        = a_tbl_update(i).sales_dly_sys_fcst_qty,
              sales_dly_sys_fcst            = a_tbl_update(i).sales_dly_sys_fcst,
              sales_dly_app_fcst_qty        = a_tbl_update(i).sales_dly_app_fcst_qty,
              sales_dly_app_fcst            = a_tbl_update(i).sales_dly_app_fcst,
              last_updated_date             = a_tbl_update(i).last_updated_date,
              STATIC_WK_DLY_APP_FCST_QTY    = a_tbl_update(i).STATIC_WK_DLY_APP_FCST_QTY
       where  post_date                     = a_tbl_update(i).post_date and
              sk1_item_no                   = a_tbl_update(i).sk1_item_no      and
              sk1_location_no               = a_tbl_update(i).sk1_location_no; /*
             (sales_dly_sys_fcst_qty       <> a_tbl_update(i).sales_dly_sys_fcst_qty or
              sales_dly_sys_fcst           <> a_tbl_update(i).sales_dly_sys_fcst     or
              sales_dly_app_fcst_qty       <> a_tbl_update(i).sales_dly_app_fcst_qty or
              sales_dly_app_fcst           <> a_tbl_update(i).sales_dly_app_fcst); */

--       g_recs_updated  := g_recs_updated  + a_tbl_update.count;
       g_recs_updated  := g_recs_updated  + sql%rowcount;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).post_date;
          dwh_log.record_error(l_module_name,sqlcode,l_message);
       end loop;
       raise;

end local_bulk_update;

--**************************************************************************************************
-- Write valid data out to the item master table
--**************************************************************************************************
procedure local_write_output as

begin
   g_found := FALSE;
   g_count :=0;
-- Check to see if item is present on table and update/insert accordingly
   select count(1)
   into   g_count
   from   DWH_PERFORMANCE.rtl_loc_item_dy_rdf_fcst
   where  post_date          = g_rec_out.post_date and
          sk1_item_no        = g_rec_out.sk1_item_no and
          sk1_location_no    = g_rec_out.sk1_location_no;

   if g_count = 1 then
      g_found := TRUE;
   end if;
/*
   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).sk1_location_no = g_rec_out.sk1_location_no and
             a_tbl_insert(i).sk1_item_no     = g_rec_out.sk1_item_no and
             a_tbl_insert(i).post_date       = g_rec_out.post_date
             then
            g_found := TRUE;
         end if;
      end loop;
   end if;
*/
-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
      a_count                 := a_count + 1;
   else
  --    if
  --      (g_sales_dly_sys_fcst_qty <> g_rec_out.sales_dly_sys_fcst_qty or
  --       g_sales_dly_sys_fcst     <> g_rec_out.sales_dly_sys_fcst or
  --       g_sales_dly_app_fcst_qty <> g_rec_out.sales_dly_app_fcst_qty or
 --        g_sales_dly_app_fcst     <> g_rec_out.sales_dly_app_fcst) then
         a_count_u                := a_count_u + 1;
         a_tbl_update(a_count_u)  := g_rec_out;
         a_count                  := a_count + 1;
  --    else
  --       g_recs_ignored           := g_recs_ignored + 1;
  --    end if;
   end if;

--   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
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
-- Main process
--**************************************************************************************************
begin
/*
    l_text := 'LOAD OF UPDATE STATS STARTED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'temp_loc_item_dy_rdf_sysfcst', DEGREE => 8);

    DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE',
                                   'temp_loc_item_dy_rdf_appfcst', DEGREE => 8);

    l_text := 'LOAD OF UPDATE STATS ENDED AT - '||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
*/
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD RTL_LOC_ITEM_DY_RDF_FCST EX TEMP TABLES STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--    g_date := '16 september 2013';
    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
execute immediate 'alter session set workarea_size_policy=manual';
execute immediate 'alter session set sort_area_size=100000000';
execute immediate 'alter session enable parallel dml';
--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************
    open c_fnd_rtl_loc_item_wk_dyfcst;
    fetch c_fnd_rtl_loc_item_wk_dyfcst bulk collect into a_stg_input limit g_forall_limit;
    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop
         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 500000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch c_fnd_rtl_loc_item_wk_dyfcst bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close c_fnd_rtl_loc_item_wk_dyfcst;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;
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
    l_text :=  'RECORDS IGNORED '||g_recs_ignored;
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

END WH_PRF_RDF_001U;
