--------------------------------------------------------
--  DDL for Procedure WH_PRF_RDF_001A_WL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_RDF_001A_WL" 
(p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
--  Date:        March 2008
--  Author:      Alfonso Joshua
--  Purpose:     Load daily forecast table in performance layer
--               with input ex RDF Sale table from foundation layer.
--  Tables:      Input  - fnd_rtl_loc_item_wk_rdf_dyfcst
--               Output - temp_loc_item_dy_rdf_sysfcst
--  Packages:    constants, dwh_log, dwh_valid
--  Maintenance
--  04 May 2009: TD-1143 - check for data duplication to prevent unique constraint as this program is insert only
--  23 Aug 2011:QC4328 - add read to DIM_ITEM and DIM_LOCATION and hence extra fields
--                        to temp_loc_item_dy_rdf_sysfcst
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
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_count              number        :=  0;
g_fnd_count          number        :=  0;
g_today_fin_day_no   number        :=  0;
g_fd_num_catlg_days  number        :=  0;
g_rec_out            temp_loc_item_dy_rdf_sysfcst%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
L_MESSAGE            SYS_DWH_ERRLOG.LOG_TEXT%TYPE;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_RDF_001A_WL';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rdf;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rdf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'TEMP LOAD RDF DAILY FCST FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i is table of temp_loc_item_dy_rdf_sysfcst%rowtype index by binary_integer;
type tbl_array_u is table of temp_loc_item_dy_rdf_sysfcst%rowtype index by binary_integer;
a_tbl_insert        tbl_array_i;
a_tbl_update        tbl_array_u;
a_empty_set_i       tbl_array_i;
a_empty_set_u       tbl_array_u;

a_count             integer       := 0;
a_count_i           integer       := 0;
a_count_u           integer       := 0;

cursor c_fnd_rtl_loc_item_wk_dyfcst is
SELECT location_no,
  item_no,
  sk1_item_no,
  sk1_location_no,
  standard_uom_code,
  random_mass_ind ,
  vat_rate_perc,
  static_mass,
  wh_fd_zone_group_no,
  wh_fd_zone_no ,
  reg_rsp,
  post_date,
  (post_date + to_number(SUBSTR(syscol,4,2)) -1) post_date_calc,
  syscol,
  sysfcst,
  appfcst,
  static_appfcst
from
  (SELECT  /*+  full(di) full(dl) full(zi) full(fr) parallel (fr,4) */  fr.*,
    sk1_item_no,
    sk1_location_no,
    standard_uom_code,
    random_mass_ind ,
    vat_rate_perc,
    static_mass,
    wh_fd_zone_group_no,
    wh_fd_zone_no ,
    reg_rsp
  FROM fnd_rtl_loc_item_wk_rdf_dyfcst fr,
    dim_item di,
    dim_location dl,
    fnd_zone_item zi
  WHERE fr.item_no                                          = di.item_no
  AND fr.location_no                                        = dl.location_no
  AND dl.wh_fd_zone_group_no                                = zi.zone_group_no
  AND dl.wh_fd_zone_no                                      = zi.zone_no
  AND fr.item_no                                            = zi.item_no
  and fr.last_updated_date                                  = g_date
  and fr.post_date = '08 jun 15'
  ) fr
  unpivot include nulls ((sysfcst,appfcst,static_appfcst) FOR syscol IN ( (dy_01_sys_fcst_qty,dy_01_app_fcst_qty,dy_01_STATIC_APP_FCST_QTY),
  (dy_02_sys_fcst_qty,dy_02_app_fcst_qty,dy_02_STATIC_APP_FCST_QTY), (dy_03_sys_fcst_qty,dy_03_app_fcst_qty,dy_03_STATIC_APP_FCST_QTY),
  (dy_04_sys_fcst_qty,dy_04_app_fcst_qty,dy_04_STATIC_APP_FCST_QTY), (dy_05_sys_fcst_qty,dy_05_app_fcst_qty,dy_05_STATIC_APP_FCST_QTY),
  (dy_06_sys_fcst_qty,dy_06_app_fcst_qty,dy_06_STATIC_APP_FCST_QTY), (dy_07_sys_fcst_qty,dy_07_app_fcst_qty,dy_07_STATIC_APP_FCST_QTY),
  (dy_08_sys_fcst_qty,dy_08_app_fcst_qty,dy_08_STATIC_APP_FCST_QTY), (dy_09_sys_fcst_qty,dy_09_app_fcst_qty,dy_09_STATIC_APP_FCST_QTY),
  (dy_10_sys_fcst_qty,dy_10_app_fcst_qty,dy_10_STATIC_APP_FCST_QTY), (dy_11_sys_fcst_qty,dy_11_app_fcst_qty,dy_11_STATIC_APP_FCST_QTY),
  (dy_12_sys_fcst_qty,dy_12_app_fcst_qty,dy_12_STATIC_APP_FCST_QTY), (dy_13_sys_fcst_qty,dy_13_app_fcst_qty,dy_13_STATIC_APP_FCST_QTY),
  (dy_14_sys_fcst_qty,dy_14_app_fcst_qty,dy_14_STATIC_APP_FCST_QTY), (dy_15_sys_fcst_qty,dy_15_app_fcst_qty,dy_15_STATIC_APP_FCST_QTY),
  (dy_16_sys_fcst_qty,dy_16_app_fcst_qty,dy_16_STATIC_APP_FCST_QTY), (dy_17_sys_fcst_qty,dy_17_app_fcst_qty,dy_17_STATIC_APP_FCST_QTY),
  (dy_18_sys_fcst_qty,dy_18_app_fcst_qty,dy_18_STATIC_APP_FCST_QTY), (dy_19_sys_fcst_qty,dy_19_app_fcst_qty,dy_19_STATIC_APP_FCST_QTY),
  (dy_20_sys_fcst_qty,dy_20_app_fcst_qty,dy_20_STATIC_APP_FCST_QTY), (dy_21_sys_fcst_qty,dy_21_app_fcst_qty,dy_21_STATIC_APP_FCST_QTY)))
WHERE to_number(SUBSTR(syscol,4,2))                         >
  (SELECT (    CASE   WHEN today_fin_day_no = 7   THEN 0
                      ELSE today_fin_day_no
               END) today_fin_day_no
   FROM dim_control  )
ORDER BY location_no,
  item_no,
  post_date DESC
  ;

g_rec_in                   c_fnd_rtl_loc_item_wk_dyfcst%rowtype;

-- For input bulk collect --
type stg_array is table of c_fnd_rtl_loc_item_wk_dyfcst%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin

      g_rec_out.item_no                := g_rec_in.item_no;
      g_rec_out.location_no            := g_rec_in.location_no;
      g_rec_out.sk1_item_no            := g_rec_in.sk1_item_no;
      g_rec_out.sk1_location_no        := g_rec_in.sk1_location_no;
      g_rec_out.standard_uom_code      := g_rec_in.standard_uom_code;
      g_rec_out.random_mass_ind        := g_rec_in.random_mass_ind;
      g_rec_out.vat_rate_perc          := g_rec_in.vat_rate_perc;
      g_rec_out.static_mass            := g_rec_in.static_mass;
      g_rec_out.wh_fd_zone_group_no    := g_rec_in.wh_fd_zone_group_no;
      g_rec_out.wh_fd_zone_no          := g_rec_in.wh_fd_zone_no;
      g_rec_out.reg_rsp                := g_rec_in.reg_rsp;
      g_rec_out.post_date              := g_rec_in.post_date_calc;
      g_rec_out.post_date_orig         := g_rec_in.post_date;
      g_rec_out.sales_dly_sys_fcst_qty := g_rec_in.sysfcst;
      g_rec_out.sales_dly_app_fcst_qty := g_rec_in.appfcst;
      g_rec_out.last_updated_date      := g_date;
      g_rec_out.STATIC_WK_DLY_APP_FCST_QTY  := g_rec_in.static_appfcst;


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
       insert  into dwh_performance.temp_loc_item_dy_rdf_sysfcst  values a_tbl_insert(i);
--     /*+append parallel(mm,4)*/  
 
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
                       ' '||a_tbl_insert(g_error_index).location_no||
                       ' '||a_tbl_insert(g_error_index).item_no||
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
              UPDATE dwh_performance.temp_loc_item_dy_rdf_sysfcst
              SET sales_dly_sys_fcst_qty = a_tbl_update(i).sales_dly_sys_fcst_qty,
                sales_dly_app_fcst_qty   = a_tbl_update(i).sales_dly_app_fcst_qty,
                post_date_orig           = a_tbl_update(i).post_date_orig,
                standard_uom_code        = a_tbl_update(i).standard_uom_code,
                random_mass_ind          = a_tbl_update(i).random_mass_ind,
                vat_rate_perc            = a_tbl_update(i).vat_rate_perc,
                static_mass              = a_tbl_update(i).static_mass,
                wh_fd_zone_group_no      = a_tbl_update(i).wh_fd_zone_group_no,
                wh_fd_zone_no            = a_tbl_update(i).wh_fd_zone_no,
                reg_rsp                  = a_tbl_update(i).reg_rsp,
                STATIC_WK_DLY_APP_FCST_QTY  = a_tbl_update(i).STATIC_WK_DLY_APP_FCST_QTY,
                last_updated_date        = a_tbl_update(i).last_updated_date
              WHERE location_no          = a_tbl_update(i).location_no
              AND item_no                = a_tbl_update(i).item_no
              AND post_date              = a_tbl_update(i).post_date;

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
                       ' '||a_tbl_update(g_error_index).location_no||
                       ' '||a_tbl_update(g_error_index).item_no||
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

   if g_fnd_count > 0 then  /* check done to determine multiple staging loads into foundation - TD-1143 */

--      select count(1)
--      into   g_count
--      from   dwh_performance.temp_loc_item_dy_rdf_sysfcst
--      where  location_no    = g_rec_out.location_no  and
--             item_no        = g_rec_out.item_no      and
 --            post_date      = g_rec_out.post_date;
 
--      if g_count = 1 then
 --        g_found := TRUE;
 --     end if;

      if a_count_i > 0 and not g_found then
         for i in a_tbl_insert.first .. a_tbl_insert.last
         loop
            if  a_tbl_insert(i).location_no = g_rec_out.location_no and
                a_tbl_insert(i).item_no     = g_rec_out.item_no and
                a_tbl_insert(i).post_date   = g_rec_out.post_date
              then
               g_found := TRUE;
           end if;
         end loop;
      end if;
   end if;
-- Place data into and array for later writing to table in bulk
   if not g_found then
      a_count_i               := a_count_i + 1;
      a_tbl_insert(a_count_i) := g_rec_out;
--   else
--      a_count_u               := a_count_u + 1;
--      a_tbl_update(a_count_u) := g_rec_out;
   end if;

   a_count := a_count + 1;
--**************************************************************************************************
-- Bulk 'write from array' loop controlling bulk inserts and updates to output table
--**************************************************************************************************

   if a_count > g_forall_limit then
      local_bulk_insert;
--      local_bulk_update;
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
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOAD temp_loc_item_dy_rdf_sysfcst EX FOUNDATION STARTED '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
--   execute immediate 'alter session set events ''10046 trace name context forever, level 12''';
 --   g_date := '16 september 2013';

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    execute immediate 'truncate table dwh_performance.temp_loc_item_dy_rdf_sysfcst';
    l_text := 'TABLE temp_loc_item_dy_rdf_sysfcst TRUNCATED.';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--l_text := 'RTL_STOCKOWN_DCR recs deleted = '||g_recs_deleted;
--DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT); 
     L_TEXT := 'TABLE temp_loc_item_dy_rdf_sysfcstupdate stats started.';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
 DBMS_STATS.GATHER_TABLE_STATS ('DWH_PERFORMANCE','TEMP_LOC_ITEM_DY_RDF_SYSFCST', DEGREE => 8);

    L_TEXT := 'TABLE temp_loc_item_dy_rdf_sysfcstupdate stats ended.';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
 
      L_TEXT := 'TABLE fnd_rtl_loc_item_wk_rdf_dyfcstupdate stats started.';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);
 DBMS_STATS.GATHER_TABLE_STATS ('DWH_FOUNDATION.FND_RTL_LOC_ITEM_WK_RDF_DYFCST', DEGREE => 8);

    L_TEXT := 'TABLE FND_RTL_LOC_ITEM_WK_RDF_DYFCST stats ended.';
    DWH_LOG.WRITE_LOG(L_NAME,L_SYSTEM_NAME,L_SCRIPT_NAME,L_PROCEDURE_NAME,L_TEXT);   
    
/*    l_text := '***** DROP INDEX start *****';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text); 
    
    l_text := 'DROP INDEX DWH_PERFORMANCE.I2_TMP_LID_RDF_SYSFCST';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    
execute immediate('DROP INDEX DWH_PERFORMANCE.I2_TMP_LID_RDF_SYSFCST');

    l_text := 'DROP INDEX DWH_PERFORMANCE.I4_TMP_LID_RDF_SYSFCST';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
execute immediate('DROP INDEX DWH_PERFORMANCE.I4_TMP_LID_RDF_SYSFCST');

    l_text := 'DROP INDEX DWH_PERFORMANCE.I6_TMP_LID_RDF_SYSFCST';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
execute immediate('DROP INDEX DWH_PERFORMANCE.I6_TMP_LID_RDF_SYSFCST');
 
    l_text := '***** DROP INDEX end *****';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
 */    

/*  TD-1143
    This check will determine whether multiple loads are present on the foundation table (fnd_rtl_loc_item_wk_rdf_dyfcst)
    Should there be duplicate item/location combinations then a lookup will be done on the temp table
    prior to data population (temp_loc_item_dy_rdf_sysfcst)
*/
    select count(*)
    into g_fnd_count
    from
      (select count(*), item_no, location_no
       from fnd_rtl_loc_item_wk_rdf_dyfcst
       where last_updated_date = g_date
       group by item_no, location_no
       having count(*) > 1);

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
--    local_bulk_update;

    l_text := '***** CREATE INDEX start *****';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   

    l_text := 'CREATE INDEX DWH_PERFORMANCE.I2_TMP_LID_RDF_SYSFCST';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
execute immediate('CREATE INDEX DWH_PERFORMANCE.I2_TMP_LID_RDF_SYSFCST
   ON DWH_PERFORMANCE.TEMP_LOC_ITEM_DY_RDF_SYSFCST (SK1_LOCATION_NO)
   TABLESPACE PRF_MASTER
   PARALLEL (DEGREE 8)
   NOLOGGING');
   commit;

execute immediate('ALTER INDEX DWH_PERFORMANCE.I2_TMP_LID_RDF_SYSFCST LOGGING NOPARALLEL');
   commit;

    l_text := 'CREATE INDEX DWH_PERFORMANCE.I4_TMP_LID_RDF_SYSFCST';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
execute immediate('CREATE INDEX DWH_PERFORMANCE.I4_TMP_LID_RDF_SYSFCST
   ON DWH_PERFORMANCE.TEMP_LOC_ITEM_DY_RDF_SYSFCST (SK1_ITEM_NO)
   PARALLEL (DEGREE 8)
   NOLOGGING
   TABLESPACE PRF_MASTER');
   commit;


execute immediate('ALTER INDEX DWH_PERFORMANCE.I4_TMP_LID_RDF_SYSFCST LOGGING NOPARALLEL');
   commit;

    l_text := 'CREATE INDEX DWH_PERFORMANCE.I6_TMP_LID_RDF_SYSFCST';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   
execute immediate('CREATE INDEX DWH_PERFORMANCE.I6_TMP_LID_RDF_SYSFCST
   ON DWH_PERFORMANCE.TEMP_LOC_ITEM_DY_RDF_SYSFCST (POST_DATE)
   PARALLEL (DEGREE 8)
   NOLOGGING
   TABLESPACE PRF_MASTER');
   commit;


execute immediate('ALTER INDEX DWH_PERFORMANCE.I6_TMP_LID_RDF_SYSFCST LOGGING NOPARALLEL');
   commit;

    l_text := '***** CREATE INDEX complete *****';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);   




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


END WH_PRF_RDF_001A_WL;
