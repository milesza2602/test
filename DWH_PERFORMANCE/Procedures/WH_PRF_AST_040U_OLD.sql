--------------------------------------------------------
--  DDL for Procedure WH_PRF_AST_040U_OLD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_AST_040U_OLD" 
(p_forall_limit in integer,p_success out boolean,p_from_loc_no in integer,p_to_loc_no in integer) as


--**************************************************************************************************
--  Date:        Jan 2013
--  Author:      Wendy Lyttle
--  Purpose:      Create the daily CHBD item catalog table with RMS stock in the performance layer
--               with input ex RP table from foundation layer.
--
--               Cloned from WH_PRF_RP_001U
--
--  Runtime instructions :
--               Due to the fact that data is sent 1 day ahead of time and that we do not have the
--               stock and sales values at that point,
--               the PERFORMANCE layer is run first in batch before the FOUNDATION layer.
--               In this procedure WH_PRF_AST_040U, 
--                       we select the data based upon the POST_DATE= batch_DATE.
--               Eg. batch_date                = '5 March 2013'
--                   Data sent from srce       = '6 March 2013'
--                   Stock_data for this batch = '5 March 2013'
--                   Therefore, PRD will load with '5 March 2013'
--                         and FND will load with '6 March 2013';
--               In the next procedure WH_PRF_AST_041U, 
--                       we select the data based upon the LAST_UPDATED_DATE= batch_DATE. 
--                       This is due to the fact that sales data can be late
--
--
--  Tables:      Input  - fnd_AST_loc_item_dy_catlg
--               Output - rtl_loc_item_dy_AST_catlg
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  W LYTTLE 15 JUNE 2016 -- TEMP FILTER FOR EXCLUDING CHAIN_NO = 40 ADDED
--                           procedure back = wh_prf_ast_040u_bck150616
--                           chg44990
--  W LYTTLE 28 JUNE 2016 -- TEMP FILTER FOR EXCLUDING CHAIN_NO = 40 removed
--                           chg??
---
--  Naming conventions
--  g_  -  Global variable
--  l_  -  Log table variable
--  a_  -  Array variable
--  v_  -  Local variable as found in packages
--  p_  -  Parameter
--  c_  -  Prefix to cursor
--**************************************************************************************************
g_forall_limit           integer       :=  dwh_constants.vc_forall_limit;
g_recs_read              integer       :=  0;
g_recs_inserted          integer       :=  0;
g_recs_updated           integer       :=  0;
g_error_count            number        :=  0;
g_error_index            number        :=  0;
g_count                  number        :=  0;
g_found                  boolean;
g_rec_out                rtl_loc_item_dy_AST_catlg%rowtype;
g_soh_qty_decatlg        number        :=  0;
g_soh_qty                number        :=  0;
g_soh_selling            number        :=  0;
g_fin_week_no            number        :=  0;
g_fin_year_no            number        :=  0;
g_item_no_decatlg        number(18,0)  :=  0;
g_sk1_item_no_decatlg    number(18,0)  :=  0;

g_date                   date;
g_this_week_start_date   date;
G_THIS_WEEK_END_DATE     date;
G_PART_NAME              varchar2(32)  := 'RLIDRMSSTKC_';

G_CNT NUMBER   :=  0;

g_sql                    varchar2(5000);

 g_sql1        varchar2(100)  :=    'select /*+ USE_HASH ( fnd_lid, di, dih, stk,diu,dcap ) */ ';
-- g_sql1        varchar2(100)  :=    'select /*+ FULL ( fnd_lid,2) FULL(di,2) FULL( dih,2) FULL(stk,2) */ ';

 g_sql2        varchar2(100)  :=    '       dl.sk1_location_no, ';
 g_sql3        varchar2(100)  :=    '       di.sk1_item_no, ';
 g_sql4        varchar2(100)  :=    '       fnd_lid.post_date, ';
 g_sql5        varchar2(100)  :=    '       nvl(DCAP.SK1_AVAIL_UDA_VALUE_NO,0) sk1_avail_uda_value_no, ';

 g_sql6        varchar2(100)  :=    '       dih.sk2_item_no, ';
 g_sql7        varchar2(100)  :=    '       dlh.sk2_location_no, ';
 g_sql8        varchar2(100)  :=    '       0 ch_catalog_ind, ';
 g_sql9        varchar2(100)  :=    '       0 ch_num_avail_days, ';
 g_sql10       varchar2(100)  :=    '       0 ch_num_catlg_days, ';
 g_sql11        varchar2(100)  :=    '       0 reg_sales_qty_catlg, ';
 g_sql12        varchar2(100)  :=    '       0 reg_sales_catlg, ';
 g_sql13        varchar2(100)  :=    '       nvl(stk.reg_soh_qty,0) reg_soh_qty_catlg, ';
 g_sql14        varchar2(100)  :=    '       nvl(stk.reg_soh_selling,0) reg_soh_selling_catlg, ';

 g_sql15        varchar2(100)  :=    '       0 prom_sales_qty_catlg, ';
 g_sql16        varchar2(100)  :=    '       0 prom_sales_catlg, ';

 g_sql17        varchar2(100)  :=    '       0 prom_reg_sales_qty_catlg, ';
 g_sql18        varchar2(100)  :=    '       0 prom_reg_sales_catlg, ';

 g_sql19        varchar2(100)  :=    '       '''||g_date||''' last_update_date, ';
 g_sql22A        varchar2(40)  :=    'NULL PROD_LINK_TYPE, ';
 g_sql22B        varchar2(40)  :=    'NULL SK1_GROUP_ITEM_NO, ';
 g_sql22C        varchar2(40)  :=    'NULL AVAIL_REG_SALES_QTY_CATLG, ';
 g_sql22D        varchar2(40)  :=    'NULL AVAIL_REG_SALES_CATLG, ';
 g_sql22E        varchar2(40)  :=    'NULL AVAIL_REG_SOH_QTY_CATLG, ';
 g_sql22F        varchar2(40)  :=    'NULL AVAIL_REG_SOH_SELLING_CATLG, ';
 g_sql22G        varchar2(40)  :=    'NULL AVAIL_PROM_SALES_QTY_CATLG, ';
 g_sql22H        varchar2(40)  :=    'NULL AVAIL_PROM_SALES_CATLG, ';
 g_sql22I        varchar2(40)  :=    'NULL AVAIL_PROM_REG_SALES_QTY_CATLG, ';
 g_sql22J        varchar2(40)  :=    'NULL AVAIL_PROM_REG_SALES_CATLG, ';
 g_sql22K        varchar2(40)  :=    'NULL AVAIL_CH_NUM_AVAIL_DAYS, ';
 g_sql22L        varchar2(40)  :=    'NULL AVAIL_CH_NUM_CATLG_DAYS, ';
 g_sql22M        varchar2(40)  :=    'NULL prod_link_ind, ';
 g_sql20        varchar2(100)  :=    '       fnd_lid.active_from_date, ';
 g_sql21        varchar2(100)  :=    '       fnd_lid.active_to_date, ';
 g_sql22        varchar2(100)  :=    '       fnd_lid.item_no ';
 g_sql23        varchar2(100)  :=    'from   fnd_ast_loc_item_dy_catlg fnd_lid ';
 g_sql24        varchar2(100)  :=    'join   dim_item di on ';
 g_sql25        varchar2(100)  :=    '       fnd_lid.item_no                      = di.item_no  ';
 g_sql26        varchar2(100)  :=    'join   dim_location dl on ';
 g_sql27        varchar2(100)  :=    '       fnd_lid.location_no                  = dl.location_no ';
 g_sql28        varchar2(100)  :=   'join   dim_item_hist dih on ';
 g_sql29        varchar2(100)  :=    '       fnd_lid.item_no                      = dih.item_no and ';
 g_sql30        varchar2(100)  :=    '       fnd_lid.post_date     between dih.sk2_active_from_date and dih.sk2_active_to_date ';
 g_sql31        varchar2(100)  :=   'join   dim_location_hist dlh on ';
 g_sql32        varchar2(100)  :=    '       fnd_lid.location_no                  = dlh.location_no and ';
 g_sql33        varchar2(100)  :=    '       fnd_lid.post_date     between dlh.sk2_active_from_date and dlh.sk2_active_to_date ';
 g_sql34        varchar2(100)  :=    'join   dim_calendar dc on ';
 G_SQL35        varchar2(100)  :=    '       fnd_lid.post_date                    = dc.calendar_date  ';
 G_SQL36        varchar2(100)  :=    'left outer join   rtl_loc_item_dy_rms_stock  subpartition (';
-- g_sql36        varchar2(100)  :=    'left outer join   rtl_loc_item_dy_rms_stock  subpartition ('||g_part_name||') stk on ';
-- g_sql36        varchar2(100)  :=    'left outer join   rtl_loc_item_dy_rms_stock   stk on ';
 g_sql37        varchar2(100)  :=    '       stk.sk1_item_no                      = di.sk1_item_no and ';
 g_sql38        varchar2(100)  :=    '       stk.sk1_location_no                  = dl.sk1_location_no and ';
 g_sql39        varchar2(100)  :=    '       stk.post_date                        = fnd_lid.post_date ';
 g_sql40        varchar2(100)  :=    'left outer join   dim_item_uda diu on ';
 g_sql41        varchar2(100)  :=    '       diu.item_no                   = fnd_lid.item_no  ';
 g_sql42        varchar2(100)  :=    'left outer join    dim_ch_avail_period dcap on ';
 g_sql43        varchar2(100)  :=    '        dcap.UDA_VALUE_SHORT_DESC             = diu.RANGE_STRUCTURE_CH_DESC_104     ';



type stkcurtyp           is ref cursor;
stk_cv                   stkcurtyp;

l_message                sys_dwh_errlog.log_text%type;
l_module_name            sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_AST_040_OLD_'||p_from_loc_no;
l_name                   sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rpl;
l_system_name            sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_pln_prf;
l_script_name            sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_rpl;
l_procedure_name         sys_dwh_log.log_procedure_name%type       := l_module_name;
L_TEXT                   SYS_DWH_LOG.LOG_TEXT%TYPE ;
l_description            sys_dwh_log_summary.log_description%type  := 'LOAD THE AST DAILY CHBD ITEM CATALOG FACTS EX FOUNDATION';
l_process_type           sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;

-- For output arrays into bulk load forall statements --
type tbl_array_i         is table of rtl_loc_item_dy_AST_catlg%rowtype index by binary_integer;
type tbl_array_u         is table of rtl_loc_item_dy_AST_catlg%rowtype index by binary_integer;
a_tbl_insert             tbl_array_i;
a_tbl_update             tbl_array_u;
a_empty_set_i            tbl_array_i;
a_empty_set_u            tbl_array_u;

a_count                  integer       := 0;
a_count_i                integer       := 0;
a_count_u                integer       := 0;

cursor c_rtl_loc_item_dy_AST_catlg is
   select r.*, trunc(sysdate) active_from_date, trunc(sysdate) active_to_date, i.item_no
   from   rtl_loc_item_dy_AST_catlg r, dim_item i
   where  1 = 2
    and   r.sk1_item_no = i.sk1_item_no;



g_rec_in         c_rtl_loc_item_dy_AST_catlg%rowtype;

-- For input bulk collect --
type stg_array is table of c_rtl_loc_item_dy_AST_catlg%rowtype;
a_stg_input      stg_array;

--**************************************************************************************************
-- Process, transform and validate the data read from the input interface
--**************************************************************************************************
procedure local_address_variables as
begin
   g_rec_out.sk1_item_no                     := g_rec_in.sk1_item_no;
   g_rec_out.sk1_location_no                 := g_rec_in.sk1_location_no;
   g_rec_out.post_date                       := g_rec_in.post_date;
   g_rec_out.sk1_avail_uda_value_no          := g_rec_in.sk1_avail_uda_value_no;

   g_rec_out.sk2_item_no                     := g_rec_in.sk2_item_no;
   g_rec_out.sk2_location_no                 := g_rec_in.sk2_location_no;

   if g_rec_in.active_from_date              <= g_this_week_start_date and
      g_rec_in.active_to_date                >= g_this_week_end_date then
      g_rec_out.ch_catalog_ind               := 1;
      g_rec_out.ch_num_catlg_days            := 1;
   else
      g_rec_out.ch_catalog_ind               := 0;
      g_rec_out.ch_num_catlg_days            := 0;
   end if;

   if g_rec_in.reg_soh_qty_catlg > 0 then
         g_rec_out.ch_num_avail_days         := 1;
   else
          g_rec_out.ch_num_avail_days         := 0;
   end if;

   g_rec_out.reg_soh_qty_catlg               := g_rec_in.reg_soh_qty_catlg;
   g_rec_out.reg_soh_selling_catlg           := g_rec_in.reg_soh_selling_catlg;

   g_rec_out.last_updated_date                := g_date;

   g_rec_out.PROD_LINK_TYPE := NULL;
   g_rec_out.SK1_GROUP_ITEM_NO := NULL;
   g_rec_out.AVAIL_REG_SALES_QTY_CATLG := NULL;
   g_rec_out.AVAIL_REG_SALES_CATLG := NULL;
   g_rec_out.AVAIL_REG_SOH_QTY_CATLG := NULL;
   g_rec_out.AVAIL_REG_SOH_SELLING_CATLG := NULL;
   g_rec_out.AVAIL_PROM_SALES_QTY_CATLG := NULL;
   g_rec_out.AVAIL_PROM_SALES_CATLG := NULL;
   g_rec_out.AVAIL_PROM_REG_SALES_QTY_CATLG := NULL;
   g_rec_out.AVAIL_PROM_REG_SALES_CATLG := NULL;
   g_rec_out.AVAIL_CH_NUM_AVAIL_DAYS := NULL;
   g_rec_out.AVAIL_CH_NUM_CATLG_DAYS := NULL;
   g_rec_out.prod_link_ind := NULL;

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
       insert into rtl_loc_item_dy_AST_catlg values a_tbl_insert(i);

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
                       ' '||a_tbl_insert(g_error_index).sk1_location_no||
                       ' '||a_tbl_insert(g_error_index).sk1_item_no||
                       ' '||a_tbl_insert(g_error_index).post_date||
                       ' '||a_tbl_insert(g_error_index).sk1_avail_uda_value_no;
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
       update rtl_loc_item_dy_AST_catlg
       set    ch_catalog_ind             = a_tbl_update(i).ch_catalog_ind,
              ch_num_avail_days          = a_tbl_update(i).ch_num_avail_days,
              ch_num_catlg_days          = a_tbl_update(i).ch_num_catlg_days,
              reg_soh_selling_catlg      = a_tbl_update(i).reg_soh_selling_catlg,
              reg_soh_qty_catlg          = a_tbl_update(i).reg_soh_qty_catlg,
              sk2_location_no            = a_tbl_update(i).sk2_location_no,
              sk2_item_no                = a_tbl_update(i).sk2_item_no,
              LAST_UPDATED_DATE           = a_tbl_update(i).LAST_UPDATED_DATE,
PROD_LINK_TYPE   =   a_tbl_update(i).PROD_LINK_TYPE  ,
SK1_GROUP_ITEM_NO   =   a_tbl_update(i).SK1_GROUP_ITEM_NO  ,
AVAIL_REG_SALES_QTY_CATLG   =   a_tbl_update(i).AVAIL_REG_SALES_QTY_CATLG  ,
AVAIL_REG_SALES_CATLG   =   a_tbl_update(i).AVAIL_REG_SALES_CATLG  ,
AVAIL_REG_SOH_QTY_CATLG   =   a_tbl_update(i).AVAIL_REG_SOH_QTY_CATLG  ,
AVAIL_REG_SOH_SELLING_CATLG   =   a_tbl_update(i).AVAIL_REG_SOH_SELLING_CATLG  ,
AVAIL_PROM_SALES_QTY_CATLG   =   a_tbl_update(i).AVAIL_PROM_SALES_QTY_CATLG  ,
AVAIL_PROM_SALES_CATLG   =   a_tbl_update(i).AVAIL_PROM_SALES_CATLG  ,
AVAIL_PROM_REG_SALES_QTY_CATLG   =   a_tbl_update(i).AVAIL_PROM_REG_SALES_QTY_CATLG  ,
AVAIL_PROM_REG_SALES_CATLG   =   a_tbl_update(i).AVAIL_PROM_REG_SALES_CATLG  ,
AVAIL_CH_NUM_AVAIL_DAYS   =   a_tbl_update(i).AVAIL_CH_NUM_AVAIL_DAYS  ,
AVAIL_CH_NUM_CATLG_DAYS   =   a_tbl_update(i).AVAIL_CH_NUM_CATLG_DAYS  ,
prod_link_ind   =   a_tbl_update(i).prod_link_ind  
       where  sk1_location_no            = a_tbl_update(i).sk1_location_no  and
              sk1_item_no                = a_tbl_update(i).sk1_item_no      and
              post_date                  = a_tbl_update(i).post_date and
              sk1_avail_uda_value_no     = a_tbl_update(i).sk1_avail_uda_value_no ;

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
                       ' '||a_tbl_update(g_error_index).sk1_location_no||
                       ' '||a_tbl_update(g_error_index).sk1_item_no||
                       ' '||a_tbl_update(g_error_index).post_date||
                       ' '||a_tbl_update(g_error_index).sk1_avail_uda_value_no;

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
   from   rtl_loc_item_dy_AST_catlg
   where  sk1_location_no            = g_rec_out.sk1_location_no  and
          sk1_item_no                = g_rec_out.sk1_item_no      and
          post_date                  = g_rec_out.post_date and
          sk1_avail_uda_value_no     = g_rec_out.sk1_avail_uda_value_no ;

   if g_count = 1 then
      g_found := TRUE;
   end if;

   if a_count_i > 0 and not g_found then
      for i in a_tbl_insert.first .. a_tbl_insert.last
      loop
         if  a_tbl_insert(i).sk1_location_no            = g_rec_out.sk1_location_no and
             a_tbl_insert(i).sk1_item_no                = g_rec_out.sk1_item_no and
             a_tbl_insert(i).post_date                  = g_rec_out.post_date and
             a_tbl_insert(i).sk1_avail_uda_value_no     = g_rec_out.sk1_avail_uda_value_no  then
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
    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF rtl_loc_item_dy_AST_catlg EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Look up batch date from dim_control
--**************************************************************************************************

    dwh_lookup.dim_control(g_date);
    g_date := '13 NOV 2016';

    l_text := 'BATCH DATE BEING PROCESSED - '||g_date;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := 'LOCATION RANGE BEING PROCESSED - '||p_from_loc_no||' to '||p_to_loc_no;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--**************************************************************************************************
-- Look up dates from dim_calendar
--**************************************************************************************************
    select this_week_start_date, this_week_end_date, fin_week_no, fin_year_no
    into g_this_week_start_date, g_this_week_end_date, g_fin_week_no, g_fin_year_no
    from dim_calendar
    where calendar_date = g_date;



    G_PART_NAME := G_PART_NAME||TO_CHAR((G_DATE + 1),'ddmmyy');
    l_text := 'subPARTITION NAME '||g_part_name;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    g_sql :=
   g_sql1||g_sql2||g_sql3||g_sql4||g_sql5||g_sql6||g_sql7||g_sql8||g_sql9||g_sql10||
    g_sql11||g_sql12||g_sql13||g_sql14||g_sql15||g_sql16||g_sql17||g_sql18
    ||g_sql19
        ||g_sql22A||g_sql22B||g_sql22C||g_sql22D||g_sql22E||g_sql22F||g_sql22G||g_sql22H||g_sql22I
    ||g_sql22J||g_sql22K||g_sql22L||g_sql22M
    ||g_sql20||
    g_sql21||g_sql22
    ||g_sql23||g_sql24||g_sql25||g_sql26||g_sql27||g_sql28||g_sql29||g_sql30||
    g_sql31||
    g_sql32||
    G_SQL33||G_SQL34||G_SQL35||
    G_SQL36||G_PART_NAME||') stk on '
    ||g_sql37||g_sql38||
    G_SQL39||G_SQL40||G_SQL41||G_SQL42||G_SQL43
    ||
    ' where  fnd_lid.post_date                    = '''||G_DATE||''' AND '||
    --   ' where  fnd_lid.last_updated_date                    = '''||g_date||''' and '||
    '       fnd_lid.location_no between '||P_FROM_LOC_NO||' and '||P_TO_LOC_NO
  --  ||' and dl.chain_no <> 40 '
    ;
--- chg44990 added line 'and dl.chain_no <> 40

delete from  DWH_PERFORMANCE.WLCHECK;
commit;
INSERT INTO DWH_PERFORMANCE.WLCHECK VALUES(G_SQL);
commit;
--

--**************************************************************************************************
-- Bulk fetch loop controlling main program execution
--**************************************************************************************************

    open stk_cv for g_sql;
    fetch stk_cv bulk collect into a_stg_input limit g_forall_limit;
    l_text := 'a_stg_input.count='||a_stg_input.count||' - g_forall_limit='||g_forall_limit;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    while a_stg_input.count > 0
    loop
      for i in 1 .. a_stg_input.count
      loop


         g_recs_read := g_recs_read + 1;
         if g_recs_read mod 200000 = 0 then
            l_text := dwh_constants.vc_log_records_processed||
            to_char(sysdate,('dd mon yyyy hh24:mi:ss'))||'  '||g_recs_read ;
            dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
         end if;

         g_rec_in                := a_stg_input(i);

         local_address_variables;
         local_write_output;

      end loop;
    fetch stk_cv bulk collect into a_stg_input limit g_forall_limit;
    end loop;
    close stk_cv;
--**************************************************************************************************
-- At end write out what remains in the arrays at end of program
--**************************************************************************************************
    local_bulk_insert;
    local_bulk_update;

--**************************************************************************************************
-- Write final log data
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'','');
    l_text :=  dwh_constants.vc_log_time_completed ||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_constants.vc_log_run_completed ||sysdate;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
        l_text :=  ' ';
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

--    commit;
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

end WH_PRF_AST_040U_OLD;
