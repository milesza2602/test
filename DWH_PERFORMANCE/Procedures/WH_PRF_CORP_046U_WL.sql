--------------------------------------------------------
--  DDL for Procedure WH_PRF_CORP_046U_WL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."WH_PRF_CORP_046U_WL" 
(p_forall_limit in integer,p_success out boolean) as
--**************************************************************************************************
-- change load due to performance issues - wendy lyttle
--**************************************************************************************************
--  Date:        March 2015
--  Author:      Wendy Lyttle
--
--     revamped version
--
--  Purpose:     Create Location Item fact table in the performance layer
--               with input ex RMS fnd_location_item table from foundation layer.
--  Tables:      Input  - fnd_location_item
--               Output - rtl_location_item
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--  30 Jan 2009 - Defect 491 - remove primary_country_code and replace with sk1_primary_country_code
--                             remove primary_supplier_no and replace with sk1_primary_supplier_no
--
--  09 May 2011 - (NJ) Defect 4282 - added a field called sale_alert_ind.
--  19 May 2011 - Defect 2981 - Add a new measure to be derived (min_shelf_life)
--                            - Add base measures min_shelf_life_tolerance & max_shelf_life_tolerance
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
g_recs_tol           integer       :=  0;
g_error_count        number        :=  0;
g_error_index        number        :=  0;
g_recs               number        :=  0;
g_count              number        :=  0;
g_name               varchar2(40);
g_cnt_rtl            number        :=  0;
g_cnt_backup         number        :=  0;
g_calc               number        :=  0;
g_cnt                number        :=  0;
g_minloc             number        :=  0;
g_maxloc             number        :=  0;
g_sk1_minloc             number        :=  0;
g_sk1_maxloc             number        :=  0;
g_split_no           number        :=  0;

g_rec_out            rtl_location_item%rowtype;
g_found              boolean;
g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;
l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CORP_046U_WL';
l_name               sys_dwh_log.log_name%type                 := dwh_constants.vc_log_name_rtl_md;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_constants.vc_log_system_name_rtl_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_constants.vc_log_script_rtl_prf_md;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%type ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD THE LOCATION ITEM FACTS EX FOUNDATION';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_constants.vc_log_process_type_n;



  
  --**************************************************************************************************
  -- Insert into SPLIT table
  --**************************************************************************************************
procedure A_INSERT_SPLIT as
BEGIN

  l_text := 'A_INSERT_SPLIT';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
 

      insert /*+ append */ into DWH_PERFORMANCE.temp_FND_LOCATION_ITEM_split 
      with selcntall
      as (select /*+ full(a) parallel(a,8) */ count(*) tabcnt from fnd_location_item a
     -- where location_no <= 110\
      ),
      --g_cnt :=- 183682494
      selcntloc as(
      select /*+ full(a) parallel(a,8) */ location_no, count(*) cntloc 
      from  fnd_location_item a
    --  where location_no <= 110
      group by location_no
      order by location_no),
      selaccum as 
      (select location_no, cntloc,
      SUM (cntloc)
      OVER (ORDER BY location_no) accum_cnt
      from selcntloc
      order by location_no
      )
      select location_no, 1 split_no, cntloc, accum_cnt , tabcnt 
      from selcntall a, selaccum b
      where  accum_cnt <= tabcnt / 4
      union all
      select location_no, 2 split_no, cntloc, accum_cnt , tabcnt 
      from  selcntall a, selaccum b
      where  accum_cnt between (tabcnt / 4) and (tabcnt / 2)
      union all
      select location_no, 3 split_no, cntloc, accum_cnt , tabcnt 
      from  selcntall a, selaccum b
      where accum_cnt between  (tabcnt / 2) and ((tabcnt / 4) * 3)
      union all
      select location_no, 4 split_no, cntloc, accum_cnt , tabcnt 
      from selcntall a, selaccum b
      where  accum_cnt >= (tabcnt / 4) * 3;
           
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        L_TEXT := 'TEMP_FND_LOCATION_ITEM_SPLIT  : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

        SELECT COUNT(DISTINCT LOCATION_NO) INTO G_CNT FROM FND_LOCATION_ITEM;
        
        IF G_CNT <> G_RECS
        THEN 
            l_text := ' *** ERROR  *** location missing from split - cnt='||G_CNT||' vs loaded='|| g_recs;
            dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        end if;


   exception
  WHEN no_data_found THEN
        l_text := 'no data found for a_insert_split';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in a_insert_split';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_insert_split';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in a_insert_split';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end A_INSERT_SPLIT;

  
  --**************************************************************************************************
  -- Insert into TEMP table
  --**************************************************************************************************
  procedure B_INSERT_TEMP as
BEGIN

  l_text := 'B_INSERT_TEMP';
  dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
  
insert /*+ append */ into DWH_PERFORMANCE.temp_RTL_LOCATION_ITEM 

 with  selfnd as (
             select /*+ FULL(fnd) PARALLEL(fnd,8) */
                    fnd.*, 
                    to_number(translate(decode(nvl(fND.NEXT_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fND.NEXT_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1322'))
                    as    NEXT_WKDELIVPATTERN,
                    to_number(translate(decode(nvl(fND.THIS_WK_DELIV_PATTERN_CODE,'0'),' ','0',nvl(fND.THIS_WK_DELIV_PATTERN_CODE,'0')), 'YNO0', '1322'))
                    as    THIS_WKDELIVPATTERN,
                    di.sk1_item_no,
                    dl.sk1_location_no,
                    dc.sk1_country_code SK1_PRIMARY_COUNTRY_CODE,  -- TD-491
                    ds.sk1_supplier_no  SK1_PRIMARY_SUPPLIER_NO   -- TD-491
             from   fnd_location_item fND,
                    dim_item di,
                    dim_location dL,
                    dim_country dc,
                    dim_supplier ds
             where  fnd.item_no                = di.item_no  and
                    fnd.location_no            = dl.location_no and
                    fnd.primary_country_code   = dc.country_code(+) and    -- TD-491
                    fnd.primary_supplier_no    = ds.supplier_no  and
                    fnd.location_no between g_minloc and g_maxloc 
                       ),
      selrtl as (
             select /*+ FULL(rtl) PARALLEL(rtl,8) */
                    rtl.*
             from   rtl_location_item rtl,
                    DWH_PERFORMANCE.temp_FND_LOCATION_ITEM_split  tmp, 
                    dim_location dl
             where  rtl.sk1_location_no = dl.sk1_location_no
                    and tmp.location_no between g_minloc and g_maxloc 
                    and tmp.location_no = dl.location_no
                    and tmp.split_no = g_split_no
                    )
      select /*+ FULL(RTL) PARALLEL(RTL,8) */
                                 nvl(SF.SK1_LOCATION_NO,rtl.sk1_location_no) sk1_location_no
                               , nvl(SF.SK1_ITEM_NO,rtl.SK1_ITEM_NO) SK1_ITEM_NO
                               , nvl(SF.SUPPLY_CHAIN_TYPE,rtl.SUPPLY_CHAIN_TYPE) SUPPLY_CHAIN_TYPE
                               , nvl(SF.NEXT_WKDELIVPATTERN,rtl.NEXT_WK_DELIV_PATTERN_CODE) NEXT_WK_DELIV_PATTERN_CODE
                               , nvl(SF.THIS_WKDELIVPATTERN,rtl.THIS_WK_DELIV_PATTERN_CODE) THIS_WK_DELIV_PATTERN_CODE
                               , nvl(SF.THIS_WK_CATALOG_IND,rtl.THIS_WK_CATALOG_IND) THIS_WK_CATALOG_IND
                               , nvl(SF.NEXT_WK_CATALOG_IND,rtl.NEXT_WK_CATALOG_IND) NEXT_WK_CATALOG_IND
                               , nvl(SF.NUM_SHELF_LIFE_DAYS,rtl.NUM_SHELF_LIFE_DAYS) NUM_SHELF_LIFE_DAYS
                               , nvl(SF.NUM_UNITS_PER_TRAY,rtl.NUM_UNITS_PER_TRAY) NUM_UNITS_PER_TRAY
                               , nvl(SF.DIRECT_PERC,rtl.DIRECT_PERC) DIRECT_PERC
                               , nvl(SF.MODEL_STOCK,rtl.MODEL_STOCK) MODEL_STOCK
                               , nvl(SF.THIS_WK_CROSS_DOCK_IND,rtl.THIS_WK_CROSS_DOCK_IND) THIS_WK_CROSS_DOCK_IND
                               , nvl(SF.NEXT_WK_CROSS_DOCK_IND,rtl.NEXT_WK_CROSS_DOCK_IND) NEXT_WK_CROSS_DOCK_IND
                               , nvl(SF.THIS_WK_DIRECT_SUPPLIER_NO,rtl.THIS_WK_DIRECT_SUPPLIER_NO) THIS_WK_DIRECT_SUPPLIER_NO
                               , nvl(SF.NEXT_WK_DIRECT_SUPPLIER_NO,rtl.NEXT_WK_DIRECT_SUPPLIER_NO) NEXT_WK_DIRECT_SUPPLIER_NO
                               , nvl(SF.UNIT_PICK_IND,rtl.UNIT_PICK_IND) UNIT_PICK_IND
                               , nvl(SF.STORE_ORDER_CALC_CODE,rtl.STORE_ORDER_CALC_CODE) STORE_ORDER_CALC_CODE
                               , nvl(SF.SAFETY_STOCK_FACTOR,rtl.SAFETY_STOCK_FACTOR) SAFETY_STOCK_FACTOR
                               , nvl(SF.MIN_ORDER_QTY,rtl.MIN_ORDER_QTY) MIN_ORDER_QTY
                               , nvl(SF.PROFILE_ID,rtl.PROFILE_ID) PROFILE_ID
                               , nvl(SF.SUB_PROFILE_ID,rtl.SUB_PROFILE_ID) SUB_PROFILE_ID
                               , nvl(SF.REG_RSP,rtl.REG_RSP) REG_RSP
                               , nvl(SF.SELLING_RSP,rtl.SELLING_RSP) SELLING_RSP
                               , nvl(SF.SELLING_UOM_CODE,rtl.SELLING_UOM_CODE) SELLING_UOM_CODE
                               , nvl(SF.PROM_RSP,rtl.PROM_RSP) PROM_RSP
                               , nvl(SF.PROM_SELLING_RSP,rtl.PROM_SELLING_RSP) PROM_SELLING_RSP
                               , nvl(SF.PROM_SELLING_UOM_CODE,rtl.PROM_SELLING_UOM_CODE) PROM_SELLING_UOM_CODE
                               , nvl(SF.CLEARANCE_IND,rtl.CLEARANCE_IND) CLEARANCE_IND
                               , nvl(SF.TAXABLE_IND,rtl.TAXABLE_IND) TAXABLE_IND
                               , nvl(SF.POS_ITEM_DESC,rtl.POS_ITEM_DESC) POS_ITEM_DESC
                               , nvl(SF.POS_SHORT_DESC,rtl.POS_SHORT_DESC) POS_SHORT_DESC
                               , nvl(SF.NUM_TI_PALLET_TIER_CASES,rtl.NUM_TI_PALLET_TIER_CASES) NUM_TI_PALLET_TIER_CASES
                               , nvl(SF.NUM_HI_PALLET_TIER_CASES,rtl.NUM_HI_PALLET_TIER_CASES) NUM_HI_PALLET_TIER_CASES
                               , nvl(SF.STORE_ORD_MULT_UNIT_TYPE_CODE,rtl.STORE_ORD_MULT_UNIT_TYPE_CODE) STORE_ORD_MULT_UNIT_TYPE_CODE
                               , nvl(SF.LOC_ITEM_STATUS_CODE,rtl.LOC_ITEM_STATUS_CODE) LOC_ITEM_STATUS_CODE
                               , nvl(SF.LOC_ITEM_STAT_CODE_UPDATE_DATE,rtl.LOC_ITEM_STAT_CODE_UPDATE_DATE) LOC_ITEM_STAT_CODE_UPDATE_DATE
                               , nvl(SF.AVG_NATURAL_DAILY_WASTE_PERC,rtl.AVG_NATURAL_DAILY_WASTE_PERC) AVG_NATURAL_DAILY_WASTE_PERC
                               , nvl(SF.MEAS_OF_EACH,rtl.MEAS_OF_EACH) MEAS_OF_EACH
                               , nvl(SF.MEAS_OF_PRICE,rtl.MEAS_OF_PRICE) MEAS_OF_PRICE
                               , nvl(SF.RSP_UOM_CODE,rtl.RSP_UOM_CODE) RSP_UOM_CODE
                               , nvl(SF.PRIMARY_VARIANT_ITEM_NO,rtl.PRIMARY_VARIANT_ITEM_NO) PRIMARY_VARIANT_ITEM_NO
                               , nvl(SF.PRIMARY_COST_PACK_ITEM_NO,rtl.PRIMARY_COST_PACK_ITEM_NO) PRIMARY_COST_PACK_ITEM_NO
                               , nvl(SF.RECEIVE_AS_PACK_TYPE,rtl.RECEIVE_AS_PACK_TYPE) RECEIVE_AS_PACK_TYPE
                               , nvl(SF.SOURCE_METHOD_LOC_TYPE,rtl.SOURCE_METHOD_LOC_TYPE) SOURCE_METHOD_LOC_TYPE
                               , nvl(SF.SOURCE_LOCATION_NO,rtl.SOURCE_LOCATION_NO) SOURCE_LOCATION_NO
                               , nvl(SF.WH_SUPPLY_CHAIN_TYPE_IND,rtl.WH_SUPPLY_CHAIN_TYPE_IND) WH_SUPPLY_CHAIN_TYPE_IND
                               , nvl(SF.LAUNCH_DATE,rtl.LAUNCH_DATE) LAUNCH_DATE
                               , nvl(SF.POS_QTY_KEY_OPTION_CODE,rtl.POS_QTY_KEY_OPTION_CODE) POS_QTY_KEY_OPTION_CODE
                               , nvl(SF.POS_MANUAL_PRICE_ENTRY_CODE,rtl.POS_MANUAL_PRICE_ENTRY_CODE) POS_MANUAL_PRICE_ENTRY_CODE
                               , nvl(SF.DEPOSIT_CODE,rtl.DEPOSIT_CODE) DEPOSIT_CODE
                               , nvl(SF.FOOD_STAMP_IND,rtl.FOOD_STAMP_IND) FOOD_STAMP_IND
                               , nvl(SF.POS_WIC_IND,rtl.POS_WIC_IND) POS_WIC_IND
                               , nvl(SF.PROPORTIONAL_TARE_PERC,rtl.PROPORTIONAL_TARE_PERC) PROPORTIONAL_TARE_PERC
                               , nvl(SF.FIXED_TARE_VALUE,rtl.FIXED_TARE_VALUE) FIXED_TARE_VALUE
                               , nvl(SF.FIXED_TARE_UOM_CODE,rtl.FIXED_TARE_UOM_CODE) FIXED_TARE_UOM_CODE
                               , nvl(SF.POS_REWARD_ELIGIBLE_IND,rtl.POS_REWARD_ELIGIBLE_IND) POS_REWARD_ELIGIBLE_IND
                               , nvl(SF.COMPARABLE_NATL_BRAND_ITEM_NO,rtl.COMPARABLE_NATL_BRAND_ITEM_NO) COMPARABLE_NATL_BRAND_ITEM_NO
                               , nvl(SF.RETURN_POLICY_CODE,rtl.RETURN_POLICY_CODE) RETURN_POLICY_CODE
                               , nvl(SF.RED_FLAG_ALERT_IND,rtl.RED_FLAG_ALERT_IND) RED_FLAG_ALERT_IND
                               , nvl(SF.POS_MARKETING_CLUB_CODE,rtl.POS_MARKETING_CLUB_CODE) POS_MARKETING_CLUB_CODE
                               , nvl(SF.REPORT_CODE,rtl.REPORT_CODE) REPORT_CODE
                               , nvl(SF.NUM_REQ_SELECT_SHELF_LIFE_DAYS,rtl.NUM_REQ_SELECT_SHELF_LIFE_DAYS) NUM_REQ_SELECT_SHELF_LIFE_DAYS
                               , nvl(SF.NUM_REQ_RCPT_SHELF_LIFE_DAYS,rtl.NUM_REQ_RCPT_SHELF_LIFE_DAYS) NUM_REQ_RCPT_SHELF_LIFE_DAYS
                               , nvl(SF.NUM_INVST_BUY_SHELF_LIFE_DAYS,rtl.NUM_INVST_BUY_SHELF_LIFE_DAYS) NUM_INVST_BUY_SHELF_LIFE_DAYS
                               , nvl(SF.RACK_SIZE_CODE,rtl.RACK_SIZE_CODE) RACK_SIZE_CODE
                               , nvl(SF.FULL_PALLET_ITEM_REORDER_IND,rtl.FULL_PALLET_ITEM_REORDER_IND) FULL_PALLET_ITEM_REORDER_IND
                               , nvl(SF.IN_STORE_MARKET_BASKET_CODE,rtl.IN_STORE_MARKET_BASKET_CODE) IN_STORE_MARKET_BASKET_CODE
                               , nvl(SF.STORAGE_LOCATION_BIN_ID,rtl.STORAGE_LOCATION_BIN_ID) STORAGE_LOCATION_BIN_ID
                               , nvl(SF.ALT_STORAGE_LOCATION_BIN_ID,rtl.ALT_STORAGE_LOCATION_BIN_ID) ALT_STORAGE_LOCATION_BIN_ID
                               , nvl(SF.STORE_REORDER_IND,rtl.STORE_REORDER_IND) STORE_REORDER_IND
                               , nvl(SF.RETURNABLE_IND,rtl.RETURNABLE_IND) RETURNABLE_IND
                               , nvl(SF.REFUNDABLE_IND,rtl.REFUNDABLE_IND) REFUNDABLE_IND
                               , nvl(SF.BACK_ORDER_IND,rtl.BACK_ORDER_IND) BACK_ORDER_IND
                               , nvl(SF.LAST_UPDATED_DATE,rtl.LAST_UPDATED_DATE) LAST_UPDATED_DATE
                               , nvl(SF.SK1_PRIMARY_COUNTRY_CODE,rtl.SK1_PRIMARY_COUNTRY_CODE) SK1_PRIMARY_COUNTRY_CODE
                               , nvl(SF.SK1_PRIMARY_SUPPLIER_NO,rtl.SK1_PRIMARY_SUPPLIER_NO) SK1_PRIMARY_SUPPLIER_NO
                               , nvl(RTL.PRODUCT_STATUS_CODE,0) PRODUCT_STATUS_CODE
                               , nvl(RTL.PRODUCT_STATUS_1_CODE,0) PRODUCT_STATUS_1_CODE
                               , nvl(RTL.WAC,0) WAC
                               , nvl(SF.SALE_ALERT_IND,rtl.SALE_ALERT_IND) SALE_ALERT_IND
                               , nvl(rtl.MIN_SHELF_LIFE,0) MIN_SHELF_LIFE
                               , nvl(rtl.MIN_SHELF_LIFE_TOLERANCE,0) MIN_SHELF_LIFE_TOLERANCE
                               , nvl(rtl.MAX_SHELF_LIFE_TOLERANCE,0) MAX_SHELF_LIFE_TOLERANCE
   from selfnd sf
   full outer join selrtl rtL
   on sf.sk1_location_no = rtl.sk1_location_no
   and sf.sk1_item_no = rtl.sk1_item_no
   ;
                  
        g_recs :=SQL%ROWCOUNT ;
        COMMIT;
        
        L_TEXT := 'TEMP_RTL_LOCATION_ITEM : recs = '||g_recs;
        dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

   exception
  WHEN no_data_found THEN
        l_text := 'no data found for b_insert temp';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
               l_text := 'error in b_insert_temp';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
        
      when dwh_errors.e_insert_error then
       l_message := dwh_constants.vc_err_lw_insert||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert_temp';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

      when others then
       l_message := dwh_constants.vc_err_lw_other||sqlcode||' '||sqlerrm;
       dwh_log.record_error(l_module_name,sqlcode,l_message);
       l_text := 'error in b_insert_temp';
        dwh_log.write_log (l_name, l_system_name, l_script_name,l_procedure_name, l_text);
       raise;

end B_INSERT_TEMP;




  --**************************************************************************************************
  --
  --
  --                    M  a  i  n    p  r  o  c  e  s  s
  --
  --
  --**************************************************************************************************
BEGIN

  --**************************************************************************************************
  -- adding a trace facility
  --**************************************************************************************************
/*0.	Make sure the session being traced has the alter session privilege, 
i.e. to whom does batch run that this tool will be used for, they need to be granted alter session privs

1/ When tracing a session a trace file is generated on the database server, dwndbprd or dwhdbdev

2. It is written to subdirectory 

/dwhprd_app/oracle/diag/rdbms/dwhprd/dwhprd/trace/

Or 

/app/oracle/diag/rdbms/dwhdev/dwhdev/trace/dwhdev/
/app/oracle/diag/rdbms/dwhdev/dwhuat/trace/dwhuat/


3. The trace filed has a difficult naming convention

4. So add to the tracing code the setting of the file name then you know what to look for as per

Alter session set file_identifier = �procedure_name� || to_char(sysdate,�hh24mmiss_ddmmyyyy�);
Alter session set event 10046 level | NN

     Then you have a date/name stamped unique trace file

Tested as follows we see these trace files

SQL> Alter session set traCEfile_identifier = 'Sean1';

Session altered.

SQL> alter session set events '10046 trace name context forever, level 12';

Session altered.

SQL> select * from dual;

D
-
X

SQL> /

D
-
X

SQL> /

D
-
X

SQL> /

D
-
X

SQL> exit


-rw-r-----    1 oracle   asmadmin        303 29 Aug 11:27 dwhdev_ora_6225946_Sean1.trm
-rw-r-----    1 oracle   asmadmin      23185 29 Aug 11:27 dwhdev_ora_6225946_Sean1.trc


*/

--   execute immediate 'Alter session set file_identifier = ''WH_PRF_CORP_046U_WL'''|| to_char(sysdate,�hh24mmiss_ddmmyyyy�);
   execute immediate 'alter session set events ''10046 trace name context forever, level 12''';
   execute immediate 'alter session set tracefile_identifier=''TABLE_A_NOCOMPRESSION_RUN_01''';

 

  --**************************************************************************************************


    if p_forall_limit is not null and p_forall_limit > dwh_constants.vc_forall_minimum then
       g_forall_limit := p_forall_limit;
    end if;
    dbms_output.put_line('Bulk write limit '||p_forall_limit||' '||g_forall_limit);
    p_success := false;

    l_text := dwh_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF RTL_LOCATION_ITEM EX FOUNDATION STARTED AT '||
    to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    dwh_log.insert_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_constants.vc_log_started,'','','','','');

--**************************************************************************************************
-- Set dates
--**************************************************************************************************
    dwh_lookup.dim_control(g_date);
    l_text := 'BATCH DATE BEING PROCESSED IS:- '||g_date;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

                execute immediate 'alter session set workarea_size_policy=manual';
                execute immediate 'alter session set sort_area_size=200000000';
                execute immediate 'alter session enable parallel dml';
/*                
                l_text := 'Running GATHER_TABLE_STATS ON FND_LOCATION_ITEM';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                DBMS_STATS.gather_table_stats ('DWH_FOUNDATION', 'FND_LOCATION_ITEM', DEGREE => 8);
                
                l_text := 'truncate table DWH_PERFORMANCE.TEMP_FND_LOCATION_ITEM_SPLIT ';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                execute immediate('truncate table DWH_PERFORMANCE.TEMP_FND_LOCATION_ITEM_SPLIT ');
                
    A_INSERT_SPLIT;     
   */ 
                l_text := 'truncate table DWH_PERFORMANCE.TEMP_RTL_LOCATION_ITEM ';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                execute immediate('truncate table DWH_PERFORMANCE.TEMP_RTL_LOCATION_ITEM');

                l_text := 'Running GATHER_TABLE_STATS ON TEMP_RTL_LOCATION_ITEM';
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
                DBMS_STATS.gather_table_stats ('DWH_PERFORMANCE', 'TEMP_RTL_LOCATION_ITEM', DEGREE => 8);

   g_split_no := 1;
   select min(a.location_no),  max(a.location_no)   into g_minloc, g_maxloc
                  from DWH_PERFORMANCE.temp_FND_LOCATION_ITEM_split a
                  where split_no = g_split_no;

                l_text := 'Split locations start='||g_minloc||' to='||g_maxloc;
                dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);    
    
                  
    B_INSERT_TEMP;
 

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

END WH_PRF_CORP_046U_WL;