-- ****** Object: Procedure W7131037.WH_PRF_CUST_280U Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."WH_PRF_CUST_280U" (p_forall_limit in integer,p_success out boolean) AS

--**************************************************************************************************
--  Date:        Dec 2015
--  Author:      Alastair de Wet
--  Purpose:    Generate product family and category code to Item master daily. Lifestyle Segmentation
--  Tables:      Input  - dim_item
--               Output - dim_item_cust_lss
--  Packages:    constants, dwh_log, dwh_valid
--
--  Maintenance:
--
--
--  Naming conventions:
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
g_recs_lev1          integer       :=  0;
g_forall_limit       integer       :=  10000;


g_date               date          := trunc(sysdate);
g_yesterday          date          := trunc(sysdate) - 1;

g_stmt                varchar(500);

l_message            sys_dwh_errlog.log_text%type;
l_module_name        sys_dwh_errlog.log_procedure_name%type    := 'WH_PRF_CUST_280U';
l_name               sys_dwh_log.log_name%type                 := dwh_cust_constants.vc_log_name_cust_cl;
l_system_name        sys_dwh_log.log_system_name%type          := dwh_cust_constants.vc_log_system_name_cust_prf;
l_script_name        sys_dwh_log.log_script_name%type          := dwh_cust_constants.vc_log_script_cust_prf;
l_procedure_name     sys_dwh_log.log_procedure_name%type       := l_module_name;
l_text               sys_dwh_log.log_text%TYPE ;
l_description        sys_dwh_log_summary.log_description%type  := 'LOAD dim_item_cust_lss EX dim_item';
l_process_type       sys_dwh_log_summary.log_process_type%type := dwh_cust_constants.vc_log_process_type_n;


--**************************************************************************************************
-- Main process loop
--**************************************************************************************************
begin

    if p_forall_limit is not null and p_forall_limit > 1000 then
       g_forall_limit := p_forall_limit;
    end if;
    p_success := false;
    l_text := dwh_cust_constants.vc_log_draw_line;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

    l_text := 'LOAD OF dim_item_cust_lss EX dim_item STARTED AT '||
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




   l_text      := 'Start of Merge to place all new changes on dim_item_cust_lss';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);


execute immediate 'alter session force parallel dml';

MERGE  /*+ parallel (rli,4) */ INTO dim_item_cust_lss rli USING
(
select    /*+ FULL(fli)  parallel (fli,4) */
          ITM.ITEM_NO,
          ITEM_DESC,
          ITEM_STATUS_CODE,
          ITEM_LEVEL_NO,
          TRAN_LEVEL_NO,
          TRAN_IND,
          ITEM_LEVEL1_NO,
          BUSINESS_UNIT_NO,
          GROUP_NO,
          SUBGROUP_NO,
          DEPARTMENT_NO,
          CLASS_NO,
          SUBCLASS_NO,
          ITM.LAST_UPDATED_DATE,
          case
          when price_tier_desc_306 = 'Good'   then 1
          when price_tier_desc_306 = 'Better' then 2
          when price_tier_desc_306 = 'Best'   then 3
          when price_tier_desc_306 = 'Killer' then 4
          else 99
          end GBB,
          case
          when cust_segmentation_desc_300 = 'Classic'      then 1
          when cust_segmentation_desc_300 = 'Modern'       then 2
          when cust_segmentation_desc_300 = 'Contemporary' then 3
          when cust_segmentation_desc_300 = 'Ethan'        then 4
          when cust_segmentation_desc_300 = 'Hannah'       then 5
          when cust_segmentation_desc_300 = 'Lola'         then 6
          when cust_segmentation_desc_300 = 'Michael'      then 7
          when cust_segmentation_desc_300 = 'Stella'       then 8
          when cust_segmentation_desc_300 = 'Thabo'        then 9
          when cust_segmentation_desc_300 = 'Thembe'       then 10
          else 99
          end CMC
   from   DIM_ITEM ITM , DIM_ITEM_UDA UDA
   where  ITM.ITEM_NO =  UDA.ITEM_NO(+)
) di
ON  (di.ITEM_NO                 = rli.ITEM_NO)
WHEN MATCHED THEN
update set   rli.ITEM_DESC         =  di.ITEM_DESC,
             rli.ITEM_STATUS_CODE  =  di.ITEM_STATUS_CODE,
             rli.ITEM_LEVEL_NO     =  di.ITEM_LEVEL_NO,
             rli.TRAN_LEVEL_NO     =  di.TRAN_LEVEL_NO,
             rli.TRAN_IND          =  di.TRAN_IND,
             rli.ITEM_LEVEL1_NO    =  di.ITEM_LEVEL1_NO ,
             rli.BUSINESS_UNIT_NO  =  di.BUSINESS_UNIT_NO,
             rli.GROUP_NO          =  di.GROUP_NO ,
             rli.SUBGROUP_NO       =  di.SUBGROUP_NO ,
             rli. DEPARTMENT_NO    =  di. DEPARTMENT_NO,
             rli. CLASS_NO         =  di. CLASS_NO,
             rli. SUBCLASS_NO      =  di. SUBCLASS_NO,
             rli. GBB              =  di. GBB,
             rli. CMC              =  di. CMC,
             rli.LAST_UPDATED_DATE =  g_date
  where      rli.ITEM_DESC         <> di.ITEM_DESC or
             rli.ITEM_STATUS_CODE  <> di.ITEM_STATUS_CODE or
             rli.ITEM_LEVEL_NO     <> di.ITEM_LEVEL_NO or
             rli.TRAN_LEVEL_NO     <> di.TRAN_LEVEL_NO or
             rli.TRAN_IND          <> di.TRAN_IND or
             rli.ITEM_LEVEL1_NO    <> di.ITEM_LEVEL1_NO  or
             rli.BUSINESS_UNIT_NO  <> di.BUSINESS_UNIT_NO  or
             rli.GROUP_NO          <> di.GROUP_NO  or
             rli.SUBGROUP_NO       <> di.SUBGROUP_NO  or
             rli.DEPARTMENT_NO     <> di.DEPARTMENT_NO or
             rli.CLASS_NO          <> di.CLASS_NO or
             rli.SUBCLASS_NO       <> di.SUBCLASS_NO or
             nvl(rli.GBB,0)        <> di.GBB or
             nvl(rli.CMC,0)        <> di.CMC
WHEN NOT MATCHED THEN
  insert (
          rli.ITEM_NO,
          rli.ITEM_DESC,
          rli.ITEM_STATUS_CODE,
          rli.ITEM_LEVEL_NO,
          rli.TRAN_LEVEL_NO,
          rli.TRAN_IND,
          rli.ITEM_LEVEL1_NO,
          rli.BUSINESS_UNIT_NO,
          rli.GROUP_NO,
          rli.SUBGROUP_NO,
          rli.DEPARTMENT_NO,
          rli.CLASS_NO,
          rli.SUBCLASS_NO,
          rli.CATEGORY_CODE,
          rli.PRODUCT_FAMILY_CODE,
          rli.LAST_UPDATED_DATE,
          rli.GBB,
          rli.CMC
         )
  values
         (
          di.ITEM_NO,
          di.ITEM_DESC,
          di.ITEM_STATUS_CODE,
          di.ITEM_LEVEL_NO,
          di.TRAN_LEVEL_NO,
          di.TRAN_IND,
          di.ITEM_LEVEL1_NO,
          di.BUSINESS_UNIT_NO,
          di.GROUP_NO,
          di.SUBGROUP_NO,
          di.DEPARTMENT_NO,
          di.CLASS_NO,
          di.SUBCLASS_NO,
          '','',
          g_date,
          di.GBB,
          di.CMC

);


g_recs_read:=g_recs_read+SQL%ROWCOUNT;
g_recs_inserted:=g_recs_inserted+SQL%ROWCOUNT;

commit;

   l_text      := 'Start of update to populate product family from level1 down to items in that level';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

MERGE  /*+ parallel (rli,4) */ INTO dim_item_cust_lss rli USING
(
   with lev1 as (
   select /*+ FULL(lss)  parallel (lss,4) */
          item_level1_no,product_family_code
   from   dim_item_cust_lss lss
   where  item_level_no = 1 and product_family_code is not null
                 )
   select /*+ parallel (4) */
          di.item_no,l1.product_family_code
   from   dim_item_cust_lss di , lev1 l1
   where  l1.item_level1_no = di.item_level1_no and
          di.item_level_no <> 1 and
          di.product_family_code is null)
 mv
ON  (mv.ITEM_NO                 = rli.ITEM_NO)
WHEN MATCHED THEN
update set   rli.product_family_code    =  mv.product_family_code,
             rli.LAST_UPDATED_DATE      =  g_date
;

   g_recs_lev1:=g_recs_lev1+SQL%ROWCOUNT;

commit;

   l_text      := 'Start of update to put in category and product family';
   dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

update dim_item_cust_lss
set category_code = case
when business_unit_no = 50 and group_no = 9006 then 'H'
when business_unit_no = 50 and group_no = 9002 then 'S'
when business_unit_no = 50 and group_no = 9004 then 'R'
when business_unit_no = 50 and group_no = 9005 then 'B'
when business_unit_no = 50 and group_no = 9008 then 'D'
when business_unit_no = 50 and group_no = 9001 then 'W'
when business_unit_no = 50 and group_no = 9000 then 'G'
when business_unit_no = 50 and group_no = 9007 then 'P'
when business_unit_no = 50 and group_no = 9010 then 'N'
when business_unit_no = 52 then 'H'
when business_unit_no = 54 then 'B'
when business_unit_no = 51 and group_no = 1 then 'O'
when business_unit_no = 51 and group_no = 2 then 'L'
when business_unit_no = 51 and group_no = 3 then 'K'
when business_unit_no = 51 and group_no = 5 then 'M'
when business_unit_no = 51 and group_no = 6 then 'F'
when business_unit_no = 51 and group_no = 10 and department_no in (107,109,537,560,656) then 'O'
when business_unit_no = 51 and group_no = 10 and department_no in (150,519,660,661)     then 'M'
when business_unit_no = 51 and group_no = 10 and department_no in (658,659)             then 'F'
when business_unit_no = 55 and department_no in (566,575,670,3105)                      then 'O'
when business_unit_no = 55 and department_no in (570,571,580,586)                       then 'M'
when business_unit_no = 55 and department_no in (567,585,671,672,676)                   then 'F'
when business_unit_no = 55 and subgroup_no in (424)                                     then 'K'
when business_unit_no = 55 and subgroup_no in (425)                     then 'H'
end
where  --category_code is null  and
       business_unit_no not in (53,70);

g_recs_updated:=g_recs_updated+SQL%ROWCOUNT;

commit;

update dim_item_cust_lss
set  product_family_code = case
when group_no in (3,4,12) then subgroup_no||'_'||class_no||'_'||subclass_no||'_'||gbb||'_'||cmc
when group_no in (1,2,5,6,7,10) then department_no||'_'||class_no||'_'||subclass_no||'_'||gbb||'_'||cmc
--when group_no in (8) and subgroup_no in (233) then class_no||'_'||subclass_no||'_'||item_level1_no
--when group_no in (8) and subgroup_no not in (233) then class_no||'_'||subclass_no||'_'||item_level1_no
--when group_no in (9)  then class_no||'_'||subclass_no||'_'||item_level1_no
ELSE null
end
where  product_family_code is null and
       business_unit_no not in (53,70,50);

g_recs_updated:=g_recs_updated+SQL%ROWCOUNT;
commit;

l_text      := 'Start of update to convert product family from long to short code';
dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);

UPDATE   DIM_ITEM_CUST_LSS DI
SET      DI.PRODUCT_FAMILY_CODE =
         (SELECT  DPC.PRODUCT_FAMILY_CODE
          FROM    W7131037.DIM_LSS_PFC_CONVERT DPC
          WHERE   DPC.GENERATED_PFC = DI.PRODUCT_FAMILY_CODE)
WHERE    DI.PRODUCT_FAMILY_CODE IS NOT NULL AND
         DI.BUSINESS_UNIT_NO NOT IN (53,70,50) AND
         LENGTH(DI.PRODUCT_FAMILY_CODE) > 8;

g_recs_updated:=g_recs_updated+SQL%ROWCOUNT;


--**************************************************************************************************
-- At end write out log totals
--**************************************************************************************************
    dwh_log.update_log_summary(l_name,l_system_name,l_script_name,l_procedure_name,l_description,
    l_process_type,dwh_cust_constants.vc_log_ended,g_recs_read,g_recs_inserted,g_recs_updated,'',g_recs_hospital);

    l_text :=  dwh_cust_constants.vc_log_time_completed||to_char(sysdate,('dd mon yyyy hh24:mi:ss'));
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_read||g_recs_read;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_updated||g_recs_updated;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  dwh_cust_constants.vc_log_records_inserted||g_recs_inserted;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text :=  'RECORDS UPDATED FROM LEVEL 1 '||g_recs_LEV1;
    dwh_log.write_log(l_name,l_system_name,l_script_name,l_procedure_name,l_text);
    l_text := dwh_cust_constants.vc_log_run_completed||sysdate;
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


END "WH_PRF_CUST_280U";
