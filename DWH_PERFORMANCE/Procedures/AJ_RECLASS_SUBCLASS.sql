--------------------------------------------------------
--  DDL for Procedure AJ_RECLASS_SUBCLASS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."AJ_RECLASS_SUBCLASS" (
    p_forall_limit in integer,
    p_success OUT BOOLEAN) as 

Cursor subc_cur Is
with 
subc_sel as
(select distinct b.subclass_no , b.sk1_subclass_no
 from dwh_datafix.aj_rtl_chain_subcl_wk_mp_plan a, dim_subclass b
 where a.sk1_subclass_no = b.sk1_subclass_no),
subc_list as
(select a.subclass_no , a.sk1_subclass_no
 from subc_sel a
 where not exists (select 1 from dwh_datafix.aj_load_valid_subclass b
                   where a.subclass_no = b.subclass_no))
                  
select a.sk1_chain_no, a.sk1_subclass_no, a.sk1_plan_type_no, a.fin_year_no, a.fin_week_no
from dwh_datafix.aj_rtl_chain_subcl_wk_mp_plan a, subc_list b
where a.sk1_subclass_no = b.sk1_subclass_no;

 begin
         FOR subc_rec IN subc_cur
         loop
              delete from dwh_datafix.aj_rtl_chain_subcl_wk_mp_plan
              where  sk1_chain_no      = subc_rec.sk1_chain_no
               and   sk1_subclass_no   = subc_rec.sk1_subclass_no
               and   sk1_plan_type_no  = subc_rec.sk1_plan_type_no
               and   fin_year_no       = subc_rec.fin_year_no
               and   fin_week_no       = subc_rec.fin_week_no;
        end loop;
   end;
