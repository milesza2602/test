--------------------------------------------------------
--  DDL for Procedure AJ_RECLASS_DEPT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "DWH_PERFORMANCE"."AJ_RECLASS_DEPT" (
    p_forall_limit in integer,
    p_success OUT BOOLEAN) as 
--Declare
--BEGIN
Cursor dept_cur Is
with dept_sel as
(select distinct b.department_no , b.sk1_department_no
 from dwh_datafix.aj_rtl_chain_dept_wk_mp_plan a, dim_department b
 where a.sk1_department_no = b.sk1_department_no),
dept_list as
(select a.department_no , a.sk1_department_no
 from dept_sel a
 where not exists (select 1 from dwh_datafix.aj_load_valid_dept b
                   where a.department_no = b.dept_no))
                  
select a.sk1_chain_no, a.sk1_department_no, a.sk1_plan_type_no, a.fin_year_no, a.fin_week_no 
from dwh_datafix.aj_rtl_chain_dept_wk_mp_plan a, dept_list b
where a.sk1_department_no = b.sk1_department_no;

   begin
         FOR dept_rec IN dept_cur
         loop
              delete from dwh_datafix.aj_rtl_chain_dept_wk_mp_plan
              where  sk1_chain_no      = dept_rec.sk1_chain_no
               and   sk1_department_no = dept_rec.sk1_department_no
               and   sk1_plan_type_no  = dept_rec.sk1_plan_type_no
               and   fin_year_no       = dept_rec.fin_year_no
               and   fin_week_no       = dept_rec.fin_week_no;
        END LOOP;
   end;
--end AJ_RECLASS_DEPT;
