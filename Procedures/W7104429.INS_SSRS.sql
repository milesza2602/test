-- ****** Object: Procedure W7104429.INS_SSRS Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "INS_SSRS" 
 
      (p_OWNER	    in VARCHAR2, p_VIEW_NAME	in VARCHAR2,  p_CLAUSE	in VARCHAR2, p_VIEWN in VARCHAR2, p_USING_CLS in VARCHAR2) 
       as
   begin
      insert into W7104429.OLAP_CRITERIA_SSRS
      (OWNER, VIEW_NAME, CLAUSE, OLAP_TP, VIEWN, USING_CLS) 
      values
       (p_OWNER, p_VIEW_NAME, p_CLAUSE, 'SSRS', p_VIEWN, p_USING_CLS);
--      (p_OWNER, p_VIEW_NAME, p_CLAUSE, 'p_OLAP_TP');
   end;
/