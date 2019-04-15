-- ****** Object: Procedure W7104429.INS Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "INS" 
 
      (p_OWNER	    in VARCHAR2, p_VIEW_NAME	in VARCHAR2,  p_CLAUSE	   in VARCHAR2)     --, p_OLAP_TP	   in VARCHAR2) 
       as
   begin
   -- added a comment to test 
      insert into W7104429.OLAP_CRITERIA 
      (OWNER, VIEW_NAME, CLAUSE, OLAP_TP) 
      values
       (p_OWNER, p_VIEW_NAME, p_CLAUSE, 'SSAS');
--      (p_OWNER, p_VIEW_NAME, p_CLAUSE, 'p_OLAP_TP');
   end;
/