-- ****** Object: Procedure W7104429.VIEW_PRF2 Script Date: 04/12/2018 11:29:27 AM ******
CREATE OR REPLACE PROCEDURE "VIEW_PRF2"  (l_sql in varchar2, l_cnt out number) authid  current_user   
as

BEGIN

   execute immediate 
   '
         DECLARE 
            cntr   number := 0; 
            l_cnt  number;
         BEGIN 
            for r in (' || l_sql || ' ) 
            loop 
                cntr := cntr + 1; 
            end loop;  
            
            l_cnt := cntr;
            dbms_output.put_line (''Rows processed '' || l_cnt);
            commit;
         END;
   ';

END  "VIEW_PRF2";
/