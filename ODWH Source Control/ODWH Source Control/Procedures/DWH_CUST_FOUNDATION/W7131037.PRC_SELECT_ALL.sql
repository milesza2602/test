-- ****** Object: Procedure W7131037.PRC_SELECT_ALL Script Date: 13/03/2019 04:41:17 PM ******
CREATE OR REPLACE PROCEDURE "W7131037"."PRC_SELECT_ALL" (vAPP_ID VARCHAR2, vPAGE_ID VARCHAR2, vUSER VARCHAR2, vTable  VARCHAR2) AS
PRAGMA autonomous_transaction;

  O_error_message  VARCHAR2(1000) := NULL;
  L_function VARCHAR2(255) := 'PRC_SELECT_ALL';

     v_report_id   NUMBER;
     v_region_id   NUMBER;
     v_report      apex_ir.t_report;
     v_query       VARCHAR2 (32767);
     v_column      VARCHAR2 (4000);
     v_position    NUMBER;
     v_count number;


BEGIN

--   SELECT region_id
--     INTO v_region_id
--     FROM apex_application_page_regions
--    WHERE application_id = vAPP_ID
--      AND page_id = vPAGE_ID
--      AND source_type = 'Interactive Report';

--   v_report_id :=
--      apex_ir.get_last_viewed_report_id (p_page_id        => vPAGE_ID,
--                                         p_region_id      => v_region_id
--                                        );
--
   v_report :=
      apex_ir.get_report (p_page_id        => vPAGE_ID,
                          p_region_id      => v_region_id,
                          p_report_id      => v_report_id
                         );
   v_query := v_report.sql_query;

 FOR i IN 1 .. v_report.binds.COUNT
   LOOP
      v_query :=
         REPLACE (v_query,
                  ':' || v_report.binds (i).NAME,
                  '''' || v_report.binds (i).VALUE || ''''
                 );
   END LOOP;

   v_query := substr(v_query,instr(v_query,'where'),instr(v_query,') r where')-instr(v_query,'where'));
   v_query := REPLACE(v_query,'Select','INCLUDE');


   IF v_query IS NULL THEN

     EXECUTE IMMEDIATE 'UPDATE W7131037.'||vTable||' d
                        SET INCLUDE = ''Y''
                        WHERE CREATED_BY = '''||vUSER||'''';

     COMMIT;

   ELSE

     EXECUTE IMMEDIATE 'UPDATE W7131037.'||vTable||' d
                        SET INCLUDE = ''Y''
                          '||v_query||'
                        AND CREATED_BY = '''||vUSER||'''';

     COMMIT;

   END IF;


  EXCEPTION
  WHEN OTHERS THEN
  O_error_message := 'Exception while Selecting All ' ||  TO_CHAR(SQLCODE) || SQLERRM || L_function;
  O_error_message := '.
                      Warning!!!  An error has occured. If you continue your changes may not be saved!
                      If error occurs again please contact IT Support
                      .';
   raise_application_error( -20001, O_error_message );

END "PRC_SELECT_ALL";
