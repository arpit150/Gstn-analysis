-- FUNCTION: mmi_master.gstndata_fuzzy(text, text, text, text)

-- DROP FUNCTION mmi_master.gstndata_fuzzy(text, text, text, text);

CREATE OR REPLACE FUNCTION mmi_master.gstndata_fuzzy(
	schema_name text,
	output_data_table text,
	master_schema_name text,
	master_table_name text DEFAULT 0)
    RETURNS text
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE 
f1 text; f2 text;
t1 text; t2 text;
DECLARE SQLQuery text;
DECLARE sqlq text;
error_tab_name text;
BEGIN 

SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"M_LOC_NME"=t1."LOC_NME","M_LOC_ID"=t1."LOC_ID",STATUS=''FUZZY_LOC_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."LOC_NME",t1."LOC_ID" from(
WITH TAB5 AS(
select raw_name,"LOC_NME",t1.srno,"LOC_ID" from (
select t1.srno,t1.raw_name,t2."LOC_NME",t2."LOC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
 '||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
||soundex(split_part(t1.raw_name,'' '',3))||soundex(split_part(t1.raw_name,'' '',4))=
soundex(split_part(t2."LOC_NME",'' '',1))||soundex(split_part(t2."LOC_NME",'' '',2)) 
||soundex(split_part(t2."LOC_NME",'' '',3))||soundex(split_part(t2."LOC_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"LOC_NME",t1.srno,"LOC_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME","LOC_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME","LOC_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("LOC_NME"),MAX("LOC_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "LOC_NME",TAB4.M5 AS "LOC_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;

SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"M_LOC_NME"=t1."LOC_NME","M_LOC_ID"=t1."LOC_ID",STATUS=''FUZZY_LOC_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."LOC_NME",t1."LOC_ID" from(
WITH TAB5 AS(
select raw_name,"LOC_NME",t1.srno,"LOC_ID" from (
select t1.srno,t1.raw_name,t2."LOC_NME",t2."LOC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
 '||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',4))||soundex(split_part(t1.raw_name,'' '',6))
||soundex(split_part(t1.raw_name,'' '',7))||soundex(split_part(t1.raw_name,'' '',8))=
soundex(split_part(t2."LOC_NME",'' '',1))||soundex(split_part(t2."LOC_NME",'' '',2)) 
||soundex(split_part(t2."LOC_NME",'' '',3))||soundex(split_part(t2."LOC_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"LOC_NME",t1.srno,"LOC_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME","LOC_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME","LOC_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("LOC_NME"),MAX("LOC_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "LOC_NME",TAB4.M5 AS "LOC_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;	

SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"M_LOC_NME"=t1."LOC_NME","M_LOC_ID"=t1."LOC_ID",STATUS=''FUZZY_LOC_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."LOC_NME",t1."LOC_ID" from(
WITH TAB5 AS(
select raw_name,"LOC_NME",t1.srno,"LOC_ID" from (
select t1.srno,t1.raw_name,t2."LOC_NME",t2."LOC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
 '||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
=
soundex(split_part(t2."LOC_NME",'' '',1))||soundex(split_part(t2."LOC_NME",'' '',2)) 
||soundex(split_part(t2."LOC_NME",'' '',3))||soundex(split_part(t2."LOC_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"LOC_NME",t1.srno,"LOC_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME","LOC_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME","LOC_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("LOC_NME"),MAX("LOC_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "LOC_NME",TAB4.M5 AS "LOC_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;	
	
SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"M_LOC_NME"=t1."LOC_NME","M_LOC_ID"=t1."LOC_ID",STATUS=''FUZZY_LOC_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."LOC_NME",t1."LOC_ID" from(
WITH TAB5 AS(
select raw_name,"LOC_NME",t1.srno,"LOC_ID" from (
select t1.srno,t1.raw_name,t2."LOC_NME",t2."LOC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
 '||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',3))||soundex(split_part(t1.raw_name,'' '',4))
=
soundex(split_part(t2."LOC_NME",'' '',1))||soundex(split_part(t2."LOC_NME",'' '',2)) 
||soundex(split_part(t2."LOC_NME",'' '',3))||soundex(split_part(t2."LOC_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"LOC_NME",t1.srno,"LOC_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME","LOC_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME","LOC_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("LOC_NME"),MAX("LOC_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "LOC_NME",TAB4.M5 AS "LOC_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;	
	
	
SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"M_LOC_NME"=t1."LOC_NME","M_LOC_ID"=t1."LOC_ID",STATUS=''FUZZY_LOC_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."LOC_NME",t1."LOC_ID" from(
WITH TAB5 AS(
select raw_name,"LOC_NME",t1.srno,"LOC_ID" from (
select t1.srno,t1.raw_name,t2."LOC_NME",t2."LOC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
 '||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',5))||soundex(split_part(t1.raw_name,'' '',6))
=
soundex(split_part(t2."LOC_NME",'' '',1))||soundex(split_part(t2."LOC_NME",'' '',2)) 
||soundex(split_part(t2."LOC_NME",'' '',3))||soundex(split_part(t2."LOC_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"LOC_NME",t1.srno,"LOC_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"LOC_NME","LOC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("LOC_NME",'' '')) U2,"LOC_NME","LOC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"LOC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"LOC_NME","LOC_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"LOC_NME","LOC_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("LOC_NME"),MAX("LOC_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "LOC_NME",TAB4.M5 AS "LOC_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;	
	
SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"SUBL_NME"=t1."SUBL_NME","SUBL_ID"=t1."SUBL_ID",STATUS=''FUZZY_SUBL_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."SUBL_NME",t1."SUBL_ID" from(
WITH TAB5 AS(
select raw_name,"SUBL_NME",t1.srno,"SUBL_ID" from (
select t1.srno,t1.raw_name,t2."SUBL_NME",t2."SUBL_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
'||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
||soundex(split_part(t1.raw_name,'' '',3))||soundex(split_part(t1.raw_name,'' '',4))=
soundex(split_part(t2."SUBL_NME",'' '',1))||soundex(split_part(t2."SUBL_NME",'' '',2)) 
||soundex(split_part(t2."SUBL_NME",'' '',3))||soundex(split_part(t2."SUBL_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"SUBL_NME",t1.srno,"SUBL_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SUBL_NME","SUBL_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SUBL_NME",'' '')) U2,"SUBL_NME","SUBL_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SUBL_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SUBL_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SUBL_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SUBL_NME","SUBL_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SUBL_NME",'' '')) U2,"SUBL_NME","SUBL_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SUBL_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SUBL_NME","SUBL_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SUBL_NME","SUBL_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("SUBL_NME"),MAX("SUBL_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "SUBL_NME",TAB4.M5 AS "SUBL_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
	
	
SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"SUBL_NME"=t1."SUBL_NME","SUBL_ID"=t1."SUBL_ID",STATUS=''FUZZY_SUBL_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."SUBL_NME",t1."SUBL_ID" from(
WITH TAB5 AS(
select raw_name,"SUBL_NME",t1.srno,"SUBL_ID" from (
select t1.srno,t1.raw_name,t2."SUBL_NME",t2."SUBL_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
'||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',5))||soundex(split_part(t1.raw_name,'' '',6))
||soundex(split_part(t1.raw_name,'' '',7))||soundex(split_part(t1.raw_name,'' '',8))=
soundex(split_part(t2."SUBL_NME",'' '',1))||soundex(split_part(t2."SUBL_NME",'' '',2)) 
||soundex(split_part(t2."SUBL_NME",'' '',3))||soundex(split_part(t2."SUBL_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"SUBL_NME",t1.srno,"SUBL_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SUBL_NME","SUBL_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SUBL_NME",'' '')) U2,"SUBL_NME","SUBL_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SUBL_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SUBL_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SUBL_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SUBL_NME","SUBL_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SUBL_NME",'' '')) U2,"SUBL_NME","SUBL_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SUBL_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SUBL_NME","SUBL_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SUBL_NME","SUBL_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("SUBL_NME"),MAX("SUBL_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "SUBL_NME",TAB4.M5 AS "SUBL_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
	
	
SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"SUBL_NME"=t1."SUBL_NME","SUBL_ID"=t1."SUBL_ID",STATUS=''FUZZY_SUBL_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."SUBL_NME",t1."SUBL_ID" from(
WITH TAB5 AS(
select raw_name,"SUBL_NME",t1.srno,"SUBL_ID" from (
select t1.srno,t1.raw_name,t2."SUBL_NME",t2."SUBL_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
'||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
=
soundex(split_part(t2."SUBL_NME",'' '',1))||soundex(split_part(t2."SUBL_NME",'' '',2)) 
||soundex(split_part(t2."SUBL_NME",'' '',3))||soundex(split_part(t2."SUBL_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"SUBL_NME",t1.srno,"SUBL_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SUBL_NME","SUBL_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SUBL_NME",'' '')) U2,"SUBL_NME","SUBL_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SUBL_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SUBL_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SUBL_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SUBL_NME","SUBL_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SUBL_NME",'' '')) U2,"SUBL_NME","SUBL_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SUBL_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SUBL_NME","SUBL_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SUBL_NME","SUBL_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("SUBL_NME"),MAX("SUBL_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "SUBL_NME",TAB4.M5 AS "SUBL_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;	
	

SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"SSLC_NME"=t1."SSLC_NME","SSLC_ID"=t1."SSLC_ID",STATUS=''FUZZY_SSLC_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."SSLC_NME",t1."SSLC_ID" from(
WITH TAB5 AS(
select raw_name,"SSLC_NME",t1.srno,"SSLC_ID" from (
select t1.srno,t1.raw_name,t2."SSLC_NME",t2."SSLC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
'||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
=
soundex(split_part(t2."SSLC_NME",'' '',1))||soundex(split_part(t2."SSLC_NME",'' '',2)) 
||soundex(split_part(t2."SSLC_NME",'' '',3))||soundex(split_part(t2."SSLC_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"SSLC_NME",t1.srno,"SSLC_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SSLC_NME","SSLC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SSLC_NME",'' '')) U2,"SSLC_NME","SSLC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SSLC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SSLC_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SSLC_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SSLC_NME","SSLC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SSLC_NME",'' '')) U2,"SSLC_NME","SSLC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SSLC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SSLC_NME","SSLC_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SSLC_NME","SSLC_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("SSLC_NME"),MAX("SSLC_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "SSLC_NME",TAB4.M5 AS "SSLC_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;	
	
	
SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET "Unmatch_String"=t1.remaining_string,"SSLC_NME"=t1."SSLC_NME","SSLC_ID"=t1."SSLC_ID",STATUS=''FUZZY_SSLC_MATCHED''
,"Match_String"=t1.RAW_NAME1 FROM(
select t1.remaining_string,t1.sim,t1.srno,t1."raw_name",t1."raw_name1",t1."SSLC_NME",t1."SSLC_ID" from(
WITH TAB5 AS(
select raw_name,"SSLC_NME",t1.srno,"SSLC_ID" from (
select t1.srno,t1.raw_name,t2."SSLC_NME",t2."SSLC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
'||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
||soundex(split_part(t1.raw_name,'' '',3))||soundex(split_part(t1.raw_name,'' '',4))=
soundex(split_part(t2."SSLC_NME",'' '',1))||soundex(split_part(t2."SSLC_NME",'' '',2)) 
||soundex(split_part(t2."SSLC_NME",'' '',3))||soundex(split_part(t2."SSLC_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"SSLC_NME",t1.srno,"SSLC_ID" 	
),
TAB3 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SSLC_NME","SSLC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SSLC_NME",'' '')) U2,"SSLC_NME","SSLC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SSLC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SSLC_NME" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SSLC_NME" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2 FROM TAB2 GROUP BY SRNO ORDER BY SRNO ),TAB4 AS(
WITH TAB2 AS(
WITH TAB1 AS(
SELECT SRNO,RAW_NAME,ROW_NUMBER() OVER() AS ID,U1,SOUNDEX(U1) S1,U2,SOUNDEX(U2) S2,"SSLC_NME","SSLC_ID" FROM(
SELECT SRNO,RAW_NAME,UNNEST(STRING_TO_ARRAY(RAW_NAME,'' '')) U1,UNNEST(STRING_TO_ARRAY("SSLC_NME",'' '')) U2,"SSLC_NME","SSLC_ID" FROM TAB5 ORDER BY SRNO
) AS T1 ) SELECT SIMILARITY(array_to_string(array_agg(U1 order by ID asc),'' ''),"SSLC_NME")*100 AS SIM,SRNO,RAW_NAME,array_to_string(array_agg(U1 order by ID asc),'' '')AS RAW_NAME1,"SSLC_NME","SSLC_ID" FROM TAB1   WHERE S1=S2 GROUP BY SRNO,RAW_NAME,"SSLC_NME","SSLC_ID" 
) SELECT MAX(SIM) M1,MAX(SRNO) M2,MAX(RAW_NAME) M3,MAX(RAW_NAME1) M4,MAX("SSLC_NME"),MAX("SSLC_ID") M5 FROM TAB2 GROUP BY SRNO ORDER BY SRNO
) SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TAB4.M3,TAB4.M4,'''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g''),'' '') as REMAINING_STRING
,TAB4.M1 AS SIM,TAB4.M2 AS SRNO,TAB4.M3 AS RAW_NAME,TAB4.M4 AS RAW_NAME1,TAB4.MAX AS "SSLC_NME",TAB4.M5 AS "SSLC_ID" FROM TAB3,TAB4 WHERE TAB3.M1=TAB4.M1 AND TAB3.M2=TAB4.M2
) as t1 where sim >= 30) as t1
WHERE A.SRNO=t1.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;	

RETURN 1;
	EXCEPTION
	WHEN OTHERS THEN
	GET STACKED DIAGNOSTICS 
		f1=MESSAGE_TEXT,
		f2=PG_EXCEPTION_CONTEXT; 
		RAISE info 'error caught:%',f1;
		RAISE info 'error caught:%',f2;
		--SQLQuery = FORMAT('INSERT INTO %1$s (table_name,table_schema,message,context) Values(''%2$s'',''%3$s'',''%4$s'',''%5$s'')',error_tab_name,output_table,outputschema,f1,f2);
		--EXECUTE SQLQuery;
		
	RETURN -1;
END

$BODY$;

ALTER FUNCTION mmi_master.gstndata_fuzzy(text, text, text, text)
    OWNER TO postgres;
