-- FUNCTION: mmi_master.gstndata_combination_process(text, text, text, text, text)

-- DROP FUNCTION mmi_master.gstndata_combination_process(text, text, text, text, text);

CREATE OR REPLACE FUNCTION mmi_master.gstndata_combination_process(
	schema_name text,
	output_data_table text,
	combo_table text,
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
error_tab_name = 'gstn_output_data.gstn_error';

sqlq= 'drop table if exists '||schema_name||'."'|| combo_table ||'"';
execute sqlq;
--RAISE INFO 'sql-> %', sqlq;

SQLQuery='create table '||schema_name||'."'|| combo_table ||'" as(
SELECT * FROM '||schema_name||'."'|| output_data_table ||'"
WHERE status = ''NOT_MATCHED'')';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;

sqlq= 'drop table if exists '||schema_name||'."'|| combo_table||'_1"';
execute sqlq;
--RAISE INFO 'sql-> %', sqlq;

SQLQuery='create table '||schema_name||'."'|| combo_table||'_1"(
srno integer,
raw_name text,
raw_name1 text)';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
	
SQLQuery='WITH TAB1 AS(
SELECT T1.SRNO,T1.RAW_NAME,T2."NAME",T2."LOC_NME",T2."LOC_ID",T2."SUBL_NME",T2."SUBL_ID" FROM 
(SELECT SRNO,REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(RAW_NAME,''[,]'','''',''g''),''[-]'','' '',''g''),''\s+'','' '',''g'') AS RAW_NAME 
FROM '||schema_name||'."'|| output_data_table ||'" WHERE STATUS=''NOT_MATCHED'') AS T1 INNER JOIN (SELECT REGEXP_REPLACE("NAME",''[,]'','''',''g'') as "NAME1",* FROM '||master_schema_name||'."'|| master_table_name ||'" ) AS T2 
ON LOWER(T1.RAW_NAME)=LOWER(T2."NAME1")) 
UPDATE '||schema_name||'."'|| output_data_table ||'" as a SET "M_LOC_NME"=TAB1."LOC_NME","M_LOC_ID"=TAB1."LOC_ID","SUBL_NME"=TAB1."SUBL_NME",
"SUBL_ID"=TAB1."SUBL_ID",STATUS=''EXACT_MATCH'',"Unmatch_String"='''' from tab1 where tab1.srno=a.srno ';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;

SQLQuery='do $$
declare 
k integer;
begin 

for k in select srno from '||schema_name||'."'|| combo_table ||'"order by srno 
loop
insert into '||schema_name||'."'|| combo_table||'_1"(srno,raw_name,RAW_NAME1)
select ct,combo,RAW_NAME1 from(															 
with recursive t(srno,i,RAW_NAME1) as (select srno, unnest(string_to_array(lower(raw_name),'' '')) ,RAW_NAME AS RAW_NAME1
from '||schema_name||'."'|| combo_table ||'" where srno=k),cte as									
(  
   	select  i as combo, i, srno as ct,RAW_NAME1 from t
   	union all
	select cte.combo||'' ''||t.i,t.i,ct,T.RAW_NAME1 from cte join t on t.i>cte.i
	
),cte2 as 
(
	select  i as combo, i,srno as ct,RAW_NAME1 from t
   	union all
	select cte2.combo||'' ''||t.i,t.i,ct,T.RAW_NAME1 from cte2 join t on t.i<cte2.i
)
select ct,combo,RAW_NAME1 from cte2 union all select ct,combo,RAW_NAME1 from cte union all select SRNO,lower(raw_name),raw_name as raw_name1 from '||schema_name||'."'|| combo_table ||'" where srno=k ) as S group by combo,ct,raw_name1 order by ct,combo,raw_name1;
												 
end loop;
end;
$$ ';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
	

SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" A set "M_LOC_NME"=K."LOC_NME","M_LOC_ID"=K."LOC_ID","Unmatch_String"=k.rg,"Match_String"="LOC_NME"
,status=''COMB_LOC_MATCHED''FROM 
(select t1.raw_name,t2."LOC_NME",t2."LOC_ID",''LOC_MATCHED'' AS MATCHED,t1.srno,t1.raw_name1,regexp_replace(trim(regexp_replace(lower(t1.raw_name1),lower("LOC_NME"),'''',''g''),''  ''),''\s+'','' '',''g'') as rg
from '||schema_name||'."'|| combo_table||'_1" as t1 inner join '||master_schema_name||'."'|| master_table_name ||'" AS t2 on
lower(t1.raw_name)=lower(t2."LOC_NME") group by t1.raw_name,t2."LOC_NME",t2."LOC_ID",t1.srno,t1.raw_name1,rg 																		 
) AS K WHERE  A.SRNO=K.SRNO ';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;

SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" A SET "Unmatch_String"=T2."Unmatch_String" FROM
(
SELECT T1.SRNO,REGEXP_REPLACE(lower(T1."Unmatch_String"),lower(t2."SUBL_NME"),'''',''g'') as "Unmatch_String"  from '||schema_name||'."'|| output_data_table ||'" as t1
inner join
(SELECT SRNO,lower("SUBL_NME") as "SUBL_NME" FROM 
(SELECT T1.RAW_NAME,T1."LOC_ID",T1."LOC_NME",T2."LOC_NME",T2."LOC_ID",T2."SUBL_ID",T2."SUBL_NME",T2.SRNO,T2.RAW_NAME1,T2.regexp_replace FROM
(select t1.raw_name,t2."LOC_NME",t2."LOC_ID",T2."SUBL_ID",''LOC_MATCHED'' AS MATCHED,t1.srno,t1.raw_name1,regexp_replace(trim(regexp_replace(lower(t1.raw_name1),lower("LOC_NME"),'''',''g''),''  ''),''\s+'','' '',''g'') as rg
from '||schema_name||'."'|| combo_table||'_1" as t1 inner join '||master_schema_name||'."'|| master_table_name ||'" AS t2 on
lower(t1.raw_name)=lower(t2."LOC_NME") group by t1.raw_name,t2."LOC_NME",t2."LOC_ID",T2."SUBL_ID",t1.srno,t1.raw_name1,rg) AS T1
INNER JOIN
(select t1.raw_name,t2."SUBL_NME",T2."LOC_ID",T2."LOC_NME",t2."SUBL_ID",''SUBL_MATCHED'' AS MATCHED,t1.srno,t1.raw_name1,regexp_replace(trim(regexp_replace(lower(t1.raw_name1),lower("SUBL_NME"),'''',''g''),''  ''),''\s+'','' '',''g'')
from '||schema_name||'."'|| combo_table||'_1" as t1 inner join '||master_schema_name||'."'|| master_table_name ||'" AS t2 on
lower(t1.raw_name)=lower(t2."SUBL_NME") group by t1.raw_name,t2."SUBL_NME",T2."LOC_ID",T2."LOC_NME",t2."SUBL_ID",t1.srno,t1.raw_name1,regexp_replace
) AS T2 ON T1."LOC_ID"=T2."LOC_ID" AND T1.SRNO=T2.SRNO GROUP BY T1.RAW_NAME,T1."LOC_ID",T1."LOC_NME",T2."LOC_NME",T2."LOC_ID",T2."SUBL_ID",T2."SUBL_NME",T2.SRNO,T2.RAW_NAME1,T2.regexp_replace
) AS K ) AS T2
on t1.srno=t2.srno) AS T2 WHERE A.srno=T2.srno ';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;

SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" A set "SUBL_NME"=K."SUBL_NME","SUBL_ID"=K."SUBL_ID","Match_String"=(K.LOC_N1||'',''||K."SUBL_NME")   
,status=''COMB_LOC_MATCHED''||'',''||''COMB_SUBL_MATCHED'' FROM 
(
SELECT T1.RAW_NAME,T1."LOC_ID" AS LOC1,T1."LOC_NME" AS LOC_N1,T2."LOC_NME" AS LOC_N2,T2."LOC_ID" AS LOC2,T2."SUBL_ID",T2."SUBL_NME",T2.SRNO,T2.RAW_NAME1,
T2.regexp_replace FROM
(select t1.raw_name,t2."LOC_NME",t2."LOC_ID",T2."SUBL_ID",''LOC_MATCHED'' AS MATCHED,t1.srno,t1.raw_name1,regexp_replace(trim(regexp_replace(lower(t1.raw_name1),lower("LOC_NME"),'''',''g''),''  ''),''\s+'','' '',''g'') as rg
from '||schema_name||'."'|| combo_table||'_1" as t1 inner join '||master_schema_name||'."'|| master_table_name ||'" AS t2 on
lower(t1.raw_name)=lower(t2."LOC_NME") group by t1.raw_name,t2."LOC_NME",t2."LOC_ID",
T2."SUBL_ID",t1.srno,t1.raw_name1,rg) AS T1
INNER JOIN
(select t1.raw_name,t2."SUBL_NME",T2."LOC_ID",T2."LOC_NME",t2."SUBL_ID",''SUBL_MATCHED'' AS MATCHED,t1.srno,t1.raw_name1,regexp_replace(trim(regexp_replace(lower(t1.raw_name1),lower("SUBL_NME"),'''',''g''),''  ''),''\s+'','' '',''g'')
from '||schema_name||'."'|| combo_table||'_1" as t1 inner join '||master_schema_name||'."'|| master_table_name ||'" AS t2 on
lower(t1.raw_name)=lower(t2."SUBL_NME") group by t1.raw_name,t2."SUBL_NME",T2."LOC_ID",T2."LOC_NME",t2."SUBL_ID",t1.srno,t1.raw_name1,regexp_replace
) AS T2 ON T1."LOC_ID"=T2."LOC_ID" AND T1.SRNO=T2.SRNO GROUP BY T1.RAW_NAME,T1."LOC_ID",T1."LOC_NME",T2."LOC_NME",T2."LOC_ID",T2."SUBL_ID",T2."SUBL_NME",T2.SRNO,T2.RAW_NAME1,T2.regexp_replace
) AS K 
WHERE A.SRNO=K.SRNO ';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;

SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" A SET "Unmatch_String"=T2."Unmatch_String" FROM
(
SELECT T1.SRNO,REGEXP_REPLACE(lower(T1."Unmatch_String"),lower(t2."SSLC_NME"),'''',''g'') as "Unmatch_String"  from '||schema_name||'."'|| output_data_table ||'" as t1
inner join
(SELECT SRNO,lower("SSLC_NME") as "SSLC_NME" FROM 
(SELECT T1.RAW_NAME,T1."LOC_ID",T1."LOC_NME",T2."LOC_NME",T2."LOC_ID",T2."SSLC_ID",T2."SSLC_NME",T2.SRNO,T2.RAW_NAME1,T2.regexp_replace FROM
(select t1.raw_name,t2."LOC_NME",t2."LOC_ID",T2."SSLC_ID",''LOC_MATCHED'' AS MATCHED,t1.srno,t1.raw_name1,regexp_replace(trim(regexp_replace(lower(t1.raw_name1),lower("LOC_NME"),'''',''g''),''  ''),''\s+'','' '',''g'') as rg
from '||schema_name||'."'|| combo_table||'_1" as t1 inner join '||master_schema_name||'."'|| master_table_name ||'" AS t2 on
lower(t1.raw_name)=lower(t2."LOC_NME") group by t1.raw_name,t2."LOC_NME",t2."LOC_ID",T2."SSLC_ID",t1.srno,t1.raw_name1,rg) AS T1
INNER JOIN
(select t1.raw_name,t2."SSLC_NME",T2."LOC_ID",T2."LOC_NME",t2."SSLC_ID",''SUBL_MATCHED'' AS MATCHED,t1.srno,t1.raw_name1,regexp_replace(trim(regexp_replace(lower(t1.raw_name1),lower("SSLC_NME"),'''',''g''),''  ''),''\s+'','' '',''g'')
from '||schema_name||'."'|| combo_table||'_1" as t1 inner join '||master_schema_name||'."'|| master_table_name ||'" AS t2 on
lower(t1.raw_name)=lower(t2."SSLC_NME") group by t1.raw_name,t2."SSLC_NME",T2."LOC_ID",T2."LOC_NME",t2."SSLC_ID",t1.srno,t1.raw_name1,regexp_replace
) AS T2 ON T1."LOC_ID"=T2."LOC_ID" AND T1.SRNO=T2.SRNO GROUP BY T1.RAW_NAME,T1."LOC_ID",T1."LOC_NME",T2."LOC_NME",T2."LOC_ID",T2."SSLC_ID",T2."SSLC_NME",T2.SRNO,T2.RAW_NAME1,T2.regexp_replace
) AS K ) AS T2
on t1.srno=t2.srno) AS T2 WHERE A.srno=T2.srno ';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
	

	

SQLQuery='UPDATE '||schema_name||'."'|| output_data_table ||'" A set "SSLC_NME"=K."SSLC_NME","SSLC_ID"=K."SSLC_ID","Match_String"=(K.LOC_N1||'',''||K."SSLC_NME")   
,status=''COMB_LOC_MATCHED''||'',''||''COMB_SSLC_MATCHED'' FROM 
(
SELECT T1.RAW_NAME,T1."LOC_ID" AS LOC1,T1."LOC_NME" AS LOC_N1,T2."LOC_NME" AS LOC_N2,T2."LOC_ID" AS LOC2,T2."SSLC_ID",T2."SSLC_NME",T2.SRNO,T2.RAW_NAME1,
T2.regexp_replace FROM
(select t1.raw_name,t2."LOC_NME",t2."LOC_ID",T2."SSLC_ID",''LOC_MATCHED'' AS MATCHED,t1.srno,t1.raw_name1,regexp_replace(trim(regexp_replace(lower(t1.raw_name1),lower("LOC_NME"),'''',''g''),''  ''),''\s+'','' '',''g'') as rg
from '||schema_name||'."'|| combo_table||'_1" as t1 inner join '||master_schema_name||'."'|| master_table_name ||'" AS t2 on
lower(t1.raw_name)=lower(t2."LOC_NME") group by t1.raw_name,t2."LOC_NME",t2."LOC_ID",
T2."SSLC_ID",t1.srno,t1.raw_name1,rg) AS T1
INNER JOIN
(select t1.raw_name,t2."SSLC_NME",T2."LOC_ID",T2."LOC_NME",t2."SSLC_ID",''SUBL_MATCHED'' AS MATCHED,t1.srno,t1.raw_name1,regexp_replace(trim(regexp_replace(lower(t1.raw_name1),lower("SSLC_NME"),'''',''g''),''  ''),''\s+'','' '',''g'')
from '||schema_name||'."'|| combo_table||'_1" as t1 inner join '||master_schema_name||'."'|| master_table_name ||'" AS t2 on
lower(t1.raw_name)=lower(t2."SSLC_NME") group by t1.raw_name,t2."SSLC_NME",T2."LOC_ID",T2."LOC_NME",t2."SSLC_ID",t1.srno,t1.raw_name1,regexp_replace
) AS T2 ON T1."LOC_ID"=T2."LOC_ID" AND T1.SRNO=T2.SRNO GROUP BY T1.RAW_NAME,T1."LOC_ID",T1."LOC_NME",T2."LOC_NME",T2."LOC_ID",T2."SSLC_ID",T2."SSLC_NME",T2.SRNO,T2.RAW_NAME1,T2.regexp_replace
) AS K 
WHERE A.SRNO=K.SRNO';
EXECUTE SQLQuery;
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;

SQLQuery='WITH TAB6 AS(
WITH TAB5 AS(
SELECT raw_name,"LOC_NME",t1.srno,"LOC_ID" FROM( select raw_name,"LOC_NME",t1.srno,"LOC_ID" from (
select t1.srno,t1.raw_name,t2."LOC_NME",t2."LOC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
 '||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
||soundex(split_part(t1.raw_name,'' '',3))||soundex(split_part(t1.raw_name,'' '',4))=
soundex(split_part(t2."LOC_NME",'' '',1))||soundex(split_part(t2."LOC_NME",'' '',2)) 
||soundex(split_part(t2."LOC_NME",'' '',3))||soundex(split_part(t2."LOC_NME",'' '',4))) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"LOC_NME",t1.srno,"LOC_ID" 	
UNION ALL													  
select raw_name,"LOC_NME",t1.srno,"LOC_ID" from (
select t1.srno,t1.raw_name,t2."LOC_NME",t2."LOC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
 '||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
=soundex(split_part(t2."LOC_NME",'' '',1))||soundex(split_part(t2."LOC_NME",'' '',2)) 
) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"LOC_NME",t1.srno,"LOC_ID"																  														  
UNION ALL
select raw_name,"LOC_NME",t1.srno,"LOC_ID" from (
select t1.srno,t1.raw_name,t2."LOC_NME",t2."LOC_ID" from (select * from '||schema_name||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') as t1,
 '||master_schema_name||'."'|| master_table_name ||'" t2 where soundex(split_part(t1.raw_name,'''',1))||soundex(split_part(t1.raw_name,'''',2))
=soundex(split_part(t2."LOC_NME",'' '',1))||soundex(split_part(t2."LOC_NME",'' '',2)) 
) as t1				   
WHERE SOUNDEX(RAW_NAME)<>SOUNDEX(''BARRA'') group by raw_name,"LOC_NME",t1.srno,"LOC_ID"	 ORDER BY SRNO
) AS T1 GROUP BY raw_name,"LOC_NME",t1.srno,"LOC_ID"),
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
) 	
UPDATE '||schema_name||'."'|| output_data_table ||'" AS A SET  "Unmatch_String"=TAB6.REMAINING_STRING,"M_LOC_NME"=TAB6."LOC_NME","M_LOC_ID"=TAB6."LOC_ID",STATUS=''FUZZY_LOC_MATCHED''
,"Match_String"=TAB6.RAW_NAME1 FROM TAB6 WHERE A.SRNO=TAB6.SRNO';
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

ALTER FUNCTION mmi_master.gstndata_combination_process(text, text, text, text, text)
    OWNER TO postgres;
