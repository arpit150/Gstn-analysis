-- FUNCTION: mmi_master.gstndata_after_cleaning_process(text, text, integer, integer, text)

-- DROP FUNCTION mmi_master.gstndata_after_cleaning_process(text, text, integer, integer, text);

CREATE OR REPLACE FUNCTION mmi_master.gstndata_after_cleaning_process(
	raw_schema text,
	raw_table text,
	dst_id integer,
	stt_id integer,
	city_name text DEFAULT 0)
    RETURNS text
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE 
f1 text; f2 text;
t1 text; t2 text;
DECLARE SQLQuery text;
error_tab_name text;
BEGIN 
error_tab_name = 'gstn_output_data.gstn_error';

SQLQuery='delete from '||raw_schema||'."'|| raw_table ||'" where raw_name ~''^[0-9]+$''';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='delete from '||raw_schema||'."'|| raw_table ||'" where length (raw_name) <=3';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='delete from '||raw_schema||'."'|| raw_table ||'" where raw_name='' '' ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='DELETE FROM '||raw_schema||'."'|| raw_table ||'" where lower(raw_name) IN 
(select LOWER("NAME") from mmi_master."SUBDISTRICT_BOUNDARY"  where "DST_ID"='||DST_ID||')';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='DELETE FROM '||raw_schema||'."'|| raw_table ||'" where lower(raw_name) IN 
(select LOWER("NAME") from mmi_master."VILLAGE_CENTRE" where "STT_ID"='||STT_ID||' and "DST_ID"='||DST_ID||')';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='update '||raw_schema||'."'|| raw_table ||'" a set "raw_name"=b.clean_name from (
with b AS (select UPPER("find_word") "find_word" ,UPPER("repl_word") "repl_word" from mmi_master.cleansing_ref group by upper("find_word"),UPPER("repl_word"))
select a.srno, a.raw_name,trim(trim(regexp_replace(upper(a.raw_name),upper(b."find_word"),coalesce("repl_word",'''')),'','')) clean_name 
from '||raw_schema||'."'|| raw_table ||'" a, b where (upper(a.raw_name) like upper(''% ''||b."find_word"||'' %'') or upper(a.raw_name) like upper(''% ''||b."find_word"||'''')
or upper(a.raw_name) like upper(''''||b."find_word"||'' %''))) b where a."raw_name"=b."raw_name" ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='with tab1 as (
select * from (select ltrim(unnest(string_to_array("raw_name",'','')),'' '') AS "raw_name1",* from '||raw_schema||'."'|| raw_table ||'"
) AS t1 where 
lower("raw_name1")  like any(string_to_array(lower("houseName"),'';'')) or 
lower("raw_name1")  like any(string_to_array(lower("poi"),'';'')) or 
lower("raw_name1")  like any(string_to_array(lower("street"),'';'')) or 
lower("raw_name1")  like any(string_to_array(lower("subSubLocality"),'';''))or 
lower("raw_name1")  like any(string_to_array(lower("subLocality"),'';''))or 
lower("raw_name1")  like any(string_to_array(lower("locality"),'';''))or
lower("raw_name1")  like any(string_to_array(lower("village"),'';'')) or
lower("raw_name1")  like any(string_to_array(lower("subDistrict"),'';''))or
lower("raw_name1")  like any(string_to_array(lower("district"),'';''))or
lower("raw_name1")  like any(string_to_array(lower("city"),'';''))or
lower("raw_name1")  like any(string_to_array(lower(state),'';''))
)
update '||raw_schema||'."'|| raw_table ||'" t1 set "raw_name"='''' from tab1 where  t1.srno=tab1.srno';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

							 
SQLQuery='with tab4 as(
with tab3 as(						   
with tab1 as(						   
select srno,"raw_name",unnest(string_to_array( "raw_name",'','')) from '||raw_schema||'."'|| raw_table ||'"),
tab2 as (select bulding from mmi_master.unique_building_bck group by bulding  )
select tab1.*,tab2.bulding
from tab1,tab2 where lower(tab1.unnest)=lower(tab2.bulding)				  
) 
select srno,array_to_string(array_remove(string_to_array(lower( "raw_name"),'',''),lower(unnest)),'','') as new_k, "raw_name",unnest from tab3)
update '||raw_schema||'."'|| raw_table ||'" t1 set  "raw_name"=tab4.new_k from tab4 where t1.srno=tab4.srno';							 
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='update '||raw_schema||'."'|| raw_table ||'"  a set "raw_name"=b.regexp_replace from (
select srno,raw_name,regexp_replace(lower("raw_name"),'''||city_name||''','''',''g'') from '||raw_schema||'."'|| raw_table ||'" where raw_name ilike ''%'||city_name||'''
)b where a."raw_name"=b."raw_name" and a.srno=b.srno';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='update '||raw_schema||'."'|| raw_table ||'"  a set "raw_name"=b.btrim from (
select srno,raw_name,trim(trim(trim(trim("raw_name",''[,]''),''[-]''),''[ ]''),''[.]'')from '||raw_schema||'."'|| raw_table ||'" 
)b where a."raw_name"=b."raw_name" and a.srno=b.srno';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='update '||raw_schema||'."'|| raw_table ||'"  a set "raw_name"=b.regexp_replace from (
select srno,raw_name,regexp_replace(regexp_replace("raw_name",''[-]'','' ''),''\s+'','' '',''g'') from '||raw_schema||'."'|| raw_table ||'"
)b where a."raw_name"=b."raw_name" and a.srno=b.srno ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='delete from '||raw_schema||'."'|| raw_table ||'"  where raw_name = '''' ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

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

ALTER FUNCTION mmi_master.gstndata_after_cleaning_process(text, text, integer, integer, text)
    OWNER TO postgres;
