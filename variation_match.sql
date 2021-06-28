-- FUNCTION: mmi_master.variation_fun(text, text, text, text)

-- DROP FUNCTION mmi_master.variation_fun(text, text, text, text);

CREATE OR REPLACE FUNCTION mmi_master.variation_fun(
	production_schema text,
	city_name text,
	state_name text,
	output_data_table text DEFAULT 0)
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
stat_code text;
outputtablename text;
BEGIN 
error_tab_name = 'gstn_output_data.gstn_error';

--stat_code = UPPER(LEFT(UPPER(admin_table), 2));
--RAISE INFO 'State Code -> %', stat_code;

--outputtableName = 'gstn_output_'||Replace(city_name,' ','')||'_'||Replace(lower(stat_code),' ','');
--RAISE INFO 'outputtableName Code -> %', outputtableName;

SQLQuery = 'Drop Table If Exists '||production_schema||'."'|| city_name ||'_variation"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='CREATE TABLE '||production_schema||'."'|| city_name ||'_variation"
(
    "srno" serial,
    "ID" character varying COLLATE pg_catalog."default",
    "Admin_Name" character varying COLLATE pg_catalog."default",
    "Standard_Name" character varying COLLATE pg_catalog."default",
    city_nme character varying COLLATE pg_catalog."default",
    sdb_nme character varying COLLATE pg_catalog."default",
    dst_nme character varying COLLATE pg_catalog."default",
    stt_nme character varying COLLATE pg_catalog."default",
    city_id character varying COLLATE pg_catalog."default",
    sdb_id character varying COLLATE pg_catalog."default",
    dst_id character varying COLLATE pg_catalog."default",
    stt_id character varying COLLATE pg_catalog."default",
    pincode character varying COLLATE pg_catalog."default",
    type character varying COLLATE pg_catalog."default"	
)';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='insert into '||production_schema||'."'|| city_name ||'_variation"(
	"ID", "Admin_Name", "Standard_Name", city_nme, sdb_nme, dst_nme, stt_nme, city_id, sdb_id, dst_id, stt_id, pincode, type
) select * from parsed_data_match.all_india_var_data where  "city_nme" ilike ''%'||Replace(city_name,'_',' ')||'%'' and "stt_nme" ilike ''%'||state_name||'%''
';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Replace_String"=t1."Admin_Name","Unmatch_String"='' '',"Match_String"=t1."raw_name",
status=''Variation_Match'' from(	
SELECT T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') AS T1 
INNER JOIN '||production_schema||'."'|| city_name ||'_variation" AS T2 ON LOWER(T1."raw_name")=LOWER(T2."Admin_Name") and t1."PIN_CD"=t2.pincode
GROUP BY T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID"
)
as t1 where  A."srno"=t1."srno" ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Replace_String"=t1."Admin_Name","Unmatch_String"='' '',"Match_String"=t1."raw_name",
status=''Variation_Match'' from(	
SELECT T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') AS T1 
INNER JOIN '||production_schema||'."'|| city_name ||'_variation" AS T2 ON LOWER(T1."raw_name")=LOWER(T2."Admin_Name") 
GROUP BY T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID"
)
as t1 where  A."srno"=t1."srno"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery; 

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Replace_String"=t1."Admin_Name","Unmatch_String"='' '',"Match_String"=t1."raw_name",
status=''Variation_Match'' from(	
SELECT T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') AS T1 
INNER JOIN '||production_schema||'."'|| city_name ||'_variation" AS T2 ON soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
||soundex(split_part(t1.raw_name,'' '',3))||soundex(split_part(t1.raw_name,'' '',4))=
soundex(split_part(t2."Admin_Name",'' '',1))||soundex(split_part(t2."Admin_Name",'' '',2)) 
||soundex(split_part(t2."Admin_Name",'' '',3))||soundex(split_part(t2."Admin_Name",'' '',4))and t1."PIN_CD"=t2.pincode				   
GROUP BY T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID"
)
as t1 where  A."srno"=t1."srno"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery; 

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Replace_String"=t1."Admin_Name","Unmatch_String"='' '',"Match_String"=t1."raw_name",
status=''Variation_Fuzzy_match'' from(	
SELECT T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') AS T1 
INNER JOIN '||production_schema||'."'|| city_name ||'_variation" AS T2 ON soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
||soundex(split_part(t1.raw_name,'' '',3))||soundex(split_part(t1.raw_name,'' '',4))=
soundex(split_part(t2."Admin_Name",'' '',1))||soundex(split_part(t2."Admin_Name",'' '',2)) 
||soundex(split_part(t2."Admin_Name",'' '',3))||soundex(split_part(t2."Admin_Name",'' '',4))			   
GROUP BY T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID"
)
as t1 where  A."srno"=t1."srno" ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Replace_String"=t1."Admin_Name","Unmatch_String"='' '',"Match_String"=t1."raw_name",
status=''Variation_Fuzzy_match'' from(	
SELECT T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') AS T1 
INNER JOIN '||production_schema||'."'|| city_name ||'_variation" AS T2 ON soundex(split_part(t1.raw_name,'' '',1))||soundex(split_part(t1.raw_name,'' '',2))
||soundex(split_part(t1.raw_name,'' '',3))||soundex(split_part(t1.raw_name,'' '',4))||soundex(split_part(t1.raw_name,'' '',5))=
soundex(split_part(t2."Admin_Name",'' '',1))||soundex(split_part(t2."Admin_Name",'' '',2)) 
||soundex(split_part(t2."Admin_Name",'' '',3))||soundex(split_part(t2."Admin_Name",'' '',4))			   
GROUP BY T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID"
)
as t1 where  A."srno"=t1."srno" ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Replace_String"=t1."Admin_Name","Unmatch_String"='' '',"Match_String"=t1."raw_name",
status=''Variation_Match'' from(	
SELECT T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') AS T1 
INNER JOIN '||production_schema||'."'|| city_name ||'_variation" AS T2 ON soundex(split_part(t1.raw_name,'''',1))||soundex(split_part(t1.raw_name,'''',2))
||soundex(split_part(t1.raw_name,'''',3))||soundex(split_part(t1.raw_name,'''',4))=
soundex(split_part(t2."Admin_Name",'' '',1))||soundex(split_part(t2."Admin_Name",'' '',2)) 
||soundex(split_part(t2."Admin_Name",'' '',3))||soundex(split_part(t2."Admin_Name",'' '',4))			   
GROUP BY T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID"
)
as t1 where  A."srno"=t1."srno"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery; 

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Replace_String"=t1."Admin_Name","Unmatch_String"='' '',"Match_String"=t1."raw_name",
status=''Variation_Match'' from(	
SELECT T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID",T2."type" from
(
select srno,"raw_name" as b ,REGEXP_REPLACE("raw_name",'' '',
'''',''g'') as raw_name from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED''
) AS T1 
INNER JOIN (select "Admin_Name" as a,"ID","type",REGEXP_REPLACE ("Admin_Name",'' '',
'''',''g'') as "Admin_Name" from '||production_schema||'."'|| city_name ||'_variation") AS T2 ON LOWER(T1."raw_name")=LOWER(T2."Admin_Name") 
GROUP BY T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID",T2."type"
)
as t1 where  A."srno"=t1."srno"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery; 

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Replace_String"=t1."Admin_Name","Unmatch_String"='' '',"Match_String"=t1."raw_name",
status=''Variation_Match'' from(	
SELECT T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'') AS T1 
INNER JOIN '||production_schema||'."'|| city_name ||'_variation" AS T2 ON LOWER(T1."raw_name")=LOWER(T2."Admin_Name") and t1."city_id"=t2.city_id
GROUP BY T1.SRNO,T1."raw_name",T2."Admin_Name",T2."ID"
)
as t1 where  A."srno"=t1."srno"';
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

ALTER FUNCTION mmi_master.variation_fun(text, text, text, text)
    OWNER TO postgres;
