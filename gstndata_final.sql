
select mmi_master.gstndata_street_final('street_raw_data','raw_street_kanpur','264','10','kanpur','15768',
										'mmi_master_road','UP_ROAD_NETWORK','street_output_data','KANPUR_ADDR_ADMIN_R')


-- FUNCTION: mmi_master.gstndata_final_parsed_2(text, text, integer, integer, text, text, integer, text, text)

-- DROP FUNCTION mmi_master.gstndata_final_parsed_2(text, text, integer, integer, text, text, integer, text, text);

CREATE OR REPLACE FUNCTION mmi_master.gstndata_final_parsed_2(
	production_schema text,
	raw_table text,
	dst_id integer,
	stt_id integer,
	city_name text,
	state_name text,
	city_id integer,
	master_schema text,
	admin_table text DEFAULT 0)
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

stat_code = UPPER(LEFT(UPPER(admin_table), 2));
RAISE INFO 'State Code -> %', stat_code;

outputtableName = 'gstn_output_'||Replace(city_name,' ','')||'_'||Replace(lower(stat_code),' ','')||'_3';
RAISE INFO 'outputtableName Code -> %', outputtableName;

SQLQuery = 'Drop Table If Exists '||production_schema||'."'|| raw_table ||'"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='CREATE TABLE '||production_schema||'."'|| raw_table ||'"
(
     srno serial,
    "FID" integer,
	"CHECKSUM" character varying ,
    "raw_name" character varying COLLATE pg_catalog."default",
    "City" character varying COLLATE pg_catalog."default",
    "District" character varying COLLATE pg_catalog."default",
    "State" character varying COLLATE pg_catalog."default",
    "pincode" character varying COLLATE pg_catalog."default",
    "ALL_FID" text COLLATE pg_catalog."default",
	"ALL_CHECKSUM" text COLLATE pg_catalog."default"
)';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='CREATE INDEX indx_srno_'||city_name||'
    ON '||production_schema||'."'|| raw_table ||'" USING btree
    (srno)
    TABLESPACE pg_default';
	
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
    
    
SQLQuery='INSERT INTO '||production_schema||'."'|| raw_table ||'"(
"FID", "CHECKSUM", "raw_name", "City", "District", "State", pincode, "ALL_FID", "ALL_CHECKSUM")
select "FID", "CHECKSUM", "raw_name", "City", "District", "State", pincode, "ALL_FID", "ALL_CHECKSUM"
from admin_match_test.gstn_bengaluru_hv_new where "City" ilike ''%'||Replace(city_name,'_',' ')||'%'' and "State" ilike ''%'||state_name||'%'' ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
    
	

SQLQuery='select mmi_master.gstndata_after_cleaning_process('''||production_schema||''','''|| raw_table ||''','''|| dst_id ||''','''|| stt_id ||''','''||city_name||''')';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
    

SQLQuery = 'Drop Table If Exists  '||production_schema||'."unique_'|| raw_table ||'"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
	
SQLQuery='create table '||production_schema||'."unique_'|| raw_table ||'" as 
select row_number() over()::integer as srno,"FID","CHECKSUM",pincode as "PIN_CD" from '||production_schema||'."'|| raw_table ||'" group by raw_name,"FID","CHECKSUM","pincode"';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
    

SQLQuery='select mmi_master.gstndata_process('''||production_schema||''',''unique_'|| raw_table ||''','''||master_schema||''','''||admin_table||''','''','''||production_schema||''','''||city_name||''','''||city_id||''')';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
	
SQLQuery='select mmi_master.gstndata_combination_process('''||production_schema||''','''||outputtableName||''',''gstn_'||city_name||'_comboination'','''||master_schema||''','''||UPPER(city_name)||'_ADDR_ADMIN_R'')';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
	
SQLQuery='select mmi_master.gstndata_ref_data('''||production_schema||''','''|| raw_table ||''','''|| city_name ||''','''||master_schema||''','''||UPPER(city_name)||'_ADDR_ADMIN_R'','''||outputtableName||''')';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
	
SQLQuery='select mmi_master.gstndata_fuzzy('''||production_schema||''','''||outputtableName||''','''||master_schema||''','''||UPPER(city_name)||'_ADDR_ADMIN_R'')';
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

ALTER FUNCTION mmi_master.gstndata_final_parsed_2(text, text, integer, integer, text, text, integer, text, text)
    OWNER TO postgres;
