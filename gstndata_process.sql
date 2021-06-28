-- FUNCTION: mmi_master.gstndata_process(text, text, text, text, text, text, text, integer)

-- DROP FUNCTION mmi_master.gstndata_process(text, text, text, text, text, text, text, integer);

CREATE OR REPLACE FUNCTION mmi_master.gstndata_process(
	gstn_raw_tab_schema text,
	gstn_raw_tab_name text,
	master_schema text,
	master_addr_admin_r text,
	master_addr_admin_p text,
	outputschema text,
	city_name text,
	city_id integer DEFAULT 0)
    RETURNS text
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE 
f1 text; f2 text;
t1 text; t2 text;
count integer;
master_column_names_admin_r text[];
master_column_names_admin_p text[];
master_column_valid text;
col_name text;
outputtableName text;
error_table text;
i text;
sqlquery text;
master_db_table text;
nloc_dicn text;
sloc_dicn text;
sslcloc_dicn text;
count_table text;
stat_code text;
BEGIN   
	stat_code = UPPER(LEFT(UPPER(master_addr_admin_r), 2));
RAISE INFO 'State Code -> %', stat_code;

	count_table = 'status_report';
	error_table = 'gstn_error';
	outputtableName = 'gstn_output_'||Replace(city_name,' ','')||'_'||Replace(lower(stat_code),' ','');
RAISE INFO 'outputtableName Code -> %', outputtableName;

	-- Check table and schema exits or not 
	If isschemaexist(gstn_raw_tab_schema)=FALSE THEN
	   RETURN FORMAT('Schema Does Not Exist %1$s',gstn_raw_tab_schema);
	END IF;
	
	If isschemaexist(master_schema)=FALSE THEN
		RETURN FORMAT('Schema Does Not Exist %1$s',master_schema);
	END IF;
	
	If isschemaexist(outputschema)=FALSE THEN
		RETURN FORMAT('Schema Does Not Exist %1$s',outputschema);
	END IF;
	
	If istableexist(gstn_raw_tab_schema,gstn_raw_tab_name)=FALSE THEN
		RETURN FORMAT('Table Does Not Exist %1$s.%2$s',gstn_raw_tab_schema,gstn_raw_tab_name);
	END IF;
	
	If istableexist(master_schema,master_addr_admin_r)=FALSE THEN
		RETURN FORMAT('Table Does Not Exist %1$s.%2$s',master_schema,master_addr_admin_r);
	END IF;
	
	/*If istableexist(master_schema,master_addr_admin_p)=FALSE THEN
		RETURN FORMAT('Table Does Not Exist %1$s.%2$s',master_schema,master_addr_admin_p);
	END IF;*/
	
	-- Create Count Table
	BEGIN
		--sqlquery = FORMAT('Drop Table If Exists %1$s.%2$s',outputschema,count_table);
		--EXECUTE sqlquery;
		
		--sqlquery = FORMAT('CREATE TABLE %1$s.%2$s (id serial,table_name text,city_name text,exact_match_count integer,not_match_count integer,loc_match_count integer,sub_loc_match_count integer,sslc_loc_match_count integer,n_loc_match_count integer,n_sub_loc_match_count integer,n_sslc_match_count integer,unmatch_count integer)',outputschema,count_table);
		sqlquery = FORMAT('CREATE TABLE IF NOT EXISTS %1$s.%2$s (id serial,table_name text,city_name text,status text,s_count integer)',outputschema,count_table);
		EXECUTE sqlquery;
		
		EXCEPTION
			WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS 
				f1=MESSAGE_TEXT,
				f2=PG_EXCEPTION_CONTEXT; 
			RAISE info 'MESSAGE:% CONTEXT:%',f1,f2;
			RETURN 'UNABLE TO CREATE COUNT TABLE';
	END;
	
	-- Create Exception Table
	BEGIN
		--sqlquery = FORMAT('Drop Table If Exists %1$s.%2$s',outputschema,error_table);
		--EXECUTE sqlquery;
		
		sqlquery = FORMAT('CREATE TABLE IF NOT EXISTS %1$s.%2$s (id serial,table_name text,message text,context text)',outputschema,error_table);
		EXECUTE sqlquery;
		
		EXCEPTION
			WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS 
				f1=MESSAGE_TEXT,
				f2=PG_EXCEPTION_CONTEXT; 
			RAISE info 'MESSAGE:% CONTEXT:%',f1,f2;
			RETURN 'UNABLE TO CREATE FUNCTION EXCEPTION TABLE';
	END;
	
	-- Output Table Create
	BEGIN
		sqlquery = FORMAT('Drop Table If Exists %1$s.%2$s',outputschema,outputtableName);
		EXECUTE sqlquery;
		
		sqlquery = Format('
		CREATE TABLE %1$s.%2$s
		(
		  srno serial not null,
		  raw_name text,
		  "M_LOC_NME" text,
		  "M_LOC_ID" integer,
		  "SUBL_NME" character varying(100),
		  "SUBL_ID" integer,
		  "SSLC_NME" character varying(100),
		  "SSLC_ID" integer,
		  "ID" integer,
		  "Unmatch_String" text,
		  "Match_String" text,
		  "pincode" character varying(100),			
		  "Replace_String" character varying(100),
		   status text,
		  "N_LOC" text,
		  "N_LOC_MATCHED" text,
		  "N_LOC_MATCHED_ID" integer,
		  "N_SUBL_MATCHED" text,
		  "N_SUBL_MATCHED_ID" integer,
		  "N_SSLC_MATCHED" text,
		  "N_SSLC-MATCHED_ID" integer,
		  "M_N_LOC" text,
		  "M_N_LOC_MATCHED_ID" integer	  
		)',outputschema,outputtableName);
		
		EXECUTE sqlquery;
		
		EXCEPTION
			WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS 
				f1=MESSAGE_TEXT,
				f2=PG_EXCEPTION_CONTEXT; 
			RAISE info 'MESSAGE:% CONTEXT:%',f1,f2;
			RETURN 'UNABLE TO CREATE OUTPUT ERROR TABLE';
	END;
	
	-- Validate Table Structure
	
	-- Raw Table Structure (raw_name)
	if iscolumnexist(gstn_raw_tab_schema,gstn_raw_tab_name,'raw_name')=FALSE THEN
		RETURN FORMAT('Column <raw_name> Does Not Exist Into Raw Table <%1$s.%2$s>',gstn_raw_tab_schema,gstn_raw_tab_name);
	end if;
	
	-- Master Table Structure ()
	master_column_names_admin_r = ARRAY['LOC_ID','LOC_NME','SSLC_ID','SSLC_NME','SUBL_ID','SUBL_NME','TYPE','CITY_ID','STT_ID','SP_GEOMETRY'];
	--master_column_names_admin_p = ARRAY['ID','NAME','ADM_CRY','ADRADMN_ID','CITY_ID','STT_ID'];
	
	master_column_valid = '';
	foreach i in array master_column_names_admin_r
	loop
		col_name = i;
		if iscolumnexist(master_schema,master_addr_admin_r,i)=FALSE then
			if master_column_valid like '' THEN
				master_column_valid = i;
			else
				master_column_valid = master_column_valid||','||i;
			end if;
		end if;
	end loop;
	
	if master_column_valid<>'' then
		RETURN FORMAT('Column <%1$s> Does Not Exist Into Raw Table <%2$s.%3$s>',master_column_valid,master_schema,master_addr_admin_r);
	end if;
	
	/*master_column_valid = '';
	foreach i in array master_column_names_admin_p
	loop
		col_name = i;
		if iscolumnexist(master_schema,master_addr_admin_p,i)=FALSE then
			if master_column_valid like '' THEN
				master_column_valid = i;
			else
				master_column_valid = master_column_valid||','||i;
			end if;
		end if;
	end loop;
	
	if master_column_valid<>'' then
		RETURN FORMAT('Column <%1$s> Does Not Exist Into Raw Table <%2$s.%3$s>',master_column_valid,master_schema,master_addr_admin_p);
	end if;*/

	-- Handle table & schema case sensetive
	IF lower(gstn_raw_tab_schema)<>gstn_raw_tab_schema THEN
	   gstn_raw_tab_schema = '"'||gstn_raw_tab_schema||'"';
	END IF;
	
	IF lower(gstn_raw_tab_name)<>gstn_raw_tab_name THEN
		gstn_raw_tab_name = '"'||gstn_raw_tab_name||'"';
	END IF;
	
	IF lower(master_schema)<>master_schema THEN
		master_schema = '"'||master_schema||'"';
	END IF;
	
	IF lower(master_addr_admin_r)<>master_addr_admin_r THEN
		master_addr_admin_r = '"'||master_addr_admin_r||'"';
	END IF;
	
	/*IF lower(master_addr_admin_p)<>master_addr_admin_p THEN
		master_addr_admin_p = '"'||master_addr_admin_p||'"';
	END IF;*/
	
	IF lower(outputschema)<>outputschema THEN
		outputschema = '"'||outputschema||'"';
	END IF;
	
	--Filteration Process START
	BEGIN
		
		--INSERT RAW DATA INTO TABLE
		sqlquery = FORMAT('insert into %1$s.%2$s (raw_name) select raw_name from %3$s.%4$s group by raw_name',outputschema,outputtableName,gstn_raw_tab_schema,gstn_raw_tab_name);
		EXECUTE sqlquery;
		
		--CREATE MASTER DB CITY TABLE
		master_db_table = UPPER(city_name)||'_ADDR_ADMIN_R';
		sqlquery = FORMAT('Drop Table If Exists %1$s."%2$s"',master_schema,master_db_table);
		RAISE INFO 'SQL_STATEMENT:%',sqlquery;
		EXECUTE sqlquery;
		
		sqlquery = FORMAT('CREATE TABLE IF NOT EXISTS mmi_master."%1$s" AS SELECT "ID",concat_ws('', '',"SSLC_NME","SUBL_NME","LOC_NME") AS "NAME","LOC_ID","LOC_NME","SSLC_ID","SSLC_NME","SUBL_ID","SUBL_NME","TYPE","CITY_ID","STT_ID","PIN1","PIN2","SP_GEOMETRY" FROM %2$s.%3$s WHERE "CITY_ID"=%4$s',master_db_table,master_schema,master_addr_admin_r,city_id);
		EXECUTE sqlquery;
		
		--EXACT MATCH
		sqlquery = FORMAT('update %1$s.%2$s t1 set status=''EXACT_MATCH'',"M_LOC_NME"=t2."LOC_NME","M_LOC_ID"=t2."LOC_ID",
						  "SUBL_NME"=t2."SUBL_NME", "SUBL_ID"=t2."SUBL_ID", "SSLC_NME"=t2."SSLC_NME", "SSLC_ID"=t2."SSLC_ID",
						  "Match_String"=t2."NAME" from mmi_master."%3$s" t2 where lower(t1.raw_name)=lower(t2."NAME")',outputschema,outputtableName,master_db_table);
		
		EXECUTE sqlquery;
		
		--LOC MATCHED
		sqlquery = FORMAT('with main as (select STRING_TO_ARRAY(trim(both '' '' from regexp_replace(coalesce(lower(raw_name),''''),''\s*,\s*'','','',''g'')),'','') as srray,* from %1$s.%2$s)
                           update %1$s.%2$s t1 set status=''LOC_MATCHED'',"M_LOC_NME"=t2."LOC_NME","M_LOC_ID"=t2."LOC_ID","SUBL_NME"=t2."SUBL_NME", "SUBL_ID"=t2."SUBL_ID", "SSLC_NME"=t2."SSLC_NME", "SSLC_ID"=t2."SSLC_ID",
                           "Unmatch_String"=t2."Unmatch_String","Match_String"=t2."Match_String"
                           from 
                           (select concat_ws('','',"Unmatch_String",array_to_string(array_remove(srray,coalesce(lower(t2."LOC_NME"),'''')),'','')) as "Unmatch_String",concat_ws('','',"Match_String",lower(t2."LOC_NME")) as "Match_String",
                           t1.srno, t1.raw_name, t2."LOC_NME", t2."LOC_ID",t2."SUBL_NME", t2."SUBL_ID", 
                           t2."SSLC_NME", t2."SSLC_ID" from main t1 inner join mmi_master."%3$s" t2 on coalesce(lower(t2."LOC_NME"),'''') LIKE ANY(srray) where coalesce(t1.status,'''')<>''EXACT_MATCH'') t2
                           where t1.srno=t2.srno',outputschema,outputtableName,master_db_table);
		
		EXECUTE sqlquery;
		
		--NOT MATCHED UPDATE
		sqlquery = FORMAT('UPDATE %1$s.%2$s SET status=''NOT_MATCHED'',"Unmatch_String"=raw_name where coalesce(status,'''')=''''',outputschema,outputtableName);
		EXECUTE sqlquery;
		
		--SUBL MATCHED
		sqlquery = FORMAT('with main as (select STRING_TO_ARRAY(trim(both '' '' from regexp_replace(coalesce(lower("Unmatch_String"),''''),''\s*,\s*'','','',''g'')),'','') as srray,* from %1$s.%2$s)
                          update %1$s.%2$s t1 set status=''SUBL_MATCHED'',"SUBL_NME"=t2."SUBL_NME", "SUBL_ID"=t2."SUBL_ID",
                          "Unmatch_String"=t2."Unmatch_String","Match_String"=t2."Match_String" FROM
                          (select t1.srno,t2."SUBL_NME",t2."SUBL_ID",array_to_string(array_remove(srray,coalesce(lower(t2."SUBL_NME"),'''')),'','') as "Unmatch_String",concat_ws('','',"Match_String",lower(t2."SUBL_NME")) as "Match_String" from main t1 inner join mmi_master."%3$s" t2 on coalesce(lower(t1."M_LOC_NME"),'''')=coalesce(lower(t2."LOC_NME"),'''') and coalesce(lower(t2."SUBL_NME"),'''') LIKE ANY(srray) where coalesce(t1.status,'''')=''LOC_MATCHED'' and coalesce(t1."Unmatch_String",'''')<>'''' and coalesce(t2."SUBL_NME",'''')<>'''') t2
                           where t1.srno=t2.srno',outputschema,outputtableName,master_db_table);
		
		EXECUTE sqlquery;
		
		--SSLC MATCHED
		sqlquery = FORMAT('with main as (select STRING_TO_ARRAY(trim(both '' '' from regexp_replace(coalesce(lower("Unmatch_String"),''''),''\s*,\s*'','','',''g'')),'','') as srray,* from %1$s.%2$s)
                          update %1$s.%2$s t1 set status=''SSLC_MATCHED'',"SSLC_NME"=t2."SSLC_NME", "SSLC_ID"=t2."SSLC_ID",
                          "Unmatch_String"=t2."Unmatch_String","Match_String"=t2."Match_String" FROM
                          (select t1.srno,t2."SSLC_NME",t2."SSLC_ID",array_to_string(array_remove(srray,coalesce(lower(t2."SSLC_NME"),'''')),'','') as "Unmatch_String",concat_ws('','',"Match_String",lower(t2."SSLC_NME")) as "Match_String" from main t1 inner join mmi_master."%3$s" t2 on coalesce(lower(t1."M_LOC_NME"),'''')=coalesce(lower(t2."LOC_NME"),'''') and coalesce(lower(t2."SSLC_NME"),'''') LIKE ANY(srray) where coalesce(t1.status,'''') in (''LOC_MATCHED'',''SUBL_MATCHED'') and coalesce(t1."Unmatch_String",'''')<>'''' and coalesce(t2."SSLC_NME",'''')<>'''') t2
                           where t1.srno=t2.srno',outputschema,outputtableName,master_db_table);
		
		EXECUTE sqlquery;
		
		--CREATE NEIGHBOUR LOC DICTIONARY
		nloc_dicn = lower(city_name)||'_nloc_dictionary';
		--Drop Table
		sqlquery = FORMAT('DROP TABLE IF EXISTS mmi_master.%1$s',nloc_dicn);
		EXECUTE sqlquery;
		sqlquery = FORMAT('create table mmi_master.%1$s as select t1."LOC_NME" AS "M_LOC",t1."LOC_ID" AS "M_LOC_ID",t2."LOC_NME" AS "N_LOC",t2."LOC_ID" AS "N_LOC_ID" FROM mmi_master."%2$s" t1 inner join 
                           mmi_master."%2$s" t2 on st_intersects(t1."SP_GEOMETRY",t2."SP_GEOMETRY")=true 
						   and t1."LOC_ID"<>t2."LOC_ID" and  t1."LOC_NME"<>t2."LOC_NME"',nloc_dicn,master_db_table);
		
		EXECUTE sqlquery;
		
		--CREATE SUB NEIGHBOUR LOC DICTIONARY
		sloc_dicn = lower(city_name)||'_sloc_dictionary';
		--Drop Table
		sqlquery = FORMAT('DROP TABLE IF EXISTS mmi_master.%1$s',sloc_dicn);
		EXECUTE sqlquery;
		sqlquery = FORMAT('create table mmi_master.%1$s as 
		                   select t1."M_LOC",t1."M_LOC_ID",t1."N_LOC",t1."N_LOC_ID",t2."SUBL_NME" AS "N_SUBL_NME",t2."SUBL_ID" AS "N_SUBL_ID" FROM mmi_master.%3$s t1 inner join 
                           mmi_master."%2$s" t2 on t1."N_LOC"=t2."LOC_NME"',sloc_dicn,master_db_table,nloc_dicn);
		
		EXECUTE sqlquery;
		
		--CREATE SSLC NEIGHBOUR LOC DICTIONARY
		sslcloc_dicn = lower(city_name)||'_sslcloc_dictionary';
		--Drop Table
		sqlquery = FORMAT('DROP TABLE IF EXISTS mmi_master.%1$s',sslcloc_dicn);
		EXECUTE sqlquery;
		sqlquery = FORMAT('create table mmi_master.%1$s as 
		                   select t1."M_LOC",t1."M_LOC_ID",t1."N_LOC",t1."N_LOC_ID",t2."SSLC_NME" AS "N_SLCL_NME",t2."SSLC_ID" AS "N_SSLC_ID" FROM mmi_master.%3$s t1 inner join 
                           mmi_master."%2$s" t2 on t1."N_LOC"=t2."LOC_NME"',sslcloc_dicn,master_db_table,nloc_dicn);
		
		EXECUTE sqlquery;
		
		--UPDATE NLOC INTO RAW TABLE
		sqlquery = FORMAT('with nloc as (Select max("M_LOC_ID") as "M_LOC_ID","M_LOC",array_to_string(array_agg(distinct("N_LOC")),'','') as "N_LOC" from mmi_master.%3$s group by "M_LOC")
                           update %1$s.%2$s t1 set "N_LOC"=t2."N_LOC" from nloc t2 where t1."M_LOC_ID"=t2."M_LOC_ID" and coalesce(status,'''') not in (''EXACT_MATCH'',''NOT_MATCHED'')',outputschema,outputtableName,nloc_dicn);
		EXECUTE sqlquery;
		
		
		--UPDATE N_LOC_MATCHED
		sqlquery = FORMAT('with main as (select STRING_TO_ARRAY(trim(both '' '' from regexp_replace(coalesce(lower("Unmatch_String"),''''),''\s*,\s*'','','',''g'')),'','') as srray,* from %1$s.%2$s where coalesce(status,'''') in (''LOC_MATCHED'',''SUBL_MATCHED'',''SSLC_MATCHED'') and coalesce("Unmatch_String",'''')<>'''')
                           update %1$s.%2$s t1 set status=''N_LOC_MATCHED'',"N_LOC_MATCHED"=t2."N_LOC", "N_LOC_MATCHED_ID"=t2."N_LOC_ID",
                          "Unmatch_String"=t2."Unmatch_String","Match_String"=t2."Match_String" FROM
                          (select t1.srno,t2."N_LOC",t2."N_LOC_ID",array_to_string(array_remove(srray,coalesce(lower(t2."N_LOC"),'''')),'','') as "Unmatch_String",concat_ws('','',"Match_String",lower(t2."N_LOC")) as "Match_String" from main t1 inner join mmi_master.%3$s t2 on coalesce(lower(t1."M_LOC_NME"),'''')=coalesce(lower(t2."M_LOC"),'''') and coalesce(lower(t2."N_LOC"),'''') LIKE ANY(srray)) t2
                          where t1.srno=t2.srno',outputschema,outputtableName,nloc_dicn);
		EXECUTE sqlquery;
		
		--UPDATE N_LOC_MATCHED
		sqlquery = FORMAT('with main as (select STRING_TO_ARRAY(trim(both '' '' from regexp_replace(coalesce(lower("Unmatch_String"),''''),''\s*,\s*'','','',''g'')),'','') as srray,* from %1$s.%2$s where coalesce(status,'''') in (''LOC_MATCHED'',''SUBL_MATCHED'',''SSLC_MATCHED'',''N_LOC_MATCHED'') and coalesce("Unmatch_String",'''')<>'''')
                           update %1$s.%2$s t1 set status=''N_SUB_LOC_MATCHED'',"N_SUBL_MATCHED"=t2."N_SUBL_NME", "N_SUBL_MATCHED_ID"=t2."N_SUBL_ID","M_N_LOC"=t2."N_LOC","M_N_LOC_MATCHED_ID"=t2."N_LOC_ID",
                           "Unmatch_String"=t2."Unmatch_String","Match_String"=t2."Match_String" FROM
                           (select t1.srno,t2."N_LOC",t2."N_LOC_ID",t2."N_SUBL_NME",t2."N_SUBL_ID",array_to_string(array_remove(srray,coalesce(lower(t2."N_SUBL_NME"),'''')),'','') as "Unmatch_String",concat_ws('','',"Match_String",lower(t2."N_SUBL_NME")) as "Match_String" from main t1 inner join mmi_master.%3$s t2 on coalesce(lower(t1."M_LOC_NME"),'''')=coalesce(lower(t2."M_LOC"),'''') and coalesce(lower(t2."N_SUBL_NME"),'''') LIKE ANY(srray)) t2
                           where t1.srno=t2.srno',outputschema,outputtableName,sloc_dicn);
		EXECUTE sqlquery;
		
		--UPDATE N_SSLC_MATCHED
		sqlquery = FORMAT('with main as (select STRING_TO_ARRAY(trim(both '' '' from regexp_replace(coalesce(lower("Unmatch_String"),''''),''\s*,\s*'','','',''g'')),'','') as srray,* from %1$s.%2$s where coalesce(status,'''') in (''LOC_MATCHED'',''SUBL_MATCHED'',''SSLC_MATCHED'',''N_LOC_MATCHED'',''N_SUB_LOC_MATCHED'') and coalesce("Unmatch_String",'''')<>'''')
                           update %1$s.%2$s t1 set status=''N_SSLC_MATCHED'',"N_SSLC_MATCHED"=t2."N_SLCL_NME", "N_SSLC-MATCHED_ID"=t2."N_SSLC_ID","M_N_LOC"=t2."N_LOC","M_N_LOC_MATCHED_ID"=t2."N_LOC_ID",
                           "Unmatch_String"=t2."Unmatch_String","Match_String"=t2."Match_String" FROM
                           (select t1.srno,t2."N_LOC",t2."N_LOC_ID",t2."N_SLCL_NME",t2."N_SSLC_ID",array_to_string(array_remove(srray,coalesce(lower(t2."N_SLCL_NME"),'''')),'','') as "Unmatch_String",concat_ws('','',"Match_String",lower(t2."N_SLCL_NME")) as "Match_String" from main t1 inner join mmi_master.%3$s t2 on coalesce(lower(t1."M_LOC_NME"),'''')=coalesce(lower(t2."M_LOC"),'''') and coalesce(lower(t2."N_SLCL_NME"),'''') LIKE ANY(srray)) t2
                           where t1.srno=t2.srno',outputschema,outputtableName,sslcloc_dicn);
		EXECUTE sqlquery;
		
		--UPDATE COUNT
		sqlquery = FORMAT('insert into %1$s.%2$s (table_name,city_name,status,s_count) select ''%4$s'',''%5$s'',status,count(*) from %1$s.%3$s group by status
                           union select ''%4$s'',''%5$s'',''UNMATCHED_COUNT'' as status,count(*) from %1$s.%3$s where coalesce("Unmatch_String",'''')=''''
                           union
                           select ''%4$s'',''%5$s'',''TOTAL_COUNT'' as status,count(*) from %1$s.%3$s',outputschema,count_table,outputtableName,gstn_raw_tab_name,city_name);
		EXECUTE sqlquery;
		
		EXCEPTION
		WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS 
		f1=MESSAGE_TEXT,
		f2=PG_EXCEPTION_CONTEXT; 
		
		sqlquery = FORMAT('insert into %1$s.%2$s (table_name,message,context) Values(''%3$s'',''%4$s'',''%5$s'')',outputschema,error_table,gstn_raw_tab_name,f1,f2);
		EXECUTE sqlquery;
		RAISE info 'error caught 2.1:%',f1;
		RAISE info 'error caught 2.2:%',f2;
		RETURN -1;
	END;
	
	RETURN 0;
	
END;	
$BODY$;

ALTER FUNCTION mmi_master.gstndata_process(text, text, text, text, text, text, text, integer)
    OWNER TO postgres;
