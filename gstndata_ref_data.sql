-- FUNCTION: mmi_master.gstndata_ref_data(text, text, text, text, text, text)

-- DROP FUNCTION mmi_master.gstndata_ref_data(text, text, text, text, text, text);

CREATE OR REPLACE FUNCTION mmi_master.gstndata_ref_data(
	production_schema text,
	raw_table text,
	city_name text,
	master_schema text,
	admin_table text,
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
BEGIN 
error_tab_name = 'gstn_output_data.gstn_error';

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "SUBL_NME"=R."SUBL_NME","SUBL_ID"=R."SUBL_ID","Match_String"=R."SUBL_NME" 
,status=''REF_SUBL_MATCHED'',"Unmatch_String"='''',pincode=R."PIN_CD" FROM (
select t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."SUBL_NME",t2."SUBL_ID",t2."PIN1"  from
(select  t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and srno not in(
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%block%'' 
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%sector%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%phase%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%pocket%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%type%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%stage%''
) ) 
as t1 inner join '||production_schema||'."unique_'|| raw_table ||'" as t2 on lower(t1."raw_name")=lower(t2."raw_name")
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD") as t1
inner join '||master_schema||'."'|| admin_table ||'"  as t2 on lower(t1."Unmatch_String")=lower(t2."SUBL_NME") and t1."PIN_CD"=t2."PIN1"
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."SUBL_NME",t2."SUBL_ID" ,t2."PIN1" 
) AS R 
WHERE A.srno=R.srno ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "SSLC_NME"=R."SSLC_NME","SSLC_ID"=R."SSLC_ID","Match_String"=R."SSLC_NME" 
,status=''REF_SSLC_MATCHED'',"Unmatch_String"='''',pincode=R."PIN_CD" FROM (
select t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."SSLC_NME",t2."SSLC_ID",t2."PIN1"  from
(select  t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and srno not in(
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%block%'' 
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%sector%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%phase%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%pocket%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%type%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%stage%''
) ) 
as t1 inner join '||production_schema||'."unique_'|| raw_table ||'" as t2 on lower(t1."raw_name")=lower(t2."raw_name")
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD") as t1
inner join '||master_schema||'."'|| admin_table ||'" as t2 on lower(t1."Unmatch_String")=lower(t2."SSLC_NME") and t1."PIN_CD"=t2."PIN1"
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."SSLC_NME",t2."SSLC_ID" ,t2."PIN1" 
) AS R 
WHERE A.srno=R.srno ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "M_LOC_NME"=R."LOC_NME","M_LOC_ID"=R."LOC_ID","Match_String"=R."LOC_NME" 
,status=''REF_LOC_MATCHED'',"Unmatch_String"='''',pincode=R."PIN_CD" FROM (
select t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."LOC_NME",t2."LOC_ID",t2."PIN1"  from
(select  t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and srno not in(
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%block%'' 
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%sector%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%phase%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%pocket%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%type%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%stage%''
) ) 
as t1 inner join '||production_schema||'."unique_'|| raw_table ||'" as t2 on lower(t1."raw_name")=lower(t2."raw_name")
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD") as t1
inner join '||master_schema||'."'|| admin_table ||'"  as t2 on lower(t1."Unmatch_String")=lower(t2."LOC_NME") and t1."PIN_CD"=t2."PIN1"
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."LOC_NME",t2."LOC_ID" ,t2."PIN1" 
) AS R 
WHERE A.srno=R.srno ';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "SSLC_NME"=t1."SSLC_NME","SSLC_ID"=t1."SSLC_ID","Match_String"=(t1."SSLC_NME")   
,status=''REF_FUZZY_SSLC_MATCHED'',"Unmatch_String"='''',pincode=t1."PIN_CD" FROM (
SELECT T1.SRNO,RAW_NAME,t1."PIN_CD","SSLC_NME",t1."PIN1",t1."Unmatch_String","SSLC_ID" FROM(
select t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."SSLC_NME",t2."SSLC_ID",t2."PIN1"  from
(select  t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and srno not in(
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%block%'' 
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%sector%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%phase%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%pocket%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%type%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%stage%''
)) 
as t1 inner join '||production_schema||'."unique_'|| raw_table ||'" as t2 on lower(t1."raw_name")=lower(t2."raw_name")
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD") as t1
inner join '||master_schema||'."'|| admin_table ||'"  as t2 on soundex(split_part(t1."Unmatch_String",'''',1))||soundex(split_part(t1."Unmatch_String",'''',2))
||soundex(split_part(t1."Unmatch_String",'''',3))||soundex(split_part(t1."Unmatch_String",'''',4))=
	soundex(split_part(t2."SSLC_NME",'' '',1))||soundex(split_part(t2."SSLC_NME",'' '',2)) 
||soundex(split_part(t2."SSLC_NME",'' '',3))||soundex(split_part(t2."SSLC_NME",'' '',4)) and t1."PIN_CD"=t2."PIN1" and soundex("SSLC_NME")<>soundex(''sector'')
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."SSLC_NME",t2."SSLC_ID" ,t2."PIN1" )
as t1
GROUP BY T1.SRNO,RAW_NAME,t1."PIN_CD","SSLC_NME",t1."PIN1",t1."Unmatch_String","SSLC_ID"
) AS T1 WHERE A.SRNO=T1.SRNO';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;
	

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "SUBL_NME"=t1."SUBL_NME","SUBL_ID"=t1."SUBL_ID","Match_String"=(t1."SUBL_NME")   
,status=''REF_FUZZY_SUBL_MATCHED'',"Unmatch_String"='''',pincode=t1."PIN_CD" FROM (
SELECT T1.SRNO,RAW_NAME,t1."PIN_CD","SUBL_NME",t1."PIN1",t1."Unmatch_String","SUBL_ID" FROM(
select t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."SUBL_NME",t2."SUBL_ID",t2."PIN1"  from
(select  t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and srno not in(
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%block%'' 
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%sector%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%phase%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%pocket%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%type%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%stage%''
) ) 
as t1 inner join '||production_schema||'."unique_'|| raw_table ||'" as t2 on lower(t1."raw_name")=lower(t2."raw_name")
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD") as t1
inner join '||master_schema||'."'|| admin_table ||'"  as t2 on soundex(split_part(t1."Unmatch_String",'''',1))||soundex(split_part(t1."Unmatch_String",'''',2))
||soundex(split_part(t1."Unmatch_String",'''',3))||soundex(split_part(t1."Unmatch_String",'''',4))=
	soundex(split_part(t2."SUBL_NME",'' '',1))||soundex(split_part(t2."SUBL_NME",'' '',2)) 
||soundex(split_part(t2."SUBL_NME",'' '',3))||soundex(split_part(t2."SUBL_NME",'' '',4)) and t1."PIN_CD"=t2."PIN1" and soundex("SUBL_NME")<>soundex(''sector'')
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."SUBL_NME",t2."SUBL_ID" ,t2."PIN1" )
as t1
GROUP BY T1.SRNO,RAW_NAME,t1."PIN_CD","SUBL_NME",t1."PIN1",t1."Unmatch_String","SUBL_ID"
) AS T1 WHERE A.SRNO=T1.SRNO';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "M_LOC_NME"=t1."LOC_NME","M_LOC_ID"=t1."LOC_ID","Match_String"=(t1."LOC_NME")   
,status=''REF_FUZZY_LOC_MATCHED'',"Unmatch_String"='''',pincode=t1."PIN_CD" FROM (
SELECT T1.SRNO,RAW_NAME,t1."PIN_CD","LOC_NME",t1."PIN1",t1."Unmatch_String","LOC_ID" FROM(
select t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."LOC_NME",t2."LOC_ID",t2."PIN1"  from
(select  t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD" from
(select * from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and srno not in(
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%block%'' 
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%sector%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%phase%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%pocket%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%type%''
union all
select srno from '||production_schema||'."'|| output_data_table ||'" where status=''NOT_MATCHED'' and raw_name ilike''%stage%''
) ) 
as t1 inner join '||production_schema||'."unique_'|| raw_table ||'" as t2 on lower(t1."raw_name")=lower(t2."raw_name")
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t2."PIN_CD") as t1
inner join '||master_schema||'."'|| admin_table ||'"  as t2 on soundex(split_part(t1."Unmatch_String",'''',1))||soundex(split_part(t1."Unmatch_String",'''',2))
||soundex(split_part(t1."Unmatch_String",'''',3))||soundex(split_part(t1."Unmatch_String",'''',4))=
	soundex(split_part(t2."LOC_NME",'' '',1))||soundex(split_part(t2."LOC_NME",'' '',2)) 
||soundex(split_part(t2."LOC_NME",'' '',3))||soundex(split_part(t2."LOC_NME",'' '',4)) and t1."PIN_CD"=t2."PIN1"
group by t1.srno,t1.raw_name,t1."Unmatch_String",t1."Match_String",t1.status,t1."PIN_CD",t2."LOC_NME",t2."LOC_ID" ,t2."PIN1" )
as t1
GROUP BY T1.SRNO,RAW_NAME,t1."PIN_CD","LOC_NME",t1."PIN1",t1."Unmatch_String","LOC_ID"
) AS T1 WHERE A.SRNO=T1.SRNO';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Unmatch_String"='''' from (
select * from '||production_schema||'."'|| output_data_table ||'" where "Unmatch_String" ~''^[0-9]+$'')
AS T1 WHERE A.SRNO=T1.SRNO';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Unmatch_String"='''' from (
select * from '||production_schema||'."'|| output_data_table ||'" where length ("Unmatch_String") <=3)
AS T1 WHERE A.SRNO=T1.SRNO';
RAISE INFO 'SQL_STATEMENT:%',SQLQuery;
EXECUTE SQLQuery;

SQLQuery='UPDATE '||production_schema||'."'|| output_data_table ||'" A set "Unmatch_String"='''' from (
select * from '||production_schema||'."'|| output_data_table ||'" where "Unmatch_String" ilike ''guwahati'')
AS T1 WHERE A.SRNO=T1.SRNO';
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

ALTER FUNCTION mmi_master.gstndata_ref_data(text, text, text, text, text, text)
    OWNER TO postgres;
