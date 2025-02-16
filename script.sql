CREATE OR REPLACE FUNCTION ddlx_identify(
  IN oid,  
  OUT oid oid, OUT classid regclass, 
  OUT name name,  OUT namespace name,  
  OUT owner name, OUT sql_kind text, 
  OUT sql_identifier text, OUT acl aclitem[])
 RETURNS record LANGUAGE sql AS $$
  WITH 
  rel_kind(k,v) AS (
         VALUES ('r','TABLE'),
                ('p','TABLE'),
                ('v','VIEW'),
                ('i','INDEX'),
                ('I','INDEX'),
                ('S','SEQUENCE'),
                ('s','SPECIAL'),
                ('m','MATERIALIZED VIEW'),
                ('c','TYPE'),
                ('t','TOAST'),
                ('f','FOREIGN TABLE')
  ),
  typ_type(k,v,v2) AS (
         VALUES ('b','BASE','TYPE'),
                ('c','COMPOSITE','TYPE'),
                ('d','DOMAIN','DOMAIN'),
                ('e','ENUM','TYPE'),
                ('p','PSEUDO','TYPE'),
                ('r','RANGE','TYPE')
  )
  SELECT coalesce(t.oid,c.oid),
         case when t.oid is not null then 'pg_type'::regclass
              else 'pg_class'::regclass end,
         c.relname AS name, n.nspname AS namespace,
         pg_get_userbyid(c.relowner) AS owner,
         coalesce(cc.v,c.relkind::text) AS sql_kind,
         cast($1::regclass AS text) AS sql_identifier,
         relacl as acl
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    LEFT JOIN pg_type t ON t.typrelid=c.oid AND t.typtype='c' AND c.relkind='c'
    LEFT JOIN rel_kind AS cc on cc.k = c.relkind
   WHERE c.oid = $1
   UNION ALL
  SELECT p.oid,'pg_proc'::regclass,
         p.proname AS name, n.nspname AS namespace, pg_get_userbyid(p.proowner) AS owner,
#if 11
         case p.prokind
           when 'f' then 'FUNCTION'
           when 'a' then 'AGGREGATE'
           when 'p' then 'PROCEDURE'
           when 'w' then 'WINDOW FUNCTION'
         end 
#else
         case
           when p.proisagg then 'AGGREGATE'
           else 'FUNCTION' 
         end 
#end
         AS sql_kind,
         cast($1::regprocedure AS text) AS sql_identifier,
         proacl as acl
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
   WHERE p.oid = $1
   UNION ALL
  SELECT coalesce(c.oid,t.oid),
         case when c.oid is not null then 'pg_class'::regclass
   else 'pg_type'::regclass end,
         t.typname AS name, n.nspname AS namespace, pg_get_userbyid(t.typowner) AS owner,
         coalesce(cc.v,tt.v2,t.typtype::text) AS sql_kind,
         format_type($1,null) AS sql_identifier,
#if 9.2
         typacl as acl
#else
         null as acl
#end
    FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
    LEFT JOIN typ_type AS tt ON tt.k = t.typtype 
    LEFT JOIN pg_class AS c ON c.oid = t.typrelid AND t.typtype='c' AND c.relkind<>'c'
    LEFT JOIN rel_kind AS cc ON cc.k = c.relkind
   WHERE t.oid = $1
   UNION ALL
  SELECT r.oid,'pg_roles'::regclass,
         r.rolname as name, null as namespace, null as owner,
         'ROLE' as sql_kind,
         quote_ident(r.rolname) as sql_identifier,
         null as acl
    FROM pg_roles r
   WHERE r.oid = $1
   UNION ALL
  SELECT r.oid,'pg_rewrite'::regclass,
         r.rulename as name, null as namespace, null as owner,
         'RULE' as sql_kind,
         quote_ident(r.rulename)||' ON '|| 
           cast(c.oid::regclass as text) sql_identifier,
         null as acl
    FROM pg_rewrite r JOIN pg_class c on (c.oid = r.ev_class)
   WHERE r.oid = $1
   UNION ALL
  SELECT n.oid,'pg_namespace'::regclass,
         n.nspname as name, current_database() as namespace, pg_get_userbyid(n.nspowner) AS owner,
         'SCHEMA' as sql_kind,
         quote_ident(n.nspname) as sql_identifier,
         nspacl as acl
    FROM pg_namespace n join pg_roles r on r.oid = n.nspowner
   WHERE n.oid = $1
   UNION ALL
  SELECT con.oid,'pg_constraint'::regclass,
         con.conname as name,
         c.relname as namespace, null as owner, 'CONSTRAINT' as sql_kind,
         quote_ident(con.conname)
         ||coalesce(' ON '||cast(c.oid::regclass as text),'') as sql_identifier,
         null as acl
    FROM pg_constraint con 
    left JOIN pg_class c ON (con.conrelid=c.oid)
    LEFT join (
         values ('f','FOREIGN KEY'), ('c','CHECK'), ('x','EXCLUDE'),
                ('u','UNIQUE'), ('p','PRIMARY KEY'), ('t','TRIGGER')) 
             as tt on tt.column1 = con.contype
   WHERE con.oid = $1
#if 14
     AND (c.oid is null or -- hack to hide duplicated oids
     NOT (c.relname like 'pg_%' or c.relnamespace = 'pg_catalog'::regnamespace)) 
#end
   UNION ALL
  SELECT t.oid,'pg_trigger'::regclass,
         t.tgname as name, c.relname as namespace, null as owner,
         'TRIGGER' as sql_kind,
         format('%I ON %s',t.tgname,cast(c.oid::regclass as text)) as sql_identifier,
         null as acl
    FROM pg_trigger t join pg_class c on (t.tgrelid=c.oid)
   WHERE t.oid = $1
   UNION ALL
  SELECT ad.oid,'pg_attrdef'::regclass,
         a.attname as name, c.relname as namespace, null as owner,
         'DEFAULT' as sql_kind,
         format('%s.%I',cast(c.oid::regclass as text),a.attname) as sql_identifier,
         null as acl
    FROM pg_attrdef ad 
    JOIN pg_class c ON (ad.adrelid=c.oid)
    JOIN pg_attribute a ON (c.oid = a.attrelid and a.attnum=ad.adnum)
   WHERE ad.oid = $1
   UNION ALL
  SELECT op.oid,'pg_operator'::regclass,
         op.oprname as name, n.nspname as namespace, pg_get_userbyid(op.oprowner) as owner,
         'OPERATOR' as sql_kind,
         cast(op.oid::regoperator as text) as sql_identifier,
         null as acl
    FROM pg_operator op JOIN pg_namespace n ON n.oid=op.oprnamespace
   WHERE op.oid = $1
   UNION ALL
  SELECT cfg.oid,'pg_ts_config'::regclass,
         cfg.cfgname as name, n.nspname as namespace, pg_get_userbyid(cfg.cfgowner) as owner,
         'TEXT SEARCH CONFIGURATION' as sql_kind,
         cast(cfg.oid::regconfig as text) as sql_identifier,
         null as acl
    FROM pg_ts_config cfg JOIN pg_namespace n ON n.oid=cfg.cfgnamespace
   WHERE cfg.oid = $1
   UNION ALL
  SELECT dict.oid,'pg_ts_dict'::regclass,
         dict.dictname as name, n.nspname as namespace, pg_get_userbyid(dict.dictowner) as owner,
         'TEXT SEARCH DICTIONARY' as sql_kind,
         cast(dict.oid::regdictionary as text) as sql_identifier,
         null as acl
    FROM pg_ts_dict dict JOIN pg_namespace n ON n.oid=dict.dictnamespace
   WHERE dict.oid = $1
   UNION ALL
  SELECT prs.oid,'pg_ts_parser'::regclass,
         prs.prsname as name, n.nspname as namespace, null as owner,
         'TEXT SEARCH PARSER' as sql_kind,
         format('%s%I',
           quote_ident(nullif(n.nspname,current_schema()))||'.',prs.prsname) 
           as sql_identifier,
         null as acl
    FROM pg_ts_parser prs JOIN pg_namespace n ON n.oid=prs.prsnamespace
   WHERE prs.oid = $1
   UNION ALL
  SELECT tmpl.oid,'pg_ts_template'::regclass,
         tmpl.tmplname as name, n.nspname as namespace, null as owner,
         'TEXT SEARCH TEMPLATE' as sql_kind,
         format('%s%I',
           quote_ident(nullif(n.nspname,current_schema()))||'.',tmpl.tmplname) 
           as sql_identifier,
         null as acl
    FROM pg_ts_template tmpl JOIN pg_namespace n ON n.oid=tmpl.tmplnamespace
   WHERE tmpl.oid = $1
   UNION ALL
  SELECT fdw.oid,'pg_foreign_data_wrapper'::regclass,
         fdw.fdwname as name, null as namespace, pg_get_userbyid(fdw.fdwowner) as owner,
         'FOREIGN DATA WRAPPER' as sql_kind,
         quote_ident(fdw.fdwname) as sql_identifier,
         fdwacl as acl
    FROM pg_foreign_data_wrapper fdw
   WHERE fdw.oid = $1
   UNION ALL
  SELECT srv.oid,'pg_foreign_server'::regclass,
         srv.srvname as name, null as namespace, pg_get_userbyid(srv.srvowner) as owner,
         'SERVER' as sql_kind,
         quote_ident(srv.srvname) as sql_identifier,
         srvacl as acl
    FROM pg_foreign_server srv
   WHERE srv.oid = $1
   UNION ALL
  SELECT ums.umid,'pg_user_mapping'::regclass,
         null as name, null as namespace, null as owner, 'USER MAPPING' as sql_kind,
         'FOR '||quote_ident(ums.usename)||
         ' SERVER '||quote_ident(ums.srvname) as sql_identifier,
         null as acl
    FROM pg_user_mappings ums
   WHERE ums.umid = $1
   UNION ALL
  SELECT ca.oid,'pg_cast'::regclass,
         null as name, null as namespace, null as owner,
         'CAST' as sql_kind,
         format('(%s AS %s)',
           format_type(ca.castsource,null),format_type(ca.casttarget,null))
           as sql_identifier,
         null as acl
    FROM pg_cast ca
   WHERE ca.oid = $1
   UNION ALL
  SELECT co.oid,'pg_collation'::regclass,
         co.collname as name, n.nspname as namespace, pg_get_userbyid(co.collowner) as owner,
         'COLLATION' as sql_kind,
         format('%s%I',
           quote_ident(nullif(n.nspname,current_schema()))||'.',co.collname) 
           as sql_identifier,
         null as acl
    FROM pg_collation co JOIN pg_namespace n ON n.oid=co.collnamespace
   WHERE co.oid = $1
   UNION ALL
  SELECT co.oid,'pg_conversion'::regclass,
         co.conname as name, n.nspname as namespace, pg_get_userbyid(co.conowner) as owner,
         'CONVERSION' as sql_kind,
         format('%s%I',
           quote_ident(nullif(n.nspname,current_schema()))||'.',co.conname) 
           as sql_identifier,
         null as acl
    FROM pg_conversion co JOIN pg_namespace n ON n.oid=co.connamespace
   WHERE co.oid = $1
   UNION ALL
  SELECT lan.oid,'pg_language'::regclass,
         lan.lanname as name, null as namespace, pg_get_userbyid(lan.lanowner) as owner,
         'LANGUAGE' as sql_kind,
         quote_ident(lan.lanname) as sql_identifier,
         lan.lanacl as acl
    FROM pg_language lan
   WHERE lan.oid = $1
   UNION ALL
  SELECT opf.oid,'pg_opfamily'::regclass,
         opf.opfname as name, n.nspname as namespace, pg_get_userbyid(opf.opfowner) as owner,
         'OPERATOR FAMILY' as sql_kind,
         format('%s%I USING %I',
           quote_ident(nullif(n.nspname,current_schema()))||'.',
           opf.opfname,
           am.amname) 
           as sql_identifier,
         null as acl
    FROM pg_opfamily opf JOIN pg_namespace n ON n.oid=opf.opfnamespace
    JOIN pg_am am on (am.oid=opf.opfmethod)
   WHERE opf.oid = $1
   UNION ALL
  SELECT dat.oid,'pg_database'::regclass,
         dat.datname as name, null as namespace, pg_get_userbyid(dat.datdba) as owner,
         'DATABASE' as sql_kind,
         quote_ident(dat.datname) as sql_identifier,
         dat.datacl as acl
    FROM pg_database dat
   WHERE dat.oid = $1
   UNION ALL
  SELECT spc.oid,'pg_tablespace'::regclass,
         spc.spcname as name, null as namespace, pg_get_userbyid(spc.spcowner) as owner,
         'TABLESPACE' as sql_kind,
         quote_ident(spc.spcname) as sql_identifier,
         spc.spcacl as acl
    FROM pg_tablespace spc
   WHERE spc.oid = $1
   UNION ALL
  SELECT opc.oid,'pg_opclass'::regclass,
         opcname as name, n.nspname as namespace, pg_get_userbyid(opc.opcowner) as owner,
         'OPERATOR CLASS' as sql_kind,
         format('%s%I USING %I',
           quote_ident(nullif(n.nspname,current_schema()))||'.',
           opc.opcname,
           am.amname) 
           as sql_identifier,
         null as acl
    FROM pg_opclass opc JOIN pg_namespace n ON n.oid=opc.opcnamespace
    JOIN pg_am am ON am.oid=opc.opcmethod
   WHERE opc.oid = $1
   UNION ALL
  SELECT e.oid, 'pg_extension'::regclass,
         e.extname AS name, e.extnamespace::text AS namespace, pg_get_userbyid(e.extowner) AS owner,
         'EXTENSION'::text AS sql_kind,
         e.extname AS sql_identifier,
         NULL::aclitem[] AS acl
    FROM pg_extension e
   WHERE e.oid = $1   
#if 9.3
   UNION ALL
  SELECT evt.oid,'pg_event_trigger'::regclass,
         evt.evtname as name, null as namespace, pg_get_userbyid(evt.evtowner) as owner,
         'EVENT TRIGGER' as sql_kind,
         quote_ident(evt.evtname) as sql_identifier,
         null as acl
    FROM pg_event_trigger evt
   WHERE evt.oid = $1
   UNION ALL
  SELECT amproc.oid,'pg_amproc'::regclass,
         'FUNCTION '||amprocnum, null as namespace, null as owner,
         'AMPROC' as sql_kind,
         format('FUNCTION %s (%s)',
          amprocnum,
          array_to_string(array[amproclefttype,amprocrighttype]::regtype[],','))
         as sql_identifier,
         null as acl
    FROM pg_amproc amproc
   WHERE amproc.oid = $1
   UNION ALL
  SELECT amop.oid,'pg_amop'::regclass,
         'OPERATOR '||amopstrategy, null as namespace, null as owner,
         'AMOP' as sql_kind,
         format('OPERATOR %s (%s)',
          amopstrategy,
          array_to_string(array[amoplefttype,amoprighttype]::regtype[],','))
         as sql_identifier,
         null as acl
    FROM pg_amop amop
   WHERE amop.oid = $1
#if 9.5
   UNION ALL
  SELECT pol.oid,'pg_policy'::regclass,
         pol.polname as name, null as namespace, null as owner,
         'POLICY' as sql_kind,
         format('%I ON %s',
                  polname,
                  cast(c.oid::regclass as text)) 
         as sql_identifier,
         null as acl
    FROM pg_policy pol JOIN pg_class c on (c.oid=pol.polrelid)
   WHERE pol.oid = $1
   UNION ALL
  SELECT trf.oid,'pg_transform'::regclass,
         null as name, null as namespace, null as owner,
         'TRANSFORM' as sql_kind,
         format('FOR %s LANGUAGE %I',
                  format_type(trf.trftype,null),
                  l.lanname) as sql_identifier,
         null as acl
    FROM pg_transform trf JOIN pg_language l on (l.oid=trf.trflang)
   WHERE trf.oid = $1
   UNION ALL
  SELECT am.oid,'pg_am'::regclass,
         am.amname as name, NULL as namespace, NULL as owner,
         'ACCESS METHOD' as sql_kind,
         quote_ident(amname) as sql_identifier,
         null as acl
    FROM pg_am am
   WHERE am.oid = $1
#if 10
   UNION ALL
  SELECT stx.oid,'pg_statistic_ext'::regclass,
         stx.stxname, n.nspname as namespace, pg_get_userbyid(stx.stxowner) as owner,
         'STATISTICS' as sql_kind,
         format('%s%I',quote_ident(nullif(n.nspname,current_schema()))||'.',stx.stxname) 
         as sql_identifier,
         null as acl
    FROM pg_statistic_ext stx join pg_namespace n on (n.oid=stxnamespace)
   WHERE stx.oid = $1
   UNION ALL
  SELECT pub.oid,'pg_publication'::regclass,
         pub.pubname, NULL as namespace, pg_get_userbyid(pub.pubowner) as owner,
         'PUBLICATION' as sql_kind,
         quote_ident(pub.pubname) as sql_identifier,
         null as acl
    FROM pg_publication pub
   WHERE pub.oid = $1
#if 14
   UNION ALL
  SELECT sub.oid,'pg_subscription'::regclass,
         sub.subname, NULL as namespace, pg_get_userbyid(sub.subowner) as owner,
         'SUBSCRIPTION' as sql_kind,
         quote_ident(sub.subname) as sql_identifier,
         null as acl
    FROM pg_subscription sub
   WHERE sub.oid = $1
#end
$$  strict;
COMMENT ON FUNCTION ddlx_identify(oid) 
     IS 'Identify any object by object id';

--------------------------------------------------------------- ---------------

CREATE OR REPLACE FUNCTION ddlx_describe(
  IN regclass, IN text[] default '{}',
  OUT ord smallint,
  OUT name name, OUT type text, OUT size integer, OUT not_null boolean,
  OUT "default" text,
  OUT ident text, OUT gen text,
  OUT comment text, OUT primary_key name,
  OUT is_local boolean, OUT storage text, OUT collation text, 
  OUT namespace name, OUT class_name name, OUT sql_identifier text,
  OUT relid oid, OUT options text[], OUT definition text,
  OUT sequence regclass,
  OUT compression text)
 RETURNS SETOF record LANGUAGE sql AS $$
SELECT  DISTINCT 
        a.attnum AS ord,
        a.attname AS name, 
        format_type(t.oid, NULL::integer) AS type,
        CASE
            WHEN (a.atttypmod - 4) > 0 THEN a.atttypmod - 4
            ELSE NULL::integer
        END AS size,
        a.attnotnull AS not_null,
        pg_get_expr(def.adbin,def.adrelid) AS "default",
#if 10
  nullif(a.attidentity::text,''),
#else
  null::text,
#end
#if 12
  nullif(a.attgenerated::text,''),
#else
  null::text,
#end
        col_description(c.oid, a.attnum::integer) AS comment,
        con.conname AS primary_key,
        a.attislocal AS is_local,
        case when a.attstorage<>t.typstorage
        then case a.attstorage
             when 'p' then 'plain'::text
             when 'e' then 'external'::text
             when 'm' then 'main'::text
             when 'x' then 'extended'::text
             else a.attstorage::text
             end
        end as storage,
        nullif(col.collcollate::text,'') AS collation,
        s.nspname AS namespace,
        c.relname AS class_name,
        format('%s.%I',text(c.oid::regclass),a.attname) AS sql_identifier,
        c.oid as relid,
#if 9.2
        attoptions||attfdwoptions as options,
#else
        attoptions as options,
#end
  format('%I %s%s%s%s%s%s%s',a.attname::text,format_type(t.oid, a.atttypmod),
#if 9.2
         case
           when a.attfdwoptions is not null
           then (
             select ' OPTIONS ( '||string_agg(
                quote_ident(option_name)||' '||quote_nullable(option_value), 
                ', ')||' ) '
               from pg_options_to_table(a.attfdwoptions))
         end,
#else
         null::text,
#end
         CASE
           WHEN length(col.collcollate) > 0
           THEN ' COLLATE ' || quote_ident(col.collcollate::text)
         END
   ,
#if 10	
   case when a.attnotnull and attidentity not in ('a','d') then ' NOT NULL' end
#else
   case when a.attnotnull THEN ' NOT NULL' end
#end
   ,
   case when 'lite' ilike any($2) then ' DEFAULT ' || pg_get_expr(def.adbin,def.adrelid) end,
#if 10
  case when attidentity in ('a','d')
       then format(' GENERATED %s AS IDENTITY',
         case attidentity
         when 'a' then 'ALWAYS'
         when 'd' then 'BY DEFAULT'
         end)
       end
#else
   null::text
#end
  ,
#if 12
  case when a.attgenerated = 's'
       then format(' GENERATED ALWAYS AS (%s) STORED', 
                  pg_get_expr(def.adbin,def.adrelid))
  end
#else
         null::text
#end
   )
        AS definition,
        pg_get_serial_sequence(c.oid::regclass::text,a.attname)::regclass as sequence,
#if 14
        nullif(case a.attcompression 
               when 'l' then 'LZ4'
               when 'p' then 'PGLZ'
               else a.attcompression::text
               end,'')
#else
        null
#end
        AS compression
   FROM pg_class c
   JOIN pg_namespace s ON s.oid = c.relnamespace
   JOIN pg_attribute a ON c.oid = a.attrelid
   LEFT JOIN pg_attrdef def ON c.oid = def.adrelid AND a.attnum = def.adnum
   LEFT JOIN pg_constraint con
     ON con.conrelid = c.oid AND (a.attnum = ANY (con.conkey)) AND con.contype = 'p'
   LEFT JOIN pg_type t ON t.oid = a.atttypid
   LEFT JOIN pg_collation col ON col.oid = a.attcollation
   JOIN pg_namespace tn ON tn.oid = t.typnamespace
   LEFT JOIN pg_depend d ON def.oid = d.objid AND d.deptype='n'
   LEFT JOIN pg_class seq ON seq.oid = d.refobjid AND seq.relkind='S'
  WHERE c.relkind IN ('r','v','c','f','p') AND a.attnum > 0 AND NOT a.attisdropped
    AND has_table_privilege(c.oid, 'select') AND has_schema_privilege(s.oid, 'usage')
    AND c.oid = $1
  ORDER BY s.nspname, c.relname, a.attnum;
$$ strict;
COMMENT ON FUNCTION ddlx_describe(regclass, text[]) IS 'Describe columns of a class';

--------------------------------------------------------------- ---------------
--------------------------------------------------------------- ---------------

CREATE OR REPLACE FUNCTION ddlx_create_table(regclass, text[] default '{}')
 RETURNS text LANGUAGE sql AS $$
  with obj as (select * from ddlx_identify($1))
  select   
    array_to_string(array[
      'CREATE '||
      case relpersistence
        when 'u' then 'UNLOGGED '
        when 't' then 'TEMPORARY '
        else ''
      end
      || obj.sql_kind || ' ' 
      || case when 'ine' ilike any($2) then 'IF NOT EXISTS ' else '' end
      || obj.sql_identifier
      || case when reloftype>0 then ' OF '||cast(reloftype::regtype as text) else '' end
      || case obj.sql_kind when 'TYPE' then ' AS' else '' end 
      ||
#if 10
  case
  when c.relispartition
  then ' PARTITION OF ' || (SELECT string_agg(i.inhparent::regclass::text,',')
                             FROM pg_inherits i WHERE i.inhrelid = $1) 
  else
#end
    case when reloftype>0
    then ''
    else
    ' (' ||coalesce(E'\n' ||
      array_to_string(array_cat(
        (SELECT array_agg('    '||definition) FROM ddlx_describe($1,$2) WHERE is_local),
        case when 'lite' ilike any($2)
        and not 'noconstraints' ilike any($2) then
          (SELECT array_agg('    '||sql) FROM
            (select ('CONSTRAINT ' || quote_ident(constraint_name) || ' ' || constraint_definition) as sql
               from ddlx_get_constraints($1) where is_local order by constraint_type desc, constraint_name) as a)
        end
      ), E',\n') || E'\n','') || ')'
    end
#if 10
  end
#end
#if 10
  ,
  case when c.relpartbound is not null
       then pg_get_expr(c.relpartbound,c.oid,true)
  end
#end
  ,
#if 10
  case
  when not c.relispartition
  then (SELECT 'INHERITS(' || string_agg(i.inhparent::regclass::text,', ') || E')'
          FROM pg_inherits i WHERE i.inhrelid = $1)
  end
#else
  (SELECT 'INHERITS(' || string_agg(i.inhparent::regclass::text,', ') || E')'
     FROM pg_inherits i WHERE i.inhrelid = $1) 
#end
#if 10
  ,
  CASE 
  WHEN p.partstrat IS NOT NULL
  THEN 'PARTITION BY ' || pg_get_partkeydef($1)
  END
#end
#unless 12
  ,
  CASE relhasoids WHEN true THEN 'WITH OIDS' END
#end
  ,
    E'SERVER '||quote_ident(fs.srvname)||E' OPTIONS (\n'||
    (select string_agg(
              '    '||quote_ident(option_name)||' '||quote_nullable(option_value), 
              E',\n')
       from pg_options_to_table(ft.ftoptions))||E'\n)'
    

  ],E'\n  ')
  ||
  E';\n'
 FROM pg_class c JOIN obj ON (true)
 LEFT JOIN pg_foreign_table  ft ON (c.oid = ft.ftrelid)
 LEFT JOIN pg_foreign_server fs ON (ft.ftserver = fs.oid)
#if 10
 LEFT JOIN pg_partitioned_table p ON p.partrelid = c.oid
#end
 WHERE c.oid = $1
-- AND relkind in ('r','c')
$$  strict;

--------------------------------------------------------------- ---------------
--------------------------------------------------------------- ---------------

CREATE OR REPLACE FUNCTION ddlx_create_constraints(regclass, text[] default '{}')
 RETURNS text LANGUAGE sql AS $function$
 with cs as (
  select
   'ALTER TABLE ' || text(regclass(regclass)) ||  
   ' ADD CONSTRAINT ' || quote_ident(constraint_name) || 
   E' ' || constraint_definition as sql
    from ddlx_get_constraints($1) gc
    join pg_constraint co on (co.oid = gc.oid)
   where is_local
     and (constraint_type not in ('CHECK') or not 'script' ilike any($2)) 
     and (co.conrelid is distinct from co.confrelid or not 'script' ilike any($2))
   order by constraint_type desc, constraint_name
 )
 select coalesce(string_agg(sql,E';\n') || E';\n\n','')
   from cs
$function$  strict;

--------------------------------------------------------------- ---------------
--------------------------------------------------------------- ---------------

CREATE OR REPLACE FUNCTION ddlx_get_constraints(
 regclass default null,
 OUT namespace name, 
 OUT class_name name, 
 OUT constraint_name name, 
 OUT constraint_type text, 
 OUT constraint_definition text, 
 OUT is_deferrable boolean, 
 OUT initially_deferred boolean, 
 OUT regclass oid, 
 OUT oid oid,
 OUT is_local boolean)
 RETURNS SETOF record LANGUAGE sql AS $$
 SELECT nc.nspname AS namespace, 
        r.relname AS class_name, 
        c.conname AS constraint_name, 
        case c.contype
            when 'c'::"char" then 'CHECK'::text
            when 'f'::"char" then 'FOREIGN KEY'::text
            when 'p'::"char" then 'PRIMARY KEY'::text
            when 'u'::"char" then 'UNIQUE'::text
            when 't'::"char" then 'TRIGGER'::text
            when 'x'::"char" then 'EXCLUDE'::text
            else c.contype::text
        end,
        pg_get_constraintdef(c.oid,true) AS constraint_definition,
        c.condeferrable AS is_deferrable, 
        c.condeferred  AS initially_deferred, 
        r.oid as regclass, c.oid AS sysid,
  d.refobjid is null AS is_local
   FROM pg_constraint c
   JOIN pg_class r ON c.conrelid = r.oid
   JOIN pg_namespace nc ON nc.oid = c.connamespace
   JOIN pg_namespace nr ON nr.oid = r.relnamespace
   LEFT JOIN pg_depend d ON d.objid = c.oid AND d.deptype='P'
  WHERE $1 IS NULL OR r.oid=$1
$$;

--------------------------------------------------------------- ---------------
--------------------------------------------------------------- ---------------

CREATE OR REPLACE FUNCTION ddlx_alter_table_defaults(regclass, text[] default '{}')
 RETURNS text LANGUAGE sql AS $function$
with
def as (
 select 
    coalesce(
      string_agg(
        format('ALTER TABLE %s ALTER %I SET DEFAULT %s;',
                text($1),name,"default"), 
        E'\n') || E'\n\n', 
    '') as ddl
   from ddlx_describe($1,$2)
  where "default" is not null
    and "sequence" is null and gen is null
),
seq as (
 select 
    coalesce(
      string_agg(
       case when 'script' ilike any($2)
            then format(e'CREATE SEQUENCE %s%s;\n%s',
#if 9.5
             'IF NOT EXISTS ',
#else
       '',
#end
                   "sequence",
       ddlx_alter_owner("sequence",$2)
           ) else '' end ||
      format(e'ALTER SEQUENCE %s OWNED BY %s;',"sequence",sql_identifier),
  E'\n') || E'\n\n', '') as ddl
   from ddlx_describe($1,$2)
  where "sequence" is not null and ident is null
)
select case when 'lite' ilike any($2) then ''
            else array_to_string(array[def.ddl,seq.ddl],'') end
  from def,seq
$function$ strict;

--------------------------------------------------------------- ---------------
--------------------------------------------------------------- ---------------

CREATE OR REPLACE FUNCTION ddlx_describe(
  IN regclass, IN text[] default '{}',
  OUT ord smallint,
  OUT name name, OUT type text, OUT size integer, OUT not_null boolean,
  OUT "default" text,
  OUT ident text, OUT gen text,
  OUT comment text, OUT primary_key name,
  OUT is_local boolean, OUT storage text, OUT collation text, 
  OUT namespace name, OUT class_name name, OUT sql_identifier text,
  OUT relid oid, OUT options text[], OUT definition text,
  OUT sequence regclass,
  OUT compression text)
 RETURNS SETOF record LANGUAGE sql AS $$
SELECT  DISTINCT 
        a.attnum AS ord,
        a.attname AS name, 
        format_type(t.oid, NULL::integer) AS type,
        CASE
            WHEN (a.atttypmod - 4) > 0 THEN a.atttypmod - 4
            ELSE NULL::integer
        END AS size,
        a.attnotnull AS not_null,
        pg_get_expr(def.adbin,def.adrelid) AS "default",
#if 10
  nullif(a.attidentity::text,''),
#else
  null::text,
#end
#if 12
  nullif(a.attgenerated::text,''),
#else
  null::text,
#end
        col_description(c.oid, a.attnum::integer) AS comment,
        con.conname AS primary_key,
        a.attislocal AS is_local,
        case when a.attstorage<>t.typstorage
        then case a.attstorage
             when 'p' then 'plain'::text
             when 'e' then 'external'::text
             when 'm' then 'main'::text
             when 'x' then 'extended'::text
             else a.attstorage::text
             end
        end as storage,
        nullif(col.collcollate::text,'') AS collation,
        s.nspname AS namespace,
        c.relname AS class_name,
        format('%s.%I',text(c.oid::regclass),a.attname) AS sql_identifier,
        c.oid as relid,
#if 9.2
        attoptions||attfdwoptions as options,
#else
        attoptions as options,
#end
  format('%I %s%s%s%s%s%s%s',a.attname::text,format_type(t.oid, a.atttypmod),
#if 9.2
         case
           when a.attfdwoptions is not null
           then (
             select ' OPTIONS ( '||string_agg(
                quote_ident(option_name)||' '||quote_nullable(option_value), 
                ', ')||' ) '
               from pg_options_to_table(a.attfdwoptions))
         end,
#else
         null::text,
#end
         CASE
           WHEN length(col.collcollate) > 0
           THEN ' COLLATE ' || quote_ident(col.collcollate::text)
         END
   ,
#if 10	
   case when a.attnotnull and attidentity not in ('a','d') then ' NOT NULL' end
#else
   case when a.attnotnull THEN ' NOT NULL' end
#end
   ,
   case when 'lite' ilike any($2) then ' DEFAULT ' || pg_get_expr(def.adbin,def.adrelid) end,
#if 10
  case when attidentity in ('a','d')
       then format(' GENERATED %s AS IDENTITY',
         case attidentity
         when 'a' then 'ALWAYS'
         when 'd' then 'BY DEFAULT'
         end)
       end
#else
   null::text
#end
  ,
#if 12
  case when a.attgenerated = 's'
       then format(' GENERATED ALWAYS AS (%s) STORED', 
                  pg_get_expr(def.adbin,def.adrelid))
  end
#else
         null::text
#end
   )
        AS definition,
        pg_get_serial_sequence(c.oid::regclass::text,a.attname)::regclass as sequence,
#if 14
        nullif(case a.attcompression 
               when 'l' then 'LZ4'
               when 'p' then 'PGLZ'
               else a.attcompression::text
               end,'')
#else
        null
#end
        AS compression
   FROM pg_class c
   JOIN pg_namespace s ON s.oid = c.relnamespace
   JOIN pg_attribute a ON c.oid = a.attrelid
   LEFT JOIN pg_attrdef def ON c.oid = def.adrelid AND a.attnum = def.adnum
   LEFT JOIN pg_constraint con
     ON con.conrelid = c.oid AND (a.attnum = ANY (con.conkey)) AND con.contype = 'p'
   LEFT JOIN pg_type t ON t.oid = a.atttypid
   LEFT JOIN pg_collation col ON col.oid = a.attcollation
   JOIN pg_namespace tn ON tn.oid = t.typnamespace
   LEFT JOIN pg_depend d ON def.oid = d.objid AND d.deptype='n'
   LEFT JOIN pg_class seq ON seq.oid = d.refobjid AND seq.relkind='S'
  WHERE c.relkind IN ('r','v','c','f','p') AND a.attnum > 0 AND NOT a.attisdropped
    AND has_table_privilege(c.oid, 'select') AND has_schema_privilege(s.oid, 'usage')
    AND c.oid = $1
  ORDER BY s.nspname, c.relname, a.attnum;
$$ strict;
COMMENT ON FUNCTION ddlx_describe(regclass, text[]) IS 'Describe columns of a class';
------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ddlx_alter_owner(oid, text[] default '{owner}')
 RETURNS text LANGUAGE sql AS $$
 select case when 'nodcl' ilike any($2) or 'noowner' ilike any($2) or 'lite' ilike any($2) then null
        else case 
          when 'owner' ilike any($2) or obj.owner is distinct from current_role
          then
          case
           when obj.sql_kind = 'INDEX' then null
           else 'ALTER '||sql_kind||' '||sql_identifier||
                 ' OWNER TO '||quote_ident(owner)||E';\n'
          end end
        end
  from ddlx_identify($1) obj 
$$  strict;
