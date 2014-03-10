/* contrib/imcs/imcs.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "create extension imcs" to load this file. \quit

create type timeseries;

create type cs_elem_type as enum ('char', 'int2', 'int4', 'date', 'int8', 'time', 'timestamp', 'money', 'float4', 'float8', 'bpchar', 'varchar', 'text', 'bool');

create type cs_sort_order as enum ('asc', 'desc');

create function cs_get_tid(type_name cs_elem_type) returns integer as $$
begin
    case type_name
    when 'char' then return 0;
    when 'int2' then return 1;
    when 'int4' then return 2;
    when 'date' then return 3;
    when 'int8' then return 4;
    when 'time' then return 5;
    when 'timestamp' then return 6;
    when 'money' then return 7;
    when 'float4' then return 8;
    when 'float8' then return 9;
    when 'bpchar' then return 10;
    when 'varchar' then return 10;
    when 'text' then return 10;
    when 'bool' then return 0;
    end case;
end;
$$ language plpgsql;

create function cs_create(table_name text, timestamp_id text, timeseries_id text default null, autoupdate bool default false) returns void as $create$
declare
    meta record;
    create_type text;       
    create_load_plsql_func text;
    create_load_func text;
    create_append_func text;
    create_delete_head_func text;
    create_delete_func text;
    create_get_func text;
    create_getall_func text;
    create_span_func text;
    create_spanall_func text;
    create_concat_func text;
    create_insert_trigger_func text;
    create_delete_trigger_func text;
    create_truncate_trigger_func text;
    create_join_func text;
    create_first_func text;
    create_last_func text;
    create_count_func text;
    create_insert_trigger text;
    create_delete_trigger text;
    create_truncate_trigger text;
    create_is_loaded_func text;
    create_drop_func text;
    create_truncate_func text;
    create_project_func text;
    trigger_args text := '';
    sep text;
    perf text;
    id_type text;
    timestamp_type text;
    relid integer;
    is_timestamp bool;
    attr_tid integer;
    timestamp_tid integer;
    timestamp_attnum integer;
    id_attnum integer := 0;
    attr_len integer;
    is_view bool;
begin
    table_name:=lower(table_name);
    select oid,relkind='v' or relkind='m' into relid,is_view from pg_class where relname=table_name;
    if relid is null then 
         raise exception 'Table % is not found',table_name;
    end if;
    
    if (timeseries_id is not null) then
        timeseries_id = lower(timeseries_id);
        select pg_type.typname,pg_attribute.attnum into id_type,id_attnum from pg_attribute,pg_type where pg_attribute.attrelid=relid and pg_attribute.atttypid=pg_type.oid and pg_attribute.attname=timeseries_id;
        if id_type is null then 
             raise exception 'No attribute % in table %',timeseries_id,table_name;
        end if;
    end if;

    timestamp_id = lower(timestamp_id);
    select pg_type.typname,pg_attribute.attnum into timestamp_type,timestamp_attnum from pg_attribute,pg_type where pg_attribute.attrelid=relid and pg_attribute.atttypid=pg_type.oid and pg_attribute.attname=timestamp_id;
    if timestamp_type is null then 
        raise exception 'No attribute % in table %',timestamp_id,table_name;
    end if;
    timestamp_tid := cs_get_tid(timestamp_type::cs_elem_type);

    create_type := 'create type '||table_name||'_timeseries as ';
    sep := '(';
    perf := 'perform ';

    if (timeseries_id is not null) then
        create_truncate_func := 'create function '||table_name||'_truncate() returns void as $$
            begin
                perform columnar_store_truncate('''||lower(table_name)||'''); 
            end; $$ language plpgsql';
    else 
        create_truncate_func := 'create function '||table_name||'_truncate() returns void as $$
            begin
                perform '||table_name||'_delete(); 
            end; $$ language plpgsql';
    end if;
    
    create_drop_func := 'create function '||table_name||'_drop() returns void as $$
        begin
            drop function '||table_name||'_load(bool,text);
            drop function '||table_name||'_is_loaded();
            drop function '||table_name||'_append('||timestamp_type||');
            drop function '||table_name||'_truncate();
            drop function '||table_name||'_project('||table_name||'_timeseries,timeseries,bool);';
    if (not is_view) then
        create_drop_func := create_drop_func||'
            drop trigger '||table_name||'_insert on '||table_name||';
            drop trigger '||table_name||'_delete on '||table_name||';
            drop trigger '||table_name||'_truncate on '||table_name||';
            drop function '||table_name||'_insert_trigger();
            drop function '||table_name||'_delete_trigger();
            drop function '||table_name||'_truncate_trigger();';
    end if;      
    create_project_func:='create function '||table_name||'_project('||table_name||'_timeseries,timeseries default null,disable_caching bool default false) returns setof '||table_name||' as ''$libdir/imcs'',''cs_project'' language C stable';

    if (timeseries_id is not null) then
        create_drop_func := create_drop_func||'
            drop function '||table_name||'_get('||id_type||','||timestamp_type||','||timestamp_type||',bigint);
            drop function '||table_name||'_get('||id_type||'[],'||timestamp_type||','||timestamp_type||',bigint);
            drop function '||table_name||'_span('||id_type||',bigint,bigint);
            drop function '||table_name||'_span('||id_type||'[],bigint,bigint);
            drop function '||table_name||'_concat('||id_type||'[],'||timestamp_type||','||timestamp_type||');
            drop function '||table_name||'_delete('||id_type||','||timestamp_type||');
            drop function '||table_name||'_delete('||id_type||','||timestamp_type||','||timestamp_type||');
            drop function '||table_name||'_first('||id_type||');
            drop function '||table_name||'_last('||id_type||');
            drop function '||table_name||'_join('||id_type||',timeseries,integer);
            drop function '||table_name||'_count('||id_type||');';
    else 
        create_drop_func := create_drop_func||'
            drop function '||table_name||'_get('||timestamp_type||','||timestamp_type||',bigint);
            drop function '||table_name||'_span(bigint,bigint);
            drop function '||table_name||'_delete('||timestamp_type||');
            drop function '||table_name||'_delete('||timestamp_type||','||timestamp_type||');
            drop function '||table_name||'_first();
            drop function '||table_name||'_last();
            drop function '||table_name||'_join(timeseries,integer);
            drop function '||table_name||'_count();';
    end if;
    create_drop_func := create_drop_func||'
            drop type '||table_name||'_timeseries;
            drop function '||table_name||'_drop(); 
        end; $$ language plpgsql';

    create_load_func :=  'create function '||table_name||'_load(already_sorted bool default false, filter text default null) returns bigint as $$ begin return columnar_store_load('''||table_name||''','||id_attnum||','||timestamp_attnum||',already_sorted,filter::cstring); end; $$ language plpgsql';

    create_is_loaded_func := 'create function '||table_name||'_is_loaded() returns bool as $$ begin return columnar_store_initialized('''||table_name||'-'||timestamp_id||''',false); end; $$ language plpgsql';

    -- PL/pgSQL version of load function is too slow... Leave it here just for reference. 
    create_load_plsql_func := 'create function '||table_name||'_load() returns bigint as $$ 
        declare 
            rec record;
            n bigint := 0;
        begin
            if not columnar_store_initialized('''||table_name||'-'||timestamp_id||''','||(timeseries_id is not null)||') then 
                for rec in select * from '||table_name||' order by '||timestamp_id||' loop 
                    n := n + 1;';

    if (timeseries_id is not null) then
        create_delete_head_func := 'create function '||table_name||'_delete('||timeseries_id||' '||id_type||',till_ts '||timestamp_type||' default null) returns bigint as $$ begin return '||table_name||'_delete('||timeseries_id||',null,till_ts); end; $$ language plpgsql';

        create_delete_func := 'create function '||table_name||'_delete('||timeseries_id||' '||id_type||',from_ts '||timestamp_type||',till_ts '||timestamp_type||') returns bigint as $$ 
        declare
            search_result timeseries;
        begin
            search_result:=columnar_store_search_'||timestamp_type||'(('''||table_name||'-'||timestamp_id||'-''||'||timeseries_id||'::text)::cstring,from_ts,till_ts,'||timestamp_tid||');';
    else
        create_delete_head_func := 'create function '||table_name||'_delete(till_ts '||timestamp_type||' default null) returns bigint as $$ begin return '||table_name||'_delete(null,till_ts); end; $$ language plpgsql';

        create_delete_func := 'create function '||table_name||'_delete(from_ts '||timestamp_type||',till_ts '||timestamp_type||') returns bigint as $$ 
        declare
            search_result timeseries;
        begin
            search_result:=columnar_store_search_'||timestamp_type||'('''||table_name||'-'||timestamp_id||''',from_ts,till_ts,'||timestamp_tid||');';
    end if;
    create_delete_func := create_delete_func||
       'if (search_result is null) then
            return 0;
        end if;';

    create_append_func := 'create function '||table_name||'_append(from_ts '||timestamp_type||') returns bigint as $$
        declare
            rec record;
            n bigint := 0;
        begin
            for rec in select * from '||table_name||' where '||timestamp_id||'>=from_ts order by '||timestamp_id||' loop
                n := n + 1;';

    if (timeseries_id is not null) then
        create_getall_func := 'create function '||table_name||'_get(ids '||id_type||'[],from_ts '||timestamp_type||' default null,till_ts '||timestamp_type||' default null, limit_ts bigint default null)
            returns setof '||table_name||'_timeseries as $$
            declare
                id '||id_type||';
                ts '||table_name||'_timeseries;
            begin
                foreach id in array ids loop 
                    ts:='||table_name||'_get(id,from_ts,till_ts,limit_ts);
                    return next ts;
                end loop;
                return;
            end; $$ language plpgsql stable';

        create_spanall_func := 'create function '||table_name||'_span(ids '||id_type||'[],from_pos bigint default 0, till_pos bigint default 9223372036854775807)
            returns setof '||table_name||'_timeseries as $$
            declare
                id '||id_type||';
                ts '||table_name||'_timeseries;
            begin
                foreach id in array ids loop 
                    ts:='||table_name||'_span(id,from_pos,till_pos);
                    return next ts;
                end loop;
                return;
            end; $$ language plpgsql stable strict';

        create_concat_func := 'create function '||table_name||'_concat(ids '||id_type||'[],from_ts '||timestamp_type||' default null,till_ts '||timestamp_type||' default null)
            returns '||table_name||'_timeseries as $$
            declare
                i integer;
                id '||id_type||';
                root '||table_name||'_timeseries;
                search_result timeseries;
            begin
                for i in reverse array_upper(ids, 1)..array_lower(ids, 1) loop 
                    id := ids[i];
                    search_result:=columnar_store_search_'||timestamp_type||'(('''||table_name||'-'||timestamp_id||'-''||id::text)::cstring,from_ts,till_ts,'||timestamp_tid||');
                    if (search_result is null) then
                        continue;
                    end if;';
    end if;

    create_get_func := 'create function '||table_name||'_get(';
    if (timeseries_id is not null) then
        create_get_func := create_get_func||timeseries_id||' '||id_type||', ';
    end if;
    create_get_func := create_get_func||'from_ts '||timestamp_type||' default null, till_ts '||timestamp_type||' default null, limit_ts bigint default null)
        returns '||table_name||'_timeseries as $$
        declare
            result '||table_name||'_timeseries;
            search_result timeseries;
        begin
            search_result:=columnar_store_search_'||timestamp_type||'(';
    if (timeseries_id is not null) then 
        create_get_func := create_get_func||'('''||table_name||'-'||timestamp_id||'-''||'||timeseries_id||'::text)::cstring';
    else
        create_get_func := create_get_func||''''||table_name||'-'||timestamp_id||'''';
    end if;
    create_get_func := create_get_func||',from_ts,till_ts,'||timestamp_tid||',limit_ts);
            if (search_result is null) then
                return null;
            end if;
            result."'||timestamp_id||'":=search_result;';


    create_span_func := 'create function '||table_name||'_span(';
    if (timeseries_id is not null) then
        create_span_func := create_span_func||timeseries_id||' '||id_type||', ';
    end if;
    create_span_func := create_span_func||'from_pos bigint default 0, till_pos bigint default 9223372036854775807) returns '||table_name||'_timeseries as $$
        declare
            result '||table_name||'_timeseries;
        begin ';

    create_insert_trigger_func := 'create function '||table_name||'_insert_trigger() returns trigger as $$ begin ';
    create_delete_trigger_func := 'create function '||table_name||'_delete_trigger() returns trigger as $$ begin perform ';
    create_truncate_trigger_func := 'create function '||table_name||'_truncate_trigger() returns trigger as $$ begin perform '||table_name||'_truncate(); return NEW; end; $$ language plpgsql';
    if (timeseries_id is not null) then 
        create_delete_trigger_func := create_delete_trigger_func||table_name||'_delete(OLD."'||timeseries_id||'",OLD."'||timestamp_id||'",OLD."'||timestamp_id||'"); return OLD; end; $$ language plpgsql';
    else 
        create_delete_trigger_func := create_delete_trigger_func||table_name||'_delete(OLD."'||timestamp_id||'",OLD."'||timestamp_id||'"); return OLD; end; $$ language plpgsql';
    end if;
   
    -- PL/pgSQL version of trigger functions atr too slow
    -- create_insert_trigger := 'create trigger '||table_name||'_insert after insert on '||table_name||' for each row execute procedure '||table_name||'_insert_trigger()';
    create_insert_trigger := 'create trigger '||table_name||'_insert after insert on '||table_name||' for each row execute procedure columnar_store_insert_trigger('''||table_name||''','||id_attnum||','||timestamp_attnum;
    create_delete_trigger := 'create trigger '||table_name||'_delete before delete on '||table_name||' for each row execute procedure '||table_name||'_delete_trigger()';
    create_truncate_trigger := 'create trigger '||table_name||'_truncate before truncate on '||table_name||' for each statement execute procedure '||table_name||'_truncate_trigger()';

    for meta in select attname,atttypid,attnum,typname,attlen,atttypmod from pg_attribute,pg_type where relid=pg_attribute.attrelid and pg_attribute.atttypid=pg_type.oid and attnum>0 loop
        attr_tid := cs_get_tid(meta.typname::cs_elem_type);
        is_timestamp := false;
        attr_len := meta.attlen;
        if (attr_len < 0) then -- char(N) type 
            attr_len := meta.atttypmod - 4; -- atttypmod = N + VARHDRSZ
            if (attr_len < 0 and meta.attname <> timeseries_id) then 
                raise exception 'Size is not specified for attribute %',meta.attname;              
            end if;
        end if;
        trigger_args := trigger_args||','''||meta.attname||''','||meta.atttypid||','||attr_len;

        if (meta.attname = timestamp_id) then
            is_timestamp := true;
            if (timeseries_id is not null) then 
                create_first_func := 'create function '||table_name||'_first('||timeseries_id||' '||id_type||') returns '||timestamp_type||
                   ' as $$ begin return columnar_store_first_'||timestamp_type||'(('''||table_name||'-'||timestamp_id||'-''||'||timeseries_id||'::text)::cstring,'||attr_tid||','||attr_len||
                   '); end; $$ language plpgsql strict stable';
                create_last_func := 'create function '||table_name||'_last('||timeseries_id||' '||id_type||') returns '||timestamp_type||
                        ' as $$ begin return columnar_store_last_'||timestamp_type||'(('''||table_name||'-'||timestamp_id||'-''||'||timeseries_id||'::text)::cstring,'||attr_tid||','||attr_len||
                        '); end; $$ language plpgsql strict stable';
                create_count_func := 'create function '||table_name||'_count('||timeseries_id||' '||id_type||') returns bigint as $$ begin
                    return columnar_store_count(('''||table_name||'-'||timestamp_id||'-''||'||timeseries_id||'::text)::cstring,'||attr_tid||','||attr_len|| 
                    '); end; $$ language plpgsql strict stable';
                create_join_func := 'create function '||table_name||'_join('||timeseries_id||' '||id_type||',ts timeseries,direction integer default 1) returns timeseries
                        as $$ begin return columnar_store_join_'||timestamp_type||'(('''||table_name||'-'||timestamp_id||'-''||'||timeseries_id||'::text)::cstring,'||attr_tid||','||attr_len||',ts,direction); end; $$ language plpgsql strict stable';
            else
                create_first_func := 'create function '||table_name||'_first() returns '||timestamp_type||
                    ' as $$ begin return columnar_store_first_'||timestamp_type||'('''||table_name||'-'||timestamp_id||''','||attr_tid||','||attr_len||
                    '); end; $$ language plpgsql strict stable';
                create_last_func := 'create function '||table_name||'_last() returns '||timestamp_type||
                        ' as $$ begin return columnar_store_last_'||timestamp_type||'('''||table_name||'-'||timestamp_id||''','||attr_tid||','||attr_len||
                        '); end; $$ language plpgsql strict stable';
                create_count_func := 'create function '||table_name||'_count() returns bigint as $$ begin 
                    return columnar_store_count('''||table_name||'-'||timestamp_id||''','||attr_tid||','||attr_len||
                    '); end; $$ language plpgsql strict stable';
                create_join_func := 'create function '||table_name||'_join(ts timeseries,direction integer default 1) returns timeseries
                        as $$ begin return columnar_store_join_'||timestamp_type||'('''||table_name||'-'||timestamp_id||''','||attr_tid||','||attr_len||',ts,direction); end; $$ language plpgsql strict stable';
            end if;
        elsif (meta.attname = timeseries_id) then
            create_type := create_type||sep||timeseries_id||' '||id_type;
            create_get_func := create_get_func||'result."'||timeseries_id||'":='||timeseries_id||';';
            create_span_func := create_span_func||'result."'||timeseries_id||'":='||timeseries_id||';';
            sep:=',';
            continue;
        end if;

        create_type := create_type||sep||meta.attname||' timeseries';
        sep:=',';

        if (timeseries_id is not null) then 
            create_load_plsql_func := create_load_plsql_func||perf||'columnar_store_append_'||meta.typname||'(('''||table_name||'-'||meta.attname||'-''||rec."'||timeseries_id||'"::text)::cstring,rec."'||meta.attname||'",'||attr_tid||','||is_timestamp||','||attr_len||')';
            create_delete_func := create_delete_func||perf||'columnar_store_delete(('''||table_name||'-'||meta.attname||'-''||'||timeseries_id||'::text)::cstring,search_result,'||attr_tid||','||is_timestamp||','||attr_len||')';
            create_append_func := create_append_func||perf||'columnar_store_append_'||meta.typname||'(('''||table_name||'-'||meta.attname||'-''||rec."'||timeseries_id||'"::text)::cstring,rec."'||meta.attname||'",'||attr_tid||','||is_timestamp||','||attr_len||')';
            create_insert_trigger_func := create_insert_trigger_func||perf||'columnar_store_append_'||meta.typname||'(('''||table_name||'-'||meta.attname||'-''||NEW."'||timeseries_id||'"::text)::cstring,NEW."'||meta.attname||'",'||attr_tid||','||is_timestamp||','||attr_len||')';
            if (not is_timestamp) then
          	    create_get_func := create_get_func||'result."'||meta.attname||'":=columnar_store_get(('''||table_name||'-'||meta.attname||'-''||'||timeseries_id||'::text)::cstring,search_result,'||attr_tid||','||attr_len||'); '; 
                create_concat_func := create_concat_func||'root."'||meta.attname||'":=cs_concat(columnar_store_get(('''||table_name||'-'||meta.attname||'-''||id::text)::cstring,search_result,'||attr_tid||','||attr_len||'), root."'||meta.attname||'"); '; 
            end if;
       	    create_span_func := create_span_func||'result."'||meta.attname||'":=columnar_store_span(('''||table_name||'-'||meta.attname||'-''||'||timeseries_id||'::text)::cstring,from_pos,till_pos,'||attr_tid||','||is_timestamp||','||attr_len||'); ';        
        else
            create_load_plsql_func := create_load_plsql_func||perf||'columnar_store_append_'||meta.typname||'('''||table_name||'-'||meta.attname||''',rec."'||meta.attname||'",'||attr_tid||','||is_timestamp||','||attr_len||')';
            create_delete_func := create_delete_func||perf||'columnar_store_delete('''||table_name||'-'||meta.attname||''',search_result,'||attr_tid||','||is_timestamp||','||attr_len||')';
            create_append_func := create_append_func||perf||'columnar_store_append_'||meta.typname||'('''||table_name||'-'||meta.attname||''',rec."'||meta.attname||'",'||attr_tid||','||is_timestamp||','||attr_len||')';
            create_insert_trigger_func := create_insert_trigger_func||perf||'columnar_store_append_'||meta.typname||'('''||table_name||'-'||meta.attname||''',NEW."'||meta.attname||'",'||attr_tid||','||is_timestamp||','||attr_len||')';
            if (not is_timestamp) then
                create_get_func := create_get_func||'result."'||meta.attname||'":=columnar_store_get('''||table_name||'-'||meta.attname||''',search_result,'||attr_tid||','||attr_len||');';
            end if;
            create_span_func := create_span_func||'result."'||meta.attname||'":=columnar_store_span('''||table_name||'-'||meta.attname||''',from_pos,till_pos,'||attr_tid||','||is_timestamp||','||attr_len||');';
        end if;
        perf:=',';
    end loop;

    create_type := create_type||')'; 
    create_load_plsql_func := create_load_plsql_func||'; end loop; end if; return n; end; $$ language plpgsql';
    create_append_func := create_append_func||'; end loop; return n; end; $$ language plpgsql';
    create_delete_func := create_delete_func||'; return cs_count(search_result); end; $$ language plpgsql';
    create_get_func := create_get_func||'return result; end; $$ language plpgsql stable';
    create_span_func := create_span_func||'return result; end; $$ language plpgsql stable strict';
    create_concat_func := create_concat_func||'end loop; return root; end; $$ language plpgsql stable';
    create_insert_trigger_func := create_insert_trigger_func||'; return NEW; end; $$ language plpgsql';
        
    create_insert_trigger := create_insert_trigger||trigger_args||')';

    execute create_type;
    execute create_load_func;
    execute create_is_loaded_func;
    execute create_get_func;
    execute create_span_func;
    if (not is_view) then 
        execute create_insert_trigger_func;
        execute create_insert_trigger;
        execute create_delete_trigger_func;
        execute create_delete_trigger;
        execute create_truncate_trigger_func;
        execute create_truncate_trigger;
    end if;
    execute create_append_func;    
    execute create_delete_func;    
    execute create_delete_head_func;    
    execute create_first_func;
    execute create_last_func;
    execute create_join_func;
    execute create_count_func;
    execute create_project_func;
    execute create_truncate_func;
    execute create_drop_func;
    if (timeseries_id is not null) then
        execute create_getall_func;
        execute create_spanall_func;
        execute create_concat_func;
    end if;
    if (not autoupdate and not is_view) then 
        execute 'alter table '||table_name||' disable trigger user';
    end if;
end;
$create$ language plpgsql;

-- Internal functions: do not use them
create function columnar_store_initialized(table_name cstring, initialize bool) returns bool  as 'MODULE_PATHNAME' language C strict;
create function columnar_store_get(cs_id cstring, search_result timeseries, field_type integer, field_size integer) returns timeseries  as 'MODULE_PATHNAME' language C stable strict;
create function columnar_store_span(cs_id cstring, from_pos bigint, till_pos bigint, field_type integer, is_timestamp bool, field_size integer) returns timeseries  as 'MODULE_PATHNAME' language C stable strict;
create function columnar_store_delete(cs_id cstring, search_result timeseries, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME' language C strict;
create function columnar_store_truncate(table_name cstring) returns void as 'MODULE_PATHNAME' language C strict;

create function columnar_store_insert_trigger() returns trigger as 'MODULE_PATHNAME' language C; 

create function columnar_store_load(table_name cstring, timeseries_attnum integer, timestamp_attnum integer, already_sorted bool,filter cstring) returns bigint as 'MODULE_PATHNAME' language C; 
create function columnar_store_append_char(cs_id cstring, val "char", field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_int8' language C strict;
create function columnar_store_append_int2(cs_id cstring, val int2, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_int16' language C strict;
create function columnar_store_append_int4(cs_id cstring, val int4, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_int32' language C strict;
create function columnar_store_append_int8(cs_id cstring, val int8, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_int64' language C strict;
create function columnar_store_append_date(cs_id cstring, val date, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_int32' language C strict;
create function columnar_store_append_time(cs_id cstring, val time, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_int64' language C strict;
create function columnar_store_append_timestamp(cs_id cstring, val timestamp, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_int64' language C strict;
create function columnar_store_append_money(cs_id cstring, val timestamp, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_int64' language C strict;
create function columnar_store_append_float4(cs_id cstring, val float4, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_float' language C strict;
create function columnar_store_append_float8(cs_id cstring, val float8, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_double' language C strict;
create function columnar_store_append_bpchar(cs_id cstring, val text, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_char' language C strict;
create function columnar_store_append_varchar(cs_id cstring, val text, field_type integer, is_timestamp bool, field_size integer) returns void  as 'MODULE_PATHNAME','columnar_store_append_char' language C strict;


create function columnar_store_search_char(cs_id cstring, from_ts "char", till_ts "char", field_type integer, limit_ts bigint default null) returns timeseries as 'MODULE_PATHNAME','columnar_store_search_int8' language C stable;
create function columnar_store_search_int2(cs_id cstring, from_ts int2, till_ts int2, field_type integer, limit_ts bigint default null) returns timeseries as 'MODULE_PATHNAME','columnar_store_search_int16' language C stable;
create function columnar_store_search_int4(cs_id cstring, from_ts int4, till_ts int4, field_type integer, limit_ts bigint default null) returns timeseries as 'MODULE_PATHNAME','columnar_store_search_int32' language C stable;
create function columnar_store_search_int8(cs_id cstring, from_ts int8, till_ts int8, field_type integer, limit_ts bigint default null) returns timeseries as 'MODULE_PATHNAME','columnar_store_search_int64' language C stable;
create function columnar_store_search_date(cs_id cstring, from_ts date, till_ts date, field_type integer, limit_ts bigint default null) returns timeseries as 'MODULE_PATHNAME','columnar_store_search_int32' language C stable;
create function columnar_store_search_time(cs_id cstring, from_ts time, till_ts time, field_type integer, limit_ts bigint default null) returns timeseries as 'MODULE_PATHNAME','columnar_store_search_int64' language C stable;
create function columnar_store_search_timestamp(cs_id cstring, from_ts timestamp, till_ts timestamp, field_type integer, limit_ts bigint default null) returns timeseries as 'MODULE_PATHNAME','columnar_store_search_int64' language C stable;
create function columnar_store_search_float4(cs_id cstring, from_ts float4, till_ts float4, field_type integer, limit_ts bigint default null) returns timeseries as 'MODULE_PATHNAME','columnar_store_search_float' language C stable;
create function columnar_store_search_float8(cs_id cstring, from_ts float8, till_ts float8, field_type integer, limit_ts bigint default null) returns timeseries as 'MODULE_PATHNAME','columnar_store_search_double' language C stable;

create function columnar_store_first_char(id cstring, field_type integer, field_size integer) returns "char" as 'MODULE_PATHNAME','columnar_store_first_int8' language C strict stable;
create function columnar_store_first_int2(id cstring, field_type integer, field_size integer) returns int2 as 'MODULE_PATHNAME','columnar_store_first_int16' language C strict stable;
create function columnar_store_first_int4(id cstring, field_type integer, field_size integer) returns int4 as 'MODULE_PATHNAME','columnar_store_first_int32' language C strict stable;
create function columnar_store_first_int8(id cstring, field_type integer, field_size integer) returns int8 as 'MODULE_PATHNAME','columnar_store_first_int64' language C strict stable;
create function columnar_store_first_date(id cstring, field_type integer, field_size integer) returns date as 'MODULE_PATHNAME','columnar_store_first_int32' language C strict stable;
create function columnar_store_first_time(id cstring, field_type integer, field_size integer) returns time as 'MODULE_PATHNAME','columnar_store_first_int64' language C strict stable;
create function columnar_store_first_timestamp(id cstring, field_type integer, field_size integer) returns timestamp as 'MODULE_PATHNAME','columnar_store_first_int64' language C strict stable;
create function columnar_store_first_float4(id cstring, field_type integer, field_size integer) returns float4 as 'MODULE_PATHNAME','columnar_store_first_float' language C strict stable;
create function columnar_store_first_float8(id cstring, field_type integer, field_size integer) returns float8 as 'MODULE_PATHNAME','columnar_store_first_double' language C strict stable;

create function columnar_store_last_char(id cstring, field_type integer, field_size integer) returns "char" as 'MODULE_PATHNAME','columnar_store_last_int8' language C strict stable;
create function columnar_store_last_int2(id cstring, field_type integer, field_size integer) returns int2 as 'MODULE_PATHNAME','columnar_store_last_int16' language C strict stable;
create function columnar_store_last_int4(id cstring, field_type integer, field_size integer) returns int4 as 'MODULE_PATHNAME','columnar_store_last_int32' language C strict stable;
create function columnar_store_last_int8(id cstring, field_type integer, field_size integer) returns int8 as 'MODULE_PATHNAME','columnar_store_last_int64' language C strict stable;
create function columnar_store_last_date(id cstring, field_type integer, field_size integer) returns date as 'MODULE_PATHNAME','columnar_store_last_int32' language C strict stable;
create function columnar_store_last_time(id cstring, field_type integer, field_size integer) returns time as 'MODULE_PATHNAME','columnar_store_last_int64' language C strict stable;
create function columnar_store_last_timestamp(id cstring, field_type integer, field_size integer) returns timestamp as 'MODULE_PATHNAME','columnar_store_last_int64' language C strict stable;
create function columnar_store_last_float4(id cstring, field_type integer, field_size integer) returns float4 as 'MODULE_PATHNAME','columnar_store_last_float' language C strict stable;
create function columnar_store_last_float8(id cstring, field_type integer, field_size integer) returns float8 as 'MODULE_PATHNAME','columnar_store_last_double' language C strict stable;

create function columnar_store_join_char(id cstring, field_type integer, field_size integer, ts timeseries,direction integer) returns timeseries as 'MODULE_PATHNAME','columnar_store_join_int8' language C strict stable;
create function columnar_store_join_int2(id cstring, field_type integer, field_size integer, ts timeseries,direction integer) returns timeseries as 'MODULE_PATHNAME','columnar_store_join_int16' language C strict stable;
create function columnar_store_join_int4(id cstring, field_type integer, field_size integer, ts timeseries,direction integer) returns timeseries as 'MODULE_PATHNAME','columnar_store_join_int32' language C strict stable;
create function columnar_store_join_int8(id cstring, field_type integer, field_size integer, ts timeseries,direction integer) returns timeseries as 'MODULE_PATHNAME','columnar_store_join_int64' language C strict stable;
create function columnar_store_join_date(id cstring, field_type integer, field_size integer, ts timeseries,direction integer) returns timeseries as 'MODULE_PATHNAME','columnar_store_join_int32' language C strict stable;
create function columnar_store_join_time(id cstring, field_type integer, field_size integer, ts timeseries,direction integer) returns timeseries as 'MODULE_PATHNAME','columnar_store_join_int64' language C strict stable;
create function columnar_store_join_timestamp(id cstring, field_type integer, field_size integer, ts timeseries,direction integer) returns timeseries as 'MODULE_PATHNAME','columnar_store_join_int64' language C strict stable;
create function columnar_store_join_float4(id cstring, field_type integer, field_size integer, ts timeseries,direction integer) returns timeseries as 'MODULE_PATHNAME','columnar_store_join_float' language C strict stable;
create function columnar_store_join_float8(id cstring, field_type integer, field_size integer, ts timeseries,direction integer) returns timeseries as 'MODULE_PATHNAME','columnar_store_join_double' language C strict stable;

create function columnar_store_count(id cstring, field_type integer, field_size integer) returns bigint as 'MODULE_PATHNAME' language C strict stable;

create function cs_delete_all() returns bigint as 'MODULE_PATHNAME' language C strict;
create function cs_used_memory() returns bigint as 'MODULE_PATHNAME' language C strict;

create function cs_parse_tid(str text, elem_type integer, elem_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_const_num(val float8, elem_type integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_const_dt(val timestamp, elem_type integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_const_str(val text, elem_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_cast_tid(input timeseries, elem_type integer, elem_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_type(timeseries) returns integer  as 'MODULE_PATHNAME' language C stable strict;
create function cs_elem_size(timeseries) returns integer  as 'MODULE_PATHNAME' language C stable strict;

-- Timeseries type implementation functions: do not use them
create function cs_input_function(cstring) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_output_function(timeseries) returns cstring  as 'MODULE_PATHNAME' language C stable strict;
create function cs_receive_function(internal) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_send_function(timeseries) returns bytea  as 'MODULE_PATHNAME' language C stable strict;

create type timeseries (
    input = cs_input_function,
    output = cs_output_function,
    receive = cs_receive_function,
    send = cs_send_function,
    alignment = double,
    internallength = 8,
    passedbyvalue
);

-- Timeseries functions and operators
create function cs_project(anyelement, positions timeseries default null, disable_caching bool default false) returns setof record  as 'MODULE_PATHNAME' language C stable;

create type cs_agg_result as (agg_val float8, group_by bytea);

create function cs_project_agg(anyelement, positions timeseries default null, disable_caching bool default false) returns setof cs_agg_result as 'MODULE_PATHNAME' language C stable;

create function cs_parse(str text, elem_type cs_elem_type, elem_size integer default 0) returns timeseries as
$$ begin return cs_parse_tid(str, cs_get_tid(elem_type), elem_size); end; $$ language plpgsql stable strict;

create function cs_const(val float8, elem_type cs_elem_type default 'float8') returns timeseries as
$$ begin return cs_const_num(val, cs_get_tid(elem_type)); end; $$ language plpgsql stable strict;

create function cs_const(val timestamp, elem_type cs_elem_type) returns timeseries as
$$ begin return cs_const_dt(val, cs_get_tid(elem_type)); end; $$ language plpgsql stable strict;

create function cs_const(val text, elem_size integer) returns timeseries as
$$ begin return cs_const_str(val, elem_size); end; $$ language plpgsql stable strict;

create function cs_const(val text) returns timeseries as
$$ begin return cs_const_str(val, length(val)); end; $$ language plpgsql stable strict;

create function cs_add(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_add_seq_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_add(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_add_num_seq(val float8, ts timeseries) returns timeseries 
as $$ begin return cs_add(cs_const_num(val, cs_type(ts)), ts); end; $$ language plpgsql stable strict;

create function cs_add_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_add(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_add_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_add(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator + (leftarg=timeseries, rightarg=timeseries, procedure=cs_add, commutator=+);
create operator + (leftarg=timeseries, rightarg=float8, procedure=cs_add_seq_num, commutator=+);
create operator + (leftarg=float8, rightarg=timeseries, procedure=cs_add_num_seq, commutator=+);
create operator + (leftarg=timeseries, rightarg=text, procedure=cs_add_str, commutator=+);
create operator + (leftarg=timeseries, rightarg=timestamp, procedure=cs_add_dt, commutator=+);

create function cs_mul(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_mul_seq_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_mul(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_mul_num_seq(val float8, ts timeseries) returns timeseries 
as $$ begin return cs_mul(cs_const_num(val, cs_type(ts)), ts); end; $$ language plpgsql stable strict;

create function cs_mul_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_mul(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator * (leftarg=timeseries, rightarg=timeseries, procedure=cs_mul, commutator= *);
create operator * (leftarg=timeseries, rightarg=float8, procedure=cs_mul_seq_num, commutator= *);
create operator * (leftarg=float8, rightarg=timeseries, procedure=cs_mul_num_seq, commutator= *);
create operator * (leftarg=timeseries, rightarg=text, procedure=cs_mul_str, commutator= *);

create function cs_sub(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_sub_seq_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_sub(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_sub_num_seq(val float8, ts timeseries) returns timeseries 
as $$ begin return cs_sub(cs_const_num(val, cs_type(ts)), ts); end; $$ language plpgsql stable strict;

create function cs_sub_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_sub(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_sub_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_sub(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator - (leftarg=timeseries, rightarg=timeseries, procedure=cs_sub);
create operator - (leftarg=timeseries, rightarg=float8, procedure=cs_sub_seq_num);
create operator - (leftarg=float8, rightarg=timeseries, procedure=cs_sub_num_seq);
create operator - (leftarg=timeseries, rightarg=text, procedure=cs_sub_str);
create operator - (leftarg=timeseries, rightarg=timestamp, procedure=cs_sub_dt);

create function cs_div(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_div_seq_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_div(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_div_num_seq(val float8, ts timeseries) returns timeseries 
as $$ begin return cs_div(cs_const_num(val, cs_type(ts)), ts); end; $$ language plpgsql stable strict;

create function cs_div_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_div(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator / (leftarg=timeseries, rightarg=timeseries, procedure=cs_div);
create operator / (leftarg=timeseries, rightarg=float8, procedure=cs_div_seq_num);
create operator / (leftarg=float8, rightarg=timeseries, procedure=cs_div_num_seq);
create operator / (leftarg=timeseries, rightarg=text, procedure=cs_div_str);

create function cs_mod(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_mod_seq_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_mod(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_mod_num_seq(val float8, ts timeseries) returns timeseries 
as $$ begin return cs_mod(cs_const_num(val, cs_type(ts)), ts); end; $$ language plpgsql stable strict;

create function cs_mod_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_mod(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator % (leftarg=timeseries, rightarg=timeseries, procedure=cs_mod);
create operator % (leftarg=timeseries, rightarg=float8, procedure=cs_mod_seq_num);
create operator % (leftarg=float8, rightarg=timeseries, procedure=cs_mod_num_seq);
create operator % (leftarg=timeseries, rightarg=text, procedure=cs_mod_str);

create function cs_pow(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_pow_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_pow(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_pow_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_pow(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator ^ (leftarg=timeseries, rightarg=timeseries, procedure=cs_pow);
create operator ^ (leftarg=timeseries, rightarg=float8, procedure=cs_pow_num);
create operator ^ (leftarg=timeseries, rightarg=text, procedure=cs_pow_str);

create function cs_and(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_and_int(ts timeseries, val bigint) returns timeseries 
as $$ begin return cs_and(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_and_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_and(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator & (leftarg=timeseries, rightarg=timeseries, procedure=cs_and, commutator= &);
create operator & (leftarg=timeseries, rightarg=bigint, procedure=cs_and_int, commutator= &);
create operator & (leftarg=timeseries, rightarg=text, procedure=cs_and_str, commutator= &);

create function cs_or(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_or_int(ts timeseries, val bigint) returns timeseries 
as $$ begin return cs_or(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_or_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_or(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator | (leftarg=timeseries, rightarg=timeseries, procedure=cs_or, commutator= |);
create operator | (leftarg=timeseries, rightarg=bigint, procedure=cs_or_int, commutator= &);
create operator | (leftarg=timeseries, rightarg=text, procedure=cs_or_str, commutator= &);

create function cs_xor(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_xor_int(ts timeseries, val bigint) returns timeseries 
as $$ begin return cs_xor(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_xor_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_xor(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator # (leftarg=timeseries, rightarg=timeseries, procedure=cs_xor, commutator= #);
create operator # (leftarg=timeseries, rightarg=bigint, procedure=cs_xor_int, commutator= &);
create operator # (leftarg=timeseries, rightarg=text, procedure=cs_xor_str, commutator= &);

create function cs_concat(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable;

create function cs_concat_seq_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_concat(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_concat_num_seq(val float8, ts timeseries) returns timeseries 
as $$ begin return cs_concat(cs_const_num(val, cs_type(ts)), ts); end; $$ language plpgsql stable strict;

create function cs_concat_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_concat(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_concat_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_concat(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator ||| (leftarg=timeseries, rightarg=timeseries, procedure=cs_concat, commutator= |||);
create operator ||| (leftarg=timeseries, rightarg=float8, procedure=cs_concat_seq_num, commutator= |||);
create operator ||| (leftarg=float8, rightarg=timeseries, procedure=cs_concat_num_seq, commutator= |||);
create operator ||| (leftarg=timeseries, rightarg=timestamp, procedure=cs_concat_dt, commutator= |||);
create operator ||| (leftarg=timeseries, rightarg=text, procedure=cs_concat_str, commutator= |||);

create function cs_cat(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_cat_seq_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_cat(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_cat_num_seq(val float8, ts timeseries) returns timeseries 
as $$ begin return cs_cat(cs_const_num(val, cs_type(ts)), ts); end; $$ language plpgsql stable strict;

create function cs_cat_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_cat(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_cat_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_cat(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator || (leftarg=timeseries, rightarg=timeseries, procedure=cs_cat, commutator= ||);
create operator || (leftarg=timeseries, rightarg=float8, procedure=cs_cat_seq_num, commutator= ||);
create operator || (leftarg=float8, rightarg=timeseries, procedure=cs_cat_num_seq, commutator= ||);
create operator || (leftarg=timeseries, rightarg=timestamp, procedure=cs_cat_dt, commutator= ||);
create operator || (leftarg=timeseries, rightarg=text, procedure=cs_cat_str, commutator= ||);

create function cs_cut(str bytea, format cstring) returns record as 'MODULE_PATHNAME' language C stable strict;
create function cs_as(str bytea, type_name cstring) returns record as 'MODULE_PATHNAME' language C stable strict;

create function cs_like(timeseries,pattern text) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_ilike(timeseries,pattern text) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create operator ~~ (leftarg=timeseries, rightarg=text, procedure=cs_ilike);

create function cs_eq(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_eq_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_eq(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_eq_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_eq(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_eq_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_eq(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator = (leftarg=timeseries, rightarg=timeseries, procedure=cs_eq, commutator= =);
create operator = (leftarg=timeseries, rightarg=float8, procedure=cs_eq_num, commutator= =);
create operator = (leftarg=timeseries, rightarg=text, procedure=cs_eq_str, commutator= =);
create operator = (leftarg=timeseries, rightarg=timestamp, procedure=cs_eq_dt, commutator= =);

create function cs_ne(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_ne_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_ne(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_ne_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_ne(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_ne_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_ne(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator <> (leftarg=timeseries, rightarg=timeseries, procedure=cs_ne, commutator= <>);
create operator <> (leftarg=timeseries, rightarg=float8, procedure=cs_ne_num, commutator= <>);
create operator <> (leftarg=timeseries, rightarg=text, procedure=cs_ne_str, commutator= <>);
create operator <> (leftarg=timeseries, rightarg=timestamp, procedure=cs_ne_dt, commutator= <>);
 
create function cs_ge(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_ge_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_ge(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_ge_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_ge(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_ge_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_ge(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator >= (leftarg=timeseries, rightarg=timeseries, procedure=cs_ge, commutator= <=);
create operator >= (leftarg=timeseries, rightarg=float8, procedure=cs_ge_num, commutator= <=);
create operator >= (leftarg=timeseries, rightarg=text, procedure=cs_ge_str, commutator= <=);
create operator >= (leftarg=timeseries, rightarg=timestamp, procedure=cs_ge_dt, commutator= <=);

create function cs_le(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_le_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_le(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_le_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_le(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_le_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_le(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator <= (leftarg=timeseries, rightarg=timeseries, procedure=cs_le, commutator= >=);
create operator <= (leftarg=timeseries, rightarg=float8, procedure=cs_le_num, commutator= >=);
create operator <= (leftarg=timeseries, rightarg=text, procedure=cs_le_str, commutator= >=);
create operator <= (leftarg=timeseries, rightarg=timestamp, procedure=cs_le_dt, commutator= >=);

create function cs_lt(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_lt_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_lt(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_lt_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_lt(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_lt_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_lt(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator < (leftarg=timeseries, rightarg=timeseries, procedure=cs_lt, commutator= >);
create operator < (leftarg=timeseries, rightarg=float8, procedure=cs_lt_num, commutator= >);
create operator < (leftarg=timeseries, rightarg=text, procedure=cs_lt_str, commutator= >);
create operator < (leftarg=timeseries, rightarg=timestamp, procedure=cs_lt_dt, commutator= >);

create function cs_gt(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_gt_num(ts timeseries, val float8) returns timeseries 
as $$ begin return cs_gt(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_gt_dt(ts timeseries, val timestamp) returns timeseries 
as $$ begin return cs_gt(ts, cs_const_dt(val, cs_type(ts))); end; $$ language plpgsql stable strict;

create function cs_gt_str(ts timeseries, val text) returns timeseries 
as $$ begin return cs_gt(ts, cs_parse_tid(val, cs_type(ts), cs_elem_size(ts))); end; $$ language plpgsql stable strict;

create operator > (leftarg=timeseries, rightarg=timeseries, procedure=cs_gt, commutator= <);
create operator > (leftarg=timeseries, rightarg=float8, procedure=cs_gt_num, commutator= <);
create operator > (leftarg=timeseries, rightarg=text, procedure=cs_gt_str, commutator= <);
create operator > (leftarg=timeseries, rightarg=timestamp, procedure=cs_gt_dt, commutator= <);

create function cs_maxof(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_maxof(ts timeseries,val float8) returns timeseries as $$
begin return cs_maxof(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;
create function cs_maxof(val float8,ts timeseries) returns timeseries as $$
begin return cs_maxof(cs_const_num(val, cs_type(ts)), ts); end; $$ language plpgsql stable strict;

create function cs_minof(timeseries,timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_minof(ts timeseries,val float8) returns timeseries as $$
begin return cs_minof(ts, cs_const_num(val, cs_type(ts))); end; $$ language plpgsql stable strict;
create function cs_minof(val float8, ts timeseries) returns timeseries as $$
begin return cs_minof(cs_const_num(val, cs_type(ts)), ts); end; $$ language plpgsql stable strict;

create function cs_neg(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create operator - (rightarg=timeseries, procedure=cs_neg);

create function cs_not(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create operator ! (rightarg=timeseries, procedure=cs_not);

create function cs_bit_not(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create operator ~ (rightarg=timeseries, procedure=cs_bit_not);

create function cs_abs(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create operator @ (rightarg=timeseries, procedure=cs_abs);

create function cs_limit(timeseries, from_pos bigint default 0, till_pos bigint default 9223372036854775807) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_head(ts timeseries, n bigint default 1) returns timeseries as $$ begin return cs_limit(ts, 0, n-1); end; $$ language plpgsql stable strict;
create function cs_tail(ts timeseries, n bigint default 1) returns timeseries as $$ begin return cs_limit(ts, -n); end; $$ language plpgsql stable strict;
create function cs_cut_head(ts timeseries, n bigint default 1) returns timeseries as $$ begin return cs_limit(ts, n); end; $$ language plpgsql stable strict;
create function cs_cut_tail(ts timeseries, n bigint default 1) returns timeseries as $$ begin return cs_limit(ts, 0, -n-1); end; $$ language plpgsql stable strict;

create operator << (leftarg=timeseries, rightarg=bigint, procedure=cs_cut_head);
create operator >> (leftarg=timeseries, rightarg=bigint, procedure=cs_cut_tail);


create function cs_sin(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_cos(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_tan(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_exp(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_asin(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_acos(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_atan(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_sqrt(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_log(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_ceil(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_floor(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_isnan(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_year(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_month(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_mday(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_wday(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_hour(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_minute(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_second(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_quarter(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_week(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_wsum(timeseries,timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_wavg(timeseries,timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_corr(timeseries,timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_cov(timeseries,timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_norm(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create operator +* (leftarg=timeseries, rightarg=timeseries, procedure=cs_wsum, commutator= +*);
create operator // (leftarg=timeseries, rightarg=timeseries, procedure=cs_wavg);
create operator ~ (leftarg=timeseries, rightarg=timeseries, procedure=cs_corr, commutator= ~);

create function cs_thin(timeseries, origin integer, step integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_iif(cond timeseries, then_ts timeseries, else_ts timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_if(cond timeseries, then_ts timeseries, else_ts timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_filter(cond timeseries, input timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_filter_pos(cond timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_filter_first_pos(timeseries, n integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create operator ? (leftarg=timeseries, rightarg=timeseries, procedure=cs_filter);
create operator ? (leftarg=timeseries, procedure=cs_filter_pos);

create function cs_unique(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_reverse(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_trend(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_diff(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_repeat(input timeseries, count integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_sort(input timeseries, sort_order cs_sort_order default 'asc') returns timeseries as $$
    begin
        return cs_sort_asc(input, sort_order = 'asc');
    end;
$$ language plpgsql stable strict;
create function cs_sort_asc(timeseries, ascent_order bool) returns timeseries as 'MODULE_PATHNAME','cs_sort' language C stable strict;

create function cs_sort_pos(input timeseries, sort_order cs_sort_order default 'asc') returns timeseries as $$
    begin
        return cs_sort_pos_asc(input, sort_order = 'asc');
    end;
$$ language plpgsql stable strict;
create function cs_sort_pos_asc(timeseries, ascent_order bool) returns timeseries as 'MODULE_PATHNAME','cs_sort_pos' language C stable strict;

create function cs_rank(input timeseries, sort_order cs_sort_order default 'asc') returns timeseries as $$
    begin
        return cs_rank_asc(input, sort_order = 'asc');
    end;
$$ language plpgsql stable strict;
create function cs_rank_asc(timeseries, ascent_order bool) returns timeseries as 'MODULE_PATHNAME','cs_rank' language C stable strict;

create function cs_dense_rank(input timeseries, sort_order cs_sort_order default 'asc') returns timeseries as $$
    begin
        return cs_dense_rank_asc(input, sort_order = 'asc');
    end;
$$ language plpgsql stable strict;
create function cs_dense_rank_asc(timeseries, ascent_order bool) returns timeseries as 'MODULE_PATHNAME','cs_dense_rank' language C stable strict;

create function cs_quantile(timeseries, q_num integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_count(timeseries) returns bigint  as 'MODULE_PATHNAME' language C stable strict;
create function cs_approxdc(timeseries) returns bigint  as 'MODULE_PATHNAME' language C stable strict;
create function cs_max(timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_min(timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_avg(timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_sum(timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_prd(timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_var(timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_dev(timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;
create function cs_all(timeseries) returns bigint  as 'MODULE_PATHNAME' language C stable strict;
create function cs_any(timeseries) returns bigint  as 'MODULE_PATHNAME' language C stable strict;
create function cs_median(timeseries) returns float8  as 'MODULE_PATHNAME' language C stable strict;

create function cs_group_count(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_approxdc(input timeseries, group_by timeseries) returns bigint  as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_max(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_min(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_avg(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_sum(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_var(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_dev(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_last(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_first(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_all(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_group_any(input timeseries, group_by timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_grid_max(timeseries, step integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_grid_min(timeseries, step integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_grid_avg(timeseries, step integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_grid_sum(timeseries, step integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_grid_var(timeseries, step integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_grid_dev(timeseries, step integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_window_max(timeseries, window_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_window_min(timeseries, window_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_window_avg(timeseries, window_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_window_sum(timeseries, window_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_window_var(timeseries, window_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_window_dev(timeseries, window_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_window_ema(timeseries, window_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_window_atr(timeseries, window_size integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_hash_count(group_by timeseries, out count timeseries, out groups timeseries) returns record  as 'MODULE_PATHNAME' language C stable strict;
create function cs_hash_dup_count(input timeseries, group_by timeseries, out count timeseries, out groups timeseries, min_occurrences integer default 1) returns record  as 'MODULE_PATHNAME' language C stable strict;
create function cs_hash_max(input timeseries, group_by timeseries, out max timeseries, out groups timeseries) returns record  as 'MODULE_PATHNAME' language C stable strict;
create function cs_hash_min(input timeseries, group_by timeseries, out min timeseries, out groups timeseries) returns record  as 'MODULE_PATHNAME' language C stable strict;
create function cs_hash_avg(input timeseries, group_by timeseries, out avg timeseries, out groups timeseries) returns record  as 'MODULE_PATHNAME' language C stable strict;
create function cs_hash_sum(input timeseries, group_by timeseries, out sum timeseries, out groups timeseries) returns record  as 'MODULE_PATHNAME' language C stable strict;
create function cs_hash_all(input timeseries, group_by timeseries, out sum timeseries, out groups timeseries) returns record  as 'MODULE_PATHNAME' language C stable strict;
create function cs_hash_any(input timeseries, group_by timeseries, out sum timeseries, out groups timeseries) returns record  as 'MODULE_PATHNAME' language C stable strict;

create function cs_top_max(timeseries, top integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_top_min(timeseries, top integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_top_max_pos(timeseries, top integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_top_min_pos(timeseries, top integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_cum_max(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_cum_min(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_cum_avg(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_cum_sum(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_cum_prd(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_cum_var(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_cum_dev(timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_histogram(input timeseries, min float8, max float8, n_intervals integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_cross(input timeseries, first_cross_direction integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_extrema(input timeseries, first_extremum integer) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_stretch(ts1 timeseries, ts2 timeseries, vals timeseries, filler float8) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_stretch0(ts1 timeseries, ts2 timeseries, vals timeseries, filler float8) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_asof_join(ts1 timeseries, ts2 timeseries, vals timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_asof_join_pos(ts1 timeseries, ts2 timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create operator -> (leftarg=timeseries, rightarg=timeseries, procedure=cs_asof_join_pos);

create function cs_join(ts1 timeseries, ts2 timeseries, vals timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_join_pos(ts1 timeseries, ts2 timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create operator <-> (leftarg=timeseries, rightarg=timeseries, procedure=cs_join_pos);

create function cs_cast(input timeseries, elem_type cs_elem_type, elem_size integer default 0) returns timeseries as $$
begin
    return cs_cast_tid(input, cs_get_tid(elem_type), elem_size);
end;
$$ language plpgsql stable strict;

create function cs_map(input timeseries, positions timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;
create function cs_union(left timeseries, right timeseries) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_empty(timeseries) returns bool as 'MODULE_PATHNAME' language C stable strict;

create function cs_call(timeseries,func oid) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create function cs_to_char_array(timeseries) returns "char"[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_int2_array(timeseries) returns int2[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_int4_array(timeseries) returns int4[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_date_array(timeseries) returns date[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_int8_array(timeseries) returns int8[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_time_array(timeseries) returns time[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_timestamp_array(timeseries) returns timestamp[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_money_array(timeseries) returns money[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_float4_array(timeseries) returns float4[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_float8_array(timeseries) returns float8[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_bpchar_array(timeseries) returns bpchar[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;
create function cs_to_varchar_array(timeseries) returns varchar[] as 'MODULE_PATHNAME','cs_to_array' language C stable strict;

create function cs_from_array(anyarray, elem_size integer default 0) returns timeseries as 'MODULE_PATHNAME' language C stable strict;

create type cs_profile_item as (command text, counter integer);
create function cs_profile(reset bool default false) returns setof cs_profile_item as 'MODULE_PATHNAME' language C stable strict;