#!/usr/bin/env python3
#
# Copyright 2020 Red Hat, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
import argparse
import faker
import logging
import os
import psycopg2
from psycopg2.extras import NamedTupleCursor
import sqlparse
import sys
from tempfile import TemporaryFile


if sys.version_info.major < 3 or sys.version_info.minor < 6:
    raise RuntimeError("This script requires Python >= 3.6")


EXTENSIONS = set()
UNPRIV_USER = '__monitor'


logging.basicConfig(level=logging.INFO, style="{", format="{filename}:{asctime}:{levelname}:{message}")
LOG = logging.getLogger('pg_ext_demo')
LINFO = LOG.info
LERROR = LOG.error
LWARN = LOG.warning
LDEBUG = LOG.debug

FAKER = faker.Faker()


def connect(db_url):
    return psycopg2.connect(db_url, application_name="pg_extension_demo", cursor_factory=NamedTupleCursor)


def parse_sql(sql):
    return sqlparse.format(sql,
                           encoding='utf-8', 
                           reindent_aligned=True, 
                           keyword_case='upper')


def mogrify_sql(cur, sql, values=None):
    try:
        mog_sql = cur.mogrify(sql, values)
    except psycopg2.ProgrammingError:
        mog_sql = sql + os.linesep + '-- PARAMETERS: {str(values)}'
    
    return parse_sql(mog_sql)


def execute(conn, sql, values=None):
    cur = conn.cursor()
    LDEBUG(f"Executiong SQL: {os.linesep}{mogrify_sql(cur, sql, values)}")
    cur.execute(sql, values)
    return cur


def create_user_db_url(conn, user, password):
    dsnp = conn.get_dsn_parameters()
    db_url = f"postgresql://{user}:{password}@{dsnp['host']}:{dsnp['port']}/{dsnp['dbname']}"
    LDEBUG(f"db_url: {db_url}")
    return db_url


def prompt(message):
    print(message, end="", flush=True)
    _ = input()


def check_preload_library(conn, lib_name):
    sql = """
select setting
  from pg_catalog.pg_settings
 where name = 'shared_preload_libraries';
"""
    res = execute(conn, sql).fetchone()
    if res is None or lib_name not in res.setting:
        LWARN(f"The shared library '{lib_name}' is not loaded.")
        return False
    else:
        return True


def setup_demo(conn):
    LINFO("Creating a new schema __demo for this demonstration")
    try:
        sql = f"""
create schema __demo authorization {conn.get_dsn_parameters()['user']};
"""
        execute(conn, sql)

        LINFO("Enabling extensions for demonstration")
        if 'pg_trgm' not in EXTENSIONS:
            sql = """create extension pg_trgm schema public;"""
            execute(conn, sql)
        else:
            LINFO("Extension \"pg_trgm\" already exists")
        if 'pg_stat_statements' not in EXTENSIONS:
            sql = """create extension pg_stat_statements;"""
            execute(conn, sql)
        else:
            LINFO("Extension \"pg_stat_statements\" already exists")

        LINFO("Setting schema search path for demonstration")
        sql = f"""set search_path = __demo, public;"""
        execute(conn, sql)
    except Exception as e:
        conn.rollback()
        raise e
    else:
        conn.commit()


def teardown_demo(conn):
    LINFO("TEARDOWN START")
    LINFO(f"Revoking permissions from {UNPRIV_USER} user")
    try:
        execute(conn, f"revoke all on function __demo.pg_stat_statements() from {UNPRIV_USER};")
    except Exception:
        conn.rollback()

    try:
        execute(conn, f"revoke all on schema __demo from {UNPRIV_USER};")
    except Exception:
        conn.rollback()

    try:
        execute(conn, f"revoke all on database postgres from {UNPRIV_USER};")
    except Exception:
        conn.rollback()

    LINFO("Dropping the __demo schema and all contained objects.")
    sql = """
drop schema if exists __demo cascade;
"""
    execute(conn, sql)

    LINFO(f"Drop user {UNPRIV_USER}")
    execute(conn, f"drop role if exists {UNPRIV_USER};")
    conn.commit()

    sql = """
select extname
  from pg_catalog.pg_extension
 where extname in ('pg_trgm', 'pg_stat_statements');
"""
    for ext in execute(conn, sql).fetchall():
        if ext.extname not in EXTENSIONS:
            LINFO(f"Dropping extension {ext.extname}")
            execute(conn, f"drop extension if exists {ext.extname};")
    
    conn.commit()


def intro_pg_stat_statements():
    print("===========================================")
    print("DEMO OF EXTENSION pg_stat_statements")
    print("===========================================", os.linesep, flush=True)
    infomsg = [
        "This extension provides extra information beyond the runtime of a query.",
        "It also provides information on the min_time, max_time, mean_time, ",
        "total_time, and stddev_time (in milliseconds) for the statement.",
        "It also provides information on the hit count, read count, write count ",
        "and dirtied count for shared blocks, local blocks, and temp blocks. ",
        "On top of that it has the block read time and the block write time and ",
        "the number of times that a distinct query was called."
    ]
    print(os.linesep.join(infomsg), os.linesep)
    infomsg = [
        "So there's a lot of information that can be gathered about performance ",
        "at the query level. This information could help resolve query performance ",
        "issues across all applications that utilize the PostgreSQL engine."
    ]
    print(os.linesep.join(infomsg), os.linesep)
    print("The data in the table can be periodically reset by means of a function {0}"
          "\"pg_stat_statements_reset()\"{0}".format(os.linesep), flush=True)
    print("More information on pg_stat_statements can be found here: https://www.postgresql.org/docs/10/pgstatstatements.html", os.linesep)
    prompt("Press enter to continue.")


def indent(txt, lindent):
    res = []
    for i, t in enumerate(txt.split('\n')):
        if i:
            t = lindent * ' ' + t
        res.append(t)
    return os.linesep.join(res)


def nonestr(x):
    return '' if x is None else x


def format_pss_record(rec):
    return os.linesep.join(f"{k:<19} : {indent(parse_sql(v), 22) if k == 'query' else nonestr(v)}" for k, v in rec._asdict().items())


def print_pss_record(recnum, rec):
    print(50 * '-')
    print(f"{'RECORD':<19} : [{recnum:>4}]")
    print(format_pss_record(rec), flush=True)


def demo_pss_table(conn):
    LINFO("Resetting pg_stat_statements")
    execute(conn, "select pg_stat_statements_reset();").close()
    conn.commit()

    print("Let's run some queries now...")
    prompt("Press enter to continue. ")

    LOG.level = logging.DEBUG
    sql = """
-- QUERY1
select street, count(*)
  from __demo.addr
 where street like '%ain%'
 group 
    by street;
"""
    for _ in range(5):
        execute(conn, sql)
    
    sql = """
-- QUERY2
select city, state, count(*) as "ct"
  from __demo.addr
 where city like '%%PO'
   and (state is null or state != %(st)s)
 group 
    by city, state
having count(*) > %(min_ct)s
 order 
    by "ct" desc,
       state, 
       city;
"""
    execute(conn, sql, {'st': 'EEK', 'min_ct': 2})

    sql = """
-- QUERY3
with house_numbers as (
select split_part(street, ' ', 1)::int as hseno
  from __demo.addr
 where street ~ '^[0-9]+'
)
select distinct hseno
  from house_numbers
 where hseno between 200 and 500;
"""
    for _ in range(3):
        execute(conn, sql)
    LOG.level = logging.INFO

    print("OK, the queries have run, let's see what pg_stat_statements has to say...")
    prompt("Press enter to continue. ")

    res = execute(conn, "select * from public.pg_stat_statements where query ~ '^\\-\\- QUERY[0-9]+' order by query;").fetchall()
    for i, rec in enumerate(res):
        print_pss_record(i + 1, rec)

    conn.rollback()
    print("Note that we did execute \"QUERY1\" 5 times and \"QUERY3\" 3 times after the reset and that the counts and aggregates are cumulative.")
    print("Also note that the queries are parameterized whether variables are used or values directly in the statements", flush=True)
    prompt("Press enter to continue. ")


def create_unpriveleged_user(conn, username, password):
    LINFO(f"Creating unpriveleged user {username}")
    sql = f"""
create user {username} with login nosuperuser nocreatedb nocreaterole noreplication encrypted password %s
"""
    execute(conn, sql, [password])
    sql = f"""
grant connect on database postgres to {username};
"""
    execute(conn, sql)
    sql = f"""
grant usage on schema __demo to {username};
"""
    execute(conn, sql)
    sql = f"""
alter user {username} set search_path = __demo, public;
"""
    execute(conn, sql)
    conn.commit()


def demo_unprivileged_user(conn, init_demo):
    print("Up until now, these queries have been executed by the superuser for this database.")
    print("This will show what happens when an unpriveleged user tries to get data from the table")
    prompt("Press enter to continue. ")

    unpriv_user = UNPRIV_USER
    unpriv_pass = '__monitorpw'
    if init_demo:
        create_unpriveleged_user(conn, unpriv_user, unpriv_pass)
    
    uconn = connect(create_user_db_url(conn, unpriv_user, unpriv_pass))

    try:
        res = execute(uconn, """select * from pg_stat_statements;""").fetchall()
        for i, rec in enumerate(res):
            print_pss_record(i + 1, rec)

        print("Note that the queries are masked. This is because of security that PostgreSQL imposes.")
        print("We can get past this with a function call that will query the pg_stat_statements table")
        print("with superuser priveleges when executed by any user. ")
        print("This function must be defined by a superuser or this will fail.", flush=True)
        prompt("Press enter to continue. ")

        sql = """
create or replace function __demo.pg_stat_statements() returns setof public.pg_stat_statements as $$
begin
    return query select *
                   from public.pg_stat_statements;
end;
$$ language plpgsql security definer;
"""
        LINFO("Creating __demo.pg_stat_statements() function")
        LOG.level = logging.DEBUG
        execute(conn, sql)
        execute(conn, f"""grant execute on function __demo.pg_stat_statements() to {unpriv_user};""")
        conn.commit()

        print("Note that we are using the unprivileged user's connection here.")
        res = execute(uconn, "select current_user;").fetchone()
        LINFO(f"Connection user is {res.current_user}")
        print("Now let's utilize the pg_stat_statements() function call as this user.")
        prompt("Press enter to continue. ")

        res = execute(uconn, """select * from __demo.pg_stat_statements();""").fetchall()
        LOG.level = logging.INFO
        for i, rec in enumerate(res):
            print_pss_record(i + 1, rec)
        
        print("Note that we can now see queries since the function is executing with the priveleges of the definer." ,flush=True)
        prompt("Press enter to continue. ")
    finally:
        uconn.rollback()
        uconn.close()
    conn.rollback()


def demo_pg_stat_statements(conn, init_demo):
    intro_pg_stat_statements()
    demo_pss_table(conn)
    demo_unprivileged_user(conn, init_demo)


def create_addr_table(conn):
    LINFO("Creating a simple address table:")
    sql = """
create table __demo.addr (
    addr_id serial primary key,
    street text not null,
    city text not null,
    state text not null,
    zipcode text not null
);
"""
    LOG.level = logging.DEBUG
    execute(conn, sql)
    LOG.level = logging.INFO
    conn.commit()


def parse_csz(csz):
    parts = csz.split(' ')
    zipcode = parts[-1]
    state = parts[-2]
    if parts[-3].endswith(','):
        parts[-3] = parts[-3][:-1]
    city = ' '.join(parts[:-2])

    return (city, state, zipcode)


def create_addr_data(datafile):
    max_recs = 100000
    curr_rec = 0
    LINFO("Creating address data...")
    while curr_rec < max_recs:
        curr_rec += 1
        street, csz = FAKER.address().split(os.linesep)
        city, state, zipcode = parse_csz(csz)
        copy_null = '\\N'
        addr_rec = [copy_null if part is None else part for part in (street, city, state, zipcode)]
        try:
            print('\t'.join(addr_rec), file=datafile)
        except Exception as e:
            print(addr_rec, file=sys.stderr)
            raise e
    
    datafile.seek(0)

    return datafile


def copy_addr_data(conn, data):
    cur = conn.cursor()
    LINFO("Copy data to addr table...")
    cur.copy_from(data, '__demo.addr', columns=['street' ,'city', 'state', 'zipcode'])
    conn.commit()


def load_addr_table(conn):
    LINFO("Populating table with 1000000 records...")
    max_copy = 10
    curr_copy = 0
    while curr_copy < max_copy:
        curr_copy += 1
        datafile = TemporaryFile(mode="w+t", newline=os.linesep)
        copy_addr_data(conn, create_addr_data(datafile))
        datafile.close()
    
    conn.commit()


def init_pg_trgm(conn):
    try:
        create_addr_table(conn)
        load_addr_table(conn)
    except Exception as e:
        conn.rollback()
        raise e
    else:
        conn.autocommit = True
        execute(conn, "analyze __demo.addr;")
        conn.autocommit = False
        prompt("Initialization complete. Press enter to continue. ")


def create_addr_btree_index(conn):
    sql = """
create index if not exists ix_addr_street on __demo.addr (street text_pattern_ops);
"""
    LOG.level = logging.DEBUG
    execute(conn, sql)
    conn.commit()
    LOG.level = logging.INFO
    conn.autocommit = True
    execute(conn, "analyze __demo.addr;")
    conn.autocommit = False


def test_addr_btree_index(conn):
    sql = """
select street, count(*)
  from __demo.addr
 where street like '10%'
 group 
    by street;
"""
    cur = conn.cursor()
    try:
        LINFO("Let's run this query:")
        LINFO(mogrify_sql(cur, sql))
        cur.execute("EXPLAIN ANALYZE " + sql)
        for plan_row in cur.fetchall():
            LINFO(plan_row.QUERY_PLAN)
    finally:
        cur.close()

    LINFO("You should see that the \"ix_addr_street\" index was utilized.")
    prompt("Press enter to continue. ")

    sql = """
select street, count(*)
  from __demo.addr
 where street like '%ain%'
 group 
    by street;
"""
    cur = conn.cursor()
    try:
        LINFO("Now, let's run this query:")
        LINFO(mogrify_sql(cur, sql))
        cur.execute("EXPLAIN ANALYZE " + sql)
        for plan_row in cur.fetchall():
            LINFO(plan_row.QUERY_PLAN)
    finally:
        cur.close()
    
    conn.rollback()
    LINFO("You should see that the \"ix_addr_street\" index was NOT utilized.")
    prompt("Press enter to continue. ")


def create_addr_gin_index(conn):
    sql = """
create index if not exists ix_addr_street_gin on __demo.addr using gin (street gin_trgm_ops);
"""
    LOG.level = logging.DEBUG
    execute(conn, sql)
    conn.commit()
    LOG.level = logging.INFO
    conn.autocommit = True
    execute(conn, "analyze __demo.addr;")
    conn.autocommit = False


def test_addr_gin_index(conn):
    sql = """
select street, count(*)
  from __demo.addr
 where street like '10%'
 group 
    by street;
"""
    cur = conn.cursor()
    try:
        LINFO("Let's run this query:")
        LINFO(mogrify_sql(cur, sql))
        cur.execute("EXPLAIN ANALYZE " + sql)
        for plan_row in cur.fetchall():
            LINFO(plan_row.QUERY_PLAN)
    finally:
        cur.close()

    LINFO("Note the index usage and execution time.")
    prompt("Press enter to continue. ")

    sql = """
select street, count(*)
  from __demo.addr
 where street like '%ain%'
 group 
    by street;
"""
    cur = conn.cursor()
    try:
        LINFO("Now, let's run this query:")
        LINFO(mogrify_sql(cur, sql))
        cur.execute("EXPLAIN ANALYZE " + sql)
        for plan_row in cur.fetchall():
            LINFO(plan_row.QUERY_PLAN)
    finally:
        cur.close()

    conn.rollback()
    LINFO("You should see that the \"ix_addr_street_gin\" index was utilized.")
    prompt("Press enter to continue. ")


def intro_pg_trgm():
    infomsg = [
        "pg_trgm is an extension that will break up text into a series of trigrams.",
        "The extension has operators for GIN or GIST indexes that will allow LIKE ",
        "operators to utilize indexes.",
        "See https://www.postgresql.org/docs/10/pgtrgm.html",
        "for more information.",
    ]
    print(os.linesep.join(infomsg), flush=True)
    prompt("Press enter to continue.")


def demo_pg_trgm(conn, init_demo):
    print("===========================================")
    print("DEMO OF EXTENSION pg_trgm")
    print("===========================================", os.linesep, flush=True)

    intro_pg_trgm()
    if init_demo:
        init_pg_trgm(conn)

    print("Use of the operator \"LIKE\" will not use an index without a special index type with special operations")
    print("Even a btree index with text_pattern_ops will not handle all LIKE cases. In fact, this will be utilized only with a startswith search (col LIKE 'ABCD%')")
    print("This can be helped with GIN indexes using pg_trgm_ops")
    print("With our addr table loaded with data, lets set a btree index on the street column.", os.linesep, flush=True)
    prompt("Press enter to continue. ")
    
    create_addr_btree_index(conn)
    test_addr_btree_index(conn)

    print("Now let's try the same query but first, we'll create a GIN index with pg_trgm_ops", flush=True)

    prompt("Press enter to continue. ")

    create_addr_gin_index(conn)
    test_addr_gin_index(conn)

    print("The caveat here is that this only works with search terms of at least three bytes.")
    print("This is a more simplistic way of partial matching with (col LIKE '%adsf%') without the expensive full-text search operations.")
    print("Further context can be found here: https://www.2ndquadrant.com/en/blog/text-search-strategies-in-postgresql/", flush=True)

    prompt("Press enter to continue. ")


def check_existing_extensions(conn):
    sql = """
select extname 
  from pg_catalog.pg_extension;
"""
    for rec in execute(conn, sql).fetchall():
        EXTENSIONS.add(rec.extname)
    conn.rollback()


def validate_demo(conn):
    if check_preload_library(conn, 'pg_stat_statements'):
        check_existing_extensions(conn)
        return True
    else:
        LERROR("The \"pg_stat_statemetns\" shared preload library is not present. This demo cannot run. Please restart the database engine with this library loaded.")
        return False


def run_demo(db_url, init_demo=True, teardown=True, block_demo=False):
    LINFO("Demo starting")
    with connect(db_url) as conn:
        try:
            if not block_demo and validate_demo(conn):
                if init_demo:
                    setup_demo(conn)
                demo_pg_trgm(conn, init_demo)
                demo_pg_stat_statements(conn, init_demo)
        finally:
            if teardown or block_demo:
                teardown_demo(conn)
            LINFO("Demo complete.")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', type=str, dest='db_url', required=True, metavar='DB_URL', help="Database connection url")
    parser.add_argument('--no-init', action='store_false', dest='init_demo', required=False, default=True, help="Don't initialize demo. (Default=initialize)")
    mxgrp = parser.add_mutually_exclusive_group(required=False)
    mxgrp.add_argument('--no-teardown', action='store_false', dest='teardown', required=False, default=True, help="Don't teardown demo. (Default=teardown)")
    mxgrp.add_argument('--only-teardown', action='store_true', dest='block_demo', required=False, default=False, help="Only run teardown. (Default=Run demo)")
    args = parser.parse_args()
    try:
        run_demo(args.db_url, 
                 init_demo=args.init_demo, 
                 teardown=args.teardown, 
                 block_demo=args.block_demo)
    except KeyboardInterrupt:
        print("Script interrupted by user.", file=sys.stderr)


