import os

import pandas as pd

import db as sql

db_url = 'test' + os.sep + 'warehouse.db'
db = sql.Database(db_url)


def run_sql(s):
    return db.conn.cursor().execute(s)


def create_test_tab():
    run_sql('DROP TABLE IF EXISTS test')
    run_sql('CREATE TABLE IF NOT EXISTS test AS SELECT 1 as a')


def test_database():
    assert db.url == db_url
    assert list(run_sql('SELECT 1')) == [(1,)]


def test_to_sql():
    create_test_tab()
    df = pd.DataFrame([2], columns=['a'])
    db.to_sql(df, 'test')
    assert list(run_sql('SELECT * FROM test')) == [(1,), (2,)]


def test_read_sql():
    create_test_tab()
    assert db.read_sql('SELECT * FROM test').to_dict() == {'a': {0: 1}}


def test_execute():
    db.execute('DROP TABLE IF EXISTS test_exec')
    db.execute('CREATE TABLE IF NOT EXISTS test_exec AS SELECT 1 as a')
    assert list(run_sql('SELECT * FROM test_exec')) == [(1,)]


def test_create_posts():
    run_sql('DROP TABLE IF EXISTS posts')
    sql.Posts(db).create()
    assert list(run_sql('SELECT * FROM posts')) == []


def test_create_tags():
    run_sql('DROP TABLE IF EXISTS tags')
    sql.Tags(db).create()
    assert list(run_sql('SELECT * FROM tags')) == []


def test_create_posts_tags():
    run_sql('DROP TABLE IF EXISTS posts_tags')
    sql.PostsTags(db).create()
    assert list(run_sql('SELECT * FROM posts_tags')) == []
