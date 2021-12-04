import os

import sparkclient as scl

sc = None


def setup_module():
    global sc
    sc = scl.SparkClient("Test", 'test')


def teardown_module():
    sc.stop()


def test_sparkclient():
    assert sc.path == 'test'
    assert len(sc.packages) == 2
    assert sc.spark.version == '3.0.3'


def test_parse_xml():
    df = sc.parse_xml(sc.path + os.sep + 'Tags.xml', 'tags', 'row').toPandas()
    assert len(df) == 991
    assert list(df.columns) == ['Count', 'ExcerptPostId', 'Id', 'TagName', 'WikiPostId']


def test_posts():
    posts = scl.Posts(sc, 'Posts.xml')
    assert posts.df.count() == 20707
    assert len(posts.df.columns) == 22

    check = posts.check().toPandas()
    assert check.shape == (26, 6)


def test_tags():
    tags = scl.Tags(sc, 'Tags.xml')
    assert tags.df.count() == 991
    assert len(tags.df.columns) == 5

    check = tags.check().toPandas()
    assert check.shape == (12, 6)


def test_posts_tags():
    posts = scl.Posts(sc, 'Posts.xml')
    tags = scl.Tags(sc, 'Tags.xml')
    posts_tags = scl.PostsTags(sc, posts.df, tags.df)
    assert posts_tags.df.count() == 31595
    assert len(posts_tags.df.columns) == 3

    check = posts_tags.check().toPandas()
    assert check.shape == (6, 6)
