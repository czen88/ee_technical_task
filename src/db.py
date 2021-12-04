import sqlite3 as sql

import pandas as pd


class Database:
    """
    Contains utilities required for SQLite database interactions
    """

    def __init__(self, url):
        """
        Creates database connection

        Parameters:
        url (string): Database URL
        """
        self.url = url
        self.conn = sql.connect(url)

    def to_sql(self, df, table, if_exists='append'):
        """
        Wrapper for Pandas to_sql() method to store DataFrame in database table

        Parameters:
        df (DataFrame):     Pandas DataFrame
        table (str):        Table name
        if_exists (str):    What to do if table exists. Append data by default.
        """
        df.to_sql(table, self.conn, if_exists=if_exists, index=False)

    def read_sql(self, s):
        """
        Wrapper for Pandas read_sql() method to execute SQL and return Pandas DataFrame as result

        Parameters:
        s (str):    SQL to execute

        Returns
        -------
        DataFrame   Result dataset for given SQL
        """
        return pd.read_sql(s, self.conn)

    def execute(self, s):
        """
        Executes given SQL on database. Normally used for DDL execution.

        Parameters:
        s (str):    SQL to execute
        """
        cur = self.conn.cursor()
        cur.execute(s)


class Posts:
    """
    Used to create Posts table
    """

    def __init__(self, db):
        """
        Set Database Instance

        Parameters:
        db (Database): Database Instance
        """
        self.db = db

    def create(self):
        """
        Recreates posts table in database.
        """
        self.db.execute('DROP TABLE IF EXISTS posts')
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS posts
            (
                Id                    INTEGER PRIMARY KEY,
                PostTypeId            INTEGER NOT NULL,
                AcceptedAnswerId      INTEGER,
                CreationDate          TIMESTAMP,
                Score                 INTEGER NOT NULL,
                ViewCount             INTEGER,
                Body                  TEXT NOT NULL,
                OwnerUserId           INTEGER,
                LastEditorUserId      INTEGER,
                LastEditDate          TIMESTAMP,
                LastActivityDate      TIMESTAMP NOT NULL,
                Title                 TEXT,
                Tags                  TEXT,
                AnswerCount           INTEGER,
                CommentCount          INTEGER NOT NULL,
                FavoriteCount         INTEGER,
                ContentLicense        TEXT NOT NULL,
                ParentId              INTEGER,
                ClosedDate            TIMESTAMP,
                CommunityOwnedDate    TIMESTAMP,
                LastEditorDisplayName TEXT,
                OwnerDisplayName      TEXT,
                FOREIGN KEY (AcceptedAnswerId) REFERENCES posts (Id),
                FOREIGN KEY (ParentId) REFERENCES posts (Id)
            )
        ''')


class Tags:
    """
    Used to create Tags table
    """

    def __init__(self, db):
        """
        Set Database Instance

        Parameters:
        db (Database): Database Instance
        """
        self.db = db

    def create(self):
        """
        Recreates tags table in database.
        """
        self.db.execute('DROP TABLE IF EXISTS tags')
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS tags
            (
                Id            INTEGER PRIMARY KEY,
                TagName       TEXT UNIQUE NOT NULL,
                Count         INTEGER NOT NULL,
                ExcerptPostId INTEGER,
                WikiPostId    INTEGER,
                FOREIGN KEY (ExcerptPostId) REFERENCES posts (Id),
                FOREIGN KEY (WikiPostId) REFERENCES posts (Id)
           )
        ''')


class PostsTags:
    """
    Used to create posts_tags table
    """

    def __init__(self, db):
        """
        Set Database Instance

        Parameters:
        db (Database): Database Instance
        """
        self.db = db

    def create(self):
        """
        Recreates posts_tags table in database.
        """
        self.db.execute('DROP TABLE IF EXISTS posts_tags')
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS posts_tags
            (
                PostId  INTEGER NOT NULL,
                TagId   INTEGER NOT NULL,
                TagName TEXT NOT NULL,
                FOREIGN KEY (PostId) REFERENCES posts (Id),
                FOREIGN KEY (TagId) REFERENCES tags (Id),
                UNIQUE(PostId,TagId)
            )
        ''')
