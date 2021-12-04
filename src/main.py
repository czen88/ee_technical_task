# Entrypoint to populate the database
import os
import sys

import db as sql
import sparkclient as scl


def main(path='uncommitted'):
    """
    Entrypoint for XML data load

    Parameters:
    path (str):    Folder where XML files are located. Also used for Parquet files and SQLite DB
    """
    # Initialise Spark Session
    sc = scl.SparkClient("XML_Import", path)

    # Load XML into Spark Dataframe
    posts = scl.Posts(sc, 'Posts.xml')
    tags = scl.Tags(sc, 'Tags.xml')

    # Create posts_tags table used to join Posts and Tags
    posts_tags = scl.PostsTags(sc, posts.df, tags.df)

    # Save to Parquet
    posts.save()
    tags.save()
    posts_tags.save()

    # Run Validation Checks
    checks = (
        posts.check()
        .union(tags.check())
        .union(posts_tags.check())
    ).toPandas()

    # Connect to SQLite DB
    db = sql.Database(path + os.sep + 'warehouse.db')

    # Save Check Results
    db.to_sql(checks, 'check_results', if_exists='replace')

    # If any check has failed -> exit with error
    failed = checks[checks['constraint_status'] != 'Success']
    if len(failed) > 0:
        sc.stop()
        sys.exit('Data validation check has failed! See check_results table for more details.')

    # Create tables in SQLite
    sql.Posts(db).create()
    sql.Tags(db).create()
    sql.PostsTags(db).create()

    # Save data in SQLite
    db.to_sql(posts.df.toPandas(), 'posts')
    db.to_sql(tags.df.toPandas(), 'tags')
    db.to_sql(posts_tags.df.toPandas(), 'posts_tags')

    # Stop Spark Session
    sc.stop()

    print('Data Loaded Successfully!')


if __name__ == "__main__":
    main()
