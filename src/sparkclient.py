import os

import pandas as pd
import pyspark.sql.functions as f
from pydeequ import deequ_maven_coord, f2j_maven_coord
from pydeequ.checks import Check, CheckLevel
from pydeequ.suggestions import DEFAULT, ConstraintSuggestionRunner
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import SparkSession


class SparkClient:
    """
    Contains utilities required for interaction with Spark
    """

    def __init__(self, app_name, path='uncommitted'):
        """
        Initialises environment and creates Spark Session

        Parameters:
        app_name (string): Spark session name
        path (string):     Path to store output datasets
        """
        self.path = path
        self.packages = ['com.databricks:spark-xml_2.12:0.12.0', deequ_maven_coord]
        os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {",".join(self.packages)} pyspark-shell'
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.jars.excludes", f2j_maven_coord)
            .getOrCreate()
        )

    def stop(self):
        """
        Stops Spark Session
        """
        try:
            # Required for pydeequ checks
            self.spark.sparkContext._gateway.shutdown_callback_server()
        except Exception:
            print('Cannot stop callback server:')
        finally:
            self.spark.stop()

    def parse_xml(self, url, root_tag, row_tag, mode='FAILFAST'):
        """
        Loads XML into Spark DataFrame using spark-xml library

        Parameters:
        url (string):      XML file path
        root_tag (string): Root tag for XML file
        row_tag (string):  Row tag for data
        mode (string):     What to do in case of wrong XML structure. Default is fail with error.

        Returns
        -------
        DataFrame          Spark Dataframe with XML data
        """
        # Pandas can read max 1GB of XML so we are using spark-xml instead
        # return pd.read_xml(path, parser='etree')
        df = (
            self.spark.read.format('xml')
                .option('mode', mode)
                .option('rootTag', root_tag)
                .option('rowTag', row_tag)
                .load(url)
        )

        # Remove "_" prefix from columns
        new_column_names = [c[1:] for c in df.columns]
        df = df.toDF(*new_column_names)
        return df

    def suggest_constraints(self, df):
        """
        Generate suggested validation checks based on Spark DataFrame data using Pydeequ.
        Prints code for validation checks

        Parameters:
        df (DataFrame):    Spark DataFrame
        """
        suggestion_result = (
            ConstraintSuggestionRunner(self.spark)
            .onData(df)
            .addConstraintRule(DEFAULT())
            .run()
        )

        # Print suggested constraints
        for i, r in pd.json_normalize(suggestion_result['constraint_suggestions'])[['code_for_constraint']].iterrows():
            print(r['code_for_constraint'])

    def check_result_to_df(self, check_result):
        """
        Converts Pydeequ check results into Spark DataFrame

        Parameters:
        check_result:   The results of the verification run

        Returns
        -------
        DataFrame        Spark Dataframe
        """
        return VerificationResult.checkResultsAsDataFrame(self.spark, check_result)

    def write_parquet(self, df, name):
        """
        Stores DataFrame in Parquet file using path set for SparkClient

        Parameters:
        df (string):    Spark DataFrame
        name (string):  Filename
        """
        df.write.mode('overwrite').parquet(self.path + os.sep + name + '.parquet')


class Posts:
    """
    Loads and validates Posts XML
    """

    def __init__(self, sc, xml):
        """
        Initialises Spark Session and Loads Posts XML

        Parameters:
        sc (SparkSession): SparkSession instance
        xml (string):      Path for Posts XML
        """
        self.sc = sc
        self.spark = sc.spark
        self.df = sc.parse_xml(self.sc.path + os.sep + xml, 'posts', 'row')

    def check(self):
        """
        Runs Validation Checks for Posts

        Returns
        -------
        DataFrame        Spark DataFrame with check results
        """
        check = Check(self.spark, CheckLevel.Warning, "Posts Check")

        check_result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check
                      .hasSize(lambda x: x >= 20000)
                      .isUnique("Id")
                      .isComplete("Id")
                      .isNonNegative("Id")
                      .isNonNegative("AcceptedAnswerId")
                      .isNonNegative("AnswerCount")
                      .hasCompleteness("AnswerCount", lambda x: x >= 0.44, "It should be above 0.44!")
                      .isComplete("Body")
                      .isComplete("LastActivityDate")
                      .hasCompleteness("Title", lambda x: x >= 0.44, "It should be above 0.44!")
                      .isNonNegative("ParentId")
                      .hasCompleteness("ParentId", lambda x: x >= 0.45, "It should be above 0.45!")
                      .hasCompleteness("LastEditDate", lambda x: x >= 0.59, "It should be above 0.59!")
                      .hasCompleteness("LastEditorUserId", lambda x: x >= 0.58, "It should be above 0.58!")
                      .isContainedIn("PostTypeId", ["2", "1", "5", "4", "6", "7"])
                      .isComplete("PostTypeId")
                      .isNonNegative("ViewCount")
                      .hasCompleteness("ViewCount", lambda x: x >= 0.44, "It should be above 0.44!")
                      .isComplete("Score")
                      .hasCompleteness("Tags", lambda x: x >= 0.44, "It should be above 0.44!")
                      .hasCompleteness("OwnerUserId", lambda x: x >= 0.98, "It should be above 0.98!")
                      .isComplete("CommentCount")
                      .isNonNegative("CommentCount")
                      .isNonNegative("FavoriteCount")
                      .isContainedIn("ContentLicense", ["CC BY-SA 4.0", "CC BY-SA 3.0"])
                      .isComplete("ContentLicense")
                      )
            .run()
        )

        return self.sc.check_result_to_df(check_result)

    def save(self):
        """
        Stores Posts in Parquet file
        """
        self.sc.write_parquet(self.df, 'posts')


class Tags:
    """
    Loads and validates Tags XML
    """

    def __init__(self, sc, xml):
        """
        Initialises Spark Session and Loads Tags XML

        Parameters:
        sc (SparkSession): SparkSession instance
        xml (string):      Path for Tags XML
        """
        self.sc = sc
        self.spark = sc.spark
        self.df = sc.parse_xml(self.sc.path + os.sep + xml, 'tags', 'row')

    def check(self):
        """
        Runs Validation Checks for Tags

        Returns
        -------
        DataFrame        Spark DataFrame with check results
        """
        check = Check(self.spark, CheckLevel.Warning, "Tags Check")

        check_result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check
                      .hasSize(lambda x: x >= 900)
                      .isComplete("Id")
                      .isNonNegative("Id")
                      .isUnique("Id")
                      .isNonNegative("ExcerptPostId")
                      .hasCompleteness("ExcerptPostId", lambda x: x >= 0.77, "It should be above 0.77!")
                      .isComplete("TagName")
                      .isUnique("TagName")
                      .isNonNegative("WikiPostId")
                      .hasCompleteness("WikiPostId", lambda x: x >= 0.77, "It should be above 0.77!")
                      .isComplete("Count")
                      .isNonNegative("Count")
                      )
            .run()
        )

        return self.sc.check_result_to_df(check_result)

    def save(self):
        """
        Stores Tags in Parquet file
        """
        self.sc.write_parquet(self.df, 'tags')


class PostsTags:
    """
    Generates posts_tags table based on Tags column in Tags dataset
    """

    def __init__(self, sc, posts_df, tags_df):
        """
        Initialises Spark Session and generates posts_tags dataset using posts and tags datasets

        Parameters:
        sc (SparkSession):         SparkSession instance
        posts_df (DataFrame):      Posts Spark dataframe
        tags_df (DataFrame):       Tags Spark dataframe

        Returns
        -------
        DataFrame                  Spark DataFrame with following columns:
        PostId:                    Id of the post
        TagId:                     Id of the tag assigned to the post
        TagName:                   Name of the tag
        """
        self.sc = sc
        self.spark = sc.spark
        self.df = (
            posts_df.alias('p')
            .join(
                tags_df.alias('t'),
                f.col('p.Tags').contains(
                    f.concat(f.lit('<'), f.col('t.TagName'), f.lit('>'))),
                how='inner'
            )
            .select(
                f.col('p.Id').alias('PostId'),
                f.col('t.Id').alias('TagId'),
                't.TagName'
            )
        )

    def check(self):
        """
        Runs Validation Checks for posts-tags

        Returns
        -------
        DataFrame        Spark DataFrame with check results
        """
        check = Check(self.spark, CheckLevel.Warning, "Posts Tags Check")

        check_result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check
                      .hasSize(lambda x: x >= 30000)
                      .isComplete("PostId")
                      .isNonNegative("PostId")
                      .isComplete("TagName")
                      .isComplete("TagId")
                      .isNonNegative("TagId")
                      )
            .run()
        )

        return self.sc.check_result_to_df(check_result)

    def save(self):
        """
        Stores posts_tags in Parquet file
        """
        self.sc.write_parquet(self.df, 'posts_tags')
