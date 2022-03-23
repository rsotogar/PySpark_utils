from utils import data_interceptor
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S')


def rename(df, columns):
    '''
    Pandas like renaming method, but for spark dataframes
    :param df: a spark dataframe
    :param columns: a dictionary containing the old column names as keys and the new column names as values
    :return: a spark dataframe with renamed columns
    '''
    if isinstance(columns, dict):
        for old_column, new_column in columns.items():
            df = df.withColumnRenamed(old_column, new_column)
    else:
        logging.info("Incorrect type error. Columns argument should be a dict")
        raise TypeError
    return df


def create_struct(struct_column_name, df, columns):
    '''

    :param struct_column_name: the name of the struct column
    :param df: the dataframe in which to create the struct
    :param columns: a list of columns from which to build the struct
    :return: spark dataframe
    '''
    df = df.withColumn(struct_column_name, F.struct(columns))
    for column in columns:
        df = df.drop(column)
    return df


def concatenate_dataframes(df1, df2):
    """
    Performs a union by name in order to stack two dataframes one on top of the other
    :return: spark dataframe
    """
    new_df = df1.unionByName(df2)
    return new_df


def get_dataframes(*paths, schema, partition_number):
    """
    :param partition_number: the number of partitions to be created from
    :param schema:
    :param paths: each path to read data from. The data contain in these paths must share the same schema
    :return: imported dataframes, cached in memory
    """
    dataframes = []

    for path in paths:
        df = data_interceptor.read_parquet(path, schema, partition_number=partition_number)
        dataframes.append(df)
    return dataframes


def convert_to_timestamp(*columns_to_timestamp, df):
    for column in columns_to_timestamp:
        df = df.withColumn(column, F.to_utc_timestamp(F.col(column), "Z"))
    return df


def create_partition_columns(df, timestamp_column):
    df = df.withColumn("year", F.year(timestamp_column)).withColumn("month", F.month(timestamp_column)) \
        .withColumn("day", F.dayofmonth(timestamp_column)).withColumn("hour", F.hour(timestamp_column))
    return df


def clean_string_columns(df):
    string_columns = [i[0] for i in df.dtypes if i[1].startswith("string")]
    for column in string_columns:
        df = df.withColumn(column, F.when(F.col(column) == "nan", "").when(F.col(column).isNull(), "") \
                           .when(F.col(column) == " ", "").otherwise(F.col(column)))
    return df


def add_empty_string_columns(*columns, df):
    for column in columns:
        df = df.withColumn(column, F.lit("").cast(StringType()))
    return df


def remove_null_values(df):
    boolean_columns = [i[0] for i in df.dtypes if i[1].startswith("boolean")]
    if len(boolean_columns) >= 1:
        non_boolean_columns = [column for column in df.columns if column not in boolean_columns]
        for column in non_boolean_columns:
            df = df.filter(F.col(column).isNotNull())
    else:
        for column in df.columns:
            df = df.filter(F.col(column).isNotNull())
    return df


def remove_duplicates(df):
    logging.info("Finding duplicates in dataframe...")
    potential_duplicates = df.groupBy(df.columns).count().filter("count >= 2")
    if bool(potential_duplicates.head(1)):
        logging.info("Duplicate rows found. Removing duplicates")
        df = df.dropDuplicates()
    else:
        logging.info("No duplicates found")
    return df


def null_to_false(df):
    boolean_columns = [i[0] for i in df.dtypes if i[1].startswith("boolean")]
    if len(boolean_columns) >= 1:
        logging.info("Converting null values to False in boolean columns")
        for column in boolean_columns:
            df = df.withColumn(column, F.when(F.col(column).isNull(), False).otherwise(F.col(column)))
    else:
        logging.info("There are no boolean columns in the dataframe")
    return df
