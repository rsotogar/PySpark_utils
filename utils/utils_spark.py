import utils
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


def concatenate_dataframes(df1,df2):
    '''
    Performs a union by name in order to stack two dataframes one on top of the other
    :return: spark dataframe
    '''
    new_df = df1.unionByName(df2)
    return new_df


def get_dataframes(*paths, schema, partition_number):
    '''
    :param paths: each path to read data from. The data contain in these paths must share the same schema
    :return: imported dataframes, cached in memory
    '''
    dataframes = []

    for path in paths:
        df = utils.data_interceptor.read_parquet(path, schema, partition_number=partition_number)
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
        df = df.withColumn(column, F.when(F.col(column) == "nan", "").when(F.col(column).isNull(), "")\
                                    .when(F.col(column) == " ", "").otherwise(F.col(column)))
    return df


def add_empty_string_columns(*columns, df):
    for column in columns:
        df = df.withColumn(column, F.lit("").cast(StringType()))
    return df



