import utils
import schemas_spark
import utils_spark

from geopy.geocoders import Nominatim


import pandas as pd
import pyarrow
import logging

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType



def process_streams(df):

    '''
    Drops duplicates and convert to datetime for each dataframe
    :param dfs: each dataframe to be processed
    :return: processed dataframes
    '''
    df = df.dropDuplicates()
    date_columns = ["timestart", "timeend", "updatedat"]
    for column in date_columns:
        df = df.withColumn(column, F.to_utc_timestamp(F.col(column), "Z"))
    return df


def select_latest_update(df):
    df.createOrReplaceTempView("updateSreams")
    df = spark.sql('''
        SELECT 
            *
        FROM (
            SELECT 
                *,
                dense_rank() OVER (PARTITION BY _id ORDER BY updatedat DESC) AS rank
            FROM updateSreams
        ) vo WHERE rank = 1

            ''')
    df = df.drop("rank")
    df = df.dropDuplicates(["_id", "updatedat"])
    return df


def extract_coordinate_columns(df):
    '''
    Given the places dataframe this method gets the latitud as longitud coordinates by their index in the array column "coordinates"
    :param df: places dataframe
    :return: places dataframe containing the latitud and longitud coordinates in separate columns.
    '''
    df = df.withColumn("longitude", F.element_at(F.col("event_location_coordinates"), 1)) \
            .withColumn("latitude", F.element_at(F.col("event_location_coordinates"), 2)).drop("event_location_coordinates")
    return df





@F.pandas_udf(StringType())
def remove_brackets(organizers_series: pd.Series) -> pd.Series:
    results = organizers_series.str.replace("[", "")
    results = results.str.replace("]", "")
    results = results.str.replace("'", "")
    return results


def get_city_state_country(full_address, key):
    if key == "city":
        if "city" not in full_address["address"].keys():
            city = ""
        else:
            city = full_address["address"]["city"]
        return city
    elif key == "state":
        if "state" not in full_address["address"].keys():
            state = ""
        else:
            state = full_address["address"]["state"]
        return state
    else:
        if "country" not in full_address["address"].keys():
            country = ""
        else:
            country = full_address["address"]["country"]
        return country




@F.pandas_udf("field_one string, city string, state string, country string")
def get_all_from_coordinates(latitude_series: pd.Series, longitude_series: pd.Series, df: pd.DataFrame) -> pd.DataFrame:
    locator = Nominatim(timeout=100, user_agent="myGeocoder")
    countries = []
    states = []
    cities = []
    longitude_list = longitude_series.tolist()
    latitude_list = latitude_series.tolist()
    for long, lat in zip(longitude_list, latitude_list):
        coordinates = (lat,long)
        location = locator.reverse(coordinates)
        if location is not None:
            full_address = location.raw
            country = get_city_state_country(full_address, "country")
            countries.append(country)
            state = get_city_state_country(full_address, "state")
            states.append(state)
            city = get_city_state_country(full_address, "city")
            cities.append(city)
        else:
            city = ""
            cities.append(city)
            state = ""
            states.append(state)
            country = ""
            countries.append(country)
    df["country"] = countries
    df["state"] = states
    df["city"] = cities
    return df



def keep_events_with_coordinates(df):
    df = df.filter(
        (F.col("longitude").isNotNull()) | (F.col("longitude") != "nan") | (F.col("longitude") != 0.0) | (
                    F.col("longitude") != "NaN") | (F.col("longitude") != "NAN"))
    df = df.filter(
        (F.col("latitude").isNotNull()) | (F.col("latitude") != "nan") | (F.col("latitude") != 0.0) | (
                    F.col("latitude") != "NaN") | (F.col("latitude") != "NAN"))
    return df


def keep_events_wo_coordinates(df):
    df = df.filter(
        (F.col("longitude").isNull()) | (F.col("longitude") == "nan") | (F.col("longitude") == 0.0) | (
                    F.col("longitude") == "NaN") | (F.col("longitude") == "NAN"))
    df = df.filter(
        (F.col("latitude").isNull()) | (F.col("latitude") == "nan") | (F.col("latitude") == 0.0) | (
                    F.col("latitude") == "NaN") | (F.col("latitude") == "NAN"))
    return df



def main():

    #get dataframes
    events_inserts_cs, events_inserts_bson = utils_spark.get_dataframes(events_inserts_path, events_bson_path, schema=raw_events_schema,
                                                                                        partition_number=4)

    #concatenate dataframes from events insert, update and existing events
    events_df = utils_spark.concatenate_dataframes(events_inserts_cs, events_inserts_bson)

    #drop duplicates
    events_df = events_df.dropDuplicates()

    #convert ISO date strings into datetime objects
    events_df = utils_spark.convert_to_timestamp("timestart", "timeend", "updatedat", df=events_df)

    #use spark sql to select the latest update within the update streams dataframex
    events_df = select_latest_update(events_df)

    #extract year, month, day an hour in order to create partitions
    events_df = utils_spark.create_partition_columns(events_df, "timestart")

    #rename columns
    events_df = utils_spark.rename(events_df, columns)

    #create struct from attendees
    events_df = utils_spark.create_struct("attendees", df=events_df, columns=struct_columns)

    #remove brackets from organizers string
    events_df = events_df.withColumn("organizers", remove_brackets("organizers"))

    #extract longitude and latitude into their own columns
    events_df = extract_coordinate_columns(events_df)

    #split dataframe into two dataframes: with/without coordinates
    events_with_coordinates = keep_events_with_coordinates(events_df)
    events_wo_coordinates = keep_events_wo_coordinates(events_df)

    #add empty fields country, state and city to those events without coordinates so we can concatenate them after reversing coordinates in the other df
    events_wo_coordinates = utils_spark.add_empty_string_columns("country", "state", "city", df=events_wo_coordinates)

    #extract city, state and country to those events with coordinates
    events_with_coordinates = events_with_coordinates.withColumn("field_one", F.lit("1")).withColumn("struct", F.struct(F.col("field_one"))) \
        .withColumn("struct_one",
                    get_all_from_coordinates(F.col("latitude"), F.col("longitude"), F.col("struct")))

    events_with_coordinates = events_with_coordinates.withColumn("country", F.col("struct_one.country"))\
                         .withColumn("state", F.col("struct_one.state"))\
                         .withColumn("city", F.col("struct_one.city")).drop("struct_one", "struct", "field_one")

    #now we concatenate both dataframes
    events_df = events_with_coordinates.unionByName(events_wo_coordinates)

    #convert any null or nan value to empty string
    events_df = utils_spark.clean_string_columns(events_df)

    #convert all nulls to False for ambassador_created
    events_df = events_df.withColumn("ambassador_created", F.when(F.col("ambassador_created").isNull(), False).otherwise(F.col("ambassador_created")))

    #drop duplicates before writing to storage
    events_df = events_df.dropDuplicates()

    #write to storage
    utils.data_interceptor.write_parquet(events_df, final_path, partition_columns=partition_columns)



if __name__ == "__main__":

    #logging configuration
    logging.basicConfig(level=logging.INFO,
                         format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                         datefmt='%Y-%m-%d:%H:%M:%S')


    #set logger and spark parameters
    utils.data_interceptor.set_logger(logging)
    utils.data_interceptor.set_spark_conf()
    utils.data_interceptor.set_spark_session()

    #get spark session and context
    spark = utils.data_interceptor.get_spark_session()
    sc = utils.data_interceptor.get_spark_context()

    #required schemas
    raw_events_schema = schemas_spark.get_schema("raw_events_schema")


    #buckets
    inserts_bucket = "data-mongodb-events-cs"
    events_cleaned = "data-mongodb-events-processed"

    #required paths
    #events
    events_inserts_path = f"s3://{inserts_bucket}/*/*/*/*/"
    events_bson_path = f"s3://{inserts_bucket}/2021/01/" #existing events in MongoDB

    final_path = f"s3://{events_cleaned}/"

    #delete files in target directory
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI(final_path), sc._jsc.hadoopConfiguration())
    file_status = fs.globStatus(Path("/*"))
    for status in file_status:
        fs.delete(status.getPath(), True)


    #partition columns
    partition_columns = ["year", "month", "day", "hour"]
    struct_columns = ["invited", "going", "interested"]

    #columns to rename
    columns = {"timestart": "time_start", "timeend":"time_end",
               "updatedat":"updated_at", "invitedattendees": "invited",
               "interestedattendees": "interested", "goingattendees":"going"}


    main()