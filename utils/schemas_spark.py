from pyspark.sql.types import *


def get_events_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("organizers", StringType(), True),
                         StructField("name", StringType(), True), StructField("timestart", StringType(), True),
                         StructField("timeend", StringType(), True), StructField("updatedat", StringType(), True),
                         StructField("location", StringType(), True),
                         StructField("invitedattendees", ArrayType(StringType(), True)),
                         StructField("interestedattendees", ArrayType(StringType(), True)),
                         StructField("goingattendees", ArrayType(StringType(), True)),
                         StructField("activities", ArrayType(StringType(), True)),
                         StructField("ambassador_created", BooleanType(), True),
                         StructField("event_location_name", StringType(), True),
                         StructField("event_location_label", StringType(), True),
                         StructField("event_location_coordinates", ArrayType(DoubleType()), True),
                         StructField("event_location_address", StringType(), True),
                         StructField("event_location_place_id", StringType(), True),
                         StructField("visibility", StringType(), True), StructField("description", StringType(), True)])
    return schema


def get_users_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("birthdate", StringType(), True),
                         StructField("interests", ArrayType(StructType([StructField("interest", StringType(), True),
                                                                        StructField("private", StringType(), True)]))),
                         StructField("updatedat", StringType(), True), StructField("region", StringType(), True),
                         StructField("timezone", StringType(), True), StructField("name", StringType(), True),
                         StructField("username", StringType(), True), StructField("push_enabled", BooleanType(), True),
                         StructField("createdat", StringType(), True),
                         StructField("marketing_opt_in", BooleanType(), True),
                         StructField("email_verified", BooleanType(), True),
                         StructField("use24hour", BooleanType(), True)])
    return schema


def get_clean_users_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("birthdate", TimestampType(), True),
                         StructField("interests", ArrayType(StructType([StructField("interest", StringType(), True),
                                                                        StructField("private", StringType(), True)]))),
                         StructField("updated_at", TimestampType(), True),
                         StructField("region", StringType(), True), StructField("timezone", IntegerType(), True),
                         StructField("name", StringType(), True), StructField("username", StringType(), True),
                         StructField("created_at", TimestampType(), True), StructField("settings", StructType(
            [StructField("use24hour", BooleanType(), True),
             StructField("notification_settings", StructType([StructField("push_enabled", BooleanType(), True)])),
             StructField("marketing_opt_in", BooleanType(), True),
             StructField("email_verified", BooleanType(), True)])), StructField("year", IntegerType(), True),
                         StructField("month", IntegerType(), True), StructField("day", IntegerType(), True),
                         StructField("hour", IntegerType(), True)])
    return schema


def get_attendances_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("attendee_id", StringType(), True),
                         StructField("event_id", StringType(), True), StructField("updatedat", StringType(), True),
                         StructField("createdat", StringType(), True), StructField("read", BooleanType(), True),
                         StructField("status", StringType(), True), StructField("timestart", StringType(), True)])
    return schema


def get_clean_attendances_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("attendee_id", StringType(), True),
                         StructField("event_id", StringType(), True), StructField("updated_at", TimestampType(), True),
                         StructField("created_at", TimestampType(), True), StructField("read", BooleanType(), True),
                         StructField("status", StringType(), True), StructField("time_start", TimestampType(), True),
                         StructField("year", IntegerType(), True), StructField("month", IntegerType(), True),
                         StructField("day", IntegerType(), True), StructField("hour", IntegerType(), True)])
    return schema


def get_places_schema():
    schema = StructType(
        [StructField("_id", StringType(), True), StructField("longlat_coordinates", ArrayType(FloatType()), True),
         StructField("longlat_type", StringType(), True), StructField("address", StringType(), True),
         StructField("place_id", StringType(), True)])
    return schema


def get_clean_events_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("organizers", StringType(), True),
                         StructField("name", StringType(), True), StructField("time_start", TimestampType(), True),
                         StructField("time_end", TimestampType(), True),
                         StructField("updated_at", TimestampType(), True),
                         StructField("location", StringType(), True),
                         StructField("attendees", StructType([StructField("invited", ArrayType(StringType(), True)),
                                                              StructField("going", ArrayType(StringType(), True)),
                                                              StructField("interested",
                                                                          ArrayType(StringType(), True))])),
                         StructField("year", StringType(), True), StructField("month", StringType(), True),
                         StructField("day", StringType(), True), StructField("hour", StringType(), True),
                         StructField("activities", ArrayType(StringType(), True)),
                         StructField("ambassador_created", BooleanType(), True),
                         StructField("city", StringType(), True), StructField("state", StringType(), True),
                         StructField("country", StringType(), True),
                         StructField("event_location_name", StringType(), True),
                         StructField("event_location_label", StringType(), True),
                         StructField("event_location_address", StringType(), True),
                         StructField("event_location_place_id", StringType(), True),
                         StructField("longitude", DoubleType(), True), StructField("latitude", DoubleType(), True),
                         StructField("visibility", StringType(), True), StructField("description", StringType(), True)])
    return schema


def get_clean_attendances_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("attendee_id", StringType(), True),
                         StructField("event_id", StringType(), True), StructField("updated_at", TimestampType(), True),
                         StructField("created_at", TimestampType(), True), StructField("read", BooleanType(), True),
                         StructField("status", StringType(), True), StructField("time_start", TimestampType(), True)])
    return schema


def get_activities_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("name", StringType(), True),
                         StructField("enabled", BooleanType(), True), StructField("interest", StringType(), True)])
    return schema


def get_interests_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("name", StringType(), True),
                         StructField("interest_group", StringType(), True), StructField("location", StringType(), True),
                         StructField("supported", BooleanType(), True)])
    return schema


def get_raw_downtoclown_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("user", StringType(), True),
                         StructField("timestart", StringType(), True), StructField("timeend", StringType(), True)])
    return schema


def get_clean_downtoclown_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("user", StringType(), True),
                         StructField("time_start", TimestampType(), True),
                         StructField("time_end", TimestampType(), True),
                         StructField("year", IntegerType(), True), StructField("month", IntegerType(), True),
                         StructField("day", IntegerType(), True), StructField("hour", IntegerType(), True)])
    return schema


def get_raw_feeds_schema():
    schema = StructType(
        [StructField("_id", StringType(), True), StructField("event_attendingfriends", ArrayType(StringType()), True),
         StructField("event_attendees", ArrayType(StringType()), True),
         StructField("event_interests", ArrayType(StringType()), True),
         StructField("event_id", StringType(), True), StructField("event_creator", StringType(), True),
         StructField("owner", StringType(), True), StructField("createdat", StringType(), True),
         StructField("time", StringType(), True), StructField("updatetime", StringType(), True),
         StructField("updatedat", StringType(), True)])
    return schema


def get_raw_availabilities_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("availabilitydate", StringType(), True),
                         StructField("user", StringType(), True), StructField("afternoon", BooleanType(), True),
                         StructField("morning", BooleanType(), True), StructField("night", BooleanType(), True)])
    return schema


def get_raw_hpf_events_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("activities", StringType(), True),
                         StructField("activity_ids", StringType(), True), StructField("country", StringType(), True),
                         StructField("description", StringType(), True), StructField("end_date", StringType(), True),
                         StructField("event_id", StringType(), True), StructField("event_name", StringType(), True),
                         StructField("event_status", StringType(), True), StructField("hostname", StringType(), True),
                         StructField("image", StringType(), True), StructField("latitude", DoubleType(), True),
                         StructField("locality", StringType(), True), StructField("longitude", DoubleType(), True),
                         StructField("place", StringType(), True), StructField("postal_code", StringType(), True),
                         StructField("region", StringType(), True), StructField("source_url", StringType(), True),
                         StructField("start_date", StringType(), True), StructField("street_address", StringType(), True),
                         StructField("tickets", StringType(), True), StructField("url", StringType(), True),
                         StructField("event_timestamp", DoubleType(), True), StructField("event_type", StringType(), True),
                         StructField("is_virtual", BooleanType(), True), StructField("keywords", StringType(), True),
                         StructField("image_status", StringType(), True),  StructField("geo_location", StructType([StructField("coordinates", ArrayType(DoubleType(), True)),
                         StructField("type", StringType(), True)])), StructField("image_shape", StringType(), True),
                         StructField("status", StringType(), True), StructField("location_type", StringType(), True),
                         StructField("avg_pixel_intensity", DoubleType(), True),
                         StructField("image_resolution_mp", DoubleType(), True), StructField("colorimetry", StringType(), True),
                         StructField("not_virtual", BooleanType(), True), StructField("has_start_date", BooleanType(), True),
                         StructField("has_start_time", BooleanType(), True), StructField("has_image", BooleanType(), True),
                         StructField("has_geo_location", BooleanType(), True), StructField("has_street_address", BooleanType(), True),
                        StructField("has_coordinates", BooleanType(), True), StructField("in_supported_country", BooleanType(), True),
                        StructField("has_title", BooleanType(), True), StructField("has_description", BooleanType(), True),
                        StructField("has_activities", BooleanType(), True),StructField("ambassadorCreated", BooleanType(), True),
                        StructField("crawlerCreated", BooleanType(), True), StructField("openInvite", BooleanType(), True),
                        StructField("visibility", StringType(), True), StructField("full_address", StringType(), True), StructField("image_byte_array_jpg", BinaryType(), True)])
    return schema


def get_raw_eventsstaged_schema():
    schema = StructType(
        [StructField("_id", StringType(), True), StructField("event_organizers", ArrayType(StringType()), True),
         StructField("invited", ArrayType(StringType()), True),
         StructField("interested", ArrayType(StringType()), True),
         StructField("going", ArrayType(StringType()), True), StructField("event_media", ArrayType(StringType()), True),
         StructField("event_activities", ArrayType(StringType()), True),
         StructField("event_visibility", StringType(), True), StructField("event_openinvite", StringType(), True),
         StructField("event_name", StringType(), True), StructField("event_description", StringType(), True),
         StructField("event_timestart", StringType(), True), StructField("location", StringType(), True),
         StructField("event_timeend", StringType(), True), StructField("ambassador_created", BooleanType(), True),
         StructField("source", StructType([StructField("description", StringType(), True)]), True),
         StructField("status", IntegerType(), True),
         StructField("agent", StringType(), True), StructField("createdat", StringType(), True),
         StructField("updatedat", StringType(), True),
         StructField("source_description", StringType(), True),
         StructField("event_coordinates", ArrayType(DoubleType()), True),
         StructField("location_label", StringType(), True), StructField("location_address", StringType(), True),
         StructField("location_name", StringType(), True)])
    return schema


def get_scored_duplicate_events():
    schema = StructType([StructField("_id_a", StringType(), True), StructField("event_id_a", StringType(), True),
                         StructField("event_name_a", StringType(), True),
                         StructField("start_date_a", TimestampType(), True),
                         StructField("latlong_a", ArrayType(DoubleType()), True),
                         StructField("sentence_embedding_bert_a", ArrayType(DoubleType()), True),
                         StructField("_id_b", StringType(), True), StructField("event_id_b", StringType(), True),
                         StructField("event_name_b", StringType(), True),
                         StructField("start_date_b", TimestampType(), True),
                         StructField("latlong_b", ArrayType(DoubleType()), True),
                         StructField("sentence_embedding_bert_b", ArrayType(DoubleType()), True),
                         StructField("distance_km", DoubleType(), True),
                         StructField("timestart_difference", FloatType(), True),
                         StructField("cosine_distance", DoubleType(), True),
                         StructField("timestart_difference_normalised", DoubleType(), True),
                         StructField("distance_km_normalised", DoubleType(), True),
                         StructField("score", DoubleType(), True)])
    return schema


def get_raw_agents_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("name", StringType(), True),
                         StructField("email", StringType(), True),
                         StructField("regions", ArrayType(StringType(), True)),
                         StructField("roles", ArrayType(IntegerType(), True)),
                         StructField("location", StringType(), True),
                         StructField("createdat", StringType(), True), StructField("updatedat", StringType(), True)])
    return schema



def get_raw_friendships_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("users", ArrayType(StringType(), True)),
                         StructField("createdat", StringType(), True), StructField("updatedat", StringType(), True),
                         StructField("status", IntegerType(), True)])
    return schema


def get_clean_friendships_schema():
  schema = StructType([StructField("_id", StringType(), True), StructField("users", ArrayType(StringType(), True)),
                         StructField("created_at", TimestampType(), True), StructField("updated_at", TimestampType(), True),
                         StructField("status", IntegerType(), True), StructField("year", IntegerType(), True), StructField("month", IntegerType(), True),
                         StructField("day", IntegerType(), True), StructField("hour", IntegerType(), True)])
  return schema


def get_raw_media_schema():
  schema = StructType([StructField("_id", StringType(), True), StructField("createdat", StringType(), True),
                       StructField("updatedat", StringType(), True), StructField("media_url", StringType(), True),
                       StructField("media_type", StringType(), True), StructField("filename", StringType(), True),
                       StructField("author", StringType(), True), StructField("metadata", ArrayType(StructType([StructField("_id", StringType(), True),
                                                                                                                StructField("key", StringType(), True),
                                                                                                                StructField("value", StringType(), True)]))),
                       StructField("resized_urls", ArrayType(StringType(), True)), StructField("location", StringType(), True)])
  return schema


def get_clean_media_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("created_at", TimestampType(), True),
                         StructField("updated_at", TimestampType(), True), StructField("media_url", StringType(), True),
                         StructField("media_type", StringType(), True), StructField("filename", StringType(), True),
                         StructField("author", StringType(), True), StructField("metadata", ArrayType(StructType(
            [StructField("_id", StringType(), True), StructField("key", StringType(), True),
             StructField("value", StringType(), True)])), True),
                         StructField("resized_urls", ArrayType(StringType(), True)),
                         StructField("location", StringType(), True), StructField("year", IntegerType(), True),
                         StructField("month", IntegerType(), True), StructField("day", IntegerType(), True),
                         StructField("hour", IntegerType(), True)])
    return schema


def get_comments_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("createdat", StringType(), True),
                         StructField("updatedat", StringType(), True), StructField("event", StringType(), True),
                         StructField("location", StringType(), True), StructField("comment_text", StringType(), True),
                         StructField("author", StringType(), True)])
    return schema


def get_clean_comments_schema():
    schema = StructType([StructField("_id", StringType(), True), StructField("created_at", TimestampType(), True),
                         StructField("updated_at", TimestampType(), True), StructField("event", StringType(), True),
                         StructField("location", StringType(), True), StructField("comment_text", StringType(), True),
                         StructField("author", StringType(), True)])
    return schema


def get_schema(key):
    schemas = {"raw_events_schema": get_events_schema(),
               "raw_users_schema": get_users_schema(),
               "raw_attendances_schema": get_attendances_schema(),
               "raw_places_schema": get_places_schema(),
               "clean_events_schema": get_clean_events_schema(),
               "clean_attendances_schema": get_clean_attendances_schema(),
               "raw_activities_schema": get_activities_schema(),
               "raw_interests_schema": get_interests_schema(),
               "raw_downtoclown_schema": get_raw_downtoclown_schema(),
               "clean_downtoclown_schema": get_clean_downtoclown_schema(),
               "raw_feeds_schema": get_raw_feeds_schema(),
               "raw_availabilities_schema": get_raw_availabilities_schema(),
               "raw_hpf_events_schema": get_raw_hpf_events_schema(),
               "clean_scored_duplicated_events": get_scored_duplicate_events(),
               "raw_eventsstaged_schema": get_raw_eventsstaged_schema(),
               "raw_agents_schema": get_raw_agents_schema(),
               "clean_users_schema": get_clean_users_schema(),
               "raw_friendships_schema": get_raw_friendships_schema(),
               "clean_friendships_schema": get_clean_friendships_schema(),
               "raw_media_schema": get_raw_media_schema(),
               "clean_media_schema": get_clean_media_schema(),
               "raw_comments_schema": get_comments_schema(),
               "clean_comments_schema": get_clean_comments_schema()}
    return schemas[key]