from utils import utils
import logging
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, StructField

def filter_data_by_name(df, data):
    for column in df.columns:
        df_filtered = df.filter(F.col(column) == data)
        if df_filtered.count() > 0:
            return True
    return False


def main():
    df = utils.data_interceptor.read_mongo("test", "actors")
    print(df.show())





if __name__ == "__main__":

    logger = logging.basicConfig()

    #SQL parameters
    local_host = "localhost"
    mysql_user = "root"
    mysql_url = f"mysql://{local_host}:3306/"
    password = "Ilovebiotech1+"
    mysql_db = "tienda_aplicaciones"
    mysql_table = "tienda_apps"

    #mongo parameters
    mongo_host = "localhost"
    mongo_user = ""
    mongo_pass = ""

    #sql queries
    query = f"SELECT * FROM {mysql_db}.{mysql_table}"

    #set sql database parameters to facilitate data access
    utils.data_interceptor.set_logger(logger)
    utils.data_interceptor.set_sql_params(local_host, mysql_user, password, sql_driver= "com.mysql.jdbc.Driver")

    #set mongo database parameters to facilitate data access
    utils.data_interceptor.set_mongo_params(mongo_host, mongo_user, mongo_pass)
    spark = utils.data_interceptor.get_spark_session()

    main()