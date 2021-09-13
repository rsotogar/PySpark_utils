from utils import utils
from utils import schemas_spark
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
    df = utils.data_interceptor.read_mongo(mongo_database="test", collection="actors")
    print(df.show())




if __name__ == "__main__":

    #logging configuration
    logging.basicConfig(level=logging.INFO,
                         format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                         datefmt='%Y-%m-%d:%H:%M:%S')

    #spark conf
    config = {"spark.driver.memory": "0.5g", "spark.driver.cores": 2}

    #SQL parameters
    local_host = "localhost"
    mysql_user = "root"
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
    utils.data_interceptor.set_logger(logging)
    utils.data_interceptor.set_spark_conf(config)
    utils.data_interceptor.set_spark()
    utils.data_interceptor.set_sql_params(local_host, mysql_user, password, sql_driver= "com.mysql.jdbc.Driver")

    #set mongo database parameters to facilitate data access
    utils.data_interceptor.set_mongo_params(mongo_host, mongo_user, mongo_pass)

    main()