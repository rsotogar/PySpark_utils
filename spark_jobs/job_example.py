from utils import utils
import logging


def main():
    df_actors = utils.data_interceptor.read_mongo(mongo_database="test", collection="actors")
    print(df_actors.show())



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

    main()