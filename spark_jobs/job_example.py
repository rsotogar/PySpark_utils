from utils import utils
import logging


def main():
    df_apps = utils.data_interceptor.read_sql(sql_query=query, cache= True)
    df_apps.show()



if __name__ == "__main__":

    logger = logging.basicConfig()

    #SQL parameters
    local_host = "localhost"
    mysql_user = "root"
    mysql_url = f"mysql://{local_host}:3306/"
    password = ""
    mysql_db = "tienda_aplicaciones"
    mysql_table = "tienda_apps"

    #queries
    query = f"SELECT * FROM {mysql_db}.{mysql_table}"

    utils.data_interceptor.set_logger(logger)
    utils.data_interceptor.set_sql_params(local_host, mysql_user, password, sql_driver= "com.mysql.jdbc.Driver")

    main()