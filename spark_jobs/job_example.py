from utils import utils
import logging



def main():
    df = utils.data_interceptor.read_mongo(mongo_database="test", collection="actors")
    print(df.show())




if __name__ == "__main__":

    #logging configuration
    logging.basicConfig(level=logging.INFO,
                         format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                         datefmt='%Y-%m-%d:%H:%M:%S')
    #SQL
    sql_type = "mysql"
    sql_user = "root"
    sql_pass = ""
    sql_host = "localhost"
    sql_driver = "com.mysql.cj.jdbc.Driver"
    sql_query = """
            (SELECT * FROM hogar.personas WHERE rol = 'hijo') as oliver
        """

    #packages
    mongo_jar = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    mysql_jar = "mysql:mysql-connector-java:8.0.26"


    #MONGO
    #parameters
    mongo_host = "localhost:27017"
    mongo_user = ""
    mongo_pass = ""


    #set sql database parameters to facilitate data access
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d:%H:%M:%S')

    # set logger and spark parameters
    utils.data_interceptor.set_logger(logging)
    utils.data_interceptor.set_spark_conf(mongo_jar)
    utils.data_interceptor.set_spark_session()

    utils.data_interceptor.set_sql_params(sql_type=sql_type, sql_password=sql_pass, sql_host=sql_host,
                                          sql_user=sql_user, sql_driver=sql_driver)

    # get spark session and context
    spark = utils.data_interceptor.get_spark_session()
    sc = utils.data_interceptor.get_spark_context()

    #set mongo database parameters to facilitate data access
    utils.data_interceptor.set_mongo_params(mongo_host, mongo_user, mongo_pass)

    main()