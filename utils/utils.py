from pyspark.sql import SparkSession
from datetime import datetime as dt

#clase contenedora del logger y otros atributos que facilitan la interaccion \\


class DataInterceptor:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.logger = None
        self.sql_driver = None
        self.sql_host = None
        self.sql_user = None
        self.sql_password = None
        self.mongo_url = None
        self.mongo_replica = None

    def set_logger(self, logger):
        self.logger = logger

    def get_spark_session(self):
        return self.spark

    def set_sql_params(self, sql_host, sql_user, sql_password, sql_driver):
        self.sql_user = sql_user
        self.sql_host = sql_host
        self.sql_password = sql_password
        self.sql_driver = sql_driver

    def read_sql(self, sql_query, sql_host = None, sql_password = None, sql_user = None, cache = False):
        if sql_host is None:
            sql_host = self.sql_host
        if sql_password is None:
            sql_password = self.sql_password
        if sql_user is None:
            sql_user = self.sql_user
        sql_url = f"jdbc:mysql://{sql_host}:3306"

        if cache:
            before_read = dt.now()
            df = self.spark.read.format("jdbc") \
                .options(url = sql_url,
                        driver = self.sql_driver,
                        user = sql_user,
                        dbtable = sql_query,
                        password = sql_password) \
                .load()
            after_read = dt.now()
            df.cache()
            df.count()

        else:
            before_read = dt.now()
            df = self.spark.read.format("jdbc") \
                .option(url = sql_url,
                        driver = self.sql_driver,
                        user = sql_user,
                        dbtable = sql_query,
                        password = sql_password)\
                .load()
            after_read = dt.now()

        num_records = df.count()
        self.logger.info(f"Total number of obtained records: {num_records}. Accesing data took: {after_read - before_read}")

        return df

    def set_mongo_params(self, mongo_url, mongo_replica):
        self.mongo_url = mongo_url
        self.mongo_replica = mongo_replica

    def read_mongo(self, mongo_database, collection, mongo_url = None, mongo_replica = None,pipeline = None):
        if mongo_url is None:
            mongo_url = self.mongo_url
        if mongo_replica is None:
            mongo_replica = self.mongo_replica

        if pipeline is None:
            before_read = dt.now()
            df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", mongo_url + mongo_database + "." + collection + mongo_replica)\
                .load()
            after_read = dt.now()
        else:
            before_read = dt.now()
            df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", mongo_url + mongo_database + "." + collection + mongo_replica)\
                .option("pipeline", pipeline) \
                .load()
            after_read = dt.now()

        num_records = df.count()
        self.logger.info(f"Total number of obtained records from {mongo_database}.{collection}: {num_records}. Accesing data took: {after_read - before_read}")
        return df

    def read_parquet(self, path, schema, partition_number):
        try:
            before_read = dt.now()
            df = self.spark.read.parquet(path).repartition(partition_number)
            after_read = dt.now()
            self.logger.info(f"Acessing data from S3 took: {after_read - before_read}")
            if df.count() == 0:
                self.logger.info("No records found. Creating empty dataframe with specified schema")
                df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

        except Exception as e:
            self.logger.info(e)
            self.logger.info("Acessing S3 path failed. Creating empty dataframe with specified schema")
            df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

        df.cache()
        df.count()

        return df

data_interceptor = DataInterceptor()