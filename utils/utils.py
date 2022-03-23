from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime as dt


class DataInterceptor:
    def __init__(self):
        pass

    def set_logger(self, logger):
        self.logger = logger

    def set_spark_conf(self, packages=None):
        if packages:
            self.conf = SparkConf().set("spark.jars.packages",
                                        packages).set("spark.port.maxRetries", "100")
        else:
            self.conf = SparkConf()

    def set_spark_session(self):
        self.spark = SparkSession.builder.config(conf=self.conf).getOrCreate()

    def get_spark_session(self):
        return self.spark

    def get_spark_context(self):
        return self.spark._sc

    def set_sql_params(self, sql_host, sql_user, sql_password, sql_driver):
        self.sql_host = sql_host
        self.sql_user = sql_user
        self.sql_password = sql_password
        self.sql_driver = sql_driver

    def read_sql(
            self,
            sql_query,
            sql_host=None,
            sql_password=None,
            sql_user=None,
            cache=False):
        '''
        :param sql_query: the SQL query to retrieve from a table in a SQL database
        :param sql_host: DNS. When None, the class attribute self.sql_host is used here
        :param sql_password: user password. When None, the class attribute self.sql_password is used here
        :param sql_user: user name. When None, the class attribute self.sql_user is used here
        :param cache: whether to cache the resulting dataframe in memory of a Spark cluster. Default is False
        :return: spark dataframe
        '''

        if not sql_host:
            sql_host = self.sql_host
        if not sql_password:
            sql_password = self.sql_password
        if not sql_user:
            sql_user = self.sql_user
        sql_url = f"jdbc:mysql://{sql_host}:3306/"

        before_read = dt.now()
        df = self.spark.read.format("jdbc") \
            .options(
            url=sql_url,
            driver=self.sql_driver,
            user=sql_user,
            dbtable=sql_query,
            password=sql_password) \
            .load()
        after_read = dt.now()

        if cache:
            df.cache()
        self.logger.info(
            f"Total number of obtained records: {df.count()}. "
            f"Accessing data took: {after_read - before_read}")

        return df

    def set_mongo_params(
            self,
            mongo_host,
            mongo_user,
            mongo_pass):
        self.mongo_host = mongo_host
        self.mongo_user = mongo_user
        self.mongo_pass = mongo_pass

    def read_mongo(
            self,
            mongo_database,
            collection,
            mongo_host=None,
            mongo_user=None,
            mongo_pass=None,
            pipeline=None,
            cache=False
    ):
        """
        :param mongo_database: the mongo database containing the collection to read from
        :param collection: the mongo collection to import as a spark dataframe
        :param mongo_host: server DNS. When None, the class attribute self.mongo_host is used here
        :param mongo_user: username. When None, the class attribute self.mongo_user is used here
        :param mongo_pass: user password. When None, the class attribute self.mongo_pass is used here
        :param pipeline: the query used to find documents within the mongo collection. When None, the entire collection is imported.
        :param cache whether to cache the dataframe in memory.
        :return: spark dataframe
        """

        if not mongo_host:
            mongo_host = self.mongo_host
        if not mongo_user:
            mongo_user = self.mongo_user
        if not mongo_pass:
            mongo_pass = self.mongo_pass
        mongo_url = f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_host}/"

        if not pipeline:
            before_read = dt.now()
            df = self.spark.read.format("mongo") \
                .option("uri", mongo_url + mongo_database + "." + collection) \
                .load()
            after_read = dt.now()
        else:
            before_read = dt.now()
            df = self.spark.read.format("mongo") \
                .option("uri", mongo_url + mongo_database + "." + collection) \
                .option("pipeline", pipeline) \
                .load()
            after_read = dt.now()
        if cache:
            df.cache()
        self.logger.info(
            f"Total number of obtained records from {mongo_database}.{collection}: {df.count()}. Accesing data took: {after_read - before_read}")
        return df

    def write_mongo(self, df, mongo_database, mongo_collection, mode, mongo_host=None, mongo_user=None,
                    mongo_pass=None):
        if mongo_host is None:
            mongo_host = self.mongo_host
        if mongo_user is None:
            mongo_user = self.mongo_user
        if mongo_pass is None:
            mongo_pass = self.mongo_pass

        mongo_url = f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_host}/"

        try:
            df.write.format("mongo").mode(mode) \
                .option("uri", mongo_url) \
                .option("database", mongo_database) \
                .option("collection", mongo_collection)
        except Exception as e:
            self.logger.info("Unable to write data to Mongo. Please see the following error log:")
            self.logger.info(e)

    def read_parquet(self, path, schema, partition_number, cache=False):
        """
        :param path: the path in the file system pointing to the parquet file to be imported.
        :param schema: the schema to use if data import fails (or when returning an empty dataframe).
        :param partition_number: the number of partitions of the resulting spark dataframe.
        :param cache whether to cache the dataframe in memory.
        :return: spark dataframe
        """
        try:
            before_read = dt.now()
            df = self.spark.read.format("parquet").\
                load(path=path, schema=schema).\
                repartition(partition_number)
            after_read = dt.now()
            self.logger.info(f"Accessing data from S3 took: {after_read - before_read}")

        except Exception as e:
            self.logger.info(
                f"Accessing S3 path failed. Creating empty dataframe with specified schema. Please review error: {e}")
            df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema) \
                .repartition(partition_number)
        if cache:
            df.cache()
        self.logger.info(f"Number of records retrieved: {df.count()}")
        return df

    def read_csv(self, path, partition_number, schema=None, cache=False):
        """
        :param path: the path pointing to the location of the csv file
        :param schema: schema to pass to the data
        :param partition_number: the number of partitions of the resulting spark dataframe
        :param cache: whether to cache the data in memory
        :return: spark dataframe
        """
        try:
            self.logger.info("Accessing CSV file in storage")
            before_read = dt.now()
            df = self.spark.read.csv(
                path,
                schema=schema if schema else None,
                header=True,
                enforceSchema=True) \
                .repartition(partition_number)
            after_read = dt.now()
            self.logger.info(f"Accessing data took {after_read - before_read}")
        except Exception as e:
            self.logger.info(f"Failed to access CSV file in storage. Please review error: {e}")
            df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)
        if cache:
            df.cache()
            df.count()
        return df

    def write_parquet(self, df, path, write_mode, partition_columns=None):
        """
        :param df: dataframe to write to storage
        :param path: S3 path where the data will be stored
        :param partition_columns: name of partition column (a list)
        :param write_mode: one of "append" or "overwrite"
        :return: None
        """
        df.write.parquet(
            path=path,
            mode=write_mode,
            partitionBy=partition_columns if partition_columns else None,
            compression="snappy")

data_interceptor = DataInterceptor()
