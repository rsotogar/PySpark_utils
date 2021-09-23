from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime as dt


# this class simplifies data import from several data sources (SQL databases, MongoDB) by abstracting the underlying pyspark methods.


class DataInterceptor:
    def __init__(self):
        self.conf = None
        self.spark = None
        self.logger = None

        # sql parameters
        self.sql_type = None
        self.sql_driver = None
        self.sql_host = None
        self.sql_user = None
        self.sql_password = None

        # mongo attributes
        self.mongo_host = None
        self.mongo_user = None
        self.mongo_pass = None

    def set_logger(self, logger):
        self.logger = logger

    def set_spark_conf(self, packages=None):
        if packages is not None:
            self.conf = SparkConf().set("spark.jars.packages",
                                        packages).set("spark.port.maxRetries", 100)
        else:
            self.conf = SparkConf()

    def set_spark_session(self):
        self.spark = SparkSession.builder.config(conf=self.conf).getOrCreate()

    def get_spark_session(self):
        return self.spark

    def get_spark_context(self):
        return self.spark._sc

    def set_sql_params(self, sql_type, sql_host, sql_user, sql_password, sql_driver):
        self.sql_type = sql_type
        self.sql_host = sql_host
        self.sql_user = sql_user
        self.sql_password = sql_password
        self.sql_driver = sql_driver

    def read_sql(self, sql_query, sql_type=None, sql_host=None, sql_password=None, sql_user=None, cache=False):
        '''
        :param sql_query: the SQL query to retrieve from a table in a SQL database
        :param sql_type: the database type to read data from (MySQL, PostgresQl, etc.)
        :param sql_host: DNS. When None, the class attribute self.sql_host is used here
        :param sql_password: user password. When None, the class attribute self.sql_password is used here
        :param sql_user: user name. When None, the class attribute self.sql_user is used here
        :param cache: whether to cache the resulting dataframe in memory of a Spark cluster. Default is False
        :return: spark dataframe
        '''
        if sql_type is None:
            sql_type = self.sql_type
        if sql_host is None:
            sql_host = self.sql_host
        if sql_password is None:
            sql_password = self.sql_password
        if sql_user is None:
            sql_user = self.sql_user
        sql_url = f"jdbc:{sql_type}://{sql_host}:3306/"

        if cache:
            before_read = dt.now()
            df = self.spark.read.format("jdbc") \
                .options(url=sql_url,
                         driver=self.sql_driver,
                         user=sql_user,
                         dbtable=sql_query,
                         password=sql_password) \
                .load()
            after_read = dt.now()
            df.cache()
            df.count()

        else:
            before_read = dt.now()
            df = self.spark.read.format("jdbc") \
                .options(url=sql_url,
                         driver=self.sql_driver,
                         user=sql_user,
                         dbtable=sql_query,
                         password=sql_password) \
                .load()
            after_read = dt.now()

        num_records = df.count()
        self.logger.info(
            f"Total number of obtained records: {num_records}. Accesing data took: {after_read - before_read}")

        return df

    def set_mongo_params(self, mongo_host, mongo_user, mongo_pass):
        self.mongo_host = mongo_host
        self.mongo_user = mongo_user
        self.mongo_pass = mongo_pass

    def read_mongo(self, mongo_database, collection, mongo_host=None, mongo_user=None, mongo_pass=None, pipeline=None):
        '''
        :param mongo_database: the mongo database containing the collection to read from
        :param collection: the mongo collection to import as a spark dataframe
        :param mongo_host: server DNS. When None, the class attribute self.mongo_host is used here
        :param mongo_user: user name. When None, the class attribute self.mongo_user is used here
        :param mongo_pass: user password. When None, the class attribute self.mongo_pass is used here
        :param pipeline: the query used to find documents within the mongo collection. When None, the entire collection is imported
        :return: spark dataframe
        '''

        if mongo_host is None:
            mongo_host = self.mongo_host
        if mongo_user is None:
            mongo_user = self.mongo_user
        if mongo_pass is None:
            mongo_pass = self.mongo_pass
        mongo_url = f"mongo://{mongo_user}:{mongo_pass}@{mongo_host}/"

        if pipeline is None:
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

        df.cache()
        num_records = df.count()
        self.logger.info(
            f"Total number of obtained records from {mongo_database}.{collection}: {num_records}. Accesing data took: {after_read - before_read}")
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

    def read_parquet(self, path, schema, partition_number):
        '''
        :param path: the path in the file system pointing to the parquet file to be imported
        :param schema: the schema to use if data import fails (or when returning an empty dataframe).
        :param partition_number: the number of partitions of the resulting spark dataframe
        :return: spark dataframe
        '''
        try:
            before_read = dt.now()
            df = self.spark.read.format("parquet").load(path=path, schema=schema).repartition(partition_number)
            after_read = dt.now()
            self.logger.info(f"Accessing data from S3 took: {after_read - before_read}")

        except Exception as e:
            self.logger.info(
                "Accessing S3 path failed. Creating empty dataframe with specified schema. Please review the folowing error:")
            self.logger.info(e)
            df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema) \
                .repartition(partition_number)

        df.cache()
        df.count()

        return df

    def read_csv(self, path, partition_number, schema=None, cache=False):
        '''
        :param path: the path pointing to the location of the csv file
        :param schema: schema to pass to the data
        :param partition_number: the number of partitions of the resulting spark dataframe
        :param cache: whether to cache the data in memory
        :return: spark dataframe
        '''

        try:
            self.logger.info("Accessing CSV file in storage")
            if schema is not None:
                before_read = dt.now()
                df = self.spark.read.csv(path, schema=schema, header=True, enforceSchema=True) \
                    .repartition(partition_number)
                after_read = dt.now()
                self.logger.info(f"Accessing data took {after_read - before_read}")
                if cache:
                    df.cache()
                    df.count()
            else:
                before_read = dt.now()
                df = self.spark.read.csv(path, header=True) \
                    .repartition(partition_number)
                after_read = dt.now()
                self.logger.info(f"Accessing data took {after_read - before_read}")
                if cache:
                    df.cache()
                    df.count()
        except Exception as e:
            self.logger.info(e)
            self.logger.info("Failed to access CSV file in storage")
            df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

        return df

    def write_parquet(self, df, path, partition_columns=None):
        '''
        :param df: dataframe to write to storage
        :param path: S3 path where the data will be stored
        :param partition_columns: name of partition column (a list)
        :return: None
        '''
        if partition_columns is not None:
            df.write.parquet(path=path, mode="append",
                             partitionBy=partition_columns, compression="snappy")
        else:
            df.write.parquet(path=path, mode="append", compression="snappy")


    def read_json(self, path, schema, partition_number):
        self.logger.info(f"Accessing data from {path}...")
        try:
            before_read = dt.now()
            df = self.spark.read.json(path=path, schema=schema).repartition(partition_number)
            after_read = dt.now()
            self.logger.info(f"Success! Accessing data took: {after_read - before_read}")
        except Exception as e:
            self.logger.info(
                f"Accessing data failed. Please review the following error: {e}. Creating dataframe with specified schema.")
            df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

        df.cache()
        df.count()
        return df


data_interceptor = DataInterceptor()