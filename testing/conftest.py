'''
Create pytest fixtures for PySpark
'''

import findspark
findspark.init()

import pytest

from pyspark.sql import SparkSession
from pyspark import SparkConf

@pytest.fixture(scope="session")
def spark_session(request):
    conf = SparkConf().setMaster("local[2]").setAppName("pytest_pyspark_testing")
    spark = SparkSession.builder.config(conf = conf).getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    return spark