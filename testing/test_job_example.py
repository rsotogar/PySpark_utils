import pytest
from spark_jobs.job_example import filter_data_by_name

pytestpymark = pytest.mark.usefixtures("spark_session")


def test_filter_data_by_name(spark_session):
    data = [("Ramon", 31), ("Mariela", 37)]
    schema = "FirstName STRING, Age INT"
    df = spark_session.createDataFrame(data, schema)
    results = filter_data_by_name(df, "Ramon")
    assert results == True





