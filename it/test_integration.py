import os
import requests
import pytest
from livy import Livy, SparkRuntimeError


LIVY_URL = os.environ.get('LIVY_TEST_URL', 'http://localhost:8998')


def livy_available():
    try:
        response = requests.get(LIVY_URL)
    except ConnectionError as e:
        return False
    return response.ok


PYSPARK_CREATE_DF = """
from pyspark.sql import Row
df = spark.createDataFrame([Row(value=i) for i in range(100)])
"""


def test_pyspark(capsys):
    assert livy_available()

    with Livy(LIVY_URL) as client:
        client.run('print("foo")')
        assert capsys.readouterr() == ('foo\n', '')

        client.run(PYSPARK_CREATE_DF)
        client.run('df.count()')
        assert capsys.readouterr() == ('100\n', '')

        with pytest.raises(SparkRuntimeError):
            client.run('1 / 0')
