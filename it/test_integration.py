import os

import pytest
import aiohttp
import pandas

from livy import Livy, SessionKind, SparkRuntimeError, run_sync


LIVY_URL = os.environ.get('LIVY_TEST_URL', 'http://localhost:8998')


async def livy_available():
    async with aiohttp.ClientSession() as session:
        async with session.get(LIVY_URL) as response:
            return response.status == 200


async def session_stopped(session_id):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{LIVY_URL}/session/{session_id}') as response:
            if response.status == 404:
                return True
            else:
                return response.json()['state'] == 'shutting_down'


SPARK_CREATE_DF = """
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
val rdd = sc.parallelize(0 to 99)
val schema = StructType(List(
    StructField("value", IntegerType, nullable = false)
))
val df = spark.createDataFrame(rdd.map { i => Row(i) }, schema)
"""


def test_spark(capsys):

    assert run_sync(livy_available())

    with Livy(LIVY_URL, kind=SessionKind.SPARK) as client:

        client.run('println("foo")')
        assert capsys.readouterr() == ('foo\n\n', '')

        client.run(SPARK_CREATE_DF)
        capsys.readouterr()

        client.run('df.count()')
        assert capsys.readouterr() == ('res1: Long = 100\n\n', '')

        with pytest.raises(SparkRuntimeError):
            client.run('1 / 0')

        expected = pandas.DataFrame({'value': range(100)})
        assert client.read('df').equals(expected)

        session_id = client.session.id_

    assert run_sync(session_stopped(session_id))


PYSPARK_CREATE_DF = """
from pyspark.sql import Row
df = spark.createDataFrame([Row(value=i) for i in range(100)])
"""


def test_pyspark(capsys):

    assert run_sync(livy_available())

    with Livy(LIVY_URL, kind=SessionKind.PYSPARK) as client:

        client.run('print("foo")')
        assert capsys.readouterr() == ('foo\n', '')

        client.run(PYSPARK_CREATE_DF)
        client.run('df.count()')
        assert capsys.readouterr() == ('100\n', '')

        with pytest.raises(SparkRuntimeError):
            client.run('1 / 0')

        expected = pandas.DataFrame({'value': range(100)})
        assert client.read('df').equals(expected)

        session_id = client.session.id_

    assert run_sync(session_stopped(session_id))


SPARKR_CREATE_DF = """
df <- createDataFrame(data.frame(value = 0:99))
"""


def test_sparkr(capsys):

    assert run_sync(livy_available())

    with Livy(LIVY_URL, kind=SessionKind.SPARKR) as client:

        client.run('print("foo")')
        assert capsys.readouterr() == ('[1] "foo"\n', '')

        client.run(SPARKR_CREATE_DF)
        client.run('count(df)')
        assert capsys.readouterr() == ('[1] 100\n', '')

        with pytest.raises(SparkRuntimeError):
            client.run('missing_function()')

        expected = pandas.DataFrame({'value': range(100)})
        assert client.read('df').equals(expected)

        session_id = client.session.id_

    assert run_sync(session_stopped(session_id))


SQL_CREATE_VIEW = """
CREATE TEMPORARY VIEW view AS SELECT * FROM RANGE(100)
"""


def test_sql():

    assert run_sync(livy_available())

    with Livy(LIVY_URL, kind=SessionKind.SQL) as client:

        client.run(SQL_CREATE_VIEW)
        output = client.run('SELECT COUNT(*) FROM view')
        assert output.json['data'] == [[100]]

        with pytest.raises(SparkRuntimeError):
            client.run('not valid SQL!')

        session_id = client.session.id_

    assert run_sync(session_stopped(session_id))
