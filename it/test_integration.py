import os

import pytest
import aiohttp
import pandas

from livy import LivySession, SessionKind, SparkRuntimeError
from livy.session import run_sync


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

    with LivySession(LIVY_URL, kind=SessionKind.SPARK) as session:

        session.run('println("foo")')
        assert capsys.readouterr() == ('foo\n\n', '')

        session.run(SPARK_CREATE_DF)
        capsys.readouterr()

        session.run('df.count()')
        assert capsys.readouterr() == ('res1: Long = 100\n\n', '')

        with pytest.raises(SparkRuntimeError):
            session.run('1 / 0')

        expected = pandas.DataFrame({'value': range(100)})
        assert session.read('df').equals(expected)

        session_id = session.session_id

    assert run_sync(session_stopped(session_id))


PYSPARK_CREATE_DF = """
from pyspark.sql import Row
df = spark.createDataFrame([Row(value=i) for i in range(100)])
"""


def test_pyspark(capsys):

    assert run_sync(livy_available())

    with LivySession(LIVY_URL, kind=SessionKind.PYSPARK) as session:

        session.run('print("foo")')
        assert capsys.readouterr() == ('foo\n', '')

        session.run(PYSPARK_CREATE_DF)
        session.run('df.count()')
        assert capsys.readouterr() == ('100\n', '')

        with pytest.raises(SparkRuntimeError):
            session.run('1 / 0')

        expected = pandas.DataFrame({'value': range(100)})
        assert session.read('df').equals(expected)

        session_id = session.session_id

    assert run_sync(session_stopped(session_id))


SPARKR_CREATE_DF = """
df <- createDataFrame(data.frame(value = 0:99))
"""


def test_sparkr(capsys):

    assert run_sync(livy_available())

    with LivySession(LIVY_URL, kind=SessionKind.SPARKR) as session:

        session.run('print("foo")')
        assert capsys.readouterr() == ('[1] "foo"\n', '')

        session.run(SPARKR_CREATE_DF)
        session.run('count(df)')
        assert capsys.readouterr() == ('[1] 100\n', '')

        with pytest.raises(SparkRuntimeError):
            session.run('missing_function()')

        expected = pandas.DataFrame({'value': range(100)})
        assert session.read('df').equals(expected)

        session_id = session.session_id

    assert run_sync(session_stopped(session_id))


SQL_CREATE_VIEW = """
CREATE TEMPORARY VIEW view AS SELECT * FROM RANGE(100)
"""


def test_sql():

    assert run_sync(livy_available())

    with LivySession(LIVY_URL, kind=SessionKind.SQL) as session:

        session.run(SQL_CREATE_VIEW)
        output = session.run('SELECT COUNT(*) FROM view')
        assert output.json['data'] == [[100]]

        with pytest.raises(SparkRuntimeError):
            session.run('not valid SQL!')

        session_id = session.session_id

    assert run_sync(session_stopped(session_id))
