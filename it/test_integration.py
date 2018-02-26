import os
import requests
import pandas
import pytest
from livy import (
    Livy, SessionKind, SessionManager, SessionState, SparkRuntimeError
)


LIVY_URL = os.environ.get('LIVY_TEST_URL', 'http://localhost:8998')


def livy_available():
    try:
        response = requests.get(LIVY_URL)
    except ConnectionError as e:
        return False
    return response.ok


def session_stopped(session_id):
    session = SessionManager(LIVY_URL).get(session_id)
    if session is None:
        return True
    else:
        return session.state == SessionState.SHUTTING_DOWN


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

    assert livy_available()

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

    assert session_stopped(session_id)


PYSPARK_CREATE_DF = """
from pyspark.sql import Row
df = spark.createDataFrame([Row(value=i) for i in range(100)])
"""


def test_pyspark(capsys):

    assert livy_available()

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

    assert session_stopped(session_id)


SPARKR_CREATE_DF = """
df <- createDataFrame(data.frame(value = 0:99))
"""


def test_sparkr(capsys):

    assert livy_available()

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

    assert session_stopped(session_id)


SQL_CREATE_VIEW = """
CREATE TEMPORARY VIEW view AS SELECT * FROM RANGE(100)
"""


def test_sql():

    assert livy_available()

    with Livy(LIVY_URL, kind=SessionKind.SQL) as client:

        client.run(SQL_CREATE_VIEW)
        output = client.run('SELECT COUNT(*) FROM view')
        assert output.json['data'] == [[100]]

        with pytest.raises(SparkRuntimeError):
            client.run('not valid SQL!')

        session_id = client.session.id_

    assert session_stopped(session_id)
