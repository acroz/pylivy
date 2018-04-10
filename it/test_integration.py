import os
from collections import namedtuple

import pytest
import requests
import pandas

from livy import (
    LivySession, SessionKind, SparkRuntimeError, SessionState
)


LIVY_URL = os.environ.get('LIVY_TEST_URL', 'http://localhost:8998')


def livy_available():
    return requests.get(LIVY_URL).status_code == 200


def session_stopped(session_id):
    response = requests.get(f'{LIVY_URL}/session/{session_id}')
    if response.status_code == 404:
        return True
    else:
        return response.get_json()['state'] == 'shutting_down'


Parameters = namedtuple(
    'Parameters',
    [
        'print_foo_code',
        'print_foo_output',
        'create_dataframe_code',
        'dataframe_count_code',
        'dataframe_count_output',
        'error_code'
    ]
)


SPARK_CREATE_DF = """
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
val rdd = sc.parallelize(0 to 99)
val schema = StructType(List(
    StructField("value", IntegerType, nullable = false)
))
val df = spark.createDataFrame(rdd.map { i => Row(i) }, schema)
"""


SPARK_TEST_PARAMETERS = Parameters(
    print_foo_code='println("foo")',
    print_foo_output='foo\n\n',
    create_dataframe_code=SPARK_CREATE_DF,
    dataframe_count_code='df.count()',
    dataframe_count_output='res1: Long = 100\n\n',
    error_code='1 / 0'
)


PYSPARK_CREATE_DF = """
from pyspark.sql import Row
df = spark.createDataFrame([Row(value=i) for i in range(100)])
"""


PYSPARK_TEST_PARAMETERS = Parameters(
    print_foo_code='print("foo")',
    print_foo_output='foo\n',
    create_dataframe_code=PYSPARK_CREATE_DF,
    dataframe_count_code='df.count()',
    dataframe_count_output='100\n',
    error_code='1 / 0'
)


SPARKR_CREATE_DF = """
df <- createDataFrame(data.frame(value = 0:99))
"""


SPARKR_TEST_PARAMETERS = Parameters(
    print_foo_code='print("foo")',
    print_foo_output='[1] "foo"\n',
    create_dataframe_code=SPARKR_CREATE_DF,
    dataframe_count_code='count(df)',
    dataframe_count_output='[1] 100\n',
    error_code='missing_function()'
)


@pytest.mark.parametrize('session_kind, params', [
    (SessionKind.SPARK, SPARK_TEST_PARAMETERS),
    (SessionKind.PYSPARK, PYSPARK_TEST_PARAMETERS),
    (SessionKind.SPARKR, SPARKR_TEST_PARAMETERS)
])
def test_session(capsys, session_kind, params):

    assert livy_available()

    with LivySession(LIVY_URL, kind=session_kind) as session:

        assert session.state == SessionState.IDLE

        session.run(params.print_foo_code)
        assert capsys.readouterr() == (params.print_foo_output, '')

        session.run(params.create_dataframe_code)
        capsys.readouterr()

        session.run(params.dataframe_count_code)
        assert capsys.readouterr() == (params.dataframe_count_output, '')

        with pytest.raises(SparkRuntimeError):
            session.run(params.error_code)

        expected = pandas.DataFrame({'value': range(100)})
        assert session.read('df').equals(expected)

    assert session_stopped(session.session_id)


SQL_CREATE_VIEW = """
CREATE TEMPORARY VIEW view AS SELECT * FROM RANGE(100)
"""


def test_sql_session():

    assert livy_available()

    with LivySession(LIVY_URL, kind=SessionKind.SQL) as session:

        assert session.state == SessionState.IDLE

        session.run(SQL_CREATE_VIEW)
        output = session.run('SELECT COUNT(*) FROM view')
        assert output.json['data'] == [[100]]

        with pytest.raises(SparkRuntimeError):
            session.run('not valid SQL!')

        expected = pandas.DataFrame({'id': range(100)})
        assert session.read_sql('SELECT * FROM view').equals(expected)

    assert session_stopped(session.session_id)
