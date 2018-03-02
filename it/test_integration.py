import os
from collections import namedtuple

import pytest
import aiohttp
import pandas

from livy import LivySession, AsyncLivySession, SessionKind, SparkRuntimeError
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


TestParameters = namedtuple(
    'TestParameters',
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


SPARK_TEST_PARAMETERS = TestParameters(
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


PYSPARK_TEST_PARAMETERS = TestParameters(
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


SPARKR_TEST_PARAMETERS = TestParameters(
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
def test_session_sync(capsys, session_kind, params):

    assert run_sync(livy_available())

    with LivySession(LIVY_URL, kind=session_kind) as session:

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

        session_id = session.session_id

    assert run_sync(session_stopped(session_id))


@pytest.mark.parametrize('session_kind, params', [
    (SessionKind.SPARK, SPARK_TEST_PARAMETERS),
    (SessionKind.PYSPARK, PYSPARK_TEST_PARAMETERS),
    (SessionKind.SPARKR, SPARKR_TEST_PARAMETERS)
])
@pytest.mark.asyncio
async def test_session_async(capsys, session_kind, params):

    assert await livy_available()

    async with AsyncLivySession(LIVY_URL, kind=session_kind) as session:

        await session.run(params.print_foo_code)
        assert capsys.readouterr() == (params.print_foo_output, '')

        await session.run(params.create_dataframe_code)
        capsys.readouterr()

        await session.run(params.dataframe_count_code)
        assert capsys.readouterr() == (params.dataframe_count_output, '')

        with pytest.raises(SparkRuntimeError):
            await session.run(params.error_code)

        expected = pandas.DataFrame({'value': range(100)})
        assert (await session.read('df')).equals(expected)

        session_id = session.session_id

    await session_stopped(session_id)


SQL_CREATE_VIEW = """
CREATE TEMPORARY VIEW view AS SELECT * FROM RANGE(100)
"""


def test_sql_session_sync():

    assert run_sync(livy_available())

    with LivySession(LIVY_URL, kind=SessionKind.SQL) as session:

        session.run(SQL_CREATE_VIEW)
        output = session.run('SELECT COUNT(*) FROM view')
        assert output.json['data'] == [[100]]

        with pytest.raises(SparkRuntimeError):
            session.run('not valid SQL!')

        session_id = session.session_id

    assert run_sync(session_stopped(session_id))


@pytest.mark.asyncio
async def test_sql_session_async():

    assert await livy_available()

    async with AsyncLivySession(LIVY_URL, kind=SessionKind.SQL) as session:

        await session.run(SQL_CREATE_VIEW)
        output = await session.run('SELECT COUNT(*) FROM view')
        assert output.json['data'] == [[100]]

        with pytest.raises(SparkRuntimeError):
            await session.run('not valid SQL!')

        session_id = session.session_id

    assert await session_stopped(session_id)
