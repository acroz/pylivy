from dataclasses import dataclass

import pytest
import requests
import pandas

from livy import (
    LivySession,
    LivyBatch,
    SessionKind,
    SparkRuntimeError,
    SessionState,
)


@dataclass
class Parameters:
    print_foo_code: str
    print_foo_output: str
    create_dataframe_code: str
    dataframe_count_code: str
    dataframe_count_output: str
    error_code: str
    dataframe_multiply_code: str
    dataframe_trim_code: str


RANGE_DATAFRAME = pandas.DataFrame({"value": range(100)})
SPECIAL_CHARACTER_EXAMPLES = [
    # Single and double quotes can terminate string literals in Scala/Python/R
    "'",
    '"',
    # Triple double quotes can terminate multiline strings in Scala - check
    # also triple single quotes for completeness
    "'''",
    '"""',
    # Various types of brackets, including [] and {} which have meaning in JSON
    " [](){} ",
    # Some special characters that require escape sequences or unicode
    "\u03bb\U0001f914\n\r\t\\",
    # Some other common special characters
    "!@£$%^&*",
]
TEXT_DATAFRAME = pandas.DataFrame(
    {"text": [" foo ", " bar"] + SPECIAL_CHARACTER_EXAMPLES}
)

SPARK_CREATE_RANGE_DATAFRAME = """
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
val rdd = sc.parallelize(0 to 99)
val schema = StructType(List(
    StructField("value", IntegerType, nullable = false)
))
val df = spark.createDataFrame(rdd.map { i => Row(i) }, schema)
"""
SPARK_MULTIPLY_DATAFRAME = """
val multiplied = uploaded.select($"value" * 2 alias "value")
"""
SPARK_TRIM_DATAFRAME = """
val trimmed = text.select(trim($"text") alias "text")
"""
SPARK_TEST_PARAMETERS = Parameters(
    print_foo_code='println("foo")',
    print_foo_output="foo\n\n",
    create_dataframe_code=SPARK_CREATE_RANGE_DATAFRAME,
    dataframe_count_code="df.count()",
    dataframe_count_output="res1: Long = 100\n\n",
    error_code="1 / 0",
    dataframe_multiply_code=SPARK_MULTIPLY_DATAFRAME,
    dataframe_trim_code=SPARK_TRIM_DATAFRAME,
)

PYSPARK_CREATE_RANGE_DATAFRAME = """
from pyspark.sql import Row
df = spark.createDataFrame([Row(value=i) for i in range(100)])
"""
PYSPARK_MULTIPLY_DATAFRAME = """
multiplied = uploaded.select((uploaded.value * 2).alias("value"))
"""
PYSPARK_TRIM_DATAFRAME = """
from pyspark.sql.functions import trim
trimmed = text.select(trim(text.text).alias("text"))
"""
PYSPARK_TEST_PARAMETERS = Parameters(
    print_foo_code='print("foo")',
    print_foo_output="foo\n",
    create_dataframe_code=PYSPARK_CREATE_RANGE_DATAFRAME,
    dataframe_count_code="df.count()",
    dataframe_count_output="100\n",
    error_code="1 / 0",
    dataframe_multiply_code=PYSPARK_MULTIPLY_DATAFRAME,
    dataframe_trim_code=PYSPARK_TRIM_DATAFRAME,
)

SPARKR_CREATE_RANGE_DATAFRAME = """
df <- createDataFrame(data.frame(value = 0:99))
"""
SPARKR_MULTIPLY_DATAFRAME = """
multiplied <- select(uploaded, alias(uploaded$value * 2L, "value"))
"""
SPARKR_TRIM_DATAFRAME = """
trimmed <- select(text, alias(trim(text$text), "text"))
"""
SPARKR_TEST_PARAMETERS = Parameters(
    print_foo_code='print("foo")',
    print_foo_output='[1] "foo"\n',
    create_dataframe_code=SPARKR_CREATE_RANGE_DATAFRAME,
    dataframe_count_code="count(df)",
    dataframe_count_output="[1] 100\n",
    error_code="missing_function()",
    dataframe_multiply_code=SPARKR_MULTIPLY_DATAFRAME,
    dataframe_trim_code=SPARKR_TRIM_DATAFRAME,
)

SQL_CREATE_VIEW = """
CREATE TEMPORARY VIEW view AS SELECT id AS value FROM RANGE(100)
"""


@pytest.mark.integration
@pytest.mark.parametrize(
    "session_kind, params",
    [
        (SessionKind.SPARK, SPARK_TEST_PARAMETERS),
        (SessionKind.PYSPARK, PYSPARK_TEST_PARAMETERS),
        (SessionKind.SPARKR, SPARKR_TEST_PARAMETERS),
    ],
)
def test_session(integration_url, capsys, session_kind, params):

    assert _livy_available(integration_url)

    with LivySession.create(integration_url, kind=session_kind) as session:

        assert session.state == SessionState.IDLE

        session.run(params.print_foo_code)
        assert capsys.readouterr() == (params.print_foo_output, "")

        session.run(params.create_dataframe_code)
        capsys.readouterr()

        session.run(params.dataframe_count_code)
        assert capsys.readouterr() == (params.dataframe_count_output, "")

        with pytest.raises(SparkRuntimeError):
            session.run(params.error_code)

        assert session.download("df").equals(RANGE_DATAFRAME)

        session.upload("uploaded", RANGE_DATAFRAME)
        session.run(params.dataframe_multiply_code)
        assert session.download("multiplied").equals(RANGE_DATAFRAME * 2)

        session.upload("text", TEXT_DATAFRAME)
        session.run(params.dataframe_trim_code)
        assert session.download("trimmed").equals(
            TEXT_DATAFRAME.applymap(lambda s: s.strip())
        )

    assert _session_stopped(integration_url, session.session_id)


@pytest.mark.integration
def test_sql_session(integration_url):

    assert _livy_available(integration_url)

    with LivySession.create(integration_url, kind=SessionKind.SQL) as session:

        assert session.state == SessionState.IDLE

        session.run(SQL_CREATE_VIEW)
        output = session.run("SELECT COUNT(*) FROM view")
        assert output.json["data"] == [[100]]

        with pytest.raises(SparkRuntimeError):
            session.run("not valid SQL!")

        assert session.download_sql("SELECT * FROM view").equals(
            RANGE_DATAFRAME
        )

    assert _session_stopped(integration_url, session.session_id)


@pytest.mark.integration
def test_batch_job(integration_url):

    assert _livy_available(integration_url)

    batch = LivyBatch.create(
        integration_url,
        file=(
            "https://repo.typesafe.com/typesafe/maven-releases/org/apache/"
            "spark/spark-examples_2.11/1.6.0-typesafe-001/"
            "spark-examples_2.11-1.6.0-typesafe-001.jar"
        ),
        class_name="org.apache.spark.examples.SparkPi",
    )

    assert batch.state == SessionState.RUNNING

    batch.wait()

    assert batch.state == SessionState.SUCCESS
    assert any(
        "spark.SparkContext: Successfully stopped SparkContext" in line
        for line in batch.log()
    )


def _livy_available(livy_url):
    return requests.get(livy_url).status_code == 200


def _session_stopped(livy_url, session_id):
    response = requests.get(f"{livy_url}/session/{session_id}")
    if response.status_code == 404:
        return True
    else:
        return response.get_json()["state"] == "shutting_down"
