import os
import requests
import pytest
from livy import Livy, SessionManager, SessionState, SparkRuntimeError


LIVY_URL = os.environ.get('LIVY_TEST_URL', 'http://localhost:8998')


def livy_available():
    try:
        response = requests.get(LIVY_URL)
    except ConnectionError as e:
        return False
    return response.ok


def session_stopped(session_id):
    session = SessionManager(LIVY_URL).get_session(session_id)
    if session is None:
        return True
    else:
        return session.state == SessionState.SHUTTING_DOWN


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

        session_id = client.session.id_

    assert session_stopped(session_id)
