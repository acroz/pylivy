import time
import multiprocessing

import pytest
from flask import Flask, request, jsonify

from livy.client import LivyClient
from livy.models import Session, SessionKind, Statement, StatementKind


MOCK_SESSION_JSON = {'mock': 'session'}
MOCK_SESSION_ID = 5
MOCK_STATEMENT_JSON = {'mock': 'statement'}
MOCK_STATEMENT_ID = 12
MOCK_CODE = 'mock code'
MOCK_SPARK_CONF = {
    "spark.master": "yarn",
    "spark.submit.deployMode": "client"
}


def mock_livy_server():

    app = Flask(__name__)

    @app.route('/version')
    def version():
        return jsonify({'version': '0.5.0-incubating'})

    @app.route('/sessions')
    def list_sessions():
        return jsonify({'sessions': [MOCK_SESSION_JSON]})

    @app.route(f'/sessions/{MOCK_SESSION_ID}')
    def get_session():
        return jsonify(MOCK_SESSION_JSON)

    @app.route('/sessions', methods=['POST'])
    def create_session():
        assert request.get_json() == {
            'kind': 'pyspark',
            'conf': MOCK_SPARK_CONF
        }
        return jsonify(MOCK_SESSION_JSON)

    @app.route(f'/sessions/{MOCK_SESSION_ID}', methods=['DELETE'])
    def delete_session():
        return jsonify({'msg': 'deleted'})

    @app.route(f'/sessions/{MOCK_SESSION_ID}/statements')
    def list_statements():
        return jsonify({'statements': [MOCK_STATEMENT_JSON]})

    @app.route(f'/sessions/{MOCK_SESSION_ID}/statements/{MOCK_STATEMENT_ID}')
    def get_statement():
        return jsonify(MOCK_STATEMENT_JSON)

    @app.route(f'/sessions/{MOCK_SESSION_ID}/statements', methods=['POST'])
    def create_statement():
        assert request.get_json() == {'code': MOCK_CODE, 'kind': 'pyspark'}
        return jsonify(MOCK_STATEMENT_JSON)

    app.run()


@pytest.fixture(scope='session')
def server():
    process = multiprocessing.Process(target=mock_livy_server)
    process.start()
    time.sleep(0.1)
    yield 'http://localhost:5000'
    process.terminate()


def test_list_sessions(mocker, server):

    mocker.patch.object(Session, 'from_json')

    client = LivyClient(server)
    sessions = client.list_sessions()

    assert sessions == [Session.from_json.return_value]
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)


def test_get_session(mocker, server):

    mocker.patch.object(Session, 'from_json')

    client = LivyClient(server)
    session = client.get_session(MOCK_SESSION_ID)

    assert session == Session.from_json.return_value
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)


def test_create_session(mocker, server):

    mocker.patch.object(Session, 'from_json')

    client = LivyClient(server)
    session = client.create_session(
        SessionKind.PYSPARK,
        spark_conf=MOCK_SPARK_CONF
    )

    assert session == Session.from_json.return_value
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)


def test_delete_session(mocker, server):
    client = LivyClient(server)
    client.delete_session(MOCK_SESSION_ID)


def test_list_statements(mocker, server):

    mocker.patch.object(Statement, 'from_json')

    client = LivyClient(server)
    statements = client.list_statements(MOCK_SESSION_ID)

    assert statements == [Statement.from_json.return_value]
    Statement.from_json.assert_called_once_with(
        MOCK_SESSION_ID,
        MOCK_STATEMENT_JSON
    )


def test_get_statement(mocker, server):

    mocker.patch.object(Statement, 'from_json')

    client = LivyClient(server)
    statement = client.get_statement(MOCK_SESSION_ID, MOCK_STATEMENT_ID)

    assert statement == Statement.from_json.return_value
    Statement.from_json.assert_called_once_with(
        MOCK_SESSION_ID,
        MOCK_STATEMENT_JSON
    )


def test_create_statement(mocker, server):

    mocker.patch.object(Statement, 'from_json')

    client = LivyClient(server)
    statement = client.create_statement(
        MOCK_SESSION_ID,
        MOCK_CODE,
        StatementKind.PYSPARK
    )

    assert statement == Statement.from_json.return_value
    Statement.from_json.assert_called_once_with(
        MOCK_SESSION_ID,
        MOCK_STATEMENT_JSON
    )
