from enum import Enum
import time
import multiprocessing

import pytest
from flask import Flask, request, jsonify
from requests.auth import HTTPBasicAuth

from livy.client import LivyClient
from livy.models import Session, SessionKind, Statement, StatementKind


MOCK_SESSION_JSON = {"mock": "session"}
MOCK_SESSION_ID = 5
MOCK_STATEMENT_JSON = {"mock": "statement"}
MOCK_STATEMENT_ID = 12
MOCK_CODE = "mock code"
MOCK_PROXY_USER = "proxy-user"
MOCK_SPARK_CONF = {"spark.master": "yarn", "spark.submit.deployMode": "client"}


class AuthType(Enum):
    NONE = 0
    BASIC_AUTH_TUPLE = 1
    BASIC_AUTH = 2


BASIC_AUTH_USER = "basic-auth-user"
BASIC_AUTH_PASSWORD = "basic-auth-password"


REQUESTS_AUTH_VALUES = {
    AuthType.NONE: None,
    AuthType.BASIC_AUTH_TUPLE: (BASIC_AUTH_USER, BASIC_AUTH_PASSWORD),
    AuthType.BASIC_AUTH: HTTPBasicAuth(BASIC_AUTH_USER, BASIC_AUTH_PASSWORD),
}


def validate_basic_auth(request):
    assert request.authorization is not None
    assert request.authorization.username == BASIC_AUTH_USER
    assert request.authorization.password == BASIC_AUTH_PASSWORD


FLASK_AUTH_VALIDATORS = {
    AuthType.NONE: lambda _: None,
    AuthType.BASIC_AUTH_TUPLE: validate_basic_auth,
    AuthType.BASIC_AUTH: validate_basic_auth,
}


def mock_livy_server(auth_type):

    app = Flask(__name__)

    @app.before_request
    def before_request():
        validator = FLASK_AUTH_VALIDATORS[auth_type]
        validator(request)

    @app.route("/version")
    def version():
        return jsonify({"version": "0.5.0-incubating"})

    @app.route("/sessions")
    def list_sessions():
        return jsonify({"sessions": [MOCK_SESSION_JSON]})

    @app.route(f"/sessions/{MOCK_SESSION_ID}")
    def get_session():
        return jsonify(MOCK_SESSION_JSON)

    @app.route("/sessions", methods=["POST"])
    def create_session():
        assert request.get_json() == {
            "kind": "pyspark",
            "proxyUser": MOCK_PROXY_USER,
            "conf": MOCK_SPARK_CONF,
        }
        return jsonify(MOCK_SESSION_JSON)

    @app.route(f"/sessions/{MOCK_SESSION_ID}", methods=["DELETE"])
    def delete_session():
        return jsonify({"msg": "deleted"})

    @app.route(f"/sessions/{MOCK_SESSION_ID}/statements")
    def list_statements():
        return jsonify({"statements": [MOCK_STATEMENT_JSON]})

    @app.route(f"/sessions/{MOCK_SESSION_ID}/statements/{MOCK_STATEMENT_ID}")
    def get_statement():
        return jsonify(MOCK_STATEMENT_JSON)

    @app.route(f"/sessions/{MOCK_SESSION_ID}/statements", methods=["POST"])
    def create_statement():
        assert request.get_json() == {"code": MOCK_CODE, "kind": "pyspark"}
        return jsonify(MOCK_STATEMENT_JSON)

    app.run()


@pytest.fixture(scope="session", params=AuthType, ids=lambda t: t.name)
def auth_type(request):
    return request.param


@pytest.fixture(scope="session")
def server(auth_type):
    process = multiprocessing.Process(
        target=mock_livy_server, args=(auth_type,)
    )
    process.start()
    time.sleep(0.1)
    yield "http://localhost:5000"
    process.terminate()


@pytest.fixture(scope="session")
def auth(auth_type):
    return REQUESTS_AUTH_VALUES[auth_type]


def test_list_sessions(mocker, server, auth):

    mocker.patch.object(Session, "from_json")

    client = LivyClient(server, auth)
    sessions = client.list_sessions()

    assert sessions == [Session.from_json.return_value]
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)


def test_get_session(mocker, server, auth):

    mocker.patch.object(Session, "from_json")

    client = LivyClient(server, auth)
    session = client.get_session(MOCK_SESSION_ID)

    assert session == Session.from_json.return_value
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)


def test_create_session(mocker, server, auth):

    mocker.patch.object(Session, "from_json")

    client = LivyClient(server, auth)
    session = client.create_session(
        SessionKind.PYSPARK,
        proxy_user=MOCK_PROXY_USER,
        spark_conf=MOCK_SPARK_CONF,
    )

    assert session == Session.from_json.return_value
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)


def test_delete_session(mocker, server, auth):
    client = LivyClient(server, auth)
    client.delete_session(MOCK_SESSION_ID)


def test_list_statements(mocker, server, auth):

    mocker.patch.object(Statement, "from_json")

    client = LivyClient(server, auth)
    statements = client.list_statements(MOCK_SESSION_ID)

    assert statements == [Statement.from_json.return_value]
    Statement.from_json.assert_called_once_with(
        MOCK_SESSION_ID, MOCK_STATEMENT_JSON
    )


def test_get_statement(mocker, server, auth):

    mocker.patch.object(Statement, "from_json")

    client = LivyClient(server, auth)
    statement = client.get_statement(MOCK_SESSION_ID, MOCK_STATEMENT_ID)

    assert statement == Statement.from_json.return_value
    Statement.from_json.assert_called_once_with(
        MOCK_SESSION_ID, MOCK_STATEMENT_JSON
    )


def test_create_statement(mocker, server, auth):

    mocker.patch.object(Statement, "from_json")

    client = LivyClient(server, auth)
    statement = client.create_statement(
        MOCK_SESSION_ID, MOCK_CODE, StatementKind.PYSPARK
    )

    assert statement == Statement.from_json.return_value
    Statement.from_json.assert_called_once_with(
        MOCK_SESSION_ID, MOCK_STATEMENT_JSON
    )
