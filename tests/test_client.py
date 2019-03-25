from livy.client import LivyClient
from livy.models import Session, SessionKind, Statement, StatementKind


MOCK_SESSION_JSON = {"mock": "session"}
MOCK_SESSION_ID = 5
MOCK_STATEMENT_JSON = {"mock": "statement"}
MOCK_STATEMENT_ID = 12
MOCK_CODE = "mock code"
MOCK_PROXY_USER = "proxy-user"
MOCK_SPARK_CONF = {"spark.master": "yarn", "spark.submit.deployMode": "client"}


def test_list_sessions(requests_mock, mocker):
    requests_mock.get(
        "http://example.com/sessions",
        json={"sessions": [MOCK_SESSION_JSON]}
    )
    mocker.patch.object(Session, "from_json")

    client = LivyClient("http://example.com")
    sessions = client.list_sessions()

    assert sessions == [Session.from_json.return_value]
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)


def test_get_session(requests_mock, mocker):
    requests_mock.get(
        f"http://example.com/sessions/{MOCK_SESSION_ID}",
        json=MOCK_SESSION_JSON
    )
    mocker.patch.object(Session, "from_json")

    client = LivyClient("http://example.com")
    session = client.get_session(MOCK_SESSION_ID)

    assert session == Session.from_json.return_value
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)


def test_create_session(requests_mock, mocker):
    requests_mock.get(
        "http://example.com/version", json={"version": "0.5.0-incubating"}
    )
    requests_mock.post(
        "http://example.com/sessions",
        json=MOCK_SESSION_JSON
    )
    mocker.patch.object(Session, "from_json")

    client = LivyClient("http://example.com")
    session = client.create_session(
        SessionKind.PYSPARK,
        proxy_user=MOCK_PROXY_USER,
        spark_conf=MOCK_SPARK_CONF,
    )

    assert session == Session.from_json.return_value
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)
    assert requests_mock.last_request.json() == {
        "kind": "pyspark",
        "proxyUser": MOCK_PROXY_USER,
        "conf": MOCK_SPARK_CONF,
    }


def test_delete_session(requests_mock):
    requests_mock.delete(
        f"http://example.com/sessions/{MOCK_SESSION_ID}",
        json={"msg": "deleted"}
    )

    client = LivyClient("http://example.com")
    client.delete_session(MOCK_SESSION_ID)

    assert requests_mock.called


def test_list_statements(requests_mock, mocker):
    requests_mock.get(
        f"http://example.com/sessions/{MOCK_SESSION_ID}/statements",
        json={"statements": [MOCK_STATEMENT_JSON]}
    )
    mocker.patch.object(Statement, "from_json")

    client = LivyClient("http://example.com")
    statements = client.list_statements(MOCK_SESSION_ID)

    assert statements == [Statement.from_json.return_value]
    Statement.from_json.assert_called_once_with(
        MOCK_SESSION_ID, MOCK_STATEMENT_JSON
    )


def test_get_statement(requests_mock, mocker):
    requests_mock.get(
        f"http://example.com/sessions/{MOCK_SESSION_ID}" +
        f"/statements/{MOCK_STATEMENT_ID}",
        json=MOCK_STATEMENT_JSON
    )
    mocker.patch.object(Statement, "from_json")

    client = LivyClient("http://example.com")
    statement = client.get_statement(MOCK_SESSION_ID, MOCK_STATEMENT_ID)

    assert statement == Statement.from_json.return_value
    Statement.from_json.assert_called_once_with(
        MOCK_SESSION_ID, MOCK_STATEMENT_JSON
    )


def test_create_statement(requests_mock, mocker):
    requests_mock.get(
        "http://example.com/version", json={"version": "0.5.0-incubating"}
    )
    requests_mock.post(
        f"http://example.com/sessions/{MOCK_SESSION_ID}/statements",
        json=MOCK_STATEMENT_JSON
    )
    mocker.patch.object(Statement, "from_json")

    client = LivyClient("http://example.com")
    statement = client.create_statement(
        MOCK_SESSION_ID, MOCK_CODE, StatementKind.PYSPARK
    )

    assert statement == Statement.from_json.return_value
    Statement.from_json.assert_called_once_with(
        MOCK_SESSION_ID, MOCK_STATEMENT_JSON
    )
    assert requests_mock.last_request.json() == {
        "code": MOCK_CODE, "kind": "pyspark"
    }
