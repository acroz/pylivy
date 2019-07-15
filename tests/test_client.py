from livy.client import LivyClient
from livy.models import Session, SessionKind, Statement, StatementKind, Batch, BatchLog


MOCK_SESSION_JSON = {"mock": "session"}
MOCK_SESSION_ID = 5
MOCK_STATEMENT_JSON = {"mock": "statement"}
MOCK_STATEMENT_ID = 12
MOCK_CODE = "mock code"
MOCK_PROXY_USER = "proxy-user"
MOCK_SPARK_CONF = {"spark.master": "yarn", "spark.submit.deployMode": "client"}
MOCK_JARS = ["mock1.jar", "mock2.jar"]
MOCK_PY_FILES = ["mock1.py", "mock2.py"]
MOCK_FILES = ["mockfile1.txt", "mockfile2.txt"]
MOCK_DRIVER_MEMORY = "512m"
MOCK_DRIVER_CORES = 2
MOCK_EXECUTOR_MEMORY = "1024m"
MOCK_EXECUTOR_CORES = 4
MOCK_NUM_EXECUTORS = 6
MOCK_ARCHIVES = ["mock1.tar.gz", "mock2.tar.gz"]
MOCK_QUEUE = "mock-queue"
MOCK_NAME = "mock-session-name"
MOCK_BATCH_JSON = {"mock": "batch"}
MOCK_BATCH_FILE = "/opt/app/app.jar"
MOCK_BATCH_CLASSNAME = "com.example.application"
MOCK_BATCH_ARGS = ["--arg1=1", "--arg2=2"]
MOCK_BATCH_ID = 2398
MOCK_BATCH_LOG_JSON = {"mock": "batch_log"}


def test_list_sessions(requests_mock, mocker):
    requests_mock.get(
        "http://example.com/sessions", json={"sessions": [MOCK_SESSION_JSON]}
    )
    mocker.patch.object(Session, "from_json")

    client = LivyClient("http://example.com")
    sessions = client.list_sessions()

    assert sessions == [Session.from_json.return_value]
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)


def test_get_session(requests_mock, mocker):
    requests_mock.get(
        f"http://example.com/sessions/{MOCK_SESSION_ID}",
        json=MOCK_SESSION_JSON,
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
    requests_mock.post("http://example.com/sessions", json=MOCK_SESSION_JSON)
    mocker.patch.object(Session, "from_json")

    client = LivyClient("http://example.com")
    session = client.create_session(
        SessionKind.PYSPARK,
        proxy_user=MOCK_PROXY_USER,
        spark_conf=MOCK_SPARK_CONF,
        jars=MOCK_JARS,
        py_files=MOCK_PY_FILES,
        files=MOCK_FILES,
        driver_memory=MOCK_DRIVER_MEMORY,
        driver_cores=MOCK_DRIVER_CORES,
        executor_memory=MOCK_EXECUTOR_MEMORY,
        executor_cores=MOCK_EXECUTOR_CORES,
        num_executors=MOCK_NUM_EXECUTORS,
        archives=MOCK_ARCHIVES,
        queue=MOCK_QUEUE,
        name=MOCK_NAME,
    )

    assert session == Session.from_json.return_value
    Session.from_json.assert_called_once_with(MOCK_SESSION_JSON)
    assert requests_mock.last_request.json() == {
        "kind": "pyspark",
        "proxyUser": MOCK_PROXY_USER,
        "conf": MOCK_SPARK_CONF,
        "jars": MOCK_JARS,
        "pyFiles": MOCK_PY_FILES,
        "files": MOCK_FILES,
        "driverMemory": MOCK_DRIVER_MEMORY,
        "driverCores": MOCK_DRIVER_CORES,
        "executorMemory": MOCK_EXECUTOR_MEMORY,
        "executorCores": MOCK_EXECUTOR_CORES,
        "numExecutors": MOCK_NUM_EXECUTORS,
        "archives": MOCK_ARCHIVES,
        "queue": MOCK_QUEUE,
        "name": MOCK_NAME,
    }


def test_delete_session(requests_mock):
    requests_mock.delete(
        f"http://example.com/sessions/{MOCK_SESSION_ID}",
        json={"msg": "deleted"},
    )

    client = LivyClient("http://example.com")
    client.delete_session(MOCK_SESSION_ID)

    assert requests_mock.called


def test_list_statements(requests_mock, mocker):
    requests_mock.get(
        f"http://example.com/sessions/{MOCK_SESSION_ID}/statements",
        json={"statements": [MOCK_STATEMENT_JSON]},
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
        f"http://example.com/sessions/{MOCK_SESSION_ID}"
        + f"/statements/{MOCK_STATEMENT_ID}",
        json=MOCK_STATEMENT_JSON,
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
        json=MOCK_STATEMENT_JSON,
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
        "code": MOCK_CODE,
        "kind": "pyspark",
    }


def test_create_batch(requests_mock, mocker):
    requests_mock.get(
        "http://example.com/version", json={"version": "0.5.0-incubating"}
    )
    requests_mock.post("http://example.com/batches", json=MOCK_BATCH_JSON)
    mocker.patch.object(Batch, "from_json")

    client = LivyClient("http://example.com")
    batch = client.create_batch(
        file=MOCK_BATCH_FILE,
        class_name=MOCK_BATCH_CLASSNAME,
        args=MOCK_BATCH_ARGS,
        proxy_user=MOCK_PROXY_USER,
        jars=MOCK_JARS,
        py_files=MOCK_PY_FILES,
        files=MOCK_FILES,
        driver_memory=MOCK_DRIVER_MEMORY,
        driver_cores=MOCK_DRIVER_CORES,
        executor_memory=MOCK_EXECUTOR_MEMORY,
        executor_cores=MOCK_EXECUTOR_CORES,
        num_executors=MOCK_NUM_EXECUTORS,
        archives=MOCK_ARCHIVES,
        queue=MOCK_QUEUE,
        name=MOCK_NAME,
        spark_conf=MOCK_SPARK_CONF,
    )

    assert batch == Batch.from_json.return_value
    Batch.from_json.assert_called_once_with(MOCK_BATCH_JSON)
    assert requests_mock.last_request.json() == {
        "file": MOCK_BATCH_FILE,
        "proxyUser": MOCK_PROXY_USER,
        "className": MOCK_BATCH_CLASSNAME,
        "args": MOCK_BATCH_ARGS,
        "jars": MOCK_JARS,
        "pyFiles": MOCK_PY_FILES,
        "files": MOCK_FILES,
        "driverMemory": MOCK_DRIVER_MEMORY,
        "driverCores": MOCK_DRIVER_CORES,
        "executorMemory": MOCK_EXECUTOR_MEMORY,
        "executorCores": MOCK_EXECUTOR_CORES,
        "numExecutors": MOCK_NUM_EXECUTORS,
        "archives": MOCK_ARCHIVES,
        "queue": MOCK_QUEUE,
        "name": MOCK_NAME,
        "conf": MOCK_SPARK_CONF,
    }


def test_get_batch(requests_mock, mocker):
    requests_mock.get(
        f"http://example.com/batches/{MOCK_BATCH_ID}",
        json=MOCK_BATCH_JSON,
    )
    mocker.patch.object(Batch, "from_json")

    client = LivyClient("http://example.com")
    batch = client.get_batch(MOCK_BATCH_ID)

    assert batch == Batch.from_json.return_value
    Batch.from_json.assert_called_once_with(MOCK_BATCH_JSON)


def test_get_batch_log(requests_mock, mocker):
    requests_mock.get(
        f"http://example.com/batches/{MOCK_BATCH_ID}/log",
        json=MOCK_BATCH_LOG_JSON,
    )
    mocker.patch.object(BatchLog, "from_json")

    client = LivyClient("http://example.com")
    batch = client.get_batch_log(MOCK_BATCH_ID)

    assert batch == BatchLog.from_json.return_value
    BatchLog.from_json.assert_called_once_with(MOCK_BATCH_LOG_JSON)
