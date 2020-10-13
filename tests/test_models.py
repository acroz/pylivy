import pytest

from livy.models import (
    Version,
    Session,
    SessionKind,
    SessionState,
    Statement,
    StatementState,
    Output,
    OutputStatus,
    SparkRuntimeError,
    BatchLog,
    Batch,
)


@pytest.mark.parametrize(
    "earlier, later",
    [
        ("0.1.0", "0.2.0"),
        ("0.1.1", "0.2.0"),
        ("1.9.0", "2.0.0"),
        ("0.1.0", "0.1.1-withsuffix"),
        ("0.1.0-suffix", "0.1.1"),
    ],
)
def test_version_less_than(earlier, later):
    assert Version(earlier) < Version(later)


@pytest.mark.parametrize(
    "first, second",
    [
        ("0.1.0", "0.1.0"),
        ("0.1.0", "0.1.0-withsuffix"),
        ("0.1.0-suffix", "0.1.0"),
    ],
)
def test_version_equals(first, second):
    assert Version(first) == Version(second)


def test_session_from_json():

    session_json = {
        "id": 5,
        "proxyUser": "user",
        "kind": "pyspark",
        "state": "idle",
    }

    expected = Session(5, "user", SessionKind.PYSPARK, SessionState.IDLE)

    assert Session.from_json(session_json) == expected


def test_statement_from_json_no_output():

    session_id = 5
    statement_json = {
        "id": 10,
        "state": "running",
        "code": "dummy code",
        "output": None,
        "progress": 0.0,
    }

    expected = Statement(
        session_id,
        statement_id=10,
        state=StatementState.RUNNING,
        code="dummy code",
        output=None,
        progress=0.0,
    )

    assert Statement.from_json(session_id, statement_json) == expected


def test_statement_from_json_with_output(mocker):

    mocker.patch.object(Output, "from_json")

    session_id = 5
    statement_json = {
        "id": 10,
        "state": "running",
        "code": "dummy code",
        "output": "dummy output",
        "progress": 0.5,
    }

    expected = Statement(
        session_id,
        statement_id=10,
        state=StatementState.RUNNING,
        code="dummy code",
        output=Output.from_json.return_value,
        progress=0.5,
    )

    assert Statement.from_json(session_id, statement_json) == expected
    Output.from_json.assert_called_once_with("dummy output")


def test_statement_from_json_no_progress(mocker):

    mocker.patch.object(Output, "from_json")

    session_id = 5
    statement_json = {
        "id": 10,
        "state": "running",
        "code": "dummy code",
        "output": "dummy output",
        "progress": None,
    }

    expected = Statement(
        session_id,
        statement_id=10,
        state=StatementState.RUNNING,
        code="dummy code",
        output=Output.from_json.return_value,
        progress=None,
    )

    assert Statement.from_json(session_id, statement_json) == expected


def test_statement_from_json_with_progress(mocker):

    mocker.patch.object(Output, "from_json")

    session_id = 5
    statement_json = {
        "id": 10,
        "state": "running",
        "code": "dummy code",
        "output": "dummy output",
        "progress": 0.5,
    }

    expected = Statement(
        session_id,
        statement_id=10,
        state=StatementState.RUNNING,
        code="dummy code",
        output=Output.from_json.return_value,
        progress=0.5,
    )

    assert Statement.from_json(session_id, statement_json) == expected


def test_output_textdata_from_json():

    output_json = {"status": "ok", "data": {"text/plain": "some output"}}

    expected = Output(
        OutputStatus.OK,
        text="some output",
        json=None,
        ename=None,
        evalue=None,
        traceback=None,
    )

    assert Output.from_json(output_json) == expected


def test_output_jsondata_from_json():

    output_json = {
        "status": "ok",
        "data": {"application/json": {"some": "data"}},
    }

    expected = Output(
        OutputStatus.OK,
        text=None,
        json={"some": "data"},
        ename=None,
        evalue=None,
        traceback=None,
    )

    assert Output.from_json(output_json) == expected


def test_output_error_from_json():

    output_json = {
        "status": "error",
        "ename": "SomeException",
        "evalue": "some error value",
        "traceback": ["traceback line 1", "traceback line 2"],
    }

    expected = Output(
        OutputStatus.ERROR,
        text=None,
        json=None,
        ename="SomeException",
        evalue="some error value",
        traceback=["traceback line 1", "traceback line 2"],
    )

    assert Output.from_json(output_json) == expected


def test_output_raise_for_status():
    ok_output = Output(OutputStatus.OK, None, None, None, None, None)
    ok_output.raise_for_status()

    error_output = Output(OutputStatus.ERROR, None, None, None, None, None)
    with pytest.raises(SparkRuntimeError):
        error_output.raise_for_status()


def test_batch_from_json():
    batch_json = {
        "id": 2398,
        "appId": "application_000000000000_000001",
        "appInfo": {"key1": "val1", "key2": "val2"},
        "log": ["log1", "log2"],
        "state": "running",
    }

    expected = Batch(
        batch_id=2398,
        app_id="application_000000000000_000001",
        app_info={"key1": "val1", "key2": "val2"},
        log=["log1", "log2"],
        state=SessionState.RUNNING,
    )

    assert Batch.from_json(batch_json) == expected


def test_batch_from_json_no_optionals():
    batch_json = {
        "id": 2398,
        "state": "starting",
    }

    expected = Batch(
        batch_id=2398,
        app_id=None,
        app_info=None,
        log=[],
        state=SessionState.STARTING,
    )

    assert Batch.from_json(batch_json) == expected


def test_batch_log_from_json():
    batch_log_json = {
        "id": 2398,
        "from": 100,
        "total": 100,
        "log": ["log1", "log2"],
    }

    expected = BatchLog(
        batch_id=2398, from_=100, total=100, lines=["log1", "log2"]
    )

    assert BatchLog.from_json(batch_log_json) == expected


def test_batch_log_from_json_no_log():
    batch_log_json = {"id": 2398, "from": 0, "total": 100}

    expected = BatchLog(batch_id=2398, from_=0, total=100, lines=[])

    assert BatchLog.from_json(batch_log_json) == expected
