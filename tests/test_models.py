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
    statement_json = {"id": 10, "state": "running", "output": None}

    expected = Statement(
        session_id, statement_id=10, state=StatementState.RUNNING, output=None
    )

    assert Statement.from_json(session_id, statement_json) == expected


def test_statement_from_json_with_output(mocker):

    mocker.patch.object(Output, "from_json")

    session_id = 5
    statement_json = {"id": 10, "state": "running", "output": "dummy output"}

    expected = Statement(
        session_id,
        statement_id=10,
        state=StatementState.RUNNING,
        output=Output.from_json.return_value,
    )

    assert Statement.from_json(session_id, statement_json) == expected
    Output.from_json.assert_called_once_with("dummy output")


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
