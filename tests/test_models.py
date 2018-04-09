import pytest

from livy.models import (
    Version,
    Session, SessionKind, SessionState,
    Statement, StatementState,
    Output, OutputStatus
)


@pytest.mark.parametrize('earlier, later', [
    ('0.1.0', '0.2.0'),
    ('0.1.1', '0.2.0'),
    ('1.9.0', '2.0.0'),
    ('0.1.0', '0.1.1-withsuffix'),
    ('0.1.0-suffix', '0.1.1')
])
def test_version(earlier, later):
    assert Version(earlier) < Version(later)


def test_session_from_json():

    session_json = {
        'id': 5,
        'kind': 'pyspark',
        'state': 'idle'
    }

    expected = Session(5, SessionKind.PYSPARK, SessionState.IDLE)

    assert Session.from_json(session_json) == expected


def test_statement_from_json_no_output():

    session_id = 5
    statement_json = {
        'id': 10,
        'state': 'running',
        'output': None
    }

    expected = Statement(
        session_id, statement_id=10, state=StatementState.RUNNING, output=None
    )

    assert Statement.from_json(session_id, statement_json) == expected


def test_statement_from_json_with_output(mocker):

    mocker.patch.object(Output, 'from_json')

    session_id = 5
    statement_json = {
        'id': 10,
        'state': 'running',
        'output': 'dummy output'
    }

    expected = Statement(
        session_id, statement_id=10, state=StatementState.RUNNING,
        output=Output.from_json.return_value
    )

    assert Statement.from_json(session_id, statement_json) == expected
    Output.from_json.assert_called_once_with('dummy output')


def test_output_textdata_from_json():

    output_json = {
        'status': 'ok',
        'data': {'text/plain': 'some output'}
    }

    expected = Output(
        OutputStatus.OK, text='some output', json=None,
        ename=None, evalue=None, traceback=None
    )

    assert Output.from_json(output_json) == expected


def test_output_jsondata_from_json():

    output_json = {
        'status': 'ok',
        'data': {'application/json': {'some': 'data'}}
    }

    expected = Output(
        OutputStatus.OK, text=None, json={'some': 'data'},
        ename=None, evalue=None, traceback=None
    )

    assert Output.from_json(output_json) == expected


def test_output_error_from_json():

    output_json = {
        'status': 'error',
        'ename': 'SomeException',
        'evalue': 'some error value',
        'traceback': [
            'traceback line 1',
            'traceback line 2'
        ]
    }

    expected = Output(
        OutputStatus.ERROR, text=None, json=None,
        ename='SomeException', evalue='some error value',
        traceback=[
            'traceback line 1',
            'traceback line 2'
        ]
    )

    assert Output.from_json(output_json) == expected
