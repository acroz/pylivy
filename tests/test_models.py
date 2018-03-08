from livy.models import (
    Session, SessionKind, SessionState, Output, OutputStatus
)


def test_session_from_json():

    session_json = {
        'id': 5,
        'kind': 'pyspark',
        'state': 'idle'
    }

    expected_session = Session(5, SessionKind.PYSPARK, SessionState.IDLE)

    assert Session.from_json(session_json) == expected_session


def test_output_textdata_from_json():

    output_json = {
        'status': 'ok',
        'data': {'text/plain': 'some output'}
    }

    expected_output = Output(
        OutputStatus.OK, text='some output', json=None,
        ename=None, evalue=None, traceback=None
    )

    assert Output.from_json(output_json) == expected_output


def test_output_jsondata_from_json():

    output_json = {
        'status': 'ok',
        'data': {'application/json': {'some': 'data'}}
    }

    expected_output = Output(
        OutputStatus.OK, text=None, json={'some': 'data'},
        ename=None, evalue=None, traceback=None
    )

    assert Output.from_json(output_json) == expected_output


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

    expected_output = Output(
        OutputStatus.ERROR, text=None, json=None,
        ename='SomeException', evalue='some error value',
        traceback=[
            'traceback line 1',
            'traceback line 2'
        ]
    )

    assert Output.from_json(output_json) == expected_output
