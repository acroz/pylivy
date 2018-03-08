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


def test_output_from_json():

    output_json = {
        'status': 'ok',
        'data': {'text/plain': 'some output'}
    }

    expected_output = Output(
        OutputStatus.OK, text='some output', json=None,
        ename=None, evalue=None, traceback=None
    )

    assert Output.from_json(output_json) == expected_output
