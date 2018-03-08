from livy.models import Session, SessionKind, SessionState


def test_session_from_json():

    session_json = {
        'id': 5,
        'kind': 'pyspark',
        'state': 'idle'
    }

    expected_session = Session(5, SessionKind.PYSPARK, SessionState.IDLE)

    assert Session.from_json(session_json) == expected_session
