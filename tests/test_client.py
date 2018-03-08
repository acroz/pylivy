import pytest
from aiohttp.web import Application, json_response

from livy.client import LivyClient
from livy.models import Session, SessionKind, Statement, StatementKind


@pytest.mark.asyncio
async def test_list_sessions(mocker, aiohttp_server):

    # Mock deserialisation of response
    mock_session_json = {'mock': 'session'}
    mocker.patch.object(Session, 'from_json')

    async def list_sessions(request):
        return json_response({'sessions': [mock_session_json]})

    app = Application()
    app.router.add_get('/sessions', list_sessions)
    server = await aiohttp_server(app)

    async with server:
        client = LivyClient(str(server.make_url('/')))
        sessions = await client.list_sessions()

    assert sessions == [Session.from_json.return_value]
    Session.from_json.assert_called_once_with(mock_session_json)


@pytest.mark.asyncio
async def test_get_session(mocker, aiohttp_server):

    # Mock deserialisation of response
    mock_session_json = {'mock': 'session'}
    mocker.patch.object(Session, 'from_json')

    async def get_session(request):
        return json_response(mock_session_json)

    app = Application()
    app.router.add_get('/sessions/5', get_session)
    server = await aiohttp_server(app)

    async with server:
        client = LivyClient(str(server.make_url('/')))
        session = await client.get_session(5)

    assert session == Session.from_json.return_value
    Session.from_json.assert_called_once_with(mock_session_json)


@pytest.mark.asyncio
async def test_create_session(mocker, aiohttp_server):

    # Mock deserialisation of response
    mock_session_json = {'mock': 'session'}
    mocker.patch.object(Session, 'from_json')

    async def version(request):
        return json_response({'version': '0.5.0-incubating'})

    async def create_session(request):
        assert (await request.json()) == {'kind': 'pyspark'}
        return json_response(mock_session_json)

    app = Application()
    app.router.add_get('/version', version)
    app.router.add_post('/sessions', create_session)
    server = await aiohttp_server(app)

    async with server:
        client = LivyClient(str(server.make_url('/')))
        session = await client.create_session(SessionKind.PYSPARK)

    assert session == Session.from_json.return_value
    Session.from_json.assert_called_once_with(mock_session_json)


@pytest.mark.asyncio
async def test_create_statement(mocker, aiohttp_server):

    session_id = 5
    code = 'some code'
    mock_statement_json = {'mock': 'statement'}
    mocker.patch.object(Statement, 'from_json')

    async def version(request):
        return json_response({'version': '0.5.0-incubating'})

    async def create_statement(request):
        assert (await request.json()) == {'code': code, 'kind': 'pyspark'}
        return json_response(mock_statement_json)

    app = Application()
    app.router.add_get('/version', version)
    app.router.add_post(f'/sessions/{session_id}/statements', create_statement)
    server = await aiohttp_server(app)

    async with server:
        client = LivyClient(str(server.make_url('/')))
        session = await client.create_statement(
            session_id, code, StatementKind.PYSPARK
        )

    assert session == Statement.from_json.return_value
    Statement.from_json.assert_called_once_with(
        session_id, mock_statement_json
    )
