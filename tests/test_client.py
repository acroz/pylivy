import pytest
from aiohttp.web import Application, json_response

from livy.client import LivyClient
from livy.models import Session


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
