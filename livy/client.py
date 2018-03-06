import logging

import aiohttp

from livy.models import Version, Session, SessionKind, Statement


LOGGER = logging.getLogger(__name__)


VALID_LEGACY_SESSION_KINDS = {
    SessionKind.SPARK, SessionKind.PYSPARK, SessionKind.PYSPARK3,
    SessionKind.SPARKR
}
VALID_SESSION_KINDS = {
    SessionKind.SPARK, SessionKind.PYSPARK, SessionKind.SPARKR,
    SessionKind.SQL, SessionKind.SHARED
}


class JsonClient:

    def __init__(self, url):
        self.url = url
        self._session_cache = None
        self._server_version_cache = None

    @property
    def session(self):
        if self._session_cache is None:
            self._session_cache = aiohttp.ClientSession()
        return self._session_cache

    async def close(self):
        await self.session.close()
        self._http_session = None

    async def get(self, endpoint=''):
        return await self._request('GET', endpoint)

    async def post(self, endpoint, data=None):
        return await self._request('POST', endpoint, data)

    async def delete(self, endpoint=''):
        return await self._request('DELETE', endpoint)

    async def _request(self, method, endpoint, data=None):
        url = self.url.rstrip('/') + endpoint
        async with self.session.request(method, url, json=data) as response:
            response.raise_for_status()
            response_data = await response.json()
        return response_data


class LivyClient:

    def __init__(self, url):
        self._client = JsonClient(url)
        self._server_version_cache = None

    async def close(self):
        await self._client.close()

    async def server_version(self):
        if self._server_version_cache is None:
            data = await self._client.get('/version')
            self._server_version_cache = Version(data['version'])
        return self._server_version_cache

    async def legacy_server(self):
        version = await self.server_version()
        return version < Version('0.5.0-incubating')

    async def list_sessions(self):
        data = await self._client.get('/sessions')
        return [
            Session.from_json(data) for item in data['sessions']
        ]

    async def create_session(self, kind):

        if await self.legacy_server():
            valid_kinds = VALID_LEGACY_SESSION_KINDS
        else:
            valid_kinds = VALID_SESSION_KINDS

        if kind not in valid_kinds:
            raise ValueError(
                f'{kind} is not a valid session kind for a Livy server of '
                f'this version (should be one of {valid_kinds})'
            )

        data = await self._client.post('/sessions', data={'kind': kind.value})
        return Session.from_json(data)

    async def get_session(self, session_id):
        try:
            data = await self._client.get(f'/sessions/{session_id}')
        except aiohttp.ClientResponseError as e:
            if e.code == 404:
                return None
            else:
                raise
        return Session.from_json(data)

    async def delete_session(self, session_id):
        await self._client.delete(f'/sessions/{session_id}')

    async def list_statements(self, session_id):
        response = await self._client.get(f'/sessions/{session_id}/statements')
        return [
            Statement.from_json(session_id, data)
            for data in response['statements']
        ]

    async def create_statement(self, session_id, code, kind=None):

        data = {'code': code}

        if kind is not None:
            if await self.legacy_server():
                LOGGER.warning('statement kind ignored on Livy<0.5.0')
            data['kind'] = kind.value

        response = await self._client.post(
            f'/sessions/{session_id}/statements',
            data=data
        )
        return Statement.from_json(session_id, response)

    async def get_statement(self, session_id, statement_id):
        response = await self._client.get(
            f'/sessions/{session_id}/statements/{statement_id}'
        )
        return Statement.from_json(session_id, response)
