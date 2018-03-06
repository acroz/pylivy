import logging
from typing import Optional, List

import aiohttp

from livy.models import Version, Session, SessionKind, Statement, StatementKind


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

    def __init__(self, url: str) -> None:
        self.url = url
        self._session_cache: Optional[aiohttp.ClientSession] = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session_cache is None:
            self._session_cache = aiohttp.ClientSession()
        return self._session_cache

    async def close(self) -> None:
        await self.session.close()
        self._session_cache = None

    async def get(self, endpoint: str='') -> dict:
        return await self._request('GET', endpoint)

    async def post(self, endpoint: str, data: dict=None) -> dict:
        return await self._request('POST', endpoint, data)

    async def delete(self, endpoint: str='') -> dict:
        return await self._request('DELETE', endpoint)

    async def _request(
        self, method: str, endpoint: str, data: dict=None
    ) -> dict:
        url = self.url.rstrip('/') + endpoint
        async with self.session.request(method, url, json=data) as response:
            response.raise_for_status()
            response_data = await response.json()
        return response_data


class LivyClient:

    def __init__(self, url: str) -> None:
        self._client = JsonClient(url)
        self._server_version_cache: Optional[Version] = None

    async def close(self) -> None:
        await self._client.close()

    async def server_version(self) -> Version:
        if self._server_version_cache is None:
            data = await self._client.get('/version')
            self._server_version_cache = Version(data['version'])
        return self._server_version_cache

    async def legacy_server(self) -> bool:
        version = await self.server_version()
        return version < Version('0.5.0-incubating')

    async def list_sessions(self) -> List[Session]:
        data = await self._client.get('/sessions')
        return [Session.from_json(item) for item in data['sessions']]

    async def create_session(self, kind: SessionKind) -> Session:

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

    async def get_session(self, session_id: int) -> Session:
        try:
            data = await self._client.get(f'/sessions/{session_id}')
        except aiohttp.ClientResponseError as e:
            if e.code == 404:
                return None
            else:
                raise
        return Session.from_json(data)

    async def delete_session(self, session_id: int) -> None:
        await self._client.delete(f'/sessions/{session_id}')

    async def list_statements(self, session_id: int) -> List[Statement]:
        response = await self._client.get(f'/sessions/{session_id}/statements')
        return [
            Statement.from_json(session_id, data)
            for data in response['statements']
        ]

    async def create_statement(
        self, session_id: int, code: str, kind: StatementKind=None
    ) -> Statement:

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

    async def get_statement(
        self, session_id: int, statement_id: int
    ) -> Statement:
        response = await self._client.get(
            f'/sessions/{session_id}/statements/{statement_id}'
        )
        return Statement.from_json(session_id, response)
