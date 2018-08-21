import logging
from typing import Any, Dict, List, Optional

import requests

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
        self.session = requests.Session()

    def close(self) -> None:
        self.session.close()

    def get(self, endpoint: str='') -> dict:
        return self._request('GET', endpoint)

    def post(self, endpoint: str, data: dict=None) -> dict:
        return self._request('POST', endpoint, data)

    def delete(self, endpoint: str='') -> dict:
        return self._request('DELETE', endpoint)

    def _request(
        self, method: str, endpoint: str, data: dict=None
    ) -> dict:
        url = self.url.rstrip('/') + endpoint
        response = self.session.request(method, url, json=data)
        response.raise_for_status()
        return response.json()


class LivyClient:

    def __init__(self, url: str) -> None:
        self._client = JsonClient(url)
        self._server_version_cache: Optional[Version] = None

    def close(self) -> None:
        self._client.close()

    def server_version(self) -> Version:
        if self._server_version_cache is None:
            data = self._client.get('/version')
            self._server_version_cache = Version(data['version'])
        return self._server_version_cache

    def legacy_server(self) -> bool:
        version = self.server_version()
        return version < Version('0.5.0-incubating')

    def list_sessions(self) -> List[Session]:
        data = self._client.get('/sessions')
        return [Session.from_json(item) for item in data['sessions']]

    def create_session(
            self, kind: SessionKind, spark_conf: Dict[str, Any]=None
    ) -> Session:
        if self.legacy_server():
            valid_kinds = VALID_LEGACY_SESSION_KINDS
        else:
            valid_kinds = VALID_SESSION_KINDS

        if kind not in valid_kinds:
            raise ValueError(
                f'{kind} is not a valid session kind for a Livy server of '
                f'this version (should be one of {valid_kinds})'
            )

        body = {'kind': kind.value}
        if spark_conf is not None:
            body['conf'] = spark_conf

        data = self._client.post('/sessions', data=body)
        return Session.from_json(data)

    def get_session(self, session_id: int) -> Optional[Session]:
        try:
            data = self._client.get(f'/sessions/{session_id}')
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise
        return Session.from_json(data)

    def delete_session(self, session_id: int) -> None:
        self._client.delete(f'/sessions/{session_id}')

    def list_statements(self, session_id: int) -> List[Statement]:
        response = self._client.get(f'/sessions/{session_id}/statements')
        return [
            Statement.from_json(session_id, data)
            for data in response['statements']
        ]

    def create_statement(
        self, session_id: int, code: str, kind: StatementKind=None
    ) -> Statement:

        data = {'code': code}

        if kind is not None:
            if self.legacy_server():
                LOGGER.warning('statement kind ignored on Livy<0.5.0')
            data['kind'] = kind.value

        response = self._client.post(
            f'/sessions/{session_id}/statements',
            data=data
        )
        return Statement.from_json(session_id, response)

    def get_statement(
        self, session_id: int, statement_id: int
    ) -> Statement:
        response = self._client.get(
            f'/sessions/{session_id}/statements/{statement_id}'
        )
        return Statement.from_json(session_id, response)
