import json
import logging
import re
import asyncio
from enum import Enum
from functools import total_ordering
from typing import NamedTuple, Optional, List

import aiohttp
import pandas


LOGGER = logging.getLogger(__name__)

DEFAULT_URL = 'http://localhost:8998'

SERIALISE_DATAFRAME_TEMPLATE_SPARK = '{}.toJSON.collect.foreach(println)'
SERIALISE_DATAFRAME_TEMPLATE_PYSPARK = """
for _livy_client_serialised_row in {}.toJSON().collect():
    print(_livy_client_serialised_row)
"""
SERIALISE_DATAFRAME_TEMPLATE_SPARKR = r"""
cat(unlist(collect(toJSON({}))), sep = '\n')
"""


def run_sync(coroutine):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(asyncio.ensure_future(coroutine))


def extract_serialised_dataframe(text):
    rows = []
    for line in text.split('\n'):
        if line:
            rows.append(json.loads(line))
    return pandas.DataFrame(rows)


class SessionKind(Enum):
    SPARK = 'spark'
    PYSPARK = 'pyspark'
    PYSPARK3 = 'pyspark3'
    SPARKR = 'sparkr'
    SQL = 'sql'
    SHARED = 'shared'


class Livy:

    def __init__(self, url=DEFAULT_URL, kind=SessionKind.PYSPARK, echo=True,
                 check=True):
        self.client = LivyClient(url)
        self.kind = kind
        self.session_id = None
        self.echo = echo
        self.check = check

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def start(self):
        session = run_sync(self.client.create_session(self.kind))
        self.session_id = session.session_id

    def close(self):

        async def _close():
            await self.client.delete_session(self.session_id)
            await self.client.close()

        run_sync(_close())

    def run(self, code):
        output = run_sync(self._execute(code))
        if self.echo and output.text:
            print(output.text)
        if self.check:
            output.raise_for_status()
        return output

    def read(self, dataframe_name):

        try:
            template = {
                SessionKind.SPARK: SERIALISE_DATAFRAME_TEMPLATE_SPARK,
                SessionKind.PYSPARK: SERIALISE_DATAFRAME_TEMPLATE_PYSPARK,
                SessionKind.SPARKR: SERIALISE_DATAFRAME_TEMPLATE_SPARKR
            }[self.kind]
        except KeyError:
            raise RuntimeError(
                f'read not supported for sessions of kind {self.kind}'
            )

        code = template.format(dataframe_name)
        output = run_sync(self._execute(code))
        output.raise_for_status()

        return extract_serialised_dataframe(output.text)

    async def _execute(self, code):
        await wait_until_session_ready(self.client, self.session_id)
        LOGGER.info('Beginning code statement execution')
        statement = await self.client.create_statement(self.session_id, code)
        await wait_until_statement_finished(
            self.client, statement.session_id, statement.statement_id
        )
        statement = await self.client.get_statement(
            statement.session_id, statement.statement_id
        )
        LOGGER.info(
            'Completed code statement execution with status '
            f'{statement.output.status}'
        )
        return statement.output


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


@total_ordering
class Version:

    def __init__(self, version):
        match = re.match(r'(\d+)\.(\d+)\.(\d+)(\S+)$', version)
        if match is None:
            raise ValueError(f'invalid version string {version!r}')
        self.major, self.minor, self.dot, self.extension = match.groups()

    def __repr__(self):
        name = self.__class__.__name__
        return f'{name}({self.major}.{self.minor}.{self.dot}{self.extension})'

    def __eq__(self, other):
        return (
            self.major == other.major and
            self.minor == other.minor and
            self.dot == other.dot
        )

    def __lt__(self, other):
        if self.major < other.major:
            return True
        elif self.major == other.major:
            if self.minor < other.minor:
                return True
            elif self.minor == other.minor:
                return self.dot < other.dot
            else:
                return False
        else:
            return False


async def wait_until_session_ready(client, session_id, interval=1.0):

    async def ready():
        session = await client.get_session(session_id)
        return session.state not in {SessionState.NOT_STARTED,
                                     SessionState.STARTING}

    while not await ready():
        await asyncio.sleep(interval)


async def wait_until_statement_finished(client, session_id, statement_id,
                                        interval=1.0):

    async def finished():
        statement = await client.get_statement(session_id, statement_id)
        return statement.state not in {StatementState.WAITING,
                                       StatementState.RUNNING}

    while not await finished():
        await asyncio.sleep(interval)


VALID_LEGACY_SESSION_KINDS = {
    SessionKind.SPARK, SessionKind.PYSPARK, SessionKind.PYSPARK3,
    SessionKind.SPARKR
}
VALID_SESSION_KINDS = {
    SessionKind.SPARK, SessionKind.PYSPARK, SessionKind.SPARKR,
    SessionKind.SQL, SessionKind.SHARED
}
VALID_STATEMENT_KINDS = {
    SessionKind.SPARK, SessionKind.PYSPARK, SessionKind.SPARKR,
    SessionKind.SQL
}


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
            if kind not in VALID_STATEMENT_KINDS:
                raise ValueError(f'invalid code kind for statement {kind}')
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


class SparkRuntimeError(Exception):

    def __init__(self, ename, evalue, traceback):
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback

    def __repr__(self):
        name = self.__class__.__name__
        components = []
        if self.ename is not None:
            components.append(f'ename={self.ename!r}')
        if self.evalue is not None:
            components.append(f'evalue={self.evalue!r}')
        return f'{name}({", ".join(components)})'


class OutputStatus(Enum):
    OK = 'ok'
    ERROR = 'error'


_Output = NamedTuple(
    '_Output',
    [
        ('status', OutputStatus),
        ('text', Optional[str]),
        ('json', Optional[dict]),
        ('ename', Optional[str]),
        ('evalue', Optional[str]),
        ('traceback', Optional[List[str]])
    ]
)


class Output(_Output):

    @classmethod
    def from_json(cls, data):
        if data is None:
            return None
        return cls(
            OutputStatus(data['status']),
            data.get('data', {}).get('text/plain'),
            data.get('data', {}).get('application/json'),
            data.get('ename'),
            data.get('evalue'),
            data.get('traceback')
        )

    def raise_for_status(self):
        if self.status == OutputStatus.ERROR:
            raise SparkRuntimeError(self.ename, self.evalue, self.traceback)


class StatementState(Enum):
    WAITING = 'waiting'
    RUNNING = 'running'
    AVAILABLE = 'available'
    ERROR = 'error'
    CANCELLING = 'cancelling'
    CANCELLED = 'cancelled'


_Statement = NamedTuple(
    '_Statement',
    [
        ('session_id', int),
        ('statement_id', int),
        ('state', StatementState),
        ('output', Optional[Output])]
)


class Statement(_Statement):

    @classmethod
    def from_json(cls, session_id, data):
        return cls(
            session_id, data['id'], StatementState(data['state']),
            Output.from_json(data['output'])
        )


class SessionState(Enum):
    NOT_STARTED = 'not_started'
    STARTING = 'starting'
    IDLE = 'idle'
    BUSY = 'busy'
    SHUTTING_DOWN = 'shutting_down'
    ERROR = 'error'
    DEAD = 'dead'
    SUCCESS = 'success'


_Session = NamedTuple(
    '_Session',
    [('session_id', int), ('kind', SessionKind), ('state', SessionState)]
)


class Session(_Session):

    @classmethod
    def from_json(cls, data):
        return cls(
            data['id'],
            SessionKind(data['kind']),
            SessionState(data['state'])
        )
