import json
import logging
import re
import asyncio
from enum import Enum
from functools import total_ordering

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
        self._client = JsonClient(url)
        self.manager = SessionManager(self._client)
        self.kind = kind
        self.session = None
        self.echo = echo
        self.check = check

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def start(self):
        loop = asyncio.get_event_loop()
        self.session = loop.run_until_complete(
            asyncio.ensure_future(
                self.manager.new(self.kind)
            )
        )

    def close(self):

        async def _close():
            await self.session.kill()
            await self._client.close()

        loop = asyncio.get_event_loop()
        self.session = loop.run_until_complete(
            asyncio.ensure_future(_close())
        )

    def run(self, code):
        output = self._execute_sync(code)
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
        output = self._execute_sync(code)
        output.raise_for_status()

        return extract_serialised_dataframe(output.text)

    def _execute_sync(self, code):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(
            asyncio.ensure_future(self._execute(code))
        )

    async def _execute(self, code):
        await self.session.wait_until_ready()
        LOGGER.info('Beginning code statement execution')
        statement = await self.session.run_statement(code)
        await statement.wait_until_finished()
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

    async def server_version(self):
        if self._server_version_cache is None:
            data = await self.get('/version')
            self._server_version_cache = Version(data['version'])
        return self._server_version_cache

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


class SessionManager:

    def __init__(self, client):
        self._client = client
        self._server_version_cache = None

    async def list(self):
        data = await self._client.get('/sessions')
        return [
            Session.from_json(self._client, data) for item in data['sessions']
        ]

    async def new(self, kind):
        valid_kinds = await self._valid_session_kinds()
        if kind not in valid_kinds:
            raise ValueError(
                f'{kind} is not a valid session kind (one of {valid_kinds})'
            )
        data = await self._client.post('/sessions', data={'kind': kind.value})
        return Session.from_json(self._client, data)

    async def get(self, session_id):
        try:
            data = await self._client.get(f'/sessions/{session_id}')
        except aiohttp.ClientResponseError as e:
            if e.code == 404:
                return None
            else:
                raise
        return Session.from_json(self._client, data)

    async def _valid_session_kinds(self):
        version = await self._client.server_version()
        if version < Version('0.5.0-incubating'):
            return VALID_LEGACY_SESSION_KINDS
        else:
            return VALID_SESSION_KINDS


class SessionState(Enum):
    NOT_STARTED = 'not_started'
    STARTING = 'starting'
    IDLE = 'idle'
    BUSY = 'busy'
    SHUTTING_DOWN = 'shutting_down'
    ERROR = 'error'
    DEAD = 'dead'
    SUCCESS = 'success'


class Session:

    def __init__(self, client, id_, kind, state):
        self._client = client
        self.id_ = id_
        self.kind = kind
        self.state = state

    @classmethod
    def from_json(cls, url, data):
        return cls(
            url,
            data['id'],
            SessionKind(data['kind']),
            SessionState(data['state'])
        )

    def __repr__(self):
        name = self.__class__.__name__
        return (
            f'{name}(id_={self.id_}, kind={self.kind}, state={self.state})'
        )

    async def run_statement(self, code, kind=None):

        data = {'code': code}

        if kind is not None:

            version = await self._client.server_version()
            if version < Version('0.5.0-incubating'):
                LOGGER.warning('statement kind ignored on Livy<0.5.0')

            if kind not in VALID_STATEMENT_KINDS:
                raise ValueError(f'invalid code kind for statement {kind}')

            data['kind'] = kind.value

        response = await self._client.post(
            f'/sessions/{self.id_}/statements',
            data=data
        )
        return Statement.from_json(self._client, self.id_, response)

    async def get_statements(self):
        response = await self._client.get(f'/sessions/{self.id_}/statements')
        return [
            Statement.from_json(self._client, self.id_, data)
            for data in response['statements']
        ]

    def ready(self):
        non_ready_states = {SessionState.NOT_STARTED, SessionState.STARTING}
        return self.state not in non_ready_states

    async def refresh(self):
        response = await self._client.get(f'/sessions/{self.id_}/state')
        self.state = SessionState(response['state'])

    async def wait_until_ready(self, interval=1.0):
        if not self.ready():
            LOGGER.info('Waiting for session to be ready')
            while not self.ready():
                await asyncio.sleep(interval)
                await self.refresh()
            LOGGER.info('Session ready')

    async def kill(self):
        await self._client.delete(f'/sessions/{self.id_}')


class StatementState(Enum):
    WAITING = 'waiting'
    RUNNING = 'running'
    AVAILABLE = 'available'
    ERROR = 'error'
    CANCELLING = 'cancelling'
    CANCELLED = 'cancelled'


class Statement:

    def __init__(self, client, session_id, id_, state, output):
        self._client = client
        self.session_id = session_id
        self.id_ = id_
        self.state = state
        self.output = output

    @classmethod
    def from_json(cls, url, session_id, data):
        return cls(
            url, session_id, data['id'], StatementState(data['state']),
            Output.from_json(data['output'])
        )

    def __repr__(self):
        name = self.__class__.__name__
        return (
            f'{name}(session_id={self.session_id}, id_={self.id_}, '
            f'state={self.state}, output={self.output!r})'
        )

    async def refresh(self):
        response = await self._client.get(
            f'/sessions/{self.session_id}/statements/{self.id_}'
        )

        if response['id'] != self.id_:
            raise RuntimeError('mismatched ids')

        self.state = StatementState(response['state'])
        self.output = Output.from_json(response['output'])

    async def wait_until_finished(self, interval=1.0):
        while self.state in {StatementState.WAITING, StatementState.RUNNING}:
            await asyncio.sleep(interval)
            await self.refresh()


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


class Output:

    def __init__(self, status, text=None, json=None, ename=None, evalue=None,
                 traceback=None):
        self.status = status
        self.text = text
        self.json = json
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback

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

    def __repr__(self):
        name = self.__class__.__name__
        components = [f'status={self.status}']
        if self.text is not None:
            components.append(f'text={self.text!r}')
        if self.json is not None:
            components.append(f'json={self.json!r}')
        if self.ename is not None:
            components.append(f'ename={self.ename!r}')
        if self.evalue is not None:
            components.append(f'evalue={self.evalue!r}')
        if self.traceback is not None:
            components.append(f'traceback={self.traceback!r}')
        return f'{name}({", ".join(components)})'

    def raise_for_status(self):
        if self.status == OutputStatus.ERROR:
            raise SparkRuntimeError(self.ename, self.evalue, self.traceback)
