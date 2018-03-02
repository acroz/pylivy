import json
import logging
import asyncio

import pandas

from livy.models import (  # noqa: F401
    Session, SessionKind, SessionState,
    Statement, StatementState,
    SparkRuntimeError
)
from livy.client import LivyClient


LOGGER = logging.getLogger(__name__)

SERIALISE_DATAFRAME_TEMPLATE_SPARK = '{}.toJSON.collect.foreach(println)'
SERIALISE_DATAFRAME_TEMPLATE_PYSPARK = """
for _livy_client_serialised_row in {}.toJSON().collect():
    print(_livy_client_serialised_row)
"""
SERIALISE_DATAFRAME_TEMPLATE_SPARKR = r"""
cat(unlist(collect(toJSON({}))), sep = '\n')
"""


def serialise_dataframe_code(dataframe_name, session_kind):
    try:
        template = {
            SessionKind.SPARK: SERIALISE_DATAFRAME_TEMPLATE_SPARK,
            SessionKind.PYSPARK: SERIALISE_DATAFRAME_TEMPLATE_PYSPARK,
            SessionKind.SPARKR: SERIALISE_DATAFRAME_TEMPLATE_SPARKR
        }[session_kind]
    except KeyError:
        raise RuntimeError(
            f'read not supported for sessions of kind {session_kind}'
        )
    return template.format(dataframe_name)


def run_sync(coroutine):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(asyncio.ensure_future(coroutine))


def deserialise_dataframe(text):
    rows = []
    for line in text.split('\n'):
        if line:
            rows.append(json.loads(line))
    return pandas.DataFrame(rows)


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


class BaseLivySession:

    def __init__(self, url, kind=SessionKind.PYSPARK, echo=True, check=True):
        self.client = LivyClient(url)
        self.kind = kind
        self.session_id = None
        self.echo = echo
        self.check = check

    async def _close(self):
        await self.client.delete_session(self.session_id)
        await self.client.close()

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


class LivySession(BaseLivySession):

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def start(self):
        session = run_sync(self.client.create_session(self.kind))
        self.session_id = session.session_id

    def close(self):
        run_sync(self._close())

    def run(self, code):
        output = run_sync(self._execute(code))
        if self.echo and output.text:
            print(output.text)
        if self.check:
            output.raise_for_status()
        return output

    def read(self, dataframe_name):
        code = serialise_dataframe_code(dataframe_name, self.kind)
        output = run_sync(self._execute(code))
        output.raise_for_status()
        return deserialise_dataframe(output.text)
