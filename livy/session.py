import time
import json
from typing import Any, Dict, Iterable, Iterator, Optional

import pandas

from livy.client import LivyClient, Auth
from livy.models import SessionKind, SessionState, StatementState, Output


SERIALISE_DATAFRAME_TEMPLATE_SPARK = '{}.toJSON.collect.foreach(println)'
SERIALISE_DATAFRAME_TEMPLATE_PYSPARK = """
for _livy_client_serialised_row in {}.toJSON().collect():
    print(_livy_client_serialised_row)
"""
SERIALISE_DATAFRAME_TEMPLATE_SPARKR = r"""
cat(unlist(collect(toJSON({}))), sep = '\n')
"""


def serialise_dataframe_code(
    dataframe_name: str, session_kind: SessionKind
) -> str:
    try:
        template = {
            SessionKind.SPARK: SERIALISE_DATAFRAME_TEMPLATE_SPARK,
            SessionKind.PYSPARK: SERIALISE_DATAFRAME_TEMPLATE_PYSPARK,
            SessionKind.PYSPARK3: SERIALISE_DATAFRAME_TEMPLATE_PYSPARK,
            SessionKind.SPARKR: SERIALISE_DATAFRAME_TEMPLATE_SPARKR
        }[session_kind]
    except KeyError:
        raise RuntimeError(
            f'read not supported for sessions of kind {session_kind}'
        )
    return template.format(dataframe_name)


def deserialise_dataframe(text: str) -> pandas.DataFrame:
    rows = []
    for line in text.split('\n'):
        if line:
            rows.append(json.loads(line))
    return pandas.DataFrame.from_records(rows)


def dataframe_from_json_output(json_output: dict) -> pandas.DataFrame:
    try:
        fields = json_output['schema']['fields']
        columns = [field['name'] for field in fields]
        data = json_output['data']
    except KeyError:
        raise ValueError('json output does not match expected structure')
    return pandas.DataFrame(data, columns=columns)


def polling_intervals(
    start: Iterable[float], rest: float, max_duration: float = None
) -> Iterator[float]:

    def _intervals():
        yield from start
        while True:
            yield rest

    cumulative = 0.0
    for interval in _intervals():
        cumulative += interval
        if max_duration is not None and cumulative > max_duration:
            break
        yield interval


class LivySession:

    def __init__(
        self, url: str, auth: Auth, kind: SessionKind = SessionKind.PYSPARK,
        spark_conf: Dict[str, Any] = None, echo: bool = True,
        check: bool = True
    ) -> None:
        self.client = LivyClient(url, auth)
        self.kind = kind
        self.echo = echo
        self.check = check
        self.session_id: Optional[int] = None
        self.spark_conf = spark_conf

    def __enter__(self) -> 'LivySession':
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def start(self) -> None:
        session = self.client.create_session(self.kind, self.spark_conf)
        self.session_id = session.session_id

        not_ready = {SessionState.NOT_STARTED, SessionState.STARTING}
        intervals = polling_intervals([0.1, 0.2, 0.3, 0.5], 1.0)

        while self.state in not_ready:
            time.sleep(next(intervals))

    @property
    def state(self) -> SessionState:
        if self.session_id is None:
            raise ValueError('session not yet started')
        session = self.client.get_session(self.session_id)
        if session is None:
            raise ValueError('session not found - it may have been shut down')
        return session.state

    def close(self) -> None:
        if self.session_id is not None:
            self.client.delete_session(self.session_id)
        self.client.close()

    def run(self, code: str) -> Output:
        output = self._execute(code)
        if self.echo and output.text:
            print(output.text)
        if self.check:
            output.raise_for_status()
        return output

    def read(self, dataframe_name: str) -> pandas.DataFrame:
        code = serialise_dataframe_code(dataframe_name, self.kind)
        output = self._execute(code)
        output.raise_for_status()
        if output.text is None:
            raise RuntimeError('statement had no text output')
        return deserialise_dataframe(output.text)

    def read_sql(self, code: str) -> pandas.DataFrame:
        if self.kind != SessionKind.SQL:
            raise ValueError('not a SQL session')
        output = self._execute(code)
        output.raise_for_status()
        if output.json is None:
            raise RuntimeError('statement had no JSON output')
        return dataframe_from_json_output(output.json)

    def _execute(self, code: str) -> Output:
        if self.session_id is None:
            raise ValueError('session not yet started')

        statement = self.client.create_statement(self.session_id, code)

        not_finished = {StatementState.WAITING, StatementState.RUNNING}
        intervals = polling_intervals([0.1, 0.2, 0.3, 0.5], 1.0)

        while statement.state in not_finished:
            time.sleep(next(intervals))
            statement = self.client.get_statement(
                statement.session_id, statement.statement_id
            )

        if statement.output is None:
            raise RuntimeError('statement had no output')

        return statement.output
