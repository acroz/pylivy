import time
import json
import warnings
from typing import Any, Dict, List

import requests
import pandas

from livy.client import LivyClient, Auth, Verify
from livy.models import (
    SessionKind,
    SessionState,
    StatementState,
    Output,
    SESSION_STATE_NOT_READY,
)
from livy.utils import polling_intervals


SERIALISE_DATAFRAME_TEMPLATE_SPARK = "{}.toJSON.collect.foreach(println)"
SERIALISE_DATAFRAME_TEMPLATE_PYSPARK = """
for _livy_client_serialised_row in {}.toJSON().collect():
    print(_livy_client_serialised_row)
"""
SERIALISE_DATAFRAME_TEMPLATE_SPARKR = r"""
cat(unlist(collect(toJSON({}))), sep = '\n')
"""


def _spark_serialise_dataframe_code(
    spark_dataframe_name: str, session_kind: SessionKind
) -> str:
    try:
        template = {
            SessionKind.SPARK: SERIALISE_DATAFRAME_TEMPLATE_SPARK,
            SessionKind.PYSPARK: SERIALISE_DATAFRAME_TEMPLATE_PYSPARK,
            SessionKind.PYSPARK3: SERIALISE_DATAFRAME_TEMPLATE_PYSPARK,
            SessionKind.SPARKR: SERIALISE_DATAFRAME_TEMPLATE_SPARKR,
        }[session_kind]
    except KeyError:
        raise RuntimeError(
            f"upload not supported for sessions of kind {session_kind}"
        )
    return template.format(spark_dataframe_name)


def _deserialise_dataframe(text: str) -> pandas.DataFrame:
    rows = []
    for line in text.split("\n"):
        if line:
            rows.append(json.loads(line))
    return pandas.DataFrame.from_records(rows)


def _dataframe_from_json_output(json_output: dict) -> pandas.DataFrame:
    try:
        fields = json_output["schema"]["fields"]
        columns = [field["name"] for field in fields]
        data = json_output["data"]
    except KeyError:
        raise ValueError("json output does not match expected structure")
    return pandas.DataFrame(data, columns=columns)


CREATE_DATAFRAME_TEMPLATE_SPARK = """
val {} = spark.read.json(spark.sparkContext.parallelize(List({})))
"""
CREATE_DATAFRAME_TEMPLATE_PYSPARK = """
{} = spark.read.json(spark.sparkContext.parallelize([{}]))
"""
CREATE_DATAFRAME_TEMPLATE_SPARKR = """
livy_client_temp_filename <- tempfile(fileext=".json")
livy_client_temp_file <- file(livy_client_temp_filename)
writeLines({1}, livy_client_temp_file)
close(livy_client_temp_file)
{0} <- read.json(livy_client_temp_filename)
persist({0}, "MEMORY_AND_DISK")
count({0})  # Force loading the data
file.remove(livy_client_temp_filename)
"""


def _spark_create_dataframe_code(
    session_kind: SessionKind,
    spark_dataframe_name: str,
    dataframe: pandas.DataFrame,
) -> str:

    try:
        template = {
            SessionKind.SPARK: CREATE_DATAFRAME_TEMPLATE_SPARK,
            SessionKind.PYSPARK: CREATE_DATAFRAME_TEMPLATE_PYSPARK,
            SessionKind.PYSPARK3: CREATE_DATAFRAME_TEMPLATE_PYSPARK,
            SessionKind.SPARKR: CREATE_DATAFRAME_TEMPLATE_SPARKR,
        }[session_kind]
    except KeyError:
        raise RuntimeError(
            f"upload not supported for sessions of kind {session_kind}"
        )

    df_as_json = dataframe.to_json(orient="records")

    # To make a string literal that works in Scala, Python and R, it needs to
    # be started/terminated with double quotes
    # Rather than roll our own repr() equivalent that forces double quotes, use
    # json.dumps to make a double-quote-terminated repr of the JSON string
    df_as_json_repr = json.dumps(df_as_json)

    return template.format(spark_dataframe_name, df_as_json_repr)


class LivySession:
    """Manages a remote Livy session and high-level interactions with it.

    :param url: The URL of the Livy server.
    :param session_id: The ID of the Livy session.
    :param auth: A requests-compatible auth object to use when making requests.
    :param verify: Either a boolean, in which case it controls whether we
        verify the server’s TLS certificate, or a string, in which case it must
        be a path to a CA bundle to use. Defaults to ``True``.
    :param requests_session: A specific ``requests.Session`` to use, allowing
        advanced customisation. The caller is responsible for closing the
        session.
    :param kind: The kind of session to create.
    :param echo: Whether to echo output printed in the remote session. Defaults
        to ``True``.
    :param check: Whether to raise an exception when a statement in the remote
        session fails. Defaults to ``True``.
    """

    def __init__(
        self,
        url: str,
        session_id: int,
        auth: Auth = None,
        verify: Verify = True,
        requests_session: requests.Session = None,
        kind: SessionKind = SessionKind.PYSPARK,
        echo: bool = True,
        check: bool = True,
    ) -> None:
        self.client = LivyClient(url, auth, verify, requests_session)
        self.session_id = session_id
        self.kind = kind
        self.echo = echo
        self.check = check

    @classmethod
    def create(
        cls,
        url: str,
        auth: Auth = None,
        verify: Verify = True,
        requests_session: requests.Session = None,
        kind: SessionKind = SessionKind.PYSPARK,
        proxy_user: str = None,
        jars: List[str] = None,
        py_files: List[str] = None,
        files: List[str] = None,
        driver_memory: str = None,
        driver_cores: int = None,
        executor_memory: str = None,
        executor_cores: int = None,
        num_executors: int = None,
        archives: List[str] = None,
        queue: str = None,
        name: str = None,
        spark_conf: Dict[str, Any] = None,
        heartbeat_timeout: int = None,
        echo: bool = True,
        check: bool = True,
    ) -> "LivySession":
        """Create a new Livy session.

        The py_files, files, jars and archives arguments are lists of URLs,
        e.g. ["s3://bucket/object", "hdfs://path/to/file", ...] and must be
        reachable by the Spark driver process. If the provided URL has no
        scheme, it's considered to be relative to the default file system
        configured in the Livy server.

        URLs in the py_files argument are copied to a temporary staging area
        and inserted into Python's sys.path ahead of the standard library
        paths. This allows you to import .py, .zip and .egg files in Python.

        URLs for jars, py_files, files and archives arguments are all copied to
        the same working directory on the Spark cluster.

        The driver_memory and executor_memory arguments have the same format as
        JVM memory strings with a size unit suffix ("k", "m", "g" or "t") (e.g.
        512m, 2g).

        See https://spark.apache.org/docs/latest/configuration.html for more
        information on Spark configuration properties.

        :param url: The URL of the Livy server.
        :param auth: A requests-compatible auth object to use when making
            requests.
        :param verify: Either a boolean, in which case it controls whether we
            verify the server’s TLS certificate, or a string, in which case it
            must be a path to a CA bundle to use. Defaults to ``True``.
        :param requests_session: A specific ``requests.Session`` to use,
            allowing advanced customisation. The caller is responsible for
            closing the session.
        :param kind: The kind of session to create.
        :param proxy_user: User to impersonate when starting the session.
        :param jars: URLs of jars to be used in this session.
        :param py_files: URLs of Python files to be used in this session.
        :param files: URLs of files to be used in this session.
        :param driver_memory: Amount of memory to use for the driver process
            (e.g. '512m').
        :param driver_cores: Number of cores to use for the driver process.
        :param executor_memory: Amount of memory to use per executor process
            (e.g. '512m').
        :param executor_cores: Number of cores to use for each executor.
        :param num_executors: Number of executors to launch for this session.
        :param archives: URLs of archives to be used in this session.
        :param queue: The name of the YARN queue to which submitted.
        :param name: The name of this session.
        :param spark_conf: Spark configuration properties.
        :param heartbeat_timeout: Optional Timeout in seconds to which session
            be automatically orphaned if no heartbeat is received.
        :param echo: Whether to echo output printed in the remote session.
            Defaults to ``True``.
        :param check: Whether to raise an exception when a statement in the
            remote session fails. Defaults to ``True``.
        """
        client = LivyClient(url, auth, verify, requests_session)
        session = client.create_session(
            kind,
            proxy_user,
            jars,
            py_files,
            files,
            driver_memory,
            driver_cores,
            executor_memory,
            executor_cores,
            num_executors,
            archives,
            queue,
            name,
            spark_conf,
            heartbeat_timeout,
        )
        client.close()
        return cls(
            url,
            session.session_id,
            auth,
            verify,
            requests_session,
            kind,
            echo,
            check,
        )

    def __enter__(self) -> "LivySession":
        self.wait()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def wait(self) -> None:
        """Wait for the session to be ready."""
        intervals = polling_intervals([0.1, 0.2, 0.3, 0.5], 1.0)
        while self.state in SESSION_STATE_NOT_READY:
            time.sleep(next(intervals))

    @property
    def state(self) -> SessionState:
        """The state of the managed Spark session."""
        session = self.client.get_session(self.session_id)
        if session is None:
            raise ValueError("session not found - it may have been shut down")
        return session.state

    def close(self) -> None:
        """Kill the managed Spark session."""
        self.client.delete_session(self.session_id)
        self.client.close()

    def run(self, code: str) -> Output:
        """Run some code in the managed Spark session.

        :param code: The code to run.
        """
        output = self._execute(code)
        if self.echo and output.text:
            print(output.text)
        if self.check:
            output.raise_for_status()
        return output

    def download(self, dataframe_name: str) -> pandas.DataFrame:
        """Evaluate and download a Spark dataframe from the managed session.

        :param dataframe_name: The name of the Spark dataframe to download.
        """
        code = _spark_serialise_dataframe_code(dataframe_name, self.kind)
        output = self._execute(code)
        output.raise_for_status()
        if output.text is None:
            raise RuntimeError("statement had no text output")
        return _deserialise_dataframe(output.text)

    def read(self, dataframe_name: str) -> pandas.DataFrame:
        """Evaluate and retrieve a Spark dataframe in the managed session.

        :param dataframe_name: The name of the Spark dataframe to read.

        .. deprecated:: 0.8.0
            Use :meth:`download` instead.
        """
        warnings.warn(
            "LivySession.read is deprecated and will be removed in a future "
            "version. Use LivySession.download instead.",
            DeprecationWarning,
        )
        return self.download(dataframe_name)

    def download_sql(self, query: str) -> pandas.DataFrame:
        """Evaluate a Spark SQL query and download the result.

        :param query: The Spark SQL query to evaluate.
        """
        if self.kind != SessionKind.SQL:
            raise ValueError("not a SQL session")
        output = self._execute(query)
        output.raise_for_status()
        if output.json is None:
            raise RuntimeError("statement had no JSON output")
        return _dataframe_from_json_output(output.json)

    def read_sql(self, code: str) -> pandas.DataFrame:
        """Evaluate a Spark SQL statement and retrieve the result.

        :param code: The Spark SQL statement to evaluate.

        .. deprecated:: 0.8.0
            Use :meth:`download_sql` instead.
        """
        warnings.warn(
            "LivySession.read_sql is deprecated and will be removed in a "
            "future version. Use LivySession.download_sql instead.",
            DeprecationWarning,
        )
        return self.download_sql(code)

    def upload(self, dataframe_name: str, data: pandas.DataFrame) -> None:
        """Upload a pandas dataframe to a Spark dataframe in the session.

        :param dataframe_name: The name of the Spark dataframe to create.
        :param data: The pandas dataframe to upload.
        """
        code = _spark_create_dataframe_code(self.kind, dataframe_name, data)
        output = self._execute(code)
        output.raise_for_status()

    def _execute(self, code: str) -> Output:
        statement = self.client.create_statement(self.session_id, code)
        intervals = polling_intervals([0.1, 0.2, 0.3, 0.5], 1.0)

        def waiting_for_output(statement):
            not_finished = statement.state in {
                StatementState.WAITING,
                StatementState.RUNNING,
            }
            available = statement.state == StatementState.AVAILABLE
            return not_finished or (available and statement.output is None)

        while waiting_for_output(statement):
            time.sleep(next(intervals))
            statement = self.client.get_statement(
                statement.session_id, statement.statement_id
            )

        if statement.output is None:
            raise RuntimeError("statement had no output")

        return statement.output
