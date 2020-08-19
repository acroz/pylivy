import logging
from typing import Any, Union, Dict, List, Tuple, Optional

import requests

from livy.models import (
    Version,
    Session,
    SessionKind,
    Statement,
    StatementKind,
    Batch,
    BatchLog,
)

Auth = Union[requests.auth.AuthBase, Tuple[str, str]]
Verify = Union[bool, str]


LOGGER = logging.getLogger(__name__)


VALID_LEGACY_SESSION_KINDS = {
    SessionKind.SPARK,
    SessionKind.PYSPARK,
    SessionKind.PYSPARK3,
    SessionKind.SPARKR,
}
VALID_SESSION_KINDS = {
    SessionKind.SPARK,
    SessionKind.PYSPARK,
    SessionKind.SPARKR,
    SessionKind.SQL,
    SessionKind.SHARED,
}


class JsonClient:
    """A wrapper for a requests session for JSON formatted requests.

    This client handles appending endpoints on to a common hostname,
    deserialising the response as JSON and raising an exception when an error
    HTTP code is received.
    """

    def __init__(
        self,
        url: str,
        auth: Auth = None,
        verify: Verify = True,
        requests_session: requests.Session = None,
    ) -> None:

        self.url = url
        self.auth = auth
        self.verify = verify

        if requests_session is None:
            self.session = requests.Session()
            self.managed_session = True
        else:
            self.session = requests_session
            self.managed_session = False

    def close(self) -> None:
        if self.managed_session:
            self.session.close()

    def get(self, endpoint: str = "", params: dict = None) -> dict:
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, data: dict = None) -> dict:
        return self._request("POST", endpoint, data)

    def delete(self, endpoint: str = "") -> dict:
        return self._request("DELETE", endpoint)

    def _request(
        self,
        method: str,
        endpoint: str,
        data: dict = None,
        params: dict = None,
    ) -> dict:
        url = self.url.rstrip("/") + endpoint
        response = self.session.request(
            method,
            url,
            auth=self.auth,
            verify=self.verify,
            json=data,
            params=params,
        )
        response.raise_for_status()
        return response.json()


class LivyClient:
    """A client for sending requests to a Livy server.

    :param url: The URL of the Livy server.
    :param auth: A requests-compatible auth object to use when making requests.
    :param verify: Either a boolean, in which case it controls whether we
        verify the server’s TLS certificate, or a string, in which case it must
        be a path to a CA bundle to use. Defaults to ``True``.
    :param requests_session: A specific ``requests.Session`` to use, allowing
        advanced customisation. The caller is responsible for closing the
        session.
    """

    def __init__(
        self,
        url: str,
        auth: Auth = None,
        verify: Verify = True,
        requests_session: requests.Session = None,
    ) -> None:
        self._client = JsonClient(url, auth, verify, requests_session)
        self._server_version_cache: Optional[Version] = None

    def close(self) -> None:
        """Close the underlying requests session, if managed by this class."""
        self._client.close()

    def server_version(self) -> Version:
        """Get the version of Livy running on the server."""
        if self._server_version_cache is None:
            data = self._client.get("/version")
            self._server_version_cache = Version(data["version"])
        return self._server_version_cache

    def legacy_server(self) -> bool:
        """Determine if the server is running a legacy version.

        Legacy versions support different session kinds than newer versions of
        Livy.
        """
        version = self.server_version()
        return version < Version("0.5.0-incubating")

    def list_sessions(self) -> List[Session]:
        """List all the active sessions in Livy."""
        data = self._client.get("/sessions")
        return [Session.from_json(item) for item in data["sessions"]]

    def create_session(
        self,
        kind: SessionKind,
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
    ) -> Session:
        """Create a new session in Livy.

        The py_files, files, jars and archives arguments are lists of URLs,
        e.g. ["s3://bucket/object", "hdfs://path/to/file", ...] and must be
        reachable by the Spark driver process.  If the provided URL has no
        scheme, it's considered to be relative to the default file system
        configured in the Livy server.

        URLs in the py_files argument are copied to a temporary staging area
        and inserted into Python's sys.path ahead of the standard library
        paths.  This allows you to import .py, .zip and .egg files in Python.

        URLs for jars, py_files, files and archives arguments are all copied
        to the same working directory on the Spark cluster.

        The driver_memory and executor_memory arguments have the same format
        as JVM memory strings with a size unit suffix ("k", "m", "g" or "t")
        (e.g. 512m, 2g).

        See https://spark.apache.org/docs/latest/configuration.html for more
        information on Spark configuration properties.

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
        """
        if self.legacy_server():
            valid_kinds = VALID_LEGACY_SESSION_KINDS
        else:
            valid_kinds = VALID_SESSION_KINDS

        if kind not in valid_kinds:
            raise ValueError(
                f"{kind} is not a valid session kind for a Livy server of "
                f"this version (should be one of {valid_kinds})"
            )

        interactive_session_params = {"kind": kind.value}
        if heartbeat_timeout is not None:
            interactive_session_params[
                "heartbeatTimeoutInSecond"
            ] = heartbeat_timeout
        common_params = _new_session_body(
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
        )
        body = {**interactive_session_params, **common_params}

        data = self._client.post("/sessions", data=body)
        return Session.from_json(data)

    def get_session(self, session_id: int) -> Optional[Session]:
        """Get information about a session.

        :param session_id: The ID of the session.
        """
        try:
            data = self._client.get(f"/sessions/{session_id}")
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise
        return Session.from_json(data)

    def delete_session(self, session_id: int) -> None:
        """Kill a session.

        :param session_id: The ID of the session.
        """
        self._client.delete(f"/sessions/{session_id}")

    def list_statements(self, session_id: int) -> List[Statement]:
        """Get all the statements in a session.

        :param session_id: The ID of the session.
        """
        response = self._client.get(f"/sessions/{session_id}/statements")
        return [
            Statement.from_json(session_id, data)
            for data in response["statements"]
        ]

    def create_statement(
        self, session_id: int, code: str, kind: StatementKind = None
    ) -> Statement:
        """Run a statement in a session.

        :param session_id: The ID of the session.
        :param code: The code to execute.
        :param kind: The kind of code to execute.
        """

        data = {"code": code}

        if kind is not None:
            if self.legacy_server():
                LOGGER.warning("statement kind ignored on Livy<0.5.0")
            data["kind"] = kind.value

        response = self._client.post(
            f"/sessions/{session_id}/statements", data=data
        )
        return Statement.from_json(session_id, response)

    def get_statement(self, session_id: int, statement_id: int) -> Statement:
        """Get information about a statement in a session.

        :param session_id: The ID of the session.
        :param statement_id: The ID of the statement.
        """
        response = self._client.get(
            f"/sessions/{session_id}/statements/{statement_id}"
        )
        return Statement.from_json(session_id, response)

    def create_batch(
        self,
        file: str,
        class_name: str = None,
        args: List[str] = None,
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
    ) -> Batch:
        """Create a new batch in Livy.

        The py_files, files, jars and archives arguments are lists of URLs,
        e.g. ["s3://bucket/object", "hdfs://path/to/file", ...] and must be
        reachable by the Spark driver process.  If the provided URL has no
        scheme, it's considered to be relative to the default file system
        configured in the Livy server.

        URLs in the py_files argument are copied to a temporary staging area
        and inserted into Python's sys.path ahead of the standard library
        paths.  This allows you to import .py, .zip and .egg files in Python.

        URLs for jars, py_files, files and archives arguments are all copied
        to the same working directory on the Spark cluster.

        The driver_memory and executor_memory arguments have the same format
        as JVM memory strings with a size unit suffix ("k", "m", "g" or "t")
        (e.g. 512m, 2g).

        See https://spark.apache.org/docs/latest/configuration.html for more
        information on Spark configuration properties.

        :param file: File containing the application to execute.
        :param class_name: Application Java/Spark main class.
        :param args: An array of strings to be passed to the Spark app.
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
        """

        batch_session_params: Dict[str, Any] = {"file": file}
        if class_name is not None:
            batch_session_params["className"] = class_name
        if args is not None:
            batch_session_params["args"] = args
        common_params = _new_session_body(
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
        )
        body = {**batch_session_params, **common_params}

        data = self._client.post("/batches", data=body)
        return Batch.from_json(data)

    def delete_batch(self, batch_id: int) -> None:
        """Kill a batch session.

        :param batch_id: The ID of the session.
        """
        self._client.delete(f"/batches/{batch_id}")

    def get_batch(self, batch_id: int) -> Optional[Batch]:
        """Get information about a batch.

        :param batch_id: The ID of the batch.
        """
        try:
            data = self._client.get(f"/batches/{batch_id}")
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise
        return Batch.from_json(data)

    def get_batch_log(
        self, batch_id: int, from_: int = None, size: int = None
    ) -> Optional[BatchLog]:
        """Get logs for a batch.

        :param batch_id: The ID of the batch.
        :param from_: The line number to start getting logs from.
        :param size: The number of lines of logs to get.
        """
        params = {}
        if from_ is not None:
            params["from"] = from_
        if size is not None:
            params["size"] = size
        try:
            data = self._client.get(f"/batches/{batch_id}/log", params=params)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            else:
                raise
        return BatchLog.from_json(data)

    def list_batches(self) -> List[Batch]:
        """List all the active batches in Livy."""
        response = self._client.get("/batches")
        return [Batch.from_json(data) for data in response["sessions"]]


def _new_session_body(
    proxy_user: Optional[str],
    jars: Optional[List[str]],
    py_files: Optional[List[str]],
    files: Optional[List[str]],
    driver_memory: Optional[str],
    driver_cores: Optional[int],
    executor_memory: Optional[str],
    executor_cores: Optional[int],
    num_executors: Optional[int],
    archives: Optional[List[str]],
    queue: Optional[str],
    name: Optional[str],
    spark_conf: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    body: Dict[str, Any] = {}
    if proxy_user is not None:
        body["proxyUser"] = proxy_user
    if jars is not None:
        body["jars"] = jars
    if py_files is not None:
        body["pyFiles"] = py_files
    if files is not None:
        body["files"] = files
    if driver_memory is not None:
        body["driverMemory"] = driver_memory
    if driver_cores is not None:
        body["driverCores"] = driver_cores
    if executor_memory is not None:
        body["executorMemory"] = executor_memory
    if executor_cores is not None:
        body["executorCores"] = executor_cores
    if num_executors is not None:
        body["numExecutors"] = num_executors
    if archives is not None:
        body["archives"] = archives
    if queue is not None:
        body["queue"] = queue
    if name is not None:
        body["name"] = name
    if spark_conf is not None:
        body["conf"] = spark_conf
    return body
