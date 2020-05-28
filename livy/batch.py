import time
from typing import Any, Dict, List

import requests

from livy.client import LivyClient, Auth, Verify
from livy.models import SessionState, SESSION_STATE_FINISHED
from livy.utils import polling_intervals


class LivyBatch:
    """Manages a remote Livy batch and high-level interactions with it.

    :param url: The URL of the Livy server.
    :param batch_id: The ID of the Livy batch.
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
        batch_id: int,
        auth: Auth = None,
        verify: Verify = True,
        requests_session: requests.Session = None,
    ) -> None:
        self.client = LivyClient(url, auth, verify, requests_session)
        self.batch_id = batch_id

    @classmethod
    def create(
        cls,
        url: str,
        file: str,
        auth: Auth = None,
        verify: Verify = True,
        requests_session: requests.Session = None,
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
    ) -> "LivyBatch":
        """Create a new Livy batch session.

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
        :param file: File containing the application to execute.
        :param auth: A requests-compatible auth object to use when making
            requests.
        :param verify: Either a boolean, in which case it controls whether we
            verify the server’s TLS certificate, or a string, in which case it
            must be a path to a CA bundle to use. Defaults to ``True``.
        :param requests_session: A specific ``requests.Session`` to use,
            allowing advanced customisation. The caller is responsible for
            closing the session.
        :param class_name: Application Java/Spark main class.
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
        client = LivyClient(url, auth, verify, requests_session)
        batch = client.create_batch(
            file,
            class_name,
            args,
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
        client.close()
        return cls(url, batch.batch_id, auth, verify, requests_session)

    def wait(self) -> SessionState:
        """Wait for the batch session to finish."""

        intervals = polling_intervals([0.1, 0.5, 1.0, 3.0], 5.0)

        while True:
            state = self.state
            if state in SESSION_STATE_FINISHED:
                break
            time.sleep(next(intervals))

        return state

    @property
    def state(self) -> SessionState:
        """The state of the managed Spark batch."""
        batch = self.client.get_batch(self.batch_id)
        if batch is None:
            raise ValueError(
                "batch session not found - it may have been shut down"
            )
        return batch.state

    def log(self, from_: int = None, size: int = None) -> List[str]:
        """Get logs for this Spark batch.

        :param from_: The line number to start getting logs from.
        :param size: The number of lines of logs to get.
        """
        log = self.client.get_batch_log(self.batch_id, from_, size)
        if log is None:
            raise ValueError(
                "batch session not found - it may have been shut down"
            )
        return log.lines

    def kill(self) -> None:
        """Kill the managed Spark batch session."""
        self.client.delete_batch(self.batch_id)
        self.client.close()
